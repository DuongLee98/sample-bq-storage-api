package examples.api;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import examples.metrics.PagedResult;
import examples.metrics.QueryMetrics;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SRAPIShardingFetcher {
    // ====== CONFIG FIELDS ======
    private final String projectId;
    private final String dataset;
    private final String location; // ví dụ: "asia-southeast1" hoặc "US"
    private final String agentIdPrefix; // không dùng ở Storage API nhưng giữ cho đồng nhất chữ ký
    private final ServiceAccountCredentials creds; // LƯU Ý: nếu dùng ADC

    // Storage API client (đóng sau khi dùng xong)
    private final BigQueryReadClient readClient;

    public SRAPIShardingFetcher(String projectId,
                                String dataset,
                                String location,
                                String serviceAccountJsonPath,
                                String agentIdPrefix,
                                boolean ignoredUseCacheForStorageApi // chỉ để đồng nhất ctor; Storage API không dùng cache
    ) throws Exception {
        this.projectId = projectId;
        this.dataset = dataset;
        this.location = location;
        this.agentIdPrefix = agentIdPrefix;

        if (!Files.exists(Paths.get(serviceAccountJsonPath))) {
            throw new IllegalArgumentException("Service account key not found: " + serviceAccountJsonPath);
        }

        this.creds = ServiceAccountCredentials.fromStream(new FileInputStream(serviceAccountJsonPath));

        BigQueryReadSettings settings = BigQueryReadSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(this.creds))
                .build();

        this.readClient = BigQueryReadClient.create(settings);
    }

    /** FQN table: projects/{p}/datasets/{d}/tables/{t} */
    private String tablePath(String tableName) {
        // prefix: tableName = agentIdPrefix + tableName;
        return String.format("projects/%s/datasets/%s/tables/%s", projectId, dataset, tableName);
    }

    /** Tạo ReadSession đọc cột 'phone' với định dạng AVRO; maxStreams là số stream tối đa; rowRestriction là filter SQL tuỳ chọn. */
    private ReadSession createReadSession(String tableName, int maxStreams, String rowRestriction) {
        TableReadOptions.Builder opts = TableReadOptions.newBuilder()
                .addSelectedFields("phone"); // chỉ đọc cột cần thiết

        if (rowRestriction != null && !rowRestriction.isBlank()) {
            opts.setRowRestriction(rowRestriction);
        }

        // Lưu ý: location ảnh hưởng routing; Storage API dùng region của bảng.
        CreateReadSessionRequest req = CreateReadSessionRequest.newBuilder()
                .setParent(String.format("projects/%s", projectId))
                .setMaxStreamCount(Math.max(1, maxStreams))
                .setReadSession(ReadSession.newBuilder()
                        .setTable(tablePath(tableName))
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(opts.build())
                        .build())
                .build();

        return readClient.createReadSession(req);
    }

    /**
     * Đọc dữ liệu qua Storage API (AVRO), gom tối đa maxRows.
     * - KHÔNG còn OFFSET/skip.
     * - rowRestriction: điều kiện SQL để lọc hoặc phân mảnh (vd: shard, range, partition).
     * - maxStreams: số stream tối đa tạo trong ReadSession (tăng để nhanh hơn; có thể đọc tuần tự hoặc song song).
     */
    public PagedResult getPhonesStorageWithMetrics(String tableName,
                                                   int maxRows,
                                                   int maxStreams,
                                                   String rowRestriction) throws Exception {
        if (maxRows <= 0) {
            return new PagedResult(List.of(), new QueryMetrics());
        }

        long t0Wall = System.nanoTime();

        ReadSession session = createReadSession(tableName, maxStreams, rowRestriction);
        List<ReadStream> streams = session.getStreamsList();

        // Schema AVRO để giải mã
        Schema avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);

        QueryMetrics m = new QueryMetrics(); // totalSlotMs sẽ luôn 0 cho Storage API
        List<String> out = new ArrayList<>(Math.min(maxRows, 1024));

        int remaining = maxRows;

        if (streams.isEmpty()) {
            // không có stream => bảng/partition rỗng
            m.wallMillis = (System.nanoTime() - t0Wall) / 1_000_000L;
            return new PagedResult(out, m);
        }

        // Đọc tuần tự từng stream (có thể đổi sang song song nếu cần)
        for (int si = 0; si < streams.size() && remaining > 0; si++) {
            ReadStream stream = streams.get(si);
            long tStream0 = System.nanoTime();
            long streamBytes = 0L;
            long streamRows = 0L;

            ReadRowsRequest rr = ReadRowsRequest.newBuilder()
                    .setReadStream(stream.getName())
                    .build();

            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(rr);

            for (ReadRowsResponse resp : responses) {
                // Mỗi response chứa một chunk AVRO + row_count
                long rc = resp.getRowCount();
                byte[] payload = resp.getAvroRows().getSerializedBinaryRows().toByteArray();
                streamBytes += payload.length;

                // Decoder AVRO cho chunk
                Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

                // Đọc từng row trong chunk
                for (long i = 0; i < rc; i++) {
                    GenericRecord rec = reader.read(null, decoder);
                    streamRows++;

                    // Lấy field "phone"
                    Object v = rec.get("phone");
                    if (v != null) {
                        if (out.size() < 10) System.out.println(v);
                        out.add(v.toString());
                        remaining--;
                        if (remaining <= 0) break;
                    }
                }

                if (remaining <= 0) break;
            }

            m.pages++;                 // coi mỗi stream là 1 "page"
            m.rows += streamRows;
            m.totalBytesProcessed += streamBytes; // bytes delivered
            long tStream1 = System.nanoTime();
            long streamWallMs = (tStream1 - tStream0) / 1_000_000L;

            // Log per-stream
            System.out.printf("[Stream %d/%d] rows=%d, bytes=%d (%.6f TB), wall=%d ms%n",
                    (si + 1), streams.size(), streamRows, streamBytes,
                    streamBytes / 1_000_000_000_000.0, streamWallMs);
        }

        // Wall time tổng thể
        long t1Wall = System.nanoTime();
        long totalWallMs = (t1Wall - t0Wall) / 1_000_000L;
        m.wallMillis = totalWallMs;

        return new PagedResult(out, m);
    }

    /**
     * Helper sharding:
     * Chia dữ liệu theo hash của 'phone' để chạy song song nhiều worker/job.
     * - shardCount: tổng số shard (N)
     * - shardIndex: chỉ số shard hiện tại [0..N-1]
     *
     * Công thức ví dụ (hash đều, tránh OFFSET):
     *   MOD(ABS(FARM_FINGERPRINT(CAST(phone AS STRING))), N) = k
     */
    public PagedResult getPhonesStorageShard(String tableName,
                                             int maxRows,
                                             int maxStreams,
                                             int shardCount,
                                             int shardIndex) throws Exception {
        if (shardCount <= 0 || shardIndex < 0 || shardIndex >= shardCount) {
            throw new IllegalArgumentException("Invalid shard parameters");
        }
        String rr = String.format("MOD(ABS(FARM_FINGERPRINT(CAST(phone AS STRING))), %d) = %d", shardCount, shardIndex);
        return getPhonesStorageWithMetrics(tableName, maxRows, maxStreams, rr);
    }

    /** Dọn tài nguyên Storage API client. Gọi khi dùng xong. */
    public void close() {
        if (readClient != null) {
            readClient.close();
        }
    }

    // --- Demo main (tuỳ chọn) ---
    public static void main(String[] args) throws Exception {
        String projectId  = "genuine-range-470419-v5";
        String dataset    = "storage_api";
        String location   = "US";
        String saJson     = "src/main/resources/genuine-range-470419-v5-6abdff4bd15c.json";
        String agentPref  = ""; // không dùng ở Storage API
        String table      = "phone_8m";

        int maxRows       = 1_00_000;  // số dòng muốn lấy về để so sánh
        int maxStreams    = 1;        // số stream tối đa (tăng để nhanh hơn)

        SRAPIShardingFetcher s = new SRAPIShardingFetcher(
                projectId, dataset, location, saJson, agentPref, true);

        try {
            // 1) Cách 1: Đọc thẳng, không OFFSET, không filter:
            PagedResult pr = s.getPhonesStorageWithMetrics(table, maxRows, maxStreams, null);

            // 2) Cách 2: Đọc theo shard (ví dụ shard 0/4):
            // PagedResult pr = s.getPhonesStorageShard(table, maxRows, maxStreams, 4, 0);

            System.out.println("Fetched rows      : " + pr.phones.size());
            System.out.println("Streams (pages)   : " + pr.metrics.pages);
            System.out.println("Bytes delivered   : " + pr.metrics.totalBytesProcessed);
            System.out.printf ("Delivered (TB)    : %.6f TB%n", pr.metrics.processedTB());
            System.out.printf ("Wall time (ms)    : %d%n", pr.metrics.wallMillis);

            pr.phones.stream().limit(10).forEach(System.out::println);
        } finally {
            s.close();
        }
    }
}