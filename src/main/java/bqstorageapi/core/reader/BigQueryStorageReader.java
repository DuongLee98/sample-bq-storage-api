package bqstorageapi.core.reader;

import bqstorageapi.api.StorageReader;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.model.Page;
import bqstorageapi.model.PagedResult;
import bqstorageapi.model.QueryMetrics;

import bqstorageapi.sink.BatchSink;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Triển khai StorageReader dựa trên BigQuery Storage Read API.
 * - Hiện đã implement: readSingleColumn(...) (AVRO, đọc tuần tự).
 * - Các hàm còn lại để TODO (sẵn khung).
 */
public class BigQueryStorageReader implements StorageReader, AutoCloseable {

    private final ClientConfig clientCfg;
    private final RetryConfig retryCfg;
    private final ReadOptions defaultReadOpts;
    private final BigQueryReadClient readClient;

    BigQueryStorageReader(ClientConfig clientCfg, RetryConfig retryCfg, ReadOptions defaultReadOpts) {
        this.clientCfg = Objects.requireNonNull(clientCfg, "clientCfg");
        this.retryCfg = Objects.requireNonNull(retryCfg, "retryCfg");
        this.defaultReadOpts = Objects.requireNonNull(defaultReadOpts, "defaultReadOpts");
        this.readClient = createClient(clientCfg);
    }

    private BigQueryReadClient createClient(ClientConfig cfg) {
        try {
            GoogleCredentials creds;
            if (cfg.serviceAccountJson != null) {
                try (InputStream in = Files.newInputStream(cfg.serviceAccountJson)) {
                    creds = ServiceAccountCredentials.fromStream(in);
                }
            } else {
                creds = GoogleCredentials.getApplicationDefault();
            }
            BigQueryReadSettings settings = BigQueryReadSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(creds))
                    // TODO: map RetryConfig -> RetrySettings nếu cần tinh chỉnh ở GAX
                    .build();
            return BigQueryReadClient.create(settings);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create BigQueryReadClient", e);
        }
    }

    private String tablePath(String table) {
        // chấp nhận "dataset.table" hoặc chỉ "table" (dùng dataset mặc định)
        String dataset = clientCfg.dataset;
        String tableName = table;
        if (table.contains(".")) {
            String[] parts = table.split("\\.", 2);
            dataset = parts[0];
            tableName = parts[1];
        }
        return "projects/" + clientCfg.projectId + "/datasets/" + dataset + "/tables/" + tableName;
    }

    private ReadSession newSession(String table, ReadOptions use,
                                   String singleColumn, List<String> moreColumns, String sqlFilter) {

        ReadSession.TableReadOptions.Builder ro = ReadSession.TableReadOptions.newBuilder();

        // de-duplicate fields
        var selected = new java.util.LinkedHashSet<String>();
        if (use.columns != null) selected.addAll(use.columns);
        if (singleColumn != null && !singleColumn.isBlank()) selected.add(singleColumn);
        if (moreColumns != null) {
            for (String c : moreColumns) {
                if (c != null && !c.isBlank()) selected.add(c);
            }
        }
        if (!selected.isEmpty()) ro.addAllSelectedFields(selected);

        // gộp filter
        String f1 = (use.rowRestriction != null && !use.rowRestriction.isBlank()) ? use.rowRestriction : null;
        String f2 = (sqlFilter != null && !sqlFilter.isBlank()) ? sqlFilter : null;
        if (f1 != null && f2 != null) ro.setRowRestriction("(" + f1 + ") AND (" + f2 + ")");
        else if (f1 != null) ro.setRowRestriction(f1);
        else if (f2 != null) ro.setRowRestriction(f2);

        // tạm thời chỉ AVRO
        return readClient.createReadSession(
                CreateReadSessionRequest.newBuilder()
                        .setParent("projects/" + clientCfg.projectId)
                        .setReadSession(ReadSession.newBuilder()
                                .setTable(tablePath(table))
                                .setDataFormat(DataFormat.AVRO) // ép AVRO ở v1
                                .setReadOptions(ro.build())
                                .build())
                        .setMaxStreamCount(Math.max(1, use.maxStreams))
                        .build()
        );
    }


//    private void processStreams(
//            List<ReadStream> streams,
//            Schema avroSchema,
//            long maxRows,                 // đổi sang long
//            QueryMetrics m,
//            RecordHandler handler
//    ) throws IOException {
//        long remaining = (maxRows > 0) ? maxRows : Long.MAX_VALUE;
//
//        for (int si = 0; si < streams.size() && remaining > 0; si++) {
//            ReadStream stream = streams.get(si);
//            long streamBytes = 0L;
//            long streamRows  = 0L;
//
//            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(
//                    ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build()
//            );
//
//            for (ReadRowsResponse resp : responses) {
//                streamBytes += resp.getAvroRows().getSerializedBinaryRows().size();
//
//                List<GenericRecord> recs = AvroUtils.decodeChunk(resp, avroSchema); // bắt IOException ở ngoài nếu muốn
//
//                for (GenericRecord gr : recs) {
//                    streamRows++;
//                    if (!handler.onRecord(gr)) { remaining = 0; break; }
//                    if (--remaining <= 0) break;
//                }
//                if (remaining <= 0) break;
//            }
//
//            m.pages++;
//            m.rows += streamRows;               // m.rows đã là long → ok
//            m.totalBytesProcessed += streamBytes;
//        }
//    }

//    private void processStreamsParallel(
//            List<ReadStream> streams,
//            Schema avroSchema,
//            long maxRows,                  // 0/neg = không giới hạn
//            QueryMetrics m,
//            RecordHandler handler,
//            int workers                    // số thread muốn dùng (<= số stream)
//    ) throws ExecutionException, InterruptedException {
//        if (streams.isEmpty()) return;
//
//        final AtomicLong remaining = new AtomicLong(maxRows > 0 ? maxRows : Long.MAX_VALUE);
//        final int poolSize = Math.min(workers <= 0 ? 1 : workers, streams.size());
//        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
//        List<Future<?>> futures = new ArrayList<>(streams.size());
//
//        for (ReadStream stream : streams) {
//            futures.add(exec.submit(() -> {
//                long streamBytes = 0L;
//                long streamRows  = 0L;
//
//                // Nếu đã đủ rows thì bỏ qua stream này sớm
//                if (remaining.get() <= 0) return null;
//
//                ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(
//                        ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build()
//                );
//
//                for (ReadRowsResponse resp : responses) {
//                    if (remaining.get() <= 0) break;
//
//                    streamBytes += resp.getAvroRows().getSerializedBinaryRows().size();
//
//                    List<GenericRecord> recs = AvroUtils.decodeChunk(resp, avroSchema);
//
//                    for (GenericRecord gr : recs) {
//                        if (remaining.get() <= 0) break;
//
//                        // Gọi handler trước rồi mới trừ remaining, để handler có thể quyết định dừng
//                        boolean cont = handler.onRecord(gr);
//                        if (!cont) {
//                            remaining.set(0);
//                            break;
//                        }
//                        long left = remaining.decrementAndGet();
//                        streamRows++;
//                        if (left <= 0) break;
//                    }
//                }
//
//                // Cập nhật metrics (đơn giản: sync trên m)
//                synchronized (m) {
//                    m.pages++; // coi mỗi stream là 1 "page"
//                    m.rows += streamRows;
//                    m.totalBytesProcessed += streamBytes;
//                }
//                return null;
//            }));
//        }
//
//        // Đợi tất cả xong (hoặc có thể cancel khi remaining==0)
//        exec.shutdown();
//
//        // nếu có exception trong task, ném ra (tuỳ chọn)
//        for (Future<?> f : futures) {
//            f.get();
//        }
//    }

    private void processStreamsParallelBatch(
            List<ReadStream> streams,
            Schema avroSchema,
            long maxRows,
            int targetBatchRows,
            long targetBatchMB,
            QueryMetrics m,
            BatchHandler handler,
            int workers
    ) throws ExecutionException, InterruptedException {

        if (streams.isEmpty()) return;

        final AtomicLong remaining = new AtomicLong(maxRows > 0 ? maxRows : Long.MAX_VALUE);
        final int poolSize = Math.min(workers <= 0 ? 1 : workers, streams.size());
        final long batchBytesThreshold = (targetBatchMB > 0 ? targetBatchMB : 0L) * 1_000_000L;
        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
        List<Future<?>> futures = new ArrayList<>(streams.size());

        for (ReadStream stream : streams) {
            futures.add(exec.submit(() -> {
                long streamBytes = 0L;
                long streamRows  = 0L;

                List<GenericRecord> batch = new ArrayList<>(Math.max(1, targetBatchRows));
                long batchApproxBytes = 0L;

                if (remaining.get() <= 0) return null;

                ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(
                        ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build()
                );

                for (ReadRowsResponse resp : responses) {
                    if (remaining.get() <= 0) break;

                    // dùng size() để tránh copy payload
                    long payloadSize = resp.getAvroRows().getSerializedBinaryRows().size();
                    long rc = resp.getRowCount();

                    streamBytes += payloadSize;

                    List<GenericRecord> recs = AvroUtils.decodeChunk(resp, avroSchema);
                    long perRec = (rc > 0 ? Math.max(1L, payloadSize / rc) : 0L);

                    for (GenericRecord gr : recs) {
                        if (remaining.get() <= 0) break;

                        batch.add(gr);
                        batchApproxBytes += perRec;

                        long left = remaining.decrementAndGet();
                        streamRows++;

                        boolean reachRows  = (targetBatchRows > 0 && batch.size() >= targetBatchRows);
                        boolean reachBytes = (batchBytesThreshold > 0 && batchApproxBytes >= batchBytesThreshold);

                        if (reachRows || reachBytes) {
                            boolean cont = handler.onBatch(batch, batchApproxBytes);
                            batch = new ArrayList<>(Math.max(1, targetBatchRows));
                            batchApproxBytes = 0L;
                            if (!cont) { remaining.set(0); break; }
                        }

                        if (left <= 0) {
                            // flush phần còn lại trước khi dừng vì đủ maxRows
                            if (!batch.isEmpty()) {
                                handler.onBatch(batch, batchApproxBytes);
                                batch.clear();
                                batchApproxBytes = 0L;
                            }
                            remaining.set(0);
                            break;
                        }
                    }
                }

                // LUÔN flush batch còn dư bất kể remaining (kể cả remaining==0)
                if (!batch.isEmpty()) {
                    handler.onBatch(batch, batchApproxBytes);
                }

                synchronized (m) {
                    m.pages++;
                    m.rows += streamRows;
                    m.totalBytesProcessed += streamBytes;
                }
                return null;
            }));
        }

        exec.shutdown();
        for (Future<?> f : futures) f.get();
    }

    // ====== API methods ======
    @Override
    public <T> PagedResult<T> readSingleColumn(
            String table,
            String column,
            ReadOptions opts,
            StorageReader.ValueMapper<T> mapper
    ) throws ExecutionException, InterruptedException {
        ReadOptions use = (opts != null) ? opts : defaultReadOpts;

        QueryMetrics m = new QueryMetrics();
        long t0 = System.nanoTime();

        ReadSession session = newSession(table, use, column, null, null);
        List<ReadStream> streams = session.getStreamsList();
        final java.util.concurrent.ConcurrentLinkedQueue<T> q =
                new java.util.concurrent.ConcurrentLinkedQueue<>();

        if (!streams.isEmpty()) {
            Schema avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());

            processStreamsParallelBatch(
                    streams,
                    avroSchema,
                    use.maxRows, use.targetBatchRows, use.targetBatchMB,
                    m,
                    (batch, approxBytes) -> {
                        for (GenericRecord gr : batch) {
                            Object v = gr.get(column);
                            if (v == null) continue;
                            // (nếu field có thể REPEATED thì bạn có thể flatten ở đây)
                            T mapped = mapper.map(v);
                            if (mapped != null) q.add(mapped);
                        }
                        return true;
                    },
                    Math.max(1, use.workers)
            );
        }

        m.wallMillis = (System.nanoTime() - t0) / 1_000_000L;
        return new PagedResult<>(new ArrayList<>(q), m);
    }

    @Override
    public <T> QueryMetrics readSingleColumnToSink(
            String table,
            String column,
            ReadOptions opts,
            StorageReader.ValueMapper<T> mapper,
            BatchSink<T> sink
    ) throws Exception {
        ReadOptions use = (opts != null) ? opts : defaultReadOpts;
        QueryMetrics m = new QueryMetrics();

        ReadSession session = newSession(table, use, column, null, null);
        List<ReadStream> streams = session.getStreamsList();
        if (streams.isEmpty()) return m;

        Schema avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());

        // dùng sẵn processStreamsParallelBatch: chia theo workers + batch
        processStreamsParallelBatch(
                streams, avroSchema,
                use.maxRows, use.targetBatchRows, use.targetBatchMB,
                m,
                (batch, approxBytes) -> {
                    // Map GenericRecord -> T rồi đẩy sang sink trong *1 lần*
                    var out = new ArrayList<T>(batch.size());
                    for (GenericRecord gr : batch) {
                        Object v = gr.get(column);
                        if (v == null) continue;
                        T mapped = mapper.map(v);
                        if (mapped != null) out.add(mapped);
                    }
                    if (!out.isEmpty()) sink.accept(out);
                    return true; // tiếp tục
                },
                Math.max(1, use.workers)
        );

        return m;
    }

    @Override
    public Page<?> readTable(String table, ReadOptions opts) {
        // TODO: tạo ReadSession đọc full bảng (không projection), decode ra List<GenericRecord> hoặc Map<String,Object>
        QueryMetrics m = new QueryMetrics();
        List<Object> items = new ArrayList<>();
        return new Page<>(items, m, null);
    }

    @Override
    public Page<?> readColumns(String table, List<String> columns, ReadOptions opts) {
        // TODO: tạo ReadSession với projection columns, decode items
        QueryMetrics m = new QueryMetrics();
        List<Object> items = new ArrayList<>();
        return new Page<>(items, m, null);
    }

    @Override
    public Page<?> readTableWhere(String table, String sqlFilter, ReadOptions opts) {
        // TODO: tạo ReadSession full bảng nhưng thêm rowRestriction = sqlFilter, decode items
        QueryMetrics m = new QueryMetrics();
        List<Object> items = new ArrayList<>();
        return new Page<>(items, m, null);
    }

    @Override
    public void close() {
        if (readClient != null) readClient.close();
    }
}