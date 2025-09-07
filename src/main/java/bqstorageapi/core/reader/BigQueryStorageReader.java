package bqstorageapi.core.reader;

import bqstorageapi.api.StorageReader;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.handler.BatchHandler;
import bqstorageapi.handler.ValueMapper;
import bqstorageapi.model.Page;
import bqstorageapi.model.PagedResult;
import bqstorageapi.model.QueryMetrics;

import bqstorageapi.handler.BatchSink;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StatusCode;
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

    private static RetrySettings toGaxRetrySettings(RetryConfig cfg) {
        var builder = RetrySettings.newBuilder()
                .setMaxAttempts(cfg.maxAttempts)
                .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(cfg.initialBackoff.toMillis()))
                .setRetryDelayMultiplier(cfg.backoffMultiplier)
                .setMaxRetryDelay(org.threeten.bp.Duration.ofMillis(cfg.maxBackoff.toMillis()));
        if (cfg.overallTimeout != null) {
            builder.setTotalTimeout(org.threeten.bp.Duration.ofMillis(cfg.overallTimeout.toMillis()));
        }
        return builder.build();
    }


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

            RetrySettings gaxRetry = toGaxRetrySettings(this.retryCfg);

            BigQueryReadSettings.Builder builder = BigQueryReadSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(creds));

            // Áp cho createReadSession
            builder.createReadSessionSettings()
                    .setRetrySettings(gaxRetry)
                    .setRetryableCodes(this.retryCfg.retryableCodes);

            // Áp cho readRows
            builder.readRowsSettings()
                    .setRetrySettings(gaxRetry)
                    .setRetryableCodes(this.retryCfg.retryableCodes);

            BigQueryReadSettings settings = builder.build();
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

    private ReadSession newSession(
            String table,
            ReadOptions use,
            String singleColumn,
            List<String> moreColumns,
            String sqlFilter
    ) {
        ReadSession.TableReadOptions.Builder ro = ReadSession.TableReadOptions.newBuilder();

        // De-duplicate selected fields
        var selected = new java.util.LinkedHashSet<String>();
        if (use.columns != null) {
            for (String c : use.columns) if (c != null && !c.isBlank()) selected.add(c);
        }
        if (singleColumn != null && !singleColumn.isBlank()) selected.add(singleColumn);
        if (moreColumns != null) {
            for (String c : moreColumns) if (c != null && !c.isBlank()) selected.add(c);
        }
        if (!selected.isEmpty()) ro.addAllSelectedFields(selected);

        // Merge filters
        String base  = (use.rowRestriction != null && !use.rowRestriction.isBlank()) ? use.rowRestriction : null;
        String extra = (sqlFilter != null && !sqlFilter.isBlank()) ? sqlFilter : null;
        if (base != null && extra != null) ro.setRowRestriction("(" + base + ") AND (" + extra + ")");
        else if (base != null)             ro.setRowRestriction(base);
        else if (extra != null)            ro.setRowRestriction(extra);

        // Build request (format AVRO cho v1)
        ReadSession.Builder sessionBuilder = ReadSession.newBuilder()
                .setTable(tablePath(table))
                .setDataFormat(DataFormat.AVRO)
                .setReadOptions(ro.build());

        CreateReadSessionRequest req = CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + clientCfg.projectId)
                .setReadSession(sessionBuilder.build())
                .setMaxStreamCount(Math.max(1, use.maxStreams))
                .build();

        return readClient.createReadSession(req);
    }

    // ở trong BigQueryStorageReader
    private boolean isRetryable(Throwable t) {
        if (!(t instanceof ApiException)) return false;
        StatusCode.Code c = ((ApiException) t).getStatusCode().getCode();
        return retryCfg.retryableCodes.contains(c);
    }

    private long calcBackoffMs(int attempt) {
        // attempt: 1,2,3,...
        double mult = Math.pow(retryCfg.backoffMultiplier, Math.max(0, attempt - 1));
        long delay = (long) Math.min(
                retryCfg.maxBackoff.toMillis(),
                retryCfg.initialBackoff.toMillis() * mult
        );
        if (retryCfg.jitter) {
            long half = delay / 2;
            long span = Math.max(1, delay - half);
            return half + (long)(Math.random() * span);
        }
        return delay;
    }

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

                // buffer batch cục bộ cho từng stream (tránh lock)
                List<GenericRecord> batch = new ArrayList<>(Math.max(1, targetBatchRows));
                long batchApproxBytes = 0L;

                if (remaining.get() <= 0) return null;

                long offset = 0L;          // <--- số row đã xử lý trong stream, dùng để resume
                int attempts = 0;

                // vòng đời đọc 1 stream với retry
                while (true) {
                    try {
                        if (remaining.get() <= 0) break;

                        ReadRowsRequest req = ReadRowsRequest.newBuilder()
                                .setReadStream(stream.getName())
                                .setOffset(offset)           // <--- resume từ offset
                                .build();

                        ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(req);

                        for (ReadRowsResponse resp : responses) {
                            if (remaining.get() <= 0) break;

                            long payloadSize = resp.getAvroRows().getSerializedBinaryRows().size();
                            long rc = resp.getRowCount();

                            streamBytes += payloadSize;

                            List<GenericRecord> recs = AvroUtils.decodeChunk(resp, avroSchema);
                            long perRec = (rc > 0 ? Math.max(1L, payloadSize / rc) : 0L);

                            for (GenericRecord gr : recs) {
                                if (remaining.get() <= 0) break;

                                batch.add(gr);
                                batchApproxBytes += perRec;

                                offset++;        // <--- đã xử lý xong 1 row trong stream
                                streamRows++;

                                long left = remaining.decrementAndGet();

                                boolean reachRows  = (targetBatchRows > 0 && batch.size() >= targetBatchRows);
                                boolean reachBytes = (batchBytesThreshold > 0 && batchApproxBytes >= batchBytesThreshold);

                                if (reachRows || reachBytes) {
                                    boolean cont = handler.onBatch(batch, batchApproxBytes);
                                    batch = new ArrayList<>(Math.max(1, targetBatchRows));
                                    batchApproxBytes = 0L;
                                    if (!cont) { remaining.set(0); break; }
                                }

                                if (left <= 0) {
                                    // đủ maxRows -> flush phần còn lại rồi dừng
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

                        // nếu đi hết responses mà không lỗi -> stream DONE
                        break;

                    } catch (Throwable t) {
                        attempts++;
                        if (!isRetryable(t) || attempts >= retryCfg.maxAttempts) {
                            // hết cơ hội retry -> flush phần còn dư để khỏi mất dữ liệu đã thu
                            if (!batch.isEmpty()) {
                                handler.onBatch(batch, batchApproxBytes);
                                batch.clear();
                                batchApproxBytes = 0L;
                            }
                            // ném lỗi để fail task stream này
                            throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
                        }
                        // backoff rồi thử lại, offset giữ nguyên (resume từ chỗ dở)
                        long sleepMs = calcBackoffMs(attempts);
                        try { Thread.sleep(sleepMs); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw ie; }
                        // quay lại while để reopen từ offset
                    }
                }

                // LUÔN flush batch còn dư (nếu chưa flush ở trên)
                if (!batch.isEmpty()) {
                    handler.onBatch(batch, batchApproxBytes);
                }

                synchronized (m) {
                    m.pages++; // mỗi stream = 1 "page"
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
            ValueMapper<T> mapper
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
            ValueMapper<T> mapper,
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