package bqstorageapi.config;

import com.google.cloud.bigquery.storage.v1.DataFormat;

import java.util.Collections;
import java.util.List;

/** Các tuỳ chọn khi đọc qua Storage API. Dùng builder để tạo. */
public final class ReadOptions {
    // row limiting
    public final long maxRows;                 // 0/âm => không giới hạn

    // projection & filtering
    public final List<String> columns;        // null/empty => all columns
    public final String rowRestriction;       // ví dụ: "colA > 0 AND colB IS NOT NULL"

    // parallelism
    public final int maxStreams;              // số stream tối đa trong ReadSession
    public final int workers;                 // số worker đọc song song (client threads)

    // batching
    public final int targetBatchRows;         // 0/âm => bỏ qua ngưỡng theo rows
    public final long targetBatchMB;          // 0/âm => bỏ qua ngưỡng theo MB (thập phân)

    // decode format (v1: cố định AVRO)
    public final DataFormat format;

    // --- TODO (tương lai):
    // - Sharding ngoài tiến trình: shardCount, shardIndex, shardKey
    // - Deadline/RetrySettings mapping sang GAX

    private ReadOptions(Builder b) {
        this.maxRows = b.maxRows;
        this.columns = b.columns == null ? null : Collections.unmodifiableList(b.columns);
        this.rowRestriction = b.rowRestriction;
        this.maxStreams = b.maxStreams;
        this.workers = b.workers;
        this.targetBatchRows = b.targetBatchRows;
        this.targetBatchMB = b.targetBatchMB;
        this.format = b.format;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private long maxRows = 0L;
        private List<String> columns = null;
        private String rowRestriction = null;

        private int maxStreams = 1;
        private int workers = 1;

        private int targetBatchRows = 0;
        private long targetBatchMB = 0L; // MB (thập phân)

        private DataFormat format = DataFormat.AVRO; // v1 chỉ AVRO

        public Builder maxRows(long v) { this.maxRows = v; return this; }
        public Builder columns(List<String> cols) { this.columns = cols; return this; }
        public Builder rowRestriction(String sql) { this.rowRestriction = sql; return this; }

        public Builder maxStreams(int v) { this.maxStreams = v; return this; }
        public Builder workers(int v) { this.workers = v; return this; }

        public Builder targetBatchRows(int v) { this.targetBatchRows = v; return this; }
        public Builder targetBatchMB(long v) { this.targetBatchMB = v; return this; }

        // v1: giữ AVRO; nếu sau này hỗ trợ ARROW thì expose setter
        // public Builder format(DataFormat f) { this.format = f; return this; }

        public ReadOptions build() { return new ReadOptions(this); }
    }
}