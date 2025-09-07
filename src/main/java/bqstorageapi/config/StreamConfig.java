package bqstorageapi.config;

/** Cấu hình điều phối stream/worker/batching; có thể được tham chiếu từ ReadOptions. */
public final class StreamConfig {
    public final int maxStreams;        // số stream tối đa khi tạo ReadSession
    public final int workers;           // số worker xử lý song song
    public final int targetBatchRows;   // batch theo records
    public final long targetBatchMB;    // batch theo MB thập phân

    private StreamConfig(Builder b) {
        this.maxStreams = b.maxStreams;
        this.workers = b.workers;
        this.targetBatchRows = b.targetBatchRows;
        this.targetBatchMB = b.targetBatchMB;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private int maxStreams = 1;
        private int workers = 1;
        private int targetBatchRows = 10_000;
        private long targetBatchMB = 16;

        public Builder maxStreams(int v) { this.maxStreams = v; return this; }
        public Builder workers(int v) { this.workers = v; return this; }
        public Builder targetBatchRows(int v) { this.targetBatchRows = v; return this; }
        public Builder targetBatchMB(long v) { this.targetBatchMB = v; return this; }

        public StreamConfig build() { return new StreamConfig(this); }
    }
}