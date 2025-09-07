package bqstorageapi.config;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Cấu hình retry/backoff độc lập với GAX để core có thể đọc/áp dụng.
 * Bạn sẽ map sang gax RetrySettings nếu muốn.
 */
public final class RetryConfig {
    /** Tập mã lỗi được phép retry (trừu tượng hoá, không phụ thuộc GAX). */
    public enum Code {
        UNAVAILABLE,
        DEADLINE_EXCEEDED,
        ABORTED,
        INTERNAL,
        RESOURCE_EXHAUSTED,
        // mở rộng nếu cần: CANCELLED, UNKNOWN...
    }

    public final int maxAttempts;               // tổng số lần thử (tính cả lần đầu)
    public final Duration initialBackoff;       // backoff khởi đầu
    public final double backoffMultiplier;      // nhân theo exponential
    public final Duration maxBackoff;           // trần backoff
    public final Duration overallTimeout;       // timeout tổng (null => không khống chế ở đây)
    public final Set<Code> retryableCodes;      // các mã lỗi cho phép retry
    public final boolean jitter;                // có dùng jitter hay không

    private RetryConfig(Builder b) {
        this.maxAttempts = b.maxAttempts;
        this.initialBackoff = b.initialBackoff;
        this.backoffMultiplier = b.backoffMultiplier;
        this.maxBackoff = b.maxBackoff;
        this.overallTimeout = b.overallTimeout;
        this.retryableCodes = Collections.unmodifiableSet(EnumSet.copyOf(b.retryableCodes));
        this.jitter = b.jitter;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private int maxAttempts = 6;                           // ví dụ: 1 + 5 lần retry
        private Duration initialBackoff = Duration.ofMillis(200);
        private double backoffMultiplier = 2.0;
        private Duration maxBackoff = Duration.ofSeconds(10);
        private Duration overallTimeout = Duration.ofMinutes(10);
        private Set<Code> retryableCodes = EnumSet.of(
                Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED,
                Code.ABORTED, Code.INTERNAL, Code.RESOURCE_EXHAUSTED
        );
        private boolean jitter = true;

        public Builder maxAttempts(int v) { this.maxAttempts = v; return this; }
        public Builder initialBackoff(Duration v) { this.initialBackoff = v; return this; }
        public Builder backoffMultiplier(double v) { this.backoffMultiplier = v; return this; }
        public Builder maxBackoff(Duration v) { this.maxBackoff = v; return this; }
        public Builder overallTimeout(Duration v) { this.overallTimeout = v; return this; }
        public Builder retryableCodes(Set<Code> v) { this.retryableCodes = v; return this; }
        public Builder jitter(boolean v) { this.jitter = v; return this; }

        public RetryConfig build() { return new RetryConfig(this); }
    }
}