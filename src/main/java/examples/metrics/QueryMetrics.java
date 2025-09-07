package examples.metrics;

/**
 * Thống kê/metric cho lần đọc qua BigQuery Storage API.
 * Các field để public cho đơn giản (POJO), đúng với cách bạn đang truy cập.
 */
public class QueryMetrics {
    /** Số "page" – trong code của bạn đang coi mỗi stream là một page. */
    public int pages = 0;

    /** Tổng số hàng đọc được. */
    public long rows = 0L;

    /** Tổng số bytes nhận được từ Storage API (delivered). */
    public long totalBytesProcessed = 0L;

    /** Tổng wall time (ms) cho toàn bộ lần đọc. */
    public long wallMillis = 0L;

    public QueryMetrics() {}

    /** Quy đổi bytes → MB (thập phân). */
    public double processedMB() {
        return totalBytesProcessed / 1_000_000.0;
    }

    /** Quy đổi bytes → GB (thập phân). */
    public double processedGB() {
        return totalBytesProcessed / 1_000_000_000.0;
    }

    /** Quy đổi bytes → TB (thập phân). Phù hợp với chỗ bạn đang printf("%.6f TB"). */
    public double processedTB() {
        return totalBytesProcessed / 1_000_000_000_000.0;
    }

    @Override
    public String toString() {
        return "QueryMetrics{" +
                "pages=" + pages +
                ", rows=" + rows +
                ", totalBytesProcessed=" + totalBytesProcessed +
                ", wallMillis=" + wallMillis +
                '}';
    }
}