package bqstorageapi.model;

/** Thống kê cho một lượt đọc qua Storage API. */
public class QueryMetrics {
    public int pages = 0;                 // ví mỗi stream là 1 page
    public long rows = 0L;                // tổng số row đọc được
    public long totalBytesProcessed = 0L; // bytes delivered
    public long wallMillis = 0L;          // tổng thời gian thực (ms)

    public double processedMB() { return totalBytesProcessed / 1_000_000.0; }
    public double processedGB() { return totalBytesProcessed / 1_000_000_000.0; }
    public double processedTB() { return totalBytesProcessed / 1_000_000_000_000.0; }

    @Override public String toString() {
        return "QueryMetrics{pages=" + pages + ", rows=" + rows +
                ", totalBytesProcessed=" + totalBytesProcessed +
                ", wallMillis=" + wallMillis + '}';
    }
}