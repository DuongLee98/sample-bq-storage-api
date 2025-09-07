package examples.metrics;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Kết quả trả về gồm:
 * - danh sách phones đã đọc
 * - metrics cho lần đọc
 */
public class PagedResult {
    /** Danh sách phone đã gom được. Code của bạn đang gọi pr.phones.size(). */
    public final List<String> phones;

    /** Thống kê/metrics đi kèm. Code của bạn truy cập pr.metrics.* */
    public final QueryMetrics metrics;

    public PagedResult(List<String> phones, QueryMetrics metrics) {
        this.phones = phones == null ? Collections.emptyList() : phones;
        this.metrics = Objects.requireNonNullElseGet(metrics, QueryMetrics::new);
    }

    @Override
    public String toString() {
        return "PagedResult{" +
                "phones=" + phones.size() + " items" +
                ", metrics=" + metrics +
                '}';
    }
}