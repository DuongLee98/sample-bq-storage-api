package bqstorageapi.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Trang dữ liệu tổng quát (cho readTable / readColumns / readTableWhere).
 * Có thể mở rộng thêm nextPageToken nếu bạn phân trang thực sự.
 */
public class Page<T> {
    public final List<T> items;
    public final QueryMetrics metrics;
    public final String nextPageToken; // null nếu không phân trang tiếp

    public Page(List<T> items, QueryMetrics metrics, String nextPageToken) {
        this.items = items == null ? Collections.emptyList() : items;
        this.metrics = Objects.requireNonNullElseGet(metrics, QueryMetrics::new);
        this.nextPageToken = nextPageToken;
    }

    public int size() { return items.size(); }

    @Override public String toString() {
        return "Page{items=" + size() + ", nextPageToken=" + nextPageToken + ", metrics=" + metrics + '}';
    }
}