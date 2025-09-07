package bqstorageapi.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Kết quả trả về có danh sách items + metrics (dùng cho readSingleColumn). */
public class PagedResult<T> {
    public final List<T> items;
    public final QueryMetrics metrics;

    public PagedResult(List<T> items, QueryMetrics metrics) {
        this.items = items == null ? Collections.emptyList() : items;
        this.metrics = Objects.requireNonNullElseGet(metrics, QueryMetrics::new);
    }

    public int size() { return items.size(); }

    @Override public String toString() {
        return "PagedResult{items=" + size() + ", metrics=" + metrics + '}';
    }
}