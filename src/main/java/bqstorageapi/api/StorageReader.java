package bqstorageapi.api;

import bqstorageapi.config.ReadOptions;
import bqstorageapi.model.Page;
import bqstorageapi.model.PagedResult;
import bqstorageapi.model.QueryMetrics;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface StorageReader {
    @FunctionalInterface
    interface ValueMapper<T> {
        T map(Object value); // value có thể là scalar hoặc phần tử của mảng
    }

    // Method generic (mỗi lần gọi, T có thể khác nhau)
    <T> PagedResult<T> readSingleColumn(String table,
                                        String column,
                                        ReadOptions opts,
                                        ValueMapper<T> mapper)
            throws ExecutionException, InterruptedException;

    <T> QueryMetrics readSingleColumnToSink(String table,
                                            String column,
                                            ReadOptions opts,
                                            ValueMapper<T> mapper,
                                            bqstorageapi.sink.BatchSink<T> sink)
            throws Exception;

    Page<?> readTable(String table, ReadOptions opts);

    Page<?> readColumns(String table, List<String> columns, ReadOptions opts);

    Page<?> readTableWhere(String table, String sqlFilter, ReadOptions opts);
}