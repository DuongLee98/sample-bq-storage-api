package bqstorageapi.api;

import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.config.ReadOptions;

/** Builder để tạo StorageReader implementation (ví dụ BigQueryStorageReader). */
public interface StorageReaderBuilder {
    StorageReaderBuilder client(ClientConfig clientConfig);
    StorageReaderBuilder retry(RetryConfig retryConfig);
    StorageReaderBuilder defaultReadOptions(ReadOptions readOptions);
    StorageReader build();
}