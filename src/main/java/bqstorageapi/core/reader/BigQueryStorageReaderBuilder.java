package bqstorageapi.core.reader;

import bqstorageapi.api.StorageReader;
import bqstorageapi.api.StorageReaderBuilder;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;

import java.util.Objects;

/** Builder chuẩn để tạo BigQueryStorageReader (chưa có StreamConfig). */
public class BigQueryStorageReaderBuilder implements StorageReaderBuilder {

    private ClientConfig clientCfg;
    private RetryConfig retryCfg;
    private ReadOptions defaultReadOpts;

    @Override
    public BigQueryStorageReaderBuilder client(ClientConfig clientConfig) {
        this.clientCfg = clientConfig;
        return this;
    }

    @Override
    public BigQueryStorageReaderBuilder retry(RetryConfig retryConfig) {
        this.retryCfg = retryConfig;
        return this;
    }

    @Override
    public BigQueryStorageReaderBuilder defaultReadOptions(ReadOptions readOptions) {
        this.defaultReadOpts = readOptions;
        return this;
    }

    @Override
    public StorageReader build() {
        Objects.requireNonNull(clientCfg, "ClientConfig is required");
        if (retryCfg == null) retryCfg = RetryConfig.newBuilder().build();
        if (defaultReadOpts == null) defaultReadOpts = ReadOptions.newBuilder().build();
        return new BigQueryStorageReader(clientCfg, retryCfg, defaultReadOpts);
    }
}