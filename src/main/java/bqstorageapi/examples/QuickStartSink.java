package bqstorageapi.examples;

import bqstorageapi.api.StorageReader;
import bqstorageapi.core.reader.BigQueryStorageReaderBuilder;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.model.QueryMetrics;

import java.nio.file.Path;

public class QuickStartSink {
    public static void main(String[] args) throws Exception {
        // Thay bằng thông tin thật của bạn
        String projectId  = "genuine-range-470419-v5";
        String dataset    = "storage_api";
        String location   = "US";
        String table      = "phone_8m";
        Path saJson       = Path.of("src/main/resources/genuine-range-470419-v5-6abdff4bd15c.json");

        StorageReader reader = new BigQueryStorageReaderBuilder()
                .client(ClientConfig.newBuilder()
                        .projectId(projectId)
                        .dataset(dataset)
                        .location(location)
                        .serviceAccountJson(saJson) // hoặc bỏ nếu dùng ADC
                        .build())
                .retry(RetryConfig.newBuilder().build())
                .defaultReadOptions(ReadOptions.newBuilder()
                        .maxRows(100_000)
                        .maxStreams(2)
                        .workers(2)
                        .targetBatchRows(10_000)   // batch size
                        .build())
                .build();

        try {
            QueryMetrics metrics = reader.readSingleColumnToSink(
                    table,
                    "phone",
                    null,                   // dùng default ReadOptions
                    Object::toString,       // mapper: Object -> String
                    batch -> {
                        // Sink: in ra độ dài batch
                        System.out.println("Batch size = " + batch.size());
                        // bạn có thể in batch.get(0) để kiểm tra nội dung
                    }
            );

            System.out.println("Fetched rows = " + metrics.rows);
            System.out.println("Fetched bytes = " + metrics.totalBytesProcessed);
        } finally {
            if (reader instanceof AutoCloseable) {
                ((AutoCloseable) reader).close();
            }
        }
    }
}