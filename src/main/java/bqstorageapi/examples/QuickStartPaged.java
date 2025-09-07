package bqstorageapi.examples;

import bqstorageapi.api.StorageReader;
import bqstorageapi.core.reader.BigQueryStorageReaderBuilder;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.model.PagedResult;

import java.nio.file.Path;

public class QuickStartPaged {
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
                        .maxRows(100_000)   // số row tối đa (0/âm => không giới hạn)
                        .maxStreams(2)
                        .workers(2)
                        .targetBatchRows(10_000)
                        .build())
                .build();

        try {
            PagedResult<String> pr = reader.readSingleColumn(table, "phone", null, Object::toString);

            System.out.println("Fetched rows    : " + pr.items.size());
            System.out.println("Pages (streams) : " + pr.metrics.pages);
            System.out.println("Bytes delivered : " + pr.metrics.totalBytesProcessed);
            System.out.printf ("Delivered (TB)  : %.6f%n", pr.metrics.processedTB());
            System.out.printf ("Wall time (ms)  : %d%n", pr.metrics.wallMillis);

            // In thử 10 số phone đầu tiên
            pr.items.stream().limit(10).forEach(System.out::println);
        } finally {
            if (reader instanceof AutoCloseable) {
                ((AutoCloseable) reader).close();
            }
        }
    }
}