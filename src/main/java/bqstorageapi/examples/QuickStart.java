package bqstorageapi.examples;

import bqstorageapi.api.StorageReader;
import bqstorageapi.core.reader.BigQueryStorageReaderBuilder;
import bqstorageapi.config.ClientConfig;
import bqstorageapi.config.ReadOptions;
import bqstorageapi.config.RetryConfig;
import bqstorageapi.model.PagedResult;
import bqstorageapi.model.QueryMetrics;

import java.nio.file.Path;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // Thay giá trị bằng dự án và bảng thật của bạn
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
                        .maxRows(1_00_000)  // 0/âm => không giới hạn
                        .maxStreams(1)
                        .workers(2)
                        .targetBatchRows(10_000)
                        .build())
                .build();

        try {
//            PagedResult<String> pr = reader.readSingleColumn(table, "phone", null, Object::toString);
//
//            System.out.println("Fetched rows    : " + pr.items.size());
//            System.out.println("Pages (streams) : " + pr.metrics.pages);
//            System.out.println("Bytes delivered : " + pr.metrics.totalBytesProcessed);
//            System.out.printf ("Delivered (TB)  : %.6f%n", pr.metrics.processedTB());
//            System.out.printf ("Wall time (ms)  : %d%n", pr.metrics.wallMillis);
//            // In thử 10 số phone đầu tiên
//            pr.items.stream().limit(10).forEach(System.out::println);


            QueryMetrics metrics = reader.readSingleColumnToSink(
                    table,
                    "phone",
                    null,                   // dùng default ReadOptions
                    Object::toString,       // mapper: Object -> String
                    batch -> {
                        // Sink: in ra độ dài batch
                        System.out.println("Batch size = " + batch.size());
                        // bạn có thể in batch.get(0) ... để debug nội dung
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