# BigQuery Storage API Java Client

A Java client library for reading data from Google BigQuery using the Storage API. Supports efficient, paged, and batched reads with customizable options.

## Features

- Read BigQuery tables using the Storage API
- Paged and batched data retrieval
- Configurable retry and read options
- Service account and ADC authentication
- Example usage included

## Directory Structure

- `api/` - Public API interfaces and builders
- `config/` - Configuration classes (client, read, retry)
- `core/reader/` - Core reader implementations
- `examples/` - Example usage and quick start
- `handler/` - Batch and value mapping handlers
- `model/` - Data models (paging, metrics, etc.)

## Getting Started

### Prerequisites

- Java 11+
- Maven
- Google Cloud project with BigQuery enabled
- Service account JSON key (if not using ADC)

## Build

Use Maven to build the project:

```
mvn clean install
```

## Usage Example

See `src/main/java/bqstorageapi/examples/QuickStartPaged.java` for a quick start:

```java
StorageReader reader = new BigQueryStorageReaderBuilder()
    .client(ClientConfig.newBuilder()
        .projectId("your-project-id")
        .dataset("your-dataset")
        .location("US")
        .serviceAccountJson(Path.of("path/to/key.json"))
        .build())
    .retry(RetryConfig.newBuilder().build())
    .defaultReadOptions(ReadOptions.newBuilder()
        .maxRows(100_000)
        .maxStreams(2)
        .workers(2)
        .targetBatchRows(10_000)
        .build())
    .build();

PagedResult<String> pr = reader.readSingleColumn("your-table", "column", null, Object::toString);
```

## Configuration

- `ClientConfig`: Set project, dataset, location, and credentials
- `ReadOptions`: Control row limits, streams, batch size, etc.
- `RetryConfig`: Configure retry behavior

## License

MIT

## Author

HTKP
