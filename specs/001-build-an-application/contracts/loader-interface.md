# Contract: DataLoader Interface

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Interface Definition

```scala
package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataLoader abstracts data loading to various sinks.
 * Implementations: KafkaLoader, PostgresLoader, MySQLLoader, S3Loader
 */
trait DataLoader {

  /**
   * Load DataFrame to configured sink.
   *
   * @param data DataFrame to load (Avro-compatible with lineage metadata)
   * @param config Sink-specific configuration (credentials, parameters, write mode)
   * @param spark Implicit Spark session
   * @return LoadResult with metrics (records written, failures)
   * @throws LoadException if load operation fails
   */
  def load(
    data: DataFrame,
    config: SinkConfig
  )(implicit spark: SparkSession): LoadResult

  /**
   * Validate sink configuration before loading.
   *
   * @param config Sink configuration to validate
   * @return ValidationResult with errors if invalid
   */
  def validateConfig(config: SinkConfig): ValidationResult

  /**
   * Sink type identifier (matches SinkConfig.type).
   *
   * @return Sink type string ("kafka" | "postgres" | "mysql" | "s3")
   */
  def sinkType: String
}
```

## Contract Tests

### Test 1: Load Returns Success Result

**Given**: Valid DataFrame and SinkConfig
**When**: `load()` is called
**Then**:
- LoadResult.success == true
- LoadResult.recordsWritten == input DataFrame row count
- LoadResult.recordsFailed == 0

### Test 2: Validate Config Detects Missing Parameters

**Given**: SinkConfig missing required parameters (e.g., no `bucket` for S3)
**When**: `validateConfig()` is called
**Then**:
- ValidationResult.isValid == false
- ValidationResult.errors contains specific error message

### Test 3: Validate Config Accepts Valid Parameters

**Given**: SinkConfig with all required parameters
**When**: `validateConfig()` is called
**Then**:
- ValidationResult.isValid == true
- ValidationResult.errors is empty

### Test 4: Load Handles Write Failure (Mocked)

**Given**: Mocked sink that throws write exception
**When**: `load()` is called
**Then**:
- LoadException is thrown
- Exception message contains sink type and error details

### Test 5: Idempotent Load (Upsert Mode)

**Given**: Same DataFrame loaded twice with writeMode="upsert"
**When**: `load()` is called twice
**Then**:
- Second load does not duplicate records
- Final record count equals input DataFrame count

### Test 6: Append Mode Adds Records

**Given**: DataFrame loaded with writeMode="append"
**When**: `load()` is called twice with different data
**Then**:
- Final record count equals sum of both DataFrames

### Test 7: Overwrite Mode Replaces Data

**Given**: DataFrame loaded with writeMode="overwrite"
**When**: `load()` is called twice
**Then**:
- Final record count equals second DataFrame count only

## Implementation Requirements

### KafkaLoader

**Required Parameters**:
- `topic`: Kafka topic name
- `bootstrapServers`: Comma-separated broker list

**Behavior**:
- Use Spark Structured Streaming Kafka sink
- Serialize DataFrame to Avro before writing
- Key by `pipelineId` or user-specified column
- Write mode: append only (Kafka is append-only)

### PostgresLoader

**Required Parameters**:
- `jdbcUrl`: PostgreSQL JDBC connection string
- `table`: Target table name
- `upsertKeys`: List[String] - Columns for upsert (if writeMode="upsert")

**Behavior**:
- Retrieve credentials from Vault
- Use Spark JDBC writer
- Append: `mode("append")`
- Overwrite: `mode("overwrite")`
- Upsert: Use PostgreSQL `ON CONFLICT` or MERGE via custom logic

### MySQLLoader

**Required Parameters**:
- `jdbcUrl`: MySQL JDBC connection string
- `table`: Target table name
- `upsertKeys`: List[String] - Columns for upsert (if writeMode="upsert")

**Behavior**:
- Retrieve credentials from Vault
- Use Spark JDBC writer
- Append: `mode("append")`
- Overwrite: `mode("overwrite")`
- Upsert: Use MySQL `INSERT ... ON DUPLICATE KEY UPDATE`

### S3Loader

**Required Parameters**:
- `bucket`: S3 bucket name
- `prefix`: Path prefix
- `format`: "avro" | "parquet" | "json"
- `partitionBy`: Option[List[String]] - Partition columns

**Behavior**:
- Retrieve AWS credentials from Vault
- Use Spark DataFrameWriter with S3A filesystem
- Append: `mode("append")`
- Overwrite: `mode("overwrite")`
- Partition by specified columns (e.g., date)

## Exception Handling

All loaders MUST throw `LoadException` with:
- Sink type
- Configuration summary (no credentials)
- Root cause exception

```scala
case class LoadException(
  sinkType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$sinkType] $message", cause)
```

## Performance Expectations

- **Kafka**: Batching for throughput (tune `batch.size`, `linger.ms`)
- **JDBC**: Batch inserts (tune `batchsize`, `numPartitions`)
- **S3**: Multipart uploads, compression (snappy for Avro/Parquet)

## Testing Strategy

- **Unit Tests**: Mock Vault client, mock Spark write operations
- **Integration Tests**: Embedded H2 (JDBC), embedded Kafka, local filesystem (S3)
- **Contract Tests**: Verify interface compliance for all four implementations
- **Idempotency Tests**: Verify upsert mode prevents duplicates
