# Contract: DataExtractor Interface

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Interface Definition

```scala
package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataExtractor abstracts data extraction from various sources.
 * Implementations: KafkaExtractor, PostgresExtractor, MySQLExtractor, S3Extractor
 */
trait DataExtractor {

  /**
   * Extract data from configured source.
   *
   * @param config Source-specific configuration (credentials, parameters)
   * @param spark Implicit Spark session for DataFrame creation
   * @return DataFrame with Avro-compatible schema and embedded lineage metadata
   * @throws ExtractionException if extraction fails
   */
  def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame

  /**
   * Validate source configuration before extraction.
   *
   * @param config Source configuration to validate
   * @return ValidationResult with errors if invalid
   */
  def validateConfig(config: SourceConfig): ValidationResult

  /**
   * Source type identifier (matches SourceConfig.type).
   *
   * @return Source type string ("kafka" | "postgres" | "mysql" | "s3")
   */
  def sourceType: String
}
```

## Contract Tests

### Test 1: Extract Returns Valid DataFrame

**Given**: Valid SourceConfig with mocked credentials
**When**: `extract()` is called
**Then**:
- DataFrame is not null
- DataFrame has at least the expected columns
- DataFrame schema is Avro-compatible
- Lineage metadata is embedded in DataFrame

### Test 2: Validate Config Detects Missing Parameters

**Given**: SourceConfig missing required parameters (e.g., no `jdbcUrl` for PostgreSQL)
**When**: `validateConfig()` is called
**Then**:
- ValidationResult.isValid == false
- ValidationResult.errors contains specific error message

### Test 3: Validate Config Accepts Valid Parameters

**Given**: SourceConfig with all required parameters
**When**: `validateConfig()` is called
**Then**:
- ValidationResult.isValid == true
- ValidationResult.errors is empty

### Test 4: Extract Handles Connection Failure (Mocked)

**Given**: Mocked source that throws connection exception
**When**: `extract()` is called
**Then**:
- ExtractionException is thrown
- Exception message contains source type and error details

### Test 5: Lineage Metadata is Embedded

**Given**: Valid SourceConfig
**When**: `extract()` is called
**Then**:
- DataFrame contains lineage columns: `sourceSystem`, `extractionTimestamp`, `pipelineId`, `runId`
- `sourceSystem` matches extractor's sourceType

## Implementation Requirements

### KafkaExtractor

**Required Parameters**:
- `topic`: Kafka topic name
- `bootstrapServers`: Comma-separated broker list
- `startingOffsets`: "earliest" | "latest"

**Behavior**:
- Use Spark Structured Streaming Kafka source
- Deserialize Avro payloads
- Add lineage metadata to each record

### PostgresExtractor

**Required Parameters**:
- `jdbcUrl`: PostgreSQL JDBC connection string
- `table` OR `query`: Table name or SQL query

**Behavior**:
- Retrieve credentials from Vault using `credentialsPath`
- Use Spark JDBC reader
- Add lineage metadata

### MySQLExtractor

**Required Parameters**:
- `jdbcUrl`: MySQL JDBC connection string
- `table` OR `query`: Table name or SQL query

**Behavior**:
- Retrieve credentials from Vault
- Use Spark JDBC reader
- Add lineage metadata

### S3Extractor

**Required Parameters**:
- `bucket`: S3 bucket name
- `prefix`: Path prefix
- `format`: "avro" | "parquet" | "json"

**Behavior**:
- Retrieve AWS credentials from Vault
- Use Spark DataFrameReader with S3A filesystem
- Add lineage metadata

## Exception Handling

All extractors MUST throw `ExtractionException` with:
- Source type
- Configuration summary (no credentials)
- Root cause exception

```scala
case class ExtractionException(
  sourceType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$sourceType] $message", cause)
```

## Performance Expectations

- **Kafka**: Stream processing with backpressure (maxOffsetsPerTrigger)
- **JDBC**: Parallel reads using `numPartitions`, `partitionColumn`
- **S3**: Predicate pushdown for Parquet, parallel file reads

## Testing Strategy

- **Unit Tests**: Mock Vault client, mock Spark read operations
- **Integration Tests**: Embedded H2 (JDBC), embedded Kafka, local filesystem (S3)
- **Contract Tests**: Verify interface compliance for all four implementations
