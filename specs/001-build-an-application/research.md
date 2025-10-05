# Research: Multi-Source ETL Application

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Technology Stack Decisions

### 1. Spark 3.5.6 + Scala 2.12 Compatibility
- **Decision**: Apache Spark 3.5.6 with Scala 2.12.18
- **Rationale**: Official Spark 3.5.x releases support both Scala 2.12 and 2.13. Scala 2.12 chosen for ecosystem maturity and broader library compatibility.
- **Alternatives Considered**:
  - Scala 2.13: More modern but some Spark ecosystem libraries lag in 2.13 support
  - Scala 3.x: Not supported by Spark 3.5.x

### 2. Avro Integration
- **Decision**: `org.apache.spark:spark-avro_2.12:3.5.6`
- **Rationale**: Official Spark module, provides `.avro()` format for DataFrames, handles schema evolution
- **Alternatives Considered**:
  - Apache Avro directly: Lower-level API, more boilerplate
  - Parquet: Not specified in requirements, Avro required for schema evolution

### 3. Dependency Injection Strategy
- **Decision**: Manual constructor-based dependency injection (no framework)
- **Rationale**: Meets "minimal dependencies" constraint, explicit and testable, no runtime reflection overhead
- **Alternatives Considered**:
  - Google Guice: Heavy dependency, adds complexity
  - MacWire: Compile-time DI, but still an additional dependency
  - Spring: Enterprise framework, massive footprint

### 4. YAML Configuration Parsing
- **Decision**: SnakeYAML `org.yaml:snakeyaml:2.0`
- **Rationale**: Lightweight (~300KB), mature, widely used, simple API
- **Alternatives Considered**:
  - Circe-YAML: Type-safe but adds Circe core dependency (~2MB+)
  - Jackson YAML: Heavier than SnakeYAML

### 5. Vault Integration
- **Decision**: HashiCorp Vault with `com.bettercloud:vault-java-driver:5.1.0`
- **Rationale**: Industry standard, Docker Compose support, production-ready, secrets versioning
- **Alternatives Considered**:
  - AWS Secrets Manager: Cloud-only, no local Docker testing
  - File-based .env: Insecure, no rotation, fails security requirements

### 6. Testing Strategy
- **Decision**:
  - Unit: ScalaTest `org.scalatest:scalatest_2.12:3.2.17`
  - Mocking: ScalaMock or manual mocks
  - Embedded DBs: H2 for JDBC tests
  - Embedded Kafka: `org.apache.kafka:kafka_2.12:3.5.1` test utilities
- **Rationale**: No Docker required for tests (fast CI), deterministic, full isolation
- **Alternatives Considered**:
  - Testcontainers: Requires Docker, slower, more realistic but violates "mocked" requirement
  - Spark Testing Base: Useful for DataFrame assertions

### 7. Structured Logging
- **Decision**: SLF4J API + Log4j2 with JSON layout
- **Rationale**: Spark uses Log4j2 internally, JSON layout for structured logs, correlation IDs support
- **Alternatives Considered**:
  - Logback: Not Spark's default logger
  - java.util.logging: Limited structured logging

### 8. Kafka Integration
- **Decision**: Spark Structured Streaming Kafka source/sink with checkpointing
- **Rationale**: Built-in offset management, exactly-once semantics, fault tolerance
- **Alternatives Considered**:
  - Kafka Consumer API directly: Complex offset handling, manual checkpoint logic

### 9. S3 Integration
- **Decision**: `org.apache.hadoop:hadoop-aws:3.3.4` with S3A filesystem
- **Rationale**: Spark's native S3 connector, optimized for large files, supports multipart uploads
- **Alternatives Considered**:
  - AWS SDK for Java: Lower-level, requires more code for Spark integration

### 10. JDBC Drivers
- **Decision**:
  - PostgreSQL: `org.postgresql:postgresql:42.6.0`
  - MySQL: `com.mysql:mysql-connector-j:8.1.0`
- **Rationale**: Official drivers, production-tested, JDBC 4.3 compliant
- **Alternatives Considered**: Generic JDBC drivers (lack optimizations)

## Dependencies Summary (Gradle)

```gradle
// Core
org.apache.spark:spark-core_2.12:3.5.6
org.apache.spark:spark-sql_2.12:3.5.6
org.apache.spark:spark-streaming_2.12:3.5.6
org.apache.spark:spark-avro_2.12:3.5.6

// Data Sources
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
org.postgresql:postgresql:42.6.0
com.mysql:mysql-connector-j:8.1.0
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262

// Configuration & Secrets
org.yaml:snakeyaml:2.0
com.bettercloud:vault-java-driver:5.1.0

// Logging
org.slf4j:slf4j-api:2.0.9
org.apache.logging.log4j:log4j-slf4j2-impl:2.20.0
org.apache.logging.log4j:log4j-core:2.20.0

// Testing
org.scalatest:scalatest_2.12:3.2.17 % Test
org.apache.kafka:kafka_2.12:3.5.1 % Test (for embedded Kafka)
com.h2database:h2:2.2.224 % Test (for JDBC tests)
org.scalamock:scalamock_2.12:5.2.0 % Test (optional mocking)
```

## Architecture Patterns

### Dependency Injection Pattern
```scala
// Trait-based abstraction
trait DataExtractor {
  def extract(config: ExtractorConfig)(implicit spark: SparkSession): DataFrame
}

// Constructor injection
class KafkaExtractor(vault: VaultClient, logger: StructuredLogger) extends DataExtractor {
  override def extract(config: ExtractorConfig)(implicit spark: SparkSession): DataFrame = {
    val credentials = vault.getSecret(config.credentialsPath)
    // ... implementation
  }
}

// Manual wiring in Main
object Main {
  def main(args: Array[String]): Unit = {
    val vault = new VaultClient(vaultConfig)
    val logger = new StructuredLogger()

    val extractors = Map(
      "kafka" -> new KafkaExtractor(vault, logger),
      "postgres" -> new PostgresExtractor(vault, logger),
      // ...
    )
  }
}
```

### Avro Schema Management
```scala
case class AvroRecord(
  schema: Schema,
  data: GenericRecord,
  lineage: LineageMetadata
)

case class LineageMetadata(
  sourceSystem: String,
  sourceTimestamp: Long,
  extractionTimestamp: Long,
  transformationChain: List[String],
  pipelineId: String,
  runId: String
)
```

### YAML Pipeline Configuration Example
```yaml
pipeline:
  name: "postgres-to-s3-aggregation"
  execution_mode: "batch"  # or "micro-batch"

  source:
    type: "postgres"
    credentials_path: "secret/postgres/prod"
    query: "SELECT * FROM orders WHERE created_at > '2025-01-01'"

  transformations:
    - type: "aggregation"
      group_by: ["customer_id", "product_id"]
      aggregates:
        - column: "amount"
          function: "sum"
          alias: "total_amount"

    - type: "filter"
      condition: "total_amount > 1000"

  sink:
    type: "s3"
    credentials_path: "secret/s3/prod"
    path: "s3a://my-bucket/aggregated-orders/"
    write_mode: "overwrite"
    format: "avro"

  performance:
    partitions: 200
    executor_memory: "4g"
    executor_cores: 2

  quality:
    schema_validation: true
    null_check_columns: ["customer_id", "product_id"]
    quarantine_path: "s3a://my-bucket/errors/"
```

## Testing Strategy

### Unit Testing Approach
- **Extractors**: Mock Vault client, mock Spark read operations
- **Transformers**: Use sample DataFrames, assert transformation logic
- **Loaders**: Mock Spark write operations, verify write parameters
- **Quality Checks**: Test schema validation, quarantine logic with invalid data

### Integration Testing Approach
- **End-to-End Pipelines**: Use embedded H2 (for JDBC), embedded Kafka, local filesystem (for S3 simulation)
- **Performance Tests**: Generate representative datasets, measure throughput
- **Error Scenarios**: Inject failures, verify error handling and quarantine

## Performance Considerations

### Spark Configuration Tuning
- **Batch Jobs**: Use `coalesce()` to reduce partitions before writing large files
- **Micro-Batch**: Set `maxOffsetsPerTrigger` for Kafka rate limiting
- **Memory**: Allocate 70% executor memory for storage, 30% for computation
- **Shuffles**: Minimize via broadcast joins for small dimension tables

### Data Skew Mitigation
- **Salting**: Add random salt to skewed keys before aggregation
- **Adaptive Query Execution**: Enable AQE in Spark 3.x for runtime optimization
- **Repartitioning**: Use `repartition()` by skewed column with higher parallelism

## Security Considerations

### Vault Integration
- Token-based authentication for local development
- AppRole authentication for production
- Secret rotation support via Vault API

### Credential Handling
- Never log credentials
- Use Vault dynamic secrets where possible
- Rotate secrets regularly

## Compliance & Data Governance

### Data Lineage Tracking
- Embed lineage metadata in Avro records
- Log lineage information to structured logs
- Track: source system, transformation chain, pipeline ID, run ID

### Audit Trail
- Log all extract/transform/load operations
- Include: timestamp, user (if applicable), records processed, success/failure status
- Retention: Logs retained for compliance period (configurable)

