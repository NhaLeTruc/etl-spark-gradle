# Quickstart Integration Tests

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Purpose

Integration tests validate end-to-end ETL pipelines with fully mocked sources and sinks. These tests serve as executable documentation and acceptance criteria.

## Test Environment

- **Databases**: Embedded H2 (simulates PostgreSQL/MySQL)
- **Kafka**: Embedded Kafka using `kafka-streams-test-utils`
- **S3**: Local filesystem with `file://` protocol
- **Vault**: Mock Vault client returning test credentials
- **Spark**: Local mode with `master("local[*]")`

## Scenario 1: Batch Pipeline - PostgreSQL to S3 with Aggregation

### Objective
Validate batch processing from JDBC source to S3 sink with aggregation transformation.

### Setup

**1. Embedded H2 Database**:
```sql
CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  product_id INT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP NOT NULL
);

-- Insert 100,000 sample rows
INSERT INTO orders VALUES ...;
```

**2. Pipeline YAML Configuration** (`test-pipelines/postgres-to-s3-batch.yaml`):
```yaml
pipeline:
  name: "postgres-to-s3-aggregation"
  execution_mode: "batch"

  source:
    type: "postgres"
    credentials_path: "test/h2/orders"
    parameters:
      jdbcUrl: "jdbc:h2:mem:testdb"
      query: "SELECT * FROM orders WHERE created_at > '2025-01-01'"

  transformations:
    - type: "aggregation"
      parameters:
        groupBy: ["customer_id", "product_id"]
        aggregates:
          - column: "amount"
            function: "sum"
            alias: "total_amount"
          - column: "order_id"
            function: "count"
            alias: "order_count"

    - type: "filter"
      parameters:
        condition: "total_amount > 1000"

  sink:
    type: "s3"
    credentials_path: "test/s3/output"
    parameters:
      bucket: "test-bucket"
      prefix: "aggregated-orders/"
      format: "avro"
    write_mode: "overwrite"

  performance:
    partitions: 10
    executor_memory: "2g"
    executor_cores: 1

  quality:
    schema_validation: true
    null_check_columns: ["customer_id", "product_id"]
    quarantine_path: "file:///tmp/quarantine/"
```

### Execution Steps

1. Load pipeline configuration from YAML
2. Validate configuration (all validators pass)
3. Execute pipeline:
   - Extract: PostgresExtractor reads from H2
   - Transform: AggregationTransformer groups and sums
   - Filter: FilterTransformer applies condition
   - Load: S3Loader writes Avro files to local filesystem

### Expected Results

**Data Assertions**:
- **Input**: 100,000 order records
- **After Aggregation**: ~5,000 unique (customer_id, product_id) pairs
- **After Filter**: ~1,200 pairs with total_amount > 1000
- **Output**: 1,200 Avro records written to `file:///tmp/aggregated-orders/`

**Schema Assertions**:
- Output Avro schema contains: `customer_id`, `product_id`, `total_amount`, `order_count`
- Lineage metadata fields: `sourceSystem="postgres"`, `transformationChain=["aggregation", "filter"]`

**Metrics Assertions**:
- ExecutionMetrics.recordsExtracted == 100,000
- ExecutionMetrics.recordsTransformed == 1,200
- ExecutionMetrics.recordsLoaded == 1,200
- ExecutionMetrics.recordsFailed == 0
- ExecutionMetrics.status == "success"

**Quality Assertions**:
- No quarantined records (valid data only)
- Null checks pass for `customer_id`, `product_id`

## Scenario 2: Micro-Batch Pipeline - Kafka to MySQL with Windowing

### Objective
Validate micro-batch streaming from Kafka to MySQL with tumbling window aggregation.

### Setup

**1. Embedded Kafka**:
```scala
val kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "localhost:9092")

// Publish test events at 1000 records/sec
(1 to 10000).foreach { i =>
  val event = s"""{"event_id":$i,"user_id":${i % 100},"timestamp":${System.currentTimeMillis()}}"""
  producer.send(new ProducerRecord("events", event))
  Thread.sleep(1) // 1000 records/sec
}
```

**2. Embedded H2 as MySQL Proxy**:
```sql
CREATE TABLE event_counts (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  user_id INT,
  event_count BIGINT,
  PRIMARY KEY (window_start, user_id)
);
```

**3. Pipeline YAML Configuration** (`test-pipelines/kafka-to-mysql-windowing.yaml`):
```yaml
pipeline:
  name: "kafka-windowing-micro-batch"
  execution_mode: "micro-batch"

  source:
    type: "kafka"
    credentials_path: "test/kafka/events"
    parameters:
      topic: "events"
      bootstrapServers: "localhost:9092"
      startingOffsets: "earliest"

  transformations:
    - type: "windowing"
      parameters:
        windowType: "tumbling"
        duration: "10 seconds"
        timestampColumn: "timestamp"

    - type: "aggregation"
      parameters:
        groupBy: ["window_start", "window_end", "user_id"]
        aggregates:
          - column: "event_id"
            function: "count"
            alias: "event_count"

  sink:
    type: "mysql"
    credentials_path: "test/h2/mysql"
    parameters:
      jdbcUrl: "jdbc:h2:mem:mysqldb"
      table: "event_counts"
      upsertKeys: ["window_start", "user_id"]
    write_mode: "upsert"

  performance:
    partitions: 5
    executor_memory: "2g"
    executor_cores: 1
    maxOffsetsPerTrigger: 10000

  quality:
    schema_validation: true
    null_check_columns: ["user_id"]
    quarantine_path: "file:///tmp/quarantine/"
```

### Execution Steps

1. Start embedded Kafka and publish events
2. Load pipeline configuration
3. Execute micro-batch pipeline:
   - Extract: KafkaExtractor reads streaming data
   - Transform: WindowingTransformer applies 10-second tumbling windows
   - Aggregate: Count events per window per user
   - Load: MySQLLoader upserts to H2

4. Run pipeline for 30 seconds (3 windows)

### Expected Results

**Data Assertions**:
- **Input**: 30,000 events (1000/sec * 30 seconds)
- **Windows**: 3 windows (0-10s, 10-20s, 20-30s)
- **Users**: 100 unique users
- **Output**: 300 rows in event_counts (3 windows * 100 users)

**Windowing Assertions**:
- Each window duration is exactly 10 seconds
- Windows do not overlap (tumbling)
- event_count per window ≈ 100 events per user (1000 events/sec ÷ 100 users × 10 sec)

**Idempotency Assertions**:
- Re-run pipeline with same Kafka data
- Upsert mode ensures no duplicates
- Final row count remains 300

**Metrics Assertions**:
- ExecutionMetrics.recordsExtracted == 30,000
- ExecutionMetrics.recordsTransformed == 300
- ExecutionMetrics.recordsLoaded == 300
- ExecutionMetrics.status == "success"

## Scenario 3: Error Handling - Invalid Schema Quarantine

### Objective
Validate data quality checks and error quarantine functionality.

### Setup

**1. Mock Data with Schema Violations**:
```scala
val validRecords = Seq(
  """{"customer_id":1,"product_id":100,"amount":50.0}""",
  """{"customer_id":2,"product_id":200,"amount":75.0}"""
)

val invalidRecords = Seq(
  """{"customer_id":null,"product_id":100,"amount":50.0}""", // NULL customer_id
  """{"product_id":200,"amount":75.0}""",                     // Missing customer_id
  """{"customer_id":"invalid","product_id":200,"amount":"not_a_number"}""" // Wrong types
)

// Publish mixed valid/invalid records to Kafka
```

**2. Pipeline YAML Configuration** (`test-pipelines/error-handling.yaml`):
```yaml
pipeline:
  name: "error-handling-test"
  execution_mode: "batch"

  source:
    type: "kafka"
    credentials_path: "test/kafka/mixed"
    parameters:
      topic: "mixed-data"
      bootstrapServers: "localhost:9092"
      startingOffsets: "earliest"

  transformations: []

  sink:
    type: "s3"
    credentials_path: "test/s3/output"
    parameters:
      bucket: "test-bucket"
      prefix: "valid-output/"
      format: "avro"
    write_mode: "append"

  performance:
    partitions: 2
    executor_memory: "2g"
    executor_cores: 1

  quality:
    schema_validation: true
    null_check_columns: ["customer_id", "product_id"]
    quarantine_path: "file:///tmp/quarantine/"
```

### Execution Steps

1. Publish 2 valid + 3 invalid records to Kafka
2. Load pipeline configuration
3. Execute pipeline:
   - Extract: KafkaExtractor reads all records
   - Validate: Schema validation detects 3 invalid records
   - Quarantine: Invalid records written to error path
   - Load: Only 2 valid records written to S3

### Expected Results

**Data Assertions**:
- **Input**: 5 total records
- **Valid Output**: 2 records in S3
- **Quarantined**: 3 records in `/tmp/quarantine/`

**Quarantine File Structure**:
```json
{
  "original_record": "{\"customer_id\":null,\"product_id\":100,\"amount\":50.0}",
  "error_type": "null_check_violation",
  "error_message": "Column 'customer_id' contains null value",
  "pipeline_id": "error-handling-test",
  "run_id": "uuid",
  "quarantine_timestamp": 1704470400000
}
```

**Metrics Assertions**:
- ExecutionMetrics.recordsExtracted == 5
- ExecutionMetrics.recordsTransformed == 2
- ExecutionMetrics.recordsLoaded == 2
- ExecutionMetrics.recordsFailed == 3
- ExecutionMetrics.status == "partial" (some records failed)

**Logging Assertions**:
- Structured logs contain:
  - `level="WARN"`, `message="Records quarantined"`, `count=3`
  - `quarantine_path="/tmp/quarantine/"`
  - Error details for each invalid record

**Continuation Assertion**:
- Pipeline completes successfully despite invalid records
- No exception thrown (graceful degradation)

## Test Execution

### Run All Scenarios
```bash
./gradlew test --tests "com.etl.integration.QuickstartSpec"
```

### Run Individual Scenario
```bash
./gradlew test --tests "com.etl.integration.QuickstartSpec.testPostgresToS3Batch"
```

## Success Criteria

All three scenarios MUST:
1. Pass all data assertions
2. Pass all schema assertions
3. Pass all metrics assertions
4. Complete within timeout (60 seconds per scenario)
5. Clean up resources (H2, Kafka, temp files)

Failure of any scenario blocks merging implementation code.
