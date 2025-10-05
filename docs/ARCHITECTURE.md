# Architecture Overview

## Table of Contents

- [System Architecture](#system-architecture)
- [Module Structure](#module-structure)
- [Design Patterns](#design-patterns)
- [Data Flow](#data-flow)
- [Component Details](#component-details)
- [Extension Points](#extension-points)

## System Architecture

The ETL Spark Gradle application follows a modular, plugin-based architecture built on Apache Spark's distributed computing framework.

```
┌─────────────────────────────────────────────────────────────┐
│                      Main Application                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           PipelineExecutor                           │   │
│  │  • Orchestrates Extract-Transform-Load phases        │   │
│  │  • Manages execution context                         │   │
│  │  • Collects metrics                                  │   │
│  └──────────────────────────────────────────────────────┘   │
│           │                  │                  │            │
│           ▼                  ▼                  ▼            │
│  ┌─────────────┐    ┌───────────────┐   ┌────────────┐     │
│  │  Extractor  │    │  Transformer  │   │   Loader   │     │
│  │  Registry   │    │   Registry    │   │  Registry  │     │
│  └─────────────┘    └───────────────┘   └────────────┘     │
│           │                  │                  │            │
└───────────┼──────────────────┼──────────────────┼────────────┘
            │                  │                  │
            ▼                  ▼                  ▼
   ┌────────────────┐  ┌─────────────────┐  ┌──────────────┐
   │  Extractors    │  │  Transformers   │  │   Loaders    │
   ├────────────────┤  ├─────────────────┤  ├──────────────┤
   │ • Kafka        │  │ • Aggregation   │  │ • Kafka      │
   │ • PostgreSQL   │  │ • Join          │  │ • PostgreSQL │
   │ • MySQL        │  │ • Windowing     │  │ • MySQL      │
   │ • S3           │  │ • Filter        │  │ • S3         │
   │                │  │ • Map           │  │              │
   └────────────────┘  └─────────────────┘  └──────────────┘
            │                  │                  │
            ▼                  ▼                  ▼
   ┌───────────────────────────────────────────────────────┐
   │              Apache Spark DataFrame API                │
   │  • Distributed processing                             │
   │  • Lazy evaluation                                    │
   │  • Catalyst query optimizer                           │
   └───────────────────────────────────────────────────────┘
```

### Cross-Cutting Concerns

```
┌────────────────────────────────────────────────────────────┐
│                  Cross-Cutting Components                  │
├────────────────────────────────────────────────────────────┤
│  Data Quality    │  Lineage       │  Logging    │  Vault   │
│  ┌────────────┐  │  ┌──────────┐  │  ┌───────┐  │  ┌────┐ │
│  │ Schema     │  │  │ Lineage  │  │  │ Struc │  │  │ Sec│ │
│  │ Validator  │  │  │ Tracker  │  │  │ tured │  │  │ ret│ │
│  ├────────────┤  │  └──────────┘  │  │ Logger│  │  │ Mgr│ │
│  │ Quarantine │  │                │  └───────┘  │  └────┘ │
│  │ Writer     │  │                │             │         │
│  └────────────┘  │                │             │         │
│  ┌────────────┐  │                │  ┌───────┐  │         │
│  │ Data       │  │                │  │ Metric│  │         │
│  │ Quality    │  │                │  │ Collec│  │         │
│  │ Checker    │  │                │  │ tor   │  │         │
│  └────────────┘  │                │  └───────┘  │         │
└────────────────────────────────────────────────────────────┘
```

## Module Structure

### Core Module (`com.etl.core`)

**Purpose**: Defines contracts and core entities used across all modules.

**Components**:
- **Traits (Interfaces)**:
  - `DataExtractor`: Contract for data extraction
  - `DataTransformer`: Contract for data transformation
  - `DataLoader`: Contract for data loading

- **Case Classes (Entities)**:
  - `SourceConfig`: Source configuration
  - `SinkConfig`: Sink configuration
  - `TransformationConfig`: Transformation configuration
  - `PipelineConfig`: Complete pipeline definition
  - `RunContext`: Execution context
  - `ExecutionMetrics`: Pipeline execution metrics
  - `LineageMetadata`: Data lineage tracking
  - `ValidationResult`: Schema validation results
  - `LoadResult`: Load operation results

- **Exceptions**:
  - `ExtractionException`: Extraction failures
  - `TransformationException`: Transformation failures
  - `LoadException`: Load failures

**Design Decision**: Using traits (interfaces) enables dependency injection and facilitates testing with mock implementations.

### Extractor Module (`com.etl.extractor`)

**Purpose**: Implements data extraction from various sources.

**Implementations**:
- `KafkaExtractor`: Kafka topic extraction
- `PostgresExtractor`: PostgreSQL table extraction via JDBC
- `MySQLExtractor`: MySQL table extraction via JDBC
- `S3Extractor`: S3 object extraction (Avro, Parquet, JSON, CSV)

**Common Pattern**:
```scala
class SomeExtractor extends DataExtractor {
  override def extract(config: SourceConfig, runContext: RunContext): ExtractionResult = {
    // 1. Validate configuration
    validateConfig(config)

    // 2. Extract data (using Spark)
    val df = spark.read.format(...).load(...)

    // 3. Embed lineage metadata
    val withLineage = lineageTracker.embedLineage(df, metadata)

    // 4. Return result with metrics
    ExtractionResult(...)
  }
}
```

### Transformer Module (`com.etl.transformer`)

**Purpose**: Implements data transformations using Spark DataFrame operations.

**Implementations**:
- `AggregationTransformer`: GroupBy and aggregations (sum, avg, count, etc.)
- `JoinTransformer`: Joins between datasets (inner, left, right, full)
- `WindowingTransformer`: Tumbling and sliding time windows
- `FilterTransformer`: Row filtering based on SQL conditions
- `MapTransformer`: Column expressions and type conversions

**Common Pattern**:
```scala
class SomeTransformer extends DataTransformer {
  override def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): TransformationResult = {
    // 1. Parse transformation parameters
    val params = parseOptions(config.options)

    // 2. Apply Spark transformation
    val transformed = input.groupBy(...).agg(...)

    // 3. Update lineage
    val withLineage = lineageTracker.updateLineage(transformed, config.name)

    // 4. Return result with metrics
    TransformationResult(...)
  }
}
```

### Loader Module (`com.etl.loader`)

**Purpose**: Implements data loading to various sinks.

**Implementations**:
- `KafkaLoader`: Kafka topic writer
- `PostgresLoader`: PostgreSQL table writer (append/overwrite/upsert)
- `MySQLLoader`: MySQL table writer (append/overwrite/upsert)
- `S3Loader`: S3 object writer (Avro, Parquet, JSON, CSV)

**Upsert Implementation**:
PostgreSQL and MySQL loaders implement upsert using database-specific syntax:
- PostgreSQL: `ON CONFLICT ... DO UPDATE`
- MySQL: `ON DUPLICATE KEY UPDATE`

### Pipeline Module (`com.etl.pipeline`)

**Purpose**: Orchestrates end-to-end pipeline execution.

**Components**:
- `PipelineExecutor`: Main orchestrator
  - `execute()`: Basic pipeline without quality checks
  - `executeWithQuality()`: Pipeline with data quality validation

- Registries (Service Locator pattern):
  - `ExtractorRegistry`: Maps source types → extractor implementations
  - `TransformerRegistry`: Maps transformation types → transformer implementations
  - `LoaderRegistry`: Maps sink types → loader implementations

**Execution Flow**:
```scala
def execute(config: PipelineConfig, runContext: RunContext): ExecutionMetrics = {
  // Phase 1: Extract
  val extractor = extractorRegistry.get(config.source.`type`)
  val extractResult = extractor.extract(config.source, runContext)
  var data = extractResult.data

  // Phase 2: Transform (chain)
  config.transformations.foreach { transformConfig =>
    val transformer = transformerRegistry.get(transformConfig.`type`)
    val transformResult = transformer.transform(data, transformConfig, runContext)
    data = transformResult.data
  }

  // Phase 3: Load
  val loader = loaderRegistry.get(config.sink.`type`)
  val loadResult = loader.load(data, config.sink, runContext)

  // Return metrics
  ExecutionMetrics(...)
}
```

### Quality Module (`com.etl.quality`)

**Purpose**: Data quality validation and quarantine.

**Components**:
- `SchemaValidator`: Validates DataFrame schema against expected schema
- `QuarantineWriter`: Writes invalid records with diagnostic metadata
- `DataQualityChecker`: Calculates quality metrics, splits valid/invalid records

**Quality Flow**:
```scala
// 1. Validate schema
val schemaResult = schemaValidator.validate(data, expectedSchema)

// 2. Check for null violations
val (validDF, invalidDF) = qualityChecker.splitValidInvalid(data, qualityConfig)

// 3. Quarantine invalid records
quarantineWriter.write(invalidDF, quarantinePath, runContext)

// 4. Proceed with valid records only
loader.load(validDF, sinkConfig, runContext)
```

### Lineage Module (`com.etl.lineage`)

**Purpose**: Track data provenance through the pipeline.

**Implementation**:
- Embeds `LineageMetadata` as JSON in `_lineage` column
- Tracks: pipeline ID, run ID, source type, transformation chain

**Usage**:
```scala
val metadata = LineageMetadata(
  pipelineId = "my-pipeline",
  runId = "run-001",
  sourceType = "postgres",
  sourceName = "orders",
  transformations = List("aggregate-by-customer")
)

val withLineage = lineageTracker.embedLineage(df, metadata)
```

### Logging Module (`com.etl.logging`)

**Purpose**: Structured logging and metrics collection.

**Components**:
- `StructuredLogger`: JSON-formatted logs with correlation IDs
- `MetricsCollector`: Tracks extraction/transformation/load metrics

**Log Format**:
```json
{
  "timestamp": 1704067200000,
  "level": "INFO",
  "message": "Pipeline execution completed",
  "correlationId": "uuid-1234",
  "pipelineId": "sales-pipeline",
  "runId": "run-001",
  "recordsExtracted": 100000,
  "recordsLoaded": 1200
}
```

### Configuration Module (`com.etl.config`)

**Purpose**: YAML pipeline configuration parsing.

**Implementation**:
- `YAMLConfigParser`: Parses YAML files into `PipelineConfig`
- Supports nested structures, collections
- Handles Vault references: `${VAULT:path/to/secret:key}`

### Vault Module (`com.etl.vault`)

**Purpose**: Secrets management integration.

**Implementation**:
- `VaultClient`: HashiCorp Vault integration
- Methods: `getSecret()`, `writeSecret()`
- Mock implementation for testing

## Design Patterns

### 1. Strategy Pattern
Used for extractors, transformers, and loaders.
- Each component type has multiple implementations
- Selected at runtime based on configuration

### 2. Registry Pattern (Service Locator)
Used for component discovery and instantiation.
- Registries map type strings to implementations
- Enables extensibility without modifying core code

### 3. Dependency Injection
Constructor-based DI throughout.
- All dependencies passed via constructor
- Facilitates testing with mock implementations

### 4. Template Method
Common execution flow in `PipelineExecutor`:
- Extract → Transform* → Load
- Metrics collection at each stage

### 5. Builder Pattern
Configuration parsing from YAML.
- Nested case classes built incrementally

## Data Flow

### Batch Pipeline Flow

```
PostgreSQL         Spark DataFrame          S3
┌────────┐        ┌─────────────┐        ┌────┐
│ sales  │  ───>  │  Extract    │  ───>  │    │
│ (100K) │        │  (JDBC)     │        │    │
└────────┘        └─────────────┘        │    │
                         │               │    │
                         ▼               │    │
                  ┌─────────────┐        │    │
                  │ Aggregate   │        │    │
                  │ (GroupBy)   │        │    │
                  │ (100K→1.2K) │        │    │
                  └─────────────┘        │    │
                         │               │    │
                         ▼               │    │
                  ┌─────────────┐        │    │
                  │ Load        │  ───>  │    │
                  │ (Parquet)   │        │ S3 │
                  └─────────────┘        └────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ Metrics     │
                  │ Collection  │
                  └─────────────┘
```

### Streaming (Micro-Batch) Flow

```
Kafka Topic       Spark DataFrame         MySQL
┌──────────┐     ┌──────────────┐       ┌──────┐
│ metrics  │ ──> │ Extract      │       │      │
│ (stream) │     │ (earliest)   │       │      │
└──────────┘     └──────────────┘       │      │
                        │               │      │
                        ▼               │      │
                 ┌──────────────┐       │      │
                 │ Parse JSON   │       │      │
                 │ (Map)        │       │      │
                 └──────────────┘       │      │
                        │               │      │
                        ▼               │      │
                 ┌──────────────┐       │      │
                 │ Window Agg   │       │      │
                 │ (1 min)      │       │      │
                 └──────────────┘       │      │
                        │               │      │
                        ▼               │      │
                 ┌──────────────┐       │      │
                 │ Load         │  ───> │ MySQL│
                 │ (Upsert)     │       │      │
                 └──────────────┘       └──────┘
```

## Component Details

### PipelineExecutor

**Responsibilities**:
1. Validate pipeline configuration
2. Resolve extractors, transformers, loaders from registries
3. Execute Extract-Transform-Load phases sequentially
4. Handle errors and rollback if necessary
5. Collect execution metrics
6. Support quality checks integration

**Error Handling**:
- Extraction failures → throw `ExtractionException`
- Transformation failures → throw `TransformationException`
- Load failures → throw `LoadException`
- All exceptions include context (pipeline ID, run ID, stage)

### Registries

**Pre-registered Implementations**:
- Extractors: `kafka`, `postgres`, `mysql`, `s3`
- Transformers: `aggregation`, `join`, `windowing`, `filter`, `map`
- Loaders: `kafka`, `postgres`, `mysql`, `s3`

**Extension**:
```scala
val customExtractor = new MyCustomExtractor()
extractorRegistry.register("custom", customExtractor)
```

### Data Quality Integration

**Quality Checks**:
1. Schema validation (optional)
2. Null checks on specified columns
3. Duplicate detection
4. Custom validation rules (extensible)

**Quarantine Mechanism**:
- Invalid records written to separate location
- Includes metadata: timestamp, pipeline ID, run ID, validation errors

## Extension Points

### Adding a New Extractor

```scala
// 1. Implement DataExtractor trait
class RedisExtractor extends DataExtractor {
  override def sourceType: String = "redis"

  override def extract(
    config: SourceConfig,
    runContext: RunContext
  ): ExtractionResult = {
    // Implementation
  }

  override def validateConfig(config: SourceConfig): Unit = {
    // Validation
  }
}

// 2. Register in ExtractorRegistry
extractorRegistry.register("redis", new RedisExtractor())
```

### Adding a New Transformation

```scala
// 1. Implement DataTransformer trait
class PivotTransformer extends DataTransformer {
  override def transformationType: String = "pivot"

  override def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): TransformationResult = {
    // Implementation
  }

  override def validateConfig(config: TransformationConfig): Unit = {
    // Validation
  }
}

// 2. Register in TransformerRegistry
transformerRegistry.register("pivot", new PivotTransformer())
```

### Adding a New Loader

```scala
// 1. Implement DataLoader trait
class ElasticsearchLoader extends DataLoader {
  override def sinkType: String = "elasticsearch"

  override def load(
    data: DataFrame,
    config: SinkConfig,
    runContext: RunContext
  ): LoadResult = {
    // Implementation
  }

  override def validateConfig(config: SinkConfig): Unit = {
    // Validation
  }
}

// 2. Register in LoaderRegistry
loaderRegistry.register("elasticsearch", new ElasticsearchLoader())
```

## Performance Considerations

### Partitioning Strategy

- **Extraction**: Leverage source partitioning (JDBC partitionColumn, Kafka partitions)
- **Transformation**: Repartition before expensive operations (joins, aggregations)
- **Loading**: Partition output by frequently-queried columns

### Caching Strategy

- Cache intermediate results for multi-use transformations
- Use `persist()` with appropriate storage level (MEMORY_AND_DISK)
- Unpersist when no longer needed

### Shuffle Optimization

- Minimize wide transformations
- Co-locate joined datasets when possible
- Use broadcast joins for small dimensions (<10MB)

### Memory Management

- Configure executor memory based on data volume
- Monitor GC metrics
- Use off-heap memory for large shuffles

## Testing Strategy

### Unit Tests
- Mock Spark sessions
- Mock external systems (databases, Kafka)
- Test each component in isolation
- Contract tests for all implementations

### Integration Tests
- Embedded databases (H2 in PostgreSQL/MySQL modes)
- Local filesystem for S3 simulation
- End-to-end pipeline execution
- Error handling scenarios

### Performance Tests
- Large dataset generation (10GB+)
- Throughput measurement (records/sec)
- Query plan analysis
- Partitioning effectiveness

## References

- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [HashiCorp Vault](https://www.vaultproject.io/docs)
- [YAML 1.2 Specification](https://yaml.org/spec/1.2/spec.html)
