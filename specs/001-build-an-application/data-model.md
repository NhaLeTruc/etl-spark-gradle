# Data Model: Multi-Source ETL Application

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Core Entities

### SourceConfig

Represents configuration for a data source.

**Fields**:
- `type`: String - Source type ("kafka" | "postgres" | "mysql" | "s3")
- `credentialsPath`: String - Vault path for credentials (e.g., "secret/postgres/prod")
- `parameters`: Map[String, String] - Type-specific parameters

**Parameters by Type**:
- **Kafka**: `topic`, `bootstrapServers`, `startingOffsets`
- **PostgreSQL**: `jdbcUrl`, `table` or `query`, `fetchSize`
- **MySQL**: `jdbcUrl`, `table` or `query`, `fetchSize`
- **S3**: `bucket`, `prefix`, `format` (avro/parquet/json)

**Scala Representation**:
```scala
case class SourceConfig(
  `type`: String,
  credentialsPath: String,
  parameters: Map[String, String]
)
```

### SinkConfig

Represents configuration for a data sink.

**Fields**:
- `type`: String - Sink type ("kafka" | "postgres" | "mysql" | "s3")
- `credentialsPath`: String - Vault path for credentials
- `parameters`: Map[String, String] - Type-specific parameters
- `writeMode`: String - Write mode ("append" | "overwrite" | "upsert")

**Parameters by Type**:
- **Kafka**: `topic`, `bootstrapServers`
- **PostgreSQL**: `jdbcUrl`, `table`, `upsertKeys` (for upsert mode)
- **MySQL**: `jdbcUrl`, `table`, `upsertKeys` (for upsert mode)
- **S3**: `bucket`, `prefix`, `format`, `partitionBy`

**Scala Representation**:
```scala
case class SinkConfig(
  `type`: String,
  credentialsPath: String,
  parameters: Map[String, String],
  writeMode: String
)
```

### TransformationConfig

Represents configuration for a transformation step.

**Fields**:
- `type`: String - Transformation type ("aggregation" | "join" | "windowing" | "filter" | "map")
- `parameters`: Map[String, Any] - Type-specific parameters

**Parameters by Type**:
- **Aggregation**: `groupBy` (List[String]), `aggregates` (List[AggregateExpr])
- **Join**: `joinType` (String), `leftOn` (String), `rightOn` (String), `rightSource` (SourceConfig)
- **Windowing**: `windowType` ("tumbling" | "sliding" | "session"), `duration` (String), `slideInterval` (Option[String])
- **Filter**: `condition` (String - SQL expression)
- **Map**: `expressions` (Map[String, String] - column → expression)

**Scala Representation**:
```scala
case class TransformationConfig(
  `type`: String,
  parameters: Map[String, Any]
)

case class AggregateExpr(
  column: String,
  function: String, // "sum", "count", "avg", "min", "max"
  alias: String
)
```

### PipelineConfig

Root configuration for an ETL pipeline (YAML deserialization target).

**Fields**:
- `name`: String - Pipeline name
- `executionMode`: String - "batch" or "micro-batch"
- `source`: SourceConfig
- `transformations`: List[TransformationConfig]
- `sink`: SinkConfig
- `performance`: PerformanceConfig
- `quality`: QualityConfig

**Scala Representation**:
```scala
case class PipelineConfig(
  name: String,
  executionMode: String,
  source: SourceConfig,
  transformations: List[TransformationConfig],
  sink: SinkConfig,
  performance: PerformanceConfig,
  quality: QualityConfig
)
```

### PerformanceConfig

Performance tuning parameters for Spark jobs.

**Fields**:
- `partitions`: Int - Number of partitions for repartitioning
- `executorMemory`: String - Executor memory (e.g., "4g")
- `executorCores`: Int - Cores per executor
- `maxOffsetsPerTrigger`: Option[Long] - Kafka rate limiting (micro-batch only)

**Scala Representation**:
```scala
case class PerformanceConfig(
  partitions: Int,
  executorMemory: String,
  executorCores: Int,
  maxOffsetsPerTrigger: Option[Long] = None
)
```

### QualityConfig

Data quality validation parameters.

**Fields**:
- `schemaValidation`: Boolean - Enable Avro schema validation
- `nullCheckColumns`: List[String] - Columns to check for nulls
- `quarantinePath`: String - Path for quarantined invalid records

**Scala Representation**:
```scala
case class QualityConfig(
  schemaValidation: Boolean,
  nullCheckColumns: List[String],
  quarantinePath: String
)
```

### ExecutionMetrics

Runtime metrics for a pipeline execution.

**Fields**:
- `pipelineId`: String - Pipeline identifier
- `runId`: String - Unique run UUID
- `startTimestamp`: Long - Start time (epoch millis)
- `endTimestamp`: Long - End time (epoch millis)
- `recordsExtracted`: Long - Records read from source
- `recordsTransformed`: Long - Records after transformations
- `recordsLoaded`: Long - Records written to sink
- `recordsFailed`: Long - Records quarantined
- `status`: String - "success" | "failed" | "partial"
- `errorDetails`: Option[String] - Error message if failed

**Scala Representation**:
```scala
case class ExecutionMetrics(
  pipelineId: String,
  runId: String,
  startTimestamp: Long,
  endTimestamp: Long,
  recordsExtracted: Long,
  recordsTransformed: Long,
  recordsLoaded: Long,
  recordsFailed: Long,
  status: String,
  errorDetails: Option[String] = None
)
```

### LineageMetadata

Data lineage tracking metadata (embedded in Avro records).

**Fields**:
- `sourceSystem`: String - Source type (e.g., "postgres")
- `sourceTimestamp`: Long - Original record timestamp
- `extractionTimestamp`: Long - When extracted
- `transformationChain`: List[String] - Ordered list of transformation types applied
- `pipelineId`: String - Pipeline identifier
- `runId`: String - Run UUID

**Scala Representation**:
```scala
case class LineageMetadata(
  sourceSystem: String,
  sourceTimestamp: Long,
  extractionTimestamp: Long,
  transformationChain: List[String],
  pipelineId: String,
  runId: String
)
```

### ValidationResult

Result of configuration validation.

**Fields**:
- `isValid`: Boolean
- `errors`: List[String] - Validation error messages

**Scala Representation**:
```scala
case class ValidationResult(
  isValid: Boolean,
  errors: List[String] = List.empty
) {
  def addError(error: String): ValidationResult =
    copy(isValid = false, errors = errors :+ error)
}

object ValidationResult {
  def valid: ValidationResult = ValidationResult(isValid = true)
  def invalid(error: String): ValidationResult =
    ValidationResult(isValid = false, errors = List(error))
}
```

### LoadResult

Result of a load operation.

**Fields**:
- `recordsWritten`: Long
- `recordsFailed`: Long
- `success`: Boolean
- `errorMessage`: Option[String]

**Scala Representation**:
```scala
case class LoadResult(
  recordsWritten: Long,
  recordsFailed: Long,
  success: Boolean,
  errorMessage: Option[String] = None
)
```

### RunContext

Pipeline execution context passed through transformation chain.

**Fields**:
- `pipelineId`: String
- `runId`: String
- `startTimestamp`: Long
- `sparkSession`: SparkSession

**Scala Representation**:
```scala
case class RunContext(
  pipelineId: String,
  runId: String,
  startTimestamp: Long,
  sparkSession: SparkSession
)
```

## Entity Relationships

```
PipelineConfig (1) ──> (1) SourceConfig
PipelineConfig (1) ──> (N) TransformationConfig
PipelineConfig (1) ──> (1) SinkConfig
PipelineConfig (1) ──> (1) PerformanceConfig
PipelineConfig (1) ──> (1) QualityConfig

DataFrame ──> (embedded) LineageMetadata (per record)

Execution ──> (1) ExecutionMetrics
Execution ──> (1) RunContext
```

## State Transitions

### Pipeline Execution Status
```
"pending" → "running" → "success" | "failed" | "partial"
```

### Record Processing
```
Source → Extract → Validate Schema → [Valid | Invalid]
  Valid → Transform → Load → Success
  Invalid → Quarantine → Logged
```

## Data Volume Assumptions

- **Batch**: Single pipeline processes up to 10GB files
- **Micro-batch**: Processes 1000 records/sec sustained throughput
- **Metadata**: Lineage adds ~200 bytes overhead per Avro record
- **Quarantine**: Assume <1% failure rate (10K invalid records per 1M input)

## Schema Evolution

Avro schemas support forward/backward compatibility:
- **Forward**: New fields added with defaults (old readers can read new data)
- **Backward**: Fields removed (new readers can read old data)
- **Full**: Both forward and backward compatibility maintained

Transformations must preserve lineage metadata fields across schema changes.
