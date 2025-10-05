# Implementation Plan: Multi-Source ETL Application

**Branch**: `001-build-an-application` | **Date**: 2025-10-05 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-build-an-application/spec.md`

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from file system structure or context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Fill the Constitution Check section based on the content of the constitution document.
4. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
5. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
6. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, `GEMINI.md` for Gemini CLI, `QWEN.md` for Qwen Code, or `AGENTS.md` for all other agents).
7. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
8. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
9. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 8. Phases 2-4 are executed by other commands:
- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary

Building a multi-source ETL application that extracts data from Kafka, PostgreSQL, MySQL, and Amazon S3, applies transformations (aggregations, joins, windowing), and loads results back to these systems. The application uses Apache Spark 3.5.6 for batch processing and Spark Streaming for micro-batch processing, with Avro as the data interchange format. All components follow dependency injection principles for modularity and reusability. Pipeline configurations are defined in YAML, credentials stored in a vault (Docker Compose compatible), and metrics exposed via structured logging. Performance targets: 10GB batch files and 1000 records/sec micro-batch throughput.

## Technical Context

**Language/Version**: Scala 2.12, Java 11 (JVM target)
**Primary Dependencies**: Apache Spark 3.5.6 (Core, SQL, Streaming), Avro 1.11.x, minimal additional libraries
**Storage**: Source/sink abstraction layer for Kafka, PostgreSQL, MySQL, Amazon S3
**Testing**: ScalaTest for unit tests, Spark Testing Base for integration tests, mocked sources/sinks
**Target Platform**: JVM (Linux/MacOS), Docker Compose for local vault testing
**Project Type**: Single project (data processing application)
**Performance Goals**: Batch: 10GB file processing, Micro-batch: 1000 records/sec throughput
**Constraints**: Minimal dependencies, dependency injection architecture, Avro format mandatory, YAML configuration only
**Scale/Scope**: 4 source/sink types (Kafka, PostgreSQL, MySQL, S3), 3 transformation categories (aggregations, joins, windowing), 3 write modes (append, overwrite, upsert)

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**I. Code Quality & Standards**
- [x] Code style guide selected: Scala Style Guide
- [x] Static analysis tools configured: ScalaStyle for Scala code
- [x] Code review process defined: Required before merge (GitHub PR workflow)

**II. Test-First Development**
- [x] Test framework identified: ScalaTest for unit tests, Spark Testing Base for integration tests
- [x] TDD workflow confirmed: Tests written before implementation (mocked data sources/sinks)
- [x] Unit test coverage target ≥80% for all extract, transform, load modules
- [x] Integration test strategy: End-to-end pipeline tests with fully mocked sources/sinks

**III. Data Quality Assurance**
- [x] Schema validation approach: Avro schema validation at ingestion and post-transformation
- [x] Data quality checks identified: Null rates, duplicate detection, schema conformance
- [x] Error quarantine strategy: Invalid records written to error tables/files with diagnostic metadata
- [x] Data lineage tracking: Metadata fields in Avro records (source, timestamp, transformation lineage)
- [x] Idempotency verified: All transformations pure functions, deterministic outputs

**IV. Big Data Performance Requirements**
- [x] Partitioning strategy documented: Per-pipeline YAML configuration for repartitioning/coalesce
- [x] Performance benchmarks established: 10GB batch files, 1000 records/sec micro-batch
- [x] Resource allocations specified: Configurable in YAML (executor memory, cores, parallelism)
- [x] Query plan review process: Spark UI analysis during integration testing
- [x] Performance regression testing planned: Benchmarking suite with representative datasets

**V. Observability & Monitoring**
- [x] Logging strategy defined: Structured logging with correlation IDs (pipeline ID, run ID, stage)
- [x] Metrics identified: Records extracted/transformed/loaded/failed, duration, data quality scores
- [x] Alert configurations planned: Log-based alerts for failures, SLA breaches (via log aggregation)
- [x] Execution metadata persistence: Results logged to structured log files (JSON format)

## Project Structure

### Documentation (this feature)
```
specs/001-build-an-application/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
│   ├── extractor-interface.md
│   ├── transformer-interface.md
│   └── loader-interface.md
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)
```
src/
├── main/
│   ├── scala/
│   │   └── com/
│   │       └── etl/
│   │           ├── config/          # YAML configuration parsing
│   │           ├── core/            # Core abstractions (DataSource, DataSink, Transformation, Pipeline)
│   │           ├── extractor/       # Extractor modules (Kafka, PostgreSQL, MySQL, S3)
│   │           ├── transformer/     # Transformer modules (Aggregation, Join, Windowing)
│   │           ├── loader/          # Loader modules (Kafka, PostgreSQL, MySQL, S3)
│   │           ├── pipeline/        # Pipeline orchestration and execution
│   │           ├── quality/         # Data quality validation and quarantine
│   │           ├── lineage/         # Data lineage tracking
│   │           ├── vault/           # Credential management (vault integration)
│   │           ├── logging/         # Structured logging utilities
│   │           └── Main.scala       # Application entry point
│   └── resources/
│       ├── application.conf         # Default application configuration
│       └── log4j2.xml              # Logging configuration
│
└── test/
    ├── scala/
    │   └── com/
    │       └── etl/
    │           ├── extractor/       # Unit tests for extractors (mocked)
    │           ├── transformer/     # Unit tests for transformers
    │           ├── loader/          # Unit tests for loaders (mocked)
    │           ├── quality/         # Unit tests for quality checks
    │           ├── pipeline/        # Integration tests for full pipelines
    │           └── fixtures/        # Test data fixtures (Avro samples)
    └── resources/
        ├── test-pipelines/          # Sample YAML pipeline configurations
        └── test-data/               # Mock data for testing
```

**Structure Decision**: Single project structure selected. This is a data processing application with no frontend/backend split or mobile components. All ETL logic resides in a unified codebase with clear module boundaries enforced through dependency injection.

## Phase 0: Outline & Research

### Research Tasks

1. **Spark 3.5.6 + Scala 2.12 Compatibility**
   - **Decision**: Spark 3.5.6 officially supports Scala 2.12 and 2.13. Using 2.12 for stability.
   - **Rationale**: Scala 2.12 has mature ecosystem, wider library compatibility.
   - **Alternatives**: Scala 2.13 considered but 2.12 chosen for compatibility with legacy Spark libraries.

2. **Avro Integration with Spark**
   - **Decision**: Use `spark-avro` library (org.apache.spark:spark-avro_2.12:3.5.6) for native Avro support.
   - **Rationale**: Official Spark library, seamless DataFrame ↔ Avro conversion.
   - **Alternatives**: Custom Avro serialization rejected (reinventing the wheel).

3. **Dependency Injection Framework**
   - **Decision**: Manual dependency injection using constructor injection (no DI framework).
   - **Rationale**: Minimal dependencies requirement, explicit control, testability via constructor mocking.
   - **Alternatives**: Guice/Spring rejected (heavy dependencies), MacWire considered but manual DI simpler for this scope.

4. **YAML Configuration Parsing**
   - **Decision**: Use SnakeYAML (org.yaml:snakeyaml:2.0) or Circe-YAML (io.circe:circe-yaml_2.12:0.14.2).
   - **Rationale**: SnakeYAML is lightweight, Circe-YAML provides type-safe parsing.
   - **Alternatives**: Typesafe Config (HOCON) rejected (YAML requirement), Jackson-YAML considered.

5. **Vault Solution for Credentials**
   - **Decision**: Use HashiCorp Vault with Docker Compose for local development, programmatic access via vault-java-driver.
   - **Rationale**: Industry-standard secret management, Docker Compose support, production-ready.
   - **Alternatives**: AWS Secrets Manager (cloud-only), file-based secrets (insecure).

6. **Mocking Strategy for Tests**
   - **Decision**: ScalaTest with mocked Spark sessions (spark-fast-tests or custom mocks), embedded databases (H2 for SQL), embedded Kafka (kafka-streams-test-utils).
   - **Rationale**: No external dependencies during tests, fast execution, deterministic.
   - **Alternatives**: Testcontainers rejected (requires Docker during CI, slower).

7. **Structured Logging**
   - **Decision**: Log4j2 with JSON layout for structured logs, SLF4J API.
   - **Rationale**: Spark uses Log4j2 natively, JSON format for log aggregation.
   - **Alternatives**: Logback considered but Log4j2 better Spark integration.

8. **Kafka Connector Best Practices**
   - **Decision**: Use Spark Structured Streaming Kafka source/sink with checkpointing for exactly-once semantics.
   - **Rationale**: Fault tolerance, offset management, idempotency.
   - **Alternatives**: Custom Kafka consumer/producer rejected (complex offset handling).

9. **S3 Integration**
   - **Decision**: Use Hadoop-AWS library (org.apache.hadoop:hadoop-aws) with S3A filesystem.
   - **Rationale**: Native Spark support, efficient large file reads/writes.
   - **Alternatives**: AWS SDK for Java rejected (lower-level API, more code).

10. **Database JDBC Connectors**
    - **Decision**: PostgreSQL JDBC (org.postgresql:postgresql:42.6.0), MySQL Connector/J (com.mysql:mysql-connector-j:8.1.0).
    - **Rationale**: Official drivers, production-tested.
    - **Alternatives**: Generic JDBC rejected (type-specific optimizations needed).

### Research Output Artifacts

**File**: [research.md](research.md)

## Phase 1: Design & Contracts

### Data Model

**File**: [data-model.md](data-model.md)

#### Core Entities

**SourceConfig**
- `type`: String ("kafka" | "postgres" | "mysql" | "s3")
- `credentialsPath`: String (Vault path)
- `parameters`: Map[String, String] (type-specific: query, topic, path, etc.)

**SinkConfig**
- `type`: String ("kafka" | "postgres" | "mysql" | "s3")
- `credentialsPath`: String (Vault path)
- `parameters`: Map[String, String] (destination table/topic/path)
- `writeMode`: String ("append" | "overwrite" | "upsert")

**TransformationConfig**
- `type`: String ("aggregation" | "join" | "windowing" | "filter" | "map")
- `parameters`: Map[String, Any] (transformation-specific configuration)

**PipelineConfig** (YAML root)
- `name`: String
- `executionMode`: String ("batch" | "micro-batch")
- `source`: SourceConfig
- `transformations`: List[TransformationConfig]
- `sink`: SinkConfig
- `performance`: PerformanceConfig
- `quality`: QualityConfig

**PerformanceConfig**
- `partitions`: Int
- `executorMemory`: String (e.g., "4g")
- `executorCores`: Int
- `maxOffsetsPerTrigger`: Option[Long] (for micro-batch)

**QualityConfig**
- `schemaValidation`: Boolean
- `nullCheckColumns`: List[String]
- `quarantinePath`: String

**ExecutionMetrics**
- `pipelineId`: String
- `runId`: String (UUID)
- `startTimestamp`: Long
- `endTimestamp`: Long
- `recordsExtracted`: Long
- `recordsTransformed`: Long
- `recordsLoaded`: Long
- `recordsFailed`: Long
- `status`: String ("success" | "failed" | "partial")
- `errorDetails`: Option[String]

**LineageMetadata** (embedded in Avro records)
- `sourceSystem`: String
- `sourceTimestamp`: Long
- `extractionTimestamp`: Long
- `transformationChain`: List[String]
- `pipelineId`: String
- `runId`: String

#### Relationships

- **PipelineConfig** → 1 SourceConfig
- **PipelineConfig** → N TransformationConfig (ordered list)
- **PipelineConfig** → 1 SinkConfig
- **PipelineConfig** → 1 PerformanceConfig
- **PipelineConfig** → 1 QualityConfig
- **DataFrame** (Avro records) → embedded LineageMetadata

### API Contracts

**File**: [contracts/extractor-interface.md](contracts/extractor-interface.md)

```scala
trait DataExtractor {
  /**
   * Extract data from configured source
   * @param config Source-specific configuration
   * @param spark Implicit Spark session
   * @return DataFrame in Avro-compatible schema with lineage metadata
   */
  def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame

  /**
   * Validate source configuration before extraction
   * @param config Source configuration to validate
   * @return Validation result with error messages if invalid
   */
  def validateConfig(config: SourceConfig): ValidationResult
}
```

**File**: [contracts/transformer-interface.md](contracts/transformer-interface.md)

```scala
trait DataTransformer {
  /**
   * Apply transformation to input DataFrame
   * @param input DataFrame with Avro schema and lineage
   * @param config Transformation-specific configuration
   * @param runContext Pipeline execution context
   * @return Transformed DataFrame with updated lineage
   */
  def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): DataFrame

  /**
   * Validate transformation configuration
   * @param config Transformation configuration
   * @param inputSchema Input DataFrame schema
   * @return Validation result
   */
  def validateConfig(
    config: TransformationConfig,
    inputSchema: StructType
  ): ValidationResult
}
```

**File**: [contracts/loader-interface.md](contracts/loader-interface.md)

```scala
trait DataLoader {
  /**
   * Load DataFrame to configured sink
   * @param data DataFrame to load (Avro-compatible)
   * @param config Sink-specific configuration
   * @param spark Implicit Spark session
   * @return LoadResult with metrics (records written, failures)
   */
  def load(
    data: DataFrame,
    config: SinkConfig
  )(implicit spark: SparkSession): LoadResult

  /**
   * Validate sink configuration before loading
   * @param config Sink configuration to validate
   * @return Validation result
   */
  def validateConfig(config: SinkConfig): ValidationResult
}
```

### Integration Test Scenarios (Quickstart)

**File**: [quickstart.md](quickstart.md)

#### Scenario 1: Batch Pipeline - PostgreSQL to S3 with Aggregation

**Setup**:
- Embedded H2 database with sample `orders` table (100K rows)
- Local filesystem as S3 proxy

**Pipeline**:
1. Extract: Read from H2 via JDBC
2. Transform: Aggregate by customer_id (sum amounts)
3. Load: Write Avro files to local filesystem
4. Validate: Verify record count, schema, lineage metadata

**Expected**:
- All records extracted
- Aggregation reduces rows (group by)
- Output Avro files contain lineage metadata
- No quarantined records (valid data)

#### Scenario 2: Micro-Batch Pipeline - Kafka to MySQL with Windowing

**Setup**:
- Embedded Kafka with test topic (streaming 1000 records/sec)
- Embedded H2 as MySQL proxy

**Pipeline**:
1. Extract: Read from Kafka with 10-second tumbling window
2. Transform: Count events per window
3. Load: Upsert window counts to H2 table
4. Validate: Check idempotency (rerun produces same results)

**Expected**:
- Windowed aggregates match expected counts
- Upsert mode prevents duplicates
- Checkpointing enables fault tolerance

#### Scenario 3: Error Handling - Invalid Schema

**Setup**:
- Mock data with schema violations (missing required fields)

**Pipeline**:
1. Extract: Read malformed data
2. Validate: Schema validation fails
3. Quarantine: Invalid records written to error path
4. Transform/Load: Only valid records processed

**Expected**:
- Invalid records quarantined with error diagnostics
- Pipeline continues with valid data
- Structured logs contain failure details

### Agent Context File

**File**: `/home/bob/WORK/etl-spark-gradle/CLAUDE.md` (generated via update-agent-context script)

```markdown
# Claude Code Agent Context - ETL Spark Gradle

## Project Overview
Multi-source ETL application using Apache Spark 3.5.6, Scala 2.12, Gradle 7.6.5.

## Tech Stack
- **Language**: Scala 2.12, Java 11
- **Framework**: Apache Spark 3.5.6 (Batch + Streaming)
- **Data Format**: Avro (interchange), YAML (config)
- **Build**: Gradle 7.6.5
- **Testing**: ScalaTest, mocked sources/sinks
- **Secrets**: HashiCorp Vault (Docker Compose)

## Architecture Principles
- Dependency injection (constructor-based, no framework)
- Reusable extract/transform/load modules per source/sink type
- 80%+ unit test coverage
- Avro schema validation, data lineage tracking
- Structured logging (JSON), metrics via logs

## Key Modules
- `extractor/`: Kafka, PostgreSQL, MySQL, S3 extractors
- `transformer/`: Aggregation, join, windowing transformers
- `loader/`: Kafka, PostgreSQL, MySQL, S3 loaders
- `quality/`: Schema validation, quarantine logic
- `pipeline/`: Orchestration, YAML config parsing

## Recent Changes
- Initial project setup (2025-10-05)
- Constitution v1.0.0 adopted
- Feature spec and plan created

## Testing Strategy
- Unit tests: ScalaTest with mocked Spark sessions
- Integration tests: End-to-end pipelines with embedded databases/Kafka
- No real external connections (mocked for CI/CD)

## Performance Targets
- Batch: 10GB file processing
- Micro-batch: 1000 records/sec throughput

## Constitutional Constraints
- TDD mandatory (tests before implementation)
- Code quality: ScalaStyle, code reviews, <10 cyclomatic complexity
- Data quality: Schema validation, quarantine, idempotency
- Observability: Structured logging with correlation IDs
```

## Phase 2: Task Planning Approach

**IMPORTANT**: This section DESCRIBES the task generation strategy. The actual tasks.md is created by the `/tasks` command.

### Task Generation Strategy

The `/tasks` command will generate tasks based on:

1. **From Contracts** (contracts/*.md):
   - Each interface (DataExtractor, DataTransformer, DataLoader) → contract test task [P]
   - Each implementation (KafkaExtractor, AggregationTransformer, etc.) → implementation task

2. **From Data Model** (data-model.md):
   - Core entities (SourceConfig, SinkConfig, PipelineConfig) → case class creation tasks [P]
   - YAML parsing → configuration loader implementation task

3. **From Quickstart** (quickstart.md):
   - Each scenario → integration test task [P]
   - Test fixtures → test data generation tasks [P]

4. **ETL-Specific Requirements**:
   - 4 source types × 1 extractor module = 4 extractor tasks
   - 4 sink types × 1 loader module = 4 loader tasks
   - 3 transformation categories × N specific transformers = ~6 transformer tasks
   - Data quality: Schema validation, quarantine, lineage tracking tasks
   - Observability: Structured logging, metrics collection tasks

5. **Infrastructure Tasks**:
   - Gradle build configuration (build.gradle, dependencies)
   - ScalaStyle configuration
   - Log4j2 configuration (JSON layout)
   - Docker Compose for Vault
   - CI/CD pipeline (GitHub Actions)

### Task Ordering

1. **Setup** (T001-T010):
   - Gradle project initialization
   - Dependency configuration
   - ScalaStyle, Log4j2 setup
   - Directory structure creation

2. **Core Abstractions - Tests First** (T011-T020):
   - Contract tests for DataExtractor, DataTransformer, DataLoader interfaces
   - Core entity case classes (SourceConfig, SinkConfig, etc.)

3. **Extractors - Tests First** (T021-T035):
   - Unit tests for each extractor (KafkaExtractor, PostgresExtractor, MySQLExtractor, S3Extractor) [P]
   - Implementations for each extractor

4. **Transformers - Tests First** (T036-T050):
   - Unit tests for transformers (Aggregation, Join, Windowing, Filter, Map) [P]
   - Implementations for each transformer

5. **Loaders - Tests First** (T051-T065):
   - Unit tests for each loader (KafkaLoader, PostgresLoader, MySQLLoader, S3Loader) [P]
   - Implementations for each loader

6. **Data Quality** (T066-T075):
   - Schema validation tests and implementation
   - Quarantine logic tests and implementation
   - Lineage tracking tests and implementation [P]

7. **Pipeline Orchestration** (T076-T085):
   - YAML config parser tests and implementation
   - Pipeline execution engine tests and implementation
   - Vault integration tests and implementation

8. **Observability** (T086-T090):
   - Structured logging utility tests and implementation [P]
   - Metrics collection tests and implementation [P]

9. **Integration Tests** (T091-T100):
   - Quickstart scenario 1: PostgreSQL to S3 batch [P]
   - Quickstart scenario 2: Kafka to MySQL micro-batch [P]
   - Quickstart scenario 3: Error handling [P]
   - Performance benchmarking tests

10. **Polish** (T101-T110):
    - Code duplication removal
    - Documentation (README, API docs)
    - Docker Compose setup for Vault
    - CI/CD pipeline configuration

### Estimated Output

**Total Tasks**: ~110 tasks
- Setup: 10 tasks
- Core: 10 tasks
- Extractors: 15 tasks (4 types × unit test + impl + integration)
- Transformers: 15 tasks (5 types × unit test + impl + integration)
- Loaders: 15 tasks (4 types × unit test + impl + integration)
- Data Quality: 10 tasks
- Pipeline: 10 tasks
- Observability: 5 tasks
- Integration: 10 tasks
- Polish: 10 tasks

**Parallelization**: Tasks marked [P] can run in parallel (different files, independent modules).

## Complexity Tracking

*No constitutional violations identified.*

All requirements align with constitutional principles:
- TDD enforced (tests before implementation)
- Code quality standards defined (ScalaStyle, code reviews)
- Data quality assured (schema validation, quarantine, lineage)
- Performance requirements specified (partitioning, benchmarks)
- Observability implemented (structured logging, metrics)

## Progress Tracking

**Phase Status**:
- [x] Phase 0: Research complete
- [x] Phase 1: Design complete
- [x] Phase 2: Task planning approach described
- [ ] Phase 3: Tasks generated (/tasks command - not part of /plan)
- [ ] Phase 4: Implementation (post-/tasks)
- [ ] Phase 5: Validation (post-implementation)

**Gate Status**:
- [x] Initial Constitution Check: PASS
- [x] Post-Design Constitution Check: PASS
- [x] All clarifications resolved (Session 2025-10-05)
- [x] No complexity deviations

---
*Based on Constitution v1.0.0 - See `.specify/memory/constitution.md`*
