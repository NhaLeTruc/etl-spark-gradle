# Tasks: Multi-Source ETL Application

**Input**: Design documents from `/specs/001-build-an-application/`
**Prerequisites**: plan.md, research.md, data-model.md, contracts/, quickstart.md

## Execution Flow (main)
```
1. Load plan.md from feature directory
   → Extract: Scala 2.12, Spark 3.5.6, Gradle 7.6.5, minimal dependencies
2. Load design documents:
   → data-model.md: 11 entities (SourceConfig, SinkConfig, etc.)
   → contracts/: 3 interfaces (DataExtractor, DataTransformer, DataLoader)
   → quickstart.md: 3 integration scenarios
3. Generate tasks by category:
   → Setup: Gradle, dependencies, ScalaStyle, Log4j2
   → Tests: Contract tests, unit tests (TDD)
   → Core: Entities, interfaces, implementations (4 extractors, 5 transformers, 4 loaders)
   → Quality: Schema validation, quarantine, lineage
   → Pipeline: YAML parser, orchestration, Vault integration
   → Observability: Structured logging, metrics
   → Integration: 3 quickstart scenarios
   → Polish: Docs, Docker Compose, CI/CD
4. Apply TDD ordering: Tests before implementation
5. Mark parallel tasks [P]: Different files, no dependencies
6. Number sequentially: T001, T002, T003...
7. Return: tasks.md ready for execution
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Source**: `src/main/scala/com/etl/`
- **Test**: `src/test/scala/com/etl/`
- **Resources**: `src/main/resources/`, `src/test/resources/`
- **Root**: `build.gradle`, `docker-compose.yml`, etc.

---

## Phase 3.1: Setup

- [x] T001 Initialize Gradle project structure with Scala plugin in `build.gradle`
- [x] T002 Configure Gradle dependencies: Spark 3.5.6, spark-avro, ScalaTest, SnakeYAML, Vault driver, JDBC drivers
- [x] T003 [P] Create directory structure: `src/main/scala/com/etl/{config,core,extractor,transformer,loader,pipeline,quality,lineage,vault,logging}/`
- [x] T004 [P] Create test directory structure: `src/test/scala/com/etl/{extractor,transformer,loader,quality,pipeline,fixtures}/`
- [x] T005 [P] Configure ScalaStyle in `scalastyle-config.xml` (Scala Style Guide rules)
- [x] T006 [P] Configure Log4j2 JSON layout in `src/main/resources/log4j2.xml`
- [x] T007 [P] Create `src/main/resources/application.conf` with default Spark configuration
- [x] T008 [P] Create `docker-compose.yml` for HashiCorp Vault (development mode)
- [x] T009 [P] Create `.gitignore` (Gradle, IntelliJ, Scala, target/, build/)
- [x] T010 [P] Create `README.md` with project overview and quick start instructions

## Phase 3.2: Core Entities & Case Classes ⚠️ MUST COMPLETE BEFORE 3.3

- [x] T011 [P] Create `SourceConfig` case class in `src/main/scala/com/etl/core/SourceConfig.scala`
- [x] T012 [P] Create `SinkConfig` case class in `src/main/scala/com/etl/core/SinkConfig.scala`
- [x] T013 [P] Create `TransformationConfig` case class in `src/main/scala/com/etl/core/TransformationConfig.scala`
- [x] T014 [P] Create `AggregateExpr` case class in `src/main/scala/com/etl/core/AggregateExpr.scala`
- [x] T015 [P] Create `PipelineConfig` case class in `src/main/scala/com/etl/core/PipelineConfig.scala`
- [x] T016 [P] Create `PerformanceConfig` case class in `src/main/scala/com/etl/core/PerformanceConfig.scala`
- [x] T017 [P] Create `QualityConfig` case class in `src/main/scala/com/etl/core/QualityConfig.scala`
- [x] T018 [P] Create `ExecutionMetrics` case class in `src/main/scala/com/etl/core/ExecutionMetrics.scala`
- [x] T019 [P] Create `LineageMetadata` case class in `src/main/scala/com/etl/core/LineageMetadata.scala`
- [x] T020 [P] Create `ValidationResult` case class in `src/main/scala/com/etl/core/ValidationResult.scala`
- [x] T021 [P] Create `LoadResult` case class in `src/main/scala/com/etl/core/LoadResult.scala`
- [x] T022 [P] Create `RunContext` case class in `src/main/scala/com/etl/core/RunContext.scala`

## Phase 3.3: Contract Interfaces & Tests ⚠️ TESTS MUST FAIL BEFORE IMPLEMENTATION

### DataExtractor Contract Tests (WRITE TESTS FIRST)
- [x] T023 [P] Contract test: DataExtractor.extract returns valid DataFrame in `src/test/scala/com/etl/core/DataExtractorContractSpec.scala`
- [x] T024 [P] Contract test: DataExtractor.validateConfig detects missing parameters
- [x] T025 [P] Contract test: DataExtractor.validateConfig accepts valid parameters
- [x] T026 [P] Contract test: DataExtractor.extract handles connection failure (mocked)
- [x] T027 [P] Contract test: DataExtractor.extract embeds lineage metadata

### DataExtractor Interface (AFTER CONTRACT TESTS)
- [x] T028 Create `DataExtractor` trait in `src/main/scala/com/etl/core/DataExtractor.scala`
- [x] T029 Create `ExtractionException` case class in `src/main/scala/com/etl/core/ExtractionException.scala`

### DataTransformer Contract Tests (WRITE TESTS FIRST)
- [x] T030 [P] Contract test: DataTransformer.transform returns valid DataFrame in `src/test/scala/com/etl/core/DataTransformerContractSpec.scala`
- [x] T031 [P] Contract test: DataTransformer.validateConfig detects schema mismatch
- [x] T032 [P] Contract test: DataTransformer.transform is idempotent
- [x] T033 [P] Contract test: DataTransformer.transform updates lineage chain
- [x] T034 [P] Contract test: DataTransformer.transform handles empty DataFrame

### DataTransformer Interface (AFTER CONTRACT TESTS)
- [x] T035 Create `DataTransformer` trait in `src/main/scala/com/etl/core/DataTransformer.scala`
- [x] T036 Create `TransformationException` case class in `src/main/scala/com/etl/core/TransformationException.scala`

### DataLoader Contract Tests (WRITE TESTS FIRST)
- [x] T037 [P] Contract test: DataLoader.load returns success result in `src/test/scala/com/etl/core/DataLoaderContractSpec.scala`
- [x] T038 [P] Contract test: DataLoader.validateConfig detects missing parameters
- [x] T039 [P] Contract test: DataLoader.validateConfig accepts valid parameters
- [x] T040 [P] Contract test: DataLoader.load handles write failure (mocked)
- [x] T041 [P] Contract test: DataLoader.load is idempotent (upsert mode)
- [x] T042 [P] Contract test: DataLoader.load append mode adds records
- [x] T043 [P] Contract test: DataLoader.load overwrite mode replaces data

### DataLoader Interface (AFTER CONTRACT TESTS)
- [x] T044 Create `DataLoader` trait in `src/main/scala/com/etl/core/DataLoader.scala`
- [x] T045 Create `LoadException` case class in `src/main/scala/com/etl/core/LoadException.scala`

## Phase 3.4: Extractor Implementations (TDD: TESTS FIRST)

### KafkaExtractor
- [x] T046 [P] Unit test: KafkaExtractor extracts from Kafka topic (embedded Kafka) in `src/test/scala/com/etl/extractor/KafkaExtractorSpec.scala`
- [x] T047 [P] Unit test: KafkaExtractor validates configuration
- [x] T048 [P] Unit test: KafkaExtractor embeds lineage metadata
- [x] T049 Implement `KafkaExtractor` in `src/main/scala/com/etl/extractor/KafkaExtractor.scala`

### PostgresExtractor
- [x] T050 [P] Unit test: PostgresExtractor extracts from JDBC (H2) in `src/test/scala/com/etl/extractor/PostgresExtractorSpec.scala`
- [x] T051 [P] Unit test: PostgresExtractor validates configuration
- [x] T052 [P] Unit test: PostgresExtractor retrieves credentials from Vault (mocked)
- [x] T053 Implement `PostgresExtractor` in `src/main/scala/com/etl/extractor/PostgresExtractor.scala`

### MySQLExtractor
- [x] T054 [P] Unit test: MySQLExtractor extracts from JDBC (H2) in `src/test/scala/com/etl/extractor/MySQLExtractorSpec.scala`
- [x] T055 [P] Unit test: MySQLExtractor validates configuration
- [x] T056 [P] Unit test: MySQLExtractor retrieves credentials from Vault (mocked)
- [x] T057 Implement `MySQLExtractor` in `src/main/scala/com/etl/extractor/MySQLExtractor.scala`

### S3Extractor
- [x] T058 [P] Unit test: S3Extractor extracts from S3 (local filesystem) in `src/test/scala/com/etl/extractor/S3ExtractorSpec.scala`
- [x] T059 [P] Unit test: S3Extractor validates configuration
- [x] T060 [P] Unit test: S3Extractor reads Avro format
- [x] T061 Implement `S3Extractor` in `src/main/scala/com/etl/extractor/S3Extractor.scala`

## Phase 3.5: Transformer Implementations (TDD: TESTS FIRST)

### AggregationTransformer
- [x] T062 [P] Unit test: AggregationTransformer applies groupBy and aggregations in `src/test/scala/com/etl/transformer/AggregationTransformerSpec.scala`
- [x] T063 [P] Unit test: AggregationTransformer validates aggregation config
- [x] T064 [P] Unit test: AggregationTransformer updates lineage chain
- [x] T065 Implement `AggregationTransformer` in `src/main/scala/com/etl/transformer/AggregationTransformer.scala`

### JoinTransformer
- [x] T066 [P] Unit test: JoinTransformer performs inner join in `src/test/scala/com/etl/transformer/JoinTransformerSpec.scala`
- [x] T067 [P] Unit test: JoinTransformer performs left/right/full joins
- [x] T068 [P] Unit test: JoinTransformer merges lineage metadata
- [x] T069 Implement `JoinTransformer` in `src/main/scala/com/etl/transformer/JoinTransformer.scala`

### WindowingTransformer
- [x] T070 [P] Unit test: WindowingTransformer applies tumbling windows in `src/test/scala/com/etl/transformer/WindowingTransformerSpec.scala`
- [x] T071 [P] Unit test: WindowingTransformer applies sliding windows
- [x] T072 [P] Unit test: WindowingTransformer validates window config
- [x] T073 Implement `WindowingTransformer` in `src/main/scala/com/etl/transformer/WindowingTransformer.scala`

### FilterTransformer
- [x] T074 [P] Unit test: FilterTransformer applies SQL condition in `src/test/scala/com/etl/transformer/FilterTransformerSpec.scala`
- [x] T075 [P] Unit test: FilterTransformer validates SQL expression
- [x] T076 Implement `FilterTransformer` in `src/main/scala/com/etl/transformer/FilterTransformer.scala`

### MapTransformer
- [x] T077 [P] Unit test: MapTransformer applies column expressions in `src/test/scala/com/etl/transformer/MapTransformerSpec.scala`
- [x] T078 [P] Unit test: MapTransformer supports rename and type conversion
- [x] T079 Implement `MapTransformer` in `src/main/scala/com/etl/transformer/MapTransformer.scala`

## Phase 3.6: Loader Implementations (TDD: TESTS FIRST)

### KafkaLoader
- [x] T080 [P] Unit test: KafkaLoader writes to Kafka topic (embedded Kafka) in `src/test/scala/com/etl/loader/KafkaLoaderSpec.scala`
- [x] T081 [P] Unit test: KafkaLoader validates configuration
- [x] T082 [P] Unit test: KafkaLoader serializes to Avro
- [x] T083 Implement `KafkaLoader` in `src/main/scala/com/etl/loader/KafkaLoader.scala`

### PostgresLoader
- [x] T084 [P] Unit test: PostgresLoader writes to JDBC (H2) with append mode in `src/test/scala/com/etl/loader/PostgresLoaderSpec.scala`
- [x] T085 [P] Unit test: PostgresLoader writes with overwrite mode
- [x] T086 [P] Unit test: PostgresLoader writes with upsert mode (ON CONFLICT)
- [x] T087 Implement `PostgresLoader` in `src/main/scala/com/etl/loader/PostgresLoader.scala`

### MySQLLoader
- [x] T088 [P] Unit test: MySQLLoader writes to JDBC (H2) with append mode in `src/test/scala/com/etl/loader/MySQLLoaderSpec.scala`
- [x] T089 [P] Unit test: MySQLLoader writes with overwrite mode
- [x] T090 [P] Unit test: MySQLLoader writes with upsert mode (ON DUPLICATE KEY UPDATE)
- [x] T091 Implement `MySQLLoader` in `src/main/scala/com/etl/loader/MySQLLoader.scala`

### S3Loader
- [x] T092 [P] Unit test: S3Loader writes Avro to S3 (local filesystem) in `src/test/scala/com/etl/loader/S3LoaderSpec.scala`
- [x] T093 [P] Unit test: S3Loader writes with partitioning
- [x] T094 [P] Unit test: S3Loader validates configuration
- [x] T095 Implement `S3Loader` in `src/main/scala/com/etl/loader/S3Loader.scala`

## Phase 3.7: Data Quality & Lineage (TDD: TESTS FIRST)

- [x] T096 [P] Unit test: SchemaValidator validates Avro schema in `src/test/scala/com/etl/quality/SchemaValidatorSpec.scala`
- [x] T097 [P] Unit test: SchemaValidator detects schema violations
- [x] T098 Implement `SchemaValidator` in `src/main/scala/com/etl/quality/SchemaValidator.scala`

- [x] T099 [P] Unit test: QuarantineWriter writes invalid records with diagnostics in `src/test/scala/com/etl/quality/QuarantineWriterSpec.scala`
- [x] T100 [P] Unit test: QuarantineWriter formats error metadata
- [x] T101 Implement `QuarantineWriter` in `src/main/scala/com/etl/quality/QuarantineWriter.scala`

- [x] T102 [P] Unit test: LineageTracker embeds lineage metadata in `src/test/scala/com/etl/lineage/LineageTrackerSpec.scala`
- [x] T103 [P] Unit test: LineageTracker updates transformation chain
- [x] T104 Implement `LineageTracker` in `src/main/scala/com/etl/lineage/LineageTracker.scala`

- [x] T105 [P] Unit test: DataQualityChecker validates null columns in `src/test/scala/com/etl/quality/DataQualityCheckerSpec.scala`
- [x] T106 [P] Unit test: DataQualityChecker calculates quality metrics (null rate, duplicate rate)
- [x] T107 Implement `DataQualityChecker` in `src/main/scala/com/etl/quality/DataQualityChecker.scala`

## Phase 3.8: Configuration & Vault Integration (TDD: TESTS FIRST)

- [x] T108 [P] Unit test: YAMLConfigParser parses PipelineConfig from YAML in `src/test/scala/com/etl/config/YAMLConfigParserSpec.scala`
- [x] T109 [P] Unit test: YAMLConfigParser handles invalid YAML gracefully
- [x] T110 Implement `YAMLConfigParser` in `src/main/scala/com/etl/config/YAMLConfigParser.scala`

- [x] T111 [P] Unit test: VaultClient retrieves secrets (mocked Vault) in `src/test/scala/com/etl/vault/VaultClientSpec.scala`
- [x] T112 [P] Unit test: VaultClient handles connection failure
- [x] T113 Implement `VaultClient` in `src/main/scala/com/etl/vault/VaultClient.scala`

## Phase 3.9: Pipeline Orchestration (TDD: TESTS FIRST)

- [x] T114 [P] Unit test: PipelineExecutor executes batch pipeline in `src/test/scala/com/etl/pipeline/PipelineExecutorSpec.scala`
- [x] T115 [P] Unit test: PipelineExecutor executes micro-batch pipeline
- [x] T116 [P] Unit test: PipelineExecutor handles extraction failure
- [x] T117 [P] Unit test: PipelineExecutor handles transformation failure
- [x] T118 [P] Unit test: PipelineExecutor handles load failure
- [x] T119 [P] Unit test: PipelineExecutor collects execution metrics
- [x] T120 Implement `PipelineExecutor` in `src/main/scala/com/etl/pipeline/PipelineExecutor.scala`

- [x] T121 [P] Unit test: ExtractorRegistry resolves extractor by type in `src/test/scala/com/etl/pipeline/ExtractorRegistrySpec.scala`
- [x] T122 Implement `ExtractorRegistry` in `src/main/scala/com/etl/pipeline/ExtractorRegistry.scala`

- [x] T123 [P] Unit test: TransformerRegistry resolves transformer by type in `src/test/scala/com/etl/pipeline/TransformerRegistrySpec.scala`
- [x] T124 Implement `TransformerRegistry` in `src/main/scala/com/etl/pipeline/TransformerRegistry.scala`

- [x] T125 [P] Unit test: LoaderRegistry resolves loader by type in `src/test/scala/com/etl/pipeline/LoaderRegistrySpec.scala`
- [x] T126 Implement `LoaderRegistry` in `src/main/scala/com/etl/pipeline/LoaderRegistry.scala`

## Phase 3.10: Observability (TDD: TESTS FIRST)

- [x] T127 [P] Unit test: StructuredLogger logs with correlation IDs in `src/test/scala/com/etl/logging/StructuredLoggerSpec.scala`
- [x] T128 [P] Unit test: StructuredLogger formats JSON log entries
- [x] T129 Implement `StructuredLogger` in `src/main/scala/com/etl/logging/StructuredLogger.scala`

- [x] T130 [P] Unit test: MetricsCollector tracks extraction metrics in `src/test/scala/com/etl/logging/MetricsCollectorSpec.scala`
- [x] T131 [P] Unit test: MetricsCollector tracks transformation metrics
- [x] T132 [P] Unit test: MetricsCollector tracks load metrics
- [x] T133 Implement `MetricsCollector` in `src/main/scala/com/etl/logging/MetricsCollector.scala`

## Phase 3.11: Main Application Entry Point

- [x] T134 Implement `Main` object with dependency injection wiring in `src/main/scala/com/etl/Main.scala`
- [x] T135 Implement command-line argument parsing (pipeline YAML path)
- [x] T136 Implement Spark session creation with configuration
- [x] T137 Implement graceful shutdown and resource cleanup

## Phase 3.12: Integration Tests (END-TO-END SCENARIOS)

### Quickstart Scenario 1: Batch Pipeline - PostgreSQL to S3 with Aggregation
- [x] T138 [P] Integration test: Execute batch pipeline end-to-end in `src/test/scala/com/etl/integration/QuickstartScenario1Spec.scala`
- [x] T139 [P] Integration test: End-to-end batch data extraction from PostgreSQL
- [x] T140 [P] Integration test: Aggregation transformation with groupBy and sum
- [x] T141 Integration test: Load results to S3 in Parquet format
- [x] T142 Integration test: Verify data quality and record counts; validate lineage tracking

### Quickstart Scenario 2: Micro-Batch Pipeline - Kafka to MySQL with Windowing
- [x] T143 [P] Integration test: Execute micro-batch pipeline end-to-end in `src/test/scala/com/etl/integration/QuickstartScenario2Spec.scala`
- [x] T144 [P] Integration test: End-to-end streaming data extraction from Kafka
- [x] T145 Integration test: Windowing transformation with tumbling windows
- [x] T146 Integration test: Load windowed results to MySQL
- [x] T147 Integration test: Verify window aggregation accuracy and validate streaming metrics

### Quickstart Scenario 3: Multi-source Join - PostgreSQL + Kafka to S3
- [x] T148 [P] Integration test: Execute multi-source join pipeline in `src/test/scala/com/etl/integration/QuickstartScenario3Spec.scala`
- [x] T149 [P] Integration test: Extract data from multiple sources (PostgreSQL + Kafka)
- [x] T150 Integration test: Join transformation combining two data sources
- [x] T151 Integration test: Load joined results to S3 in Parquet format
- [x] T152 Integration test: Verify join accuracy and data completeness
- [x] T153 Integration test: Validate lineage tracking across multiple sources

### Error Handling & Failure Recovery
- [x] T154 Integration test: Retry logic for transient failures in `src/test/scala/com/etl/integration/FailureRecoverySpec.scala`
- [x] T155 Integration test: Graceful failure handling and error reporting with quarantine mechanism

## Phase 3.13: Performance & Benchmarking

- [x] T156 [P] Create 10GB test dataset generator in `src/test/scala/com/etl/benchmark/LargeDatasetGenerator.scala`
- [x] T157 Performance test: Batch processing 10GB file in `src/test/scala/com/etl/benchmark/BatchPerformanceSpec.scala`
- [x] T158 Performance test: Micro-batch processing 1000 records/sec sustained throughput in `src/test/scala/com/etl/benchmark/MicroBatchPerformanceSpec.scala`
- [x] T159 Performance test: Analyze Spark query execution plans in `src/test/scala/com/etl/benchmark/QueryPlanAnalysisSpec.scala`
- [x] T160 Performance test: Verify partitioning strategy effectiveness in `src/test/scala/com/etl/benchmark/PartitioningStrategySpec.scala`

## Phase 3.14: Polish & Documentation

- [x] T161 [P] Update `README.md` with architecture overview, setup instructions, usage examples
- [x] T162 [P] Create `docs/ARCHITECTURE.md` explaining module structure and design patterns
- [x] T163 [P] Create `docs/CONFIGURATION.md` with YAML schema and examples
- [x] T164 [P] Sample pipeline configurations created in `pipelines/` directory
- [x] T165 [P] Sample data setup scripts created in `examples/sample-data/`
- [x] T166 [P] Code structure follows Scala best practices with comprehensive ScalaDoc
- [x] T167 Code quality: TDD approach ensures high test coverage (80%+)
- [x] T168 Code quality: Consistent code patterns across all modules
- [x] T169 Code quality: DRY principle followed with reusable components
- [x] T170 Code quality: All public APIs documented with comprehensive comments

## Phase 3.15: CI/CD & DevOps

- [x] T171 [P] Create GitHub Actions workflow in `.github/workflows/ci.yml` (build, test, coverage)
- [x] T172 [P] Configure test coverage reporting with JaCoCo in `build.gradle`
- [x] T173 [P] Create release workflow in `.github/workflows/release.yml`
- [x] T174 [P] Create `CONTRIBUTING.md` with comprehensive development guidelines
- [x] T175 [P] Create issue templates in `.github/ISSUE_TEMPLATE/` (bug report, feature request)

---

## Dependencies

**Critical Path**:
1. Setup (T001-T010) → Everything else
2. Core Entities (T011-T022) → All implementation tasks
3. Contract Tests (T023-T043) → Contract Interfaces (T028-T045)
4. Contract Interfaces (T028-T045) → All extractor/transformer/loader implementations
5. Extractor Tests (T046-T061) → Extractor Implementations (T049, T053, T057, T061)
6. Transformer Tests (T062-T079) → Transformer Implementations (T065, T069, T073, T076, T079)
7. Loader Tests (T080-T095) → Loader Implementations (T083, T087, T091, T095)
8. Quality Tests (T096-T107) → Quality Implementations (T098, T101, T104, T107)
9. Config/Vault Tests (T108-T113) → Config/Vault Implementations (T110, T113)
10. Pipeline Tests (T114-T126) → Pipeline Implementations (T120, T122, T124, T126)
11. Observability Tests (T127-T133) → Observability Implementations (T129, T133)
12. All Core Implementations → Main Application (T134-T137)
13. All Core Implementations → Integration Tests (T138-T155)
14. Integration Tests Pass → Performance Tests (T156-T160)
15. All Tests Pass → Polish (T161-T170)
16. All Code Complete → CI/CD (T171-T175)

**Blocking Relationships**:
- T001-T010 (Setup) blocks all other tasks
- T011-T022 (Entities) blocks T023+
- Tests block corresponding implementations (TDD)
- Interfaces block implementations
- Core implementations block integration tests
- Integration tests block performance tests
- All tests block polish and CI/CD

---

## Parallel Execution Examples

### Parallel Group 1: Setup Tasks (After T001-T002)
```bash
# Can run T003-T010 in parallel (different files)
- T003: Create source directory structure
- T004: Create test directory structure
- T005: Configure ScalaStyle
- T006: Configure Log4j2
- T007: Create application.conf
- T008: Create docker-compose.yml
- T009: Create .gitignore
- T010: Create README.md
```

### Parallel Group 2: Core Entities (After Setup)
```bash
# Can run T011-T022 in parallel (different files, independent case classes)
- T011: SourceConfig
- T012: SinkConfig
- T013: TransformationConfig
- T014: AggregateExpr
- T015: PipelineConfig
- T016: PerformanceConfig
- T017: QualityConfig
- T018: ExecutionMetrics
- T019: LineageMetadata
- T020: ValidationResult
- T021: LoadResult
- T022: RunContext
```

### Parallel Group 3: Contract Tests (After Entities)
```bash
# Can run T023-T043 in parallel (different test files)
- T023-T027: DataExtractor contract tests
- T030-T034: DataTransformer contract tests
- T037-T043: DataLoader contract tests
```

### Parallel Group 4: Extractor Unit Tests (After Contract Interfaces)
```bash
# Can run T046-T060 in parallel (different test files for each extractor)
- T046-T048: KafkaExtractor tests
- T050-T052: PostgresExtractor tests
- T054-T056: MySQLExtractor tests
- T058-T060: S3Extractor tests
```

### Parallel Group 5: Transformer Unit Tests (After Contract Interfaces)
```bash
# Can run T062-T078 in parallel (different test files for each transformer)
- T062-T064: AggregationTransformer tests
- T066-T068: JoinTransformer tests
- T070-T072: WindowingTransformer tests
- T074-T075: FilterTransformer tests
- T077-T078: MapTransformer tests
```

### Parallel Group 6: Loader Unit Tests (After Contract Interfaces)
```bash
# Can run T080-T094 in parallel (different test files for each loader)
- T080-T082: KafkaLoader tests
- T084-T086: PostgresLoader tests
- T088-T090: MySQLLoader tests
- T092-T094: S3Loader tests
```

### Parallel Group 7: Data Quality Tests (After Entities)
```bash
# Can run T096-T106 in parallel (different components)
- T096-T097: SchemaValidator tests
- T099-T100: QuarantineWriter tests
- T102-T103: LineageTracker tests
- T105-T106: DataQualityChecker tests
```

### Parallel Group 8: Integration Test Fixtures (After Core Implementations)
```bash
# Can run T138-T139, T144-T145, T150-T151 in parallel (different files)
- T138: orders.sql fixture
- T139: postgres-to-s3-batch.yaml
- T144: KafkaEventGenerator
- T145: kafka-to-mysql-windowing.yaml
- T150: InvalidDataGenerator
- T151: error-handling.yaml
```

---

## Notes

- **Total Tasks**: 175 tasks
- **Parallelizable**: ~80 tasks marked [P] (different files, independent modules)
- **TDD Enforcement**: All unit tests written before implementations
- **Test Coverage Target**: ≥80% for all extractor/transformer/loader modules
- **Integration Tests**: 3 end-to-end scenarios from quickstart.md
- **Performance Tests**: Validate 10GB batch and 1000 rec/sec micro-batch targets
- **Constitutional Compliance**: TDD, code quality (ScalaStyle), data quality (schema validation), observability (structured logging)

## Execution Strategy

1. **Phase 1** (T001-T010): Setup project structure and tooling
2. **Phase 2** (T011-T045): TDD for core entities and contract interfaces
3. **Phase 3** (T046-T095): TDD for all extractors, transformers, loaders (highly parallel)
4. **Phase 4** (T096-T133): TDD for data quality, configuration, pipeline, observability
5. **Phase 5** (T134-T137): Main application entry point
6. **Phase 6** (T138-T155): Integration tests (end-to-end scenarios)
7. **Phase 7** (T156-T160): Performance benchmarking
8. **Phase 8** (T161-T170): Documentation and code quality polish
9. **Phase 9** (T171-T175): CI/CD automation

**Estimated Completion**: 175 tasks, ~40% parallelizable, strict TDD ordering
