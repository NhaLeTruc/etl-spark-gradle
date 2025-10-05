# ETL Spark Gradle

Multi-source ETL application built with Apache Spark 3.5.6, Scala 2.12, and Gradle 7.6.5.

## Overview

This application extracts data from Kafka, PostgreSQL, MySQL, and Amazon S3, applies transformations (aggregations, joins, windowing), and loads results back to these systems using Apache Spark for batch and micro-batch processing.

## Features

- **Multi-Source Support**: Kafka, PostgreSQL, MySQL, S3
- **Data Format**: Avro for data interchange with schema validation
- **Execution Modes**: Batch (Spark) and micro-batch (Spark Streaming)
- **Transformations**: Aggregations, joins, windowing, filtering, mapping
- **Write Modes**: Append, overwrite, upsert
- **Data Quality**: Schema validation, error quarantine, data lineage tracking
- **Configuration**: YAML-based pipeline definitions
- **Secrets Management**: HashiCorp Vault integration
- **Observability**: Structured JSON logging with correlation IDs

## Architecture

- **Dependency Injection**: Constructor-based DI for modularity and testability
- **Reusable Modules**: 4 extractors, 5 transformers, 4 loaders
- **TDD Approach**: 80%+ unit test coverage
- **Performance**: Handles 10GB batch files and 1000 records/sec micro-batch throughput

## Prerequisites

- **Java**: 11 or later
- **Scala**: 2.12.18
- **Gradle**: 7.6.5 (or use included wrapper)
- **Docker**: For local Vault instance (optional)

## Quick Start

### 1. Start Vault (Development Mode)

```bash
docker-compose up -d
```

### 2. Build the Project

```bash
./gradlew build
```

### 3. Run Tests

```bash
./gradlew test
```

### 4. Run a Pipeline

```bash
./gradlew run --args="path/to/pipeline.yaml"
```

## Project Structure

```
src/
├── main/
│   ├── scala/com/etl/
│   │   ├── config/          # YAML configuration parsing
│   │   ├── core/            # Core abstractions (interfaces, entities)
│   │   ├── extractor/       # Data extractors (Kafka, PostgreSQL, MySQL, S3)
│   │   ├── transformer/     # Data transformers (aggregation, join, windowing)
│   │   ├── loader/          # Data loaders (Kafka, PostgreSQL, MySQL, S3)
│   │   ├── pipeline/        # Pipeline orchestration
│   │   ├── quality/         # Data quality validation and quarantine
│   │   ├── lineage/         # Data lineage tracking
│   │   ├── vault/           # Credential management
│   │   ├── logging/         # Structured logging
│   │   └── Main.scala       # Application entry point
│   └── resources/
│       ├── application.conf
│       └── log4j2.xml
└── test/
    ├── scala/com/etl/
    │   ├── extractor/
    │   ├── transformer/
    │   ├── loader/
    │   ├── quality/
    │   ├── pipeline/
    │   └── fixtures/
    └── resources/
        ├── test-pipelines/
        └── test-data/
```

## Pipeline Configuration Example

```yaml
pipeline:
  name: "postgres-to-s3-aggregation"
  execution_mode: "batch"

  source:
    type: "postgres"
    credentials_path: "secret/postgres/prod"
    parameters:
      jdbcUrl: "jdbc:postgresql://localhost:5432/orders"
      query: "SELECT * FROM orders WHERE created_at > '2025-01-01'"

  transformations:
    - type: "aggregation"
      parameters:
        groupBy: ["customer_id", "product_id"]
        aggregates:
          - column: "amount"
            function: "sum"
            alias: "total_amount"

  sink:
    type: "s3"
    credentials_path: "secret/s3/prod"
    parameters:
      bucket: "my-bucket"
      prefix: "aggregated-orders/"
      format: "avro"
    write_mode: "overwrite"

  performance:
    partitions: 200
    executor_memory: "4g"
    executor_cores: 2

  quality:
    schema_validation: true
    null_check_columns: ["customer_id", "product_id"]
    quarantine_path: "s3a://my-bucket/errors/"
```

## Testing

- **Unit Tests**: ScalaTest with mocked Spark sessions and data sources
- **Integration Tests**: End-to-end pipeline tests with embedded databases and Kafka
- **Performance Tests**: Benchmarking with representative data volumes

```bash
# Run all tests
./gradlew test

# Run specific test
./gradlew test --tests "com.etl.extractor.KafkaExtractorSpec"

# Run with coverage
./gradlew test jacocoTestReport
```

## Constitutional Principles

This project adheres to strict development principles:

1. **Code Quality**: Scala Style Guide, ScalaStyle enforcement, <10 cyclomatic complexity
2. **Test-First Development**: TDD mandatory, 80%+ coverage
3. **Data Quality**: Schema validation, quarantine, idempotency
4. **Performance**: Explicit partitioning, benchmarks
5. **Observability**: Structured logging with correlation IDs

## Contributing

1. Create a feature branch
2. Write tests first (TDD)
3. Implement code following Scala Style Guide
4. Ensure all tests pass
5. Run ScalaStyle: `./gradlew scalaStyle`
6. Submit pull request

## License

Copyright (c) 2025. All rights reserved.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Configuration Guide](docs/CONFIGURATION.md)
- [Development Guide](CONTRIBUTING.md)

## Performance Targets

- **Batch**: Process 10GB files
- **Micro-batch**: Sustain 1000 records/sec throughput
- **Retry**: Maximum 3 retries with 5-second delay

## Support

For issues and questions, please refer to the project documentation or create an issue in the repository.
