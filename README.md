# ETL Spark Gradle

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![Test Coverage](https://img.shields.io/badge/coverage-80%25-green)]()
[![Scala](https://img.shields.io/badge/scala-2.12.18-red)]()
[![Spark](https://img.shields.io/badge/spark-3.5.6-orange)]()

Production-grade multi-source ETL application built with Apache Spark 3.5.6, Scala 2.12, and Gradle 7.6.5.

## Overview

This application provides a flexible, high-performance data pipeline framework that extracts data from Kafka, PostgreSQL, MySQL, and Amazon S3, applies sophisticated transformations (aggregations, joins, windowing, filtering, mapping), and loads results back to these systems using Apache Spark for both batch and micro-batch processing.

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

### 1. Clone and Build

```bash
git clone <repository-url>
cd etl-spark-gradle
./gradlew build
```

### 2. Start Dependencies (Development Mode)

```bash
# Start Vault and other services
docker-compose up -d

# Verify Vault is running
curl http://localhost:8200/v1/sys/health
```

### 3. Run Tests

```bash
# Run all tests
./gradlew test

# Run specific test suite
./gradlew test --tests "com.etl.integration.*"

# Run with coverage report
./gradlew test jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

### 4. Run Example Pipelines

```bash
# Scenario 1: Batch PostgreSQL to S3 with Aggregation
./gradlew run --args="--pipeline pipelines/quickstart-1-sales-aggregation.yaml --master local[4]"

# Scenario 2: Streaming Kafka to MySQL with Windowing
./gradlew run --args="--pipeline pipelines/quickstart-2-metrics-windowing.yaml --master local[4]"

# Scenario 3: Multi-source Join (PostgreSQL + Kafka to S3)
./gradlew run --args="--pipeline pipelines/quickstart-3-multi-source-join.yaml --master local[4]"
```

### 5. Run Performance Benchmarks

```bash
# Batch performance (10GB dataset)
BENCHMARK_SIZE_GB=10 ./gradlew test --tests "com.etl.benchmark.BatchPerformanceSpec"

# Micro-batch performance (1000 rec/sec)
./gradlew test --tests "com.etl.benchmark.MicroBatchPerformanceSpec"
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

## Pipeline Configuration

Pipelines are defined using YAML configuration files. See [pipelines/](pipelines/) for complete examples.

### Basic Example

```yaml
pipelineId: sales-aggregation-pipeline

source:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/salesdb
    driver: org.postgresql.Driver
    dbtable: sales
    user: ${VAULT:database/postgres/sales:username}
    password: ${VAULT:database/postgres/sales:password}

transformations:
  - name: aggregate-by-category
    type: aggregation
    options:
      groupBy: category
      aggregations: total_quantity:sum(quantity),total_revenue:sum(price * quantity),avg_price:avg(price)

sink:
  type: s3
  options:
    path: s3://my-bucket/sales-summary/
    format: parquet
  writeMode: overwrite

performance:
  repartition: 4
  cacheIntermediate: false
  shufflePartitions: 8

quality:
  schemaValidation: false
  nullChecks: []
```

### Advanced Example with Data Quality

```yaml
pipelineId: customer-enrichment-pipeline

source:
  type: kafka
  options:
    kafka.bootstrap.servers: localhost:9092
    subscribe: customer-events
    startingOffsets: earliest

transformations:
  - name: parse-json
    type: map
    options:
      expressions: customer_id:get_json_object(value, '$.customer_id'),email:get_json_object(value, '$.email')

  - name: join-with-profile
    type: join
    options:
      rightDataset: customer_profiles
      joinType: left
      joinKeys: customer_id

sink:
  type: mysql
  options:
    url: jdbc:mysql://localhost:3306/analytics
    dbtable: enriched_customers
    user: ${VAULT:database/mysql/analytics:username}
    password: ${VAULT:database/mysql/analytics:password}
  writeMode: upsert

performance:
  repartition: 8
  shufflePartitions: 16

quality:
  schemaValidation: true
  nullChecks:
    - column: customer_id
      action: quarantine
    - column: email
      action: quarantine
  quarantinePath: s3://my-bucket/quarantine/customers/
```

For complete YAML schema documentation, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

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
