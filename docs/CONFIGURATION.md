# Pipeline Configuration Guide

## Table of Contents

- [Overview](#overview)
- [YAML Schema](#yaml-schema)
- [Source Configuration](#source-configuration)
- [Transformation Configuration](#transformation-configuration)
- [Sink Configuration](#sink-configuration)
- [Performance Tuning](#performance-tuning)
- [Data Quality](#data-quality)
- [Vault Integration](#vault-integration)
- [Complete Examples](#complete-examples)

## Overview

Pipelines are defined using YAML configuration files that specify:
- **Source**: Where to extract data from
- **Transformations**: How to process the data
- **Sink**: Where to load the results
- **Performance**: Spark tuning parameters
- **Quality**: Data validation rules

## YAML Schema

### Top-Level Structure

```yaml
pipelineId: string              # Unique pipeline identifier (required)

source:                         # Source configuration (required)
  type: string                  # Source type: kafka, postgres, mysql, s3
  options: map                  # Source-specific options
  schemaPath: string            # Optional Avro schema path

transformations:                # List of transformations (optional)
  - name: string                # Transformation name
    type: string                # Transformation type
    options: map                # Transformation-specific options

sink:                           # Sink configuration (required)
  type: string                  # Sink type: kafka, postgres, mysql, s3
  options: map                  # Sink-specific options
  writeMode: string             # append, overwrite, or upsert
  partitionBy: string           # Optional partition column

performance:                    # Performance tuning (optional)
  repartition: int              # Number of partitions
  cacheIntermediate: boolean    # Cache intermediate results
  shufflePartitions: int        # Shuffle partition count

quality:                        # Data quality (optional)
  schemaValidation: boolean     # Enable schema validation
  nullChecks: list              # Null check configurations
  duplicateCheck: map           # Duplicate detection config
  quarantinePath: string        # Path for invalid records
```

## Source Configuration

### Kafka Source

```yaml
source:
  type: kafka
  options:
    kafka.bootstrap.servers: localhost:9092    # Kafka brokers (required)
    subscribe: topic-name                      # Topic to subscribe to (required)
    startingOffsets: earliest                  # earliest or latest (optional, default: latest)
    # Additional Kafka options
    kafka.security.protocol: SASL_SSL
    kafka.sasl.mechanism: PLAIN
```

**Supported Options**:
- `kafka.bootstrap.servers`: Comma-separated list of Kafka brokers
- `subscribe`: Topic name or pattern
- `startingOffsets`: `earliest`, `latest`, or JSON offset specification
- `kafka.*`: Any valid Kafka consumer configuration

**Output Schema**:
| Column | Type | Description |
|--------|------|-------------|
| key | string | Message key |
| value | string | Message value |
| topic | string | Source topic |
| partition | int | Partition number |
| offset | long | Message offset |
| timestamp | timestamp | Message timestamp |

### PostgreSQL Source

```yaml
source:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/dbname  # JDBC URL (required)
    driver: org.postgresql.Driver                 # JDBC driver (required)
    dbtable: schema.table                         # Table or query (required)
    user: ${VAULT:database/postgres/prod:username}
    password: ${VAULT:database/postgres/prod:password}
    # Optional performance tuning
    partitionColumn: id
    numPartitions: 4
    lowerBound: 1
    upperBound: 1000000
```

**Supported Options**:
- `url`: PostgreSQL JDBC connection string
- `driver`: Always `org.postgresql.Driver`
- `dbtable`: Table name or SQL query wrapped in parentheses: `(SELECT * FROM table WHERE ...) AS alias`
- `user`, `password`: Credentials (use Vault references)
- `partitionColumn`: Column for partitioned reading (numeric/date)
- `numPartitions`: Number of partitions for parallel reading
- `lowerBound`, `upperBound`: Bounds for partition generation

### MySQL Source

```yaml
source:
  type: mysql
  options:
    url: jdbc:mysql://localhost:3306/dbname
    driver: com.mysql.cj.jdbc.Driver
    dbtable: table_name
    user: ${VAULT:database/mysql/prod:username}
    password: ${VAULT:database/mysql/prod:password}
```

**Options**: Same as PostgreSQL, with `driver: com.mysql.cj.jdbc.Driver`

### S3 Source

```yaml
source:
  type: s3
  options:
    path: s3://bucket/prefix/                     # S3 path (required)
    format: parquet                               # Format: avro, parquet, json, csv (required)
    # Format-specific options
    header: "true"                                # CSV: first row is header
    delimiter: ","                                # CSV: field delimiter
    mergeSchema: "true"                           # Parquet: merge schemas from multiple files
```

**Supported Formats**:
- **Avro**: Schema embedded in files
- **Parquet**: Columnar format with embedded schema
- **JSON**: One JSON object per line
- **CSV**: Comma-separated values

**Format-Specific Options**:

**CSV**:
```yaml
options:
  format: csv
  header: "true"           # First row contains column names
  delimiter: ","           # Field delimiter
  quote: "\""              # Quote character
  escape: "\\"             # Escape character
  inferSchema: "true"      # Infer column types
```

**JSON**:
```yaml
options:
  format: json
  multiLine: "false"       # Single JSON object per line (default)
```

**Parquet**:
```yaml
options:
  format: parquet
  mergeSchema: "true"      # Merge schemas from multiple files
```

## Transformation Configuration

### Aggregation

```yaml
transformations:
  - name: aggregate-sales
    type: aggregation
    options:
      groupBy: customer_id,product_id           # Comma-separated columns
      aggregations: total_amount:sum(amount),avg_price:avg(price),sale_count:count(*)
```

**Format**: `alias:function(column)` separated by commas

**Supported Functions**:
- `sum(column)`: Sum of values
- `avg(column)`: Average
- `count(*)` or `count(column)`: Count
- `min(column)`: Minimum
- `max(column)`: Maximum
- `first(column)`: First value
- `last(column)`: Last value
- `collect_list(column)`: Collect values as array
- `collect_set(column)`: Collect unique values as set

### Join

```yaml
transformations:
  - name: join-with-customers
    type: join
    options:
      rightDataset: customers                   # Right DataFrame (must exist in context)
      joinType: inner                           # inner, left, right, full
      joinKeys: customer_id                     # Comma-separated join columns
      selectColumns: order_id,customer_id,customer_name,amount  # Optional column selection
```

**Join Types**:
- `inner`: Only matching records
- `left`: All left records, matching right records
- `right`: All right records, matching left records
- `full`: All records from both sides

### Windowing

```yaml
transformations:
  - name: window-aggregation
    type: windowing
    options:
      windowType: tumbling                      # tumbling or sliding
      windowDuration: 1 minute                  # Window size
      slideDuration: 30 seconds                 # Slide interval (sliding windows only)
      timestampColumn: event_time               # Timestamp column
      groupBy: user_id                          # Optional grouping columns
      aggregations: event_count:count(*),total_value:sum(value)
```

**Window Types**:
- **Tumbling**: Non-overlapping fixed-size windows
  ```
  [0-5min][5-10min][10-15min]
  ```
- **Sliding**: Overlapping windows
  ```
  [0-5min]
     [2-7min]
        [4-9min]
  ```

**Duration Format**: `<number> <unit>` where unit is:
- `seconds`, `minutes`, `hours`, `days`, `weeks`

### Filter

```yaml
transformations:
  - name: filter-active-users
    type: filter
    options:
      condition: status = 'active' AND created_at >= '2024-01-01'
```

**Condition**: SQL WHERE clause expression

**Supported Operators**:
- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Pattern matching: `LIKE`, `RLIKE` (regex)
- Null checks: `IS NULL`, `IS NOT NULL`
- Set membership: `IN (value1, value2, ...)`

### Map

```yaml
transformations:
  - name: add-calculated-columns
    type: map
    options:
      expressions: revenue:price * quantity,year:year(order_date),full_name:concat(first_name, ' ', last_name)
```

**Format**: `alias:expression` separated by commas

**Supported Functions**:
- **Math**: `+`, `-`, `*`, `/`, `%`, `abs()`, `sqrt()`, `round()`
- **String**: `concat()`, `upper()`, `lower()`, `substring()`, `length()`
- **Date**: `year()`, `month()`, `day()`, `date_add()`, `datediff()`
- **Type conversion**: `cast(column as type)`
- **JSON**: `get_json_object(column, '$.path')`
- **Conditional**: `case when ... then ... else ... end`

## Sink Configuration

### Kafka Sink

```yaml
sink:
  type: kafka
  options:
    kafka.bootstrap.servers: localhost:9092
    topic: output-topic
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
    format: json                                 # json or avro
    keyColumn: id                                # Optional: column for message key
  writeMode: append
```

**Write Modes**: Only `append` supported for Kafka

### PostgreSQL Sink

```yaml
sink:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/dbname
    driver: org.postgresql.Driver
    dbtable: schema.target_table
    user: ${VAULT:database/postgres/prod:username}
    password: ${VAULT:database/postgres/prod:password}
    primaryKey: id                               # Required for upsert mode
  writeMode: upsert
```

**Write Modes**:
- `append`: Insert new records
- `overwrite`: Truncate and insert
- `upsert`: Insert or update based on primary key

**Upsert Requirements**:
- `primaryKey` option must be specified
- Uses PostgreSQL `ON CONFLICT ... DO UPDATE` syntax

### MySQL Sink

```yaml
sink:
  type: mysql
  options:
    url: jdbc:mysql://localhost:3306/dbname
    driver: com.mysql.cj.jdbc.Driver
    dbtable: target_table
    user: ${VAULT:database/mysql/prod:username}
    password: ${VAULT:database/mysql/prod:password}
    primaryKey: id
  writeMode: upsert
```

**Options**: Same as PostgreSQL
**Upsert**: Uses `ON DUPLICATE KEY UPDATE` syntax

### S3 Sink

```yaml
sink:
  type: s3
  options:
    path: s3://bucket/output/
    format: parquet
    compression: snappy                          # Optional: none, snappy, gzip, lz4
  writeMode: overwrite
  partitionBy: year,month                        # Optional: comma-separated columns
```

**Write Modes**:
- `append`: Add new files
- `overwrite`: Delete existing and write new

**Partitioning**:
```yaml
partitionBy: year,month,day
# Creates structure:
# s3://bucket/output/year=2024/month=01/day=15/*.parquet
```

**Compression Options**:
- `none`: No compression
- `snappy`: Fast compression (default for Parquet)
- `gzip`: High compression ratio
- `lz4`: Very fast compression

## Performance Tuning

### Repartitioning

```yaml
performance:
  repartition: 200          # Shuffle data into 200 partitions
```

**When to Use**:
- Before expensive operations (joins, aggregations)
- To balance partition sizes
- To match executor count

**Guidelines**:
- Small datasets (<1GB): 4-8 partitions
- Medium datasets (1-10GB): 8-32 partitions
- Large datasets (10GB+): 100-200 partitions
- Rule of thumb: 128MB-1GB per partition

### Shuffle Partitions

```yaml
performance:
  shufflePartitions: 200    # Default: 200
```

**Controls**: Number of partitions after shuffle operations (joins, aggregations)

**Tuning**:
- Too few: Large partitions, memory pressure
- Too many: Small partitions, task overhead

### Intermediate Caching

```yaml
performance:
  cacheIntermediate: true
```

**When to Enable**:
- Transformation results used multiple times
- Expensive computations (joins, complex aggregations)

**When to Disable**:
- Linear pipeline (each result used once)
- Large datasets that don't fit in memory

## Data Quality

### Schema Validation

```yaml
quality:
  schemaValidation: true
  schemaPath: schemas/orders.avsc      # Optional: path to Avro schema
```

**Validates**:
- Column names match expected
- Column types match expected
- Nullable constraints

### Null Checks

```yaml
quality:
  nullChecks:
    - column: customer_id
      action: quarantine               # quarantine or fail
    - column: order_date
      action: fail
```

**Actions**:
- `quarantine`: Write invalid records to quarantine path
- `fail`: Abort pipeline execution

### Duplicate Detection

```yaml
quality:
  duplicateCheck:
    columns:
      - customer_id
      - order_id
    action: quarantine
```

**Configuration**:
- `columns`: List of columns for duplicate detection
- `action`: `quarantine` or `fail`

### Quarantine Path

```yaml
quality:
  quarantinePath: s3://bucket/quarantine/orders/
```

**Quarantine Metadata**:
Quarantined records include additional columns:
- `quarantine_timestamp`: When record was quarantined
- `pipeline_id`: Pipeline identifier
- `run_id`: Execution run identifier
- `validation_errors`: Reasons for quarantine

## Vault Integration

### Vault References

Use `${VAULT:path/to/secret:key}` syntax to retrieve secrets:

```yaml
source:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/db
    user: ${VAULT:database/postgres/prod:username}
    password: ${VAULT:database/postgres/prod:password}
```

**Format**: `${VAULT:<vault-path>:<secret-key>}`

### Setting Vault Environment Variables

```bash
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=dev-token
```

### Storing Secrets in Vault

```bash
# Using Vault CLI
vault kv put database/postgres/prod username=dbuser password=secret123

# Using Vault API
curl -X POST http://localhost:8200/v1/database/postgres/prod \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -d '{"username":"dbuser","password":"secret123"}'
```

## Complete Examples

### Example 1: Batch Sales Aggregation

**Scenario**: Daily aggregation of sales from PostgreSQL to S3

```yaml
pipelineId: daily-sales-aggregation

source:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/sales
    driver: org.postgresql.Driver
    dbtable: sales
    user: ${VAULT:database/postgres/sales:username}
    password: ${VAULT:database/postgres/sales:password}
    partitionColumn: sale_id
    numPartitions: 8

transformations:
  - name: filter-today
    type: filter
    options:
      condition: sale_date = CURRENT_DATE

  - name: aggregate-by-product
    type: aggregation
    options:
      groupBy: product_id,category
      aggregations: total_quantity:sum(quantity),total_revenue:sum(price * quantity),avg_price:avg(price),sale_count:count(*)

sink:
  type: s3
  options:
    path: s3://analytics-bucket/sales-summary/
    format: parquet
    compression: snappy
  writeMode: overwrite
  partitionBy: category

performance:
  repartition: 16
  cacheIntermediate: false
  shufflePartitions: 16

quality:
  schemaValidation: false
  nullChecks:
    - column: product_id
      action: quarantine
  quarantinePath: s3://analytics-bucket/quarantine/sales/
```

### Example 2: Real-Time Event Processing

**Scenario**: Kafka event stream with windowing to MySQL

```yaml
pipelineId: realtime-metrics-pipeline

source:
  type: kafka
  options:
    kafka.bootstrap.servers: kafka1:9092,kafka2:9092
    subscribe: user-events
    startingOffsets: latest
    kafka.security.protocol: SASL_SSL
    kafka.sasl.mechanism: SCRAM-SHA-256

transformations:
  - name: parse-json-events
    type: map
    options:
      expressions: user_id:get_json_object(value, '$.user_id'),event_type:get_json_object(value, '$.event_type'),event_value:cast(get_json_object(value, '$.value') as double),event_time:cast(get_json_object(value, '$.timestamp') as timestamp)

  - name: tumbling-window
    type: windowing
    options:
      windowType: tumbling
      windowDuration: 5 minutes
      timestampColumn: event_time
      groupBy: user_id,event_type
      aggregations: event_count:count(*),total_value:sum(event_value),avg_value:avg(event_value)

sink:
  type: mysql
  options:
    url: jdbc:mysql://mysql-server:3306/metrics
    driver: com.mysql.cj.jdbc.Driver
    dbtable: user_metrics
    user: ${VAULT:database/mysql/metrics:username}
    password: ${VAULT:database/mysql/metrics:password}
    primaryKey: user_id,event_type,window_start
  writeMode: upsert

performance:
  repartition: 8
  shufflePartitions: 8

quality:
  schemaValidation: true
  nullChecks:
    - column: user_id
      action: fail
    - column: event_type
      action: fail
```

### Example 3: Multi-Source Join

**Scenario**: Join orders (PostgreSQL) with product details (S3)

```yaml
pipelineId: order-enrichment-pipeline

source:
  type: postgres
  options:
    url: jdbc:postgresql://localhost:5432/orders
    driver: org.postgresql.Driver
    dbtable: orders
    user: ${VAULT:database/postgres/orders:username}
    password: ${VAULT:database/postgres/orders:password}

transformations:
  # Note: In practice, you'd need to load product_details separately
  # This example assumes it's available in the execution context
  - name: join-products
    type: join
    options:
      rightDataset: product_details
      joinType: left
      joinKeys: product_id
      selectColumns: order_id,customer_id,product_id,product_name,category,quantity,price

  - name: calculate-totals
    type: map
    options:
      expressions: total_amount:quantity * price,order_year:year(order_date),order_month:month(order_date)

  - name: aggregate-by-category
    type: aggregation
    options:
      groupBy: category,order_year,order_month
      aggregations: total_orders:count(*),total_quantity:sum(quantity),total_revenue:sum(total_amount)

sink:
  type: s3
  options:
    path: s3://analytics/enriched-orders/
    format: parquet
  writeMode: append
  partitionBy: order_year,order_month

performance:
  repartition: 16
  cacheIntermediate: true
  shufflePartitions: 32

quality:
  schemaValidation: true
  nullChecks:
    - column: order_id
      action: quarantine
    - column: product_id
      action: quarantine
  duplicateCheck:
    columns:
      - order_id
    action: quarantine
  quarantinePath: s3://analytics/quarantine/enriched-orders/
```

## Validation

Use the `YAMLConfigParser` to validate your configuration:

```bash
# Parse and validate configuration
./gradlew run --args="--pipeline path/to/pipeline.yaml --validate-only"
```

## Troubleshooting

### Common Configuration Errors

**Invalid YAML syntax**:
```
Error: mapping values are not allowed here
```
Solution: Check indentation (use spaces, not tabs)

**Missing required field**:
```
Error: Required field 'pipelineId' is missing
```
Solution: Add all required top-level fields

**Invalid transformation options**:
```
Error: Invalid aggregation expression: 'invalid_func(column)'
```
Solution: Use supported aggregation functions

**Vault reference not resolved**:
```
Error: Could not resolve Vault reference: ${VAULT:path/to/secret:key}
```
Solution: Verify Vault is running, token is valid, and secret exists

## Best Practices

1. **Use Vault for Credentials**: Never hardcode passwords
2. **Partition Large Datasets**: Improve read/write performance
3. **Enable Quality Checks**: Catch data issues early
4. **Tune Partitions**: Match your cluster size
5. **Monitor Metrics**: Track execution time and record counts
6. **Test Locally**: Validate pipelines with small datasets first
7. **Version Control**: Track configuration changes in Git
8. **Document Transformations**: Use descriptive transformation names

## References

- [Apache Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- [Avro Schema Specification](https://avro.apache.org/docs/current/spec.html)
- [YAML Specification](https://yaml.org/spec/1.2/spec.html)
- [HashiCorp Vault API](https://www.vaultproject.io/api-docs)
