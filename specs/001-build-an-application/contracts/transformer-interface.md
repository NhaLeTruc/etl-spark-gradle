# Contract: DataTransformer Interface

**Date**: 2025-10-05
**Feature**: 001-build-an-application

## Interface Definition

```scala
package com.etl.core

import org.apache.spark.sql.{DataFrame, types.StructType}

/**
 * DataTransformer abstracts data transformations.
 * Implementations: AggregationTransformer, JoinTransformer, WindowingTransformer,
 *                  FilterTransformer, MapTransformer
 */
trait DataTransformer {

  /**
   * Apply transformation to input DataFrame.
   *
   * @param input DataFrame with Avro schema and lineage metadata
   * @param config Transformation-specific configuration
   * @param runContext Pipeline execution context (pipelineId, runId, etc.)
   * @return Transformed DataFrame with updated lineage metadata
   * @throws TransformationException if transformation fails
   */
  def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): DataFrame

  /**
   * Validate transformation configuration against input schema.
   *
   * @param config Transformation configuration
   * @param inputSchema Input DataFrame schema for validation
   * @return ValidationResult with errors if invalid
   */
  def validateConfig(
    config: TransformationConfig,
    inputSchema: StructType
  ): ValidationResult

  /**
   * Transformation type identifier (matches TransformationConfig.type).
   *
   * @return Transformation type ("aggregation" | "join" | "windowing" | "filter" | "map")
   */
  def transformationType: String
}
```

## Contract Tests

### Test 1: Transform Returns Valid DataFrame

**Given**: Valid input DataFrame and TransformationConfig
**When**: `transform()` is called
**Then**:
- Output DataFrame is not null
- Output DataFrame schema is Avro-compatible
- Lineage metadata updated with transformation type

### Test 2: Validate Config Detects Schema Mismatch

**Given**: TransformationConfig referencing non-existent column
**When**: `validateConfig()` is called with input schema
**Then**:
- ValidationResult.isValid == false
- ValidationResult.errors contains column not found message

### Test 3: Transform is Idempotent

**Given**: Same input DataFrame and config
**When**: `transform()` is called twice
**Then**:
- Both outputs are identical (same schema, same data, same row count)

### Test 4: Lineage Chain is Updated

**Given**: Input DataFrame with existing lineage chain `["extraction"]`
**When**: `transform()` is called with AggregationTransformer
**Then**:
- Output lineage chain is `["extraction", "aggregation"]`

### Test 5: Transform Handles Empty DataFrame

**Given**: Empty input DataFrame (0 rows)
**When**: `transform()` is called
**Then**:
- Output DataFrame is empty
- Schema is still valid
- No exception thrown

## Implementation Requirements

### AggregationTransformer

**Required Parameters**:
- `groupBy`: List[String] - Columns to group by
- `aggregates`: List[AggregateExpr] - Aggregation expressions

**Behavior**:
- Use Spark `groupBy()` and `agg()`
- Support: sum, count, avg, min, max
- Update lineage chain

### JoinTransformer

**Required Parameters**:
- `joinType`: "inner" | "left" | "right" | "full"
- `leftOn`: String - Left join column
- `rightOn`: String - Right join column
- `rightSource`: SourceConfig - Right DataFrame source

**Behavior**:
- Extract right DataFrame using DataExtractor
- Perform Spark join
- Merge lineage metadata from both sides

### WindowingTransformer

**Required Parameters**:
- `windowType`: "tumbling" | "sliding" | "session"
- `duration`: String - Window duration (e.g., "10 seconds")
- `slideInterval`: Option[String] - For sliding windows

**Behavior**:
- Use Spark Structured Streaming windowing
- Requires `timestamp` column in input
- Update lineage with window parameters

### FilterTransformer

**Required Parameters**:
- `condition`: String - SQL expression (e.g., "amount > 1000")

**Behavior**:
- Use Spark `filter()` or `where()`
- Validate SQL expression syntax
- Update lineage

### MapTransformer

**Required Parameters**:
- `expressions`: Map[String, String] - Column → SQL expression

**Behavior**:
- Use Spark `selectExpr()` or `withColumn()`
- Support column rename, type conversion, derived columns
- Update lineage

## Exception Handling

All transformers MUST throw `TransformationException` with:
- Transformation type
- Configuration summary
- Root cause exception

```scala
case class TransformationException(
  transformationType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$transformationType] $message", cause)
```

## Performance Expectations

- **Aggregation**: Use `coalesce()` before aggregation for large groups
- **Join**: Broadcast small dimension tables (<100MB)
- **Windowing**: Watermarking for late data handling
- **Filter**: Predicate pushdown where possible
- **Map**: Column pruning before transformation

## Testing Strategy

- **Unit Tests**: Sample DataFrames with known inputs/outputs
- **Contract Tests**: Verify interface compliance for all five implementations
- **Integration Tests**: Chain transformations end-to-end
