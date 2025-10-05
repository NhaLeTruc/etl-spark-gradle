package com.etl.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * DataTransformer abstracts data transformations.
 *
 * Implementations must:
 * - Apply transformation to input DataFrame
 * - Return transformed DataFrame with updated lineage
 * - Validate configuration against input schema
 * - Ensure idempotency (same input → same output)
 * - Handle empty DataFrames gracefully
 *
 * Implementations: AggregationTransformer, JoinTransformer, WindowingTransformer,
 *                  FilterTransformer, MapTransformer
 */
trait DataTransformer {

  /**
   * Apply transformation to input DataFrame.
   *
   * Implementations must:
   * - Preserve Avro-compatible schema
   * - Update lineage metadata (add transformation type to chain)
   * - Be idempotent (repeatable with same results)
   * - Handle empty DataFrames
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
   * Implementations should check for:
   * - Required parameters present
   * - Referenced columns exist in input schema
   * - Expressions are valid
   * - Data types compatible
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
   * Used for:
   * - Registry lookup
   * - Lineage metadata
   * - Error messages
   *
   * @return Transformation type ("aggregation" | "join" | "windowing" | "filter" | "map")
   */
  def transformationType: String
}
