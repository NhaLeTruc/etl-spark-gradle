package com.etl.pipeline

import com.etl.core.DataTransformer
import com.etl.transformer._

/**
 * Registry for data transformers.
 *
 * Maps transformation types to transformer implementations.
 */
class TransformerRegistry {

  private var transformers: Map[String, DataTransformer] = Map(
    "aggregation" -> new AggregationTransformer(),
    "join" -> new JoinTransformer(),
    "windowing" -> new WindowingTransformer(),
    "filter" -> new FilterTransformer(),
    "map" -> new MapTransformer()
  )

  /**
   * Get transformer by transformation type.
   *
   * @param transformationType Transformation type identifier
   * @return Data transformer instance
   * @throws IllegalArgumentException if transformation type is not registered
   */
  def get(transformationType: String): DataTransformer = {
    transformers.getOrElse(
      transformationType,
      throw new IllegalArgumentException(s"Unknown transformation type: $transformationType")
    )
  }

  /**
   * Register custom transformer.
   *
   * @param transformationType Transformation type identifier
   * @param transformer Transformer implementation
   */
  def register(transformationType: String, transformer: DataTransformer): Unit = {
    transformers = transformers + (transformationType -> transformer)
  }

  /**
   * List all registered transformation types.
   *
   * @return List of registered transformation types
   */
  def list(): List[String] = {
    transformers.keys.toList
  }
}
