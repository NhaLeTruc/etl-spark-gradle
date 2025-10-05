package com.etl.core

/**
 * Configuration for a transformation step.
 *
 * @param `type` Transformation type ("aggregation" | "join" | "windowing" | "filter" | "map")
 * @param parameters Type-specific parameters (varies by transformation type)
 */
case class TransformationConfig(
  `type`: String,
  parameters: Map[String, Any]
)
