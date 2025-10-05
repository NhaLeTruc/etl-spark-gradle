package com.etl.core

/**
 * Root configuration for an ETL pipeline (YAML deserialization target).
 *
 * @param name Pipeline name
 * @param executionMode Execution mode ("batch" | "micro-batch")
 * @param source Source configuration
 * @param transformations List of transformation configurations (applied in order)
 * @param sink Sink configuration
 * @param performance Performance tuning configuration
 * @param quality Data quality configuration
 */
case class PipelineConfig(
  name: String,
  executionMode: String,
  source: SourceConfig,
  transformations: List[TransformationConfig],
  sink: SinkConfig,
  performance: PerformanceConfig,
  quality: QualityConfig
)
