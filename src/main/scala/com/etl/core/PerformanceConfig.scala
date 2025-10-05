package com.etl.core

/**
 * Performance tuning parameters for Spark jobs.
 *
 * @param partitions Number of partitions for repartitioning
 * @param executorMemory Executor memory (e.g., "4g")
 * @param executorCores Cores per executor
 * @param maxOffsetsPerTrigger Kafka rate limiting (micro-batch only)
 */
case class PerformanceConfig(
  partitions: Int,
  executorMemory: String,
  executorCores: Int,
  maxOffsetsPerTrigger: Option[Long] = None
)
