package com.etl.core

import org.apache.spark.sql.SparkSession

/**
 * Pipeline execution context passed through transformation chain.
 *
 * @param pipelineId Pipeline identifier
 * @param runId Unique run UUID
 * @param startTimestamp Pipeline start time (epoch millis)
 * @param sparkSession Spark session for the pipeline
 */
case class RunContext(
  pipelineId: String,
  runId: String,
  startTimestamp: Long,
  sparkSession: SparkSession
) {
  /**
   * Create lineage metadata for this run.
   */
  def createLineageMetadata(
    sourceSystem: String,
    sourceTimestamp: Long
  ): LineageMetadata =
    LineageMetadata(
      sourceSystem = sourceSystem,
      sourceTimestamp = sourceTimestamp,
      extractionTimestamp = System.currentTimeMillis(),
      transformationChain = List.empty,
      pipelineId = pipelineId,
      runId = runId
    )
}

object RunContext {
  /**
   * Create a new run context.
   */
  def create(
    pipelineId: String,
    sparkSession: SparkSession
  ): RunContext = {
    RunContext(
      pipelineId = pipelineId,
      runId = java.util.UUID.randomUUID().toString,
      startTimestamp = System.currentTimeMillis(),
      sparkSession = sparkSession
    )
  }
}
