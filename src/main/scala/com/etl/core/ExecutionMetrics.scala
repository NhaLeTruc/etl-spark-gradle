package com.etl.core

/**
 * Runtime metrics for a pipeline execution.
 *
 * @param pipelineId Pipeline identifier
 * @param runId Unique run UUID
 * @param startTimestamp Start time (epoch millis)
 * @param endTimestamp End time (epoch millis)
 * @param recordsExtracted Records read from source
 * @param recordsTransformed Records after transformations
 * @param recordsLoaded Records written to sink
 * @param recordsFailed Records quarantined
 * @param status Execution status ("success" | "failed" | "partial")
 * @param errorDetails Error message if failed
 */
case class ExecutionMetrics(
  pipelineId: String,
  runId: String,
  startTimestamp: Long,
  endTimestamp: Long,
  recordsExtracted: Long,
  recordsTransformed: Long,
  recordsLoaded: Long,
  recordsFailed: Long,
  status: String,
  errorDetails: Option[String] = None
) {
  /**
   * Duration in milliseconds.
   */
  def duration: Long = endTimestamp - startTimestamp

  /**
   * Duration in seconds.
   */
  def durationSeconds: Double = duration / 1000.0
}
