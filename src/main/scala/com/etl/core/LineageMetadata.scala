package com.etl.core

/**
 * Data lineage tracking metadata (embedded in Avro records).
 *
 * @param sourceSystem Source type (e.g., "postgres", "kafka")
 * @param sourceTimestamp Original record timestamp
 * @param extractionTimestamp When extracted
 * @param transformationChain Ordered list of transformation types applied
 * @param pipelineId Pipeline identifier
 * @param runId Run UUID
 */
case class LineageMetadata(
  sourceSystem: String,
  sourceTimestamp: Long,
  extractionTimestamp: Long,
  transformationChain: List[String],
  pipelineId: String,
  runId: String
) {
  /**
   * Add a transformation to the lineage chain.
   */
  def withTransformation(transformationType: String): LineageMetadata =
    copy(transformationChain = transformationChain :+ transformationType)
}
