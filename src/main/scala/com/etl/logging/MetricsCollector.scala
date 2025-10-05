package com.etl.logging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Metrics collector for pipeline observability.
 *
 * Tracks extraction, transformation, and load metrics.
 */
class MetricsCollector {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // In-memory storage (in production, would write to metrics system)
  private var extractionMetrics = Map.empty[(String, String), ExtractionMetrics]
  private var transformationMetrics = Map.empty[(String, String), List[TransformationMetrics]]
  private var loadMetrics = Map.empty[(String, String), LoadMetrics]
  private var qualityMetrics = Map.empty[(String, String), QualityMetrics]

  /**
   * Extraction metrics.
   */
  case class ExtractionMetrics(
    sourceType: String,
    recordCount: Long,
    durationMs: Long
  )

  /**
   * Transformation metrics.
   */
  case class TransformationMetrics(
    transformationType: String,
    inputRecords: Long,
    outputRecords: Long,
    durationMs: Long
  )

  /**
   * Load metrics.
   */
  case class LoadMetrics(
    sinkType: String,
    recordCount: Long,
    durationMs: Long,
    writeMode: String
  )

  /**
   * Quality metrics.
   */
  case class QualityMetrics(
    totalRecords: Long,
    validRecords: Long,
    invalidRecords: Long,
    nullViolations: Long,
    duplicates: Long
  ) {
    def dataQualityRate: Double = {
      if (totalRecords > 0) validRecords.toDouble / totalRecords.toDouble else 0.0
    }
  }

  /**
   * Pipeline summary metrics.
   */
  case class PipelineSummary(
    totalRecordsExtracted: Long,
    totalRecordsLoaded: Long,
    transformationCount: Int,
    totalDurationMs: Long
  )

  /**
   * Record extraction metrics.
   */
  def recordExtraction(
    pipelineId: String,
    runId: String,
    sourceType: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    extractionMetrics = extractionMetrics + ((pipelineId, runId) -> ExtractionMetrics(sourceType, recordCount, durationMs))
  }

  /**
   * Record transformation metrics.
   */
  def recordTransformation(
    pipelineId: String,
    runId: String,
    transformationType: String,
    inputRecords: Long,
    outputRecords: Long,
    durationMs: Long
  ): Unit = {
    val key = (pipelineId, runId)
    val existing = transformationMetrics.getOrElse(key, List.empty)
    val newMetric = TransformationMetrics(transformationType, inputRecords, outputRecords, durationMs)
    transformationMetrics = transformationMetrics + (key -> (existing :+ newMetric))
  }

  /**
   * Record load metrics.
   */
  def recordLoad(
    pipelineId: String,
    runId: String,
    sinkType: String,
    recordCount: Long,
    durationMs: Long,
    writeMode: String
  ): Unit = {
    loadMetrics = loadMetrics + ((pipelineId, runId) -> LoadMetrics(sinkType, recordCount, durationMs, writeMode))
  }

  /**
   * Record quality metrics.
   */
  def recordQuality(
    pipelineId: String,
    runId: String,
    totalRecords: Long,
    validRecords: Long,
    invalidRecords: Long,
    nullViolations: Long,
    duplicates: Long
  ): Unit = {
    qualityMetrics = qualityMetrics + ((pipelineId, runId) -> QualityMetrics(
      totalRecords, validRecords, invalidRecords, nullViolations, duplicates
    ))
  }

  /**
   * Get extraction metrics.
   */
  def getExtractionMetrics(pipelineId: String, runId: String): Option[ExtractionMetrics] = {
    extractionMetrics.get((pipelineId, runId))
  }

  /**
   * Get transformation metrics.
   */
  def getTransformationMetrics(pipelineId: String, runId: String): List[TransformationMetrics] = {
    transformationMetrics.getOrElse((pipelineId, runId), List.empty)
  }

  /**
   * Get load metrics.
   */
  def getLoadMetrics(pipelineId: String, runId: String): Option[LoadMetrics] = {
    loadMetrics.get((pipelineId, runId))
  }

  /**
   * Get quality metrics.
   */
  def getQualityMetrics(pipelineId: String, runId: String): Option[QualityMetrics] = {
    qualityMetrics.get((pipelineId, runId))
  }

  /**
   * Get pipeline summary.
   */
  def getPipelineSummary(pipelineId: String, runId: String): PipelineSummary = {
    val extraction = getExtractionMetrics(pipelineId, runId)
    val transformations = getTransformationMetrics(pipelineId, runId)
    val load = getLoadMetrics(pipelineId, runId)

    val totalExtracted = extraction.map(_.recordCount).getOrElse(0L)
    val totalLoaded = load.map(_.recordCount).getOrElse(0L)
    val transformCount = transformations.length

    val totalDuration = extraction.map(_.durationMs).getOrElse(0L) +
      transformations.map(_.durationMs).sum +
      load.map(_.durationMs).getOrElse(0L)

    PipelineSummary(totalExtracted, totalLoaded, transformCount, totalDuration)
  }

  /**
   * Calculate throughput (records per second).
   */
  def calculateThroughput(recordCount: Long, durationMs: Long): Double = {
    if (durationMs > 0) {
      (recordCount.toDouble / durationMs.toDouble) * 1000.0
    } else {
      0.0
    }
  }

  /**
   * Get all runs for a pipeline.
   */
  def getAllRuns(pipelineId: String): List[String] = {
    extractionMetrics.keys.filter(_._1 == pipelineId).map(_._2).toList.distinct
  }

  /**
   * Export metrics as JSON.
   */
  def exportAsJson(pipelineId: String, runId: String): String = {
    val metricsMap = Map(
      "pipelineId" -> pipelineId,
      "runId" -> runId,
      "extraction" -> extractionMetrics.get((pipelineId, runId)),
      "transformations" -> transformationMetrics.get((pipelineId, runId)),
      "load" -> loadMetrics.get((pipelineId, runId)),
      "quality" -> qualityMetrics.get((pipelineId, runId)),
      "summary" -> getPipelineSummary(pipelineId, runId)
    )

    mapper.writeValueAsString(metricsMap)
  }
}
