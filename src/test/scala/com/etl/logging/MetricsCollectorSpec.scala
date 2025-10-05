package com.etl.logging

import com.etl.core._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MetricsCollector.
 *
 * Tests cover:
 * - T130: Track extraction metrics
 * - T131: Track transformation metrics
 * - T132: Track load metrics
 */
class MetricsCollectorSpec extends AnyFlatSpec with Matchers {

  // T130: Track extraction metrics
  "MetricsCollector" should "track extraction metrics" in {
    val collector = new MetricsCollector()

    collector.recordExtraction(
      pipelineId = "test-pipeline",
      runId = "run-001",
      sourceType = "kafka",
      recordCount = 1000,
      durationMs = 5000
    )

    val metrics = collector.getExtractionMetrics("test-pipeline", "run-001")
    metrics.isDefined shouldBe true
    metrics.get.recordCount shouldBe 1000
    metrics.get.durationMs shouldBe 5000
    metrics.get.sourceType shouldBe "kafka"
  }

  // T131: Track transformation metrics
  it should "track transformation metrics" in {
    val collector = new MetricsCollector()

    collector.recordTransformation(
      pipelineId = "test-pipeline",
      runId = "run-001",
      transformationType = "filter",
      inputRecords = 1000,
      outputRecords = 750,
      durationMs = 2000
    )

    val metrics = collector.getTransformationMetrics("test-pipeline", "run-001")
    metrics should not be empty
    metrics.head.transformationType shouldBe "filter"
    metrics.head.inputRecords shouldBe 1000
    metrics.head.outputRecords shouldBe 750
  }

  // T132: Track load metrics
  it should "track load metrics" in {
    val collector = new MetricsCollector()

    collector.recordLoad(
      pipelineId = "test-pipeline",
      runId = "run-001",
      sinkType = "postgres",
      recordCount = 750,
      durationMs = 3000,
      writeMode = "append"
    )

    val metrics = collector.getLoadMetrics("test-pipeline", "run-001")
    metrics.isDefined shouldBe true
    metrics.get.recordCount shouldBe 750
    metrics.get.sinkType shouldBe "postgres"
    metrics.get.writeMode shouldBe "append"
  }

  it should "track complete pipeline metrics" in {
    val collector = new MetricsCollector()

    // Track extraction
    collector.recordExtraction("pipeline-1", "run-1", "kafka", 1000, 5000)

    // Track transformations
    collector.recordTransformation("pipeline-1", "run-1", "filter", 1000, 800, 2000)
    collector.recordTransformation("pipeline-1", "run-1", "aggregation", 800, 100, 3000)

    // Track load
    collector.recordLoad("pipeline-1", "run-1", "s3", 100, 1000, "overwrite")

    // Get summary
    val summary = collector.getPipelineSummary("pipeline-1", "run-1")
    summary.totalRecordsExtracted shouldBe 1000
    summary.totalRecordsLoaded shouldBe 100
    summary.transformationCount shouldBe 2
    summary.totalDurationMs shouldBe 11000 // 5000 + 2000 + 3000 + 1000
  }

  it should "calculate throughput metrics" in {
    val collector = new MetricsCollector()

    collector.recordExtraction("pipeline-2", "run-2", "postgres", 10000, 5000)

    val metrics = collector.getExtractionMetrics("pipeline-2", "run-2").get
    val throughput = collector.calculateThroughput(metrics.recordCount, metrics.durationMs)

    throughput shouldBe 2000.0 // 10000 records / 5 seconds
  }

  it should "track multiple pipeline runs" in {
    val collector = new MetricsCollector()

    collector.recordExtraction("pipeline-3", "run-1", "kafka", 1000, 5000)
    collector.recordExtraction("pipeline-3", "run-2", "kafka", 1500, 6000)
    collector.recordExtraction("pipeline-3", "run-3", "kafka", 2000, 7000)

    val allRuns = collector.getAllRuns("pipeline-3")
    allRuns.length shouldBe 3
    allRuns should contain allOf ("run-1", "run-2", "run-3")
  }

  it should "export metrics as JSON" in {
    val collector = new MetricsCollector()

    collector.recordExtraction("pipeline-4", "run-4", "s3", 5000, 10000)
    collector.recordTransformation("pipeline-4", "run-4", "map", 5000, 5000, 3000)
    collector.recordLoad("pipeline-4", "run-4", "mysql", 5000, 8000, "upsert")

    val json = collector.exportAsJson("pipeline-4", "run-4")
    json should include("\"pipelineId\":\"pipeline-4\"")
    json should include("\"runId\":\"run-4\"")
    json should include("extraction")
    json should include("transformations")
    json should include("load")
  }

  it should "calculate data quality metrics" in {
    val collector = new MetricsCollector()

    collector.recordQuality(
      pipelineId = "pipeline-5",
      runId = "run-5",
      totalRecords = 1000,
      validRecords = 950,
      invalidRecords = 50,
      nullViolations = 30,
      duplicates = 20
    )

    val qualityMetrics = collector.getQualityMetrics("pipeline-5", "run-5")
    qualityMetrics.isDefined shouldBe true
    qualityMetrics.get.validRecords shouldBe 950
    qualityMetrics.get.invalidRecords shouldBe 50
    qualityMetrics.get.dataQualityRate shouldBe 0.95 // 950/1000
  }
}
