package com.etl.logging

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for StructuredLogger.
 *
 * Tests cover:
 * - T127: Log with correlation IDs
 * - T128: Format JSON log entries
 */
class StructuredLoggerSpec extends AnyFlatSpec with Matchers {

  // T127: Log with correlation IDs
  "StructuredLogger" should "log with correlation IDs" in {
    val logger = new StructuredLogger("test-logger")

    val correlationId = "corr-12345"
    logger.info("Test message", Map("key" -> "value"), Some(correlationId))

    // Verify correlation ID is included (would check log output in real implementation)
    logger.getLastCorrelationId() shouldBe Some(correlationId)
  }

  // T128: Format JSON log entries
  it should "format log entries as JSON" in {
    val logger = new StructuredLogger("test-logger")

    val jsonLog = logger.formatAsJson(
      level = "INFO",
      message = "Pipeline started",
      context = Map(
        "pipelineId" -> "test-pipeline",
        "runId" -> "run-001",
        "timestamp" -> "2025-01-01T10:00:00Z"
      ),
      correlationId = Some("corr-123")
    )

    jsonLog should include("\"level\":\"INFO\"")
    jsonLog should include("\"message\":\"Pipeline started\"")
    jsonLog should include("\"pipelineId\":\"test-pipeline\"")
    jsonLog should include("\"correlationId\":\"corr-123\"")
  }

  it should "log at different levels" in {
    val logger = new StructuredLogger("test-logger")

    logger.debug("Debug message", Map("debug" -> "true"))
    logger.info("Info message", Map("info" -> "true"))
    logger.warn("Warning message", Map("warn" -> "true"))
    logger.error("Error message", Map("error" -> "true"))

    // Verify logs were created (in real implementation, would check log output)
    logger.getLogCount() should be > 0
  }

  it should "include exception details in error logs" in {
    val logger = new StructuredLogger("test-logger")

    val exception = new RuntimeException("Test exception")
    val jsonLog = logger.formatError(
      message = "Operation failed",
      exception = exception,
      context = Map("operation" -> "extract")
    )

    jsonLog should include("\"level\":\"ERROR\"")
    jsonLog should include("\"message\":\"Operation failed\"")
    jsonLog should include("\"exception\":\"RuntimeException\"")
    jsonLog should include("Test exception")
  }

  it should "support structured context fields" in {
    val logger = new StructuredLogger("test-logger")

    val context = Map(
      "pipelineId" -> "pipeline-123",
      "runId" -> "run-456",
      "phase" -> "extract",
      "recordCount" -> "1000",
      "duration" -> "5.2"
    )

    val jsonLog = logger.formatAsJson(
      level = "INFO",
      message = "Extraction completed",
      context = context,
      correlationId = None
    )

    jsonLog should include("\"pipelineId\":\"pipeline-123\"")
    jsonLog should include("\"phase\":\"extract\"")
    jsonLog should include("\"recordCount\":\"1000\"")
  }

  it should "generate correlation IDs automatically" in {
    val logger = new StructuredLogger("test-logger")

    val correlationId1 = logger.generateCorrelationId()
    val correlationId2 = logger.generateCorrelationId()

    correlationId1 should not be empty
    correlationId2 should not be empty
    correlationId1 should not equal correlationId2
  }
}
