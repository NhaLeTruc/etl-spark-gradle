package com.etl.logging

import org.slf4j.LoggerFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util.UUID

/**
 * Structured logger with JSON formatting and correlation ID support.
 *
 * Provides structured logging for observability and debugging.
 */
class StructuredLogger(loggerName: String) {

  private val logger = LoggerFactory.getLogger(loggerName)
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // For testing purposes
  private var lastCorrelationId: Option[String] = None
  private var logCount: Int = 0

  /**
   * Log debug message with structured context.
   */
  def debug(message: String, context: Map[String, String] = Map.empty, correlationId: Option[String] = None): Unit = {
    val jsonLog = formatAsJson("DEBUG", message, context, correlationId)
    logger.debug(jsonLog)
    updateTestState(correlationId)
  }

  /**
   * Log info message with structured context.
   */
  def info(message: String, context: Map[String, String] = Map.empty, correlationId: Option[String] = None): Unit = {
    val jsonLog = formatAsJson("INFO", message, context, correlationId)
    logger.info(jsonLog)
    updateTestState(correlationId)
  }

  /**
   * Log warning message with structured context.
   */
  def warn(message: String, context: Map[String, String] = Map.empty, correlationId: Option[String] = None): Unit = {
    val jsonLog = formatAsJson("WARN", message, context, correlationId)
    logger.warn(jsonLog)
    updateTestState(correlationId)
  }

  /**
   * Log error message with structured context.
   */
  def error(message: String, context: Map[String, String] = Map.empty, correlationId: Option[String] = None): Unit = {
    val jsonLog = formatAsJson("ERROR", message, context, correlationId)
    logger.error(jsonLog)
    updateTestState(correlationId)
  }

  /**
   * Log error with exception details.
   */
  def error(message: String, exception: Throwable, context: Map[String, String] = Map.empty, correlationId: Option[String] = None): Unit = {
    val jsonLog = formatError(message, exception, context, correlationId)
    logger.error(jsonLog, exception)
    updateTestState(correlationId)
  }

  /**
   * Format log entry as JSON.
   */
  def formatAsJson(
    level: String,
    message: String,
    context: Map[String, String],
    correlationId: Option[String]
  ): String = {
    val logEntry = scala.collection.mutable.Map[String, Any](
      "timestamp" -> System.currentTimeMillis(),
      "level" -> level,
      "message" -> message,
      "logger" -> loggerName
    )

    // Add correlation ID if present
    correlationId.foreach { id =>
      logEntry("correlationId") = id
    }

    // Add all context fields
    context.foreach { case (key, value) =>
      logEntry(key) = value
    }

    mapper.writeValueAsString(logEntry.toMap)
  }

  /**
   * Format error log with exception details.
   */
  def formatError(
    message: String,
    exception: Throwable,
    context: Map[String, String] = Map.empty,
    correlationId: Option[String] = None
  ): String = {
    val exceptionContext = context ++ Map(
      "exception" -> exception.getClass.getSimpleName,
      "exceptionMessage" -> exception.getMessage,
      "stackTrace" -> exception.getStackTrace.take(5).mkString("\n")
    )

    formatAsJson("ERROR", message, exceptionContext, correlationId)
  }

  /**
   * Generate unique correlation ID.
   */
  def generateCorrelationId(): String = {
    UUID.randomUUID().toString
  }

  // Test helper methods
  private def updateTestState(correlationId: Option[String]): Unit = {
    lastCorrelationId = correlationId
    logCount += 1
  }

  def getLastCorrelationId(): Option[String] = lastCorrelationId
  def getLogCount(): Int = logCount
}

/**
 * Structured logger factory.
 */
object StructuredLogger {
  /**
   * Create structured logger for a class.
   */
  def apply(clazz: Class[_]): StructuredLogger = {
    new StructuredLogger(clazz.getName)
  }

  /**
   * Create structured logger with custom name.
   */
  def apply(name: String): StructuredLogger = {
    new StructuredLogger(name)
  }
}
