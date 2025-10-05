package com.etl.core

/**
 * Exception thrown when data extraction fails.
 *
 * @param sourceType Type of source that failed ("kafka", "postgres", etc.)
 * @param message Error message describing the failure
 * @param cause Root cause exception
 */
case class ExtractionException(
  sourceType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$sourceType] $message", cause) {

  /**
   * Constructor without cause.
   */
  def this(sourceType: String, message: String) = {
    this(sourceType, message, null)
  }
}
