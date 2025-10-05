package com.etl.core

/**
 * Exception thrown when data loading fails.
 *
 * @param sinkType Type of sink that failed ("kafka", "postgres", "mysql", "s3")
 * @param message Error message describing the failure
 * @param cause Root cause exception
 */
case class LoadException(
  sinkType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$sinkType] $message", cause) {

  /**
   * Constructor without cause.
   */
  def this(sinkType: String, message: String) = {
    this(sinkType, message, null)
  }
}
