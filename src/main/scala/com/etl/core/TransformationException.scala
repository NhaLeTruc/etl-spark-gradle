package com.etl.core

/**
 * Exception thrown when data transformation fails.
 *
 * @param transformationType Type of transformation that failed ("aggregation", "join", etc.)
 * @param message Error message describing the failure
 * @param cause Root cause exception
 */
case class TransformationException(
  transformationType: String,
  message: String,
  cause: Throwable
) extends RuntimeException(s"[$transformationType] $message", cause) {

  /**
   * Constructor without cause.
   */
  def this(transformationType: String, message: String) = {
    this(transformationType, message, null)
  }
}
