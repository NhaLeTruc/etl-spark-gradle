package com.etl.core

/**
 * Result of a load operation.
 *
 * @param recordsWritten Number of records successfully written
 * @param recordsFailed Number of records that failed to write
 * @param success Whether the load operation succeeded overall
 * @param errorMessage Error message if load failed
 */
case class LoadResult(
  recordsWritten: Long,
  recordsFailed: Long,
  success: Boolean,
  errorMessage: Option[String] = None
)

object LoadResult {
  /**
   * Create a successful load result.
   */
  def successful(recordsWritten: Long): LoadResult =
    LoadResult(
      recordsWritten = recordsWritten,
      recordsFailed = 0,
      success = true
    )

  /**
   * Create a failed load result.
   */
  def failed(errorMessage: String): LoadResult =
    LoadResult(
      recordsWritten = 0,
      recordsFailed = 0,
      success = false,
      errorMessage = Some(errorMessage)
    )

  /**
   * Create a partial success result.
   */
  def partial(recordsWritten: Long, recordsFailed: Long): LoadResult =
    LoadResult(
      recordsWritten = recordsWritten,
      recordsFailed = recordsFailed,
      success = recordsWritten > 0
    )
}
