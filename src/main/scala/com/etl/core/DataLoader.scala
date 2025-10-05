package com.etl.core

import org.apache.spark.sql.DataFrame

/**
 * DataLoader abstracts data loading to various sinks.
 *
 * Implementations must:
 * - Load data to configured sink
 * - Return LoadResult with success/failure status
 * - Validate configuration before loading
 * - Handle errors gracefully
 * - Support multiple write modes (append, overwrite, upsert)
 *
 * Implementations: KafkaLoader, PostgresLoader, MySQLLoader, S3Loader
 */
trait DataLoader {

  /**
   * Load data to configured sink.
   *
   * @param data DataFrame to load
   * @param config Sink-specific configuration (credentials, parameters)
   * @param runContext Pipeline execution context (pipelineId, runId, etc.)
   * @return LoadResult with success status and recordsWritten count
   * @throws LoadException if load fails
   */
  def load(
    data: DataFrame,
    config: SinkConfig,
    runContext: RunContext
  ): LoadResult

  /**
   * Validate sink configuration before loading.
   *
   * Implementations should check for:
   * - Required parameters present
   * - Parameter values valid (e.g., valid URLs, paths)
   * - Credentials path specified
   * - Write mode supported
   *
   * @param config Sink configuration to validate
   * @return ValidationResult with errors if invalid
   */
  def validateConfig(config: SinkConfig): ValidationResult

  /**
   * Sink type identifier (matches SinkConfig.type).
   *
   * Used for:
   * - Registry lookup
   * - Lineage metadata
   * - Error messages
   *
   * @return Sink type string ("kafka" | "postgres" | "mysql" | "s3")
   */
  def sinkType: String
}
