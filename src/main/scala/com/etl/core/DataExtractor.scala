package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataExtractor abstracts data extraction from various sources.
 *
 * Implementations must:
 * - Extract data from configured source
 * - Return DataFrame with Avro-compatible schema
 * - Embed lineage metadata in DataFrame
 * - Validate configuration before extraction
 * - Handle errors gracefully
 *
 * Implementations: KafkaExtractor, PostgresExtractor, MySQLExtractor, S3Extractor
 */
trait DataExtractor {

  /**
   * Extract data from configured source.
   *
   * @param config Source-specific configuration (credentials, parameters)
   * @param spark Implicit Spark session for DataFrame creation
   * @return DataFrame with Avro-compatible schema and embedded lineage metadata
   * @throws ExtractionException if extraction fails
   */
  def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame

  /**
   * Validate source configuration before extraction.
   *
   * Implementations should check for:
   * - Required parameters present
   * - Parameter values valid (e.g., valid URLs, paths)
   * - Credentials path specified
   *
   * @param config Source configuration to validate
   * @return ValidationResult with errors if invalid
   */
  def validateConfig(config: SourceConfig): ValidationResult

  /**
   * Source type identifier (matches SourceConfig.type).
   *
   * Used for:
   * - Registry lookup
   * - Lineage metadata
   * - Error messages
   *
   * @return Source type string ("kafka" | "postgres" | "mysql" | "s3")
   */
  def sourceType: String
}
