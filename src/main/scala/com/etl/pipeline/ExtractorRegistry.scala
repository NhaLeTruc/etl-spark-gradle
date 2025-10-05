package com.etl.pipeline

import com.etl.core.DataExtractor
import com.etl.extractor._

/**
 * Registry for data extractors.
 *
 * Maps source types to extractor implementations.
 */
class ExtractorRegistry {

  private var extractors: Map[String, DataExtractor] = Map(
    "kafka" -> new KafkaExtractor(),
    "postgres" -> new PostgresExtractor(),
    "mysql" -> new MySQLExtractor(),
    "s3" -> new S3Extractor()
  )

  /**
   * Get extractor by source type.
   *
   * @param sourceType Source type identifier
   * @return Data extractor instance
   * @throws IllegalArgumentException if source type is not registered
   */
  def get(sourceType: String): DataExtractor = {
    extractors.getOrElse(
      sourceType,
      throw new IllegalArgumentException(s"Unknown source type: $sourceType")
    )
  }

  /**
   * Register custom extractor.
   *
   * @param sourceType Source type identifier
   * @param extractor Extractor implementation
   */
  def register(sourceType: String, extractor: DataExtractor): Unit = {
    extractors = extractors + (sourceType -> extractor)
  }

  /**
   * List all registered source types.
   *
   * @return List of registered source types
   */
  def list(): List[String] = {
    extractors.keys.toList
  }
}
