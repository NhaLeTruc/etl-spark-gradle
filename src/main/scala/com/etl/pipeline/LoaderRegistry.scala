package com.etl.pipeline

import com.etl.core.DataLoader
import com.etl.loader._

/**
 * Registry for data loaders.
 *
 * Maps sink types to loader implementations.
 */
class LoaderRegistry {

  private var loaders: Map[String, DataLoader] = Map(
    "kafka" -> new KafkaLoader(),
    "postgres" -> new PostgresLoader(),
    "mysql" -> new MySQLLoader(),
    "s3" -> new S3Loader()
  )

  /**
   * Get loader by sink type.
   *
   * @param sinkType Sink type identifier
   * @return Data loader instance
   * @throws IllegalArgumentException if sink type is not registered
   */
  def get(sinkType: String): DataLoader = {
    loaders.getOrElse(
      sinkType,
      throw new IllegalArgumentException(s"Unknown sink type: $sinkType")
    )
  }

  /**
   * Register custom loader.
   *
   * @param sinkType Sink type identifier
   * @param loader Loader implementation
   */
  def register(sinkType: String, loader: DataLoader): Unit = {
    loaders = loaders + (sinkType -> loader)
  }

  /**
   * List all registered sink types.
   *
   * @return List of registered sink types
   */
  def list(): List[String] = {
    loaders.keys.toList
  }
}
