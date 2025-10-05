package com.etl.core

/**
 * Configuration for a data source.
 *
 * @param `type` Source type ("kafka" | "postgres" | "mysql" | "s3")
 * @param credentialsPath Vault path for credentials (e.g., "secret/postgres/prod")
 * @param parameters Type-specific parameters (e.g., jdbcUrl, topic, bucket)
 */
case class SourceConfig(
  `type`: String,
  credentialsPath: String,
  parameters: Map[String, String]
)
