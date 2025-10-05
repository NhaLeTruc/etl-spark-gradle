package com.etl.core

/**
 * Configuration for a data sink.
 *
 * @param `type` Sink type ("kafka" | "postgres" | "mysql" | "s3")
 * @param credentialsPath Vault path for credentials
 * @param parameters Type-specific parameters (e.g., jdbcUrl, topic, bucket)
 * @param writeMode Write mode ("append" | "overwrite" | "upsert")
 */
case class SinkConfig(
  `type`: String,
  credentialsPath: String,
  parameters: Map[String, String],
  writeMode: String
)
