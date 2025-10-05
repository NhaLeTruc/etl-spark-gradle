package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Kafka data extractor.
 *
 * Extracts data from Kafka topics using Spark's Kafka integration.
 * Supports batch reading from earliest/latest offsets.
 *
 * Required parameters:
 * - bootstrap.servers: Kafka broker addresses
 * - topic: Topic name to read from
 *
 * Optional parameters:
 * - startingOffsets: "earliest" | "latest" (default: "latest")
 * - maxOffsetsPerTrigger: Rate limit (records per trigger)
 */
class KafkaExtractor extends DataExtractor {

  override def sourceType: String = "kafka"

  override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    // Validate configuration first
    val validation = validateConfig(config)
    if (!validation.isValid) {
      throw new ExtractionException(
        sourceType = sourceType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val bootstrapServers = config.parameters("bootstrap.servers")
    val topic = config.parameters("topic")
    val startingOffsets = config.parameters.getOrElse("startingOffsets", "latest")

    try {
      // Read from Kafka (batch mode)
      val df = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", startingOffsets)
        .load()

      // Cast key and value to strings for easier processing
      val processedDf = df
        .selectExpr(
          "CAST(key AS STRING) as key",
          "CAST(value AS STRING) as value",
          "topic",
          "partition",
          "offset",
          "timestamp"
        )

      // Embed lineage metadata
      embedLineage(processedDf, config)

    } catch {
      case e: Exception =>
        throw new ExtractionException(
          sourceType = sourceType,
          message = s"Failed to extract from Kafka topic '$topic': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SourceConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("bootstrap.servers")) {
      errors += "Missing required parameter: bootstrap.servers"
    }

    if (!config.parameters.contains("topic")) {
      errors += "Missing required parameter: topic"
    }

    // Validate startingOffsets if provided
    config.parameters.get("startingOffsets").foreach { offsets =>
      if (!Set("earliest", "latest").contains(offsets)) {
        errors += s"Invalid startingOffsets value: $offsets (must be 'earliest' or 'latest')"
      }
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }

  /**
   * Embed lineage metadata into DataFrame.
   */
  private def embedLineage(df: DataFrame, config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    val lineage = LineageMetadata(
      sourceType = sourceType,
      sourceIdentifier = config.parameters("topic"),
      extractionTimestamp = System.currentTimeMillis(),
      transformationChain = List.empty
    )

    // Serialize lineage to JSON
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val lineageJson = mapper.writeValueAsString(lineage)

    // Add lineage as a column
    df.withColumn("_lineage", lit(lineageJson))
  }
}
