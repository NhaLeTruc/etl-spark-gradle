package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * S3 data extractor.
 *
 * Extracts data from S3 (or S3-compatible storage) using Spark.
 * Supports multiple formats: Avro, Parquet, JSON, CSV.
 *
 * Required parameters:
 * - path: S3 path (e.g., s3://bucket/prefix/data)
 * - format: File format (avro, parquet, json, csv)
 *
 * Optional parameters:
 * - schema: JSON schema string (for CSV/JSON)
 * - header: "true" | "false" (for CSV, default: "true")
 * - delimiter: Field delimiter (for CSV, default: ",")
 */
class S3Extractor extends DataExtractor {

  override def sourceType: String = "s3"

  private val supportedFormats = Set("avro", "parquet", "json", "csv")

  override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    // Validate configuration first
    val validation = validateConfig(config)
    if (!validation.isValid) {
      throw new ExtractionException(
        sourceType = sourceType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val path = config.parameters("path")
    val format = config.parameters("format")

    try {
      // Build reader based on format
      var reader = spark.read.format(format)

      // Add format-specific options
      format match {
        case "csv" =>
          reader = reader
            .option("header", config.parameters.getOrElse("header", "true"))
            .option("delimiter", config.parameters.getOrElse("delimiter", ","))
            .option("inferSchema", config.parameters.getOrElse("inferSchema", "true"))

        case "json" =>
          // JSON can optionally use multiLine mode
          config.parameters.get("multiLine").foreach { multiLine =>
            reader = reader.option("multiLine", multiLine)
          }

        case _ =>
          // Avro and Parquet have default options
      }

      val df = reader.load(path)

      // Embed lineage metadata
      embedLineage(df, config)

    } catch {
      case e: Exception =>
        throw new ExtractionException(
          sourceType = sourceType,
          message = s"Failed to extract from S3 path '$path': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SourceConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("path")) {
      errors += "Missing required parameter: path"
    }

    if (!config.parameters.contains("format")) {
      errors += "Missing required parameter: format"
    } else {
      val format = config.parameters("format")
      if (!supportedFormats.contains(format)) {
        errors += s"Invalid format: $format (supported: ${supportedFormats.mkString(", ")})"
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
      sourceIdentifier = config.parameters("path"),
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
