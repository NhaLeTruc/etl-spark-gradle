package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * MySQL data extractor.
 *
 * Extracts data from MySQL tables using JDBC.
 * Supports credential retrieval from Vault.
 *
 * Required parameters:
 * - url: JDBC connection URL (e.g., jdbc:mysql://localhost:3306/dbname)
 * - table: Table name or SQL query
 *
 * Optional parameters (if not retrieved from Vault):
 * - user: Database username
 * - password: Database password
 * - partitionColumn: Column for parallel reading
 * - numPartitions: Number of parallel readers
 * - fetchsize: JDBC fetch size
 */
class MySQLExtractor(vaultClient: Option[Any] = None) extends DataExtractor {

  override def sourceType: String = "mysql"

  override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    // Validate configuration first
    val validation = validateConfig(config)
    if (!validation.isValid) {
      throw new ExtractionException(
        sourceType = sourceType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val url = config.parameters("url")
    val table = config.parameters("table")

    // Get credentials from Vault or config
    val credentials = getCredentials(config)
    val user = credentials.getOrElse("user", config.parameters.getOrElse("user", ""))
    val password = credentials.getOrElse("password", config.parameters.getOrElse("password", ""))

    try {
      // Build JDBC reader
      var reader = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.mysql.cj.jdbc.Driver")

      // Add optional partitioning parameters
      config.parameters.get("partitionColumn").foreach { partCol =>
        reader = reader.option("partitionColumn", partCol)
        reader = reader.option("numPartitions", config.parameters.getOrElse("numPartitions", "4"))
      }

      config.parameters.get("fetchsize").foreach { size =>
        reader = reader.option("fetchsize", size)
      }

      val df = reader.load()

      // Embed lineage metadata
      embedLineage(df, config)

    } catch {
      case e: Exception =>
        throw new ExtractionException(
          sourceType = sourceType,
          message = s"Failed to extract from MySQL table '$table': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SourceConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("url")) {
      errors += "Missing required parameter: url"
    } else {
      val url = config.parameters("url")
      if (!url.startsWith("jdbc:mysql://")) {
        errors += "Invalid MySQL JDBC URL (must start with 'jdbc:mysql://')"
      }
    }

    if (!config.parameters.contains("table")) {
      errors += "Missing required parameter: table"
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }

  /**
   * Get credentials from Vault or return empty map.
   */
  private def getCredentials(config: SourceConfig): Map[String, String] = {
    vaultClient match {
      case Some(client) =>
        // Use reflection to call getSecret method
        try {
          val method = client.getClass.getMethod("getSecret", classOf[String])
          method.invoke(client, config.credentialsPath).asInstanceOf[Map[String, String]]
        } catch {
          case _: Exception => Map.empty
        }
      case None => Map.empty
    }
  }

  /**
   * Embed lineage metadata into DataFrame.
   */
  private def embedLineage(df: DataFrame, config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    val lineage = LineageMetadata(
      sourceType = sourceType,
      sourceIdentifier = config.parameters("table"),
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
