package com.etl.quality

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Quarantine writer for invalid/failed records.
 *
 * Writes failed records with diagnostic metadata for debugging and reprocessing.
 */
class QuarantineWriter {

  /**
   * Write invalid records to quarantine storage.
   *
   * Adds metadata columns:
   * - quarantine_timestamp: When the record was quarantined
   * - pipeline_id: Pipeline that quarantined the record
   * - run_id: Specific run that quarantined the record
   *
   * @param data Invalid records to quarantine
   * @param path Quarantine storage path
   * @param pipelineId Pipeline identifier
   * @param runId Run identifier
   */
  def writeQuarantine(
    data: DataFrame,
    path: String,
    pipelineId: String,
    runId: String
  )(implicit spark: SparkSession): Unit = {
    // Add quarantine metadata
    val quarantined = data
      .withColumn("quarantine_timestamp", lit(System.currentTimeMillis()))
      .withColumn("pipeline_id", lit(pipelineId))
      .withColumn("run_id", lit(runId))

    // Write to quarantine (append mode to accumulate failed records)
    quarantined.write
      .format("parquet")
      .mode(SaveMode.Append)
      .save(path)
  }

  /**
   * Write invalid records with custom error details.
   *
   * @param data Invalid records
   * @param path Quarantine storage path
   * @param pipelineId Pipeline identifier
   * @param runId Run identifier
   * @param errorColumn Column name containing error details
   */
  def writeQuarantineWithErrors(
    data: DataFrame,
    path: String,
    pipelineId: String,
    runId: String,
    errorColumn: String = "error_details"
  )(implicit spark: SparkSession): Unit = {
    // Ensure error column exists
    val withError = if (data.columns.contains(errorColumn)) {
      data
    } else {
      data.withColumn(errorColumn, lit("Unknown error"))
    }

    writeQuarantine(withError, path, pipelineId, runId)
  }

  /**
   * Read quarantined records for analysis or reprocessing.
   *
   * @param path Quarantine storage path
   * @param pipelineId Optional filter by pipeline
   * @param runId Optional filter by run
   * @return DataFrame of quarantined records
   */
  def readQuarantine(
    path: String,
    pipelineId: Option[String] = None,
    runId: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {
    var quarantined = spark.read.format("parquet").load(path)

    // Apply filters if specified
    pipelineId.foreach { pid =>
      quarantined = quarantined.filter(col("pipeline_id") === pid)
    }

    runId.foreach { rid =>
      quarantined = quarantined.filter(col("run_id") === rid)
    }

    quarantined
  }
}
