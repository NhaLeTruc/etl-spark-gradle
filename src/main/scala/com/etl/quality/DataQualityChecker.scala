package com.etl.quality

import com.etl.core.QualityConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Data quality checker.
 *
 * Calculates quality metrics and identifies data issues.
 */
class DataQualityChecker {

  /**
   * Quality metrics result.
   */
  case class QualityMetrics(
    totalRecords: Long,
    nullViolations: Long,
    nullRate: Double,
    duplicateRecords: Long,
    duplicateRate: Double
  )

  /**
   * Check data quality against configuration.
   *
   * @param data DataFrame to check
   * @param config Quality configuration
   * @return Quality metrics
   */
  def checkQuality(data: DataFrame, config: QualityConfig): QualityMetrics = {
    val totalRecords = data.count()

    // Calculate null violations
    val (nullViolations, nullRate) = if (config.nullChecks.nonEmpty) {
      calculateNullMetrics(data, config.nullChecks, totalRecords)
    } else {
      (0L, 0.0)
    }

    // Calculate duplicate rate
    val (duplicateRecords, duplicateRate) = if (config.duplicateCheck) {
      calculateDuplicateMetrics(data, totalRecords)
    } else {
      (0L, 0.0)
    }

    QualityMetrics(
      totalRecords = totalRecords,
      nullViolations = nullViolations,
      nullRate = nullRate,
      duplicateRecords = duplicateRecords,
      duplicateRate = duplicateRate
    )
  }

  /**
   * Calculate null metrics.
   */
  private def calculateNullMetrics(
    data: DataFrame,
    nullCheckColumns: List[String],
    totalRecords: Long
  ): (Long, Double) = {
    val validColumns = nullCheckColumns.filter(data.columns.contains)

    if (validColumns.isEmpty) {
      return (0L, 0.0)
    }

    // Count total nulls across all checked columns
    val nullCounts = validColumns.map { colName =>
      data.filter(col(colName).isNull).count()
    }

    val totalNulls = nullCounts.sum
    val totalCells = totalRecords * validColumns.length
    val nullRate = if (totalCells > 0) totalNulls.toDouble / totalCells.toDouble else 0.0

    (totalNulls, nullRate)
  }

  /**
   * Calculate duplicate metrics.
   */
  private def calculateDuplicateMetrics(
    data: DataFrame,
    totalRecords: Long
  ): (Long, Double) = {
    val uniqueRecords = data.distinct().count()
    val duplicateRecords = totalRecords - uniqueRecords
    val duplicateRate = if (totalRecords > 0) duplicateRecords.toDouble / totalRecords.toDouble else 0.0

    (duplicateRecords, duplicateRate)
  }

  /**
   * Get null rates for individual columns.
   *
   * @param data DataFrame to analyze
   * @param columns Columns to check
   * @return Map of column name to null rate
   */
  def getColumnNullRates(data: DataFrame, columns: List[String]): Map[String, Double] = {
    val totalRecords = data.count()
    val validColumns = columns.filter(data.columns.contains)

    validColumns.map { colName =>
      val nullCount = data.filter(col(colName).isNull).count()
      val nullRate = if (totalRecords > 0) nullCount.toDouble / totalRecords.toDouble else 0.0
      colName -> nullRate
    }.toMap
  }

  /**
   * Get quality report as formatted string.
   *
   * @param metrics Quality metrics
   * @return Human-readable report
   */
  def getReport(metrics: QualityMetrics): String = {
    s"""Data Quality Report:
       |  Total Records: ${metrics.totalRecords}
       |  Null Violations: ${metrics.nullViolations}
       |  Null Rate: ${(metrics.nullRate * 100).formatted("%.2f")}%
       |  Duplicate Records: ${metrics.duplicateRecords}
       |  Duplicate Rate: ${(metrics.duplicateRate * 100).formatted("%.2f")}%
       |""".stripMargin
  }

  /**
   * Filter out invalid records based on quality checks.
   *
   * @param data DataFrame to filter
   * @param config Quality configuration
   * @return Tuple of (valid records, invalid records)
   */
  def splitValidInvalid(data: DataFrame, config: QualityConfig): (DataFrame, DataFrame) = {
    var validCondition = lit(true)

    // Add null checks
    config.nullChecks.foreach { colName =>
      if (data.columns.contains(colName)) {
        validCondition = validCondition && col(colName).isNotNull
      }
    }

    val validRecords = data.filter(validCondition)
    val invalidRecords = data.filter(!validCondition)

    (validRecords, invalidRecords)
  }
}
