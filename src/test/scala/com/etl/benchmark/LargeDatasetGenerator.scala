package com.etl.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.Path

/**
 * Utility for generating large test datasets for performance benchmarking.
 *
 * Supports:
 * - Generating datasets of configurable size (MB, GB, TB)
 * - Various data formats (Parquet, Avro, CSV, JSON)
 * - Realistic data distributions
 * - Partitioned output for scalability testing
 */
object LargeDatasetGenerator {

  /**
   * Generate a large sales dataset.
   *
   * @param targetSizeGB Target dataset size in gigabytes
   * @param spark Spark session
   * @return DataFrame with generated sales data
   */
  def generateSalesDataset(targetSizeGB: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Estimate records needed for target size
    // Average row size ~200 bytes (with all columns)
    val avgRowSizeBytes = 200
    val targetBytes = targetSizeGB.toLong * 1024L * 1024L * 1024L
    val estimatedRows = targetBytes / avgRowSizeBytes

    println(s"Generating dataset with ~$estimatedRows rows (target: ${targetSizeGB}GB)")

    // Generate data in chunks to avoid memory issues
    val chunkSize = 10000000 // 10M rows per chunk
    val numChunks = Math.ceil(estimatedRows.toDouble / chunkSize).toInt

    val chunks = (0 until numChunks).map { chunkId =>
      val startId = chunkId.toLong * chunkSize
      val endId = Math.min(startId + chunkSize, estimatedRows)

      spark.range(startId, endId)
        .select(
          $"id".alias("sale_id"),
          concat(lit("ORD-"), $"id").alias("order_id"),
          concat(lit("CUST-"), ($"id" % 100000)).alias("customer_id"),
          concat(lit("PROD-"), ($"id" % 10000)).alias("product_id"),
          (($"id" % 10) + 1).cast("int").alias("quantity"),
          (10.0 + (($"id" % 1000) / 10.0)).cast("decimal(10,2)").alias("price"),
          when($"id" % 5 === 0, "Electronics")
            .when($"id" % 5 === 1, "Clothing")
            .when($"id" % 5 === 2, "Books")
            .when($"id" % 5 === 3, "Home")
            .otherwise("Sports").alias("category"),
          date_add(lit("2024-01-01"), ($"id" % 365).cast("int")).alias("sale_date"),
          from_unixtime(unix_timestamp(lit("2024-01-01 00:00:00")) + ($"id" % 86400))
            .cast("timestamp").alias("sale_timestamp"),
          when($"id" % 10 < 8, "completed")
            .when($"id" % 10 === 8, "pending")
            .otherwise("cancelled").alias("status"),
          concat(lit("Store-"), ($"id" % 500)).alias("store_id"),
          concat(lit("Region-"), ($"id" % 50)).alias("region")
        )
    }

    // Union all chunks
    chunks.reduce(_ union _)
  }

  /**
   * Generate a streaming events dataset (for micro-batch testing).
   *
   * @param numEvents Number of events to generate
   * @param spark Spark session
   * @return DataFrame with generated event data
   */
  def generateStreamingEvents(numEvents: Long)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.range(0, numEvents)
      .select(
        $"id".alias("event_id"),
        (unix_timestamp(lit("2024-01-01 00:00:00")) + $"id").alias("event_timestamp"),
        when($"id" % 3 === 0, "click")
          .when($"id" % 3 === 1, "view")
          .otherwise("purchase").alias("event_type"),
        concat(lit("USER-"), ($"id" % 10000)).alias("user_id"),
        concat(lit("SESSION-"), ($"id" % 50000)).alias("session_id"),
        (10.0 + (($"id" % 100) / 2.0)).cast("double").alias("event_value"),
        map(
          lit("browser"), when($"id" % 4 === 0, "Chrome").when($"id" % 4 === 1, "Firefox").when($"id" % 4 === 2, "Safari").otherwise("Edge"),
          lit("platform"), when($"id" % 2 === 0, "desktop").otherwise("mobile"),
          lit("country"), when($"id" % 5 === 0, "US").when($"id" % 5 === 1, "UK").when($"id" % 5 === 2, "CA").when($"id" % 5 === 3, "AU").otherwise("DE")
        ).alias("properties")
      )
  }

  /**
   * Generate a customer dimension dataset.
   *
   * @param numCustomers Number of customers to generate
   * @param spark Spark session
   * @return DataFrame with generated customer data
   */
  def generateCustomers(numCustomers: Long)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.range(0, numCustomers)
      .select(
        concat(lit("CUST-"), $"id").alias("customer_id"),
        concat(lit("FirstName"), $"id").alias("first_name"),
        concat(lit("LastName"), $"id").alias("last_name"),
        concat(lit("user"), $"id", lit("@example.com")).alias("email"),
        concat(lit("+1-555-"), lpad(($"id" % 10000).cast("string"), 4, "0")).alias("phone"),
        when($"id" % 50 === 0, "NY")
          .when($"id" % 50 === 1, "CA")
          .when($"id" % 50 === 2, "TX")
          .when($"id" % 50 === 3, "FL")
          .otherwise("OTHER").alias("state"),
        when($"id" % 3 === 0, "Premium")
          .when($"id" % 3 === 1, "Standard")
          .otherwise("Basic").alias("tier"),
        date_add(lit("2020-01-01"), ($"id" % 1460).cast("int")).alias("join_date"),
        ($"id" % 2 === 0).alias("is_active")
      )
  }

  /**
   * Write dataset to disk in specified format.
   *
   * @param df DataFrame to write
   * @param path Output path
   * @param format Output format (parquet, avro, json, csv)
   * @param partitionBy Optional partition columns
   */
  def writeDataset(
    df: DataFrame,
    path: String,
    format: String = "parquet",
    partitionBy: Option[Seq[String]] = None
  ): Unit = {
    var writer = df.write.format(format).mode("overwrite")

    partitionBy.foreach { cols =>
      writer = writer.partitionBy(cols: _*)
    }

    println(s"Writing dataset to $path in $format format...")
    val startTime = System.currentTimeMillis()

    writer.save(path)

    val duration = System.currentTimeMillis() - startTime
    println(s"Dataset written in ${duration}ms (${duration / 1000.0}s)")
  }

  /**
   * Calculate actual dataset size on disk.
   *
   * @param path Path to dataset
   * @return Size in bytes
   */
  def calculateDatasetSize(path: Path): Long = {
    import scala.jdk.CollectionConverters._
    import java.nio.file.Files

    if (!Files.exists(path)) {
      0L
    } else {
      Files.walk(path)
        .iterator()
        .asScala
        .filter(p => Files.isRegularFile(p))
        .map(p => Files.size(p))
        .sum
    }
  }

  /**
   * Format bytes as human-readable string.
   */
  def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"${bytes}B"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2fKB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2fMB"
    else f"${bytes / (1024.0 * 1024 * 1024)}%.2fGB"
  }

  /**
   * Main method for standalone dataset generation.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Large Dataset Generator")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    try {
      // Generate 10GB sales dataset
      val salesDF = generateSalesDataset(10)(spark)

      // Write in Parquet format with partitioning
      writeDataset(
        salesDF,
        "/tmp/benchmark-data/sales",
        format = "parquet",
        partitionBy = Some(Seq("sale_date"))
      )

      // Calculate actual size
      val actualSize = calculateDatasetSize(java.nio.file.Paths.get("/tmp/benchmark-data/sales"))
      println(s"Actual dataset size: ${formatBytes(actualSize)}")

      // Generate streaming events dataset
      val eventsDF = generateStreamingEvents(30000000)(spark)
      writeDataset(eventsDF, "/tmp/benchmark-data/events", format = "json")

      // Generate customer dimension
      val customersDF = generateCustomers(1000000)(spark)
      writeDataset(customersDF, "/tmp/benchmark-data/customers", format = "parquet")

      println("Dataset generation complete!")

    } finally {
      spark.stop()
    }
  }
}
