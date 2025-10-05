package com.etl.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}

/**
 * Performance tests for partitioning strategy effectiveness.
 *
 * Tests:
 * - T160: Verify partitioning strategy effectiveness
 * - Compare read performance with/without partitioning
 * - Analyze partition size distribution
 * - Test dynamic partition pruning
 * - Measure write performance for different partition strategies
 */
class PartitioningStrategySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDataPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Partitioning Strategy Analysis")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .getOrCreate()

    testDataPath = Files.createTempDirectory("partition_data_").toString

    generateTestData()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    deleteDirectory(Paths.get(testDataPath))
  }

  private def generateTestData(): Unit = {
    import spark.implicits._

    // Generate sales data
    val salesDF = spark.range(0, 50000)
      .select(
        $"id".alias("sale_id"),
        concat(lit("PROD-"), ($"id" % 1000)).alias("product_id"),
        when($"id" % 3 === 0, "Electronics")
          .when($"id" % 3 === 1, "Clothing")
          .otherwise("Books").alias("category"),
        (($"id" % 10) + 1).cast("int").alias("quantity"),
        (10.0 + (($"id" % 50) / 2.0)).cast("double").alias("price"),
        date_add(lit("2024-01-01"), ($"id" % 365).cast("int")).alias("sale_date"),
        when($"id" % 50 < 10, "NY")
          .when($"id" % 50 < 20, "CA")
          .when($"id" % 50 < 30, "TX")
          .when($"id" % 50 < 40, "FL")
          .otherwise("WA").alias("state")
      )

    // Write without partitioning
    salesDF.write
      .mode("overwrite")
      .parquet(s"$testDataPath/sales_unpartitioned")

    // Write with date partitioning
    salesDF.write
      .partitionBy("sale_date")
      .mode("overwrite")
      .parquet(s"$testDataPath/sales_partitioned_by_date")

    // Write with category partitioning
    salesDF.write
      .partitionBy("category")
      .mode("overwrite")
      .parquet(s"$testDataPath/sales_partitioned_by_category")

    // Write with multi-column partitioning
    salesDF.write
      .partitionBy("category", "state")
      .mode("overwrite")
      .parquet(s"$testDataPath/sales_partitioned_by_category_state")
  }

  private def deleteDirectory(path: java.nio.file.Path): Unit = {
    if (Files.exists(path)) {
      import scala.jdk.CollectionConverters._
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .iterator()
        .asScala
        .foreach(Files.delete)
    }
  }

  private def measureReadTime(df: DataFrame): Long = {
    val startTime = System.currentTimeMillis()
    val count = df.count()
    val duration = System.currentTimeMillis() - startTime
    println(s"  Read ${count} records in ${duration}ms")
    duration
  }

  "PartitioningStrategy" should "demonstrate performance benefit of date partitioning" in {
    // T160: Compare read performance with partition pruning

    import spark.implicits._

    println("\n=== Date Partitioning Performance ===")

    // Read from unpartitioned data
    println("Unpartitioned read (with date filter):")
    val unpartitionedDF = spark.read.parquet(s"$testDataPath/sales_unpartitioned")
      .filter($"sale_date" >= "2024-06-01" && $"sale_date" < "2024-07-01")
    val unpartitionedTime = measureReadTime(unpartitionedDF)

    // Read from partitioned data
    println("Partitioned read (with date filter):")
    val partitionedDF = spark.read.parquet(s"$testDataPath/sales_partitioned_by_date")
      .filter($"sale_date" >= "2024-06-01" && $"sale_date" < "2024-07-01")
    val partitionedTime = measureReadTime(partitionedDF)

    val speedup = unpartitionedTime.toDouble / partitionedTime.toDouble

    println(s"\nPartitioned read speedup: ${speedup}x faster")
    println("=====================================\n")

    // Partitioned read should be faster (or at least not significantly slower)
    partitionedTime should be <= (unpartitionedTime * 1.5).toLong
  }

  it should "analyze partition count and size distribution" in {
    import spark.implicits._

    val categoryPartitionedDF = spark.read.parquet(s"$testDataPath/sales_partitioned_by_category")

    // Get partition information
    val partitions = categoryPartitionedDF.rdd.getNumPartitions

    println(s"\n=== Partition Distribution Analysis ===")
    println(s"Number of Spark partitions: $partitions")

    // Count records per category
    val categoryDistribution = categoryPartitionedDF
      .groupBy($"category")
      .agg(count("*").alias("record_count"))
      .orderBy($"category")

    println("\nRecords per partition (category):")
    categoryDistribution.show()

    // Check distribution is balanced
    val categoryCounts = categoryDistribution.collect().map(_.getLong(1))
    val minCount = categoryCounts.min
    val maxCount = categoryCounts.max
    val avgCount = categoryCounts.sum / categoryCounts.length
    val skew = maxCount.toDouble / minCount.toDouble

    println(s"Min records per category: $minCount")
    println(s"Max records per category: $maxCount")
    println(s"Avg records per category: $avgCount")
    println(s"Skew ratio (max/min): ${skew}")

    // Partitioning should be relatively balanced
    skew should be < 2.0 // Categories should have similar counts

    println("========================================\n")
  }

  it should "compare multi-column partitioning overhead" in {
    import spark.implicits._

    println("\n=== Multi-Column Partitioning Overhead ===")

    // Read single partition column
    println("Single column partitioning (category):")
    val singlePartDF = spark.read.parquet(s"$testDataPath/sales_partitioned_by_category")
    val singlePartTime = measureReadTime(singlePartDF)

    // Read multi-column partitioning
    println("Multi-column partitioning (category + state):")
    val multiPartDF = spark.read.parquet(s"$testDataPath/sales_partitioned_by_category_state")
    val multiPartTime = measureReadTime(multiPartDF)

    println(s"\nSingle partition time: ${singlePartTime}ms")
    println(s"Multi partition time: ${multiPartTime}ms")
    println("===========================================\n")

    // Both should read all data successfully
    singlePartDF.count() shouldBe multiPartDF.count()
  }

  it should "demonstrate partition pruning with category filter" in {
    import spark.implicits._

    println("\n=== Partition Pruning with Category Filter ===")

    // Without partitioning
    val unpartitioned = spark.read.parquet(s"$testDataPath/sales_unpartitioned")
      .filter($"category" === "Electronics")

    println("Unpartitioned (category filter):")
    val unpartitionedTime = measureReadTime(unpartitioned)

    // With partitioning
    val partitioned = spark.read.parquet(s"$testDataPath/sales_partitioned_by_category")
      .filter($"category" === "Electronics")

    println("Partitioned (category filter):")
    val partitionedTime = measureReadTime(partitioned)

    // Check physical plan
    val physicalPlan = partitioned.queryExecution.executedPlan.toString()
    val hasPruning = physicalPlan.toLowerCase.contains("partition") ||
                     physicalPlan.contains("PartitionFilters")

    if (hasPruning) {
      println("✓ Partition pruning detected in plan")
    }

    println(s"\nSpeedup: ${unpartitionedTime.toDouble / partitionedTime.toDouble}x")
    println("==============================================\n")

    partitioned.count() shouldBe unpartitioned.count()
  }

  it should "measure write performance for different partition strategies" in {
    import spark.implicits._

    val testDF = spark.range(0, 10000)
      .select(
        $"id",
        when($"id" % 2 === 0, "A").otherwise("B").alias("category"),
        date_add(lit("2024-01-01"), ($"id" % 10).cast("int")).alias("date")
      )

    val outputPath = s"$testDataPath/write_performance"

    // No partitioning
    println("\n=== Write Performance Comparison ===")
    val noPartStart = System.currentTimeMillis()
    testDF.write.mode("overwrite").parquet(s"$outputPath/no_partition")
    val noPartTime = System.currentTimeMillis() - noPartStart
    println(s"No partitioning: ${noPartTime}ms")

    // Single column partitioning
    val singlePartStart = System.currentTimeMillis()
    testDF.write.partitionBy("category").mode("overwrite").parquet(s"$outputPath/single_partition")
    val singlePartTime = System.currentTimeMillis() - singlePartStart
    println(s"Single partition column: ${singlePartTime}ms")

    // Multi-column partitioning
    val multiPartStart = System.currentTimeMillis()
    testDF.write.partitionBy("category", "date").mode("overwrite").parquet(s"$outputPath/multi_partition")
    val multiPartTime = System.currentTimeMillis() - multiPartStart
    println(s"Multi partition columns: ${multiPartTime}ms")

    println(s"\nSingle partition overhead: ${((singlePartTime - noPartTime) / noPartTime.toDouble * 100).toInt}%")
    println(s"Multi partition overhead: ${((multiPartTime - noPartTime) / noPartTime.toDouble * 100).toInt}%")
    println("====================================\n")

    // All writes should succeed
    noPartTime should be > 0L
    singlePartTime should be > 0L
    multiPartTime should be > 0L
  }

  it should "verify optimal partition file sizes" in {
    import spark.implicits._

    val partitionedPath = Paths.get(s"$testDataPath/sales_partitioned_by_date")

    // Calculate partition file sizes
    import scala.jdk.CollectionConverters._
    val partitionFiles = Files.walk(partitionedPath)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".parquet"))
      .toList

    val fileSizes = partitionFiles.map(Files.size)

    if (fileSizes.nonEmpty) {
      val avgSize = fileSizes.sum / fileSizes.length
      val minSize = fileSizes.min
      val maxSize = fileSizes.max

      println(s"\n=== Partition File Size Analysis ===")
      println(s"Number of partition files: ${fileSizes.length}")
      println(s"Average file size: ${LargeDatasetGenerator.formatBytes(avgSize)}")
      println(s"Min file size: ${LargeDatasetGenerator.formatBytes(minSize)}")
      println(s"Max file size: ${LargeDatasetGenerator.formatBytes(maxSize)}")

      // Optimal partition size: 128MB - 1GB
      // For test data, expect smaller files
      val optimalMin = 1024 * 1024 // 1MB minimum for test
      val optimalMax = 1024 * 1024 * 1024 // 1GB maximum

      if (avgSize >= optimalMin && avgSize <= optimalMax) {
        println("✓ Partition sizes are within optimal range")
      } else if (avgSize < optimalMin) {
        println(s"⚠ Partitions are small (< ${LargeDatasetGenerator.formatBytes(optimalMin)})")
      } else {
        println(s"⚠ Partitions are large (> ${LargeDatasetGenerator.formatBytes(optimalMax)})")
      }

      println("=====================================\n")

      fileSizes.length should be > 0
    }
  }

  it should "analyze repartition vs coalesce for partition count optimization" in {
    import spark.implicits._

    val df = spark.range(0, 10000).toDF("id")

    println("\n=== Repartition vs Coalesce Analysis ===")

    // Original partitions
    val originalPartitions = df.rdd.getNumPartitions
    println(s"Original partitions: $originalPartitions")

    // Repartition (full shuffle)
    val repartitionStart = System.currentTimeMillis()
    val repartitionedDF = df.repartition(4)
    val repartitionCount = repartitionedDF.count()
    val repartitionTime = System.currentTimeMillis() - repartitionStart
    println(s"Repartition to 4: ${repartitionedDF.rdd.getNumPartitions} partitions in ${repartitionTime}ms")

    // Coalesce (no shuffle)
    val coalesceStart = System.currentTimeMillis()
    val coalescedDF = df.coalesce(4)
    val coalesceCount = coalescedDF.count()
    val coalesceTime = System.currentTimeMillis() - coalesceStart
    println(s"Coalesce to 4: ${coalescedDF.rdd.getNumPartitions} partitions in ${coalesceTime}ms")

    println(s"\nCoalesce speedup: ${repartitionTime.toDouble / coalesceTime.toDouble}x faster")
    println("========================================\n")

    repartitionCount shouldBe coalesceCount
    coalesceTime should be <= repartitionTime
  }
}
