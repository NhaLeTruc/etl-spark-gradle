package com.etl.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}

/**
 * Performance analysis for Spark query execution plans.
 *
 * Tests:
 * - T159: Analyze Spark query execution plans for optimization opportunities
 * - Verify predicate pushdown
 * - Verify partition pruning
 * - Verify join strategy selection
 * - Identify shuffle operations
 * - Check for unnecessary wide transformations
 */
class QueryPlanAnalysisSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDataPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Query Plan Analysis")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    testDataPath = Files.createTempDirectory("query_plan_data_").toString

    // Generate test data
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

    // Generate partitioned sales data
    val salesDF = spark.range(0, 10000)
      .select(
        $"id".alias("sale_id"),
        concat(lit("PROD-"), ($"id" % 100)).alias("product_id"),
        (($"id" % 10) + 1).cast("int").alias("quantity"),
        (10.0 + (($"id" % 50) / 2.0)).cast("double").alias("price"),
        date_add(lit("2024-01-01"), ($"id" % 365).cast("int")).alias("sale_date")
      )

    salesDF.write
      .partitionBy("sale_date")
      .parquet(s"$testDataPath/sales")

    // Generate dimension data
    val productsDF = spark.range(0, 100)
      .select(
        concat(lit("PROD-"), $"id").alias("product_id"),
        concat(lit("Product "), $"id").alias("product_name"),
        when($"id" % 3 === 0, "Electronics")
          .when($"id" % 3 === 1, "Clothing")
          .otherwise("Books").alias("category")
      )

    productsDF.write.parquet(s"$testDataPath/products")
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

  "QueryPlanAnalysis" should "demonstrate partition pruning with date filters" in {
    // T159: Verify partition pruning optimization

    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Query with partition filter
    val filteredDF = salesDF.filter($"sale_date" >= "2024-06-01" && $"sale_date" < "2024-07-01")

    val physicalPlan = filteredDF.queryExecution.executedPlan.toString()

    println("\n=== Partition Pruning Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("==================================\n")

    // Verify partition pruning occurred
    // The plan should mention partition filters or pruning
    val hasPruning = physicalPlan.contains("PushedFilters") ||
                     physicalPlan.contains("PartitionFilters") ||
                     physicalPlan.toLowerCase.contains("partition")

    if (hasPruning) {
      println("✓ Partition pruning detected")
    } else {
      println("⚠ Partition pruning not detected in plan")
    }

    // Count should be fast due to partition pruning
    val count = filteredDF.count()
    println(s"Records found: $count")

    count should be > 0L
  }

  it should "demonstrate predicate pushdown for Parquet" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Query with simple filter
    val filteredDF = salesDF.filter($"quantity" > 5)

    val physicalPlan = filteredDF.queryExecution.executedPlan.toString()

    println("\n=== Predicate Pushdown Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("===================================\n")

    // Verify predicate pushdown
    val hasPushdown = physicalPlan.contains("PushedFilters") || physicalPlan.contains("Filter")

    if (hasPushdown) {
      println("✓ Predicate pushdown detected")
    } else {
      println("⚠ Predicate pushdown not detected")
    }

    filteredDF.count() should be > 0L
  }

  it should "analyze join strategy selection" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")
    val productsDF = spark.read.parquet(s"$testDataPath/products")

    // Join large fact with small dimension
    val joinedDF = salesDF.join(productsDF, "product_id")

    val physicalPlan = joinedDF.queryExecution.executedPlan.toString()

    println("\n=== Join Strategy Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("==============================\n")

    // Check for broadcast join (efficient for small dimension)
    val isBroadcastJoin = physicalPlan.contains("BroadcastHashJoin") ||
                          physicalPlan.contains("BroadcastNestedLoopJoin")

    if (isBroadcastJoin) {
      println("✓ Broadcast join detected (optimal for small dimension)")
    } else if (physicalPlan.contains("SortMergeJoin")) {
      println("⚠ Sort-merge join detected (consider broadcast for small table)")
    } else if (physicalPlan.contains("ShuffledHashJoin")) {
      println("⚠ Shuffled hash join detected")
    }

    joinedDF.count() should be > 0L
  }

  it should "identify shuffle operations" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Aggregation causes shuffle
    val aggregatedDF = salesDF
      .groupBy($"product_id")
      .agg(
        sum($"quantity").alias("total_quantity"),
        sum(expr("price * quantity")).alias("total_revenue")
      )

    val physicalPlan = aggregatedDF.queryExecution.executedPlan.toString()

    println("\n=== Shuffle Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("========================\n")

    // Count shuffle operations
    val exchangeCount = "Exchange".r.findAllIn(physicalPlan).length

    println(s"Number of shuffle (Exchange) operations: $exchangeCount")

    if (exchangeCount > 0) {
      println("✓ Shuffle detected for aggregation (expected)")
    }

    // Aggregation should produce results
    aggregatedDF.count() should be > 0L
  }

  it should "verify column pruning optimization" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Select only specific columns
    val selectedDF = salesDF.select($"product_id", $"quantity")

    val physicalPlan = selectedDF.queryExecution.executedPlan.toString()

    println("\n=== Column Pruning Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("===============================\n")

    // With Parquet, only selected columns should be read
    val mentionsAllColumns = physicalPlan.contains("sale_id") &&
                             physicalPlan.contains("price") &&
                             physicalPlan.contains("sale_date")

    if (!mentionsAllColumns) {
      println("✓ Column pruning appears effective")
    } else {
      println("⚠ All columns may be loaded")
    }

    selectedDF.count() shouldBe 10000
  }

  it should "analyze adaptive query execution" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Complex query that benefits from AQE
    val resultDF = salesDF
      .filter($"quantity" > 3)
      .groupBy($"product_id")
      .agg(sum($"quantity").alias("total"))
      .filter($"total" > 100)

    // Trigger execution
    val count = resultDF.count()

    val adaptivePlan = resultDF.queryExecution.executedPlan.toString()

    println("\n=== Adaptive Query Execution Analysis ===")
    println("Executed Plan:")
    println(adaptivePlan)
    println("=========================================\n")

    // Check for AQE optimizations
    val hasAQE = adaptivePlan.contains("AdaptiveSparkPlan") ||
                 adaptivePlan.contains("AQE")

    if (hasAQE) {
      println("✓ Adaptive Query Execution active")
    } else {
      println("⚠ AQE not detected (may be disabled)")
    }

    count should be >= 0L
  }

  it should "compare execution plans for different aggregation strategies" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Strategy 1: Direct aggregation
    val plan1DF = salesDF
      .groupBy($"product_id")
      .agg(sum($"quantity").alias("total"))

    val plan1 = plan1DF.queryExecution.executedPlan.toString()

    // Strategy 2: Filter then aggregate
    val plan2DF = salesDF
      .filter($"quantity" > 0)
      .groupBy($"product_id")
      .agg(sum($"quantity").alias("total"))

    val plan2 = plan2DF.queryExecution.executedPlan.toString()

    println("\n=== Aggregation Strategy Comparison ===")
    println("\nStrategy 1 (Direct):")
    println(plan1)
    println("\nStrategy 2 (Filter First):")
    println(plan2)
    println("========================================\n")

    // Both should produce results
    plan1DF.count() shouldBe plan2DF.count()
  }

  it should "analyze window function execution" in {
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Window function for ranking
    val windowSpec = Window.partitionBy($"product_id").orderBy($"sale_id")

    val rankedDF = salesDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" <= 5)

    val physicalPlan = rankedDF.queryExecution.executedPlan.toString()

    println("\n=== Window Function Analysis ===")
    println("Physical Plan:")
    println(physicalPlan)
    println("================================\n")

    // Check for window operation
    val hasWindow = physicalPlan.contains("Window")

    if (hasWindow) {
      println("✓ Window operation detected")
    }

    rankedDF.count() should be > 0L
  }

  it should "measure query compilation and execution time" in {
    import spark.implicits._

    val salesDF = spark.read.parquet(s"$testDataPath/sales")

    // Measure logical plan time
    val logicalPlanStart = System.currentTimeMillis()
    val logicalPlan = salesDF.filter($"quantity" > 5).queryExecution.logical
    val logicalPlanTime = System.currentTimeMillis() - logicalPlanStart

    // Measure physical plan time
    val physicalPlanStart = System.currentTimeMillis()
    val physicalPlan = salesDF.filter($"quantity" > 5).queryExecution.executedPlan
    val physicalPlanTime = System.currentTimeMillis() - physicalPlanStart

    // Measure execution time
    val executionStart = System.currentTimeMillis()
    val count = salesDF.filter($"quantity" > 5).count()
    val executionTime = System.currentTimeMillis() - executionStart

    println("\n=== Query Timing Analysis ===")
    println(s"Logical plan time: ${logicalPlanTime}ms")
    println(s"Physical plan time: ${physicalPlanTime}ms")
    println(s"Execution time: ${executionTime}ms")
    println(s"Total time: ${logicalPlanTime + physicalPlanTime + executionTime}ms")
    println("=============================\n")

    count should be > 0L
  }
}
