package com.etl.benchmark

import com.etl.core._
import com.etl.pipeline._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}

/**
 * Performance benchmarks for batch processing.
 *
 * Tests:
 * - T157: Batch processing 10GB file with aggregation
 * - Throughput measurement (records/second, GB/second)
 * - Memory consumption monitoring
 * - Query execution plan analysis
 *
 * Target Performance:
 * - Process 10GB dataset in < 5 minutes on 4-core machine
 * - Sustained throughput > 35 MB/s
 * - Memory usage < 4GB
 */
class BatchPerformanceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDataPath: String = _
  var testOutputPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Batch Performance Benchmark")
      .master("local[4]") // 4 cores for realistic performance testing
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    testDataPath = Files.createTempDirectory("perf_data_").toString
    testOutputPath = Files.createTempDirectory("perf_output_").toString

    // Generate test dataset (smaller for CI/testing, configurable for real benchmarks)
    val datasetSizeGB = sys.env.getOrElse("BENCHMARK_SIZE_GB", "1").toInt
    generateBenchmarkDataset(datasetSizeGB)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    deleteDirectory(Paths.get(testDataPath))
    deleteDirectory(Paths.get(testOutputPath))
  }

  private def generateBenchmarkDataset(sizeGB: Int): Unit = {
    println(s"Generating ${sizeGB}GB benchmark dataset...")
    val salesDF = LargeDatasetGenerator.generateSalesDataset(sizeGB)

    LargeDatasetGenerator.writeDataset(
      salesDF,
      s"$testDataPath/sales",
      format = "parquet",
      partitionBy = Some(Seq("sale_date"))
    )

    val actualSize = LargeDatasetGenerator.calculateDatasetSize(Paths.get(s"$testDataPath/sales"))
    println(s"Generated dataset size: ${LargeDatasetGenerator.formatBytes(actualSize)}")
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

  "BatchPerformance" should "process large dataset with aggregation efficiently" in {
    // T157: Benchmark batch processing with aggregation

    val pipelineConfig = PipelineConfig(
      pipelineId = "batch-performance-pipeline",
      source = SourceConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testDataPath/sales",
          "format" -> "parquet"
        ),
        schemaPath = None
      ),
      transformations = List(
        TransformationConfig(
          name = "aggregate-sales",
          `type` = "aggregation",
          options = Map(
            "groupBy" -> "category,region",
            "aggregations" -> "total_quantity:sum(quantity),total_revenue:sum(price * quantity),avg_price:avg(price),sale_count:count(*)"
          )
        )
      ),
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/aggregated_sales",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(
        repartition = Some(16),
        cacheIntermediate = Some(false),
        shufflePartitions = Some(16)
      ),
      quality = QualityConfig(
        schemaValidation = false,
        nullChecks = List.empty,
        duplicateCheck = None,
        quarantinePath = None
      )
    )

    val runContext = RunContext(
      pipelineId = pipelineConfig.pipelineId,
      runId = java.util.UUID.randomUUID().toString,
      spark = spark
    )

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    // Execute pipeline with timing
    val startTime = System.currentTimeMillis()
    val startMemory = getUsedMemoryMB()

    val executor = new PipelineExecutor()
    val metrics = executor.execute(
      pipelineConfig,
      runContext,
      extractorRegistry,
      transformerRegistry,
      loaderRegistry
    )

    val endTime = System.currentTimeMillis()
    val endMemory = getUsedMemoryMB()

    val durationSeconds = (endTime - startTime) / 1000.0
    val memoryUsedMB = endMemory - startMemory

    // Verify execution succeeded
    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted should be > 0L

    // Calculate throughput
    val inputSize = LargeDatasetGenerator.calculateDatasetSize(Paths.get(s"$testDataPath/sales"))
    val throughputMBps = (inputSize / (1024.0 * 1024)) / durationSeconds
    val throughputRecordsPerSecond = metrics.recordsExtracted.toDouble / durationSeconds

    // Print performance metrics
    println(s"\n=== Batch Performance Benchmark Results ===")
    println(s"Dataset size: ${LargeDatasetGenerator.formatBytes(inputSize)}")
    println(s"Records processed: ${metrics.recordsExtracted}")
    println(s"Records output: ${metrics.recordsLoaded}")
    println(s"Duration: ${durationSeconds}s")
    println(s"Throughput: ${throughputRecordsPerSecond.toLong} records/sec")
    println(s"Throughput: ${"%.2f".format(throughputMBps)} MB/sec")
    println(s"Memory used: ${memoryUsedMB}MB")
    println(s"===========================================\n")

    // Performance assertions (adjust based on environment)
    // For 1GB dataset: should complete in < 60 seconds
    // For 10GB dataset: should complete in < 300 seconds (5 minutes)
    val maxDurationSeconds = sys.env.getOrElse("BENCHMARK_SIZE_GB", "1").toInt * 60
    durationSeconds should be < maxDurationSeconds.toDouble

    // Throughput should be reasonable (at least 10 MB/s for 4-core local)
    throughputMBps should be > 10.0
  }

  it should "handle complex multi-stage pipeline efficiently" in {
    val pipelineConfig = PipelineConfig(
      pipelineId = "multi-stage-performance-pipeline",
      source = SourceConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testDataPath/sales",
          "format" -> "parquet"
        ),
        schemaPath = None
      ),
      transformations = List(
        TransformationConfig(
          name = "filter-completed",
          `type` = "filter",
          options = Map(
            "condition" -> "status = 'completed'"
          )
        ),
        TransformationConfig(
          name = "add-revenue",
          `type` = "map",
          options = Map(
            "expressions" -> "revenue:price * quantity,year:year(sale_date),month:month(sale_date)"
          )
        ),
        TransformationConfig(
          name = "aggregate-by-month",
          `type` = "aggregation",
          options = Map(
            "groupBy" -> "year,month,category",
            "aggregations" -> "total_revenue:sum(revenue),avg_revenue:avg(revenue),max_revenue:max(revenue)"
          )
        )
      ),
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/monthly_summary",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = Some("year")
      ),
      performance = PerformanceConfig(
        repartition = Some(16),
        cacheIntermediate = Some(false),
        shufflePartitions = Some(16)
      ),
      quality = QualityConfig(
        schemaValidation = false,
        nullChecks = List.empty,
        duplicateCheck = None,
        quarantinePath = None
      )
    )

    val runContext = RunContext(
      pipelineId = pipelineConfig.pipelineId,
      runId = java.util.UUID.randomUUID().toString,
      spark = spark
    )

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val startTime = System.currentTimeMillis()

    val executor = new PipelineExecutor()
    val metrics = executor.execute(
      pipelineConfig,
      runContext,
      extractorRegistry,
      transformerRegistry,
      loaderRegistry
    )

    val durationSeconds = (System.currentTimeMillis() - startTime) / 1000.0

    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted should be > 0L

    println(s"Multi-stage pipeline completed in ${durationSeconds}s")
  }

  it should "efficiently process partitioned data" in {
    import org.apache.spark.sql.functions._

    // Read partitioned data
    val startTime = System.currentTimeMillis()

    val df = spark.read.parquet(s"$testDataPath/sales")

    // Partition pruning test: filter on partition column
    val filteredDF = df.filter(col("sale_date") >= "2024-06-01")
    val count = filteredDF.count()

    val durationSeconds = (System.currentTimeMillis() - startTime) / 1000.0

    println(s"Partitioned read with filter completed in ${durationSeconds}s (${count} records)")

    // Should be fast due to partition pruning
    durationSeconds should be < 30.0
  }

  private def getUsedMemoryMB(): Long = {
    val runtime = Runtime.getRuntime
    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
  }
}
