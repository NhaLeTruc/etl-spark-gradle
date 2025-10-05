package com.etl.integration

import com.etl.config.YAMLConfigParser
import com.etl.core._
import com.etl.pipeline._
import com.etl.quality.{DataQualityChecker, QuarantineWriter}
import com.etl.vault.VaultClient
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import java.sql.DriverManager

/**
 * Integration test for Quickstart Scenario 1: Batch Pipeline.
 *
 * Pipeline: PostgreSQL -> Aggregation -> S3 (Parquet)
 *
 * Tests:
 * - T138: End-to-end batch data extraction from PostgreSQL
 * - T139: Aggregation transformation with groupBy and sum
 * - T140: Load results to S3 in Parquet format
 * - T141: Verify data quality and record counts
 * - T142: Validate lineage tracking through pipeline
 */
class QuickstartScenario1Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDbPath: String = _
  var testOutputPath: String = _

  override def beforeAll(): Unit = {
    // Create Spark session for integration tests
    spark = SparkSession.builder()
      .appName("Quickstart Scenario 1 Integration Test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    // Setup test database path
    testDbPath = Files.createTempDirectory("test_db_").toString
    testOutputPath = Files.createTempDirectory("test_output_").toString

    // Initialize H2 database in PostgreSQL mode with sample data
    setupTestDatabase()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    // Cleanup test directories
    deleteDirectory(Paths.get(testDbPath))
    deleteDirectory(Paths.get(testOutputPath))
  }

  private def setupTestDatabase(): Unit = {
    val jdbcUrl = s"jdbc:h2:$testDbPath/salesdb;MODE=PostgreSQL"
    val conn = DriverManager.getConnection(jdbcUrl, "sa", "")

    try {
      val stmt = conn.createStatement()

      // Create sales table
      stmt.execute(
        """
          |CREATE TABLE sales (
          |  sale_id INT PRIMARY KEY,
          |  product_id VARCHAR(50),
          |  category VARCHAR(50),
          |  quantity INT,
          |  price DECIMAL(10, 2),
          |  sale_date DATE
          |)
          |""".stripMargin
      )

      // Insert sample data (100 sales records)
      for (i <- 1 to 100) {
        val productId = s"PROD-${(i % 10) + 1}"
        val category = if (i % 3 == 0) "Electronics" else if (i % 3 == 1) "Clothing" else "Books"
        val quantity = (i % 5) + 1
        val price = 10.0 + (i % 50)
        val saleDate = s"2024-01-${(i % 28) + 1}"

        stmt.execute(
          s"""
             |INSERT INTO sales (sale_id, product_id, category, quantity, price, sale_date)
             |VALUES ($i, '$productId', '$category', $quantity, $price, '$saleDate')
             |""".stripMargin
        )
      }

      stmt.close()
    } finally {
      conn.close()
    }
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

  "Quickstart Scenario 1" should "execute end-to-end batch pipeline from PostgreSQL to S3" in {
    // T138-T142: Complete batch pipeline integration test

    // Setup pipeline configuration
    val pipelineConfig = PipelineConfig(
      pipelineId = "sales-aggregation-pipeline",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> s"jdbc:h2:$testDbPath/salesdb;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "sales",
          "user" -> "sa",
          "password" -> ""
        ),
        schemaPath = None
      ),
      transformations = List(
        TransformationConfig(
          name = "aggregate-by-category",
          `type` = "aggregation",
          options = Map(
            "groupBy" -> "category",
            "aggregations" -> "total_quantity:sum(quantity),total_revenue:sum(price * quantity),avg_price:avg(price)"
          )
        )
      ),
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/sales_summary",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(
        repartition = Some(2),
        cacheIntermediate = Some(false),
        shufflePartitions = Some(2)
      ),
      quality = QualityConfig(
        schemaValidation = false,
        nullChecks = List.empty,
        duplicateCheck = None,
        quarantinePath = None
      )
    )

    // Create run context
    val runContext = RunContext(
      pipelineId = pipelineConfig.pipelineId,
      runId = java.util.UUID.randomUUID().toString,
      spark = spark
    )

    // Setup mock Vault
    val vaultClient = VaultClient.mock()
    vaultClient.writeSecret("database/postgres/sales", Map(
      "username" -> "sa",
      "password" -> ""
    ))

    // Initialize registries
    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    // Execute pipeline
    val executor = new PipelineExecutor()
    val metrics = executor.execute(
      pipelineConfig,
      runContext,
      extractorRegistry,
      transformerRegistry,
      loaderRegistry
    )

    // T141: Verify execution metrics
    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted shouldBe 100
    metrics.recordsLoaded shouldBe 3 // 3 categories: Electronics, Clothing, Books
    metrics.pipelineId shouldBe "sales-aggregation-pipeline"
    metrics.runId shouldBe runContext.runId

    // T140: Verify output data in S3 (Parquet)
    val outputPath = s"$testOutputPath/sales_summary"
    Files.exists(Paths.get(outputPath)) shouldBe true

    val resultDf = spark.read.parquet(outputPath)
    resultDf.count() shouldBe 3

    // Verify aggregation results
    val categories = resultDf.select("category").collect().map(_.getString(0)).sorted
    categories shouldBe Array("Books", "Clothing", "Electronics")

    // Verify aggregation columns exist
    resultDf.columns should contain allOf ("category", "total_quantity", "total_revenue", "avg_price")

    // T142: Validate lineage tracking
    resultDf.columns should contain("_lineage")
    val lineageData = resultDf.select("_lineage").first().getString(0)
    lineageData should include("sales-aggregation-pipeline")
    lineageData should include("postgres")
    lineageData should include("aggregate-by-category")
  }

  it should "handle aggregation transformations correctly" in {
    // T139: Verify aggregation logic with detailed checks

    val jdbcUrl = s"jdbc:h2:$testDbPath/salesdb;MODE=PostgreSQL"

    // Read source data directly
    val sourceDf = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "sales")
      .option("user", "sa")
      .option("password", "")
      .load()

    sourceDf.count() shouldBe 100

    // Apply aggregation transformation manually
    import org.apache.spark.sql.functions._
    val aggregatedDf = sourceDf
      .groupBy("category")
      .agg(
        sum("quantity").alias("total_quantity"),
        sum(expr("price * quantity")).alias("total_revenue"),
        avg("price").alias("avg_price")
      )

    aggregatedDf.count() shouldBe 3

    // Verify each category has aggregated values
    val electronicsRow = aggregatedDf.filter(col("category") === "Electronics").first()
    electronicsRow.getLong(electronicsRow.fieldIndex("total_quantity")) should be > 0L
    electronicsRow.getDouble(electronicsRow.fieldIndex("total_revenue")) should be > 0.0
    electronicsRow.getDouble(electronicsRow.fieldIndex("avg_price")) should be > 0.0
  }

  it should "support pipeline execution with quality checks disabled" in {
    // Verify that pipeline works with quality checks disabled (as configured)

    val pipelineConfig = PipelineConfig(
      pipelineId = "sales-no-quality",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> s"jdbc:h2:$testDbPath/salesdb;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "sales",
          "user" -> "sa",
          "password" -> ""
        ),
        schemaPath = None
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/sales_raw",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(
        repartition = None,
        cacheIntermediate = None,
        shufflePartitions = None
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

    val executor = new PipelineExecutor()
    val metrics = executor.execute(
      pipelineConfig,
      runContext,
      extractorRegistry,
      transformerRegistry,
      loaderRegistry
    )

    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted shouldBe 100
    metrics.recordsLoaded shouldBe 100
  }
}
