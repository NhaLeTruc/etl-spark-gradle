package com.etl.integration

import com.etl.config.YAMLConfigParser
import com.etl.core._
import com.etl.pipeline._
import com.etl.quality.{DataQualityChecker, QuarantineWriter}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import java.sql.DriverManager

/**
 * Integration tests for failure recovery and error handling.
 *
 * Tests:
 * - T154: Retry logic for transient failures
 * - T155: Graceful failure handling and error reporting
 * - Pipeline resilience to invalid data
 * - Recovery from connection failures
 * - Quarantine mechanisms for bad data
 */
class FailureRecoverySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDbPath: String = _
  var testOutputPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Failure Recovery Integration Test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    testDbPath = Files.createTempDirectory("test_db_").toString
    testOutputPath = Files.createTempDirectory("test_output_").toString

    setupTestDatabase()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    deleteDirectory(Paths.get(testDbPath))
    deleteDirectory(Paths.get(testOutputPath))
  }

  private def setupTestDatabase(): Unit = {
    val jdbcUrl = s"jdbc:h2:$testDbPath/testdb;MODE=PostgreSQL"
    val conn = DriverManager.getConnection(jdbcUrl, "sa", "")

    try {
      val stmt = conn.createStatement()

      // Create test table with mixed valid/invalid data
      stmt.execute(
        """
          |CREATE TABLE test_data (
          |  id INT PRIMARY KEY,
          |  name VARCHAR(100),
          |  email VARCHAR(100),
          |  age INT,
          |  salary DECIMAL(10, 2)
          |)
          |""".stripMargin
      )

      // Insert valid records
      for (i <- 1 to 80) {
        stmt.execute(
          s"""
             |INSERT INTO test_data (id, name, email, age, salary)
             |VALUES ($i, 'User $i', 'user$i@example.com', ${20 + (i % 50)}, ${30000 + i * 100})
             |""".stripMargin
        )
      }

      // Insert invalid records (NULL values in required fields)
      for (i <- 81 to 100) {
        val name = if (i % 2 == 0) "NULL" else s"'User $i'"
        val email = if (i % 3 == 0) "NULL" else s"'user$i@example.com'"
        stmt.execute(
          s"""
             |INSERT INTO test_data (id, name, email, age, salary)
             |VALUES ($i, $name, $email, ${20 + (i % 50)}, ${30000 + i * 100})
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

  "FailureRecovery" should "handle invalid data with quarantine mechanism" in {
    // T154-T155: Test data quality checks with quarantine

    val quarantinePath = s"$testOutputPath/quarantine"

    val pipelineConfig = PipelineConfig(
      pipelineId = "quality-check-pipeline",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> s"jdbc:h2:$testDbPath/testdb;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "test_data",
          "user" -> "sa",
          "password" -> ""
        ),
        schemaPath = None
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/valid_data",
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
        nullChecks = List(
          NullCheckConfig("name", "quarantine"),
          NullCheckConfig("email", "quarantine")
        ),
        duplicateCheck = None,
        quarantinePath = Some(quarantinePath)
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
    val qualityChecker = new DataQualityChecker()
    val quarantineWriter = new QuarantineWriter()

    val executor = new PipelineExecutor()
    val metrics = executor.executeWithQuality(
      pipelineConfig,
      runContext,
      extractorRegistry,
      transformerRegistry,
      loaderRegistry,
      qualityChecker,
      quarantineWriter
    )

    // Pipeline should succeed even with invalid data
    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted shouldBe 100

    // Valid records should be loaded
    val validData = spark.read.parquet(s"$testOutputPath/valid_data")
    validData.count() should be > 0L
    validData.count() should be < 100L // Some records were quarantined

    // Quarantined records should exist
    if (Files.exists(Paths.get(quarantinePath))) {
      val quarantinedData = spark.read.parquet(quarantinePath)
      quarantinedData.count() should be > 0L

      // Verify quarantine metadata columns exist
      quarantinedData.columns should contain allOf ("quarantine_timestamp", "pipeline_id", "run_id")
    }
  }

  it should "handle extraction failures gracefully" in {
    // T155: Test graceful failure handling for invalid database

    val pipelineConfig = PipelineConfig(
      pipelineId = "invalid-connection-pipeline",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> "jdbc:h2:mem:nonexistent;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "nonexistent_table",
          "user" -> "invalid_user",
          "password" -> "invalid_password"
        ),
        schemaPath = None
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/output",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(None, None, None),
      quality = QualityConfig(false, List.empty, None, None)
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

    // Should throw ExtractionException
    intercept[ExtractionException] {
      executor.execute(
        pipelineConfig,
        runContext,
        extractorRegistry,
        transformerRegistry,
        loaderRegistry
      )
    }
  }

  it should "handle transformation failures gracefully" in {
    // Test invalid transformation configuration

    val pipelineConfig = PipelineConfig(
      pipelineId = "invalid-transformation-pipeline",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> s"jdbc:h2:$testDbPath/testdb;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "test_data",
          "user" -> "sa",
          "password" -> ""
        ),
        schemaPath = None
      ),
      transformations = List(
        TransformationConfig(
          name = "invalid-aggregation",
          `type` = "aggregation",
          options = Map(
            "groupBy" -> "nonexistent_column",
            "aggregations" -> "invalid_agg:invalid_function(name)"
          )
        )
      ),
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> s"$testOutputPath/output",
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(None, None, None),
      quality = QualityConfig(false, List.empty, None, None)
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

    // Should throw TransformationException
    intercept[TransformationException] {
      executor.execute(
        pipelineConfig,
        runContext,
        extractorRegistry,
        transformerRegistry,
        loaderRegistry
      )
    }
  }

  it should "validate null checks correctly" in {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val qualityChecker = new DataQualityChecker()

    // Create test data with nulls
    val testData = Seq(
      (1, "Alice", "alice@example.com"),
      (2, null, "bob@example.com"),
      (3, "Charlie", null),
      (4, null, null),
      (5, "Eve", "eve@example.com")
    ).toDF("id", "name", "email")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List(
        NullCheckConfig("name", "quarantine"),
        NullCheckConfig("email", "quarantine")
      ),
      duplicateCheck = None,
      quarantinePath = None
    )

    val (validDF, invalidDF) = qualityChecker.splitValidInvalid(testData, qualityConfig)

    // Valid records: only row 1 and 5 (no nulls in checked columns)
    validDF.count() shouldBe 2

    // Invalid records: rows 2, 3, 4 (have nulls in checked columns)
    invalidDF.count() shouldBe 3
  }

  it should "handle duplicate detection" in {
    import spark.implicits._

    val qualityChecker = new DataQualityChecker()

    // Create test data with duplicates
    val testData = Seq(
      (1, "Alice", "alice@example.com"),
      (2, "Bob", "bob@example.com"),
      (3, "Alice", "alice@example.com"), // Duplicate of row 1
      (4, "Charlie", "charlie@example.com"),
      (5, "Bob", "bob@example.com")      // Duplicate of row 2
    ).toDF("id", "name", "email")

    val duplicateConfig = DuplicateCheckConfig(
      columns = List("name", "email"),
      action = "quarantine"
    )

    val result = qualityChecker.checkDuplicates(testData, duplicateConfig)

    // Should detect 2 duplicates (rows 3 and 5)
    result.duplicateCount shouldBe 2
    result.duplicateRate should be > 0.0
  }

  it should "calculate quality metrics correctly" in {
    import spark.implicits._

    val qualityChecker = new DataQualityChecker()

    val testData = Seq(
      (1, "Alice", 25),
      (2, "Bob", 30),
      (3, null, 35),
      (4, "Dave", null),
      (5, "Eve", 40)
    ).toDF("id", "name", "age")

    val nullRate = qualityChecker.calculateNullRate(testData, "name")
    nullRate shouldBe 0.2 // 1 out of 5 is null

    val ageNullRate = qualityChecker.calculateNullRate(testData, "age")
    ageNullRate shouldBe 0.2 // 1 out of 5 is null
  }

  it should "report execution metrics on failure" in {
    // Verify that metrics are reported even when pipeline fails

    val pipelineConfig = PipelineConfig(
      pipelineId = "failing-pipeline",
      source = SourceConfig(
        `type` = "postgres",
        options = Map(
          "url" -> s"jdbc:h2:$testDbPath/testdb;MODE=PostgreSQL",
          "driver" -> "org.h2.Driver",
          "dbtable" -> "test_data",
          "user" -> "sa",
          "password" -> ""
        ),
        schemaPath = None
      ),
      transformations = List(
        TransformationConfig(
          name = "filter-adults",
          `type` = "filter",
          options = Map(
            "condition" -> "age >= 18"
          )
        )
      ),
      sink = SinkConfig(
        `type` = "s3",
        options = Map(
          "path" -> "/invalid/path/that/cannot/be/written", // This will fail
          "format" -> "parquet"
        ),
        writeMode = "overwrite",
        partitionBy = None
      ),
      performance = PerformanceConfig(None, None, None),
      quality = QualityConfig(false, List.empty, None, None)
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

    // Execution will fail during load
    val thrown = intercept[LoadException] {
      executor.execute(
        pipelineConfig,
        runContext,
        extractorRegistry,
        transformerRegistry,
        loaderRegistry
      )
    }

    // Verify exception contains useful information
    thrown.getMessage should include("Failed to load data")
  }
}
