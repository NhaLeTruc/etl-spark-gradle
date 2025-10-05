package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.nio.file.{Files, Paths}

/**
 * Unit tests for S3Loader.
 *
 * Tests cover:
 * - T092: Write Avro to S3 (using local filesystem)
 * - T093: Write with partitioning
 * - T094: Validate configuration
 *
 * Note: Uses local filesystem for testing instead of actual S3.
 */
class S3LoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val testOutputPath = "/tmp/s3-loader-test"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("S3LoaderSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create test directory
    Files.createDirectories(Paths.get(testOutputPath))
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    // Cleanup test files
    import scala.reflect.io.Directory
    val directory = new Directory(new java.io.File(testOutputPath))
    directory.deleteRecursively()
  }

  import spark.implicits._

  // T092: Write Avro to S3
  "S3Loader" should "write data to S3 in Avro format" in {
    val loader = new S3Loader()

    val data = Seq(
      ("user1", 25, "alice@example.com"),
      ("user2", 30, "bob@example.com"),
      ("user3", 35, "charlie@example.com")
    ).toDF("username", "age", "email")

    val outputPath = s"$testOutputPath/avro-output"

    val config = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "overwrite",
      parameters = Map(
        "path" -> outputPath,
        "format" -> "avro"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.recordsWritten shouldBe 3
    result.sinkType shouldBe "s3"

    // Verify data was written
    val written = spark.read.format("avro").load(outputPath)
    written.count() shouldBe 3
    written.columns should contain allOf ("username", "age", "email")

    loader.sinkType shouldBe "s3"
  }

  // T093: Write with partitioning
  it should "write data with partitioning" in {
    val loader = new S3Loader()

    val data = Seq(
      ("2025-01-01", "user1", 100),
      ("2025-01-01", "user2", 200),
      ("2025-01-02", "user3", 300),
      ("2025-01-02", "user4", 400)
    ).toDF("date", "user", "amount")

    val outputPath = s"$testOutputPath/partitioned-output"

    val config = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "overwrite",
      parameters = Map(
        "path" -> outputPath,
        "format" -> "parquet",
        "partitionBy" -> "date"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.recordsWritten shouldBe 4

    // Verify partitioned structure exists
    val partitions = new java.io.File(outputPath).list()
    partitions should contain("date=2025-01-01")
    partitions should contain("date=2025-01-02")

    // Verify data integrity
    val written = spark.read.format("parquet").load(outputPath)
    written.count() shouldBe 4
  }

  // T094: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val loader = new S3Loader()

    // Missing path
    val invalidConfig1 = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "append",
      parameters = Map("format" -> "avro")
    )
    val result1 = loader.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: path")

    // Missing format
    val invalidConfig2 = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "append",
      parameters = Map("path" -> "s3://bucket/data")
    )
    val result2 = loader.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: format")

    // Invalid format
    val invalidConfig3 = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "append",
      parameters = Map(
        "path" -> "s3://bucket/data",
        "format" -> "invalid"
      )
    )
    val result3 = loader.validateConfig(invalidConfig3)
    result3.isValid shouldBe false
    result3.errors should contain("Invalid format: invalid (supported: avro, parquet, json, csv)")

    // Valid configuration
    val validConfig = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "append",
      parameters = Map(
        "path" -> "s3://bucket/data",
        "format" -> "avro"
      )
    )
    val result4 = loader.validateConfig(validConfig)
    result4.isValid shouldBe true
    result4.errors shouldBe empty
  }

  it should "handle different write modes" in {
    val loader = new S3Loader()

    val data = Seq(
      ("user1", 100)
    ).toDF("user", "amount")

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    // Append mode
    val appendConfig = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "append",
      parameters = Map(
        "path" -> s"$testOutputPath/append-output",
        "format" -> "parquet"
      )
    )
    val appendResult = loader.load(data, appendConfig, runContext)
    appendResult.writeMode shouldBe "append"

    // Overwrite mode
    val overwriteConfig = SinkConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      writeMode = "overwrite",
      parameters = Map(
        "path" -> s"$testOutputPath/overwrite-output",
        "format" -> "parquet"
      )
    )
    val overwriteResult = loader.load(data, overwriteConfig, runContext)
    overwriteResult.writeMode shouldBe "overwrite"
  }
}
