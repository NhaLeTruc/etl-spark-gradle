package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.nio.file.{Files, Paths}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.avro._

/**
 * Unit tests for S3Extractor.
 *
 * Tests cover:
 * - T058: Extract from S3 (using local filesystem for testing)
 * - T059: Validate configuration
 * - T060: Read Avro format
 *
 * Note: Uses local filesystem for testing instead of actual S3.
 */
class S3ExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val testDataPath = "/tmp/s3-extractor-test"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("S3ExtractorSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create test directory
    Files.createDirectories(Paths.get(testDataPath))

    // Create test Avro file
    createTestAvroFile()

    // Create test Parquet file
    createTestParquetFile()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    // Cleanup test files
    import scala.reflect.io.Directory
    val directory = new Directory(new java.io.File(testDataPath))
    directory.deleteRecursively()
  }

  private def createTestAvroFile(): Unit = {
    import spark.implicits._

    val data = Seq(
      ("user1", 25, "alice@example.com"),
      ("user2", 30, "bob@example.com"),
      ("user3", 35, "charlie@example.com")
    ).toDF("username", "age", "email")

    data.write
      .format("avro")
      .mode("overwrite")
      .save(s"$testDataPath/avro-data")
  }

  private def createTestParquetFile(): Unit = {
    import spark.implicits._

    val data = Seq(
      ("order1", 100.0, "2025-01-01"),
      ("order2", 200.0, "2025-01-02"),
      ("order3", 300.0, "2025-01-03")
    ).toDF("order_id", "amount", "date")

    data.write
      .format("parquet")
      .mode("overwrite")
      .save(s"$testDataPath/parquet-data")
  }

  // T058: Extract from S3 (local filesystem)
  "S3Extractor" should "extract data from S3 path and return valid DataFrame" in {
    val extractor = new S3Extractor()
    val config = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map(
        "path" -> s"$testDataPath/avro-data",
        "format" -> "avro"
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3
    result.columns should contain allOf ("username", "age", "email")

    val firstRow = result.orderBy("username").first()
    firstRow.getString(0) shouldBe "user1"
    firstRow.getInt(1) shouldBe 25

    extractor.sourceType shouldBe "s3"
  }

  // T059: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val extractor = new S3Extractor()

    // Missing path
    val invalidConfig1 = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map("format" -> "avro")
    )
    val result1 = extractor.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: path")

    // Missing format
    val invalidConfig2 = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map("path" -> "s3://bucket/data")
    )
    val result2 = extractor.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: format")

    // Invalid format
    val invalidConfig3 = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map(
        "path" -> "s3://bucket/data",
        "format" -> "invalid"
      )
    )
    val result3 = extractor.validateConfig(invalidConfig3)
    result3.isValid shouldBe false
    result3.errors should contain("Invalid format: invalid (supported: avro, parquet, json, csv)")

    // Valid configuration
    val validConfig = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map(
        "path" -> "s3://bucket/data",
        "format" -> "avro"
      )
    )
    val result4 = extractor.validateConfig(validConfig)
    result4.isValid shouldBe true
    result4.errors shouldBe empty
  }

  // T060: Read Avro format
  it should "read Avro format and parse schema correctly" in {
    val extractor = new S3Extractor()
    val config = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map(
        "path" -> s"$testDataPath/avro-data",
        "format" -> "avro"
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3

    // Verify schema was correctly parsed
    val schema = result.schema
    schema.fieldNames should contain allOf ("username", "age", "email")

    // Verify data types
    schema("username").dataType.typeName shouldBe "string"
    schema("age").dataType.typeName shouldBe "integer"
    schema("email").dataType.typeName shouldBe "string"

    // Verify data content
    val rows = result.orderBy("username").collect()
    rows(0).getString(0) shouldBe "user1"
    rows(1).getString(0) shouldBe "user2"
    rows(2).getString(0) shouldBe "user3"
  }

  it should "read Parquet format" in {
    val extractor = new S3Extractor()
    val config = SourceConfig(
      `type` = "s3",
      credentialsPath = "secret/s3",
      parameters = Map(
        "path" -> s"$testDataPath/parquet-data",
        "format" -> "parquet"
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3
    result.columns should contain allOf ("order_id", "amount", "date")
  }
}
