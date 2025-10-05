package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Contract tests for DataExtractor interface.
 * These tests define the expected behavior that all extractor implementations must satisfy.
 *
 * Note: These tests will fail initially (TDD Red phase) until DataExtractor trait is created.
 */
class DataExtractorContractSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataExtractorContractSpec")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // Mock implementation for testing
  class MockExtractor extends DataExtractor {
    override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      Seq(
        ("data1", "value1"),
        ("data2", "value2")
      ).toDF("key", "value")
    }

    override def validateConfig(config: SourceConfig): ValidationResult = {
      if (config.parameters.contains("required_param")) {
        ValidationResult.valid
      } else {
        ValidationResult.invalid("Missing required_param")
      }
    }

    override def sourceType: String = "mock"
  }

  // T023: Contract test - extract returns valid DataFrame
  "DataExtractor.extract" should "return a non-null DataFrame with expected columns" in {
    val extractor = new MockExtractor()
    val config = SourceConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      parameters = Map("required_param" -> "value")
    )

    val result = extractor.extract(config)(spark)

    result should not be null
    result.columns should contain allOf ("key", "value")
    result.count() shouldBe 2
  }

  // T024: Contract test - validateConfig detects missing parameters
  it should "detect missing required parameters" in {
    val extractor = new MockExtractor()
    val invalidConfig = SourceConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      parameters = Map.empty // Missing required_param
    )

    val result = extractor.validateConfig(invalidConfig)

    result.isValid shouldBe false
    result.errors should not be empty
    result.errors.head should include("required_param")
  }

  // T025: Contract test - validateConfig accepts valid parameters
  it should "accept valid configuration" in {
    val extractor = new MockExtractor()
    val validConfig = SourceConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      parameters = Map("required_param" -> "value")
    )

    val result = extractor.validateConfig(validConfig)

    result.isValid shouldBe true
    result.errors shouldBe empty
  }

  // T026: Contract test - extract handles connection failure (mocked)
  it should "throw ExtractionException on connection failure" in {
    class FailingExtractor extends DataExtractor {
      override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
        throw new ExtractionException("mock", "Connection failed", new RuntimeException("Network error"))
      }

      override def validateConfig(config: SourceConfig): ValidationResult = ValidationResult.valid
      override def sourceType: String = "failing-mock"
    }

    val extractor = new FailingExtractor()
    val config = SourceConfig("mock", "secret/mock", Map.empty)

    val exception = intercept[ExtractionException] {
      extractor.extract(config)(spark)
    }

    exception.getMessage should include("mock")
    exception.getMessage should include("Connection failed")
  }

  // T027: Contract test - extract embeds lineage metadata
  it should "embed lineage metadata in extracted DataFrame" in {
    class LineageExtractor extends DataExtractor {
      override def extract(config: SourceConfig)(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        import org.apache.spark.sql.functions._

        val data = Seq(("data1", "value1")).toDF("key", "value")

        // Add lineage metadata columns
        data.withColumn("sourceSystem", lit(sourceType))
          .withColumn("extractionTimestamp", lit(System.currentTimeMillis()))
          .withColumn("pipelineId", lit("test-pipeline"))
          .withColumn("runId", lit("test-run-id"))
      }

      override def validateConfig(config: SourceConfig): ValidationResult = ValidationResult.valid
      override def sourceType: String = "lineage-mock"
    }

    val extractor = new LineageExtractor()
    val config = SourceConfig("mock", "secret/mock", Map.empty)

    val result = extractor.extract(config)(spark)

    result.columns should contain allOf ("sourceSystem", "extractionTimestamp", "pipelineId", "runId")

    val firstRow = result.first()
    firstRow.getAs[String]("sourceSystem") shouldBe "lineage-mock"
  }
}
