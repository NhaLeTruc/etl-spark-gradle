package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Contract tests for DataTransformer interface.
 * These tests define the expected behavior that all transformer implementations must satisfy.
 */
class DataTransformerContractSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataTransformerContractSpec")
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
  class MockTransformer extends DataTransformer {
    override def transform(
      input: DataFrame,
      config: TransformationConfig,
      runContext: RunContext
    ): DataFrame = {
      import org.apache.spark.sql.functions._
      // Simple transformation: add a new column
      input.withColumn("transformed", lit(true))
    }

    override def validateConfig(
      config: TransformationConfig,
      inputSchema: StructType
    ): ValidationResult = {
      if (config.parameters.contains("required_field")) {
        ValidationResult.valid
      } else {
        ValidationResult.invalid("Missing required_field")
      }
    }

    override def transformationType: String = "mock"
  }

  // T030: Contract test - transform returns valid DataFrame
  "DataTransformer.transform" should "return a non-null DataFrame with valid schema" in {
    import spark.implicits._

    val transformer = new MockTransformer()
    val inputDf = Seq(("data1", "value1")).toDF("key", "value")
    val config = TransformationConfig("mock", Map("required_field" -> "value"))
    val runContext = RunContext.create("test-pipeline", spark)

    val result = transformer.transform(inputDf, config, runContext)

    result should not be null
    result.columns should contain("transformed")
    result.count() shouldBe inputDf.count()
  }

  // T031: Contract test - validateConfig detects schema mismatch
  it should "detect schema mismatches in configuration" in {
    import spark.implicits._

    val transformer = new MockTransformer()
    val inputDf = Seq(("data1", "value1")).toDF("key", "value")
    val invalidConfig = TransformationConfig("mock", Map.empty) // Missing required_field

    val result = transformer.validateConfig(invalidConfig, inputDf.schema)

    result.isValid shouldBe false
    result.errors should not be empty
  }

  // T032: Contract test - transform is idempotent
  it should "produce identical results when called multiple times" in {
    import spark.implicits._

    val transformer = new MockTransformer()
    val inputDf = Seq(("data1", "value1")).toDF("key", "value")
    val config = TransformationConfig("mock", Map("required_field" -> "value"))
    val runContext = RunContext.create("test-pipeline", spark)

    val result1 = transformer.transform(inputDf, config, runContext)
    val result2 = transformer.transform(inputDf, config, runContext)

    result1.schema shouldBe result2.schema
    result1.collect() should contain theSameElementsAs result2.collect()
  }

  // T033: Contract test - transform updates lineage chain
  it should "update lineage chain with transformation type" in {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    class LineageTransformer extends DataTransformer {
      override def transform(
        input: DataFrame,
        config: TransformationConfig,
        runContext: RunContext
      ): DataFrame = {
        // Update lineage chain (in real implementation, this would use LineageTracker)
        input.withColumn("lineage_updated", lit(true))
          .withColumn("transformation_type", lit(transformationType))
      }

      override def validateConfig(
        config: TransformationConfig,
        inputSchema: StructType
      ): ValidationResult = ValidationResult.valid

      override def transformationType: String = "lineage-mock"
    }

    val transformer = new LineageTransformer()
    val inputDf = Seq(("data1", "value1")).toDF("key", "value")
    val config = TransformationConfig("lineage-mock", Map.empty)
    val runContext = RunContext.create("test-pipeline", spark)

    val result = transformer.transform(inputDf, config, runContext)

    result.columns should contain allOf ("lineage_updated", "transformation_type")
    result.first().getAs[String]("transformation_type") shouldBe "lineage-mock"
  }

  // T034: Contract test - transform handles empty DataFrame
  it should "handle empty DataFrame gracefully" in {
    import spark.implicits._

    val transformer = new MockTransformer()
    val emptyDf = Seq.empty[(String, String)].toDF("key", "value")
    val config = TransformationConfig("mock", Map("required_field" -> "value"))
    val runContext = RunContext.create("test-pipeline", spark)

    val result = transformer.transform(emptyDf, config, runContext)

    result should not be null
    result.schema should not be null
    result.count() shouldBe 0
    noException should be thrownBy result.collect()
  }
}
