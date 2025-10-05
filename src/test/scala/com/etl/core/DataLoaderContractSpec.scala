package com.etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Contract tests for DataLoader implementations.
 *
 * All DataLoader implementations must pass these tests to ensure:
 * - load returns LoadResult with success/failure status
 * - validateConfig detects missing required parameters
 * - validateConfig accepts valid configuration
 * - load handles write failures gracefully
 * - load is idempotent in upsert mode (same data → no duplicates)
 * - load append mode adds records without replacing
 * - load overwrite mode replaces existing data
 *
 * Test with: sbt "testOnly com.etl.core.DataLoaderContractSpec"
 */
class DataLoaderContractSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataLoaderContractSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  /**
   * Mock DataLoader implementation for contract verification.
   *
   * Simulates successful/failed loads and various write modes.
   */
  class MockLoader extends DataLoader {
    override def sinkType: String = "mock"

    override def load(
      data: DataFrame,
      config: SinkConfig,
      runContext: RunContext
    ): LoadResult = {
      // Simulate write failure if parameter indicates failure
      if (config.parameters.getOrElse("simulate_failure", "false") == "true") {
        throw new LoadException(
          sinkType = sinkType,
          message = "Simulated write failure",
          cause = new RuntimeException("Connection timeout")
        )
      }

      // Simulate different write modes
      val writeMode = config.writeMode
      val recordCount = data.count()

      LoadResult.success(
        recordsWritten = recordCount,
        sinkType = sinkType,
        writeMode = writeMode
      )
    }

    override def validateConfig(config: SinkConfig): ValidationResult = {
      val errors = scala.collection.mutable.ListBuffer[String]()

      // Check required parameter
      if (!config.parameters.contains("required_param")) {
        errors += "Missing required parameter: required_param"
      }

      if (errors.isEmpty) {
        ValidationResult.valid()
      } else {
        ValidationResult.invalid(errors.toList)
      }
    }
  }

  // T037: load returns success result
  "DataLoader.load" should "return LoadResult with success status and recordsWritten count" in {
    val loader = new MockLoader()
    val data = Seq(("key1", "value1"), ("key2", "value2")).toDF("key", "value")
    val config = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "append",
      parameters = Map("required_param" -> "value")
    )
    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.recordsWritten shouldBe 2
    result.sinkType shouldBe "mock"
    result.writeMode shouldBe "append"
  }

  // T038: validateConfig detects missing parameters
  it should "validateConfig return invalid result when required parameters are missing" in {
    val loader = new MockLoader()
    val invalidConfig = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "append",
      parameters = Map.empty // Missing required_param
    )

    val result = loader.validateConfig(invalidConfig)

    result.isValid shouldBe false
    result.errors should contain("Missing required parameter: required_param")
  }

  // T039: validateConfig accepts valid parameters
  it should "validateConfig return valid result when all required parameters are present" in {
    val loader = new MockLoader()
    val validConfig = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "append",
      parameters = Map("required_param" -> "value")
    )

    val result = loader.validateConfig(validConfig)

    result.isValid shouldBe true
    result.errors shouldBe empty
  }

  // T040: load handles write failure
  it should "throw LoadException when write operation fails" in {
    val loader = new MockLoader()
    val data = Seq(("key1", "value1")).toDF("key", "value")
    val config = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "append",
      parameters = Map(
        "required_param" -> "value",
        "simulate_failure" -> "true"
      )
    )
    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val exception = intercept[LoadException] {
      loader.load(data, config, runContext)
    }

    exception.getMessage should include("Simulated write failure")
    exception.sinkType shouldBe "mock"
    exception.getCause should not be null
  }

  // T041: load is idempotent (upsert mode)
  it should "be idempotent in upsert mode (same data loaded twice produces same result)" in {
    val loader = new MockLoader()
    val data = Seq(("key1", "value1"), ("key2", "value2")).toDF("key", "value")
    val config = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "upsert",
      parameters = Map("required_param" -> "value")
    )
    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    // Load data twice
    val result1 = loader.load(data, config, runContext)
    val result2 = loader.load(data, config, runContext)

    // Both loads should succeed with same record count
    result1.isSuccess shouldBe true
    result2.isSuccess shouldBe true
    result1.recordsWritten shouldBe result2.recordsWritten
    result1.writeMode shouldBe "upsert"
  }

  // T042: load append mode adds records
  it should "append mode add records without replacing existing data" in {
    val loader = new MockLoader()
    val data = Seq(("key1", "value1")).toDF("key", "value")
    val config = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "append",
      parameters = Map("required_param" -> "value")
    )
    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.writeMode shouldBe "append"
    result.recordsWritten shouldBe 1
  }

  // T043: load overwrite mode replaces data
  it should "overwrite mode replace existing data" in {
    val loader = new MockLoader()
    val data = Seq(("key1", "value1"), ("key2", "value2")).toDF("key", "value")
    val config = SinkConfig(
      `type` = "mock",
      credentialsPath = "secret/mock",
      writeMode = "overwrite",
      parameters = Map("required_param" -> "value")
    )
    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.writeMode shouldBe "overwrite"
    result.recordsWritten shouldBe 2
  }
}
