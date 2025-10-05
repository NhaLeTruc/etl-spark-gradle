package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for FilterTransformer.
 *
 * Tests cover:
 * - T074: Apply SQL condition
 * - T075: Validate SQL expression
 */
class FilterTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("FilterTransformerSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  import spark.implicits._

  // T074: Apply SQL condition
  "FilterTransformer" should "apply SQL condition and filter rows correctly" in {
    val transformer = new FilterTransformer()

    val inputData = Seq(
      ("user1", 25, "active"),
      ("user2", 30, "inactive"),
      ("user3", 35, "active"),
      ("user4", 40, "active")
    ).toDF("username", "age", "status")

    val config = TransformationConfig(
      `type` = "filter",
      parameters = Map(
        "condition" -> "status = 'active' AND age > 30"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.count() shouldBe 2 // user3 and user4
    result.columns should contain allOf ("username", "age", "status")

    val rows = result.orderBy("username").collect()
    rows(0).getString(0) shouldBe "user3"
    rows(1).getString(0) shouldBe "user4"

    transformer.transformationType shouldBe "filter"
  }

  it should "handle complex SQL expressions" in {
    val transformer = new FilterTransformer()

    val inputData = Seq(
      ("product1", 100.0, "electronics"),
      ("product2", 50.0, "books"),
      ("product3", 200.0, "electronics"),
      ("product4", 30.0, "books")
    ).toDF("product", "price", "category")

    val config = TransformationConfig(
      `type` = "filter",
      parameters = Map(
        "condition" -> "(category = 'electronics' AND price > 150) OR (category = 'books' AND price < 40)"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.count() shouldBe 2 // product3 (electronics > 150) and product4 (books < 40)
  }

  // T075: Validate SQL expression
  it should "validate SQL expression and detect missing parameters" in {
    val transformer = new FilterTransformer()
    val schema = Seq(("col1", 123)).toDF().schema

    // Missing condition
    val invalidConfig1 = TransformationConfig(
      `type` = "filter",
      parameters = Map.empty
    )
    val result1 = transformer.validateConfig(invalidConfig1, schema)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: condition")

    // Valid configuration
    val validConfig = TransformationConfig(
      `type` = "filter",
      parameters = Map(
        "condition" -> "_1 = 'test'"
      )
    )
    val result2 = transformer.validateConfig(validConfig, schema)
    result2.isValid shouldBe true
    result2.errors shouldBe empty
  }

  it should "update lineage metadata with filter condition" in {
    val transformer = new FilterTransformer()

    val inputData = Seq(
      ("user1", 25),
      ("user2", 35)
    ).toDF("username", "age")
      .withColumn("_lineage", lit("""{"sourceType":"postgres","sourceIdentifier":"users","extractionTimestamp":1234567890,"transformationChain":[]}"""))

    val config = TransformationConfig(
      `type` = "filter",
      parameters = Map(
        "condition" -> "age > 30"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.columns should contain("_lineage")

    // Verify lineage was updated
    val lineageData = result.select("_lineage").first().getString(0)
    lineageData should include("filter")
    lineageData should include("transformationChain")
  }
}
