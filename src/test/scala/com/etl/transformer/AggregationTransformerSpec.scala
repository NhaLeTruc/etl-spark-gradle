package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for AggregationTransformer.
 *
 * Tests cover:
 * - T062: Apply groupBy and aggregations
 * - T063: Validate aggregation config
 * - T064: Update lineage chain
 */
class AggregationTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AggregationTransformerSpec")
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

  // T062: Apply groupBy and aggregations
  "AggregationTransformer" should "apply groupBy and aggregations correctly" in {
    val transformer = new AggregationTransformer()

    val inputData = Seq(
      ("2025-01-01", "product1", 100.0),
      ("2025-01-01", "product1", 150.0),
      ("2025-01-01", "product2", 200.0),
      ("2025-01-02", "product1", 120.0)
    ).toDF("date", "product", "amount")

    val config = TransformationConfig(
      `type` = "aggregation",
      parameters = Map(
        "groupBy" -> "date,product"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "amount", function = "sum", alias = "total_amount"),
        AggregateExpr(column = "amount", function = "avg", alias = "avg_amount"),
        AggregateExpr(column = "*", function = "count", alias = "count")
      ))
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.count() shouldBe 3
    result.columns should contain allOf ("date", "product", "total_amount", "avg_amount", "count")

    // Verify aggregation results
    val row1 = result.filter($"date" === "2025-01-01" && $"product" === "product1").first()
    row1.getDouble(row1.fieldIndex("total_amount")) shouldBe 250.0
    row1.getDouble(row1.fieldIndex("avg_amount")) shouldBe 125.0
    row1.getLong(row1.fieldIndex("count")) shouldBe 2

    transformer.transformationType shouldBe "aggregation"
  }

  // T063: Validate aggregation config
  it should "validate aggregation config and detect missing parameters" in {
    val transformer = new AggregationTransformer()
    val schema = Seq(("col1", "col2", 123)).toDF().schema

    // Missing groupBy
    val invalidConfig1 = TransformationConfig(
      `type` = "aggregation",
      parameters = Map.empty,
      aggregations = Some(List(
        AggregateExpr(column = "col2", function = "sum", alias = "total")
      ))
    )
    val result1 = transformer.validateConfig(invalidConfig1, schema)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: groupBy")

    // Missing aggregations
    val invalidConfig2 = TransformationConfig(
      `type` = "aggregation",
      parameters = Map("groupBy" -> "col1"),
      aggregations = None
    )
    val result2 = transformer.validateConfig(invalidConfig2, schema)
    result2.isValid shouldBe false
    result2.errors should contain("Missing aggregations list")

    // Invalid groupBy column
    val invalidConfig3 = TransformationConfig(
      `type` = "aggregation",
      parameters = Map("groupBy" -> "nonexistent_col"),
      aggregations = Some(List(
        AggregateExpr(column = "col2", function = "sum", alias = "total")
      ))
    )
    val result3 = transformer.validateConfig(invalidConfig3, schema)
    result3.isValid shouldBe false
    result3.errors.exists(_.contains("nonexistent_col")) shouldBe true

    // Valid configuration
    val validConfig = TransformationConfig(
      `type` = "aggregation",
      parameters = Map("groupBy" -> "col1"),
      aggregations = Some(List(
        AggregateExpr(column = "col2", function = "sum", alias = "total")
      ))
    )
    val result4 = transformer.validateConfig(validConfig, schema)
    result4.isValid shouldBe true
    result4.errors shouldBe empty
  }

  // T064: Update lineage chain
  it should "update lineage chain with transformation details" in {
    val transformer = new AggregationTransformer()

    val inputData = Seq(
      ("user1", 100),
      ("user1", 200),
      ("user2", 300)
    ).toDF("user", "amount")
      .withColumn("_lineage", lit("""{"sourceType":"kafka","sourceIdentifier":"test-topic","extractionTimestamp":1234567890,"transformationChain":[]}"""))

    val config = TransformationConfig(
      `type` = "aggregation",
      parameters = Map("groupBy" -> "user"),
      aggregations = Some(List(
        AggregateExpr(column = "amount", function = "sum", alias = "total")
      ))
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
    lineageData should include("aggregation")
    lineageData should include("transformationChain")
  }
}
