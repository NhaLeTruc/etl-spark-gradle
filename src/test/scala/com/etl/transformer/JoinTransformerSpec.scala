package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for JoinTransformer.
 *
 * Tests cover:
 * - T066: Perform inner join
 * - T067: Perform left/right/full joins
 * - T068: Merge lineage metadata
 */
class JoinTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("JoinTransformerSpec")
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

  // T066: Perform inner join
  "JoinTransformer" should "perform inner join correctly" in {
    val transformer = new JoinTransformer()

    val leftData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val rightData = Seq(
      (1, "alice@example.com"),
      (2, "bob@example.com"),
      (4, "david@example.com")
    ).toDF("id", "email")

    val config = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "inner",
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    // Store right DataFrame in SparkSession for transformer to access
    rightData.createOrReplaceTempView("right_table")

    val result = transformer.transform(leftData, config, runContext)

    result.count() shouldBe 2 // Only matching rows (1, 2)
    result.columns should contain allOf ("id", "name", "email")

    val rows = result.orderBy("id").collect()
    rows(0).getInt(0) shouldBe 1
    rows(0).getString(1) shouldBe "Alice"
    rows(1).getInt(0) shouldBe 2

    transformer.transformationType shouldBe "join"
  }

  // T067: Perform left/right/full joins
  it should "perform left, right, and full outer joins correctly" in {
    val transformer = new JoinTransformer()

    val leftData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val rightData = Seq(
      (1, "alice@example.com"),
      (2, "bob@example.com"),
      (4, "david@example.com")
    ).toDF("id", "email")

    rightData.createOrReplaceTempView("right_table")

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    // Left join
    val leftConfig = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "left",
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )
    val leftResult = transformer.transform(leftData, leftConfig, runContext)
    leftResult.count() shouldBe 3 // All left rows

    // Right join
    val rightConfig = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "right",
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )
    val rightResult = transformer.transform(leftData, rightConfig, runContext)
    rightResult.count() shouldBe 3 // All right rows

    // Full outer join
    val fullConfig = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "full",
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )
    val fullResult = transformer.transform(leftData, fullConfig, runContext)
    fullResult.count() shouldBe 4 // All unique rows from both sides
  }

  // T068: Merge lineage metadata
  it should "merge lineage metadata from both DataFrames" in {
    val transformer = new JoinTransformer()

    val leftData = Seq(
      (1, "Alice")
    ).toDF("id", "name")
      .withColumn("_lineage", lit("""{"sourceType":"postgres","sourceIdentifier":"users","extractionTimestamp":1234567890,"transformationChain":[]}"""))

    val rightData = Seq(
      (1, "alice@example.com")
    ).toDF("id", "email")
      .withColumn("_lineage", lit("""{"sourceType":"mysql","sourceIdentifier":"contacts","extractionTimestamp":1234567890,"transformationChain":[]}"""))

    rightData.createOrReplaceTempView("right_table")

    val config = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "inner",
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(leftData, config, runContext)

    result.columns should contain("_lineage")

    // Verify lineage contains join information
    val lineageData = result.select("_lineage").first().getString(0)
    lineageData should include("join")
    lineageData should include("transformationChain")
  }

  it should "validate configuration and detect missing parameters" in {
    val transformer = new JoinTransformer()
    val schema = Seq((1, "name")).toDF().schema

    // Missing joinType
    val invalidConfig1 = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "leftKey" -> "id",
        "rightKey" -> "id"
      )
    )
    val result1 = transformer.validateConfig(invalidConfig1, schema)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: joinType")

    // Missing leftKey
    val invalidConfig2 = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "inner",
        "rightKey" -> "id"
      )
    )
    val result2 = transformer.validateConfig(invalidConfig2, schema)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: leftKey")

    // Valid configuration
    val validConfig = TransformationConfig(
      `type` = "join",
      parameters = Map(
        "joinType" -> "inner",
        "leftKey" -> "_1",
        "rightKey" -> "id"
      )
    )
    val result3 = transformer.validateConfig(validConfig, schema)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }
}
