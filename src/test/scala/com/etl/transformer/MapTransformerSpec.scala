package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for MapTransformer.
 *
 * Tests cover:
 * - T077: Apply column expressions
 * - T078: Support rename and type conversion
 */
class MapTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MapTransformerSpec")
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

  // T077: Apply column expressions
  "MapTransformer" should "apply column expressions correctly" in {
    val transformer = new MapTransformer()

    val inputData = Seq(
      ("John", "Doe", 1000.0),
      ("Jane", "Smith", 1500.0),
      ("Bob", "Johnson", 2000.0)
    ).toDF("first_name", "last_name", "salary")

    val config = TransformationConfig(
      `type` = "map",
      parameters = Map(
        "expressions" -> "full_name:concat(first_name, ' ', last_name),annual_salary:salary * 12,bonus:salary * 0.1"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.columns should contain allOf ("first_name", "last_name", "salary", "full_name", "annual_salary", "bonus")

    val firstRow = result.orderBy("first_name").first()
    firstRow.getString(firstRow.fieldIndex("full_name")) shouldBe "John Doe"
    firstRow.getDouble(firstRow.fieldIndex("annual_salary")) shouldBe 12000.0
    firstRow.getDouble(firstRow.fieldIndex("bonus")) shouldBe 100.0

    transformer.transformationType shouldBe "map"
  }

  // T078: Support rename and type conversion
  it should "support column rename and type conversion" in {
    val transformer = new MapTransformer()

    val inputData = Seq(
      ("user1", "25", "100.5"),
      ("user2", "30", "200.75"),
      ("user3", "35", "300.25")
    ).toDF("username", "age_str", "amount_str")

    val config = TransformationConfig(
      `type` = "map",
      parameters = Map(
        "expressions" -> "user_id:username,age:CAST(age_str AS INT),amount:CAST(amount_str AS DOUBLE)"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result.columns should contain allOf ("username", "age_str", "amount_str", "user_id", "age", "amount")

    // Verify types
    val schema = result.schema
    schema("user_id").dataType.typeName shouldBe "string"
    schema("age").dataType.typeName shouldBe "integer"
    schema("amount").dataType.typeName shouldBe "double"

    // Verify values
    val firstRow = result.orderBy("username").first()
    firstRow.getString(firstRow.fieldIndex("user_id")) shouldBe "user1"
    firstRow.getInt(firstRow.fieldIndex("age")) shouldBe 25
    firstRow.getDouble(firstRow.fieldIndex("amount")) shouldBe 100.5
  }

  it should "validate configuration and detect missing parameters" in {
    val transformer = new MapTransformer()
    val schema = Seq(("col1", 123)).toDF().schema

    // Missing expressions
    val invalidConfig1 = TransformationConfig(
      `type` = "map",
      parameters = Map.empty
    )
    val result1 = transformer.validateConfig(invalidConfig1, schema)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: expressions")

    // Valid configuration
    val validConfig = TransformationConfig(
      `type` = "map",
      parameters = Map(
        "expressions" -> "new_col:_1 * 2"
      )
    )
    val result2 = transformer.validateConfig(validConfig, schema)
    result2.isValid shouldBe true
    result2.errors shouldBe empty
  }

  it should "update lineage metadata with map expressions" in {
    val transformer = new MapTransformer()

    val inputData = Seq(
      ("user1", 100)
    ).toDF("username", "amount")
      .withColumn("_lineage", lit("""{"sourceType":"kafka","sourceIdentifier":"events","extractionTimestamp":1234567890,"transformationChain":[]}"""))

    val config = TransformationConfig(
      `type` = "map",
      parameters = Map(
        "expressions" -> "doubled:amount * 2"
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
    lineageData should include("map")
    lineageData should include("transformationChain")
  }
}
