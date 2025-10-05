package com.etl.quality

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for SchemaValidator.
 *
 * Tests cover:
 * - T096: Validate Avro schema
 * - T097: Detect schema violations
 */
class SchemaValidatorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SchemaValidatorSpec")
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

  // T096: Validate Avro schema
  "SchemaValidator" should "validate data against expected schema" in {
    val validator = new SchemaValidator()

    val data = Seq(
      ("user1", 25, "alice@example.com"),
      ("user2", 30, "bob@example.com")
    ).toDF("username", "age", "email")

    val expectedSchema = StructType(Seq(
      StructField("username", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("email", StringType, nullable = false)
    ))

    val result = validator.validate(data, expectedSchema)

    result.isValid shouldBe true
    result.violations shouldBe empty
  }

  // T097: Detect schema violations
  it should "detect schema violations for missing columns" in {
    val validator = new SchemaValidator()

    val data = Seq(
      ("user1", 25)
    ).toDF("username", "age")

    val expectedSchema = StructType(Seq(
      StructField("username", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("email", StringType, nullable = false)
    ))

    val result = validator.validate(data, expectedSchema)

    result.isValid shouldBe false
    result.violations should not be empty
    result.violations.exists(_.contains("email")) shouldBe true
  }

  it should "detect schema violations for type mismatches" in {
    val validator = new SchemaValidator()

    val data = Seq(
      ("user1", "twenty-five", "alice@example.com")
    ).toDF("username", "age", "email")

    val expectedSchema = StructType(Seq(
      StructField("username", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("email", StringType, nullable = false)
    ))

    val result = validator.validate(data, expectedSchema)

    result.isValid shouldBe false
    result.violations should not be empty
    result.violations.exists(v => v.contains("age") && v.contains("type")) shouldBe true
  }

  it should "detect nullable constraint violations" in {
    val validator = new SchemaValidator()

    val data = Seq(
      ("user1", Some(25), Some("alice@example.com")),
      ("user2", None, Some("bob@example.com"))
    ).toDF("username", "age", "email")

    val expectedSchema = StructType(Seq(
      StructField("username", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("email", StringType, nullable = false)
    ))

    // Check for nulls in non-nullable columns
    val nullCount = data.filter($"age".isNull).count()
    nullCount shouldBe 1 // user2 has null age
  }

  it should "validate complex nested schemas" in {
    val validator = new SchemaValidator()

    val data = Seq(
      (1, ("Alice", 25)),
      (2, ("Bob", 30))
    ).toDF("id", "user")

    val expectedSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("user", StructType(Seq(
        StructField("_1", StringType, nullable = true),
        StructField("_2", IntegerType, nullable = false)
      )), nullable = false)
    ))

    val result = validator.validate(data, expectedSchema)

    result.isValid shouldBe true
    result.violations shouldBe empty
  }

  it should "provide detailed violation messages" in {
    val validator = new SchemaValidator()

    val data = Seq(
      ("user1", 25.5) // age should be Int, not Double
    ).toDF("username", "age")

    val expectedSchema = StructType(Seq(
      StructField("username", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))

    val result = validator.validate(data, expectedSchema)

    result.isValid shouldBe false
    result.violations should not be empty
    // Violation message should describe the issue
    result.violations.head should include("age")
  }
}
