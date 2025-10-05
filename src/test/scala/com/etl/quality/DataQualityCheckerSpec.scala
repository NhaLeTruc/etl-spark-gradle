package com.etl.quality

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for DataQualityChecker.
 *
 * Tests cover:
 * - T105: Validate null columns
 * - T106: Calculate quality metrics (null rate, duplicate rate)
 */
class DataQualityCheckerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataQualityCheckerSpec")
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

  // T105: Validate null columns
  "DataQualityChecker" should "validate null constraints and detect violations" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      (Some("user1"), Some(25), Some("alice@example.com")),
      (Some("user2"), None, Some("bob@example.com")),
      (None, Some(35), Some("charlie@example.com")),
      (Some("user4"), Some(40), None)
    ).toDF("username", "age", "email")

    val qualityConfig = QualityConfig(
      schemaValidation = true,
      nullChecks = List("username", "age", "email"),
      duplicateCheck = false,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    metrics.totalRecords shouldBe 4
    metrics.nullViolations should be > 0L
    metrics.nullRate should be > 0.0
  }

  // T106: Calculate quality metrics
  it should "calculate null rate correctly" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      (Some("user1"), Some(25)),
      (Some("user2"), None),
      (None, Some(30)),
      (Some("user4"), Some(35))
    ).toDF("username", "age")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List("username", "age"),
      duplicateCheck = false,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    metrics.totalRecords shouldBe 4
    metrics.nullViolations shouldBe 2 // One null in username, one in age
    metrics.nullRate shouldBe 0.25 // 2 nulls out of 8 total values (4 records * 2 columns)
  }

  it should "calculate duplicate rate correctly" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      ("user1", 25),
      ("user2", 30),
      ("user1", 25), // Duplicate
      ("user3", 35),
      ("user2", 30)  // Duplicate
    ).toDF("username", "age")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List.empty,
      duplicateCheck = true,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    metrics.totalRecords shouldBe 5
    metrics.duplicateRecords shouldBe 2 // 2 duplicate records
    metrics.duplicateRate shouldBe 0.4 // 2 out of 5
  }

  it should "identify specific columns with high null rates" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      (Some("user1"), None, Some("alice@example.com")),
      (Some("user2"), None, Some("bob@example.com")),
      (Some("user3"), None, Some("charlie@example.com")),
      (Some("user4"), Some(40), None)
    ).toDF("username", "age", "email")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List("username", "age", "email"),
      duplicateCheck = false,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    // Age column has 3 nulls out of 4 records
    val columnMetrics = checker.getColumnNullRates(data, qualityConfig.nullChecks)
    columnMetrics("age") shouldBe 0.75
    columnMetrics("username") shouldBe 0.0
  }

  it should "return passing metrics for clean data" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      ("user1", 25, "alice@example.com"),
      ("user2", 30, "bob@example.com"),
      ("user3", 35, "charlie@example.com")
    ).toDF("username", "age", "email")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List("username", "age", "email"),
      duplicateCheck = true,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    metrics.totalRecords shouldBe 3
    metrics.nullViolations shouldBe 0
    metrics.nullRate shouldBe 0.0
    metrics.duplicateRecords shouldBe 0
    metrics.duplicateRate shouldBe 0.0
  }

  it should "provide detailed quality report" in {
    val checker = new DataQualityChecker()

    val data = Seq(
      (Some("user1"), Some(25)),
      (Some("user2"), None),
      (None, Some(30))
    ).toDF("username", "age")

    val qualityConfig = QualityConfig(
      schemaValidation = false,
      nullChecks = List("username", "age"),
      duplicateCheck = false,
      customRules = None
    )

    val metrics = checker.checkQuality(data, qualityConfig)

    val report = checker.getReport(metrics)
    report should include("Total Records")
    report should include("Null Violations")
    report should include("Null Rate")
  }
}
