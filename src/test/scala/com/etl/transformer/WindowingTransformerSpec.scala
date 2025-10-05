package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.sql.Timestamp

/**
 * Unit tests for WindowingTransformer.
 *
 * Tests cover:
 * - T070: Apply tumbling windows
 * - T071: Apply sliding windows
 * - T072: Validate window config
 */
class WindowingTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("WindowingTransformerSpec")
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

  // T070: Apply tumbling windows
  "WindowingTransformer" should "apply tumbling windows correctly" in {
    val transformer = new WindowingTransformer()

    val inputData = Seq(
      (Timestamp.valueOf("2025-01-01 10:00:00"), "event1", 100),
      (Timestamp.valueOf("2025-01-01 10:05:00"), "event2", 200),
      (Timestamp.valueOf("2025-01-01 10:15:00"), "event3", 300),
      (Timestamp.valueOf("2025-01-01 10:20:00"), "event4", 400)
    ).toDF("timestamp", "event", "value")

    val config = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "tumbling",
        "timeColumn" -> "timestamp",
        "windowDuration" -> "10 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "value", function = "sum", alias = "total_value")
      ))
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result should not be null
    result.columns should contain allOf ("window", "total_value")

    // Tumbling windows of 10 minutes should create non-overlapping windows
    val windowCount = result.count()
    windowCount should be >= 2L // At least 2 windows (10:00-10:10, 10:10-10:20, 10:20-10:30)

    transformer.transformationType shouldBe "windowing"
  }

  // T071: Apply sliding windows
  it should "apply sliding windows correctly" in {
    val transformer = new WindowingTransformer()

    val inputData = Seq(
      (Timestamp.valueOf("2025-01-01 10:00:00"), "event1", 100),
      (Timestamp.valueOf("2025-01-01 10:05:00"), "event2", 200),
      (Timestamp.valueOf("2025-01-01 10:15:00"), "event3", 300)
    ).toDF("timestamp", "event", "value")

    val config = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "sliding",
        "timeColumn" -> "timestamp",
        "windowDuration" -> "10 minutes",
        "slideDuration" -> "5 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "value", function = "sum", alias = "total_value")
      ))
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = transformer.transform(inputData, config, runContext)

    result should not be null
    result.columns should contain allOf ("window", "total_value")

    // Sliding windows should create overlapping windows
    val windowCount = result.count()
    windowCount should be >= 2L // Multiple overlapping windows
  }

  // T072: Validate window config
  it should "validate window configuration and detect missing parameters" in {
    val transformer = new WindowingTransformer()
    val schema = Seq((Timestamp.valueOf("2025-01-01 10:00:00"), 123)).toDF().schema

    // Missing windowType
    val invalidConfig1 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "timeColumn" -> "_1",
        "windowDuration" -> "10 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result1 = transformer.validateConfig(invalidConfig1, schema)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: windowType")

    // Missing timeColumn
    val invalidConfig2 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "tumbling",
        "windowDuration" -> "10 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result2 = transformer.validateConfig(invalidConfig2, schema)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: timeColumn")

    // Missing windowDuration
    val invalidConfig3 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "tumbling",
        "timeColumn" -> "_1"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result3 = transformer.validateConfig(invalidConfig3, schema)
    result3.isValid shouldBe false
    result3.errors should contain("Missing required parameter: windowDuration")

    // Missing slideDuration for sliding window
    val invalidConfig4 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "sliding",
        "timeColumn" -> "_1",
        "windowDuration" -> "10 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result4 = transformer.validateConfig(invalidConfig4, schema)
    result4.isValid shouldBe false
    result4.errors should contain("Sliding window requires slideDuration parameter")

    // Valid tumbling configuration
    val validConfig1 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "tumbling",
        "timeColumn" -> "_1",
        "windowDuration" -> "10 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result5 = transformer.validateConfig(validConfig1, schema)
    result5.isValid shouldBe true
    result5.errors shouldBe empty

    // Valid sliding configuration
    val validConfig2 = TransformationConfig(
      `type` = "windowing",
      parameters = Map(
        "windowType" -> "sliding",
        "timeColumn" -> "_1",
        "windowDuration" -> "10 minutes",
        "slideDuration" -> "5 minutes"
      ),
      aggregations = Some(List(
        AggregateExpr(column = "_2", function = "sum", alias = "total")
      ))
    )
    val result6 = transformer.validateConfig(validConfig2, schema)
    result6.isValid shouldBe true
    result6.errors shouldBe empty
  }
}
