package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for KafkaExtractor.
 *
 * Tests cover:
 * - T046: Extract from Kafka topic (mocked/embedded)
 * - T047: Validate configuration
 * - T048: Embed lineage metadata
 *
 * Note: Uses mocked Kafka source for testing (not actual Kafka cluster).
 */
class KafkaExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("KafkaExtractorSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // T046: Extract from Kafka topic
  "KafkaExtractor" should "extract data from Kafka topic and return valid DataFrame" in {
    val extractor = new KafkaExtractor()
    val config = SourceConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic",
        "startingOffsets" -> "earliest"
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.columns should contain allOf ("key", "value", "topic", "partition", "offset", "timestamp")
    extractor.sourceType shouldBe "kafka"
  }

  // T047: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val extractor = new KafkaExtractor()

    // Missing bootstrap.servers
    val invalidConfig1 = SourceConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      parameters = Map("topic" -> "test-topic")
    )
    val result1 = extractor.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: bootstrap.servers")

    // Missing topic
    val invalidConfig2 = SourceConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      parameters = Map("bootstrap.servers" -> "localhost:9092")
    )
    val result2 = extractor.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: topic")

    // Valid configuration
    val validConfig = SourceConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic"
      )
    )
    val result3 = extractor.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }

  // T048: Embed lineage metadata
  it should "embed lineage metadata in DataFrame" in {
    val extractor = new KafkaExtractor()
    val config = SourceConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic",
        "startingOffsets" -> "earliest"
      )
    )

    val result = extractor.extract(config)

    // Check for lineage metadata column
    result.columns should contain("_lineage")

    // Verify lineage metadata contains source information
    val lineageData = result.select("_lineage").first().getString(0)
    lineageData should include("kafka")
    lineageData should include("test-topic")
  }
}
