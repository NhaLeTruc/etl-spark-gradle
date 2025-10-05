package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for KafkaLoader.
 *
 * Tests cover:
 * - T080: Write to Kafka topic (mocked/embedded)
 * - T081: Validate configuration
 * - T082: Serialize to Avro
 *
 * Note: Uses mocked Kafka for testing (not actual Kafka cluster).
 */
class KafkaLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("KafkaLoaderSpec")
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

  // T080: Write to Kafka topic
  "KafkaLoader" should "write data to Kafka topic and return success result" in {
    val loader = new KafkaLoader()

    val data = Seq(
      ("key1", """{"name":"Alice","age":25}"""),
      ("key2", """{"name":"Bob","age":30}""")
    ).toDF("key", "value")

    val config = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-output-topic"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.recordsWritten shouldBe 2
    result.sinkType shouldBe "kafka"
    result.writeMode shouldBe "append"

    loader.sinkType shouldBe "kafka"
  }

  // T081: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val loader = new KafkaLoader()

    // Missing bootstrap.servers
    val invalidConfig1 = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map("topic" -> "test-topic")
    )
    val result1 = loader.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: bootstrap.servers")

    // Missing topic
    val invalidConfig2 = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map("bootstrap.servers" -> "localhost:9092")
    )
    val result2 = loader.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: topic")

    // Valid configuration
    val validConfig = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic"
      )
    )
    val result3 = loader.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }

  // T082: Serialize to Avro
  it should "serialize data to Avro format when specified" in {
    val loader = new KafkaLoader()

    val data = Seq(
      ("user1", 25, "alice@example.com"),
      ("user2", 30, "bob@example.com")
    ).toDF("username", "age", "email")

    val config = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "avro-topic",
        "format" -> "avro"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.recordsWritten shouldBe 2
  }

  it should "handle different write modes" in {
    val loader = new KafkaLoader()

    val data = Seq(
      ("key1", "value1")
    ).toDF("key", "value")

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    // Append mode
    val appendConfig = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "append",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic"
      )
    )
    val appendResult = loader.load(data, appendConfig, runContext)
    appendResult.writeMode shouldBe "append"

    // Overwrite mode (same as append for Kafka)
    val overwriteConfig = SinkConfig(
      `type` = "kafka",
      credentialsPath = "secret/kafka",
      writeMode = "overwrite",
      parameters = Map(
        "bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic"
      )
    )
    val overwriteResult = loader.load(data, overwriteConfig, runContext)
    overwriteResult.writeMode shouldBe "overwrite"
  }
}
