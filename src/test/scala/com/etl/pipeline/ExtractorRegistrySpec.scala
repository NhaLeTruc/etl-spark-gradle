package com.etl.pipeline

import com.etl.core._
import com.etl.extractor._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for ExtractorRegistry.
 *
 * Tests cover:
 * - T121: Resolve extractor by type
 */
class ExtractorRegistrySpec extends AnyFlatSpec with Matchers {

  "ExtractorRegistry" should "resolve extractor by type" in {
    val registry = new ExtractorRegistry()

    val kafkaExtractor = registry.get("kafka")
    kafkaExtractor.sourceType shouldBe "kafka"

    val postgresExtractor = registry.get("postgres")
    postgresExtractor.sourceType shouldBe "postgres"

    val mysqlExtractor = registry.get("mysql")
    mysqlExtractor.sourceType shouldBe "mysql"

    val s3Extractor = registry.get("s3")
    s3Extractor.sourceType shouldBe "s3"
  }

  it should "throw exception for unknown extractor type" in {
    val registry = new ExtractorRegistry()

    intercept[IllegalArgumentException] {
      registry.get("unknown")
    }
  }

  it should "support registering custom extractors" in {
    val registry = new ExtractorRegistry()

    val mockExtractor = new MockExtractor()
    registry.register("mock", mockExtractor)

    val retrieved = registry.get("mock")
    retrieved.sourceType shouldBe "mock"
  }

  it should "list all registered extractors" in {
    val registry = new ExtractorRegistry()

    val types = registry.list()
    types should contain allOf ("kafka", "postgres", "mysql", "s3")
  }

  class MockExtractor extends DataExtractor {
    override def sourceType: String = "mock"

    override def extract(config: SourceConfig)(implicit spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
      import spark.implicits._
      Seq(("key1", "value1"), ("key2", "value2")).toDF("key", "value")
    }

    override def validateConfig(config: SourceConfig): ValidationResult = {
      ValidationResult.valid()
    }
  }
}
