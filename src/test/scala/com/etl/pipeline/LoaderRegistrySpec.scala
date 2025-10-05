package com.etl.pipeline

import com.etl.core._
import com.etl.loader._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for LoaderRegistry.
 *
 * Tests cover:
 * - T125: Resolve loader by type
 */
class LoaderRegistrySpec extends AnyFlatSpec with Matchers {

  "LoaderRegistry" should "resolve loader by type" in {
    val registry = new LoaderRegistry()

    val kafkaLoader = registry.get("kafka")
    kafkaLoader.sinkType shouldBe "kafka"

    val postgresLoader = registry.get("postgres")
    postgresLoader.sinkType shouldBe "postgres"

    val mysqlLoader = registry.get("mysql")
    mysqlLoader.sinkType shouldBe "mysql"

    val s3Loader = registry.get("s3")
    s3Loader.sinkType shouldBe "s3"
  }

  it should "throw exception for unknown loader type" in {
    val registry = new LoaderRegistry()

    intercept[IllegalArgumentException] {
      registry.get("unknown")
    }
  }

  it should "support registering custom loaders" in {
    val registry = new LoaderRegistry()

    val mockLoader = new MockLoader()
    registry.register("mock", mockLoader)

    val retrieved = registry.get("mock")
    retrieved.sinkType shouldBe "mock"
  }

  it should "list all registered loaders" in {
    val registry = new LoaderRegistry()

    val types = registry.list()
    types should contain allOf ("kafka", "postgres", "mysql", "s3")
  }

  class MockLoader extends DataLoader {
    override def sinkType: String = "mock"

    override def load(
      data: org.apache.spark.sql.DataFrame,
      config: SinkConfig,
      runContext: RunContext
    ): LoadResult = {
      LoadResult.success(
        recordsWritten = data.count(),
        sinkType = sinkType,
        writeMode = config.writeMode
      )
    }

    override def validateConfig(config: SinkConfig): ValidationResult = {
      ValidationResult.valid()
    }
  }
}
