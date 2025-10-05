package com.etl.pipeline

import com.etl.core._
import com.etl.transformer._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for TransformerRegistry.
 *
 * Tests cover:
 * - T123: Resolve transformer by type
 */
class TransformerRegistrySpec extends AnyFlatSpec with Matchers {

  "TransformerRegistry" should "resolve transformer by type" in {
    val registry = new TransformerRegistry()

    val aggregationTransformer = registry.get("aggregation")
    aggregationTransformer.transformationType shouldBe "aggregation"

    val joinTransformer = registry.get("join")
    joinTransformer.transformationType shouldBe "join"

    val windowingTransformer = registry.get("windowing")
    windowingTransformer.transformationType shouldBe "windowing"

    val filterTransformer = registry.get("filter")
    filterTransformer.transformationType shouldBe "filter"

    val mapTransformer = registry.get("map")
    mapTransformer.transformationType shouldBe "map"
  }

  it should "throw exception for unknown transformer type" in {
    val registry = new TransformerRegistry()

    intercept[IllegalArgumentException] {
      registry.get("unknown")
    }
  }

  it should "support registering custom transformers" in {
    val registry = new TransformerRegistry()

    val mockTransformer = new MockTransformer()
    registry.register("mock", mockTransformer)

    val retrieved = registry.get("mock")
    retrieved.transformationType shouldBe "mock"
  }

  it should "list all registered transformers" in {
    val registry = new TransformerRegistry()

    val types = registry.list()
    types should contain allOf ("aggregation", "join", "windowing", "filter", "map")
  }

  class MockTransformer extends DataTransformer {
    override def transformationType: String = "mock"

    override def transform(
      input: org.apache.spark.sql.DataFrame,
      config: TransformationConfig,
      runContext: RunContext
    ): org.apache.spark.sql.DataFrame = input

    override def validateConfig(
      config: TransformationConfig,
      inputSchema: org.apache.spark.sql.types.StructType
    ): ValidationResult = ValidationResult.valid()
  }
}
