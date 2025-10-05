package com.etl.pipeline

import com.etl.core._
import com.etl.extractor._
import com.etl.transformer._
import com.etl.loader._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for PipelineExecutor.
 *
 * Tests cover:
 * - T114: Execute batch pipeline
 * - T115: Execute micro-batch pipeline
 * - T116: Handle extraction failure
 * - T117: Handle transformation failure
 * - T118: Handle load failure
 * - T119: Collect execution metrics
 */
class PipelineExecutorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PipelineExecutorSpec")
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

  // T114: Execute batch pipeline
  "PipelineExecutor" should "execute complete batch pipeline successfully" in {
    val executor = new PipelineExecutor()

    // Setup registries
    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-batch-pipeline",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("records" -> "100")
      ),
      transformations = List(
        TransformationConfig(
          `type` = "filter",
          parameters = Map("condition" -> "age > 18"),
          aggregations = None
        )
      ),
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("output" -> "test")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-001",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.pipelineId shouldBe "test-batch-pipeline"
    metrics.status shouldBe "SUCCESS"
    metrics.recordsExtracted should be > 0L
  }

  // T115: Execute micro-batch pipeline (streaming simulation)
  it should "execute micro-batch pipeline" in {
    val executor = new PipelineExecutor()

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-microbatch-pipeline",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("records" -> "50", "streaming" -> "true")
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("output" -> "test")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-002",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.status shouldBe "SUCCESS"
  }

  // T116: Handle extraction failure
  it should "handle extraction failure gracefully" in {
    val executor = new PipelineExecutor()

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-extraction-failure",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("simulate_failure" -> "true")
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("output" -> "test")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-003",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.status shouldBe "FAILED"
    metrics.errorDetails.isDefined shouldBe true
  }

  // T117: Handle transformation failure
  it should "handle transformation failure gracefully" in {
    val executor = new PipelineExecutor()

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-transformation-failure",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("records" -> "10")
      ),
      transformations = List(
        TransformationConfig(
          `type` = "invalid",
          parameters = Map("fail" -> "true"),
          aggregations = None
        )
      ),
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("output" -> "test")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-004",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.status shouldBe "FAILED"
  }

  // T118: Handle load failure
  it should "handle load failure gracefully" in {
    val executor = new PipelineExecutor()

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-load-failure",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("records" -> "10")
      ),
      transformations = List.empty,
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("simulate_failure" -> "true")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-005",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.status shouldBe "FAILED"
  }

  // T119: Collect execution metrics
  it should "collect detailed execution metrics" in {
    val executor = new PipelineExecutor()

    val extractorRegistry = new ExtractorRegistry()
    val transformerRegistry = new TransformerRegistry()
    val loaderRegistry = new LoaderRegistry()

    val config = PipelineConfig(
      pipelineId = "test-metrics",
      source = SourceConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        parameters = Map("records" -> "100")
      ),
      transformations = List(
        TransformationConfig(
          `type` = "filter",
          parameters = Map("condition" -> "age > 25"),
          aggregations = None
        )
      ),
      sink = SinkConfig(
        `type` = "mock",
        credentialsPath = "secret/mock",
        writeMode = "append",
        parameters = Map("output" -> "test")
      ),
      performance = PerformanceConfig(shufflePartitions = 200, batchSize = 10000),
      quality = QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    )

    val runContext = RunContext(
      pipelineId = config.pipelineId,
      runId = "test-run-006",
      spark = spark
    )

    val metrics = executor.execute(config, runContext, extractorRegistry, transformerRegistry, loaderRegistry)

    metrics.pipelineId shouldBe "test-metrics"
    metrics.runId shouldBe "test-run-006"
    metrics.recordsExtracted should be > 0L
    metrics.recordsTransformed should be >= 0L
    metrics.recordsLoaded should be >= 0L
    metrics.duration should be > 0L
    metrics.status should not be empty
  }
}
