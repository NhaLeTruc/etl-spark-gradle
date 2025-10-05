package com.etl.quality

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.nio.file.{Files, Paths}

/**
 * Unit tests for QuarantineWriter.
 *
 * Tests cover:
 * - T099: Write invalid records with diagnostics
 * - T100: Format error metadata
 */
class QuarantineWriterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val quarantinePath = "/tmp/quarantine-test"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("QuarantineWriterSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create test directory
    Files.createDirectories(Paths.get(quarantinePath))
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    // Cleanup test files
    import scala.reflect.io.Directory
    val directory = new Directory(new java.io.File(quarantinePath))
    directory.deleteRecursively()
  }

  import spark.implicits._

  // T099: Write invalid records with diagnostics
  "QuarantineWriter" should "write invalid records with error diagnostics" in {
    val writer = new QuarantineWriter()

    val invalidRecords = Seq(
      (1, "user1", null, "Missing email"),
      (2, "user2", "invalid-email", "Invalid email format"),
      (3, null, "user3@example.com", "Missing username")
    ).toDF("id", "username", "email", "error_reason")

    val outputPath = s"$quarantinePath/invalid-records"

    writer.writeQuarantine(
      data = invalidRecords,
      path = outputPath,
      pipelineId = "test-pipeline",
      runId = "test-run-001"
    )

    // Verify quarantine data was written
    val quarantined = spark.read.parquet(outputPath)
    quarantined.count() shouldBe 3
    quarantined.columns should contain allOf ("id", "username", "email", "error_reason", "quarantine_timestamp", "pipeline_id", "run_id")
  }

  // T100: Format error metadata
  it should "format error metadata with timestamp and context" in {
    val writer = new QuarantineWriter()

    val invalidRecords = Seq(
      (1, "user1", "Schema violation")
    ).toDF("id", "username", "error")

    val outputPath = s"$quarantinePath/formatted-errors"

    writer.writeQuarantine(
      data = invalidRecords,
      path = outputPath,
      pipelineId = "pipeline-123",
      runId = "run-456"
    )

    // Verify metadata columns
    val quarantined = spark.read.parquet(outputPath)

    quarantined.columns should contain("quarantine_timestamp")
    quarantined.columns should contain("pipeline_id")
    quarantined.columns should contain("run_id")

    val firstRow = quarantined.first()
    firstRow.getAs[String]("pipeline_id") shouldBe "pipeline-123"
    firstRow.getAs[String]("run_id") shouldBe "run-456"
    firstRow.getAs[Long]("quarantine_timestamp") should be > 0L
  }

  it should "append to existing quarantine data" in {
    val writer = new QuarantineWriter()

    val batch1 = Seq(
      (1, "user1", "Error 1")
    ).toDF("id", "username", "error")

    val batch2 = Seq(
      (2, "user2", "Error 2")
    ).toDF("id", "username", "error")

    val outputPath = s"$quarantinePath/append-test"

    // Write first batch
    writer.writeQuarantine(
      data = batch1,
      path = outputPath,
      pipelineId = "test-pipeline",
      runId = "run-001"
    )

    // Write second batch (append)
    writer.writeQuarantine(
      data = batch2,
      path = outputPath,
      pipelineId = "test-pipeline",
      runId = "run-002"
    )

    // Verify both batches are present
    val quarantined = spark.read.parquet(outputPath)
    quarantined.count() shouldBe 2

    val runIds = quarantined.select("run_id").distinct().collect().map(_.getString(0))
    runIds should contain allOf ("run-001", "run-002")
  }

  it should "handle records with complex error details" in {
    val writer = new QuarantineWriter()

    val invalidRecords = Seq(
      (1, """{"field":"age","expected":"int","actual":"string","value":"twenty-five"}"""),
      (2, """{"field":"email","rule":"regex","pattern":"^[a-z]+@[a-z]+\\.[a-z]+$","value":"INVALID"}""")
    ).toDF("id", "error_details")

    val outputPath = s"$quarantinePath/complex-errors"

    writer.writeQuarantine(
      data = invalidRecords,
      path = outputPath,
      pipelineId = "test-pipeline",
      runId = "test-run-001"
    )

    // Verify error details are preserved
    val quarantined = spark.read.parquet(outputPath)
    quarantined.count() shouldBe 2

    val errorDetails = quarantined.select("error_details").collect().map(_.getString(0))
    errorDetails(0) should include("age")
    errorDetails(1) should include("email")
  }
}
