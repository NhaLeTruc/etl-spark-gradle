package com.etl.lineage

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for LineageTracker.
 *
 * Tests cover:
 * - T102: Embed lineage metadata
 * - T103: Update transformation chain
 */
class LineageTrackerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("LineageTrackerSpec")
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

  // T102: Embed lineage metadata
  "LineageTracker" should "embed lineage metadata in DataFrame" in {
    val tracker = new LineageTracker()

    val data = Seq(
      ("user1", 25),
      ("user2", 30)
    ).toDF("username", "age")

    val lineage = LineageMetadata(
      sourceType = "kafka",
      sourceIdentifier = "users-topic",
      extractionTimestamp = System.currentTimeMillis(),
      transformationChain = List.empty
    )

    val result = tracker.embedLineage(data, lineage)

    result.columns should contain("_lineage")
    result.count() shouldBe 2

    val lineageData = result.select("_lineage").first().getString(0)
    lineageData should include("kafka")
    lineageData should include("users-topic")
  }

  // T103: Update transformation chain
  it should "update transformation chain with new steps" in {
    val tracker = new LineageTracker()

    val data = Seq(
      ("user1", 25)
    ).toDF("username", "age")

    // Initial lineage
    val initialLineage = LineageMetadata(
      sourceType = "postgres",
      sourceIdentifier = "users_table",
      extractionTimestamp = System.currentTimeMillis(),
      transformationChain = List.empty
    )

    val withLineage = tracker.embedLineage(data, initialLineage)

    // Add transformation step
    val afterFilter = tracker.addTransformation(withLineage, "filter(age > 20)")

    afterFilter.columns should contain("_lineage")

    val lineageData = afterFilter.select("_lineage").first().getString(0)
    lineageData should include("filter(age > 20)")
    lineageData should include("transformationChain")
  }

  it should "maintain lineage through multiple transformations" in {
    val tracker = new LineageTracker()

    val data = Seq(
      ("user1", 25, "active")
    ).toDF("username", "age", "status")

    // Initial lineage
    val initialLineage = LineageMetadata(
      sourceType = "s3",
      sourceIdentifier = "s3://bucket/users.avro",
      extractionTimestamp = System.currentTimeMillis(),
      transformationChain = List.empty
    )

    var result = tracker.embedLineage(data, initialLineage)

    // Apply multiple transformations
    result = tracker.addTransformation(result, "filter(status='active')")
    result = tracker.addTransformation(result, "map(age=age+1)")
    result = tracker.addTransformation(result, "aggregation(groupBy=status)")

    val lineageData = result.select("_lineage").first().getString(0)
    lineageData should include("filter(status='active')")
    lineageData should include("map(age=age+1)")
    lineageData should include("aggregation(groupBy=status)")
  }

  it should "extract lineage metadata from DataFrame" in {
    val tracker = new LineageTracker()

    val data = Seq(
      ("user1", 25)
    ).toDF("username", "age")

    val lineage = LineageMetadata(
      sourceType = "mysql",
      sourceIdentifier = "customers",
      extractionTimestamp = 1234567890L,
      transformationChain = List("filter", "map")
    )

    val withLineage = tracker.embedLineage(data, lineage)

    // Extract lineage back
    val extracted = tracker.extractLineage(withLineage)

    extracted.isDefined shouldBe true
    extracted.get.sourceType shouldBe "mysql"
    extracted.get.sourceIdentifier shouldBe "customers"
    extracted.get.transformationChain should contain allOf ("filter", "map")
  }

  it should "handle DataFrames without lineage" in {
    val tracker = new LineageTracker()

    val data = Seq(
      ("user1", 25)
    ).toDF("username", "age")

    // Extract lineage from DataFrame without lineage column
    val extracted = tracker.extractLineage(data)

    extracted.isDefined shouldBe false
  }
}
