package com.etl.integration

import com.etl.config.YAMLConfigParser
import com.etl.core._
import com.etl.pipeline._
import com.etl.quality.{DataQualityChecker, QuarantineWriter}
import com.etl.vault.VaultClient
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import java.sql.DriverManager

/**
 * Integration test for Quickstart Scenario 3: Multi-source Join Pipeline.
 *
 * Pipeline: PostgreSQL + Kafka -> Join -> S3 (Parquet)
 *
 * Tests:
 * - T148: Extract data from multiple sources (PostgreSQL + Kafka)
 * - T149: Join transformation combining two data sources
 * - T150: Load joined results to S3 in Parquet format
 * - T151: Verify join accuracy and data completeness
 * - T152: Validate lineage tracking across multiple sources
 * - T153: Test different join types (inner, left, right)
 */
class QuickstartScenario3Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var testDbPath: String = _
  var testKafkaPath: String = _
  var testOutputPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Quickstart Scenario 3 Integration Test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    testDbPath = Files.createTempDirectory("test_db_").toString
    testKafkaPath = Files.createTempDirectory("test_kafka_").toString
    testOutputPath = Files.createTempDirectory("test_output_").toString

    // Setup test databases and mock Kafka
    setupPostgresDatabase()
    setupMockKafkaData()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    deleteDirectory(Paths.get(testDbPath))
    deleteDirectory(Paths.get(testKafkaPath))
    deleteDirectory(Paths.get(testOutputPath))
  }

  private def setupPostgresDatabase(): Unit = {
    val jdbcUrl = s"jdbc:h2:$testDbPath/ordersdb;MODE=PostgreSQL"
    val conn = DriverManager.getConnection(jdbcUrl, "sa", "")

    try {
      val stmt = conn.createStatement()

      // Create orders table (PostgreSQL source)
      stmt.execute(
        """
          |CREATE TABLE orders (
          |  order_id VARCHAR(50) PRIMARY KEY,
          |  customer_id VARCHAR(50) NOT NULL,
          |  product_id VARCHAR(50) NOT NULL,
          |  quantity INT NOT NULL,
          |  order_date DATE NOT NULL
          |)
          |""".stripMargin
      )

      // Insert 50 orders
      for (i <- 1 to 50) {
        val orderId = f"ORD-$i%03d"
        val customerId = f"CUST-${(i % 10) + 1}%03d"
        val productId = f"PROD-${(i % 20) + 1}%03d"
        val quantity = (i % 5) + 1
        val orderDate = s"2024-01-${(i % 28) + 1}"

        stmt.execute(
          s"""
             |INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date)
             |VALUES ('$orderId', '$customerId', '$productId', $quantity, '$orderDate')
             |""".stripMargin
        )
      }

      stmt.close()
    } finally {
      conn.close()
    }
  }

  private def setupMockKafkaData(): Unit = {
    import spark.implicits._

    // Create product details (Kafka source)
    // Each product has: product_id, product_name, category, price
    val productDetails = (1 to 20).map { i =>
      val productId = f"PROD-$i%03d"
      val productName = s"Product $i"
      val category = if (i % 3 == 0) "Electronics" else if (i % 3 == 1) "Clothing" else "Books"
      val price = 10.0 + (i * 5)

      s"""{"product_id":"$productId","product_name":"$productName","category":"$category","price":$price}"""
    }

    // Write as mock Kafka messages (key=null, value=JSON)
    val mockKafkaDF = productDetails.map(json => (null, json)).toDF("key", "value")
    mockKafkaDF.write
      .format("json")
      .save(s"$testKafkaPath/products")
  }

  private def deleteDirectory(path: java.nio.file.Path): Unit = {
    if (Files.exists(path)) {
      import scala.jdk.CollectionConverters._
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .iterator()
        .asScala
        .foreach(Files.delete)
    }
  }

  "Quickstart Scenario 3" should "execute multi-source join pipeline" in {
    // T148-T153: Complete multi-source join integration test

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // T148: Extract from PostgreSQL (orders)
    val ordersDF = spark.read
      .format("jdbc")
      .option("url", s"jdbc:h2:$testDbPath/ordersdb;MODE=PostgreSQL")
      .option("dbtable", "orders")
      .option("user", "sa")
      .option("password", "")
      .load()

    ordersDF.count() shouldBe 50

    // T148: Extract from Kafka (product details)
    val productsDF = spark.read
      .format("json")
      .load(s"$testKafkaPath/products")
      .select(
        get_json_object($"value", "$.product_id").alias("product_id"),
        get_json_object($"value", "$.product_name").alias("product_name"),
        get_json_object($"value", "$.category").alias("category"),
        get_json_object($"value", "$.price").cast("double").alias("price")
      )

    productsDF.count() shouldBe 20

    // T149: Join transformation (inner join on product_id)
    val joinedDF = ordersDF
      .join(productsDF, Seq("product_id"), "inner")
      .select(
        $"order_id",
        $"customer_id",
        $"product_id",
        $"product_name",
        $"category",
        $"quantity",
        $"price",
        ($"quantity" * $"price").alias("total_amount"),
        $"order_date"
      )

    // T151: Verify join accuracy
    joinedDF.count() shouldBe 50 // All orders should match products

    // Verify joined columns
    joinedDF.columns should contain allOf (
      "order_id", "customer_id", "product_id", "product_name",
      "category", "quantity", "price", "total_amount", "order_date"
    )

    // Verify calculated total_amount
    val sampleRow = joinedDF.first()
    val quantity = sampleRow.getInt(sampleRow.fieldIndex("quantity"))
    val price = sampleRow.getDouble(sampleRow.fieldIndex("price"))
    val totalAmount = sampleRow.getDouble(sampleRow.fieldIndex("total_amount"))
    totalAmount shouldBe (quantity * price)

    // T150: Load to S3 (Parquet)
    val outputPath = s"$testOutputPath/enriched_orders"
    joinedDF.write
      .format("parquet")
      .mode("overwrite")
      .save(outputPath)

    // Verify output
    Files.exists(Paths.get(outputPath)) shouldBe true

    val resultDF = spark.read.parquet(outputPath)
    resultDF.count() shouldBe 50

    // Verify data integrity
    val categories = resultDF.select("category").distinct().collect().map(_.getString(0))
    categories.length should be > 0
    categories should contain oneOf ("Electronics", "Clothing", "Books")
  }

  it should "support different join types" in {
    // T153: Test left, right, and full joins

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Create test datasets with partial overlap
    val ordersDF = Seq(
      ("ORD-001", "PROD-001", 2),
      ("ORD-002", "PROD-002", 3),
      ("ORD-003", "PROD-999", 1) // Product not in product catalog
    ).toDF("order_id", "product_id", "quantity")

    val productsDF = Seq(
      ("PROD-001", "Product 1", 10.0),
      ("PROD-002", "Product 2", 20.0),
      ("PROD-003", "Product 3", 30.0) // Product with no orders
    ).toDF("product_id", "product_name", "price")

    // Inner join - only matching records
    val innerJoin = ordersDF.join(productsDF, Seq("product_id"), "inner")
    innerJoin.count() shouldBe 2

    // Left join - all orders, matching products
    val leftJoin = ordersDF.join(productsDF, Seq("product_id"), "left")
    leftJoin.count() shouldBe 3
    leftJoin.filter($"product_name".isNull).count() shouldBe 1 // PROD-999 has no match

    // Right join - all products, matching orders
    val rightJoin = ordersDF.join(productsDF, Seq("product_id"), "right")
    rightJoin.count() shouldBe 3
    rightJoin.filter($"order_id".isNull).count() shouldBe 1 // PROD-003 has no orders

    // Full join - all records from both sides
    val fullJoin = ordersDF.join(productsDF, Seq("product_id"), "full")
    fullJoin.count() shouldBe 4
  }

  it should "handle complex join conditions" in {
    import spark.implicits._

    val ordersDF = Seq(
      ("ORD-001", "PROD-001", 2, "2024-01-15"),
      ("ORD-002", "PROD-001", 3, "2024-01-20"),
      ("ORD-003", "PROD-002", 1, "2024-01-25")
    ).toDF("order_id", "product_id", "quantity", "order_date")

    val productsDF = Seq(
      ("PROD-001", "Product 1", 10.0),
      ("PROD-002", "Product 2", 20.0)
    ).toDF("product_id", "product_name", "price")

    // Join and aggregate
    val result = ordersDF
      .join(productsDF, "product_id")
      .groupBy("product_id", "product_name")
      .agg(
        org.apache.spark.sql.functions.sum("quantity").alias("total_quantity"),
        org.apache.spark.sql.functions.sum(org.apache.spark.sql.functions.expr("quantity * price")).alias("total_revenue")
      )

    result.count() shouldBe 2

    val prod1 = result.filter($"product_id" === "PROD-001").first()
    prod1.getLong(prod1.fieldIndex("total_quantity")) shouldBe 5 // 2 + 3
  }

  it should "preserve lineage metadata through joins" in {
    // T152: Validate lineage tracking across multiple sources

    import com.etl.lineage.{LineageTracker, LineageMetadata}
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val tracker = new LineageTracker()

    // Create source data with lineage
    val ordersDF = Seq(
      ("ORD-001", "PROD-001", 2)
    ).toDF("order_id", "product_id", "quantity")

    val productsDF = Seq(
      ("PROD-001", "Product 1", 10.0)
    ).toDF("product_id", "product_name", "price")

    // Embed lineage in both sources
    val ordersWithLineage = tracker.embedLineage(
      ordersDF,
      LineageMetadata(
        pipelineId = "test-pipeline",
        runId = "run-001",
        sourceType = "postgres",
        sourceName = "orders",
        transformations = List.empty
      )
    )

    val productsWithLineage = tracker.embedLineage(
      productsDF,
      LineageMetadata(
        pipelineId = "test-pipeline",
        runId = "run-001",
        sourceType = "kafka",
        sourceName = "products",
        transformations = List.empty
      )
    )

    // Join preserves lineage from left side
    val joined = ordersWithLineage.join(productsWithLineage, Seq("product_id"), "inner")

    joined.columns should contain("_lineage")

    val lineageJson = joined.select("_lineage").first().getString(0)
    lineageJson should include("postgres")
    lineageJson should include("orders")
  }

  it should "validate data completeness after join" in {
    // T151: Comprehensive data completeness checks

    import org.apache.spark.sql.functions._

    val ordersDF = spark.read
      .format("jdbc")
      .option("url", s"jdbc:h2:$testDbPath/ordersdb;MODE=PostgreSQL")
      .option("dbtable", "orders")
      .option("user", "sa")
      .option("password", "")
      .load()

    val productsDF = spark.read
      .format("json")
      .load(s"$testKafkaPath/products")
      .select(
        get_json_object($"value", "$.product_id").alias("product_id"),
        get_json_object($"value", "$.product_name").alias("product_name"),
        get_json_object($"value", "$.price").cast("double").alias("price")
      )

    val joinedDF = ordersDF.join(productsDF, Seq("product_id"), "inner")

    // Verify no null values in key columns
    joinedDF.filter($"order_id".isNull).count() shouldBe 0
    joinedDF.filter($"product_id".isNull).count() shouldBe 0
    joinedDF.filter($"customer_id".isNull).count() shouldBe 0

    // Verify all orders have matching products
    val uniqueProductsInOrders = ordersDF.select("product_id").distinct().count()
    val joinedProductCount = joinedDF.select("product_id").distinct().count()
    joinedProductCount shouldBe uniqueProductsInOrders
  }
}
