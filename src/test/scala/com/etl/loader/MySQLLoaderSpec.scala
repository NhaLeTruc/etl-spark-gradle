package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.sql.DriverManager

/**
 * Unit tests for MySQLLoader.
 *
 * Tests cover:
 * - T088: Write to JDBC with append mode (using H2)
 * - T089: Write with overwrite mode
 * - T090: Write with upsert mode (ON DUPLICATE KEY UPDATE)
 *
 * Note: Uses H2 in-memory database for testing instead of actual MySQL.
 */
class MySQLLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val h2Url = "jdbc:h2:mem:testdb_mysql_loader;MODE=MySQL;DB_CLOSE_DELAY=-1"
  val h2Driver = "org.h2.Driver"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MySQLLoaderSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Setup H2 in-memory database
    Class.forName(h2Driver)
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS output_products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2))")
    stmt.close()
    conn.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  import spark.implicits._

  // T088: Write with append mode
  "MySQLLoader" should "write data to MySQL with append mode" in {
    val loader = new MySQLLoader()

    // Clear table
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_products")
    stmt.close()
    conn.close()

    val data = Seq(
      (1, "Laptop", 999.99),
      (2, "Mouse", 29.99)
    ).toDF("id", "name", "price")

    val config = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "append",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_products",
        "user" -> "sa",
        "password" -> ""
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
    result.writeMode shouldBe "append"

    // Verify data was written
    val written = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "output_products")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 2

    loader.sinkType shouldBe "mysql"
  }

  // T089: Write with overwrite mode
  it should "write data to MySQL with overwrite mode" in {
    val loader = new MySQLLoader()

    // Insert initial data
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_products")
    stmt.execute("INSERT INTO output_products VALUES (1, 'Old Product', 100.00)")
    stmt.close()
    conn.close()

    val data = Seq(
      (2, "New Product", 200.00)
    ).toDF("id", "name", "price")

    val config = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "overwrite",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_products",
        "user" -> "sa",
        "password" -> ""
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.writeMode shouldBe "overwrite"

    // Verify old data was replaced
    val written = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "output_products")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 1
    written.first().getInt(0) shouldBe 2 // Only new record
  }

  // T090: Write with upsert mode (ON DUPLICATE KEY UPDATE)
  it should "write data to MySQL with upsert mode" in {
    val loader = new MySQLLoader()

    // Insert initial data
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_products")
    stmt.execute("INSERT INTO output_products VALUES (1, 'Laptop', 999.99)")
    stmt.close()
    conn.close()

    val data = Seq(
      (1, "Laptop Pro", 1299.99), // Update existing
      (3, "Keyboard", 79.99) // Insert new
    ).toDF("id", "name", "price")

    val config = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "upsert",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_products",
        "user" -> "sa",
        "password" -> "",
        "primaryKey" -> "id"
      )
    )

    val runContext = RunContext(
      pipelineId = "test-pipeline",
      runId = "test-run-001",
      spark = spark
    )

    val result = loader.load(data, config, runContext)

    result.isSuccess shouldBe true
    result.writeMode shouldBe "upsert"

    // Verify upsert worked
    val written = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "output_products")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 2
    val laptop = written.filter($"id" === 1).first()
    laptop.getString(1) shouldBe "Laptop Pro" // Updated
  }

  it should "validate configuration and detect missing parameters" in {
    val loader = new MySQLLoader()

    // Missing url
    val invalidConfig1 = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "append",
      parameters = Map("table" -> "products")
    )
    val result1 = loader.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: url")

    // Missing table
    val invalidConfig2 = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "append",
      parameters = Map("url" -> "jdbc:mysql://localhost:3306/db")
    )
    val result2 = loader.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: table")

    // Valid configuration
    val validConfig = SinkConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      writeMode = "append",
      parameters = Map(
        "url" -> "jdbc:mysql://localhost:3306/db",
        "table" -> "products"
      )
    )
    val result3 = loader.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }
}
