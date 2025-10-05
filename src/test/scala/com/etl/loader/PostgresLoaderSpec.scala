package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.sql.DriverManager

/**
 * Unit tests for PostgresLoader.
 *
 * Tests cover:
 * - T084: Write to JDBC with append mode (using H2)
 * - T085: Write with overwrite mode
 * - T086: Write with upsert mode (ON CONFLICT)
 *
 * Note: Uses H2 in-memory database for testing instead of actual PostgreSQL.
 */
class PostgresLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val h2Url = "jdbc:h2:mem:testdb_loader;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
  val h2Driver = "org.h2.Driver"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PostgresLoaderSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Setup H2 in-memory database
    Class.forName(h2Driver)
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS output_users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")
    stmt.close()
    conn.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  import spark.implicits._

  // T084: Write with append mode
  "PostgresLoader" should "write data to PostgreSQL with append mode" in {
    val loader = new PostgresLoader()

    // Clear table
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_users")
    stmt.close()
    conn.close()

    val data = Seq(
      (1, "Alice", "alice@example.com"),
      (2, "Bob", "bob@example.com")
    ).toDF("id", "name", "email")

    val config = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "append",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_users",
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
      .option("dbtable", "output_users")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 2

    loader.sinkType shouldBe "postgres"
  }

  // T085: Write with overwrite mode
  it should "write data to PostgreSQL with overwrite mode" in {
    val loader = new PostgresLoader()

    // Insert initial data
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_users")
    stmt.execute("INSERT INTO output_users VALUES (1, 'Old User', 'old@example.com')")
    stmt.close()
    conn.close()

    val data = Seq(
      (2, "New User", "new@example.com")
    ).toDF("id", "name", "email")

    val config = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "overwrite",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_users",
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
      .option("dbtable", "output_users")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 1
    written.first().getInt(0) shouldBe 2 // Only new record
  }

  // T086: Write with upsert mode (ON CONFLICT)
  it should "write data to PostgreSQL with upsert mode" in {
    val loader = new PostgresLoader()

    // Insert initial data
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("DELETE FROM output_users")
    stmt.execute("INSERT INTO output_users VALUES (1, 'Alice', 'alice@example.com')")
    stmt.close()
    conn.close()

    val data = Seq(
      (1, "Alice Updated", "alice.new@example.com"), // Update existing
      (3, "Charlie", "charlie@example.com") // Insert new
    ).toDF("id", "name", "email")

    val config = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "upsert",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "output_users",
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
      .option("dbtable", "output_users")
      .option("user", "sa")
      .option("password", "")
      .load()

    written.count() shouldBe 2
    val alice = written.filter($"id" === 1).first()
    alice.getString(1) shouldBe "Alice Updated" // Updated
  }

  it should "validate configuration and detect missing parameters" in {
    val loader = new PostgresLoader()

    // Missing url
    val invalidConfig1 = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "append",
      parameters = Map("table" -> "users")
    )
    val result1 = loader.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: url")

    // Missing table
    val invalidConfig2 = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "append",
      parameters = Map("url" -> "jdbc:postgresql://localhost:5432/db")
    )
    val result2 = loader.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: table")

    // Valid configuration
    val validConfig = SinkConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      writeMode = "append",
      parameters = Map(
        "url" -> "jdbc:postgresql://localhost:5432/db",
        "table" -> "users"
      )
    )
    val result3 = loader.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }
}
