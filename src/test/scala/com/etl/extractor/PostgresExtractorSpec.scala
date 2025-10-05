package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.sql.DriverManager

/**
 * Unit tests for PostgresExtractor.
 *
 * Tests cover:
 * - T050: Extract from JDBC (using H2 in-memory database as PostgreSQL substitute)
 * - T051: Validate configuration
 * - T052: Retrieve credentials from Vault (mocked)
 *
 * Note: Uses H2 in-memory database for testing instead of actual PostgreSQL.
 */
class PostgresExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val h2Url = "jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
  val h2Driver = "org.h2.Driver"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PostgresExtractorSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Setup H2 in-memory database with test data
    Class.forName(h2Driver)
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")
    stmt.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
    stmt.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")
    stmt.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')")
    stmt.close()
    conn.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // T050: Extract from JDBC
  "PostgresExtractor" should "extract data from PostgreSQL table and return valid DataFrame" in {
    val extractor = new PostgresExtractor()
    val config = SourceConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "users",
        "user" -> "sa",
        "password" -> ""
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3
    result.columns should contain allOf ("id", "name", "email")

    val firstRow = result.orderBy("id").first()
    firstRow.getInt(0) shouldBe 1
    firstRow.getString(1) shouldBe "Alice"
    firstRow.getString(2) shouldBe "alice@example.com"

    extractor.sourceType shouldBe "postgres"
  }

  // T051: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val extractor = new PostgresExtractor()

    // Missing url
    val invalidConfig1 = SourceConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      parameters = Map("table" -> "users")
    )
    val result1 = extractor.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: url")

    // Missing table
    val invalidConfig2 = SourceConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      parameters = Map("url" -> "jdbc:postgresql://localhost:5432/db")
    )
    val result2 = extractor.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: table")

    // Valid configuration
    val validConfig = SourceConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      parameters = Map(
        "url" -> "jdbc:postgresql://localhost:5432/db",
        "table" -> "users"
      )
    )
    val result3 = extractor.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }

  // T052: Retrieve credentials from Vault (mocked)
  it should "retrieve credentials from Vault when not provided in config" in {
    // Mock Vault client that returns credentials
    val mockVaultClient = new MockVaultClient(Map(
      "secret/postgres" -> Map(
        "user" -> "dbuser",
        "password" -> "dbpass"
      )
    ))

    val extractor = new PostgresExtractor(Some(mockVaultClient))
    val config = SourceConfig(
      `type` = "postgres",
      credentialsPath = "secret/postgres",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "users"
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3
  }

  /**
   * Mock Vault client for testing credential retrieval.
   */
  class MockVaultClient(secrets: Map[String, Map[String, String]]) {
    def getSecret(path: String): Map[String, String] = {
      secrets.getOrElse(path, Map.empty)
    }
  }
}
