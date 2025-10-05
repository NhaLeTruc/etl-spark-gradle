package com.etl.extractor

import com.etl.core._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.sql.DriverManager

/**
 * Unit tests for MySQLExtractor.
 *
 * Tests cover:
 * - T054: Extract from JDBC (using H2 in-memory database as MySQL substitute)
 * - T055: Validate configuration
 * - T056: Retrieve credentials from Vault (mocked)
 *
 * Note: Uses H2 in-memory database for testing instead of actual MySQL.
 */
class MySQLExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val h2Url = "jdbc:h2:mem:testdb_mysql;MODE=MySQL;DB_CLOSE_DELAY=-1"
  val h2Driver = "org.h2.Driver"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MySQLExtractorSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Setup H2 in-memory database with test data
    Class.forName(h2Driver)
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2))")
    stmt.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)")
    stmt.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)")
    stmt.execute("INSERT INTO products VALUES (3, 'Keyboard', 79.99)")
    stmt.close()
    conn.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // T054: Extract from JDBC
  "MySQLExtractor" should "extract data from MySQL table and return valid DataFrame" in {
    val extractor = new MySQLExtractor()
    val config = SourceConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "products",
        "user" -> "sa",
        "password" -> ""
      )
    )

    val result = extractor.extract(config)

    result should not be null
    result.count() shouldBe 3
    result.columns should contain allOf ("id", "name", "price")

    val firstRow = result.orderBy("id").first()
    firstRow.getInt(0) shouldBe 1
    firstRow.getString(1) shouldBe "Laptop"

    extractor.sourceType shouldBe "mysql"
  }

  // T055: Validate configuration
  it should "validate configuration and detect missing parameters" in {
    val extractor = new MySQLExtractor()

    // Missing url
    val invalidConfig1 = SourceConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      parameters = Map("table" -> "products")
    )
    val result1 = extractor.validateConfig(invalidConfig1)
    result1.isValid shouldBe false
    result1.errors should contain("Missing required parameter: url")

    // Missing table
    val invalidConfig2 = SourceConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      parameters = Map("url" -> "jdbc:mysql://localhost:3306/db")
    )
    val result2 = extractor.validateConfig(invalidConfig2)
    result2.isValid shouldBe false
    result2.errors should contain("Missing required parameter: table")

    // Valid configuration
    val validConfig = SourceConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      parameters = Map(
        "url" -> "jdbc:mysql://localhost:3306/db",
        "table" -> "products"
      )
    )
    val result3 = extractor.validateConfig(validConfig)
    result3.isValid shouldBe true
    result3.errors shouldBe empty
  }

  // T056: Retrieve credentials from Vault (mocked)
  it should "retrieve credentials from Vault when not provided in config" in {
    // Mock Vault client that returns credentials
    val mockVaultClient = new MockVaultClient(Map(
      "secret/mysql" -> Map(
        "user" -> "dbuser",
        "password" -> "dbpass"
      )
    ))

    val extractor = new MySQLExtractor(Some(mockVaultClient))
    val config = SourceConfig(
      `type` = "mysql",
      credentialsPath = "secret/mysql",
      parameters = Map(
        "url" -> h2Url,
        "table" -> "products"
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
