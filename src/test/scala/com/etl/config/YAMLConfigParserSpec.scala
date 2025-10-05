package com.etl.config

import com.etl.core._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for YAMLConfigParser.
 *
 * Tests cover:
 * - T108: Parse PipelineConfig from YAML
 * - T109: Handle invalid YAML gracefully
 */
class YAMLConfigParserSpec extends AnyFlatSpec with Matchers {

  // T108: Parse PipelineConfig from YAML
  "YAMLConfigParser" should "parse valid YAML into PipelineConfig" in {
    val parser = new YAMLConfigParser()

    val yamlContent =
      """
        |pipelineId: "user-etl-pipeline"
        |source:
        |  type: "kafka"
        |  credentialsPath: "secret/kafka"
        |  parameters:
        |    bootstrap.servers: "localhost:9092"
        |    topic: "users"
        |transformations:
        |  - type: "filter"
        |    parameters:
        |      condition: "age > 18"
        |  - type: "aggregation"
        |    parameters:
        |      groupBy: "country"
        |    aggregations:
        |      - column: "age"
        |        function: "avg"
        |        alias: "avg_age"
        |sink:
        |  type: "postgres"
        |  credentialsPath: "secret/postgres"
        |  writeMode: "append"
        |  parameters:
        |    url: "jdbc:postgresql://localhost:5432/db"
        |    table: "user_stats"
        |performance:
        |  shufflePartitions: 200
        |  batchSize: 10000
        |quality:
        |  schemaValidation: true
        |  nullChecks:
        |    - "user_id"
        |    - "age"
        |  duplicateCheck: true
        |""".stripMargin

    val config = parser.parse(yamlContent)

    config.pipelineId shouldBe "user-etl-pipeline"
    config.source.`type` shouldBe "kafka"
    config.source.parameters("topic") shouldBe "users"
    config.transformations.length shouldBe 2
    config.transformations(0).`type` shouldBe "filter"
    config.transformations(1).`type` shouldBe "aggregation"
    config.sink.`type` shouldBe "postgres"
    config.sink.writeMode shouldBe "append"
    config.performance.shufflePartitions shouldBe 200
    config.quality.schemaValidation shouldBe true
  }

  it should "parse minimal valid YAML" in {
    val parser = new YAMLConfigParser()

    val yamlContent =
      """
        |pipelineId: "simple-pipeline"
        |source:
        |  type: "s3"
        |  credentialsPath: "secret/s3"
        |  parameters:
        |    path: "s3://bucket/data"
        |    format: "avro"
        |sink:
        |  type: "s3"
        |  credentialsPath: "secret/s3"
        |  writeMode: "overwrite"
        |  parameters:
        |    path: "s3://bucket/output"
        |    format: "parquet"
        |""".stripMargin

    val config = parser.parse(yamlContent)

    config.pipelineId shouldBe "simple-pipeline"
    config.transformations shouldBe empty
    config.performance.shufflePartitions shouldBe 200 // Default value
  }

  // T109: Handle invalid YAML gracefully
  it should "handle invalid YAML syntax gracefully" in {
    val parser = new YAMLConfigParser()

    val invalidYaml =
      """
        |pipelineId: "test"
        |source:
        |  type: kafka
        |  invalid syntax here!!!
        |""".stripMargin

    intercept[Exception] {
      parser.parse(invalidYaml)
    }
  }

  it should "handle missing required fields" in {
    val parser = new YAMLConfigParser()

    val missingFields =
      """
        |pipelineId: "test"
        |source:
        |  type: "kafka"
        |""".stripMargin

    // Should throw exception for missing credentialsPath
    intercept[Exception] {
      parser.parse(missingFields)
    }
  }

  it should "parse transformations with aggregations" in {
    val parser = new YAMLConfigParser()

    val yamlContent =
      """
        |pipelineId: "agg-pipeline"
        |source:
        |  type: "postgres"
        |  credentialsPath: "secret/postgres"
        |  parameters:
        |    url: "jdbc:postgresql://localhost:5432/db"
        |    table: "sales"
        |transformations:
        |  - type: "aggregation"
        |    parameters:
        |      groupBy: "product_id,region"
        |    aggregations:
        |      - column: "amount"
        |        function: "sum"
        |        alias: "total_sales"
        |      - column: "quantity"
        |        function: "count"
        |        alias: "num_orders"
        |sink:
        |  type: "s3"
        |  credentialsPath: "secret/s3"
        |  writeMode: "overwrite"
        |  parameters:
        |    path: "s3://bucket/sales-summary"
        |    format: "parquet"
        |""".stripMargin

    val config = parser.parse(yamlContent)

    config.transformations.length shouldBe 1
    config.transformations(0).aggregations.isDefined shouldBe true
    config.transformations(0).aggregations.get.length shouldBe 2
    config.transformations(0).aggregations.get(0).column shouldBe "amount"
    config.transformations(0).aggregations.get(0).function shouldBe "sum"
  }

  it should "parse complex pipeline with multiple transformations" in {
    val parser = new YAMLConfigParser()

    val yamlContent =
      """
        |pipelineId: "complex-pipeline"
        |source:
        |  type: "kafka"
        |  credentialsPath: "secret/kafka"
        |  parameters:
        |    bootstrap.servers: "localhost:9092"
        |    topic: "events"
        |transformations:
        |  - type: "filter"
        |    parameters:
        |      condition: "event_type = 'purchase'"
        |  - type: "map"
        |    parameters:
        |      expressions: "total:price * quantity,date:CAST(timestamp AS DATE)"
        |  - type: "aggregation"
        |    parameters:
        |      groupBy: "date,product_id"
        |    aggregations:
        |      - column: "total"
        |        function: "sum"
        |        alias: "daily_revenue"
        |sink:
        |  type: "mysql"
        |  credentialsPath: "secret/mysql"
        |  writeMode: "upsert"
        |  parameters:
        |    url: "jdbc:mysql://localhost:3306/analytics"
        |    table: "daily_product_revenue"
        |    primaryKey: "date,product_id"
        |quality:
        |  schemaValidation: true
        |  nullChecks:
        |    - "date"
        |    - "product_id"
        |    - "daily_revenue"
        |  duplicateCheck: false
        |""".stripMargin

    val config = parser.parse(yamlContent)

    config.pipelineId shouldBe "complex-pipeline"
    config.transformations.length shouldBe 3
    config.sink.parameters("primaryKey") shouldBe "date,product_id"
    config.quality.nullChecks.length shouldBe 3
  }
}
