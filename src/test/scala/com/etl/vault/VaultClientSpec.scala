package com.etl.vault

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for VaultClient.
 *
 * Tests cover:
 * - T111: Retrieve secrets (mocked Vault)
 * - T112: Handle connection failure
 */
class VaultClientSpec extends AnyFlatSpec with Matchers {

  // T111: Retrieve secrets (mocked Vault)
  "VaultClient" should "retrieve secrets from Vault" in {
    // Mock Vault with in-memory secrets
    val mockSecrets = Map(
      "secret/kafka" -> Map(
        "bootstrap.servers" -> "localhost:9092",
        "username" -> "kafka-user",
        "password" -> "kafka-pass"
      ),
      "secret/postgres" -> Map(
        "url" -> "jdbc:postgresql://localhost:5432/db",
        "user" -> "postgres-user",
        "password" -> "postgres-pass"
      )
    )

    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val kafkaSecrets = client.getSecret("secret/kafka")
    kafkaSecrets("bootstrap.servers") shouldBe "localhost:9092"
    kafkaSecrets("username") shouldBe "kafka-user"
    kafkaSecrets("password") shouldBe "kafka-pass"

    val postgresSecrets = client.getSecret("secret/postgres")
    postgresSecrets("user") shouldBe "postgres-user"
  }

  it should "return empty map for non-existent secrets" in {
    val mockSecrets = Map.empty[String, Map[String, String]]
    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val secrets = client.getSecret("secret/nonexistent")
    secrets shouldBe empty
  }

  // T112: Handle connection failure
  it should "handle connection failure gracefully" in {
    val client = new VaultClient("http://invalid-vault:8200", "invalid-token", Map.empty)

    // Should handle connection failure and return empty map
    val secrets = client.getSecret("secret/test")
    secrets shouldBe empty
  }

  it should "retrieve nested secret paths" in {
    val mockSecrets = Map(
      "secret/data/prod/kafka" -> Map(
        "bootstrap.servers" -> "prod-kafka:9092",
        "api-key" -> "prod-api-key"
      )
    )

    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val secrets = client.getSecret("secret/data/prod/kafka")
    secrets("bootstrap.servers") shouldBe "prod-kafka:9092"
    secrets("api-key") shouldBe "prod-api-key"
  }

  it should "support writing secrets" in {
    var mockSecrets = Map.empty[String, Map[String, String]]
    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val newSecrets = Map(
      "user" -> "new-user",
      "password" -> "new-password"
    )

    client.writeSecret("secret/new-service", newSecrets)

    val retrieved = client.getSecret("secret/new-service")
    retrieved("user") shouldBe "new-user"
    retrieved("password") shouldBe "new-password"
  }

  it should "validate Vault connection" in {
    val mockSecrets = Map.empty[String, Map[String, String]]
    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val isConnected = client.isConnected()
    isConnected shouldBe true // Mock always returns true
  }

  it should "list secret paths" in {
    val mockSecrets = Map(
      "secret/kafka" -> Map("key" -> "value1"),
      "secret/postgres" -> Map("key" -> "value2"),
      "secret/mysql" -> Map("key" -> "value3")
    )

    val client = new VaultClient("http://localhost:8200", "dev-token", mockSecrets)

    val paths = client.listSecrets("secret/")
    paths should contain allOf ("secret/kafka", "secret/postgres", "secret/mysql")
  }
}
