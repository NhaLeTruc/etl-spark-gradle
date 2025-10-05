package com.etl.vault

/**
 * Vault client for secrets management.
 *
 * Provides interface to HashiCorp Vault for retrieving and storing secrets.
 * Supports mocked implementation for testing.
 */
class VaultClient(
  vaultUrl: String,
  token: String,
  mockSecrets: Map[String, Map[String, String]] = Map.empty
) {

  // In-memory storage for mock mode or cache
  private var secretsStore: Map[String, Map[String, String]] = mockSecrets

  /**
   * Retrieve secret from Vault.
   *
   * @param path Secret path (e.g., "secret/kafka")
   * @return Map of secret key-value pairs
   */
  def getSecret(path: String): Map[String, String] = {
    // In production, this would make HTTP request to Vault API
    // For now, use mock implementation
    secretsStore.getOrElse(path, Map.empty)
  }

  /**
   * Write secret to Vault.
   *
   * @param path Secret path
   * @param secrets Secret key-value pairs
   */
  def writeSecret(path: String, secrets: Map[String, String]): Unit = {
    // In production, this would make HTTP POST to Vault API
    // For now, update mock storage
    secretsStore = secretsStore + (path -> secrets)
  }

  /**
   * Check if Vault is accessible.
   *
   * @return true if Vault is reachable, false otherwise
   */
  def isConnected(): Boolean = {
    // In production, make health check request to Vault
    // For mock, always return true
    try {
      if (mockSecrets.nonEmpty || vaultUrl.contains("localhost")) {
        true
      } else {
        // Simulate connection check
        false
      }
    } catch {
      case _: Exception => false
    }
  }

  /**
   * List secret paths under a given prefix.
   *
   * @param prefix Path prefix (e.g., "secret/")
   * @return List of secret paths
   */
  def listSecrets(prefix: String): List[String] = {
    // In production, this would call Vault LIST API
    // For mock, filter stored secrets
    secretsStore.keys.filter(_.startsWith(prefix)).toList
  }

  /**
   * Delete secret from Vault.
   *
   * @param path Secret path
   */
  def deleteSecret(path: String): Unit = {
    // In production, this would make HTTP DELETE to Vault API
    secretsStore = secretsStore - path
  }

  /**
   * Get Vault configuration.
   */
  def getVaultUrl: String = vaultUrl
  def getToken: String = token
}

/**
 * Vault client factory.
 */
object VaultClient {
  /**
   * Create Vault client from environment variables.
   *
   * Uses VAULT_ADDR and VAULT_TOKEN environment variables.
   */
  def fromEnvironment(): VaultClient = {
    val vaultUrl = sys.env.getOrElse("VAULT_ADDR", "http://localhost:8200")
    val token = sys.env.getOrElse("VAULT_TOKEN", "dev-token")
    new VaultClient(vaultUrl, token)
  }

  /**
   * Create mock Vault client for testing.
   */
  def mock(secrets: Map[String, Map[String, String]]): VaultClient = {
    new VaultClient("http://localhost:8200", "mock-token", secrets)
  }
}
