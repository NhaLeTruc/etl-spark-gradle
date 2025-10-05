package com.etl.core

/**
 * Result of configuration validation.
 *
 * @param isValid Whether validation passed
 * @param errors List of validation error messages
 */
case class ValidationResult(
  isValid: Boolean,
  errors: List[String] = List.empty
) {
  /**
   * Add an error to the validation result.
   */
  def addError(error: String): ValidationResult =
    copy(isValid = false, errors = errors :+ error)

  /**
   * Combine with another validation result.
   */
  def combine(other: ValidationResult): ValidationResult =
    ValidationResult(
      isValid = this.isValid && other.isValid,
      errors = this.errors ++ other.errors
    )
}

object ValidationResult {
  /**
   * Create a valid result.
   */
  def valid: ValidationResult = ValidationResult(isValid = true)

  /**
   * Create an invalid result with an error message.
   */
  def invalid(error: String): ValidationResult =
    ValidationResult(isValid = false, errors = List(error))
}
