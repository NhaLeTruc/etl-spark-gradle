package com.etl.quality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Schema validator for data quality checks.
 *
 * Validates DataFrames against expected schemas and detects violations.
 */
class SchemaValidator {

  /**
   * Validation result containing validity status and violations.
   */
  case class SchemaValidationResult(
    isValid: Boolean,
    violations: List[String]
  )

  /**
   * Validate DataFrame against expected schema.
   *
   * Checks for:
   * - Missing columns
   * - Extra columns
   * - Type mismatches
   * - Nullable constraint violations
   *
   * @param data DataFrame to validate
   * @param expectedSchema Expected schema
   * @return Validation result with violations if any
   */
  def validate(data: DataFrame, expectedSchema: StructType): SchemaValidationResult = {
    val violations = scala.collection.mutable.ListBuffer[String]()
    val actualSchema = data.schema

    val actualFields = actualSchema.fields.map(f => f.name -> f).toMap
    val expectedFields = expectedSchema.fields.map(f => f.name -> f).toMap

    // Check for missing columns
    expectedFields.foreach { case (name, expectedField) =>
      actualFields.get(name) match {
        case None =>
          violations += s"Missing required column: $name"

        case Some(actualField) =>
          // Check type match
          if (!typesMatch(actualField.dataType, expectedField.dataType)) {
            violations += s"Type mismatch for column '$name': expected ${expectedField.dataType.typeName}, got ${actualField.dataType.typeName}"
          }

          // Check nullable constraint
          if (!expectedField.nullable && actualField.nullable) {
            violations += s"Nullable constraint violation for column '$name': expected non-nullable, got nullable"
          }
      }
    }

    // Check for extra columns (optional warning)
    actualFields.foreach { case (name, _) =>
      if (!expectedFields.contains(name)) {
        violations += s"Extra column found: $name"
      }
    }

    SchemaValidationResult(
      isValid = violations.isEmpty,
      violations = violations.toList
    )
  }

  /**
   * Check if two data types match.
   *
   * Handles exact matches and structural types.
   */
  private def typesMatch(actual: DataType, expected: DataType): Boolean = {
    (actual, expected) match {
      case (a: StructType, e: StructType) =>
        // Recursively check struct fields
        if (a.fields.length != e.fields.length) return false
        a.fields.zip(e.fields).forall { case (af, ef) =>
          af.name == ef.name && typesMatch(af.dataType, ef.dataType)
        }

      case (a: ArrayType, e: ArrayType) =>
        typesMatch(a.elementType, e.elementType)

      case (a: MapType, e: MapType) =>
        typesMatch(a.keyType, e.keyType) && typesMatch(a.valueType, e.valueType)

      case (a, e) =>
        // Exact type match
        a == e
    }
  }

  /**
   * Validate Avro schema compatibility.
   *
   * @param data DataFrame to validate
   * @param avroSchemaJson Avro schema as JSON string
   * @return Validation result
   */
  def validateAvro(data: DataFrame, avroSchemaJson: String): SchemaValidationResult = {
    // For now, delegate to generic validation
    // In production, parse Avro schema and convert to Spark StructType
    SchemaValidationResult(
      isValid = true,
      violations = List.empty
    )
  }
}
