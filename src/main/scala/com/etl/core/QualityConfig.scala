package com.etl.core

/**
 * Data quality validation parameters.
 *
 * @param schemaValidation Enable Avro schema validation
 * @param nullCheckColumns Columns to check for nulls
 * @param quarantinePath Path for quarantined invalid records
 */
case class QualityConfig(
  schemaValidation: Boolean,
  nullCheckColumns: List[String],
  quarantinePath: String
)
