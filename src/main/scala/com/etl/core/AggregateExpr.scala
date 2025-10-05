package com.etl.core

/**
 * Aggregation expression for aggregation transformations.
 *
 * @param column Column to aggregate
 * @param function Aggregation function ("sum" | "count" | "avg" | "min" | "max")
 * @param alias Output column alias
 */
case class AggregateExpr(
  column: String,
  function: String,
  alias: String
)
