/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.stats

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

import org.slf4j.LoggerFactory

/**
 * Simplifies filter expressions based on schema knowledge and logical rules.
 *
 * Optimizations performed:
 * - isNull on non-nullable column -> AlwaysFalse
 * - isNotNull on non-nullable column -> AlwaysTrue
 * - AND with AlwaysFalse -> AlwaysFalse
 * - AND with AlwaysTrue -> other operand
 * - OR with AlwaysTrue -> AlwaysTrue
 * - OR with AlwaysFalse -> other operand
 * - NOT(NOT(x)) -> x
 * - Constant folding for trivially true/false comparisons
 */
object ExpressionSimplifier {

  private val logger = LoggerFactory.getLogger(ExpressionSimplifier.getClass)

  /**
   * Sentinel filters for always true/false conditions.
   * We use special case classes that wrap meaningful predicates
   * to work around Filter being sealed.
   */
  // AlwaysTrue: represented as "1" = "1"
  val AlwaysTrue: Filter = EqualTo("__always_true__", "__always_true__")

  // AlwaysFalse: represented as "1" = "0"
  val AlwaysFalse: Filter = EqualTo("__always_false__", "__never_matches__")

  /**
   * Check if a filter is our sentinel AlwaysTrue
   */
  def isAlwaysTrue(filter: Filter): Boolean = filter == AlwaysTrue

  /**
   * Check if a filter is our sentinel AlwaysFalse
   */
  def isAlwaysFalse(filter: Filter): Boolean = filter == AlwaysFalse

  /**
   * Simplify an array of filters based on schema knowledge.
   * Filters combined with AND logic, so if any is AlwaysFalse, result is AlwaysFalse.
   * AlwaysTrue filters are removed from the array.
   *
   * @param filters Array of filters to simplify
   * @param schema Schema for nullable column detection
   * @return Simplified array of filters
   */
  def simplify(filters: Array[Filter], schema: StructType): Array[Filter] = {
    if (filters.isEmpty) return filters

    val simplified = filters.map(f => simplifyFilter(f, schema))

    // If any filter is AlwaysFalse, the whole AND is false
    if (simplified.exists(isAlwaysFalse)) {
      logger.debug("Expression simplified to AlwaysFalse (no files can match)")
      return Array(AlwaysFalse)
    }

    // Remove AlwaysTrue filters (they don't constrain anything)
    val filtered = simplified.filterNot(isAlwaysTrue)

    if (filtered.isEmpty) {
      // All filters were AlwaysTrue
      logger.debug("All filters simplified to AlwaysTrue")
      Array.empty
    } else if (filtered.length < filters.length) {
      logger.debug(s"Simplified ${filters.length} filters to ${filtered.length}")
      filtered
    } else {
      filtered
    }
  }

  /**
   * Simplify a single filter expression.
   */
  def simplifyFilter(filter: Filter, schema: StructType): Filter = {
    filter match {
      // IsNull on non-nullable column is always false
      case IsNull(attribute) =>
        getFieldNullable(schema, attribute) match {
          case Some(false) =>
            logger.debug(s"IsNull($attribute) simplified to AlwaysFalse (column is non-nullable)")
            AlwaysFalse
          case _ => filter
        }

      // IsNotNull on non-nullable column is always true
      case IsNotNull(attribute) =>
        getFieldNullable(schema, attribute) match {
          case Some(false) =>
            logger.debug(s"IsNotNull($attribute) simplified to AlwaysTrue (column is non-nullable)")
            AlwaysTrue
          case _ => filter
        }

      // IN with empty list is always false
      case In(_, values) if values.isEmpty =>
        logger.debug(s"IN with empty list simplified to AlwaysFalse")
        AlwaysFalse

      // IN with single value is equivalent to EqualTo
      case In(attribute, values) if values.length == 1 =>
        logger.debug(s"IN($attribute, [${values.head}]) simplified to EqualTo")
        EqualTo(attribute, values.head)

      // AND simplification
      case And(left, right) =>
        val simplifiedLeft = simplifyFilter(left, schema)
        val simplifiedRight = simplifyFilter(right, schema)
        if (isAlwaysFalse(simplifiedLeft) || isAlwaysFalse(simplifiedRight)) {
          AlwaysFalse
        } else if (isAlwaysTrue(simplifiedLeft)) {
          simplifiedRight
        } else if (isAlwaysTrue(simplifiedRight)) {
          simplifiedLeft
        } else if (simplifiedLeft == simplifiedRight) {
          simplifiedLeft // AND(x, x) = x
        } else {
          And(simplifiedLeft, simplifiedRight)
        }

      // OR simplification
      case Or(left, right) =>
        val simplifiedLeft = simplifyFilter(left, schema)
        val simplifiedRight = simplifyFilter(right, schema)
        if (isAlwaysTrue(simplifiedLeft) || isAlwaysTrue(simplifiedRight)) {
          AlwaysTrue
        } else if (isAlwaysFalse(simplifiedLeft)) {
          simplifiedRight
        } else if (isAlwaysFalse(simplifiedRight)) {
          simplifiedLeft
        } else if (simplifiedLeft == simplifiedRight) {
          simplifiedLeft // OR(x, x) = x
        } else {
          Or(simplifiedLeft, simplifiedRight)
        }

      // NOT simplification
      case Not(child) =>
        val simplifiedChild = simplifyFilter(child, schema)
        if (isAlwaysTrue(simplifiedChild)) {
          AlwaysFalse
        } else if (isAlwaysFalse(simplifiedChild)) {
          AlwaysTrue
        } else {
          simplifiedChild match {
            case Not(inner) => inner // NOT(NOT(x)) = x
            case c => Not(c)
          }
        }

      // EqualTo with null value on non-nullable column
      case EqualTo(attribute, null) =>
        getFieldNullable(schema, attribute) match {
          case Some(false) =>
            logger.debug(s"EqualTo($attribute, null) simplified to AlwaysFalse (column is non-nullable)")
            AlwaysFalse
          case _ => filter
        }

      // Default: no simplification
      case _ => filter
    }
  }

  /**
   * Check if filters can be satisfied at all (none are AlwaysFalse).
   */
  def canBeSatisfied(filters: Array[Filter], schema: StructType): Boolean = {
    val simplified = simplify(filters, schema)
    !simplified.exists(isAlwaysFalse)
  }

  /**
   * Check if the result of simplification indicates no filtering is needed.
   */
  def isNoOp(filters: Array[Filter], schema: StructType): Boolean = {
    val simplified = simplify(filters, schema)
    simplified.isEmpty || simplified.forall(isAlwaysTrue)
  }

  /**
   * Get the nullable flag for a field, handling nested fields with dot notation.
   */
  private def getFieldNullable(schema: StructType, attribute: String): Option[Boolean] = {
    // Handle dot notation for nested fields (e.g., "user.name")
    val parts = attribute.split("\\.")

    var currentSchema: StructType = schema
    var i = 0
    while (i < parts.length - 1) {
      currentSchema.fields.find(_.name.equalsIgnoreCase(parts(i))) match {
        case Some(StructField(_, nested: StructType, _, _)) =>
          currentSchema = nested
          i += 1
        case _ =>
          return None // Field not found or not a struct
      }
    }

    // Find the final field
    currentSchema.fields.find(_.name.equalsIgnoreCase(parts.last)).map(_.nullable)
  }

  /**
   * Extract the set of columns referenced by a filter.
   * Useful for determining which stats columns to load.
   */
  def getReferencedColumns(filter: Filter): Set[String] = {
    filter match {
      case EqualTo(attr, _) => Set(attr)
      case EqualNullSafe(attr, _) => Set(attr)
      case GreaterThan(attr, _) => Set(attr)
      case GreaterThanOrEqual(attr, _) => Set(attr)
      case LessThan(attr, _) => Set(attr)
      case LessThanOrEqual(attr, _) => Set(attr)
      case In(attr, _) => Set(attr)
      case IsNull(attr) => Set(attr)
      case IsNotNull(attr) => Set(attr)
      case StringStartsWith(attr, _) => Set(attr)
      case StringEndsWith(attr, _) => Set(attr)
      case StringContains(attr, _) => Set(attr)
      case And(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
      case Or(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
      case Not(child) => getReferencedColumns(child)
      case _ => Set.empty
    }
  }

  /**
   * Get referenced columns from multiple filters.
   */
  def getReferencedColumns(filters: Array[Filter]): Set[String] = {
    filters.flatMap(getReferencedColumns).toSet
  }
}
