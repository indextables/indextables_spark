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

package io.indextables.spark.sync

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And => CatalystAnd, Not => CatalystNot, Or => CatalystOr}
import org.apache.spark.sql.catalyst.expressions.{EqualTo => CatalystEqualTo, GreaterThan => CatalystGreaterThan}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.{GreaterThanOrEqual => CatalystGreaterThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{
  In => CatalystIn,
  IsNotNull => CatalystIsNotNull,
  IsNull => CatalystIsNull
}
import org.apache.spark.sql.catalyst.expressions.{
  LessThan => CatalystLessThan,
  LessThanOrEqual => CatalystLessThanOrEqual
}
import org.apache.spark.sql.types._

import io.indextables.spark.transaction.PartitionPredicateUtils
import io.indextables.tantivy4java.filter.PartitionFilter
import org.slf4j.LoggerFactory

/**
 * Converts Spark SQL WHERE predicate strings into tantivy4java PartitionFilter objects for native-side partition
 * pruning. Filtered-out entries never cross the JNI boundary, reducing serialization and Java-side processing.
 *
 * Pattern follows PartitionPredicateUtils.expressionToFilter() but targets PartitionFilter instead of Spark Filter.
 */
object SparkPredicateToPartitionFilter {

  private val logger = LoggerFactory.getLogger(SparkPredicateToPartitionFilter.getClass)

  /**
   * Convert Spark SQL WHERE predicate strings into a tantivy4java PartitionFilter.
   *
   * @param predicates
   *   Raw predicate strings from the WHERE clause
   * @param partitionColumns
   *   Partition column names (used to determine which predicates apply)
   * @param sparkSession
   *   SparkSession for parsing expressions
   * @param fullSchema
   *   Optional full table schema for type-aware conversion (IntegerType/LongType → withType("long"))
   * @return
   *   Some(PartitionFilter) if at least one predicate converts successfully, None otherwise
   */
  def convert(
    predicates: Seq[String],
    partitionColumns: Seq[String],
    sparkSession: SparkSession,
    fullSchema: Option[StructType] = None
  ): Option[PartitionFilter] = {
    if (predicates.isEmpty || partitionColumns.isEmpty) return None

    val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns, fullSchema)
    val partColSet      = partitionColumns.map(_.toLowerCase).toSet

    val filters = predicates.flatMap { predicate =>
      try {
        val expression = sparkSession.sessionState.sqlParser.parseExpression(predicate)
        expressionToPartitionFilter(expression, partColSet, partitionSchema)
      } catch {
        case e: Exception =>
          logger.debug(s"Cannot parse predicate for native filter: ${e.getMessage}")
          None
      }
    }

    filters match {
      case Seq()       => None
      case Seq(single) => Some(single)
      case multiple    => Some(PartitionFilter.and(multiple: _*))
    }
  }

  private def expressionToPartitionFilter(
    expression: Expression,
    partColSet: Set[String],
    partitionSchema: StructType
  ): Option[PartitionFilter] =
    expression match {
      // Equality: column = null → isNull; column = value → eq
      case CatalystEqualTo(UnresolvedAttribute(nameParts), Literal(null, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase)) Some(PartitionFilter.isNull(col)) else None
      case CatalystEqualTo(Literal(null, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase)) Some(PartitionFilter.isNull(col)) else None
      case CatalystEqualTo(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.eq(col, literalToString(value)))
        else None
      case CatalystEqualTo(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.eq(col, literalToString(value)))
        else None

      // Greater than: column > value
      case CatalystGreaterThan(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.gt(col, literalToString(value)), col, partitionSchema))
        else None
      case CatalystGreaterThan(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.lt(col, literalToString(value)), col, partitionSchema))
        else None

      // Greater than or equal: column >= value
      case CatalystGreaterThanOrEqual(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.gte(col, literalToString(value)), col, partitionSchema))
        else None
      case CatalystGreaterThanOrEqual(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.lte(col, literalToString(value)), col, partitionSchema))
        else None

      // Less than: column < value
      case CatalystLessThan(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.lt(col, literalToString(value)), col, partitionSchema))
        else None
      case CatalystLessThan(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.gt(col, literalToString(value)), col, partitionSchema))
        else None

      // Less than or equal: column <= value
      case CatalystLessThanOrEqual(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.lte(col, literalToString(value)), col, partitionSchema))
        else None
      case CatalystLessThanOrEqual(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(maybeWithType(PartitionFilter.gte(col, literalToString(value)), col, partitionSchema))
        else None

      // IN: column IN (value1, value2, ...)
      case CatalystIn(UnresolvedAttribute(nameParts), values) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase)) {
          val literalValues = values.collect { case Literal(v, _) => literalToString(v) }
          if (literalValues.length == values.length) {
            Some(PartitionFilter.in(col, literalValues: _*))
          } else None
        } else None

      // IS NULL
      case CatalystIsNull(UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.isNull(col))
        else None

      // IS NOT NULL
      case CatalystIsNotNull(UnresolvedAttribute(nameParts)) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.isNotNull(col))
        else None

      // AND: left AND right
      case CatalystAnd(left, right) =>
        (expressionToPartitionFilter(left, partColSet, partitionSchema),
          expressionToPartitionFilter(right, partColSet, partitionSchema)) match {
          case (Some(l), Some(r)) => Some(PartitionFilter.and(l, r))
          case (Some(l), None)    => Some(l)
          case (None, Some(r))    => Some(r)
          case (None, None)       => None
        }

      // OR: left OR right (both sides must convert)
      case CatalystOr(left, right) =>
        (expressionToPartitionFilter(left, partColSet, partitionSchema),
          expressionToPartitionFilter(right, partColSet, partitionSchema)) match {
          case (Some(l), Some(r)) => Some(PartitionFilter.or(l, r))
          case _                  => None
        }

      // NOT(EqualTo) → neq (more efficient than not(eq(...)))
      case CatalystNot(CatalystEqualTo(UnresolvedAttribute(nameParts), Literal(value, _))) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.neq(col, literalToString(value)))
        else None
      case CatalystNot(CatalystEqualTo(Literal(value, _), UnresolvedAttribute(nameParts))) =>
        val col = nameParts.mkString(".")
        if (partColSet.contains(col.toLowerCase))
          Some(PartitionFilter.neq(col, literalToString(value)))
        else None

      // NOT: NOT expr (general case)
      case CatalystNot(child) =>
        expressionToPartitionFilter(child, partColSet, partitionSchema).map(PartitionFilter.not)

      case _ =>
        logger.debug(s"Cannot convert expression to PartitionFilter: ${expression.getClass.getSimpleName}")
        None
    }

  /** Add .withType("long") for IntegerType/LongType partition columns so Rust compares numerically. */
  private def maybeWithType(filter: PartitionFilter, col: String, schema: StructType): PartitionFilter =
    try {
      schema(schema.fieldIndex(col)).dataType match {
        case IntegerType | LongType => filter.withType("long")
        case _                      => filter
      }
    } catch {
      case _: IllegalArgumentException => filter
    }

  private def literalToString(value: Any): String = value match {
    case null                                => null
    case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
    case other                               => other.toString
  }
}
