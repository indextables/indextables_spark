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

package io.indextables.spark.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.slf4j.LoggerFactory

/**
 * Builder for bucket aggregation function expressions.
 *
 * Handles parsing of function arguments for:
 *   - indextables_date_histogram(column, interval, [offset], [min_doc_count])
 *   - indextables_histogram(column, interval, [offset], [min_doc_count])
 *   - indextables_range(column, name1, from1, to1, name2, from2, to2, ...)
 */
object BucketFunctionBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Build a DateHistogramExpression from function arguments.
   *
   * Syntax: indextables_date_histogram(column, interval) indextables_date_histogram(column, interval, offset)
   * indextables_date_histogram(column, interval, offset, min_doc_count)
   */
  def buildDateHistogram(args: Seq[Expression]): DateHistogramExpression = {
    logger.debug(s"Building DateHistogramExpression from ${args.length} arguments")

    require(
      args.length >= 2,
      s"indextables_date_histogram requires at least 2 arguments (column, interval), got ${args.length}"
    )

    val field    = args(0)
    val interval = extractStringLiteral(args(1), "interval")

    val offset = if (args.length >= 3) {
      Some(extractStringLiteral(args(2), "offset"))
    } else {
      None
    }

    val minDocCount = if (args.length >= 4) {
      extractLongLiteral(args(3), "min_doc_count")
    } else {
      0L
    }

    logger.debug(
      s"Created DateHistogramExpression: field=$field, interval=$interval, offset=$offset, minDocCount=$minDocCount"
    )

    DateHistogramExpression(
      field = field,
      interval = interval,
      offset = offset,
      minDocCount = minDocCount
    )
  }

  /**
   * Build a HistogramExpression from function arguments.
   *
   * Syntax: indextables_histogram(column, interval) indextables_histogram(column, interval, offset)
   * indextables_histogram(column, interval, offset, min_doc_count)
   */
  def buildHistogram(args: Seq[Expression]): HistogramExpression = {
    logger.debug(s"Building HistogramExpression from ${args.length} arguments")

    require(
      args.length >= 2,
      s"indextables_histogram requires at least 2 arguments (column, interval), got ${args.length}"
    )

    val field    = args(0)
    val interval = extractDoubleLiteral(args(1), "interval")

    val offset = if (args.length >= 3) {
      extractDoubleLiteral(args(2), "offset")
    } else {
      0.0
    }

    val minDocCount = if (args.length >= 4) {
      extractLongLiteral(args(3), "min_doc_count")
    } else {
      0L
    }

    logger.debug(
      s"Created HistogramExpression: field=$field, interval=$interval, offset=$offset, minDocCount=$minDocCount"
    )

    HistogramExpression(
      field = field,
      interval = interval,
      offset = offset,
      minDocCount = minDocCount
    )
  }

  /**
   * Build a RangeExpression from function arguments.
   *
   * Syntax: indextables_range(column, name1, from1, to1, name2, from2, to2, ...)
   *
   * Each range is specified as 3 consecutive arguments: name, from, to Use NULL for unbounded from/to values.
   */
  def buildRange(args: Seq[Expression]): RangeExpression = {
    logger.debug(s"Building RangeExpression from ${args.length} arguments")

    require(
      args.length >= 4,
      s"indextables_range requires at least 4 arguments (column, name, from, to), got ${args.length}"
    )

    // Remaining args after column should be in groups of 3: (name, from, to)
    require(
      (args.length - 1) % 3 == 0,
      s"indextables_range range arguments must be in groups of 3 (name, from, to). " +
        s"Got ${args.length - 1} range arguments which is not divisible by 3."
    )

    val field     = args(0)
    val rangeArgs = args.drop(1)

    val ranges = rangeArgs
      .grouped(3)
      .zipWithIndex
      .map {
        case (Seq(nameExpr, fromExpr, toExpr), idx) =>
          val name = extractStringLiteral(nameExpr, s"range[$idx].name")
          val from = extractOptionalDouble(fromExpr)
          val to   = extractOptionalDouble(toExpr)

          require(
            from.isDefined || to.isDefined,
            s"Range '$name' must have at least one bound defined (from or to)"
          )

          RangeBucket(name, from, to)

        case (other, idx) =>
          throw new IllegalArgumentException(
            s"Invalid range arguments at index $idx: expected 3 arguments (name, from, to), got ${other.length}"
          )
      }
      .toSeq

    logger.debug(s"Created RangeExpression: field=$field, ranges=${ranges.map(_.toString).mkString(", ")}")

    RangeExpression(field = field, ranges = ranges)
  }

  // Helper methods for literal extraction

  private def extractStringLiteral(expr: Expression, paramName: String): String =
    expr match {
      case Literal(value: UTF8String, StringType)      => value.toString
      case Literal(value: String, StringType)          => value
      case Literal(value, StringType) if value != null => value.toString
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a string literal, got: $expr (${expr.getClass.getSimpleName})"
        )
    }

  private def extractDoubleLiteral(expr: Expression, paramName: String): Double =
    expr match {
      // Spark's Decimal type (used for SQL numeric literals like 50.0)
      case Literal(value: Decimal, _: DecimalType) => value.toDouble
      case Literal(value: Decimal, _)              => value.toDouble
      // Java BigDecimal
      case Literal(value: java.math.BigDecimal, _) => value.doubleValue()
      // Scala BigDecimal
      case Literal(value: scala.math.BigDecimal, _) => value.toDouble
      // Standard numeric types
      case Literal(value: Number, _)                    => value.doubleValue()
      case Literal(value: java.lang.Double, DoubleType) => value.doubleValue()
      case Literal(value: java.lang.Float, _)           => value.doubleValue()
      case Literal(value: java.lang.Integer, _)         => value.doubleValue()
      case Literal(value: java.lang.Long, _)            => value.doubleValue()
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a numeric literal, got: $expr (${expr.getClass.getSimpleName})"
        )
    }

  private def extractLongLiteral(expr: Expression, paramName: String): Long =
    expr match {
      // Spark's Decimal type
      case Literal(value: Decimal, _: DecimalType) => value.toLong
      case Literal(value: Decimal, _)              => value.toLong
      // Java BigDecimal
      case Literal(value: java.math.BigDecimal, _) => value.longValue()
      // Scala BigDecimal
      case Literal(value: scala.math.BigDecimal, _) => value.toLong
      // Standard numeric types
      case Literal(value: Number, _)                => value.longValue()
      case Literal(value: java.lang.Long, LongType) => value.longValue()
      case Literal(value: java.lang.Integer, _)     => value.longValue()
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a numeric literal, got: $expr (${expr.getClass.getSimpleName})"
        )
    }

  private def extractOptionalDouble(expr: Expression): Option[Double] =
    expr match {
      case Literal(null, _) => None
      // Spark's Decimal type (used for SQL numeric literals like 50.0)
      case Literal(value: Decimal, _: DecimalType) => Some(value.toDouble)
      case Literal(value: Decimal, _)              => Some(value.toDouble)
      // Java BigDecimal
      case Literal(value: java.math.BigDecimal, _) => Some(value.doubleValue())
      // Scala BigDecimal
      case Literal(value: scala.math.BigDecimal, _) => Some(value.toDouble)
      // Standard numeric types
      case Literal(value: Number, _)            => Some(value.doubleValue())
      case Literal(value: java.lang.Double, _)  => Some(value.doubleValue())
      case Literal(value: java.lang.Float, _)   => Some(value.doubleValue())
      case Literal(value: java.lang.Integer, _) => Some(value.doubleValue())
      case Literal(value: java.lang.Long, _)    => Some(value.doubleValue())
      case _                                    => None // Treat non-literal as unbounded
    }
}
