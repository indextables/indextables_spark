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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Base trait for bucket aggregation expressions.
 *
 * These expressions are designed for pushdown to the tantivy4java data source. They carry configuration for bucket
 * aggregations (DateHistogram, Histogram, Range) that are executed efficiently inside Quickwit/Tantivy rather than in
 * Spark.
 */
sealed trait BucketExpression extends Expression {

  /** The field/column to bucket on */
  def field: Expression

  /** Extract the field name from the field expression */
  def fieldName: String = field match {
    case attr: AttributeReference       => attr.name
    case UnresolvedAttribute(nameParts) => nameParts.mkString(".")
    case other                          => other.toString
  }

  /** Check if this expression can be pushed down to the data source */
  def canPushDown: Boolean = field match {
    case _: AttributeReference  => true
    case _: UnresolvedAttribute => true
    case _                      => false
  }
}

/**
 * Date histogram bucket expression for time-series aggregations.
 *
 * Groups data into time-based buckets with a fixed interval.
 *
 * Usage: indextables_date_histogram(timestamp_column, '1h') indextables_date_histogram(timestamp_column, '1d', '6h') --
 * with offset
 *
 * @param field
 *   The timestamp/date column to bucket
 * @param interval
 *   The bucket interval (e.g., "1h", "1d", "30m")
 * @param offset
 *   Optional offset to shift bucket boundaries (e.g., "6h" to start day at 6am)
 * @param minDocCount
 *   Minimum document count for bucket to be returned (default: 0)
 * @param hardBoundsMin
 *   Optional minimum timestamp bound (epoch microseconds)
 * @param hardBoundsMax
 *   Optional maximum timestamp bound (epoch microseconds)
 * @param extendedBoundsMin
 *   Optional extended bounds minimum (force bucket creation)
 * @param extendedBoundsMax
 *   Optional extended bounds maximum (force bucket creation)
 */
case class DateHistogramExpression(
  field: Expression,
  interval: String,
  offset: Option[String] = None,
  minDocCount: Long = 0,
  hardBoundsMin: Option[Long] = None,
  hardBoundsMax: Option[Long] = None,
  extendedBoundsMin: Option[Long] = None,
  extendedBoundsMax: Option[Long] = None)
    extends BucketExpression
    with Unevaluable {

  // Validate interval format at construction time
  require(
    DateHistogramExpression.isValidInterval(interval),
    s"Invalid date histogram interval: '$interval'. " +
      s"Supported formats: 1ms, 10ms, 100ms, 1s, 30s, 1m, 5m, 15m, 30m, 1h, 6h, 12h, 1d, 7d"
  )

  override def dataType: DataType = TimestampType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(field)

  override def prettyName: String = "indextables_date_histogram"

  override def sql: String = {
    val offsetPart = offset.map(o => s", offset => '$o'").getOrElse("")
    val minDocPart = if (minDocCount > 0) s", min_doc_count => $minDocCount" else ""
    s"$prettyName(${field.sql}, '$interval'$offsetPart$minDocPart)"
  }

  override def toString: String = {
    val opts = Seq(
      offset.map(o => s"offset=$o"),
      if (minDocCount > 0) Some(s"minDocCount=$minDocCount") else None,
      hardBoundsMin.map(v => s"hardBoundsMin=$v"),
      hardBoundsMax.map(v => s"hardBoundsMax=$v")
    ).flatten
    val optsStr = if (opts.nonEmpty) s", ${opts.mkString(", ")}" else ""
    s"DateHistogramExpression($field, $interval$optsStr)"
  }

  override def checkInputDataTypes(): TypeCheckResult =
    field.dataType match {
      case TimestampType | DateType => TypeCheckResult.TypeCheckSuccess
      case other =>
        TypeCheckResult.TypeCheckFailure(
          s"indextables_date_histogram requires a timestamp or date column, got $other"
        )
    }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)
}

object DateHistogramExpression {

  /** Pattern for valid interval formats: number + unit */
  private val intervalPattern = """(\d+)(ms|s|m|h|d)""".r

  /** Check if the interval string is valid */
  def isValidInterval(interval: String): Boolean =
    interval match {
      case intervalPattern(_, _) => true
      case _                     => false
    }

  /** Common interval presets */
  val INTERVAL_1_MINUTE   = "1m"
  val INTERVAL_5_MINUTES  = "5m"
  val INTERVAL_15_MINUTES = "15m"
  val INTERVAL_30_MINUTES = "30m"
  val INTERVAL_1_HOUR     = "1h"
  val INTERVAL_6_HOURS    = "6h"
  val INTERVAL_12_HOURS   = "12h"
  val INTERVAL_1_DAY      = "1d"
  val INTERVAL_7_DAYS     = "7d"
}

/**
 * Numeric histogram bucket expression for distribution analysis.
 *
 * Groups data into fixed-width numeric buckets.
 *
 * Usage: indextables_histogram(price, 100) -- $100 buckets indextables_histogram(score, 10, 5) -- buckets offset by 5
 *
 * @param field
 *   The numeric column to bucket
 * @param interval
 *   The fixed bucket width
 * @param offset
 *   Optional offset to shift bucket boundaries (default: 0.0)
 * @param minDocCount
 *   Minimum document count for bucket to be returned (default: 0)
 * @param hardBoundsMin
 *   Optional minimum bound
 * @param hardBoundsMax
 *   Optional maximum bound
 * @param extendedBoundsMin
 *   Optional extended bounds minimum (force bucket creation)
 * @param extendedBoundsMax
 *   Optional extended bounds maximum (force bucket creation)
 */
case class HistogramExpression(
  field: Expression,
  interval: Double,
  offset: Double = 0.0,
  minDocCount: Long = 0,
  hardBoundsMin: Option[Double] = None,
  hardBoundsMax: Option[Double] = None,
  extendedBoundsMin: Option[Double] = None,
  extendedBoundsMax: Option[Double] = None)
    extends BucketExpression
    with Unevaluable {

  require(interval > 0, s"Histogram interval must be positive, got: $interval")

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(field)

  override def prettyName: String = "indextables_histogram"

  override def sql: String = {
    val offsetPart = if (offset != 0.0) s", offset => $offset" else ""
    val minDocPart = if (minDocCount > 0) s", min_doc_count => $minDocCount" else ""
    s"$prettyName(${field.sql}, $interval$offsetPart$minDocPart)"
  }

  override def toString: String = {
    val opts = Seq(
      if (offset != 0.0) Some(s"offset=$offset") else None,
      if (minDocCount > 0) Some(s"minDocCount=$minDocCount") else None,
      hardBoundsMin.map(v => s"hardBoundsMin=$v"),
      hardBoundsMax.map(v => s"hardBoundsMax=$v")
    ).flatten
    val optsStr = if (opts.nonEmpty) s", ${opts.mkString(", ")}" else ""
    s"HistogramExpression($field, $interval$optsStr)"
  }

  override def checkInputDataTypes(): TypeCheckResult =
    field.dataType match {
      case _: NumericType => TypeCheckResult.TypeCheckSuccess
      case other =>
        TypeCheckResult.TypeCheckFailure(
          s"indextables_histogram requires a numeric column, got $other"
        )
    }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)
}

/**
 * A single range bucket definition for RangeExpression.
 *
 * @param key
 *   The name/key for this bucket
 * @param from
 *   Lower bound (inclusive), None for unbounded
 * @param to
 *   Upper bound (exclusive), None for unbounded
 */
case class RangeBucket(
  key: String,
  from: Option[Double],
  to: Option[Double]) {
  require(key.nonEmpty, "Range bucket key cannot be empty")
  require(from.isDefined || to.isDefined, s"Range bucket '$key' must have at least one bound defined")

  override def toString: String = {
    val fromStr = from.map(_.toString).getOrElse("*")
    val toStr   = to.map(_.toString).getOrElse("*")
    s"$key:[$fromStr,$toStr)"
  }
}

/**
 * Range bucket expression for custom-defined range buckets.
 *
 * Groups data into user-defined named ranges.
 *
 * Usage: indextables_range(price, 'budget', NULL, 50, 'mid', 50, 200, 'premium', 200, NULL)
 *
 * @param field
 *   The numeric column to bucket
 * @param ranges
 *   Sequence of named ranges with (name, from, to)
 */
case class RangeExpression(
  field: Expression,
  ranges: Seq[RangeBucket])
    extends BucketExpression
    with Unevaluable {

  require(ranges.nonEmpty, "Range expression requires at least one range")

  // Validate ranges don't overlap (tantivy requirement)
  validateNoOverlap()

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(field)

  override def prettyName: String = "indextables_range"

  override def sql: String = {
    val rangesStr = ranges
      .map { r =>
        val fromStr = r.from.map(_.toString).getOrElse("NULL")
        val toStr   = r.to.map(_.toString).getOrElse("NULL")
        s"'${r.key}', $fromStr, $toStr"
      }
      .mkString(", ")
    s"$prettyName(${field.sql}, $rangesStr)"
  }

  override def toString: String =
    s"RangeExpression($field, [${ranges.mkString(", ")}])"

  override def checkInputDataTypes(): TypeCheckResult =
    field.dataType match {
      case _: NumericType => TypeCheckResult.TypeCheckSuccess
      case other =>
        TypeCheckResult.TypeCheckFailure(
          s"indextables_range requires a numeric column, got $other"
        )
    }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)

  private def validateNoOverlap(): Unit = {
    val sortedRanges = ranges.sortBy(_.from.getOrElse(Double.MinValue))
    for (i <- 0 until sortedRanges.length - 1) {
      val current = sortedRanges(i)
      val next    = sortedRanges(i + 1)
      current.to.foreach { currentTo =>
        next.from.foreach { nextFrom =>
          require(
            currentTo <= nextFrom,
            s"Overlapping ranges not supported by tantivy: '${current.key}' ends at $currentTo " +
              s"but '${next.key}' starts at $nextFrom"
          )
        }
      }
    }
  }
}

/** Companion object with utility methods for bucket expressions. */
object BucketExpressions {

  /** Check if an expression is a bucket expression. */
  def isBucketExpression(expr: Expression): Boolean =
    expr match {
      case _: DateHistogramExpression => true
      case _: HistogramExpression     => true
      case _: RangeExpression         => true
      case _                          => false
    }

  /** Extract bucket expression from a complex expression tree. Returns the first bucket expression found. */
  def extractBucketExpression(expr: Expression): Option[BucketExpression] =
    expr match {
      case b: BucketExpression => Some(b)
      case _                   => None
    }
}
