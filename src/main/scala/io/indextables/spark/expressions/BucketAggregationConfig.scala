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

/**
 * Configuration for bucket aggregations extracted from GROUP BY expressions. These configurations are used by the
 * ScanBuilder to create appropriate tantivy4java aggregations.
 */
sealed trait BucketAggregationConfig extends Serializable {

  /** The name of the field to bucket on */
  def fieldName: String

  /** Human-readable description of the bucket configuration */
  def description: String
}

/**
 * Configuration for date histogram bucket aggregation.
 *
 * @param fieldName
 *   The timestamp/date field to bucket
 * @param interval
 *   The bucket interval (e.g., "1h", "1d", "30m")
 * @param offset
 *   Optional offset to shift bucket boundaries
 * @param minDocCount
 *   Minimum document count for bucket to be returned
 * @param hardBoundsMin
 *   Optional minimum timestamp bound (epoch microseconds)
 * @param hardBoundsMax
 *   Optional maximum timestamp bound (epoch microseconds)
 * @param extendedBoundsMin
 *   Optional extended bounds minimum
 * @param extendedBoundsMax
 *   Optional extended bounds maximum
 */
case class DateHistogramConfig(
  fieldName: String,
  interval: String,
  offset: Option[String] = None,
  minDocCount: Long = 0,
  hardBoundsMin: Option[Long] = None,
  hardBoundsMax: Option[Long] = None,
  extendedBoundsMin: Option[Long] = None,
  extendedBoundsMax: Option[Long] = None)
    extends BucketAggregationConfig {

  override def description: String = {
    val opts = Seq(
      offset.map(o => s"offset=$o"),
      if (minDocCount > 0) Some(s"minDocCount=$minDocCount") else None
    ).flatten
    val optsStr = if (opts.nonEmpty) s" (${opts.mkString(", ")})" else ""
    s"DateHistogram($fieldName, interval=$interval$optsStr)"
  }
}

/**
 * Configuration for numeric histogram bucket aggregation.
 *
 * @param fieldName
 *   The numeric field to bucket
 * @param interval
 *   The fixed bucket width
 * @param offset
 *   Offset to shift bucket boundaries (default: 0.0)
 * @param minDocCount
 *   Minimum document count for bucket to be returned
 * @param hardBoundsMin
 *   Optional minimum bound
 * @param hardBoundsMax
 *   Optional maximum bound
 * @param extendedBoundsMin
 *   Optional extended bounds minimum
 * @param extendedBoundsMax
 *   Optional extended bounds maximum
 */
case class HistogramConfig(
  fieldName: String,
  interval: Double,
  offset: Double = 0.0,
  minDocCount: Long = 0,
  hardBoundsMin: Option[Double] = None,
  hardBoundsMax: Option[Double] = None,
  extendedBoundsMin: Option[Double] = None,
  extendedBoundsMax: Option[Double] = None)
    extends BucketAggregationConfig {

  override def description: String = {
    val opts = Seq(
      if (offset != 0.0) Some(s"offset=$offset") else None,
      if (minDocCount > 0) Some(s"minDocCount=$minDocCount") else None
    ).flatten
    val optsStr = if (opts.nonEmpty) s" (${opts.mkString(", ")})" else ""
    s"Histogram($fieldName, interval=$interval$optsStr)"
  }
}

/**
 * Configuration for range bucket aggregation.
 *
 * @param fieldName
 *   The numeric field to bucket
 * @param ranges
 *   Sequence of named ranges
 */
case class RangeConfig(
  fieldName: String,
  ranges: Seq[RangeBucket])
    extends BucketAggregationConfig {

  override def description: String =
    s"Range($fieldName, ranges=[${ranges.map(_.toString).mkString(", ")}])"
}

/** Utility object for creating bucket configurations from expressions. */
object BucketAggregationConfig {

  /** Create a BucketAggregationConfig from a BucketExpression. */
  def fromExpression(expr: BucketExpression): BucketAggregationConfig =
    expr match {
      case dh: DateHistogramExpression =>
        DateHistogramConfig(
          fieldName = dh.fieldName,
          interval = dh.interval,
          offset = dh.offset,
          minDocCount = dh.minDocCount,
          hardBoundsMin = dh.hardBoundsMin,
          hardBoundsMax = dh.hardBoundsMax,
          extendedBoundsMin = dh.extendedBoundsMin,
          extendedBoundsMax = dh.extendedBoundsMax
        )

      case h: HistogramExpression =>
        HistogramConfig(
          fieldName = h.fieldName,
          interval = h.interval,
          offset = h.offset,
          minDocCount = h.minDocCount,
          hardBoundsMin = h.hardBoundsMin,
          hardBoundsMax = h.hardBoundsMax,
          extendedBoundsMin = h.extendedBoundsMin,
          extendedBoundsMax = h.extendedBoundsMax
        )

      case r: RangeExpression =>
        RangeConfig(
          fieldName = r.fieldName,
          ranges = r.ranges
        )
    }
}
