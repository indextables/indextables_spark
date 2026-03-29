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

package io.indextables.spark.transaction

import org.apache.spark.sql.sources._
import io.indextables.tantivy4java.filter.PartitionFilter

/**
 * Converts Spark DataSource V2 Filter objects to tantivy4java PartitionFilter JSON.
 *
 * Used for both partition filters (evaluated against partition_values) and data filters
 * (evaluated against min_values/max_values for data skipping). The same JSON format is
 * used for both — the native layer determines the evaluation target.
 */
object SparkFilterToNativeFilter {

  /** Convert filters to JSON string, or null if empty. */
  def convertOrNull(filters: Seq[Filter]): String = {
    if (filters.isEmpty) return null
    val converted = filters.flatMap(convert)
    if (converted.isEmpty) return null
    if (converted.length == 1) converted.head.toJson
    else PartitionFilter.and(converted: _*).toJson
  }

  /** Convert a single Spark Filter to a PartitionFilter. Returns None for unsupported filters. */
  def convert(filter: Filter): Option[PartitionFilter] = filter match {
    case EqualTo(attr, value)            => Some(PartitionFilter.eq(attr, value.toString))
    case EqualNullSafe(attr, value)      =>
      if (value == null) Some(PartitionFilter.isNull(attr))
      else Some(PartitionFilter.eq(attr, value.toString))
    case GreaterThan(attr, value)        => Some(PartitionFilter.gt(attr, value.toString))
    case GreaterThanOrEqual(attr, value) => Some(PartitionFilter.gte(attr, value.toString))
    case LessThan(attr, value)           => Some(PartitionFilter.lt(attr, value.toString))
    case LessThanOrEqual(attr, value)    => Some(PartitionFilter.lte(attr, value.toString))
    case In(attr, values)                =>
      val strValues = values.filter(_ != null).map(_.toString)
      if (strValues.nonEmpty) Some(PartitionFilter.in(attr, strValues: _*))
      else None
    case IsNull(attr)                    => Some(PartitionFilter.isNull(attr))
    case IsNotNull(attr)                 => Some(PartitionFilter.isNotNull(attr))
    case StringStartsWith(attr, value)   => Some(PartitionFilter.stringStartsWith(attr, value))
    case StringEndsWith(attr, value)     => Some(PartitionFilter.stringEndsWith(attr, value))
    case StringContains(attr, value)     => Some(PartitionFilter.stringContains(attr, value))
    case Not(child)                      =>
      convert(child).map(PartitionFilter.not)
    case And(left, right)                =>
      (convert(left), convert(right)) match {
        case (Some(l), Some(r)) => Some(PartitionFilter.and(l, r))
        case (Some(l), None)    => Some(l)
        case (None, Some(r))    => Some(r)
        case (None, None)       => None
      }
    case Or(left, right)                 =>
      (convert(left), convert(right)) match {
        case (Some(l), Some(r)) => Some(PartitionFilter.or(l, r))
        case _                  => None // Can't push OR if one side is unsupported
      }
    case _ => None
  }
}
