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

package io.indextables.spark.util

import org.apache.spark.sql.sources._

/** Shared utilities for extracting metadata from Spark Filter objects. */
object FilterUtils {

  /**
   * Extract the set of field names referenced by a filter, recursing through And/Or/Not.
   *
   * @param filter
   *   A Spark V1 Filter
   * @return
   *   Set of field/attribute names referenced in the filter
   */
  def extractFieldNames(filter: Filter): Set[String] = filter match {
    case EqualTo(attr, _)            => Set(attr)
    case EqualNullSafe(attr, _)      => Set(attr)
    case GreaterThan(attr, _)        => Set(attr)
    case GreaterThanOrEqual(attr, _) => Set(attr)
    case LessThan(attr, _)           => Set(attr)
    case LessThanOrEqual(attr, _)    => Set(attr)
    case In(attr, _)                 => Set(attr)
    case IsNull(attr)                => Set(attr)
    case IsNotNull(attr)             => Set(attr)
    case StringStartsWith(attr, _)   => Set(attr)
    case StringEndsWith(attr, _)     => Set(attr)
    case StringContains(attr, _)     => Set(attr)
    case And(left, right)            => extractFieldNames(left) ++ extractFieldNames(right)
    case Or(left, right)             => extractFieldNames(left) ++ extractFieldNames(right)
    case Not(child)                  => extractFieldNames(child)
    case _                           => Set.empty
  }

  /** Extract field names from multiple filters. */
  def extractFieldNames(filters: Array[Filter]): Set[String] =
    filters.flatMap(extractFieldNames).toSet

  /**
   * Extract field names only from value-bearing filters (excludes IsNull/IsNotNull).
   *
   * Used by removeRedundantIsNotNull to determine which fields have value constraints that imply non-null.
   */
  def extractValueFilterFieldNames(filter: Filter): Set[String] = filter match {
    case EqualTo(attr, _)            => Set(attr)
    case EqualNullSafe(attr, _)      => Set(attr)
    case GreaterThan(attr, _)        => Set(attr)
    case GreaterThanOrEqual(attr, _) => Set(attr)
    case LessThan(attr, _)           => Set(attr)
    case LessThanOrEqual(attr, _)    => Set(attr)
    case In(attr, _)                 => Set(attr)
    case StringStartsWith(attr, _)   => Set(attr)
    case StringEndsWith(attr, _)     => Set(attr)
    case StringContains(attr, _)     => Set(attr)
    case And(left, right)            => extractValueFilterFieldNames(left) ++ extractValueFilterFieldNames(right)
    case Or(left, right)             => extractValueFilterFieldNames(left) ++ extractValueFilterFieldNames(right)
    case Not(child)                  => extractValueFilterFieldNames(child)
    case _                           => Set.empty
  }

  /** Extract value-bearing field names from multiple filters. */
  def extractValueFilterFieldNames(filters: Array[Filter]): Set[String] =
    filters.flatMap(extractValueFilterFieldNames).toSet
}
