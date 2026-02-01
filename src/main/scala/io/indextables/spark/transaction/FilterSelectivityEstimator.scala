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

import org.slf4j.LoggerFactory

/**
 * Estimates filter selectivity for optimal filter ordering. More selective filters (lower score) are evaluated first to
 * enable early termination in AND conditions.
 *
 * Selectivity scores (lower = more selective = evaluate first):
 *   - EQUALITY_SCORE = 1 (most selective, single value match)
 *   - IN_SMALL_SCORE = 2 (<=5 values, still very selective)
 *   - IN_LARGE_SCORE = 5 (>5 values)
 *   - RANGE_SCORE = 10 (ranges match multiple values)
 *   - IS_NULL_SCORE = 3 (null checks are usually selective)
 *   - IS_NOT_NULL_SCORE = 8 (most values are non-null)
 *   - STRING_STARTS_WITH = 4 (prefix match, moderately selective)
 *   - STRING_ENDS_WITH = 6 (suffix match, less selective)
 *   - STRING_CONTAINS = 7 (substring match, least selective)
 *   - COMPOUND_AND = (sum of children) (preserve order for short-circuit)
 *   - COMPOUND_OR = (max of children) (only as selective as least selective branch)
 *   - NOT = (child score) (same selectivity as negated filter)
 *   - UNKNOWN = 100 (conservative default)
 */
object FilterSelectivityEstimator {

  private val logger = LoggerFactory.getLogger(FilterSelectivityEstimator.getClass)

  // Selectivity scores - lower means more selective (evaluate first)
  val EQUALITY_SCORE           = 1
  val IN_SMALL_SCORE           = 2 // <= 5 values
  val IN_LARGE_SCORE           = 5 // > 5 values
  val IS_NULL_SCORE            = 3
  val STRING_STARTS_WITH_SCORE = 4
  val IN_MEDIUM_SCORE          = 4 // 6-10 values
  val STRING_ENDS_WITH_SCORE   = 6
  val STRING_CONTAINS_SCORE    = 7
  val IS_NOT_NULL_SCORE        = 8
  val RANGE_SCORE              = 10
  val UNKNOWN_SCORE            = 100

  // Threshold for "small" IN clause
  val SMALL_IN_THRESHOLD  = 5
  val MEDIUM_IN_THRESHOLD = 10

  /**
   * Estimate the selectivity of a filter.
   *
   * @param filter
   *   The filter to estimate
   * @return
   *   Selectivity score (lower = more selective)
   */
  def estimateSelectivity(filter: Filter): Int =
    filter match {
      // Most selective: equality
      case EqualTo(_, _)       => EQUALITY_SCORE
      case EqualNullSafe(_, _) => EQUALITY_SCORE

      // IN clauses: depends on number of values
      case In(_, values) =>
        if (values.length <= SMALL_IN_THRESHOLD) IN_SMALL_SCORE
        else if (values.length <= MEDIUM_IN_THRESHOLD) IN_MEDIUM_SCORE
        else IN_LARGE_SCORE

      // Null checks
      case IsNull(_)    => IS_NULL_SCORE
      case IsNotNull(_) => IS_NOT_NULL_SCORE

      // String patterns
      case StringStartsWith(_, _) => STRING_STARTS_WITH_SCORE
      case StringEndsWith(_, _)   => STRING_ENDS_WITH_SCORE
      case StringContains(_, _)   => STRING_CONTAINS_SCORE

      // Range filters (less selective, match multiple values)
      case GreaterThan(_, _)        => RANGE_SCORE
      case GreaterThanOrEqual(_, _) => RANGE_SCORE
      case LessThan(_, _)           => RANGE_SCORE
      case LessThanOrEqual(_, _)    => RANGE_SCORE

      // Compound filters
      case And(left, right) =>
        // For AND, evaluate most selective first
        estimateSelectivity(left) + estimateSelectivity(right)

      case Or(left, right) =>
        // For OR, selectivity is limited by least selective branch
        math.max(estimateSelectivity(left), estimateSelectivity(right))

      case Not(child) =>
        // NOT inverts selectivity, but we use same score for simplicity
        estimateSelectivity(child)

      // Unknown filter types
      case _ => UNKNOWN_SCORE
    }

  /**
   * Order filters by selectivity for optimal evaluation. Most selective filters (lowest score) come first.
   *
   * @param filters
   *   Array of filters to order
   * @return
   *   Array of filters ordered by selectivity (most selective first)
   */
  def orderBySelectivity(filters: Array[Filter]): Array[Filter] =
    if (filters.length <= 1) {
      filters
    } else {
      val ordered = filters.sortBy(estimateSelectivity)
      logger.debug(
        s"Ordered ${filters.length} filters by selectivity: " +
          ordered.map(f => s"${f.getClass.getSimpleName}(${estimateSelectivity(f)})").mkString(", ")
      )
      ordered
    }

  /**
   * Check if a filter can use the partition index for O(1) lookup. Only equality and small IN filters qualify.
   *
   * @param filter
   *   The filter to check
   * @return
   *   true if the filter can use index lookup
   */
  def canUseIndexLookup(filter: Filter): Boolean =
    filter match {
      case EqualTo(_, _)       => true
      case EqualNullSafe(_, _) => true
      case In(_, values)       => values.length <= MEDIUM_IN_THRESHOLD
      case _                   => false
    }

  /**
   * Extract filters that can use index lookup vs those requiring evaluation.
   *
   * @param filters
   *   Array of filters
   * @return
   *   Tuple of (indexable filters, remaining filters)
   */
  def partitionFilters(filters: Array[Filter]): (Array[Filter], Array[Filter]) =
    filters.partition(canUseIndexLookup)

  /** Get a human-readable selectivity description. */
  def describeSelectivity(filter: Filter): String = {
    val score = estimateSelectivity(filter)
    val level = score match {
      case s if s <= 2  => "very high"
      case s if s <= 5  => "high"
      case s if s <= 10 => "medium"
      case s if s <= 50 => "low"
      case _            => "very low"
    }
    s"${filter.getClass.getSimpleName}: $level selectivity (score=$score)"
  }
}
