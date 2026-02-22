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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.sources._

import org.slf4j.LoggerFactory

/**
 * Partition pruner for Avro state manifests.
 *
 * This pruner filters manifests based on partition bounds, allowing the reader to skip manifests that cannot contain
 * matching files based on partition predicates.
 *
 * The pruner supports:
 *   - Equality filters (EqualTo)
 *   - Comparison filters (GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual)
 *   - IN filters (In)
 *   - Logical operators (And, Or, Not)
 *   - IsNull, IsNotNull filters
 */
object PartitionPruner {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Numeric-aware string comparison. When both values are parseable as numbers (Long or Double),
   * compares them numerically. Otherwise falls back to lexicographic string comparison.
   *
   * This is critical for partition bounds pruning where partition values like "1", "9", "10", "12"
   * are stored as strings but represent numeric values. Without numeric-aware comparison,
   * "9" > "10" lexicographically, which would incorrectly prune manifests containing months 10-12
   * when filtering for month > 9.
   *
   * @return negative if a < b, zero if a == b, positive if a > b
   */
  private[avro] def numericAwareCompare(a: String, b: String): Int =
    try {
      // Try Long first (most common for partition values like month, year, day)
      val aLong = a.toLong
      val bLong = b.toLong
      java.lang.Long.compare(aLong, bLong)
    } catch {
      case _: NumberFormatException =>
        try {
          // Try Double for decimal partition values
          val aDouble = a.toDouble
          val bDouble = b.toDouble
          java.lang.Double.compare(aDouble, bDouble)
        } catch {
          case _: NumberFormatException =>
            // Fall back to lexicographic comparison for non-numeric values
            a.compareTo(b)
        }
    }

  /**
   * Prune manifests based on partition filter.
   *
   * @param manifests
   *   List of manifest info with partition bounds
   * @param filter
   *   Spark filter expression
   * @return
   *   Manifests that may contain matching files
   */
  def pruneManifests(manifests: Seq[ManifestInfo], filter: Filter): Seq[ManifestInfo] = {
    if (manifests.isEmpty) {
      return Seq.empty
    }

    val result = manifests.filter { manifest =>
      manifest.partitionBounds match {
        case Some(bounds) => boundsMatchFilter(bounds, filter)
        case None         => true // No bounds means we can't prune, include it
      }
    }

    val prunedCount = manifests.size - result.size
    if (prunedCount > 0) {
      log.debug(s"Partition pruning: pruned $prunedCount of ${manifests.size} manifests")
    }

    result
  }

  /**
   * Prune manifests based on multiple filters (ANDed together).
   *
   * @param manifests
   *   List of manifest info with partition bounds
   * @param filters
   *   Multiple Spark filter expressions (all must match)
   * @return
   *   Manifests that may contain matching files
   */
  def pruneManifests(manifests: Seq[ManifestInfo], filters: Seq[Filter]): Seq[ManifestInfo] = {
    if (filters.isEmpty) {
      return manifests
    }

    // Combine all filters with AND
    val combinedFilter = filters.reduceLeft[Filter]((a, b) => And(a, b))
    pruneManifests(manifests, combinedFilter)
  }

  /**
   * Check if partition bounds may contain data matching the filter.
   *
   * @param bounds
   *   Partition bounds map (column -> min/max)
   * @param filter
   *   Spark filter expression
   * @return
   *   True if the bounds may contain matching data, false if they definitely don't
   */
  def boundsMatchFilter(bounds: Map[String, PartitionBounds], filter: Filter): Boolean =
    filter match {
      case EqualTo(attr, value) =>
        boundsContainValue(bounds, attr, value.toString)

      case GreaterThan(attr, value) =>
        boundsGreaterThan(bounds, attr, value.toString, inclusive = false)

      case GreaterThanOrEqual(attr, value) =>
        boundsGreaterThan(bounds, attr, value.toString, inclusive = true)

      case LessThan(attr, value) =>
        boundsLessThan(bounds, attr, value.toString, inclusive = false)

      case LessThanOrEqual(attr, value) =>
        boundsLessThan(bounds, attr, value.toString, inclusive = true)

      case In(attr, values) =>
        // At least one value must be within bounds
        values.exists(v => boundsContainValue(bounds, attr, v.toString))

      case And(left, right) =>
        boundsMatchFilter(bounds, left) && boundsMatchFilter(bounds, right)

      case Or(left, right) =>
        boundsMatchFilter(bounds, left) || boundsMatchFilter(bounds, right)

      case Not(child) =>
        // For NOT, we can't safely prune unless the child is simple
        // For complex NOT expressions, we conservatively include
        child match {
          case EqualTo(attr, value) =>
            // NOT(x = v) -> include if min != v OR max != v
            // We can only prune if the entire range equals v
            bounds.get(attr) match {
              case Some(b) =>
                val vStr = value.toString
                !(b.min.contains(vStr) && b.max.contains(vStr) && b.min == b.max)
              case None => true
            }
          case _ => true // Can't prune for complex NOT expressions
        }

      case IsNull(attr) =>
        // If we have bounds for this attribute, assume nulls might exist
        // (we don't track null counts in bounds)
        true

      case IsNotNull(attr) =>
        // If we have non-null bounds, there are non-null values
        bounds.get(attr) match {
          case Some(b) => b.min.isDefined || b.max.isDefined
          case None    => true
        }

      case StringStartsWith(attr, value) =>
        // Check if any value in the range could start with the prefix
        boundsOverlapPrefix(bounds, attr, value)

      case StringEndsWith(_, _) | StringContains(_, _) =>
        // Can't efficiently prune for EndsWith or Contains
        true

      case AlwaysTrue() =>
        true

      case AlwaysFalse() =>
        false

      case _ =>
        // Unknown filter type - can't prune, include
        log.debug(s"Unknown filter type for partition pruning: ${filter.getClass.getSimpleName}")
        true
    }

  /** Check if value falls within partition bounds. Uses numeric-aware comparison. */
  private def boundsContainValue(
    bounds: Map[String, PartitionBounds],
    attr: String,
    value: String
  ): Boolean =
    bounds.get(attr) match {
      case Some(b) =>
        val minOk = b.min.forall(min => numericAwareCompare(min, value) <= 0)
        val maxOk = b.max.forall(max => numericAwareCompare(max, value) >= 0)
        minOk && maxOk
      case None =>
        // Attribute not in bounds - can't prune
        true
    }

  /** Check if bounds have values greater than the given value. Uses numeric-aware comparison. */
  private def boundsGreaterThan(
    bounds: Map[String, PartitionBounds],
    attr: String,
    value: String,
    inclusive: Boolean
  ): Boolean =
    bounds.get(attr) match {
      case Some(b) =>
        b.max match {
          case Some(max) =>
            val cmp = numericAwareCompare(max, value)
            if (inclusive) cmp >= 0 else cmp > 0
          case None =>
            // No max bound - could have any value
            true
        }
      case None =>
        true
    }

  /** Check if bounds have values less than the given value. Uses numeric-aware comparison. */
  private def boundsLessThan(
    bounds: Map[String, PartitionBounds],
    attr: String,
    value: String,
    inclusive: Boolean
  ): Boolean =
    bounds.get(attr) match {
      case Some(b) =>
        b.min match {
          case Some(min) =>
            val cmp = numericAwareCompare(min, value)
            if (inclusive) cmp <= 0 else cmp < 0
          case None =>
            // No min bound - could have any value
            true
        }
      case None =>
        true
    }

  /** Check if any value in the bounds could start with the given prefix. */
  private def boundsOverlapPrefix(
    bounds: Map[String, PartitionBounds],
    attr: String,
    prefix: String
  ): Boolean =
    bounds.get(attr) match {
      case Some(b) =>
        (b.min, b.max) match {
          case (Some(min), Some(max)) =>
            // Check if the prefix range [prefix, prefix + '\uffff'...] overlaps [min, max]
            // A string S starts with prefix P if:
            //   P <= S and S < P + '\uffff' (effectively)
            // For bounds [min, max], there exists S in [min, max] starting with P if:
            //   max >= P and min <= P + high_char
            val prefixMax = prefix + "\uffff" // Upper bound for prefix matching
            max >= prefix && min <= prefixMax
          case (Some(min), None) =>
            // No max - check if min could be >= prefix (prefix could match)
            true
          case (None, Some(max)) =>
            // No min - check if max >= prefix
            max >= prefix
          case (None, None) =>
            true
        }
      case None =>
        true
    }

  /**
   * Extract partition column names from a filter expression.
   *
   * @param filter
   *   Spark filter expression
   * @return
   *   Set of referenced column names
   */
  def extractReferencedColumns(filter: Filter): Set[String] =
    filter match {
      case EqualTo(attr, _)            => Set(attr)
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
      case And(left, right)            => extractReferencedColumns(left) ++ extractReferencedColumns(right)
      case Or(left, right)             => extractReferencedColumns(left) ++ extractReferencedColumns(right)
      case Not(child)                  => extractReferencedColumns(child)
      case AlwaysTrue()                => Set.empty
      case AlwaysFalse()               => Set.empty
      case _                           => Set.empty
    }

  /**
   * Check if a filter only references the given partition columns.
   *
   * @param filter
   *   Spark filter expression
   * @param partitionColumns
   *   Set of partition column names
   * @return
   *   True if the filter only references partition columns
   */
  def isPartitionFilter(filter: Filter, partitionColumns: Set[String]): Boolean = {
    val referencedColumns = extractReferencedColumns(filter)
    referencedColumns.subsetOf(partitionColumns)
  }

  /**
   * Separate filters into partition and data filters.
   *
   * @param filters
   *   All filters to evaluate
   * @param partitionColumns
   *   Set of partition column names
   * @return
   *   (partitionFilters, dataFilters) tuple
   */
  def separateFilters(
    filters: Seq[Filter],
    partitionColumns: Set[String]
  ): (Seq[Filter], Seq[Filter]) =
    filters.partition(f => isPartitionFilter(f, partitionColumns))
}
