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
 * Utilities for partition pruning based on transaction log metadata. Enables efficient querying by skipping irrelevant
 * split files based on partition values.
 */
object PartitionPruning {

  private val logger = LoggerFactory.getLogger(PartitionPruning.getClass)

  /**
   * Filter AddActions based on partition predicates. Returns only the AddActions that could potentially match the given
   * filters.
   */
  def prunePartitions(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter]
  ): Seq[AddAction] = {

    if (partitionColumns.isEmpty || filters.isEmpty) {
      return addActions
    }

    // Extract partition filters from the filter array
    // Use enhanced extraction to handle mixed partition/non-partition filters
    val partitionFilters = filters.flatMap(extractPartitionFilter(_, partitionColumns))

    if (partitionFilters.isEmpty) {
      logger.debug("No partition filters found, returning all splits")
      return addActions
    }

    logger.info(s"Applying partition pruning with ${partitionFilters.length} filters on columns: ${partitionColumns.mkString(", ")}")

    val prunedActions = addActions.filter { addAction =>
      evaluatePartitionFilters(addAction.partitionValues, partitionFilters)
    }

    val pruned = addActions.length - prunedActions.length
    if (pruned > 0) {
      logger.info(s"Partition pruning: filtered out $pruned of ${addActions.length} split files")
    }

    prunedActions
  }

  /**
   * Extract the partition-filterable parts of a complex filter. This handles cases where a filter contains both
   * partition and non-partition column references.
   */
  private def extractPartitionFilter(filter: Filter, partitionColumns: Seq[String]): Option[Filter] =
    filter match {
      // Simple filters - check if they reference only partition columns
      case f @ EqualTo(attribute, _) if partitionColumns.contains(attribute)            => Some(f)
      case f @ EqualNullSafe(attribute, _) if partitionColumns.contains(attribute)      => Some(f)
      case f @ GreaterThan(attribute, _) if partitionColumns.contains(attribute)        => Some(f)
      case f @ GreaterThanOrEqual(attribute, _) if partitionColumns.contains(attribute) => Some(f)
      case f @ LessThan(attribute, _) if partitionColumns.contains(attribute)           => Some(f)
      case f @ LessThanOrEqual(attribute, _) if partitionColumns.contains(attribute)    => Some(f)
      case f @ In(attribute, _) if partitionColumns.contains(attribute)                 => Some(f)
      case f @ IsNull(attribute) if partitionColumns.contains(attribute)                => Some(f)
      case f @ IsNotNull(attribute) if partitionColumns.contains(attribute)             => Some(f)
      case f @ StringStartsWith(attribute, _) if partitionColumns.contains(attribute)   => Some(f)
      case f @ StringEndsWith(attribute, _) if partitionColumns.contains(attribute)     => Some(f)
      case f @ StringContains(attribute, _) if partitionColumns.contains(attribute)     => Some(f)

      // Complex filters - recursively extract partition parts
      case And(left, right) =>
        val leftPartition  = extractPartitionFilter(left, partitionColumns)
        val rightPartition = extractPartitionFilter(right, partitionColumns)
        (leftPartition, rightPartition) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None)    => Some(l)
          case (None, Some(r))    => Some(r)
          case (None, None)       => None
        }

      case Or(left, right) =>
        val leftPartition  = extractPartitionFilter(left, partitionColumns)
        val rightPartition = extractPartitionFilter(right, partitionColumns)
        (leftPartition, rightPartition) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case (Some(l), None)    => Some(l) // Conservative: if either side can't be evaluated, we can't prune
          case (None, Some(r))    => Some(r) // Conservative: if either side can't be evaluated, we can't prune
          case (None, None)       => None
        }

      case Not(child) =>
        extractPartitionFilter(child, partitionColumns).map(Not(_))

      // Non-partition column references
      case _ => None
    }

  /** Extract column names referenced by a filter. */
  private def getReferencedColumns(filter: Filter): Set[String] =
    filter match {
      case EqualTo(attribute, _)            => Set(attribute)
      case EqualNullSafe(attribute, _)      => Set(attribute)
      case GreaterThan(attribute, _)        => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _)           => Set(attribute)
      case LessThanOrEqual(attribute, _)    => Set(attribute)
      case In(attribute, _)                 => Set(attribute)
      case IsNull(attribute)                => Set(attribute)
      case IsNotNull(attribute)             => Set(attribute)
      case StringStartsWith(attribute, _)   => Set(attribute)
      case StringEndsWith(attribute, _)     => Set(attribute)
      case StringContains(attribute, _)     => Set(attribute)
      case And(left, right)                 => getReferencedColumns(left) ++ getReferencedColumns(right)
      case Or(left, right)                  => getReferencedColumns(left) ++ getReferencedColumns(right)
      case Not(child)                       => getReferencedColumns(child)
      case _                                => Set.empty
    }

  /** Evaluate whether partition values satisfy the given filters. */
  private def evaluatePartitionFilters(partitionValues: Map[String, String], filters: Array[Filter]): Boolean =
    filters.forall(evaluateFilter(partitionValues, _))

  /** Evaluate a single filter against partition values. Package-private for testing. */
  private[transaction] def evaluateFilter(partitionValues: Map[String, String], filter: Filter): Boolean =
    filter match {
      case EqualTo(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => compareValues(partitionValue, value) == 0
          case None                 => false
        }

      case GreaterThan(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => compareValues(partitionValue, value) > 0
          case None                 => false
        }

      case GreaterThanOrEqual(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => compareValues(partitionValue, value) >= 0
          case None                 => false
        }

      case LessThan(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => compareValues(partitionValue, value) < 0
          case None                 => false
        }

      case LessThanOrEqual(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => compareValues(partitionValue, value) <= 0
          case None                 => false
        }

      case In(attribute, values) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => values.exists(value => compareValues(partitionValue, value) == 0)
          case None                 => values.contains(null)
        }

      case And(left, right) =>
        evaluateFilter(partitionValues, left) && evaluateFilter(partitionValues, right)

      case Or(left, right) =>
        evaluateFilter(partitionValues, left) || evaluateFilter(partitionValues, right)

      case _ =>
        logger.debug(s"Unsupported filter for partition pruning: $filter")
        true // Conservative approach: include the partition if we can't evaluate
    }

  /** Compare two values for filtering. Handles different types gracefully. */
  private def compareValues(partitionValue: String, filterValue: Any): Int = {
    if (partitionValue == null && filterValue == null) return 0
    if (partitionValue == null) return -1
    if (filterValue == null) return 1

    try
      filterValue match {
        case s: String  => partitionValue.compareTo(s)
        case i: Int     => partitionValue.toInt.compareTo(i)
        case l: Long    => partitionValue.toLong.compareTo(l)
        case f: Float   => partitionValue.toFloat.compareTo(f)
        case d: Double  => partitionValue.toDouble.compareTo(d)
        case b: Boolean => partitionValue.toBoolean.compareTo(b)

        // Date type support - partitionValue is stored as YYYY-MM-DD string format
        case date: java.sql.Date =>
          partitionValue.compareTo(date.toString) // Date.toString() returns YYYY-MM-DD format

        // Timestamp type support - convert both to comparable format
        case timestamp: java.sql.Timestamp =>
          // For timestamp partitions, partitionValue might be stored as ISO string or epoch millis
          try {
            // Try parsing as ISO timestamp first
            val partitionTimestamp = java.sql.Timestamp.valueOf(partitionValue)
            partitionTimestamp.compareTo(timestamp)
          } catch {
            case _: Exception =>
              // Fallback to epoch millis comparison if stored as long
              val partitionMillis = partitionValue.toLong
              val filterMillis    = timestamp.getTime
              partitionMillis.compareTo(filterMillis)
          }

        // BigDecimal type support - for precise numeric comparisons
        case bd: java.math.BigDecimal =>
          val partitionDecimal = new java.math.BigDecimal(partitionValue)
          partitionDecimal.compareTo(bd)

        // Scala BigDecimal support
        case bd: scala.math.BigDecimal =>
          val partitionDecimal = scala.math.BigDecimal(partitionValue)
          partitionDecimal.compareTo(bd)

        case _ => partitionValue.compareTo(filterValue.toString)
      }
    catch {
      case _: Exception =>
        logger.debug(s"Failed to compare partition value '$partitionValue' with filter value '$filterValue', using string comparison")
        partitionValue.compareTo(filterValue.toString)
    }
  }

  /** Get statistics about partition distribution for debugging. */
  def getPartitionStatistics(addActions: Seq[AddAction], partitionColumns: Seq[String]): Map[String, Any] = {
    if (partitionColumns.isEmpty) {
      return Map("partitioned" -> false)
    }

    val totalFiles      = addActions.length
    val partitionCounts = scala.collection.mutable.Map[Map[String, String], Int]()

    addActions.foreach { action =>
      val partitionKey = partitionColumns.map(col => col -> action.partitionValues.getOrElse(col, null)).toMap

      partitionCounts(partitionKey) = partitionCounts.getOrElse(partitionKey, 0) + 1
    }

    Map(
      "partitioned"              -> true,
      "partitionColumns"         -> partitionColumns,
      "totalFiles"               -> totalFiles,
      "uniquePartitions"         -> partitionCounts.size,
      "averageFilesPerPartition" -> (if (partitionCounts.nonEmpty) totalFiles.toDouble / partitionCounts.size else 0.0),
      "partitionDistribution"    -> partitionCounts.toMap
    )
  }
}
