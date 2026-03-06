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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

import org.apache.spark.sql.sources._

import org.slf4j.LoggerFactory

/**
 * Utilities for partition pruning based on transaction log metadata. Enables efficient querying by skipping irrelevant
 * split files based on partition values.
 *
 * Optimizations implemented:
 *   1. Filter evaluation cache - caches results of filter evaluations 2. Pre-grouped files + per-column index - O(1)
 *      equality lookups 3. Filter selectivity ordering - most selective filters first 4. Lazy filter evaluation -
 *      short-circuit AND/OR 5. Parallel partition evaluation - for large partition counts
 */
object PartitionPruning {

  private val logger = LoggerFactory.getLogger(PartitionPruning.getClass)

  // Configuration defaults
  val DEFAULT_FILTER_CACHE_ENABLED = true
  val DEFAULT_INDEX_ENABLED        = true
  val DEFAULT_PARALLEL_THRESHOLD   = 100
  val DEFAULT_SELECTIVITY_ORDERING = true

  /**
   * Filter AddActions based on partition predicates. Returns only the AddActions that could potentially match the given
   * filters.
   *
   * This is the main entry point that uses all optimizations.
   */
  def prunePartitions(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter]
  ): Seq[AddAction] =
    prunePartitionsOptimized(
      addActions,
      partitionColumns,
      filters,
      filterCacheEnabled = DEFAULT_FILTER_CACHE_ENABLED,
      indexEnabled = DEFAULT_INDEX_ENABLED,
      parallelThreshold = DEFAULT_PARALLEL_THRESHOLD,
      selectivityOrdering = DEFAULT_SELECTIVITY_ORDERING
    )

  /**
   * Filter AddActions with configurable optimization settings.
   *
   * @param addActions
   *   Files to filter
   * @param partitionColumns
   *   Partition column names
   * @param filters
   *   Filters to apply
   * @param filterCacheEnabled
   *   Whether to use filter evaluation cache
   * @param indexEnabled
   *   Whether to use partition index
   * @param parallelThreshold
   *   Threshold for parallel evaluation
   * @param selectivityOrdering
   *   Whether to order filters by selectivity
   * @param tablePath
   *   Optional table path for PartitionIndex caching
   * @param tableVersion
   *   Optional table version for PartitionIndex caching
   */
  def prunePartitionsOptimized(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter],
    filterCacheEnabled: Boolean = DEFAULT_FILTER_CACHE_ENABLED,
    indexEnabled: Boolean = DEFAULT_INDEX_ENABLED,
    parallelThreshold: Int = DEFAULT_PARALLEL_THRESHOLD,
    selectivityOrdering: Boolean = DEFAULT_SELECTIVITY_ORDERING,
    tablePath: Option[String] = None,
    tableVersion: Option[Long] = None
  ): Seq[AddAction] = {

    // Early return if no partition columns or filters
    if (partitionColumns.isEmpty || filters.isEmpty) {
      return addActions
    }

    if (addActions.isEmpty) {
      return addActions
    }

    // Extract partition filters from the filter array
    val partitionFilters = filters.flatMap(extractPartitionFilter(_, partitionColumns))

    if (partitionFilters.isEmpty) {
      logger.debug("No partition filters found, returning all splits")
      return addActions
    }

    logger.debug(s"Applying partition pruning with ${partitionFilters.length} filters on columns: ${partitionColumns.mkString(", ")}")

    // Order filters by selectivity (most selective first)
    val orderedFilters = if (selectivityOrdering) {
      FilterSelectivityEstimator.orderBySelectivity(partitionFilters)
    } else {
      partitionFilters
    }

    // Try to use index-based pruning if enabled
    if (indexEnabled) {
      val result = pruneWithIndex(
        addActions,
        partitionColumns,
        orderedFilters,
        filterCacheEnabled,
        parallelThreshold,
        tablePath,
        tableVersion
      )
      if (result.isDefined) {
        val prunedActions = result.get
        logPruningResults(addActions.length, prunedActions.length)
        return prunedActions
      }
    }

    // Fallback to standard evaluation
    val prunedActions = if (filterCacheEnabled) {
      pruneWithCache(addActions, orderedFilters)
    } else {
      pruneStandard(addActions, orderedFilters)
    }

    logPruningResults(addActions.length, prunedActions.length)
    prunedActions
  }

  /**
   * Prune using the partition index for O(1) lookups on equality/IN filters. Returns None if index-based pruning is not
   * applicable.
   *
   * @param tablePath
   *   Optional table path for PartitionIndex caching
   * @param tableVersion
   *   Optional table version for PartitionIndex caching
   */
  private def pruneWithIndex(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter],
    filterCacheEnabled: Boolean,
    parallelThreshold: Int,
    tablePath: Option[String] = None,
    tableVersion: Option[Long] = None
  ): Option[Seq[AddAction]] = {

    // Build or retrieve cached partition index
    // When table path and version are provided, use the global cache for O(1) repeated lookups
    val index = (tablePath, tableVersion) match {
      case (Some(path), Some(version)) =>
        PartitionIndex.buildCached(path, version, addActions, partitionColumns)
      case _ =>
        PartitionIndex.build(addActions, partitionColumns)
    }

    if (index.isEmpty) {
      return None
    }

    // Separate indexable filters from those requiring evaluation
    val (indexableFilters, remainingFilters) = FilterSelectivityEstimator.partitionFilters(filters)

    if (indexableFilters.isEmpty) {
      // No indexable filters, fall back to partition-based evaluation
      return Some(pruneByPartitions(index, filters, filterCacheEnabled, parallelThreshold))
    }

    // Use index for initial pruning
    var candidatePartitions = index.uniquePartitions

    // Apply indexable filters for O(1) pruning
    indexableFilters.foreach { filter =>
      candidatePartitions = applyIndexableFilter(index, filter, candidatePartitions)
      if (candidatePartitions.isEmpty) {
        logger.debug("Index lookup eliminated all partitions")
        return Some(Seq.empty)
      }
    }

    // If there are remaining filters, evaluate them
    val matchingPartitions = if (remainingFilters.nonEmpty) {
      if (candidatePartitions.size >= parallelThreshold) {
        evaluatePartitionsParallel(candidatePartitions, remainingFilters, filterCacheEnabled)
      } else {
        evaluatePartitionsSequential(candidatePartitions, remainingFilters, filterCacheEnabled)
      }
    } else {
      candidatePartitions
    }

    // Collect files from matching partitions
    Some(index.getFilesForPartitions(matchingPartitions))
  }

  /** Apply an indexable filter using the partition index. */
  private def applyIndexableFilter(
    index: PartitionIndex,
    filter: Filter,
    currentPartitions: Set[Map[String, String]]
  ): Set[Map[String, String]] =
    filter match {
      case EqualTo(attribute, value) =>
        val matchingPartitions = index.getPartitionsForEquality(attribute, valueToString(value))
        currentPartitions.intersect(matchingPartitions)

      case EqualNullSafe(attribute, value) =>
        val matchingPartitions = index.getPartitionsForEquality(attribute, valueToString(value))
        currentPartitions.intersect(matchingPartitions)

      case In(attribute, values) =>
        val stringValues       = values.map(valueToString).toSeq
        val matchingPartitions = index.getPartitionsForIn(attribute, stringValues)
        currentPartitions.intersect(matchingPartitions)

      case _ =>
        // Non-indexable filter, keep all partitions
        currentPartitions
    }

  /** Convert a filter value to string for index lookup. */
  private def valueToString(value: Any): String =
    if (value == null) null else value.toString

  /** Prune by evaluating filters on unique partitions instead of individual files. */
  private def pruneByPartitions(
    index: PartitionIndex,
    filters: Array[Filter],
    filterCacheEnabled: Boolean,
    parallelThreshold: Int
  ): Seq[AddAction] = {
    val partitions = index.uniquePartitions

    val matchingPartitions = if (partitions.size >= parallelThreshold) {
      evaluatePartitionsParallel(partitions, filters, filterCacheEnabled)
    } else {
      evaluatePartitionsSequential(partitions, filters, filterCacheEnabled)
    }

    index.getFilesForPartitions(matchingPartitions)
  }

  /** Evaluate filters on partitions sequentially. */
  private def evaluatePartitionsSequential(
    partitions: Set[Map[String, String]],
    filters: Array[Filter],
    filterCacheEnabled: Boolean
  ): Set[Map[String, String]] =
    partitions.filter { partitionValues =>
      if (filterCacheEnabled) {
        PartitionFilterCache.getOrCompute(filters, partitionValues, evaluatePartitionFilters(partitionValues, filters))
      } else {
        evaluatePartitionFilters(partitionValues, filters)
      }
    }

  /** Evaluate filters on partitions in parallel. */
  private def evaluatePartitionsParallel(
    partitions: Set[Map[String, String]],
    filters: Array[Filter],
    filterCacheEnabled: Boolean
  ): Set[Map[String, String]] = {
    implicit val ec: ExecutionContext = TransactionLogThreadPools.parallelReadThreadPool.executionContext

    val futures = partitions.toSeq.map { partitionValues =>
      Future {
        val matches = if (filterCacheEnabled) {
          PartitionFilterCache.getOrCompute(filters, partitionValues, evaluatePartitionFilters(partitionValues, filters))
        } else {
          evaluatePartitionFilters(partitionValues, filters)
        }
        if (matches) Some(partitionValues) else None
      }
    }

    Try {
      Await.result(Future.sequence(futures), 60.seconds).flatten.toSet
    }.getOrElse {
      logger.warn("Parallel partition evaluation timed out, falling back to sequential")
      evaluatePartitionsSequential(partitions, filters, filterCacheEnabled)
    }
  }

  /** Prune with filter cache but without partition index. */
  private def pruneWithCache(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] =
    addActions.filter { addAction =>
      PartitionFilterCache.getOrCompute(
        filters,
        addAction.partitionValues,
        evaluatePartitionFilters(addAction.partitionValues, filters)
      )
    }

  /** Standard pruning without optimizations. */
  private def pruneStandard(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] =
    addActions.filter(addAction => evaluatePartitionFilters(addAction.partitionValues, filters))

  /** Log pruning results. */
  private def logPruningResults(totalFiles: Int, remainingFiles: Int): Unit = {
    val pruned = totalFiles - remainingFiles
    if (pruned > 0) {
      logger.info(s"Partition pruning: filtered out $pruned of $totalFiles split files")
    }
  }

  /**
   * Extract the partition-filterable parts of a complex filter. This handles cases where a filter contains both
   * partition and non-partition column references.
   */
  private[transaction] def extractPartitionFilter(filter: Filter, partitionColumns: Seq[String]): Option[Filter] =
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
          case _                  => None // If either side can't be evaluated, no pruning is safe
        }

      case Not(child) =>
        extractPartitionFilter(child, partitionColumns).map(Not(_))

      // Non-partition column references
      case _ => None
    }

  /** Evaluate whether partition values satisfy the given filters. Uses lazy AND evaluation. */
  private def evaluatePartitionFilters(partitionValues: Map[String, String], filters: Array[Filter]): Boolean = {
    // Use lazy evaluation for short-circuit AND
    var i = 0
    while (i < filters.length) {
      if (!evaluateFilter(partitionValues, filters(i))) {
        return false // Short-circuit on first false
      }
      i += 1
    }
    true
  }

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

      case EqualNullSafe(attribute, value) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) =>
            if (value == null) partitionValue == null
            else if (partitionValue == null) false
            else compareValues(partitionValue, value) == 0
          case None => value == null
        }

      case IsNull(attribute) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => partitionValue == null
          case None                 => true // Conservative: unknown column could be null
        }

      case IsNotNull(attribute) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => partitionValue != null
          case None                 => true // Conservative: unknown column could be non-null
        }

      case Not(child) =>
        !evaluateFilter(partitionValues, child)

      case StringStartsWith(attribute, prefix) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => partitionValue != null && partitionValue.startsWith(prefix)
          case None                 => true // Conservative
        }

      case StringEndsWith(attribute, suffix) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => partitionValue != null && partitionValue.endsWith(suffix)
          case None                 => true // Conservative
        }

      case StringContains(attribute, substr) =>
        partitionValues.get(attribute) match {
          case Some(partitionValue) => partitionValue != null && partitionValue.contains(substr)
          case None                 => true // Conservative
        }

      case And(left, right) =>
        // Lazy AND evaluation
        evaluateFilter(partitionValues, left) && evaluateFilter(partitionValues, right)

      case Or(left, right) =>
        // Lazy OR evaluation
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
          // For timestamp partitions, partitionValue might be stored as ISO-8601 string, JDBC format, or epoch millis
          try {
            // Try ISO-8601 format first (matches storage format)
            val instant            = java.time.Instant.parse(partitionValue)
            val partitionTimestamp = java.sql.Timestamp.from(instant)
            partitionTimestamp.compareTo(timestamp)
          } catch {
            case _: Exception =>
              try {
                // Fallback to JDBC timestamp format (yyyy-MM-dd HH:mm:ss)
                val partitionTimestamp = java.sql.Timestamp.valueOf(partitionValue)
                partitionTimestamp.compareTo(timestamp)
              } catch {
                case _: Exception =>
                  // Fallback to epoch millis comparison if stored as long
                  val partitionMillis = partitionValue.toLong
                  val filterMillis    = timestamp.getTime
                  partitionMillis.compareTo(filterMillis)
              }
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

  /** Get optimization statistics for monitoring. */
  def getOptimizationStats(): Map[String, Any] = {
    val (hits, misses, hitRate) = PartitionFilterCache.getStats()
    Map(
      "filterCacheHits"    -> hits,
      "filterCacheMisses"  -> misses,
      "filterCacheHitRate" -> f"${hitRate * 100}%.1f%%",
      "filterCacheSize"    -> PartitionFilterCache.size()
    )
  }

  /** Invalidate all optimization caches. Should be called when the file list changes. */
  def invalidateCaches(): Unit = {
    PartitionFilterCache.invalidate()
    PartitionIndex.invalidateCache()
  }
}
