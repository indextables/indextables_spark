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

package io.indextables.spark.core

import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  Statistics,
  SupportsReportStatistics
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.prewarm.{IndexComponentMapping, PreWarmManager}
import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.transaction.{AddAction, PartitionPredicateUtils, PartitionPruning, TransactionLog}
import io.indextables.spark.util.TimestampUtils
// Removed unused imports
import org.slf4j.LoggerFactory

class IndexTables4SparkScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  readSchema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  limit: Option[Int] = None,
  config: Map[String, String], // Direct config instead of broadcast
  indexQueryFilters: Array[Any] = Array.empty)
    extends Scan
    with Batch
    with SupportsReportStatistics {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkScan])

  // Full table schema for data skipping type lookup (not just projected columns)
  // This is needed because filters may reference columns not in the projection
  private lazy val fullTableSchema: StructType = transactionLog.getSchema().getOrElse(readSchema)

  // Optional metrics accumulator for collecting batch optimization statistics
  // Set via enableMetricsCollection() for testing/monitoring
  private var metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None

  logger.debug(s"SCAN CONSTRUCTION: IndexTables4SparkScan created with ${pushedFilters.length} pushed filters")
  pushedFilters.foreach(f => logger.debug(s"SCAN CONSTRUCTION:   - Filter: $f"))

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    logger.debug(s"PLAN PARTITIONS: planInputPartitions called with ${pushedFilters.length} pushed filters")
    pushedFilters.foreach(f => logger.debug(s"PLAN PARTITIONS:   - Filter: $f"))

    // Capture baseline metrics at scan start for delta computation
    // User can call getMetricsDelta() after query to get per-query metrics
    val tablePath = transactionLog.getTablePath().toString
    try {
      io.indextables.spark.storage.BatchOptMetricsRegistry.captureBaseline(tablePath)
      logger.debug(s"Captured baseline batch optimization metrics for scan: $tablePath")
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to capture baseline batch optimization metrics: ${ex.getMessage}")
    }

    val addActions = transactionLog.listFiles()
    logger.debug(s"PLAN PARTITIONS: Found ${addActions.length} files in transaction log")

    // Get available hosts from SparkContext for driver-based locality assignment
    val sparkContext   = sparkSession.sparkContext
    val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sparkContext)
    logger.debug(s"Available hosts for locality: ${availableHosts.mkString(", ")}")

    // Auto-register metrics accumulator if metrics collection is enabled
    val metricsEnabled = config.getOrElse("spark.indextables.read.batchOptimization.metrics.enabled", "false").toBoolean
    if (metricsEnabled && metricsAccumulator.isEmpty) {
      try {
        val acc       = enableMetricsCollection()
        val tablePath = transactionLog.getTablePath().toString
        io.indextables.spark.storage.BatchOptMetricsRegistry.register(tablePath, acc)
        logger.debug(s"Auto-registered batch optimization metrics for table: $tablePath")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to register batch optimization metrics: ${ex.getMessage}", ex)
      }
    }

    // Apply comprehensive data skipping (includes both partition pruning and min/max filtering)
    val filteredActions = applyDataSkipping(addActions, pushedFilters)
    logger.debug(s"PLAN PARTITIONS: After data skipping: ${filteredActions.length} splits remaining (started with ${addActions.length})")

    // Check if pre-warm is enabled (supports both old and new config paths)
    val isPreWarmEnabled = config
      .getOrElse(
        "spark.indextables.prewarm.enabled",
        config.getOrElse("spark.indextables.cache.prewarm.enabled", "false")
      )
      .toBoolean

    // Execute pre-warm phase if enabled
    if (isPreWarmEnabled && filteredActions.nonEmpty) {
      try {
        val sparkContext = sparkSession.sparkContext
        logger.info(s"Prewarm enabled: initiating cache warming for ${filteredActions.length} splits")

        // Parse segment configuration
        val segmentConfig = config.getOrElse("spark.indextables.prewarm.segments", "")
        val segments = if (segmentConfig.isEmpty) {
          IndexComponentMapping.defaultComponents
        } else {
          IndexComponentMapping.parseSegments(segmentConfig)
        }

        // Parse field configuration (empty = all fields)
        val fieldConfig = config.getOrElse("spark.indextables.prewarm.fields", "")
        val fields      = if (fieldConfig.isEmpty) None else Some(fieldConfig.split(",").map(_.trim).toSeq)

        // Parse parallelism (splits per task)
        val splitsPerTask = config.getOrElse("spark.indextables.prewarm.splitsPerTask", "2").toInt

        // Apply partition filter for prewarm if specified
        val partitionFilterConfig = config.getOrElse("spark.indextables.prewarm.partitionFilter", "")
        val prewarmActions = if (partitionFilterConfig.nonEmpty) {
          try {
            val metadata = transactionLog.getMetadata()
            val partitionSchema = StructType(
              metadata.partitionColumns.map(name =>
                org.apache.spark.sql.types.StructField(name, org.apache.spark.sql.types.StringType, nullable = true)
              )
            )

            if (partitionSchema.nonEmpty) {
              val predicates = Seq(partitionFilterConfig)
              val parsedPredicates =
                PartitionPredicateUtils.parseAndValidatePredicates(predicates, partitionSchema, sparkSession)
              val filtered =
                PartitionPredicateUtils.filterAddActionsByPredicates(filteredActions, partitionSchema, parsedPredicates)
              logger.info(s"Prewarm partition filter applied: ${filtered.length} of ${filteredActions.length} splits match '$partitionFilterConfig'")
              filtered
            } else {
              logger.warn("Prewarm partition filter specified but table has no partition columns")
              filteredActions
            }
          } catch {
            case ex: Exception =>
              logger.warn(s"Failed to apply prewarm partition filter '$partitionFilterConfig': ${ex.getMessage}")
              filteredActions
          }
        } else {
          filteredActions
        }

        logger.info(s"Prewarm config: segments=${segments.map(_.name()).mkString(",")}, fields=${fields.getOrElse("all")}, splitsPerTask=$splitsPerTask")

        // Use enhanced prewarm with component selection
        val preWarmResult = PreWarmManager.executePreWarmWithComponents(
          sparkContext,
          prewarmActions,
          segments,
          fields,
          splitsPerTask,
          config
        )

        if (preWarmResult.warmupInitiated) {
          logger.info(s"Prewarm completed: ${preWarmResult.totalWarmupsCreated} warmup tasks across ${preWarmResult.warmupAssignments.size} hosts")

          // Check for catch-up behavior for new hosts/splits
          val catchUpEnabled = config.getOrElse("spark.indextables.prewarm.catchUpNewHosts", "false").toBoolean
          if (catchUpEnabled) {
            val catchUpSplits = DriverSplitLocalityManager.getSplitsNeedingCatchUp(availableHosts, segments)
            if (catchUpSplits.nonEmpty) {
              val totalCatchUpSplits = catchUpSplits.values.map(_.size).sum
              logger.info(s"Catch-up prewarm needed for $totalCatchUpSplits splits across ${catchUpSplits.size} hosts")
              // Convert to AddActions for prewarm (respect partition filter)
              val catchUpAddActions = catchUpSplits.values.flatten.flatMap { splitPath =>
                prewarmActions.find(_.path == splitPath)
              }.toSeq
              if (catchUpAddActions.nonEmpty) {
                PreWarmManager.executePreWarmWithComponents(
                  sparkContext,
                  catchUpAddActions,
                  segments,
                  fields,
                  splitsPerTask,
                  config
                )
                logger.info(s"Catch-up prewarm initiated for ${catchUpAddActions.size} splits")
              }
            }
          }
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Prewarm failed but continuing with query execution: ${ex.getMessage}", ex)
      }
    }

    logger.debug(s"SCAN DEBUG: Planning ${filteredActions.length} partitions from ${addActions.length} total files")

    // Batch-assign all splits for this query using per-query load balancing
    val splitPaths  = filteredActions.map(_.path)
    val assignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)
    logger.debug(s"Assigned ${assignments.size} splits to hosts")

    val partitions = filteredActions.zipWithIndex.map {
      case (addAction, index) =>
        val preferredHost = assignments.get(addAction.path)
        new IndexTables4SparkInputPartition(
          addAction,
          readSchema,
          fullTableSchema,
          pushedFilters,
          index,
          limit,
          indexQueryFilters,
          preferredHost
        )
    }

    // Log summary at info level, details at debug level
    val totalPreferred = partitions.count(_.preferredLocations().nonEmpty)
    if (logger.isDebugEnabled) {
      val hostDistribution = assignments.values.groupBy(identity).map { case (h, s) => s"$h=${s.size}" }.mkString(", ")
      logger.debug(s"Partition host distribution: $hostDistribution")
    }
    logger.info(s"Planned ${partitions.length} partitions ($totalPreferred with locality hints)")

    partitions.toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val tablePath = transactionLog.getTablePath()
    new IndexTables4SparkReaderFactory(readSchema, limit, config, tablePath, metricsAccumulator)
  }

  /**
   * Enable metrics collection for batch optimization validation.
   *
   * This method registers an accumulator with Spark to collect batch optimization statistics from executors. The
   * accumulator can be read after query completion to validate consolidation ratios, cost savings, and other metrics.
   *
   * Usage:
   * {{{
   * val scan = ... // get scan from ScanBuilder
   * val metricsAcc = scan.enableMetricsCollection(spark)
   *
   * // Execute query
   * val result = df.collect()
   *
   * // Check metrics
   * val metrics = metricsAcc.value
   * println(s"Consolidation ratio: \${metrics.consolidationRatio}x")
   * println(s"Cost savings: \${metrics.costSavingsPercent}%")
   * }}}
   *
   * @return
   *   The metrics accumulator that will collect statistics
   */
  def enableMetricsCollection(): io.indextables.spark.storage.BatchOptimizationMetricsAccumulator = {
    val acc = new io.indextables.spark.storage.BatchOptimizationMetricsAccumulator()
    sparkSession.sparkContext.register(acc, "batch-optimization-metrics")
    metricsAccumulator = Some(acc)
    logger.debug("Enabled batch optimization metrics collection")
    acc
  }

  override def estimateStatistics(): Statistics =
    try {
      logger.info("Estimating statistics for IndexTables4Spark table")
      val addActions = transactionLog.listFiles()

      // Apply the same data skipping logic used in planInputPartitions to get accurate statistics
      // for the filtered dataset that will actually be read
      val filteredActions = applyDataSkipping(addActions, pushedFilters)

      val statistics = IndexTables4SparkStatistics.fromAddActions(filteredActions)

      logger.info(
        s"Table statistics: ${statistics.sizeInBytes().orElse(0L)} bytes, ${statistics.numRows().orElse(0L)} rows"
      )

      statistics
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to estimate statistics: ${ex.getMessage}", ex)
        // Return unknown statistics rather than failing the query
        IndexTables4SparkStatistics.unknown()
    }

  def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    logger.debug(
      s"DATA SKIPPING DEBUG: applyDataSkipping called with ${addActions.length} files and ${filters.length} filters"
    )
    filters.foreach(f => logger.debug(s"DATA SKIPPING DEBUG: Filter: $f"))

    if (filters.isEmpty) {
      logger.debug(s"DATA SKIPPING DEBUG: No filters, returning all ${addActions.length} files")
      return addActions
    }

    val partitionColumns = transactionLog.getPartitionColumns()
    logger.debug(s"DATA SKIPPING DEBUG: Partition columns: ${partitionColumns.mkString(", ")}")
    val initialCount = addActions.length

    // Debug: Print AddAction details
    addActions.zipWithIndex.foreach {
      case (action, index) =>
        logger.debug(s"DATA SKIPPING DEBUG: AddAction $index - path: ${action.path}")
        logger.debug(s"DATA SKIPPING DEBUG: AddAction $index - partitionValues: ${action.partitionValues}")
        action.numRecords.foreach { numRecs =>
          logger.debug(s"DATA SKIPPING DEBUG: AddAction $index - numRecords: $numRecs")
        }
        action.minValues.foreach { minVals =>
          logger.debug(s"DATA SKIPPING DEBUG: AddAction $index - minValues: $minVals")
        }
        action.maxValues.foreach { maxVals =>
          logger.debug(s"DATA SKIPPING DEBUG: AddAction $index - maxValues: $maxVals")
        }
    }

    // Step 1: Apply partition pruning
    val partitionPrunedActions = if (partitionColumns.nonEmpty) {
      val pruned      = PartitionPruning.prunePartitions(addActions, partitionColumns, filters)
      val prunedCount = addActions.length - pruned.length
      if (prunedCount > 0) {
        logger.info(s"Partition pruning: filtered out $prunedCount of ${addActions.length} split files")
      }
      pruned
    } else {
      addActions
    }

    // Step 2: Apply min/max value skipping on remaining files
    val nonPartitionFilters = filters.filterNot { filter =>
      // Only apply min/max skipping to non-partition columns to avoid double filtering
      getFilterReferencedColumns(filter).exists(partitionColumns.contains)
    }

    val finalActions = if (nonPartitionFilters.nonEmpty) {
      val skipped = partitionPrunedActions.filter { addAction =>
        // Improved data skipping logic that handles OR predicates correctly
        canFileMatchFilters(addAction, nonPartitionFilters)
      }
      val skippedCount = partitionPrunedActions.length - skipped.length
      if (skippedCount > 0) {
        logger.info(s"Data skipping (min/max): filtered out $skippedCount of ${partitionPrunedActions.length} files")
      }
      skipped
    } else {
      partitionPrunedActions
    }

    val totalSkipped = initialCount - finalActions.length
    if (totalSkipped > 0) {
      logger.info(
        s"Total data skipping: $initialCount files -> ${finalActions.length} files (skipped $totalSkipped total)"
      )
    }

    finalActions
  }

  // TODO: Fix options flow from read operations to scan for proper field type detection
  // /**
  //  * Check if a field is configured as a text field (tokenized) for data skipping purposes.
  //  * Uses the indexing configuration to determine field type.
  //  * Currently disabled due to options flow issues.
  //  */
  // private def isTextFieldForTokenization(fieldName: String): Boolean = {
  //   // Implementation commented out - options don't flow correctly from read to scan
  //   false
  // }

  private def getFilterReferencedColumns(filter: Filter): Set[String] = {
    import org.apache.spark.sql.sources._
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
      case And(left, right)                 => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right)                  => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child)                       => getFilterReferencedColumns(child)
      case _                                => Set.empty
    }
  }

  /**
   * Determine if a file can potentially match the given filters. This implements proper OR/AND logic for data skipping.
   */
  private def canFileMatchFilters(addAction: AddAction, filters: Array[Filter]): Boolean = {
    logger.debug(s"canFileMatchFilters: Checking file ${addAction.path} against ${filters.length} filters")
    filters.foreach(f => logger.debug(s"canFileMatchFilters: Filter: $f"))

    // If no min/max values available, conservatively keep the file
    if (addAction.minValues.isEmpty || addAction.maxValues.isEmpty) {
      logger.debug(s"canFileMatchFilters: No min/max values, keeping file")
      return true
    }

    // A file can match only if ALL filters can potentially match
    // Filters at this level are combined with AND logic by Spark
    val result = filters.forall(filter => canFilterMatchFile(addAction, filter))
    logger.debug(s"canFileMatchFilters: Result for file ${addAction.path}: $result (min=${addAction.minValues}, max=${addAction.maxValues})")
    result
  }

  /** Determine if a single filter can potentially match a file. Handles complex nested AND/OR logic correctly. */
  private def canFilterMatchFile(addAction: AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    filter match {
      case And(left, right) =>
        // For AND: both sides must be able to match
        canFilterMatchFile(addAction, left) && canFilterMatchFile(addAction, right)

      case Or(left, right) =>
        // For OR: at least one side must be able to match
        canFilterMatchFile(addAction, left) || canFilterMatchFile(addAction, right)

      case Not(child) =>
        // For NOT filters with min/max range data: we cannot prove that ALL values
        // in the range fail to satisfy the NOT condition. For example, if range is
        // [business, tech] and filter is NOT(category = business), the range contains
        // both business AND non-business values, so the file should not be skipped.
        // NOT filters should only skip based on exact partition values, not ranges.
        true

      case _ =>
        // For all other filters, use the existing shouldSkipFile logic (inverted)
        !shouldSkipFile(addAction, filter)
    }
  }

  private def shouldSkipFile(addAction: AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    (addAction.minValues, addAction.maxValues) match {
      case (Some(minVals), Some(maxVals)) =>
        filter match {
          case EqualTo(attribute, value) =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            logger.debug(s"DATA SKIPPING EqualTo: attribute=$attribute, value=$value, minVal=$minVal, maxVal=$maxVal")
            (minVal, maxVal) match {
              case (Some(min), Some(max)) if min.nonEmpty && max.nonEmpty =>
                val (convertedValue, convertedMin, convertedMax) = convertValuesForComparison(attribute, value, min, max)

                // Simple lexicographic comparison: skip if value is outside [min, max] range
                // EqualTo should always be exact equality regardless of field type
                val shouldSkip =
                  convertedValue.compareTo(convertedMin) < 0 || convertedValue.compareTo(convertedMax) > 0

                logger.debug(s"DATA SKIPPING EqualTo: convertedValue=$convertedValue (${convertedValue.getClass.getSimpleName}), convertedMin=$convertedMin (${convertedMin.getClass.getSimpleName}), convertedMax=$convertedMax (${convertedMax.getClass.getSimpleName})")
                logger.debug(s"DATA SKIPPING EqualTo: compareToMin=${convertedValue.compareTo(convertedMin)}, compareToMax=${convertedValue.compareTo(convertedMax)}, shouldSkip=$shouldSkip")
                shouldSkip
              case _ =>
                logger.debug(s"DATA SKIPPING DEBUG: No min/max values found, not skipping")
                false
            }
          case GreaterThan(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) if max.nonEmpty =>
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max)
                convertedMax.compareTo(convertedValue) <= 0
              case _ => false // No statistics or empty string - don't skip
            }
          case LessThan(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) if min.nonEmpty =>
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "")
                val compareResult                     = convertedMin.compareTo(convertedValue)
                val shouldSkip                        = compareResult >= 0
                logger.debug(s"DATA SKIPPING LessThan: attribute=$attribute, min=$min, filterValue=$value")
                logger.debug(s"DATA SKIPPING LessThan: convertedMin=$convertedMin (${convertedMin.getClass.getSimpleName}), convertedValue=$convertedValue (${convertedValue.getClass.getSimpleName})")
                logger.debug(s"DATA SKIPPING LessThan: compareResult=$compareResult, shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false // No statistics or empty string - don't skip
            }
          case GreaterThanOrEqual(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) if max.nonEmpty =>
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max)
                val valueStr                          = convertedValue.toString
                val maxStr                            = convertedMax.toString
                // Skip if max < filterValue (all values in file are too small)
                // BUT don't skip if filterValue starts with max (truncated max means actual value could be >= filterValue)
                // Example: max="aaa" (truncated), filterValue="aaab" - actual could be "aaac" which is >= "aaab"
                convertedMax.compareTo(convertedValue) < 0 && !valueStr.startsWith(maxStr)
              case _ => false // No statistics or empty string - don't skip
            }
          case LessThanOrEqual(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) if min.nonEmpty =>
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "")
                val valueStr                          = convertedValue.toString
                val minStr                            = convertedMin.toString
                // Skip if min > filterValue (all values in file are too large)
                // BUT don't skip if filterValue starts with min (truncated min means actual value could be <= filterValue)
                // Example: min="bbb" (truncated), filterValue="bbbc" - actual could be "bbba" which is <= "bbbc"
                convertedMin.compareTo(convertedValue) > 0 && !valueStr.startsWith(minStr)
              case _ => false // No statistics or empty string - don't skip
            }
          case StringStartsWith(attribute, value) =>
            // For startsWith, check if any string in [min, max] could start with the value
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) if min.nonEmpty && max.nonEmpty =>
                val valueStr = value.toString
                // Skip if the prefix is lexicographically greater than max value
                // or if max value is shorter than prefix and doesn't start with it
                val shouldSkip = valueStr.compareTo(max) > 0 ||
                  (!max.startsWith(valueStr) && max.compareTo(valueStr) < 0)
                logger.debug(s"DATA SKIPPING DEBUG: StringStartsWith($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case StringEndsWith(attribute, value) =>
            // For endsWith, this is harder to optimize with min/max, so be conservative
            // Only skip if we can determine with certainty
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                // Very conservative: only skip if min and max are identical and don't end with value
                val shouldSkip = min == max && !min.endsWith(valueStr)
                logger.debug(s"DATA SKIPPING DEBUG: StringEndsWith($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case StringContains(attribute, value) =>
            // For contains, be conservative - only skip if min==max and doesn't contain value
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr   = value.toString
                val shouldSkip = min == max && !min.contains(valueStr)
                logger.debug(s"DATA SKIPPING DEBUG: StringContains($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  /** Generic utility to convert numeric values for comparison, handling optional min/max */
  private def convertNumericValues[T](
    typeName: String,
    filterValue: Any,
    minValue: String,
    maxValue: String,
    parser: String => T,
    boxer: T => Comparable[Any],
    logger: org.slf4j.Logger
  ): (Comparable[Any], Comparable[Any], Comparable[Any]) =
    try {
      val filterNum = parser(filterValue.toString)
      val minNum    = if (minValue.nonEmpty) Some(parser(minValue)) else None
      val maxNum    = if (maxValue.nonEmpty) Some(parser(maxValue)) else None

      logger.debug(s"$typeName CONVERSION SUCCESS: filter=$filterNum, min=$minNum, max=$maxNum")

      (minNum, maxNum) match {
        case (Some(min), Some(max)) =>
          (boxer(filterNum), boxer(min), boxer(max))
        case (Some(min), None) =>
          (boxer(filterNum), boxer(min), "".asInstanceOf[Comparable[Any]])
        case (None, Some(max)) =>
          (boxer(filterNum), "".asInstanceOf[Comparable[Any]], boxer(max))
        case (None, None) =>
          (
            filterValue.toString.asInstanceOf[Comparable[Any]],
            "".asInstanceOf[Comparable[Any]],
            "".asInstanceOf[Comparable[Any]]
          )
      }
    } catch {
      case ex: Exception =>
        logger.debug(s"$typeName CONVERSION FAILED: $filterValue - ${ex.getMessage}")
        (
          filterValue.toString.asInstanceOf[Comparable[Any]],
          minValue.asInstanceOf[Comparable[Any]],
          maxValue.asInstanceOf[Comparable[Any]]
        )
    }

  private def convertValuesForComparison(
    attribute: String,
    filterValue: Any,
    minValue: String,
    maxValue: String
  ): (Comparable[Any], Comparable[Any], Comparable[Any]) = {
    import java.time.LocalDate
    import java.sql.Date
    import org.apache.spark.sql.types.TimestampType

    // Find the field data type in the FULL table schema (not just projected columns)
    // This is critical because filters may reference columns not in the projection
    val fieldType = fullTableSchema.fields.find(_.name == attribute).map(_.dataType)

    fieldType match {
      case Some(DateType) =>
        // For DateType, the table stores values as days since epoch (integer)
        logger.debug(s"DATE CONVERSION: Processing DateType field $attribute")
        logger.debug(s"DATE CONVERSION: filterValue=$filterValue (${filterValue.getClass.getSimpleName})")
        try {
          val filterDaysSinceEpoch = filterValue match {
            case dateStr: String =>
              logger.debug(s"DATE CONVERSION: Parsing string date: $dateStr")
              val filterDate = LocalDate.parse(dateStr)
              val epochDate  = LocalDate.of(1970, 1, 1)
              val days       = epochDate.until(filterDate).getDays
              logger.debug(
                s"DATE CONVERSION: String '$dateStr' -> LocalDate '$filterDate' -> days since epoch: $days"
              )
              days
            case sqlDate: Date =>
              logger.debug(s"DATE CONVERSION: Converting SQL Date: $sqlDate")
              // Use direct calculation from milliseconds since epoch
              val millisSinceEpoch = sqlDate.getTime
              val daysSinceEpoch   = (millisSinceEpoch / (24 * 60 * 60 * 1000)).toInt
              logger.debug(
                s"DATE CONVERSION: SQL Date '$sqlDate' -> millis=$millisSinceEpoch -> days since epoch: $daysSinceEpoch"
              )
              daysSinceEpoch
            case intVal: Int =>
              logger.debug(s"DATE CONVERSION: Using int value directly: $intVal")
              intVal
            case _ =>
              logger.debug(s"DATE CONVERSION: Fallback parsing toString: ${filterValue.toString}")
              val filterDate = LocalDate.parse(filterValue.toString)
              val epochDate  = LocalDate.of(1970, 1, 1)
              val days       = epochDate.until(filterDate).getDays
              logger.debug(s"DATE CONVERSION: Fallback '${filterValue.toString}' -> LocalDate '$filterDate' -> days since epoch: $days")
              days
          }

          // Helper function to parse date string or integer to days since epoch
          def parseDateOrInt(value: String): Int =
            if (value.contains("-")) {
              // Date string format (e.g., "2023-02-15")
              val date      = LocalDate.parse(value)
              val epochDate = LocalDate.of(1970, 1, 1)
              java.time.temporal.ChronoUnit.DAYS.between(epochDate, date).toInt
            } else {
              value.toInt
            }

          val minDays = parseDateOrInt(minValue)
          val maxDays = parseDateOrInt(maxValue)
          // logger.debug(s"DATE CONVERSION RESULT: filterDaysSinceEpoch=$filterDaysSinceEpoch, minDays=$minDays, maxDays=$maxDays")
          (
            filterDaysSinceEpoch.asInstanceOf[Comparable[Any]],
            minDays.asInstanceOf[Comparable[Any]],
            maxDays.asInstanceOf[Comparable[Any]]
          )
        } catch {
          case ex: Exception =>
            logger.warn(
              s"DATE CONVERSION FAILED: $filterValue (${filterValue.getClass.getSimpleName}) - ${ex.getMessage}"
            )
            // Fallback to string comparison
            (
              filterValue.toString.asInstanceOf[Comparable[Any]],
              minValue.asInstanceOf[Comparable[Any]],
              maxValue.asInstanceOf[Comparable[Any]]
            )
        }

      case Some(IntegerType) =>
        convertNumericValues[Int](
          "INTEGER",
          filterValue,
          minValue,
          maxValue,
          (s: String) => s.toInt,
          (n: Int) => Integer.valueOf(n).asInstanceOf[Comparable[Any]],
          logger
        )

      case Some(LongType) =>
        convertNumericValues[Long](
          "LONG",
          filterValue,
          minValue,
          maxValue,
          (s: String) => s.toLong,
          (n: Long) => Long.box(n).asInstanceOf[Comparable[Any]],
          logger
        )

      case Some(FloatType) =>
        convertNumericValues[Float](
          "FLOAT",
          filterValue,
          minValue,
          maxValue,
          (s: String) => s.toFloat,
          (n: Float) => Float.box(n).asInstanceOf[Comparable[Any]],
          logger
        )

      case Some(DoubleType) =>
        convertNumericValues[Double](
          "DOUBLE",
          filterValue,
          minValue,
          maxValue,
          (s: String) => s.toDouble,
          (n: Double) => Double.box(n).asInstanceOf[Comparable[Any]],
          logger
        )

      case Some(TimestampType) =>
        // Convert timestamps to microseconds since epoch for numeric comparison
        import java.sql.Timestamp

        // Convert filter value to microseconds
        val filterMicros: Long = filterValue match {
          case ts: Timestamp =>
            // Spark stores timestamps as microseconds since epoch
            TimestampUtils.toMicros(ts)
          case l: Long => l
          case _       =>
            // Try to parse as timestamp string
            val ts = Timestamp.valueOf(filterValue.toString)
            TimestampUtils.toMicros(ts)
        }

        // Min/max values are stored as Long strings (microseconds since epoch)
        val minMicros = if (minValue.isEmpty) 0L else minValue.toLong
        val maxMicros = if (maxValue.isEmpty) 0L else maxValue.toLong

        (
          Long.box(filterMicros).asInstanceOf[Comparable[Any]],
          Long.box(minMicros).asInstanceOf[Comparable[Any]],
          Long.box(maxMicros).asInstanceOf[Comparable[Any]]
        )

      case _ =>
        // For other data types (strings, etc.), use string comparison
        logger.debug(s"STRING CONVERSION: Using string comparison for $attribute")
        (
          filterValue.toString.asInstanceOf[Comparable[Any]],
          minValue.asInstanceOf[Comparable[Any]],
          maxValue.asInstanceOf[Comparable[Any]]
        )
    }
  }
}
