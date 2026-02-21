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

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
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

import io.indextables.spark.metrics._
import io.indextables.spark.prewarm.{IndexComponentMapping, PreWarmManager}
import io.indextables.spark.stats.{DataSkippingMetrics, ExpressionSimplifier, FilterExpressionCache}
import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.transaction.{AddAction, PartitionPredicateUtils, PartitionPruning, TransactionLog}
import io.indextables.spark.util.{PartitionUtils, SplitsPerTaskCalculator, TimestampUtils}
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

  // Data skipping metrics for Spark UI reporting (captured during applyDataSkipping)
  @volatile private var lastScanTotalFiles: Long           = 0L
  @volatile private var lastScanPartitionPrunedFiles: Long = 0L
  @volatile private var lastScanDataSkippedFiles: Long     = 0L
  @volatile private var lastScanResultFiles: Long          = 0L
  @volatile private var lastScanTotalSkipRate: Double      = 0.0

  // Cache for filtered actions (computed once, reused between planInputPartitions and estimateStatistics)
  // This avoids duplicate calls to transactionLog.listFiles() and applyDataSkipping()
  // Uses lazy val for thread-safe, once-only initialization (replaces @volatile + check-then-act pattern)
  private lazy val cachedFilteredActions: Seq[AddAction] = {
    // Extract partition-only filters for Avro manifest pruning optimization
    val partitionColumns = transactionLog.getPartitionColumns()
    val partitionFilters = if (partitionColumns.nonEmpty && pushedFilters.nonEmpty) {
      pushedFilters.filter { filter =>
        val referencedCols = getFilterReferencedColumns(filter)
        referencedCols.nonEmpty && referencedCols.forall(partitionColumns.contains)
      }.toSeq
    } else {
      Seq.empty
    }

    // Use partition filter pass-through for Avro state format optimization
    val addActions = if (partitionFilters.nonEmpty) {
      logger.debug(s"Passing ${partitionFilters.length} partition filters for Avro manifest pruning")
      transactionLog.listFilesWithPartitionFilters(partitionFilters)
    } else {
      transactionLog.listFiles()
    }

    applyDataSkipping(addActions, pushedFilters)
  }

  // Pre-computed filter hash for cache lookups (computed once in constructor)
  // This avoids recomputing O(filters) hash on every cache lookup in applyDataSkipping
  private val precomputedFilterHash: Long = computeFilterHash(pushedFilters)

  /**
   * Compute a hash for the filter array (computed once in constructor). Uses toString to capture full filter structure,
   * matching PartitionFilterCache pattern.
   */
  private def computeFilterHash(filters: Array[Filter]): Long = {
    if (filters.isEmpty) return 0L
    val filterString = filters.map(_.toString).sorted.mkString("|")
    filterString.hashCode.toLong & 0xffffffffL
  }

  /**
   * Get filtered actions from the lazy cache. Thread-safe via lazy val initialization.
   *
   * For Avro state format, partition-only filters are passed through to enable manifest pruning at the checkpoint read
   * level, avoiding I/O for manifests that don't contain matching partitions.
   */
  private def getFilteredActions(): Seq[AddAction] = cachedFilteredActions

  if (logger.isDebugEnabled) {
    logger.debug(s"SCAN CONSTRUCTION: IndexTables4SparkScan created with ${pushedFilters.length} pushed filters")
    pushedFilters.foreach(f => logger.debug(s"SCAN CONSTRUCTION:   - Filter: $f"))
  }

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // Capture baseline metrics at scan start for delta computation
    // User can call getMetricsDelta() after query to get per-query metrics
    val tablePath = transactionLog.getTablePath().toString
    try
      io.indextables.spark.storage.BatchOptMetricsRegistry.captureBaseline(tablePath)
    catch {
      case ex: Exception =>
        logger.warn(s"Failed to capture baseline batch optimization metrics: ${ex.getMessage}")
    }

    // Get available hosts from SparkContext for driver-based locality assignment
    val sparkContext   = sparkSession.sparkContext
    val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sparkContext)

    // Auto-register metrics accumulator if metrics collection is enabled
    val metricsEnabled = config.getOrElse("spark.indextables.read.batchOptimization.metrics.enabled", "false").toBoolean
    if (metricsEnabled && metricsAccumulator.isEmpty) {
      try {
        val acc       = enableMetricsCollection()
        val tablePath = transactionLog.getTablePath().toString
        io.indextables.spark.storage.BatchOptMetricsRegistry.register(tablePath, acc)
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to register batch optimization metrics: ${ex.getMessage}", ex)
      }
    }

    // Use cached filtered actions (avoids duplicate listFiles + applyDataSkipping calls)
    val filteredActions = getFilteredActions()

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
            val partitionSchema = transactionLog.getPartitionSchema()

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

    logger.debug(s"SCAN DEBUG: Planning ${filteredActions.length} partitions")

    // Get splitsPerTask configuration
    // - "auto" or absent: auto-select based on cluster size and split count
    // - numeric value: use explicit value
    val configuredSplitsPerTask = config.get("spark.indextables.read.splitsPerTask")
    val maxSplitsPerTask = config
      .get("spark.indextables.read.maxSplitsPerTask")
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .getOrElse(SplitsPerTaskCalculator.DefaultMaxSplitsPerTask)

    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = filteredActions.length,
      defaultParallelism = sparkSession.sparkContext.defaultParallelism,
      configuredValue = configuredSplitsPerTask,
      maxSplitsPerTask = maxSplitsPerTask
    )

    // Batch-assign all splits for this query using per-query load balancing
    val splitPaths  = filteredActions.map(_.path)
    val assignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)
    logger.debug(s"Assigned ${assignments.size} splits to hosts")

    // Group splits by assigned host for locality-aware batching
    val splitsByHost: Map[String, Seq[AddAction]] = filteredActions
      .groupBy(a => assignments.getOrElse(a.path, "unknown"))

    // Create multi-split partitions (or single-split if splitsPerTask == 1)
    // Interleave partitions by host for better cluster utilization (h1,h2,h3,h1,h2,h3,... instead of h1,h1,h1,h2,h2,h2,...)
    var partitionIndex = 0
    val partitions = if (splitsPerTask == 1) {
      // Fallback to single-split behavior for backward compatibility
      // Still interleave by host for better distribution
      val batchesByHost: Map[String, Seq[IndexTables4SparkInputPartition]] = splitsByHost.map {
        case (host, hostSplits) =>
          host -> hostSplits.map { addAction =>
            new IndexTables4SparkInputPartition(
              addAction,
              readSchema,
              fullTableSchema,
              pushedFilters,
              0, // Will be reassigned after interleaving
              limit,
              indexQueryFilters,
              if (host == "unknown") None else Some(host)
            )
          }
      }
      // Interleave: take one partition from each host in round-robin order
      PartitionUtils.interleaveByHost(batchesByHost).zipWithIndex.map {
        case (p, idx) =>
          new IndexTables4SparkInputPartition(
            p.addAction,
            p.readSchema,
            p.fullTableSchema,
            p.filters,
            idx,
            p.limit,
            p.indexQueryFilters,
            p.preferredHost
          )
      }
    } else {
      // Group splits by host and batch them
      val batchesByHost: Map[String, Seq[IndexTables4SparkMultiSplitInputPartition]] = splitsByHost.map {
        case (host, hostSplits) =>
          host -> hostSplits
            .grouped(splitsPerTask)
            .map { batch =>
              new IndexTables4SparkMultiSplitInputPartition(
                addActions = batch,
                readSchema = readSchema,
                fullTableSchema = fullTableSchema,
                filters = pushedFilters,
                partitionId = 0, // Will be reassigned after interleaving
                limit = limit,
                indexQueryFilters = indexQueryFilters,
                preferredHost = if (host == "unknown") None else Some(host)
              )
            }
            .toSeq
      }
      // Interleave: take one partition from each host in round-robin order
      PartitionUtils.interleaveByHost(batchesByHost).zipWithIndex.map {
        case (p, idx) =>
          new IndexTables4SparkMultiSplitInputPartition(
            p.addActions,
            p.readSchema,
            p.fullTableSchema,
            p.filters,
            idx,
            p.limit,
            p.indexQueryFilters,
            p.preferredHost
          )
      }
    }

    // Log summary at info level, details at debug level
    val totalPreferred = partitions.count(_.preferredLocations().nonEmpty)
    if (logger.isDebugEnabled) {
      val hostDistribution = assignments.values.groupBy(identity).map { case (h, s) => s"$h=${s.size}" }.mkString(", ")
      logger.debug(s"Partition host distribution: $hostDistribution")
    }
    logger.info(s"Planned ${partitions.length} multi-split partitions (${filteredActions.length} splits, $splitsPerTask per task, $totalPreferred with locality hints)")

    partitions.toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val tablePath = transactionLog.getTablePath()

    // PERFORMANCE OPTIMIZATION: Resolve credentials on driver to avoid executor-side HTTP calls
    // For UnityCatalogAWSCredentialProvider, this means a single HTTP call on the driver
    // instead of N calls from N executors
    // Note: companion mode config (parquetTableRoot) is already injected by ScanBuilder.effectiveConfig
    // Read path only needs PATH_READ credentials (no write operations via table credentials)
    val readConfig     = config + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    val resolvedConfig = io.indextables.spark.utils.CredentialProviderFactory.resolveCredentialsOnDriver(readConfig, tablePath.toString)

    // Diagnostic: log companion config state on driver before serialization to executors
    val hasCompanionKey = resolvedConfig.contains("spark.indextables.companion.parquetTableRoot")
    val companionRoot   = resolvedConfig.get("spark.indextables.companion.parquetTableRoot")
    val hasParquetCreds = resolvedConfig.contains("spark.indextables.companion.parquet.aws.accessKey")
    logger.info(
      s"[DRIVER] createReaderFactory: companionMode=$hasCompanionKey, " +
        s"parquetTableRoot=${companionRoot.getOrElse("NONE")}, " +
        s"parquetCredentials=$hasParquetCreds, " +
        s"totalConfigKeys=${resolvedConfig.size}"
    )

    new IndexTables4SparkReaderFactory(readSchema, limit, resolvedConfig, tablePath, metricsAccumulator)
  }

  // Credential resolution centralized in CredentialProviderFactory.resolveCredentialsOnDriver()

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

      // Use cached filtered actions (avoids duplicate listFiles + applyDataSkipping calls)
      // This is the same filtered dataset that will be read, reused from planInputPartitions
      val filteredActions = getFilteredActions()

      // Extract columns referenced in WHERE clause filters for optimized column statistics computation
      // This reduces complexity from O(addActions × allColumns) to O(addActions × referencedColumns)
      val referencedColumns = pushedFilters.flatMap(getFilterReferencedColumns).toSet

      val statistics = IndexTables4SparkStatistics.fromAddActions(filteredActions, referencedColumns)

      logger.info(
        s"Table statistics: ${statistics.sizeInBytes().orElse(0L)} bytes, ${statistics.numRows().orElse(0L)} rows" +
          (if (referencedColumns.nonEmpty) s", column stats for ${referencedColumns.size} columns"
           else ", no column stats (no filters)")
      )

      statistics
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to estimate statistics: ${ex.getMessage}", ex)
        // Return unknown statistics rather than failing the query
        IndexTables4SparkStatistics.unknown()
    }

  def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    if (filters.isEmpty) {
      // Record metrics even for no-filter case
      val tablePath = transactionLog.getTablePath().toString
      DataSkippingMetrics.recordScan(tablePath, addActions.length, addActions.length, addActions.length)
      // Update Spark UI metrics
      lastScanTotalFiles = addActions.length
      lastScanPartitionPrunedFiles = 0
      lastScanDataSkippedFiles = 0
      lastScanResultFiles = addActions.length
      lastScanTotalSkipRate = 0.0
      return addActions
    }

    val partitionColumns = transactionLog.getPartitionColumns()
    val initialCount     = addActions.length

    // Step 0: Apply expression simplification (cached, using pre-computed filter hash)
    val simplifiedFilters = FilterExpressionCache.getOrSimplifyWithHash(filters, fullTableSchema, precomputedFilterHash)

    // Check if simplification determined no files can match
    if (simplifiedFilters.exists(ExpressionSimplifier.isAlwaysFalse)) {
      logger.info("Expression simplification determined no files can match - returning empty result")
      val tablePath = transactionLog.getTablePath().toString
      DataSkippingMetrics.recordScan(tablePath, initialCount, 0, 0, Map("ExpressionSimplification" -> initialCount))
      // Update Spark UI metrics
      lastScanTotalFiles = initialCount
      lastScanPartitionPrunedFiles = initialCount
      lastScanDataSkippedFiles = 0
      lastScanResultFiles = 0
      lastScanTotalSkipRate = 1.0
      return Seq.empty
    }

    // Check if simplification determined all files match (no filtering needed)
    val effectiveFilters =
      if (simplifiedFilters.isEmpty || simplifiedFilters.forall(ExpressionSimplifier.isAlwaysTrue)) {
        Array.empty[Filter]
      } else {
        // Filter out AlwaysTrue entries
        simplifiedFilters.filterNot(ExpressionSimplifier.isAlwaysTrue)
      }

    if (effectiveFilters.isEmpty) {
      val tablePath = transactionLog.getTablePath().toString
      DataSkippingMetrics.recordScan(tablePath, initialCount, initialCount, initialCount)
      // Update Spark UI metrics
      lastScanTotalFiles = initialCount
      lastScanPartitionPrunedFiles = 0
      lastScanDataSkippedFiles = 0
      lastScanResultFiles = initialCount
      lastScanTotalSkipRate = 0.0
      return addActions
    }

    // Step 1: Apply partition pruning with configurable optimizations
    val partitionPrunedActions = if (partitionColumns.nonEmpty) {
      val filterCacheEnabled =
        config.getOrElse("spark.indextables.partitionPruning.filterCacheEnabled", "true").toBoolean
      val indexEnabled      = config.getOrElse("spark.indextables.partitionPruning.indexEnabled", "true").toBoolean
      val parallelThreshold = config.getOrElse("spark.indextables.partitionPruning.parallelThreshold", "100").toInt
      val selectivityOrdering =
        config.getOrElse("spark.indextables.partitionPruning.selectivityOrdering", "true").toBoolean

      // Get table path and version for PartitionIndex caching
      // This enables O(1) cache hits on repeated queries to the same table version
      val tablePath    = transactionLog.getTablePath().toString
      val tableVersion = transactionLog.getVersions().lastOption.getOrElse(-1L)

      val pruned = PartitionPruning.prunePartitionsOptimized(
        addActions,
        partitionColumns,
        effectiveFilters,
        filterCacheEnabled = filterCacheEnabled,
        indexEnabled = indexEnabled,
        parallelThreshold = parallelThreshold,
        selectivityOrdering = selectivityOrdering,
        tablePath = Some(tablePath),
        tableVersion = Some(tableVersion)
      )
      val prunedCount = addActions.length - pruned.length
      if (prunedCount > 0) {
        logger.info(s"Partition pruning: filtered out $prunedCount of ${addActions.length} split files")
      }
      pruned
    } else {
      addActions
    }

    // Step 2: Apply min/max value skipping on remaining files
    val nonPartitionFilters = effectiveFilters.filterNot { filter =>
      // Only apply min/max skipping to non-partition columns to avoid double filtering
      getFilterReferencedColumns(filter).exists(partitionColumns.contains)
    }

    // Track which filter types skip files for metrics (thread-safe for parallel processing)
    val filterTypeSkips = new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicLong]()

    // Get parallelism threshold from config (reuse partition pruning threshold)
    val dataSkippingParallelThreshold = config
      .getOrElse(
        "spark.indextables.dataSkipping.parallelThreshold",
        config.getOrElse("spark.indextables.partitionPruning.parallelThreshold", "100")
      )
      .toInt

    val finalActions = if (nonPartitionFilters.nonEmpty) {
      // Use parallel processing for large file counts
      val useParallel = partitionPrunedActions.length >= dataSkippingParallelThreshold

      val skipped = if (useParallel) {
        partitionPrunedActions.par.filter { addAction =>
          val canMatch = canFileMatchFilters(addAction, nonPartitionFilters)
          if (!canMatch) {
            // Track which filter types contributed to skipping (thread-safe)
            nonPartitionFilters.foreach { filter =>
              if (!canFilterMatchFile(addAction, filter)) {
                val filterType = getFilterTypeName(filter)
                filterTypeSkips
                  .computeIfAbsent(filterType, _ => new java.util.concurrent.atomic.AtomicLong(0L))
                  .incrementAndGet()
              }
            }
          }
          canMatch
        }.seq
      } else {
        partitionPrunedActions.filter { addAction =>
          val canMatch = canFileMatchFilters(addAction, nonPartitionFilters)
          if (!canMatch) {
            // Track which filter types contributed to skipping
            nonPartitionFilters.foreach { filter =>
              if (!canFilterMatchFile(addAction, filter)) {
                val filterType = getFilterTypeName(filter)
                filterTypeSkips
                  .computeIfAbsent(filterType, _ => new java.util.concurrent.atomic.AtomicLong(0L))
                  .incrementAndGet()
              }
            }
          }
          canMatch
        }
      }

      val skippedCount = partitionPrunedActions.length - skipped.length
      if (skippedCount > 0) {
        logger.info(
          s"Data skipping (min/max): filtered out $skippedCount of ${partitionPrunedActions.length} files" +
            (if (useParallel) " (parallel)" else "")
        )
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

    // Record metrics
    val tablePath = transactionLog.getTablePath().toString
    // Convert ConcurrentHashMap[String, AtomicLong] to Map[String, Long]
    import scala.jdk.CollectionConverters._
    val filterTypeSkipsMap = filterTypeSkips.asScala.map { case (k, v) => k -> v.get() }.toMap
    DataSkippingMetrics.recordScan(
      tablePath,
      initialCount,
      partitionPrunedActions.length,
      finalActions.length,
      filterTypeSkipsMap
    )

    // Update Spark UI metrics
    lastScanTotalFiles = initialCount
    lastScanPartitionPrunedFiles = initialCount - partitionPrunedActions.length
    lastScanDataSkippedFiles = partitionPrunedActions.length - finalActions.length
    lastScanResultFiles = finalActions.length
    lastScanTotalSkipRate = if (initialCount > 0) (initialCount - finalActions.length).toDouble / initialCount else 0.0

    finalActions
  }

  /** Get a human-readable name for a filter type (for metrics). */
  private def getFilterTypeName(filter: Filter): String = {
    import org.apache.spark.sql.sources._
    filter match {
      case _: EqualTo            => "EqualTo"
      case _: EqualNullSafe      => "EqualNullSafe"
      case _: GreaterThan        => "GreaterThan"
      case _: GreaterThanOrEqual => "GreaterThanOrEqual"
      case _: LessThan           => "LessThan"
      case _: LessThanOrEqual    => "LessThanOrEqual"
      case _: In                 => "In"
      case _: IsNull             => "IsNull"
      case _: IsNotNull          => "IsNotNull"
      case _: StringStartsWith   => "StringStartsWith"
      case _: StringEndsWith     => "StringEndsWith"
      case _: StringContains     => "StringContains"
      case _: And                => "And"
      case _: Or                 => "Or"
      case _: Not                => "Not"
      case _                     => filter.getClass.getSimpleName
    }
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
    // If no min/max values available, conservatively keep the file
    if (addAction.minValues.isEmpty || addAction.maxValues.isEmpty) {
      return true
    }

    // A file can match only if ALL filters can potentially match
    // Filters at this level are combined with AND logic by Spark
    filters.forall(filter => canFilterMatchFile(addAction, filter))
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
            (minVal, maxVal) match {
              case (Some(min), Some(max)) if min.nonEmpty && max.nonEmpty =>
                val (convertedValue, convertedMin, convertedMax) = convertValuesForComparison(attribute, value, min, max)

                // Simple lexicographic comparison: skip if value is outside [min, max] range
                // EqualTo should always be exact equality regardless of field type
                convertedValue.compareTo(convertedMin) < 0 || convertedValue.compareTo(convertedMax) > 0
              case _ =>
                false
            }

          // IN filter optimization: check if file's [min,max] range overlaps with IN list's [min,max]
          case In(attribute, values) if values.nonEmpty =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(fileMin), Some(fileMax)) if fileMin.nonEmpty && fileMax.nonEmpty =>
                // Get or compute the IN list's min/max range (cached)
                FilterExpressionCache.getOrComputeInRange(attribute, values) match {
                  case Some(inRange) =>
                    // Convert file stats and IN range values to comparable types
                    val (_, convertedFileMin, convertedFileMax) =
                      convertValuesForComparison(attribute, inRange.minValue, fileMin, fileMax)

                    // Also convert IN list min/max to same type for comparison
                    val inMinStr               = inRange.minValue.toString
                    val inMaxStr               = inRange.maxValue.toString
                    val (convertedInMin, _, _) = convertValuesForComparison(attribute, inMinStr, inMinStr, inMinStr)
                    val (convertedInMax, _, _) = convertValuesForComparison(attribute, inMaxStr, inMaxStr, inMaxStr)

                    // Skip if file's range doesn't overlap with IN list's range
                    // No overlap if: fileMax < inMin OR fileMin > inMax
                    convertedFileMax.compareTo(convertedInMin) < 0 ||
                    convertedFileMin.compareTo(convertedInMax) > 0

                  case None =>
                    // Could not compute IN range - don't skip
                    false
                }
              case _ =>
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
                convertedMin.compareTo(convertedValue) >= 0
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
                valueStr.compareTo(max) > 0 || (!max.startsWith(valueStr) && max.compareTo(valueStr) < 0)
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
                min == max && !min.endsWith(valueStr)
              case _ => false
            }
          case StringContains(attribute, value) =>
            // For contains, be conservative - only skip if min==max and doesn't contain value
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                min == max && !min.contains(valueStr)
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
      case _: Exception =>
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
        try {
          val filterDaysSinceEpoch = filterValue match {
            case dateStr: String =>
              val filterDate = LocalDate.parse(dateStr)
              val epochDate  = LocalDate.of(1970, 1, 1)
              epochDate.until(filterDate).getDays
            case sqlDate: Date =>
              // Use direct calculation from milliseconds since epoch
              val millisSinceEpoch = sqlDate.getTime
              (millisSinceEpoch / (24 * 60 * 60 * 1000)).toInt
            case intVal: Int =>
              intVal
            case _ =>
              val filterDate = LocalDate.parse(filterValue.toString)
              val epochDate  = LocalDate.of(1970, 1, 1)
              epochDate.until(filterDate).getDays
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

  // ============================================================================
  // DataSource V2 Custom Metrics for Spark UI
  // ============================================================================

  /**
   * Returns the custom metrics supported by this scan. These metrics appear in the Spark UI SQL tab under the scan
   * operator.
   */
  override def supportedCustomMetrics(): Array[CustomMetric] =
    Array(
      new TotalFilesConsidered(),
      new PartitionPrunedFiles(),
      new DataSkippedFiles(),
      new ResultFiles(),
      new TotalSkipRate()
    )

  /**
   * Reports driver-side metrics collected during scan planning. Called by Spark after the scan completes to populate
   * metrics in the UI.
   */
  override def reportDriverMetrics(): Array[CustomTaskMetric] =
    Array(
      new TaskTotalFilesConsidered(lastScanTotalFiles),
      new TaskPartitionPrunedFiles(lastScanPartitionPrunedFiles),
      new TaskDataSkippedFiles(lastScanDataSkippedFiles),
      new TaskResultFiles(lastScanResultFiles),
      new TaskTotalSkipRate(lastScanTotalSkipRate)
    )
}
