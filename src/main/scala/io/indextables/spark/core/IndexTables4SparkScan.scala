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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.metrics._
import io.indextables.spark.prewarm.{IndexComponentMapping, PreWarmManager}
import io.indextables.spark.stats.DataSkippingMetrics
import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.transaction.{
  AddAction,
  NativeFilteringMetrics,
  NativeListFilesResult,
  NativeTransactionLog,
  PartitionPredicateUtils,
  SparkFilterToNativeFilter,
  TransactionLogInterface
}
import io.indextables.spark.util.{PartitionUtils, SplitsPerTaskCalculator, TimestampUtils}
// Removed unused imports
import org.slf4j.LoggerFactory

class IndexTables4SparkScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLogInterface,
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

  // Data skipping metrics for Spark UI reporting (captured from native filtering metrics)
  @volatile private var lastScanTotalFiles: Long           = 0L
  @volatile private var lastScanPartitionPrunedFiles: Long = 0L
  @volatile private var lastScanDataSkippedFiles: Long     = 0L
  @volatile private var lastScanResultFiles: Long          = 0L
  @volatile private var lastScanTotalSkipRate: Double      = 0.0

  // Read pipeline timing metrics for driver-side planning
  private val planningMetrics = new ScanPlanningMetrics(transactionLog.getTablePath().toString)

  // Cache for filtered actions (computed once, reused between planInputPartitions and estimateStatistics)
  // Uses lazy val for thread-safe, once-only initialization
  private lazy val cachedFilteredActions: Seq[AddAction] = cachedListFilesResult.files

  // Full result including metadata and metrics — computed once via single native JNI call
  // Replaces: getPartitionColumns + listFilesWithPartitionFilters + applyDataSkipping + getSchema
  private lazy val cachedListFilesResult: NativeListFilesResult = {
    // Separate partition vs data filters using shared utility
    val partitionColumns                = transactionLog.getPartitionColumns()
    val (partitionFilters, dataFilters) = SparkFilterToNativeFilter.splitFilters(pushedFilters, partitionColumns)

    // Single native call: partition pruning + data skipping + cooldown filtering + metadata
    val result = transactionLog match {
      case ntl: NativeTransactionLog =>
        ntl.listFilesWithMetadata(partitionFilters, dataFilters, excludeCooldown = false)
      case _ =>
        // Fallback for non-native implementations (tests with mock txlog)
        val files = transactionLog.listFilesWithAllFilters(partitionFilters, dataFilters)
        NativeListFilesResult(
          files = files,
          schema = transactionLog.getSchema(),
          partitionColumns = transactionLog.getPartitionColumns(),
          protocol = transactionLog.getProtocol(),
          metadataConfig = Map.empty,
          metrics = NativeFilteringMetrics(files.size, files.size, files.size, files.size, 0, 0)
        )
    }

    // Record native filtering metrics
    val m = result.metrics
    lastScanTotalFiles = m.totalFilesBeforeFiltering
    lastScanPartitionPrunedFiles = m.totalFilesBeforeFiltering - m.filesAfterPartitionPruning
    lastScanDataSkippedFiles = m.filesAfterPartitionPruning - m.filesAfterDataSkipping
    lastScanResultFiles = result.files.size
    lastScanTotalSkipRate = if (m.totalFilesBeforeFiltering > 0) {
      1.0 - (result.files.size.toDouble / m.totalFilesBeforeFiltering)
    } else 0.0

    DataSkippingMetrics.recordScan(
      transactionLog.getTablePath().toString,
      m.totalFilesBeforeFiltering,
      m.filesAfterPartitionPruning,
      result.files.size.toLong
    )

    logger.debug(
      s"Native filtering: ${m.totalFilesBeforeFiltering} total → " +
        s"${m.filesAfterPartitionPruning} after partition → " +
        s"${m.filesAfterDataSkipping} after data skip → " +
        s"${result.files.size} final (${m.manifestsPruned}/${m.manifestsTotal} manifests pruned)"
    )

    result
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
    val planStart = System.nanoTime()

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

    // Use cached filtered actions (avoids duplicate native listFiles calls)
    // Time this call to capture native filtering cost within the planInputPartitions window.
    // If estimateStatistics() already triggered the lazy val, this returns instantly (0 ns).
    val nativeFilterStart = System.nanoTime()
    val filteredActions   = getFilteredActions()
    planningMetrics.nativeFilteringNs = System.nanoTime() - nativeFilterStart
    planningMetrics.totalFilesBeforeFiltering = cachedListFilesResult.metrics.totalFilesBeforeFiltering
    planningMetrics.resultFiles = filteredActions.size

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
    val localityStart = System.nanoTime()
    val splitPaths    = filteredActions.map(_.path)
    val assignments   = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)
    planningMetrics.localityAssignmentNs = System.nanoTime() - localityStart
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

    planningMetrics.totalPlanNs = System.nanoTime() - planStart
    planningMetrics.resultPartitions = partitions.length
    planningMetrics.logSummary()

    partitions.toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val tablePath = transactionLog.getTablePath()

    // PERFORMANCE OPTIMIZATION: Resolve credentials on driver to avoid executor-side HTTP calls
    // For UnityCatalogAWSCredentialProvider, this means a single HTTP call on the driver
    // instead of N calls from N executors
    // Note: companion mode config (parquetTableRoot) is already injected by ScanBuilder.effectiveConfig
    // Read path only needs PATH_READ credentials (no write operations via table credentials)
    val readConfig = config + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    val resolvedConfig =
      io.indextables.spark.utils.CredentialProviderFactory.resolveCredentialsOnDriver(readConfig, tablePath.toString)

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

      // Use cached filtered actions (avoids duplicate native listFiles calls)
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

  // ============================================================================
  // Filter utilities
  // ============================================================================

  private def getFilterReferencedColumns(filter: Filter): Set[String] =
    SparkFilterToNativeFilter.extractReferencedColumns(filter)

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
      new TotalSkipRate(),
      new NativeFilteringTime(),
      new ScanPlanTotalTime(),
      new SplitEngineCreationTime(),
      new QueryBuildTime(),
      new StreamingSessionStartTime(),
      new NextBatchTime(),
      new BatchAssemblyTime()
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
      new TaskTotalSkipRate(lastScanTotalSkipRate),
      new TaskNativeFilteringTime(Math.round(planningMetrics.nativeFilteringMs)),
      new TaskScanPlanTotalTime(Math.round(planningMetrics.totalPlanMs))
    )
}
