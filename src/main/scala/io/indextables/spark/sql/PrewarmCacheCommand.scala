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

package io.indextables.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.prewarm.{AsyncPrewarmJobManager, AsyncPrewarmJobResult, IndexComponentMapping}
import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.{EnhancedTransactionLogCache, PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils, ProtocolNormalizer, SplitMetadataFactory}
import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.slf4j.LoggerFactory

/**
 * SQL command to prewarm index caches across all executors.
 *
 * Syntax:
 * {{{
 * PREWARM INDEXTABLES CACHE 's3://bucket/path'
 *    [FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM, DOC_STORE)]
 *    [ON FIELDS (field1, field2, fieldN)]
 *    [WITH PERWORKER PARALLELISM OF N]
 *    [WHERE partition_col = 'value']
 * }}}
 *
 * Segment aliases are mapped to tantivy4java IndexComponent:
 *   - TERM_DICT / TERM_DICTIONARY -> IndexComponent.TERM
 *   - FAST_FIELD / FASTFIELD -> IndexComponent.FASTFIELD
 *   - POSTINGS / POSTING_LISTS -> IndexComponent.POSTINGS
 *   - POSITIONS / POSITION_LISTS -> IndexComponent.POSITIONS
 *   - FIELD_NORM / FIELDNORM -> IndexComponent.FIELDNORM
 *   - DOC_STORE / STORE -> IndexComponent.STORE
 *
 * Default segments (when not specified): TERM_DICT, POSTINGS (minimal set for query operations) Default fields: All
 * fields in the index Default parallelism: 2 splits per task
 *
 * @param tablePath
 *   Table path or identifier
 * @param segments
 *   Sequence of segment names to prewarm (empty = use defaults)
 * @param fields
 *   Optional sequence of field names to prewarm (None = all fields)
 * @param splitsPerTask
 *   Number of splits per Spark task (default: 2)
 * @param wherePredicates
 *   Partition predicates for filtering splits
 */
case class PrewarmCacheCommand(
  tablePath: String,
  segments: Seq[String],
  fields: Option[Seq[String]],
  splitsPerTask: Int,
  wherePredicates: Seq[String],
  failOnMissingField: Boolean = true,
  asyncMode: Boolean = false)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

  /** Build a combined segments string including both tantivy components and parquet segments. */
  private def formatSegments(components: Set[IndexComponent], parquetSegs: Set[String]): String = {
    val tantivy = components.map(_.name()).toSeq.sorted
    val parquet = parquetSegs.toSeq.sorted.map(_.toLowerCase)
    (tantivy ++ parquet).mkString(",")
  }

  /**
   * Resolve AWS credentials on the driver and return a modified config. This eliminates executor-side HTTP calls for
   * credential providers like UnityCatalogAWSCredentialProvider.
   */
  private def resolveCredentialsOnDriver(config: Map[String, String], tablePath: String): Map[String, String] = {
    val providerClass = config
      .get("spark.indextables.aws.credentialsProviderClass")
      .orElse(config.get("spark.indextables.aws.credentialsproviderclass"))

    providerClass match {
      case Some(className) if className.nonEmpty =>
        try {
          val normalizedPath = io.indextables.spark.util.TablePathNormalizer.normalizeToTablePath(tablePath)
          val credentials = io.indextables.spark.utils.CredentialProviderFactory.resolveAWSCredentialsFromConfig(
            config,
            normalizedPath
          )

          credentials match {
            case Some(creds) =>
              logger.info(s"[DRIVER] Resolved AWS credentials from provider: $className")
              var newConfig = config -
                "spark.indextables.aws.credentialsProviderClass" -
                "spark.indextables.aws.credentialsproviderclass" +
                ("spark.indextables.aws.accessKey" -> creds.accessKey) +
                ("spark.indextables.aws.secretKey" -> creds.secretKey)

              creds.sessionToken.foreach(token => newConfig = newConfig + ("spark.indextables.aws.sessionToken" -> token))
              newConfig

            case None =>
              logger.warn(s"[DRIVER] Failed to resolve credentials from provider $className, passing to executors")
              config
          }
        } catch {
          case ex: Exception =>
            logger.warn(s"[DRIVER] Driver-side credential resolution failed: ${ex.getMessage}, passing to executors")
            config
        }

      case None => config
    }
  }

  override val output: Seq[Attribute] = Seq(
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("assigned_host", StringType, nullable = false)(),
    AttributeReference("locality_hits", IntegerType, nullable = false)(),
    AttributeReference("locality_misses", IntegerType, nullable = false)(),
    AttributeReference("splits_prewarmed", IntegerType, nullable = false)(),
    AttributeReference("segments", StringType, nullable = false)(),
    AttributeReference("fields", StringType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("skipped_fields", StringType, nullable = true)(),
    AttributeReference("retries", IntegerType, nullable = false)(),
    AttributeReference("failed_splits_by_host", StringType, nullable = true)(),
    AttributeReference("job_id", StringType, nullable = true)(),
    AttributeReference("async_mode", BooleanType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    if (asyncMode) {
      runAsync(sparkSession)
    } else {
      runSync(sparkSession)
    }

  /** Execute synchronous prewarm (original behavior). */
  private def runSync(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Starting synchronous prewarm cache for table: $tablePath")
    val startTime = System.currentTimeMillis()

    val sc = sparkSession.sparkContext

    // Resolve segment aliases to IndexComponent set, extracting parquet companion aliases separately
    val segmentString                = segments.mkString(",")
    val parquetSegments: Set[String] = IndexComponentMapping.parseParquetSegments(segmentString)
    if (parquetSegments.nonEmpty) logger.info(s"Companion parquet preload segments: ${parquetSegments.mkString(", ")}")
    val resolvedSegments: Set[IndexComponent] = if (segments.isEmpty) {
      logger.info("Using default segments: TERM, POSTINGS")
      IndexComponentMapping.defaultComponents
    } else {
      val resolved = IndexComponentMapping.parseSegments(segmentString)
      if (resolved.nonEmpty) logger.info(s"Using specified segments: ${resolved.map(_.name()).mkString(", ")}")
      // Only fall back to defaults if no parquet segments were specified either
      if (resolved.isEmpty && parquetSegments.isEmpty) IndexComponentMapping.defaultComponents else resolved
    }

    // Get session config for credentials and cache settings
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs =
      ConfigNormalization.extractTantivyConfigsFromHadoop(sparkSession.sparkContext.hadoopConfiguration)
    val baseConfig = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // PERFORMANCE OPTIMIZATION: Resolve credentials on driver to avoid executor-side HTTP calls
    // Prewarm is read-only, request PATH_READ credentials
    val readConfig   = baseConfig + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    val mergedConfig = resolveCredentialsOnDriver(readConfig, tablePath)

    // Create transaction log - CloudStorageProvider will handle credential resolution
    // with proper refresh logic via V1ToV2CredentialsProviderAdapter
    import scala.jdk.CollectionConverters._
    val transactionLog = TransactionLogFactory.create(
      new Path(tablePath),
      sparkSession,
      new CaseInsensitiveStringMap(mergedConfig.asJava)
    )

    try {
      // Get partition schema for predicate validation
      val metadata = transactionLog.getMetadata()
      val partitionSchema = StructType(
        metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
      )
      val partitionColumnSet = metadata.partitionColumns.map(_.toLowerCase).toSet

      // Parse predicates and convert to Spark Filters for Avro manifest pruning
      val (parsedPredicates, partitionFilters) = if (wherePredicates.nonEmpty) {
        val parsed  = PartitionPredicateUtils.parseAndValidatePredicates(wherePredicates, partitionSchema, sparkSession)
        val filters = PartitionPredicateUtils.expressionsToFilters(parsed)
        logger.info(s"Converted ${filters.length} of ${parsed.length} predicates to Spark Filters for manifest pruning")
        (parsed, filters)
      } else {
        (Seq.empty, Seq.empty)
      }

      // Get active splits with manifest-level pruning if filters are available
      var addActions = if (partitionFilters.nonEmpty) {
        val actions = transactionLog.listFilesWithPartitionFilters(partitionFilters)
        logger.info(s"Found ${actions.length} splits using Avro manifest pruning")
        actions
      } else {
        val actions = transactionLog.listFiles()
        logger.info(s"Found ${actions.length} splits in transaction log")
        actions
      }

      // Sort by modificationTime descending (newest first) so newest data gets hot first
      addActions = addActions.sortBy(_.modificationTime)(Ordering[Long].reverse)

      // Apply in-memory partition filtering for any predicates not converted to Spark Filters
      // This handles complex predicates that couldn't be pushed down to manifest pruning
      if (parsedPredicates.nonEmpty) {
        addActions = PartitionPredicateUtils.filterAddActionsByPredicates(addActions, partitionSchema, parsedPredicates)
        logger.info(s"After in-memory partition filtering: ${addActions.length} splits")
      }

      if (addActions.isEmpty) {
        logger.info("No splits to prewarm")
        return Seq(
          Row(
            "none", // host
            "none", // assigned_host
            0,      // locality_hits (no tasks)
            0,      // locality_misses (no tasks)
            0,      // splits_prewarmed
            formatSegments(resolvedSegments, parquetSegments),
            fields.map(_.mkString(",")).getOrElse("all"),
            0L,          // duration_ms
            "no_splits", // status
            null,        // skipped_fields
            0,           // retries
            null,        // failed_splits_by_host
            null,        // job_id (sync mode has no job ID)
            false        // async_mode
          )
        )
      }

      // Get available hosts and assign splits
      val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sc)
      val splitPaths     = addActions.map(_.path)
      val assignments    = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

      // Group splits by assigned host
      val splitsByHost = addActions
        .groupBy(action => assignments.getOrElse(action.path, "any"))
        .filter(_._1 != "any")

      logger.info(s"Distributing prewarm across ${splitsByHost.size} hosts")

      // Read failOnMissingField config from session (can be overridden by command parameter)
      val effectiveFailOnMissingField = mergedConfig
        .get("spark.indextables.prewarm.failOnMissingField")
        .map(_.toBoolean)
        .getOrElse(failOnMissingField)

      // Create prewarm tasks with batching based on splitsPerTask
      val prewarmTasks = splitsByHost.flatMap {
        case (hostname, hostSplits) =>
          hostSplits.grouped(splitsPerTask).zipWithIndex.map {
            case (batch, batchIndex) =>
              PrewarmTask(
                batchIndex = batchIndex,
                hostname = hostname,
                addActions = batch,
                tablePath = tablePath,
                segments = resolvedSegments,
                fields = fields,
                failOnMissingField = effectiveFailOnMissingField,
                parquetSegments = parquetSegments,
                partitionColumns = partitionColumnSet
              )
          }
      }.toSeq

      logger.info(s"Created ${prewarmTasks.length} prewarm tasks ($splitsPerTask splits per task)")

      // Broadcast config for executor access
      val broadcastConfig = sc.broadcast(mergedConfig)

      // Build lookup: splitPath -> assigned hostname (for retry task creation)
      val splitToHost: Map[String, String] = splitsByHost.flatMap {
        case (host, actions) => actions.map(_.path -> host)
      }.toMap

      // Get max retries from config
      val maxRetries = mergedConfig.getOrElse("spark.indextables.prewarm.maxRetries", "10").toInt

      // Helper function to dispatch tasks and collect results
      def dispatchTasks(tasks: Seq[PrewarmTask], retryNum: Int): Seq[PrewarmTaskResult] = {
        val jobGroup = s"tantivy4spark-prewarm-${System.currentTimeMillis()}"
        val jobDescription = if (retryNum == 0) {
          s"Prewarming ${addActions.length} splits across ${splitsByHost.size} hosts"
        } else {
          s"Prewarm retry $retryNum: ${tasks.flatMap(_.addActions).size} splits"
        }

        sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

        val tasksWithLocations = tasks.map(t => (t, Seq(t.hostname)))

        try
          sc.makeRDD(tasksWithLocations)
            .setName(if (retryNum == 0) s"Prewarm Cache: ${addActions.length} splits" else s"Prewarm Retry $retryNum")
            .map(task => executePrewarmTask(task, broadcastConfig.value))
            .collect()
            .toSeq
        finally
          sc.clearJobGroup()
      }

      // Retry loop - dispatch, collect wrong_host, rebuild per-split tasks, repeat
      var retryCount            = 0
      var pendingTasks          = prewarmTasks
      var allSuccessfulResults  = Seq.empty[PrewarmTaskResult]
      var finalWrongHostResults = Seq.empty[PrewarmTaskResult] // Track final failures

      while (pendingTasks.nonEmpty) {
        val taskResults = dispatchTasks(pendingTasks, retryCount)

        val (succeeded, wrongHost) = taskResults.partition(_.status != "wrong_host")
        allSuccessfulResults ++= succeeded

        if (wrongHost.isEmpty) {
          // All succeeded - exit loop
          pendingTasks = Seq.empty
        } else if (retryCount >= maxRetries) {
          // Exhausted retries - record final failures and exit
          finalWrongHostResults = wrongHost
          logger.warn(s"Prewarm exhausted max retries ($maxRetries), ${wrongHost.size} tasks still on wrong host")
          pendingTasks = Seq.empty
        } else {
          // Retry misrouted splits
          retryCount += 1

          // Get misrouted split paths from results and their original AddActions
          val misroutedPaths   = wrongHost.flatMap(_.splitPaths).toSet
          val misroutedActions = addActions.filter(a => misroutedPaths.contains(a.path))

          // Warn if any splits can't be found in splitToHost map
          val unknownSplits = misroutedActions.filter(a => !splitToHost.contains(a.path))
          if (unknownSplits.nonEmpty) {
            logger.warn(s"${unknownSplits.size} splits have no assigned host, will use original assignment")
          }

          // Create individual task per split for better locality on retry
          pendingTasks = misroutedActions.zipWithIndex.map {
            case (addAction, idx) =>
              PrewarmTask(
                batchIndex = idx,
                hostname = splitToHost.getOrElse(
                  addAction.path,
                  wrongHost.find(_.splitPaths.contains(addAction.path)).map(_.assignedHost).getOrElse("unknown")
                ),
                addActions = Seq(addAction),
                tablePath = tablePath,
                segments = resolvedSegments,
                fields = fields,
                failOnMissingField = effectiveFailOnMissingField,
                parquetSegments = parquetSegments,
                partitionColumns = partitionColumnSet
              )
          }
          logger.warn(s"Prewarm retry $retryCount/$maxRetries: ${pendingTasks.size} splits misrouted, retrying with per-split tasks")
        }
      }

      // Track final failures by assigned host (from the wrong_host results)
      val failedByHost: Map[String, Int] = if (finalWrongHostResults.nonEmpty) {
        finalWrongHostResults.groupBy(_.assignedHost).map {
          case (h, results) =>
            h -> results.flatMap(_.splitPaths).size
        }
      } else Map.empty

      val failedByHostStr: String = if (failedByHost.nonEmpty) {
        failedByHost.map { case (h, count) => s"$h:$count" }.mkString(",")
      } else null

      if (failedByHost.nonEmpty) {
        logger.warn(
          s"Prewarm completed with ${failedByHost.values.sum} splits failed after $retryCount retries: $failedByHostStr"
        )
      }

      // Aggregate results by host
      val hostResults = allSuccessfulResults
        .groupBy(_.hostname)
        .map {
          case (hostname, results) =>
            val totalSplits   = results.map(_.splitsPrewarmed).sum
            val totalDuration = results.map(_.durationMs).max // Use max since tasks run in parallel
            val allSkipped    = results.flatMap(_.skippedFields).distinct
            val hasErrors     = results.exists(_.status.startsWith("error"))
            val hasPartial    = results.exists(_.status == "partial")
            val hasFailed     = failedByHost.contains(hostname)
            val overallStatus = if (hasErrors) {
              // Surface the first error message so users can see what went wrong
              results.find(_.status.startsWith("error")).map(_.status).getOrElse("error")
            } else if (hasFailed) "partial"
            else if (hasPartial) "partial"
            else "success"
            val segmentsStr = results.head.segments
            val fieldsStr   = results.head.fields
            val skippedStr  = if (allSkipped.isEmpty) null else allSkipped.mkString(",")
            // Locality tracking - count hits/misses for visibility into scheduling behavior
            val assignedHost   = results.head.assignedHost
            val localityHits   = results.count(_.localityMatch)
            val localityMisses = results.count(!_.localityMatch)

            Row(
              hostname,
              assignedHost,
              localityHits,
              localityMisses,
              totalSplits,
              segmentsStr,
              fieldsStr,
              totalDuration,
              overallStatus,
              skippedStr,
              retryCount,
              failedByHostStr,
              null, // job_id (sync mode has no job ID)
              false // async_mode
            )
        }
        .toSeq

      val totalDuration = System.currentTimeMillis() - startTime
      logger.info(s"Prewarm completed in ${totalDuration}ms: ${addActions.length} splits across ${hostResults.length} hosts (retries: $retryCount)")

      hostResults

    } finally
      transactionLog.close()
  }

  /** Execute asynchronous prewarm - starts jobs in background and returns immediately. */
  private def runAsync(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Starting asynchronous prewarm cache for table: $tablePath")
    val jobId = java.util.UUID.randomUUID().toString

    val sc = sparkSession.sparkContext

    // Resolve segment aliases to IndexComponent set, extracting parquet companion aliases separately
    val asyncSegmentString                = segments.mkString(",")
    val asyncParquetSegments: Set[String] = IndexComponentMapping.parseParquetSegments(asyncSegmentString)
    if (asyncParquetSegments.nonEmpty)
      logger.info(s"Companion parquet preload segments: ${asyncParquetSegments.mkString(", ")}")
    val resolvedSegments: Set[IndexComponent] = if (segments.isEmpty) {
      logger.info("Using default segments: TERM, POSTINGS")
      IndexComponentMapping.defaultComponents
    } else {
      val resolved = IndexComponentMapping.parseSegments(asyncSegmentString)
      if (resolved.nonEmpty) logger.info(s"Using specified segments: ${resolved.map(_.name()).mkString(", ")}")
      // Only fall back to defaults if no parquet segments were specified either
      if (resolved.isEmpty && asyncParquetSegments.isEmpty) IndexComponentMapping.defaultComponents else resolved
    }

    // Get session config for credentials and cache settings
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs =
      ConfigNormalization.extractTantivyConfigsFromHadoop(sparkSession.sparkContext.hadoopConfiguration)
    val mergedConfig = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Create transaction log
    import scala.jdk.CollectionConverters._
    val transactionLog = TransactionLogFactory.create(
      new Path(tablePath),
      sparkSession,
      new CaseInsensitiveStringMap(mergedConfig.asJava)
    )

    try {
      // Get partition schema for predicate validation
      val metadata = transactionLog.getMetadata()
      val partitionSchema = StructType(
        metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
      )

      // Parse predicates and convert to Spark Filters for Avro manifest pruning
      val (parsedPredicates, partitionFilters) = if (wherePredicates.nonEmpty) {
        val parsed  = PartitionPredicateUtils.parseAndValidatePredicates(wherePredicates, partitionSchema, sparkSession)
        val filters = PartitionPredicateUtils.expressionsToFilters(parsed)
        logger.info(s"Converted ${filters.length} of ${parsed.length} predicates to Spark Filters for manifest pruning")
        (parsed, filters)
      } else {
        (Seq.empty, Seq.empty)
      }

      // Get active splits with manifest-level pruning if filters are available
      var addActions = if (partitionFilters.nonEmpty) {
        val actions = transactionLog.listFilesWithPartitionFilters(partitionFilters)
        logger.info(s"Found ${actions.length} splits using Avro manifest pruning")
        actions
      } else {
        val actions = transactionLog.listFiles()
        logger.info(s"Found ${actions.length} splits in transaction log")
        actions
      }

      // Sort by modificationTime descending (newest first) so newest data gets hot first
      addActions = addActions.sortBy(_.modificationTime)(Ordering[Long].reverse)

      // Apply in-memory partition filtering for any predicates not converted to Spark Filters
      // This handles complex predicates that couldn't be pushed down to manifest pruning
      if (parsedPredicates.nonEmpty) {
        addActions = PartitionPredicateUtils.filterAddActionsByPredicates(addActions, partitionSchema, parsedPredicates)
        logger.info(s"After in-memory partition filtering: ${addActions.length} splits")
      }

      if (addActions.isEmpty) {
        logger.info("No splits to prewarm")
        return Seq(
          Row(
            "none", // host
            "none", // assigned_host
            0,      // locality_hits
            0,      // locality_misses
            0,      // splits_prewarmed
            formatSegments(resolvedSegments, asyncParquetSegments),
            fields.map(_.mkString(",")).getOrElse("all"),
            0L,          // duration_ms
            "no_splits", // status
            null,        // skipped_fields
            0,           // retries
            null,        // failed_splits_by_host
            jobId,       // job_id
            true         // async_mode
          )
        )
      }

      // Get available hosts and assign splits
      val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sc)
      val splitPaths     = addActions.map(_.path)
      val assignments    = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

      // Group ALL splits by assigned host (one task per host for async mode)
      val splitsByHost = addActions
        .groupBy(action => assignments.getOrElse(action.path, "any"))
        .filter(_._1 != "any")

      logger.info(s"Distributing async prewarm across ${splitsByHost.size} hosts")

      // Read config values
      val effectiveFailOnMissingField = mergedConfig
        .get("spark.indextables.prewarm.failOnMissingField")
        .map(_.toBoolean)
        .getOrElse(failOnMissingField)
      val maxRetries    = mergedConfig.getOrElse("spark.indextables.prewarm.maxRetries", "10").toInt
      val maxConcurrent = mergedConfig.getOrElse("spark.indextables.prewarm.async.maxConcurrent", "1").toInt
      val completedRetentionMs =
        mergedConfig.getOrElse("spark.indextables.prewarm.async.completedJobRetentionMs", "3600000").toLong

      // Create one async task per host with ALL splits for that host
      val asyncTasks = splitsByHost.map {
        case (hostname, hostSplits) =>
          AsyncPrewarmTask(
            jobId = jobId,
            hostname = hostname,
            addActions = hostSplits,
            tablePath = tablePath,
            segments = resolvedSegments,
            fields = fields,
            failOnMissingField = effectiveFailOnMissingField,
            maxConcurrent = maxConcurrent,
            completedRetentionMs = completedRetentionMs,
            parquetSegments = asyncParquetSegments
          )
      }.toSeq

      logger.info(s"Created ${asyncTasks.length} async prewarm tasks (one per host)")

      // Broadcast config for executor access
      val broadcastConfig = sc.broadcast(mergedConfig)

      // Build lookup: splitPath -> assigned hostname (for retry task creation)
      val splitToHost: Map[String, String] = splitsByHost.flatMap {
        case (host, actions) => actions.map(_.path -> host)
      }.toMap

      // Helper function to dispatch async tasks
      def dispatchAsyncTasks(tasks: Seq[AsyncPrewarmTask], retryNum: Int): Seq[AsyncPrewarmStartResult] = {
        val jobGroup = s"tantivy4spark-async-prewarm-${System.currentTimeMillis()}"
        val jobDescription = if (retryNum == 0) {
          s"Starting async prewarm for ${addActions.length} splits across ${splitsByHost.size} hosts"
        } else {
          s"Async prewarm retry $retryNum: ${tasks.flatMap(_.addActions).size} splits"
        }

        sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

        val tasksWithLocations = tasks.map(t => (t, Seq(t.hostname)))

        try
          sc.makeRDD(tasksWithLocations)
            .setName(
              if (retryNum == 0) s"Async Prewarm: ${addActions.length} splits" else s"Async Prewarm Retry $retryNum"
            )
            .map(task => executeAsyncPrewarmTask(task, broadcastConfig.value))
            .collect()
            .toSeq
        finally
          sc.clearJobGroup()
      }

      // Retry loop - dispatch, collect wrong_host, rebuild tasks, repeat
      var retryCount            = 0
      var pendingTasks          = asyncTasks
      var allSuccessfulResults  = Seq.empty[AsyncPrewarmStartResult]
      var finalWrongHostResults = Seq.empty[AsyncPrewarmStartResult]

      while (pendingTasks.nonEmpty) {
        val taskResults = dispatchAsyncTasks(pendingTasks, retryCount)

        val (succeeded, wrongHost) = taskResults.partition(_.status != "wrong_host")
        allSuccessfulResults ++= succeeded

        if (wrongHost.isEmpty) {
          pendingTasks = Seq.empty
        } else if (retryCount >= maxRetries) {
          finalWrongHostResults = wrongHost
          logger.warn(s"Async prewarm exhausted max retries ($maxRetries), ${wrongHost.size} tasks still on wrong host")
          pendingTasks = Seq.empty
        } else {
          retryCount += 1

          // Get misrouted split paths and rebuild tasks
          val misroutedPaths   = wrongHost.flatMap(_.splitPaths).toSet
          val misroutedActions = addActions.filter(a => misroutedPaths.contains(a.path))

          // Create individual tasks per split for better locality on retry
          pendingTasks = misroutedActions.map { addAction =>
            AsyncPrewarmTask(
              jobId = jobId,
              hostname = splitToHost.getOrElse(addAction.path, "unknown"),
              addActions = Seq(addAction),
              tablePath = tablePath,
              segments = resolvedSegments,
              fields = fields,
              failOnMissingField = effectiveFailOnMissingField,
              maxConcurrent = maxConcurrent,
              completedRetentionMs = completedRetentionMs,
              parquetSegments = asyncParquetSegments
            )
          }
          logger.warn(s"Async prewarm retry $retryCount/$maxRetries: ${pendingTasks.size} splits misrouted")
        }
      }

      // Build result rows
      val resultRows = allSuccessfulResults.map { result =>
        Row(
          result.hostname,     // host
          result.assignedHost, // assigned_host
          0,                   // locality_hits (not tracked at start)
          0,                   // locality_misses
          0,                   // splits_prewarmed (job just started)
          result.segments,     // segments
          result.fields,       // fields
          0L,                  // duration_ms (job just started)
          result.status,       // status
          null,                // skipped_fields
          retryCount,          // retries
          null,                // failed_splits_by_host
          result.jobId,        // job_id
          true                 // async_mode
        )
      }

      // Add rows for final wrong_host failures
      val wrongHostRows = finalWrongHostResults.map { result =>
        Row(
          result.hostname,
          result.assignedHost,
          0,
          0,
          0,
          result.segments,
          result.fields,
          0L,
          "wrong_host",
          null,
          retryCount,
          null,
          result.jobId,
          true
        )
      }

      logger.info(s"Async prewarm started: jobId=$jobId, ${resultRows.size} hosts, retries=$retryCount")
      resultRows ++ wrongHostRows

    } finally
      transactionLog.close()
  }

  /** Execute async prewarm task - starts background job and returns immediately. */
  private def executeAsyncPrewarmTask(
    task: AsyncPrewarmTask,
    config: Map[String, String]
  ): AsyncPrewarmStartResult = {
    val taskLogger = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

    // Get actual hostname
    val actualHostname = org.apache.spark.SparkEnv.get.blockManager.blockManagerId.host

    // Verify locality
    if (task.hostname != actualHostname) {
      taskLogger.warn(s"LOCALITY MISMATCH: Async task assigned to '${task.hostname}' but running on '$actualHostname'")
      return AsyncPrewarmStartResult(
        hostname = actualHostname,
        assignedHost = task.hostname,
        jobId = task.jobId,
        status = "wrong_host",
        segments = formatSegments(task.segments, task.parquetSegments),
        fields = task.fields.map(_.mkString(",")).getOrElse("all"),
        totalSplits = task.addActions.size,
        message = s"Wrong host: expected ${task.hostname}, got $actualHostname",
        splitPaths = task.addActions.map(_.path)
      )
    }

    // Configure AsyncPrewarmJobManager
    AsyncPrewarmJobManager.configure(task.maxConcurrent, task.completedRetentionMs)

    // Define the prewarm work (closure that does actual prewarming)
    val prewarmWork: () => AsyncPrewarmJobResult = () => {
      val startTime                    = System.currentTimeMillis()
      var splitsPrewarmed              = 0
      var errorMessage: Option[String] = None

      try {
        // Create cache config
        val cacheConfig  = ConfigUtils.createSplitCacheConfig(config, Some(task.tablePath))
        val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

        task.addActions.foreach { addAction =>
          // Check for cancellation
          if (AsyncPrewarmJobManager.isJobCancelled(task.jobId)) {
            taskLogger.info(s"Async prewarm job ${task.jobId} cancelled")
            throw new InterruptedException("Job cancelled")
          }

          try {
            // Normalize path
            val fullPath =
              if (ProtocolNormalizer.isS3Path(addAction.path) || ProtocolNormalizer.isAzurePath(addAction.path)) {
                addAction.path
              } else {
                s"${task.tablePath}/${addAction.path}"
              }
            val actualPath = ProtocolNormalizer.normalizeAllProtocols(fullPath)

            // Create split metadata and searcher
            val splitMetadata = SplitMetadataFactory.fromAddAction(addAction, task.tablePath)
            val splitSearcher = cacheManager.createSplitSearcher(actualPath, splitMetadata)

            try {
              // Prewarm based on field selection
              task.fields match {
                case Some(requestedFields) =>
                  // Field-specific preloading
                  val futures = task.segments.toSeq.map { component =>
                    splitSearcher.preloadFields(component, requestedFields: _*)
                  }
                  futures.foreach(_.join())

                case None =>
                  // Full component preloading
                  val fut = splitSearcher.preloadComponents(task.segments.toArray: _*)
                  fut.join()
              }

              // Companion parquet preloading (after standard components)
              // Auto-detect: if FAST_FIELD was requested and some requested fields aren't in the
              // split's tantivy fast fields, auto-preload parquet fast fields for companion splits.
              val asyncCompanionMode = addAction.companionFastFieldMode.getOrElse("DISABLED")
              val asyncAutoPreload = task.parquetSegments.isEmpty &&
                task.segments.contains(IndexComponent.FASTFIELD) &&
                (asyncCompanionMode == "PARQUET_ONLY" || asyncCompanionMode == "HYBRID") && {
                  val tantivyFastFields = EnhancedTransactionLogCache
                    .getDocMappingMetadata(addAction)
                    .fastFields
                    .map(_.toLowerCase)
                  task.fields match {
                    case Some(requestedFields) =>
                      requestedFields.exists(f => !tantivyFastFields.contains(f.toLowerCase))
                    case None => true
                  }
                }
              val asyncShouldPreloadFast = task.parquetSegments.contains("PARQUET_FAST_FIELDS") || asyncAutoPreload
              val asyncShouldPreloadCols = task.parquetSegments.contains("PARQUET_COLUMNS")

              if (asyncShouldPreloadFast || asyncShouldPreloadCols) {
                try
                  if (splitSearcher.hasParquetCompanion()) {
                    val fieldArgs = task.fields.getOrElse(Seq.empty)
                    if (asyncShouldPreloadFast) {
                      taskLogger.debug(
                        s"Preloading parquet fast fields for ${addAction.path}" +
                          (if (asyncAutoPreload) s" (auto-detected, companionMode=$asyncCompanionMode)" else "")
                      )
                      splitSearcher.preloadParquetFastFields(fieldArgs: _*).join()
                    }
                    if (asyncShouldPreloadCols) {
                      taskLogger.debug(s"Preloading parquet columns for ${addAction.path}")
                      splitSearcher.preloadParquetColumns(fieldArgs: _*).join()
                    }
                  }
                catch {
                  case e: Exception =>
                    taskLogger.warn(s"Parquet preload failed for ${addAction.path}: ${e.getMessage}")
                }
              }

              splitsPrewarmed += 1
              AsyncPrewarmJobManager.incrementProgress(task.jobId)

              // Record prewarm completion
              DriverSplitLocalityManager.recordPrewarmCompletion(
                addAction.path,
                actualHostname,
                task.segments,
                task.fields.map(_.toSet)
              )

            } finally
              try splitSearcher.close()
              catch { case _: Exception => }
          } catch {
            case e: InterruptedException => throw e
            case e: Exception =>
              taskLogger.warn(s"Failed to prewarm split ${addAction.path}: ${e.getMessage}")
          }
        }
      } catch {
        case e: InterruptedException =>
          errorMessage = Some("Job cancelled")
        case e: Exception =>
          taskLogger.error(s"Async prewarm job ${task.jobId} failed: ${e.getMessage}", e)
          errorMessage = Some(e.getMessage)
      }

      val durationMs = System.currentTimeMillis() - startTime
      taskLogger.info(
        s"Async prewarm job ${task.jobId} completed: $splitsPrewarmed/${task.addActions.size} splits in ${durationMs}ms"
      )

      AsyncPrewarmJobResult(
        jobId = task.jobId,
        tablePath = task.tablePath,
        hostname = actualHostname,
        totalSplits = task.addActions.size,
        splitsPrewarmed = splitsPrewarmed,
        durationMs = durationMs,
        success = errorMessage.isEmpty && splitsPrewarmed > 0,
        errorMessage = errorMessage
      )
    }

    // Try to start the job
    AsyncPrewarmJobManager.tryStartJob(
      task.jobId,
      task.tablePath,
      actualHostname,
      task.addActions.size,
      prewarmWork
    ) match {
      case Right(job) =>
        taskLogger.info(s"Started async prewarm job ${task.jobId} on $actualHostname (${task.addActions.size} splits)")
        AsyncPrewarmStartResult(
          hostname = actualHostname,
          assignedHost = task.hostname,
          jobId = task.jobId,
          status = "job_started",
          segments = formatSegments(task.segments, task.parquetSegments),
          fields = task.fields.map(_.mkString(",")).getOrElse("all"),
          totalSplits = task.addActions.size,
          message = s"Job started with ${task.addActions.size} splits",
          splitPaths = task.addActions.map(_.path)
        )

      case Left(reason) =>
        taskLogger.warn(s"Async prewarm job ${task.jobId} rejected on $actualHostname: $reason")
        AsyncPrewarmStartResult(
          hostname = actualHostname,
          assignedHost = task.hostname,
          jobId = task.jobId,
          status = "rejected",
          segments = formatSegments(task.segments, task.parquetSegments),
          fields = task.fields.map(_.mkString(",")).getOrElse("all"),
          totalSplits = task.addActions.size,
          message = reason,
          splitPaths = task.addActions.map(_.path)
        )
    }
  }

  /** Execute prewarm for a task (batch of splits) on an executor. */
  private def executePrewarmTask(
    task: PrewarmTask,
    config: Map[String, String]
  ): PrewarmTaskResult = {
    val taskStartTime = System.currentTimeMillis()
    val taskLogger    = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

    // Get actual hostname using BlockManager's blockManagerId.host - this is the exact format
    // Spark uses for task scheduling and is what getExecutorMemoryStatus returns.
    // This ensures we compare apples to apples for locality verification.
    val actualHostname = org.apache.spark.SparkEnv.get.blockManager.blockManagerId.host

    // task.hostname comes from DriverSplitLocalityManager.getAvailableHosts() which extracts
    // hosts from sc.getExecutorMemoryStatus.keys - same source as blockManagerId.host
    val localityMatch = task.hostname == actualHostname

    // Log locality verification and skip prewarm if on wrong host
    if (!localityMatch) {
      taskLogger.warn(s"LOCALITY MISMATCH: Task assigned to '${task.hostname}' but running on '$actualHostname'. Skipping prewarm for retry.")
      val duration = System.currentTimeMillis() - taskStartTime
      return PrewarmTaskResult(
        hostname = actualHostname,
        assignedHost = task.hostname,
        localityMatch = false,
        splitsPrewarmed = 0,
        segments = formatSegments(task.segments, task.parquetSegments),
        fields = task.fields.map(_.mkString(",")).getOrElse("all"),
        durationMs = duration,
        status = "wrong_host",
        skippedFields = None,
        splitPaths = task.addActions.map(_.path)
      )
    } else {
      taskLogger.debug(s"LOCALITY VERIFIED: Task correctly running on assigned host '$actualHostname'")
    }

    try {
      // Create cache config
      val cacheConfig  = ConfigUtils.createSplitCacheConfig(config, Some(task.tablePath))
      val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

      val skippedFields = scala.collection.mutable.Set.empty[String]

      // Phase 1: Prepare all splits and launch all preload futures in parallel
      case class PrewarmWork(
        addAction: io.indextables.spark.transaction.AddAction,
        actualPath: String,
        splitSearcher: io.indextables.tantivy4java.split.SplitSearcher,
        futures: Seq[java.util.concurrent.CompletableFuture[Void]],
        invalidFields: Seq[String])

      val preparedWork: Seq[PrewarmWork] = task.addActions.flatMap { addAction =>
        try {
          // Normalize path for tantivy4java compatibility
          val fullPath =
            if (ProtocolNormalizer.isS3Path(addAction.path) || ProtocolNormalizer.isAzurePath(addAction.path)) {
              addAction.path
            } else {
              s"${task.tablePath}/${addAction.path}"
            }
          val actualPath = ProtocolNormalizer.normalizeAllProtocols(fullPath)

          // Create split metadata from AddAction
          val splitMetadata = SplitMetadataFactory.fromAddAction(addAction, task.tablePath)

          // Create split searcher
          val splitSearcher = cacheManager.createSplitSearcher(actualPath, splitMetadata)

          val (futures, invalidFields): (Seq[java.util.concurrent.CompletableFuture[Void]], Seq[String]) =
            task.fields match {
              case Some(requestedFields) =>
                // Filter out partition columns - they're not indexed in the split
                val (partCols, indexedRequestedFields) =
                  requestedFields.partition(f => task.partitionColumns.contains(f.toLowerCase))
                if (partCols.nonEmpty) {
                  taskLogger.debug(s"Filtering partition columns from prewarm: ${partCols.mkString(",")}")
                }

                // Field-specific preloading
                val availableFields = extractFieldsFromMetadata(addAction)

                val (validFields, invalid) = if (availableFields.nonEmpty) {
                  indexedRequestedFields.partition(f => availableFields.contains(f.toLowerCase))
                } else {
                  // Can't validate - assume all fields are valid
                  (indexedRequestedFields, Seq.empty[String])
                }

                // Handle invalid fields based on failOnMissingField setting
                if (invalid.nonEmpty && task.failOnMissingField) {
                  splitSearcher.close()
                  throw new IllegalArgumentException(
                    s"Field(s) not found in split ${addAction.path}: ${invalid.mkString(", ")}. " +
                      s"Available fields: ${availableFields.mkString(", ")}"
                  )
                }

                if (validFields.nonEmpty) {
                  // Launch all segment preloads in parallel for this split
                  val futs = task.segments.toSeq.map { component =>
                    taskLogger.debug(s"Launching async preload of ${component.name()} for fields ${validFields.mkString(",")} in split $actualPath")
                    splitSearcher.preloadFields(component, validFields: _*)
                  }
                  (futs, invalid)
                } else {
                  (Seq.empty, invalid)
                }

              case None =>
                // Full component preloading (all fields) - launch single async call
                if (task.segments.nonEmpty) {
                  val componentsArray = task.segments.toArray
                  taskLogger.debug(s"Launching async preload of components ${componentsArray.map(_.name()).mkString(",")} for all fields in split $actualPath")
                  val fut = splitSearcher.preloadComponents(componentsArray: _*)
                  (Seq(fut), Seq.empty)
                } else {
                  // No tantivy components requested (parquet-only prewarm)
                  (Seq.empty, Seq.empty)
                }
            }

          Some(PrewarmWork(addAction, actualPath, splitSearcher, futures, invalidFields))

        } catch {
          case e: IllegalArgumentException if task.failOnMissingField =>
            throw e
          case e: Exception =>
            taskLogger.warn(s"Failed to prepare split ${addAction.path} for prewarm: ${e.getMessage}")
            None
        }
      }

      taskLogger.info(
        s"Launched ${preparedWork.map(_.futures.size).sum} async preload operations for ${preparedWork.size} splits"
      )

      // Phase 2: Wait for all futures to complete (parallel execution happening now)
      var prewarmedCount = 0
      val failedSplits   = scala.collection.mutable.ArrayBuffer.empty[String]

      preparedWork.foreach { work =>
        try {
          // Join all futures for this split
          work.futures.foreach(_.join())

          // Companion parquet preloading (after standard components)
          // Auto-detect: if FAST_FIELD was requested and some requested fields aren't in the
          // split's tantivy fast fields, auto-preload parquet fast fields for companion splits.
          // This handles PARQUET_ONLY (no tantivy fast fields) and HYBRID (some fields in each).
          val companionMode = work.addAction.companionFastFieldMode.getOrElse("DISABLED")
          val autoPreloadParquetFast = task.parquetSegments.isEmpty &&
            task.segments.contains(IndexComponent.FASTFIELD) &&
            (companionMode == "PARQUET_ONLY" || companionMode == "HYBRID") && {
              // Check if any requested fields are missing from tantivy fast fields
              val tantivyFastFields = EnhancedTransactionLogCache
                .getDocMappingMetadata(work.addAction)
                .fastFields
                .map(_.toLowerCase)
              task.fields match {
                case Some(requestedFields) =>
                  // Auto-preload if any requested field is NOT a tantivy fast field
                  requestedFields.exists(f => !tantivyFastFields.contains(f.toLowerCase))
                case None =>
                  // No specific fields requested — auto-preload if companion has fast fields
                  true
              }
            }
          val shouldPreloadParquetFast = task.parquetSegments.contains("PARQUET_FAST_FIELDS") || autoPreloadParquetFast
          val shouldPreloadParquetCols = task.parquetSegments.contains("PARQUET_COLUMNS")

          if (shouldPreloadParquetFast || shouldPreloadParquetCols) {
            try
              if (work.splitSearcher.hasParquetCompanion()) {
                val fieldArgs = task.fields.getOrElse(Seq.empty)
                if (shouldPreloadParquetFast) {
                  taskLogger.debug(
                    s"Preloading parquet fast fields for ${work.addAction.path}" +
                      (if (autoPreloadParquetFast) s" (auto-detected, companionMode=$companionMode)" else "")
                  )
                  work.splitSearcher.preloadParquetFastFields(fieldArgs: _*).join()
                }
                if (shouldPreloadParquetCols) {
                  taskLogger.debug(s"Preloading parquet columns for ${work.addAction.path}")
                  work.splitSearcher.preloadParquetColumns(fieldArgs: _*).join()
                }
              }
            catch {
              case e: Exception =>
                taskLogger.warn(s"Parquet preload failed for ${work.addAction.path}: ${e.getMessage}")
            }
          }

          prewarmedCount += 1

          // Track skipped fields
          if (work.invalidFields.nonEmpty) {
            taskLogger.warn(
              s"Skipping missing fields for split ${work.addAction.path}: ${work.invalidFields.mkString(", ")}"
            )
            skippedFields ++= work.invalidFields
          }

          // Record prewarm completion for catch-up tracking
          DriverSplitLocalityManager.recordPrewarmCompletion(
            work.addAction.path,
            actualHostname,
            task.segments,
            task.fields.map(_.toSet)
          )

        } catch {
          case e: Exception =>
            taskLogger.warn(s"Failed to complete prewarm for split ${work.addAction.path}: ${e.getMessage}")
            failedSplits += work.addAction.path
        } finally
          // Always close the split searcher
          try
            work.splitSearcher.close()
          catch {
            case e: Exception =>
              taskLogger.debug(s"Error closing split searcher for ${work.actualPath}: ${e.getMessage}")
          }
      }

      val duration = System.currentTimeMillis() - taskStartTime
      val status   = if (failedSplits.nonEmpty) "partial" else if (skippedFields.nonEmpty) "partial" else "success"

      taskLogger.info(s"Prewarm completed: $prewarmedCount/${preparedWork.size} splits in ${duration}ms")

      PrewarmTaskResult(
        hostname = actualHostname,
        assignedHost = task.hostname,
        localityMatch = localityMatch,
        splitsPrewarmed = prewarmedCount,
        segments = formatSegments(task.segments, task.parquetSegments),
        fields = task.fields.map(_.mkString(",")).getOrElse("all"),
        durationMs = duration,
        status = status,
        skippedFields = if (skippedFields.isEmpty) None else Some(skippedFields.toSeq.distinct.mkString(",")),
        splitPaths = task.addActions.map(_.path)
      )
    } catch {
      case e: Exception =>
        taskLogger.error(s"Prewarm task failed on host $actualHostname: ${e.getMessage}", e)
        val duration = System.currentTimeMillis() - taskStartTime
        PrewarmTaskResult(
          hostname = actualHostname,
          assignedHost = task.hostname,
          localityMatch = localityMatch,
          splitsPrewarmed = 0,
          segments = formatSegments(task.segments, task.parquetSegments),
          fields = task.fields.map(_.mkString(",")).getOrElse("all"),
          durationMs = duration,
          status = s"error: ${e.getMessage}",
          skippedFields = None,
          splitPaths = task.addActions.map(_.path)
        )
    }
  }

  /**
   * Extract field names from AddAction metadata (docMappingJson). Uses cached DocMappingMetadata to avoid repeated JSON
   * parsing.
   */
  private def extractFieldsFromMetadata(addAction: io.indextables.spark.transaction.AddAction): Set[String] = {
    // Use cached DocMappingMetadata - no JSON parsing here
    val metadata = EnhancedTransactionLogCache.getDocMappingMetadata(addAction)
    // Return field names in lowercase for case-insensitive matching
    metadata.fieldNames.map(_.toLowerCase)
  }
}

/** Internal task representation for prewarm distribution. */
private[sql] case class PrewarmTask(
  batchIndex: Int,
  hostname: String,
  addActions: Seq[io.indextables.spark.transaction.AddAction],
  tablePath: String,
  segments: Set[IndexComponent],
  fields: Option[Seq[String]],
  failOnMissingField: Boolean,
  parquetSegments: Set[String] = Set.empty,
  partitionColumns: Set[String] = Set.empty)
    extends Serializable

/** Result from a single prewarm task execution. */
private[sql] case class PrewarmTaskResult(
  hostname: String,
  assignedHost: String,
  localityMatch: Boolean,
  splitsPrewarmed: Int,
  segments: String,
  fields: String,
  durationMs: Long,
  status: String,
  skippedFields: Option[String],
  splitPaths: Seq[String] = Seq.empty) // Split paths for retry tracking
    extends Serializable

/** Internal task representation for async prewarm distribution. */
private[sql] case class AsyncPrewarmTask(
  jobId: String,
  hostname: String,
  addActions: Seq[io.indextables.spark.transaction.AddAction],
  tablePath: String,
  segments: Set[IndexComponent],
  fields: Option[Seq[String]],
  failOnMissingField: Boolean,
  maxConcurrent: Int,
  completedRetentionMs: Long,
  parquetSegments: Set[String] = Set.empty)
    extends Serializable

/** Result from starting an async prewarm task (returned immediately). */
private[sql] case class AsyncPrewarmStartResult(
  hostname: String,
  assignedHost: String,
  jobId: String,
  status: String, // "job_started", "rejected", or "wrong_host"
  segments: String,
  fields: String,
  totalSplits: Int,
  message: String,
  splitPaths: Seq[String] = Seq.empty)
    extends Serializable
