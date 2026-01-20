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

import io.indextables.spark.prewarm.IndexComponentMapping
import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.{PartitionPredicateUtils, TransactionLogFactory}
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
  failOnMissingField: Boolean = true)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

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
    AttributeReference("failed_splits_by_host", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Starting prewarm cache for table: $tablePath")
    val startTime = System.currentTimeMillis()

    val sc = sparkSession.sparkContext

    // Resolve segment aliases to IndexComponent set
    val resolvedSegments: Set[IndexComponent] = if (segments.isEmpty) {
      logger.info("Using default segments: TERM, POSTINGS")
      IndexComponentMapping.defaultComponents
    } else {
      val resolved = segments.map { seg =>
        IndexComponentMapping.aliasToComponent.getOrElse(
          seg,
          throw new IllegalArgumentException(
            s"Unknown segment type: $seg. Valid types: ${IndexComponentMapping.aliasToComponent.keys.mkString(", ")}"
          )
        )
      }.toSet
      logger.info(s"Using specified segments: ${resolved.map(_.name()).mkString(", ")}")
      resolved
    }

    // Get session config for credentials and cache settings
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs =
      ConfigNormalization.extractTantivyConfigsFromHadoop(sparkSession.sparkContext.hadoopConfiguration)
    val mergedConfig = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

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

      // Get all active splits
      var addActions = transactionLog.listFiles()
      logger.info(s"Found ${addActions.length} splits in transaction log")

      // Apply partition predicates if specified
      if (wherePredicates.nonEmpty) {
        val parsedPredicates =
          PartitionPredicateUtils.parseAndValidatePredicates(wherePredicates, partitionSchema, sparkSession)
        addActions = PartitionPredicateUtils.filterAddActionsByPredicates(addActions, partitionSchema, parsedPredicates)
        logger.info(s"After partition filtering: ${addActions.length} splits")
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
            resolvedSegments.map(_.name()).toSeq.sorted.mkString(","),
            fields.map(_.mkString(",")).getOrElse("all"),
            0L,          // duration_ms
            "no_splits", // status
            null,        // skipped_fields
            0,           // retries
            null         // failed_splits_by_host
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
                failOnMissingField = effectiveFailOnMissingField
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
                failOnMissingField = effectiveFailOnMissingField
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
            val overallStatus =
              if (hasErrors) "error" else if (hasFailed) "partial" else if (hasPartial) "partial" else "success"
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
              failedByHostStr
            )
        }
        .toSeq

      val totalDuration = System.currentTimeMillis() - startTime
      logger.info(s"Prewarm completed in ${totalDuration}ms: ${addActions.length} splits across ${hostResults.length} hosts (retries: $retryCount)")

      hostResults

    } finally
      transactionLog.close()
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
        segments = task.segments.map(_.name()).toSeq.sorted.mkString(","),
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
                // Field-specific preloading
                val availableFields = extractFieldsFromMetadata(addAction)

                val (validFields, invalid) = if (availableFields.nonEmpty) {
                  requestedFields.partition(f => availableFields.contains(f.toLowerCase))
                } else {
                  // Can't validate - assume all fields are valid
                  (requestedFields, Seq.empty[String])
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
                val componentsArray = task.segments.toArray
                taskLogger.debug(s"Launching async preload of components ${componentsArray.map(_.name()).mkString(",")} for all fields in split $actualPath")
                val fut = splitSearcher.preloadComponents(componentsArray: _*)
                (Seq(fut), Seq.empty)
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
        segments = task.segments.map(_.name()).toSeq.sorted.mkString(","),
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
          segments = task.segments.map(_.name()).toSeq.sorted.mkString(","),
          fields = task.fields.map(_.mkString(",")).getOrElse("all"),
          durationMs = duration,
          status = s"error: ${e.getMessage}",
          skippedFields = None,
          splitPaths = task.addActions.map(_.path)
        )
    }
  }

  /** Extract field names from AddAction metadata (docMappingJson). */
  private def extractFieldsFromMetadata(addAction: io.indextables.spark.transaction.AddAction): Set[String] =
    addAction.docMappingJson match {
      case Some(json) =>
        try {
          // Parse docMappingJson to extract field names
          // Format: {"field_mappings":[{"name":"field1",...},{"name":"field2",...}]}
          val mapper        = new com.fasterxml.jackson.databind.ObjectMapper()
          val root          = mapper.readTree(json)
          val fieldMappings = root.path("field_mappings")
          if (fieldMappings.isArray) {
            fieldMappings
              .elements()
              .asScala
              .flatMap(fm => Option(fm.path("name").asText(null)))
              .map(_.toLowerCase)
              .toSet
          } else {
            Set.empty[String]
          }
        } catch {
          case e: Exception =>
            logger.debug(s"Failed to parse docMappingJson for field extraction: ${e.getMessage}")
            Set.empty[String]
        }
      case None =>
        Set.empty[String]
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
  failOnMissingField: Boolean)
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
