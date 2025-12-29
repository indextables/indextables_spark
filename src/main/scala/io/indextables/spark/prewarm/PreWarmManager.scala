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

package io.indextables.spark.prewarm

import java.net.InetAddress
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext

import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.storage.{DriverSplitLocalityManager, SplitCacheConfig}
import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.SplitSearcher
import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.slf4j.LoggerFactory

/**
 * Manages pre-warming of split caches across the Spark cluster to optimize query performance.
 *
 * The pre-warming process works in two phases:
 *   1. Pre-scan: Distribute warmup tasks to executors that have cached splits 2. Post-warm: Main query execution joins
 *      on warmup futures for optimal performance
 *
 * Integration with preferredLocations system ensures that warmup tasks are scheduled on the same executors that will
 * handle the actual query execution.
 */
object PreWarmManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // Global registry of warmup futures keyed by (splitPath, hostname, queryHash)
  private val warmupFutures = new ConcurrentHashMap[String, CompletableFuture[Void]]()

  // Statistics tracking
  private val warmupStats = TrieMap[String, PreWarmStats]()

  /**
   * Execute pre-warming phase for all splits that will be queried. This should be called before the main query
   * execution to warm caches proactively.
   */
  def executePreWarm(
    sc: SparkContext,
    addActions: Seq[AddAction],
    readSchema: StructType,
    allFilters: Array[Any],
    config: Map[String, String], // Direct config instead of broadcast
    isPreWarmEnabled: Boolean
  ): PreWarmResult = {

    if (!isPreWarmEnabled) {
      logger.info("Pre-warm is disabled, skipping cache warming")
      return PreWarmResult(warmupInitiated = false, Map.empty, 0)
    }

    val startTime = System.currentTimeMillis()
    logger.info(s"🔥 Starting pre-warm for ${addActions.length} splits")

    // Get available hosts and assign splits using driver-based locality
    val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sc)
    val splitPaths     = addActions.map(_.path)
    val assignments    = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

    // Group splits by their assigned hosts for efficient distribution
    val splitsByHost = groupSplitsByAssignedHosts(addActions, assignments)
    logger.info(
      s"🔥 Pre-warm distribution: ${splitsByHost.size} hosts, ${splitsByHost.map(_._2.size).sum} split assignments"
    )

    // Create a query hash for warmup future identification
    val queryHash = generateQueryHash(allFilters)

    // Distribute warmup tasks to executors
    val warmupAssignments = distributePreWarmTasks(sc, splitsByHost, readSchema, allFilters, config, queryHash)

    val endTime = System.currentTimeMillis()
    val stats = PreWarmStats(
      totalSplits = addActions.length,
      hostsInvolved = splitsByHost.size,
      warmupTasksCreated = warmupAssignments.size,
      preWarmTimeMs = endTime - startTime
    )

    warmupStats.put(queryHash, stats)
    logger.info(s"🔥 Pre-warm completed in ${stats.preWarmTimeMs}ms: ${stats.warmupTasksCreated} tasks across ${stats.hostsInvolved} hosts")

    PreWarmResult(
      warmupInitiated = true,
      warmupAssignments = warmupAssignments,
      totalWarmupsCreated = warmupAssignments.size
    )
  }

  /**
   * Execute pre-warming with specific component selection for all splits that will be queried.
   * This is the enhanced version that supports segment/field selection from configuration.
   *
   * @param sc SparkContext for distributed execution
   * @param addActions Sequence of splits to prewarm
   * @param segments Set of IndexComponents to prewarm (e.g., TERM, FASTFIELD, POSTINGS)
   * @param fields Optional sequence of field names to prewarm (None = all fields)
   * @param splitsPerTask Number of splits to batch per Spark task (default: 2)
   * @param config Configuration map for cache and storage settings
   * @return PreWarmResult with warmup statistics
   */
  def executePreWarmWithComponents(
    sc: SparkContext,
    addActions: Seq[AddAction],
    segments: Set[IndexComponent],
    fields: Option[Seq[String]],
    splitsPerTask: Int,
    config: Map[String, String]
  ): PreWarmResult = {

    if (addActions.isEmpty) {
      logger.info("No splits to prewarm")
      return PreWarmResult(warmupInitiated = false, Map.empty, 0)
    }

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting component-based prewarm for ${addActions.length} splits")
    logger.info(s"Prewarming components: ${segments.map(_.name()).mkString(", ")}")

    // Get available hosts and assign splits using driver-based locality
    val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sc)
    val splitPaths     = addActions.map(_.path)
    val assignments    = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

    // Group splits by their assigned hosts for efficient distribution
    val splitsByHost = groupSplitsByAssignedHosts(addActions, assignments)
    logger.info(
      s"Prewarm distribution: ${splitsByHost.size} hosts, ${splitsByHost.map(_._2.size).sum} split assignments"
    )

    // Create batched prewarm tasks based on splitsPerTask
    val componentPrewarmTasks = splitsByHost.flatMap {
      case (hostname, hostSplits) =>
        hostSplits.grouped(splitsPerTask).zipWithIndex.map {
          case (batch, batchIndex) =>
            ComponentPrewarmTask(
              batchIndex = batchIndex,
              hostname = hostname,
              addActions = batch,
              segments = segments,
              fields = fields
            )
        }
    }.toSeq

    logger.info(s"Created ${componentPrewarmTasks.length} prewarm tasks (${splitsPerTask} splits per task)")

    // Broadcast config for executor access
    val broadcastConfig = sc.broadcast(config)

    // Execute prewarm tasks on executors using makeRDD for locality-aware scheduling
    val jobGroup       = s"tantivy4spark-prewarm-${System.currentTimeMillis()}"
    val jobDescription = s"Prewarming ${addActions.length} splits across ${splitsByHost.size} hosts"

    sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

    // Create (task, preferredLocations) tuples for makeRDD
    // This ensures prewarm tasks run on the same hosts where queries will execute
    val tasksWithLocations = componentPrewarmTasks.map { task =>
      (task, Seq(task.hostname))
    }

    val taskResults =
      try
        sc.makeRDD(tasksWithLocations)
          .setName(s"Prewarm Cache: ${addActions.length} splits")
          .map { task =>
            executeComponentPrewarmTask(task, broadcastConfig)
          }
          .collect()
          .toSeq
      finally
        sc.clearJobGroup()

    // Aggregate results by hostname
    val warmupAssignments = taskResults.groupBy(_.hostname).map {
      case (hostname, results) => hostname -> results.map(_.splitsPrewarmed).sum
    }

    val endTime = System.currentTimeMillis()
    logger.info(s"Component-based prewarm completed in ${endTime - startTime}ms: ${warmupAssignments.map { case (h, c) => s"$h: $c splits" }.mkString(", ")}")

    PreWarmResult(
      warmupInitiated = true,
      warmupAssignments = warmupAssignments,
      totalWarmupsCreated = componentPrewarmTasks.length
    )
  }

  /**
   * Execute a component-based prewarm task on an executor.
   * Supports both full-component preloading and field-specific preloading.
   */
  private def executeComponentPrewarmTask(
    task: ComponentPrewarmTask,
    broadcastConfig: Broadcast[Map[String, String]]
  ): ComponentPrewarmTaskResult = {
    val taskStartTime = System.currentTimeMillis()
    val actualHostname = getCurrentHostname
    val config = broadcastConfig.value
    val failOnMissingField = config.getOrElse("spark.indextables.prewarm.failOnMissingField", "true").toBoolean

    try {
      var prewarmedCount = 0
      var skippedFields = scala.collection.mutable.Set[String]()

      task.addActions.foreach { addAction =>
        // Create cache configuration
        val cacheConfig = createCacheConfigFromBroadcast(config, addAction.path)

        // Create split search engine
        val splitSearchEngine = createSplitSearchEngine(addAction, StructType(Seq.empty), cacheConfig)

        try {
          val splitSearcher = splitSearchEngine.getSplitSearcher()

          task.fields match {
            case Some(requestedFields) =>
              // Field-specific preloading using preloadFields()
              // Validate fields exist in the split metadata if available
              val availableFields = extractFieldsFromMetadata(addAction)

              val (validFields, invalidFields) = if (availableFields.nonEmpty) {
                requestedFields.partition(f => availableFields.contains(f.toLowerCase))
              } else {
                // If we can't determine available fields, assume all are valid
                (requestedFields, Seq.empty[String])
              }

              if (invalidFields.nonEmpty) {
                if (failOnMissingField) {
                  throw new IllegalArgumentException(
                    s"Fields not found in split ${addAction.path}: ${invalidFields.mkString(", ")}. " +
                    s"Available fields: ${availableFields.mkString(", ")}"
                  )
                } else {
                  logger.warn(s"Skipping missing fields for split ${addAction.path}: ${invalidFields.mkString(", ")}")
                  skippedFields ++= invalidFields
                }
              }

              if (validFields.nonEmpty) {
                // Preload each component for the valid fields
                val futures = task.segments.map { component =>
                  logger.debug(s"Preloading ${component.name()} for fields ${validFields.mkString(",")} in split ${addAction.path}")
                  splitSearcher.preloadFields(component, validFields: _*)
                }
                // Wait for all to complete
                futures.foreach(_.join())
              }

            case None =>
              // Full component preloading (all fields)
              val componentsArray = task.segments.toArray
              logger.debug(s"Preloading components ${componentsArray.map(_.name()).mkString(",")} for all fields in split ${addAction.path}")
              val prewarmFuture = splitSearcher.preloadComponents(componentsArray: _*)
              prewarmFuture.join()
          }

          prewarmedCount += 1

          // Record prewarm completion for catch-up tracking
          DriverSplitLocalityManager.recordPrewarmCompletion(
            addAction.path,
            actualHostname,
            task.segments,
            task.fields.map(_.toSet)
          )

        } catch {
          case e: Exception =>
            logger.warn(s"Failed to prewarm split ${addAction.path}: ${e.getMessage}")
        }
      }

      val duration = System.currentTimeMillis() - taskStartTime
      logger.info(s"Prewarm task completed on $actualHostname: $prewarmedCount splits in ${duration}ms")

      ComponentPrewarmTaskResult(
        hostname = actualHostname,
        splitsPrewarmed = prewarmedCount,
        durationMs = duration,
        success = true,
        errorMessage = None,
        skippedFields = if (skippedFields.isEmpty) None else Some(skippedFields.toSet)
      )
    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - taskStartTime
        logger.error(s"Prewarm task failed on $actualHostname: ${e.getMessage}", e)
        ComponentPrewarmTaskResult(
          hostname = actualHostname,
          splitsPrewarmed = 0,
          durationMs = duration,
          success = false,
          errorMessage = Some(e.getMessage),
          skippedFields = None
        )
    }
  }

  /**
   * Extract field names from AddAction metadata (docMappingJson).
   */
  private def extractFieldsFromMetadata(addAction: AddAction): Set[String] =
    addAction.docMappingJson match {
      case Some(json) =>
        try {
          // Parse docMappingJson to extract field names
          // Format: {"field_mappings":[{"name":"field1",...},{"name":"field2",...}]}
          val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
          val root = mapper.readTree(json)
          val fieldMappings = root.path("field_mappings")
          if (fieldMappings.isArray) {
            import scala.jdk.CollectionConverters._
            fieldMappings.elements().asScala.flatMap { fm =>
              Option(fm.path("name").asText(null))
            }.map(_.toLowerCase).toSet
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

  /**
   * Join on a warmup future for a specific split during query execution. This should be called in the partition reader
   * before executing the query.
   */
  def joinWarmupFuture(
    splitPath: String,
    queryHash: String,
    isPreWarmEnabled: Boolean
  ): Boolean = {
    if (!isPreWarmEnabled) {
      return false
    }

    val currentHostname = getCurrentHostname
    val futureKey       = buildFutureKey(splitPath, currentHostname, queryHash)

    Option(warmupFutures.get(futureKey)) match {
      case Some(warmupFuture) =>
        try {
          logger.debug(s"🔥 Joining warmup future for split: $splitPath on host: $currentHostname")
          // Wait for warmup to complete with a reasonable timeout
          warmupFuture.get(30, TimeUnit.SECONDS)
          logger.info(s"🔥 Successfully joined warmup future for split: $splitPath")
          true
        } catch {
          case e: Exception =>
            logger.warn(s"🔥 Failed to join warmup future for split $splitPath: ${e.getMessage}")
            false
        } finally
          // Clean up the future to prevent memory leaks
          warmupFutures.remove(futureKey)
      case None =>
        if (isPreWarmEnabled) {
          logger.warn(s"⚠️  Pre-warm enabled but no warmup future found for split $splitPath on host $currentHostname")
        }
        false
    }
  }

  /** Group splits by their assigned hosts based on driver-based locality assignments. */
  private def groupSplitsByAssignedHosts(
    addActions: Seq[AddAction],
    assignments: Map[String, String]
  ): Map[String, Seq[AddAction]] =
    addActions
      .groupBy { addAction =>
        // Use the assigned host from DriverSplitLocalityManager
        assignments.getOrElse(addAction.path, "any")
      }
      .filter(_._1 != "any") // Only include splits with known host assignments

  /** Distribute pre-warm tasks to executors using Spark's task distribution system. */
  private def distributePreWarmTasks(
    sc: SparkContext,
    splitsByHost: Map[String, Seq[AddAction]],
    readSchema: StructType,
    allFilters: Array[Any],
    config: Map[String, String], // Direct config instead of broadcast
    queryHash: String
  ): Map[String, Int] = {

    if (splitsByHost.isEmpty) {
      logger.info("🔥 No splits with preferred hosts found, skipping pre-warm task distribution")
      return Map.empty
    }

    // Create tasks for each host's splits
    val preWarmTasks = splitsByHost.flatMap {
      case (hostname, splits) =>
        splits.map(addAction => PreWarmTask(addAction, hostname, readSchema, allFilters, queryHash))
    }.toSeq

    logger.info(s"🔥 Distributing ${preWarmTasks.length} pre-warm tasks across ${splitsByHost.size} hosts")

    // Broadcast config for executor access
    val broadcastConfig = sc.broadcast(config)

    // Execute pre-warm tasks on executors with descriptive Spark UI names
    // Using makeRDD for locality-aware scheduling to ensure prewarm runs on intended hosts
    val stageName = s"Pre-warm Cache: ${preWarmTasks.length} splits across ${splitsByHost.size} hosts"
    val jobGroup  = s"tantivy4spark-prewarm-$queryHash"
    val jobDescription =
      s"Pre-warming Tantivy split caches for query ${queryHash.take(8)}... (${preWarmTasks.length} splits)"

    sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

    // Create (task, preferredLocations) tuples for makeRDD
    val tasksWithLocations = preWarmTasks.map { task =>
      (task, Seq(task.preferredHostname))
    }

    val taskResults =
      try
        sc.makeRDD(tasksWithLocations)
          .setName(stageName) // This shows up in Spark UI as the RDD name
          .mapPartitions { tasks =>
            val hostname = getCurrentHostname
            val results  = tasks.map(task => executePreWarmTask(task, broadcastConfig, hostname)).toList
            results.iterator
          }
          .setName("Pre-warm Task Results") // Name for the mapped RDD
          .collect()
      finally
        // Clear the job group to avoid affecting subsequent operations
        sc.clearJobGroup()

    // Aggregate results by hostname
    val assignments = taskResults.groupBy(_.hostname).mapValues(_.length).toMap
    logger.info(
      s"🔥 Pre-warm task distribution completed: ${assignments.map { case (h, c) => s"$h: $c tasks" }.mkString(", ")}"
    )

    assignments
  }

  /** Execute a single pre-warm task on an executor. */
  private def executePreWarmTask(
    task: PreWarmTask,
    broadcastConfig: Broadcast[Map[String, String]],
    actualHostname: String
  ): PreWarmTaskResult = {

    logger.debug(s"🔥 Executing pre-warm task for split: ${task.addAction.path} on host: $actualHostname")

    try {
      // Create cache configuration
      val cacheConfig = createCacheConfigFromBroadcast(broadcastConfig.value, task.addAction.path)

      // Create split search engine (this will populate the cache)
      val splitSearchEngine = createSplitSearchEngine(task.addAction, task.readSchema, cacheConfig)

      // Initiate async warmup using tantivy4java component preloading
      // This is more efficient than query-based warmup and doesn't require any query objects
      val splitSearcher = splitSearchEngine.getSplitSearcher()
      logger.info(s"🔥 Using component preloading for split warmup: ${task.addAction.path}")
      import io.indextables.tantivy4java.split.SplitSearcher
      val warmupFuture = splitSearcher.preloadComponents(
        SplitSearcher.IndexComponent.POSTINGS,
        SplitSearcher.IndexComponent.POSITIONS,
        SplitSearcher.IndexComponent.FASTFIELD,
        SplitSearcher.IndexComponent.FIELDNORM
      )

      // Store the future for later joining during query execution
      val futureKey = buildFutureKey(task.addAction.path, actualHostname, task.queryHash)
      warmupFutures.put(futureKey, warmupFuture)

      // Note: Split locality is now tracked on the driver side via DriverSplitLocalityManager
      // No executor-side recording needed - assignments are managed during partition planning

      logger.info(s"🔥 Pre-warm initiated for split: ${task.addAction.path} on host: $actualHostname")

      PreWarmTaskResult(
        splitPath = task.addAction.path,
        hostname = actualHostname,
        success = true,
        errorMessage = None
      )

    } catch {
      case e: Exception =>
        logger.error(s"🔥 Pre-warm failed for split: ${task.addAction.path} on host: $actualHostname", e)
        PreWarmTaskResult(
          splitPath = task.addAction.path,
          hostname = actualHostname,
          success = false,
          errorMessage = Some(e.getMessage)
        )
    }
  }

  /** Create a SplitCacheConfig from broadcast configuration. */
  private def createCacheConfigFromBroadcast(configMap: Map[String, String], tablePath: String): SplitCacheConfig = {
    def getConfig(key: String, default: String): String = configMap.getOrElse(key, default)
    def getConfigOption(key: String): Option[String]    = configMap.get(key).filter(_.trim.nonEmpty)

    SplitCacheConfig(
      cacheName = {
        val configName = getConfig("spark.indextables.cache.name", "")
        if (configName.trim.nonEmpty) configName.trim
        else s"tantivy4spark-${tablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
      },
      maxCacheSize = getConfig("spark.indextables.cache.maxSize", "200000000").toLong,
      maxConcurrentLoads = getConfig("spark.indextables.cache.maxConcurrentLoads", "8").toInt,
      enableQueryCache = getConfig("spark.indextables.cache.queryCache", "true").toBoolean,
      enableDocBatch = getConfig("spark.indextables.docBatch.enabled", "true").toBoolean,
      docBatchMaxSize = getConfig("spark.indextables.docBatch.maxSize", "1000").toInt,
      awsAccessKey = getConfigOption("spark.indextables.aws.accessKey"),
      awsSecretKey = getConfigOption("spark.indextables.aws.secretKey"),
      awsSessionToken = getConfigOption("spark.indextables.aws.sessionToken"),
      awsRegion = getConfigOption("spark.indextables.aws.region"),
      awsEndpoint = getConfigOption("spark.indextables.s3.endpoint"),
      awsPathStyleAccess = getConfigOption("spark.indextables.s3.pathStyleAccess").map(_.toLowerCase == "true"),
      azureAccountName = getConfigOption("spark.indextables.azure.accountName"),
      azureAccountKey = getConfigOption("spark.indextables.azure.accountKey"),
      azureConnectionString = getConfigOption("spark.indextables.azure.connectionString"),
      azureEndpoint = getConfigOption("spark.indextables.azure.endpoint"),
      gcpProjectId = getConfigOption("spark.indextables.gcp.projectId"),
      gcpServiceAccountKey = getConfigOption("spark.indextables.gcp.serviceAccountKey"),
      gcpCredentialsFile = getConfigOption("spark.indextables.gcp.credentialsFile"),
      gcpEndpoint = getConfigOption("spark.indextables.gcp.endpoint")
    )
  }

  /** Create a SplitSearchEngine for the given AddAction. */
  private def createSplitSearchEngine(
    addAction: AddAction,
    readSchema: StructType,
    cacheConfig: SplitCacheConfig
  ): SplitSearchEngine = {
    val filePath = addAction.path

    // Normalize path for tantivy4java compatibility
    val actualPath = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(filePath)

    // Footer offset metadata is required for all split reading operations
    if (!addAction.hasFooterOffsets || addAction.footerStartOffset.isEmpty) {
      throw new RuntimeException(
        s"AddAction for $actualPath does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata."
      )
    }

    // Handle potential Integer/Long type conversion from JSON deserialization
    def safeLong(opt: Option[Any], fieldName: String): Long = opt match {
      case Some(value) =>
        value match {
          case i if i.isInstanceOf[Integer] => i.asInstanceOf[Integer].toLong
          case l if l.isInstanceOf[Long]    => l.asInstanceOf[Long]
          case other                        => other.asInstanceOf[Number].longValue()
        }
      case None => throw new RuntimeException(s"Footer offset field $fieldName is None but hasFooterOffsets is true")
    }

    // Safe conversion functions for Option[Any] to Long
    def toLongSafeOption(opt: Option[Any]): Long = opt match {
      case Some(value) =>
        value match {
          case l: Long              => l
          case i: Int               => i.toLong
          case i: java.lang.Integer => i.toLong
          case l: java.lang.Long    => l
          case _                    => value.toString.toLong
        }
      case None => 0L
    }

    val splitMetadata = new io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata(
      addAction.path.split("/").last.replace(".split", ""),         // splitId from filename
      "tantivy4spark-index",                                        // indexUid (NEW - required)
      0L,                                                           // partitionId (NEW - required)
      "tantivy4spark-source",                                       // sourceId (NEW - required)
      "tantivy4spark-node",                                         // nodeId (NEW - required)
      toLongSafeOption(addAction.numRecords),                       // numDocs
      toLongSafeOption(addAction.uncompressedSizeBytes),            // uncompressedSizeBytes
      addAction.timeRangeStart.map(java.time.Instant.parse).orNull, // timeRangeStart
      addAction.timeRangeEnd.map(java.time.Instant.parse).orNull,   // timeRangeEnd
      System.currentTimeMillis() / 1000,                            // createTimestamp (NEW - required)
      "Mature",                                                     // maturity (NEW - required)
      addAction.splitTags.getOrElse(Set.empty[String]).asJava,      // tags
      safeLong(addAction.footerStartOffset, "footerStartOffset"),   // footerStartOffset
      safeLong(addAction.footerEndOffset, "footerEndOffset"),       // footerEndOffset
      toLongSafeOption(addAction.deleteOpstamp),                    // deleteOpstamp
      addAction.numMergeOps.getOrElse(0),                           // numMergeOps (Int is OK for this field)
      "doc-mapping-uid",                                            // docMappingUid (NEW - required)
      addAction.docMappingJson.orNull,                              // docMappingJson (MOVED - for performance)
      java.util.Collections.emptyList[String]()                     // skippedSplits
    )
    SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig)
  }

  /** Generate a hash for the query to uniquely identify warmup futures. */
  private def generateQueryHash(allFilters: Array[Any]): String = {
    val filterString = allFilters.map(_.toString).mkString("|")
    java.util.UUID.nameUUIDFromBytes(filterString.getBytes).toString.take(8)
  }

  /** Build a unique key for warmup future storage. */
  private def buildFutureKey(
    splitPath: String,
    hostname: String,
    queryHash: String
  ): String =
    s"$splitPath|$hostname|$queryHash"

  /** Get current hostname for this JVM. */
  private def getCurrentHostname: String =
    try
      InetAddress.getLocalHost.getHostName
    catch {
      case _: Exception => "unknown"
    }

  /** Get statistics about pre-warm operations. */
  def getPreWarmStats(queryHash: String): Option[PreWarmStats] =
    warmupStats.get(queryHash)

  /** Clear all warmup futures and statistics (useful for testing). */
  def clearAll(): Unit = {
    warmupFutures.clear()
    warmupStats.clear()
    logger.info("Cleared all pre-warm state")
  }
}

/** Represents a pre-warm task to be executed on an executor. */
case class PreWarmTask(
  addAction: AddAction,
  preferredHostname: String,
  readSchema: StructType,
  allFilters: Array[Any],
  queryHash: String)

/** Result of executing a pre-warm task. */
case class PreWarmTaskResult(
  splitPath: String,
  hostname: String,
  success: Boolean,
  errorMessage: Option[String])

/** Overall result of the pre-warm phase. */
case class PreWarmResult(
  warmupInitiated: Boolean,
  warmupAssignments: Map[String, Int],
  totalWarmupsCreated: Int)

/** Statistics about pre-warm operations. */
case class PreWarmStats(
  totalSplits: Int,
  hostsInvolved: Int,
  warmupTasksCreated: Int,
  preWarmTimeMs: Long)

/** Task for component-based prewarm with segment/field selection. */
case class ComponentPrewarmTask(
  batchIndex: Int,
  hostname: String,
  addActions: Seq[AddAction],
  segments: Set[IndexComponent],
  fields: Option[Seq[String]])
    extends Serializable

/** Result from a component-based prewarm task execution. */
case class ComponentPrewarmTaskResult(
  hostname: String,
  splitsPrewarmed: Int,
  durationMs: Long,
  success: Boolean,
  errorMessage: Option[String],
  skippedFields: Option[Set[String]] = None)
    extends Serializable
