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

package com.tantivy4spark.prewarm

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.Filter
import com.tantivy4spark.transaction.AddAction
import com.tantivy4spark.storage.{SplitCacheConfig, GlobalSplitCacheManager, BroadcastSplitLocalityManager}
import com.tantivy4spark.search.SplitSearchEngine
import com.tantivy4spark.core.FiltersToQueryConverter
import com.tantivy4java.Query
import org.slf4j.LoggerFactory
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.util.{Try, Success, Failure}
import java.net.InetAddress

/**
 * Manages pre-warming of split caches across the Spark cluster to optimize query performance.
 * 
 * The pre-warming process works in two phases:
 * 1. Pre-scan: Distribute warmup tasks to executors that have cached splits
 * 2. Post-warm: Main query execution joins on warmup futures for optimal performance
 * 
 * Integration with preferredLocations system ensures that warmup tasks are scheduled
 * on the same executors that will handle the actual query execution.
 */
object PreWarmManager {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Global registry of warmup futures keyed by (splitPath, hostname, queryHash)
  private val warmupFutures = new ConcurrentHashMap[String, CompletableFuture[Void]]()
  
  // Statistics tracking
  private val warmupStats = TrieMap[String, PreWarmStats]()
  
  /**
   * Execute pre-warming phase for all splits that will be queried.
   * This should be called before the main query execution to warm caches proactively.
   */
  def executePreWarm(
      sc: SparkContext,
      addActions: Seq[AddAction],
      readSchema: StructType,
      allFilters: Array[Any],
      broadcastConfig: Broadcast[Map[String, String]],
      isPreWarmEnabled: Boolean
  ): PreWarmResult = {
    
    if (!isPreWarmEnabled) {
      logger.info("Pre-warm is disabled, skipping cache warming")
      return PreWarmResult(warmupInitiated = false, Map.empty, 0)
    }
    
    val startTime = System.currentTimeMillis()
    logger.info(s"🔥 Starting pre-warm for ${addActions.length} splits")
    
    // Update broadcast locality to get current cache distribution
    BroadcastSplitLocalityManager.updateBroadcastLocality(sc)
    
    // Group splits by their preferred hosts for efficient distribution
    val splitsByHost = groupSplitsByPreferredHosts(addActions)
    logger.info(s"🔥 Pre-warm distribution: ${splitsByHost.size} hosts, ${splitsByHost.map(_._2.size).sum} split assignments")
    
    // Create a query hash for warmup future identification
    val queryHash = generateQueryHash(allFilters)
    
    // Distribute warmup tasks to executors
    val warmupAssignments = distributePreWarmTasks(sc, splitsByHost, readSchema, allFilters, broadcastConfig, queryHash)
    
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
   * Join on a warmup future for a specific split during query execution.
   * This should be called in the partition reader before executing the query.
   */
  def joinWarmupFuture(splitPath: String, queryHash: String, isPreWarmEnabled: Boolean): Boolean = {
    if (!isPreWarmEnabled) {
      return false
    }
    
    val currentHostname = getCurrentHostname
    val futureKey = buildFutureKey(splitPath, currentHostname, queryHash)
    
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
        } finally {
          // Clean up the future to prevent memory leaks
          warmupFutures.remove(futureKey)
        }
      case None =>
        if (isPreWarmEnabled) {
          logger.warn(s"⚠️  Pre-warm enabled but no warmup future found for split $splitPath on host $currentHostname")
        }
        false
    }
  }
  
  /**
   * Group splits by their preferred hosts based on cache locality information.
   */
  private def groupSplitsByPreferredHosts(addActions: Seq[AddAction]): Map[String, Seq[AddAction]] = {
    addActions.groupBy { addAction =>
      val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(addAction.path)
      // Use the first preferred host, or "any" if no preference
      if (preferredHosts.nonEmpty) preferredHosts.head else "any"
    }.filter(_._1 != "any") // Only include splits with known preferred hosts
  }
  
  /**
   * Distribute pre-warm tasks to executors using Spark's task distribution system.
   */
  private def distributePreWarmTasks(
      sc: SparkContext,
      splitsByHost: Map[String, Seq[AddAction]],
      readSchema: StructType,
      allFilters: Array[Any],
      broadcastConfig: Broadcast[Map[String, String]],
      queryHash: String
  ): Map[String, Int] = {
    
    if (splitsByHost.isEmpty) {
      logger.info("🔥 No splits with preferred hosts found, skipping pre-warm task distribution")
      return Map.empty
    }
    
    // Create tasks for each host's splits
    val preWarmTasks = splitsByHost.flatMap { case (hostname, splits) =>
      splits.map(addAction => PreWarmTask(addAction, hostname, readSchema, allFilters, queryHash))
    }.toSeq
    
    logger.info(s"🔥 Distributing ${preWarmTasks.length} pre-warm tasks across ${splitsByHost.size} hosts")
    
    // Execute pre-warm tasks on executors with descriptive Spark UI names
    val stageName = s"Pre-warm Cache: ${preWarmTasks.length} splits across ${splitsByHost.size} hosts"
    val jobGroup = s"tantivy4spark-prewarm-${queryHash}"
    val jobDescription = s"Pre-warming Tantivy split caches for query ${queryHash.take(8)}... (${preWarmTasks.length} splits)"
    
    sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)
    
    val taskResults = try {
      sc.parallelize(preWarmTasks, math.min(preWarmTasks.length, sc.defaultParallelism))
        .setName(stageName) // This shows up in Spark UI as the RDD name
        .mapPartitions { tasks =>
          val hostname = getCurrentHostname
          val results = tasks.map { task =>
            executePreWarmTask(task, broadcastConfig, hostname)
          }.toList
          results.iterator
        }
        .setName("Pre-warm Task Results") // Name for the mapped RDD
        .collect()
    } finally {
      // Clear the job group to avoid affecting subsequent operations
      sc.clearJobGroup()
    }
    
    // Aggregate results by hostname
    val assignments = taskResults.groupBy(_.hostname).mapValues(_.length).toMap
    logger.info(s"🔥 Pre-warm task distribution completed: ${assignments.map { case (h, c) => s"$h: $c tasks" }.mkString(", ")}")
    
    assignments
  }
  
  /**
   * Execute a single pre-warm task on an executor.
   */
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
      
      // Convert filters to query
      val query = convertFiltersToQuery(task.allFilters, splitSearchEngine)
      
      // Initiate async warmup using tantivy4java native API
      val splitSearcher = splitSearchEngine.getSplitSearcher()
      logger.info(s"🔥 Using native tantivy4java warmupQuery API for split: ${task.addAction.path}")
      val warmupFuture = splitSearcher.warmupQuery(query)
      
      // Store the future for later joining during query execution
      val futureKey = buildFutureKey(task.addAction.path, actualHostname, task.queryHash)
      warmupFutures.put(futureKey, warmupFuture)
      
      // Record that this host has accessed this split for locality tracking
      BroadcastSplitLocalityManager.recordSplitAccess(task.addAction.path, actualHostname)
      
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
  
  /**
   * Create a SplitCacheConfig from broadcast configuration.
   */
  private def createCacheConfigFromBroadcast(configMap: Map[String, String], tablePath: String): SplitCacheConfig = {
    def getConfig(key: String, default: String): String = configMap.getOrElse(key, default)
    def getConfigOption(key: String): Option[String] = configMap.get(key).filter(_.trim.nonEmpty)
    
    SplitCacheConfig(
      cacheName = {
        val configName = getConfig("spark.tantivy4spark.cache.name", "")
        if (configName.trim.nonEmpty) configName.trim
        else s"tantivy4spark-${tablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
      },
      maxCacheSize = getConfig("spark.tantivy4spark.cache.maxSize", "200000000").toLong,
      maxConcurrentLoads = getConfig("spark.tantivy4spark.cache.maxConcurrentLoads", "8").toInt,
      enableQueryCache = getConfig("spark.tantivy4spark.cache.queryCache", "true").toBoolean,
      awsAccessKey = getConfigOption("spark.tantivy4spark.aws.accessKey"),
      awsSecretKey = getConfigOption("spark.tantivy4spark.aws.secretKey"),
      awsSessionToken = getConfigOption("spark.tantivy4spark.aws.sessionToken"),
      awsRegion = getConfigOption("spark.tantivy4spark.aws.region"),
      awsEndpoint = getConfigOption("spark.tantivy4spark.s3.endpoint"),
      azureAccountName = getConfigOption("spark.tantivy4spark.azure.accountName"),
      azureAccountKey = getConfigOption("spark.tantivy4spark.azure.accountKey"),
      azureConnectionString = getConfigOption("spark.tantivy4spark.azure.connectionString"),
      azureEndpoint = getConfigOption("spark.tantivy4spark.azure.endpoint"),
      gcpProjectId = getConfigOption("spark.tantivy4spark.gcp.projectId"),
      gcpServiceAccountKey = getConfigOption("spark.tantivy4spark.gcp.serviceAccountKey"),
      gcpCredentialsFile = getConfigOption("spark.tantivy4spark.gcp.credentialsFile"),
      gcpEndpoint = getConfigOption("spark.tantivy4spark.gcp.endpoint")
    )
  }
  
  /**
   * Create a SplitSearchEngine for the given AddAction.
   */
  private def createSplitSearchEngine(addAction: AddAction, readSchema: StructType, cacheConfig: SplitCacheConfig): SplitSearchEngine = {
    val filePath = addAction.path
    
    // Normalize path for tantivy4java compatibility
    val actualPath = if (filePath.startsWith("s3a://") || filePath.startsWith("s3n://")) {
      filePath.replaceFirst("^s3[an]://", "s3://")
    } else {
      filePath
    }
    
    // Use footer offset optimization if available
    if (addAction.hasFooterOffsets && addAction.footerStartOffset.isDefined) {
      val splitMetadata = new com.tantivy4java.QuickwitSplit.SplitMetadata(
        actualPath,
        addAction.numRecords.getOrElse(0L),
        addAction.size,
        null, null,
        java.util.Collections.emptySet(),
        0L, 0,
        addAction.footerStartOffset.get,
        addAction.footerEndOffset.get,
        addAction.hotcacheStartOffset.get,
        addAction.hotcacheLength.get
      )
      SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig)
    } else {
      SplitSearchEngine.fromSplitFile(readSchema, actualPath, cacheConfig)
    }
  }
  
  /**
   * Convert filters to a tantivy4java Query object.
   */
  private def convertFiltersToQuery(allFilters: Array[Any], splitSearchEngine: SplitSearchEngine): Query = {
    if (allFilters.isEmpty) {
      Query.allQuery()
    } else {
      // Get schema field names for validation
      val splitSchema = splitSearchEngine.getSchema()
      val splitFieldNames = try {
        import scala.jdk.CollectionConverters._
        Some(splitSchema.getFieldNames().asScala.toSet)
      } catch {
        case _: Exception => None
      }
      
      FiltersToQueryConverter.convertToQuery(allFilters, splitSearchEngine, splitFieldNames)
    }
  }
  
  /**
   * Generate a hash for the query to uniquely identify warmup futures.
   */
  private def generateQueryHash(allFilters: Array[Any]): String = {
    val filterString = allFilters.map(_.toString).mkString("|")
    java.util.UUID.nameUUIDFromBytes(filterString.getBytes).toString.take(8)
  }
  
  /**
   * Build a unique key for warmup future storage.
   */
  private def buildFutureKey(splitPath: String, hostname: String, queryHash: String): String = {
    s"$splitPath|$hostname|$queryHash"
  }
  
  /**
   * Get current hostname for this JVM.
   */
  private def getCurrentHostname: String = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Exception => "unknown"
    }
  }
  
  /**
   * Get statistics about pre-warm operations.
   */
  def getPreWarmStats(queryHash: String): Option[PreWarmStats] = {
    warmupStats.get(queryHash)
  }
  
  /**
   * Clear all warmup futures and statistics (useful for testing).
   */
  def clearAll(): Unit = {
    warmupFutures.clear()
    warmupStats.clear()
    logger.info("Cleared all pre-warm state")
  }
}

/**
 * Represents a pre-warm task to be executed on an executor.
 */
case class PreWarmTask(
  addAction: AddAction,
  preferredHostname: String,
  readSchema: StructType,
  allFilters: Array[Any],
  queryHash: String
)

/**
 * Result of executing a pre-warm task.
 */
case class PreWarmTaskResult(
  splitPath: String,
  hostname: String,
  success: Boolean,
  errorMessage: Option[String]
)

/**
 * Overall result of the pre-warm phase.
 */
case class PreWarmResult(
  warmupInitiated: Boolean,
  warmupAssignments: Map[String, Int],
  totalWarmupsCreated: Int
)

/**
 * Statistics about pre-warm operations.
 */
case class PreWarmStats(
  totalSplits: Int,
  hostsInvolved: Int,
  warmupTasksCreated: Int,
  preWarmTimeMs: Long
)