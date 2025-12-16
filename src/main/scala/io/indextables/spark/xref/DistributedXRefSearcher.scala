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

package io.indextables.spark.xref

import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter

import io.indextables.spark.storage.{BroadcastSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.AddXRefAction
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils, SplitMetadataFactory}
import io.indextables.tantivy4java.split.SplitQuery
import io.indextables.spark.core.FiltersToQueryConverter
import org.slf4j.LoggerFactory

/**
 * Partition for distributed XRef search operations.
 *
 * Each partition represents one XRef split to search, with preferred location
 * hints for data locality.
 */
private[xref] class XRefSearchPartition(
  val partitionId: Int,
  val xrefPath: String,
  val xref: AddXRefAction,
  val preferredHosts: Array[String]
) extends Partition with Serializable {
  override def index: Int = partitionId
}

/**
 * RDD for distributed XRef search operations.
 *
 * This RDD searches XRef splits on executors, honoring locality preferences
 * from the BroadcastSplitLocalityManager to ensure searches run on nodes
 * where XRef splits are cached.
 */
private[xref] class XRefSearchRDD(
  sc: SparkContext,
  xrefs: Seq[(String, AddXRefAction)], // (xrefPath, xref)
  filters: Array[Filter],
  tablePath: String,
  timeoutMs: Int,
  mergedConfigMap: Map[String, String] // Already contains properly merged AWS/Azure credentials
) extends RDD[(String, Seq[String])](sc, Nil) {

  private val logger = LoggerFactory.getLogger(classOf[XRefSearchRDD])

  // Broadcast the filter array to all executors
  private val broadcastFilters = sc.broadcast(filters)
  private val broadcastTablePath = sc.broadcast(tablePath)
  private val broadcastConfig = sc.broadcast(mergedConfigMap)

  override protected def getPartitions: Array[Partition] = {
    xrefs.zipWithIndex.map { case ((xrefPath, xref), idx) =>
      // Get preferred hosts from locality manager
      val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(xrefPath)
      logger.debug(s"XRef search partition $idx for ${xref.xrefId} with preferred hosts: ${preferredHosts.mkString(",")}")
      new XRefSearchPartition(idx, xrefPath, xref, preferredHosts)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[XRefSearchPartition].preferredHosts.toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(String, Seq[String])] = {
    val partition = split.asInstanceOf[XRefSearchPartition]
    val xrefPath = partition.xrefPath
    val xref = partition.xref
    val filters = broadcastFilters.value
    val tablePath = broadcastTablePath.value
    val configMap = broadcastConfig.value

    val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
    logger.info(s"Executing XRef search for ${xref.xrefId} on host $hostname")

    // Record that we're accessing this XRef split for locality tracking
    BroadcastSplitLocalityManager.recordSplitAccess(xrefPath, hostname)

    Try {
      executeXRefSearchOnExecutor(xrefPath, xref, filters, tablePath, configMap)
    } match {
      case Success(matchingSplits) =>
        logger.info(s"XRef ${xref.xrefId} search returned ${matchingSplits.size} matching splits")
        Iterator((xref.xrefId, matchingSplits))

      case Failure(e) =>
        logger.warn(s"XRef search failed for ${xref.xrefId} on host $hostname: ${e.getMessage}")
        // On failure, conservatively return all source splits
        val allSplits = xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
        Iterator((xref.xrefId, allSplits))
    }
  }

  private val XREF_URI_FIELD = "_xref_uri"

  /**
   * Execute XRef search on an executor.
   */
  private def executeXRefSearchOnExecutor(
    xrefPath: String,
    xref: AddXRefAction,
    filters: Array[Filter],
    tablePath: String,
    configMap: Map[String, String]
  ): Seq[String] = {

    // Get local path (downloads to local cache if enabled and not already cached)
    // The configMap already contains properly merged AWS/Azure credentials from the driver
    val localOrRemotePath = XRefLocalCache.getLocalPath(xrefPath, configMap)

    // Normalize the XRef path for tantivy4java
    val normalizedXRefPath = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(localOrRemotePath)

    // Create SplitCacheConfig from broadcast config
    val splitCacheConfig = ConfigUtils.createSplitCacheConfig(configMap, Some(tablePath))

    // Get the global cache manager
    val cacheManager = GlobalSplitCacheManager.getInstance(splitCacheConfig)

    // Create SplitMetadata from XRef action
    val splitMetadata = SplitMetadataFactory.fromXRefAction(xref)

    var splitSearcher: io.indextables.tantivy4java.split.SplitSearcher = null
    try {
      // Create searcher using the shared cache manager
      splitSearcher = cacheManager.createSplitSearcher(normalizedXRefPath, splitMetadata)

      // Convert Spark filters to SplitQuery
      val splitQuery = FiltersToQueryConverter.convertToSplitQuery(filters, splitSearcher)

      // Transform range queries to match-all for XRef searches
      val transformedQuery = transformRangeQueriesToMatchAll(splitQuery)

      // Search the XRef split
      val searchResult = splitSearcher.search(transformedQuery, xref.sourceSplitPaths.size)

      // Extract _xref_uri from each matching document
      val matchingUris = new scala.collection.mutable.ArrayBuffer[String]()
      try {
        for (hit <- searchResult.getHits.asScala) {
          val doc = splitSearcher.doc(hit.getDocAddress)
          try {
            val uriObj = doc.getFirst(XREF_URI_FIELD)
            if (uriObj != null) {
              matchingUris += uriObj.toString
            }
          } finally {
            doc.close()
          }
        }
      } finally {
        searchResult.close()
      }

      // Convert full URIs to filenames
      matchingUris.map(XRefStorageUtils.extractFileName).toSeq
    } finally {
      if (splitSearcher != null) {
        splitSearcher.close()
      }
    }
  }

  /**
   * Transform range queries to match-all queries for XRef searches.
   */
  private def transformRangeQueriesToMatchAll(query: SplitQuery): SplitQuery = {
    import io.indextables.tantivy4java.split.{SplitRangeQuery, SplitMatchAllQuery}

    query match {
      case _: SplitRangeQuery =>
        new SplitMatchAllQuery()
      case other =>
        other
    }
  }
}

/**
 * Distributed XRef search executor.
 *
 * This object provides distributed XRef search capabilities that run on executors
 * with locality preferences, using the existing BroadcastSplitLocalityManager
 * infrastructure for cache-aware scheduling.
 *
 * Key benefits:
 * 1. XRef searches run on executors where XRef splits are cached (locality)
 * 2. Multiple XRefs can be searched in parallel across the cluster
 * 3. Results are collected back to driver for partition planning
 * 4. Automatic fallback to all splits on search failure
 */
object DistributedXRefSearcher {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Search multiple XRefs in parallel across executors with locality preferences.
   *
   * @param xrefs XRefs to search with their full paths
   * @param filters Spark filters from predicate pushdown
   * @param tablePath Table path for config resolution
   * @param timeoutMs Query timeout
   * @param sparkSession The Spark session
   * @param configMap Pre-merged config map (includes read options with highest precedence).
   *                  This must be provided - it should include the properly merged configs
   *                  from hadoop < spark < read options.
   * @return Map of xrefId -> matching split filenames
   */
  def searchXRefsDistributed(
    xrefs: Seq[(String, AddXRefAction)],
    filters: Array[Filter],
    tablePath: String,
    timeoutMs: Int,
    sparkSession: SparkSession,
    configMap: Map[String, String]
  ): Map[String, Seq[String]] = {

    if (xrefs.isEmpty) {
      return Map.empty
    }

    // If test search override is set, use driver-side search with XRefSearcher
    // (test overrides don't propagate to executors)
    if (XRefSearcher.hasTestSearchOverride) {
      logger.debug("Test search override detected, using driver-side search")
      return xrefs.map { case (xrefPath, xref) =>
        val matchingSplits = XRefSearcher.searchSplits(
          xrefPath, xref, filters, timeoutMs, tablePath, sparkSession
        )
        xref.xrefId -> matchingSplits
      }.toMap
    }

    if (!XRefSearcher.isAvailable()) {
      logger.debug("XRef API not available, returning all source splits for each XRef")
      return xrefs.map { case (_, xref) =>
        xref.xrefId -> xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
      }.toMap
    }

    val sc = sparkSession.sparkContext

    // First update broadcast locality so we have current cache information
    try {
      BroadcastSplitLocalityManager.updateBroadcastLocality(sc)
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to update broadcast locality before XRef search: ${ex.getMessage}")
    }

    logger.debug(s"Using config map with ${configMap.size} entries for distributed XRef search")

    logger.info(s"Executing distributed XRef search across ${xrefs.size} XRefs")

    try {
      // Create and execute the XRef search RDD with properly merged configs
      val xrefSearchRDD = new XRefSearchRDD(
        sc,
        xrefs,
        filters,
        tablePath,
        timeoutMs,
        configMap
      )

      // Set job description for Spark UI
      sc.setJobGroup(
        "tantivy4spark-xref-search",
        s"XRef search: querying ${xrefs.size} cross-reference indexes",
        interruptOnCancel = false
      )

      try {
        val results = xrefSearchRDD.collect()
        logger.info(s"Distributed XRef search completed: ${results.length} XRefs processed")
        results.toMap
      } finally {
        sc.clearJobGroup()
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Distributed XRef search failed: ${ex.getMessage}", ex)
        // On failure, conservatively return all source splits for each XRef
        xrefs.map { case (_, xref) =>
          xref.xrefId -> xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
        }.toMap
    }
  }

  /**
   * Check if distributed XRef search should be used.
   *
   * Distributed search is beneficial when:
   * 1. Multiple XRefs need to be searched
   * 2. XRefs are large enough to benefit from parallelism
   * 3. Cluster has multiple executors
   */
  def shouldUseDistributedSearch(
    xrefs: Seq[AddXRefAction],
    sparkSession: SparkSession
  ): Boolean = {
    // Use distributed search if we have multiple XRefs or cluster has enough executors
    val numExecutors = sparkSession.sparkContext.getExecutorMemoryStatus.size
    xrefs.size > 1 || numExecutors > 1
  }
}
