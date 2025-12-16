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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter

import io.indextables.spark.core.FiltersToQueryConverter
import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.transaction.AddXRefAction
import io.indextables.spark.util.{ConfigUtils, SplitMetadataFactory}
import io.indextables.tantivy4java.split.SplitQuery
import org.slf4j.LoggerFactory

/**
 * Wrapper for tantivy4java XRef search functionality.
 *
 * This object provides a bridge between the Spark-level XRef query routing and the low-level tantivy4java XRef search
 * API. It handles:
 *   - Checking XRef API availability
 *   - Opening XRef splits for search
 *   - Executing term-based queries to identify matching source splits
 *   - Managing resources and timeouts
 *
 * The XRef search returns split filenames that contain documents matching the query, enabling efficient query routing
 * that skips splits that cannot contain matching results.
 */
object XRefSearcher {

  private val logger = LoggerFactory.getLogger(getClass)

  // Flag to track if we've already checked for API availability
  @volatile private var availabilityChecked: Boolean = false
  @volatile private var xrefApiAvailable: Boolean = false

  /**
   * Test hook: Optional search override function for testing.
   *
   * When set, this function is called instead of the normal search path, allowing tests to simulate XRef search
   * behavior without requiring the actual tantivy4java XRef API.
   *
   * The function receives (xref, query, actualSourceSplits) and returns the matching split filenames.
   *
   * @note
   *   This is intended for testing only. Clear with clearTestSearchOverride() after each test.
   */
  @volatile private var testSearchOverride: Option[(AddXRefAction, String, Seq[String]) => Seq[String]] = None

  /**
   * Set a test search override function.
   *
   * When set, searchSplits will use this function instead of the normal search path.
   *
   * @param searchFn
   *   Function that takes (xref, query, actualSourceSplitFileNames) and returns matching split filenames
   */
  def setTestSearchOverride(searchFn: (AddXRefAction, String, Seq[String]) => Seq[String]): Unit =
    synchronized {
      testSearchOverride = Some(searchFn)
      logger.info("Test search override set")
    }

  /**
   * Clear the test search override.
   */
  def clearTestSearchOverride(): Unit =
    synchronized {
      testSearchOverride = None
      logger.info("Test search override cleared")
    }

  /**
   * Check if a test search override is set.
   */
  def hasTestSearchOverride: Boolean = testSearchOverride.isDefined

  /**
   * Check if the XRef search API is available.
   *
   * This checks whether the tantivy4java XRef classes are on the classpath and functional. The check is performed once
   * and cached for subsequent calls.
   *
   * Note: When a test override is set, this always returns true to allow testing of the XRef routing code path.
   *
   * @return
   *   true if XRef search is available (or test override is set), false otherwise
   */
  def isAvailable(): Boolean = {
    // If test override is set, always return true to allow testing
    if (testSearchOverride.isDefined) {
      return true
    }

    if (!availabilityChecked) {
      synchronized {
        if (!availabilityChecked) {
          xrefApiAvailable = checkXRefApiAvailability()
          availabilityChecked = true
          if (xrefApiAvailable) {
            logger.info("XRef search API is available")
          } else {
            logger.info("XRef search API is not available - XRef queries will return all source splits")
          }
        }
      }
    }
    xrefApiAvailable
  }

  /**
   * Reset availability check (for testing).
   */
  def resetAvailabilityCheck(): Unit =
    synchronized {
      availabilityChecked = false
      xrefApiAvailable = false
    }

  /**
   * Search an XRef for splits containing documents matching the filters.
   *
   * @param xrefPath
   *   Full path to the XRef split file
   * @param xref
   *   XRef action containing metadata (footer offsets, source splits)
   * @param filters
   *   Spark filters from predicate pushdown
   * @param timeoutMs
   *   Query timeout in milliseconds
   * @param tablePath
   *   Path to the table (for resolving relative paths)
   * @param sparkSession
   *   The Spark session
   * @return
   *   Sequence of split filenames that may contain matching documents
   */
  def searchSplits(
    xrefPath: String,
    xref: AddXRefAction,
    filters: Array[Filter],
    timeoutMs: Int,
    tablePath: String,
    sparkSession: SparkSession
  ): Seq[String] = {

    logger.debug(s"Searching XRef ${xref.xrefId} with ${filters.length} filters")
    filters.foreach(f => logger.trace(s"XRef filter: $f"))

    // Check for test override first
    testSearchOverride.foreach { searchFn =>
      val actualSourceSplitFileNames = xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
      logger.debug(s"Using test search override for XRef ${xref.xrefId}")
      // For test override, convert filters to simple query string for compatibility
      val queryStr = filtersToQueryString(filters)
      val result = searchFn(xref, queryStr, actualSourceSplitFileNames)
      logger.debug(s"Test search override returned ${result.size} matching splits")
      return result
    }

    if (!isAvailable()) {
      logger.debug("XRef API not available, returning all source splits")
      return xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }

    Try {
      executeXRefSearch(xrefPath, xref, filters, timeoutMs, tablePath, sparkSession)
    } match {
      case Success(matchingSplits) =>
        logger.debug(s"XRef search returned ${matchingSplits.size} matching splits")
        matchingSplits

      case Failure(e) =>
        logger.warn(s"XRef search failed for ${xref.xrefId}: ${e.getMessage}")
        // On failure, conservatively return all source splits
        xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }
  }

  /**
   * Convert filters to a simple query string for test override compatibility.
   */
  private def filtersToQueryString(filters: Array[Filter]): String = {
    import org.apache.spark.sql.sources._
    filters.flatMap {
      case EqualTo(attr, value) => Some(s"$attr:$value")
      case StringContains(attr, value) => Some(s"$attr:*$value*")
      case StringStartsWith(attr, value) => Some(s"$attr:$value*")
      case _ => None
    }.mkString(" ")
  }

  /**
   * Check if the tantivy4java XRef API classes are available.
   */
  private def checkXRefApiAvailability(): Boolean =
    Try {
      // Check if the XRef-specific classes exist
      Class.forName("io.indextables.tantivy4java.xref.XRefSplit")
      true
    } match {
      case Success(available) => available
      case Failure(_)         => false
    }

  /** XRef field name that stores the source split URI */
  private val XREF_URI_FIELD = "_xref_uri"

  /**
   * Execute the actual XRef search using GlobalSplitCacheManager.
   *
   * This uses shared infrastructure from ConfigUtils for cache configuration,
   * while directly using GlobalSplitCacheManager for searcher creation.
   * XRef splits require a simpler validation path than regular data splits
   * since they are merged term dictionaries without full docMappingJson.
   *
   * XRef splits are regular Quickwit splits that can be searched with SplitSearcher.
   * Each document in the XRef represents one source split, and the _xref_uri field
   * contains the source split path.
   *
   * @param xrefPath
   *   Full path to the XRef split
   * @param xref
   *   XRef metadata
   * @param filters
   *   Spark filters from predicate pushdown
   * @param timeoutMs
   *   Timeout (not currently used - tantivy4java doesn't support query timeouts)
   * @param tablePath
   *   Table path
   * @param sparkSession
   *   Spark session for file system access
   * @return
   *   Matching split filenames
   */
  private def executeXRefSearch(
    xrefPath: String,
    xref: AddXRefAction,
    filters: Array[Filter],
    timeoutMs: Int,
    tablePath: String,
    sparkSession: SparkSession
  ): Seq[String] = {
    import scala.jdk.CollectionConverters._

    logger.debug(s"Executing XRef search with ${filters.length} filters on XRef at $xrefPath")

    // Normalize the XRef path for tantivy4java (s3a:// -> s3://, abfss:// -> azure://)
    val normalizedXRefPath = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(xrefPath)

    // Create SplitCacheConfig using shared ConfigUtils (handles credential propagation)
    val sparkConf = sparkSession.sparkContext.getConf
    val splitCacheConfig = ConfigUtils.createSplitCacheConfig(
      sparkConf.getAll.toMap,
      Some(tablePath)
    )

    // Get the global cache manager with properly configured credentials
    val cacheManager = GlobalSplitCacheManager.getInstance(splitCacheConfig)

    // Create SplitMetadata from XRef action using shared factory
    // This includes docMappingJson which is required for proper tokenization of query terms
    val splitMetadata = SplitMetadataFactory.fromXRefAction(xref)

    var splitSearcher: io.indextables.tantivy4java.split.SplitSearcher = null
    try {
      // Create searcher using the shared cache manager
      splitSearcher = cacheManager.createSplitSearcher(normalizedXRefPath, splitMetadata)

      // Convert Spark filters to SplitQuery
      val splitQuery = FiltersToQueryConverter.convertToSplitQuery(filters, splitSearcher)

      // Transform range queries to match-all for XRef searches
      // (XRef splits may not have fast fields for range query evaluation)
      val transformedQuery = transformRangeQueriesToMatchAll(splitQuery)

      logger.debug(s"Converted ${filters.length} filters to SplitQuery: ${transformedQuery.getClass.getSimpleName}")

      // Search the XRef split - each hit is an XRef document representing a source split
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

      logger.debug(s"XRef search returned ${matchingUris.size} matching splits out of ${xref.sourceSplitPaths.size}")

      // Convert full URIs to filenames
      matchingUris.map(XRefStorageUtils.extractFileName).toSeq
    } finally {
      if (splitSearcher != null) {
        splitSearcher.close()
      }
    }
  }

  /**
   * Transform range queries to match-all queries.
   *
   * XRef splits may not have fast fields (columnar data), so range queries cannot be evaluated.
   * To ensure conservative results (never miss a split that might contain matching documents),
   * range queries are transformed to match-all.
   *
   * This transformation is applied after FiltersToQueryConverter processes filters.
   * The converter may have already filtered out unsupported range queries, but this
   * provides an additional safety net for XRef-specific query handling.
   */
  private def transformRangeQueriesToMatchAll(query: SplitQuery): SplitQuery = {
    import io.indextables.tantivy4java.split.{SplitRangeQuery, SplitMatchAllQuery}

    query match {
      case _: SplitRangeQuery =>
        // Transform range query to match-all for conservative XRef results
        logger.debug("Transforming range query to match-all for XRef search")
        new SplitMatchAllQuery()

      case other =>
        // Return other query types unchanged
        // Note: We don't recurse into boolean queries here because
        // FiltersToQueryConverter should already handle filter decomposition
        other
    }
  }

}
