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
import io.indextables.spark.util.{ConfigUtils, XRefMetadataFactory}
import io.indextables.tantivy4java.xref.{XRefSearcher => TantivyXRefSearcher}
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

  /**
   * Execute the actual XRef search using the new FuseXRef API.
   *
   * FuseXRef uses Binary Fuse8 filters for fast query routing. This is much more
   * efficient than the previous Tantivy-index-based approach:
   * - ~70% smaller file sizes
   * - ~5x faster builds
   * - ~5x faster queries
   * - Range queries are automatically handled (returns hasUnevaluatedClauses=true)
   *
   * @param xrefPath
   *   Full path to the XRef split
   * @param xref
   *   XRef action containing metadata
   * @param filters
   *   Spark filters from predicate pushdown
   * @param timeoutMs
   *   Timeout (not currently used)
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

    logger.debug(s"Executing FuseXRef search with ${filters.length} filters on XRef at $xrefPath")

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

    // Create XRefMetadata from XRef action using shared factory
    val xrefMetadata = XRefMetadataFactory.fromAddXRefAction(xref)

    var xrefSearcher: TantivyXRefSearcher = null
    try {
      // Open FuseXRef using the new XRefSearcher API
      xrefSearcher = TantivyXRefSearcher.open(cacheManager, normalizedXRefPath, xrefMetadata)

      // Convert Spark filters to query string for XRefSearcher
      // XRefSearcher.search(String, int) parses the query using the stored schema
      val queryString = FiltersToQueryConverter.convertToQueryString(filters)

      logger.debug(s"Converted ${filters.length} filters to query string: $queryString")

      // Search the XRef - FuseXRef handles range query transformation internally
      val searchResult = xrefSearcher.search(queryString, xref.sourceSplitPaths.size)

      if (searchResult.hasUnevaluatedClauses) {
        logger.debug("Query contained unevaluated clauses (range/wildcard) - results may include extra splits")
      }

      // Get matching splits directly from result (no need to extract from documents)
      val matchingUris = searchResult.getMatchingSplits.asScala.map(_.getUri)

      logger.debug(s"FuseXRef search returned ${matchingUris.size} matching splits out of ${xref.sourceSplitPaths.size}")

      // Convert full URIs to filenames
      matchingUris.map(XRefStorageUtils.extractFileName).toSeq
    } finally {
      if (xrefSearcher != null) {
        xrefSearcher.close()
      }
    }
  }

}
