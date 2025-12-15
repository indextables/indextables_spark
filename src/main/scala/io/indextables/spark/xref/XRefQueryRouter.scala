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

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.{AddAction, AddXRefAction, TransactionLog}
import org.slf4j.LoggerFactory

/**
 * Result of XRef query routing.
 *
 * @param candidateSplits
 *   Splits that may contain matching documents
 * @param skippedSplits
 *   Number of splits skipped by XRef pre-scan
 * @param xrefsQueried
 *   Number of XRefs consulted
 * @param xrefQueryTimeMs
 *   Time spent querying XRefs
 * @param usedXRef
 *   Whether XRef was actually used (vs. fallback to full scan)
 * @param fallbackReason
 *   Reason if XRef was not used
 */
case class XRefRoutingResult(
  candidateSplits: Seq[AddAction],
  skippedSplits: Int,
  xrefsQueried: Int,
  xrefQueryTimeMs: Long,
  usedXRef: Boolean,
  fallbackReason: Option[String] = None
)

/**
 * Routes queries through XRef indexes for fast split identification.
 *
 * The XRefQueryRouter pre-scans XRef indexes to identify which splits may contain documents matching the query. This
 * enables 10-100x faster split identification for selective queries by avoiding the need to open every data split.
 *
 * Query routing flow:
 *   1. Check if XRef usage is enabled and we have enough candidate splits
 *   2. Load available XRefs from transaction log
 *   3. For each relevant XRef, search for matching split filenames
 *   4. Filter candidate splits to only those that appear in XRef results
 *   5. Return filtered splits (or all splits if XRef query fails)
 *
 * @param transactionLog
 *   Transaction log for the table
 * @param config
 *   XRef configuration
 * @param sparkSession
 *   The Spark session
 */
class XRefQueryRouter(
  transactionLog: TransactionLog,
  config: XRefConfig,
  sparkSession: SparkSession
) {

  private val logger = LoggerFactory.getLogger(classOf[XRefQueryRouter])
  private val tablePath = transactionLog.getTablePath().toString

  /**
   * Route a query through XRef to filter candidate splits.
   *
   * @param candidateSplits
   *   Splits after partition pruning and data skipping
   * @param filters
   *   Pushed down filters (used to extract search terms)
   * @param indexQueryFilters
   *   IndexQuery filters if present
   * @return
   *   XRefRoutingResult with filtered splits
   */
  def routeQuery(
    candidateSplits: Seq[AddAction],
    filters: Array[Filter],
    indexQueryFilters: Array[Any] = Array.empty
  ): XRefRoutingResult = {
    val startTime = System.currentTimeMillis()

    logger.debug(s"XRef routeQuery called: ${candidateSplits.size} splits, ${filters.length} filters, ${indexQueryFilters.length} indexQueryFilters")
    filters.foreach(f => logger.trace(s"XRef filter: $f"))

    // Check if XRef query is enabled
    if (!config.query.enabled) {
      logger.debug("XRef query routing disabled by configuration")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = 0,
        usedXRef = false,
        fallbackReason = Some("XRef query disabled")
      )
    }

    // Check if we have enough splits to warrant XRef usage
    if (candidateSplits.size < config.query.minSplitsForXRef) {
      logger.debug(s"Not enough splits for XRef (${candidateSplits.size} < ${config.query.minSplitsForXRef})")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = System.currentTimeMillis() - startTime,
        usedXRef = false,
        fallbackReason = Some(s"Below threshold (${candidateSplits.size} < ${config.query.minSplitsForXRef})")
      )
    }

    // Extract search query from filters
    val searchQuery = extractSearchQuery(filters, indexQueryFilters)
    if (searchQuery.isEmpty) {
      logger.debug(s"No searchable query extracted from filters (filters: ${filters.length}, indexQueryFilters: ${indexQueryFilters.length})")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = System.currentTimeMillis() - startTime,
        usedXRef = false,
        fallbackReason = Some("No searchable query")
      )
    }
    logger.debug(s"Extracted search query: ${searchQuery.get}")

    // Get available XRefs from transaction log
    val availableXRefs = transactionLog.listXRefs()
    logger.debug(s"Available XRefs: ${availableXRefs.size}")
    if (availableXRefs.isEmpty) {
      logger.debug("No XRefs available for table")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = System.currentTimeMillis() - startTime,
        usedXRef = false,
        fallbackReason = Some("No XRefs available")
      )
    }

    // Find XRefs that cover candidate splits
    val candidateSplitFileNames = candidateSplits.map(s => XRefStorageUtils.extractFileName(s.path)).toSet
    logger.trace(s"Candidate split filenames: ${candidateSplitFileNames.mkString(", ")}")
    val relevantXRefs = findRelevantXRefs(availableXRefs, candidateSplitFileNames)
    logger.debug(s"Relevant XRefs: ${relevantXRefs.size}")

    if (relevantXRefs.isEmpty) {
      logger.debug("No XRefs cover the candidate splits")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = System.currentTimeMillis() - startTime,
        usedXRef = false,
        fallbackReason = Some("No relevant XRefs")
      )
    }

    // Identify which candidate splits are covered by XRefs vs uncovered
    val coveredSplitFileNames = relevantXRefs.flatMap { xref =>
      xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }.toSet
    logger.trace(s"Covered split filenames: ${coveredSplitFileNames.mkString(", ")}")

    val (coveredSplits, uncoveredSplits) = candidateSplits.partition { split =>
      val fileName = XRefStorageUtils.extractFileName(split.path)
      coveredSplitFileNames.contains(fileName)
    }
    logger.debug(s"Covered splits: ${coveredSplits.size}, Uncovered splits: ${uncoveredSplits.size}")

    logger.info(s"Querying ${relevantXRefs.size} XRef(s) to filter ${coveredSplits.size} covered splits " +
      s"(${uncoveredSplits.size} splits have no XRef coverage and will be included)")

    // Query XRefs to get matching split filenames
    Try {
      queryXRefsForMatchingSplits(relevantXRefs, searchQuery.get, config.query.timeoutMs)
    } match {
      case Success(matchingSplitFileNames) =>
        // Filter COVERED splits to only those that matched in XRef
        // UNCOVERED splits must always be included (we can't prove they don't match)
        val matchedCoveredSplits = coveredSplits.filter { split =>
          val fileName = XRefStorageUtils.extractFileName(split.path)
          matchingSplitFileNames.contains(fileName)
        }

        // Final result: matched covered splits + all uncovered splits
        val filteredSplits = matchedCoveredSplits ++ uncoveredSplits

        val skipped = coveredSplits.size - matchedCoveredSplits.size
        val duration = System.currentTimeMillis() - startTime

        logger.info(s"XRef routing complete: ${filteredSplits.size} splits remaining " +
          s"(${matchedCoveredSplits.size} matched + ${uncoveredSplits.size} uncovered), $skipped skipped, ${duration}ms")

        XRefRoutingResult(
          candidateSplits = filteredSplits,
          skippedSplits = skipped,
          xrefsQueried = relevantXRefs.size,
          xrefQueryTimeMs = duration,
          usedXRef = true
        )

      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.warn(s"XRef query failed, falling back to full scan: ${e.getMessage}")

        if (config.query.fallbackOnError) {
          XRefRoutingResult(
            candidateSplits = candidateSplits,
            skippedSplits = 0,
            xrefsQueried = relevantXRefs.size,
            xrefQueryTimeMs = duration,
            usedXRef = false,
            fallbackReason = Some(s"XRef query failed: ${e.getMessage}")
          )
        } else {
          throw e
        }
    }
  }

  /**
   * Extract a searchable query string from filters.
   *
   * This extracts terms from:
   *   - IndexQuery filters (highest priority)
   *   - EqualTo filters on text/string fields
   *   - StringContains, StringStartsWith filters
   */
  private def extractSearchQuery(filters: Array[Filter], indexQueryFilters: Array[Any]): Option[String] = {
    // First, try to extract from IndexQuery filters
    if (indexQueryFilters.nonEmpty) {
      // IndexQuery filters contain the actual query string
      val queryStrings = indexQueryFilters.flatMap { filter =>
        extractQueryFromIndexQueryFilter(filter)
      }
      if (queryStrings.nonEmpty) {
        return Some(queryStrings.mkString(" "))
      }
    }

    // Fall back to extracting from regular filters
    val queryTerms = filters.flatMap { filter =>
      extractQueryTermsFromFilter(filter)
    }

    if (queryTerms.nonEmpty) {
      Some(queryTerms.mkString(" "))
    } else {
      None
    }
  }

  /**
   * Extract query string from IndexQuery filter.
   */
  private def extractQueryFromIndexQueryFilter(filter: Any): Option[String] =
    // Handle IndexQueryExpression or similar
    // The actual implementation depends on how IndexQuery is represented
    filter match {
      case tuple: (String, String) @unchecked =>
        // (columnName, queryString) format
        Some(tuple._2)
      case _ =>
        // Try to extract via reflection or string parsing
        val filterStr = filter.toString
        if (filterStr.contains("indexquery")) {
          // Parse query from string representation
          val queryPattern = """'([^']+)'""".r
          queryPattern.findFirstMatchIn(filterStr).map(_.group(1))
        } else {
          None
        }
    }

  /**
   * Extract query terms from a standard Spark filter.
   */
  private def extractQueryTermsFromFilter(filter: Filter): Seq[String] =
    filter match {
      case org.apache.spark.sql.sources.EqualTo(_, value) if value.isInstanceOf[String] =>
        Seq(value.asInstanceOf[String])
      case org.apache.spark.sql.sources.StringContains(_, value) =>
        Seq(value)
      case org.apache.spark.sql.sources.StringStartsWith(_, value) =>
        Seq(value)
      case org.apache.spark.sql.sources.And(left, right) =>
        extractQueryTermsFromFilter(left) ++ extractQueryTermsFromFilter(right)
      case org.apache.spark.sql.sources.Or(left, right) =>
        extractQueryTermsFromFilter(left) ++ extractQueryTermsFromFilter(right)
      case _ =>
        Seq.empty
    }

  /**
   * Find XRefs that cover any of the candidate splits.
   *
   * An XRef is relevant if any of its source splits are in the candidate set.
   */
  private def findRelevantXRefs(
    xrefs: Seq[AddXRefAction],
    candidateSplitFileNames: Set[String]
  ): Seq[AddXRefAction] =
    xrefs.filter { xref =>
      val xrefSourceFileNames = xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName).toSet
      // XRef is relevant if it covers any candidate splits
      xrefSourceFileNames.intersect(candidateSplitFileNames).nonEmpty
    }

  /**
   * Query XRefs to find which splits contain documents matching the query.
   *
   * @param xrefs
   *   XRefs to query
   * @param query
   *   Search query string
   * @param timeoutMs
   *   Query timeout in milliseconds
   * @return
   *   Set of split filenames that matched
   */
  private def queryXRefsForMatchingSplits(
    xrefs: Seq[AddXRefAction],
    query: String,
    timeoutMs: Int
  ): Set[String] = {

    logger.debug(s"Querying ${xrefs.size} XRefs for query: $query")

    // Query each XRef and collect matching split filenames
    val matchingSplits = xrefs.flatMap { xref =>
      try {
        queryXRef(xref, query, timeoutMs)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to query XRef ${xref.xrefId}: ${e.getMessage}")
          if (config.query.fallbackOnError) {
            // On error, conservatively include all splits covered by this XRef
            xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
          } else {
            throw e
          }
      }
    }

    matchingSplits.toSet
  }

  /**
   * Query a single XRef for matching splits.
   *
   * This is the integration point with tantivy4java XRef search.
   *
   * @param xref
   *   The XRef to query
   * @param query
   *   Search query string
   * @param timeoutMs
   *   Query timeout
   * @return
   *   Sequence of split filenames that matched
   */
  private def queryXRef(xref: AddXRefAction, query: String, timeoutMs: Int): Seq[String] = {
    val xrefFullPath = XRefStorageUtils.getXRefFullPathString(tablePath, xref.xrefId, config.storage.directory)

    logger.debug(s"Querying XRef ${xref.xrefId} at $xrefFullPath")

    // TODO: Integrate with actual tantivy4java XRef search API
    // The expected API would be something like:
    //
    // val searcher = XRefSearcher.open(xrefFullPath, xref.footerStartOffset, xref.footerEndOffset)
    // try {
    //   val results = searcher.searchSplits(query, timeoutMs)
    //   results.map(XRefStorageUtils.extractFileName)
    // } finally {
    //   searcher.close()
    // }

    // Check if XRef API is available
    if (!XRefSearcher.isAvailable()) {
      logger.debug("XRef search API not available, conservatively including all source splits")
      // Conservatively return all source splits
      return xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }

    // Use XRefSearcher to query
    XRefSearcher.searchSplits(xrefFullPath, xref, query, timeoutMs, tablePath, sparkSession)
  }
}

object XRefQueryRouter {

  /**
   * Create XRefQueryRouter for a transaction log.
   */
  def apply(transactionLog: TransactionLog, sparkSession: SparkSession): XRefQueryRouter = {
    val config = XRefConfig.fromSparkSession(sparkSession)
    new XRefQueryRouter(transactionLog, config, sparkSession)
  }

  /**
   * Create XRefQueryRouter with explicit config.
   */
  def apply(
    transactionLog: TransactionLog,
    config: XRefConfig,
    sparkSession: SparkSession
  ): XRefQueryRouter =
    new XRefQueryRouter(transactionLog, config, sparkSession)
}
