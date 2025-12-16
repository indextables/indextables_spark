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
 * @param mergedConfigMap
 *   Pre-merged config map with proper precedence: hadoop < spark < read options.
 *   This is passed to distributed XRef search for cloud credential handling.
 */
class XRefQueryRouter(
  transactionLog: TransactionLog,
  config: XRefConfig,
  sparkSession: SparkSession,
  mergedConfigMap: Map[String, String]
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

    // Combine filters and indexQueryFilters into a single array
    // IndexQueryFilters need to be converted to Filter type if they aren't already
    val indexQueryAsFilters: Seq[Filter] = indexQueryFilters.toSeq.collect {
      case f: Filter => f
      case iqf: io.indextables.spark.filters.IndexQueryFilter => iqf.asInstanceOf[Filter]
    }
    val allFilters: Array[Filter] = (filters.toSeq ++ indexQueryAsFilters).toArray

    // Check if we have any searchable filters
    if (allFilters.isEmpty) {
      logger.debug(s"No filters provided for XRef query (filters: ${filters.length}, indexQueryFilters: ${indexQueryFilters.length})")
      return XRefRoutingResult(
        candidateSplits = candidateSplits,
        skippedSplits = 0,
        xrefsQueried = 0,
        xrefQueryTimeMs = System.currentTimeMillis() - startTime,
        usedXRef = false,
        fallbackReason = Some("No filters provided")
      )
    }
    logger.debug(s"Using ${allFilters.length} filters for XRef query")

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
      queryXRefsForMatchingSplits(relevantXRefs, allFilters, config.query.timeoutMs)
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
   * Query XRefs to find which splits contain documents matching the filters.
   *
   * This method uses distributed search when beneficial (multiple XRefs, cluster has executors),
   * otherwise falls back to driver-side search for simplicity.
   *
   * @param xrefs
   *   XRefs to query
   * @param filters
   *   Spark filters from predicate pushdown
   * @param timeoutMs
   *   Query timeout in milliseconds
   * @return
   *   Set of split filenames that matched
   */
  private def queryXRefsForMatchingSplits(
    xrefs: Seq[AddXRefAction],
    filters: Array[Filter],
    timeoutMs: Int
  ): Set[String] = {

    logger.debug(s"Querying ${xrefs.size} XRefs with ${filters.length} filters")

    // Always use distributed XRef search for executor locality benefits
    logger.info(s"Using distributed XRef search across ${xrefs.size} XRefs")
    queryXRefsDistributed(xrefs, filters, timeoutMs)
  }

  /**
   * Query XRefs using distributed search on executors.
   *
   * This distributes XRef searches across the cluster with locality preferences,
   * ensuring searches run on nodes where XRef splits are cached.
   */
  private def queryXRefsDistributed(
    xrefs: Seq[AddXRefAction],
    filters: Array[Filter],
    timeoutMs: Int
  ): Set[String] = {
    // Prepare XRefs with their full paths
    val xrefsWithPaths = xrefs.map { xref =>
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(tablePath, xref.xrefId, config.storage.directory)
      (xrefFullPath, xref)
    }

    // Execute distributed search with merged config map
    val resultsByXRef = DistributedXRefSearcher.searchXRefsDistributed(
      xrefsWithPaths,
      filters,
      tablePath,
      timeoutMs,
      sparkSession,
      mergedConfigMap
    )

    // Flatten results from all XRefs
    resultsByXRef.values.flatten.toSet
  }
}

object XRefQueryRouter {

  /**
   * Create XRefQueryRouter with merged config map.
   *
   * @param transactionLog Transaction log for the table
   * @param mergedConfigMap Pre-merged config map with proper precedence: hadoop < spark < read options
   * @param sparkSession The Spark session
   */
  def apply(
    transactionLog: TransactionLog,
    mergedConfigMap: Map[String, String],
    sparkSession: SparkSession
  ): XRefQueryRouter = {
    val config = XRefConfig.fromSparkSession(sparkSession)
    new XRefQueryRouter(transactionLog, config, sparkSession, mergedConfigMap)
  }

  /**
   * Create XRefQueryRouter with explicit XRef config and merged config map.
   *
   * @param transactionLog Transaction log for the table
   * @param config XRef configuration
   * @param mergedConfigMap Pre-merged config map with proper precedence: hadoop < spark < read options
   * @param sparkSession The Spark session
   */
  def apply(
    transactionLog: TransactionLog,
    config: XRefConfig,
    mergedConfigMap: Map[String, String],
    sparkSession: SparkSession
  ): XRefQueryRouter =
    new XRefQueryRouter(transactionLog, config, sparkSession, mergedConfigMap)
}
