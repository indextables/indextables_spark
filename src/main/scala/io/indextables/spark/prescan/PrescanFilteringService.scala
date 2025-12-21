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

package io.indextables.spark.prescan

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.core.{FiltersToQueryConverter, IndexTables4SparkOptions}
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.{ProtocolNormalizer, SplitMetadataFactory}
import io.indextables.tantivy4java.split.{SplitCacheManager, SplitInfo, SplitQuery, SplitMatchAllQuery}

import org.slf4j.LoggerFactory

/**
 * Service that performs prescan filtering on the Spark driver.
 *
 * Uses tantivy4java's prescan API to check if each split could possibly have results
 * by examining the FST (Finite State Transducer) term dictionary. This is 10-100x faster
 * than a full search because it only downloads term dictionaries, not posting lists.
 *
 * Key characteristics:
 * - Runs exclusively on the driver with configurable parallelism
 * - Conservative: never produces false negatives (may include extra splits, never excludes valid ones)
 * - Handles errors by conservatively including the split
 * - Supports caching - subsequent prescans benefit from cached term dictionaries
 */
object PrescanFilteringService {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Apply prescan filtering to candidate splits.
   *
   * @param addActions Candidate splits after data skipping
   * @param filters Pushed down Spark filters
   * @param indexQueryFilters Custom IndexQuery filters
   * @param config Prescan configuration
   * @param cacheConfig Split cache configuration (for credentials)
   * @param sparkSession Active Spark session
   * @param sparkSchema The Spark table schema (for proper query conversion)
   * @param options Read options (for field type configuration)
   * @param tablePath Base table path for resolving relative split paths
   * @return PrescanResult containing filtered AddActions and metrics
   */
  def applyPrescanFiltering(
    addActions: Seq[AddAction],
    filters: Array[Filter],
    indexQueryFilters: Array[Any],
    config: PrescanConfig,
    cacheConfig: SplitCacheConfig,
    sparkSession: SparkSession,
    sparkSchema: StructType,
    options: Option[CaseInsensitiveStringMap],
    tablePath: String
  ): PrescanResult = {

    val startTime = System.currentTimeMillis()
    val splitsBeforePrescan = addActions.length

    // Check if prescan should be applied
    if (!config.enabled) {
      logger.debug("Prescan filtering disabled")
      return PrescanResult(addActions, PrescanMetricsData.empty)
    }

    if (addActions.length < config.minSplitThreshold) {
      logger.warn(s"PRESCAN: SKIPPED (${addActions.length} splits below threshold ${config.minSplitThreshold}) | " +
        s"splits_considered=${addActions.length}, splits_eliminated=0, splits_remaining=${addActions.length}")
      return PrescanResult(addActions, PrescanMetricsData(
        splitsBeforePrescan = splitsBeforePrescan,
        splitsAfterPrescan = splitsBeforePrescan
      ))
    }

    if (filters.isEmpty && indexQueryFilters.isEmpty) {
      logger.debug("Skipping prescan: no filters to evaluate")
      return PrescanResult(addActions, PrescanMetricsData(
        splitsBeforePrescan = splitsBeforePrescan,
        splitsAfterPrescan = splitsBeforePrescan
      ))
    }

    logger.info(s"Starting prescan filtering for ${addActions.length} splits " +
      s"with concurrency ${config.maxConcurrency}")

    // Get the global cache manager (configured with credentials)
    val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

    // Build SplitInfo objects from AddActions
    val splitInfoPairs: Seq[(AddAction, Option[SplitInfo])] = addActions.map { action =>
      (action, buildSplitInfo(action, tablePath))
    }

    // Separate splits that can be prescanned from those that can't
    val (prescannableSplits, unprescannableSplits) = splitInfoPairs.partition(_._2.isDefined)

    if (unprescannableSplits.nonEmpty) {
      logger.warn(s"${unprescannableSplits.length} splits lack footer offsets, keeping them conservatively")
    }

    // Get docMappingJson from first action (all splits in same table share the same mapping)
    val docMappingJson = addActions.headOption.flatMap(_.docMappingJson).getOrElse {
      logger.warn(s"PRESCAN: SKIPPED (no docMappingJson found) | " +
        s"splits_considered=$splitsBeforePrescan, splits_eliminated=0, splits_remaining=$splitsBeforePrescan")
      return PrescanResult(addActions, PrescanMetricsData(
        splitsBeforePrescan = splitsBeforePrescan,
        splitsAfterPrescan = splitsBeforePrescan
      ))
    }

    // Convert Spark filters to prescan query using the IDENTICAL conversion path as regular scans.
    // Create a temporary SplitSearchEngine from the first split to get proper field type handling
    // and query parsing. This ensures prescan uses the exact same query as the actual scan.
    val firstAction = addActions.head

    // Resolve full split path: if the action path is relative, join with table path
    // Uses same pattern as IndexTables4SparkPartitions.isAbsolutePath
    val isAbsolute = firstAction.path.startsWith("/") ||
                     firstAction.path.contains("://") ||
                     firstAction.path.startsWith("file:")
    val rawSplitPath = if (isAbsolute) firstAction.path else new Path(tablePath, firstAction.path).toString
    // Normalize cloud paths for tantivy4java compatibility:
    // s3a:// -> s3://, abfss:// -> azure://, etc.
    val fullSplitPath = ProtocolNormalizer.normalizeAllProtocols(rawSplitPath)

    val splitQuery: SplitQuery = try {
      // Create SplitMetadata from the first AddAction
      val metadata = SplitMetadataFactory.fromAddAction(firstAction, tablePath)

      // Create a temporary SplitSearchEngine for query conversion
      val tempEngine = SplitSearchEngine.fromSplitFileWithMetadata(
        sparkSchema,
        fullSplitPath,
        metadata,
        cacheConfig,
        options.map(opts => new IndexTables4SparkOptions(opts))
      )

      try {
        // Use the IDENTICAL conversion path as regular scans
        // Combine filters and indexQueryFilters for unified conversion
        val allFilters: Array[Any] = filters.map(_.asInstanceOf[Any]) ++ indexQueryFilters
        FiltersToQueryConverter.convertToSplitQuery(allFilters, tempEngine, None, options)
      } finally {
        tempEngine.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"PRESCAN: FAILED (filter conversion error: ${e.getMessage}) | " +
          s"splits_considered=$splitsBeforePrescan, splits_eliminated=0, splits_remaining=$splitsBeforePrescan")
        return PrescanResult(addActions, PrescanMetricsData(
          splitsBeforePrescan = splitsBeforePrescan,
          splitsAfterPrescan = splitsBeforePrescan
        ))
    }

    if (splitQuery.isInstanceOf[SplitMatchAllQuery]) {
      logger.warn(s"PRESCAN: SKIPPED (query matches all documents) | " +
        s"splits_considered=$splitsBeforePrescan, splits_eliminated=0, splits_remaining=$splitsBeforePrescan")
      return PrescanResult(addActions, PrescanMetricsData(
        splitsBeforePrescan = splitsBeforePrescan,
        splitsAfterPrescan = splitsBeforePrescan
      ))
    }

    // Execute prescan with a single call - parallelism is handled by tantivy4java/Rust
    val prescanResults = prescanAllSplits(
      cacheManager,
      prescannableSplits.map { case (action, infoOpt) => (action, infoOpt.get) },
      docMappingJson,
      splitQuery
    )

    // Combine results: matching prescanned splits + unprescannable splits (kept conservatively)
    val matchingSplits = prescanResults.filter(_.couldHaveResults).map(_.action)
    val unprescannableActions = unprescannableSplits.map(_._1)
    val allMatchingSplits = matchingSplits ++ unprescannableActions

    val splitsAfterPrescan = allMatchingSplits.length
    val prescanTimeMs = System.currentTimeMillis() - startTime

    val eliminatedCount = splitsBeforePrescan - splitsAfterPrescan
    val eliminationPct = if (splitsBeforePrescan > 0)
      (eliminatedCount.toDouble / splitsBeforePrescan * 100).toInt else 0
    val errorCount = prescanResults.count(_.error.isDefined)

    // Always log prescan results at WARN level for visibility
    logger.warn(s"PRESCAN: SUCCESS | " +
      s"splits_considered=$splitsBeforePrescan, splits_eliminated=$eliminatedCount, " +
      s"splits_remaining=$splitsAfterPrescan ($eliminationPct% reduction), " +
      s"time_ms=$prescanTimeMs, errors=$errorCount")

    val metrics = PrescanMetricsData(
      splitsBeforePrescan = splitsBeforePrescan,
      splitsAfterPrescan = splitsAfterPrescan,
      prescanTimeMs = prescanTimeMs,
      cacheHits = prescanResults.count(_.cacheHit),
      cacheMisses = prescanResults.count(!_.cacheHit),
      errors = prescanResults.count(_.error.isDefined)
    )

    PrescanResult(allMatchingSplits, metrics)
  }

  /**
   * Build a SplitInfo from AddAction metadata.
   * Returns None if required footer offsets are missing.
   *
   * @param action AddAction with split metadata
   * @param tablePath Base table path for resolving relative split paths
   */
  private def buildSplitInfo(action: AddAction, tablePath: String): Option[SplitInfo] = {
    // Require both footerStartOffset and footerEndOffset
    (action.footerStartOffset, action.footerEndOffset) match {
      case (Some(footerStart), Some(footerEnd)) =>
        // Resolve full split path: if the action path is relative, join with table path
        val isAbsolute = action.path.startsWith("/") ||
                         action.path.contains("://") ||
                         action.path.startsWith("file:")
        val fullPath = if (isAbsolute) action.path else new Path(tablePath, action.path).toString

        // Normalize cloud paths for tantivy4java compatibility:
        // s3a:// -> s3://, abfss:// -> azure://, etc.
        val normalizedPath = ProtocolNormalizer.normalizeAllProtocols(fullPath)

        // Validate that footerEndOffset > footerStartOffset
        if (footerEnd <= footerStart) {
          logger.warn(s"Skipping prescan for ${action.path}: invalid footer offsets " +
            s"(footerEndOffset=$footerEnd must be > footerStartOffset=$footerStart)")
          None
        } else {
          // Use footerEndOffset as fileSize (it marks the end of the split file)
          Some(new SplitInfo(normalizedPath, footerStart, footerEnd))
        }

      case _ =>
        // Missing footer offsets - skip this split for prescan
        logger.debug(s"Skipping prescan for ${action.path}: missing footer offsets")
        None
    }
  }

  /**
   * Execute prescan for all splits with a single call.
   * Parallelism is handled internally by tantivy4java/Rust.
   */
  private def prescanAllSplits(
    cacheManager: SplitCacheManager,
    splitInfos: Seq[(AddAction, SplitInfo)],
    docMappingJson: String,
    query: SplitQuery
  ): Seq[PrescanSplitResult] = {

    if (splitInfos.isEmpty) {
      return Seq.empty
    }

    try {
      // Make a single call with all splits - Rust handles parallelism
      val allSplitInfos = splitInfos.map(_._2).asJava
      val prescanResults = cacheManager.prescanSplits(allSplitInfos, docMappingJson, query)

      // Map results back to corresponding AddActions
      val resultsList = prescanResults.asScala.toSeq

      if (resultsList.length != splitInfos.length) {
        logger.warn(s"Prescan returned ${resultsList.length} results for ${splitInfos.length} splits, " +
          "returning conservative results for missing splits")
      }

      splitInfos.zipWithIndex.map { case ((action, _), idx) =>
        if (idx < resultsList.length) {
          val result = resultsList(idx)
          val couldHave = result.couldHaveResults()
          val error = getPrescanError(result)

          // Log errors at warn level for troubleshooting
          if (error.isDefined) {
            logger.warn(s"Prescan returned non-SUCCESS for ${action.path}: " +
              s"status=${Try(result.getStatus).getOrElse("N/A")}, error=${error.getOrElse("none")}")
          }

          PrescanSplitResult(
            action,
            couldHaveResults = couldHave,
            cacheHit = isCacheHit(result),
            error = error
          )
        } else {
          // Missing result - keep split conservatively
          PrescanSplitResult(action, couldHaveResults = true, cacheHit = false,
            error = Some("Missing prescan result"))
        }
      }
    } catch {
      case e: Exception =>
        // On error, conservatively keep all splits
        logger.warn(s"Prescan error: ${e.getMessage}")
        splitInfos.map { case (action, _) =>
          PrescanSplitResult(action, couldHaveResults = true, cacheHit = false,
            error = Some(e.getMessage))
        }
    }
  }

  /**
   * Check if the prescan result came from cache.
   * This method safely checks for the wasCacheHit method which may not exist in all versions.
   */
  private def isCacheHit(result: io.indextables.tantivy4java.split.PrescanResult): Boolean = {
    Try {
      // Use reflection to safely call wasCacheHit if it exists
      val method = result.getClass.getMethod("wasCacheHit")
      method.invoke(result).asInstanceOf[Boolean]
    }.getOrElse(false)
  }

  /**
   * Get error message from prescan result if any.
   */
  private def getPrescanError(result: io.indextables.tantivy4java.split.PrescanResult): Option[String] = {
    Try {
      val status = result.getStatus
      if (status.toString != "SUCCESS") {
        Option(result.getErrorMessage)
      } else {
        None
      }
    }.getOrElse(None)
  }
}

/**
 * Result of prescan filtering for a single split.
 *
 * @param action The original AddAction
 * @param couldHaveResults Whether the split could potentially have matching results
 * @param cacheHit Whether the prescan data was already cached
 * @param error Optional error message if prescan failed
 */
case class PrescanSplitResult(
  action: AddAction,
  couldHaveResults: Boolean,
  cacheHit: Boolean,
  error: Option[String]
)

/**
 * Result of prescan filtering operation.
 *
 * @param filteredActions Splits that passed prescan (could have results)
 * @param metrics Collected metrics about the prescan operation
 */
case class PrescanResult(
  filteredActions: Seq[AddAction],
  metrics: PrescanMetricsData
)
