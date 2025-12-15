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

import io.indextables.spark.transaction.{AddAction, TransactionLog}
import org.slf4j.LoggerFactory

/**
 * Result of auto-indexing evaluation and execution.
 *
 * @param triggered
 *   Whether auto-indexing was triggered
 * @param reason
 *   Reason for the decision (triggered or skipped)
 * @param xrefsBuilt
 *   Number of XRefs built (0 if not triggered)
 * @param splitsIndexed
 *   Number of splits indexed (0 if not triggered)
 * @param durationMs
 *   Time taken for auto-indexing (0 if not triggered)
 * @param error
 *   Error message if auto-indexing failed
 */
case class XRefAutoIndexResult(
  triggered: Boolean,
  reason: String,
  xrefsBuilt: Int = 0,
  splitsIndexed: Int = 0,
  durationMs: Long = 0,
  error: Option[String] = None
)

/**
 * Automatic XRef indexing after commit operations.
 *
 * XRefAutoIndexer evaluates whether to build XRef indexes after data is committed to the table. This enables automatic
 * maintenance of XRef indexes without manual intervention.
 *
 * Auto-indexing triggers when:
 *   - Enabled in configuration (spark.indextables.xref.autoIndex.enabled)
 *   - Number of uncovered splits exceeds threshold (minUncoveredSplitsToTrigger)
 *   - Sufficient time has passed since last auto-index (minIntervalMs)
 *
 * Auto-indexing runs:
 *   - Synchronously by default (blocks until complete)
 *   - Can be configured to run asynchronously (spark.indextables.xref.autoIndex.async)
 *
 * @param transactionLog
 *   Transaction log for the table
 * @param config
 *   XRef configuration
 * @param sparkSession
 *   The Spark session
 */
class XRefAutoIndexer(
  transactionLog: TransactionLog,
  config: XRefConfig,
  sparkSession: SparkSession
) {

  private val logger = LoggerFactory.getLogger(classOf[XRefAutoIndexer])
  private val tablePath = transactionLog.getTablePath().toString

  // Track last auto-index time per table (in-memory, resets on restart)
  private var lastAutoIndexTime: Long = 0L

  /**
   * Evaluate and potentially trigger auto-indexing after a commit.
   *
   * This method should be called after new files are added to the transaction log. It evaluates whether auto-indexing
   * should run based on configuration and current table state.
   *
   * @param newlyAddedSplits
   *   Splits that were just added in the commit
   * @return
   *   Result indicating whether auto-indexing was triggered and outcome
   */
  def onCommit(newlyAddedSplits: Seq[AddAction]): XRefAutoIndexResult = {
    val startTime = System.currentTimeMillis()

    // Check if auto-indexing is enabled
    if (!config.autoIndex.enabled) {
      logger.debug("XRef auto-indexing disabled by configuration")
      return XRefAutoIndexResult(
        triggered = false,
        reason = "Auto-indexing disabled"
      )
    }

    // Check minimum interval since last auto-index
    val timeSinceLastAutoIndex = startTime - lastAutoIndexTime
    if (timeSinceLastAutoIndex < config.autoIndex.minIntervalMs) {
      logger.debug(s"Skipping auto-index: only ${timeSinceLastAutoIndex}ms since last run (min: ${config.autoIndex.minIntervalMs}ms)")
      return XRefAutoIndexResult(
        triggered = false,
        reason = s"Below minimum interval (${timeSinceLastAutoIndex}ms < ${config.autoIndex.minIntervalMs}ms)"
      )
    }

    // Get current state of splits and XRefs
    val allSplits = transactionLog.listFiles()
    val existingXRefs = transactionLog.listXRefs()

    // Calculate uncovered splits
    val coveredSplitFileNames = existingXRefs.flatMap(_.sourceSplitPaths).map(XRefStorageUtils.extractFileName).toSet
    val uncoveredSplits = allSplits.filterNot { split =>
      val fileName = XRefStorageUtils.extractFileName(split.path)
      coveredSplitFileNames.contains(fileName)
    }

    logger.debug(s"Auto-index evaluation: ${uncoveredSplits.size} uncovered splits out of ${allSplits.size} total")

    // Check if we have enough uncovered splits to trigger
    if (uncoveredSplits.size < config.autoIndex.minUncoveredSplitsToTrigger) {
      logger.debug(s"Not enough uncovered splits to trigger auto-index (${uncoveredSplits.size} < ${config.autoIndex.minUncoveredSplitsToTrigger})")
      return XRefAutoIndexResult(
        triggered = false,
        reason = s"Below threshold (${uncoveredSplits.size} < ${config.autoIndex.minUncoveredSplitsToTrigger} uncovered splits)"
      )
    }

    // Trigger auto-indexing
    logger.info(s"Triggering XRef auto-indexing: ${uncoveredSplits.size} uncovered splits")

    Try {
      executeAutoIndex(uncoveredSplits)
    } match {
      case Success(result) =>
        lastAutoIndexTime = System.currentTimeMillis()
        val duration = System.currentTimeMillis() - startTime
        logger.info(s"XRef auto-indexing completed: ${result.xrefsBuilt} XRefs built, ${result.splitsIndexed} splits indexed in ${duration}ms")
        result.copy(durationMs = duration)

      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"XRef auto-indexing failed: ${e.getMessage}", e)
        XRefAutoIndexResult(
          triggered = true,
          reason = "Auto-indexing failed",
          durationMs = duration,
          error = Some(e.getMessage)
        )
    }
  }

  /**
   * Execute auto-indexing for uncovered splits.
   *
   * @param uncoveredSplits
   *   Splits that need to be indexed
   * @return
   *   Result with build statistics
   */
  private def executeAutoIndex(uncoveredSplits: Seq[AddAction]): XRefAutoIndexResult = {
    val buildManager = new XRefBuildManager(
      tablePath = tablePath,
      transactionLog = transactionLog,
      config = config,
      sparkSession = sparkSession
    )

    // Build XRefs for uncovered splits
    val results = buildManager.buildCrossReferences(
      whereClause = None,
      forceRebuild = false,
      dryRun = false
    )

    // Count successful builds
    val successfulBuilds = results.filter(_.action == "built")
    val totalSplitsIndexed = successfulBuilds.map(_.sourceSplitsCount).sum

    XRefAutoIndexResult(
      triggered = true,
      reason = "Auto-indexing triggered",
      xrefsBuilt = successfulBuilds.size,
      splitsIndexed = totalSplitsIndexed
    )
  }

  /**
   * Force immediate auto-indexing regardless of thresholds.
   *
   * This can be called manually to trigger XRef building without waiting for thresholds.
   *
   * @return
   *   Result with build statistics
   */
  def forceAutoIndex(): XRefAutoIndexResult = {
    val startTime = System.currentTimeMillis()

    if (!config.autoIndex.enabled) {
      return XRefAutoIndexResult(
        triggered = false,
        reason = "Auto-indexing disabled (even for forced run)"
      )
    }

    logger.info("Forcing XRef auto-indexing")

    val allSplits = transactionLog.listFiles()
    val existingXRefs = transactionLog.listXRefs()

    val coveredSplitFileNames = existingXRefs.flatMap(_.sourceSplitPaths).map(XRefStorageUtils.extractFileName).toSet
    val uncoveredSplits = allSplits.filterNot { split =>
      coveredSplitFileNames.contains(XRefStorageUtils.extractFileName(split.path))
    }

    if (uncoveredSplits.isEmpty) {
      return XRefAutoIndexResult(
        triggered = false,
        reason = "No uncovered splits to index"
      )
    }

    Try {
      executeAutoIndex(uncoveredSplits)
    } match {
      case Success(result) =>
        lastAutoIndexTime = System.currentTimeMillis()
        val duration = System.currentTimeMillis() - startTime
        result.copy(durationMs = duration)

      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        XRefAutoIndexResult(
          triggered = true,
          reason = "Forced auto-indexing failed",
          durationMs = duration,
          error = Some(e.getMessage)
        )
    }
  }
}

object XRefAutoIndexer {

  /**
   * Create an XRefAutoIndexer for a transaction log.
   */
  def apply(transactionLog: TransactionLog, sparkSession: SparkSession): XRefAutoIndexer = {
    val config = XRefConfig.fromSparkSession(sparkSession)
    new XRefAutoIndexer(transactionLog, config, sparkSession)
  }

  /**
   * Create an XRefAutoIndexer with explicit config.
   */
  def apply(
    transactionLog: TransactionLog,
    config: XRefConfig,
    sparkSession: SparkSession
  ): XRefAutoIndexer =
    new XRefAutoIndexer(transactionLog, config, sparkSession)
}
