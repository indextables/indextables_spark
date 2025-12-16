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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.{AddAction, AddXRefAction, PartitionPredicateUtils, RemoveXRefAction, TransactionLog}
import org.slf4j.LoggerFactory

/**
 * Result of an XRef build/rebuild operation.
 */
case class XRefBuildResult(
  tablePath: String,
  action: String,               // "created", "rebuilt", "unchanged", "error"
  xrefPath: Option[String],
  sourceSplitsCount: Int,
  totalTerms: Long,
  xrefSizeBytes: Long,
  buildDurationMs: Long,
  errorMessage: Option[String] = None
)

/**
 * Plan for XRef rebuild operations.
 */
case class XRefRebuildPlan(
  xrefsToRemove: Seq[AddXRefAction],
  splitsToIndex: Seq[AddAction],
  existingValidXRefs: Seq[AddXRefAction],
  reason: String
)

/**
 * Manager for building and maintaining cross-reference (XRef) splits.
 *
 * XRef splits consolidate term dictionaries from multiple source splits into lightweight indexes that enable fast query
 * routing. This manager handles:
 *   - Evaluating when XRef rebuild is needed
 *   - Orchestrating distributed XRef builds
 *   - Atomic transaction log updates
 *   - Integration with PURGE for cleanup
 *
 * @param tablePath
 *   The table root path
 * @param transactionLog
 *   The transaction log for this table
 * @param config
 *   XRef configuration
 * @param sparkSession
 *   The Spark session
 * @param overrideConfigMap
 *   Optional pre-merged config map with proper precedence (hadoop < spark < write options).
 *   When provided (e.g., during write operations), this is passed to DistributedXRefBuilder.
 *   When None, DistributedXRefBuilder will extract configs from hadoop + spark.
 */
class XRefBuildManager(
  tablePath: String,
  transactionLog: TransactionLog,
  config: XRefConfig,
  sparkSession: SparkSession,
  overrideConfigMap: Option[Map[String, String]] = None
) {

  private val logger = LoggerFactory.getLogger(classOf[XRefBuildManager])
  private val tablePathObj = new Path(tablePath)

  /**
   * Build cross-references for the table.
   *
   * This is the main entry point for building XRef splits. It evaluates the current state, determines what needs to be
   * built/rebuilt, and orchestrates the build process.
   *
   * @param whereClause
   *   Optional partition filter (for partition-specific builds)
   * @param forceRebuild
   *   Force rebuild even if XRef is valid
   * @param dryRun
   *   Preview mode - don't actually build
   * @return
   *   Sequence of build results
   */
  def buildCrossReferences(
    whereClause: Option[Expression] = None,
    forceRebuild: Boolean = false,
    dryRun: Boolean = false
  ): Seq[XRefBuildResult] = {
    logger.info(s"Building cross-references for table: $tablePath (forceRebuild=$forceRebuild, dryRun=$dryRun)")

    // Step 1: Get current state from transaction log
    val allActiveSplits = transactionLog.listFiles()
    val existingXRefs = transactionLog.listXRefs()

    // Step 1a: Apply WHERE clause filter if provided
    val activeSplits = whereClause match {
      case Some(predicate) =>
        val partitionColumns = transactionLog.getPartitionColumns()
        if (partitionColumns.isEmpty) {
          throw new IllegalArgumentException(
            "WHERE clause not supported for non-partitioned tables"
          )
        }
        val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns)
        PartitionPredicateUtils.validatePartitionColumnReferences(predicate, partitionSchema)
        val filtered = PartitionPredicateUtils.filterAddActionsByPredicates(
          allActiveSplits, partitionSchema, Seq(predicate)
        )
        logger.info(s"WHERE clause filtered ${allActiveSplits.size} splits to ${filtered.size}")
        filtered
      case None =>
        allActiveSplits
    }

    logger.info(s"Table state: ${activeSplits.size} active splits, ${existingXRefs.size} existing XRefs")

    // Step 2: Check if we have enough splits to warrant XRef
    if (activeSplits.size < config.autoIndex.minSplitsToTrigger) {
      logger.info(s"Not enough splits (${activeSplits.size} < ${config.autoIndex.minSplitsToTrigger}) - skipping XRef build")
      return Seq(XRefBuildResult(
        tablePath = tablePath,
        action = "skipped",
        xrefPath = None,
        sourceSplitsCount = activeSplits.size,
        totalTerms = 0,
        xrefSizeBytes = 0,
        buildDurationMs = 0,
        errorMessage = Some(s"Not enough splits (${activeSplits.size} < ${config.autoIndex.minSplitsToTrigger})")
      ))
    }

    // Step 3: Evaluate what needs to be built/rebuilt
    val rebuildPlan = evaluateXRefRebuild(existingXRefs, activeSplits, forceRebuild)

    if (rebuildPlan.splitsToIndex.isEmpty && rebuildPlan.xrefsToRemove.isEmpty) {
      logger.info("No XRef changes needed - all splits are covered by valid XRefs")
      return Seq(XRefBuildResult(
        tablePath = tablePath,
        action = "unchanged",
        xrefPath = rebuildPlan.existingValidXRefs.headOption.map(_.path),
        sourceSplitsCount = activeSplits.size,
        totalTerms = rebuildPlan.existingValidXRefs.map(_.totalTerms).sum,
        xrefSizeBytes = rebuildPlan.existingValidXRefs.map(_.size).sum,
        buildDurationMs = 0
      ))
    }

    logger.info(s"Rebuild plan: remove ${rebuildPlan.xrefsToRemove.size} XRefs, index ${rebuildPlan.splitsToIndex.size} splits")
    logger.info(s"Rebuild reason: ${rebuildPlan.reason}")

    if (dryRun) {
      logger.info("Dry run mode - not executing build")
      return Seq(XRefBuildResult(
        tablePath = tablePath,
        action = "would_rebuild",
        xrefPath = None,
        sourceSplitsCount = rebuildPlan.splitsToIndex.size,
        totalTerms = 0,
        xrefSizeBytes = 0,
        buildDurationMs = 0,
        errorMessage = Some(s"Dry run: would rebuild with ${rebuildPlan.splitsToIndex.size} splits")
      ))
    }

    // Step 4: Execute the build
    executeBuild(rebuildPlan)
  }

  /**
   * Evaluate what XRef rebuild operations are needed.
   *
   * This implements the rebuild algorithm from the design document:
   *   1. Identify stale XRefs (reference non-existent splits)
   *   2. Identify uncovered splits (not in any valid XRef)
   *   3. Determine optimal rebuild strategy
   */
  private def evaluateXRefRebuild(
    existingXRefs: Seq[AddXRefAction],
    activeSplits: Seq[AddAction],
    forceRebuild: Boolean
  ): XRefRebuildPlan = {

    val activeSplitPaths = activeSplits.map(_.path).toSet
    val activeSplitFileNames = activeSplits.map(s => XRefStorageUtils.extractFileName(s.path)).toSet

    // Step 1: Identify stale XRefs (reference non-existent splits)
    val staleXRefs = existingXRefs.filter { xref =>
      xref.sourceSplitPaths.exists { sourcePath =>
        val fileName = XRefStorageUtils.extractFileName(sourcePath)
        !activeSplitFileNames.contains(fileName)
      }
    }

    if (staleXRefs.nonEmpty) {
      logger.info(s"Found ${staleXRefs.size} stale XRefs referencing removed splits")
    }

    // Step 2: Identify valid (non-stale) XRefs
    val validXRefs = existingXRefs.filterNot(staleXRefs.contains)

    // Step 3: Identify splits covered by valid XRefs
    val coveredSplitFileNames = validXRefs
      .flatMap(_.sourceSplitPaths)
      .map(XRefStorageUtils.extractFileName)
      .toSet

    // Step 4: Identify uncovered splits
    val uncoveredSplits = activeSplits.filterNot { split =>
      coveredSplitFileNames.contains(XRefStorageUtils.extractFileName(split.path))
    }

    if (uncoveredSplits.nonEmpty) {
      logger.info(s"Found ${uncoveredSplits.size} splits not covered by any XRef")
    }

    // Step 5: Determine rebuild strategy
    if (forceRebuild) {
      // Force rebuild: remove all XRefs and rebuild from scratch
      XRefRebuildPlan(
        xrefsToRemove = existingXRefs,
        splitsToIndex = activeSplits,
        existingValidXRefs = Seq.empty,
        reason = "force_rebuild"
      )
    } else if (staleXRefs.nonEmpty && config.autoIndex.rebuildOnSourceChange) {
      // Stale XRefs: rebuild with all active splits
      XRefRebuildPlan(
        xrefsToRemove = staleXRefs,
        splitsToIndex = activeSplits, // Rebuild with all splits
        existingValidXRefs = validXRefs,
        reason = "source_splits_changed"
      )
    } else if (uncoveredSplits.nonEmpty) {
      // Uncovered splits: check if we can add them to existing XRef or need new one
      val canExpandExisting = validXRefs.exists { xref =>
        xref.sourceSplitCount + uncoveredSplits.size <= config.build.maxSourceSplits
      }

      if (canExpandExisting && validXRefs.nonEmpty) {
        // Rebuild existing XRef to include uncovered splits
        XRefRebuildPlan(
          xrefsToRemove = validXRefs, // Remove to rebuild
          splitsToIndex = activeSplits, // Include all active splits
          existingValidXRefs = Seq.empty,
          reason = "expand_existing_xref"
        )
      } else if (validXRefs.isEmpty) {
        // No existing XRefs, build new one
        XRefRebuildPlan(
          xrefsToRemove = Seq.empty,
          splitsToIndex = activeSplits,
          existingValidXRefs = Seq.empty,
          reason = "initial_build"
        )
      } else {
        // Create additional XRef for uncovered splits
        XRefRebuildPlan(
          xrefsToRemove = Seq.empty,
          splitsToIndex = uncoveredSplits,
          existingValidXRefs = validXRefs,
          reason = "add_new_xref_for_uncovered"
        )
      }
    } else {
      // No changes needed
      XRefRebuildPlan(
        xrefsToRemove = Seq.empty,
        splitsToIndex = Seq.empty,
        existingValidXRefs = validXRefs,
        reason = "no_changes_needed"
      )
    }
  }

  /**
   * Execute the XRef build based on the rebuild plan.
   *
   * When building multiple XRefs, uses parallel batch execution with atomic commits per batch.
   * Each batch contains up to `batchSize` XRefs (default: defaultParallelism), matching the
   * MERGE SPLITS command's transaction granularity for consistent behavior.
   */
  private def executeBuild(plan: XRefRebuildPlan): Seq[XRefBuildResult] = {
    val startTime = System.currentTimeMillis()

    try {
      // Group splits into chunks respecting maxSourceSplits limit
      val allSplitGroups = plan.splitsToIndex.grouped(config.build.maxSourceSplits).toSeq

      // Apply maxXRefsPerRun limit if configured
      val (splitGroups, skippedGroups) = config.build.maxXRefsPerRun match {
        case Some(limit) if allSplitGroups.size > limit =>
          val (toProcess, toSkip) = allSplitGroups.splitAt(limit)
          val skippedSplits = toSkip.flatten.size
          logger.info(s"Limiting build to $limit XRef(s) out of ${allSplitGroups.size} total " +
            s"(skipping $skippedSplits splits for subsequent runs)")
          (toProcess, toSkip)
        case _ =>
          (allSplitGroups, Seq.empty)
      }

      val totalSplitsToIndex = splitGroups.flatten.size
      logger.info(s"Building ${splitGroups.size} XRef(s) for $totalSplitsToIndex splits" +
        (if (skippedGroups.nonEmpty) s" (${skippedGroups.size} XRefs deferred)" else ""))

      // Prepare XRef specifications for batch build
      val xrefSpecs = splitGroups.map { splits =>
        val xrefId = XRefStorageUtils.generateXRefId()
        val xrefFullPath = XRefStorageUtils.getXRefFullPathString(tablePath, xrefId, config.storage.directory)
        (xrefId, xrefFullPath, splits)
      }

      // Get batch size for transaction commits (default: defaultParallelism, matching MERGE SPLITS)
      val defaultParallelism = sparkSession.sparkContext.defaultParallelism
      val transactionBatchSize = config.build.batchSize.getOrElse(defaultParallelism)

      // Split XRef specs into transaction batches
      val xrefBatches = xrefSpecs.grouped(transactionBatchSize).toSeq

      logger.info(s"Starting batch build of ${xrefSpecs.size} XRefs in ${xrefBatches.size} transaction batch(es) " +
        s"(batchSize=$transactionBatchSize, defaultParallelism=$defaultParallelism)")

      // Process each transaction batch
      val allResults = xrefBatches.zipWithIndex.flatMap { case (batch, batchIdx) =>
        val batchNum = batchIdx + 1
        val batchStartTime = System.currentTimeMillis()

        logger.info(s"Starting transaction batch $batchNum/${xrefBatches.size}: ${batch.size} XRefs")

        // Build all XRefs in this batch in parallel
        val buildOutputs = DistributedXRefBuilder.buildXRefsDistributedBatch(
          tablePath = tablePath,
          xrefSpecs = batch,
          config = config,
          sparkSession = sparkSession,
          overrideConfigMap = overrideConfigMap
        )

        val batchBuildDuration = System.currentTimeMillis() - batchStartTime

        // Convert build outputs to results and collect actions for this batch
        val (addActions, results) = batch.map { case (xrefId, xrefFullPath, splits) =>
          buildOutputs.get(xrefId) match {
            case Some(buildOutput) =>
              val xrefRelativePath = XRefStorageUtils.getXRefRelativePath(xrefId, config.storage.directory)

              // Create the AddXRefAction for transaction log
              val addXRefAction = AddXRefAction(
                path = xrefRelativePath,
                xrefId = xrefId,
                sourceSplitPaths = splits.map(_.path),
                sourceSplitCount = splits.size,
                size = buildOutput.sizeBytes,
                totalTerms = buildOutput.totalTerms,
                footerStartOffset = buildOutput.footerStartOffset,
                footerEndOffset = buildOutput.footerEndOffset,
                createdTime = System.currentTimeMillis(),
                buildDurationMs = batchBuildDuration / batch.size, // Approximate per-XRef time
                maxSourceSplits = config.build.maxSourceSplits,
                docMappingJson = buildOutput.docMappingJson
              )

              logger.info(s"XRef $xrefId built successfully: ${splits.size} splits, ${buildOutput.totalTerms} terms, ${buildOutput.sizeBytes} bytes")

              val result = XRefBuildResult(
                tablePath = tablePath,
                action = "created",
                xrefPath = Some(xrefRelativePath),
                sourceSplitsCount = splits.size,
                totalTerms = buildOutput.totalTerms,
                xrefSizeBytes = buildOutput.sizeBytes,
                buildDurationMs = batchBuildDuration / batch.size
              )

              (Some(addXRefAction), result)

            case None =>
              logger.error(s"No build output returned for XRef $xrefId")
              val result = XRefBuildResult(
                tablePath = tablePath,
                action = "error",
                xrefPath = None,
                sourceSplitsCount = splits.size,
                totalTerms = 0,
                xrefSizeBytes = 0,
                buildDurationMs = batchBuildDuration / batch.size,
                errorMessage = Some(s"No build output returned for XRef $xrefId")
              )

              (None, result)
          }
        }.unzip

        // Collect successful add actions for this batch
        val xrefAddActions = addActions.flatten

        // Commit this batch atomically if we have successful builds
        if (xrefAddActions.nonEmpty) {
          // Only include remove actions in the first batch to avoid duplicate removals
          val xrefRemoveActions = if (batchIdx == 0) {
            plan.xrefsToRemove.map(xref => RemoveXRefAction(
              path = xref.path,
              xrefId = xref.xrefId,
              deletionTimestamp = System.currentTimeMillis(),
              reason = "replaced"
            ))
          } else {
            Seq.empty
          }

          // Commit this batch atomically
          transactionLog.commitWithXRefUpdates(
            dataRemoveActions = Seq.empty,
            dataAddActions = Seq.empty,
            xrefRemoveActions = xrefRemoveActions,
            xrefAddActions = xrefAddActions
          )

          val batchDuration = System.currentTimeMillis() - batchStartTime
          logger.info(s"Committed batch $batchNum/${xrefBatches.size}: ${xrefAddActions.size} XRef(s)" +
            (if (xrefRemoveActions.nonEmpty) s" and ${xrefRemoveActions.size} removal(s)" else "") +
            s" in ${batchDuration}ms")
        }

        results
      }

      val totalDuration = System.currentTimeMillis() - startTime
      logger.info(s"XRef build completed in ${totalDuration}ms - ${allResults.count(_.action == "created")} created in ${xrefBatches.size} batch(es)")

      allResults
    } catch {
      case e: Exception =>
        logger.error(s"XRef build failed: ${e.getMessage}", e)
        Seq(XRefBuildResult(
          tablePath = tablePath,
          action = "error",
          xrefPath = None,
          sourceSplitsCount = plan.splitsToIndex.size,
          totalTerms = 0,
          xrefSizeBytes = 0,
          buildDurationMs = System.currentTimeMillis() - startTime,
          errorMessage = Some(e.getMessage)
        ))
    }
  }

  /**
   * Build a single XRef from a group of splits.
   *
   * Uses distributed execution when enabled and beneficial, otherwise falls back
   * to driver-side execution.
   */
  private def buildSingleXRef(
    splits: Seq[AddAction],
    groupIndex: Int,
    xrefsToRemove: Seq[AddXRefAction]
  ): XRefBuildResult = {
    val startTime = System.currentTimeMillis()
    val xrefId = XRefStorageUtils.generateXRefId()
    val xrefRelativePath = XRefStorageUtils.getXRefRelativePath(xrefId, config.storage.directory)
    val xrefFullPath = XRefStorageUtils.getXRefFullPathString(tablePath, xrefId, config.storage.directory)

    logger.info(s"Building XRef $xrefId with ${splits.size} source splits")

    try {
      // Always use distributed XRef build for executor locality benefits
      logger.info(s"Using distributed XRef build for $xrefId (${splits.size} splits)")
      val buildResult = DistributedXRefBuilder.buildXRefDistributed(
        tablePath = tablePath,
        xrefId = xrefId,
        outputPath = xrefFullPath,
        sourceSplits = splits,
        config = config,
        sparkSession = sparkSession,
        overrideConfigMap = overrideConfigMap
      )

      val buildDuration = System.currentTimeMillis() - startTime

      // Create the AddXRefAction for transaction log
      val addXRefAction = AddXRefAction(
        path = xrefRelativePath,
        xrefId = xrefId,
        sourceSplitPaths = splits.map(_.path),
        sourceSplitCount = splits.size,
        size = buildResult.sizeBytes,
        totalTerms = buildResult.totalTerms,
        footerStartOffset = buildResult.footerStartOffset,
        footerEndOffset = buildResult.footerEndOffset,
        createdTime = System.currentTimeMillis(),
        buildDurationMs = buildDuration,
        maxSourceSplits = config.autoIndex.maxSourceSplits,
        docMappingJson = buildResult.docMappingJson
      )

      // Commit to transaction log atomically
      val removeActions = if (groupIndex == 0) {
        // Only remove old XRefs on first group to avoid duplicate removals
        xrefsToRemove.map(xref => RemoveXRefAction(
          path = xref.path,
          xrefId = xref.xrefId,
          deletionTimestamp = System.currentTimeMillis(),
          reason = "replaced"
        ))
      } else {
        Seq.empty
      }

      transactionLog.commitWithXRefUpdates(
        dataRemoveActions = Seq.empty,
        dataAddActions = Seq.empty,
        xrefRemoveActions = removeActions,
        xrefAddActions = Seq(addXRefAction)
      )

      logger.info(s"XRef $xrefId built successfully: ${splits.size} splits, ${buildResult.totalTerms} terms, ${buildResult.sizeBytes} bytes")

      XRefBuildResult(
        tablePath = tablePath,
        action = "created",
        xrefPath = Some(xrefRelativePath),
        sourceSplitsCount = splits.size,
        totalTerms = buildResult.totalTerms,
        xrefSizeBytes = buildResult.sizeBytes,
        buildDurationMs = buildDuration
      )
    } catch {
      case e: Exception =>
        logger.error(s"Failed to build XRef $xrefId: ${e.getMessage}", e)
        XRefBuildResult(
          tablePath = tablePath,
          action = "error",
          xrefPath = Some(xrefRelativePath),
          sourceSplitsCount = splits.size,
          totalTerms = 0,
          xrefSizeBytes = 0,
          buildDurationMs = System.currentTimeMillis() - startTime,
          errorMessage = Some(e.getMessage)
        )
    }
  }

  /**
   * Check if XRef auto-build should be triggered after a commit.
   *
   * This is called from the transaction log commit path when auto-indexing is enabled.
   *
   * @param newSplits
   *   Splits added in the current commit
   * @return
   *   true if auto-build should be triggered
   */
  def shouldTriggerAutoBuild(newSplits: Seq[AddAction]): Boolean = {
    if (!config.autoIndex.enabled) {
      return false
    }

    val totalSplits = transactionLog.listFiles().size
    val existingXRefs = transactionLog.listXRefs()

    // Check if we've crossed the threshold
    if (totalSplits >= config.autoIndex.minSplitsToTrigger && existingXRefs.isEmpty) {
      logger.info(s"Auto-build triggered: $totalSplits splits >= ${config.autoIndex.minSplitsToTrigger} threshold, no existing XRefs")
      return true
    }

    // Check if existing XRefs need rebuild due to new splits
    if (existingXRefs.nonEmpty) {
      val coveredPaths = existingXRefs.flatMap(_.sourceSplitPaths).map(XRefStorageUtils.extractFileName).toSet
      val uncoveredNewSplits = newSplits.filterNot(s => coveredPaths.contains(XRefStorageUtils.extractFileName(s.path)))

      if (uncoveredNewSplits.nonEmpty) {
        logger.info(s"Auto-build triggered: ${uncoveredNewSplits.size} new splits not covered by existing XRefs")
        return true
      }
    }

    false
  }
}

object XRefBuildManager {

  /**
   * Create XRefBuildManager for a table path.
   *
   * @param tablePath Table path
   * @param transactionLog Transaction log
   * @param sparkSession Spark session
   * @param overrideConfigMap Optional pre-merged config map (hadoop < spark < write options)
   */
  def apply(
    tablePath: String,
    transactionLog: TransactionLog,
    sparkSession: SparkSession,
    overrideConfigMap: Option[Map[String, String]] = None
  ): XRefBuildManager = {
    val config = XRefConfig.fromSparkSession(sparkSession)
    new XRefBuildManager(tablePath, transactionLog, config, sparkSession, overrideConfigMap)
  }

  /**
   * Static entry point for building cross-references.
   *
   * @param tablePath
   *   Table path
   * @param transactionLog
   *   Transaction log
   * @param whereClause
   *   Optional partition filter
   * @param forceRebuild
   *   Force rebuild
   * @param dryRun
   *   Preview mode
   * @param sparkSession
   *   Spark session
   * @param overrideConfigMap
   *   Optional pre-merged config map (hadoop < spark < write options)
   * @return
   *   Build results
   */
  def buildCrossReferences(
    tablePath: String,
    transactionLog: TransactionLog,
    whereClause: Option[Expression],
    forceRebuild: Boolean,
    dryRun: Boolean,
    sparkSession: SparkSession,
    overrideConfigMap: Option[Map[String, String]] = None
  ): Seq[XRefBuildResult] = {
    val manager = apply(tablePath, transactionLog, sparkSession, overrideConfigMap)
    manager.buildCrossReferences(whereClause, forceRebuild, dryRun)
  }
}
