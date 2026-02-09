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

package io.indextables.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.sync.{
  DeltaAddFile,
  DeltaLogReader,
  SyncConfig,
  SyncIndexingGroup,
  SyncMetricsAccumulator,
  SyncTaskExecutor
}
import io.indextables.spark.transaction.{
  AddAction, MetadataAction, PartitionPredicateUtils, RemoveAction, TransactionLogFactory
}
import io.indextables.spark.util.{ConfigNormalization, SizeParser}

import org.slf4j.LoggerFactory

/**
 * SQL command to sync an IndexTables companion index from a Delta table.
 *
 * Creates minimal Quickwit splits that reference external parquet files instead
 * of duplicating data (45-70% split size reduction).
 *
 * Syntax:
 *   SYNC INDEXTABLES WITH DELTA '<delta_table_path>'
 *     [INDEXING MODES (field1:mode1, field2:mode2)]
 *     [FASTFIELDS MODE (HYBRID | DISABLED | PARQUET_ONLY)]
 *     [TARGET INPUT SIZE <size>]
 *     [FROM VERSION <version>]
 *     [WHERE <partition_predicates>]
 *     AT LOCATION '<index_table_path>'
 *     [DRY RUN]
 *
 * Note: "TO DELTA" is also accepted for backward compatibility.
 *
 * Examples:
 *   - SYNC INDEXTABLES WITH DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'
 *   - SYNC INDEXTABLES WITH DELTA 's3://data/events' WHERE year >= 2024 AT LOCATION 's3://index/events'
 *   - SYNC INDEXTABLES WITH DELTA 's3://data/events' FROM VERSION 500 AT LOCATION 's3://index/events'
 */
case class SyncToExternalCommand(
  sourceFormat: String,
  sourcePath: String,
  destPath: String,
  indexingModes: Map[String, String],
  fastFieldMode: String,
  targetInputSize: Option[Long],
  fromVersion: Option[Long] = None,
  wherePredicates: Seq[String] = Seq.empty,
  dryRun: Boolean)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[SyncToExternalCommand])

  /** Default target input size per indexing group: 2GB */
  private val DEFAULT_TARGET_INPUT_SIZE: Long = 2L * 1024L * 1024L * 1024L

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("source_path", StringType)(),
    AttributeReference("status", StringType)(),
    AttributeReference("delta_version", LongType, nullable = true)(),
    AttributeReference("splits_created", IntegerType)(),
    AttributeReference("splits_invalidated", IntegerType)(),
    AttributeReference("parquet_files_indexed", IntegerType)(),
    AttributeReference("parquet_bytes_downloaded", LongType)(),
    AttributeReference("split_bytes_uploaded", LongType)(),
    AttributeReference("duration_ms", LongType)(),
    AttributeReference("message", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Starting SYNC INDEXTABLES WITH DELTA: source=$sourcePath, dest=$destPath, " +
      s"fastFieldMode=$fastFieldMode, fromVersion=$fromVersion, " +
      s"wherePredicates=${wherePredicates.mkString(",")}, dryRun=$dryRun")

    try {
      executeSyncInternal(sparkSession, startTime)
    } catch {
      case e: Exception =>
        val durationMs = System.currentTimeMillis() - startTime
        logger.error(s"SYNC failed: ${e.getMessage}", e)
        Seq(Row(
          destPath, sourcePath, "error",
          null, // delta_version
          0, 0, 0, 0L, 0L, durationMs,
          s"Error: ${e.getMessage}"
        ))
    }
  }

  private def executeSyncInternal(sparkSession: SparkSession, startTime: Long): Seq[Row] = {
    // 1. Read Delta table log
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val deltaReader = new DeltaLogReader(sourcePath, hadoopConf)
    val deltaVersion = deltaReader.currentVersion()

    // Check for non-existent Delta table (version -1 means no commits exist)
    if (deltaVersion < 0) {
      val durationMs = System.currentTimeMillis() - startTime
      logger.error(s"Delta table at $sourcePath does not exist or has no commits (version=$deltaVersion)")
      return Seq(Row(
        destPath, sourcePath, "error",
        null, // delta_version
        0, 0, 0, 0L, 0L, durationMs,
        s"Delta table at $sourcePath does not exist or has no commits"
      ))
    }

    val deltaSchema = deltaReader.schema()
    val partitionColumns = deltaReader.partitionColumns()

    logger.info(s"Delta table at $sourcePath: version=$deltaVersion, " +
      s"partitionColumns=${partitionColumns.mkString(",")}, schema=${deltaSchema.simpleString}")

    // 2. DRY RUN mode: compute plan from Delta table only, no filesystem modifications
    if (dryRun) {
      var allFiles = if (fromVersion.isDefined) {
        deltaReader.getFilesAddedSinceVersion(fromVersion.get)
      } else {
        deltaReader.getAllFiles()
      }
      allFiles = applyWhereFilter(allFiles, partitionColumns, sparkSession)
      val maxGroupSize = targetInputSize.getOrElse(DEFAULT_TARGET_INPUT_SIZE)
      val groups = planIndexingGroups(allFiles, maxGroupSize)
      val durationMs = System.currentTimeMillis() - startTime
      return Seq(Row(
        destPath, sourcePath, "dry_run", deltaVersion.asInstanceOf[java.lang.Long],
        groups.size, 0, allFiles.size,
        0L, 0L, durationMs,
        s"Would create ${groups.size} companion splits from ${allFiles.size} parquet files"
      ))
    }

    // 3. Extract merged configuration for transaction log access
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Read batch size config (0 = all-at-once, preserving backward compatibility)
    val batchSize = mergedConfigs.get("spark.indextables.companion.sync.batchSize")
      .map(_.toInt)
      .getOrElse(0)

    // 4. Open IndexTables transaction log at destination
    val destTablePath = new Path(destPath)
    val transactionLog = TransactionLogFactory.create(
      destTablePath, sparkSession,
      new CaseInsensitiveStringMap(mergedConfigs.asJava)
    )

    try {
      // 5. Determine sync mode: initial vs incremental
      val (existingFiles, lastSyncedVersion, isInitialSync) = determineSyncMode(transactionLog,
        deltaSchema, partitionColumns)

      // 6. Check if already up-to-date
      if (!isInitialSync && lastSyncedVersion >= deltaVersion) {
        val durationMs = System.currentTimeMillis() - startTime
        logger.info(s"Already synced to delta version $deltaVersion, nothing to do")
        return Seq(Row(
          destPath, sourcePath, "no_action", deltaVersion.asInstanceOf[java.lang.Long],
          0, 0, 0, 0L, 0L, durationMs,
          s"Already synced to delta version $deltaVersion"
        ))
      }

      // 6b. On incremental sync, fall back to stored indexing modes/WHERE if not specified
      val effectiveIndexingModes = if (indexingModes.nonEmpty) {
        indexingModes
      } else if (!isInitialSync) {
        try {
          val existingMeta = transactionLog.getMetadata()
          existingMeta.configuration.get("indextables.companion.indexingModes").map { json =>
            import com.fasterxml.jackson.databind.ObjectMapper
            import com.fasterxml.jackson.core.`type`.TypeReference
            val mapper = new ObjectMapper()
            mapper.readValue(json, new TypeReference[java.util.Map[String, String]]() {})
              .asScala.toMap
          }.getOrElse(Map.empty)
        } catch {
          case _: Exception => Map.empty[String, String]
        }
      } else {
        Map.empty[String, String]
      }

      // Resolve effective WHERE predicates: use stored WHERE from metadata if not specified
      val effectiveWherePredicates = if (wherePredicates.nonEmpty) {
        wherePredicates
      } else if (!isInitialSync) {
        try {
          val existingMeta = transactionLog.getMetadata()
          existingMeta.configuration.get("indextables.companion.whereClause")
            .map(Seq(_))
            .getOrElse(Seq.empty)
        } catch {
          case _: Exception => Seq.empty[String]
        }
      } else {
        Seq.empty[String]
      }

      // 7. Determine what needs indexing
      val (rawParquetFiles, splitsToInvalidate) = if (isInitialSync) {
        // Full snapshot or FROM VERSION subset
        val allFiles = if (fromVersion.isDefined) {
          deltaReader.getFilesAddedSinceVersion(fromVersion.get)
        } else {
          deltaReader.getAllFiles()
        }
        logger.info(s"Initial sync: ${allFiles.size} parquet files to index" +
          fromVersion.map(v => s" (from version $v)").getOrElse(""))
        (allFiles, Seq.empty[AddAction])
      } else {
        // Incremental: compute changes since last sync
        computeIncrementalChanges(deltaReader, existingFiles, lastSyncedVersion, deltaVersion)
      }

      // 7b. Apply WHERE partition filter
      val filteredFiles = applyWhereFilter(rawParquetFiles, partitionColumns, sparkSession,
        effectiveWherePredicates)

      // 7c. Idempotency filter: skip parquet files already indexed in existing companion splits
      // Finding #4: Normalize paths (strip leading slashes, URL decode) to ensure
      // consistent comparison between companionSourceFiles and Delta AddFile paths
      val existingCompanionFiles = existingFiles
        .flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
        .map(normalizePath).toSet
      val parquetFilesToIndex = filteredFiles.filterNot(f =>
        existingCompanionFiles.contains(normalizePath(f.path)))

      if (parquetFilesToIndex.size < filteredFiles.size) {
        logger.info(s"Idempotency filter: skipped ${filteredFiles.size - parquetFilesToIndex.size} " +
          s"already-indexed files, ${parquetFilesToIndex.size} files remaining")
      }

      if (parquetFilesToIndex.isEmpty && splitsToInvalidate.isEmpty) {
        val durationMs = System.currentTimeMillis() - startTime
        return Seq(Row(
          destPath, sourcePath, "no_action", deltaVersion.asInstanceOf[java.lang.Long],
          0, 0, 0, 0L, 0L, durationMs,
          "No changes to sync"
        ))
      }

      // 8. Plan indexing groups (respecting partition boundaries and target size)
      val maxGroupSize = targetInputSize.getOrElse(DEFAULT_TARGET_INPUT_SIZE)
      val groups = planIndexingGroups(parquetFilesToIndex, maxGroupSize)

      logger.info(s"Sync plan: ${groups.size} indexing groups, " +
        s"${parquetFilesToIndex.size} parquet files, " +
        s"${splitsToInvalidate.size} splits to invalidate")

      // 9. Resolve credentials for both split and parquet storage
      val splitCredentials = resolveCredentials(mergedConfigs, destPath)
      val parquetCredentials = resolveCredentials(mergedConfigs, sourcePath)

      // 9b. Read companion indexing config from Spark properties
      val writerHeapSize = mergedConfigs.get("spark.indextables.companion.writerHeapSize")
        .map(SizeParser.parseSize)
        .getOrElse(2L * 1024L * 1024L * 1024L) // 2GB default
      val readerBatchSize = mergedConfigs.get("spark.indextables.companion.readerBatchSize")
        .map(_.toInt)
        .getOrElse(8192)

      val syncConfig = SyncConfig(
        indexingModes = effectiveIndexingModes,
        fastFieldMode = fastFieldMode,
        splitCredentials = splitCredentials,
        parquetCredentials = parquetCredentials,
        splitTablePath = destPath,
        writerHeapSize = writerHeapSize,
        readerBatchSize = readerBatchSize
      )

      // 10. Dispatch Spark tasks and commit
      if (batchSize > 0 && groups.size > batchSize) {
        // Batched path: dispatch and commit in batches for incremental progress
        dispatchSyncTasksBatched(
          sparkSession, groups, syncConfig, transactionLog,
          splitsToInvalidate, effectiveIndexingModes,
          effectiveWherePredicates, deltaVersion, batchSize, startTime)
      } else {
        // Legacy single-job path
        dispatchSyncTasksAndCommit(
          sparkSession, groups, syncConfig, transactionLog,
          splitsToInvalidate, effectiveIndexingModes,
          effectiveWherePredicates, deltaVersion, startTime)
      }
    } finally {
      transactionLog.close()
    }
  }

  /**
   * Legacy single-job dispatch: all groups in one Spark job, one atomic commit.
   */
  private def dispatchSyncTasksAndCommit(
    sparkSession: SparkSession,
    groups: Seq[SyncIndexingGroupPlan],
    syncConfig: SyncConfig,
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    splitsToInvalidate: Seq[AddAction],
    effectiveIndexingModes: Map[String, String],
    effectiveWherePredicates: Seq[String],
    deltaVersion: Long,
    startTime: Long
  ): Seq[Row] = {
    val results = dispatchSyncTasks(sparkSession, groups, syncConfig)

    // Build companion metadata for transaction log
    val updatedMetadata = buildCompanionMetadata(
      transactionLog, effectiveIndexingModes, effectiveWherePredicates, deltaVersion)

    // Commit transaction log with metadata update
    val removeActions = buildRemoveActions(splitsToInvalidate)
    val addActions = results.map(_.addAction.copy(
      companionDeltaVersion = Some(deltaVersion)
    ))

    if (addActions.nonEmpty || removeActions.nonEmpty) {
      val version = transactionLog.commitSyncActions(
        removeActions, addActions, Some(updatedMetadata))
      transactionLog.invalidateCache()
      logger.info(s"Committed ${addActions.size} adds and ${removeActions.size} removes " +
        s"with companion metadata at transaction log version $version")
    }

    buildResultRow(results, splitsToInvalidate.size, deltaVersion, startTime)
  }

  /**
   * Batched dispatch: process groups in batches with per-batch commits for
   * incremental progress on large tables.
   */
  private def dispatchSyncTasksBatched(
    sparkSession: SparkSession,
    groups: Seq[SyncIndexingGroupPlan],
    syncConfig: SyncConfig,
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    splitsToInvalidate: Seq[AddAction],
    effectiveIndexingModes: Map[String, String],
    effectiveWherePredicates: Seq[String],
    deltaVersion: Long,
    batchSize: Int,
    startTime: Long
  ): Seq[Row] = {
    val batches = groups.grouped(batchSize).toSeq
    val allResults = scala.collection.mutable.ArrayBuffer[io.indextables.spark.sync.SyncTaskResult]()

    logger.info(s"Batched sync: ${groups.size} groups in ${batches.size} batches of $batchSize")

    batches.zipWithIndex.foreach { case (batch, batchIdx) =>
      val isFirstBatch = batchIdx == 0
      val isLastBatch = batchIdx == batches.size - 1

      logger.info(s"Dispatching batch ${batchIdx + 1}/${batches.size} " +
        s"(${batch.size} groups)")

      // Refresh credentials for each batch (handles token expiration)
      val mergedConfigs = {
        val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
        val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(
          sparkSession.sparkContext.hadoopConfiguration)
        ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)
      }
      val refreshedConfig = syncConfig.copy(
        splitCredentials = resolveCredentials(mergedConfigs, destPath),
        parquetCredentials = resolveCredentials(mergedConfigs, sourcePath)
      )

      val batchResults = dispatchSyncTasks(sparkSession, batch, refreshedConfig)
      allResults ++= batchResults

      // Commit this batch's results
      val batchAddActions = batchResults.map(_.addAction.copy(
        companionDeltaVersion = Some(deltaVersion)
      ))

      // First batch also commits RemoveActions (invalidation splits)
      val batchRemoveActions = if (isFirstBatch) {
        buildRemoveActions(splitsToInvalidate)
      } else {
        Seq.empty
      }

      // Last batch updates metadata with lastSyncedVersion
      val metadataUpdate = if (isLastBatch) {
        Some(buildCompanionMetadata(
          transactionLog, effectiveIndexingModes, effectiveWherePredicates, deltaVersion))
      } else {
        None
      }

      if (batchAddActions.nonEmpty || batchRemoveActions.nonEmpty || metadataUpdate.isDefined) {
        val version = transactionLog.commitSyncActions(
          batchRemoveActions, batchAddActions, metadataUpdate)
        transactionLog.invalidateCache()
        logger.info(s"Batch ${batchIdx + 1}/${batches.size}: committed " +
          s"${batchAddActions.size} adds, ${batchRemoveActions.size} removes " +
          s"at transaction log version $version")
      }
    }

    buildResultRow(allResults.toSeq, splitsToInvalidate.size, deltaVersion, startTime)
  }

  /**
   * Apply WHERE partition filter to DeltaAddFile list.
   * Uses the explicitly provided predicates, or falls back to effectiveWherePredicates.
   */
  private def applyWhereFilter(
    files: Seq[DeltaAddFile],
    partitionColumns: Seq[String],
    sparkSession: SparkSession,
    predicates: Seq[String] = Seq.empty
  ): Seq[DeltaAddFile] = {
    val effectivePreds = if (predicates.nonEmpty) predicates else wherePredicates
    if (effectivePreds.isEmpty || partitionColumns.isEmpty) return files

    val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns)
    val parsedPredicates = PartitionPredicateUtils.parseAndValidatePredicates(
      effectivePreds, partitionSchema, sparkSession)

    val filtered = files.filter(f =>
      PartitionPredicateUtils.evaluatePredicates(f.partitionValues, partitionSchema, parsedPredicates))

    if (filtered.size < files.size) {
      logger.info(s"WHERE filter: ${files.size} -> ${filtered.size} files " +
        s"(pruned ${files.size - filtered.size})")
    }
    filtered
  }

  /**
   * Build companion metadata configuration for transaction log commit.
   */
  private def buildCompanionMetadata(
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    effectiveIndexingModes: Map[String, String],
    effectiveWherePredicates: Seq[String],
    deltaVersion: Long
  ): MetadataAction = {
    val existingMetadata = transactionLog.getMetadata()
    val companionConfig = existingMetadata.configuration ++ Map(
      "indextables.companion.enabled" -> "true",
      "indextables.companion.sourceTablePath" -> sourcePath,
      "indextables.companion.sourceFormat" -> sourceFormat,
      "indextables.companion.lastSyncedVersion" -> deltaVersion.toString,
      "indextables.companion.fastFieldMode" -> fastFieldMode
    ) ++ (if (effectiveIndexingModes.nonEmpty) {
      import com.fasterxml.jackson.databind.ObjectMapper
      val mapper = new ObjectMapper()
      Map("indextables.companion.indexingModes" -> mapper.writeValueAsString(
        effectiveIndexingModes.asJava
      ))
    } else Map.empty) ++ (if (effectiveWherePredicates.nonEmpty) {
      Map("indextables.companion.whereClause" -> effectiveWherePredicates.head)
    } else Map.empty) ++ fromVersion.map(v =>
      "indextables.companion.fromVersion" -> v.toString
    )
    existingMetadata.copy(configuration = companionConfig)
  }

  /**
   * Build RemoveAction entries for invalidated splits.
   */
  private def buildRemoveActions(splitsToInvalidate: Seq[AddAction]): Seq[RemoveAction] =
    splitsToInvalidate.map { split =>
      RemoveAction(
        path = split.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = Some(true),
        partitionValues = Some(split.partitionValues),
        size = Some(split.size)
      )
    }

  /**
   * Build the result row from sync task results.
   */
  private def buildResultRow(
    results: Seq[io.indextables.spark.sync.SyncTaskResult],
    splitsInvalidated: Int,
    deltaVersion: Long,
    startTime: Long
  ): Seq[Row] = {
    val totalBytesDownloaded = results.map(_.bytesDownloaded).sum
    val totalBytesUploaded = results.map(_.bytesUploaded).sum
    val totalFilesIndexed = results.map(_.parquetFilesIndexed).sum
    val durationMs = System.currentTimeMillis() - startTime

    logger.info(s"SYNC completed: ${results.length} splits created, " +
      s"$splitsInvalidated invalidated, " +
      s"$totalFilesIndexed files indexed, " +
      s"downloaded ${totalBytesDownloaded} bytes, uploaded ${totalBytesUploaded} bytes, " +
      s"duration ${durationMs}ms")

    Seq(Row(
      destPath, sourcePath, "success", deltaVersion.asInstanceOf[java.lang.Long],
      results.length, splitsInvalidated, totalFilesIndexed,
      totalBytesDownloaded, totalBytesUploaded, durationMs,
      s"Synced $totalFilesIndexed parquet files into ${results.length} companion splits"
    ))
  }

  /**
   * Determine whether this is an initial sync or incremental, by reading
   * the existing transaction log state.
   *
   * @return (existingFiles, lastSyncedVersion, isInitialSync)
   */
  private def determineSyncMode(
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    deltaSchema: org.apache.spark.sql.types.StructType,
    partitionColumns: Seq[String]
  ): (Seq[AddAction], Long, Boolean) =
    try {
      transactionLog.getMetadata()
      val files = transactionLog.listFiles()
      // Derive lastSyncedVersion from the max companionDeltaVersion across all files
      val versions = files.flatMap(_.companionDeltaVersion)
      val maxDeltaVersion = if (versions.nonEmpty) versions.max else -1L
      logger.info(s"Existing table: ${files.size} files, lastSyncedVersion=$maxDeltaVersion")
      (files, maxDeltaVersion, false)
    } catch {
      // Finding #5: Narrow catch to expected exceptions for missing/uninitialized table
      case _: java.io.FileNotFoundException | _: java.io.IOException |
           _: IllegalStateException | _: NoSuchElementException =>
        logger.info(s"No existing table at $destPath, initializing for initial sync")
        transactionLog.initialize(deltaSchema, partitionColumns)
        (Seq.empty[AddAction], -1L, true)
    }

  /**
   * For incremental sync: compute which parquet files need indexing and which
   * companion splits need invalidation.
   */
  private def computeIncrementalChanges(
    deltaReader: DeltaLogReader,
    existingFiles: Seq[AddAction],
    lastSyncedVersion: Long,
    currentDeltaVersion: Long
  ): (Seq[DeltaAddFile], Seq[AddAction]) = {
    val (added, removed) = deltaReader.getChanges(lastSyncedVersion, currentDeltaVersion)

    // Find companion splits affected by removed parquet files
    val removedPaths = removed.map(_.path).toSet
    val splitsToInvalidate = existingFiles.filter { split =>
      split.companionSourceFiles.exists(_.exists(removedPaths.contains))
    }

    // Finding #6: Collect remaining valid files from invalidated splits, preserving
    // partition values from the parent split so they group correctly in planIndexingGroups
    val remainingAsDeltaAddFiles = splitsToInvalidate.flatMap { split =>
      split.companionSourceFiles.getOrElse(Seq.empty)
        .filterNot(removedPaths.contains)
        .map(relPath => DeltaAddFile(relPath, split.partitionValues, 0L))
    }.groupBy(_.path).map(_._2.head).toSeq

    // Merge added files with remaining files from invalidated splits, dedup by path
    // (prefer added files since they have accurate size from Delta log)
    val allFilesToIndex = (added ++ remainingAsDeltaAddFiles)
      .groupBy(_.path).map(_._2.head).toSeq

    logger.info(s"Incremental sync: ${added.size} added, ${removed.size} removed, " +
      s"${splitsToInvalidate.size} splits to invalidate, " +
      s"${remainingAsDeltaAddFiles.size} files to re-index from invalidated splits")

    (allFilesToIndex, splitsToInvalidate)
  }

  /**
   * Group parquet files into indexing groups, respecting partition boundaries
   * and target input size.
   */
  private def planIndexingGroups(
    files: Seq[DeltaAddFile],
    maxGroupSize: Long
  ): Seq[SyncIndexingGroupPlan] = {
    // Group by partition values
    val byPartition = files.groupBy(_.partitionValues)

    byPartition.flatMap { case (partValues, partFiles) =>
      val groups = scala.collection.mutable.ArrayBuffer[SyncIndexingGroupPlan]()
      var currentFiles = scala.collection.mutable.ArrayBuffer[DeltaAddFile]()
      var currentSize = 0L

      partFiles.foreach { file =>
        if (currentSize + file.size > maxGroupSize && currentFiles.nonEmpty) {
          groups += SyncIndexingGroupPlan(currentFiles.toSeq, partValues)
          currentFiles = scala.collection.mutable.ArrayBuffer[DeltaAddFile]()
          currentSize = 0L
        }
        currentFiles += file
        currentSize += file.size
      }

      if (currentFiles.nonEmpty) {
        groups += SyncIndexingGroupPlan(currentFiles.toSeq, partValues)
      }

      groups.toSeq
    }.toSeq
  }

  /**
   * Dispatch indexing tasks to Spark executors and collect results.
   */
  private def dispatchSyncTasks(
    sparkSession: SparkSession,
    groups: Seq[SyncIndexingGroupPlan],
    syncConfig: SyncConfig
  ): Seq[io.indextables.spark.sync.SyncTaskResult] = {
    // Register accumulator for Spark UI metrics
    val metricsAccumulator = new SyncMetricsAccumulator()
    sparkSession.sparkContext.register(metricsAccumulator, "sync_metrics")

    // Broadcast config to all executors
    val broadcastConfig = sparkSession.sparkContext.broadcast(syncConfig)

    // Convert plans to serializable groups
    val syncGroups = groups.zipWithIndex.map { case (plan, idx) =>
      SyncIndexingGroup(
        parquetFiles = plan.files.map(f => resolveAbsolutePath(f.path, sourcePath)),
        parquetTableRoot = sourcePath,
        partitionValues = plan.partitionValues,
        groupIndex = idx
      )
    }

    // Set job group for Spark UI visibility
    sparkSession.sparkContext.setJobGroup(
      "sync_indextables",
      s"SYNC INDEXTABLES WITH DELTA: ${syncGroups.size} groups from $sourcePath"
    )

    try {
      // Parallelize across executors - one task per group
      val numPartitions = math.max(1, syncGroups.size)
      val groupsRDD = sparkSession.sparkContext
        .parallelize(syncGroups, numPartitions)
        .setName(s"SYNC INDEXTABLES WITH DELTA [${syncGroups.size} groups]")

      val results = groupsRDD.map { group =>
        val result = SyncTaskExecutor.execute(group, broadcastConfig.value)
        result
      }.collect().toSeq

      results
    } finally {
      broadcastConfig.destroy()
      sparkSession.sparkContext.clearJobGroup()
    }
  }

  /**
   * Normalize a file path for consistent comparison (Finding #4).
   * Handles URL-encoded paths from Delta log and strips trailing slashes.
   */
  private def normalizePath(path: String): String = {
    val decoded = try {
      java.net.URLDecoder.decode(path, "UTF-8")
    } catch {
      case _: Exception => path
    }
    decoded.stripSuffix("/")
  }

  /**
   * Resolve an absolute path from a potentially relative parquet file path.
   */
  private def resolveAbsolutePath(path: String, tableRoot: String): String =
    if (path.startsWith("s3://") || path.startsWith("s3a://") ||
      path.startsWith("abfss://") || path.startsWith("wasbs://") ||
      path.startsWith("abfs://") || path.startsWith("/")) {
      path
    } else {
      s"${tableRoot.stripSuffix("/")}/$path"
    }

  /**
   * Resolve credentials for a given storage path. Returns a flat map of
   * credential properties suitable for serialization to executors.
   */
  private def resolveCredentials(
    configs: Map[String, String],
    path: String
  ): Map[String, String] = {
    import io.indextables.spark.utils.CredentialProviderFactory

    val result = scala.collection.mutable.Map[String, String]()

    // Resolve AWS credentials via provider
    val creds = CredentialProviderFactory.resolveAWSCredentialsFromConfig(configs, path)
    creds.foreach { c =>
      result += ("spark.indextables.aws.accessKey" -> c.accessKey)
      result += ("spark.indextables.aws.secretKey" -> c.secretKey)
      c.sessionToken.foreach(t => result += ("spark.indextables.aws.sessionToken" -> t))
    }

    // Copy region if present
    configs.get("spark.indextables.aws.region").foreach { r =>
      result += ("spark.indextables.aws.region" -> r)
    }

    // Copy Azure credentials if present
    configs.foreach { case (k, v) =>
      if (k.startsWith("spark.indextables.azure.") ||
        k.startsWith("fs.azure.") ||
        k.startsWith("fs.abfss.")) {
        result += (k -> v)
      }
    }

    result.toMap
  }
}

/** Internal grouping plan for SYNC operation (driver-side only, not serialized). */
private[sql] case class SyncIndexingGroupPlan(
  files: Seq[DeltaAddFile],
  partitionValues: Map[String, String])
