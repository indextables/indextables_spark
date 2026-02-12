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
 *   BUILD INDEXTABLES COMPANION FROM DELTA '<delta_table_path>'
 *     [INDEXING MODES (field1:mode1, field2:mode2)]
 *     [FASTFIELDS MODE (HYBRID | DISABLED | PARQUET_ONLY)]
 *     [TARGET INPUT SIZE <size>]
 *     [FROM VERSION <version>]
 *     [WHERE <partition_predicates>]
 *     AT LOCATION '<index_table_path>'
 *     [DRY RUN]
 *
 * Note: "WITH DELTA" is also accepted for backward compatibility.
 *
 * Examples:
 *   - BUILD INDEXTABLES COMPANION FROM DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'
 *   - BUILD INDEXTABLES COMPANION FROM DELTA 's3://data/events' WHERE year >= 2024 AT LOCATION 's3://index/events'
 *   - BUILD INDEXTABLES COMPANION FROM DELTA 's3://data/events' FROM VERSION 500 AT LOCATION 's3://index/events'
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
    logger.info(s"Starting BUILD INDEXTABLES COMPANION FROM DELTA: source=$sourcePath, dest=$destPath, " +
      s"fastFieldMode=$fastFieldMode, fromVersion=$fromVersion, " +
      s"wherePredicates=${wherePredicates.mkString(",")}, dryRun=$dryRun")

    try {
      executeSyncInternal(sparkSession, startTime)
    } catch {
      case e: Exception =>
        val durationMs = System.currentTimeMillis() - startTime
        logger.error(s"BUILD COMPANION failed: ${e.getMessage}", e)
        Seq(Row(
          destPath, sourcePath, "error",
          null, // delta_version
          0, 0, 0, 0L, 0L, durationMs,
          s"Error: ${e.getMessage}"
        ))
    }
  }

  private def executeSyncInternal(sparkSession: SparkSession, startTime: Long): Seq[Row] = {
    // 1. Extract merged configuration and resolve credentials for Delta table access
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Resolve credentials for the SOURCE Delta table (not the destination split table).
    // These are passed to DeltaLogReader which uses tantivy4java's DeltaTableReader
    // (delta-kernel-rs) — no Hadoop dependency, native S3/Azure support.
    val sourceCredentials = resolveCredentials(mergedConfigs, sourcePath)
    val deltaReader = new DeltaLogReader(sourcePath, sourceCredentials)
    executeSyncWithReader(sparkSession, deltaReader, mergedConfigs, startTime)
  }

  private def executeSyncWithReader(
    sparkSession: SparkSession,
    deltaReader: DeltaLogReader,
    mergedConfigs: Map[String, String],
    startTime: Long
  ): Seq[Row] = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
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

    val partitionColumns = deltaReader.partitionColumns()
    val allDeltaFiles = deltaReader.getAllFiles()

    logger.info(s"Delta table at $sourcePath: version=$deltaVersion, " +
      s"partitionColumns=${partitionColumns.mkString(",")}, files=${allDeltaFiles.size}")

    // 2. DRY RUN mode: compute plan from Delta table only, no filesystem modifications
    if (dryRun) {
      var allFiles = allDeltaFiles
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

    // 3. Use merged configuration (resolved earlier) for transaction log access

    // Batch size: indexing tasks per group (each group = one Spark job).
    // Default: defaultParallelism to fill the cluster with each group.
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val batchSize = mergedConfigs.get("spark.indextables.companion.sync.batchSize")
      .map(_.toInt)
      .getOrElse(defaultParallelism)

    // Max concurrent groups (Spark jobs) running simultaneously.
    val maxConcurrentBatches = mergedConfigs.get("spark.indextables.companion.sync.maxConcurrentBatches")
      .map(_.toInt)
      .getOrElse(6)

    // 4. Open IndexTables transaction log at destination
    val destTablePath = new Path(destPath)
    val transactionLog = TransactionLogFactory.create(
      destTablePath, sparkSession,
      new CaseInsensitiveStringMap(mergedConfigs.asJava)
    )

    try {
      // 5. Determine sync mode: initial vs incremental
      val (existingFiles, lastSyncedVersion, isInitialSync) = determineSyncMode(transactionLog,
        deltaReader.schema(), partitionColumns)

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

      // 7. Determine what needs indexing via anti-join (works for both initial and incremental)
      val (rawParquetFiles, splitsToInvalidate) =
        computeAntiJoinChanges(deltaReader, existingFiles, isInitialSync)

      // 7b. Apply WHERE partition filter
      val parquetFilesToIndex = applyWhereFilter(rawParquetFiles, partitionColumns, sparkSession,
        effectiveWherePredicates)

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

      // 10. Dispatch groups as concurrent batches (3 Spark jobs at a time by default)
      dispatchSyncTasksBatched(
        sparkSession, groups, syncConfig, transactionLog,
        splitsToInvalidate, effectiveIndexingModes,
        effectiveWherePredicates, deltaVersion, batchSize, maxConcurrentBatches, startTime)
    } finally {
      transactionLog.close()
    }
  }

  /**
   * Batched concurrent dispatch: indexing tasks are grouped into batches of
   * batchSize, with up to maxConcurrentBatches Spark jobs running at once.
   * Uses Scala parallel collections with ForkJoinPool (same pattern as
   * MergeSplitsCommand) and sets spark.scheduler.pool per thread so the
   * FAIR scheduler on Databricks actually runs batches concurrently.
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
    maxConcurrentBatches: Int,
    startTime: Long
  ): Seq[Row] = {
    import scala.collection.parallel.ForkJoinTaskSupport
    import scala.util.{Try, Success, Failure}

    val totalTasks = groups.size

    // Convert plans to serializable groups
    val syncGroups = groups.zipWithIndex.map { case (plan, idx) =>
      SyncIndexingGroup(
        parquetFiles = plan.files.map(f => resolveAbsolutePath(f.path, sourcePath)),
        parquetTableRoot = sourcePath,
        partitionValues = plan.partitionValues,
        groupIndex = idx
      )
    }

    // Split into batches of batchSize tasks each
    val batches = syncGroups.grouped(batchSize).toSeq
    val totalBatches = batches.size

    // Pre-compute per-batch input sizes from the original plans (which have file sizes)
    val batchInputSizes = groups.grouped(batchSize).map { batchPlans =>
      val totalFiles = batchPlans.map(_.files.size).sum
      val totalBytes = batchPlans.map(_.files.map(_.size).sum).sum
      (totalFiles, totalBytes)
    }.toSeq

    val grandTotalBytes = batchInputSizes.map(_._2).sum
    val grandTotalFiles = batchInputSizes.map(_._1).sum

    logger.info(s"Batched sync: $totalTasks indexing tasks in $totalBatches batches " +
      s"($batchSize tasks/batch, $maxConcurrentBatches concurrent), " +
      f"$grandTotalFiles files, ${grandTotalBytes / 1024.0 / 1024.0 / 1024.0}%.2f GB total input")

    // Broadcast config once for all batches
    val broadcastConfig = sparkSession.sparkContext.broadcast(syncConfig)

    // Thread-safe accumulators
    val allResults = new java.util.concurrent.ConcurrentLinkedQueue[io.indextables.spark.sync.SyncTaskResult]()
    val commitLock = new Object()
    val removesCommitted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val completedBatches = new java.util.concurrent.atomic.AtomicInteger(0)

    // Scala parallel collections with ForkJoinPool for controlled concurrency
    // (same pattern as MergeSplitsCommand — proven to run concurrent Spark jobs)
    val parBatches = batches.zipWithIndex.par
    val forkJoinPool = new java.util.concurrent.ForkJoinPool(maxConcurrentBatches)
    parBatches.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    // Define task function OUTSIDE parallel block to avoid closure capture issues
    val taskFunc = (_: org.apache.spark.TaskContext, iter: Iterator[SyncIndexingGroup]) => {
      val group = iter.next()
      SyncTaskExecutor.execute(group, broadcastConfig.value)
    }

    try {
      parBatches.foreach { case (batch, batchIdx) =>
        val batchNum = batchIdx + 1
        val (batchFiles, batchBytes) = batchInputSizes(batchIdx)
        val batchSizeGB = batchBytes / 1024.0 / 1024.0 / 1024.0

        // Set job group for Spark UI visibility (thread-local, safe for concurrent use)
        sparkSession.sparkContext.setJobGroup(
          s"tantivy4spark-build-companion-batch-$batchNum",
          f"BUILD COMPANION Batch $batchNum/$totalBatches: ${batch.size} tasks, $batchFiles files ($batchSizeGB%.2f GB)",
          interruptOnCancel = true
        )

        try {
          val batchRDD = sparkSession.sparkContext
            .parallelize(batch, batch.size)
            .setName(f"Building batch $batchNum of $totalBatches: ${batch.size} tasks, $batchFiles files ($batchSizeGB%.2f GB)")

          // Run Spark job for this batch
          val batchResults = new java.util.ArrayList[io.indextables.spark.sync.SyncTaskResult]()

          sparkSession.sparkContext.runJob(
            batchRDD,
            taskFunc,
            0 until batch.size,
            (_: Int, result: io.indextables.spark.sync.SyncTaskResult) => {
              batchResults.add(result)
              allResults.add(result)
            }
          )

          // Commit this batch's results (synchronized for thread safety).
          // The entire commit sequence (build actions, read metadata, write)
          // must be inside the lock because concurrent threads invalidate the
          // transaction log cache after each commit.
          val addActions = batchResults.asScala.toSeq.map(_.addAction.copy(
            companionDeltaVersion = Some(deltaVersion)
          ))

          commitLock.synchronized {
            // First batch to commit also includes RemoveActions for invalidated splits
            val removeActions = if (removesCommitted.compareAndSet(false, true)) {
              buildRemoveActions(splitsToInvalidate)
            } else {
              Seq.empty
            }

            // Last batch to complete includes metadata update
            val completed = completedBatches.incrementAndGet()
            val metadataUpdate = if (completed == totalBatches) {
              Some(buildCompanionMetadata(
                transactionLog, effectiveIndexingModes, effectiveWherePredicates, deltaVersion))
            } else {
              None
            }

            if (addActions.nonEmpty || removeActions.nonEmpty || metadataUpdate.isDefined) {
              val version = transactionLog.commitSyncActions(
                removeActions, addActions, metadataUpdate)
              transactionLog.invalidateCache()
              logger.info(s"Batch $batchNum/$totalBatches committed: " +
                s"${addActions.size} adds at version $version " +
                s"($completed/$totalBatches batches complete)")
            }
          }
        } finally {
          sparkSession.sparkContext.clearJobGroup()
        }
      }
    } finally {
      forkJoinPool.shutdown()
      broadcastConfig.destroy()
    }

    buildResultRow(allResults.asScala.toSeq, splitsToInvalidate.size, deltaVersion, startTime)
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

    logger.info(s"BUILD COMPANION completed: ${results.length} splits created, " +
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
      val versions = files.flatMap(_.companionDeltaVersion)
      val maxDeltaVersion = if (versions.nonEmpty) versions.max else -1L
      logger.info(s"Existing table: ${files.size} files, lastSyncedVersion=$maxDeltaVersion")
      (files, maxDeltaVersion, false)
    } catch {
      // Only catch the specific exception thrown by TransactionLog.getMetadata() when no
      // table/metadata exists. All other exceptions (network errors, credential failures,
      // etc.) propagate — this prevents accidentally re-initializing an existing table
      // on a transient failure.
      case e: RuntimeException if e.getMessage != null &&
          e.getMessage.contains("No metadata found in transaction log") =>
        logger.info(s"No existing table at $destPath, initializing for initial sync")
        transactionLog.initialize(deltaSchema, partitionColumns)
        (Seq.empty[AddAction], -1L, true)
    }

  /**
   * Compute which parquet files need indexing and which companion splits need
   * invalidation, using an anti-join between the current Delta snapshot and
   * existing companion splits.
   *
   * This replaces the changelog-replay approach (getChanges) with a simpler
   * set-reconciliation approach that works with delta-kernel (which has no
   * getChanges API) and is more robust against missed changes.
   */
  private def computeAntiJoinChanges(
    deltaReader: DeltaLogReader,
    existingFiles: Seq[AddAction],
    isInitialSync: Boolean
  ): (Seq[DeltaAddFile], Seq[AddAction]) = {
    val currentDeltaFiles = deltaReader.getAllFiles()

    if (isInitialSync) {
      logger.info(s"Initial sync: ${currentDeltaFiles.size} parquet files to index")
      return (currentDeltaFiles, Seq.empty[AddAction])
    }

    val currentDeltaPaths = currentDeltaFiles.map(f => normalizePath(f.path)).toSet

    // Collect all parquet file paths tracked by existing companion splits
    val companionPaths = existingFiles
      .flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
      .map(normalizePath).toSet

    // Anti-join: files in Delta but not in any companion split → need indexing
    val filesToIndex = currentDeltaFiles.filterNot(f =>
      companionPaths.contains(normalizePath(f.path)))

    // Files tracked by companion splits but no longer in Delta → gone from Delta
    val pathsGoneFromDelta = companionPaths -- currentDeltaPaths

    // Find companion splits that reference files gone from Delta → need invalidation
    val splitsToInvalidate = if (pathsGoneFromDelta.nonEmpty) {
      existingFiles.filter { split =>
        split.companionSourceFiles.exists(_.exists(f =>
          pathsGoneFromDelta.contains(normalizePath(f))))
      }
    } else {
      Seq.empty
    }

    // Collect remaining valid files from invalidated splits that still exist in Delta,
    // preserving partition values from the parent split for correct grouping.
    // Look up accurate file sizes from the current Delta snapshot.
    val deltaSizeByPath = currentDeltaFiles.map(f => normalizePath(f.path) -> f.size).toMap
    val remainingFromInvalidated = splitsToInvalidate.flatMap { split =>
      split.companionSourceFiles.getOrElse(Seq.empty)
        .filterNot(f => pathsGoneFromDelta.contains(normalizePath(f)))
        .map { relPath =>
          val size = deltaSizeByPath.getOrElse(normalizePath(relPath), 0L)
          DeltaAddFile(relPath, split.partitionValues, size)
        }
    }.groupBy(f => normalizePath(f.path)).map(_._2.head).toSeq

    // Merge new files with remaining files from invalidated splits, dedup by path.
    // Prefer the entry with the largest size (Delta files have accurate size, invalidated = 0L).
    val allFilesToIndex = (filesToIndex ++ remainingFromInvalidated)
      .groupBy(f => normalizePath(f.path))
      .map(_._2.maxBy(_.size))
      .toSeq

    logger.info(s"Anti-join sync: ${currentDeltaFiles.size} in Delta, " +
      s"${companionPaths.size} in companion, ${filesToIndex.size} new, " +
      s"${pathsGoneFromDelta.size} gone, ${splitsToInvalidate.size} splits to invalidate, " +
      s"${remainingFromInvalidated.size} files to re-index from invalidated splits")

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
      val totalBytes = partFiles.map(_.size).sum
      val numGroups = math.max(1, math.ceil(totalBytes.toDouble / maxGroupSize).toInt)

      if (numGroups == 1) {
        // All files fit in one group
        Seq(SyncIndexingGroupPlan(partFiles.toSeq, partValues))
      } else {
        // Distribute files evenly: target per group = totalBytes / numGroups
        val targetPerGroup = totalBytes.toDouble / numGroups
        val groups = Array.fill(numGroups)(scala.collection.mutable.ArrayBuffer[DeltaAddFile]())
        val groupSizes = Array.fill(numGroups)(0L)

        // Sort largest files first (First Fit Decreasing) for better bin-packing balance
        partFiles.sortBy(-_.size).foreach { file =>
          // Assign to the group that is most under its target
          // (i.e., the group with the largest remaining capacity relative to target)
          var bestIdx = 0
          var bestDeficit = groupSizes(0) - targetPerGroup
          var i = 1
          while (i < numGroups) {
            val deficit = groupSizes(i) - targetPerGroup
            if (deficit < bestDeficit) {
              bestDeficit = deficit
              bestIdx = i
            }
            i += 1
          }
          groups(bestIdx) += file
          groupSizes(bestIdx) += file.size
        }

        val nonEmpty = groups.filter(_.nonEmpty)
        if (logger.isInfoEnabled) {
          val sizesStr = groupSizes.filter(_ > 0).map(s => f"${s / 1024.0 / 1024.0}%.1fMB").mkString(", ")
          logger.info(s"Partition $partValues: $numGroups groups, target ${targetPerGroup / 1024 / 1024}MB each, actual: [$sizesStr]")
        }
        nonEmpty.map(g => SyncIndexingGroupPlan(g.toSeq, partValues)).toSeq
      }
    }.toSeq
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

/** Internal grouping plan for BUILD COMPANION operation (driver-side only, not serialized). */
private[sql] case class SyncIndexingGroupPlan(
  files: Seq[DeltaAddFile],
  partitionValues: Map[String, String])
