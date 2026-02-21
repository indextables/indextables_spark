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
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.sync.{
  CompanionSourceFile,
  CompanionSourceReader,
  DeltaSourceReader,
  IcebergSourceReader,
  ParquetDirectoryReader,
  SyncConfig,
  SyncIndexingGroup,
  SyncTaskExecutor
}
import io.indextables.spark.transaction.{
  AddAction,
  MetadataAction,
  PartitionPredicateUtils,
  RemoveAction,
  TransactionLogFactory
}
import io.indextables.spark.util.{ConfigNormalization, SizeParser}
import org.slf4j.LoggerFactory

/**
 * SQL command to sync an IndexTables companion index from a source table.
 *
 * Creates minimal Quickwit splits that reference external parquet files instead of duplicating data (45-70% split size
 * reduction).
 *
 * Syntax: BUILD INDEXTABLES COMPANION FOR (DELTA|PARQUET|ICEBERG) '<source_path>' [INDEXING MODES (field1:mode1,
 * field2:mode2)] [FASTFIELDS MODE (HYBRID | DISABLED | PARQUET_ONLY)] [TARGET INPUT SIZE <size>] [FROM VERSION
 * <version>] [WHERE <partition_predicates>] AT LOCATION '<index_table_path>' [DRY RUN]
 *
 * Examples:
 *   - BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'
 *   - BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/data' AT LOCATION 's3://bucket/index'
 *   - BUILD INDEXTABLES COMPANION FOR ICEBERG 'db.events' CATALOG 'glue' AT LOCATION 's3://bucket/index'
 */
case class SyncToExternalCommand(
  sourceFormat: String,
  sourcePath: String,
  destPath: String,
  indexingModes: Map[String, String],
  fastFieldMode: String,
  targetInputSize: Option[Long],
  writerHeapSize: Option[Long] = None,
  fromVersion: Option[Long] = None,
  fromSnapshot: Option[Long] = None,
  schemaSourcePath: Option[String] = None,
  catalogName: Option[String] = None,
  catalogType: Option[String] = None,
  warehouse: Option[String] = None,
  wherePredicates: Seq[String] = Seq.empty,
  dryRun: Boolean)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[SyncToExternalCommand])

  /**
   * CATALOG and WAREHOUSE are interchangeable in SQL syntax: "CATALOG" is the UC term, "WAREHOUSE" is the Iceberg REST
   * term, but they refer to the same concept. When only one is specified, it fills in the other.
   */
  private def effectiveCatalogName: Option[String] = catalogName.orElse(warehouse)
  private def effectiveWarehouse: Option[String]   = warehouse.orElse(catalogName)

  /** Default target input size per indexing group: 2GB */
  private val DEFAULT_TARGET_INPUT_SIZE: Long = 2L * 1024L * 1024L * 1024L

  /** Default writer heap size: 1GB */
  private val DEFAULT_WRITER_HEAP_SIZE: Long = 1L * 1024L * 1024L * 1024L

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("source_path", StringType)(),
    AttributeReference("status", StringType)(),
    AttributeReference("source_version", LongType, nullable = true)(),
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
    logger.info(
      s"Starting BUILD INDEXTABLES COMPANION FOR ${sourceFormat.toUpperCase}: " +
        s"source=$sourcePath, dest=$destPath, " +
        s"fastFieldMode=$fastFieldMode, fromVersion=$fromVersion, " +
        s"wherePredicates=${wherePredicates.mkString(",")}, dryRun=$dryRun"
    )

    try
      executeSyncInternal(sparkSession, startTime)
    catch {
      case e: Exception =>
        val durationMs = System.currentTimeMillis() - startTime
        logger.error(s"BUILD COMPANION failed: ${e.getMessage}", e)
        Seq(
          Row(
            destPath,
            sourcePath,
            "error",
            null, // source_version
            0,
            0,
            0,
            0L,
            0L,
            durationMs,
            s"Error: ${e.getMessage}"
          )
        )
    }
  }

  private def executeSyncInternal(sparkSession: SparkSession, startTime: Long): Seq[Row] = {
    // 1. Extract merged configuration and resolve credentials for source table access
    val hadoopConf        = sparkSession.sparkContext.hadoopConfiguration
    val sparkConfigs      = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs     = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val baseMergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs) +
      ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")

    // For Iceberg and Delta + table credential providers: auto-derive catalog config
    // defaults, resolve the table UUID (and storage location for Delta) on the driver,
    // and inject into configs so executors can use the table-based credential API.
    val (mergedConfigs, resolvedStorageLocation) = if (sourceFormat == "iceberg" || sourceFormat == "delta") {
      resolveTableCredentialProvider(baseMergedConfigs) match {
        case Some(provider) if isTableName(sourcePath) =>
          // Auto-derive catalog defaults for Iceberg (URI, token, catalogType)
          val withDefaults = if (sourceFormat == "iceberg") {
            val catalogDefaults = provider.icebergCatalogDefaults(baseMergedConfigs)
            catalogDefaults.foldLeft(baseMergedConfigs) {
              case (acc, (key, value)) =>
                if (acc.contains(key)) acc else acc + (key -> value)
            }
          } else baseMergedConfigs

          val fullTableName = buildFullTableName(effectiveCatalogName, sourcePath)
          logger.info(s"Resolving table info for '$fullTableName' via ${provider.getClass.getName}")
          try {
            val tableInfo = provider.resolveTableInfo(fullTableName, withDefaults)
            // Inject tableId so executors can use table-based credential API (Priority 1.5
            // in CredentialProviderFactory). Key name is spark.indextables.iceberg.uc.tableId
            // for both Iceberg and Delta (internal key, checked generically by the factory).
            val configWithTableId = withDefaults +
              ("spark.indextables.iceberg.uc.tableId" -> tableInfo.tableId)

            if (sourceFormat == "delta" && tableInfo.storageLocation.nonEmpty) {
              (configWithTableId, Some(tableInfo.storageLocation))
            } else {
              (configWithTableId, None)
            }
          } catch {
            case e: Exception =>
              throw new RuntimeException(s"Failed to resolve table info for '$fullTableName': ${e.getMessage}", e)
          }
        case Some(_) =>
          // Provider exists but sourcePath looks like a storage path, not a table name
          (baseMergedConfigs, None)
        case None =>
          (baseMergedConfigs, None)
      }
    } else {
      (baseMergedConfigs, None)
    }

    // Resolve credentials for the SOURCE table (not the destination split table).
    // For Delta UC table name resolution, use the resolved storage location for credential resolution.
    val credentialPath    = resolvedStorageLocation.getOrElse(sourcePath)
    val sourceCredentials = resolveCredentials(mergedConfigs, credentialPath)

    // Create format-specific source reader
    val reader: CompanionSourceReader = sourceFormat match {
      case "delta" =>
        val deltaPath = resolvedStorageLocation.getOrElse(sourcePath)
        new DeltaSourceReader(deltaPath, sourceCredentials)
      case "parquet" =>
        new ParquetDirectoryReader(sourcePath, sourceCredentials, schemaSourcePath)
      case "iceberg" =>
        val icebergConfig = buildIcebergConfig(mergedConfigs, sourceCredentials)
        new IcebergSourceReader(sourcePath, effectiveCatalogName.getOrElse("default"), icebergConfig, fromSnapshot)
      case other =>
        throw new IllegalArgumentException(s"Unsupported source format: $other")
    }

    try
      executeSyncWithReader(sparkSession, reader, mergedConfigs, resolvedStorageLocation, startTime)
    finally
      reader.close()
  }

  /**
   * Check if the credential provider implements the TableCredentialProvider trait. Delegates to
   * CredentialProviderFactory for consistent reflection-based detection.
   */
  private def resolveTableCredentialProvider(
    configs: Map[String, String]
  ): Option[io.indextables.spark.utils.TableCredentialProvider] =
    configs
      .get("spark.indextables.aws.credentialsProviderClass")
      .filter(_.nonEmpty)
      .flatMap(io.indextables.spark.utils.CredentialProviderFactory.resolveTableCredentialProvider)

  /**
   * Determine whether a source path looks like a catalog table name (e.g., "schema.table") vs a storage path (e.g.,
   * "s3://bucket/path" or "/local/path"). Table names don't contain "://" and don't start with "/".
   */
  private def isTableName(path: String): Boolean =
    !path.contains("://") && !path.startsWith("/")

  /**
   * Build the full three-part table name for Unity Catalog table resolution. Format: {catalog}.{sourcePath} where
   * sourcePath is "namespace.table".
   */
  private def buildFullTableName(catalogNameOpt: Option[String], path: String): String =
    catalogNameOpt match {
      case Some(catalog) => s"$catalog.$path"
      case None          => path
    }

  private def executeSyncWithReader(
    sparkSession: SparkSession,
    reader: CompanionSourceReader,
    mergedConfigs: Map[String, String],
    resolvedStorageLocation: Option[String],
    startTime: Long
  ): Seq[Row] = {
    val sourceVersionOpt                  = reader.sourceVersion()
    val sourceVersionLong: java.lang.Long = sourceVersionOpt.map(Long.box).orNull

    // Check for non-existent source (Delta: version -1, Parquet: empty dir, Iceberg: no snapshot)
    val allSourceFiles = reader.getAllFiles()
    if (allSourceFiles.isEmpty && sourceFormat == "delta" && sourceVersionOpt.isEmpty) {
      val durationMs = System.currentTimeMillis() - startTime
      logger.error(s"Source table at $sourcePath does not exist or has no data")
      return Seq(
        Row(
          destPath,
          sourcePath,
          "error",
          null, // source_version
          0,
          0,
          0,
          0L,
          0L,
          durationMs,
          s"Source table at $sourcePath does not exist or has no data"
        )
      )
    }

    // For Iceberg: the storage root is the actual S3/Azure base path. This is needed
    // because sourcePath is a table identifier (e.g., "prod.events"), not an S3 path.
    // The reader computes this internally and returns relative file paths from getAllFiles().
    val icebergStorageRoot: Option[String] = reader.storageRoot()

    val partitionColumns = reader.partitionColumns()
    val sourceSchema     = Some(reader.schema())

    logger.info(
      s"Source table at $sourcePath: version=${sourceVersionOpt.getOrElse("none")}, " +
        s"partitionColumns=${partitionColumns.mkString(",")}, files=${allSourceFiles.size}"
    )

    // 2. DRY RUN mode: compute plan from source table only, no filesystem modifications
    if (dryRun) {
      var allFiles = allSourceFiles
      allFiles = applyWhereFilter(allFiles, partitionColumns, sparkSession, fullSchema = sourceSchema)
      val maxGroupSize = targetInputSize.getOrElse(DEFAULT_TARGET_INPUT_SIZE)
      val groups       = planIndexingGroups(allFiles, maxGroupSize)
      val durationMs   = System.currentTimeMillis() - startTime
      return Seq(
        Row(
          destPath,
          sourcePath,
          "dry_run",
          sourceVersionLong,
          groups.size,
          0,
          allFiles.size,
          0L,
          0L,
          durationMs,
          s"Would create ${groups.size} companion splits from ${allFiles.size} parquet files"
        )
      )
    }

    // 3. Use merged configuration (resolved earlier) for transaction log access

    // Batch size: indexing tasks per group (each group = one Spark job).
    // Default: defaultParallelism to fill the cluster with each group.
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val batchSize = mergedConfigs
      .get("spark.indextables.companion.sync.batchSize")
      .map(_.toInt)
      .getOrElse(defaultParallelism)

    // Max concurrent groups (Spark jobs) running simultaneously.
    val maxConcurrentBatches = mergedConfigs
      .get("spark.indextables.companion.sync.maxConcurrentBatches")
      .map(_.toInt)
      .getOrElse(6)

    // 4. Open IndexTables transaction log at destination
    val destTablePath = new Path(destPath)
    val transactionLog = TransactionLogFactory.create(
      destTablePath,
      sparkSession,
      new CaseInsensitiveStringMap(mergedConfigs.asJava)
    )

    try {
      // 5. Determine sync mode: initial vs incremental
      val (existingFiles, isInitialSync) = determineSyncMode(transactionLog, reader.schema(), partitionColumns)

      // 6. On incremental sync, fall back to stored indexing modes/WHERE if not specified
      val effectiveIndexingModes = if (indexingModes.nonEmpty) {
        indexingModes
      } else if (!isInitialSync) {
        try {
          val existingMeta = transactionLog.getMetadata()
          existingMeta.configuration
            .get("indextables.companion.indexingModes")
            .map { json =>
              import com.fasterxml.jackson.databind.ObjectMapper
              import com.fasterxml.jackson.core.`type`.TypeReference
              val mapper = new ObjectMapper()
              mapper.readValue(json, new TypeReference[java.util.Map[String, String]]() {}).asScala.toMap
            }
            .getOrElse(Map.empty)
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
          existingMeta.configuration
            .get("indextables.companion.whereClause")
            .map(Seq(_))
            .getOrElse(Seq.empty)
        } catch {
          case _: Exception => Seq.empty[String]
        }
      } else {
        Seq.empty[String]
      }

      // 7. Determine what needs indexing via anti-join (works for both initial and incremental).
      // All source readers return relative paths, so the anti-join is bucket-independent
      // and works across cross-region failover (different S3 buckets, same relative paths).
      val (rawParquetFiles, splitsToInvalidate) =
        computeAntiJoinChanges(allSourceFiles, existingFiles, isInitialSync)

      // 7b. Apply WHERE partition filter
      val parquetFilesToIndex =
        applyWhereFilter(rawParquetFiles, partitionColumns, sparkSession, effectiveWherePredicates, sourceSchema)

      // Source version for commits (Delta version, Iceberg snapshot ID, or -1 for Parquet)
      val sourceVersion = sourceVersionOpt.getOrElse(-1L)

      if (parquetFilesToIndex.isEmpty && splitsToInvalidate.isEmpty) {
        val durationMs = System.currentTimeMillis() - startTime
        return Seq(
          Row(
            destPath,
            sourcePath,
            "no_action",
            sourceVersionLong,
            0,
            0,
            0,
            0L,
            0L,
            durationMs,
            "No changes to sync"
          )
        )
      }

      // 8. Plan indexing groups (respecting partition boundaries and target size)
      val maxGroupSize = targetInputSize.getOrElse(DEFAULT_TARGET_INPUT_SIZE)
      val groups       = planIndexingGroups(parquetFilesToIndex, maxGroupSize)

      logger.info(
        s"Sync plan: ${groups.size} indexing groups, " +
          s"${parquetFilesToIndex.size} parquet files, " +
          s"${splitsToInvalidate.size} splits to invalidate"
      )

      // 9. Pass raw merged config to executors for JIT credential resolution.
      // Executors resolve credentials just-in-time before each download/upload via
      // CredentialProviderFactory, ensuring temporary credentials (e.g., Unity Catalog)
      // are always fresh regardless of task queue delay.

      // 9b. Read companion indexing config from Spark properties
      // Priority: SQL clause > spark config > 1GB default
      val resolvedWriterHeapSize = writerHeapSize.getOrElse(
        mergedConfigs
          .get("spark.indextables.companion.writerHeapSize")
          .map(SizeParser.parseSize)
          .getOrElse(DEFAULT_WRITER_HEAP_SIZE)
      )
      val readerBatchSize = mergedConfigs
        .get("spark.indextables.companion.readerBatchSize")
        .map(_.toInt)
        .getOrElse(8192)

      val syncConfig = SyncConfig(
        indexingModes = effectiveIndexingModes,
        fastFieldMode = fastFieldMode,
        storageConfig = mergedConfigs,
        splitTablePath = destPath,
        writerHeapSize = resolvedWriterHeapSize,
        readerBatchSize = readerBatchSize,
        schemaSourceParquetFile = reader.schemaSourceParquetFile(),
        columnNameMapping = reader.columnNameMapping(),
        autoDetectNameMapping = sourceFormat == "iceberg"
      )

      // 10. Dispatch groups as concurrent batches (3 Spark jobs at a time by default)
      // For Delta UC, resolvedStorageLocation provides the actual S3 path (sourcePath is a table name).
      // For Iceberg, icebergStorageRoot provides the S3 path (sourcePath is a table identifier).
      val externalStorageRoot = icebergStorageRoot.orElse(resolvedStorageLocation)
      dispatchSyncTasksBatched(
        sparkSession,
        groups,
        syncConfig,
        transactionLog,
        splitsToInvalidate,
        effectiveIndexingModes,
        effectiveWherePredicates,
        sourceVersion,
        batchSize,
        maxConcurrentBatches,
        externalStorageRoot,
        startTime
      )
    } finally
      transactionLog.close()
  }

  /**
   * Batched concurrent dispatch: indexing tasks are grouped into batches of batchSize, with up to maxConcurrentBatches
   * Spark jobs running at once. Uses Scala parallel collections with ForkJoinTaskSupport to run up to
   * maxConcurrentBatches Spark jobs concurrently (same pattern as MergeSplitsCommand). Sets spark.scheduler.pool per
   * thread so the FAIR scheduler on Databricks actually runs batches concurrently.
   */
  private def dispatchSyncTasksBatched(
    sparkSession: SparkSession,
    groups: Seq[SyncIndexingGroupPlan],
    syncConfig: SyncConfig,
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    splitsToInvalidate: Seq[AddAction],
    effectiveIndexingModes: Map[String, String],
    effectiveWherePredicates: Seq[String],
    sourceVersion: Long,
    batchSize: Int,
    maxConcurrentBatches: Int,
    externalStorageRoot: Option[String],
    startTime: Long
  ): Seq[Row] = {
    import scala.collection.parallel.ForkJoinTaskSupport
    import java.util.concurrent.ForkJoinPool
    import scala.util.{Try, Success, Failure}

    val totalTasks = groups.size

    // Convert plans to serializable groups.
    // For Iceberg, use the actual S3 storage root as parquetTableRoot so that
    // extractRelativePath() can compute correct relative paths (sourcePath is a
    // table identifier like "prod.events", not an S3 path).
    val effectiveParquetTableRoot = externalStorageRoot.getOrElse(sourcePath)
    val syncGroups = groups.zipWithIndex.map {
      case (plan, idx) =>
        SyncIndexingGroup(
          parquetFiles = plan.files.map(f => resolveAbsolutePath(f.path, effectiveParquetTableRoot)),
          parquetTableRoot = effectiveParquetTableRoot,
          partitionValues = plan.partitionValues,
          groupIndex = idx
        )
    }

    // Split into batches of batchSize tasks each
    val batches      = syncGroups.grouped(batchSize).toSeq
    val totalBatches = batches.size

    // Pre-compute per-batch input sizes from the original plans (which have file sizes)
    val batchInputSizes = groups
      .grouped(batchSize)
      .map { batchPlans =>
        val totalFiles = batchPlans.map(_.files.size).sum
        val totalBytes = batchPlans.map(_.files.map(_.size).sum).sum
        (totalFiles, totalBytes)
      }
      .toSeq

    val grandTotalBytes = batchInputSizes.map(_._2).sum
    val grandTotalFiles = batchInputSizes.map(_._1).sum

    logger.info(
      s"Batched sync: $totalTasks indexing tasks in $totalBatches batches " +
        s"($batchSize tasks/batch, $maxConcurrentBatches concurrent), " +
        f"$grandTotalFiles files, ${grandTotalBytes / 1024.0 / 1024.0 / 1024.0}%.2f GB total input"
    )

    // Broadcast config once for all batches
    val broadcastConfig = sparkSession.sparkContext.broadcast(syncConfig)

    // Thread-safe accumulators
    val allResults       = new java.util.concurrent.ConcurrentLinkedQueue[io.indextables.spark.sync.SyncTaskResult]()
    val commitLock       = new Object()
    val removesCommitted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val completedBatches = new java.util.concurrent.atomic.AtomicInteger(0)

    // Define task function OUTSIDE parallel block to avoid closure capture issues
    val taskFunc = (_: org.apache.spark.TaskContext, iter: Iterator[SyncIndexingGroup]) => {
      val group = iter.next()
      SyncTaskExecutor.execute(group, broadcastConfig.value)
    }

    // Process batches with controlled concurrency using Scala parallel collections
    // (same pattern as MergeSplitsCommand)
    val parBatches        = batches.zipWithIndex.par
    val customTaskSupport = new ForkJoinTaskSupport(new ForkJoinPool(maxConcurrentBatches))
    parBatches.tasksupport = customTaskSupport

    try
      parBatches.foreach {
        case (batch, batchIdx) =>
          Try {
            val batchNum                 = batchIdx + 1
            val (batchFiles, batchBytes) = batchInputSizes(batchIdx)
            val batchSizeGB              = batchBytes / 1024.0 / 1024.0 / 1024.0

            // Set job group for Spark UI visibility (thread-local, safe for concurrent use)
            sparkSession.sparkContext.setJobGroup(
              s"tantivy4spark-build-companion-batch-$batchNum",
              f"BUILD COMPANION Batch $batchNum/$totalBatches: ${batch.size} tasks, $batchFiles files ($batchSizeGB%.2f GB)",
              interruptOnCancel = true
            )

            // Set scheduler pool so FAIR scheduler runs batches concurrently.
            // Without this, Spark's default FIFO scheduler serializes all runJob calls.
            val schedulerPool = sparkSession.conf
              .getOption("spark.indextables.companion.schedulerPool")
              .getOrElse("indextables-companion")
            sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", schedulerPool)

            try {
              val batchRDD = sparkSession.sparkContext
                .parallelize(batch, batch.size)
                .setName(
                  f"Building batch $batchNum of $totalBatches: ${batch.size} tasks, $batchFiles files ($batchSizeGB%.2f GB)"
                )

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
              val addActions = batchResults.asScala.toSeq.map(
                _.addAction.copy(
                  companionDeltaVersion = Some(sourceVersion)
                )
              )

              commitLock.synchronized {
                // First batch to commit also includes RemoveActions for invalidated splits
                val removeActions = if (removesCommitted.compareAndSet(false, true)) {
                  buildRemoveActions(splitsToInvalidate)
                } else {
                  Seq.empty
                }

                // Write companion metadata on every batch so that any Avro state
                // written mid-sync contains the full companion config. Without this,
                // a reader in a different process would fail with "parquet_table_root
                // was not set" because intermediate states had no MetadataAction.
                val completed = completedBatches.incrementAndGet()
                val metadataUpdate = Some(
                  buildCompanionMetadata(
                    transactionLog,
                    effectiveIndexingModes,
                    effectiveWherePredicates,
                    sourceVersion,
                    externalStorageRoot
                  )
                )

                if (addActions.nonEmpty || removeActions.nonEmpty || metadataUpdate.isDefined) {
                  val version = transactionLog.commitSyncActions(removeActions, addActions, metadataUpdate)
                  transactionLog.invalidateCache()
                  logger.info(
                    s"Batch $batchNum/$totalBatches committed: " +
                      s"${addActions.size} adds at version $version " +
                      s"($completed/$totalBatches batches complete)"
                  )
                }
              }
            } finally {
              sparkSession.sparkContext.clearJobGroup()
              sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
            }
          } match {
            case Failure(e) => throw e
            case Success(_) => // batch succeeded
          }
      }
    finally {
      customTaskSupport.forkJoinPool.shutdown()
      broadcastConfig.destroy()
    }

    buildResultRow(allResults.asScala.toSeq, splitsToInvalidate.size, sourceVersion, startTime)
  }

  /**
   * Apply WHERE partition filter to CompanionSourceFile list. Uses the explicitly provided predicates, or falls back to
   * effectiveWherePredicates.
   */
  private def applyWhereFilter(
    files: Seq[CompanionSourceFile],
    partitionColumns: Seq[String],
    sparkSession: SparkSession,
    predicates: Seq[String] = Seq.empty,
    fullSchema: Option[StructType] = None
  ): Seq[CompanionSourceFile] = {
    val effectivePreds = if (predicates.nonEmpty) predicates else wherePredicates
    if (effectivePreds.isEmpty || partitionColumns.isEmpty) return files

    val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns, fullSchema)
    val parsedPredicates =
      PartitionPredicateUtils.parseAndValidatePredicates(effectivePreds, partitionSchema, sparkSession)

    val filtered =
      files.filter(f => PartitionPredicateUtils.evaluatePredicates(f.partitionValues, partitionSchema, parsedPredicates))

    if (filtered.size < files.size) {
      logger.info(
        s"WHERE filter: ${files.size} -> ${filtered.size} files " +
          s"(pruned ${files.size - filtered.size})"
      )
    }
    filtered
  }

  /**
   * Build companion metadata configuration for transaction log commit.
   *
   * @param externalStorageRoot
   *   For Iceberg: the S3 storage root from catalog. For Delta UC: the storage_location from UC API. None for
   *   path-based Delta/Parquet.
   */
  private def buildCompanionMetadata(
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    effectiveIndexingModes: Map[String, String],
    effectiveWherePredicates: Seq[String],
    sourceVersion: Long,
    externalStorageRoot: Option[String]
  ): MetadataAction = {
    val existingMetadata = transactionLog.getMetadata()
    val companionConfig = existingMetadata.configuration ++ Map(
      "indextables.companion.enabled"           -> "true",
      "indextables.companion.sourceTablePath"   -> sourcePath,
      "indextables.companion.sourceFormat"      -> sourceFormat,
      "indextables.companion.lastSyncedVersion" -> sourceVersion.toString,
      "indextables.companion.fastFieldMode"     -> fastFieldMode
    ) ++ (if (sourceFormat == "iceberg") {
            Map(
              "indextables.companion.icebergCatalog" -> effectiveCatalogName.getOrElse("default")
            ) ++ effectiveWarehouse.map(w => "indextables.companion.icebergWarehouse" -> w) ++ catalogType.map(ct =>
              "indextables.companion.icebergCatalogType" -> ct
            ) ++ externalStorageRoot.map(root => "indextables.companion.parquetStorageRoot" -> root)
          } else if (sourceFormat == "delta" && isTableName(sourcePath) && externalStorageRoot.isDefined) {
            // Delta UC table name resolution: store metadata for read-path credential resolution
            Map(
              "indextables.companion.parquetStorageRoot" -> externalStorageRoot.get,
              "indextables.companion.deltaTableName"     -> sourcePath,
              "indextables.companion.deltaCatalog"       -> effectiveCatalogName.getOrElse("")
            )
          } else Map.empty) ++ (if (effectiveIndexingModes.nonEmpty) {
                                  import com.fasterxml.jackson.databind.ObjectMapper
                                  val mapper = new ObjectMapper()
                                  Map(
                                    "indextables.companion.indexingModes" -> mapper.writeValueAsString(
                                      effectiveIndexingModes.asJava
                                    )
                                  )
                                } else Map.empty) ++ (if (effectiveWherePredicates.nonEmpty) {
                                                        Map(
                                                          "indextables.companion.whereClause" -> effectiveWherePredicates.head
                                                        )
                                                      } else Map.empty) ++ fromVersion.map(v =>
      "indextables.companion.fromVersion" -> v.toString
    )
    existingMetadata.copy(configuration = companionConfig)
  }

  /** Build RemoveAction entries for invalidated splits. */
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

  /** Build the result row from sync task results. */
  private def buildResultRow(
    results: Seq[io.indextables.spark.sync.SyncTaskResult],
    splitsInvalidated: Int,
    sourceVersion: Long,
    startTime: Long
  ): Seq[Row] = {
    val totalBytesDownloaded = results.map(_.bytesDownloaded).sum
    val totalBytesUploaded   = results.map(_.bytesUploaded).sum
    val totalFilesIndexed    = results.map(_.parquetFilesIndexed).sum
    val durationMs           = System.currentTimeMillis() - startTime

    logger.info(
      s"BUILD COMPANION completed: ${results.length} splits created, " +
        s"$splitsInvalidated invalidated, " +
        s"$totalFilesIndexed files indexed, " +
        s"downloaded $totalBytesDownloaded bytes, uploaded $totalBytesUploaded bytes, " +
        s"duration ${durationMs}ms"
    )

    val versionLong: java.lang.Long = if (sourceVersion >= 0) sourceVersion else null
    Seq(
      Row(
        destPath,
        sourcePath,
        "success",
        versionLong,
        results.length,
        splitsInvalidated,
        totalFilesIndexed,
        totalBytesDownloaded,
        totalBytesUploaded,
        durationMs,
        s"Synced $totalFilesIndexed parquet files into ${results.length} companion splits"
      )
    )
  }

  /**
   * Determine whether this is an initial sync or incremental, by reading the existing transaction log state.
   *
   * The "already synced" check is intentionally omitted here. The anti-join at computeAntiJoinChanges() handles this
   * correctly — if everything is already synced, parquetFilesToIndex will be empty and the command returns "No changes
   * to sync". This avoids a false-positive "already synced" when a previous run committed some batches but not all
   * (partial failure).
   *
   * @return
   *   (existingFiles, isInitialSync)
   */
  private def determineSyncMode(
    transactionLog: io.indextables.spark.transaction.TransactionLog,
    deltaSchema: org.apache.spark.sql.types.StructType,
    partitionColumns: Seq[String]
  ): (Seq[AddAction], Boolean) =
    try {
      transactionLog.getMetadata()
      val files = transactionLog.listFiles()
      logger.info(s"Existing table: ${files.size} files")
      (files, false)
    } catch {
      // Only catch the specific exception thrown by TransactionLog.getMetadata() when no
      // table/metadata exists. All other exceptions (network errors, credential failures,
      // etc.) propagate — this prevents accidentally re-initializing an existing table
      // on a transient failure.
      case e: RuntimeException
          if e.getMessage != null &&
            e.getMessage.contains("No metadata found in transaction log") =>
        logger.info(s"No existing table at $destPath, initializing for initial sync")
        transactionLog.initialize(deltaSchema, partitionColumns)
        (Seq.empty[AddAction], true)
    }

  /**
   * Compute which parquet files need indexing and which companion splits need invalidation, using an anti-join between
   * the current Delta snapshot and existing companion splits.
   *
   * This replaces the changelog-replay approach (getChanges) with a simpler set-reconciliation approach that works with
   * delta-kernel (which has no getChanges API) and is more robust against missed changes.
   */
  private def computeAntiJoinChanges(
    currentSourceFiles: Seq[CompanionSourceFile],
    existingFiles: Seq[AddAction],
    isInitialSync: Boolean
  ): (Seq[CompanionSourceFile], Seq[AddAction]) = {
    if (isInitialSync) {
      logger.info(s"Initial sync: ${currentSourceFiles.size} parquet files to index")
      return (currentSourceFiles, Seq.empty[AddAction])
    }

    val currentSourcePaths = currentSourceFiles.map(f => normalizePath(f.path)).toSet

    // Collect all parquet file paths tracked by existing companion splits.
    // All source readers return relative paths (bucket-independent), and companion
    // splits store the same relative paths. Direct comparison works across
    // cross-region failover (different S3 buckets, same relative paths).
    val companionPaths = existingFiles
      .flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
      .map(normalizePath)
      .toSet

    // Anti-join: files in source but not in any companion split → need indexing
    val filesToIndex = currentSourceFiles.filterNot(f => companionPaths.contains(normalizePath(f.path)))

    // Files tracked by companion splits but no longer in source → gone
    val pathsGoneFromSource = companionPaths -- currentSourcePaths

    // Find companion splits that reference files gone from source → need invalidation
    val splitsToInvalidate = if (pathsGoneFromSource.nonEmpty) {
      existingFiles.filter { split =>
        split.companionSourceFiles.exists(_.exists(f => pathsGoneFromSource.contains(normalizePath(f))))
      }
    } else {
      Seq.empty
    }

    // Collect remaining valid files from invalidated splits that still exist in source,
    // preserving partition values from the parent split for correct grouping.
    // Look up accurate file sizes from the current source snapshot.
    val sourceSizeByPath = currentSourceFiles.map(f => normalizePath(f.path) -> f.size).toMap
    val remainingFromInvalidated = splitsToInvalidate
      .flatMap { split =>
        split.companionSourceFiles
          .getOrElse(Seq.empty)
          .filterNot(f => pathsGoneFromSource.contains(normalizePath(f)))
          .map { relPath =>
            val normalized = normalizePath(relPath)
            val size       = sourceSizeByPath.getOrElse(normalized, 0L)
            CompanionSourceFile(normalized, split.partitionValues, size)
          }
      }
      .groupBy(f => normalizePath(f.path))
      .map(_._2.head)
      .toSeq

    // Merge new files with remaining files from invalidated splits, dedup by path.
    // Prefer the entry with the largest size (Delta files have accurate size, invalidated = 0L).
    val allFilesToIndex = (filesToIndex ++ remainingFromInvalidated)
      .groupBy(f => normalizePath(f.path))
      .map(_._2.maxBy(_.size))
      .toSeq

    logger.info(
      s"Anti-join sync: ${currentSourceFiles.size} in source, " +
        s"${companionPaths.size} in companion, ${filesToIndex.size} new, " +
        s"${pathsGoneFromSource.size} gone, ${splitsToInvalidate.size} splits to invalidate, " +
        s"${remainingFromInvalidated.size} files to re-index from invalidated splits"
    )

    (allFilesToIndex, splitsToInvalidate)
  }

  /** Group parquet files into indexing groups, respecting partition boundaries and target input size. */
  private def planIndexingGroups(
    files: Seq[CompanionSourceFile],
    maxGroupSize: Long
  ): Seq[SyncIndexingGroupPlan] = {
    // Group by partition values
    val byPartition = files.groupBy(_.partitionValues)

    byPartition.flatMap {
      case (partValues, partFiles) =>
        val totalBytes = partFiles.map(_.size).sum
        val numGroups  = math.max(1, math.ceil(totalBytes.toDouble / maxGroupSize).toInt)

        if (numGroups == 1) {
          // All files fit in one group
          Seq(SyncIndexingGroupPlan(partFiles.toSeq, partValues))
        } else {
          // Distribute files evenly: target per group = totalBytes / numGroups
          val targetPerGroup = totalBytes.toDouble / numGroups
          val groups         = Array.fill(numGroups)(scala.collection.mutable.ArrayBuffer[CompanionSourceFile]())
          val groupSizes     = Array.fill(numGroups)(0L)

          // Sort largest files first (First Fit Decreasing) for better bin-packing balance
          partFiles.sortBy(-_.size).foreach { file =>
            // Assign to the group that is most under its target
            // (i.e., the group with the largest remaining capacity relative to target)
            var bestIdx     = 0
            var bestDeficit = groupSizes(0) - targetPerGroup
            var i           = 1
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
            logger.info(
              s"Partition $partValues: $numGroups groups, target ${targetPerGroup / 1024 / 1024}MB each, actual: [$sizesStr]"
            )
          }
          nonEmpty.map(g => SyncIndexingGroupPlan(g.toSeq, partValues)).toSeq
        }
    }.toSeq
  }

  /**
   * Normalize a file path for consistent comparison (Finding #4). Handles URL-encoded paths from Delta log and strips
   * trailing slashes.
   */
  private def normalizePath(path: String): String = {
    val decoded =
      try
        java.net.URLDecoder.decode(path, "UTF-8")
      catch {
        case _: Exception => path
      }
    decoded.stripSuffix("/")
  }

  /** Resolve an absolute path from a potentially relative parquet file path. */
  private def resolveAbsolutePath(path: String, tableRoot: String): String =
    if (
      path.startsWith("s3://") || path.startsWith("s3a://") ||
      path.startsWith("abfss://") || path.startsWith("wasbs://") ||
      path.startsWith("abfs://") || path.startsWith("file:") ||
      path.startsWith("/")
    ) {
      path
    } else {
      s"${tableRoot.stripSuffix("/")}/$path"
    }

  /**
   * Build Iceberg catalog configuration from merged Spark configs and source credentials. Maps
   * spark.indextables.iceberg.* properties to IcebergTableReader config keys.
   */
  private def buildIcebergConfig(
    mergedConfigs: Map[String, String],
    sourceCredentials: Map[String, String]
  ): java.util.Map[String, String] = {
    val config = new java.util.HashMap[String, String]()

    // Catalog config: SQL clause values take precedence over spark.indextables.iceberg.*
    val effectiveCatalogType = catalogType
      .orElse(mergedConfigs.get("spark.indextables.iceberg.catalogType"))
    val effectiveWh = effectiveWarehouse
      .orElse(mergedConfigs.get("spark.indextables.iceberg.warehouse"))

    effectiveCatalogType.foreach(v => config.put("catalog_type", v))
    mergedConfigs.get("spark.indextables.iceberg.uri").foreach(v => config.put("uri", v))
    effectiveWh.foreach(v => config.put("warehouse", v))
    mergedConfigs.get("spark.indextables.iceberg.token").foreach(v => config.put("token", v))
    mergedConfigs.get("spark.indextables.iceberg.credential").foreach(v => config.put("credential", v))

    // S3 storage config
    sourceCredentials.get("spark.indextables.aws.accessKey").foreach(v => config.put("s3.access-key-id", v))
    sourceCredentials.get("spark.indextables.aws.secretKey").foreach(v => config.put("s3.secret-access-key", v))
    sourceCredentials.get("spark.indextables.aws.sessionToken").foreach(v => config.put("s3.session-token", v))
    sourceCredentials.get("spark.indextables.aws.region").foreach(v => config.put("s3.region", v))
    mergedConfigs.get("spark.indextables.iceberg.s3Endpoint").foreach(v => config.put("s3.endpoint", v))
    mergedConfigs
      .get("spark.indextables.iceberg.s3PathStyleAccess")
      .foreach(v => config.put("s3.path-style-access", v))

    // Azure storage credentials
    sourceCredentials.get("spark.indextables.azure.accountName").foreach(v => config.put("adls.account-name", v))
    sourceCredentials.get("spark.indextables.azure.accountKey").foreach(v => config.put("adls.account-key", v))

    config
  }

  /**
   * Resolve credentials for a given storage path. Returns a flat map of credential properties suitable for immediate
   * use.
   *
   * Note: This is used only for driver-side operations (e.g., DeltaLogReader). Executor credential resolution happens
   * JIT via CredentialProviderFactory.resolveAWSCredentialsFromConfig() using the raw storageConfig passed through
   * SyncConfig.
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
    configs.get("spark.indextables.aws.region").foreach(r => result += ("spark.indextables.aws.region" -> r))

    // Copy Azure credentials if present
    configs.foreach {
      case (k, v) =>
        if (
          k.startsWith("spark.indextables.azure.") ||
          k.startsWith("fs.azure.") ||
          k.startsWith("fs.abfss.")
        ) {
          result += (k -> v)
        }
    }

    result.toMap
  }
}

/** Internal grouping plan for BUILD COMPANION operation (driver-side only, not serialized). */
private[sql] case class SyncIndexingGroupPlan(
  files: Seq[CompanionSourceFile],
  partitionValues: Map[String, String])
