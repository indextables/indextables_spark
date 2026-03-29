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

package io.indextables.spark.transaction

import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Try

import io.indextables.spark.util.JsonUtil
import io.indextables.spark.arrow.ArrowFfiBridge
import io.indextables.spark.stats.DataSkippingMetrics
import io.indextables.jni.txlog.{TransactionLogReader, TransactionLogWriter, TxLogSnapshotInfo, WriteResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.slf4j.LoggerFactory

/**
 * Transaction log implementation backed by tantivy4java's native Rust txlog module via JNI.
 *
 * Delegates all I/O to `TransactionLogReader` and `TransactionLogWriter`, which handle:
 *   - Optimistic concurrency with automatic retry
 *   - Avro state format (checkpoints, manifests)
 *   - GZIP compression
 *   - Distributed scanning (getSnapshotInfo + readManifest + readPostCheckpointChanges)
 *   - Internal LRU caching with configurable TTL
 *   - Auto-checkpoint based on configurable interval
 *   - Graceful fallback when no checkpoint exists (version file scanning)
 *
 * This replaces the Scala TransactionLog and OptimizedTransactionLog implementations.
 */
class NativeTransactionLog(
  private val tablePath: Path,
  private val options: CaseInsensitiveStringMap)
    extends TransactionLogInterface {

  private val logger = LoggerFactory.getLogger(classOf[NativeTransactionLog])

  private val mapper = JsonUtil.mapper

  /** Native table path string (with scheme normalization) */
  private val nativeTablePath: String = ConfigMapper.normalizeTablePath(tablePath)

  /** Native config map (credentials + cache TTL + checkpoint interval) */
  private val nativeConfig: java.util.Map[String, String] = {
    val config = ConfigMapper.toNativeConfig(options)
    // Pass cache TTL from Spark config to native layer
    val cacheTtlMs = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 300L) * 1000L
    config.put("cache.ttl.ms", cacheTtlMs.toString)
    // Pass checkpoint interval to native layer for auto-checkpoint
    val checkpointInterval = options.getInt("spark.indextables.checkpoint.interval", 10)
    config.put("checkpoint_interval", checkpointInterval.toString)
    config
  }

  /** Last retry metrics from the most recent write operation */
  @volatile private var lastRetryMetrics: Option[TxRetryMetrics] = None

  /** Cached parsed table schema — invalidated on cache invalidation */
  @volatile private var cachedParsedSchema: Option[StructType] = null // null = not yet computed

  // ------------------------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------------------------

  override def close(): Unit = {}

  override def getTablePath(): Path = tablePath

  // ------------------------------------------------------------------------------------
  // Initialization
  // ------------------------------------------------------------------------------------

  override def initialize(schema: StructType): Unit =
    initialize(schema, Seq.empty)

  override def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
    // Validate partition columns exist in schema
    val schemaFieldNames = schema.fieldNames.toSet
    partitionColumns.foreach { col =>
      if (!schemaFieldNames.contains(col)) {
        throw new IllegalArgumentException(
          s"Partition column '$col' not found in schema. Available columns: ${schemaFieldNames.mkString(", ")}"
        )
      }
    }

    // Idempotent: skip if already initialized
    val snapshot = getOrRefreshSnapshot()
    if (snapshot != null) {
      logger.debug(s"Table at $nativeTablePath already initialized, skipping")
      return
    }

    val protocol = ProtocolVersion.defaultProtocol()
    val metadata = MetadataAction(
      id = UUID.randomUUID().toString,
      name = None,
      description = None,
      format = FileFormat("indextables", Map.empty),
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      configuration = Map.empty,
      createdTime = Some(System.currentTimeMillis())
    )

    val protocolJson = ActionJsonSerializer.protocolToJson(protocol)
    val metadataJson = ActionJsonSerializer.metadataToJson(metadata)

    TransactionLogWriter.initializeTable(nativeTablePath, nativeConfig, protocolJson, metadataJson)
    logger.info(s"Initialized table at $nativeTablePath with ${partitionColumns.size} partition columns")
  }

  // ------------------------------------------------------------------------------------
  // Write Operations
  // ------------------------------------------------------------------------------------

  override def addFiles(addActions: Seq[AddAction]): Long = {
    assertTableWritable()
    val result = writeActionsViaArrow(addActions, retry = true)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def overwriteFiles(addActions: Seq[AddAction]): Long = {
    assertTableWritable()
    // overwriteFiles must re-read the file list on each retry attempt to capture
    // files added by concurrent writers. Use single-attempt in a manual retry loop.
    retryWithBackoff("overwrite files") { () =>
      val currentFiles = listFiles()
      val removeActions = currentFiles.map { f =>
        RemoveAction(
          path = f.path,
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = true,
          extendedFileMetadata = None,
          partitionValues = Some(f.partitionValues),
          size = Some(f.size),
          tags = None
        )
      }
      val actions: Seq[Action] = removeActions ++ addActions
      writeActionsViaArrowOnce(actions)
    }
  }

  override def removeFile(path: String, deletionTimestamp: Long): Long = {
    val removeAction = RemoveAction(
      path = path,
      deletionTimestamp = Some(deletionTimestamp),
      dataChange = true,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None,
      tags = None
    )
    val result = writeActionsViaArrow(Seq(removeAction), retry = true)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val actions: Seq[Action] = removeActions ++ addActions
    val result = writeActionsViaArrow(actions, retry = true)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitSyncActions(
    removeActions: Seq[RemoveAction],
    addActions: Seq[AddAction],
    metadataUpdate: Option[MetadataAction]
  ): Long = {
    val actions: Seq[Action] = removeActions ++ addActions ++ metadataUpdate.toSeq
    val result = writeActionsViaArrow(actions, retry = true)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitMetadataUpdate(transform: MetadataAction => MetadataAction): Long =
    // Must re-read metadata on each retry to compose safely with concurrent updates
    retryWithBackoff("commit metadata update") { () =>
      val currentMetadata = getMetadata()
      val updatedMetadata = transform(currentMetadata)
      writeActionsViaArrowOnce(Seq(updatedMetadata))
    }

  override def commitRemoveActions(removeActions: Seq[RemoveAction]): Long = {
    val result = writeActionsViaArrow(removeActions, retry = true)
    recordRetryMetrics(result)
    result.getVersion
  }

  /** Write actions via Arrow FFI (unified schema with action_type discriminator). */
  private def writeActionsViaArrow(actions: Seq[Action], retry: Boolean): WriteResult = {
    val (arrowArray, arrowSchema, arrayAddr, schemaAddr) = ActionsToArrowConverter.exportAsFfi(actions)
    try {
      TransactionLogWriter.writeVersionArrowFfi(nativeTablePath, nativeConfig, arrayAddr, schemaAddr, retry)
    } finally {
      arrowArray.close()
      arrowSchema.close()
    }
  }

  /** Write actions via Arrow FFI, single attempt (for retryWithBackoff loops). */
  private def writeActionsViaArrowOnce(actions: Seq[Action]): WriteResult =
    writeActionsViaArrow(actions, retry = false)

  override def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit = {
    val autoUpgrade = options.getBoolean(ProtocolVersion.PROTOCOL_AUTO_UPGRADE, true)
    if (!autoUpgrade) {
      logger.debug("Protocol auto-upgrade disabled via configuration, skipping")
      return
    }
    val current       = getProtocol()
    val effectiveReader = math.max(current.minReaderVersion, newMinReaderVersion)
    val effectiveWriter = math.max(current.minWriterVersion, newMinWriterVersion)
    if (effectiveReader == current.minReaderVersion && effectiveWriter == current.minWriterVersion) {
      logger.debug(s"Protocol already at $effectiveReader/$effectiveWriter, skipping upgrade")
      return
    }
    val protocol    = ProtocolAction(effectiveReader, effectiveWriter)
    val actionsJson = ActionJsonSerializer.actionsToJsonLines(Seq(protocol))
    TransactionLogWriter.writeVersion(nativeTablePath, nativeConfig, actionsJson)
  }

  // ------------------------------------------------------------------------------------
  // Read Operations
  // ------------------------------------------------------------------------------------

  override def listFiles(): Seq[AddAction] =
    // Include stats — admin commands, statistics, REPAIR need minValues/maxValues
    listFilesArrow(
      partitionFilters = null,
      dataFilters = null,
      excludeCooldown = false,
      includeStats = true
    ).files

  override def listFilesWithPartitionFilters(partitionFilters: Seq[Filter]): Seq[AddAction] =
    listFilesWithAllFilters(partitionFilters, Seq.empty)

  override def listFilesWithAllFilters(partitionFilters: Seq[Filter], dataFilters: Seq[Filter]): Seq[AddAction] = {
    // Data skipping already applied natively — no need to export stats back to JVM
    listFilesArrow(
      partitionFilters = SparkFilterToNativeFilter.convertOrNull(partitionFilters),
      dataFilters = SparkFilterToNativeFilter.convertOrNull(dataFilters),
      excludeCooldown = false,
      includeStats = false
    ).files
  }

  /**
   * List files with all filtering applied natively in a single JNI call.
   * Returns files + table metadata + filtering metrics.
   *
   * Replaces the old multi-step pipeline:
   * getSnapshotInfo → readManifest × N → readPostCheckpointChanges →
   * JVM log replay → partition pruning → data skipping → cooldown filtering → schema restore
   */
  def listFilesWithMetadata(
    partitionFilters: Seq[Filter],
    dataFilters: Seq[Filter],
    excludeCooldown: Boolean
  ): NativeListFilesResult =
    listFilesArrow(
      partitionFilters = SparkFilterToNativeFilter.convertOrNull(partitionFilters),
      dataFilters = SparkFilterToNativeFilter.convertOrNull(dataFilters),
      excludeCooldown = excludeCooldown,
      includeStats = false // scan path — data skipping already applied natively
    )

  /**
   * List files excluding those in cooldown/skip state.
   * Replaces the old pattern: listFiles() then filterFilesInCooldown().
   */
  def listFilesExcludingCooldown(filters: Seq[Filter] = Seq.empty): Seq[AddAction] = {
    listFilesArrow(
      partitionFilters = SparkFilterToNativeFilter.convertOrNull(filters),
      dataFilters = null,
      excludeCooldown = true,
      includeStats = true // merge path needs full metadata
    ).files
  }

  private def listFilesArrow(
    partitionFilters: String,
    dataFilters: String,
    excludeCooldown: Boolean,
    includeStats: Boolean
  ): NativeListFilesResult = {
    // Allocate Arrow FFI structs — dynamic based on partition columns + stats
    val partColCount = try { getPartitionColumns().size } catch { case _: Exception => 0 }
    val maxCols = 19 + partColCount + (if (includeStats) 2 else 0)
    val bridge = new ArrowFfiBridge()
    try {
      val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(maxCols)

      val resultJson = try {
        TransactionLogReader.listFilesArrowFfi(
          nativeTablePath, nativeConfig,
          partitionFilters,
          dataFilters,
          excludeCooldown,
          includeStats,
          arrayAddrs, schemaAddrs
        )
      } catch {
        case e: RuntimeException if e.getMessage != null && e.getMessage.contains("not initialized") =>
          logger.debug(s"Table not yet initialized at $nativeTablePath")
          null
      }

      if (resultJson == null) {
        closeUnusedStructs(arrays, schemas, 0)
        return NativeListFilesResult(
          files = Seq.empty,
          schema = None,
          partitionColumns = Seq.empty,
          protocol = ProtocolVersion.defaultProtocol(),
          metadataConfig = Map.empty,
          metrics = NativeFilteringMetrics(0, 0, 0, 0, 0, 0)
        )
      }

      // Parse result metadata
      val resultNode = mapper.readTree(resultJson)
      val numRows = resultNode.get("numRows").asLong()
      val numColumns = resultNode.get("numColumns").asInt()

      // Extract table metadata (eliminates separate getSchema/getPartitionColumns/getProtocol calls)
      val schemaJson = if (resultNode.has("schemaJson") && !resultNode.get("schemaJson").isNull)
        resultNode.get("schemaJson").asText() else null
      val schema = if (schemaJson != null && schemaJson.nonEmpty) {
        try {
          Some(DataType.fromJson(schemaJson).asInstanceOf[StructType])
        } catch {
          case _: Exception =>
            logger.debug(s"Could not parse schemaJson from native, falling back to getSchema()")
            getSchema()
        }
      } else getSchema()

      val partitionColumns = if (resultNode.has("partitionColumns")) {
        val arr = resultNode.get("partitionColumns")
        (0 until arr.size()).map(i => arr.get(i).asText()).toSeq
      } else Seq.empty

      val protocol = if (resultNode.has("protocolJson") && !resultNode.get("protocolJson").isNull) {
        mapper.readValue(resultNode.get("protocolJson").asText(), classOf[ProtocolAction])
      } else ProtocolVersion.defaultProtocol()

      val metadataConfig = if (resultNode.has("metadataConfigJson") && !resultNode.get("metadataConfigJson").isNull) {
        val configNode = mapper.readTree(resultNode.get("metadataConfigJson").asText())
        val entries = scala.collection.mutable.Map[String, String]()
        val it = configNode.fields()
        while (it.hasNext) {
          val entry = it.next()
          entries.put(entry.getKey, entry.getValue.asText())
        }
        entries.toMap
      } else Map.empty[String, String]

      // Extract filtering metrics
      val metricsNode = resultNode.get("metrics")
      val metrics = if (metricsNode != null) {
        NativeFilteringMetrics(
          totalFilesBeforeFiltering = metricsNode.get("totalFilesBeforeFiltering").asLong(),
          filesAfterPartitionPruning = metricsNode.get("filesAfterPartitionPruning").asLong(),
          filesAfterDataSkipping = metricsNode.get("filesAfterDataSkipping").asLong(),
          filesAfterCooldownFiltering = metricsNode.get("filesAfterCooldownFiltering").asLong(),
          manifestsTotal = metricsNode.get("manifestsTotal").asLong(),
          manifestsPruned = metricsNode.get("manifestsPruned").asLong()
        )
      } else NativeFilteringMetrics(0, 0, 0, 0, 0, 0)

      // Import Arrow batch and extract AddAction objects (only for surviving files)
      // Close unused Arrow structs that native didn't fill
      val usedCols = if (numRows > 0 && numColumns > 0) numColumns else 0
      closeUnusedStructs(arrays, schemas, usedCols)

      val files = if (usedCols > 0) {
        require(numRows <= Int.MaxValue, s"numRows $numRows exceeds Int.MaxValue")
        val batch = bridge.importAsColumnarBatch(
          arrays.take(numColumns),
          schemas.take(numColumns),
          numRows.toInt
        )
        try {
          ArrowFileEntryExtractor.extract(batch, partitionColumns)
        } finally {
          batch.close()
        }
      } else {
        Seq.empty
      }

      NativeListFilesResult(
        files = files,
        schema = schema,
        partitionColumns = partitionColumns,
        protocol = protocol,
        metadataConfig = metadataConfig,
        metrics = metrics
      )
    } finally {
      bridge.close()
    }
  }

  override def getTotalRowCount(): Long =
    listFiles().flatMap(_.numRecords).sum

  override def getSchema(): Option[StructType] = {
    val cached = cachedParsedSchema
    if (cached != null) return cached
    val result = withSnapshot[Option[StructType]](None) { snapshot =>
      val metadataJson = snapshot.getMetadataJson
      if (metadataJson == null || metadataJson.isEmpty) return None
      val metadata = parseMetadataJson(metadataJson)
      if (metadata.schemaString == null || metadata.schemaString.isEmpty) None
      else Some(DataType.fromJson(metadata.schemaString).asInstanceOf[StructType])
    }
    cachedParsedSchema = result
    result
  }

  override def getPartitionColumns(): Seq[String] = withSnapshot[Seq[String]](Seq.empty) { snapshot =>
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) return Seq.empty
    parseMetadataJson(metadataJson).partitionColumns
  }

  override def isPartitioned(): Boolean =
    getPartitionColumns().nonEmpty

  override def getMetadata(): MetadataAction = withSnapshot[MetadataAction](
    throw new RuntimeException(s"No metadata found in transaction log for $nativeTablePath")
  ) { snapshot =>
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) {
      throw new RuntimeException(s"No metadata found in transaction log for $nativeTablePath")
    }
    parseMetadataJson(metadataJson)
  }

  override def getProtocol(): ProtocolAction = withSnapshot(ProtocolVersion.defaultProtocol()) { snapshot =>
    val protocolJson = snapshot.getProtocolJson
    if (protocolJson == null || protocolJson.isEmpty) ProtocolVersion.legacyProtocol()
    else mapper.readValue(protocolJson, classOf[ProtocolAction])
  }

  override def assertTableReadable(): Unit = {
    val checkEnabled = options.getBoolean(ProtocolVersion.PROTOCOL_CHECK_ENABLED, true)
    if (checkEnabled) ProtocolVersion.validateReaderVersion(getProtocol())
  }

  override def assertTableWritable(): Unit = {
    val checkEnabled = options.getBoolean(ProtocolVersion.PROTOCOL_CHECK_ENABLED, true)
    if (checkEnabled) ProtocolVersion.validateWriterVersion(getProtocol())
  }

  override def getCheckpointActions(): Option[Seq[Action]] = {
    // Use listFiles (Arrow FFI path with stats) for file entries
    val addActions = listFiles()
    if (addActions.isEmpty) return None

    // Include protocol and metadata actions alongside file entries,
    // since checkpoint represents the complete consolidated state.
    // Note: RemoveActions and SkipActions are not included — they are
    // only visible through readVersion() for individual version files.
    val protocol = getProtocol()
    val metadata = getMetadata()
    Some(Seq(protocol, metadata) ++ addActions)
  }

  override def getVersions(): Seq[Long] =
    TransactionLogReader.listVersions(nativeTablePath, nativeConfig).toSeq

  override def readVersion(version: Long): Seq[Action] = {
    val content = TransactionLogReader.readVersion(nativeTablePath, nativeConfig, version)
    parseActionsFromContent(content)
  }

  // ------------------------------------------------------------------------------------
  // Cooldown / Skip Operations
  // ------------------------------------------------------------------------------------

  override def recordSkippedFile(
    filePath: String,
    reason: String,
    operation: String,
    partitionValues: Option[Map[String, String]] = None,
    size: Option[Long] = None,
    cooldownHours: Int = 24
  ): Long = {
    // Check existing skip actions to increment skipCount
    val existingCount = getSkippedFiles().filter(_.path == filePath).map(_.skipCount).sum
    val skipAction = SkipAction(
      path = filePath,
      skipTimestamp = System.currentTimeMillis(),
      reason = reason,
      operation = operation,
      partitionValues = partitionValues,
      size = size,
      retryAfter = Some(System.currentTimeMillis() + (cooldownHours * 3600 * 1000L)),
      skipCount = existingCount + 1
    )
    val skipJson = ActionJsonSerializer.skipActionToJson(skipAction)
    val version  = TransactionLogWriter.skipFile(nativeTablePath, nativeConfig, skipJson)
    version
  }

  override def getSkippedFiles(): Seq[SkipAction] = {
    // Use native listSkipActions which scans version files backward from latest,
    // independent of checkpoint state. This works with checkpoint-every-write where
    // postCheckpointPaths is always empty.
    val cooldownMs = options.getLong("spark.indextables.skippedFiles.cooldownDuration", 24L) * 3600 * 1000
    val nativeSkips = TransactionLogReader.listSkipActions(nativeTablePath, nativeConfig, cooldownMs)

    import scala.jdk.CollectionConverters._
    nativeSkips.asScala.map { s =>
      SkipAction(
        path = s.getPath,
        skipTimestamp = s.getSkipTimestamp,
        reason = Option(s.getReason).getOrElse(""),
        operation = Option(s.getOperation).getOrElse(""),
        partitionValues = if (s.getPartitionValues.isEmpty) None else Some(s.getPartitionValues.asScala.toMap),
        size = if (s.getSize >= 0) Some(s.getSize) else None,
        retryAfter = if (s.getRetryAfter > 0) Some(s.getRetryAfter) else None,
        skipCount = s.getSkipCount
      )
    }.toSeq
  }

  override def getFilesInCooldown(): Map[String, Long] =
    getSkippedFiles()
      .filter(s => s.retryAfter.exists(_ > System.currentTimeMillis()))
      .map(s => s.path -> s.retryAfter.getOrElse(0L))
      .toMap

  override def filterFilesInCooldown(candidateFiles: Seq[AddAction]): Seq[AddAction] = {
    val cooldownPaths = getFilesInCooldown().keySet
    if (cooldownPaths.isEmpty) candidateFiles
    else candidateFiles.filterNot(f => cooldownPaths.contains(f.path))
  }

  // ------------------------------------------------------------------------------------
  // Cache Management
  // ------------------------------------------------------------------------------------

  override def invalidateCache(): Unit = {
    cachedParsedSchema = null
    TransactionLogReader.invalidateCache(nativeTablePath)
  }

  override def getCacheStats(): Option[CacheStats] = {
    val expirationSecs = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 300L)
    Some(CacheStats(hits = 0, misses = 0, hitRate = 0.0, versionsInCache = 0, expirationSeconds = expirationSecs))
  }

  override def getLastRetryMetrics(): Option[TxRetryMetrics] = lastRetryMetrics

  override def getLastCheckpointVersion(): Option[Long] = withSnapshot[Option[Long]](None) { snapshot =>
    if (snapshot.getCheckpointVersion >= 0) Some(snapshot.getCheckpointVersion) else None
  }

  // ------------------------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------------------------

  /**
   * Get the current snapshot from the native layer. Returns null if the table is not yet
   * initialized (e.g., during the write path before any data is committed).
   *
   * Caching is handled entirely by the native layer's global CACHE_REGISTRY, which is
   * automatically invalidated by write operations across all instances.
   */
  private def getOrRefreshSnapshot(): TxLogSnapshotInfo =
    try {
      TransactionLogReader.getSnapshotInfo(nativeTablePath, nativeConfig)
    } catch {
      case e: RuntimeException if e.getMessage != null && e.getMessage.contains("not initialized") =>
        logger.debug(s"Table not yet initialized at $nativeTablePath")
        null
    }

  /** Execute a function with the current snapshot, returning a default if no snapshot exists. */
  private def withSnapshot[T](default: => T)(f: TxLogSnapshotInfo => T): T = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) default else f(snapshot)
  }

  /**
   * Retry an operation with exponential backoff. The operation is called on each attempt
   * and should return a WriteResult from writeVersionOnce.
   */
  private def retryWithBackoff(operationName: String)(operation: () => WriteResult): Long = {
    val maxAttempts = options.getInt("spark.indextables.state.retry.maxAttempts", 10)
    var attempt     = 0
    var conflicts   = Seq.empty[Long]

    while (attempt < maxAttempts) {
      attempt += 1
      val result = operation()

      if (result.getVersion >= 0) {
        lastRetryMetrics = Some(TxRetryMetrics(attempt, conflicts.size, result.getVersion, conflicts))
        return result.getVersion
      }

      conflicts = conflicts ++ result.getConflictedVersions.asScala.map(_.toLong)

      if (attempt < maxAttempts) {
        val delay = Math.min(100L * (1L << (attempt - 1)), 5000L)
        Thread.sleep(delay)
      }
    }

    throw new TransactionConflictException(
      s"Failed to $operationName after $maxAttempts attempts",
      -1,
      maxAttempts
    )
  }

  private def recordRetryMetrics(result: WriteResult): Unit = {
    lastRetryMetrics = Some(
      TxRetryMetrics(
        attemptsMade = result.getRetries + 1,
        conflictsEncountered = result.getConflictedVersions.size(),
        finalVersion = result.getVersion,
        conflictedVersions = result.getConflictedVersions.asScala.map(_.toLong).toSeq
      )
    )
  }

  private def parseMetadataJson(metadataJson: String): MetadataAction =
    mapper.readValue(metadataJson, classOf[MetadataAction])

  /** Close Arrow FFI structs that weren't consumed by importAsColumnarBatch. */
  private def closeUnusedStructs(
    arrays: Array[org.apache.arrow.c.ArrowArray],
    schemas: Array[org.apache.arrow.c.ArrowSchema],
    usedCount: Int
  ): Unit = {
    var i = usedCount
    while (i < arrays.length) {
      try { arrays(i).close() } catch { case _: Exception => }
      try { schemas(i).close() } catch { case _: Exception => }
      i += 1
    }
  }

  // extractMetadataConfigJson and restoreSchemas removed — native listFilesArrowFfi
  // handles schema deduplication restoration (step 8 in the developer guide).

  private def parseActionsFromContent(content: String): Seq[Action] =
    content
      .split("\n")
      .filter(_.nonEmpty)
      .flatMap { line =>
        Try {
          val jsonNode = mapper.readTree(line)
          ActionJsonSerializer.parseActionFromJsonNode(jsonNode)
        }.toOption.flatten.toSeq
      }
      .toSeq
}
