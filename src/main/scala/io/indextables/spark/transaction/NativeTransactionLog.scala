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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.indextables.jni.txlog.{TransactionLogReader, TransactionLogWriter, TxLogSnapshotInfo, WriteResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
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

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

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
    val addsJson = ActionJsonSerializer.addActionsToJson(addActions)
    val result   = TransactionLogWriter.addFiles(nativeTablePath, nativeConfig, addsJson)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def overwriteFiles(addActions: Seq[AddAction]): Long = {
    assertTableWritable()
    // overwriteFiles must re-read the file list on each retry attempt to capture
    // files added by concurrent writers. Use writeVersionOnce in a manual retry loop.
    val maxAttempts = options.getInt("spark.indextables.state.retry.maxAttempts", 10)
    var attempt     = 0
    var conflicts   = Seq.empty[Long]

    while (attempt < maxAttempts) {
      attempt += 1

      // Read current visible files
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
      val actionsJson          = ActionJsonSerializer.actionsToJsonLines(actions)
      val result               = TransactionLogWriter.writeVersionOnce(nativeTablePath, nativeConfig, actionsJson)

      if (result.getVersion >= 0) {
        lastRetryMetrics = Some(TxRetryMetrics(attempt, conflicts.size, result.getVersion, conflicts))
        return result.getVersion
      }

      // Conflict — re-read and retry
      conflicts = conflicts ++ result.getConflictedVersions.asScala.map(_.toLong)

      if (attempt < maxAttempts) {
        val delay = Math.min(100L * (1L << (attempt - 1)), 5000L)
        Thread.sleep(delay)
      }
    }

    throw new TransactionConflictException(
      s"Failed to overwrite files after $maxAttempts attempts",
      -1,
      maxAttempts
    )
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
    val actionsJson = ActionJsonSerializer.actionsToJsonLines(Seq(removeAction))
    val result      = TransactionLogWriter.writeVersion(nativeTablePath, nativeConfig, actionsJson)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val actions: Seq[Action] = removeActions ++ addActions
    val actionsJson          = ActionJsonSerializer.actionsToJsonLines(actions)
    val result               = TransactionLogWriter.writeVersion(nativeTablePath, nativeConfig, actionsJson)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitSyncActions(
    removeActions: Seq[RemoveAction],
    addActions: Seq[AddAction],
    metadataUpdate: Option[MetadataAction]
  ): Long = {
    val actions: Seq[Action] = removeActions ++ addActions ++ metadataUpdate.toSeq
    val actionsJson          = ActionJsonSerializer.actionsToJsonLines(actions)
    val result               = TransactionLogWriter.writeVersion(nativeTablePath, nativeConfig, actionsJson)
    recordRetryMetrics(result)
    result.getVersion
  }

  override def commitMetadataUpdate(transform: MetadataAction => MetadataAction): Long = {
    // Must re-read metadata on each retry to compose safely with concurrent updates
    val maxAttempts = options.getInt("spark.indextables.state.retry.maxAttempts", 10)
    var attempt     = 0
    var conflicts   = Seq.empty[Long]

    while (attempt < maxAttempts) {
      attempt += 1

      val currentMetadata = getMetadata()
      val updatedMetadata = transform(currentMetadata)
      val actionsJson     = ActionJsonSerializer.actionsToJsonLines(Seq(updatedMetadata))
      val result          = TransactionLogWriter.writeVersionOnce(nativeTablePath, nativeConfig, actionsJson)

      if (result.getVersion >= 0) {
        lastRetryMetrics = Some(TxRetryMetrics(attempt, conflicts.size, result.getVersion, conflicts))
        return result.getVersion
      }

      // Conflict — re-read metadata and retry
      conflicts = conflicts ++ result.getConflictedVersions.asScala.map(_.toLong)

      if (attempt < maxAttempts) {
        val delay = Math.min(100L * (1L << (attempt - 1)), 5000L)
        Thread.sleep(delay)
      }
    }

    throw new TransactionConflictException(
      s"Failed to commit metadata update after $maxAttempts attempts",
      -1,
      maxAttempts
    )
  }

  override def commitRemoveActions(removeActions: Seq[RemoveAction]): Long = {
    val actionsJson = ActionJsonSerializer.actionsToJsonLines(removeActions)
    val result      = TransactionLogWriter.writeVersion(nativeTablePath, nativeConfig, actionsJson)
    recordRetryMetrics(result)
    result.getVersion
  }

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

  override def listFiles(): Seq[AddAction] = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return Seq.empty
    assertTableReadable()
    val metadataConfigJson = extractMetadataConfigJson(snapshot)

    // Read manifests (checkpoint state)
    val manifestEntries = snapshot.getManifestPaths.asScala.flatMap { manifestPath =>
      TransactionLogReader
        .readManifest(nativeTablePath, nativeConfig, snapshot.getStateDir, manifestPath, metadataConfigJson)
        .asScala
    }

    // Read post-checkpoint changes
    val postCheckpointPaths = snapshot.getPostCheckpointPaths
    val changes = if (postCheckpointPaths.isEmpty) {
      None
    } else {
      val versionPathsJson = mapper.writeValueAsString(postCheckpointPaths)
      Some(TransactionLogReader.readPostCheckpointChanges(nativeTablePath, nativeConfig, versionPathsJson, metadataConfigJson))
    }

    // Merge: checkpoint entries + added files - removed paths
    val checkpointFiles = manifestEntries.map(AddActionConverter.toAddAction)
    val addedFiles      = changes.map(c => AddActionConverter.toAddActions(c.getAddedFiles)).getOrElse(Seq.empty)
    val removedPaths    = changes.map(_.getRemovedPaths.asScala.toSet).getOrElse(Set.empty)

    val allFiles = (checkpointFiles ++ addedFiles).filterNot(f => removedPaths.contains(f.path))

    // Restore schemas via deduplication registry
    restoreSchemas(allFiles, snapshot)
  }

  override def listFilesWithPartitionFilters(partitionFilters: Seq[Filter]): Seq[AddAction] = {
    val allFiles = listFiles()
    if (partitionFilters.isEmpty) return allFiles

    val partitionColumns = getPartitionColumns()
    if (partitionColumns.isEmpty) return allFiles

    PartitionPruning.prunePartitions(allFiles, partitionColumns, partitionFilters.toArray)
  }

  override def getTotalRowCount(): Long =
    listFiles().flatMap(_.numRecords).sum

  override def getSchema(): Option[StructType] = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return None
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) return None
    val metadata = parseMetadataJson(metadataJson)
    if (metadata.schemaString == null || metadata.schemaString.isEmpty) None
    else Some(DataType.fromJson(metadata.schemaString).asInstanceOf[StructType])
  }

  override def getPartitionColumns(): Seq[String] = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return Seq.empty
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) return Seq.empty
    parseMetadataJson(metadataJson).partitionColumns
  }

  override def isPartitioned(): Boolean =
    getPartitionColumns().nonEmpty

  override def getMetadata(): MetadataAction = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) {
      throw new RuntimeException(s"No metadata found in transaction log for $nativeTablePath")
    }
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) {
      throw new RuntimeException(s"No metadata found in transaction log for $nativeTablePath")
    }
    parseMetadataJson(metadataJson)
  }

  override def getProtocol(): ProtocolAction = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return ProtocolVersion.defaultProtocol()
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
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return None
    if (snapshot.getManifestPaths.isEmpty) return None

    val metadataConfigJson = extractMetadataConfigJson(snapshot)
    val entries = snapshot.getManifestPaths.asScala.flatMap { manifestPath =>
      TransactionLogReader
        .readManifest(nativeTablePath, nativeConfig, snapshot.getStateDir, manifestPath, metadataConfigJson)
        .asScala
    }

    val addActions = entries.map(AddActionConverter.toAddAction).toSeq

    // Include protocol and metadata actions alongside file entries,
    // since checkpoint represents the complete consolidated state
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
    // No-op: caching is handled entirely by the native layer's global CACHE_REGISTRY,
    // which is automatically invalidated by write operations across all instances.
  }

  override def getCacheStats(): Option[CacheStats] = {
    val expirationSecs = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 300L)
    Some(CacheStats(hits = 0, misses = 0, hitRate = 0.0, versionsInCache = 0, expirationSeconds = expirationSecs))
  }

  override def getLastRetryMetrics(): Option[TxRetryMetrics] = lastRetryMetrics

  override def getLastCheckpointVersion(): Option[Long] = {
    val snapshot = getOrRefreshSnapshot()
    if (snapshot == null) return None
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

  private def extractMetadataConfigJson(snapshot: TxLogSnapshotInfo): String = {
    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null || metadataJson.isEmpty) return null

    try {
      val metadata = parseMetadataJson(metadataJson)
      if (metadata.configuration.isEmpty) return null

      // Extract schema registry entries for manifest reads
      val schemaKeyPrefix = "docMappingSchema."
      val schemaEntries = metadata.configuration.filter { case (k, _) =>
        k.startsWith(schemaKeyPrefix)
      }
      if (schemaEntries.isEmpty) return null

      mapper.writeValueAsString(schemaEntries.asJava)
    } catch {
      case _: Exception => null
    }
  }

  private def restoreSchemas(files: Seq[AddAction], snapshot: TxLogSnapshotInfo): Seq[AddAction] = {
    // Check if any files use schema dedup refs
    val hasRefs = files.exists(_.docMappingRef.isDefined)
    if (!hasRefs) return files

    val metadataJson = snapshot.getMetadataJson
    if (metadataJson == null) return files

    try {
      val metadata = parseMetadataJson(metadataJson)
      val schemaKeyPrefix = "docMappingSchema."
      val registry = metadata.configuration.collect {
        case (key, value) if key.startsWith(schemaKeyPrefix) =>
          key.stripPrefix(schemaKeyPrefix) -> value
      }
      if (registry.isEmpty) return files

      files.map { file =>
        file.docMappingRef match {
          case Some(ref) if file.docMappingJson.isEmpty =>
            registry.get(ref) match {
              case Some(schema) => file.copy(docMappingJson = Some(schema))
              case None         =>
                logger.warn(s"Schema ref '$ref' not found in registry for file ${file.path}")
                file
            }
          case _ => file
        }
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to restore schemas from registry: ${e.getMessage}")
        files
    }
  }

  private def parseActionsFromContent(content: String): Seq[Action] =
    content
      .split("\n")
      .filter(_.nonEmpty)
      .flatMap { line =>
        Try {
          val jsonNode = mapper.readTree(line)
          if (jsonNode.has("protocol")) {
            Some(mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction]))
          } else if (jsonNode.has("metaData")) {
            Some(mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
          } else if (jsonNode.has("add")) {
            Some(mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
          } else if (jsonNode.has("remove")) {
            Some(mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
          } else if (jsonNode.has("skip") || jsonNode.has("mergeskip")) {
            val skipNode = if (jsonNode.has("skip")) jsonNode.get("skip") else jsonNode.get("mergeskip")
            Some(mapper.treeToValue(skipNode, classOf[SkipAction]))
          } else {
            None
          }
        }.toOption.flatten.toSeq
      }
      .toSeq
}
