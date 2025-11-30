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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Optimized transaction log implementation that integrates all performance enhancements from the optimization plan
 * including thread pools, advanced caching, parallel operations, memory optimization, and advanced features.
 */
class OptimizedTransactionLog(
  tablePath: Path,
  spark: SparkSession,
  options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
    extends TransactionLogInterface {

  private val logger = LoggerFactory.getLogger(classOf[OptimizedTransactionLog])

  // Cloud storage provider
  private val cloudProvider = CloudStorageProviderFactory.createProvider(
    tablePath.toString,
    options,
    spark.sparkContext.hadoopConfiguration
  )
  private val transactionLogPath = new Path(tablePath, "_transaction_log")

  // Enhanced caching system with support for legacy expirationSeconds setting
  private val enhancedCache = {
    // Convert legacy expirationSeconds to minutes for enhanced cache compatibility
    // For test compatibility, allow sub-minute expiration by using fraction of minute
    val legacyExpirationSeconds = options.getLong("spark.indextables.transaction.cache.expirationSeconds", -1)
    val legacyExpirationMinutes = if (legacyExpirationSeconds > 0) {
      if (legacyExpirationSeconds < 60) 1L // Use 1 minute minimum for very short times
      else legacyExpirationSeconds / 60
    } else -1L

    new EnhancedTransactionLogCache(
      logCacheSize = options.getLong("spark.indextables.cache.log.size", 1000),
      logCacheTTLMinutes =
        if (legacyExpirationMinutes > 0) legacyExpirationMinutes
        else options.getLong("spark.indextables.cache.log.ttl", 5),
      snapshotCacheSize = options.getLong("spark.indextables.cache.snapshot.size", 100),
      snapshotCacheTTLMinutes =
        if (legacyExpirationMinutes > 0) legacyExpirationMinutes
        else options.getLong("spark.indextables.cache.snapshot.ttl", 10),
      fileListCacheSize = options.getLong("spark.indextables.cache.filelist.size", 50),
      fileListCacheTTLMinutes =
        if (legacyExpirationMinutes > 0) legacyExpirationMinutes
        else options.getLong("spark.indextables.cache.filelist.ttl", 2),
      metadataCacheSize = options.getLong("spark.indextables.cache.metadata.size", 100),
      metadataCacheTTLMinutes =
        if (legacyExpirationMinutes > 0) legacyExpirationMinutes
        else options.getLong("spark.indextables.cache.metadata.ttl", 30)
    )
  }

  // Parallel operations handler
  private val parallelOps = new ParallelTransactionLogOperations(
    transactionLogPath,
    cloudProvider,
    spark
  )

  // Memory optimized operations
  private val memoryOps = new MemoryOptimizedOperations(
    transactionLogPath,
    cloudProvider,
    spark
  )

  // Advanced optimizations
  private val advancedOps = new AdvancedOptimizations(
    transactionLogPath,
    cloudProvider,
    spark
  )

  // Checkpoint handler
  private val checkpointEnabled = options.getBoolean("spark.indextables.checkpoint.enabled", true)
  private val checkpoint = if (checkpointEnabled) {
    Some(new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options))
  } else None

  // Current snapshot reference for lock-free reading
  private val currentSnapshot = new AtomicReference[Option[Snapshot]](None)

  // Version counter for atomic increment
  private val versionCounter = new AtomicLong(-1L)

  // Performance configuration
  private val maxStaleness        = options.getLong("spark.indextables.snapshot.maxStaleness", 5000).millis
  private val parallelReadEnabled = options.getBoolean("spark.indextables.parallel.read.enabled", true)
  private val asyncUpdatesEnabled = options.getBoolean("spark.indextables.async.updates.enabled", false) // Disabled by default for stability

  def getTablePath(): Path = tablePath

  override def close(): Unit = {
    // Clean up resources
    enhancedCache.clearAll()
    checkpoint.foreach(_.close())
    cloudProvider.close()
    // Note: Thread pools are managed globally and not closed here
  }

  /** Initialize transaction log with schema (single parameter version for interface compatibility) */
  def initialize(schema: StructType): Unit = initialize(schema, Seq.empty)

  /** Initialize transaction log with schema and partition columns */
  def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
    val version0Path = new Path(transactionLogPath, "00000000000000000000.json").toString

    // Check if already initialized by looking for version 0 file
    if (cloudProvider.exists(version0Path)) {
      logger.debug(s"Transaction log already initialized at $transactionLogPath")
      return
    }

    // Create directory if it doesn't exist
    if (!cloudProvider.exists(transactionLogPath.toString)) {
      cloudProvider.createDirectory(transactionLogPath.toString)
    }

    // Validate partition columns
    val schemaFields         = schema.fieldNames.toSet
    val invalidPartitionCols = partitionColumns.filterNot(schemaFields.contains)
    if (invalidPartitionCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Partition columns ${invalidPartitionCols.mkString(", ")} not found in schema"
      )
    }

    logger.info(s"Initializing transaction log with schema: ${schema.prettyJson}")

    // Write protocol and metadata in version 0
    val protocolAction = ProtocolVersion.defaultProtocol()
    val metadataAction = MetadataAction(
      id = java.util.UUID.randomUUID().toString,
      name = None,
      description = None,
      format = FileFormat("indextables", Map.empty),
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      configuration = Map.empty,
      createdTime = Some(System.currentTimeMillis())
    )

    writeActions(0, Seq(protocolAction, metadataAction))
    logger.info(
      s"Initialized table with protocol version ${protocolAction.minReaderVersion}/${protocolAction.minWriterVersion}"
    )
  }

  /** Add files using parallel batch write */
  def addFiles(addActions: Seq[AddAction]): Long = {
    // Check protocol before writing
    assertTableWritable()

    if (addActions.isEmpty) {
      return getLatestVersion()
    }

    // Use atomic version generation to prevent race conditions
    val version = getNextVersion()
    logger.debug(s" Assigning version $version for addFiles with ${addActions.size} actions")

    // Write actions directly - removed "parallel write" which was just blocking on a thread pool
    writeActions(version, addActions)

    // Invalidate all caches to ensure consistency (matches overwriteFiles pattern)
    enhancedCache.invalidateTable(tablePath.toString)

    // Schedule async snapshot update if enabled
    if (asyncUpdatesEnabled) {
      advancedOps.scheduleAsyncUpdate {
        updateSnapshot(version)
      }
    }

    version
  }

  /** Remove a single file by adding a RemoveAction */
  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long = {
    // Use atomic version generation to prevent race conditions
    val version = getNextVersion()
    logger.debug(s" Assigning version $version for removeFile '$path'")

    val removeAction = RemoveAction(
      path = path,
      deletionTimestamp = Some(deletionTimestamp),
      dataChange = true,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None
    )

    writeActions(version, Seq(removeAction))

    // Invalidate caches after remove operation
    enhancedCache.invalidateVersionDependentCaches(tablePath.toString)

    // Clear current snapshot to force recomputation
    currentSnapshot.set(None)

    logger.debug(s" Removed file '$path' at version $version")
    version
  }

  /**
   * Overwrite files with optimized operations For overwrite, we need to:
   *   1. Get all existing files 2. Create RemoveActions for all of them 3. Add the new files 4. Write both removes and
   *      adds in the same transaction
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long = {
    logger.debug(s" Starting overwrite operation with ${addActions.size} files")
    logger.info(s"Starting overwrite operation with ${addActions.size} files")

    // Get existing files using optimized listing
    val existingFiles = listFilesOptimized()
    logger.debug(s" Found ${existingFiles.size} existing files to remove: ${existingFiles.map(_.path).mkString(", ")}")
    val removeActions = existingFiles.map { file =>
      RemoveAction(
        path = file.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = None,
        partitionValues = Some(file.partitionValues),
        size = Some(file.size)
      )
    }

    // Use atomic version generation to prevent race conditions (same as addFiles)
    val version = getNextVersion()
    logger.debug(s" Using atomic version assignment: $version")
    // Write removes first, then adds - this ensures proper overwrite semantics
    val allActions: Seq[Action] = removeActions ++ addActions
    logger.debug(s" Writing ${allActions.size} actions (${removeActions.size} removes + ${addActions.size} adds) to version $version")

    // Write actions directly - removed "parallel write" which was just blocking on a thread pool
    writeActions(version, allActions)

    // Clear all caches after overwrite
    enhancedCache.invalidateTable(tablePath.toString)

    // Clear current snapshot to force recomputation
    currentSnapshot.set(None)

    logger.info(
      s"Overwrite complete: removed ${removeActions.length} files, added ${addActions.length} files"
    )
    logger.info(s"After overwrite, visible files should be: ${addActions.length} (adds) - 0 (active removes) = ${addActions.length}")

    version
  }

  /** List files with enhanced caching and parallel operations */
  def listFiles(): Seq[AddAction] = {
    // Check protocol before reading
    assertTableReadable()
    listFilesOptimized()
  }

  private def listFilesOptimized(): Seq[AddAction] = {
    // Get versions once and pass consistently to avoid FileSystem caching issues
    val versions      = getVersions()
    val latestVersion = versions.sorted.lastOption.getOrElse(-1L)
    val checksum      = computeCurrentChecksumWithVersions(versions)
    logger.debug(s" Computed checksum: $checksum, latest version: $latestVersion")

    val cached = enhancedCache.getOrComputeFileList(
      tablePath.toString,
      checksum, {
        logger.debug(s" Cache miss, computing file list for checksum: $checksum")
        logger.info(s"Computing file list using optimized operations for checksum: $checksum")

        // Try to get from current snapshot first, but verify it's up to date with latest version
        currentSnapshot.get() match {
          case Some(snapshot) if snapshot.age < maxStaleness.toMillis && snapshot.version >= latestVersion =>
            logger.debug(s" Using cached snapshot (version ${snapshot.version}) with ${snapshot.files.size} files")
            snapshot.files

          case Some(snapshot) =>
            logger.debug(s" Snapshot is stale (version ${snapshot.version} < $latestVersion), rebuilding")
            // Clear stale snapshot
            currentSnapshot.set(None)
            reconstructStateStandard(versions)

          case None =>
            // Always use standard reconstruction with consistent versions to avoid caching issues
            logger.debug(
              s" No snapshot available, using standard reconstruction with versions: ${versions.sorted.mkString(", ")}"
            )
            reconstructStateStandard(versions)
        }
      }
    )

    logger.debug(s" Returning ${cached.size} files from listFilesOptimized")
    cached
  }

  /** Get schema from metadata (for interface compatibility) */
  def getSchema(): Option[StructType] =
    Try {
      val metadata = getMetadata()
      import org.apache.spark.sql.types.DataType
      DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
    }.toOption

  /** Get metadata with enhanced caching */
  def getMetadata(): MetadataAction =
    enhancedCache.getOrComputeMetadata(
      tablePath.toString, {
        logger.info("Computing metadata from transaction log")

        // Try checkpoint first if available - uses cached checkpoint actions
        getCheckpointActionsCached() match {
          case Some(checkpointActions) =>
            checkpointActions.collectFirst { case metadata: MetadataAction => metadata } match {
              case Some(metadata) =>
                logger.info("Found metadata in checkpoint (cached)")
                return metadata
              case None => // Continue searching in version files
            }
          case None => // No checkpoint, continue with version files
        }

        // Look for metadata in reverse chronological order
        val latestVersion = getLatestVersion()
        for (version <- latestVersion to 0L by -1) {
          val actions = readVersionOptimized(version)
          actions.collectFirst { case metadata: MetadataAction => metadata } match {
            case Some(metadata) => return metadata
            case None           => // Continue searching
          }
        }

        throw new RuntimeException("No metadata found in transaction log")
      }
    )

  /** Get protocol with enhanced caching */
  def getProtocol(): ProtocolAction =
    enhancedCache.getOrComputeProtocol(
      tablePath.toString, {
        logger.info("Computing protocol from transaction log")

        // Try checkpoint first if available - uses cached checkpoint actions
        getCheckpointActionsCached() match {
          case Some(checkpointActions) =>
            checkpointActions.collectFirst { case protocol: ProtocolAction => protocol } match {
              case Some(protocol) =>
                logger.info("Found protocol in checkpoint (cached)")
                return protocol
              case None => // Continue searching in version files
            }
          case None => // No checkpoint, continue with version files
        }

        // Look for protocol in reverse chronological order
        val latestVersion = getLatestVersion()
        for (version <- latestVersion to 0L by -1) {
          val actions = readVersionOptimized(version)
          actions.collectFirst { case protocol: ProtocolAction => protocol } match {
            case Some(protocol) => return protocol
            case None           => // Continue searching
          }
        }

        // No protocol found - default to version 1 for legacy tables
        logger.warn(s"No protocol action found in transaction log at $tablePath, defaulting to version 1 (legacy table)")
        ProtocolVersion.legacyProtocol()
      }
    )

  /** Check if the current client can read this table */
  def assertTableReadable(): Unit = {
    val checkEnabled = options.getBoolean(ProtocolVersion.PROTOCOL_CHECK_ENABLED, true)
    if (!checkEnabled) {
      logger.warn("Protocol version checking is disabled - skipping reader version check")
      return
    }

    val protocol = getProtocol()

    if (protocol.minReaderVersion > ProtocolVersion.CURRENT_READER_VERSION) {
      throw new ProtocolVersionException(
        s"""Table at $tablePath requires a newer version of IndexTables4Spark to read.
           |
           |This table requires:
           |  minReaderVersion = ${protocol.minReaderVersion}
           |  minWriterVersion = ${protocol.minWriterVersion}
           |
           |Your current version supports:
           |  readerVersion = ${ProtocolVersion.CURRENT_READER_VERSION}
           |  writerVersion = ${ProtocolVersion.CURRENT_WRITER_VERSION}
           |
           |Please upgrade IndexTables4Spark to a newer version.
           |""".stripMargin
      )
    }

    // Check for unsupported reader features
    protocol.readerFeatures.foreach { features =>
      val unsupportedFeatures = features -- ProtocolVersion.SUPPORTED_READER_FEATURES
      if (unsupportedFeatures.nonEmpty) {
        throw new ProtocolVersionException(
          s"""Table at $tablePath requires unsupported reader features: ${unsupportedFeatures.mkString(", ")}
             |
             |Supported features: ${ProtocolVersion.SUPPORTED_READER_FEATURES.mkString(", ")}
             |""".stripMargin
        )
      }
    }

    logger.debug(s"Protocol read check passed: table requires ${protocol.minReaderVersion}, current reader ${ProtocolVersion.CURRENT_READER_VERSION}")
  }

  /** Check if the current client can write to this table */
  def assertTableWritable(): Unit = {
    val checkEnabled = options.getBoolean(ProtocolVersion.PROTOCOL_CHECK_ENABLED, true)
    if (!checkEnabled) {
      logger.warn("Protocol version checking is disabled - skipping writer version check")
      return
    }

    val protocol = getProtocol()

    if (protocol.minWriterVersion > ProtocolVersion.CURRENT_WRITER_VERSION) {
      throw new ProtocolVersionException(
        s"""Table at $tablePath requires a newer version of IndexTables4Spark to write.
           |
           |This table requires:
           |  minReaderVersion = ${protocol.minReaderVersion}
           |  minWriterVersion = ${protocol.minWriterVersion}
           |
           |Your current version supports:
           |  readerVersion = ${ProtocolVersion.CURRENT_READER_VERSION}
           |  writerVersion = ${ProtocolVersion.CURRENT_WRITER_VERSION}
           |
           |Please upgrade IndexTables4Spark to a newer version.
           |""".stripMargin
      )
    }

    // Check for unsupported writer features
    protocol.writerFeatures.foreach { features =>
      val unsupportedFeatures = features -- ProtocolVersion.SUPPORTED_WRITER_FEATURES
      if (unsupportedFeatures.nonEmpty) {
        throw new ProtocolVersionException(
          s"""Table at $tablePath requires unsupported writer features: ${unsupportedFeatures.mkString(", ")}
             |
             |Supported features: ${ProtocolVersion.SUPPORTED_WRITER_FEATURES.mkString(", ")}
             |""".stripMargin
        )
      }
    }

    logger.debug(s"Protocol write check passed: table requires ${protocol.minWriterVersion}, current writer ${ProtocolVersion.CURRENT_WRITER_VERSION}")
  }

  /** Upgrade protocol to a new version */
  def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit = {
    val autoUpgradeEnabled = options.getBoolean(ProtocolVersion.PROTOCOL_AUTO_UPGRADE, true)
    if (!autoUpgradeEnabled) {
      logger.warn("Protocol auto-upgrade is disabled - skipping upgrade")
      return
    }

    val currentProtocol = getProtocol()

    // Only upgrade if necessary
    if (
      newMinReaderVersion > currentProtocol.minReaderVersion ||
      newMinWriterVersion > currentProtocol.minWriterVersion
    ) {

      val updatedProtocol = ProtocolAction(
        minReaderVersion = math.max(newMinReaderVersion, currentProtocol.minReaderVersion),
        minWriterVersion = math.max(newMinWriterVersion, currentProtocol.minWriterVersion)
      )

      val version = getNextVersion()
      writeAction(version, updatedProtocol)

      // Invalidate protocol cache
      enhancedCache.invalidateProtocol(tablePath.toString)

      logger.info(
        s"Upgraded protocol from ${currentProtocol.minReaderVersion}/${currentProtocol.minWriterVersion} " +
          s"to ${updatedProtocol.minReaderVersion}/${updatedProtocol.minWriterVersion}"
      )
    } else {
      logger.debug(
        s"Protocol upgrade not needed: current ${currentProtocol.minReaderVersion}/${currentProtocol.minWriterVersion}, " +
          s"requested $newMinReaderVersion/$newMinWriterVersion"
      )
    }
  }

  /** Initialize protocol for legacy tables */
  private def initializeProtocolIfNeeded(): Unit = {
    val actions     = getVersions().flatMap(readVersionOptimized)
    val hasProtocol = actions.exists(_.isInstanceOf[ProtocolAction])

    if (!hasProtocol) {
      logger.info(s"Initializing protocol for legacy table at $tablePath")
      val version = getNextVersion()
      writeAction(version, ProtocolVersion.defaultProtocol())
      enhancedCache.invalidateProtocol(tablePath.toString)
    }
  }

  /** Get total row count using optimized operations */
  def getTotalRowCount(): Long =
    // First try to get from checkpoint info
    checkpoint.flatMap(_.getLastCheckpointInfo()) match {
      case Some(info) if info.numFiles > 0 =>
        // Estimate from checkpoint
        val files = listFilesOptimized()
        files.flatMap(_.numRecords).sum

      case _ =>
        // Compute from files
        listFilesOptimized().flatMap(_.numRecords).sum
    }

  /** Get partition columns */
  def getPartitionColumns(): Seq[String] =
    Try(getMetadata().partitionColumns).getOrElse(Seq.empty)

  /** Check if table is partitioned */
  def isPartitioned(): Boolean =
    getPartitionColumns().nonEmpty

  /** Get last checkpoint version */
  def getLastCheckpointVersion(): Option[Long] =
    checkpoint.flatMap(_.getLastCheckpointVersion())

  /**
   * Aggressively populate the transaction log cache to make subsequent reads faster. This should be called once during
   * table initialization (V2 DataSource). Populates: protocol, metadata, file list, and recent versions.
   */
  def prewarmCache(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Starting aggressive cache prewarm for table at $tablePath")

    try {
      // 1. Load protocol into cache
      val protocol = getProtocol()
      logger.debug(s"Prewarmed protocol: ${protocol.minReaderVersion}/${protocol.minWriterVersion}")

      // 2. Load metadata into cache
      val metadata = getMetadata()
      logger.debug(s"Prewarmed metadata for table: ${metadata.name}")

      // 3. Load file list into cache (this also loads all version actions)
      val files = listFilesOptimized()
      logger.debug(s"Prewarmed file list: ${files.size} files")

      // 4. Load checkpoint info if available
      checkpoint.foreach { cp =>
        cp.getLastCheckpointInfo().foreach { info =>
          logger.debug(s"Prewarmed checkpoint info: version ${info.version}")
        }
      }

      val elapsed = System.currentTimeMillis() - startTime
      logger.info(s"Cache prewarm completed in ${elapsed}ms - loaded ${files.size} files from transaction log")
    } catch {
      case e: Exception =>
        logger.debug(s"Failed to prewarm cache (non-fatal): ${e.getMessage}")
    }
  }

  // Private helper methods

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    val latest   = versions.sorted.lastOption.getOrElse(-1L)
    logger.debug(s" getLatestVersion: found versions ${versions.sorted.mkString(", ")}, latest = $latest")

    // Update version counter if we found a higher version
    versionCounter.updateAndGet(current => math.max(current, latest))
    latest
  }

  private def getNextVersion(): Long = {
    // First, ensure version counter is initialized
    if (versionCounter.get() == -1L) {
      getLatestVersion()
    }

    // Atomically increment and return
    versionCounter.incrementAndGet()
  }

  private def getVersions(): Seq[Long] = {
    val files = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
    logger.debug(
      s" getVersions: cloudProvider returned ${files.size} files: ${files.map(_.path.split("/").last).mkString(", ")}"
    )
    val versions = files.flatMap(file => parseVersionFromPath(file.path)).distinct
    logger.debug(s" getVersions: parsed versions: ${versions.sorted.mkString(", ")}")
    versions
  }

  private def parseVersionFromPath(path: String): Option[Long] =
    Try {
      val fileName = new Path(path).getName
      if (fileName.endsWith(".json") && !fileName.contains(".checkpoint.")) {
        val versionStr = fileName.replace(".json", "").replaceAll("_.*", "")
        versionStr.toLong
      } else {
        -1L
      }
    }.toOption.filter(_ >= 0)

  private def readVersionOptimized(version: Long): Seq[Action] =
    // Use cache with proper invalidation - version-specific caching should be safe
    enhancedCache.getOrComputeVersionActions(tablePath.toString, version, readVersionDirect(version))

  /**
   * Get checkpoint actions with caching.
   * This is the key optimization: caches both the last checkpoint info and the checkpoint actions
   * to avoid repeated reads of _last_checkpoint and checkpoint files.
   */
  private def getCheckpointActionsCached(): Option[Seq[Action]] = {
    // First, get the last checkpoint info (cached)
    val lastCheckpointInfo = enhancedCache.getOrComputeLastCheckpointInfo(
      tablePath.toString,
      checkpoint.flatMap(_.getLastCheckpointInfo())
    )

    // If no checkpoint info, return None
    lastCheckpointInfo match {
      case None =>
        logger.debug(s"No checkpoint info available for $tablePath")
        None
      case Some(info) =>
        // Get checkpoint actions for this version (cached)
        enhancedCache.getOrComputeCheckpointActions(
          tablePath.toString,
          info.version,
          // Only read from storage if not cached
          checkpoint.flatMap(_.getActionsFromCheckpoint())
        )
    }
  }

  private def readVersionDirect(version: Long): Seq[Action] = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    if (!cloudProvider.exists(versionFilePath)) {
      logger.debug(s" Version file $versionFilePath does not exist")
      return Seq.empty
    }

    Try {
      val rawBytes = cloudProvider.readFile(versionFilePath)
      // Decompress if needed (handles both compressed and uncompressed files)
      val decompressedBytes = CompressionUtils.readTransactionFile(rawBytes)
      val content           = new String(decompressedBytes, "UTF-8")
      logger.debug(s" Read version $version content: '${content.take(100)}...' (${content.length} chars)")
      val actions = parseActionsFromContent(content)
      logger.debug(s" Parsed ${actions.size} actions from version $version")
      actions
    }.getOrElse {
      logger.debug(s" Failed to read/parse version $version")
      Seq.empty
    }
  }

  private def parseActionsFromContent(content: String): Seq[Action] =
    content
      .split("\n")
      .filter(_.nonEmpty)
      .flatMap { line =>
        Try {
          val jsonNode = JsonUtil.mapper.readTree(line)
          parseAction(jsonNode)
        }.toOption
      }
      .flatten
      .toSeq

  private def parseAction(jsonNode: com.fasterxml.jackson.databind.JsonNode): Option[Action] =
    if (jsonNode.has("protocol")) {
      val protocolNode = jsonNode.get("protocol")
      Some(JsonUtil.mapper.readValue(protocolNode.toString, classOf[ProtocolAction]))
    } else if (jsonNode.has("metaData")) {
      val metadataNode = jsonNode.get("metaData")
      Some(JsonUtil.mapper.readValue(metadataNode.toString, classOf[MetadataAction]))
    } else if (jsonNode.has("add")) {
      val addNode = jsonNode.get("add")
      Some(JsonUtil.mapper.readValue(addNode.toString, classOf[AddAction]))
    } else if (jsonNode.has("remove")) {
      val removeNode = jsonNode.get("remove")
      Some(JsonUtil.mapper.readValue(removeNode.toString, classOf[RemoveAction]))
    } else if (jsonNode.has("mergeskip")) {
      val skipNode = jsonNode.get("mergeskip")
      Some(JsonUtil.mapper.readValue(skipNode.toString, classOf[SkipAction]))
    } else {
      None
    }

  private def writeAction(version: Long, action: Action): Unit =
    writeActions(version, Seq(action))

  /**
   * Get the compression codec based on configuration. Checks thread-local options first (from write operation), then
   * falls back to table options.
   */
  private def getCompressionCodec(): Option[CompressionCodec] = {
    // Get compression config with priority: Thread-local options > Table options
    val effectiveOptions = TransactionLog.getWriteOptions().getOrElse(options)

    val compressionEnabled = effectiveOptions.getBoolean("spark.indextables.transaction.compression.enabled", true)
    val compressionCodec =
      Option(effectiveOptions.get("spark.indextables.transaction.compression.codec")).getOrElse("gzip")
    val compressionLevel = effectiveOptions.getInt("spark.indextables.transaction.compression.gzip.level", 6)

    try
      CompressionUtils.getCodec(compressionEnabled, compressionCodec, compressionLevel)
    catch {
      case e: IllegalArgumentException =>
        logger.warn(s"Invalid compression configuration: ${e.getMessage}. Falling back to no compression.")
        None
    }
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    val content = new StringBuilder()
    actions.foreach { action =>
      val wrappedAction = action match {
        case protocol: ProtocolAction => Map("protocol" -> protocol)
        case metadata: MetadataAction => Map("metaData" -> metadata)
        case add: AddAction           => Map("add" -> add)
        case remove: RemoveAction     => Map("remove" -> remove)
        case skip: SkipAction         => Map("mergeskip" -> skip)
      }
      content.append(JsonUtil.mapper.writeValueAsString(wrappedAction)).append("\n")
    }

    logger.debug(s" Writing version $version to $versionFilePath with ${actions.size} actions")

    // Apply compression if enabled - evaluate codec lazily to pick up write-time options
    val jsonBytes       = content.toString.getBytes("UTF-8")
    val codec           = getCompressionCodec()
    val bytesToWrite    = CompressionUtils.writeTransactionFile(jsonBytes, codec)
    val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

    logger.info(
      s"Writing ${actions.length} actions to version $version$compressionInfo: ${actions.map(_.getClass.getSimpleName).mkString(", ")}"
    )

    // CRITICAL: Use conditional write to prevent overwriting transaction log files
    // Transaction log files are immutable and should never be overwritten
    val writeSucceeded = cloudProvider.writeFileIfNotExists(versionFilePath, bytesToWrite)

    if (!writeSucceeded) {
      throw new IllegalStateException(
        s"Failed to write transaction log version $version - file already exists at $versionFilePath. " +
          "This indicates a concurrent write conflict or version counter synchronization issue. " +
          "Transaction log files are immutable and must never be overwritten to ensure data integrity."
      )
    }

    logger.debug(s" Successfully wrote version $version")
    // Add a small delay to ensure file system consistency for local file systems
    // This helps with race conditions where listFiles() is called immediately after writeFile()
    Thread.sleep(10) // 10ms should be sufficient for local file system consistency

    // Write-through cache management: proactively update caches with new data
    enhancedCache.putVersionActions(tablePath.toString, version, actions)
    logger.debug(s" Cached version actions for ${tablePath.toString} version $version")

    // Update file list cache if we have AddActions
    val addActions = actions.collect { case add: AddAction => add }
    if (addActions.nonEmpty) {
      // Compute new file list state by getting current state and applying changes
      val currentFiles =
        try
          listFiles()
        catch {
          case _: Exception => Seq.empty[AddAction]
        }

      // Create a checksum for the current file list state
      val fileListChecksum = currentFiles.map(_.path).sorted.mkString(",").hashCode.toString
      enhancedCache.putFileList(tablePath.toString, fileListChecksum, currentFiles)
      logger.debug(s" Updated file list cache for ${tablePath.toString} with checksum $fileListChecksum")
    }

    // Update metadata cache if we have MetadataAction
    actions.find(_.isInstanceOf[MetadataAction]).foreach { metadata =>
      enhancedCache.putMetadata(tablePath.toString, metadata.asInstanceOf[MetadataAction])
      logger.debug(s" Cached metadata for ${tablePath.toString}")
    }

    // Create checkpoint if needed (synchronously to ensure consistency)
    checkpoint.foreach { cp =>
      if (cp.shouldCreateCheckpoint(version)) {
        try
          createCheckpointOptimized(version)
        catch {
          case e: Exception =>
            logger.warn(s"Failed to create checkpoint at version $version", e)
        }
      }
    }
  }

  private def createCheckpointOptimized(version: Long): Unit =
    try {
      val allActions = getAllCurrentActions(version)

      // Use standard checkpoint creation to ensure _last_checkpoint file is written
      checkpoint.foreach(_.createCheckpoint(version, allActions))

      // Note: Cleanup of old transaction log files is handled explicitly via
      // PURGE ORPHANED SPLITS command, not automatically during writes.
      // This gives users control over when cleanup occurs.
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create checkpoint at version $version", e)
    }

  private def getAllCurrentActions(upToVersion: Long): Seq[Action] = {
    val versions = getVersions().filter(_ <= upToVersion)

    // Load initial state from checkpoint if available - this allows reading even when early versions are deleted
    val initialFiles = scala.collection.mutable.HashMap[String, AddAction]()
    val versionsToProcess = checkpoint.flatMap(_.getLastCheckpointVersion()) match {
      case Some(checkpointVersion) if versions.contains(checkpointVersion) =>
        logger.info(s"Loading initial state from checkpoint at version $checkpointVersion for getAllCurrentActions")

        // Load state from checkpoint (uses cached checkpoint actions)
        getCheckpointActionsCached().foreach { checkpointActions =>
          checkpointActions.foreach {
            case add: AddAction =>
              initialFiles(add.path) = add
            case remove: RemoveAction =>
              initialFiles.remove(remove.path)
            case _ => // Ignore other actions (protocol, metadata)
          }
        }

        logger.info(s"Loaded ${initialFiles.size} files from checkpoint, processing versions after $checkpointVersion")
        // Only process versions after checkpoint
        versions.filter(_ > checkpointVersion)
      case _ =>
        // No checkpoint or checkpoint not in version range, process all versions
        versions
    }

    val addActions = if (parallelReadEnabled && versionsToProcess.size > 10) {
      // Use parallel reconstruction for remaining versions
      val newFiles = parallelOps.reconstructStateParallel(versionsToProcess)
      // BUG FIX: Apply the changes from versionsToProcess on top of checkpoint state
      // Don't just concatenate and take .last - we need to handle REMOVE actions properly
      val finalFiles = scala.collection.mutable.HashMap[String, AddAction]()
      finalFiles ++= initialFiles
      // Now apply changes from versionsToProcess - this will override adds and handle removes
      for (version <- versionsToProcess.sorted) {
        val actions = readVersionOptimized(version)
        actions.foreach {
          case add: AddAction       => finalFiles(add.path) = add
          case remove: RemoveAction => finalFiles.remove(remove.path)
          case _                    => // Ignore other actions
        }
      }
      finalFiles.values.toSeq
    } else {
      // Standard reconstruction with consistent versions
      // Note: reconstructStateStandard handles checkpoint internally, so we pass all versions
      // but also pass initial files from checkpoint
      if (initialFiles.nonEmpty) {
        // BUG FIX: Apply the changes from versionsToProcess on top of checkpoint state
        // Don't use reconstructStateFromVersions and merge - that loses REMOVE information
        val finalFiles = scala.collection.mutable.HashMap[String, AddAction]()
        finalFiles ++= initialFiles
        // Now apply changes from versionsToProcess
        for (version <- versionsToProcess.sorted) {
          val actions = readVersionOptimized(version)
          actions.foreach {
            case add: AddAction       => finalFiles(add.path) = add
            case remove: RemoveAction => finalFiles.remove(remove.path)
            case _                    => // Ignore other actions
          }
        }
        finalFiles.values.toSeq
      } else {
        reconstructStateStandard(versions)
      }
    }

    // Build complete action sequence: protocol + metadata + add actions (with truncated stats)
    val allActions = scala.collection.mutable.ListBuffer[Action]()

    // Add protocol first
    try
      allActions += getProtocol()
    catch {
      case _: Exception => // No protocol found, skip
    }

    // Add metadata second
    val metadata =
      try {
        val md = getMetadata()
        allActions += md
        Some(md)
      } catch {
        case _: Exception => None // No metadata found, skip
      }

    // Apply statistics truncation to add actions before adding to checkpoint
    val truncatedAddActions = applyStatisticsTruncation(addActions, metadata)
    allActions ++= truncatedAddActions

    allActions.toSeq
  }

  /**
   * Apply statistics truncation to AddActions for checkpoint creation. Preserves partition column statistics while
   * truncating data column statistics.
   */
  private def applyStatisticsTruncation(
    addActions: Seq[AddAction],
    metadata: Option[MetadataAction]
  ): Seq[AddAction] = {
    import io.indextables.spark.util.StatisticsTruncation

    // Get configuration from Spark session and options
    import scala.jdk.CollectionConverters._
    val sparkConf  = spark.conf.getAll
    val optionsMap = options.asCaseSensitiveMap().asScala.toMap
    val config     = sparkConf ++ optionsMap

    // Get partition columns to protect their statistics
    val partitionColumns = metadata.map(_.partitionColumns.toSet).getOrElse(Set.empty[String])

    addActions.map { add =>
      val originalMinValues = add.minValues.getOrElse(Map.empty[String, String])
      val originalMaxValues = add.maxValues.getOrElse(Map.empty[String, String])

      // Separate partition column stats from data column stats
      val (partitionMinValues, dataMinValues) = originalMinValues.partition {
        case (col, _) =>
          partitionColumns.contains(col)
      }
      val (partitionMaxValues, dataMaxValues) = originalMaxValues.partition {
        case (col, _) =>
          partitionColumns.contains(col)
      }

      // Only truncate data column statistics, not partition column statistics
      val (truncatedDataMinValues, truncatedDataMaxValues) =
        StatisticsTruncation.truncateStatistics(dataMinValues, dataMaxValues, config)

      // Merge partition stats back (they are never truncated)
      val finalMinValues = partitionMinValues ++ truncatedDataMinValues
      val finalMaxValues = partitionMaxValues ++ truncatedDataMaxValues

      // Return new AddAction with truncated statistics
      add.copy(
        minValues = if (finalMinValues.nonEmpty) Some(finalMinValues) else None,
        maxValues = if (finalMaxValues.nonEmpty) Some(finalMaxValues) else None
      )
    }
  }

  /** Helper method to reconstruct state from a sequence of versions without checkpoint handling */
  private def reconstructStateFromVersions(versions: Seq[Long]): Seq[AddAction] = {
    val files = scala.collection.mutable.HashMap[String, AddAction]()

    for (version <- versions.sorted) {
      val actions = readVersionOptimized(version)
      actions.foreach {
        case add: AddAction =>
          files(add.path) = add
        case remove: RemoveAction =>
          files.remove(remove.path)
        case _ => // Ignore other actions
      }
    }

    files.values.toSeq
  }

  private def reconstructStateStandard(versions: Seq[Long]): Seq[AddAction] = {
    val files = scala.collection.mutable.HashMap[String, AddAction]()

    logger.debug(s" Reconstructing state from versions: ${versions.sorted}")
    logger.info(s"Reconstructing state from versions: ${versions.sorted}")

    // Try to start from checkpoint if available - this allows reading even when early versions are deleted
    val (startVersion, versionsToProcess) = checkpoint.flatMap(_.getLastCheckpointVersion()) match {
      case Some(checkpointVersion) if versions.contains(checkpointVersion) =>
        logger.info(s"Starting reconstruction from checkpoint at version $checkpointVersion")

        // Load state from checkpoint (uses cached checkpoint actions)
        getCheckpointActionsCached().foreach { checkpointActions =>
          checkpointActions.foreach {
            case add: AddAction =>
              files(add.path) = add
              logger.debug(s"Loaded from checkpoint: ${add.path}")
            case remove: RemoveAction =>
              files.remove(remove.path)
              logger.debug(s"Removed from checkpoint: ${remove.path}")
            case _ => // Ignore other actions (protocol, metadata)
          }
        }

        // Only process versions after checkpoint
        (checkpointVersion, versions.filter(_ > checkpointVersion).sorted)
      case _ =>
        // No checkpoint or checkpoint not in version range, process all versions
        (0L, versions.sorted)
    }

    logger.info(s"Processing ${versionsToProcess.size} versions starting from version $startVersion")

    for (version <- versionsToProcess) {
      val actions = readVersionOptimized(version)
      logger.debug(
        s"Version $version has ${actions.size} actions: ${actions.map(_.getClass.getSimpleName).mkString(", ")}"
      )
      actions.foreach { action =>
        logger.debug(s"Processing action: ${action.getClass.getSimpleName}")
        action match {
          case add: AddAction =>
            files(add.path) = add
            logger.debug(s"Added file: ${add.path}")
          case remove: RemoveAction =>
            val removed = files.remove(remove.path)
            logger.debug(s"Removed file: ${remove.path}, was present: ${removed.isDefined}")
          case _ => // Ignore other actions
        }
      }
    }

    logger.debug(s" Final state has ${files.size} files: ${files.keys.mkString(", ")}")
    logger.info(s"Final state has ${files.size} files: ${files.keys.mkString(", ")}")
    files.values.toSeq
  }

  // Backward compatibility method
  private def reconstructStateStandard(): Seq[AddAction] =
    reconstructStateStandard(getVersions())

  private def reconstructStateFromListing(listing: FileListingResult): Seq[AddAction] = {
    // Implement state reconstruction from listing result
    val files = scala.collection.mutable.HashMap[String, AddAction]()

    // Process transaction files in order
    listing.transactionFiles.sortBy(_.version).foreach { tf =>
      val actions = readVersionOptimized(tf.version)
      actions.foreach {
        case add: AddAction =>
          files(add.path) = add
        case remove: RemoveAction =>
          files.remove(remove.path)
        case _ => // Ignore
      }
    }

    files.values.toSeq
  }

  private def updateSnapshot(version: Long): Unit = {
    // Avoid recursive calls to listFilesOptimized - compute files directly
    val versions = getVersions()
    val files    = reconstructStateStandard(versions)
    val metadata = getMetadata()
    val snapshot = Snapshot(version, files, metadata)
    currentSnapshot.set(Some(snapshot))
    logger.debug(s" Updated snapshot to version $version with ${files.size} files")
  }

  private def computeCurrentChecksum(): String =
    // Simple checksum based on latest version
    s"${tablePath.toString}-${getLatestVersion()}"

  private def computeCurrentChecksumWithVersions(versions: Seq[Long]): String = {
    // Include sorted version list in key, not just latest - this ensures cache invalidation
    // when new versions are written
    val versionHash = versions.sorted.mkString(",").hashCode
    s"${tablePath.toString}-$versionHash"
  }

  /**
   * Get cache statistics for monitoring and debugging. Returns the enhanced cache statistics from the multi-level cache
   * system.
   */
  def getCacheStatistics(): CacheStatistics =
    enhancedCache.getStatistics()

  /** Invalidate all cached data for this table. Useful for testing or after external modifications. */
  def invalidateCache(tablePath: String): Unit =
    enhancedCache.invalidateTable(tablePath)
}
