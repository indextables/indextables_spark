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

import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
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

  // Checkpoint handler
  private val checkpointEnabled = options.getBoolean("spark.indextables.checkpoint.enabled", true)
  private val checkpoint = if (checkpointEnabled) {
    Some(new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options))
  } else None

  // Current snapshot reference for lock-free reading
  private val currentSnapshot = new AtomicReference[Option[Snapshot]](None)

  // Version counter for atomic increment
  private val versionCounter = new AtomicLong(-1L)

  // Schema registry cache for write-time deduplication
  // Tracks which schemas have been written to MetadataAction.configuration
  // Initialized lazily from existing table state on first write
  @volatile private var schemaRegistryCache: Option[Map[String, String]] = None
  private val schemaRegistryLock = new Object()

  // Performance configuration
  private val maxStaleness        = options.getLong("spark.indextables.snapshot.maxStaleness", 5000).millis
  private val parallelReadEnabled = options.getBoolean("spark.indextables.parallel.read.enabled", true)

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

    // Re-cache metadata after invalidation since writeActions() may have written updated metadata
    // with new schema registry entries from schema deduplication
    try {
      val metadata = getMetadata()
      enhancedCache.putMetadata(tablePath.toString, metadata)
      logger.debug(s" Re-cached metadata after addFiles for ${tablePath.toString}")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to re-cache metadata after addFiles: ${e.getMessage}")
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

    // Re-cache metadata after invalidation since writeActions() wrote updated metadata
    // This is needed because invalidateTable() clears all caches including metadata
    // The getMetadata() call will recompute and cache the correct metadata from disk
    // We proactively cache it here to avoid the compute overhead on subsequent calls
    try {
      val metadata = getMetadata()
      enhancedCache.putMetadata(tablePath.toString, metadata)
      logger.debug(s" Re-cached metadata after overwrite for ${tablePath.toString}")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to re-cache metadata after overwrite: ${e.getMessage}")
    }

    logger.info(
      s"Overwrite complete: removed ${removeActions.length} files, added ${addActions.length} files"
    )
    logger.info(s"After overwrite, visible files should be: ${addActions.length} (adds) - 0 (active removes) = ${addActions.length}")

    version
  }

  /** Commits a merge splits operation atomically. */
  override def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val version = getNextVersion()
    val actions = removeActions ++ addActions
    writeActions(version, actions)

    // Invalidate caches
    enhancedCache.invalidateVersionDependentCaches(tablePath.toString)
    currentSnapshot.set(None)

    // Re-cache metadata after invalidation since writeActions() may have written updated metadata
    // with new schema registry entries from schema deduplication
    try {
      val metadata = getMetadata()
      enhancedCache.putMetadata(tablePath.toString, metadata)
      logger.debug(s" Re-cached metadata after commitMergeSplits for ${tablePath.toString}")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to re-cache metadata after commitMergeSplits: ${e.getMessage}")
    }

    version
  }

  /** Commits remove actions to mark files as logically deleted. */
  override def commitRemoveActions(removeActions: Seq[RemoveAction]): Long = {
    if (removeActions.isEmpty) {
      return getLatestVersion()
    }

    val version = getNextVersion()
    writeActions(version, removeActions)

    // Invalidate caches
    enhancedCache.invalidateVersionDependentCaches(tablePath.toString)
    currentSnapshot.set(None)

    logger.info(s"Committed ${removeActions.length} remove actions in version $version")
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

    // Restore schemas if any AddActions have docMappingRef (schema deduplication was used)
    val result = restoreSchemasInAddActions(cached)
    logger.debug(s" Returning ${result.size} files from listFilesOptimized")
    result
  }

  /**
   * Restore schemas in AddActions if they use schema references (docMappingRef).
   *
   * This handles schema deduplication by replacing docMappingRef with docMappingJson using the schema registry stored in
   * MetadataAction.configuration.
   *
   * @throws IllegalStateException
   *   if schema restoration fails
   */
  private def restoreSchemasInAddActions(addActions: Seq[AddAction]): Seq[AddAction] = {
    // Check if any AddActions have schema references that need restoration
    val hasSchemaRefs = addActions.exists(a => a.docMappingRef.isDefined && a.docMappingJson.isEmpty)

    if (!hasSchemaRefs) {
      // No schema refs to restore (legacy format or no schemas)
      return addActions
    }

    // Get MetadataAction which contains the schema registry
    val metadata = getMetadata()
    val schemaRegistry = metadata.configuration

    // Check if registry has any schemas
    if (!schemaRegistry.keys.exists(_.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))) {
      throw new IllegalStateException(
        "AddActions have docMappingRef but no schema registry found in MetadataAction.configuration. " +
          "This indicates a corrupted transaction log or checkpoint."
      )
    }

    // Restore schemas
    logger.debug(s"Restoring schemas in ${addActions.count(_.docMappingRef.isDefined)} AddActions")
    val restored = SchemaDeduplication.restoreSchemas(addActions, schemaRegistry).collect { case a: AddAction => a }

    // Verify all schemas were restored
    val unresolvedRefs = restored.filter(a => a.docMappingRef.isDefined && a.docMappingJson.isEmpty)
    if (unresolvedRefs.nonEmpty) {
      val missingHashes = unresolvedRefs.flatMap(_.docMappingRef).distinct.mkString(", ")
      throw new IllegalStateException(
        s"Failed to restore schemas for ${unresolvedRefs.size} AddActions. " +
          s"Missing schema hashes: $missingHashes"
      )
    }

    restored
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

        // Get checkpoint version and metadata (if available)
        val checkpointVersion = checkpoint.flatMap(_.getLastCheckpointVersion())
        var baseMetadata: Option[MetadataAction] = None

        // Try checkpoint first if available - uses cached checkpoint actions
        getCheckpointActionsCached() match {
          case Some(checkpointActions) =>
            checkpointActions.collectFirst { case metadata: MetadataAction => metadata } match {
              case Some(metadata) =>
                logger.info(s"Found metadata in checkpoint (version ${checkpointVersion.getOrElse("unknown")})")
                baseMetadata = Some(metadata)
              case None => // No metadata in checkpoint, continue searching
            }
          case None => // No checkpoint, continue with version files
        }

        // CRITICAL: Check versions AFTER checkpoint for MetadataAction updates
        // Schema deduplication may have registered new schemas after the checkpoint
        val latestVersion = getLatestVersion()
        val startVersion = checkpointVersion.map(_ + 1).getOrElse(latestVersion)

        // Search from checkpoint+1 to latest for any metadata updates
        for (version <- latestVersion to startVersion by -1) {
          val actions = readVersionOptimized(version)
          actions.collectFirst { case metadata: MetadataAction => metadata } match {
            case Some(metadata) =>
              // Found newer metadata - merge schema registries
              baseMetadata match {
                case Some(base) =>
                  // Merge configurations: newer entries override older ones
                  val mergedConfig = base.configuration ++ metadata.configuration
                  logger.info(s"Merging metadata from version $version with checkpoint (${metadata.configuration.size} + ${base.configuration.size} = ${mergedConfig.size} config entries)")
                  return metadata.copy(configuration = mergedConfig)
                case None =>
                  logger.info(s"Found metadata in version $version")
                  return metadata
              }
            case None => // Continue searching
          }
        }

        // If we found checkpoint metadata but no newer updates, return checkpoint metadata
        baseMetadata match {
          case Some(metadata) =>
            logger.info("Using checkpoint metadata (no newer updates found)")
            return metadata
          case None =>
            // No checkpoint, search all versions from latest to 0
            for (version <- latestVersion to 0L by -1) {
              val actions = readVersionOptimized(version)
              actions.collectFirst { case metadata: MetadataAction => metadata } match {
                case Some(metadata) => return metadata
                case None           => // Continue searching
              }
            }
        }

        throw new RuntimeException("No metadata found in transaction log")
      }
    )

  /**
   * Get the current schema registry, initializing from existing table state if needed.
   *
   * This method is thread-safe and ensures the registry is initialized only once.
   * The registry is extracted from MetadataAction.configuration.
   *
   * @return Schema registry map (hash -> schema JSON)
   */
  private def getSchemaRegistry(): Map[String, String] = {
    schemaRegistryCache match {
      case Some(registry) => registry
      case None =>
        schemaRegistryLock.synchronized {
          // Double-check pattern
          schemaRegistryCache match {
            case Some(registry) => registry
            case None =>
              // Initialize from existing MetadataAction.configuration
              val registry = try {
                val metadata = getMetadata()
                val extracted = SchemaDeduplication.extractSchemaRegistry(metadata.configuration)
                logger.info(s"Initialized schema registry with ${extracted.size} existing schemas")
                extracted
              } catch {
                case _: RuntimeException =>
                  // No metadata yet (new table), return empty registry
                  logger.debug("No existing metadata, initializing empty schema registry")
                  Map.empty[String, String]
              }
              schemaRegistryCache = Some(registry)
              registry
          }
        }
    }
  }

  /**
   * Update the schema registry cache with new schemas.
   *
   * This method is called after writing a MetadataAction with new schema entries.
   * It updates the in-memory cache to reflect the newly written schemas.
   *
   * @param newSchemas New schema entries (hash -> schema JSON)
   */
  private def updateSchemaRegistry(newSchemas: Map[String, String]): Unit = {
    schemaRegistryLock.synchronized {
      val currentRegistry = schemaRegistryCache.getOrElse(Map.empty)
      schemaRegistryCache = Some(currentRegistry ++ newSchemas)
      logger.debug(s"Updated schema registry: added ${newSchemas.size} new schemas, total ${schemaRegistryCache.get.size}")
    }
  }

  /**
   * Invalidate the schema registry cache.
   *
   * This should be called when the MetadataAction.configuration is updated
   * outside of the normal write flow (e.g., during cache invalidation or external modifications).
   */
  private def invalidateSchemaRegistry(): Unit = {
    schemaRegistryLock.synchronized {
      schemaRegistryCache = None
      logger.debug("Invalidated schema registry cache")
    }
  }

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

  /** Get the full checkpoint info (including multi-part checkpoint details). */
  def getLastCheckpointInfo(): Option[LastCheckpointInfo] =
    checkpoint.flatMap(_.getLastCheckpointInfo())

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
   * Get checkpoint actions with caching. This is the key optimization: caches both the last checkpoint info and the
   * checkpoint actions to avoid repeated reads of _last_checkpoint and checkpoint files.
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
      // Use full streaming: cloud storage -> decompression -> line parsing
      // This avoids OOM for large version files (>1GB)
      val actions = parseActionsFromStream(versionFilePath)
      logger.debug(s" Parsed ${actions.size} actions from version $version using streaming")
      actions
    }.getOrElse {
      logger.debug(s" Failed to read/parse version $version")
      Seq.empty
    }
  }

  /**
   * Parse actions directly from a cloud storage file using full streaming.
   *
   * This method provides the most memory-efficient parsing by streaming data from
   * cloud storage directly through decompression and into line-by-line parsing,
   * without ever loading the entire file into memory.
   *
   * Flow: CloudStorage InputStream -> Decompressing InputStream -> BufferedReader -> Line parsing
   */
  private def parseActionsFromStream(filePath: String): Seq[Action] = {
    val rawStream = cloudProvider.openInputStream(filePath)
    val decompressingStream = CompressionUtils.createDecompressingInputStream(rawStream)
    val reader = new BufferedReader(new InputStreamReader(decompressingStream, "UTF-8"))
    val actions = ListBuffer[Action]()

    try {
      var line = reader.readLine()
      while (line != null) {
        if (line.nonEmpty) {
          Try {
            val jsonNode = JsonUtil.mapper.readTree(line)
            parseAction(jsonNode)
          }.toOption.flatten.foreach(actions += _)
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally {
      reader.close()
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

  // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
  private def parseAction(jsonNode: com.fasterxml.jackson.databind.JsonNode): Option[Action] =
    if (jsonNode.has("protocol")) {
      Some(JsonUtil.mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction]))
    } else if (jsonNode.has("metaData")) {
      Some(JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
    } else if (jsonNode.has("add")) {
      Some(JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
    } else if (jsonNode.has("remove")) {
      Some(JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
    } else if (jsonNode.has("mergeskip")) {
      Some(JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction]))
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

  /**
   * Apply schema deduplication to actions before writing to transaction log.
   *
   * This method:
   * 1. Gets the current schema registry (initializes from table if needed)
   * 2. Identifies new schemas that need registration
   * 3. Deduplicates AddActions (replaces docMappingJson with docMappingRef)
   * 4. Returns deduplicated actions + optional MetadataAction update
   *
   * @param actions Actions to write
   * @return Tuple of (deduplicated actions, optional metadata update with new schemas)
   */
  private def applySchemaDeduplicationForWrite(
    actions: Seq[Action]
  ): (Seq[Action], Option[MetadataAction]) = {

    // Skip deduplication if no AddActions with schemas
    val hasSchemas = actions.exists {
      case add: AddAction if add.docMappingJson.isDefined => true
      case _ => false
    }

    if (!hasSchemas) {
      logger.debug("No schemas to deduplicate in write actions")
      return (actions, None)
    }

    // Get current schema registry (initializes from table if needed)
    val currentRegistry = getSchemaRegistry()

    // Convert registry from (hash -> schema) to configuration format (prefixed keys)
    val registryAsConfiguration = currentRegistry.map {
      case (hash, schema) => (SchemaDeduplication.SCHEMA_KEY_PREFIX + hash, schema)
    }

    // Deduplicate schemas
    val (deduplicatedActions, newSchemaRegistry) =
      SchemaDeduplication.deduplicateSchemas(actions, registryAsConfiguration)

    // Check if new schemas were found (keys not in existing registry)
    val newSchemas = newSchemaRegistry.filterNot {
      case (key, _) => registryAsConfiguration.contains(key)
    }

    if (newSchemas.nonEmpty) {
      // Log new schemas being registered
      val newHashes = newSchemas.keys.map(_.stripPrefix(SchemaDeduplication.SCHEMA_KEY_PREFIX))
      logger.info(s"Schema deduplication: found ${newSchemas.size} new schemas to register: ${newHashes.mkString(", ")}")

      // Get current metadata to merge new schemas
      val currentMetadata = try {
        getMetadata()
      } catch {
        case _: RuntimeException =>
          // No metadata yet - this shouldn't happen in normal write path (initialize is called first)
          // but handle gracefully by skipping deduplication
          logger.warn("No existing metadata found during write - skipping schema deduplication")
          return (actions, None)
      }

      // Merge new schemas into metadata configuration
      val updatedConfiguration =
        SchemaDeduplication.mergeIntoConfiguration(currentMetadata.configuration, newSchemas)

      val updatedMetadata = currentMetadata.copy(configuration = updatedConfiguration)

      (deduplicatedActions, Some(updatedMetadata))
    } else {
      // All schemas already registered, no metadata update needed
      logger.debug(s"Schema deduplication: all schemas already registered (${currentRegistry.size} existing)")
      (deduplicatedActions, None)
    }
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    logger.debug(s" Writing version $version to $versionFilePath with ${actions.size} actions")

    // Apply schema deduplication to reduce transaction log size
    val (deduplicatedActions, metadataUpdate) = applySchemaDeduplicationForWrite(actions)

    // Build final actions list, including metadata update if needed
    val actionsToWrite = metadataUpdate match {
      case Some(updatedMetadata) =>
        // Check if MetadataAction is already in actions
        val hasMetadataInActions = actions.exists(_.isInstanceOf[MetadataAction])

        if (hasMetadataInActions) {
          // Replace existing MetadataAction with updated one
          deduplicatedActions.map {
            case _: MetadataAction => updatedMetadata
            case other => other
          }
        } else {
          // Prepend MetadataAction to actions
          updatedMetadata +: deduplicatedActions
        }

      case None =>
        // No new schemas, use deduplicated actions as-is
        deduplicatedActions
    }

    // Use streaming write to avoid OOM for large transaction log versions
    // This prevents StringBuilder exceeding JVM's ~2GB array size limit
    val codec           = getCompressionCodec()
    val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

    logger.info(
      s"Writing ${actionsToWrite.length} actions to version $version$compressionInfo: ${actionsToWrite.map(_.getClass.getSimpleName).mkString(", ")}"
    )

    // CRITICAL: Use conditional write (ifNotExists=true) to prevent overwriting transaction log files
    // Transaction log files are immutable and should never be overwritten
    val writeSucceeded = StreamingActionWriter.writeActionsStreaming(
      actions = actionsToWrite,
      cloudProvider = cloudProvider,
      path = versionFilePath,
      codec = codec,
      ifNotExists = true // Conditional write for transaction log integrity
    )

    if (!writeSucceeded) {
      throw new IllegalStateException(
        s"Failed to write transaction log version $version - file already exists at $versionFilePath. " +
          "This indicates a concurrent write conflict or version counter synchronization issue. " +
          "Transaction log files are immutable and must never be overwritten to ensure data integrity."
      )
    }

    logger.debug(s" Successfully wrote version $version")

    // Update schema registry cache after successful write
    metadataUpdate.foreach { updatedMetadata =>
      val newSchemas = SchemaDeduplication.extractSchemaRegistry(updatedMetadata.configuration)
      updateSchemaRegistry(newSchemas)
    }

    // Add a small delay to ensure file system consistency for local file systems
    // This helps with race conditions where listFiles() is called immediately after writeFile()
    Thread.sleep(10) // 10ms should be sufficient for local file system consistency

    // Write-through cache management: proactively update caches with new data
    enhancedCache.putVersionActions(tablePath.toString, version, actionsToWrite)
    logger.debug(s" Cached version actions for ${tablePath.toString} version $version")

    // IMPORTANT: Update metadata cache BEFORE listFiles() call below
    // The listFiles() -> restoreSchemasInAddActions() -> getMetadata() path needs the updated
    // metadata to contain the new schema registry entries
    actionsToWrite.find(_.isInstanceOf[MetadataAction]).foreach { metadata =>
      enhancedCache.putMetadata(tablePath.toString, metadata.asInstanceOf[MetadataAction])
      logger.debug(s" Cached metadata for ${tablePath.toString}")
    }

    // Update file list cache if we have AddActions
    val addActions = actionsToWrite.collect { case add: AddAction => add }
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
      parallelOps.reconstructStateParallel(versionsToProcess)
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

    logger.debug(s"Final state has ${files.size} files: ${files.keys.mkString(", ")}")
    logger.info(s"Final state has ${files.size} files")
    files.values.toSeq
  }

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

  /**
   * Get all SkipActions from the transaction log using cached checkpoint data.
   * This is more efficient than the base TransactionLog implementation because
   * it reuses the cached checkpoint actions.
   */
  def getSkippedFiles(): Seq[SkipAction] = {
    val skips = scala.collection.mutable.ListBuffer[SkipAction]()

    // Get checkpoint version for determining which versions to read
    val checkpointVersion = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)

    // Use cached checkpoint actions (shared with other operations)
    getCheckpointActionsCached() match {
      case Some(checkpointActions) =>
        // Extract SkipActions from cached checkpoint
        checkpointActions.foreach {
          case skip: SkipAction => skips += skip
          case _ => // Ignore other actions
        }
        logger.debug(s"Found ${skips.size} SkipActions in cached checkpoint")

      case None =>
        // No checkpoint, will read all versions below
        logger.debug("No cached checkpoint available for SkipActions")
    }

    // Read versions after checkpoint for any additional SkipActions
    val allVersions = getVersions()
    val versionsToRead = allVersions.filter(_ > checkpointVersion)

    if (versionsToRead.nonEmpty) {
      logger.debug(s"Reading ${versionsToRead.size} versions after checkpoint $checkpointVersion for SkipActions")
      versionsToRead.foreach { version =>
        val actions = readVersionOptimized(version)
        actions.foreach {
          case skip: SkipAction => skips += skip
          case _ => // Ignore other actions
        }
      }
    }

    skips.toSeq
  }

  /**
   * Get files currently in cooldown period.
   * Returns a map of file path to retry-after timestamp.
   */
  def getFilesInCooldown(): Map[String, Long] = {
    val now = System.currentTimeMillis()
    getSkippedFiles()
      .filter(skip => skip.retryAfter.exists(_ > now))
      .groupBy(_.path)
      .map {
        case (path, skips) =>
          // Get the latest retry time for this path
          val latestRetryAfter = skips.flatMap(_.retryAfter).max
          path -> latestRetryAfter
      }
  }

  /**
   * Filter out files that are in cooldown from a list of candidate files for merge.
   * Uses cached checkpoint data for efficiency.
   */
  def filterFilesInCooldown(candidateFiles: Seq[AddAction]): Seq[AddAction] = {
    val cooldownMap = getFilesInCooldown()
    val filesInCooldown = cooldownMap.keySet
    val filtered = candidateFiles.filterNot(file => filesInCooldown.contains(file.path))

    val filteredCount = candidateFiles.length - filtered.length
    if (filteredCount > 0) {
      logger.info(s"Filtered out $filteredCount files currently in cooldown period")
      filesInCooldown.foreach { path =>
        val retryTime = cooldownMap.get(path)
        logger.debug(s"File in cooldown: $path (retry after: ${retryTime.map(java.time.Instant.ofEpochMilli)})")
      }
    }

    filtered
  }

  /** Invalidate all cached data for this table. Useful for testing or after external modifications. */
  def invalidateCache(tablePath: String): Unit = {
    enhancedCache.invalidateTable(tablePath)
    invalidateSchemaRegistry()
  }
}
