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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

object TransactionLog {
  // Thread-local storage for write-time options
  // This allows writes to pass their DataFrameWriter options (including compression settings) to TransactionLog
  private val writeOptions = new ThreadLocal[Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]]()

  def setWriteOptions(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit =
    writeOptions.set(Some(options))

  def clearWriteOptions(): Unit =
    writeOptions.remove()

  def getWriteOptions(): Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] =
    Option(writeOptions.get()).flatten
}

class TransactionLog(
  tablePath: Path,
  spark: SparkSession,
  options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
    extends TransactionLogInterface {

  // Check if this is being used directly instead of through the factory
  private val allowDirectUsage = options.getBoolean("spark.indextables.transaction.allowDirectUsage", false)
  if (!allowDirectUsage) {
    throw new RuntimeException(
      s"TransactionLog should no longer be used directly. Use TransactionLogFactory.create() instead. " +
        s"If you need to use this class directly for testing, set spark.indextables.transaction.allowDirectUsage=true"
    )
  }

  private val logger = LoggerFactory.getLogger(classOf[TransactionLog])

  // Determine if we should use cloud-optimized transaction log
  private val protocol = ProtocolBasedIOFactory.determineProtocol(tablePath.toString)
  private val useCloudOptimized = protocol match {
    case ProtocolBasedIOFactory.S3Protocol => !options.getBoolean("spark.indextables.transaction.force.hadoop", false)
    case _                                 => false
  }

  // Use cloud storage provider instead of direct Hadoop filesystem
  private val cloudProvider =
    CloudStorageProviderFactory.createProvider(tablePath.toString, options, spark.sparkContext.hadoopConfiguration)
  private val transactionLogPath    = new Path(tablePath, "_transaction_log")
  private val transactionLogPathStr = transactionLogPath.toString

  // Cache configuration and initialization
  private val cacheEnabled = options.getBoolean("spark.indextables.transaction.cache.enabled", true)
  private val cacheExpirationSeconds = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 5 * 60L) // 5 minutes default
  private val cache = if (cacheEnabled) Some(new TransactionLogCache(cacheExpirationSeconds)) else None

  // Checkpoint configuration and initialization
  private val checkpointEnabled = options.getBoolean("spark.indextables.checkpoint.enabled", true)
  private val checkpoint =
    if (checkpointEnabled) Some(new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)) else None

  // Compression configuration - check write-time options first, then Hadoop conf, then table options
  // Simple approach: check thread-local override options, then fall back to table-level options
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

  // Atomic version counter for thread-safe version assignment
  private val versionCounter = new AtomicLong(-1L)

  def getTablePath(): Path = tablePath

  override def close(): Unit = {
    cache.foreach(_.shutdown())
    checkpoint.foreach(_.close())
    cloudProvider.close()
  }

  def initialize(schema: StructType): Unit =
    initialize(schema, Seq.empty)

  def initialize(schema: StructType, partitionColumns: Seq[String]): Unit =
    if (!cloudProvider.exists(transactionLogPathStr)) {
      cloudProvider.createDirectory(transactionLogPathStr)

      // Validate partition columns exist in schema
      val schemaFields         = schema.fieldNames.toSet
      val invalidPartitionCols = partitionColumns.filterNot(schemaFields.contains)
      if (invalidPartitionCols.nonEmpty) {
        throw new IllegalArgumentException(
          s"Partition columns ${invalidPartitionCols.mkString(", ")} not found in schema"
        )
      }

      // DEBUG: Log the original schema being written
      logger.info(s"Writing schema to transaction log: ${schema.prettyJson}")
      logger.info(s"Partition columns: ${partitionColumns.mkString(", ")}")
      schema.fields.foreach { field =>
        logger.info(s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}")
      }

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

  override def addFile(addAction: AddAction): Long = {
    // Legacy Hadoop implementation
    val version = getNextVersion()
    writeAction(version, addAction)
    version
  }

  /**
   * Add multiple files in a single transaction (like Delta Lake). This creates one JSON file with multiple ADD entries.
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
    // Legacy Hadoop implementation
    if (addActions.isEmpty) {
      return getLatestVersion()
    }

    // Check protocol before writing
    initializeProtocolIfNeeded()
    assertTableWritable()

    val version = getNextVersion()
    writeActions(version, addActions)
    version
  }

  /**
   * Add files in overwrite mode - removes all existing files and adds new ones. This is similar to Delta Lake's
   * overwrite mode.
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      logger.warn("Overwrite operation with no files to add")
    }

    // Get all existing files to remove
    val existingFiles = listFiles()
    val removeActions = existingFiles.map { existingFile =>
      RemoveAction(
        path = existingFile.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = None,
        partitionValues = Some(existingFile.partitionValues),
        size = Some(existingFile.size)
      )
    }

    val version = getNextVersion()

    // Write both REMOVE and ADD actions in a single transaction
    val allActions = removeActions ++ addActions
    writeActions(version, allActions)

    logger.info(s"Overwrite operation: removed ${removeActions.length} files, added ${addActions.length} files in version $version")
    version
  }

  def listFiles(): Seq[AddAction] = {
    // Check protocol before reading
    assertTableReadable()

    // Check cache first
    cache.flatMap(_.getCachedFiles()) match {
      case Some(cachedFiles) =>
        logger.debug(s"Using cached files list: ${cachedFiles.length} files")
        cachedFiles
      case None =>
        // Use optimized checkpoint + parallel retrieval approach
        val files = ListBuffer[AddAction]()

        // Try to get base state from checkpoint first
        checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
          case Some(checkpointActions) =>
            // Apply checkpoint actions first
            checkpointActions.foreach {
              case add: AddAction       => files += add
              case remove: RemoveAction => files --= files.filter(_.path == remove.path)
              case _                    => // Ignore other actions for file listing
            }

            // Then apply incremental changes since checkpoint
            val checkpointVersion       = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)
            val allVersions             = getVersions()
            val versionsAfterCheckpoint = allVersions.filter(_ > checkpointVersion)

            if (versionsAfterCheckpoint.nonEmpty) {
              logger.debug(
                s"Reading ${versionsAfterCheckpoint.length} versions after checkpoint $checkpointVersion in parallel"
              )
              val parallelResults = checkpoint.get.readVersionsInParallel(versionsAfterCheckpoint)

              // Apply changes in version order
              for (version <- versionsAfterCheckpoint.sorted)
                parallelResults.get(version).foreach { actions =>
                  actions.foreach {
                    case add: AddAction       => files += add
                    case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                    case _                    => // Ignore other actions for file listing
                  }
                }
            }
          case None =>
            // No checkpoint available - use parallel retrieval for all versions
            val versions = getVersions()
            if (versions.nonEmpty) {
              logger.debug(s"No checkpoint available, reading ${versions.length} versions in parallel")

              checkpoint match {
                case Some(cp) =>
                  val parallelResults = cp.readVersionsInParallel(versions)
                  // Apply changes in version order
                  for (version <- versions.sorted)
                    parallelResults.get(version).foreach { actions =>
                      actions.foreach {
                        case add: AddAction       => files += add
                        case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                        case _                    => // Ignore other actions for file listing
                      }
                    }
                case None =>
                  // Fallback to sequential reading (original behavior)
                  for (version <- versions) {
                    val actions = readVersion(version)
                    actions.foreach {
                      case add: AddAction       => files += add
                      case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                      case _                    => // Ignore other actions for file listing
                    }
                  }
              }
            }
        }

        val result = files.toSeq
        // Cache the result
        cache.foreach(_.cacheFiles(result))
        logger.debug(s"Computed and cached files list: ${result.length} files")
        result
    }
  }

  /** Get the total row count across all active files. */
  def getTotalRowCount(): Long =
    listFiles().map { file =>
      file.numRecords
        .map { (count: Any) =>
          // Handle any numeric type and convert to Long
          count match {
            case l: Long              => l
            case i: Int               => i.toLong
            case i: java.lang.Integer => i.toLong
            case _                    => count.toString.toLong
          }
        }
        .getOrElse(0L)
    }.sum

  def getSchema(): Option[StructType] =
    // Legacy Hadoop implementation
    Try {
      // Try checkpoint first if available - this allows reading even when early versions are deleted
      val metadataOpt = checkpoint.flatMap(_.getActionsFromCheckpoint()).flatMap { checkpointActions =>
        checkpointActions.collectFirst {
          case metadata: MetadataAction =>
            logger.info(s"Reading schema from checkpoint: ${metadata.schemaString}")
            val deserializedSchema = DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
            logger.info(s"Deserialized schema from checkpoint: ${deserializedSchema.prettyJson}")
            deserializedSchema
        }
      }

      metadataOpt.orElse {
        // Fallback to reading from version files
        val versions = getVersions()
        if (versions.nonEmpty) {
          val actions = readVersion(versions.head)
          actions.collectFirst {
            case metadata: MetadataAction =>
              // DEBUG: Log the schema being read from transaction log
              logger.info(s"Reading schema from transaction log: ${metadata.schemaString}")
              val deserializedSchema = DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
              logger.info(s"Deserialized schema: ${deserializedSchema.prettyJson}")
              deserializedSchema.fields.foreach { field =>
                logger.info(
                  s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}"
                )
              }
              deserializedSchema
          }
        } else {
          None
        }
      }
    }.getOrElse(None)

  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long = {
    // Legacy Hadoop implementation
    val version = getNextVersion()
    val removeAction = RemoveAction(
      path = path,
      deletionTimestamp = Some(deletionTimestamp),
      dataChange = true,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None
    )
    writeAction(version, removeAction)
    version
  }

  /**
   * Atomically commit a set of REMOVE and ADD actions in a single transaction. This is used for operations like MERGE
   * SPLITS where we need to atomically replace old split files with merged ones.
   *
   * @param removeActions
   *   Files to remove
   * @param addActions
   *   Files to add
   * @return
   *   The new version number
   */
  override def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val version = getNextVersion()
    val actions = removeActions ++ addActions
    writeActions(version, actions)
    version
  }

  /**
   * Commits remove actions to mark files as logically deleted.
   *
   * This operation marks files as removed in the transaction log without physically deleting them. The files become
   * invisible to queries but remain on disk for recovery and time-travel purposes.
   */
  override def commitRemoveActions(removeActions: Seq[RemoveAction]): Long = {
    if (removeActions.isEmpty) {
      return getLatestVersion()
    }

    val version = getNextVersion()
    writeActions(version, removeActions)

    // Invalidate cache since file list has changed
    cache.foreach(_.invalidateAll())

    logger.info(s"Committed ${removeActions.length} remove actions in version $version")
    version
  }

  private def writeAction(version: Long, action: Action): Unit =
    writeActions(version, Seq(action))

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    val content = new StringBuilder()
    actions.foreach { action =>
      // Wrap actions in the appropriate delta log format
      val wrappedAction = action match {
        case protocol: ProtocolAction     => Map("protocol" -> protocol)
        case metadata: MetadataAction     => Map("metaData" -> metadata)
        case add: AddAction               => Map("add" -> add)
        case remove: RemoveAction         => Map("remove" -> remove)
        case skip: SkipAction             => Map("mergeskip" -> skip)
        case addXRef: AddXRefAction       => Map("addXRef" -> addXRef)
        case removeXRef: RemoveXRefAction => Map("removeXRef" -> removeXRef)
      }

      val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
      content.append(actionJson).append("\n")
    }

    // Apply compression if enabled - evaluate codec lazily to pick up write-time options
    val jsonBytes       = content.toString.getBytes("UTF-8")
    val codec           = getCompressionCodec()
    val bytesToWrite    = CompressionUtils.writeTransactionFile(jsonBytes, codec)
    val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

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

    // Invalidate caches after any write operation since the transaction log state has changed
    cache.foreach(_.invalidateVersionDependentCaches())

    logger.info(
      s"Written ${actions.length} actions to version $version$compressionInfo: ${actions.map(_.getClass.getSimpleName).mkString(", ")}"
    )

    // Check if we should create a checkpoint
    checkpoint.foreach { cp =>
      if (cp.shouldCreateCheckpoint(version)) {
        try {
          // Get all current actions to create checkpoint
          val allCurrentActions = getAllCurrentActions(version)
          cp.createCheckpoint(version, allCurrentActions)

          // Note: Cleanup of old transaction log files is handled explicitly via
          // PURGE ORPHANED SPLITS command, not automatically during writes.
          // This gives users control over when cleanup occurs.
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to create checkpoint at version $version", e)
          // Continue - checkpoint failure shouldn't fail the write operation
        }
      }
    }
  }

  /** Get all current actions up to the specified version for checkpoint creation. */
  private def getAllCurrentActions(upToVersion: Long): Seq[Action] = {
    val allActions                             = ListBuffer[Action]()
    var latestProtocol: Option[ProtocolAction] = None
    var latestMetadata: Option[MetadataAction] = None
    val activeFiles                            = ListBuffer[AddAction]()

    val versions = getVersions().filter(_ <= upToVersion)

    // Try to start from checkpoint if available - this allows reading even when early versions are deleted
    val versionsToProcess = checkpoint.flatMap(_.getLastCheckpointVersion()) match {
      case Some(checkpointVersion) if versions.contains(checkpointVersion) =>
        logger.info(s"Loading initial state from checkpoint at version $checkpointVersion")

        // Load state from checkpoint
        checkpoint.flatMap(_.getActionsFromCheckpoint()).foreach { checkpointActions =>
          checkpointActions.foreach {
            case protocol: ProtocolAction =>
              latestProtocol = Some(protocol)
            case metadata: MetadataAction =>
              latestMetadata = Some(metadata)
            case add: AddAction =>
              activeFiles += add
            case remove: RemoveAction =>
              activeFiles --= activeFiles.filter(_.path == remove.path)
            case _: SkipAction       => // Skip actions are transient
            case _: AddXRefAction    => // XRef actions handled separately
            case _: RemoveXRefAction => // XRef actions handled separately
          }
        }

        logger.info(s"Loaded ${activeFiles.size} files from checkpoint, processing versions after $checkpointVersion")
        // Only process versions after checkpoint
        versions.filter(_ > checkpointVersion)
      case _ =>
        // No checkpoint or checkpoint not in version range, process all versions
        versions
    }

    // Process remaining versions to get the current state
    for (version <- versionsToProcess.sorted) {
      val actions = readVersion(version)
      actions.foreach {
        case protocol: ProtocolAction =>
          latestProtocol = Some(protocol)
        case metadata: MetadataAction =>
          latestMetadata = Some(metadata)
        case add: AddAction =>
          // Remove any existing file with the same path and add the new one
          activeFiles --= activeFiles.filter(_.path == add.path)
          activeFiles += add
        case remove: RemoveAction =>
          activeFiles --= activeFiles.filter(_.path == remove.path)
        case _: SkipAction       => // Skip actions are transient and not included in checkpoints
        case _: AddXRefAction    => // XRef actions handled separately
        case _: RemoveXRefAction => // XRef actions handled separately
      }
    }

    // Add protocol first (if present)
    latestProtocol.foreach(allActions += _)

    // Add metadata second
    latestMetadata.foreach(allActions += _)

    // Apply statistics truncation to active files before adding to checkpoint
    // This prevents checkpoint bloat from old transaction files with long statistics
    val truncatedActiveFiles = applyStatisticsTruncation(activeFiles.toSeq, latestMetadata)
    allActions ++= truncatedActiveFiles

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

  /** Get partition columns from metadata. */
  def getPartitionColumns(): Seq[String] =
    try
      getMetadata().partitionColumns
    catch {
      case _: Exception => Seq.empty
    }

  /** Check if the table is partitioned. */
  def isPartitioned(): Boolean =
    getPartitionColumns().nonEmpty

  /** Get the current metadata action from the transaction log. */
  def getMetadata(): MetadataAction =
    // Check cache first
    cache.flatMap(_.getCachedMetadata()) match {
      case Some(cachedMetadata) =>
        logger.debug("Using cached metadata")
        cachedMetadata
      case None =>
        // Try to get metadata from checkpoint first to avoid reading all versions
        checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
          case Some(checkpointActions) =>
            // Metadata should be in checkpoint (second action after protocol)
            checkpointActions.collectFirst { case metadata: MetadataAction => metadata } match {
              case Some(metadata) =>
                cache.foreach(_.cacheMetadata(metadata))
                logger.debug(s"Found metadata in checkpoint: ${metadata.id}")
                return metadata
              case None =>
                // No metadata in checkpoint, fall through to version scanning
                logger.debug("No metadata found in checkpoint, scanning versions")
            }
          case None =>
            logger.debug("No checkpoint available for metadata lookup")
        }

        // Fallback: scan versions in reverse chronological order
        val latestVersion = getLatestVersion()

        // Look for metadata in reverse chronological order
        for (version <- latestVersion to 0L by -1) {
          val actions = readVersion(version)
          actions.collectFirst { case metadata: MetadataAction => metadata } match {
            case Some(metadata) =>
              // Cache the result
              cache.foreach(_.cacheMetadata(metadata))
              logger.debug(s"Computed and cached metadata: ${metadata.id}")
              return metadata
            case None => // Continue searching
          }
        }

        throw new RuntimeException("No metadata found in transaction log")
    }

  /** Get the current protocol action from the transaction log. */
  def getProtocol(): ProtocolAction =
    // Check cache first
    cache.flatMap(_.getCachedProtocol()) match {
      case Some(cachedProtocol) =>
        logger.debug("Using cached protocol")
        cachedProtocol
      case None =>
        // Try to get protocol from checkpoint first to avoid reading all versions
        checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
          case Some(checkpointActions) =>
            // Protocol should be first action in checkpoint
            checkpointActions.collectFirst { case protocol: ProtocolAction => protocol } match {
              case Some(protocol) =>
                cache.foreach(_.cacheProtocol(protocol))
                logger.debug(s"Found protocol in checkpoint: ${protocol.minReaderVersion}/${protocol.minWriterVersion}")
                return protocol
              case None =>
                // No protocol in checkpoint, fall through to version scanning
                logger.debug("No protocol found in checkpoint, scanning versions")
            }
          case None =>
            logger.debug("No checkpoint available for protocol lookup")
        }

        // Fallback: scan versions in reverse chronological order
        val latestVersion = getLatestVersion()

        // Look for protocol in reverse chronological order
        for (version <- latestVersion to 0L by -1) {
          val actions = readVersion(version)
          actions.collectFirst { case protocol: ProtocolAction => protocol } match {
            case Some(protocol) =>
              // Cache the result
              cache.foreach(_.cacheProtocol(protocol))
              logger.debug(s"Computed and cached protocol: ${protocol.minReaderVersion}/${protocol.minWriterVersion}")
              return protocol
            case None => // Continue searching
          }
        }

        // No protocol found - default to version 1 for legacy tables
        logger.warn(s"No protocol action found in transaction log at $tablePath, defaulting to version 1 (legacy table)")
        val legacyProtocol = ProtocolVersion.legacyProtocol()
        cache.foreach(_.cacheProtocol(legacyProtocol))
        legacyProtocol
    }

  /**
   * Check if the current client can read this table based on protocol version. Throws ProtocolVersionException if the
   * table requires a newer reader version.
   */
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

    // Check for unsupported reader features (for version 3+)
    protocol.readerFeatures.foreach { features =>
      val unsupportedFeatures = features -- ProtocolVersion.SUPPORTED_READER_FEATURES
      if (unsupportedFeatures.nonEmpty) {
        throw new ProtocolVersionException(
          s"""Table at $tablePath requires unsupported reader features: ${unsupportedFeatures.mkString(", ")}
             |
             |Supported features: ${ProtocolVersion.SUPPORTED_READER_FEATURES.mkString(", ")}
             |
             |Please upgrade IndexTables4Spark to a version that supports these features.
             |""".stripMargin
        )
      }
    }

    logger.debug(s"Protocol read check passed: table requires ${protocol.minReaderVersion}, current reader ${ProtocolVersion.CURRENT_READER_VERSION}")
  }

  /**
   * Check if the current client can write to this table based on protocol version. Throws ProtocolVersionException if
   * the table requires a newer writer version.
   */
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

    // Check for unsupported writer features (for version 3+)
    protocol.writerFeatures.foreach { features =>
      val unsupportedFeatures = features -- ProtocolVersion.SUPPORTED_WRITER_FEATURES
      if (unsupportedFeatures.nonEmpty) {
        throw new ProtocolVersionException(
          s"""Table at $tablePath requires unsupported writer features: ${unsupportedFeatures.mkString(", ")}
             |
             |Supported features: ${ProtocolVersion.SUPPORTED_WRITER_FEATURES.mkString(", ")}
             |
             |Please upgrade IndexTables4Spark to a version that supports these features.
             |""".stripMargin
        )
      }
    }

    logger.debug(s"Protocol write check passed: table requires ${protocol.minWriterVersion}, current writer ${ProtocolVersion.CURRENT_WRITER_VERSION}")
  }

  /**
   * Upgrade the table protocol to a new version. This is only allowed to increase version numbers. Auto-upgrade can be
   * controlled via spark.indextables.protocol.autoUpgrade configuration.
   */
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
      cache.foreach(_.invalidateProtocol())

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

  /**
   * Initialize protocol for legacy tables that don't have a protocol action. This is called automatically on first
   * write to a legacy table.
   */
  private def initializeProtocolIfNeeded(): Unit = {
    val actions     = getVersions().flatMap(readVersion)
    val hasProtocol = actions.exists(_.isInstanceOf[ProtocolAction])

    if (!hasProtocol) {
      logger.info(s"Initializing protocol for legacy table at $tablePath")
      val version = getNextVersion()
      writeAction(version, ProtocolVersion.defaultProtocol())
      cache.foreach(_.invalidateProtocol())
    }
  }

  def readVersion(version: Long): Seq[Action] =
    // Check cache first
    cache.flatMap(_.getCachedVersion(version)) match {
      case Some(cachedActions) =>
        logger.debug(s"Using cached version $version: ${cachedActions.length} actions")
        cachedActions
      case None =>
        // Read from storage
        val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
        val versionFilePath = versionFile.toString

        if (!cloudProvider.exists(versionFilePath)) {
          return Seq.empty
        }

        Try {
          // Read raw bytes and decompress if needed
          val rawBytes          = cloudProvider.readFile(versionFilePath)
          val decompressedBytes = CompressionUtils.readTransactionFile(rawBytes)
          val content           = new String(decompressedBytes, "UTF-8")
          val lines             = content.split("\n").filter(_.nonEmpty)

          lines.map { line =>
            val jsonNode = JsonUtil.mapper.readTree(line)

            if (jsonNode.has("protocol")) {
              val protocolNode = jsonNode.get("protocol")
              JsonUtil.mapper.readValue(protocolNode.toString, classOf[ProtocolAction])
            } else if (jsonNode.has("metaData")) {
              val metadataNode = jsonNode.get("metaData")
              JsonUtil.mapper.readValue(metadataNode.toString, classOf[MetadataAction])
            } else if (jsonNode.has("add")) {
              val addNode = jsonNode.get("add")
              JsonUtil.mapper.readValue(addNode.toString, classOf[AddAction])
            } else if (jsonNode.has("remove")) {
              val removeNode = jsonNode.get("remove")
              JsonUtil.mapper.readValue(removeNode.toString, classOf[RemoveAction])
            } else if (jsonNode.has("mergeskip")) {
              val skipNode = jsonNode.get("mergeskip")
              JsonUtil.mapper.readValue(skipNode.toString, classOf[SkipAction])
            } else if (jsonNode.has("addXRef")) {
              val addXRefNode = jsonNode.get("addXRef")
              JsonUtil.mapper.readValue(addXRefNode.toString, classOf[AddXRefAction])
            } else if (jsonNode.has("removeXRef")) {
              val removeXRefNode = jsonNode.get("removeXRef")
              JsonUtil.mapper.readValue(removeXRefNode.toString, classOf[RemoveXRefAction])
            } else {
              throw new IllegalArgumentException(s"Unknown action type in line: $line")
            }
          }.toSeq
        } match {
          case Success(actions) =>
            // Cache the result
            cache.foreach(_.cacheVersion(version, actions))
            logger.debug(s"Read and cached version $version: ${actions.length} actions")
            actions
          case Failure(ex) =>
            logger.error(s"Failed to read transaction log version $version", ex)
            throw new RuntimeException(s"Failed to read transaction log: ${ex.getMessage}", ex)
        }
    }

  def getVersions(): Seq[Long] =
    // Check cache first
    cache.flatMap(_.getCachedVersions()) match {
      case Some(cachedVersions) =>
        logger.debug(s"Using cached versions list: ${cachedVersions.length} versions")
        cachedVersions
      case None =>
        // Read from storage
        Try {
          val files = cloudProvider.listFiles(transactionLogPathStr, recursive = false)
          val result: Seq[Long] = files
            .filter(!_.isDirectory)
            .map { fileInfo =>
              val path = new Path(fileInfo.path)
              path.getName
            }
            .filter(_.endsWith(".json"))
            .filterNot(_.contains("checkpoint")) // Exclude checkpoint files
            .filterNot(_.startsWith("_"))        // Exclude metadata files like _last_checkpoint
            .map(_.replace(".json", "").toLong)
            .sorted

          // Cache the result
          cache.foreach(_.cacheVersions(result))
          logger.debug(s"Read and cached versions list: ${result.length} versions")
          result
        } match {
          case Success(versions) => versions
          case Failure(_) =>
            logger.debug(s"Transaction log directory does not exist yet (normal for new tables): $transactionLogPathStr")
            Seq.empty[Long]
        }
    }

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    val latest   = if (versions.nonEmpty) versions.max else -1L

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

  /** Get cache statistics for monitoring and debugging. Returns None if caching is disabled. */
  def getCacheStats(): Option[CacheStats] =
    cache.map(_.getStats())

  /**
   * Manually invalidate all cached data. Useful for debugging or when you know the transaction log has been modified
   * externally.
   */
  def invalidateCache(): Unit = {
    cache.foreach(_.invalidateAll())
    logger.info("Transaction log cache invalidated manually")
  }

  /** Get the current checkpoint version for debugging. */
  def getLastCheckpointVersion(): Option[Long] =
    checkpoint.flatMap(_.getLastCheckpointVersion())

  /**
   * Prewarm the transaction log cache for faster subsequent reads. Default implementation is a no-op; optimized
   * implementations may override.
   */
  def prewarmCache(): Unit = {
    // Default no-op implementation for standard TransactionLog
    // OptimizedTransactionLog overrides this with aggressive cache population
  }

  /**
   * Record a skipped file in the transaction log with timestamp and reason. This allows tracking of files that couldn't
   * be processed due to corruption or other issues.
   */
  def recordSkippedFile(
    filePath: String,
    reason: String,
    operation: String,
    partitionValues: Option[Map[String, String]] = None,
    size: Option[Long] = None,
    cooldownHours: Int = 24
  ): Long = {
    val timestamp  = System.currentTimeMillis()
    val retryAfter = timestamp + (cooldownHours * 60 * 60 * 1000L) // Convert hours to milliseconds

    // Check if this file was already skipped recently and increment skip count
    val existingSkips = getSkippedFiles().filter(_.path == filePath)
    val skipCount = if (existingSkips.nonEmpty) {
      existingSkips.map(_.skipCount).max + 1
    } else {
      1
    }

    val skipAction = SkipAction(
      path = filePath,
      skipTimestamp = timestamp,
      reason = reason,
      operation = operation,
      partitionValues = partitionValues,
      size = size,
      retryAfter = Some(retryAfter),
      skipCount = skipCount
    )

    val version = getNextVersion()
    writeActions(version, Seq(skipAction))

    logger.debug(s"Recorded skipped file: $filePath (reason: $reason, skip count: $skipCount, retry after: ${java.time.Instant.ofEpochMilli(retryAfter)})")
    version
  }

  /**
   * Record a skipped file with custom timestamps (for testing purposes). This overloaded version allows specifying
   * custom timestamps to test cooldown expiration logic.
   */
  def recordSkippedFileWithTimestamp(
    filePath: String,
    reason: String,
    operation: String,
    skipTimestamp: Long,
    cooldownHours: Int,
    partitionValues: Option[Map[String, String]] = None,
    size: Option[Long] = None
  ): Long = {
    val retryAfter = skipTimestamp + (cooldownHours * 60 * 60 * 1000L) // Convert hours to milliseconds

    // Check if this file was already skipped recently and increment skip count
    val existingSkips = getSkippedFiles().filter(_.path == filePath)
    val skipCount = if (existingSkips.nonEmpty) {
      existingSkips.map(_.skipCount).max + 1
    } else {
      1
    }

    val skipAction = SkipAction(
      path = filePath,
      skipTimestamp = skipTimestamp,
      reason = reason,
      operation = operation,
      partitionValues = partitionValues,
      size = size,
      retryAfter = Some(retryAfter),
      skipCount = skipCount
    )

    val version = getNextVersion()
    writeActions(version, Seq(skipAction))

    logger.debug(s"Recorded skipped file with custom timestamp: $filePath (reason: $reason, skip count: $skipCount, retry after: ${java.time.Instant.ofEpochMilli(retryAfter)})")
    version
  }

  /** Read all actions from all transaction log versions. */
  private def readAllActions(): Seq[Action] = {
    val versions = getVersions()
    versions.flatMap(readVersion)
  }

  /** Get all skipped files from the transaction log. */
  def getSkippedFiles(): Seq[SkipAction] =
    readAllActions().collect { case skip: SkipAction => skip }

  /** Check if a file is currently in cooldown (should not be retried yet). */
  def isFileInCooldown(filePath: String): Boolean = {
    val now = System.currentTimeMillis()
    val recentSkips = getSkippedFiles()
      .filter(_.path == filePath)
      .filter(skip => skip.retryAfter.exists(_ > now))

    recentSkips.nonEmpty
  }

  /** Get files that are currently in cooldown with their retry timestamps. */
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

  /** Filter out files that are in cooldown from a list of candidate files for merge. */
  def filterFilesInCooldown(candidateFiles: Seq[AddAction]): Seq[AddAction] = {
    val filesInCooldown = getFilesInCooldown().keySet
    val filtered        = candidateFiles.filterNot(file => filesInCooldown.contains(file.path))

    val filteredCount = candidateFiles.length - filtered.length
    if (filteredCount > 0) {
      logger.info(s"Filtered out $filteredCount files currently in cooldown period")
      filesInCooldown.foreach { path =>
        val retryTime = getFilesInCooldown().get(path)
        logger.debug(s"File in cooldown: $path (retry after: ${retryTime.map(java.time.Instant.ofEpochMilli)})")
      }
    }

    filtered
  }

  // ============================================================================
  // XRef (Cross-Reference) Operations
  // ============================================================================

  /**
   * Add a cross-reference (XRef) split to the transaction log.
   *
   * XRef splits enable fast query routing by consolidating term dictionaries from
   * multiple source splits into a single lightweight index.
   *
   * @param xrefAction
   *   The XRef action to add
   * @return
   *   The new version number
   */
  def addXRef(xrefAction: AddXRefAction): Long = {
    // Check protocol before writing - requires version 3+
    initializeProtocolIfNeeded()
    assertTableWritable()

    val version = getNextVersion()
    writeActions(version, Seq(xrefAction))

    // Invalidate cache since XRef list has changed
    cache.foreach(_.invalidateAll())

    logger.info(
      s"Added XRef ${xrefAction.xrefId} with ${xrefAction.sourceSplitCount} source splits " +
        s"(${xrefAction.totalTerms} terms, ${xrefAction.size} bytes) in version $version"
    )
    version
  }

  /**
   * Remove a cross-reference (XRef) split from the transaction log.
   *
   * @param path
   *   Path of the XRef to remove
   * @param reason
   *   Why the XRef is being removed: "replaced", "source_changed", or "explicit"
   * @return
   *   The new version number
   */
  def removeXRef(path: String, reason: String): Long = {
    // Find the XRef to get its ID
    val existingXRefs = listXRefs()
    val xrefToRemove = existingXRefs.find(_.path == path)

    val xrefId = xrefToRemove.map(_.xrefId).getOrElse {
      // Extract ID from path if not found (e.g., "_xrefsplits/abcd/xref-{id}.split")
      val fileName = path.split("/").last
      fileName.stripPrefix("xref-").stripSuffix(".split")
    }

    val removeAction = RemoveXRefAction(
      path = path,
      xrefId = xrefId,
      deletionTimestamp = System.currentTimeMillis(),
      reason = reason
    )

    val version = getNextVersion()
    writeActions(version, Seq(removeAction))

    // Invalidate cache since XRef list has changed
    cache.foreach(_.invalidateAll())

    logger.info(s"Removed XRef $xrefId (reason: $reason) in version $version")
    version
  }

  /**
   * Atomically replace an XRef (remove old + add new) in a single transaction.
   *
   * This is used when rebuilding an XRef with updated source splits.
   *
   * @param removeAction
   *   The XRef to remove
   * @param addAction
   *   The new XRef to add
   * @return
   *   The new version number
   */
  def replaceXRef(removeAction: RemoveXRefAction, addAction: AddXRefAction): Long = {
    // Check protocol before writing
    initializeProtocolIfNeeded()
    assertTableWritable()

    val version = getNextVersion()
    writeActions(version, Seq(removeAction, addAction))

    // Invalidate cache since XRef list has changed
    cache.foreach(_.invalidateAll())

    logger.info(
      s"Replaced XRef ${removeAction.xrefId} with ${addAction.xrefId} " +
        s"(${addAction.sourceSplitCount} source splits) in version $version"
    )
    version
  }

  /**
   * List all active (not removed) XRef splits.
   *
   * XRef splits are partition-agnostic and can contain source splits from multiple partitions.
   *
   * @return
   *   Sequence of active XRef actions
   */
  def listXRefs(): Seq[AddXRefAction] = {
    val activeXRefs  = ListBuffer[AddXRefAction]()
    val removedPaths = scala.collection.mutable.Set[String]()

    // Process all versions to build current XRef state
    val versions = getVersions()
    for (version <- versions.sorted) {
      val actions = readVersion(version)
      actions.foreach {
        case addXRef: AddXRefAction =>
          // Remove any existing XRef with same path and add new one
          activeXRefs --= activeXRefs.filter(_.path == addXRef.path)
          if (!removedPaths.contains(addXRef.path)) {
            activeXRefs += addXRef
          }
        case removeXRef: RemoveXRefAction =>
          activeXRefs --= activeXRefs.filter(_.path == removeXRef.path)
          removedPaths += removeXRef.path
        case _ => // Ignore other actions
      }
    }

    activeXRefs.toSeq
  }

  /**
   * List all removed XRef splits (for DESCRIBE TRANSACTION LOG XREFS view).
   *
   * @return
   *   Sequence of removed XRef actions
   */
  def listRemovedXRefs(): Seq[RemoveXRefAction] =
    readAllActions().collect { case removeXRef: RemoveXRefAction => removeXRef }

  /**
   * Get the XRef that covers a specific split (by filename).
   *
   * @param splitPath
   *   Path of the split to find
   * @return
   *   The XRef containing this split, if any
   */
  def getXRefForSplit(splitPath: String): Option[AddXRefAction] = {
    val splitFileName = splitPath.split("/").last
    listXRefs().find { xref =>
      xref.sourceSplitPaths.exists(_.split("/").last == splitFileName)
    }
  }

  /**
   * Get all splits NOT covered by any active XRef.
   *
   * @return
   *   Sequence of AddActions for uncovered splits
   */
  def getSplitsWithoutXRef(): Seq[AddAction] = {
    val activeXRefs = listXRefs()
    val coveredSplitFileNames = activeXRefs
      .flatMap(_.sourceSplitPaths)
      .map(_.split("/").last)
      .toSet

    listFiles().filterNot { addAction =>
      val fileName = addAction.path.split("/").last
      coveredSplitFileNames.contains(fileName)
    }
  }

  /**
   * Commit XRef actions along with data split changes in a single atomic transaction.
   *
   * This is used when merge-on-write or auto-indexing needs to update both data splits
   * and XRef splits atomically.
   *
   * @param dataRemoveActions
   *   Data splits to remove
   * @param dataAddActions
   *   Data splits to add
   * @param xrefRemoveActions
   *   XRefs to remove
   * @param xrefAddActions
   *   XRefs to add
   * @return
   *   The new version number
   */
  def commitWithXRefUpdates(
    dataRemoveActions: Seq[RemoveAction],
    dataAddActions: Seq[AddAction],
    xrefRemoveActions: Seq[RemoveXRefAction],
    xrefAddActions: Seq[AddXRefAction]
  ): Long = {
    val allActions: Seq[Action] = dataRemoveActions ++ dataAddActions ++ xrefRemoveActions ++ xrefAddActions

    if (allActions.isEmpty) {
      return getLatestVersion()
    }

    val version = getNextVersion()
    writeActions(version, allActions)

    // Invalidate cache since file/XRef lists have changed
    cache.foreach(_.invalidateAll())

    logger.info(
      s"Committed mixed transaction in version $version: " +
        s"${dataRemoveActions.size} removes, ${dataAddActions.size} adds, " +
        s"${xrefRemoveActions.size} XRef removes, ${xrefAddActions.size} XRef adds"
    )
    version
  }
}
