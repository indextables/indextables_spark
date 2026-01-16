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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Exception thrown when a transaction log write fails due to concurrent write conflict
 * after all retry attempts have been exhausted.
 *
 * @param message Descriptive error message
 * @param version The version number that experienced the conflict
 * @param attemptsMade Number of retry attempts that were made
 * @param cause The underlying cause (if any)
 */
class TransactionConflictException(
  message: String,
  val version: Long,
  val attemptsMade: Int,
  cause: Throwable = null
) extends RuntimeException(message, cause)

/**
 * Configuration for transaction log write retry behavior.
 *
 * @param maxAttempts Maximum number of retry attempts (default: 10)
 * @param baseDelayMs Initial backoff delay in milliseconds (default: 100)
 * @param maxDelayMs Maximum backoff delay cap in milliseconds (default: 5000)
 */
case class TxRetryConfig(
  maxAttempts: Int = 10,
  baseDelayMs: Long = 100,
  maxDelayMs: Long = 5000
)

/**
 * Metrics from the most recent write operation with retry.
 *
 * @param attemptsMade Total number of attempts made (1 = no retry needed)
 * @param conflictsEncountered Number of conflicts encountered (attemptsMade - 1 if successful)
 * @param finalVersion The version number that was successfully written
 * @param conflictedVersions List of version numbers where conflicts were detected
 */
case class TxRetryMetrics(
  attemptsMade: Int,
  conflictsEncountered: Int,
  finalVersion: Long,
  conflictedVersions: Seq[Long]
)

object TransactionLog {
  // Configuration keys for retry behavior
  val RETRY_MAX_ATTEMPTS = "spark.indextables.transaction.retry.maxAttempts"
  val RETRY_BASE_DELAY_MS = "spark.indextables.transaction.retry.baseDelayMs"
  val RETRY_MAX_DELAY_MS = "spark.indextables.transaction.retry.maxDelayMs"

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

  // Schema registry cache for write-time deduplication
  // Tracks which schemas have been written to MetadataAction.configuration
  // Initialized lazily from existing table state on first write
  @volatile private var schemaRegistryCache: Option[Map[String, String]] = None
  private val schemaRegistryLock = new Object()

  // Retry metrics tracking for testing and monitoring
  // Stores metrics from the most recent write operation with retry
  @volatile private var lastRetryMetrics: Option[TxRetryMetrics] = None

  def getTablePath(): Path = tablePath

  /**
   * Get metrics from the most recent write operation with retry.
   * Useful for testing and monitoring concurrent write behavior.
   *
   * @return The retry metrics from the last write, or None if no writes have occurred
   */
  def getLastRetryMetrics(): Option[TxRetryMetrics] = lastRetryMetrics

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
   *
   * This method uses automatic retry on concurrent write conflict, making it safe for concurrent append operations
   * from multiple processes.
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      return getLatestVersion()
    }

    // Check protocol before writing
    initializeProtocolIfNeeded()
    assertTableWritable()

    // Use retry-enabled write for concurrent safety
    writeActionsWithRetry(addActions)
  }

  /**
   * Add files in overwrite mode - removes all existing files and adds new ones. This is similar to Delta Lake's
   * overwrite mode.
   *
   * This method uses automatic retry on concurrent write conflict. On each retry attempt, the list of files
   * to remove is recalculated to include any files that may have been added by competing writers.
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      logger.warn("Overwrite operation with no files to add")
    }

    val retryConfig = getRetryConfig()
    var attempt = 1
    var lastConflictVersion = -1L
    val conflictedVersions = scala.collection.mutable.ListBuffer[Long]()

    while (attempt <= retryConfig.maxAttempts) {
      // Get current version for this attempt
      val version = if (attempt == 1) {
        getNextVersion()
      } else {
        refreshVersionCounterFromDisk()
        versionCounter.incrementAndGet()
      }

      // Recalculate files to remove on each attempt (may have changed due to concurrent writes)
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

      val allActions: Seq[Action] = removeActions ++ addActions
      val versionFile = new Path(transactionLogPath, f"$version%020d.json")
      val versionFilePath = versionFile.toString

      // Apply schema deduplication
      val (deduplicatedActions, metadataUpdate) = applySchemaDeduplicationForWrite(allActions)
      val actionsToWrite = metadataUpdate match {
        case Some(updatedMetadata) =>
          val hasMetadataInActions = allActions.exists(_.isInstanceOf[MetadataAction])
          if (hasMetadataInActions) {
            deduplicatedActions.map {
              case _: MetadataAction => updatedMetadata
              case other => other
            }
          } else {
            updatedMetadata +: deduplicatedActions
          }
        case None =>
          deduplicatedActions
      }

      val codec = getCompressionCodec()
      val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

      val writeSucceeded = StreamingActionWriter.writeActionsStreaming(
        actions = actionsToWrite,
        cloudProvider = cloudProvider,
        path = versionFilePath,
        codec = codec,
        ifNotExists = true
      )

      if (writeSucceeded) {
        metadataUpdate.foreach { updatedMetadata =>
          val newSchemas = SchemaDeduplication.extractSchemaRegistry(updatedMetadata.configuration)
          updateSchemaRegistry(newSchemas)
        }
        cache.foreach(_.invalidateVersionDependentCaches())

        // Store retry metrics for testing and monitoring
        lastRetryMetrics = Some(TxRetryMetrics(
          attemptsMade = attempt,
          conflictsEncountered = attempt - 1,
          finalVersion = version,
          conflictedVersions = conflictedVersions.toSeq
        ))

        if (attempt > 1) {
          logger.info(s"Overwrite operation: removed ${removeActions.length} files, added ${addActions.length} files in version $version after $attempt attempts")
        } else {
          logger.info(s"Overwrite operation: removed ${removeActions.length} files, added ${addActions.length} files in version $version")
        }

        handleCheckpointCreation(version)
        return version
      }

      // Conflict detected
      conflictedVersions += version
      lastConflictVersion = version
      logger.warn(
        s"Concurrent write conflict during overwrite at version $version (attempt $attempt/${retryConfig.maxAttempts}). Retrying."
      )

      if (attempt < retryConfig.maxAttempts) {
        val delay = calculateRetryDelay(attempt, retryConfig)
        Thread.sleep(delay)
      }

      attempt += 1
    }

    throw new TransactionConflictException(
      s"Overwrite operation failed after ${retryConfig.maxAttempts} attempts. Last conflicted version: $lastConflictVersion.",
      lastConflictVersion,
      retryConfig.maxAttempts
    )
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
        // Restore schemas if any AddActions have docMappingRef (schema deduplication was used)
        val restoredResult = restoreSchemasInAddActions(result)
        // Cache the result
        cache.foreach(_.cacheFiles(restoredResult))
        logger.debug(s"Computed and cached files list: ${restoredResult.length} files")
        restoredResult
    }
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
   * This method uses automatic retry on concurrent write conflict. Merge operations are idempotent since
   * RemoveActions for already-removed files are no-ops.
   *
   * @param removeActions
   *   Files to remove
   * @param addActions
   *   Files to add
   * @return
   *   The new version number
   */
  override def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val actions: Seq[Action] = removeActions ++ addActions
    writeActionsWithRetry(actions)
  }

  /**
   * Commits remove actions to mark files as logically deleted.
   *
   * This operation marks files as removed in the transaction log without physically deleting them. The files become
   * invisible to queries but remain on disk for recovery and time-travel purposes.
   *
   * This method uses automatic retry on concurrent write conflict. Remove operations are idempotent.
   */
  override def commitRemoveActions(removeActions: Seq[RemoveAction]): Long = {
    if (removeActions.isEmpty) {
      return getLatestVersion()
    }

    val version = writeActionsWithRetry(removeActions)

    // Invalidate cache since file list has changed
    cache.foreach(_.invalidateAll())

    logger.info(s"Committed ${removeActions.length} remove actions in version $version")
    version
  }

  private def writeAction(version: Long, action: Action): Unit =
    writeActions(version, Seq(action))

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

  /**
   * Write actions with automatic retry on concurrent write conflict.
   *
   * This method handles version assignment and retry logic internally:
   * 1. Assigns a version number
   * 2. Attempts to write the actions
   * 3. On conflict (file already exists), refreshes version from disk and retries
   * 4. Uses exponential backoff with jitter between retries
   *
   * @param actions The actions to write (without version - version is assigned internally)
   * @return The version number that was successfully written
   * @throws TransactionConflictException if all retry attempts fail
   */
  private def writeActionsWithRetry(actions: Seq[Action]): Long = {
    val retryConfig = getRetryConfig()
    var attempt = 1
    var lastConflictVersion = -1L
    val conflictedVersions = scala.collection.mutable.ListBuffer[Long]()

    while (attempt <= retryConfig.maxAttempts) {
      // Get next version - on first attempt uses local counter, on retry reads from disk first
      val version = if (attempt == 1) {
        getNextVersion()
      } else {
        // On retry, refresh from disk to see versions written by other processes
        refreshVersionCounterFromDisk()
        versionCounter.incrementAndGet()
      }

      val versionFile = new Path(transactionLogPath, f"$version%020d.json")
      val versionFilePath = versionFile.toString

      // Apply schema deduplication fresh on each attempt
      // This is important because a competing writer may have registered new schemas
      val (deduplicatedActions, metadataUpdate) = applySchemaDeduplicationForWrite(actions)

      // Build final actions list, including metadata update if needed
      val actionsToWrite = metadataUpdate match {
        case Some(updatedMetadata) =>
          val hasMetadataInActions = actions.exists(_.isInstanceOf[MetadataAction])
          if (hasMetadataInActions) {
            deduplicatedActions.map {
              case _: MetadataAction => updatedMetadata
              case other => other
            }
          } else {
            updatedMetadata +: deduplicatedActions
          }
        case None =>
          deduplicatedActions
      }

      val codec = getCompressionCodec()
      val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

      // Attempt the write with conditional check (ifNotExists=true)
      val writeSucceeded = StreamingActionWriter.writeActionsStreaming(
        actions = actionsToWrite,
        cloudProvider = cloudProvider,
        path = versionFilePath,
        codec = codec,
        ifNotExists = true
      )

      if (writeSucceeded) {
        // Success! Update caches and return
        metadataUpdate.foreach { updatedMetadata =>
          val newSchemas = SchemaDeduplication.extractSchemaRegistry(updatedMetadata.configuration)
          updateSchemaRegistry(newSchemas)
        }
        cache.foreach(_.invalidateVersionDependentCaches())

        // Store retry metrics for testing and monitoring
        lastRetryMetrics = Some(TxRetryMetrics(
          attemptsMade = attempt,
          conflictsEncountered = attempt - 1,
          finalVersion = version,
          conflictedVersions = conflictedVersions.toSeq
        ))

        if (attempt > 1) {
          logger.info(
            s"Written ${actionsToWrite.length} actions to version $version$compressionInfo after $attempt attempts (conflict on version $lastConflictVersion): ${actionsToWrite.map(_.getClass.getSimpleName).mkString(", ")}"
          )
        } else {
          logger.info(
            s"Written ${actionsToWrite.length} actions to version $version$compressionInfo: ${actionsToWrite.map(_.getClass.getSimpleName).mkString(", ")}"
          )
        }

        // Handle checkpoint creation after successful write
        handleCheckpointCreation(version)

        return version
      }

      // Write failed - concurrent conflict detected
      lastConflictVersion = version
      conflictedVersions += version
      logger.warn(
        s"Concurrent write conflict at version $version (attempt $attempt/${retryConfig.maxAttempts}). " +
          "Another process wrote to this version first. Retrying with next version."
      )

      if (attempt < retryConfig.maxAttempts) {
        val delay = calculateRetryDelay(attempt, retryConfig)
        logger.debug(s"Waiting ${delay}ms before retry attempt ${attempt + 1}")
        Thread.sleep(delay)
      }

      attempt += 1
    }

    // All retries exhausted
    throw new TransactionConflictException(
      s"Failed to write transaction log after ${retryConfig.maxAttempts} attempts. " +
        s"Last conflicted version: $lastConflictVersion. " +
        "This may indicate high write contention on this table. " +
        "Consider increasing spark.indextables.transaction.retry.maxAttempts or reducing concurrent writers.",
      lastConflictVersion,
      retryConfig.maxAttempts
    )
  }

  /**
   * Handle checkpoint creation after a successful write.
   * Extracted to keep writeActionsWithRetry readable.
   */
  private def handleCheckpointCreation(version: Long): Unit = {
    checkpoint.foreach { cp =>
      if (cp.shouldCreateCheckpoint(version)) {
        try {
          val allCurrentActions = getAllCurrentActions(version)
          cp.createCheckpoint(version, allCurrentActions)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to create checkpoint at version $version", e)
        }
      }
    }
  }

  /**
   * Write actions to a specific version (internal method without retry).
   *
   * This method is used for initialization (version 0) where retry doesn't make sense,
   * and for protocol upgrades. For normal data writes, use writeActionsWithRetry instead.
   */
  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

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

    // Update schema registry cache after successful write
    metadataUpdate.foreach { updatedMetadata =>
      val newSchemas = SchemaDeduplication.extractSchemaRegistry(updatedMetadata.configuration)
      updateSchemaRegistry(newSchemas)
    }

    // Invalidate caches after any write operation since the transaction log state has changed
    cache.foreach(_.invalidateVersionDependentCaches())

    logger.info(
      s"Written ${actionsToWrite.length} actions to version $version$compressionInfo: ${actionsToWrite.map(_.getClass.getSimpleName).mkString(", ")}"
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
            case _: SkipAction => // Skip actions are transient
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
        case _: SkipAction =>
        // Skip actions are transient and not included in checkpoints
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

  override def readVersion(version: Long): Seq[Action] =
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
          // Use full streaming: cloud storage -> decompression -> line parsing
          // This avoids OOM for large version files (>1GB)
          parseActionsFromStream(versionFilePath)
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

  override def getVersions(): Seq[Long] =
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

  /**
   * Get retry configuration from options.
   */
  private def getRetryConfig(): TxRetryConfig = {
    TxRetryConfig(
      maxAttempts = options.getInt(TransactionLog.RETRY_MAX_ATTEMPTS, 10),
      baseDelayMs = options.getLong(TransactionLog.RETRY_BASE_DELAY_MS, 100),
      maxDelayMs = options.getLong(TransactionLog.RETRY_MAX_DELAY_MS, 5000)
    )
  }

  /**
   * Refresh the version counter from disk to handle concurrent writes from other JVMs.
   *
   * This method reads the actual version files from storage to determine the latest version,
   * ignoring the JVM-local version counter. This is necessary when a concurrent write from
   * another process has created a version file that our local counter didn't know about.
   *
   * @return The latest version number found on disk
   */
  private def refreshVersionCounterFromDisk(): Long = {
    // Invalidate version-related caches to force re-read from storage
    cache.foreach(_.invalidateVersionDependentCaches())

    // Read versions directly from storage
    val versions = getVersions()
    val latest = if (versions.nonEmpty) versions.max else -1L

    // Update our local counter to match
    versionCounter.set(latest)

    logger.debug(s"Refreshed version counter from disk: latest version is $latest")
    latest
  }

  /**
   * Calculate exponential backoff delay with jitter for retry attempts.
   *
   * Uses the same pattern as CloudDownloadManager for consistency.
   *
   * @param attempt The current attempt number (1-based)
   * @param config The retry configuration
   * @return The delay in milliseconds to wait before the next attempt
   */
  private def calculateRetryDelay(attempt: Int, config: TxRetryConfig): Long = {
    val exponentialDelay = config.baseDelayMs * math.pow(2, attempt - 1).toLong
    val cappedDelay = math.min(exponentialDelay, config.maxDelayMs)
    // Add 10% jitter to prevent thundering herd
    val jitter = (cappedDelay * 0.1 * scala.util.Random.nextDouble()).toLong
    cappedDelay + jitter
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
    invalidateSchemaRegistry()
    logger.info("Transaction log cache invalidated manually")
  }

  /** Get the current checkpoint version for debugging. */
  def getLastCheckpointVersion(): Option[Long] =
    checkpoint.flatMap(_.getLastCheckpointVersion())

  /** Get the full checkpoint info (including multi-part checkpoint details). */
  def getLastCheckpointInfo(): Option[LastCheckpointInfo] =
    checkpoint.flatMap(_.getLastCheckpointInfo())

  /** Get all actions from the latest checkpoint (consolidated state). */
  override def getCheckpointActions(): Option[Seq[Action]] =
    checkpoint.flatMap(_.getActionsFromCheckpoint())

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
          val jsonNode = JsonUtil.mapper.readTree(line)

          // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
          val action: Action = if (jsonNode.has("protocol")) {
            JsonUtil.mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction])
          } else if (jsonNode.has("metaData")) {
            JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction])
          } else if (jsonNode.has("add")) {
            JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction])
          } else if (jsonNode.has("remove")) {
            JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction])
          } else if (jsonNode.has("mergeskip")) {
            JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction])
          } else {
            throw new IllegalArgumentException(s"Unknown action type in line: $line")
          }
          actions += action
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally {
      reader.close()
    }
  }

  /**
   * Get all skipped files from the transaction log using checkpoint-aware reading.
   * This is optimized to read from checkpoint + incremental versions, not all versions.
   */
  def getSkippedFiles(): Seq[SkipAction] = {
    val skips = ListBuffer[SkipAction]()

    // Try to get base state from checkpoint first (same pattern as listFiles)
    checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
      case Some(checkpointActions) =>
        // Extract SkipActions from checkpoint
        checkpointActions.foreach {
          case skip: SkipAction => skips += skip
          case _                => // Ignore other actions
        }

        // Then read incremental changes since checkpoint
        val checkpointVersion       = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)
        val allVersions             = getVersions()
        val versionsAfterCheckpoint = allVersions.filter(_ > checkpointVersion)

        if (versionsAfterCheckpoint.nonEmpty) {
          logger.debug(
            s"Reading ${versionsAfterCheckpoint.length} versions after checkpoint $checkpointVersion for SkipActions"
          )
          checkpoint.get.readVersionsInParallel(versionsAfterCheckpoint).foreach {
            case (_, actions) =>
              actions.foreach {
                case skip: SkipAction => skips += skip
                case _                => // Ignore other actions
              }
          }
        }

      case None =>
        // No checkpoint available - read all versions (fallback)
        val versions = getVersions()
        if (versions.nonEmpty) {
          logger.debug(s"No checkpoint available, reading ${versions.length} versions for SkipActions")
          checkpoint match {
            case Some(cp) =>
              cp.readVersionsInParallel(versions).foreach {
                case (_, actions) =>
                  actions.foreach {
                    case skip: SkipAction => skips += skip
                    case _                => // Ignore other actions
                  }
              }
            case None =>
              // Sequential fallback
              for (version <- versions) {
                val actions = readVersion(version)
                actions.foreach {
                  case skip: SkipAction => skips += skip
                  case _                => // Ignore other actions
                }
              }
          }
        }
    }

    skips.toSeq
  }

  /** Check if a file is currently in cooldown (should not be retried yet). */
  def isFileInCooldown(filePath: String): Boolean = {
    val now = System.currentTimeMillis()
    // Use getFilesInCooldown which already filters for active cooldowns
    getFilesInCooldown().contains(filePath)
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
    val cooldownMap     = getFilesInCooldown()
    val filesInCooldown = cooldownMap.keySet
    val filtered        = candidateFiles.filterNot(file => filesInCooldown.contains(file.path))

    val filteredCount = candidateFiles.length - filtered.length
    if (filteredCount > 0) {
      logger.info(s"Filtered out $filteredCount files currently in cooldown period")
      filesInCooldown.foreach { path =>
        // Reuse the already-computed map instead of calling getFilesInCooldown() again
        val retryTime = cooldownMap.get(path)
        logger.debug(s"File in cooldown: $path (retry after: ${retryTime.map(java.time.Instant.ofEpochMilli)})")
      }
    }

    filtered
  }
}
