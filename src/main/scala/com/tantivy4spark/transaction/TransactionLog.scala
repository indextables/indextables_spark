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


package com.tantivy4spark.transaction

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.util.JsonUtil
import com.tantivy4spark.io.{ProtocolBasedIOFactory, CloudStorageProviderFactory}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicLong

class TransactionLog(tablePath: Path, spark: SparkSession, options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())) extends AutoCloseable {

  // Check if this is being used directly instead of through the factory
  private val allowDirectUsage = options.getBoolean("spark.tantivy4spark.transaction.allowDirectUsage", false)
  if (!allowDirectUsage) {
    throw new RuntimeException(
      s"TransactionLog should no longer be used directly. Use TransactionLogFactory.create() instead. " +
      s"If you need to use this class directly for testing, set spark.tantivy4spark.transaction.allowDirectUsage=true"
    )
  }

  private val logger = LoggerFactory.getLogger(classOf[TransactionLog])
  
  // Determine if we should use cloud-optimized transaction log
  private val protocol = ProtocolBasedIOFactory.determineProtocol(tablePath.toString)
  private val useCloudOptimized = protocol match {
    case ProtocolBasedIOFactory.S3Protocol => !options.getBoolean("spark.tantivy4spark.transaction.force.hadoop", false)
    case _ => false
  }
  
  // Use cloud storage provider instead of direct Hadoop filesystem
  private val cloudProvider = CloudStorageProviderFactory.createProvider(tablePath.toString, options, spark.sparkContext.hadoopConfiguration)
  private val transactionLogPath = new Path(tablePath, "_transaction_log")
  private val transactionLogPathStr = transactionLogPath.toString
  
  // Cache configuration and initialization
  private val cacheEnabled = options.getBoolean("spark.tantivy4spark.transaction.cache.enabled", true)
  private val cacheExpirationSeconds = options.getLong("spark.tantivy4spark.transaction.cache.expirationSeconds", 5 * 60L) // 5 minutes default
  private val cache = if (cacheEnabled) Some(new TransactionLogCache(cacheExpirationSeconds)) else None

  // Checkpoint configuration and initialization
  private val checkpointEnabled = options.getBoolean("spark.tantivy4spark.checkpoint.enabled", true)
  private val checkpoint = if (checkpointEnabled) Some(new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)) else None

  // Atomic version counter for thread-safe version assignment
  private val versionCounter = new AtomicLong(-1L)

  def getTablePath(): Path = tablePath

  override def close(): Unit = {
    cache.foreach(_.shutdown())
    checkpoint.foreach(_.close())
    cloudProvider.close()
  }

  def initialize(schema: StructType): Unit = {
    initialize(schema, Seq.empty)
  }

  def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
        if (!cloudProvider.exists(transactionLogPathStr)) {
          cloudProvider.createDirectory(transactionLogPathStr)
          
          // Validate partition columns exist in schema
          val schemaFields = schema.fieldNames.toSet
          val invalidPartitionCols = partitionColumns.filterNot(schemaFields.contains)
          if (invalidPartitionCols.nonEmpty) {
            throw new IllegalArgumentException(s"Partition columns ${invalidPartitionCols.mkString(", ")} not found in schema")
          }
          
          // DEBUG: Log the original schema being written
          logger.info(s"Writing schema to transaction log: ${schema.prettyJson}")
          logger.info(s"Partition columns: ${partitionColumns.mkString(", ")}")
          schema.fields.foreach { field =>
            logger.info(s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}")
          }
          
          // Write initial metadata file
          val metadataAction = MetadataAction(
            id = java.util.UUID.randomUUID().toString,
            name = None,
            description = None,
            format = FileFormat("tantivy4spark", Map.empty),
            schemaString = schema.json,
            partitionColumns = partitionColumns,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
          
          writeAction(0, metadataAction)
        }
  }

  def addFile(addAction: AddAction): Long = {
        // Legacy Hadoop implementation
        val version = getNextVersion()
        writeAction(version, addAction)
        version
  }

  /**
   * Add multiple files in a single transaction (like Delta Lake).
   * This creates one JSON file with multiple ADD entries.
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
        // Legacy Hadoop implementation
        if (addActions.isEmpty) {
          return getLatestVersion()
        }

        val version = getNextVersion()
        writeActions(version, addActions)
        version
  }
  
  /**
   * Add files in overwrite mode - removes all existing files and adds new ones.
   * This is similar to Delta Lake's overwrite mode.
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
                  case add: AddAction => files += add
                  case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                  case _ => // Ignore other actions for file listing
                }

                // Then apply incremental changes since checkpoint
                val checkpointVersion = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)
                val allVersions = getVersions()
                val versionsAfterCheckpoint = allVersions.filter(_ > checkpointVersion)

                if (versionsAfterCheckpoint.nonEmpty) {
                  logger.debug(s"Reading ${versionsAfterCheckpoint.length} versions after checkpoint $checkpointVersion in parallel")
                  val parallelResults = checkpoint.get.readVersionsInParallel(versionsAfterCheckpoint)

                  // Apply changes in version order
                  for (version <- versionsAfterCheckpoint.sorted) {
                    parallelResults.get(version).foreach { actions =>
                      actions.foreach {
                        case add: AddAction => files += add
                        case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                        case _ => // Ignore other actions for file listing
                      }
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
                      for (version <- versions.sorted) {
                        parallelResults.get(version).foreach { actions =>
                          actions.foreach {
                            case add: AddAction => files += add
                            case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                            case _ => // Ignore other actions for file listing
                          }
                        }
                      }
                    case None =>
                      // Fallback to sequential reading (original behavior)
                      for (version <- versions) {
                        val actions = readVersion(version)
                        actions.foreach {
                          case add: AddAction => files += add
                          case remove: RemoveAction => files --= files.filter(_.path == remove.path)
                          case _ => // Ignore other actions for file listing
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
  
  /**
   * Get the total row count across all active files.
   */
  def getTotalRowCount(): Long = {
    listFiles().map { file =>
      file.numRecords.map { (count: Any) =>
        // Handle any numeric type and convert to Long
        count match {
          case l: Long => l
          case i: Int => i.toLong
          case i: java.lang.Integer => i.toLong
          case _ => count.toString.toLong
        }
      }.getOrElse(0L)
    }.sum
  }

  def getSchema(): Option[StructType] = {
        // Legacy Hadoop implementation
        Try {
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
                  logger.info(s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}")
                }
                deserializedSchema
            }
          } else {
            None
          }
        }.getOrElse(None)
  }

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
   * Atomically commit a set of REMOVE and ADD actions in a single transaction.
   * This is used for operations like MERGE SPLITS where we need to atomically
   * replace old split files with merged ones.
   * 
   * @param removeActions Files to remove
   * @param addActions Files to add
   * @return The new version number
   */
  def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long = {
    val version = getNextVersion()
    val actions = removeActions ++ addActions
    writeActions(version, actions)
    version
  }

  private def writeAction(version: Long, action: Action): Unit = {
    writeActions(version, Seq(action))
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    val content = new StringBuilder()
    actions.foreach { action =>
      // Wrap actions in the appropriate delta log format
      val wrappedAction = action match {
        case metadata: MetadataAction => Map("metaData" -> metadata)
        case add: AddAction => Map("add" -> add)
        case remove: RemoveAction => Map("remove" -> remove)
        case skip: SkipAction => Map("mergeskip" -> skip)
      }

      val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
      content.append(actionJson).append("\n")
    }

    cloudProvider.writeFile(versionFilePath, content.toString.getBytes("UTF-8"))

    // Invalidate caches after any write operation since the transaction log state has changed
    cache.foreach(_.invalidateVersionDependentCaches())

    logger.info(s"Written ${actions.length} actions to version $version: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")

    // Check if we should create a checkpoint
    checkpoint.foreach { cp =>
      if (cp.shouldCreateCheckpoint(version)) {
        try {
          // Get all current actions to create checkpoint
          val allCurrentActions = getAllCurrentActions(version)
          cp.createCheckpoint(version, allCurrentActions)

          // Clean up old transaction log files after successful checkpoint
          cp.cleanupOldVersions(version)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to create checkpoint at version $version", e)
            // Continue - checkpoint failure shouldn't fail the write operation
        }
      }
    }
  }

  /**
   * Get all current actions up to the specified version for checkpoint creation.
   */
  private def getAllCurrentActions(upToVersion: Long): Seq[Action] = {
    val allActions = ListBuffer[Action]()
    var latestMetadata: Option[MetadataAction] = None
    val activeFiles = ListBuffer[AddAction]()

    val versions = getVersions().filter(_ <= upToVersion)

    // Process all versions to get the current state
    for (version <- versions.sorted) {
      val actions = readVersion(version)
      actions.foreach {
        case metadata: MetadataAction =>
          latestMetadata = Some(metadata)
        case add: AddAction =>
          // Remove any existing file with the same path and add the new one
          activeFiles --= activeFiles.filter(_.path == add.path)
          activeFiles += add
        case remove: RemoveAction =>
          activeFiles --= activeFiles.filter(_.path == remove.path)
      }
    }

    // Add metadata first
    latestMetadata.foreach(allActions += _)

    // Add all active files
    allActions ++= activeFiles

    allActions.toSeq
  }

  /**
   * Get partition columns from metadata.
   */
  def getPartitionColumns(): Seq[String] = {
    try {
      getMetadata().partitionColumns
    } catch {
      case _: Exception => Seq.empty
    }
  }
  
  /**
   * Check if the table is partitioned.
   */
  def isPartitioned(): Boolean = {
    getPartitionColumns().nonEmpty
  }
  
  /**
   * Get the current metadata action from the transaction log.
   */
  def getMetadata(): MetadataAction = {
    // Check cache first
    cache.flatMap(_.getCachedMetadata()) match {
      case Some(cachedMetadata) =>
        logger.debug("Using cached metadata")
        cachedMetadata
      case None =>
        // Compute from transaction log
        val latestVersion = getLatestVersion()
        
        // Look for metadata in reverse chronological order
        for (version <- latestVersion to 0L by -1) {
          val actions = readVersion(version)
          actions.collectFirst {
            case metadata: MetadataAction => metadata
          } match {
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
  }

  def readVersion(version: Long): Seq[Action] = {
    // Check cache first
    cache.flatMap(_.getCachedVersion(version)) match {
      case Some(cachedActions) =>
        logger.debug(s"Using cached version $version: ${cachedActions.length} actions")
        cachedActions
      case None =>
        // Read from storage
        val versionFile = new Path(transactionLogPath, f"$version%020d.json")
        val versionFilePath = versionFile.toString
        
        if (!cloudProvider.exists(versionFilePath)) {
          return Seq.empty
        }

        Try {
          val content = new String(cloudProvider.readFile(versionFilePath), "UTF-8")
          val lines = content.split("\n").filter(_.nonEmpty)
            
            lines.map { line =>
              val jsonNode = JsonUtil.mapper.readTree(line)

              if (jsonNode.has("metaData")) {
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
  }

  def getVersions(): Seq[Long] = {
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
            .filterNot(_.startsWith("_")) // Exclude metadata files like _last_checkpoint
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
  }

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    val latest = if (versions.nonEmpty) versions.max else -1L

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
   * Get cache statistics for monitoring and debugging.
   * Returns None if caching is disabled.
   */
  def getCacheStats(): Option[CacheStats] = {
    cache.map(_.getStats())
  }
  
  /**
   * Manually invalidate all cached data.
   * Useful for debugging or when you know the transaction log has been modified externally.
   */
  def invalidateCache(): Unit = {
    cache.foreach(_.invalidateAll())
    logger.info("Transaction log cache invalidated manually")
  }

  /**
   * Get the current checkpoint version for debugging.
   */
  def getLastCheckpointVersion(): Option[Long] = {
    checkpoint.flatMap(_.getLastCheckpointVersion())
  }

  /**
   * Record a skipped file in the transaction log with timestamp and reason.
   * This allows tracking of files that couldn't be processed due to corruption or other issues.
   */
  def recordSkippedFile(
    filePath: String,
    reason: String,
    operation: String,
    partitionValues: Option[Map[String, String]] = None,
    size: Option[Long] = None,
    cooldownHours: Int = 24
  ): Long = {
    val timestamp = System.currentTimeMillis()
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

    logger.info(s"Recorded skipped file: $filePath (reason: $reason, skip count: $skipCount, retry after: ${java.time.Instant.ofEpochMilli(retryAfter)})")
    version
  }

  /**
   * Record a skipped file with custom timestamps (for testing purposes).
   * This overloaded version allows specifying custom timestamps to test cooldown expiration logic.
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

    logger.info(s"Recorded skipped file with custom timestamp: $filePath (reason: $reason, skip count: $skipCount, retry after: ${java.time.Instant.ofEpochMilli(retryAfter)})")
    version
  }

  /**
   * Read all actions from all transaction log versions.
   */
  private def readAllActions(): Seq[Action] = {
    val versions = getVersions()
    versions.flatMap(readVersion)
  }

  /**
   * Get all skipped files from the transaction log.
   */
  def getSkippedFiles(): Seq[SkipAction] = {
    readAllActions().collect { case skip: SkipAction => skip }
  }

  /**
   * Check if a file is currently in cooldown (should not be retried yet).
   */
  def isFileInCooldown(filePath: String): Boolean = {
    val now = System.currentTimeMillis()
    val recentSkips = getSkippedFiles()
      .filter(_.path == filePath)
      .filter(skip => skip.retryAfter.exists(_ > now))

    recentSkips.nonEmpty
  }

  /**
   * Get files that are currently in cooldown with their retry timestamps.
   */
  def getFilesInCooldown(): Map[String, Long] = {
    val now = System.currentTimeMillis()
    getSkippedFiles()
      .filter(skip => skip.retryAfter.exists(_ > now))
      .groupBy(_.path)
      .map { case (path, skips) =>
        // Get the latest retry time for this path
        val latestRetryAfter = skips.flatMap(_.retryAfter).max
        path -> latestRetryAfter
      }
  }

  /**
   * Filter out files that are in cooldown from a list of candidate files for merge.
   */
  def filterFilesInCooldown(candidateFiles: Seq[AddAction]): Seq[AddAction] = {
    val filesInCooldown = getFilesInCooldown().keySet
    val filtered = candidateFiles.filterNot(file => filesInCooldown.contains(file.path))

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
}
