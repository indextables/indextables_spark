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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.util.JsonUtil
import com.tantivy4spark.io.{ProtocolBasedIOFactory, CloudStorageProviderFactory}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import java.util.concurrent.atomic.{AtomicReference, AtomicLong}

/**
 * Optimized transaction log implementation that integrates all performance enhancements
 * from the optimization plan including thread pools, advanced caching, parallel operations,
 * memory optimization, and advanced features.
 */
class OptimizedTransactionLog(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
) extends AutoCloseable {

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
    val legacyExpirationSeconds = options.getLong("spark.tantivy4spark.transaction.cache.expirationSeconds", -1)
    val legacyExpirationMinutes = if (legacyExpirationSeconds > 0) {
      if (legacyExpirationSeconds < 60) 1L // Use 1 minute minimum for very short times
      else legacyExpirationSeconds / 60
    } else -1L

    new EnhancedTransactionLogCache(
      logCacheSize = options.getLong("spark.tantivy4spark.cache.log.size", 1000),
      logCacheTTLMinutes = if (legacyExpirationMinutes > 0) legacyExpirationMinutes else options.getLong("spark.tantivy4spark.cache.log.ttl", 5),
      snapshotCacheSize = options.getLong("spark.tantivy4spark.cache.snapshot.size", 100),
      snapshotCacheTTLMinutes = if (legacyExpirationMinutes > 0) legacyExpirationMinutes else options.getLong("spark.tantivy4spark.cache.snapshot.ttl", 10),
      fileListCacheSize = options.getLong("spark.tantivy4spark.cache.filelist.size", 50),
      fileListCacheTTLMinutes = if (legacyExpirationMinutes > 0) legacyExpirationMinutes else options.getLong("spark.tantivy4spark.cache.filelist.ttl", 2),
      metadataCacheSize = options.getLong("spark.tantivy4spark.cache.metadata.size", 100),
      metadataCacheTTLMinutes = if (legacyExpirationMinutes > 0) legacyExpirationMinutes else options.getLong("spark.tantivy4spark.cache.metadata.ttl", 30)
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
  private val checkpointEnabled = options.getBoolean("spark.tantivy4spark.checkpoint.enabled", true)
  private val checkpoint = if (checkpointEnabled) {
    Some(new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options))
  } else None

  // Current snapshot reference for lock-free reading
  private val currentSnapshot = new AtomicReference[Option[Snapshot]](None)

  // Version counter for atomic increment
  private val versionCounter = new AtomicLong(-1L)

  // Performance configuration
  private val maxStaleness = options.getLong("spark.tantivy4spark.snapshot.maxStaleness", 5000).millis
  private val parallelReadEnabled = options.getBoolean("spark.tantivy4spark.parallel.read.enabled", true)
  private val asyncUpdatesEnabled = options.getBoolean("spark.tantivy4spark.async.updates.enabled", false) // Disabled by default for stability

  def getTablePath(): Path = tablePath

  override def close(): Unit = {
    // Clean up resources
    enhancedCache.clearAll()
    checkpoint.foreach(_.close())
    cloudProvider.close()
    // Note: Thread pools are managed globally and not closed here
  }

  /**
   * Initialize transaction log with schema
   */
  def initialize(schema: StructType, partitionColumns: Seq[String] = Seq.empty): Unit = {
    if (!cloudProvider.exists(transactionLogPath.toString)) {
      cloudProvider.createDirectory(transactionLogPath.toString)

      // Validate partition columns
      val schemaFields = schema.fieldNames.toSet
      val invalidPartitionCols = partitionColumns.filterNot(schemaFields.contains)
      if (invalidPartitionCols.nonEmpty) {
        throw new IllegalArgumentException(
          s"Partition columns ${invalidPartitionCols.mkString(", ")} not found in schema"
        )
      }

      logger.info(s"Initializing transaction log with schema: ${schema.prettyJson}")

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

  /**
   * Add files using parallel batch write
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      return getLatestVersion()
    }

    // Use atomic version generation to prevent race conditions
    val version = getNextVersion()
    println(s"[DEBUG] Assigning version $version for addFiles with ${addActions.size} actions")

    // Use parallel write for large batches
    if (addActions.size > 100 && parallelReadEnabled) {
      val future = parallelOps.writeBatchParallel(version, addActions)
      Await.result(future, 60.seconds)
    } else {
      writeActions(version, addActions)
    }

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

  /**
   * Remove a single file by adding a RemoveAction
   */
  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long = {
    // Use atomic version generation to prevent race conditions
    val version = getNextVersion()
    println(s"[DEBUG] Assigning version $version for removeFile '$path'")

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

    println(s"[DEBUG] Removed file '$path' at version $version")
    version
  }

  /**
   * Overwrite files with optimized operations
   * For overwrite, we need to:
   * 1. Get all existing files
   * 2. Create RemoveActions for all of them
   * 3. Add the new files
   * 4. Write both removes and adds in the same transaction
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long = {
    println(s"[DEBUG OVERWRITE] Starting overwrite operation with ${addActions.size} files")
    logger.info(s"Starting overwrite operation with ${addActions.size} files")

    // Get existing files using optimized listing
    val existingFiles = listFilesOptimized()
    println(s"[DEBUG OVERWRITE] Found ${existingFiles.size} existing files to remove: ${existingFiles.map(_.path).mkString(", ")}")
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
    println(s"[DEBUG OVERWRITE] Using atomic version assignment: $version")
    // Write removes first, then adds - this ensures proper overwrite semantics
    val allActions: Seq[Action] = removeActions ++ addActions
    println(s"[DEBUG OVERWRITE] Writing ${allActions.size} actions (${removeActions.size} removes + ${addActions.size} adds) to version $version")

    // Use parallel write for large batches
    if (allActions.size > 100 && parallelReadEnabled) {
      val future = parallelOps.writeBatchParallel(version, allActions)
      Await.result(future, 60.seconds)
    } else {
      writeActions(version, allActions)
    }

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

  /**
   * List files with enhanced caching and parallel operations
   */
  def listFiles(): Seq[AddAction] = {
    listFilesOptimized()
  }

  private def listFilesOptimized(): Seq[AddAction] = {
    // Get versions once and pass consistently to avoid FileSystem caching issues
    val versions = getVersions()
    val latestVersion = versions.sorted.lastOption.getOrElse(-1L)
    val checksum = computeCurrentChecksumWithVersions(versions)
    println(s"[DEBUG] Computed checksum: $checksum, latest version: $latestVersion")

    val cached = enhancedCache.getOrComputeFileList(tablePath.toString, checksum, {
      println(s"[DEBUG] Cache miss, computing file list for checksum: $checksum")
      logger.info(s"Computing file list using optimized operations for checksum: $checksum")

      // Try to get from current snapshot first, but verify it's up to date with latest version
      currentSnapshot.get() match {
        case Some(snapshot) if snapshot.age < maxStaleness.toMillis && snapshot.version >= latestVersion =>
          println(s"[DEBUG] Using cached snapshot (version ${snapshot.version}) with ${snapshot.files.size} files")
          snapshot.files

        case Some(snapshot) =>
          println(s"[DEBUG] Snapshot is stale (version ${snapshot.version} < $latestVersion), rebuilding")
          // Clear stale snapshot
          currentSnapshot.set(None)
          reconstructStateStandard(versions)

        case None =>
          // Always use standard reconstruction with consistent versions to avoid caching issues
          println(s"[DEBUG] No snapshot available, using standard reconstruction with versions: ${versions.sorted.mkString(", ")}")
          reconstructStateStandard(versions)
      }
    })

    println(s"[DEBUG] Returning ${cached.size} files from listFilesOptimized")
    cached
  }

  /**
   * Get metadata with enhanced caching
   */
  def getMetadata(): MetadataAction = {
    enhancedCache.getOrComputeMetadata(tablePath.toString, {
      logger.info("Computing metadata from transaction log")

      // Look for metadata in reverse chronological order
      val latestVersion = getLatestVersion()
      for (version <- latestVersion to 0L by -1) {
        val actions = readVersionOptimized(version)
        actions.collectFirst {
          case metadata: MetadataAction => metadata
        } match {
          case Some(metadata) => return metadata
          case None => // Continue searching
        }
      }

      throw new RuntimeException("No metadata found in transaction log")
    })
  }

  /**
   * Get total row count using optimized operations
   */
  def getTotalRowCount(): Long = {
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
  }

  /**
   * Get partition columns
   */
  def getPartitionColumns(): Seq[String] = {
    Try(getMetadata().partitionColumns).getOrElse(Seq.empty)
  }

  /**
   * Check if table is partitioned
   */
  def isPartitioned(): Boolean = {
    getPartitionColumns().nonEmpty
  }

  // Private helper methods

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    val latest = versions.sorted.lastOption.getOrElse(-1L)
    println(s"[DEBUG] getLatestVersion: found versions ${versions.sorted.mkString(", ")}, latest = $latest")

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
    println(s"[DEBUG] getVersions: cloudProvider returned ${files.size} files: ${files.map(_.path.split("/").last).mkString(", ")}")
    val versions = files.flatMap { file =>
      parseVersionFromPath(file.path)
    }.distinct
    println(s"[DEBUG] getVersions: parsed versions: ${versions.sorted.mkString(", ")}")
    versions
  }

  private def parseVersionFromPath(path: String): Option[Long] = {
    Try {
      val fileName = new Path(path).getName
      if (fileName.endsWith(".json") && !fileName.contains(".checkpoint.")) {
        val versionStr = fileName.replace(".json", "").replaceAll("_.*", "")
        versionStr.toLong
      } else {
        -1L
      }
    }.toOption.filter(_ >= 0)
  }

  private def readVersionOptimized(version: Long): Seq[Action] = {
    // Use cache with proper invalidation - version-specific caching should be safe
    enhancedCache.getOrComputeVersionActions(tablePath.toString, version, {
      readVersionDirect(version)
    })
  }

  private def readVersionDirect(version: Long): Seq[Action] = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    if (!cloudProvider.exists(versionFilePath)) {
      println(s"[DEBUG] Version file $versionFilePath does not exist")
      return Seq.empty
    }

    Try {
      val content = new String(cloudProvider.readFile(versionFilePath), "UTF-8")
      println(s"[DEBUG] Read version $version content: '${content.take(100)}...' (${content.length} chars)")
      val actions = parseActionsFromContent(content)
      println(s"[DEBUG] Parsed ${actions.size} actions from version $version")
      actions
    }.getOrElse {
      println(s"[DEBUG] Failed to read/parse version $version")
      Seq.empty
    }
  }

  private def parseActionsFromContent(content: String): Seq[Action] = {
    content.split("\n").filter(_.nonEmpty).flatMap { line =>
      Try {
        val jsonNode = JsonUtil.mapper.readTree(line)
        parseAction(jsonNode)
      }.toOption
    }.flatten.toSeq
  }

  private def parseAction(jsonNode: com.fasterxml.jackson.databind.JsonNode): Option[Action] = {
    if (jsonNode.has("metaData")) {
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
  }

  private def writeAction(version: Long, action: Action): Unit = {
    writeActions(version, Seq(action))
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    val content = new StringBuilder()
    actions.foreach { action =>
      val wrappedAction = action match {
        case metadata: MetadataAction => Map("metaData" -> metadata)
        case add: AddAction => Map("add" -> add)
        case remove: RemoveAction => Map("remove" -> remove)
        case skip: SkipAction => Map("mergeskip" -> skip)
      }
      content.append(JsonUtil.mapper.writeValueAsString(wrappedAction)).append("\n")
    }

    println(s"[DEBUG] Writing version $version to $versionFilePath with ${actions.size} actions")
    cloudProvider.writeFile(versionFilePath, content.toString.getBytes("UTF-8"))
    println(s"[DEBUG] Successfully wrote version $version")
    // Add a small delay to ensure file system consistency for local file systems
    // This helps with race conditions where listFiles() is called immediately after writeFile()
    Thread.sleep(10) // 10ms should be sufficient for local file system consistency

    // Write-through cache management: proactively update caches with new data
    enhancedCache.putVersionActions(tablePath.toString, version, actions)
    println(s"[DEBUG] Cached version actions for ${tablePath.toString} version $version")

    // Update file list cache if we have AddActions
    val addActions = actions.collect { case add: AddAction => add }
    if (addActions.nonEmpty) {
      // Compute new file list state by getting current state and applying changes
      val currentFiles = try {
        listFiles()
      } catch {
        case _: Exception => Seq.empty[AddAction]
      }

      // Create a checksum for the current file list state
      val fileListChecksum = currentFiles.map(_.path).sorted.mkString(",").hashCode.toString
      enhancedCache.putFileList(tablePath.toString, fileListChecksum, currentFiles)
      println(s"[DEBUG] Updated file list cache for ${tablePath.toString} with checksum $fileListChecksum")
    }

    // Update metadata cache if we have MetadataAction
    actions.find(_.isInstanceOf[MetadataAction]).foreach { metadata =>
      enhancedCache.putMetadata(tablePath.toString, metadata.asInstanceOf[MetadataAction])
      println(s"[DEBUG] Cached metadata for ${tablePath.toString}")
    }

    // Create checkpoint if needed
    checkpoint.foreach { cp =>
      if (cp.shouldCreateCheckpoint(version)) {
        advancedOps.scheduleAsyncUpdate {
          createCheckpointOptimized(version)
        }
      }
    }
  }

  private def createCheckpointOptimized(version: Long): Unit = {
    try {
      val allActions = getAllCurrentActions(version)

      if (allActions.size > 50000) {
        // Use streaming checkpoint for large tables
        val iterator = allActions.iterator
        val info = memoryOps.createStreamingCheckpoint(version, iterator)
        logger.info(s"Created streaming checkpoint at version $version with ${info.size} actions")
      } else {
        // Use parallel checkpoint creation
        val future = parallelOps.createCheckpointParallel(version, allActions)
        Await.result(future, 60.seconds)
      }

      // Clean up old versions
      checkpoint.foreach(_.cleanupOldVersions(version))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create checkpoint at version $version", e)
    }
  }

  private def getAllCurrentActions(upToVersion: Long): Seq[Action] = {
    val versions = getVersions().filter(_ <= upToVersion)

    if (parallelReadEnabled && versions.size > 10) {
      // Use parallel reconstruction for many versions
      parallelOps.reconstructStateParallel(versions)
    } else {
      // Standard reconstruction with consistent versions
      reconstructStateStandard(versions)
    }
  }

  private def reconstructStateStandard(versions: Seq[Long]): Seq[AddAction] = {
    val files = scala.collection.mutable.HashMap[String, AddAction]()

    println(s"[DEBUG] Reconstructing state from versions: ${versions.sorted}")
    logger.info(s"Reconstructing state from versions: ${versions.sorted}")

    for (version <- versions.sorted) {
      val actions = readVersionOptimized(version)
      println(s"[DEBUG] Version $version has ${actions.size} actions: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")
      logger.info(s"Version $version has ${actions.size} actions: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")
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

    println(s"[DEBUG] Final state has ${files.size} files: ${files.keys.mkString(", ")}")
    logger.info(s"Final state has ${files.size} files: ${files.keys.mkString(", ")}")
    files.values.toSeq
  }

  // Backward compatibility method
  private def reconstructStateStandard(): Seq[AddAction] = {
    reconstructStateStandard(getVersions())
  }

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
    val files = reconstructStateStandard(versions)
    val metadata = getMetadata()
    val snapshot = Snapshot(version, files, metadata)
    currentSnapshot.set(Some(snapshot))
    println(s"[DEBUG] Updated snapshot to version $version with ${files.size} files")
  }

  private def computeCurrentChecksum(): String = {
    // Simple checksum based on latest version
    s"${tablePath.toString}-${getLatestVersion()}"
  }

  private def computeCurrentChecksumWithVersions(versions: Seq[Long]): String = {
    // Include sorted version list in key, not just latest - this ensures cache invalidation
    // when new versions are written
    val versionHash = versions.sorted.mkString(",").hashCode
    s"${tablePath.toString}-${versionHash}"
  }

  /**
   * Get cache statistics for monitoring and debugging.
   * Returns the enhanced cache statistics from the multi-level cache system.
   */
  def getCacheStatistics(): CacheStatistics = {
    enhancedCache.getStatistics()
  }

  /**
   * Invalidate all cached data for this table.
   * Useful for testing or after external modifications.
   */
  def invalidateCache(tablePath: String): Unit = {
    enhancedCache.invalidateTable(tablePath)
  }
}