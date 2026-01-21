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

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.util.concurrent.{Executors, ThreadPoolExecutor}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.avro.{AvroManifestReader, PartitionPruner, StateConfig, StateManifestIO}
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}

import org.apache.spark.sql.sources.Filter
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

case class CheckpointInfo(
  version: Long,
  size: Long, // Number of actions in checkpoint
  sizeInBytes: Long,
  numFiles: Long, // Number of AddActions
  createdTime: Long,
  parts: Option[Int] = None,          // Number of parts for multi-part checkpoint (None = single file)
  checkpointId: Option[String] = None // Unique ID for multi-part checkpoint
)

case class LastCheckpointInfo(
  version: Long,
  size: Long,
  sizeInBytes: Long,
  numFiles: Long,
  createdTime: Long,
  parts: Option[Int] = None,          // Number of parts for multi-part checkpoint (None = single file)
  checkpointId: Option[String] = None, // Unique ID for multi-part checkpoint
  format: Option[String] = None,      // Checkpoint format: "json", "json-multipart", "avro-state"
  stateDir: Option[String] = None     // State directory name for avro-state format (e.g., "state-v00000000000000000100")
)

/**
 * Manifest for multi-part checkpoints.
 *
 * This file is written last (with ifNotExists) to "commit" the checkpoint. It contains the list of part files that make
 * up the complete checkpoint.
 *
 * The format field allows for future extensibility to support different checkpoint file formats (parquet, avro, etc.)
 * without breaking backward compatibility.
 *
 * PURGE operations should:
 *   1. Keep manifest files for retained checkpoints 2. Keep all part files referenced by retained manifests 3. Delete
 *      orphaned part files (parts from failed checkpoint attempts not referenced by any manifest)
 */
case class MultiPartCheckpointManifest(
  version: Long,
  checkpointId: String,
  parts: Seq[String], // List of part filenames
  createdTime: Long,
  format: String = "json" // File format: "json", "parquet", "avro" (future)
)

class TransactionLogCheckpoint(
  transactionLogPath: Path,
  cloudProvider: CloudStorageProvider,
  options: org.apache.spark.sql.util.CaseInsensitiveStringMap) {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCheckpoint])

  // Configuration matching Delta Lake capabilities but using our own parameters
  private val checkpointInterval = options.getInt("spark.indextables.checkpoint.interval", 10)
  private val maxRetries         = options.getInt("spark.indextables.checkpoint.maxRetries", 3)
  private val parallelism        = options.getInt("spark.indextables.checkpoint.parallelism", 4)

  // Retention configuration - matching Delta's log retention behavior
  private val logRetentionDuration =
    options.getLong("spark.indextables.logRetention.duration", 30 * 24 * 60 * 60 * 1000L) // 30 days in ms
  private val checkpointRetentionDuration =
    options.getLong("spark.indextables.checkpointRetention.duration", 2 * 60 * 60 * 1000L) // 2 hours in ms

  // Performance configuration
  private val checkpointReadTimeoutSeconds = options.getInt("spark.indextables.checkpoint.read.timeoutSeconds", 30)

  // Compression configuration
  private val compressionEnabled = options.getBoolean("spark.indextables.checkpoint.compression.enabled", true)
  private val compressionCodec = Option(options.get("spark.indextables.transaction.compression.codec")).getOrElse("gzip")
  private val compressionLevel = options.getInt("spark.indextables.transaction.compression.gzip.level", 6)

  private val codec: Option[CompressionCodec] =
    try
      if (compressionEnabled) CompressionUtils.getCodec(true, compressionCodec, compressionLevel) else None
    catch {
      case e: IllegalArgumentException =>
        logger.warn(
          s"Invalid compression configuration for checkpoint: ${e.getMessage}. Falling back to no compression."
        )
        None
    }

  // Multi-part checkpoint configuration
  private val multiPartEnabled = options.getBoolean("spark.indextables.checkpoint.multiPart.enabled", true)
  private val actionsPerPart   = options.getInt("spark.indextables.checkpoint.actionsPerPart", 100000)

  private val executor                      = Executors.newFixedThreadPool(parallelism).asInstanceOf[ThreadPoolExecutor]
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  val LAST_CHECKPOINT = new Path(transactionLogPath, "_last_checkpoint")

  def close(): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
  }

  def shouldCreateCheckpoint(currentVersion: Long): Boolean =
    getLastCheckpointVersion() match {
      case Some(lastCheckpointVersion) =>
        (currentVersion - lastCheckpointVersion) >= checkpointInterval
      case None =>
        currentVersion >= checkpointInterval
    }

  def createCheckpoint(
    currentVersion: Long,
    allActions: Seq[Action]
  ): Unit = {
    logger.info(s"Creating checkpoint for version $currentVersion with ${allActions.length} actions")

    try {
      // Apply schema deduplication to reduce checkpoint size
      // This replaces docMappingJson with docMappingRef in AddActions
      val deduplicatedActions = applySchemaDeduplication(allActions)

      // Determine if we should use multi-part checkpoint
      val shouldUseMultiPart = multiPartEnabled && deduplicatedActions.length > actionsPerPart

      // Always upgrade to V3 protocol for new checkpoints
      // This ensures consistent behavior and enables V3 features (schema deduplication, multi-part)
      val finalActions = upgradeProtocolForV3Features(deduplicatedActions)

      val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")
      val numFiles        = allActions.count(_.isInstanceOf[AddAction])
      val estimatedSize   = StreamingActionWriter.estimateSerializedSize(allActions)

      val checkpointInfoOpt = if (shouldUseMultiPart) {
        createMultiPartCheckpoint(currentVersion, finalActions, compressionInfo)
      } else {
        Some(createSinglePartCheckpoint(currentVersion, finalActions, compressionInfo))
      }

      checkpointInfoOpt match {
        case Some(checkpointInfo) =>
          writeLastCheckpointFile(checkpointInfo.copy(numFiles = numFiles, sizeInBytes = estimatedSize))
          val partInfo = checkpointInfo.parts.map(p => s" ($p parts)").getOrElse("")
          logger.info(
            s"Successfully created checkpoint at version $currentVersion with ${allActions.length} actions$partInfo$compressionInfo"
          )
        case None =>
          // Another writer completed the checkpoint first (for multi-part only)
          logger.info(s"Checkpoint for version $currentVersion was created by another writer")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create checkpoint for version $currentVersion", e)
        throw e
    }
  }

  /** Create a single-file checkpoint (traditional format). */
  private def createSinglePartCheckpoint(
    currentVersion: Long,
    actions: Seq[Action],
    compressionInfo: String
  ): CheckpointInfo = {
    val checkpointPath    = new Path(transactionLogPath, f"$currentVersion%020d.checkpoint.json")
    val checkpointPathStr = checkpointPath.toString

    logger.debug(s"Writing single-part checkpoint using streaming approach$compressionInfo")

    StreamingActionWriter.writeActionsStreaming(
      actions = actions,
      cloudProvider = cloudProvider,
      path = checkpointPathStr,
      codec = codec,
      ifNotExists = false
    )

    CheckpointInfo(
      version = currentVersion,
      size = actions.length,
      sizeInBytes = 0L, // Will be updated by caller
      numFiles = 0L,    // Will be updated by caller
      createdTime = System.currentTimeMillis(),
      parts = None
    )
  }

  /**
   * Create a multi-part checkpoint for large tables.
   *
   * Architecture for race condition safety:
   *   1. Generate a unique checkpoint ID (UUID) for this attempt 2. Write all parts with UUID in filename:
   *      `<version>.checkpoint.<uuid>.<partNum>.json` 3. Write manifest file using ifNotExists:
   *      `<version>.checkpoint.json` 4. First writer to successfully write manifest wins 5. Orphaned parts from failed
   *      attempts are cleaned up by PURGE operations
   *
   * Part 1 contains: ProtocolAction + MetadataAction + first batch of AddActions Parts 2+ contain: AddActions only
   *
   * @return
   *   Some(CheckpointInfo) if checkpoint was created, None if another writer completed first
   */
  private def createMultiPartCheckpoint(
    currentVersion: Long,
    actions: Seq[Action],
    compressionInfo: String
  ): Option[CheckpointInfo] = {
    // Generate unique ID for this checkpoint attempt
    val checkpointId = java.util.UUID.randomUUID().toString.replace("-", "").take(12)

    // Separate protocol/metadata from add actions
    val protocolAction = actions.collectFirst { case p: ProtocolAction => p }
    val metadataAction = actions.collectFirst { case m: MetadataAction => m }
    val addActions     = actions.collect { case a: AddAction => a }
    val otherActions = actions.filter {
      case _: ProtocolAction => false
      case _: MetadataAction => false
      case _: AddAction      => false
      case _                 => true
    }

    // Calculate number of parts needed
    val headerActions    = protocolAction.toSeq ++ metadataAction.toSeq ++ otherActions
    val addsForFirstPart = math.max(0, actionsPerPart - headerActions.length)
    val remainingAdds    = if (addsForFirstPart >= addActions.length) Seq.empty else addActions.drop(addsForFirstPart)
    val additionalParts =
      if (remainingAdds.isEmpty) 0 else math.ceil(remainingAdds.length.toDouble / actionsPerPart).toInt
    val totalParts = 1 + additionalParts

    logger.info(
      s"Creating multi-part checkpoint (id=$checkpointId) with $totalParts parts for ${actions.length} actions"
    )

    // Write all parts with checkpoint ID in filename
    val partFiles = scala.collection.mutable.ListBuffer[String]()

    // Write part 1: header + first batch of adds
    val part1Actions = headerActions ++ addActions.take(addsForFirstPart)
    val part1File    = writeCheckpointPartWithId(currentVersion, checkpointId, 1, part1Actions)
    partFiles += part1File

    // Write remaining parts
    var partNum      = 2
    var remainingIdx = 0
    while (remainingIdx < remainingAdds.length) {
      val partActions = remainingAdds.slice(remainingIdx, remainingIdx + actionsPerPart)
      val partFile    = writeCheckpointPartWithId(currentVersion, checkpointId, partNum, partActions)
      partFiles += partFile
      remainingIdx += actionsPerPart
      partNum += 1
    }

    // Write manifest file with ifNotExists - first writer wins
    val manifestWritten = writeCheckpointManifest(currentVersion, checkpointId, partFiles.toSeq)

    if (manifestWritten) {
      logger.info(s"Successfully created multi-part checkpoint manifest for version $currentVersion (id=$checkpointId)")
      Some(
        CheckpointInfo(
          version = currentVersion,
          size = actions.length,
          sizeInBytes = 0L,
          numFiles = 0L,
          createdTime = System.currentTimeMillis(),
          parts = Some(totalParts),
          checkpointId = Some(checkpointId)
        )
      )
    } else {
      // Another writer completed the checkpoint first - our parts become orphans (cleaned up by PURGE)
      logger.info(
        s"Another writer completed checkpoint for version $currentVersion first, " +
          s"orphaned parts (id=$checkpointId) will be cleaned up by PURGE"
      )
      None
    }
  }

  /**
   * Write a single part of a multi-part checkpoint with unique ID.
   *
   * @return
   *   The filename of the written part
   */
  private def writeCheckpointPartWithId(
    version: Long,
    checkpointId: String,
    partNum: Int,
    actions: Seq[Action]
  ): String = {
    val fileName    = f"$version%020d.checkpoint.$checkpointId.$partNum%05d.json"
    val partPath    = new Path(transactionLogPath, fileName)
    val partPathStr = partPath.toString

    logger.debug(s"Writing checkpoint part $partNum (id=$checkpointId) with ${actions.length} actions to $partPathStr")

    StreamingActionWriter.writeActionsStreaming(
      actions = actions,
      cloudProvider = cloudProvider,
      path = partPathStr,
      codec = codec,
      ifNotExists = false // Parts are unique per checkpoint ID, no conflict possible
    )

    fileName
  }

  /**
   * Write the checkpoint manifest file.
   *
   * The manifest contains metadata about the checkpoint and lists all part files. Uses ifNotExists to ensure only one
   * writer succeeds for a given version.
   *
   * @return
   *   true if manifest was written, false if another writer completed first
   */
  private def writeCheckpointManifest(
    version: Long,
    checkpointId: String,
    partFiles: Seq[String]
  ): Boolean = {
    val manifestPath    = new Path(transactionLogPath, f"$version%020d.checkpoint.json")
    val manifestPathStr = manifestPath.toString

    val manifest = MultiPartCheckpointManifest(
      version = version,
      checkpointId = checkpointId,
      parts = partFiles,
      createdTime = System.currentTimeMillis()
    )

    val manifestJson = JsonUtil.mapper.writeValueAsString(manifest)

    // Use ifNotExists - first writer to complete wins
    cloudProvider.writeFileIfNotExists(manifestPathStr, manifestJson.getBytes("UTF-8"))
  }

  def getActionsFromCheckpoint(): Option[Seq[Action]] =
    getActionsFromCheckpointWithFilters(Seq.empty)

  /**
   * Get actions from checkpoint with optional partition filters for Avro state format.
   *
   * When partition filters are provided and the checkpoint is in Avro state format, the reader can
   * prune entire manifest files that don't contain matching partitions, significantly reducing I/O
   * for large tables with many partitions.
   *
   * @param partitionFilters
   *   Partition filters to apply for manifest pruning (only used with Avro state format)
   * @return
   *   Actions from the checkpoint, or None if no checkpoint exists
   */
  def getActionsFromCheckpointWithFilters(partitionFilters: Seq[Filter]): Option[Seq[Action]] =
    getLastCheckpointInfo().flatMap { info =>
      Try {
        val actions = info.format match {
          case Some(StateConfig.Format.AVRO_STATE) =>
            // Avro state format: read from state directory with partition pruning
            readAvroStateCheckpoint(info, partitionFilters)
          case _ =>
            // JSON formats (legacy and multi-part) - emit deprecation warning
            if (StateConfig.Format.isJsonFormat(info.format)) {
              logger.warn(StateConfig.JSON_FORMAT_DEPRECATION_WARNING)
            }
            info.checkpointId match {
              case Some(_) =>
                // New format: manifest-based multi-part checkpoint
                readMultiPartCheckpointFromManifest(info.version)
              case None =>
                // Legacy format: single-file checkpoint (actions directly in file)
                readLegacySingleFileCheckpoint(info.version)
            }
        }

        // Validate protocol version before proceeding
        validateProtocolVersion(actions)

        // Restore schemas from registry (handles both legacy and deduplicated checkpoints)
        restoreSchemas(actions)
      } match {
        case Success(actions)                     => Some(actions)
        case Failure(e: ProtocolVersionException) =>
          // Re-throw protocol version exceptions to the caller
          throw e
        case Failure(e) =>
          logger.error("Failed to read checkpoint file", e)
          None
      }
    }

  /**
   * Read checkpoint data from Avro state format.
   *
   * The Avro state format stores file entries in binary Avro manifests, providing 10x faster reads than JSON and enabling
   * partition pruning for gigantic tables.
   */
  private def readAvroStateCheckpoint(info: LastCheckpointInfo): Seq[Action] = {
    readAvroStateCheckpoint(info, partitionFilters = Seq.empty)
  }

  /**
   * Read checkpoint data from Avro state format with partition filtering.
   *
   * When partition filters are provided, the pruner can skip entire manifests that are known
   * not to contain matching files based on partition bounds, providing significant speedup
   * for partition-filtered queries on large tables.
   *
   * @param info
   *   Checkpoint info with stateDir
   * @param partitionFilters
   *   Optional partition filters for manifest pruning
   * @return
   *   Actions from the checkpoint
   */
  private def readAvroStateCheckpoint(info: LastCheckpointInfo, partitionFilters: Seq[Filter]): Seq[Action] = {
    val stateDir = info.stateDir.getOrElse(
      throw new IllegalStateException(s"Avro state checkpoint is missing stateDir: version=${info.version}")
    )

    val stateDirPath = new Path(transactionLogPath, stateDir).toString
    logger.debug(s"Reading Avro state checkpoint: version=${info.version}, stateDir=$stateDir")

    val manifestIO = StateManifestIO(cloudProvider)
    val manifestReader = AvroManifestReader(cloudProvider)

    // Read the state manifest (_manifest.json)
    val stateManifest = manifestIO.readStateManifest(stateDirPath)

    // Apply partition pruning if filters are provided
    val manifestsToRead = if (partitionFilters.nonEmpty) {
      val prunedManifests = PartitionPruner.pruneManifests(stateManifest.manifests, partitionFilters)
      logger.info(
        s"Partition pruning: reading ${prunedManifests.size} of ${stateManifest.manifests.size} manifests")
      prunedManifests
    } else {
      stateManifest.manifests
    }

    // Read selected Avro manifests in parallel
    val manifestPaths = manifestsToRead.map(m => s"$stateDirPath/${m.path}")
    val fileEntries = manifestReader.readManifestsParallel(manifestPaths)

    // Apply tombstones to filter out removed files
    val liveEntries = manifestIO.applyTombstones(fileEntries, stateManifest.tombstones)

    // Convert FileEntries to AddActions with schema registry for docMappingJson restoration
    val addActions = manifestReader.toAddActions(liveEntries, stateManifest.schemaRegistry)

    // Build the full action list (Protocol + Metadata + AddActions)
    val actions = scala.collection.mutable.ListBuffer[Action]()

    // Add Protocol action for compatibility
    actions += ProtocolAction(
      minReaderVersion = stateManifest.protocolVersion,
      minWriterVersion = stateManifest.protocolVersion,
      readerFeatures = Some(Set("avroState")),
      writerFeatures = Some(Set("avroState"))
    )

    // Log schema registry usage
    if (stateManifest.schemaRegistry.nonEmpty) {
      logger.debug(s"Restored docMappingJson from ${stateManifest.schemaRegistry.size} schema registry entries")
    }

    actions ++= addActions

    logger.info(
      s"Read Avro state checkpoint: version=${info.version}, " +
        s"files=${liveEntries.size}, tombstones=${stateManifest.tombstones.size}")

    actions.toSeq
  }

  /**
   * Read a legacy single-file checkpoint (actions directly in file).
   *
   * Uses full streaming from cloud storage through decompression to parsing. This avoids OOM for large checkpoint files
   * (>1GB) by never loading the entire file into memory.
   */
  private def readLegacySingleFileCheckpoint(version: Long): Seq[Action] = {
    val checkpointPath    = new Path(transactionLogPath, f"$version%020d.checkpoint.json")
    val checkpointPathStr = checkpointPath.toString

    if (!cloudProvider.exists(checkpointPathStr)) {
      throw new java.io.FileNotFoundException(s"Checkpoint file does not exist: $checkpointPathStr")
    }

    // Use full streaming: cloud storage -> decompression -> line parsing
    // Never loads entire file into memory
    parseActionsFromStream(checkpointPathStr)
  }

  /**
   * Read a multi-part checkpoint using the manifest file.
   *
   * The manifest file (`<version>.checkpoint.json`) contains the list of part files. Each part file is named
   * `<version>.checkpoint.<checkpointId>.<partNum>.json`.
   */
  private def readMultiPartCheckpointFromManifest(version: Long): Seq[Action] = {
    val manifestPath    = new Path(transactionLogPath, f"$version%020d.checkpoint.json")
    val manifestPathStr = manifestPath.toString

    if (!cloudProvider.exists(manifestPathStr)) {
      throw new java.io.FileNotFoundException(s"Checkpoint manifest does not exist: $manifestPathStr")
    }

    // Read and parse manifest
    val manifestBytes   = cloudProvider.readFile(manifestPathStr)
    val manifestContent = new String(manifestBytes, "UTF-8")
    val manifest        = JsonUtil.mapper.readValue(manifestContent, classOf[MultiPartCheckpointManifest])

    logger.debug(
      s"Reading multi-part checkpoint version $version (id=${manifest.checkpointId}) with ${manifest.parts.size} parts"
    )

    // Read all parts listed in manifest
    val allActions = scala.collection.mutable.ListBuffer[Action]()

    for (partFile <- manifest.parts) {
      val partPath    = new Path(transactionLogPath, partFile)
      val partPathStr = partPath.toString

      if (!cloudProvider.exists(partPathStr)) {
        throw new java.io.FileNotFoundException(
          s"Checkpoint part does not exist: $partPathStr (referenced in manifest)"
        )
      }

      // Use full streaming: cloud storage -> decompression -> line parsing
      val partActions = parseActionsFromStream(partPathStr)

      logger.debug(s"Read ${partActions.length} actions from checkpoint part $partFile")
      allActions ++= partActions
    }

    logger.debug(s"Total actions from multi-part checkpoint: ${allActions.length}")
    allActions.toSeq
  }

  def readVersionsInParallel(versions: Seq[Long]): Map[Long, Seq[Action]] = {
    logger.debug(s"Reading ${versions.length} versions in parallel with parallelism=$parallelism")

    val futures = versions.map { version =>
      Future {
        version -> readSingleVersion(version)
      }
    }

    val results = Future.sequence(futures)
    val timeout = Duration(checkpointReadTimeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)

    try {
      val completedResults = scala.concurrent.Await.result(results, timeout)
      completedResults.toMap
    } catch {
      case e: Exception =>
        logger.error("Failed to read versions in parallel", e)
        throw e
    }
  }

  private def readSingleVersion(version: Long): Seq[Action] = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    if (!cloudProvider.exists(versionFilePath)) {
      return Seq.empty
    }

    var retries = 0
    while (retries <= maxRetries)
      try
        // Use full streaming: cloud storage -> decompression -> line parsing
        return parseActionsFromStream(versionFilePath)
      catch {
        case e: Exception =>
          retries += 1
          if (retries > maxRetries) {
            logger.error(s"Failed to read version $version after $maxRetries retries", e)
            throw e
          } else {
            logger.warn(s"Retry $retries for version $version due to: ${e.getMessage}")
            Thread.sleep(100 * retries) // Exponential backoff
          }
      }

    Seq.empty // Should never reach here
  }

  /**
   * Parse actions directly from a cloud storage file using full streaming.
   *
   * This method provides the most memory-efficient parsing by streaming data from cloud storage directly through
   * decompression and into line-by-line parsing, without ever loading the entire file into memory.
   *
   * Flow: CloudStorage InputStream -> Decompressing InputStream -> BufferedReader -> Line parsing
   */
  private def parseActionsFromStream(filePath: String): Seq[Action] = {
    val rawStream           = cloudProvider.openInputStream(filePath)
    val decompressingStream = CompressionUtils.createDecompressingInputStream(rawStream)
    val reader              = new BufferedReader(new InputStreamReader(decompressingStream, "UTF-8"))
    val actions             = ListBuffer[Action]()

    try {
      var line = reader.readLine()
      while (line != null) {
        if (line.nonEmpty) {
          actions += parseActionLine(line)
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally
      reader.close()
    // Closing reader closes the underlying streams
  }

  /**
   * Parse actions from a byte array using streaming line-by-line parsing.
   *
   * This method avoids OOM for large files (>1GB) by reading line-by-line instead of loading the entire content as a
   * single String. Java Strings have a maximum size limit of ~1GB (UTF-16 encoding), so files larger than that would
   * fail with "UTF16 String size is X, should be less than Y" error.
   *
   * Each line is small (typically a few KB), so parsing line-by-line allows GC to reclaim memory between lines.
   */
  private def parseActionsFromBytes(bytes: Array[Byte]): Seq[Action] = {
    val reader  = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes), "UTF-8"))
    val actions = ListBuffer[Action]()

    try {
      var line = reader.readLine()
      while (line != null) {
        if (line.nonEmpty) {
          actions += parseActionLine(line)
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally
      reader.close()
  }

  /**
   * Parse a single action line.
   *
   * Uses treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes.
   */
  private def parseActionLine(line: String): Action = {
    val jsonNode = JsonUtil.mapper.readTree(line)

    if (jsonNode.has("protocol")) {
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
  }

  private def parseActionsFromContent(content: String): Seq[Action] = {
    val lines = content.split("\n").filter(_.nonEmpty)

    lines.map { line =>
      val jsonNode = JsonUtil.mapper.readTree(line)

      // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes
      if (jsonNode.has("protocol")) {
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
    }.toSeq
  }

  private def writeLastCheckpointFile(checkpointInfo: CheckpointInfo): Unit = {
    val lastCheckpointInfo = LastCheckpointInfo(
      version = checkpointInfo.version,
      size = checkpointInfo.size,
      sizeInBytes = checkpointInfo.sizeInBytes,
      numFiles = checkpointInfo.numFiles,
      createdTime = checkpointInfo.createdTime,
      parts = checkpointInfo.parts,
      checkpointId = checkpointInfo.checkpointId
    )

    val json = JsonUtil.mapper.writeValueAsString(lastCheckpointInfo)
    cloudProvider.writeFile(LAST_CHECKPOINT.toString, json.getBytes("UTF-8"))
  }

  def getLastCheckpointInfo(): Option[LastCheckpointInfo] = {
    if (!cloudProvider.exists(LAST_CHECKPOINT.toString)) {
      return None
    }

    Try {
      val content = new String(cloudProvider.readFile(LAST_CHECKPOINT.toString), "UTF-8")
      JsonUtil.mapper.readValue(content, classOf[LastCheckpointInfo])
    } match {
      case Success(info) => Some(info)
      case Failure(e) =>
        logger.warn("Failed to read last checkpoint info", e)
        None
    }
  }

  def getLastCheckpointVersion(): Option[Long] =
    getLastCheckpointInfo().map(_.version)

  def cleanupOldVersions(currentVersion: Long): Unit =
    getLastCheckpointVersion() match {
      case Some(checkpointVersion) if checkpointVersion <= currentVersion =>
        val currentTime = System.currentTimeMillis()

        // Clean up old log files based on retention policy
        val versionsToCheck = (0L until currentVersion).filter(_ < checkpointVersion)
        var deletedCount    = 0

        versionsToCheck.foreach { version =>
          val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
          val versionFilePath = versionFile.toString

          if (cloudProvider.exists(versionFilePath)) {
            try {
              val fileInfo = cloudProvider
                .listFiles(transactionLogPath.toString, recursive = false)
                .find(f => new Path(f.path).getName == f"$version%020d.json")

              fileInfo match {
                case Some(info) =>
                  val fileAge = currentTime - info.modificationTime
                  if (fileAge > logRetentionDuration && version < checkpointVersion) {
                    cloudProvider.deleteFile(versionFilePath)
                    deletedCount += 1
                    logger.debug(s"Deleted old version file: $versionFilePath (age: ${fileAge / 1000}s)")
                  }
                case None =>
                  logger.debug(s"Could not get file info for $versionFilePath")
              }
            } catch {
              case e: Exception =>
                logger.warn(s"Failed to process version file $versionFilePath", e)
            }
          }
        }

        if (deletedCount > 0) {
          logger.info(s"Cleaned up $deletedCount old transaction log files (retention: ${logRetentionDuration / 1000}s)")
        }

        // Also clean up old checkpoints based on checkpoint retention
        cleanupOldCheckpoints(currentTime)

      case _ =>
        logger.debug("No cleanup needed - no checkpoint available or checkpoint is current")
    }

  private def cleanupOldCheckpoints(currentTime: Long): Unit =
    try {
      val files           = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
      val checkpointFiles = files.filter(_.path.contains(".checkpoint.json"))

      var deletedCheckpoints = 0
      checkpointFiles.foreach { file =>
        val fileAge = currentTime - file.modificationTime
        if (fileAge > checkpointRetentionDuration) {
          // Keep at least the most recent checkpoint
          val fileName = new Path(file.path).getName
          if (!getLastCheckpointInfo().exists(info => fileName.startsWith(f"${info.version}%020d"))) {
            try {
              cloudProvider.deleteFile(file.path)
              deletedCheckpoints += 1
              logger.debug(s"Deleted old checkpoint file: ${file.path} (age: ${fileAge / 1000}s)")
            } catch {
              case e: Exception =>
                logger.warn(s"Failed to delete old checkpoint file ${file.path}", e)
            }
          }
        }
      }

      if (deletedCheckpoints > 0) {
        logger.info(
          s"Cleaned up $deletedCheckpoints old checkpoint files (retention: ${checkpointRetentionDuration / 1000}s)"
        )
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed during checkpoint cleanup", e)
    }

  /**
   * Apply schema deduplication to checkpoint actions.
   *
   * This replaces docMappingJson with docMappingRef in AddActions and stores the schema registry in
   * MetadataAction.configuration. This can reduce checkpoint size by up to 99% for tables with large schemas.
   *
   * Additionally, this method consolidates duplicate schema mappings that may have been created by an older version of
   * the hash calculation algorithm (which used raw string hashing instead of canonical JSON hashing).
   */
  private def applySchemaDeduplication(actions: Seq[Action]): Seq[Action] = {
    // Find the MetadataAction (should be present in checkpoint)
    val metadataOpt = actions.collectFirst { case m: MetadataAction => m }

    metadataOpt match {
      case Some(metadata) =>
        // Get existing schema registry from metadata configuration
        val existingRegistry = metadata.configuration

        // Deduplicate schemas in actions
        val (deduplicatedActions, newSchemaRegistry) =
          SchemaDeduplication.deduplicateSchemas(actions, existingRegistry)

        // If new schemas were found, update MetadataAction with merged registry
        val (actionsAfterDedup, configAfterDedup) = if (newSchemaRegistry.nonEmpty) {
          val originalSize     = actions.collect { case a: AddAction => a }.flatMap(_.docMappingJson).map(_.length).sum
          val uniqueSchemaSize = newSchemaRegistry.values.map(_.length).sum
          logger.info(
            s"Schema deduplication: ${newSchemaRegistry.size} unique schemas, " +
              s"original size ~${originalSize / 1024}KB, deduplicated size ~${uniqueSchemaSize / 1024}KB"
          )

          // Merge new schemas into existing configuration
          val updatedConfiguration =
            SchemaDeduplication.mergeIntoConfiguration(metadata.configuration, newSchemaRegistry)

          (deduplicatedActions, updatedConfiguration)
        } else {
          (deduplicatedActions, metadata.configuration)
        }

        // STEP 2: Consolidate duplicate schema mappings from old buggy hash calculation
        // This handles tables that were affected by the old string-based hash calculation
        // which produced different hashes for semantically identical JSON (different property order)
        val (consolidatedActions, consolidatedConfig, duplicatesRemoved) =
          SchemaDeduplication.consolidateDuplicateSchemas(actionsAfterDedup, configAfterDedup)

        if (duplicatesRemoved > 0) {
          logger.info(
            s"Schema consolidation: removed $duplicatesRemoved duplicate schema mappings from legacy hash calculation"
          )
        }

        // Replace MetadataAction with final configuration
        if (configAfterDedup != metadata.configuration || duplicatesRemoved > 0) {
          consolidatedActions.map {
            case _: MetadataAction => metadata.copy(configuration = consolidatedConfig)
            case other             => other
          }
        } else {
          // No changes needed
          actions
        }

      case None =>
        // No MetadataAction found (shouldn't happen in valid checkpoint)
        logger.warn("No MetadataAction found in checkpoint actions, skipping schema deduplication")
        actions
    }
  }

  /**
   * Restore schemas in checkpoint actions from the schema registry.
   *
   * This replaces docMappingRef with docMappingJson in AddActions using the schema registry stored in
   * MetadataAction.configuration.
   *
   * @throws IllegalStateException
   *   if schema restoration fails
   */
  private def restoreSchemas(actions: Seq[Action]): Seq[Action] = {
    // Check if there are any schema references to restore
    val hasSchemaRefs = actions.exists {
      case add: AddAction => add.docMappingRef.isDefined && add.docMappingJson.isEmpty
      case _              => false
    }

    if (!hasSchemaRefs) {
      // No schema refs to restore (legacy checkpoint or no schemas)
      return actions
    }

    // Find the MetadataAction to get schema registry
    val metadataOpt = actions.collectFirst { case m: MetadataAction => m }

    metadataOpt match {
      case Some(metadata) =>
        // Extract schema registry from configuration
        val schemaRegistry = metadata.configuration

        // Check if registry has any schemas
        if (!schemaRegistry.keys.exists(_.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))) {
          throw new IllegalStateException(
            "Checkpoint has AddActions with docMappingRef but no schema registry found in MetadataAction.configuration. " +
              "This indicates a corrupted checkpoint file."
          )
        }

        logger.debug(s"Restoring schemas from registry with ${schemaRegistry.count(_._1.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))} entries")
        val restored = SchemaDeduplication.restoreSchemas(actions, schemaRegistry)

        // Verify all schemas were restored
        val unresolvedRefs = restored.collect {
          case add: AddAction if add.docMappingRef.isDefined && add.docMappingJson.isEmpty => add
        }
        if (unresolvedRefs.nonEmpty) {
          val missingHashes = unresolvedRefs.flatMap(_.docMappingRef).distinct.mkString(", ")
          throw new IllegalStateException(
            s"Failed to restore schemas for ${unresolvedRefs.size} AddActions in checkpoint. " +
              s"Missing schema hashes: $missingHashes"
          )
        }

        restored

      case None =>
        throw new IllegalStateException(
          "Checkpoint has AddActions with docMappingRef but no MetadataAction found. " +
            "This indicates a corrupted checkpoint file."
        )
    }
  }

  /**
   * Validate that the current system can read a checkpoint with the given protocol.
   *
   * @param actions
   *   The checkpoint actions containing ProtocolAction
   * @throws ProtocolVersionException
   *   if the checkpoint requires a newer reader version
   */
  private def validateProtocolVersion(actions: Seq[Action]): Unit = {
    val protocolOpt = actions.collectFirst { case p: ProtocolAction => p }
    protocolOpt.foreach { protocol =>
      ProtocolVersion.validateReaderVersion(protocol)
      logger.debug(s"Checkpoint protocol version validated: reader=${protocol.minReaderVersion}, writer=${protocol.minWriterVersion}")
    }
  }

  /**
   * Upgrade protocol to V3 if V3 features are being used.
   *
   * V3 features include:
   *   - Multi-part checkpoints (multiPartCheckpoint)
   *   - Schema deduplication with docMappingRef (schemaDeduplication)
   *
   * If the existing protocol is already V3+, this method does nothing. If the existing protocol is V2 or below, it
   * upgrades to V3.
   *
   * @param actions
   *   The checkpoint actions containing ProtocolAction
   * @return
   *   Actions with ProtocolAction upgraded to V3 if necessary
   */
  private def upgradeProtocolForV3Features(actions: Seq[Action]): Seq[Action] = {
    val existingProtocol = actions.collectFirst { case p: ProtocolAction => p }

    existingProtocol match {
      case Some(protocol) if protocol.minReaderVersion >= 3 =>
        // Already V3+, no upgrade needed
        actions

      case Some(protocol) =>
        // Upgrade to V3
        val upgradedProtocol = ProtocolAction(
          minReaderVersion = ProtocolVersion.CURRENT_READER_VERSION,
          minWriterVersion = ProtocolVersion.CURRENT_WRITER_VERSION
        )
        logger.info(
          s"Upgrading protocol from V${protocol.minReaderVersion} to V${upgradedProtocol.minReaderVersion} " +
            "due to V3 features (schema deduplication or multi-part checkpoint)"
        )
        actions.map {
          case _: ProtocolAction => upgradedProtocol
          case other             => other
        }

      case None =>
        // No protocol found, add V3 protocol
        logger.info("Adding V3 protocol for checkpoint with V3 features")
        ProtocolVersion.defaultProtocol() +: actions
    }
  }
}
