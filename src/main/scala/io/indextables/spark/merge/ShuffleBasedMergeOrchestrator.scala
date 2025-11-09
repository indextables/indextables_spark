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

package io.indextables.spark.merge

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.security.MessageDigest
import java.util.UUID

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.conf.Configuration

import io.indextables.spark.io.{CloudStorageProvider, CloudStorageProviderFactory}
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.SizeParser
import org.slf4j.LoggerFactory

/**
 * Split data container for RDD shuffle
 *
 * This class holds split metadata AND the split file bytes for shuffling.
 * Spark will automatically spill to disk if memory is tight.
 *
 * @param info Split metadata (lightweight)
 * @param bytes Split file bytes (can be large, will spill to disk)
 */
case class ShuffledSplitData(
  info: StagedSplitInfo,
  bytes: Array[Byte]
) extends Serializable {

  // Efficient size check without deserializing bytes
  def size: Long = bytes.length.toLong

  /**
   * Write bytes to a temporary .split file for merge processing
   */
  def writeToTempFile(tempDir: File): String = {
    val splitFile = new File(tempDir, s"shuffled-${info.uuid}.split")
    val fos = new FileOutputStream(splitFile)
    try {
      fos.write(bytes)
      fos.flush()
      fos.getFD().sync()  // Ensure bytes are actually written to disk
    } finally {
      fos.close()
    }
    splitFile.getAbsolutePath
  }
}

/**
 * Merge group with split data (not just references)
 */
case class ShuffledMergeGroup(
  groupId: String,
  splits: Seq[ShuffledSplitData],
  partition: Map[String, String],
  estimatedSize: Long,
  schemaFingerprint: String
) extends Serializable

/**
 * Companion object with serializable static functions for RDD operations
 */
object ShuffleBasedMergeOrchestrator {
  /**
   * Execute merge for a single group (on executor) - static to avoid closure capture
   *
   * Split bytes are already local via shuffle - no download needed!
   */
  def executeMergeGroup(
    group: ShuffledMergeGroup,
    tablePathBC: String,
    optionsMapBC: Map[String, String]
  ): Option[AddAction] = {
    val logger = LoggerFactory.getLogger(classOf[ShuffleBasedMergeOrchestrator])

    logger.info(s"Executing merge for group ${group.groupId}: ${group.splits.size} splits, " +
      s"${group.estimatedSize / 1024 / 1024}MB")

    // Create temp directory for this merge
    val tempDir = Files.createTempDirectory("shuffle-merge-").toFile

    try {
      // Recreate CaseInsensitiveStringMap from serializable map
      val optionsBC = new CaseInsensitiveStringMap(optionsMapBC.asJava)

      // Write shuffled bytes to temp .split files
      val totalInputRecords = group.splits.map(_.info.numRecords).sum
      logger.debug(s"MERGE GROUP DEBUG: Group ${group.groupId} has ${group.splits.size} splits with $totalInputRecords total records")

      val splitPaths = group.splits.map { splitData =>
        val splitPath = splitData.writeToTempFile(tempDir)
        logger.debug(s"MERGE GROUP DEBUG: Wrote split with ${splitData.info.numRecords} records to: $splitPath (${splitData.size / 1024}KB)")
        splitPath
      }

      // Execute merge using tantivy4java (use empty hadoopConf on executor)
      val mergeEngine = new WorkerLocalSplitMerger(optionsBC, new Configuration())
      val (mergedSplitPath, metadata) = mergeEngine.mergeSplits(
        inputSplits = splitPaths,
        outputDir = tempDir.getAbsolutePath,
        partitionId = 0
      )

      // Upload merged split to S3 (use empty hadoopConf on executor)
      val cloudProvider = CloudStorageProviderFactory.createProvider(tablePathBC, optionsBC, new Configuration())
      try {
        val partitionPath = if (group.partition.nonEmpty) {
          group.partition.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
        } else {
          ""
        }
        val relativePath = s"${partitionPath}merged-${group.groupId}.split"
        val finalPath = s"$tablePathBC/$relativePath"

        val mergedFile = new File(mergedSplitPath)
        cloudProvider.writeFileFromStream(
          finalPath,
          new FileInputStream(mergedSplitPath),
          Some(mergedFile.length())
        )

        logger.info(s"Uploaded merged split: $finalPath (${mergedFile.length() / 1024 / 1024}MB)")

        // Extract metadata
        val inputMinValues = group.splits.map(_.info.minValues)
        val inputMaxValues = group.splits.map(_.info.maxValues)
        val mergedMetadata = mergeEngine.extractMetadataFromMergeResult(
          metadata,
          mergedSplitPath,
          inputMinValues,
          inputMaxValues
        )

        logger.debug(s"MERGE GROUP DEBUG: Merged split has ${mergedMetadata.numRecords} records (input had $totalInputRecords records)")

        // Create AddAction
        val addAction = AddAction(
          path = relativePath,
          size = mergedMetadata.size,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(mergedMetadata.numRecords),
          minValues = Some(mergedMetadata.minValues),
          maxValues = Some(mergedMetadata.maxValues),
          partitionValues = group.partition,
          footerStartOffset = mergedMetadata.footerStartOffset,
          footerEndOffset = mergedMetadata.footerEndOffset,
          hasFooterOffsets = mergedMetadata.footerStartOffset.isDefined && mergedMetadata.footerEndOffset.isDefined,
          timeRangeStart = mergedMetadata.timeRangeStart,
          timeRangeEnd = mergedMetadata.timeRangeEnd,
          splitTags = mergedMetadata.splitTags,
          deleteOpstamp = mergedMetadata.deleteOpstamp,
          docMappingJson = mergedMetadata.docMappingJson,
          uncompressedSizeBytes = mergedMetadata.uncompressedSizeBytes
        )

        Some(addAction)

      } finally {
        cloudProvider.close()
      }

    } catch {
      case e: Exception =>
        logger.error(s"Failed to merge group ${group.groupId}", e)
        None  // Return None instead of failing the job
    } finally {
      // CRITICAL: Cleanup temp directory in finally block to ensure cleanup even on error
      // This prevents temp file accumulation when tasks fail or executors crash
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
        logger.debug(s"Cleaned up temp directory: ${tempDir.getAbsolutePath}")
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to cleanup temp directory: ${tempDir.getAbsolutePath}", e)
      }
    }
  }

  /**
   * Upload a single split directly to final location (no merge needed)
   * Used when split count < minSplitsToMerge threshold
   */
  def uploadSplitDirectly(
    shuffledSplit: ShuffledSplitData,  // Changed to ShuffledSplitData
    tablePathBC: String,
    optionsMapBC: Map[String, String]
  ): Option[AddAction] = {
    val logger = LoggerFactory.getLogger(classOf[ShuffleBasedMergeOrchestrator])
    val splitInfo = shuffledSplit.info  // Extract metadata

    logger.info(s"Uploading split directly from in-memory bytes (< minSplitsToMerge): ${splitInfo.uuid}")

    try {
      // Recreate CaseInsensitiveStringMap from serializable map
      val optionsBC = new CaseInsensitiveStringMap(optionsMapBC.asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(tablePathBC, optionsBC, new Configuration())

      // Generate final split path
      val finalSplitName = s"part-${splitInfo.uuid}.split"
      val relativePathComponents = if (splitInfo.partitionValues.nonEmpty) {
        val partitionPath = splitInfo.partitionValues.toSeq.sortBy(_._1).map {
          case (key, value) => s"$key=$value"
        }.mkString("/")
        s"$partitionPath/$finalSplitName"
      } else {
        finalSplitName
      }

      val cloudPath = if (tablePathBC.startsWith("file:")) {
        s"$tablePathBC/$relativePathComponents"
      } else {
        val normalized = tablePathBC.stripSuffix("/")
        s"$normalized/$relativePathComponents"
      }

      logger.info(s"Uploading direct split to: $cloudPath")

      // IN-MEMORY SHUFFLE: Upload directly from in-memory bytes (no file I/O!)
      val inputStream = new java.io.ByteArrayInputStream(shuffledSplit.bytes)
      try {
        cloudProvider.writeFileFromStream(cloudPath, inputStream, Some(shuffledSplit.bytes.length.toLong))
      } finally {
        inputStream.close()
      }

      val uploadedSize = shuffledSplit.bytes.length.toLong
      logger.info(s"Direct upload complete from memory: $cloudPath (${uploadedSize / 1024}KB)")

      // Create AddAction
      val addAction = AddAction(
        path = relativePathComponents,
        size = uploadedSize,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        numRecords = Some(splitInfo.numRecords),
        minValues = Some(splitInfo.minValues),
        maxValues = Some(splitInfo.maxValues),
        partitionValues = splitInfo.partitionValues,
        footerStartOffset = splitInfo.footerStartOffset,
        footerEndOffset = splitInfo.footerEndOffset,
        hasFooterOffsets = splitInfo.footerStartOffset.isDefined && splitInfo.footerEndOffset.isDefined,
        timeRangeStart = splitInfo.timeRangeStart,
        timeRangeEnd = splitInfo.timeRangeEnd,
        splitTags = splitInfo.splitTags,
        deleteOpstamp = splitInfo.deleteOpstamp,
        numMergeOps = Some(0),  // No merge operations - direct upload
        docMappingJson = splitInfo.docMappingJson,
        uncompressedSizeBytes = splitInfo.uncompressedSizeBytes
      )

      cloudProvider.close()

      Some(addAction)

    } catch {
      case e: Exception =>
        logger.error(s"Failed to upload split directly: ${splitInfo.uuid}", e)
        None
    }
  }
}

/**
 * Shuffle-based merge orchestrator that eliminates local file persistence issues
 *
 * ARCHITECTURE:
 * 1. Write phase creates splits and keeps bytes in RDD (not local files)
 * 2. Shuffle phase uses Spark's shuffle to distribute split bytes to merge executors
 * 3. Merge phase writes shuffled bytes to temp .split files and merges them
 * 4. Cleanup phase deletes temp files (not shared staging directories)
 *
 * ADVANTAGES over file-based approach:
 * - 100% locality: Spark shuffle ensures data is local to merge executor
 * - No file persistence issues: Splits exist in RDD, not fragile local files
 * - Automatic disk spilling: Spark spills large shuffles to disk automatically
 * - Simpler: No local staging directory management
 * - Fault tolerant: Shuffle blocks are tracked by Spark
 *
 * CONFIGURATION for aggressive disk spilling:
 * - spark.shuffle.spill: true (default, enables spilling)
 * - spark.shuffle.spill.compress: true (compress spilled data)
 * - spark.memory.fraction: 0.3 (lower = more aggressive spilling, default 0.6)
 * - spark.memory.storageFraction: 0.2 (within memory fraction, how much for cache vs execution)
 * - spark.shuffle.file.buffer: 32k (smaller = more frequent spilling)
 */
class ShuffleBasedMergeOrchestrator(
  tablePath: String,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  @transient hadoopConf: Configuration
) {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[ShuffleBasedMergeOrchestrator])

  // Configuration
  private val targetSizeBytes = SizeParser.parseSize(
    options.getOrDefault("spark.indextables.mergeOnWrite.targetSize", "4G")
  )
  private val minSplitsToMerge = options.getOrDefault(
    "spark.indextables.mergeOnWrite.minSplitsToMerge", "2"
  ).toInt
  private val minDiskSpaceGB = options.getOrDefault(
    "spark.indextables.mergeOnWrite.minDiskSpaceGB", "20"
  ).toInt

  /**
   * Orchestrate merge-on-write using Spark's execution framework with in-memory shuffle
   *
   * IN-MEMORY SHUFFLE: Split bytes are already in memory (from commit messages).
   * No file reading needed - data moves purely through Spark's framework!
   *
   * @param shuffledSplits Splits with metadata AND bytes (from write phase)
   * @param saveMode Save mode (Overwrite or Append)
   * @return Sequence of AddActions for merged splits
   */
  def executeMergeOnWrite(
    shuffledSplits: Seq[ShuffledSplitData],  // Changed: now includes bytes!
    saveMode: org.apache.spark.sql.SaveMode
  ): Seq[AddAction] = {

    logger.info(s"Shuffle-based merge orchestrator starting with ${shuffledSplits.size} shuffled splits (in-memory shuffle, mode: $saveMode)")

    val spark = org.apache.spark.sql.SparkSession.active

    logger.info("=" * 80)
    logger.info("IN-MEMORY SHUFFLE-BASED MERGE-ON-WRITE")
    logger.info(s"Input: ${shuffledSplits.size} splits with in-memory bytes")
    logger.info(s"Total data: ${shuffledSplits.map(_.size).sum / 1024 / 1024}MB")
    logger.info(s"Target merge size: ${targetSizeBytes / 1024 / 1024}MB")
    logger.info(s"Minimum splits to merge: $minSplitsToMerge")
    logger.info("=" * 80)

    // Broadcast configuration to avoid serialization issues
    val optionsMap: Map[String, String] = options.asCaseSensitiveMap().asScala.toMap
    val broadcastTablePath = spark.sparkContext.broadcast(tablePath)
    val broadcastOptionsMap = spark.sparkContext.broadcast(optionsMap)

    logger.info(s"Creating RDD from ${shuffledSplits.size} in-memory split objects...")

    // IN-MEMORY SHUFFLE ARCHITECTURE:
    // 1. Split bytes are ALREADY in memory (loaded during write phase, returned in commit messages)
    // 2. Create RDD directly from in-memory data - NO file reading needed!
    // 3. Spark's shuffle mechanism distributes bytes to merge executors
    // 4. Merge tasks write consolidated splits directly to S3
    // 5. Pure in-memory data movement through Spark's execution framework!
    //
    // This eliminates file-based coordination and locality issues completely.
    val splitsRDD = spark.sparkContext.parallelize(shuffledSplits, shuffledSplits.size)

    logger.info(s"Created RDD with ${shuffledSplits.size} split data entries (no file I/O needed!)")

    // Step 1: Create merge groups (bin packing by partition and size)
    // Extract StagedSplitInfo from ShuffledSplitData for bin packing logic
    val stagedSplits = shuffledSplits.map(_.info)
    val mergeGroupsResult = createMergeGroups(stagedSplits)
    val mergeGroups = mergeGroupsResult.groupsToMerge
    val directUploadSplits = mergeGroupsResult.directUploadSplits

    logger.info(s"Created ${mergeGroups.size} merge groups from ${shuffledSplits.size} splits")
    mergeGroups.take(5).foreach { group =>
      logger.info(s"  Group ${group.groupId}: ${group.splits.size} splits, " +
        s"${group.estimatedSize / 1024 / 1024}MB, partition: ${group.partition}")
    }

    if (directUploadSplits.nonEmpty) {
      logger.info(s"${directUploadSplits.size} splits will be uploaded directly (< minSplitsToMerge threshold)")
    }

    if (mergeGroups.isEmpty && directUploadSplits.isEmpty) {
      logger.info("No merge groups or direct uploads - returning empty")
      return Seq.empty
    }

    // Step 2: Handle direct uploads (splits < minSplitsToMerge threshold)
    val directUploadActions = if (directUploadSplits.nonEmpty) {
      logger.info(s"Processing ${directUploadSplits.size} direct upload splits...")

      // Create lookup map from UUID to ShuffledSplitData
      val splitsByUUID = shuffledSplits.map(s => s.info.uuid -> s).toMap

      // Get ShuffledSplitData for direct uploads
      val directUploadShuffledSplits = directUploadSplits.flatMap { splitInfo =>
        splitsByUUID.get(splitInfo.uuid)
      }

      val directUploadsRDD = spark.sparkContext.parallelize(directUploadShuffledSplits, directUploadShuffledSplits.size).mapPartitions { splits =>
        splits.flatMap { shuffledSplit =>
          ShuffleBasedMergeOrchestrator.uploadSplitDirectly(
            shuffledSplit,  // Pass ShuffledSplitData (with bytes)
            broadcastTablePath.value,
            broadcastOptionsMap.value
          )
        }
      }
      directUploadsRDD.collect().toSeq
    } else {
      Seq.empty
    }

    // Step 3: Shuffle split data to merge groups (if any)
    val mergedActions = if (mergeGroups.nonEmpty) {
      val shuffledGroups = shuffleSplitsToMergeGroups(mergeGroups, splitsRDD, spark)

      // Execute merge on each partition (split data is local via shuffle)
      // Use broadcast variables to avoid closure capture
      val mergedSplitsRDD = shuffledGroups.mapPartitions { groups =>
        groups.flatMap { group =>
          ShuffleBasedMergeOrchestrator.executeMergeGroup(
            group,
            broadcastTablePath.value,
            broadcastOptionsMap.value
          )
        }
      }

      // Collect results and immediately unpersist to free memory
      val results = mergedSplitsRDD.collect().toSeq

      // CRITICAL: Unpersist cached RDD to prevent memory leak
      // This frees both memory and disk space used by persisted shuffle data
      shuffledGroups.unpersist(blocking = false)

      results
    } else {
      Seq.empty
    }

    // Step 4: Combine results (no cleanup needed - files already deleted after reading bytes)
    val allActions = directUploadActions ++ mergedActions

    logger.info(s"Merge-on-write completed: ${directUploadActions.size} direct uploads + ${mergedActions.size} merged splits = ${allActions.size} total")
    logger.info("=" * 80)

    allActions
  }

  /**
   * Create merge groups using optimal bin-packing algorithm
   *
   * With shuffle-based merging, we don't need to worry about locality (Spark handles it).
   * This allows us to optimize purely for packing efficiency.
   *
   * Groups splits by:
   * 1. Partition values (must match)
   * 2. Schema fingerprint (must match)
   * 3. Size-optimal bin packing (first-fit-decreasing algorithm)
   *
   * The first-fit-decreasing algorithm:
   * - Sorts splits by size (largest first)
   * - Places each split in the first bin with sufficient space
   * - Creates new bin if no existing bin has space
   * - Produces near-optimal packing (within 11/9 OPT + 6/9)
   *
   * Returns both merge groups and splits that should be uploaded directly (< minSplitsToMerge)
   */
  private def createMergeGroups(stagedSplits: Seq[StagedSplitInfo]): MergeGroupsResult = {

    logger.info(s"Creating merge groups from ${stagedSplits.size} splits using size-optimal bin packing")

    // Group by partition first (splits can only merge within same partition)
    val splitsByPartition = stagedSplits.groupBy(_.partitionValues)

    val allGroups = mutable.ArrayBuffer[MergeGroupInfo]()
    val allDirectUploads = mutable.ArrayBuffer[StagedSplitInfo]()
    var totalBins = 0
    var totalWastedSpace = 0L

    splitsByPartition.foreach { case (partition, partitionSplits) =>

      // Further group by schema fingerprint (splits must have same schema to merge)
      val splitsBySchema = partitionSplits.groupBy(_.schemaFingerprint)

      splitsBySchema.foreach { case (schemaFingerprint, schemaSplits) =>

        logger.debug(s"Bin packing ${schemaSplits.size} splits for partition $partition")

        // First-Fit-Decreasing (FFD) bin packing:
        // Sort splits by size descending (largest first)
        // This produces better packing than random or ascending order
        val sortedSplits = schemaSplits.sortBy(-_.size)
        val bins = mutable.ArrayBuffer[mutable.ArrayBuffer[StagedSplitInfo]]()

        sortedSplits.foreach { split =>
          // Find first bin with sufficient space
          val binWithSpace = bins.find { bin =>
            val currentSize = bin.map(_.size).sum
            // Allow 10% tolerance above target for better packing
            currentSize + split.size <= targetSizeBytes * 1.1
          }

          binWithSpace match {
            case Some(bin) =>
              // Add to existing bin (good packing)
              bin += split
            case None =>
              // Create new bin (unavoidable)
              val newBin = mutable.ArrayBuffer[StagedSplitInfo](split)
              bins += newBin
          }
        }

        // Separate bins into those that will be merged vs. uploaded directly
        val (binsToMerge, binsToUploadDirectly) = bins.partition(_.size >= minSplitsToMerge)

        // Convert bins to merge groups
        val mergeGroups = binsToMerge.map { bin =>
          val groupId = UUID.randomUUID().toString
          val estimatedSize = bin.map(_.size).sum
          val wastedSpace = targetSizeBytes - estimatedSize

          // Track packing efficiency
          if (wastedSpace > 0) {
            totalWastedSpace += wastedSpace
          }

          MergeGroupInfo(
            groupId = groupId,
            splits = bin.toSeq,
            partition = partition,
            estimatedSize = estimatedSize,
            schemaFingerprint = schemaFingerprint
          )
        }

        // Collect splits that will be uploaded directly (< minSplitsToMerge threshold)
        val directUploads = binsToUploadDirectly.flatten

        totalBins += bins.size
        allGroups ++= mergeGroups
        allDirectUploads ++= directUploads

        logger.debug(s"Created ${mergeGroups.size} merge groups and ${directUploads.size} direct uploads " +
          s"(${bins.size} total bins, ${binsToUploadDirectly.size} bins < minSplitsToMerge=$minSplitsToMerge)")
      }
    }

    val packingEfficiency = if (totalBins > 0) {
      val totalCapacity = totalBins * targetSizeBytes
      val totalUsed = totalCapacity - totalWastedSpace
      (totalUsed.toDouble / totalCapacity * 100).formatted("%.1f")
    } else {
      "N/A"
    }

    logger.info(s"Bin packing complete: ${allGroups.size} merge groups and ${allDirectUploads.size} direct uploads from $totalBins bins " +
      s"(packing efficiency: $packingEfficiency%, wasted: ${totalWastedSpace / 1024 / 1024}MB)")

    MergeGroupsResult(
      groupsToMerge = allGroups.toSeq,
      directUploadSplits = allDirectUploads.toSeq
    )
  }

  /**
   * Shuffle split bytes to merge executors
   *
   * Uses Spark's shuffle to distribute split data. Spark will automatically:
   * - Keep small splits in memory
   * - Spill large splits to disk
   * - Transfer data efficiently to merge executors
   *
   * @param mergeGroups Merge group assignments
   * @param splitsRDD RDD with split bytes
   * @return RDD of merge groups with split data co-located
   */
  private def shuffleSplitsToMergeGroups(
    mergeGroups: Seq[MergeGroupInfo],
    splitsRDD: RDD[ShuffledSplitData],
    spark: SparkSession
  ): RDD[ShuffledMergeGroup] = {

    // Create mapping: split UUID -> merge group ID
    val splitToGroupMap = mergeGroups.flatMap { group =>
      group.splits.map { split =>
        (split.uuid, group.groupId)
      }
    }.toMap

    // Broadcast the mapping (lightweight)
    val splitToGroupBC = spark.sparkContext.broadcast(splitToGroupMap)

    // Create mapping for merge group metadata
    val groupMetadataMap = mergeGroups.map { group =>
      (group.groupId, (group.partition, group.schemaFingerprint))
    }.toMap
    val groupMetadataBC = spark.sparkContext.broadcast(groupMetadataMap)

    logger.info(s"Shuffling ${splitsRDD.count()} splits to ${mergeGroups.size} merge groups...")

    // Map each split to its merge group ID
    val splitsByGroup: RDD[(String, ShuffledSplitData)] = splitsRDD.flatMap { splitData =>
      splitToGroupBC.value.get(splitData.info.uuid).map { groupId =>
        (groupId, splitData)
      }
    }

    // CRITICAL: Configure storage level to allow disk spilling
    // MEMORY_AND_DISK_SER will serialize data and spill to disk when memory is tight
    splitsByGroup.persist(StorageLevel.MEMORY_AND_DISK_SER)

    try {
      // Shuffle splits to their merge groups
      // groupByKey will trigger shuffle, and Spark will spill to disk as needed
      val groupedSplits: RDD[(String, Iterable[ShuffledSplitData])] = splitsByGroup.groupByKey()

      // Convert to ShuffledMergeGroup
      val shuffledGroups = groupedSplits.map { case (groupId, splits) =>
        val (partition, schemaFingerprint) = groupMetadataBC.value(groupId)
        val estimatedSize = splits.map(_.size).sum

        ShuffledMergeGroup(
          groupId = groupId,
          splits = splits.toSeq,
          partition = partition,
          estimatedSize = estimatedSize,
          schemaFingerprint = schemaFingerprint
        )
      }

      // Repartition to ensure good parallelism across executors
      val numPartitions = Math.min(mergeGroups.size, spark.sparkContext.defaultParallelism * 2)
      shuffledGroups.repartition(numPartitions)

    } finally {
      // CRITICAL: Unpersist after shuffle completes to prevent memory leak
      // This frees memory/disk used by the persisted (groupId, splitData) pairs
      splitsByGroup.unpersist(blocking = false)
    }
  }

  /**
   * Compute schema fingerprint for merge group compatibility
   */
  private def computeSchemaFingerprint(schema: StructType): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val schemaJson = schema.json
    val hash = digest.digest(schemaJson.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString
  }

  /**
   * Cleanup local split files after successful merge/upload
   *
   * After shuffle-based merge completes and uploads merged splits to S3,
   * the original split files in merge-on-write-staging are no longer needed.
   * This method deletes them to free disk space.
   *
   * SAFETY: Only deletes split files that belong to THIS merge job (based on stagedSplits UUID).
   * Concurrent merge jobs will have different UUIDs and won't interfere.
   *
   * IMPORTANT: Only called after successful merge/upload to avoid data loss.
   */
  private def cleanupLocalSplitFiles(stagedSplits: Seq[StagedSplitInfo]): Unit = {
    logger.info(s"Cleaning up ${stagedSplits.size} local split files from this merge job...")

    var deletedCount = 0
    var deletedBytes = 0L
    var failedCount = 0

    stagedSplits.foreach { splitInfo =>
      try {
        val localFile = new File(splitInfo.localPath)
        if (localFile.exists()) {
          // Verify the file name matches the expected UUID from this job
          // This prevents accidental deletion of files from concurrent merge jobs
          val expectedFileName = s"part-${splitInfo.uuid}.split"
          if (localFile.getName.endsWith(splitInfo.uuid + ".split")) {
            val fileSize = localFile.length()
            if (localFile.delete()) {
              deletedCount += 1
              deletedBytes += fileSize
              logger.debug(s"Deleted local split file: ${splitInfo.localPath} (${fileSize / 1024}KB)")
            } else {
              logger.warn(s"Failed to delete local split file (delete() returned false): ${splitInfo.localPath}")
              failedCount += 1
            }
          } else {
            logger.warn(s"Skipping cleanup of file with unexpected name: ${localFile.getName} " +
              s"(expected UUID: ${splitInfo.uuid})")
            failedCount += 1
          }
        } else {
          logger.debug(s"Local split file already deleted: ${splitInfo.localPath}")
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to delete local split file: ${splitInfo.localPath}", e)
          failedCount += 1
      }
    }

    logger.info(s"Cleanup complete: deleted $deletedCount files (${deletedBytes / 1024 / 1024}MB), " +
      s"$failedCount failures")

    // NOTE: We do NOT attempt to delete merge-on-write-staging directory
    // because concurrent merge jobs may be using it. The directory will be
    // cleaned up by OS temp directory cleanup or manual maintenance.
  }
}

/**
 * Result of merge group creation
 */
case class MergeGroupsResult(
  groupsToMerge: Seq[MergeGroupInfo],
  directUploadSplits: Seq[StagedSplitInfo]
) extends Serializable

/**
 * Lightweight merge group info (for planning, before shuffle)
 */
case class MergeGroupInfo(
  groupId: String,
  splits: Seq[StagedSplitInfo],
  partition: Map[String, String],
  estimatedSize: Long,
  schemaFingerprint: String
) extends Serializable
