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
 * Merge group with split metadata (splits on S3 staging)
 */
case class ShuffledMergeGroup(
  groupId: String,
  splits: Seq[StagedSplitInfo],
  partition: Map[String, String],
  estimatedSize: Long,
  schemaFingerprint: String
) extends Serializable

/**
 * Companion object with serializable static functions for RDD operations
 */
object MergeOnWriteOrchestrator {
  /**
   * Execute merge for a single group (on executor) - static to avoid closure capture
   *
   * Downloads splits from S3 staging, merges them, uploads to final location
   */
  def executeMergeGroup(
    group: ShuffledMergeGroup,
    tablePathBC: String,
    optionsMapBC: Map[String, String]
  ): Option[AddAction] = {
    val logger = LoggerFactory.getLogger(classOf[MergeOnWriteOrchestrator])

    logger.info(s"Executing merge for group ${group.groupId}: ${group.splits.size} splits, " +
      s"${group.estimatedSize / 1024 / 1024}MB")

    // Create temp directory for this merge
    val tempDir = Files.createTempDirectory("shuffle-merge-").toFile

    try {
      // Recreate CaseInsensitiveStringMap from serializable map
      val optionsBC = new CaseInsensitiveStringMap(optionsMapBC.asJava)

      // Download splits from S3 staging to temp .split files
      val totalInputRecords = group.splits.map(_.numRecords).sum
      logger.debug(s"MERGE GROUP DEBUG: Group ${group.groupId} has ${group.splits.size} splits with $totalInputRecords total records")

      val cloudProvider = CloudStorageProviderFactory.createProvider(tablePathBC, optionsBC, new Configuration())

      val splitPaths = group.splits.map { splitInfo =>
        // Download split from S3 staging
        val localSplitPath = new File(tempDir, s"staged-${splitInfo.uuid}.split").getAbsolutePath
        logger.debug(s"MERGE GROUP DEBUG: Downloading split from S3 staging: ${splitInfo.stagingPath} -> $localSplitPath")

        // Read split bytes from S3 staging
        val splitBytes = cloudProvider.readFile(splitInfo.stagingPath)

        // Write bytes to local file
        val outputStream = new FileOutputStream(localSplitPath)
        try {
          outputStream.write(splitBytes)
          outputStream.flush()
        } finally {
          outputStream.close()
        }

        logger.debug(s"MERGE GROUP DEBUG: Downloaded split with ${splitInfo.numRecords} records to: $localSplitPath (${splitInfo.size / 1024}KB)")
        localSplitPath
      }

      // Execute merge using tantivy4java (use empty hadoopConf on executor)
      val mergeEngine = new WorkerLocalSplitMerger(optionsBC, new Configuration())
      val (mergedSplitPath, metadata) = mergeEngine.mergeSplits(
        inputSplits = splitPaths,
        outputDir = tempDir.getAbsolutePath,
        partitionId = 0
      )

      // Upload merged split to S3 (reuse cloudProvider from download phase)
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
        val inputMinValues = group.splits.map(_.minValues)
        val inputMaxValues = group.splits.map(_.maxValues)
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
    splitInfo: StagedSplitInfo,  // Metadata only
    tablePathBC: String,
    optionsMapBC: Map[String, String]
  ): Option[AddAction] = {
    val logger = LoggerFactory.getLogger(classOf[MergeOnWriteOrchestrator])

    logger.info(s"Uploading split directly from S3 staging (< minSplitsToMerge): ${splitInfo.uuid}")

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

      logger.info(s"Copying split from S3 staging to final location: ${splitInfo.stagingPath} -> $cloudPath")

      // S3 STAGING: Copy from staging to final location
      // Read from staging, write to final location
      val splitBytes = cloudProvider.readFile(splitInfo.stagingPath)
      val inputStream = new java.io.ByteArrayInputStream(splitBytes)
      try {
        cloudProvider.writeFileFromStream(cloudPath, inputStream, Some(splitInfo.size))
      } finally {
        inputStream.close()
      }

      val uploadedSize = splitInfo.size
      logger.info(s"Direct upload complete from S3 staging: $cloudPath (${uploadedSize / 1024}KB)")

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
 * S3 staging-based merge orchestrator for merge-on-write
 *
 * ARCHITECTURE:
 * 1. Write phase creates splits locally and uploads to S3 staging location
 * 2. Driver receives only metadata (~1KB per split) via task results
 * 3. Shuffle phase distributes metadata (not bytes) to merge executors
 * 4. Merge phase downloads splits from S3 staging, merges locally, uploads to final location
 * 5. Cleanup phase deletes S3 staging files after successful merge
 *
 * ADVANTAGES over in-memory shuffle:
 * - Unlimited scalability: No driver memory bottleneck
 * - Metadata-only shuffle: Only ~1KB per split travels through Spark
 * - S3 as coordination layer: Natural fit for cloud-native environments
 * - Fault tolerant: S3 staging files persist across retries
 * - Production-ready: Proven pattern (Delta Lake, Iceberg, Hudi)
 *
 * CONFIGURATION:
 * - spark.indextables.mergeOnWrite.stagingDir: "_staging" (default, S3 staging subdirectory)
 * - spark.indextables.mergeOnWrite.maxRetries: 3 (S3 upload retry attempts)
 * - spark.indextables.mergeOnWrite.retryDelayMs: 1000 (retry delay with exponential backoff)
 */
class MergeOnWriteOrchestrator(
  tablePath: String,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  @transient hadoopConf: Configuration
) {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[MergeOnWriteOrchestrator])

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
   * Orchestrate merge-on-write using Spark's execution framework with S3 staging
   *
   * S3 STAGING: Split metadata is in memory, splits are on S3 staging location.
   * Merge executors download from S3 staging as needed!
   *
   * @param stagedSplits Splits with metadata only (bytes on S3 staging)
   * @param saveMode Save mode (Overwrite or Append)
   * @return Sequence of AddActions for merged splits
   */
  def executeMergeOnWrite(
    stagedSplits: Seq[StagedSplitInfo],  // Metadata only
    saveMode: org.apache.spark.sql.SaveMode
  ): Seq[AddAction] = {

    logger.info(s"Shuffle-based merge orchestrator starting with ${stagedSplits.size} staged splits (S3 staging, mode: $saveMode)")

    val spark = org.apache.spark.sql.SparkSession.active

    logger.info("=" * 80)
    logger.info("S3 STAGING-BASED MERGE-ON-WRITE")
    logger.info(s"Input: ${stagedSplits.size} staged splits (metadata only)")
    logger.info(s"Total data: ${stagedSplits.map(_.size).sum / 1024 / 1024}MB")
    logger.info(s"Target merge size: ${targetSizeBytes / 1024 / 1024}MB")
    logger.info(s"Minimum splits to merge: $minSplitsToMerge")
    logger.info("=" * 80)

    // Broadcast configuration to avoid serialization issues
    val optionsMap: Map[String, String] = options.asCaseSensitiveMap().asScala.toMap
    val broadcastTablePath = spark.sparkContext.broadcast(tablePath)
    val broadcastOptionsMap = spark.sparkContext.broadcast(optionsMap)

    logger.info(s"Creating RDD from ${stagedSplits.size} staged split metadata objects...")

    // S3 STAGING ARCHITECTURE:
    // 1. Split metadata is in memory (loaded from commit messages - ~1KB per split)
    // 2. Split files are on S3 staging location (uploaded during write phase)
    // 3. Create RDD from metadata only
    // 4. Spark shuffle distributes metadata to merge executors
    // 5. Merge tasks download splits from S3 staging, merge, upload to final location
    // 6. Scalable to any data size (no driver memory bottleneck)
    //
    // This eliminates driver memory bottleneck while maintaining distributed merge.
    val splitsRDD = spark.sparkContext.parallelize(stagedSplits, stagedSplits.size)

    logger.info(s"Created RDD with ${stagedSplits.size} split metadata entries (splits on S3 staging)")

    // Step 1: Create merge groups (bin packing by partition and size)
    val mergeGroupsResult = createMergeGroups(stagedSplits)
    val mergeGroups = mergeGroupsResult.groupsToMerge
    val directUploadSplits = mergeGroupsResult.directUploadSplits

    logger.info(s"Created ${mergeGroups.size} merge groups from ${stagedSplits.size} splits")
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

      val directUploadsRDD = spark.sparkContext.parallelize(directUploadSplits, directUploadSplits.size).mapPartitions { splits =>
        splits.flatMap { splitInfo =>
          MergeOnWriteOrchestrator.uploadSplitDirectly(
            splitInfo,  // Pass StagedSplitInfo (metadata only)
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
          MergeOnWriteOrchestrator.executeMergeGroup(
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

    // Step 4: Combine results
    val allActions = directUploadActions ++ mergedActions

    logger.info(s"Merge-on-write completed: ${directUploadActions.size} direct uploads + ${mergedActions.size} merged splits = ${allActions.size} total")

    // Step 5: Cleanup S3 staging files after successful merge/upload
    // Only clean up if we successfully created AddActions (merge/upload succeeded)
    if (allActions.nonEmpty) {
      cleanupS3StagingFiles(stagedSplits)
    }

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
   * Shuffle split metadata to merge executors
   *
   * Uses Spark's shuffle to distribute split metadata. Lightweight since we're only
   * shuffling metadata (~1KB per split), not the actual split bytes.
   *
   * @param mergeGroups Merge group assignments
   * @param splitsRDD RDD with split metadata
   * @return RDD of merge groups with split metadata co-located
   */
  private def shuffleSplitsToMergeGroups(
    mergeGroups: Seq[MergeGroupInfo],
    splitsRDD: RDD[StagedSplitInfo],
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

    logger.info(s"Shuffling ${splitsRDD.count()} split metadata entries to ${mergeGroups.size} merge groups...")

    // Map each split to its merge group ID
    val splitsByGroup: RDD[(String, StagedSplitInfo)] = splitsRDD.flatMap { splitInfo =>
      splitToGroupBC.value.get(splitInfo.uuid).map { groupId =>
        (groupId, splitInfo)
      }
    }

    // Shuffle splits to their merge groups (lightweight - only metadata)
    val groupedSplits: RDD[(String, Iterable[StagedSplitInfo])] = splitsByGroup.groupByKey()

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
   * Cleanup S3 staging files after successful merge/upload
   *
   * After merge-on-write completes and uploads merged/promoted splits to final location,
   * the original split files in S3 staging (_staging/) are no longer needed.
   * This method deletes them to free S3 storage and avoid costs.
   *
   * SAFETY: Only deletes split files that belong to THIS merge job (based on stagedSplits UUID).
   * Concurrent merge jobs will have different UUIDs and won't interfere.
   *
   * IMPORTANT: Only called after successful merge/upload to avoid data loss.
   *
   * ERROR HANDLING: Best-effort deletion - failures are logged but don't fail the merge.
   * S3 staging files have TTL/lifecycle policies as backup cleanup mechanism.
   */
  private def cleanupS3StagingFiles(stagedSplits: Seq[StagedSplitInfo]): Unit = {
    if (stagedSplits.isEmpty) {
      logger.debug("No S3 staging files to clean up (empty split list)")
      return
    }

    logger.info(s"Cleaning up ${stagedSplits.size} S3 staging files after successful merge/upload...")

    var deletedCount = 0
    var deletedBytes = 0L
    var failedCount = 0

    // Create cloud provider for S3 operations
    val cloudProvider = try {
      CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to create cloud provider for S3 staging cleanup: ${e.getMessage}")
        return
    }

    try {
      stagedSplits.foreach { splitInfo =>
        if (splitInfo.stagingPath.nonEmpty && splitInfo.stagingAvailable) {
          try {
            // Delete split file from S3 staging
            cloudProvider.deleteFile(splitInfo.stagingPath)
            deletedCount += 1
            deletedBytes += splitInfo.size
            logger.debug(s"Deleted S3 staging file: ${splitInfo.stagingPath} (${splitInfo.size / 1024}KB)")
          } catch {
            case e: Exception =>
              // Log but don't fail - S3 cleanup is best-effort
              logger.warn(s"Failed to delete S3 staging file: ${splitInfo.stagingPath}: ${e.getMessage}")
              failedCount += 1
          }
        } else {
          logger.debug(s"Skipping S3 cleanup for split ${splitInfo.uuid} (no staging path or not available)")
        }
      }

      logger.info(s"S3 staging cleanup complete: deleted $deletedCount files (${deletedBytes / 1024 / 1024}MB), " +
        s"$failedCount failures")

      if (failedCount > 0) {
        logger.warn(s"Some S3 staging files could not be deleted. Consider setting up S3 lifecycle policies " +
          s"to automatically delete files in _staging/ after 7 days.")
      }

    } finally {
      // Always close cloud provider
      try {
        cloudProvider.close()
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to close cloud provider during S3 staging cleanup: ${e.getMessage}")
      }
    }
  }

  /**
   * Cleanup local split files after successful merge/upload
   *
   * OBSOLETE: Local files are now cleaned up immediately after S3 upload in createLocalSplitForMergeOnWrite().
   * This method is kept for backwards compatibility but should not be called in S3 staging mode.
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
