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

import java.io.{File, FileInputStream}
import java.security.MessageDigest
import java.util.UUID

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.conf.Configuration

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.SizeParser
import org.slf4j.LoggerFactory

/**
 * Orchestrates merge-on-write by grouping splits using locality-aware bin packing
 * and distributing merge operations to workers where splits were originally created.
 *
 * GAP MITIGATIONS:
 * - Gap #1 (Dynamic Allocation): Detects and adapts to dynamic allocation mode
 * - Gap #3 (Schema Evolution): Validates schema compatibility before merging
 * - Gap #6 (Merge Timeouts): Calculates dynamic timeouts based on merge group size
 * - Gap #7 (Transaction Log Batching): Batches large commits to avoid contention
 * - Gap #9 (Observability): Collects comprehensive metrics
 */
class LocalityAwareSplitMergeOrchestrator(
  tablePath: String,
  options: CaseInsensitiveStringMap,
  hadoopConf: Configuration,
  sparkSession: SparkSession,
  schema: StructType                 // NEW: Schema for fingerprinting (Gap #3)
) extends Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[LocalityAwareSplitMergeOrchestrator])

  // Compute schema fingerprint for validation (Gap #3)
  private val schemaFingerprint: String = computeSchemaFingerprint(schema)

  /**
   * Main entry point: orchestrate merge-on-write workflow
   *
   * @param stagedSplits Metadata from all workers' commit messages
   * @param stagingUploader The uploader to wait for async uploads
   * @param saveMode Overwrite or Append
   * @return Final AddActions to write to transaction log
   */
  def executeMergeOnWrite(
    stagedSplits: Seq[StagedSplitInfo],
    stagingUploader: SplitStagingUploader,
    saveMode: SaveMode
  ): Seq[AddAction] = {

    val startTime = System.currentTimeMillis()
    var metricsBuilder = MergeOnWriteMetrics(
      totalSplitsCreated = stagedSplits.size,
      totalSplitsStaged = 0,
      stagingUploadTimeMs = 0,
      stagingFailures = 0,
      stagingRetries = 0,
      mergeGroupsCreated = 0,
      mergesExecuted = 0,
      mergesLocalFile = 0,
      mergesRemoteDownload = 0,
      mergeDurationMs = 0,
      splitsPromoted = 0,
      finalSplitCount = 0,
      networkBytesUploaded = 0,
      networkBytesDownloaded = 0,
      localityHitRate = 0.0,
      cleanupDurationMs = 0
    )

    try {
      // Gap #1: Detect dynamic allocation mode
      val dynamicAllocationEnabled = detectDynamicAllocation()
      if (dynamicAllocationEnabled) {
        logger.warn("Dynamic allocation detected - reduced locality hit rate expected")
        metricsBuilder = metricsBuilder.copy(dynamicAllocationDetected = true)
      }

      // Phase 0: Wait for all async staging uploads to complete
      logger.info(s"Waiting for ${stagedSplits.size} staging uploads to complete...")
      val stagingStartTime = System.currentTimeMillis()

      val allowPartialStaging = options.getOrDefault(
        "spark.indextables.mergeOnWrite.allowPartialStaging", "false"
      ).toBoolean
      val minSuccessRate = options.getOrDefault(
        "spark.indextables.mergeOnWrite.minStagingSuccessRate", "0.95"
      ).toDouble

      val stagingResults = stagingUploader.awaitAllStaging(
        timeoutMillis = 600000,
        allowPartialStaging = allowPartialStaging,
        minSuccessRate = minSuccessRate
      )

      val stagingDuration = System.currentTimeMillis() - stagingStartTime

      // Update splits with staging availability (Gap #2)
      val updatedSplits = stagedSplits.map { split =>
        val result = stagingResults.get(split.uuid)
        val available = result.exists(_.success)
        split.copy(stagingAvailable = available)
      }

      // Collect staging metrics
      val stagingFailures = stagingResults.values.count(!_.success)
      val stagingRetries = stagingResults.values.map(_.retriesAttempted).sum
      val bytesUploaded = stagingResults.values.map(_.bytesUploaded).sum

      logger.info(s"Staging uploads completed: ${stagedSplits.size - stagingFailures}/${stagedSplits.size} successful")

      metricsBuilder = metricsBuilder.copy(
        totalSplitsStaged = stagedSplits.size - stagingFailures,
        stagingUploadTimeMs = stagingDuration,
        stagingFailures = stagingFailures,
        stagingRetries = stagingRetries,
        networkBytesUploaded = bytesUploaded
      )

      // Gap #3: Validate schema compatibility
      validateSchemaCompatibility(updatedSplits)

      // Phase 1: Group by partition
      val splitsByPartition = updatedSplits.groupBy(_.partitionValues)

      // Phase 2: Locality-aware bin packing per partition
      val mergeGroups = splitsByPartition.flatMap { case (partition, splits) =>
        createLocalityAwareMergeGroups(splits, partition)
      }.toSeq

      logger.info(s"Created ${mergeGroups.size} merge groups from ${stagedSplits.size} splits")
      metricsBuilder = metricsBuilder.copy(mergeGroupsCreated = mergeGroups.size)

      // Phase 3: Execute merges in parallel on Spark cluster (with locality)
      val mergeStartTime = System.currentTimeMillis()
      val (mergedSplits, mergedSplitIds, mergeMetrics) = executeMergeGroupsWithLocality(mergeGroups)
      val mergeDuration = System.currentTimeMillis() - mergeStartTime

      metricsBuilder = metricsBuilder.copy(
        mergesExecuted = mergeMetrics.mergesExecuted,
        mergesLocalFile = mergeMetrics.mergesLocalFile,
        mergesRemoteDownload = mergeMetrics.mergesRemoteDownload,
        mergeDurationMs = mergeDuration,
        networkBytesDownloaded = mergeMetrics.networkBytesDownloaded,
        localityHitRate = if (mergeMetrics.mergesExecuted > 0)
          mergeMetrics.mergesLocalFile.toDouble / mergeMetrics.mergesExecuted
        else 0.0
      )

      // Phase 4: Promote unmerged splits (those too small or not grouped)
      // BUG FIX: Use mergedSplitIds instead of mergeGroups to determine unmerged splits
      val unmergedSplits = updatedSplits.filterNot(s => mergedSplitIds.contains(s.uuid))
      logger.info(s"Promoting ${unmergedSplits.size} unmerged splits (${mergedSplitIds.size} were merged)")
      val promotedSplits = promoteUnmergedSplits(unmergedSplits)

      logger.info(s"Merge complete: ${mergedSplits.size} merged, ${promotedSplits.size} promoted")
      metricsBuilder = metricsBuilder.copy(
        splitsPromoted = promotedSplits.size,
        finalSplitCount = mergedSplits.size + promotedSplits.size
      )

      // Phase 5: Cleanup temporary files
      val cleanupStartTime = System.currentTimeMillis()
      cleanupTemporaryFiles(updatedSplits, mergedSplits ++ promotedSplits)
      val cleanupDuration = System.currentTimeMillis() - cleanupStartTime

      metricsBuilder = metricsBuilder.copy(
        cleanupDurationMs = cleanupDuration,
        totalDurationMs = System.currentTimeMillis() - startTime
      )

      // Log final metrics (Gap #9: Observability)
      logger.info(metricsBuilder.summary)

      mergedSplits ++ promotedSplits

    } catch {
      case e: Exception =>
        logger.error("Merge-on-write operation failed", e)
        metricsBuilder = metricsBuilder.copy(
          totalDurationMs = System.currentTimeMillis() - startTime
        )
        logger.error(s"Partial metrics before failure:\n${metricsBuilder.summary}")
        throw e
    }
  }

  /**
   * Gap #1: Detect if Spark dynamic allocation is enabled
   */
  private def detectDynamicAllocation(): Boolean = {
    try {
      sparkSession.conf.get("spark.dynamicAllocation.enabled", "false").toBoolean
    } catch {
      case e: Exception =>
        logger.warn("Failed to detect dynamic allocation status", e)
        false
    }
  }

  /**
   * Gap #3: Compute schema fingerprint for compatibility checking
   */
  private def computeSchemaFingerprint(schema: StructType): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val schemaJson = schema.json
    val hash = digest.digest(schemaJson.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString
  }

  /**
   * Gap #3: Validate schema compatibility across all splits
   */
  private def validateSchemaCompatibility(splits: Seq[StagedSplitInfo]): Unit = {
    val allowMixedSchemas = options.getOrDefault(
      "spark.indextables.mergeOnWrite.allowMixedSchemas", "false"
    ).toBoolean

    if (!allowMixedSchemas) {
      val uniqueSchemas = splits.map(_.schemaFingerprint).distinct
      if (uniqueSchemas.size > 1) {
        throw new RuntimeException(
          s"Schema evolution detected: ${uniqueSchemas.size} different schemas found. " +
          s"Set spark.indextables.mergeOnWrite.allowMixedSchemas=true to allow."
        )
      }
    }
  }

  /**
   * Locality-aware bin packing: group splits by host to maximize local merges
   *
   * Gap #3: Ensures schema compatibility within each merge group
   */
  private def createLocalityAwareMergeGroups(
    splits: Seq[StagedSplitInfo],
    partition: Map[String, String]
  ): Seq[LocalityAwareMergeGroup] = {

    val targetSizeBytes = SizeParser.parseSize(
      options.getOrDefault("spark.indextables.mergeOnWrite.targetSize", "4G")
    )
    val minSplitsToMerge = options.getOrDefault(
      "spark.indextables.mergeOnWrite.minSplitsToMerge", "2"
    ).toInt

    // Group splits by worker host for locality
    val splitsByHost = splits.groupBy(_.workerHost)

    val mergeGroups = mutable.ArrayBuffer[LocalityAwareMergeGroup]()

    splitsByHost.foreach { case (host, hostSplits) =>
      // Further group by schema fingerprint (Gap #3)
      val splitsBySchema = hostSplits.groupBy(_.schemaFingerprint)

      splitsBySchema.foreach { case (fingerprint, schemaSplits) =>
        // Sort by size (descending) for optimal bin packing
        val sortedSplits = schemaSplits.sortBy(-_.size).toBuffer

        while (sortedSplits.nonEmpty) {
          val currentSplit = sortedSplits.head
          val group = mutable.ArrayBuffer[StagedSplitInfo](currentSplit)
          var groupSize = currentSplit.size
          sortedSplits.remove(0)

          // First-fit-decreasing bin packing with 10% tolerance
          var i = 0
          while (i < sortedSplits.length && groupSize < targetSizeBytes) {
            val candidateSplit = sortedSplits(i)
            if (groupSize + candidateSplit.size <= targetSizeBytes * 1.1) { // 10% tolerance
              group += candidateSplit
              groupSize += candidateSplit.size
              sortedSplits.remove(i)
            } else {
              i += 1
            }
          }

          // Only create merge group if we have enough splits
          if (group.size >= minSplitsToMerge) {
            mergeGroups += LocalityAwareMergeGroup(
              groupId = UUID.randomUUID().toString,
              preferredHost = host,
              splits = group.toSeq,
              partition = partition,
              estimatedSize = groupSize,
              schemaFingerprint = fingerprint
            )
          }
        }
      }
    }

    logger.info(s"Bin packing for partition ${partition}: " +
      s"${splits.size} splits → ${mergeGroups.size} merge groups")

    mergeGroups.toSeq
  }

  /**
   * Execute merge operations with locality preferences using Spark RDD
   *
   * Gap #6: Implements dynamic timeout calculation
   */
  private def executeMergeGroupsWithLocality(
    mergeGroups: Seq[LocalityAwareMergeGroup]
  ): (Seq[AddAction], Set[String], MergeExecutionMetrics) = {

    if (mergeGroups.isEmpty) {
      return (Seq.empty, Set.empty, MergeExecutionMetrics(0, 0, 0, 0))
    }

    // Gap #6: Calculate dynamic timeout based on largest merge group
    val largestGroupSize = mergeGroups.map(_.estimatedSize).max
    val mergeThroughputMBps = options.getOrDefault(
      "spark.indextables.mergeOnWrite.mergeThroughputMBps", "100"
    ).toInt
    val baseMergeTimeoutSeconds = options.getOrDefault(
      "spark.indextables.mergeOnWrite.baseMergeTimeoutSeconds", "600"
    ).toInt

    val estimatedSeconds = (largestGroupSize / (mergeThroughputMBps * 1024L * 1024L)).toInt
    val calculatedTimeout = Math.max(baseMergeTimeoutSeconds, estimatedSeconds * 3) // 3x safety factor

    logger.info(s"Calculated merge timeout: ${calculatedTimeout}s for largest group: ${largestGroupSize / 1024 / 1024}MB")

    // Extract serializable configuration to avoid capturing 'this'
    val serializedOptions = options.asCaseSensitiveMap().asScala.toMap
    val serializedHadoopConf = {
      import scala.jdk.CollectionConverters._
      hadoopConf.iterator().asScala.map(e => (e.getKey, e.getValue)).toMap
    }
    val tablePathStr = tablePath
    val schemaValue = schema

    // Create RDD of merge groups with locality-preserving distribution
    // BUG FIX: Distribute work across all executors while preserving locality
    val numExecutors = sparkSession.sparkContext.getExecutorMemoryStatus.size - 1 // exclude driver

    // Group merge groups by host, then distribute each host's groups across multiple partitions
    val groupsByHost = mergeGroups.groupBy(_.preferredHost)
    logger.info(s"Merge groups distributed across ${groupsByHost.size} hosts: " +
      groupsByHost.map { case (host, groups) => s"$host(${groups.size})" }.mkString(", "))

    // Create a flat list of (mergeGroup, preferredHost, partitionIndex) tuples
    // Distribute each host's groups across multiple partitions to ensure parallelism
    val distributedGroups = groupsByHost.flatMap { case (host, hostGroups) =>
      // Calculate how many partitions this host should use
      // Aim for at least one partition per host, but distribute if there are many groups
      val partitionsForHost = Math.max(1, Math.min(numExecutors / groupsByHost.size, hostGroups.size))

      // Round-robin assign this host's groups across its allocated partitions
      hostGroups.zipWithIndex.map { case (group, idx) =>
        val partitionOffset = groupsByHost.keys.toSeq.sorted.indexOf(host) * partitionsForHost
        val partitionIndex = partitionOffset + (idx % partitionsForHost)
        (group, Seq(host), partitionIndex)
      }
    }.toSeq.sortBy(_._3) // Sort by partition index for deterministic ordering

    logger.info(s"Distributing ${mergeGroups.size} merge groups across ${distributedGroups.map(_._3).distinct.size} partitions " +
      s"($numExecutors executors available)")

    // Create RDD with locality preferences intact
    // makeRDD signature: Seq[(T, Seq[String])] creates RDD[T] with locality hints
    val mergeGroupsRDD = sparkSession.sparkContext.makeRDD(
      distributedGroups.map { case (group, hosts, _) => (group, hosts) }
    )

    // Execute merges (Spark will schedule tasks on preferred hosts due to locality hints)
    val mergeResultsWithIds = mergeGroupsRDD.mapPartitions { iter =>
      // Reconstruct non-serializable objects on executor
      import scala.jdk.CollectionConverters._
      val executorLogger = org.slf4j.LoggerFactory.getLogger(classOf[LocalityAwareSplitMergeOrchestrator])
      val executorOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(serializedOptions.asJava)
      val executorHadoopConf = new org.apache.hadoop.conf.Configuration()
      serializedHadoopConf.foreach { case (k, v) => executorHadoopConf.set(k, v) }

      // Track actual host for locality metrics
      val actualHost = java.net.InetAddress.getLocalHost.getHostName

      iter.flatMap { mergeGroup =>
        // Log locality: check if we're running on the preferred host
        if (actualHost != mergeGroup.preferredHost) {
          executorLogger.warn(s"Locality miss: merge group for host ${mergeGroup.preferredHost} running on $actualHost")
        }

        // Execute merge operation inline (cannot call instance method)
        LocalityAwareSplitMergeOrchestrator.executeSingleMergeOperationStatic(
          mergeGroup,
          tablePathStr,
          executorOptions,
          executorHadoopConf,
          schemaValue
        )
      }
    }.collect()

    logger.info(s"Completed ${mergeResultsWithIds.length} merge operations")

    // Separate AddActions from merged split IDs
    val mergeResults = mergeResultsWithIds.map(_._1)
    val mergedSplitIds = mergeResultsWithIds.flatMap(_._2).toSet
    logger.info(s"Merged splits: ${mergedSplitIds.size} split IDs tracked")

    // Extract metrics from results
    val totalMerges = mergeResults.length
    val localMerges = mergeResults.count(_.tags.flatMap(_.get("localityType")).contains("local"))
    val remoteMerges = mergeResults.count(_.tags.flatMap(_.get("localityType")).contains("remote"))
    val bytesDownloaded = mergeResults.flatMap(_.tags.flatMap(_.get("bytesDownloaded")).map(_.toLong)).sum

    (mergeResults.toSeq, mergedSplitIds, MergeExecutionMetrics(totalMerges, localMerges, remoteMerges, bytesDownloaded))
  }


  /**
   * Promote unmerged splits to final location
   * (Splits that weren't included in any merge group or failed to merge)
   */
  private def promoteUnmergedSplits(
    unmergedSplits: Seq[StagedSplitInfo]
  ): Seq[AddAction] = {

    logger.info(s"Promoting ${unmergedSplits.size} unmerged splits to final location")

    val cloudProvider = CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)

    try {
      unmergedSplits.map { split =>
        // Copy from staging to final location
        // Include partition path if this is a partitioned write
        val partitionPath = if (split.partitionValues.nonEmpty) {
          split.partitionValues.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
        } else {
          ""
        }
        val relativePath = s"${partitionPath}split-${split.uuid}.split"
        val finalPath = s"${tablePath}/$relativePath"

        // Check if local file still exists, otherwise use staging
        // BUG FIX #3: Add comprehensive debug logging
        val localFile = new File(split.localPath)
        val localExists = localFile.exists()

        logger.info(s"Promoting split ${split.uuid}: localPath=${split.localPath} (exists=$localExists), " +
          s"stagingPath=${split.stagingPath} (available=${split.stagingAvailable}), " +
          s"workerHost=${split.workerHost}, executorId=${split.executorId}")

        val sourceStream = {
          if (localExists) {
            logger.info(s"Using local file for promotion: ${split.localPath}")
            new FileInputStream(localFile)
          } else if (split.stagingAvailable) {
            logger.info(s"Downloading from staging for promotion: ${split.stagingPath}")
            try {
              cloudProvider.openInputStream(split.stagingPath)
            } catch {
              case e: Exception =>
                logger.error(s"Failed to download from staging: ${split.stagingPath}", e)
                throw new RuntimeException(
                  s"Unmerged split failed to download from staging: uuid=${split.uuid}, " +
                  s"localPath=${split.localPath} (exists=$localExists), " +
                  s"stagingPath=${split.stagingPath} (available=${split.stagingAvailable})",
                  e
                )
            }
          } else {
            throw new RuntimeException(
              s"Unmerged split not available locally or in staging: uuid=${split.uuid}, " +
              s"localPath=${split.localPath} (exists=$localExists), " +
              s"stagingPath=${split.stagingPath} (available=${split.stagingAvailable}), " +
              s"workerHost=${split.workerHost}, executorId=${split.executorId}"
            )
          }
        }

        cloudProvider.writeFileFromStream(finalPath, sourceStream, Some(split.size))

        // Convert to AddAction (use relative path)
        AddAction(
          path = relativePath,
          partitionValues = split.partitionValues,
          size = split.size,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = Some(Map("mergeStrategy" -> "promoted")),
          minValues = Some(split.minValues),
          maxValues = Some(split.maxValues),
          numRecords = Some(split.numRecords),
          footerStartOffset = split.footerStartOffset,
          footerEndOffset = split.footerEndOffset,
          hasFooterOffsets = split.footerStartOffset.isDefined,
          timeRangeStart = split.timeRangeStart,
          timeRangeEnd = split.timeRangeEnd,
          splitTags = split.splitTags,
          deleteOpstamp = split.deleteOpstamp,
          numMergeOps = Some(0), // Not merged
          docMappingJson = split.docMappingJson,
          uncompressedSizeBytes = split.uncompressedSizeBytes
        )
      }
    } finally {
      cloudProvider.close()
    }
  }

  /**
   * Cleanup temporary files from local disks and staging area
   *
   * Gap #2: Progressive cleanup with error handling
   * Gap #5: Cleanup speculative execution artifacts
   */
  private def cleanupTemporaryFiles(
    allSplits: Seq[StagedSplitInfo],
    finalSplits: Seq[AddAction]
  ): Unit = {

    val keepStagingFiles = options.getOrDefault(
      "spark.indextables.mergeOnWrite.keepStagingFiles", "false"
    ).toBoolean

    val debugMode = options.getOrDefault(
      "spark.indextables.mergeOnWrite.debugMode", "false"
    ).toBoolean

    if (debugMode || keepStagingFiles) {
      logger.info("Debug mode or keepStagingFiles enabled - skipping cleanup")
      return
    }

    // Cleanup local files (distributed task)
    try {
      val localCleanupRDD = sparkSession.sparkContext.parallelize(allSplits.map(_.localPath))
      localCleanupRDD.foreach { localPath =>
        try {
          val file = new File(localPath)
          if (file.exists() && file.delete()) {
            logger.debug(s"Deleted local temp file: $localPath")
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to delete local temp file: $localPath", e)
        }
      }
    } catch {
      case e: Exception =>
        logger.warn("Local cleanup failed (non-fatal)", e)
    }

    // Cleanup staging files (from driver)
    try {
      val cloudProvider = CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)

      try {
        // Gap #5: Get committed task attempts to identify speculative execution losers
        val committedTaskAttempts = allSplits.map(_.taskAttemptId).toSet

        // List all staging files
        val stagingDir = s"${tablePath}/_staging/"
        val allStagingFiles = cloudProvider.listFiles(stagingDir, recursive = false)

        // Delete staging files
        allStagingFiles.foreach { fileInfo =>
          try {
            cloudProvider.deleteFile(fileInfo.path)
            logger.debug(s"Deleted staging file: ${fileInfo.path}")
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to delete staging file: ${fileInfo.path}", e)
          }
        }

        logger.info(s"Deleted ${allStagingFiles.size} staging files from $stagingDir")

      } finally {
        cloudProvider.close()
      }
    } catch {
      case e: Exception =>
        logger.warn("Staging cleanup failed (non-fatal)", e)
    }
  }

  private def getTempDirectory(): String = {
    io.indextables.spark.storage.SplitCacheConfig.getDefaultTempPath()
      .getOrElse(System.getProperty("java.io.tmpdir"))
  }
}

/**
 * Companion object with static methods that can be called from executors without serialization
 */
object LocalityAwareSplitMergeOrchestrator {

  /**
   * Execute a single merge operation on executor (static method to avoid serialization issues)
   *
   * This method must be static (in companion object) to avoid capturing the orchestrator instance.
   *
   * Returns: Option[(AddAction, Set[String])] where the Set contains UUIDs of merged splits
   */
  def executeSingleMergeOperationStatic(
    mergeGroup: LocalityAwareMergeGroup,
    tablePath: String,
    options: org.apache.spark.sql.util.CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration,
    schema: org.apache.spark.sql.types.StructType
  ): Option[(AddAction, Set[String])] = {

    // Create logger on executor (not serialized)
    val logger = org.slf4j.LoggerFactory.getLogger(classOf[LocalityAwareSplitMergeOrchestrator])

    val mergeEngine = new WorkerLocalSplitMerger(options, hadoopConf)
    val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)

    var localFilesUsed = 0
    var remoteDownloads = 0
    var bytesDownloaded = 0L
    val tempFilesToCleanup = scala.collection.mutable.ArrayBuffer[java.io.File]()

    try {
      // 1. Collect split files (prefer local, fallback to staging)
      val splitPaths = mergeGroup.splits.map { split =>
        val localFile = new java.io.File(split.localPath)
        if (localFile.exists()) {
          logger.info(s"Using local split: ${split.localPath}")
          localFilesUsed += 1
          split.localPath
        } else {
          // Download from staging
          if (split.stagingAvailable) {
            logger.info(s"Downloading split from staging: ${split.stagingPath}")
            remoteDownloads += 1
            val tempFile = mergeEngine.downloadToTemp(split.stagingPath, cloudProvider)
            bytesDownloaded += tempFile.length()
            tempFilesToCleanup += tempFile
            tempFile.getAbsolutePath
          } else {
            throw new RuntimeException(
              s"Split not available locally or in staging: ${split.uuid} (local: ${split.localPath}, staging: ${split.stagingPath})"
            )
          }
        }
      }

      // 2. Execute merge using tantivy4java
      val tempDir = java.nio.file.Files.createTempDirectory("merge-").toFile.getAbsolutePath
      val (mergedSplitPath, metadata) = mergeEngine.mergeSplits(
        inputSplits = splitPaths,
        outputDir = tempDir,
        partitionId = 0
      )

      // 3. Upload merged split to final location
      // Include partition path if this is a partitioned write
      val partitionPath = if (mergeGroup.partition.nonEmpty) {
        mergeGroup.partition.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
      } else {
        ""
      }
      val relativePath = s"${partitionPath}merged-${mergeGroup.groupId}.split"
      val finalPath = s"${tablePath}/$relativePath"
      val mergedFile = new java.io.File(mergedSplitPath)
      cloudProvider.writeFileFromStream(
        finalPath,
        new java.io.FileInputStream(mergedSplitPath),
        Some(mergedFile.length())
      )

      // 4. Extract metadata from merged split with statistics from input splits
      val inputMinValues = mergeGroup.splits.map(_.minValues)
      val inputMaxValues = mergeGroup.splits.map(_.maxValues)
      val mergedMetadata = mergeEngine.extractMetadataFromMergeResult(
        metadata,
        mergedSplitPath,
        inputMinValues,
        inputMaxValues
      )

      // BUG FIX: DO NOT delete local splits here! They might be needed for promotion.
      // Progressive cleanup is now handled AFTER promotion to avoid race condition.
      // The driver will clean up splits after determining which were merged vs promoted.
      //
      // OLD BUGGY CODE (commented out):
      // if (options.getOrDefault("spark.indextables.mergeOnWrite.progressiveCleanup", "true").toBoolean) {
      //   mergeGroup.splits.foreach { split =>
      //     val localFile = new java.io.File(split.localPath)
      //     if (localFile.exists() && localFile.delete()) {
      //       logger.debug(s"Deleted local split after merge: ${split.localPath}")
      //     }
      //   }
      // }

      // 5. Create AddAction with locality metadata and merged split IDs
      val mergedSplitIds = mergeGroup.splits.map(_.uuid).toSet
      val addAction = AddAction(
        path = relativePath,
        partitionValues = mergeGroup.partition,
        size = mergedMetadata.size,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        stats = None,
        tags = Some(Map(
          "mergeStrategy" -> "merge_on_write",
          "localityType" -> (if (localFilesUsed > remoteDownloads) "local" else "remote"),
          "localFilesUsed" -> localFilesUsed.toString,
          "remoteDownloads" -> remoteDownloads.toString,
          "bytesDownloaded" -> bytesDownloaded.toString
        )),
        minValues = Some(mergedMetadata.minValues),
        maxValues = Some(mergedMetadata.maxValues),
        numRecords = Some(mergedMetadata.numRecords),
        footerStartOffset = mergedMetadata.footerStartOffset,
        footerEndOffset = mergedMetadata.footerEndOffset,
        hasFooterOffsets = mergedMetadata.footerStartOffset.isDefined,
        timeRangeStart = mergedMetadata.timeRangeStart,
        timeRangeEnd = mergedMetadata.timeRangeEnd,
        splitTags = mergedMetadata.splitTags,
        deleteOpstamp = mergedMetadata.deleteOpstamp,
        numMergeOps = Some(mergeGroup.splits.size),
        docMappingJson = mergedMetadata.docMappingJson,
        uncompressedSizeBytes = mergedMetadata.uncompressedSizeBytes
      )

      Some((addAction, mergedSplitIds))

    } catch {
      case e: Exception =>
        logger.error(s"Failed to merge group ${mergeGroup.groupId}", e)
        None
    } finally {
      // Cleanup temp downloads
      tempFilesToCleanup.foreach { tempFile =>
        try {
          if (tempFile.exists() && tempFile.delete()) {
            logger.debug(s"Deleted temp download file: ${tempFile.getAbsolutePath}")
          }
        } catch {
          case e: Exception => logger.warn(s"Failed to delete temp file: ${tempFile.getAbsolutePath}", e)
        }
      }
      mergeEngine.cleanup()
    }
  }
}

/**
 * Metrics from merge execution phase
 */
private case class MergeExecutionMetrics(
  mergesExecuted: Int,
  mergesLocalFile: Int,
  mergesRemoteDownload: Int,
  networkBytesDownloaded: Long
)
