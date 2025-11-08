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

import io.indextables.spark.io.{CloudStorageProvider, CloudStorageProviderFactory}
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

      // Phase 0: Staging uploads are now SYNCHRONOUS - already completed during write phase
      // The stagingAvailable flag has been set correctly by the executors
      logger.info(s"Processing ${stagedSplits.size} staged splits (synchronous upload mode)")

      // Validate staging succeeded for all splits
      val stagingFailures = stagedSplits.count(!_.stagingAvailable)
      if (stagingFailures > 0) {
        val failureRate = stagingFailures.toDouble / stagedSplits.size
        logger.warn(s"$stagingFailures/${stagedSplits.size} splits failed staging upload (${(failureRate * 100).toInt}%)")

        // If too many failures, abort
        val minSuccessRate = options.getOrDefault(
          "spark.indextables.mergeOnWrite.minStagingSuccessRate", "0.95"
        ).toDouble
        if (failureRate > (1.0 - minSuccessRate)) {
          throw new RuntimeException(
            s"Staging upload failure rate ${(failureRate * 100).toInt}% exceeds threshold " +
            s"${((1.0 - minSuccessRate) * 100).toInt}%"
          )
        }
      } else {
        logger.info(s"All ${stagedSplits.size} staging uploads succeeded")
      }

      val updatedSplits = stagedSplits

      metricsBuilder = metricsBuilder.copy(
        totalSplitsStaged = stagedSplits.size - stagingFailures,
        stagingUploadTimeMs = 0, // Now tracked during write phase
        stagingFailures = stagingFailures,
        stagingRetries = 0, // Now tracked during write phase
        networkBytesUploaded = 0 // Now tracked during write phase
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

      // FIX: Clean up after successful merge
      cleanupLocalStagingDirectory(updatedSplits)
      cleanupRemoteStagingFiles(updatedSplits, mergedSplitIds)

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
   * Clean up local staging files after merge completes
   *
   * CRITICAL: Only deletes the specific split files that were merged, NOT the entire directory.
   * Multiple concurrent merges may share the same merge-on-write-staging directory,
   * so we must only remove our own files.
   */
  private def cleanupLocalStagingDirectory(splits: Seq[StagedSplitInfo]): Unit = {
    try {
      // Delete only the specific files that were merged
      var filesDeleted = 0
      var filesFailed = 0

      splits.foreach { split =>
        val localFile = new File(split.localPath)
        if (localFile.exists()) {
          try {
            if (localFile.delete()) {
              filesDeleted += 1
              logger.debug(s"Deleted local split file: ${split.localPath}")
            } else {
              filesFailed += 1
              logger.warn(s"Failed to delete local split file: ${split.localPath}")
            }
          } catch {
            case e: Exception =>
              filesFailed += 1
              logger.warn(s"Exception deleting local split file: ${split.localPath}", e)
          }
        }
      }

      if (filesDeleted > 0) {
        logger.info(s"Cleaned up $filesDeleted local split files (${filesFailed} failures)")
      }

      // Note: We do NOT delete the merge-on-write-staging directory itself
      // because other concurrent merge operations may be using it
    } catch {
      case e: Exception =>
        logger.warn("Failed to clean up local staging files", e)
        // Don't fail the operation if cleanup fails
    }
  }

  /**
   * Clean up remote staging files after merge completes
   *
   * Deletes staging files from S3/cloud storage for splits that were successfully merged.
   * Promoted (unmerged) splits' staging files are kept as they may be needed for recovery.
   */
  private def cleanupRemoteStagingFiles(splits: Seq[StagedSplitInfo], mergedSplitIds: Set[String]): Unit = {
    val cloudProvider = CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)

    try {
      // Only delete staging files for splits that were merged
      val splitsToCleanup = splits.filter(s => mergedSplitIds.contains(s.uuid) && s.stagingAvailable)

      if (splitsToCleanup.isEmpty) {
        logger.info("No remote staging files to clean up")
        return
      }

      logger.info(s"Cleaning up ${splitsToCleanup.size} staging files from cloud storage...")

      var successCount = 0
      var failureCount = 0

      splitsToCleanup.foreach { split =>
        try {
          cloudProvider.deleteFile(split.stagingPath)
          successCount += 1
          logger.debug(s"Deleted staging file: ${split.stagingPath}")
        } catch {
          case e: Exception =>
            failureCount += 1
            logger.warn(s"Failed to delete staging file: ${split.stagingPath}", e)
            // Don't fail the operation if cleanup fails
        }
      }

      logger.info(s"Staging cleanup completed: $successCount deleted, $failureCount failed")

    } catch {
      case e: Exception =>
        logger.warn("Failed to clean up remote staging files", e)
        // Don't fail the operation if cleanup fails
    } finally {
      try {
        cloudProvider.close()
      } catch {
        case e: Exception =>
          logger.warn("Failed to close cloud provider during staging cleanup", e)
      }
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

    // DIAGNOSTIC: Log what Spark knows about executors
    logger.info("=" * 80)
    logger.info("EXECUTOR HOSTNAME DIAGNOSTICS")
    logger.info("=" * 80)
    val executorMemoryStatus = sparkSession.sparkContext.getExecutorMemoryStatus
    logger.info(s"Number of executors (including driver): ${executorMemoryStatus.size}")
    executorMemoryStatus.foreach { case (execId, (host, port)) =>
      logger.info(s"  Executor ID: $execId")
      logger.info(s"    Host (from BlockManagerId): $host")
      logger.info(s"    Port: $port")
    }

    // Try to get executor info from StatusTracker (may have more details)
    try {
      val executorInfos = sparkSession.sparkContext.statusTracker.getExecutorInfos
      logger.info(s"Executor infos from StatusTracker: ${executorInfos.length} total")
      executorInfos.foreach { info =>
        logger.info(s"  Host: ${info.host()}, Port: ${info.port()}, Running tasks: ${info.numRunningTasks()}")
      }
    } catch {
      case e: Exception =>
        logger.warn("Could not get executor info from StatusTracker", e)
    }
    logger.info("=" * 80)

    // Group merge groups by host, then distribute each host's groups across multiple partitions
    val groupsByHost = mergeGroups.groupBy(_.preferredHost)
    logger.info(s"Merge groups distributed across ${groupsByHost.size} hosts: " +
      groupsByHost.map { case (host, groups) => s"$host(${groups.size})" }.mkString(", "))

    // DIAGNOSTIC: Show what hostnames we're about to pass to makeRDD
    logger.info("=" * 80)
    logger.info("LOCALITY HINTS BEING PASSED TO makeRDD:")
    logger.info("=" * 80)
    groupsByHost.foreach { case (host, groups) =>
      logger.info(s"  Host: '$host' -> ${groups.size} merge groups")
    }
    logger.info("=" * 80)

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
      val actualExecutorId = org.apache.spark.SparkEnv.get.executorId

      // DIAGNOSTIC: Log where this merge task is actually running
      executorLogger.info("=" * 80)
      executorLogger.info("MERGE TASK EXECUTION LOCATION")
      executorLogger.info(s"  Actual hostname: $actualHost")
      executorLogger.info(s"  Actual executor ID: $actualExecutorId")
      executorLogger.info(s"  Task context available: ${org.apache.spark.TaskContext.get() != null}")
      if (org.apache.spark.TaskContext.get() != null) {
        val tc = org.apache.spark.TaskContext.get()
        executorLogger.info(s"  Task attempt ID: ${tc.taskAttemptId()}")
        executorLogger.info(s"  Partition ID: ${tc.partitionId()}")
      }
      executorLogger.info("=" * 80)

      iter.flatMap { mergeGroup =>
        // Log locality: check if we're running on the preferred host
        executorLogger.info(s"Processing merge group ${mergeGroup.groupId}:")
        executorLogger.info(s"  Preferred host: ${mergeGroup.preferredHost}")
        executorLogger.info(s"  Actual host: $actualHost")
        executorLogger.info(s"  Number of splits in group: ${mergeGroup.splits.size}")
        mergeGroup.splits.foreach { split =>
          executorLogger.info(s"    Split ${split.uuid}: workerHost=${split.workerHost}, executorId=${split.executorId}")
        }

        if (actualHost != mergeGroup.preferredHost) {
          executorLogger.warn(s"LOCALITY MISS: merge group for host ${mergeGroup.preferredHost} running on $actualHost")
        } else {
          executorLogger.info(s"LOCALITY HIT: merge group running on preferred host $actualHost")
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
            // FIX: Add diagnostic logging to help debug file availability issues
            logSplitAvailabilityDiagnostics(split, cloudProvider)

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

  /**
   * Log detailed diagnostics when a split is not available
   * Helps debug issues with local file cleanup or staging upload failures
   */
  private def logSplitAvailabilityDiagnostics(
    split: StagedSplitInfo,
    cloudProvider: CloudStorageProvider
  ): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    logger.error("=" * 80)
    logger.error("SPLIT AVAILABILITY DIAGNOSTICS")
    logger.error("=" * 80)
    logger.error(s"Split UUID: ${split.uuid}")
    logger.error(s"Split size: ${split.size / 1024 / 1024}MB")
    logger.error(s"Worker host: ${split.workerHost}")
    logger.error(s"Executor ID: ${split.executorId}")
    logger.error(s"Staging available flag: ${split.stagingAvailable}")
    logger.error("")

    // Check local file
    logger.error("LOCAL FILE DIAGNOSTICS:")
    logger.error(s"  Expected path: ${split.localPath}")
    val localFile = new java.io.File(split.localPath)
    logger.error(s"  File exists: ${localFile.exists()}")

    if (localFile.exists()) {
      logger.error(s"  File size: ${localFile.length() / 1024 / 1024}MB")
      logger.error(s"  Readable: ${localFile.canRead()}")
    } else {
      // Check parent directory
      val parentDir = localFile.getParentFile
      if (parentDir != null) {
        logger.error(s"  Parent dir: ${parentDir.getAbsolutePath}")
        logger.error(s"  Parent exists: ${parentDir.exists()}")

        if (parentDir.exists()) {
          val filesInDir = parentDir.listFiles()
          if (filesInDir != null) {
            logger.error(s"  Files in parent dir (${filesInDir.length} total):")
            filesInDir.take(10).foreach { f =>
              logger.error(s"    - ${f.getName} (${f.length() / 1024}KB)")
            }
            if (filesInDir.length > 10) {
              logger.error(s"    ... and ${filesInDir.length - 10} more files")
            }
          } else {
            logger.error("  Parent directory not readable or empty")
          }
        }
      }
    }

    logger.error("")
    logger.error("STAGING DIAGNOSTICS:")
    logger.error(s"  Expected path: ${split.stagingPath}")

    try {
      // Try to check if staging file exists
      val stagingExists = cloudProvider.exists(split.stagingPath)
      logger.error(s"  File exists in cloud: $stagingExists")

      if (!stagingExists) {
        // List files in staging directory to see what's actually there
        val stagingDir = split.stagingPath.substring(0, split.stagingPath.lastIndexOf('/'))
        logger.error(s"  Staging directory: $stagingDir")

        try {
          val filesInStaging = cloudProvider.listFiles(stagingDir, recursive = false)
          logger.error(s"  Files in staging dir (${filesInStaging.length} total):")
          filesInStaging.take(10).foreach { fileInfo =>
            logger.error(s"    - ${fileInfo.path.split('/').last} (${fileInfo.size / 1024}KB)")
          }
          if (filesInStaging.length > 10) {
            logger.error(s"    ... and ${filesInStaging.length - 10} more files")
          }
        } catch {
          case e: Exception =>
            logger.error(s"  Failed to list staging directory: ${e.getMessage}")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"  Failed to check staging file: ${e.getMessage}")
    }

    logger.error("")
    logger.error("POSSIBLE CAUSES:")
    if (!split.stagingAvailable) {
      logger.error("  1. Staging upload never completed or failed")
      logger.error("  2. stagingAvailable flag was not updated after async upload")
      logger.error("  3. Upload skipped due to size threshold (should not happen with fix)")
    }
    val localFile2 = new java.io.File(split.localPath)
    if (!localFile2.exists()) {
      logger.error("  4. Local temp directory was cleaned up prematurely")
      logger.error("  5. Wrong local path (check if using shared 'merge-on-write-staging' directory)")
      logger.error("  6. Task ran on different executor than the one that created the file")
    }
    logger.error("=" * 80)
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
