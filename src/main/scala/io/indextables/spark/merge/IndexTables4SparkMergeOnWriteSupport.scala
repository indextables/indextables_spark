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

import java.io.File
import java.security.MessageDigest
import java.util.UUID

import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.TaskContext

import org.apache.hadoop.conf.Configuration

import io.indextables.tantivy4java.split.merge.QuickwitSplit

import io.indextables.spark.search.TantivySearchEngine
import io.indextables.spark.storage.SplitCacheConfig
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.StatisticsCalculator
import org.slf4j.LoggerFactory

/**
 * Extended commit message that can carry either traditional AddActions or StagedSplitInfo for merge-on-write
 */
case class IndexTables4SparkMergeOnWriteCommitMessage(
  addActions: Seq[AddAction] = Seq.empty,
  stagedSplits: Seq[StagedSplitInfo] = Seq.empty
) extends WriterCommitMessage

/**
 * Helper methods to support merge-on-write functionality in IndexTables4SparkDataWriter
 *
 * ARCHITECTURE: Shuffle-based merge
 * - Write phase: Creates splits and keeps bytes in RDD for shuffle
 * - Merge phase: Spark shuffle distributes split bytes to merge executors
 * - No local file persistence issues (100% locality via shuffle)
 */
object MergeOnWriteHelper {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a local split file for merge-on-write mode
   *
   * Creates the split in a local temp directory for shuffle-based merge.
   * No S3 staging - Spark shuffle provides durability.
   *
   * @param searchEngine The TantivySearchEngine with indexed data
   * @param writeSchema Schema for computing fingerprint
   * @param statistics Statistics collected during write
   * @param recordCount Number of records written
   * @param partitionValues Partition values (if partitioned table)
   * @param partitionId Spark partition ID
   * @param taskId Spark task ID
   * @param options Configuration options
   * @param hadoopConf Hadoop configuration
   * @param stagingUploader Uploader for async staging
   * @param stagingBasePath Base path for staging files
   * @return StagedSplitInfo with split metadata and staging information
   */
  def createLocalSplitForMergeOnWrite(
    searchEngine: TantivySearchEngine,
    writeSchema: StructType,
    statistics: StatisticsCalculator.DatasetStatistics,
    recordCount: Long,
    partitionValues: Map[String, String],
    partitionId: Int,
    taskId: Long,
    options: CaseInsensitiveStringMap,
    hadoopConf: Configuration
  ): StagedSplitInfo = {

    // Get or create local temp directory
    val localTempDir = SplitCacheConfig.getDefaultTempPath()
      .getOrElse(System.getProperty("java.io.tmpdir"))

    // Generate unique split ID and filename
    val splitUuid = UUID.randomUUID().toString
    val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(0L)
    val fileName = f"part-$partitionId%05d-$taskId-$splitUuid.split"

    // CRITICAL FIX: Create split to a tantivy-controlled temp location first
    // Tantivy4java may delete files in its staging locations, so we can't trust them to persist
    val tantivyTempDir = new File(localTempDir, s"tantivy-split-creation-$splitUuid")
    tantivyTempDir.mkdirs()
    val tantivyTempSplitPath = new File(tantivyTempDir, fileName).getAbsolutePath

    // Our local temp directory (separate from tantivy's temp location)
    // Files here persist until shuffle-based merge reads them and they're uploaded to final location
    val mergeOnWriteStagingDir = new File(localTempDir, "merge-on-write-staging")
    mergeOnWriteStagingDir.mkdirs()
    val finalLocalPath = new File(mergeOnWriteStagingDir, fileName).getAbsolutePath

    // Get worker identification
    val workerHost = java.net.InetAddress.getLocalHost.getHostName

    // Get executor ID from SparkEnv (must be available during write phase)
    val sparkEnv = org.apache.spark.SparkEnv.get
    if (sparkEnv == null) {
      throw new IllegalStateException(
        "SparkEnv is null - createLocalSplitForMergeOnWrite must be called from executor context"
      )
    }
    val executorId = sparkEnv.executorId

    // Generate node ID for the split
    val nodeId = s"$workerHost-$executorId"

    // DIAGNOSTIC: Log hostname and executor details
    logger.info("=" * 80)
    logger.info("SPLIT CREATION HOSTNAME DIAGNOSTICS")
    logger.info(s"  InetAddress.getLocalHost.getHostName: $workerHost")
    logger.info(s"  InetAddress.getLocalHost.getHostAddress: ${java.net.InetAddress.getLocalHost.getHostAddress}")
    logger.info(s"  InetAddress.getLocalHost.getCanonicalHostName: ${java.net.InetAddress.getLocalHost.getCanonicalHostName}")
    logger.info(s"  SparkEnv.get.executorId: $executorId")
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      logger.info(s"  TaskContext.taskAttemptId(): ${taskContext.taskAttemptId()}")
      logger.info(s"  TaskContext.partitionId(): ${taskContext.partitionId()}")
    }
    logger.info(s"  Node ID: $nodeId")
    logger.info("=" * 80)

    logger.info(s"Creating local split in tantivy temp location: $tantivyTempSplitPath (worker: $workerHost, executor: $executorId)")

    // Create split from the index using the search engine (in tantivy-controlled temp location)
    val (tantivySplitPath, splitMetadata) = searchEngine.commitAndCreateSplit(tantivyTempSplitPath, partitionId.toLong, nodeId)

    // CRITICAL: Immediately copy the split to our local temp directory
    // This protects against tantivy4java deleting the file during cleanup
    logger.info(s"Copying split from tantivy temp to local merge staging: $tantivySplitPath → $finalLocalPath")
    val tantivySplitFile = new File(tantivySplitPath)
    val finalLocalFile = new File(finalLocalPath)

    // Use NIO for efficient file copy
    java.nio.file.Files.copy(
      tantivySplitFile.toPath,
      finalLocalFile.toPath,
      java.nio.file.StandardCopyOption.REPLACE_EXISTING
    )

    val splitSize = finalLocalFile.length()
    logger.info(s"Split copied to permanent location: $finalLocalPath (${splitSize / 1024 / 1024}MB, $recordCount records)")

    // Clean up tantivy temp directory now that we have our copy
    try {
      tantivySplitFile.delete()
      tantivyTempDir.delete()
      logger.debug(s"Cleaned up tantivy temp directory: $tantivyTempDir")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to clean up tantivy temp directory: ${e.getMessage}")
    }

    // Extract metadata from split
    val rawMinValues = statistics.getMinValues
    val rawMaxValues = statistics.getMaxValues

    // Apply statistics truncation
    import io.indextables.spark.util.StatisticsTruncation
    import scala.jdk.CollectionConverters._
    val configMap = options.asCaseSensitiveMap().asScala.toMap
    val (minValues, maxValues) = StatisticsTruncation.truncateStatistics(
      rawMinValues,
      rawMaxValues,
      configMap
    )

    // Extract complete metadata from tantivy4java
    val (
      footerStartOffset,
      footerEndOffset,
      timeRangeStart,
      timeRangeEnd,
      splitTags,
      deleteOpstamp,
      docMappingJson,
      uncompressedSizeBytes
    ) = extractSplitMetadata(splitMetadata, writeSchema)

    // Compute schema fingerprint for compatibility checking (Gap #3)
    val schemaFingerprint = computeSchemaFingerprint(writeSchema)

    // No S3 staging needed - Spark shuffle provides durability
    // The split file is already persisted locally in merge-on-write-staging directory
    logger.info(s"Split ready for shuffle-based merge (no S3 staging needed): $finalLocalPath")

    // Build and return StagedSplitInfo
    // Note: stagingPath and stagingAvailable are not used in shuffle-based merge
    StagedSplitInfo(
      uuid = splitUuid,
      taskAttemptId = taskAttemptId,
      workerHost = workerHost,
      executorId = executorId,
      localPath = finalLocalPath,  // Split persisted in merge-on-write-staging directory
      stagingPath = "",  // Not used in shuffle-based merge
      stagingAvailable = false,  // Not used in shuffle-based merge
      size = splitSize,
      numRecords = recordCount,
      minValues = minValues,
      maxValues = maxValues,
      footerStartOffset = footerStartOffset,
      footerEndOffset = footerEndOffset,
      partitionValues = partitionValues,
      timeRangeStart = timeRangeStart,
      timeRangeEnd = timeRangeEnd,
      splitTags = splitTags,
      deleteOpstamp = deleteOpstamp,
      docMappingJson = docMappingJson,
      uncompressedSizeBytes = uncompressedSizeBytes,
      schemaFingerprint = schemaFingerprint
    )
  }

  /**
   * Extract metadata from tantivy4java SplitMetadata
   */
  private def extractSplitMetadata(
    splitMetadata: QuickwitSplit.SplitMetadata,
    writeSchema: StructType
  ): (
    Option[Long],      // footerStartOffset
    Option[Long],      // footerEndOffset
    Option[String],    // timeRangeStart
    Option[String],    // timeRangeEnd
    Option[Set[String]], // splitTags
    Option[Long],      // deleteOpstamp
    Option[String],    // docMappingJson
    Option[Long]       // uncompressedSizeBytes
  ) = {
    if (splitMetadata != null) {
      val timeStart = Option(splitMetadata.getTimeRangeStart()).map(_.toString)
      val timeEnd = Option(splitMetadata.getTimeRangeEnd()).map(_.toString)
      val tags = Option(splitMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
        import scala.jdk.CollectionConverters._
        tagSet.asScala.toSet
      }

      val originalDocMapping = Option(splitMetadata.getDocMappingJson())
      val docMapping = if (originalDocMapping.isDefined) {
        originalDocMapping
      } else {
        // WORKAROUND: Create minimal schema mapping if missing
        logger.warn("tantivy4java docMappingJson missing - creating minimal field mapping")
        val fieldMappings = writeSchema.fields
          .map { field =>
            val fieldType = field.dataType.typeName match {
              case "string" => "text"
              case "integer" | "long" => "i64"
              case "float" | "double" => "f64"
              case "boolean" => "bool"
              case "date" | "timestamp" => "datetime"
              case _ => "text"
            }
            s""""${field.name}": {"type": "$fieldType", "indexed": true}"""
          }
          .mkString(", ")
        Some(s"""{"fields": {$fieldMappings}}""")
      }

      if (splitMetadata.hasFooterOffsets()) {
        (
          Some(splitMetadata.getFooterStartOffset()),
          Some(splitMetadata.getFooterEndOffset()),
          timeStart,
          timeEnd,
          tags,
          Some(splitMetadata.getDeleteOpstamp()),
          docMapping,
          Some(splitMetadata.getUncompressedSizeBytes())
        )
      } else {
        (
          None,
          None,
          timeStart,
          timeEnd,
          tags,
          Some(splitMetadata.getDeleteOpstamp()),
          docMapping,
          Some(splitMetadata.getUncompressedSizeBytes())
        )
      }
    } else {
      (None, None, None, None, None, None, None, None)
    }
  }

  /**
   * Compute schema fingerprint for compatibility checking (Gap #3)
   */
  private def computeSchemaFingerprint(schema: StructType): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val schemaJson = schema.json
    val hash = digest.digest(schemaJson.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString
  }

  /**
   * Create ShuffledSplitData for RDD-based merge (reads split file into memory)
   *
   * This method:
   * 1. Reads the split file bytes from finalLocalPath
   * 2. Uploads to S3 for durability (in case shuffle fails)
   * 3. Returns ShuffledSplitData with bytes for RDD shuffle
   *
   * @param splitInfo Split metadata
   * @param finalLocalPath Path to split file on local disk
   * @param stagingUploader Uploader for S3 durability
   * @return ShuffledSplitData with split bytes
   */
  def createShuffledSplitData(
    splitInfo: StagedSplitInfo,
    finalLocalPath: String,
    stagingUploader: SplitStagingUploader
  ): ShuffledSplitData = {

    // Read split bytes into memory
    val splitFile = new File(finalLocalPath)
    val splitBytes = java.nio.file.Files.readAllBytes(splitFile.toPath)

    logger.info(s"Read split bytes for shuffle: ${splitBytes.length / 1024 / 1024}MB from $finalLocalPath")

    // No S3 upload needed - Spark shuffle provides durability
    // The final merged splits will be uploaded to S3 after merge completes
    logger.info(s"Skip S3 upload - using Spark shuffle for durability")

    // Return shuffled data
    ShuffledSplitData(
      info = splitInfo,
      bytes = splitBytes
    )
  }

  /**
   * Check if merge-on-write is enabled in options
   */
  def isMergeOnWriteEnabled(options: CaseInsensitiveStringMap): Boolean = {
    options.getOrDefault("spark.indextables.mergeOnWrite.enabled", "false").toBoolean
  }

  /**
   * Get staging base path from options or construct from table path
   */
  def getStagingBasePath(tablePath: String, options: CaseInsensitiveStringMap): String = {
    val stagingDir = options.getOrDefault("spark.indextables.mergeOnWrite.stagingDir", "_staging")
    s"$tablePath/$stagingDir"
  }
}
