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
 * This provides the logic to create splits on local disk and stage them asynchronously
 * instead of immediately uploading to cloud storage.
 */
object MergeOnWriteHelper {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a local split file for merge-on-write mode
   *
   * Instead of uploading directly to cloud storage, creates the split in a local temp directory
   * and initiates async staging upload.
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
    hadoopConf: Configuration,
    stagingUploader: SplitStagingUploader,
    stagingBasePath: String
  ): StagedSplitInfo = {

    // Get or create local temp directory
    val localTempDir = SplitCacheConfig.getDefaultTempPath()
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val workingDir = new File(localTempDir, s"merge-on-write-${UUID.randomUUID()}")
    workingDir.mkdirs()

    // Generate unique split ID and filename
    val splitUuid = UUID.randomUUID().toString
    val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(0L)
    val fileName = f"part-$partitionId%05d-$taskId-$splitUuid.split"

    // Create local split file path
    val localSplitPath = new File(workingDir, fileName).getAbsolutePath

    // Get worker identification
    val workerHost = java.net.InetAddress.getLocalHost.getHostName
    val executorId = Option(System.getProperty("spark.executor.id")).getOrElse("driver")

    // Generate node ID for the split
    val nodeId = s"$workerHost-$executorId"

    logger.info(s"Creating local split for merge-on-write: $localSplitPath (worker: $workerHost, executor: $executorId)")

    // Create split from the index using the search engine
    val (splitPath, splitMetadata) = searchEngine.commitAndCreateSplit(localSplitPath, partitionId.toLong, nodeId)

    // Get split file size
    val splitFile = new File(splitPath)
    val splitSize = splitFile.length()

    logger.info(s"Created local split: $splitPath (${splitSize / 1024 / 1024}MB, $recordCount records)")

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

    // Create staging path with task attempt ID (Gap #5: speculative execution handling)
    val stagingPath = s"$stagingBasePath/task-$taskAttemptId-$splitUuid.tmp"

    // Build StagedSplitInfo
    val stagedSplit = StagedSplitInfo(
      uuid = splitUuid,
      taskAttemptId = taskAttemptId,
      workerHost = workerHost,
      executorId = executorId,
      localPath = splitPath,
      stagingPath = stagingPath,
      stagingAvailable = false, // Will be updated after staging completes
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

    // Initiate async staging upload (Gap #2: async with retry support)
    logger.info(s"Initiating async staging upload: $splitPath → $stagingPath")
    stagingUploader.stageAsync(splitUuid, splitPath, stagedSplit)

    stagedSplit
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
