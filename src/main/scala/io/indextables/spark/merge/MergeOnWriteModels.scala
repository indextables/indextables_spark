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

/**
 * Serializable configuration for merge-on-write feature.
 * Can be safely passed from driver to executors.
 *
 * This replaces passing non-serializable objects like CloudStorageProvider.
 */
case class MergeOnWriteConfig(
  enabled: Boolean,
  stagingBasePath: String,
  serializedHadoopConfig: Map[String, String],
  serializedOptions: Map[String, String]
) extends Serializable

/**
 * Metadata for a split file staged during merge-on-write operation.
 * Contains all information needed to track split location and facilitate locality-aware merging.
 *
 * Addresses Gap #3 (Schema Evolution): includes schemaFingerprint for compatibility checking
 * Addresses Gap #2 (Partial Staging Failures): tracks stagingAvailable status
 * Addresses Gap #5 (Speculative Execution): includes taskAttemptId for conflict resolution
 */
case class StagedSplitInfo(
  uuid: String,
  taskAttemptId: Long,              // NEW: Spark task attempt ID for speculative execution handling
  workerHost: String,
  executorId: String,
  localPath: String,                // OBSOLETE in S3 staging mode - local files cleaned up after upload
  stagingPath: String,               // S3 staging location: e.g., s3://bucket/path/_staging/part-00001-abc123.split
  stagingAvailable: Boolean = false, // NEW: true if S3 staging upload succeeded (always true in S3 mode)
  size: Long,
  numRecords: Long,
  minValues: Map[String, String],
  maxValues: Map[String, String],
  footerStartOffset: Option[Long],
  footerEndOffset: Option[Long],
  partitionValues: Map[String, String],
  timeRangeStart: Option[String] = None,
  timeRangeEnd: Option[String] = None,
  splitTags: Option[Set[String]] = None,
  deleteOpstamp: Option[Long] = None,
  docMappingJson: Option[String] = None,
  uncompressedSizeBytes: Option[Long] = None,
  schemaFingerprint: String          // NEW: Schema hash for compatibility checking (Gap #3)
) extends Serializable

/**
 * Represents a group of splits to be merged together with locality preference.
 * The preferredHost indicates where these splits were originally created,
 * allowing Spark to schedule the merge task on that host for local file access.
 *
 * Addresses Gap #3 (Schema Evolution): validates schema compatibility within group
 */
case class LocalityAwareMergeGroup(
  groupId: String,
  preferredHost: String,
  splits: Seq[StagedSplitInfo],
  partition: Map[String, String],
  estimatedSize: Long,
  schemaFingerprint: String          // NEW: Schema fingerprint for validation (Gap #3)
) extends Serializable

/**
 * Result of a staging upload operation
 *
 * Addresses Gap #2 (Partial Staging Failures): provides detailed error tracking
 */
case class StagingResult(
  success: Boolean,
  stagingPath: String,
  bytesUploaded: Long,
  durationMs: Long = 0,              // NEW: Track upload duration for metrics
  error: Option[String] = None,
  retriesAttempted: Int = 0          // NEW: Track retry attempts (Gap #2)
) extends Serializable

/**
 * Metadata extracted from a merged split file
 */
case class MergedSplitMetadata(
  size: Long,
  numRecords: Long,
  minValues: Map[String, String],
  maxValues: Map[String, String],
  footerStartOffset: Option[Long],
  footerEndOffset: Option[Long],
  timeRangeStart: Option[String],
  timeRangeEnd: Option[String],
  splitTags: Option[Set[String]],
  deleteOpstamp: Option[Long],
  docMappingJson: Option[String],
  uncompressedSizeBytes: Option[Long]
) extends Serializable

/**
 * Comprehensive metrics for merge-on-write operations
 *
 * Addresses Gap #9 (Observability): provides detailed tracking and visibility
 */
case class MergeOnWriteMetrics(
  totalSplitsCreated: Int,
  totalSplitsStaged: Int,
  stagingUploadTimeMs: Long,
  stagingFailures: Int,
  stagingRetries: Int,               // NEW: Track retry attempts

  mergeGroupsCreated: Int,
  mergesExecuted: Int,
  mergesLocalFile: Int,              // Used local files
  mergesRemoteDownload: Int,         // Downloaded from staging
  mergeDurationMs: Long,
  mergeTimeoutOccurred: Boolean = false, // NEW: Track timeout events (Gap #6)

  splitsPromoted: Int,               // Unmerged
  finalSplitCount: Int,

  networkBytesUploaded: Long,        // Staging + final uploads
  networkBytesDownloaded: Long,      // Remote merge downloads
  localityHitRate: Double,           // mergesLocalFile / mergesExecuted

  diskSpaceExhausted: Boolean = false,   // NEW: Track disk space issues (Gap #2)
  memoryPressureDetected: Boolean = false, // NEW: Track memory issues (Gap #3)
  dynamicAllocationDetected: Boolean = false, // NEW: Track dynamic allocation (Gap #1)

  cleanupDurationMs: Long,
  totalDurationMs: Long = 0
) extends Serializable {

  /**
   * Convert metrics to JSON string for structured logging
   */
  def toJson: String = {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this)
  }

  /**
   * Create a summary string for logging
   */
  def summary: String = {
    s"""
       |Merge-on-Write Summary:
       |  Splits Created: $totalSplitsCreated
       |  Splits Staged: $totalSplitsStaged (failures: $stagingFailures, retries: $stagingRetries)
       |  Merge Groups: $mergeGroupsCreated
       |  Merges Executed: $mergesExecuted (local: $mergesLocalFile, remote: $mergesRemoteDownload)
       |  Locality Hit Rate: ${(localityHitRate * 100).formatted("%.1f")}%
       |  Splits Promoted (unmerged): $splitsPromoted
       |  Final Splits: $finalSplitCount
       |  Network I/O: Uploaded ${networkBytesUploaded / 1024 / 1024}MB, Downloaded ${networkBytesDownloaded / 1024 / 1024}MB
       |  Total Duration: ${totalDurationMs / 1000}s (staging: ${stagingUploadTimeMs / 1000}s, merge: ${mergeDurationMs / 1000}s, cleanup: ${cleanupDurationMs / 1000}s)
       |  Issues: ${if (diskSpaceExhausted) "DISK_SPACE " else ""}${if (memoryPressureDetected) "MEMORY " else ""}${if (mergeTimeoutOccurred) "TIMEOUT " else ""}${if (dynamicAllocationDetected) "DYNAMIC_ALLOC " else ""}
       """.stripMargin.trim
  }
}
