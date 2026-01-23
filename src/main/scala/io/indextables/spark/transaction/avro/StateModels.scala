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

package io.indextables.spark.transaction.avro

import io.indextables.spark.transaction.{AddAction, SchemaDeduplication}

/**
 * Data models for Avro-based state file format.
 *
 * The state file format provides fast reads (10x faster than JSON), incremental writes, streaming support, and
 * partition pruning for gigantic tables.
 */

/**
 * A file entry representing a split in the table state.
 *
 * Field IDs are assigned for Avro schema evolution:
 *   - 100-109: Basic file info
 *   - 110-119: Statistics
 *   - 120-129: Footer offsets
 *   - 130-139: Split metadata
 *   - 140-149: Streaming (addedAtVersion, addedAtTimestamp)
 *
 * @param path
 *   Relative path to the split file
 * @param partitionValues
 *   Partition column values as string map
 * @param size
 *   File size in bytes
 * @param modificationTime
 *   File modification time (epoch milliseconds)
 * @param dataChange
 *   Whether this file represents a data change
 * @param stats
 *   JSON-encoded statistics
 * @param minValues
 *   Minimum values per column for data skipping
 * @param maxValues
 *   Maximum values per column for data skipping
 * @param numRecords
 *   Number of records in the file
 * @param footerStartOffset
 *   Byte offset where footer/metadata begins
 * @param footerEndOffset
 *   Byte offset where footer/metadata ends
 * @param hasFooterOffsets
 *   Whether footer offsets are populated
 * @param splitTags
 *   Tags associated with this split
 * @param numMergeOps
 *   Number of merge operations this split has been through
 * @param docMappingRef
 *   Reference to doc mapping in schema registry
 * @param uncompressedSizeBytes
 *   Uncompressed size of the split data
 * @param addedAtVersion
 *   Transaction version when this file was added
 * @param addedAtTimestamp
 *   Timestamp when this file was added (epoch milliseconds)
 */
case class FileEntry(
    // Basic file info (100-109)
    path: String,
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    // Statistics (110-119)
    stats: Option[String] = None,
    minValues: Option[Map[String, String]] = None,
    maxValues: Option[Map[String, String]] = None,
    numRecords: Option[Long] = None,
    // Footer offsets (120-129)
    footerStartOffset: Option[Long] = None,
    footerEndOffset: Option[Long] = None,
    hasFooterOffsets: Boolean = false,
    // Split metadata (130-139)
    splitTags: Option[Set[String]] = None,
    numMergeOps: Option[Int] = None,
    docMappingRef: Option[String] = None,
    uncompressedSizeBytes: Option[Long] = None,
    // Streaming (140-149)
    addedAtVersion: Long,
    addedAtTimestamp: Long)
    extends Serializable

object FileEntry {

  /**
   * Convert an AddAction to a FileEntry.
   *
   * @param add
   *   The AddAction to convert
   * @param version
   *   Transaction version when this file was added
   * @param timestamp
   *   Timestamp when this file was added (epoch milliseconds)
   * @return
   *   FileEntry representation
   */
  def fromAddAction(add: AddAction, version: Long, timestamp: Long): FileEntry = {
    FileEntry(
      path = add.path,
      partitionValues = add.partitionValues,
      size = add.size,
      modificationTime = add.modificationTime,
      dataChange = add.dataChange,
      stats = add.stats,
      minValues = add.minValues,
      maxValues = add.maxValues,
      numRecords = add.numRecords,
      footerStartOffset = add.footerStartOffset,
      footerEndOffset = add.footerEndOffset,
      hasFooterOffsets = add.hasFooterOffsets,
      splitTags = add.splitTags,
      numMergeOps = add.numMergeOps,
      docMappingRef = add.docMappingRef,
      uncompressedSizeBytes = add.uncompressedSizeBytes,
      addedAtVersion = version,
      addedAtTimestamp = timestamp
    )
  }

  /**
   * Convert a FileEntry back to an AddAction.
   *
   * Note: Some AddAction fields (tags, timeRangeStart, timeRangeEnd, deleteOpstamp, hotcacheStartOffset,
   * hotcacheLength) are not preserved in the FileEntry format as they are less commonly used.
   *
   * @param entry
   *   The FileEntry to convert
   * @param schemaRegistry
   *   Optional schema registry for restoring docMappingJson from docMappingRef
   * @return
   *   AddAction representation
   */
  def toAddAction(entry: FileEntry, schemaRegistry: Map[String, String] = Map.empty): AddAction = {
    // Restore docMappingJson from schema registry if docMappingRef is present
    // Apply filterEmptyObjectMappings to handle always-null struct/array fields
    // (same fix as in SchemaDeduplication.restoreSchemas for JSON code path)
    val docMappingJson = entry.docMappingRef
      .flatMap(ref => schemaRegistry.get(ref))
      .map(SchemaDeduplication.filterEmptyObjectMappings)

    AddAction(
      path = entry.path,
      partitionValues = entry.partitionValues,
      size = entry.size,
      modificationTime = entry.modificationTime,
      dataChange = entry.dataChange,
      stats = entry.stats,
      minValues = entry.minValues,
      maxValues = entry.maxValues,
      numRecords = entry.numRecords,
      footerStartOffset = entry.footerStartOffset,
      footerEndOffset = entry.footerEndOffset,
      hasFooterOffsets = entry.hasFooterOffsets,
      splitTags = entry.splitTags,
      numMergeOps = entry.numMergeOps,
      docMappingRef = entry.docMappingRef,
      docMappingJson = docMappingJson,
      uncompressedSizeBytes = entry.uncompressedSizeBytes
    )
  }
}

/**
 * Partition bounds for a manifest, enabling partition pruning.
 *
 * @param min
 *   Minimum value for this partition column (None if all nulls)
 * @param max
 *   Maximum value for this partition column (None if all nulls)
 */
case class PartitionBounds(min: Option[String], max: Option[String]) extends Serializable

/**
 * Metadata about a manifest file within a state directory.
 *
 * @param path
 *   Relative path to the manifest file (e.g., "manifest-a1b2c3d4.avro")
 * @param numEntries
 *   Number of file entries in this manifest
 * @param minAddedAtVersion
 *   Minimum addedAtVersion across all entries
 * @param maxAddedAtVersion
 *   Maximum addedAtVersion across all entries
 * @param partitionBounds
 *   Optional partition bounds for partition pruning
 */
case class ManifestInfo(
    path: String,
    numEntries: Long,
    minAddedAtVersion: Long,
    maxAddedAtVersion: Long,
    partitionBounds: Option[Map[String, PartitionBounds]] = None)
    extends Serializable

/**
 * The state manifest (`_manifest.json`) that describes the complete table state.
 *
 * @param formatVersion
 *   Version of the state file format
 * @param stateVersion
 *   Transaction version this state represents
 * @param createdAt
 *   Timestamp when this state was created (epoch milliseconds)
 * @param numFiles
 *   Total number of live files (after applying tombstones)
 * @param totalBytes
 *   Total size of all live files in bytes
 * @param manifests
 *   List of manifest files containing file entries
 * @param tombstones
 *   List of paths that have been removed (applied during read)
 * @param schemaRegistry
 *   Schema registry for doc mapping deduplication
 * @param protocolVersion
 *   Protocol version (4 for Avro state format)
 */
case class StateManifest(
    formatVersion: Int,
    stateVersion: Long,
    createdAt: Long,
    numFiles: Long,
    totalBytes: Long,
    manifests: Seq[ManifestInfo],
    tombstones: Seq[String] = Seq.empty,
    schemaRegistry: Map[String, String] = Map.empty,
    protocolVersion: Int = 4)
    extends Serializable

/**
 * Result of reading a state, containing all live files and schema registry.
 *
 * @param version
 *   Transaction version this snapshot represents
 * @param files
 *   All live file entries (tombstones already applied)
 * @param schemaRegistry
 *   Schema registry for doc mapping restoration
 */
case class StateSnapshot(version: Long, files: Seq[FileEntry], schemaRegistry: Map[String, String] = Map.empty)
    extends Serializable

/**
 * Result of getting changes since a specific version (for streaming).
 *
 * @param adds
 *   Files added since the requested version
 * @param removes
 *   Files removed since the requested version
 * @param newVersion
 *   Current version after all changes
 */
case class ChangeSet(adds: Seq[FileEntry], removes: Seq[String], newVersion: Long) extends Serializable

/**
 * Configuration for state file operations.
 */
object StateConfig {

  /** Configuration key prefix */
  val PREFIX = "spark.indextables.state"

  // Format configuration
  val FORMAT_KEY = s"$PREFIX.format"
  val FORMAT_DEFAULT = "avro" // Avro is now the default (Phase 6)

  val COMPRESSION_KEY = s"$PREFIX.compression"
  val COMPRESSION_DEFAULT = "zstd"

  val COMPRESSION_LEVEL_KEY = s"$PREFIX.compressionLevel"
  val COMPRESSION_LEVEL_DEFAULT = 3

  val ENTRIES_PER_MANIFEST_KEY = s"$PREFIX.entriesPerManifest"
  val ENTRIES_PER_MANIFEST_DEFAULT = 50000

  // Compaction configuration
  val COMPACTION_TOMBSTONE_THRESHOLD_KEY = s"$PREFIX.compaction.tombstoneThreshold"
  val COMPACTION_TOMBSTONE_THRESHOLD_DEFAULT = 0.10

  val COMPACTION_MAX_MANIFESTS_KEY = s"$PREFIX.compaction.maxManifests"
  val COMPACTION_MAX_MANIFESTS_DEFAULT = 20

  val COMPACTION_AFTER_MERGE_KEY = s"$PREFIX.compaction.afterMerge"
  val COMPACTION_AFTER_MERGE_DEFAULT = true

  // Read configuration
  val READ_PARALLELISM_KEY = s"$PREFIX.read.parallelism"
  val READ_PARALLELISM_DEFAULT = 8

  // Retention configuration
  val RETENTION_VERSIONS_KEY = s"$PREFIX.retention.versions"
  val RETENTION_VERSIONS_DEFAULT = 2

  val RETENTION_HOURS_KEY = s"$PREFIX.retention.hours"
  val RETENTION_HOURS_DEFAULT = 168 // 7 days

  /** State format identifiers */
  object Format {
    val JSON = "json"
    val JSON_MULTIPART = "json-multipart"
    val AVRO_STATE = "avro-state"

    /** Check if a format is a legacy JSON format (deprecated) */
    def isJsonFormat(format: Option[String]): Boolean = format match {
      case Some(JSON) | Some(JSON_MULTIPART) | None => true
      case _                                        => false
    }
  }

  /** Deprecation message for JSON format */
  val JSON_FORMAT_DEPRECATION_WARNING =
    "JSON checkpoint format is deprecated and will be removed in a future release. " +
      "Run 'CHECKPOINT INDEXTABLES <path>' to upgrade to the Avro state format for 10x faster reads."

  /** Compression codec identifiers */
  object Compression {
    val ZSTD = "zstd"
    val SNAPPY = "snappy"
    val NONE = "none"
  }

  // Retry configuration for concurrent write conflicts
  val RETRY_MAX_ATTEMPTS_KEY = s"$PREFIX.retry.maxAttempts"
  val RETRY_MAX_ATTEMPTS_DEFAULT = 10

  val RETRY_BASE_DELAY_MS_KEY = s"$PREFIX.retry.baseDelayMs"
  val RETRY_BASE_DELAY_MS_DEFAULT = 100L

  val RETRY_MAX_DELAY_MS_KEY = s"$PREFIX.retry.maxDelayMs"
  val RETRY_MAX_DELAY_MS_DEFAULT = 5000L
}

/**
 * Configuration for state write retry behavior on concurrent conflicts.
 *
 * @param maxAttempts
 *   Maximum number of retry attempts (default: 10)
 * @param baseDelayMs
 *   Base delay in milliseconds for exponential backoff (default: 100)
 * @param maxDelayMs
 *   Maximum delay in milliseconds (default: 5000)
 */
case class StateRetryConfig(
    maxAttempts: Int = StateConfig.RETRY_MAX_ATTEMPTS_DEFAULT,
    baseDelayMs: Long = StateConfig.RETRY_BASE_DELAY_MS_DEFAULT,
    maxDelayMs: Long = StateConfig.RETRY_MAX_DELAY_MS_DEFAULT)

/**
 * Result of a state write operation.
 *
 * @param stateDir
 *   Path to the state directory that was written
 * @param version
 *   Transaction version of the written state
 * @param attempts
 *   Number of attempts taken (1 = no retry needed)
 * @param conflictDetected
 *   Whether a concurrent write conflict was detected
 */
case class StateWriteResult(
    stateDir: String,
    version: Long,
    attempts: Int,
    conflictDetected: Boolean)

/**
 * Exception thrown when state write fails after all retry attempts due to concurrent conflicts.
 *
 * @param message
 *   Error message describing the failure
 * @param lastAttemptedVersion
 *   The last version that was attempted
 * @param attempts
 *   Total number of attempts made
 */
class ConcurrentStateWriteException(message: String, val lastAttemptedVersion: Long, val attempts: Int)
    extends RuntimeException(message)
