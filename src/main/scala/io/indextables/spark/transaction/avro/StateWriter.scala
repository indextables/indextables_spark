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

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.AddAction

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Writer for Avro state files with support for incremental and compacted writes.
 *
 * This writer supports:
 *   - Incremental writes: Reuse existing manifests, add new manifest for new files
 *   - Compacted writes: Full rewrite with partition sorting for optimal pruning
 *   - Automatic compaction detection based on tombstone ratio and manifest count
 *   - Partition bounds computation for partition pruning
 */
class StateWriter(
    cloudProvider: CloudStorageProvider,
    transactionLogPath: String,
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT,
    entriesPerManifest: Int = StateConfig.ENTRIES_PER_MANIFEST_DEFAULT) {

  private val log = LoggerFactory.getLogger(getClass)

  private val manifestIO = StateManifestIO(cloudProvider)
  private val manifestWriter = AvroManifestWriter(cloudProvider)
  private val manifestReader = AvroManifestReader(cloudProvider)

  /**
   * Write a new state, either incrementally or compacted.
   *
   * @param currentVersion
   *   Transaction version this state represents
   * @param newFiles
   *   New file entries to add (already converted to FileEntry)
   * @param removedPaths
   *   Paths of files that were removed
   * @param schemaRegistry
   *   Schema registry for doc mapping deduplication
   * @param forceCompaction
   *   Force full compaction even if not needed
   * @return
   *   The new state directory path
   */
  def writeState(
      currentVersion: Long,
      newFiles: Seq[FileEntry],
      removedPaths: Set[String],
      schemaRegistry: Map[String, String] = Map.empty,
      forceCompaction: Boolean = false): String = {

    val newStateDir = s"$transactionLogPath/${manifestIO.formatStateDir(currentVersion)}"

    // Check if there's an existing state to build upon
    val existingState = findLatestState()

    existingState match {
      case Some((oldStateDir, oldManifest)) =>
        // Decide whether to compact or append
        val shouldCompact = forceCompaction || needsCompaction(oldManifest, removedPaths.size)

        if (shouldCompact) {
          log.info(s"Writing compacted state for version $currentVersion")
          writeCompactedState(newStateDir, currentVersion, newFiles, removedPaths, schemaRegistry)
        } else {
          log.info(s"Writing incremental state for version $currentVersion")
          writeIncrementalState(newStateDir, oldStateDir, oldManifest, newFiles, removedPaths, currentVersion, schemaRegistry)
        }

      case None =>
        // No existing state - write initial state
        log.info(s"Writing initial state for version $currentVersion")
        writeInitialState(newStateDir, newFiles, currentVersion, schemaRegistry)
    }

    newStateDir
  }

  /**
   * Write an incremental state that reuses existing manifests.
   */
  private def writeIncrementalState(
      newStateDir: String,
      oldStateDir: String,
      oldManifest: StateManifest,
      newFiles: Seq[FileEntry],
      removedPaths: Set[String],
      currentVersion: Long,
      schemaRegistry: Map[String, String]): Unit = {

    cloudProvider.createDirectory(newStateDir)

    // Start with existing manifests
    var newManifests = oldManifest.manifests

    // Write new manifest if there are new files
    if (newFiles.nonEmpty) {
      val manifestId = manifestWriter.generateManifestId()
      val manifestPath = s"manifest-$manifestId.avro"
      val fullManifestPath = s"$newStateDir/$manifestPath"

      manifestWriter.writeManifest(fullManifestPath, newFiles, compression, compressionLevel)

      val manifestInfo = manifestWriter.createManifestInfo(manifestPath, newFiles)
      newManifests = newManifests :+ manifestInfo
    }

    // Merge tombstones
    val newTombstones = oldManifest.tombstones ++ removedPaths

    // Calculate live file count (need to read existing entries to count)
    val totalEntriesInManifests = newManifests.map(_.numEntries).sum
    val numLiveFiles = totalEntriesInManifests - newTombstones.size

    // Calculate total bytes (estimate - would need to read all manifests for exact)
    val totalBytes = oldManifest.totalBytes + newFiles.map(_.size).sum

    // Merge schema registries
    val mergedRegistry = oldManifest.schemaRegistry ++ schemaRegistry

    // Write new manifest
    val newManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = numLiveFiles,
      totalBytes = totalBytes,
      manifests = newManifests,
      tombstones = newTombstones.toSeq,
      schemaRegistry = mergedRegistry,
      protocolVersion = 4
    )

    manifestIO.writeStateManifest(newStateDir, newManifest)

    log.info(
      s"Wrote incremental state: version=$currentVersion, newFiles=${newFiles.size}, " +
        s"newTombstones=${removedPaths.size}, totalManifests=${newManifests.size}")
  }

  /**
   * Write a compacted state that rewrites all live files with partition sorting.
   *
   * @param newStateDir
   *   Directory to write the new state to
   * @param currentVersion
   *   Transaction version
   * @param newFiles
   *   New files to add (will be included after compaction)
   * @param removedPaths
   *   Paths of files to remove (will be filtered out during compaction)
   * @param schemaRegistry
   *   Schema registry for doc mapping deduplication
   */
  def writeCompactedState(
      newStateDir: String,
      currentVersion: Long,
      newFiles: Seq[FileEntry] = Seq.empty,
      removedPaths: Set[String] = Set.empty,
      schemaRegistry: Map[String, String]): Unit = {

    cloudProvider.createDirectory(newStateDir)

    // Read all live files from current state
    val existingState = findLatestState()
    val existingFiles = existingState match {
      case Some((oldStateDir, oldManifest)) =>
        val manifestPaths = oldManifest.manifests.map(m => s"$oldStateDir/${m.path}")
        val allEntries = manifestReader.readManifestsParallel(manifestPaths)
        manifestIO.applyTombstones(allEntries, oldManifest.tombstones)
      case None =>
        Seq.empty[FileEntry]
    }

    // Combine existing files with new files, then filter out removed paths
    val combinedFiles = existingFiles ++ newFiles
    val liveFiles = if (removedPaths.isEmpty) {
      combinedFiles
    } else {
      combinedFiles.filterNot(f => removedPaths.contains(f.path))
    }

    writeCompactedStateFromFiles(newStateDir, liveFiles, currentVersion, schemaRegistry)
  }

  /**
   * Write a compacted state from a list of live files.
   *
   * This is used both for compaction and for initial checkpoint creation from AddActions.
   */
  def writeCompactedStateFromFiles(
      newStateDir: String,
      liveFiles: Seq[FileEntry],
      currentVersion: Long,
      schemaRegistry: Map[String, String]): Unit = {

    if (!cloudProvider.exists(newStateDir)) {
      cloudProvider.createDirectory(newStateDir)
    }

    // Sort files by partition values for locality
    val sortedFiles = liveFiles.sortBy { entry =>
      entry.partitionValues.toSeq.sorted.map(_._2).mkString("|")
    }

    // Partition into manifest chunks
    val manifestChunks = if (sortedFiles.isEmpty) {
      Seq(Seq.empty[FileEntry])
    } else {
      sortedFiles.grouped(entriesPerManifest).toSeq
    }

    // Write new manifests with partition bounds
    val newManifests = manifestChunks.zipWithIndex.map { case (chunk, idx) =>
      val manifestId = manifestWriter.generateManifestId()
      val manifestPath = s"manifest-$manifestId.avro"
      val fullManifestPath = s"$newStateDir/$manifestPath"

      if (chunk.nonEmpty) {
        manifestWriter.writeManifest(fullManifestPath, chunk, compression, compressionLevel)
      } else {
        // Write empty manifest for consistency
        manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
      }

      manifestWriter.createManifestInfo(manifestPath, chunk)
    }

    // Write clean manifest (no tombstones after compaction)
    val newManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = liveFiles.size,
      totalBytes = liveFiles.map(_.size).sum,
      manifests = newManifests,
      tombstones = Seq.empty, // Clean!
      schemaRegistry = schemaRegistry,
      protocolVersion = 4
    )

    manifestIO.writeStateManifest(newStateDir, newManifest)

    log.info(
      s"Wrote compacted state: version=$currentVersion, files=${liveFiles.size}, " +
        s"manifests=${newManifests.size}, tombstones=0")
  }

  /**
   * Write an initial state (no existing state to build upon).
   */
  private def writeInitialState(
      newStateDir: String,
      files: Seq[FileEntry],
      currentVersion: Long,
      schemaRegistry: Map[String, String]): Unit = {

    // Initial state is always compacted (sorted by partition)
    writeCompactedStateFromFiles(newStateDir, files, currentVersion, schemaRegistry)
  }

  /**
   * Determine if compaction is needed based on tombstone ratio and manifest count.
   */
  def needsCompaction(manifest: StateManifest, newRemoves: Int): Boolean = {
    val totalTombstones = manifest.tombstones.size + newRemoves
    val estimatedLiveFiles = manifest.numFiles - newRemoves

    // Avoid division by zero
    if (estimatedLiveFiles <= 0) {
      return true // Compact if no files left
    }

    val tombstoneRatio = totalTombstones.toDouble / estimatedLiveFiles
    val manifestCount = manifest.manifests.size

    // Compact when:
    // 1. Tombstones exceed threshold (default 10%)
    val tombstoneThresholdExceeded = tombstoneRatio > StateConfig.COMPACTION_TOMBSTONE_THRESHOLD_DEFAULT

    // 2. Too many manifests (fragmentation)
    val tooManyManifests = manifestCount > StateConfig.COMPACTION_MAX_MANIFESTS_DEFAULT

    // 3. Large number of removes in single operation (e.g., after merge)
    val largeRemoveOperation = newRemoves > 1000

    val shouldCompact = tombstoneThresholdExceeded || tooManyManifests || largeRemoveOperation

    if (shouldCompact) {
      log.debug(
        s"Compaction needed: tombstoneRatio=$tombstoneRatio (threshold=${StateConfig.COMPACTION_TOMBSTONE_THRESHOLD_DEFAULT}), " +
          s"manifestCount=$manifestCount (max=${StateConfig.COMPACTION_MAX_MANIFESTS_DEFAULT}), " +
          s"newRemoves=$newRemoves")
    }

    shouldCompact
  }

  /**
   * Find the latest state directory and its manifest.
   */
  private def findLatestState(): Option[(String, StateManifest)] = {
    Try {
      // List all files recursively and look for _manifest.json files in state directories
      val files = cloudProvider.listFiles(transactionLogPath, recursive = true)
      val manifestFiles = files
        .filter(f => f.path.endsWith("/_manifest.json"))
        .filter(f => f.path.contains("/state-v"))

      if (manifestFiles.isEmpty) {
        return None
      }

      // Extract state directory paths from manifest file paths
      val stateDirs = manifestFiles.map { f =>
        // Remove /_manifest.json from the path to get the state directory
        f.path.stripSuffix("/_manifest.json")
      }.filter(p => manifestIO.parseStateDirVersion(p).isDefined)

      if (stateDirs.isEmpty) {
        return None
      }

      // Find the one with the highest version
      val latestStateDir = stateDirs.maxBy(p => manifestIO.parseStateDirVersion(p).getOrElse(0L))

      // Read its manifest
      if (manifestIO.stateExists(latestStateDir)) {
        val manifest = manifestIO.readStateManifest(latestStateDir)
        Some((latestStateDir, manifest))
      } else {
        None
      }
    } match {
      case Success(result) => result
      case Failure(e) =>
        log.debug(s"No existing state found: ${e.getMessage}")
        None
    }
  }

  /**
   * Convert AddActions to FileEntries for writing.
   */
  def convertAddActionsToFileEntries(
      adds: Seq[AddAction],
      version: Long,
      timestamp: Long = System.currentTimeMillis()): Seq[FileEntry] = {
    adds.map(add => FileEntry.fromAddAction(add, version, timestamp))
  }
}

object StateWriter {

  /**
   * Create a state writer for the given cloud provider and transaction log path.
   *
   * @param cloudProvider
   *   Cloud storage provider for file access
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @param compression
   *   Compression codec to use (default: zstd)
   * @param compressionLevel
   *   Compression level (default: 3)
   * @param entriesPerManifest
   *   Maximum entries per manifest file (default: 50000)
   * @return
   *   StateWriter instance
   */
  def apply(
      cloudProvider: CloudStorageProvider,
      transactionLogPath: String,
      compression: String = StateConfig.COMPRESSION_DEFAULT,
      compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT,
      entriesPerManifest: Int = StateConfig.ENTRIES_PER_MANIFEST_DEFAULT): StateWriter = {
    new StateWriter(cloudProvider, transactionLogPath, compression, compressionLevel, entriesPerManifest)
  }
}
