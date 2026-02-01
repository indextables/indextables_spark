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

import scala.util.{Failure, Success, Try}
import scala.util.Random

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

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
  entriesPerManifest: Int = StateConfig.ENTRIES_PER_MANIFEST_DEFAULT,
  retryConfig: StateRetryConfig = StateRetryConfig()) {

  private val log = LoggerFactory.getLogger(getClass)

  private val manifestIO     = StateManifestIO(cloudProvider)
  private val manifestWriter = AvroManifestWriter(cloudProvider)
  private val manifestReader = AvroManifestReader(cloudProvider)
  private val random         = new Random()

  /**
   * Write a new state with automatic retry.
   *
   * This is a convenience method that delegates to writeIncrementalWithRetry.
   *
   * @param currentVersion
   *   Transaction version (used as starting point, may be incremented on conflict)
   * @param newFiles
   *   New file entries to add
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
    forceCompaction: Boolean = false
  ): String = {

    val compactionConfig = if (forceCompaction) {
      CompactionConfig(forceCompaction = true)
    } else {
      CompactionConfig()
    }

    val result = writeIncrementalWithRetry(
      newFiles = newFiles,
      removedPaths = removedPaths,
      schemaRegistry = schemaRegistry,
      compactionConfig = compactionConfig,
      minVersion = Some(currentVersion)
    )

    result.stateDir
  }

  /**
   * Write a new state with automatic retry on concurrent write conflict.
   *
   * This method handles version assignment and retry logic internally. If a concurrent writer creates the same state
   * version, this method will increment the version and retry.
   *
   * @param currentVersion
   *   Initial transaction version to attempt
   * @param liveFiles
   *   All live file entries (for compacted state)
   * @param schemaRegistry
   *   Schema registry for doc mapping deduplication
   * @param metadata
   *   Optional JSON-encoded MetadataAction for fast getMetadata() in read path
   * @return
   *   StateWriteResult containing the written state directory, actual version, and retry info
   * @throws ConcurrentStateWriteException
   *   if all retry attempts fail due to concurrent conflicts
   */
  def writeStateWithRetry(
    currentVersion: Long,
    liveFiles: Seq[FileEntry],
    schemaRegistry: Map[String, String],
    metadata: Option[String] = None
  ): StateWriteResult = {

    var attempt             = 1
    var version             = currentVersion
    var lastConflictVersion = 0L

    while (attempt <= retryConfig.maxAttempts) {
      val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(version)}"

      log.debug(s"Attempting to write state at version $version (attempt $attempt/${retryConfig.maxAttempts})")

      // Check if state directory already exists (quick check before doing work)
      if (manifestIO.stateExists(stateDir)) {
        log.debug(s"State directory already exists: $stateDir")
        lastConflictVersion = version
        version = findNextAvailableVersion(version)
        attempt += 1

        if (attempt <= retryConfig.maxAttempts) {
          val delay = calculateRetryDelay(attempt)
          log.info(
            s"Concurrent state conflict at version $lastConflictVersion (attempt ${attempt - 1}/${retryConfig.maxAttempts}). " +
              s"Retrying with version $version after ${delay}ms"
          )
          Thread.sleep(delay)
        }
      } else {
        // Try to write the state with conditional manifest write
        val success = tryWriteCompactedState(stateDir, liveFiles, version, schemaRegistry, metadata)

        if (success) {
          log.info(s"Successfully wrote state at version $version (attempt $attempt)")
          return StateWriteResult(
            stateDir = stateDir,
            version = version,
            attempts = attempt,
            conflictDetected = attempt > 1
          )
        } else {
          // Conditional write failed - another writer created the state
          log.debug(s"Conditional write failed for state version $version - concurrent writer detected")
          lastConflictVersion = version
          version = findNextAvailableVersion(version)
          attempt += 1

          if (attempt <= retryConfig.maxAttempts) {
            val delay = calculateRetryDelay(attempt)
            log.info(
              s"Concurrent state conflict at version $lastConflictVersion (attempt ${attempt - 1}/${retryConfig.maxAttempts}). " +
                s"Retrying with version $version after ${delay}ms"
            )
            Thread.sleep(delay)
          }
        }
      }
    }

    throw new ConcurrentStateWriteException(
      s"Failed to write state after ${retryConfig.maxAttempts} attempts. " +
        s"Last conflicted version: $lastConflictVersion. " +
        s"Consider increasing spark.indextables.state.retry.maxAttempts or reducing concurrent writers.",
      lastConflictVersion,
      retryConfig.maxAttempts
    )
  }

  /**
   * Write a new state incrementally with automatic retry on concurrent write conflict.
   *
   * This method performs incremental writes (reusing existing manifests) when possible, falling back to compaction when
   * needed. On each retry, it re-reads the latest base state to incorporate changes from concurrent writers.
   *
   * CRITICAL: Re-reads base state on EVERY retry attempt to avoid stale manifest lists.
   *
   * @param newFiles
   *   New file entries to add
   * @param removedPaths
   *   Paths of files to remove (become tombstones)
   * @param schemaRegistry
   *   Schema registry for doc mapping deduplication
   * @param compactionConfig
   *   Configuration for compaction thresholds
   * @param metadata
   *   Optional JSON-encoded MetadataAction for fast getMetadata()
   * @return
   *   StateWriteResult containing the written state directory, actual version, and retry info
   * @throws ConcurrentStateWriteException
   *   if all retry attempts fail due to concurrent conflicts
   */
  def writeIncrementalWithRetry(
    newFiles: Seq[FileEntry],
    removedPaths: Set[String],
    schemaRegistry: Map[String, String],
    compactionConfig: CompactionConfig = CompactionConfig(),
    metadata: Option[String] = None,
    minVersion: Option[Long] = None
  ): StateWriteResult = {

    var attempt             = 1
    var lastConflictVersion = 0L

    // Cache the base state - only re-read on version conflict
    // This optimization avoids re-reading manifests on retry when the conflict
    // was detected via conditional write (the base state hasn't changed)
    var cachedBaseState: Option[(String, StateManifest)] = None
    var lastReadBaseVersion                              = -1L

    while (attempt <= retryConfig.maxAttempts) {
      // Only re-read base state if:
      // 1. First attempt (no cache)
      // 2. Version conflict detected (state directory already existed)
      val needsReread = cachedBaseState.isEmpty || lastConflictVersion > lastReadBaseVersion

      val existingState = if (needsReread) {
        log.debug(s"Reading base state (attempt $attempt, lastConflict=$lastConflictVersion)")
        val state = findLatestState()
        cachedBaseState = state
        lastReadBaseVersion = state.map(_._2.stateVersion).getOrElse(0L)
        state
      } else {
        log.debug(s"Using cached base state (version $lastReadBaseVersion)")
        cachedBaseState
      }

      val (baseVersion, baseManifest) = existingState match {
        case Some((_, manifest)) => (manifest.stateVersion, Some(manifest))
        case None                => (0L, None)
      }

      val newVersion = math.max(baseVersion + 1, minVersion.getOrElse(1L))
      val stateDir   = s"$transactionLogPath/${manifestIO.formatStateDir(newVersion)}"

      log.debug(s"Attempting incremental write at version $newVersion (attempt $attempt/${retryConfig.maxAttempts})")

      // Check if this version already exists (concurrent writer won)
      if (manifestIO.stateExists(stateDir)) {
        log.debug(s"State directory already exists: $stateDir")
        lastConflictVersion = newVersion
        attempt += 1

        if (attempt <= retryConfig.maxAttempts) {
          val delay = calculateRetryDelay(attempt)
          log.info(
            s"Concurrent state conflict at version $newVersion (attempt ${attempt - 1}/${retryConfig.maxAttempts}). " +
              s"Re-reading base state and retrying after ${delay}ms"
          )
          Thread.sleep(delay)
        }
      } else {
        val success = baseManifest match {
          case Some(manifest) =>
            val shouldCompact = needsCompaction(manifest, removedPaths.size, compactionConfig)
            if (shouldCompact) {
              log.info(s"Writing compacted state for version $newVersion (compaction triggered)")
              tryWriteCompactedStateIncremental(
                stateDir,
                manifest,
                newFiles,
                removedPaths,
                newVersion,
                schemaRegistry,
                metadata,
                compactionConfig
              )
            } else {
              log.info(s"Writing incremental state for version $newVersion")
              tryWriteIncrementalStateToShared(
                stateDir,
                manifest,
                newFiles,
                removedPaths,
                newVersion,
                schemaRegistry,
                metadata
              )
            }
          case None =>
            log.info(s"Writing initial state for version $newVersion")
            tryWriteInitialStateToShared(stateDir, newFiles, newVersion, schemaRegistry, metadata)
        }

        if (success) {
          log.info(s"Successfully wrote state at version $newVersion (attempt $attempt)")
          return StateWriteResult(
            stateDir = stateDir,
            version = newVersion,
            attempts = attempt,
            conflictDetected = attempt > 1
          )
        } else {
          // Conditional write failed - another writer created the state
          // Don't force re-read here since the conditional write failure means
          // someone else wrote to this exact version - we just need to try a higher version
          log.debug(s"Conditional write failed for state version $newVersion - concurrent writer detected")
          lastConflictVersion = newVersion
          attempt += 1

          if (attempt <= retryConfig.maxAttempts) {
            val delay = calculateRetryDelay(attempt)
            log.info(
              s"Concurrent state conflict at version $newVersion (attempt ${attempt - 1}/${retryConfig.maxAttempts}). " +
                s"Retrying with next version after ${delay}ms"
            )
            Thread.sleep(delay)
          }
        }
      }
    }

    throw new ConcurrentStateWriteException(
      s"Failed to write incremental state after ${retryConfig.maxAttempts} attempts. " +
        s"Last conflicted version: $lastConflictVersion. " +
        s"Consider increasing spark.indextables.state.retry.maxAttempts or reducing concurrent writers.",
      lastConflictVersion,
      retryConfig.maxAttempts
    )
  }

  /**
   * Try to write an incremental state to the shared manifest location.
   *
   * New manifests are written to the shared `manifests/` directory and referenced by relative path from the state's
   * `_manifest.avro`.
   *
   * @return
   *   true if write succeeded, false if concurrent conflict detected
   */
  private def tryWriteIncrementalStateToShared(
    newStateDir: String,
    baseManifest: StateManifest,
    newFiles: Seq[FileEntry],
    removedPaths: Set[String],
    currentVersion: Long,
    schemaRegistry: Map[String, String],
    metadata: Option[String]
  ): Boolean = {

    // Create state directory
    if (!cloudProvider.exists(newStateDir)) {
      cloudProvider.createDirectory(newStateDir)
    }

    // Start with existing manifest references, converting legacy paths to include state dir
    // Legacy paths are relative to their original state dir (e.g., "manifest-xxx.avro")
    // We convert them to include the state dir (e.g., "state-v00000001/manifest-xxx.avro")
    // so they can be resolved correctly from the new state
    var newManifests = normalizeManifestPaths(baseManifest.manifests, baseManifest.stateVersion)

    // Write NEW manifest to shared location (if new files exist)
    if (newFiles.nonEmpty) {
      ensureSharedManifestDir()

      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

      manifestWriter.writeManifest(fullManifestPath, newFiles, compression, compressionLevel)

      val manifestInfo = manifestWriter.createManifestInfo(manifestRelPath, newFiles)
      newManifests = newManifests :+ manifestInfo
    }

    // Merge tombstones using Set for deduplication (O(n) instead of potential duplicates)
    // This prevents tombstone accumulation when the same file is removed multiple times
    val existingTombstoneSet = baseManifest.tombstones.toSet
    val newTombstoneSet      = existingTombstoneSet ++ removedPaths
    val newTombstones        = newTombstoneSet.toSeq

    // Calculate live file count
    val totalEntriesInManifests = newManifests.map(_.numEntries).sum
    val numLiveFiles            = totalEntriesInManifests - newTombstoneSet.size

    // Calculate total bytes (estimate)
    val totalBytes = baseManifest.totalBytes + newFiles.map(_.size).sum

    // Merge schema registries
    val mergedRegistry = baseManifest.schemaRegistry ++ schemaRegistry

    // Create state manifest
    val stateManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = numLiveFiles,
      totalBytes = totalBytes,
      manifests = newManifests,
      tombstones = newTombstones.toSeq,
      schemaRegistry = mergedRegistry,
      protocolVersion = 4,
      metadata = metadata
    )

    // Use conditional write for _manifest.avro - this is the commit point
    val written = manifestIO.writeStateManifestIfNotExists(newStateDir, stateManifest)

    if (written) {
      log.info(
        s"Wrote incremental state: version=$currentVersion, newFiles=${newFiles.size}, " +
          s"newTombstones=${removedPaths.size}, totalManifests=${newManifests.size}"
      )
    }

    written
  }

  /**
   * Try to write a compacted state during incremental write (when compaction is needed).
   *
   * Uses selective compaction when beneficial (only rewrite dirty manifests), falling back to full compaction
   * (streaming or in-memory) when selective isn't worthwhile.
   *
   * @return
   *   true if write succeeded, false if concurrent conflict detected
   */
  private def tryWriteCompactedStateIncremental(
    newStateDir: String,
    baseManifest: StateManifest,
    newFiles: Seq[FileEntry],
    removedPaths: Set[String],
    currentVersion: Long,
    schemaRegistry: Map[String, String],
    metadata: Option[String],
    compactionConfig: CompactionConfig = CompactionConfig()
  ): Boolean = {

    // Create state directory
    if (!cloudProvider.exists(newStateDir)) {
      cloudProvider.createDirectory(newStateDir)
    }

    // Find the state directory for the base manifest to resolve manifest paths
    val baseStateDir = s"$transactionLogPath/${manifestIO.formatStateDir(baseManifest.stateVersion)}"

    // Build complete tombstone set for filtering (use Set for O(1) lookups)
    val tombstoneSet = baseManifest.tombstones.toSet ++ removedPaths

    // Ensure shared manifest directory exists
    ensureSharedManifestDir()

    // Try selective compaction first (only rewrite dirty manifests)
    val selectiveResult = trySelectiveCompaction(
      newStateDir,
      baseManifest,
      baseStateDir,
      newFiles,
      tombstoneSet,
      currentVersion,
      schemaRegistry,
      metadata,
      compactionConfig
    )

    if (selectiveResult.isDefined) {
      return selectiveResult.get
    }

    // Fall back to full compaction
    log.info("Using full compaction (selective not beneficial)")

    // Estimate if we need streaming compaction (avoid loading 1M+ entries into memory)
    val estimatedTotalEntries = baseManifest.manifests.map(_.numEntries).sum + newFiles.size
    val useStreaming          = estimatedTotalEntries > 100000 // Use streaming for 100K+ entries

    val (newManifests, liveFileCount, totalBytes) = if (useStreaming) {
      log.info(s"Using streaming compaction for $estimatedTotalEntries estimated entries")
      streamingCompact(baseManifest, baseStateDir, newFiles, tombstoneSet, currentVersion)
    } else {
      // Original in-memory approach for smaller tables
      fullCompactInMemory(baseManifest, baseStateDir, newFiles, tombstoneSet)
    }

    // Merge schema registries
    val mergedRegistry = baseManifest.schemaRegistry ++ schemaRegistry

    // Create state manifest (no tombstones after compaction)
    val stateManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = liveFileCount,
      totalBytes = totalBytes,
      manifests = newManifests,
      tombstones = Seq.empty, // Clean after compaction!
      schemaRegistry = mergedRegistry,
      protocolVersion = 4,
      metadata = metadata
    )

    // Use conditional write for _manifest.avro - this is the commit point
    val written = manifestIO.writeStateManifestIfNotExists(newStateDir, stateManifest)

    if (written) {
      log.info(
        s"Wrote compacted state: version=$currentVersion, files=$liveFileCount, " +
          s"manifests=${newManifests.size}, tombstones=0, streaming=$useStreaming"
      )
    }

    written
  }

  /**
   * In-memory full compaction for smaller tables.
   *
   * @return
   *   (newManifests, liveFileCount, totalBytes)
   */
  private def fullCompactInMemory(
    baseManifest: StateManifest,
    baseStateDir: String,
    newFiles: Seq[FileEntry],
    tombstoneSet: Set[String]
  ): (Seq[ManifestInfo], Long, Long) = {

    val manifestPaths       = manifestIO.resolveManifestPaths(baseManifest, transactionLogPath, baseStateDir)
    val existingEntries     = manifestReader.readManifestsParallel(manifestPaths)
    val liveExistingEntries = manifestIO.applyTombstones(existingEntries, baseManifest.tombstones)

    // Combine with new files, filter out removed
    val allFiles  = liveExistingEntries ++ newFiles
    val liveFiles = allFiles.filterNot(f => tombstoneSet.contains(f.path))

    // Sort files by partition values for locality
    val sortedFiles = liveFiles.sortBy(entry => entry.partitionValues.toSeq.sorted.map(_._2).mkString("|"))

    // Partition into manifest chunks and write to shared location
    val manifestChunks = if (sortedFiles.isEmpty) {
      Seq(Seq.empty[FileEntry])
    } else {
      sortedFiles.grouped(entriesPerManifest).toSeq
    }

    val manifests = manifestChunks.map { chunk =>
      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

      if (chunk.nonEmpty) {
        manifestWriter.writeManifest(fullManifestPath, chunk, compression, compressionLevel)
      } else {
        manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
      }

      manifestWriter.createManifestInfo(manifestRelPath, chunk)
    }

    (manifests, liveFiles.size.toLong, liveFiles.map(_.size).sum)
  }

  /**
   * Try selective compaction - only rewrite manifests with high tombstone ratios.
   *
   * For a table with 10,000 partitions where only 1 partition has high tombstone ratio, this allows rewriting just that
   * 1 manifest instead of all 10,000 partitions.
   *
   * @return
   *   Some(true) if write succeeded, Some(false) if conflict, None if selective not beneficial
   */
  private def trySelectiveCompaction(
    newStateDir: String,
    baseManifest: StateManifest,
    baseStateDir: String,
    newFiles: Seq[FileEntry],
    tombstoneSet: Set[String],
    currentVersion: Long,
    schemaRegistry: Map[String, String],
    metadata: Option[String],
    compactionConfig: CompactionConfig
  ): Option[Boolean] = {

    // Normalize manifest paths for reuse
    val normalizedManifests = normalizeManifestPaths(baseManifest.manifests, baseManifest.stateVersion)

    // Distribute tombstones to manifests based on partition bounds
    val manifestsWithTombstones = TombstoneDistributor.distributeTombstones(normalizedManifests, tombstoneSet)

    // Partition into keep (clean) and rewrite (dirty)
    val (keepManifests, rewriteManifests) =
      TombstoneDistributor.selectivePartition(manifestsWithTombstones, compactionConfig.tombstoneThreshold)

    // Check if selective compaction is beneficial
    if (!TombstoneDistributor.isSelectiveCompactionBeneficial(keepManifests, rewriteManifests)) {
      return None // Fall back to full compaction
    }

    log.info(
      s"Using selective compaction: keeping ${keepManifests.size} clean manifests, " +
        s"rewriting ${rewriteManifests.size} dirty manifests"
    )

    // Process dirty manifests - read entries, filter tombstones, write new manifest
    val rewrittenManifests = rewriteManifests.flatMap { dirtyManifest =>
      val manifestPath = manifestIO.resolveManifestPath(dirtyManifest, transactionLogPath, baseStateDir)
      val entries      = manifestReader.readManifest(manifestPath)

      // Filter out tombstones
      val liveEntries = entries.filterNot(e => tombstoneSet.contains(e.path))

      if (liveEntries.isEmpty) {
        // No live entries - don't write empty manifest
        None
      } else {
        // Sort by partition for locality
        val sortedEntries = liveEntries.sortBy(entry => entry.partitionValues.toSeq.sorted.map(_._2).mkString("|"))

        val manifestId       = manifestWriter.generateManifestId()
        val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
        val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

        manifestWriter.writeManifest(fullManifestPath, sortedEntries, compression, compressionLevel)
        Some(manifestWriter.createManifestInfo(manifestRelPath, sortedEntries))
      }
    }

    // Add new files manifest
    val newFilesManifest = if (newFiles.nonEmpty) {
      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

      manifestWriter.writeManifest(fullManifestPath, newFiles, compression, compressionLevel)
      Some(manifestWriter.createManifestInfo(manifestRelPath, newFiles))
    } else {
      None
    }

    // Combine: kept manifests (cleared tombstone counts) + rewritten + new files
    val cleanedKeepManifests = keepManifests.map(_.copy(tombstoneCount = 0, liveEntryCount = -1))
    val allManifests         = cleanedKeepManifests ++ rewrittenManifests ++ newFilesManifest.toSeq

    // Calculate totals
    val liveFileCount = allManifests.map(_.numEntries).sum
    val totalBytes    = baseManifest.totalBytes + newFiles.map(_.size).sum // Estimate

    // Merge schema registries
    val mergedRegistry = baseManifest.schemaRegistry ++ schemaRegistry

    // Create state manifest (no tombstones after selective compaction!)
    val stateManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = liveFileCount,
      totalBytes = totalBytes,
      manifests = allManifests,
      tombstones = Seq.empty, // Clean! All tombstones applied during selective compaction
      schemaRegistry = mergedRegistry,
      protocolVersion = 4,
      metadata = metadata
    )

    // Use conditional write for _manifest.avro - this is the commit point
    val written = manifestIO.writeStateManifestIfNotExists(newStateDir, stateManifest)

    if (written) {
      log.info(
        s"Wrote selectively compacted state: version=$currentVersion, files=$liveFileCount, " +
          s"kept=${keepManifests.size}, rewritten=${rewriteManifests.size}, " +
          s"manifests=${allManifests.size}, tombstones=0"
      )
    }

    Some(written)
  }

  /**
   * Streaming compaction that processes manifests one at a time to avoid loading all entries into memory. For tables
   * with 1M+ files, this reduces memory from ~500MB to ~50MB per manifest chunk.
   *
   * @return
   *   (newManifests, liveFileCount, totalBytes)
   */
  private def streamingCompact(
    baseManifest: StateManifest,
    baseStateDir: String,
    newFiles: Seq[FileEntry],
    tombstoneSet: Set[String],
    currentVersion: Long
  ): (Seq[ManifestInfo], Long, Long) = {

    import scala.collection.mutable.ArrayBuffer

    val newManifests  = ArrayBuffer[ManifestInfo]()
    var currentChunk  = ArrayBuffer[FileEntry]()
    var liveFileCount = 0L
    var totalBytes    = 0L

    // Helper to flush current chunk to a new manifest
    def flushChunk(): Unit =
      if (currentChunk.nonEmpty) {
        // Sort chunk by partition values for locality before writing
        val sortedChunk = currentChunk.sortBy { entry =>
          entry.partitionValues.toSeq.sorted.map(_._2).mkString("|")
        }.toSeq

        val manifestId       = manifestWriter.generateManifestId()
        val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
        val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

        manifestWriter.writeManifest(fullManifestPath, sortedChunk, compression, compressionLevel)
        newManifests += manifestWriter.createManifestInfo(manifestRelPath, sortedChunk)

        currentChunk.clear()
      }

    // Process existing manifests one at a time
    baseManifest.manifests.foreach { manifestInfo =>
      val manifestPath = manifestIO.resolveManifestPath(manifestInfo, transactionLogPath, baseStateDir)
      val entries      = manifestReader.readManifest(manifestPath)

      // Filter out tombstones and add to current chunk
      entries.foreach { entry =>
        if (!tombstoneSet.contains(entry.path)) {
          currentChunk += entry
          liveFileCount += 1
          totalBytes += entry.size

          // Flush when chunk reaches target size
          if (currentChunk.size >= entriesPerManifest) {
            flushChunk()
          }
        }
      }
    }

    // Add new files
    newFiles.foreach { entry =>
      if (!tombstoneSet.contains(entry.path)) {
        currentChunk += entry
        liveFileCount += 1
        totalBytes += entry.size

        if (currentChunk.size >= entriesPerManifest) {
          flushChunk()
        }
      }
    }

    // Flush any remaining entries
    flushChunk()

    // Handle edge case of no live files
    if (newManifests.isEmpty) {
      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"
      manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
      newManifests += manifestWriter.createManifestInfo(manifestRelPath, Seq.empty)
    }

    log.info(
      s"Streaming compaction complete: processed ${baseManifest.manifests.size} source manifests, " +
        s"wrote ${newManifests.size} output manifests, $liveFileCount live files"
    )

    (newManifests.toSeq, liveFileCount, totalBytes)
  }

  /**
   * Try to write an initial state (no existing state) to the shared manifest location.
   *
   * @return
   *   true if write succeeded, false if concurrent conflict detected
   */
  private def tryWriteInitialStateToShared(
    newStateDir: String,
    files: Seq[FileEntry],
    currentVersion: Long,
    schemaRegistry: Map[String, String],
    metadata: Option[String]
  ): Boolean = {

    // Create state directory
    if (!cloudProvider.exists(newStateDir)) {
      cloudProvider.createDirectory(newStateDir)
    }

    // Sort files by partition values for locality
    val sortedFiles = files.sortBy(entry => entry.partitionValues.toSeq.sorted.map(_._2).mkString("|"))

    // Ensure shared manifest directory exists
    ensureSharedManifestDir()

    // Partition into manifest chunks and write to shared location
    val manifestChunks = if (sortedFiles.isEmpty) {
      Seq(Seq.empty[FileEntry])
    } else {
      sortedFiles.grouped(entriesPerManifest).toSeq
    }

    val newManifests = manifestChunks.map { chunk =>
      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

      if (chunk.nonEmpty) {
        manifestWriter.writeManifest(fullManifestPath, chunk, compression, compressionLevel)
      } else {
        manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
      }

      manifestWriter.createManifestInfo(manifestRelPath, chunk)
    }

    // Create state manifest
    val stateManifest = StateManifest(
      formatVersion = 1,
      stateVersion = currentVersion,
      createdAt = System.currentTimeMillis(),
      numFiles = files.size,
      totalBytes = files.map(_.size).sum,
      manifests = newManifests,
      tombstones = Seq.empty,
      schemaRegistry = schemaRegistry,
      protocolVersion = 4,
      metadata = metadata
    )

    // Use conditional write for _manifest.avro - this is the commit point
    val written = manifestIO.writeStateManifestIfNotExists(newStateDir, stateManifest)

    if (written) {
      log.info(
        s"Wrote initial state: version=$currentVersion, files=${files.size}, " +
          s"manifests=${newManifests.size}"
      )
    }

    written
  }

  /**
   * Normalize manifest paths for referencing from a new state.
   *
   * Legacy paths (created by CHECKPOINT) are relative to their original state directory. When referencing them from a
   * new state, we need to include the state directory prefix so they can be resolved correctly.
   *
   * Path formats:
   *   - "manifests/manifest-xxx.avro" → shared manifest, keep as-is
   *   - "state-v00000001/manifest-xxx.avro" → already normalized, keep as-is
   *   - "manifest-xxx.avro" → legacy per-state, convert to "state-v00000001/manifest-xxx.avro"
   */
  private def normalizeManifestPaths(manifests: Seq[ManifestInfo], baseStateVersion: Long): Seq[ManifestInfo] =
    manifests.map { m =>
      val path = m.path
      if (
        path.startsWith(StateConfig.SHARED_MANIFEST_DIR + "/") ||
        path.startsWith("state-v") ||
        path.startsWith("s3://") ||
        path.startsWith("abfss://") ||
        path.startsWith("wasbs://") ||
        path.startsWith("gs://") ||
        path.startsWith("/")
      ) {
        // Already normalized or absolute path
        m
      } else {
        // Legacy format: relative to original state dir, convert to include state dir prefix
        val normalizedPath = s"${manifestIO.formatStateDir(baseStateVersion)}/$path"
        m.copy(path = normalizedPath)
      }
    }

  /** Ensure the shared manifest directory exists. */
  private def ensureSharedManifestDir(): Unit = {
    val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
    if (!cloudProvider.exists(sharedDir)) {
      cloudProvider.createDirectory(sharedDir)
    }
  }

  /**
   * Try to write a compacted state with conditional _manifest.avro write.
   *
   * Writes manifests to the shared `manifests/` directory for cross-version reuse.
   *
   * @return
   *   true if write succeeded, false if concurrent conflict detected
   */
  private def tryWriteCompactedState(
    stateDir: String,
    liveFiles: Seq[FileEntry],
    version: Long,
    schemaRegistry: Map[String, String],
    metadata: Option[String]
  ): Boolean = {

    // Create directory (safe - directory names are versioned)
    if (!cloudProvider.exists(stateDir)) {
      cloudProvider.createDirectory(stateDir)
    }

    // Ensure shared manifest directory exists
    ensureSharedManifestDir()

    // Sort files by partition values for locality
    val sortedFiles = liveFiles.sortBy(entry => entry.partitionValues.toSeq.sorted.map(_._2).mkString("|"))

    // Partition into manifest chunks
    val manifestChunks = if (sortedFiles.isEmpty) {
      Seq(Seq.empty[FileEntry])
    } else {
      sortedFiles.grouped(entriesPerManifest).toSeq
    }

    // Write new manifests to shared location (safe - UUID naming ensures uniqueness)
    val newManifests = manifestChunks.map { chunk =>
      val manifestId       = manifestWriter.generateManifestId()
      val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
      val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

      if (chunk.nonEmpty) {
        manifestWriter.writeManifest(fullManifestPath, chunk, compression, compressionLevel)
      } else {
        manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
      }

      manifestWriter.createManifestInfo(manifestRelPath, chunk)
    }

    // Create state manifest
    val stateManifest = StateManifest(
      formatVersion = 1,
      stateVersion = version,
      createdAt = System.currentTimeMillis(),
      numFiles = liveFiles.size,
      totalBytes = liveFiles.map(_.size).sum,
      manifests = newManifests,
      tombstones = Seq.empty,
      schemaRegistry = schemaRegistry,
      protocolVersion = 4,
      metadata = metadata
    )

    // Use conditional write for _manifest.avro - this is the commit point
    val written = manifestIO.writeStateManifestIfNotExists(stateDir, stateManifest)

    if (written) {
      log.info(s"Wrote compacted state: version=$version, files=${liveFiles.size}, manifests=${newManifests.size}")
    }

    written
  }

  /** Find the next available state version by scanning existing state directories. */
  private def findNextAvailableVersion(afterVersion: Long): Long =
    Try {
      val files = cloudProvider.listFiles(transactionLogPath, recursive = true)
      val manifestFiles = files
        .filter(f => f.path.endsWith("/_manifest.avro"))
        .filter(f => f.path.contains("/state-v"))

      if (manifestFiles.isEmpty) {
        afterVersion + 1
      } else {
        val versions = manifestFiles.flatMap { f =>
          manifestIO.parseStateDirVersion(f.path.stripSuffix("/_manifest.avro"))
        }
        val maxVersion = if (versions.isEmpty) afterVersion else versions.max
        math.max(afterVersion, maxVersion) + 1
      }
    } match {
      case Success(v) => v
      case Failure(_) => afterVersion + 1
    }

  /** Calculate exponential backoff delay with jitter for retry attempts. */
  private def calculateRetryDelay(attempt: Int): Long = {
    val exponentialDelay = retryConfig.baseDelayMs * math.pow(2, attempt - 1).toLong
    val cappedDelay      = math.min(exponentialDelay, retryConfig.maxDelayMs)
    // Add jitter: 75% to 125% of the calculated delay
    val jitter = 0.75 + random.nextDouble() * 0.5
    (cappedDelay * jitter).toLong
  }

  /**
   * Write a compacted state from a list of live files.
   *
   * This is used both for compaction and for initial checkpoint creation from AddActions. Writes manifests to the
   * shared `manifests/` directory for cross-version reuse.
   */
  def writeCompactedStateFromFiles(
    newStateDir: String,
    liveFiles: Seq[FileEntry],
    currentVersion: Long,
    schemaRegistry: Map[String, String]
  ): Unit = {

    if (!cloudProvider.exists(newStateDir)) {
      cloudProvider.createDirectory(newStateDir)
    }

    // Ensure shared manifest directory exists
    ensureSharedManifestDir()

    // Sort files by partition values for locality
    val sortedFiles = liveFiles.sortBy(entry => entry.partitionValues.toSeq.sorted.map(_._2).mkString("|"))

    // Partition into manifest chunks
    val manifestChunks = if (sortedFiles.isEmpty) {
      Seq(Seq.empty[FileEntry])
    } else {
      sortedFiles.grouped(entriesPerManifest).toSeq
    }

    // Write new manifests to shared location with partition bounds
    val newManifests = manifestChunks.zipWithIndex.map {
      case (chunk, idx) =>
        val manifestId       = manifestWriter.generateManifestId()
        val manifestRelPath  = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-$manifestId.avro"
        val fullManifestPath = s"$transactionLogPath/$manifestRelPath"

        if (chunk.nonEmpty) {
          manifestWriter.writeManifest(fullManifestPath, chunk, compression, compressionLevel)
        } else {
          // Write empty manifest for consistency
          manifestWriter.writeManifest(fullManifestPath, Seq.empty, compression, compressionLevel)
        }

        manifestWriter.createManifestInfo(manifestRelPath, chunk)
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
        s"manifests=${newManifests.size}, tombstones=0"
    )
  }

  /**
   * Determine if compaction is needed based on tombstone ratio and manifest count.
   *
   * @param manifest
   *   Current state manifest
   * @param newRemoves
   *   Number of files being removed in this operation
   * @param config
   *   Compaction configuration (thresholds)
   * @return
   *   true if compaction should be performed
   */
  def needsCompaction(
    manifest: StateManifest,
    newRemoves: Int,
    config: CompactionConfig = CompactionConfig()
  ): Boolean = {

    // Force compaction if requested
    if (config.forceCompaction) {
      log.debug("Compaction forced via config")
      return true
    }

    val totalTombstones = manifest.tombstones.size + newRemoves

    // Use numFiles as the base for tombstone ratio calculation
    // This represents "what percentage of our tracked entries are tombstones"
    // and avoids issues when removing more files than exist
    if (manifest.numFiles <= 0) {
      return true // Compact if no files tracked
    }

    val tombstoneRatio = totalTombstones.toDouble / manifest.numFiles
    val manifestCount  = manifest.manifests.size

    // Compact when:
    // 1. Tombstones exceed threshold (configurable, default 10%)
    val tombstoneThresholdExceeded = tombstoneRatio > config.tombstoneThreshold

    // 2. Too many manifests (fragmentation) (configurable, default 20)
    val tooManyManifests = manifestCount > config.maxManifests

    // 3. Large number of removes in single operation (configurable, default disabled)
    val largeRemoveOperation = newRemoves > config.largeRemoveThreshold

    val shouldCompact = tombstoneThresholdExceeded || tooManyManifests || largeRemoveOperation

    if (shouldCompact) {
      log.debug(
        s"Compaction needed: tombstoneRatio=$tombstoneRatio (threshold=${config.tombstoneThreshold}), " +
          s"manifestCount=$manifestCount (max=${config.maxManifests}), " +
          s"newRemoves=$newRemoves (threshold=${config.largeRemoveThreshold})"
      )
    }

    shouldCompact
  }

  /** Find the latest state directory and its manifest. */
  private def findLatestState(): Option[(String, StateManifest)] =
    Try {
      // List all files recursively and look for _manifest.avro files in state directories
      val files = cloudProvider.listFiles(transactionLogPath, recursive = true)
      val manifestFiles = files
        .filter(f => f.path.endsWith("/_manifest.avro"))
        .filter(f => f.path.contains("/state-v"))

      if (manifestFiles.isEmpty) {
        return None
      }

      // Extract state directory paths from manifest file paths
      val stateDirs = manifestFiles
        .map { f =>
          // Remove /_manifest.avro from the path to get the state directory
          f.path.stripSuffix("/_manifest.avro")
        }
        .filter(p => manifestIO.parseStateDirVersion(p).isDefined)

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

  /** Convert AddActions to FileEntries for writing. */
  def convertAddActionsToFileEntries(
    adds: Seq[AddAction],
    version: Long,
    timestamp: Long = System.currentTimeMillis()
  ): Seq[FileEntry] =
    adds.map(add => FileEntry.fromAddAction(add, version, timestamp))
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
   * @param retryConfig
   *   Retry configuration for concurrent write conflicts (default: StateRetryConfig())
   * @return
   *   StateWriter instance
   */
  def apply(
    cloudProvider: CloudStorageProvider,
    transactionLogPath: String,
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT,
    entriesPerManifest: Int = StateConfig.ENTRIES_PER_MANIFEST_DEFAULT,
    retryConfig: StateRetryConfig = StateRetryConfig()
  ): StateWriter =
    new StateWriter(cloudProvider, transactionLogPath, compression, compressionLevel, entriesPerManifest, retryConfig)
}
