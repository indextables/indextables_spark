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

/**
 * Streaming-aware reader for Avro state files.
 *
 * This reader supports:
 *   - Reading changes since a specific version (for Spark Structured Streaming)
 *   - Filtering files by addedAtVersion for efficient incremental processing
 *   - Version range queries for time travel and auditing
 */
class StreamingStateReader(cloudProvider: CloudStorageProvider, transactionLogPath: String) {

  private val log = LoggerFactory.getLogger(getClass)
  private val manifestIO = StateManifestIO(cloudProvider)
  private val manifestReader = AvroManifestReader(cloudProvider)

  /**
   * Get files added since a specific version.
   *
   * This is the primary method for Spark Structured Streaming integration, allowing efficient
   * incremental processing of new data without rescanning existing files.
   *
   * @param sinceVersion
   *   The version after which to look for changes (exclusive)
   * @return
   *   ChangeSet containing added and removed file paths
   */
  def getChangesSince(sinceVersion: Long): ChangeSet = {
    val latestState = findLatestState()

    latestState match {
      case Some((stateDir, manifest)) =>
        if (manifest.stateVersion <= sinceVersion) {
          // No changes since the requested version
          log.debug(s"No changes since version $sinceVersion (current: ${manifest.stateVersion})")
          ChangeSet(
            adds = Seq.empty,
            removes = Seq.empty,
            newVersion = manifest.stateVersion
          )
        } else {
          // Read and filter by addedAtVersion
          getChangesBetweenVersions(sinceVersion, manifest.stateVersion, stateDir, manifest)
        }

      case None =>
        log.warn(s"No state found in $transactionLogPath")
        ChangeSet(
          adds = Seq.empty,
          removes = Seq.empty,
          newVersion = sinceVersion
        )
    }
  }

  /**
   * Get files added between two versions.
   *
   * @param fromVersion
   *   Start version (exclusive)
   * @param toVersion
   *   End version (inclusive)
   * @return
   *   ChangeSet containing added and removed file paths in the version range
   */
  def getChangesBetween(fromVersion: Long, toVersion: Long): ChangeSet = {
    val latestState = findLatestState()

    latestState match {
      case Some((stateDir, manifest)) =>
        if (manifest.stateVersion < toVersion) {
          log.warn(s"Requested toVersion $toVersion is beyond current state version ${manifest.stateVersion}")
          getChangesBetweenVersions(fromVersion, manifest.stateVersion, stateDir, manifest)
        } else {
          getChangesBetweenVersions(fromVersion, toVersion, stateDir, manifest)
        }

      case None =>
        log.warn(s"No state found in $transactionLogPath")
        ChangeSet(
          adds = Seq.empty,
          removes = Seq.empty,
          newVersion = toVersion
        )
    }
  }

  /**
   * Get the current state version.
   *
   * @return
   *   Current version, or 0 if no state exists
   */
  def getCurrentVersion: Long = {
    findLatestState() match {
      case Some((_, manifest)) => manifest.stateVersion
      case None => 0L
    }
  }

  /**
   * Get state metadata for the current version.
   *
   * @return
   *   StateManifest if state exists, None otherwise
   */
  def getStateMetadata: Option[StateManifest] = {
    findLatestState().map(_._2)
  }

  /**
   * Internal implementation of version range query.
   */
  private def getChangesBetweenVersions(
      fromVersion: Long,
      toVersion: Long,
      stateDir: String,
      manifest: StateManifest): ChangeSet = {

    log.debug(s"Getting changes from version $fromVersion to $toVersion")

    // First, prune manifests by version bounds if possible
    val relevantManifests = manifest.manifests.filter { m =>
      // Include manifest if its version range overlaps with query range
      m.maxAddedAtVersion > fromVersion
    }

    log.debug(s"Reading ${relevantManifests.size} of ${manifest.manifests.size} manifests for version range")

    // Read all relevant manifests
    val manifestPaths = relevantManifests.map(m => s"$stateDir/${m.path}")
    val allEntries = manifestReader.readManifestsParallel(manifestPaths)

    // Filter by version range
    val addedFiles = allEntries
      .filter(e => e.addedAtVersion > fromVersion && e.addedAtVersion <= toVersion)
      .filterNot(e => manifest.tombstones.contains(e.path))

    // For removed files, we look at tombstones
    // Note: Tombstones don't have version info, so we can't filter by version
    // In a full implementation, we'd need to track when files were removed
    val removedFiles = manifest.tombstones
      .filter(path => allEntries.exists(_.path == path))

    log.info(s"Found ${addedFiles.size} added files between versions $fromVersion and $toVersion")

    ChangeSet(
      adds = addedFiles,
      removes = removedFiles,
      newVersion = toVersion
    )
  }

  /**
   * Get all files added at a specific version.
   *
   * @param version
   *   The exact version to query
   * @return
   *   Files added at that version
   */
  def getFilesAtVersion(version: Long): Seq[FileEntry] = {
    val latestState = findLatestState()

    latestState match {
      case Some((stateDir, manifest)) =>
        // Find manifests that may contain files at this version
        val relevantManifests = manifest.manifests.filter { m =>
          m.minAddedAtVersion <= version && m.maxAddedAtVersion >= version
        }

        if (relevantManifests.isEmpty) {
          Seq.empty
        } else {
          val manifestPaths = relevantManifests.map(m => s"$stateDir/${m.path}")
          val allEntries = manifestReader.readManifestsParallel(manifestPaths)
          allEntries.filter(_.addedAtVersion == version)
        }

      case None =>
        Seq.empty
    }
  }

  /**
   * Convert FileEntries to AddActions for compatibility with existing code.
   *
   * This overload automatically retrieves the schema registry from the current state
   * to restore docMappingJson from docMappingRef.
   */
  def toAddActions(entries: Seq[FileEntry]): Seq[AddAction] = {
    val schemaRegistry = getStateMetadata.map(_.schemaRegistry).getOrElse(Map.empty)
    manifestReader.toAddActions(entries, schemaRegistry)
  }

  /**
   * Convert FileEntries to AddActions with explicit schema registry.
   *
   * Use this overload when you have the schema registry available (e.g., from a
   * StateManifest you've already read) to avoid an extra state lookup.
   *
   * @param entries
   *   FileEntries to convert
   * @param schemaRegistry
   *   Schema registry for restoring docMappingJson from docMappingRef
   * @return
   *   AddAction representations with docMappingJson restored
   */
  def toAddActions(entries: Seq[FileEntry], schemaRegistry: Map[String, String]): Seq[AddAction] = {
    manifestReader.toAddActions(entries, schemaRegistry)
  }

  /**
   * Find the latest state directory.
   */
  private def findLatestState(): Option[(String, StateManifest)] = {
    try {
      val files = cloudProvider.listFiles(transactionLogPath, recursive = true)
      val manifestFiles = files
        .filter(f => f.path.endsWith("/_manifest.json"))
        .filter(f => f.path.contains("/state-v"))

      if (manifestFiles.isEmpty) {
        return None
      }

      // Extract versions from the manifest file paths
      val versions = manifestFiles.flatMap { f =>
        manifestIO.parseStateDirVersion(f.path.stripSuffix("/_manifest.json"))
      }

      if (versions.isEmpty) {
        return None
      }

      val latestVersion = versions.max

      // Construct the state directory path using the same method as StateWriter
      // This ensures consistent path format between writing and reading
      val latestStateDir = s"$transactionLogPath/${manifestIO.formatStateDir(latestVersion)}"

      if (manifestIO.stateExists(latestStateDir)) {
        val manifest = manifestIO.readStateManifest(latestStateDir)
        Some((latestStateDir, manifest))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.debug(s"No state found: ${e.getMessage}")
        None
    }
  }
}

object StreamingStateReader {

  /**
   * Create a streaming state reader.
   *
   * @param cloudProvider
   *   Cloud storage provider
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @return
   *   StreamingStateReader instance
   */
  def apply(cloudProvider: CloudStorageProvider, transactionLogPath: String): StreamingStateReader = {
    new StreamingStateReader(cloudProvider, transactionLogPath)
  }
}
