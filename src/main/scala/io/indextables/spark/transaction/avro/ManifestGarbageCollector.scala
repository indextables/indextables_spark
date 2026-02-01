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

import scala.collection.mutable
import scala.util.Try

import io.indextables.spark.io.CloudStorageProvider
import org.slf4j.LoggerFactory

/**
 * Iceberg-style manifest garbage collector.
 *
 * Tracks which manifests are reachable from retained state versions and safely deletes orphaned manifests from the
 * shared manifest directory.
 *
 * Safety features:
 *   - Age-based protection: Never deletes manifests younger than minManifestAgeHours
 *   - Version-based retention: Keeps manifests referenced by retentionVersions + buffer
 *   - Dry-run mode: Preview deletions without actually deleting
 *
 * @param cloudProvider
 *   Cloud storage provider for file access
 * @param transactionLogPath
 *   Path to the transaction log directory
 */
class ManifestGarbageCollector(
  cloudProvider: CloudStorageProvider,
  transactionLogPath: String) {

  private val log        = LoggerFactory.getLogger(getClass)
  private val manifestIO = StateManifestIO(cloudProvider)

  /**
   * Find all manifests reachable from any state version within retention.
   *
   * Uses version-based approach: reads _last_checkpoint to get current version, then directly constructs paths for
   * retained versions without listing. This minimizes cloud API calls.
   *
   * @param config
   *   GC configuration
   * @return
   *   Set of reachable manifest paths (relative to transaction log root)
   */
  def findReachableManifests(config: GCConfig = GCConfig()): Set[String] = {
    val reachable = mutable.Set[String]()

    // Get current version from _last_checkpoint
    val currentVersion = manifestIO.getCurrentCheckpointVersion(transactionLogPath)

    currentVersion match {
      case Some(version) =>
        // Compute versions to retain: current, current-1, ..., current-retentionVersions
        // Add extra buffer for safety
        val versionsToCheck = (0 to config.retentionVersions + 2)
          .map(version - _)
          .filter(_ >= 0)

        log.debug(s"Checking ${versionsToCheck.size} state versions for reachable manifests (current=$version)")

        // Read each state's manifest list
        versionsToCheck.foreach { v =>
          val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(v)}"
          Try {
            if (manifestIO.stateExists(stateDir)) {
              val manifest = manifestIO.readStateManifest(stateDir)
              manifest.manifests.foreach(m => reachable.add(m.path))
            }
          }.recover {
            case e: Exception =>
              log.debug(s"Could not read state $stateDir: ${e.getMessage}")
          }
        }

      case None =>
        log.info("No _last_checkpoint found, cannot determine reachable manifests")
    }

    log.info(s"Found ${reachable.size} reachable manifests from retained state versions")
    reachable.toSet
  }

  /**
   * List all manifests in the shared manifest directory.
   *
   * @return
   *   Sequence of (relativePath, modificationTime) tuples
   */
  def listSharedManifests(): Seq[(String, Long)] = {
    val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"

    if (!cloudProvider.exists(sharedDir)) {
      log.debug(s"Shared manifest directory does not exist: $sharedDir")
      return Seq.empty
    }

    val files = cloudProvider.listFiles(sharedDir, recursive = false)
    files
      .filter(_.path.endsWith(".avro"))
      .map { f =>
        val relativePath = s"${StateConfig.SHARED_MANIFEST_DIR}/${f.path.split("/").last}"
        (relativePath, f.modificationTime)
      }
  }

  /**
   * Find orphaned manifests that are not reachable from any retained state.
   *
   * @param reachable
   *   Set of reachable manifest paths
   * @param config
   *   GC configuration
   * @return
   *   Sequence of orphaned manifest paths that are safe to delete
   */
  def findOrphanedManifests(reachable: Set[String], config: GCConfig = GCConfig()): Seq[String] = {
    val allManifests = listSharedManifests()
    val now          = System.currentTimeMillis()
    val minAgeMs     = config.minManifestAgeHours * 3600 * 1000L

    allManifests
      .filterNot { case (path, _) => reachable.contains(path) }
      .filter {
        case (_, modTime) =>
          // Only delete if old enough (age-based protection)
          val age = now - modTime
          age >= minAgeMs
      }
      .map(_._1)
  }

  /**
   * Delete orphaned manifests from the shared directory.
   *
   * @param orphaned
   *   Paths of orphaned manifests to delete
   * @param dryRun
   *   If true, only log what would be deleted without actually deleting
   * @return
   *   Number of manifests deleted (or would be deleted in dry-run mode)
   */
  def deleteOrphanedManifests(orphaned: Seq[String], dryRun: Boolean): Long = {
    if (orphaned.isEmpty) {
      log.debug("No orphaned manifests to delete")
      return 0L
    }

    if (dryRun) {
      log.info(s"DRY RUN: Would delete ${orphaned.size} orphaned manifests:")
      orphaned.foreach(path => log.info(s"  DRY RUN: Would delete $path"))
      return orphaned.size.toLong
    }

    var deleted = 0L
    orphaned.foreach { relativePath =>
      val fullPath = s"$transactionLogPath/$relativePath"
      try {
        cloudProvider.deleteFile(fullPath)
        deleted += 1
        log.debug(s"Deleted orphaned manifest: $relativePath")
      } catch {
        case e: Exception =>
          log.warn(s"Failed to delete manifest $relativePath: ${e.getMessage}")
      }
    }

    log.info(s"Deleted $deleted orphaned manifests")
    deleted
  }

  /**
   * Run full garbage collection: find and delete orphaned manifests.
   *
   * @param config
   *   GC configuration
   * @param dryRun
   *   If true, only preview what would be deleted
   * @return
   *   GCResult with statistics
   */
  def collectGarbage(config: GCConfig = GCConfig(), dryRun: Boolean = false): GCResult = {
    log.info(
      s"Starting manifest garbage collection (dryRun=$dryRun, retentionVersions=${config.retentionVersions}, " +
        s"minManifestAgeHours=${config.minManifestAgeHours})"
    )

    val reachable = findReachableManifests(config)
    val orphaned  = findOrphanedManifests(reachable, config)
    val deleted   = deleteOrphanedManifests(orphaned, dryRun)

    GCResult(
      reachableManifests = reachable.size,
      orphanedManifests = orphaned.size,
      deletedManifests = deleted,
      dryRun = dryRun
    )
  }
}

/**
 * Result of garbage collection operation.
 *
 * @param reachableManifests
 *   Number of manifests reachable from retained states
 * @param orphanedManifests
 *   Number of orphaned manifests found
 * @param deletedManifests
 *   Number of manifests actually deleted (or would be deleted in dry-run)
 * @param dryRun
 *   Whether this was a dry-run (no actual deletions)
 */
case class GCResult(
  reachableManifests: Int,
  orphanedManifests: Int,
  deletedManifests: Long,
  dryRun: Boolean)

object ManifestGarbageCollector {

  /**
   * Create a ManifestGarbageCollector for the given cloud provider and transaction log path.
   *
   * @param cloudProvider
   *   Cloud storage provider for file access
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @return
   *   ManifestGarbageCollector instance
   */
  def apply(cloudProvider: CloudStorageProvider, transactionLogPath: String): ManifestGarbageCollector =
    new ManifestGarbageCollector(cloudProvider, transactionLogPath)
}
