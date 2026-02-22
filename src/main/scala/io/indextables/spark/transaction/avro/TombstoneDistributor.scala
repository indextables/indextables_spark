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

import org.slf4j.LoggerFactory

/**
 * Distributes tombstones to manifests based on partition bounds.
 *
 * This enables selective compaction where only manifests with high tombstone ratios are rewritten, while clean
 * manifests are kept as-is.
 *
 * For a table with 10,000 partitions where only 1 partition has high tombstone ratio, this allows rewriting just 1
 * manifest instead of all 10,000 partitions worth of data.
 */
object TombstoneDistributor {

  private val log = LoggerFactory.getLogger(getClass)

  // Pattern to extract partition key=value pairs from file paths
  // Matches: /key=value/ or /key=value at end of path component
  private val PartitionPattern = """(?:^|/)([^/=]+)=([^/]+)(?=/|$)""".r

  /**
   * Parse partition values from a file path.
   *
   * File paths in partitioned tables follow patterns like:
   *   - s3://bucket/table/date=2024-01-01/region=us-east/file.split
   *   - /local/path/year=2024/month=01/file.split
   *
   * @param path
   *   File path to parse
   * @return
   *   Map of partition column names to values, empty if no partitions found
   */
  def extractPartitionValues(path: String): Map[String, String] =
    PartitionPattern.findAllMatchIn(path).map(m => m.group(1) -> m.group(2)).toMap

  /**
   * Check if partition values fall within the given partition bounds.
   *
   * A tombstone "matches" a manifest if its partition values fall within the manifest's partition bounds for all shared
   * partition columns.
   *
   * @param partitionValues
   *   Partition values from a tombstone path
   * @param bounds
   *   Partition bounds from a manifest
   * @return
   *   true if values could be in this manifest
   */
  def valuesWithinBounds(
    partitionValues: Map[String, String],
    bounds: Option[Map[String, PartitionBounds]]
  ): Boolean =
    bounds match {
      case None =>
        // No partition bounds - manifest contains unpartitioned data
        // Only match if tombstone also has no partition values
        partitionValues.isEmpty

      case Some(boundsMap) if boundsMap.isEmpty =>
        // Empty bounds map - same as no partitions
        partitionValues.isEmpty

      case Some(boundsMap) =>
        // Check each partition column
        // Tombstone matches if its values fall within bounds for ALL columns present in bounds
        boundsMap.forall {
          case (column, bound) =>
            partitionValues.get(column) match {
              case None =>
                // Tombstone doesn't have this partition column
                // This is unusual but could happen if manifest has more partition columns
                // Be conservative and say it could match
                true

              case Some(value) =>
                // Check if value is within min/max bounds (numeric-aware comparison)
                val withinMin = bound.min.forall(min => PartitionPruner.numericAwareCompare(value, min) >= 0)
                val withinMax = bound.max.forall(max => PartitionPruner.numericAwareCompare(value, max) <= 0)
                withinMin && withinMax
            }
        }
    }

  /**
   * Distribute tombstones to manifests based on partition bounds.
   *
   * For each manifest, counts how many tombstones fall within its partition bounds. Updates the manifest's
   * tombstoneCount and liveEntryCount fields.
   *
   * @param manifests
   *   Current manifests with partition bounds
   * @param tombstones
   *   All tombstone paths (existing + new)
   * @return
   *   Updated manifests with tombstone counts
   */
  def distributeTombstones(
    manifests: Seq[ManifestInfo],
    tombstones: Set[String]
  ): Seq[ManifestInfo] = {

    if (tombstones.isEmpty) {
      // No tombstones - all manifests are clean
      return manifests.map(m => m.copy(tombstoneCount = 0, liveEntryCount = m.numEntries))
    }

    // Parse partition values for all tombstones once
    val tombstonePartitions = tombstones.map(path => path -> extractPartitionValues(path)).toMap

    // For each manifest, count tombstones that fall within its bounds
    manifests.map { manifest =>
      val count = tombstonePartitions.count {
        case (_, partitionValues) =>
          valuesWithinBounds(partitionValues, manifest.partitionBounds)
      }

      // Cap at numEntries (can't have more tombstones than entries)
      val cappedCount = math.min(count.toLong, manifest.numEntries)
      val liveCount   = manifest.numEntries - cappedCount

      if (count > 0) {
        log.debug(s"Manifest ${manifest.path}: $cappedCount tombstones, $liveCount live entries")
      }

      manifest.copy(
        tombstoneCount = cappedCount,
        liveEntryCount = liveCount
      )
    }
  }

  /**
   * Partition manifests into "keep" (clean) and "rewrite" (dirty) based on tombstone ratio.
   *
   * @param manifests
   *   Manifests with tombstone counts
   * @param threshold
   *   Tombstone ratio threshold (e.g., 0.10 for 10%)
   * @return
   *   (manifestsToKeep, manifestsToRewrite)
   */
  def selectivePartition(
    manifests: Seq[ManifestInfo],
    threshold: Double
  ): (Seq[ManifestInfo], Seq[ManifestInfo]) =
    manifests.partition { m =>
      if (m.numEntries == 0) {
        // Empty manifests - keep them (nothing to compact)
        true
      } else {
        val tombstoneRatio = m.tombstoneCount.toDouble / m.numEntries
        // Keep if below threshold
        tombstoneRatio <= threshold
      }
    }

  /**
   * Determine if selective compaction is beneficial.
   *
   * Selective compaction is worthwhile when:
   *   1. There are manifests that can be kept (clean) 2. The manifests to rewrite have significant entries
   *
   * @param keepManifests
   *   Manifests that would be kept as-is
   * @param rewriteManifests
   *   Manifests that need rewriting
   * @return
   *   true if selective compaction should be used
   */
  def isSelectiveCompactionBeneficial(
    keepManifests: Seq[ManifestInfo],
    rewriteManifests: Seq[ManifestInfo]
  ): Boolean = {

    // If nothing to keep, full compaction is same as selective
    if (keepManifests.isEmpty) {
      return false
    }

    // If nothing to rewrite, no compaction needed at all
    if (rewriteManifests.isEmpty) {
      return false
    }

    // Calculate savings
    val entriesToKeep    = keepManifests.map(_.numEntries).sum
    val entriesToRewrite = rewriteManifests.map(_.numEntries).sum
    val totalEntries     = entriesToKeep + entriesToRewrite

    // Selective is beneficial if we're skipping at least 10% of entries
    val savingsRatio = entriesToKeep.toDouble / totalEntries

    val beneficial = savingsRatio >= 0.10

    if (beneficial) {
      log.info(
        s"Selective compaction beneficial: keeping ${keepManifests.size} manifests " +
          s"($entriesToKeep entries), rewriting ${rewriteManifests.size} manifests " +
          s"($entriesToRewrite entries), savings=${(savingsRatio * 100).toInt}%"
      )
    } else {
      log.debug(s"Selective compaction not beneficial: savings only ${(savingsRatio * 100).toInt}%")
    }

    beneficial
  }
}
