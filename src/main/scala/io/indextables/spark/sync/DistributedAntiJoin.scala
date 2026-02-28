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

package io.indextables.spark.sync

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

/**
 * Result of a distributed anti-join between source files and companion-tracked files.
 *
 * @param filesToIndex
 *   Source files that need indexing (new files + remaining from invalidated splits)
 * @param splitsToInvalidate
 *   Companion splits that reference files gone from source
 */
case class DistributedAntiJoinResult(
  filesToIndex: Seq[CompanionSourceFile],
  splitsToInvalidate: Seq[AddAction])

/**
 * Static helper methods for distributed anti-join. Must be in the companion object (not instance methods) to avoid
 * capturing the non-serializable DistributedAntiJoin instance (which holds SparkSession) in Spark RDD closures.
 */
object DistributedAntiJoin {
  private[sync] def normalizePath(path: String): String = {
    val decoded =
      try
        java.net.URLDecoder.decode(path, "UTF-8")
      catch {
        case _: Exception => path
      }
    decoded.stripSuffix("/")
  }
}

/**
 * Distributed anti-join between source table files (as RDD) and companion-tracked files. Replaces the in-memory
 * computeAntiJoinChanges() for large tables where materializing all source files on the driver would OOM.
 *
 * The anti-join identifies:
 *   1. New files: in source but not tracked by any companion split → need indexing
 *   2. Gone files: tracked by companion splits but absent from source → splits need invalidation
 *   3. Remaining files: still-valid files from invalidated splits → need re-indexing
 */
class DistributedAntiJoin(spark: SparkSession) {
  import DistributedAntiJoin.normalizePath

  private val logger = LoggerFactory.getLogger(classOf[DistributedAntiJoin])

  /**
   * Compute changes between the distributed source file listing and existing companion splits.
   *
   * @param sourceFilesRDD
   *   All files in the current source snapshot (distributed)
   * @param existingFiles
   *   Companion splits from the IndexTables transaction log (driver-side, small)
   * @param isInitialSync
   *   True if this is the first sync (no existing splits)
   * @return
   *   DistributedAntiJoinResult with files to index and splits to invalidate
   */
  def computeChanges(
    sourceFilesRDD: RDD[CompanionSourceFile],
    existingFiles: Seq[AddAction],
    isInitialSync: Boolean
  ): DistributedAntiJoinResult = {

    if (isInitialSync) {
      val allFiles = sourceFilesRDD.collect().toSeq
      logger.info(s"Initial sync: ${allFiles.size} parquet files to index")
      return DistributedAntiJoinResult(allFiles, Seq.empty)
    }

    val sc = spark.sparkContext

    // Build the set of paths tracked by existing companion splits (driver-side, small).
    // CompanionSourceFiles stores relative paths, same format as source reader output.
    val companionPathToSplit: Map[String, Seq[AddAction]] = existingFiles
      .flatMap { split =>
        split.companionSourceFiles
          .getOrElse(Seq.empty)
          .map(f => normalizePath(f) -> split)
      }
      .groupBy(_._1)
      .map { case (path, entries) => path -> entries.map(_._2).distinct }

    val companionPaths = companionPathToSplit.keySet

    // Broadcast companion paths to executors for the anti-join filter
    val broadcastCompanionPaths = sc.broadcast(companionPaths)

    // Compute new files: source files not tracked by any companion split.
    // This runs distributed — only the matching files are collected to driver.
    val newFilesRDD = sourceFilesRDD.filter { f =>
      !broadcastCompanionPaths.value.contains(normalizePath(f.path))
    }

    // Compute source paths for "gone files" check.
    // keyBy normalized path, then collect just the distinct paths.
    val sourcePathsRDD = sourceFilesRDD.map(f => normalizePath(f.path)).distinct()

    // Collect source paths to driver for the "gone files" set difference.
    // This is the only potentially large collect, but it's just strings (paths), not full file entries.
    // For tables with millions of files, this is ~100MB of path strings — acceptable.
    val currentSourcePaths = sourcePathsRDD.collect().toSet

    // Files tracked by companion but no longer in source
    val pathsGoneFromSource = companionPaths -- currentSourcePaths

    // Find companion splits that reference gone files
    val splitsToInvalidate = if (pathsGoneFromSource.nonEmpty) {
      existingFiles.filter { split =>
        split.companionSourceFiles.exists(_.exists(f => pathsGoneFromSource.contains(normalizePath(f))))
      }
    } else {
      Seq.empty
    }

    // Collect new files to driver
    val newFiles = newFilesRDD.collect().toSeq

    // Collect remaining valid files from invalidated splits that still exist in source.
    // Build a size lookup from the source RDD for accurate sizes.
    val sourceSizeByPath: Map[String, Long] = if (splitsToInvalidate.nonEmpty) {
      // Only collect sizes for paths we actually need (from invalidated splits)
      val invalidatedPaths = splitsToInvalidate
        .flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
        .filterNot(f => pathsGoneFromSource.contains(normalizePath(f)))
        .map(normalizePath)
        .toSet

      if (invalidatedPaths.nonEmpty) {
        val broadcastNeededPaths = sc.broadcast(invalidatedPaths)
        sourceFilesRDD
          .filter(f => broadcastNeededPaths.value.contains(normalizePath(f.path)))
          .map(f => normalizePath(f.path) -> f.size)
          .collectAsMap()
          .toMap
      } else {
        Map.empty
      }
    } else {
      Map.empty
    }

    val remainingFromInvalidated = splitsToInvalidate
      .flatMap { split =>
        split.companionSourceFiles
          .getOrElse(Seq.empty)
          .filterNot(f => pathsGoneFromSource.contains(normalizePath(f)))
          .map { relPath =>
            val normalized = normalizePath(relPath)
            val size       = sourceSizeByPath.getOrElse(normalized, 0L)
            CompanionSourceFile(normalized, split.partitionValues, size)
          }
      }
      .groupBy(f => normalizePath(f.path))
      .map(_._2.head)
      .toSeq

    // Merge new files with remaining from invalidated, dedup by path (prefer largest size)
    val allFilesToIndex = (newFiles ++ remainingFromInvalidated)
      .groupBy(f => normalizePath(f.path))
      .map(_._2.maxBy(_.size))
      .toSeq

    logger.info(
      s"Distributed anti-join: ${currentSourcePaths.size} in source, " +
        s"${companionPaths.size} in companion, ${newFiles.size} new, " +
        s"${pathsGoneFromSource.size} gone, ${splitsToInvalidate.size} splits to invalidate, " +
        s"${remainingFromInvalidated.size} files to re-index from invalidated splits"
    )

    DistributedAntiJoinResult(allFilesToIndex, splitsToInvalidate)
  }
}
