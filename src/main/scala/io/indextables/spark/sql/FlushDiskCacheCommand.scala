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

package io.indextables.spark.sql

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{LongType, StringType}

import io.indextables.spark.config.IndexTables4SparkSQLConf
import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager, SplitCacheConfig}
import org.slf4j.LoggerFactory

/**
 * SQL command to flush IndexTables4Spark disk cache across all executors.
 *
 * Syntax: FLUSH INDEXTABLES DISK CACHE
 *
 * This command:
 *   1. Flushes all JVM-wide split cache managers (closes disk cache handles)
 *   2. Deletes all disk cache files on all executors
 *   3. Clears split locality assignments
 *   4. Clears prewarm state tracking
 *
 * This is useful for:
 *   - Testing: Ensuring clean cache state between tests
 *   - Maintenance: Clearing stale cache entries
 *   - Troubleshooting: Resetting cache when issues are suspected
 */
case class FlushDiskCacheCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[FlushDiskCacheCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("executor_id", StringType, nullable = false)(),
    AttributeReference("cache_type", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("bytes_freed", LongType, nullable = false)(),
    AttributeReference("files_deleted", LongType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Executing FLUSH INDEXTABLES DISK CACHE command")

    val sc = sparkSession.sparkContext
    val results = scala.collection.mutable.ArrayBuffer[Row]()

    // Get the disk cache path using the same resolution logic as SplitCacheConfig:
    // 1. Explicitly configured path (spark.indextables.cache.disk.path)
    // 2. Auto-detected default path (/local_disk0/tantivy4spark_slicecache if available)
    val explicitPath = sparkSession.conf.getOption(IndexTables4SparkSQLConf.TANTIVY4SPARK_DISK_CACHE_PATH)
    val effectiveDiskCachePath = explicitPath.orElse(SplitCacheConfig.getDefaultDiskCachePath())
    logger.info(s"Disk cache path - explicit: ${explicitPath.getOrElse("not configured")}, " +
      s"effective: ${effectiveDiskCachePath.getOrElse("none")}")

    // Broadcast the effective path to executors
    val diskCachePathBroadcast = sc.broadcast(effectiveDiskCachePath)

    try {
      // 1. Clear driver-side state first
      val driverResult = flushDriverState(effectiveDiskCachePath)
      results ++= driverResult

      // 2. Flush disk cache on all executors
      val numExecutors = math.max(1, sc.getExecutorMemoryStatus.size)

      val executorResults = sc
        .parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val executorId = s"executor-$index"
          val diskCachePath = diskCachePathBroadcast.value
          Iterator(flushExecutorDiskCache(executorId, diskCachePath))
        }
        .collect()
        .toSeq

      results ++= executorResults

      logger.info(s"FLUSH INDEXTABLES DISK CACHE completed. Total results: ${results.length}")

    } catch {
      case ex: Exception =>
        logger.error("Error executing FLUSH INDEXTABLES DISK CACHE", ex)
        results += Row(
          "driver",
          "error",
          "failed",
          0L,
          0L,
          s"Disk cache flush failed: ${ex.getMessage}"
        )
    } finally {
      diskCachePathBroadcast.destroy()
    }

    results.toSeq
  }

  private def flushDriverState(configuredDiskCachePath: Option[String]): Seq[Row] = {
    val results = scala.collection.mutable.ArrayBuffer[Row]()

    // 1. Flush split cache managers (closes disk cache handles)
    try {
      val splitCacheResult = GlobalSplitCacheManager.flushAllCaches()
      results += Row(
        "driver",
        "split_cache",
        "success",
        0L,
        splitCacheResult.flushedManagers.toLong,
        s"Flushed ${splitCacheResult.flushedManagers} split cache managers"
      )
    } catch {
      case ex: Exception =>
        results += Row(
          "driver",
          "split_cache",
          "failed",
          0L,
          0L,
          s"Failed to flush split cache: ${ex.getMessage}"
        )
    }

    // 2. Clear locality manager and prewarm state
    try {
      DriverSplitLocalityManager.clear()
      results += Row(
        "driver",
        "locality_manager",
        "success",
        0L,
        0L,
        "Cleared split locality assignments and prewarm state"
      )
    } catch {
      case ex: Exception =>
        results += Row(
          "driver",
          "locality_manager",
          "failed",
          0L,
          0L,
          s"Failed to clear locality manager: ${ex.getMessage}"
        )
    }

    // 3. Delete driver disk cache files using the configured path
    try {
      val diskCacheResult = deleteDiskCacheFiles(configuredDiskCachePath)
      results += Row(
        "driver",
        "disk_cache_files",
        if (diskCacheResult.success) "success" else "skipped",
        diskCacheResult.bytesFreed,
        diskCacheResult.filesDeleted,
        diskCacheResult.message
      )
    } catch {
      case ex: Exception =>
        results += Row(
          "driver",
          "disk_cache_files",
          "failed",
          0L,
          0L,
          s"Failed to delete disk cache files: ${ex.getMessage}"
        )
    }

    results.toSeq
  }

  private def flushExecutorDiskCache(executorId: String, configuredDiskCachePath: Option[String]): Row =
    try {
      // 1. Flush split cache managers on this executor
      GlobalSplitCacheManager.flushAllCaches()

      // 2. Delete disk cache files on this executor using the configured path
      val diskCacheResult = deleteDiskCacheFiles(configuredDiskCachePath)

      Row(
        executorId,
        "disk_cache",
        if (diskCacheResult.success) "success" else "skipped",
        diskCacheResult.bytesFreed,
        diskCacheResult.filesDeleted,
        diskCacheResult.message
      )
    } catch {
      case ex: Exception =>
        Row(
          executorId,
          "disk_cache",
          "failed",
          0L,
          0L,
          s"Failed: ${ex.getMessage}"
        )
    }

  private def deleteDiskCacheFiles(configuredDiskCachePath: Option[String]): DiskCacheDeleteResult =
    configuredDiskCachePath match {
      case Some(path) =>
        // Use the explicitly configured disk cache path
        val cacheDir = new File(path)
        if (cacheDir.exists() && cacheDir.isDirectory) {
          val (bytes, files) = deleteDirectoryContents(cacheDir)
          DiskCacheDeleteResult(
            success = true,
            bytesFreed = bytes,
            filesDeleted = files,
            message = s"Deleted $files files ($bytes bytes) from configured path: $path"
          )
        } else {
          DiskCacheDeleteResult(
            success = true,
            bytesFreed = 0L,
            filesDeleted = 0L,
            message = s"Configured disk cache path does not exist or is not a directory: $path"
          )
        }
      case None =>
        // No disk cache path configured - nothing to flush
        DiskCacheDeleteResult(
          success = false,
          bytesFreed = 0L,
          filesDeleted = 0L,
          message = "No disk cache path configured (spark.indextables.cache.disk.path not set)"
        )
    }

  private def deleteDirectoryContents(dir: File): (Long, Long) = {
    var bytesFreed = 0L
    var filesDeleted = 0L

    if (dir.exists() && dir.isDirectory) {
      val files = dir.listFiles()
      if (files != null) {
        files.foreach { file =>
          if (file.isDirectory) {
            val (subBytes, subFiles) = deleteDirectoryContents(file)
            bytesFreed += subBytes
            filesDeleted += subFiles
            file.delete()
          } else {
            bytesFreed += file.length()
            file.delete()
            filesDeleted += 1
          }
        }
      }
    }

    (bytesFreed, filesDeleted)
  }
}

case class DiskCacheDeleteResult(
  success: Boolean,
  bytesFreed: Long,
  filesDeleted: Long,
  message: String)
