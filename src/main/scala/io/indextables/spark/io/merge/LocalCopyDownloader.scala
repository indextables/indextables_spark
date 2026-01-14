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

package io.indextables.spark.io.merge

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.concurrent.CompletableFuture

import org.slf4j.LoggerFactory

/**
 * Local filesystem "downloader" that copies files from one local path to another.
 *
 * Used for non-cloud tables where source files are already on the local filesystem or a shared filesystem (NFS, HDFS
 * via FUSE, etc.).
 */
class LocalCopyDownloader extends AsyncDownloader {

  private val logger = LoggerFactory.getLogger(classOf[LocalCopyDownloader])

  override def protocol: String = "file"

  override def canHandle(path: String): Boolean =
    path.startsWith("file://") || path.startsWith("/") || !path.contains("://")

  override def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult] = {
    val future = new CompletableFuture[DownloadResult]()

    // Run the copy operation asynchronously
    CompletableFuture.runAsync(
      new Runnable {
        override def run(): Unit = {
          val startTime = System.currentTimeMillis()

          try {
            val sourcePath = normalizeLocalPath(request.sourcePath)
            val destPath   = request.destinationPath

            val sourceFile = new File(sourcePath)
            val destFile   = new File(destPath)

            // Check that source file exists
            if (!sourceFile.exists()) {
              throw new java.io.FileNotFoundException(s"Source file does not exist: $sourcePath")
            }

            // Ensure parent directory exists
            val parentDir = destFile.getParentFile
            if (parentDir != null && !parentDir.exists()) {
              parentDir.mkdirs()
            }

            logger.debug(s"Copying local file: $sourcePath -> $destPath")

            // Copy the file
            Files.copy(sourceFile.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)

            val durationMs = System.currentTimeMillis() - startTime
            val fileSize   = destFile.length()

            logger.debug(s"Copied $sourcePath ($fileSize bytes in ${durationMs}ms)")

            future.complete(
              DownloadResult.success(
                request = request,
                localPath = destFile.getAbsolutePath,
                bytesDownloaded = fileSize,
                durationMs = durationMs
              )
            )
          } catch {
            case ex: Exception =>
              val durationMs = System.currentTimeMillis() - startTime

              logger.warn(s"Failed to copy file: ${ex.getMessage}")

              future.complete(
                DownloadResult.failure(
                  request = request,
                  error = ex,
                  durationMs = durationMs
                )
              )
          }
        }
      },
      MergeIOThreadPools.downloadCoordinationPool
    )

    future
  }

  /**
   * Normalize a local path by removing file:// prefix if present.
   */
  private def normalizeLocalPath(path: String): String =
    if (path.startsWith("file://")) {
      path.stripPrefix("file://")
    } else {
      path
    }
}
