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

package io.indextables.spark.xref

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.MessageDigest

import org.apache.hadoop.conf.Configuration

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.util.CloudConfigExtractor
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * Manages local caching of XRef splits on executors.
 *
 * When enabled, XRef splits are lazily downloaded from cloud storage to local disk
 * before being searched. This improves performance for repeated queries by avoiding
 * repeated cloud storage reads.
 *
 * The cache uses a content-addressable naming scheme based on the XRef path hash
 * to ensure uniqueness and enable safe concurrent access.
 */
object XRefLocalCache {

  private val logger = LoggerFactory.getLogger(getClass)

  // Cache subdirectory name
  private val CACHE_SUBDIR = "_xref_cache"

  /**
   * Get the local path for an XRef split, downloading if necessary.
   *
   * If local caching is disabled, returns the original path unchanged.
   * If local caching is enabled:
   *   - Checks if XRef already exists in local cache
   *   - If not, downloads from cloud storage to local cache
   *   - Returns the local cache path
   *
   * @param xrefPath Original XRef path (may be cloud or local)
   * @param configMap Configuration map for cloud credentials and cache settings
   * @return Path to use for XRef access (local cache path if caching enabled)
   */
  def getLocalPath(
    xrefPath: String,
    configMap: Map[String, String]
  ): String = {
    // Check if local caching is enabled
    val localCacheEnabled = configMap
      .get("spark.indextables.xref.localCache.enabled")
      .exists(_.toBoolean)

    if (!localCacheEnabled) {
      logger.debug(s"Local cache disabled, using original path: $xrefPath")
      return xrefPath
    }

    // Check if path is a cloud path that needs caching
    if (!CloudConfigExtractor.isCloudPath(xrefPath)) {
      logger.debug(s"Not a cloud path, using original: $xrefPath")
      return xrefPath
    }

    // Resolve cache directory
    val cacheDir = resolveCacheDirectory(configMap)
    val localPath = getLocalCachePath(xrefPath, cacheDir)

    // Check if already cached
    val localFile = new File(localPath)
    if (localFile.exists() && localFile.length() > 0) {
      logger.debug(s"XRef already cached locally: $localPath")
      return localPath
    }

    // Download to local cache
    logger.info(s"Downloading XRef to local cache: $xrefPath -> $localPath")
    downloadToLocalCache(xrefPath, localPath, configMap)

    localPath
  }


  /**
   * Resolve the cache directory to use.
   *
   * Priority:
   * 1. spark.indextables.xref.localCache.directory
   * 2. spark.indextables.xref.build.tempDirectoryPath
   * 3. spark.indextables.indexWriter.tempDirectoryPath
   * 4. /local_disk0/tmp (if exists)
   * 5. System temp directory
   */
  private def resolveCacheDirectory(configMap: Map[String, String]): File = {
    val basePath = configMap.get("spark.indextables.xref.localCache.directory")
      .orElse(configMap.get("spark.indextables.xref.build.tempDirectoryPath"))
      .orElse(configMap.get("spark.indextables.indexWriter.tempDirectoryPath"))
      .orElse(detectLocalDisk0())
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val cacheDir = new File(basePath, CACHE_SUBDIR)
    if (!cacheDir.exists()) {
      logger.info(s"Creating XRef local cache directory: ${cacheDir.getAbsolutePath}")
      cacheDir.mkdirs()
    }
    cacheDir
  }

  /**
   * Generate a local cache path for an XRef.
   *
   * Uses a hash of the original path to create a unique, predictable filename.
   */
  private def getLocalCachePath(xrefPath: String, cacheDir: File): String = {
    // Create a hash of the path for uniqueness
    val pathHash = hashPath(xrefPath)
    val fileName = extractFileName(xrefPath)

    // Use format: <hash>_<original_filename>
    new File(cacheDir, s"${pathHash}_$fileName").getAbsolutePath
  }

  /**
   * Download an XRef split from cloud storage to local cache.
   * Uses configMap for credentials (legacy method for backward compatibility).
   */
  private def downloadToLocalCache(
    cloudPath: String,
    localPath: String,
    configMap: Map[String, String]
  ): Unit = {
    // Ensure parent directory exists
    val localFile = new File(localPath)
    val parentDir = localFile.getParentFile
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs()
    }

    // Use a temp file for atomic download
    val tempFile = new File(localPath + ".tmp")

    try {
      // Create cloud provider with properly constructed options and hadoop config
      val options = new java.util.HashMap[String, String]()
      configMap.foreach { case (k, v) => if (v != null) options.put(k, v) }
      val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)

      // Create Hadoop configuration with credentials from configMap
      val hadoopConf = new Configuration()
      configMap.foreach { case (k, v) => if (v != null) hadoopConf.set(k, v) }

      val cloudProvider = CloudStorageProviderFactory.createProvider(cloudPath, caseInsensitiveOptions, hadoopConf)
      try {
        // Download file content
        val content = cloudProvider.readFile(cloudPath)

        // Write to temp file
        Files.write(tempFile.toPath, content)
      } finally {
        cloudProvider.close()
      }

      // Atomic rename to final location
      Files.move(tempFile.toPath, localFile.toPath, StandardCopyOption.ATOMIC_MOVE)

      logger.info(s"Downloaded XRef to local cache: ${localFile.length()} bytes")
    } catch {
      case e: Exception =>
        // Clean up temp file on failure
        if (tempFile.exists()) {
          tempFile.delete()
        }
        throw new RuntimeException(s"Failed to download XRef to local cache: $cloudPath", e)
    }
  }

  /**
   * Generate a short hash of a path for use in cache filenames.
   */
  private def hashPath(path: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    val hash = digest.digest(path.getBytes("UTF-8"))
    // Use first 8 bytes for a 16-character hex string
    hash.take(8).map(b => f"$b%02x").mkString
  }

  /**
   * Extract filename from path.
   */
  private def extractFileName(path: String): String = {
    path.split('/').last
  }

  /**
   * Detect /local_disk0 if available (Databricks/EMR).
   */
  private def detectLocalDisk0(): Option[String] = {
    val localDisk0 = new File("/local_disk0/tmp")
    if (localDisk0.exists() || localDisk0.mkdirs()) {
      Some("/local_disk0/tmp")
    } else {
      None
    }
  }

  /**
   * Clear all cached XRef splits.
   *
   * This is useful for testing or when cache becomes stale.
   */
  def clearCache(configMap: Map[String, String]): Int = {
    val cacheDir = resolveCacheDirectory(configMap)
    if (!cacheDir.exists()) {
      return 0
    }

    var deleted = 0
    cacheDir.listFiles().foreach { file =>
      if (file.isFile && file.getName.endsWith(".split")) {
        if (file.delete()) {
          deleted += 1
        }
      }
    }

    logger.info(s"Cleared $deleted cached XRef splits from ${cacheDir.getAbsolutePath}")
    deleted
  }

  /**
   * Get cache statistics.
   */
  def getCacheStats(configMap: Map[String, String]): XRefCacheStats = {
    val cacheDir = resolveCacheDirectory(configMap)
    if (!cacheDir.exists()) {
      return XRefCacheStats(0, 0, cacheDir.getAbsolutePath)
    }

    val files = cacheDir.listFiles().filter(f => f.isFile && f.getName.endsWith(".split"))
    val totalSize = files.map(_.length()).sum

    XRefCacheStats(files.length, totalSize, cacheDir.getAbsolutePath)
  }
}

/**
 * Statistics about the XRef local cache.
 */
case class XRefCacheStats(
  cachedSplits: Int,
  totalSizeBytes: Long,
  cacheDirectory: String
)
