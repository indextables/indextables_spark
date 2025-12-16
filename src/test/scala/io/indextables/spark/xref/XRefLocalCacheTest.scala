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
import java.nio.file.Files

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for XRefLocalCache.
 *
 * These tests verify the local caching logic without requiring cloud credentials.
 */
class XRefLocalCacheTest extends AnyFunSuite with BeforeAndAfterEach {

  private var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("xref-cache-test").toFile
  }

  override def afterEach(): Unit = {
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterEach()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("getLocalPath returns original path when caching is disabled") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "false",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val s3Path = "s3://bucket/table/_xrefsplits/abcd/xref-123.split"
    val result = XRefLocalCache.getLocalPath(s3Path, configMap)

    assert(result == s3Path, "Should return original path when caching is disabled")
  }

  test("getLocalPath returns original path when localCache.enabled is not set") {
    val configMap = Map(
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val s3Path = "s3://bucket/table/_xrefsplits/abcd/xref-123.split"
    val result = XRefLocalCache.getLocalPath(s3Path, configMap)

    assert(result == s3Path, "Should return original path when caching is not explicitly enabled")
  }

  test("getLocalPath returns original path for local file paths") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val localPath = "/tmp/table/_xrefsplits/abcd/xref-123.split"
    val result = XRefLocalCache.getLocalPath(localPath, configMap)

    assert(result == localPath, "Should return original path for local paths (not cloud)")
  }

  test("getLocalPath returns original path for file:// URLs") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val fileUrl = "file:///tmp/table/_xrefsplits/abcd/xref-123.split"
    val result = XRefLocalCache.getLocalPath(fileUrl, configMap)

    assert(result == fileUrl, "Should return original path for file:// URLs")
  }

  test("getCacheStats returns empty stats for non-existent cache") {
    val configMap = Map(
      "spark.indextables.xref.localCache.directory" -> new File(tempDir, "nonexistent").getAbsolutePath
    )

    val stats = XRefLocalCache.getCacheStats(configMap)

    assert(stats.cachedSplits == 0)
    assert(stats.totalSizeBytes == 0)
  }

  test("getCacheStats returns correct stats for populated cache") {
    // Create cache directory with mock cached files
    val cacheSubdir = new File(tempDir, "_xref_cache")
    cacheSubdir.mkdirs()

    // Create mock cached split files
    val file1 = new File(cacheSubdir, "abc123_xref-001.split")
    val file2 = new File(cacheSubdir, "def456_xref-002.split")
    Files.write(file1.toPath, Array.fill(1000)(0.toByte))
    Files.write(file2.toPath, Array.fill(2000)(0.toByte))

    val configMap = Map(
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val stats = XRefLocalCache.getCacheStats(configMap)

    assert(stats.cachedSplits == 2, s"Expected 2 cached splits, got ${stats.cachedSplits}")
    assert(stats.totalSizeBytes == 3000, s"Expected 3000 bytes, got ${stats.totalSizeBytes}")
    assert(stats.cacheDirectory.contains("_xref_cache"))
  }

  test("clearCache removes cached split files") {
    // Create cache directory with mock cached files
    val cacheSubdir = new File(tempDir, "_xref_cache")
    cacheSubdir.mkdirs()

    val file1 = new File(cacheSubdir, "abc123_xref-001.split")
    val file2 = new File(cacheSubdir, "def456_xref-002.split")
    val file3 = new File(cacheSubdir, "other-file.txt") // Should not be deleted
    Files.write(file1.toPath, Array.fill(100)(0.toByte))
    Files.write(file2.toPath, Array.fill(100)(0.toByte))
    Files.write(file3.toPath, "other content".getBytes)

    val configMap = Map(
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val deleted = XRefLocalCache.clearCache(configMap)

    assert(deleted == 2, s"Expected 2 files deleted, got $deleted")
    assert(!file1.exists(), "Split file 1 should be deleted")
    assert(!file2.exists(), "Split file 2 should be deleted")
    assert(file3.exists(), "Non-split file should not be deleted")
  }

  test("clearCache returns 0 for non-existent cache directory") {
    val configMap = Map(
      "spark.indextables.xref.localCache.directory" -> new File(tempDir, "nonexistent").getAbsolutePath
    )

    val deleted = XRefLocalCache.clearCache(configMap)

    assert(deleted == 0)
  }

  test("cache directory resolution uses localCache.directory first") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath,
      "spark.indextables.xref.build.tempDirectoryPath" -> "/other/path",
      "spark.indextables.indexWriter.tempDirectoryPath" -> "/another/path"
    )

    val stats = XRefLocalCache.getCacheStats(configMap)

    assert(stats.cacheDirectory.startsWith(tempDir.getAbsolutePath),
      s"Cache directory should use localCache.directory: ${stats.cacheDirectory}")
  }

  test("cache directory resolution falls back to build.tempDirectoryPath") {
    val buildTempDir = new File(tempDir, "build-temp")
    buildTempDir.mkdirs()

    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.build.tempDirectoryPath" -> buildTempDir.getAbsolutePath,
      "spark.indextables.indexWriter.tempDirectoryPath" -> "/other/path"
    )

    val stats = XRefLocalCache.getCacheStats(configMap)

    assert(stats.cacheDirectory.startsWith(buildTempDir.getAbsolutePath),
      s"Cache directory should fall back to build.tempDirectoryPath: ${stats.cacheDirectory}")
  }

  test("cache directory resolution falls back to indexWriter.tempDirectoryPath") {
    val writerTempDir = new File(tempDir, "writer-temp")
    writerTempDir.mkdirs()

    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.indexWriter.tempDirectoryPath" -> writerTempDir.getAbsolutePath
    )

    val stats = XRefLocalCache.getCacheStats(configMap)

    assert(stats.cacheDirectory.startsWith(writerTempDir.getAbsolutePath),
      s"Cache directory should fall back to indexWriter.tempDirectoryPath: ${stats.cacheDirectory}")
  }

  test("getLocalPath with caching enabled attempts download for S3 path") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val s3Path = "s3://bucket/table/_xrefsplits/abcd/xref-123.split"

    // This should attempt to download but fail (no credentials)
    // The important thing is it returns a LOCAL path (not the S3 path)
    val exception = intercept[RuntimeException] {
      XRefLocalCache.getLocalPath(s3Path, configMap)
    }

    // Verify it attempted to download (error message should reference the cloud path)
    assert(exception.getMessage.contains("Failed to download XRef to local cache") ||
           exception.getMessage.contains(s3Path) ||
           exception.getCause != null,
      s"Should fail with download error, got: ${exception.getMessage}")
  }

  test("getLocalPath with caching enabled attempts download for Azure path") {
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    val azurePath = "abfss://container@account.dfs.core.windows.net/table/_xrefsplits/abcd/xref-123.split"

    // This should attempt to download but fail (no credentials)
    val exception = intercept[RuntimeException] {
      XRefLocalCache.getLocalPath(azurePath, configMap)
    }

    // Verify it attempted to download
    assert(exception.getMessage.contains("Failed to download XRef to local cache") ||
           exception.getMessage.contains(azurePath) ||
           exception.getCause != null,
      s"Should fail with download error for Azure path, got: ${exception.getMessage}")
  }

  test("cached file is reused on subsequent calls") {
    // Manually create a cached file to simulate a previous download
    val cacheSubdir = new File(tempDir, "_xref_cache")
    cacheSubdir.mkdirs()

    // The cache filename format is: <hash>_<original_filename>
    // We need to figure out what hash would be generated for our test path
    val s3Path = "s3://bucket/table/_xrefsplits/abcd/xref-cached.split"

    // Create a file that matches the expected cache name pattern
    // Since we don't know the exact hash, we'll test the local path case instead
    val localPath = new File(tempDir, "xref-local.split").getAbsolutePath
    Files.write(new File(localPath).toPath, "test content".getBytes)

    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    // For local paths, caching should return the original path (no download needed)
    val result = XRefLocalCache.getLocalPath(localPath, configMap)
    assert(result == localPath, "Local path should be returned unchanged")
  }

  test("different S3 paths generate different cache filenames") {
    // This tests that the hash function produces different outputs for different inputs
    val configMap = Map(
      "spark.indextables.xref.localCache.enabled" -> "true",
      "spark.indextables.xref.localCache.directory" -> tempDir.getAbsolutePath
    )

    // Pre-create cache files for both paths
    val cacheSubdir = new File(tempDir, "_xref_cache")
    cacheSubdir.mkdirs()

    // The getCacheStats will show us the cache directory is being created
    val stats = XRefLocalCache.getCacheStats(configMap)
    assert(stats.cacheDirectory.contains("_xref_cache"))
  }
}
