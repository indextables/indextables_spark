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

package io.indextables.spark.io

import java.io.File
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for S3 pagination fix.
 *
 * Background: S3's listObjectsV2 API returns a maximum of 1000 objects per request. Without proper pagination handling,
 * directories with >1000 files would only return partial results, causing data corruption in PURGE operations.
 *
 * The fix uses s3Client.listObjectsV2Paginator() which automatically handles pagination.
 *
 * These tests verify:
 *   1. Local filesystem listing works with >1000 files (flow test) 2. CloudStorageProvider interface handles large file
 *      counts correctly
 *
 * For real S3 testing, see RealS3PaginationTest (requires AWS credentials).
 */
class S3PaginationTest extends AnyFunSuite with TestBase {

  private def createTempDir(): File = {
    val dir = new File(System.getProperty("java.io.tmpdir"), s"s3_pagination_test_${UUID.randomUUID()}")
    dir.mkdirs()
    dir
  }

  // Use inherited deleteRecursively from TestBase

  test("HadoopCloudStorageProvider should list more than 1000 files") {
    val tempDir = createTempDir()

    try {
      val numFiles = 1500 // More than S3's 1000 limit
      val testPath = new File(tempDir, "many_files")
      testPath.mkdirs()

      // Create 1500 files
      println(s"Creating $numFiles test files...")
      (1 to numFiles).foreach { i =>
        val file = new File(testPath, f"file_$i%05d.split")
        file.createNewFile()
      }
      println(s"Created $numFiles files in ${testPath.getAbsolutePath}")

      // Use HadoopCloudStorageProvider to list them
      val hadoopConf = new Configuration()
      val provider   = new HadoopCloudStorageProvider(hadoopConf)

      try {
        val files = provider.listFiles(testPath.getAbsolutePath, recursive = false)

        println(s"Listed ${files.size} files")
        assert(
          files.size == numFiles,
          s"Expected $numFiles files but got ${files.size}. " +
            "This would indicate a pagination bug if this were S3."
        )

        // Verify all files are present
        val fileNames = files.map(f => new Path(f.path).getName).toSet
        (1 to numFiles).foreach { i =>
          val expectedName = f"file_$i%05d.split"
          assert(fileNames.contains(expectedName), s"Missing file: $expectedName")
        }

        println(s"✓ Successfully listed all $numFiles files")
      } finally
        provider.close()
    } finally
      deleteRecursively(tempDir)
  }

  test("HadoopCloudStorageProvider should list more than 1000 files recursively") {
    val tempDir = createTempDir()

    try {
      val filesPerDir = 400
      val numDirs     = 4
      val totalFiles  = filesPerDir * numDirs // 1600 files total

      // Create nested directory structure like partitioned tables
      println(s"Creating $totalFiles test files across $numDirs directories...")
      (1 to numDirs).foreach { dirNum =>
        val subDir = new File(tempDir, s"partition=$dirNum")
        subDir.mkdirs()

        (1 to filesPerDir).foreach { fileNum =>
          val file = new File(subDir, f"file_$fileNum%05d.split")
          file.createNewFile()
        }
      }
      println(s"Created $totalFiles files in ${tempDir.getAbsolutePath}")

      // Use HadoopCloudStorageProvider to list them recursively
      val hadoopConf = new Configuration()
      val provider   = new HadoopCloudStorageProvider(hadoopConf)

      try {
        val files = provider.listFiles(tempDir.getAbsolutePath, recursive = true)

        println(s"Listed ${files.size} files recursively")
        assert(
          files.size == totalFiles,
          s"Expected $totalFiles files but got ${files.size}. " +
            "This would indicate a pagination bug in recursive listing."
        )

        println(s"✓ Successfully listed all $totalFiles files recursively")
      } finally
        provider.close()
    } finally
      deleteRecursively(tempDir)
  }

  test("CloudStorageProvider listing should handle empty directories") {
    val tempDir = createTempDir()

    try {
      val hadoopConf = new Configuration()
      val provider   = new HadoopCloudStorageProvider(hadoopConf)

      try {
        val files = provider.listFiles(tempDir.getAbsolutePath, recursive = false)
        assert(files.isEmpty, "Empty directory should return empty list")
        println("✓ Empty directory handled correctly")
      } finally
        provider.close()
    } finally
      deleteRecursively(tempDir)
  }

  /**
   * This test documents the S3 pagination fix.
   *
   * BEFORE THE FIX (buggy code):
   * {{{
   * val response = s3Client.listObjectsV2(request)
   * val files = response.contents().asScala.map { ... }
   * // BUG: Only returns first 1000 files!
   * }}}
   *
   * AFTER THE FIX:
   * {{{
   * val paginator = s3Client.listObjectsV2Paginator(request)
   * paginator.forEach { response =>
   *   response.contents().asScala.foreach { ... }
   * }
   * // FIXED: Iterates through ALL pages
   * }}}
   *
   * To test with real S3:
   *   1. Set AWS credentials in environment or spark config 2. Create a test bucket with >1000 files 3. Run
   *      RealS3PaginationTest
   */
  test("S3 pagination fix documentation") {
    // This test just documents the fix - see method scaladoc above
    println("S3 pagination fix:")
    println("  - S3 listObjectsV2 returns max 1000 objects per request")
    println("  - Without pagination, PURGE would only see first 1000 files")
    println("  - This caused files beyond 1000 to be incorrectly marked as orphaned")
    println("  - Fix: Use listObjectsV2Paginator() which handles continuation tokens")
    println("✓ Documentation test passed")
  }
}
