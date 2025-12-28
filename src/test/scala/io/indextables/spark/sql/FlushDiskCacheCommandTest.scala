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
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

/**
 * Tests for FLUSH INDEXTABLES DISK CACHE command.
 *
 * Tests cover:
 *   - SQL parsing for both INDEXTABLES and TANTIVY4SPARK keywords
 *   - Command execution with and without disk cache configured
 *   - Output schema validation
 *   - Integration with GlobalSplitCacheManager and DriverSplitLocalityManager
 */
class FlushDiskCacheCommandTest extends AnyFunSuite with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[FlushDiskCacheCommandTest])

  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FlushDiskCacheCommandTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  // ===== Parsing Tests =====

  test("FLUSH INDEXTABLES DISK CACHE should parse correctly") {
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows = result.collect()

    assert(rows.nonEmpty, "FLUSH command should return results")
    logger.info(s"FLUSH INDEXTABLES DISK CACHE returned ${rows.length} rows")
  }

  test("FLUSH TANTIVY4SPARK DISK CACHE should parse correctly (alternate syntax)") {
    val result = spark.sql("FLUSH TANTIVY4SPARK DISK CACHE")
    val rows = result.collect()

    assert(rows.nonEmpty, "FLUSH command with TANTIVY4SPARK keyword should return results")
    logger.info(s"FLUSH TANTIVY4SPARK DISK CACHE returned ${rows.length} rows")
  }

  test("FLUSH command should be case-insensitive") {
    val result1 = spark.sql("flush indextables disk cache")
    val result2 = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val result3 = spark.sql("Flush IndexTables Disk Cache")

    assert(result1.collect().nonEmpty, "lowercase should work")
    assert(result2.collect().nonEmpty, "uppercase should work")
    assert(result3.collect().nonEmpty, "mixed case should work")
  }

  // ===== Output Schema Tests =====

  test("FLUSH command output should have correct schema") {
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val schema = result.schema

    val expectedColumns = Set(
      "executor_id",
      "cache_type",
      "status",
      "bytes_freed",
      "files_deleted",
      "message"
    )

    val actualColumns = schema.fieldNames.toSet
    assert(expectedColumns.subsetOf(actualColumns),
      s"Schema should contain expected columns. Expected: $expectedColumns, Actual: $actualColumns")

    logger.info(s"FLUSH command schema: ${schema.fieldNames.mkString(", ")}")
  }

  test("FLUSH command should return driver results") {
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows = result.collect()

    // Should have at least driver-side results
    val driverRows = rows.filter(_.getAs[String]("executor_id") == "driver")
    assert(driverRows.nonEmpty, "Should have driver results")

    // Driver should have multiple cache_type entries
    val cacheTypes = driverRows.map(_.getAs[String]("cache_type")).toSet
    logger.info(s"Driver cache types: $cacheTypes")

    assert(cacheTypes.contains("split_cache"), "Should have split_cache result")
    assert(cacheTypes.contains("locality_manager"), "Should have locality_manager result")
  }

  // ===== Behavior Tests =====

  test("FLUSH without configured disk cache should return skipped status") {
    // No disk cache path configured
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows = result.collect()

    val diskCacheRows = rows.filter(_.getAs[String]("cache_type").contains("disk_cache"))
    diskCacheRows.foreach { row =>
      val status = row.getAs[String]("status")
      val message = row.getAs[String]("message")
      logger.info(s"Disk cache status: $status, message: $message")

      // Without configured path, should be skipped or report no path
      assert(status == "skipped" || status == "success",
        s"Without configured path, status should be skipped or success, got: $status")
    }
  }

  test("FLUSH with configured disk cache path should delete files") {
    if (spark != null) {
      spark.stop()
    }

    val diskCachePath = Files.createTempDirectory("flush_test_cache_").toFile.getAbsolutePath

    // Create some dummy files to be deleted
    val dummyFile = new File(diskCachePath, "test_cache_file.dat")
    dummyFile.createNewFile()
    java.nio.file.Files.write(dummyFile.toPath, "test data".getBytes)

    try {
      val testSpark = SparkSession
        .builder()
        .master("local[2]")
        .appName("FlushDiskCacheWithPathTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
        .config("spark.indextables.cache.disk.enabled", "true")
        .config("spark.indextables.cache.disk.path", diskCachePath)
        .getOrCreate()

      try {
        val result = testSpark.sql("FLUSH INDEXTABLES DISK CACHE")
        val rows = result.collect()

        logger.info(s"FLUSH with configured path results: ${rows.map(_.toString).mkString(", ")}")

        // Should have driver disk cache result
        val driverDiskCacheRows = rows.filter { row =>
          row.getAs[String]("executor_id") == "driver" &&
            row.getAs[String]("cache_type") == "disk_cache_files"
        }

        assert(driverDiskCacheRows.nonEmpty, "Should have driver disk_cache_files result")

        val diskCacheRow = driverDiskCacheRows.head
        val status = diskCacheRow.getAs[String]("status")
        val filesDeleted = diskCacheRow.getAs[Long]("files_deleted")
        val bytesFreed = diskCacheRow.getAs[Long]("bytes_freed")

        logger.info(s"Disk cache flush: status=$status, files=$filesDeleted, bytes=$bytesFreed")

        assert(status == "success", s"Status should be success, got: $status")
        assert(filesDeleted >= 1, s"Should have deleted at least 1 file, got: $filesDeleted")
        assert(bytesFreed > 0, s"Should have freed some bytes, got: $bytesFreed")

        // Verify the file was actually deleted
        assert(!dummyFile.exists(), "Dummy file should have been deleted")

      } finally {
        testSpark.stop()
      }

    } finally {
      // Cleanup
      deleteRecursively(new File(diskCachePath))
      // Restore spark for afterEach
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("FlushDiskCacheCommandTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
        .getOrCreate()
    }
  }

  test("FLUSH should clear locality manager state") {
    import io.indextables.spark.storage.DriverSplitLocalityManager

    // Add some state to locality manager via assignSplitsForQuery
    val testSplits = Seq("test-split-1", "test-split-2", "test-split-3")
    val testHosts = Set("host1", "host2")
    DriverSplitLocalityManager.assignSplitsForQuery(testSplits, testHosts)

    val statsBefore = DriverSplitLocalityManager.getStats()
    logger.info(s"Locality stats before FLUSH: ${statsBefore.totalTrackedSplits} splits")
    assert(statsBefore.totalTrackedSplits > 0, "Should have some tracked splits before FLUSH")

    // Execute FLUSH
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows = result.collect()

    // Check locality manager result
    val localityRows = rows.filter { row =>
      row.getAs[String]("executor_id") == "driver" &&
        row.getAs[String]("cache_type") == "locality_manager"
    }

    assert(localityRows.nonEmpty, "Should have locality_manager result")

    val localityRow = localityRows.head
    assert(localityRow.getAs[String]("status") == "success",
      s"Locality manager flush should succeed, got: ${localityRow.getAs[String]("status")}")

    // Verify state was cleared
    val statsAfter = DriverSplitLocalityManager.getStats()
    logger.info(s"Locality stats after FLUSH: ${statsAfter.totalTrackedSplits} splits")

    assert(statsAfter.totalTrackedSplits == 0, "Locality manager should be cleared after FLUSH")
  }

  test("FLUSH should clear split cache managers") {
    import io.indextables.spark.storage.GlobalSplitCacheManager

    // Execute FLUSH
    val result = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows = result.collect()

    // Check split cache result
    val splitCacheRows = rows.filter { row =>
      row.getAs[String]("executor_id") == "driver" &&
        row.getAs[String]("cache_type") == "split_cache"
    }

    assert(splitCacheRows.nonEmpty, "Should have split_cache result")

    val splitCacheRow = splitCacheRows.head
    assert(splitCacheRow.getAs[String]("status") == "success",
      s"Split cache flush should succeed, got: ${splitCacheRow.getAs[String]("status")}")

    logger.info(s"Split cache flush message: ${splitCacheRow.getAs[String]("message")}")
  }

  test("FLUSH should be idempotent - multiple calls should succeed") {
    // First flush
    val result1 = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows1 = result1.collect()
    assert(rows1.nonEmpty, "First FLUSH should return results")

    // Second flush immediately after
    val result2 = spark.sql("FLUSH INDEXTABLES DISK CACHE")
    val rows2 = result2.collect()
    assert(rows2.nonEmpty, "Second FLUSH should also return results")

    // Both should have success or skipped status (not failed)
    val allStatuses = (rows1 ++ rows2).map(_.getAs[String]("status")).toSet
    assert(!allStatuses.contains("failed"),
      s"Neither flush should fail. Statuses: $allStatuses")
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
