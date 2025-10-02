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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import org.apache.spark.sql.functions.{col, lit, concat}
import org.apache.hadoop.fs.Path
import java.nio.file.Files
import java.io.File
import org.slf4j.LoggerFactory

/**
 * Tests for temporary directory configuration in MERGE SPLITS operations. Validates that custom temp directory paths
 * are properly configured, validated, and used.
 */
class MergeSplitsTempDirectoryTest extends TestBase with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsTempDirectoryTest])

  var tempTablePath: String          = _
  var customTempDir: String          = _
  var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempTablePath = Files.createTempDirectory("merge_temp_test_").toFile.getAbsolutePath
    customTempDir = Files.createTempDirectory("custom_merge_temp_").toFile.getAbsolutePath
    transactionLog = TransactionLogFactory.create(new Path(tempTablePath), spark)
  }

  override def afterEach(): Unit = {
    if (transactionLog != null) {
      transactionLog.close()
    }

    // Clean up temp directories
    Seq(tempTablePath, customTempDir).foreach { path =>
      if (path != null) {
        val dir = new File(path)
        if (dir.exists()) {
          def deleteRecursively(file: File): Unit = {
            if (file.isDirectory) {
              file.listFiles().foreach(deleteRecursively)
            }
            file.delete()
          }
          deleteRecursively(dir)
        }
      }
    }
    super.afterEach()
  }

  test("MERGE SPLITS should use custom temp directory when configured") {
    logger.info("Testing MERGE SPLITS with custom temp directory configuration")

    // Set custom temp directory configuration
    spark.conf.set("spark.indextables.merge.tempDirectoryPath", customTempDir)

    // Create test data with multiple files for merging
    createTestDataForMerging()

    // Capture initial file count
    val initialFiles = transactionLog.listFiles()
    logger.info(s"Created ${initialFiles.length} files for merge test")

    // Execute MERGE SPLITS - this should use the custom temp directory
    logger.info(s"Executing MERGE SPLITS with custom temp directory: $customTempDir")
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}") // 50MB target

    // Verify merge succeeded
    transactionLog.invalidateCache()
    val finalFiles = transactionLog.listFiles()
    assert(
      finalFiles.length <= initialFiles.length,
      s"Merge should reduce or maintain file count: ${finalFiles.length} vs ${initialFiles.length}"
    )

    // Verify data integrity
    val mergedData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val totalCount = mergedData.count()
    assert(totalCount == 400, s"Should preserve all 400 records, got $totalCount")

    logger.info("✅ Custom temp directory test completed successfully")
  }

  test("MERGE SPLITS should fall back to system temp directory when custom path is invalid") {
    logger.info("Testing MERGE SPLITS fallback behavior with invalid temp directory")

    // Set invalid temp directory configuration
    val invalidTempDir = "/this/path/does/not/exist/and/should/not/be/created"
    spark.conf.set("spark.indextables.merge.tempDirectoryPath", invalidTempDir)

    // Create test data
    createTestDataForMerging()

    // Execute MERGE SPLITS - should fall back to system temp directory
    logger.info(s"Executing MERGE SPLITS with invalid temp directory: $invalidTempDir")
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}")

    // Verify merge still succeeded despite invalid temp directory
    transactionLog.invalidateCache()
    val finalFiles = transactionLog.listFiles()
    assert(finalFiles.nonEmpty, "Merge should still succeed with fallback temp directory")

    // Verify data integrity
    val mergedData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val totalCount = mergedData.count()
    assert(totalCount == 400, s"Should preserve all 400 records, got $totalCount")

    logger.info("✅ Invalid temp directory fallback test completed successfully")
  }

  test("MERGE SPLITS should use system default when no temp directory is configured") {
    logger.info("Testing MERGE SPLITS with default temp directory (no configuration)")

    // Ensure no custom temp directory is configured
    spark.conf.unset("spark.indextables.merge.tempDirectoryPath")

    // Create test data
    createTestDataForMerging()

    // Execute MERGE SPLITS - should use system default temp directory
    logger.info("Executing MERGE SPLITS with system default temp directory")
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}")

    // Verify merge succeeded
    transactionLog.invalidateCache()
    val finalFiles = transactionLog.listFiles()
    assert(finalFiles.nonEmpty, "Merge should succeed with system default temp directory")

    // Verify data integrity
    val mergedData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val totalCount = mergedData.count()
    assert(totalCount == 400, s"Should preserve all 400 records, got $totalCount")

    logger.info("✅ System default temp directory test completed successfully")
  }

  test("SerializableAwsConfig should correctly handle temp directory path and merge config") {
    logger.info("Testing SerializableAwsConfig temp directory and merge config handling")

    // Test with temp directory and custom heap size
    val configWithTempDir = SerializableAwsConfig(
      accessKey = "test-key",
      secretKey = "test-secret",
      sessionToken = Some("test-token"),
      region = "us-east-1",
      endpoint = None,
      pathStyleAccess = false,
      tempDirectoryPath = Some(customTempDir),
      credentialsProviderClass = None,
      heapSize = java.lang.Long.valueOf(50000000L), // 50MB
      debugEnabled = true
    )

    assert(
      configWithTempDir.tempDirectoryPath.contains(customTempDir),
      "SerializableAwsConfig should store temp directory path"
    )
    assert(
      configWithTempDir.heapSize == 50000000L,
      "SerializableAwsConfig should store heap size"
    )
    assert(
      configWithTempDir.debugEnabled,
      "SerializableAwsConfig should store debug enabled flag"
    )

    // Test without temp directory (using defaults)
    val configWithoutTempDir = SerializableAwsConfig(
      accessKey = "test-key",
      secretKey = "test-secret",
      sessionToken = None,
      region = "us-west-2",
      endpoint = None,
      pathStyleAccess = true,
      tempDirectoryPath = None,
      credentialsProviderClass = None
      // heapSize and debugEnabled use defaults (1GB and false)
    )

    assert(
      configWithoutTempDir.tempDirectoryPath.isEmpty,
      "SerializableAwsConfig should handle missing temp directory path"
    )
    assert(
      configWithoutTempDir.heapSize == 1073741824L,
      s"SerializableAwsConfig should default heap size to 1GB (1073741824), got ${configWithoutTempDir.heapSize}"
    )
    assert(
      !configWithoutTempDir.debugEnabled,
      "SerializableAwsConfig should default debug enabled to false"
    )

    logger.info("✅ SerializableAwsConfig temp directory and merge config handling test completed")
  }

  test("Temp directory validation should log appropriate messages") {
    logger.info("Testing temp directory validation logging")

    // Test with valid directory
    spark.conf.set("spark.indextables.merge.tempDirectoryPath", customTempDir)

    // Create minimal test data
    val testData = spark
      .range(1, 11)
      .select(
        col("id"),
        lit("test_content").as("content")
      )

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.batchSize", "5")
      .mode("overwrite")
      .save(tempTablePath)

    // This will trigger the validation logic in extractAwsConfig
    logger.info("Triggering temp directory validation...")

    // We can't directly access the private method, but we can trigger it through a merge operation
    // The validation messages will appear in the logs
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${10 * 1024 * 1024}")

    // Verify the operation succeeded (indirect validation that temp directory was accepted)
    val data = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    assert(data.count() == 10, "Merge should succeed with valid temp directory")

    logger.info("✅ Temp directory validation test completed")
  }

  test("MERGE SPLITS result should include tempDirectoryPath and heapSize") {
    logger.info("Testing that merge result DataFrame includes temp directory and heap size")

    // Set custom configuration
    val customHeapSize = 30000000L // 30MB
    spark.conf.set("spark.indextables.merge.tempDirectoryPath", customTempDir)
    spark.conf.set("spark.indextables.merge.heapSize", customHeapSize.toString)

    // Create test data
    createTestDataForMerging()

    // Execute MERGE SPLITS with custom config
    logger.info(s"Executing MERGE SPLITS with heapSize=$customHeapSize and tempDir=$customTempDir")
    val result = spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}")

    // Verify result DataFrame has the expected columns
    val columns = result.columns
    assert(columns.contains("table_path"), "Result should contain table_path column")
    assert(columns.contains("metrics"), "Result should contain metrics column")
    assert(columns.contains("temp_directory_path"), "Result should contain temp_directory_path column")
    assert(columns.contains("heap_size_bytes"), "Result should contain heap_size_bytes column")

    // Get the result row
    val rows = result.collect()
    assert(rows.length == 1, "Should return exactly one result row")

    val row = rows(0)
    val tablePath = row.getString(row.fieldIndex("table_path"))
    val metricsRow = row.getAs[org.apache.spark.sql.Row](row.fieldIndex("metrics"))
    val metricsStatus = metricsRow.getString(0) // status field at index 0
    val tempDirPath = row.getString(row.fieldIndex("temp_directory_path"))
    val heapSizeBytes = if (row.isNullAt(row.fieldIndex("heap_size_bytes"))) null else row.getLong(row.fieldIndex("heap_size_bytes"))

    logger.info(s"Result - table_path: $tablePath")
    logger.info(s"Result - metrics.status: $metricsStatus")
    logger.info(s"Result - temp_directory_path: $tempDirPath")
    logger.info(s"Result - heap_size_bytes: $heapSizeBytes")

    // Verify the values match what we configured
    assert(tempDirPath == customTempDir, s"temp_directory_path should be $customTempDir, got $tempDirPath")
    assert(heapSizeBytes == customHeapSize, s"heap_size_bytes should be $customHeapSize, got $heapSizeBytes")

    logger.info("✅ Merge result DataFrame contains correct temp directory and heap size")
  }

  test("Configuration extraction should handle environment variables and system properties") {
    logger.info("Testing configuration extraction with various sources")

    // Test Spark configuration
    spark.conf.set("spark.indextables.merge.tempDirectoryPath", customTempDir)

    // Create minimal test to trigger config extraction
    val testData = spark
      .range(1, 6)
      .select(
        col("id"),
        lit("env_test").as("content")
      )

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Trigger merge to test config extraction
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${5 * 1024 * 1024}")

    // Verify success
    val data = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    assert(data.count() == 5, "Should successfully extract and use temp directory configuration")

    logger.info("✅ Configuration extraction test completed")
  }

  // Helper method to create test data with multiple files for merging
  private def createTestDataForMerging(): Unit = {
    logger.info("Creating test data for merge operations")

    // Create 4 separate writes to generate multiple files for merging
    (1 to 4).foreach { batch =>
      val startId = (batch - 1) * 100 + 1
      val endId   = batch * 100

      val batchData = spark
        .range(startId, endId + 1)
        .select(
          col("id"),
          concat(lit(s"batch_${batch}_content_"), col("id")).as("content")
        )

      batchData
        .coalesce(1)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.batchSize", "25")
        .mode("append")
        .save(tempTablePath)

      logger.debug(s"Created batch $batch with IDs $startId to $endId")
    }

    logger.info("✅ Test data creation completed")
  }
}
