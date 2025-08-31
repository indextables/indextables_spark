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

package com.tantivy4spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import com.tantivy4spark.TestBase
import com.tantivy4spark.transaction.{TransactionLog, AddAction, RemoveAction}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, IntegerType, LongType, StructType, StructField}
import org.apache.hadoop.fs.Path
import java.nio.file.Files
import java.io.File
import scala.util.Random
import org.slf4j.LoggerFactory

/**
 * Comprehensive validation tests for MERGE SPLITS functionality.
 * These tests validate:
 * 1. Target size enforcement and validation
 * 2. Reads access merged splits, not original constituent splits
 * 3. WHERE predicate filtering on partitions
 * 4. Statistics merging accuracy without reading file contents
 * 5. Transaction log REMOVE+ADD atomic operations
 */
class MergeSplitsValidationTest extends TestBase with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsValidationTest])
  
  var tempTablePath: String = _
  var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempTablePath = Files.createTempDirectory("optimize_validation_").toFile.getAbsolutePath
    transactionLog = new TransactionLog(new Path(tempTablePath), spark)
  }

  override def afterEach(): Unit = {
    if (transactionLog != null) {
      transactionLog.close()
    }
    
    // Clean up temp directory
    if (tempTablePath != null) {
      val dir = new File(tempTablePath)
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
    super.afterEach()
  }

  test("Target size validation should enforce minimum and maximum limits") {
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    
    // Test cases for different target sizes
    val testCases = Seq(
      (0L, "zero size", true), // Should fail
      (512L, "512 bytes", true), // Should fail (below 1MB minimum)
      (1024L * 1024L, "1MB", false), // Should pass (minimum)
      (1024L * 1024L * 1024L, "1GB", false), // Should pass
      (5L * 1024L * 1024L * 1024L, "5GB", false), // Should pass (default)
      (10L * 1024L * 1024L * 1024L, "10GB", false), // Should pass
      (-1L, "negative size", true) // Should fail
    )
    
    for ((targetSize, description, shouldFail) <- testCases) {
      logger.info(s"Testing target size validation: $description ($targetSize bytes)")
      
      val command = sqlParser.parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE $targetSize")
        .asInstanceOf[MergeSplitsCommand]
      
      if (shouldFail) {
        assertThrows[IllegalArgumentException] {
          command.run(spark)
        }
        logger.info(s"✓ Target size $description correctly rejected")
      } else {
        // Should not throw validation error (may fail for other reasons like missing table)
        try {
          command.run(spark)
          logger.info(s"✓ Target size $description accepted")
        } catch {
          case _: IllegalArgumentException if command.targetSize.contains(targetSize) =>
            fail(s"Target size $description should be valid but was rejected")
          case _: Exception => 
            // Other errors are expected (table doesn't exist, etc.)
            logger.info(s"✓ Target size $description accepted (other error occurred as expected)")
        }
      }
    }
  }

  test("MERGE SPLITS should create proper transaction log entries with REMOVE+ADD pattern") {
    // Create a mock table with multiple small split files (exact sizes to avoid randomness)
    val initialFiles = Seq(
      createMockAddAction("split1.split", size = 80 * 1024 * 1024, partitionValues = Map("year" -> "2023", "month" -> "01")), // 80MB
      createMockAddAction("split2.split", size = 90 * 1024 * 1024, partitionValues = Map("year" -> "2023", "month" -> "01")), // 90MB  
      createMockAddAction("split3.split", size = 70 * 1024 * 1024, partitionValues = Map("year" -> "2023", "month" -> "01")), // 70MB
      createMockAddAction("split4.split", size = 85 * 1024 * 1024, partitionValues = Map("year" -> "2023", "month" -> "01")), // 85MB
      createMockAddAction("split5.split", size = 95 * 1024 * 1024, partitionValues = Map("year" -> "2023", "month" -> "01"))  // 95MB
    )
    // Total: 420MB with 200MB target should create 2 groups: [80+90]=170MB, [70+85]=155MB, leaving [95] alone
    
    // Initialize transaction log with schema and add initial files
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("year", StringType, nullable = true),
      StructField("month", StringType, nullable = true)
    ))
    transactionLog.initialize(schema, Seq("year", "month"))
    
    initialFiles.foreach(transactionLog.addFile)
    
    // Record initial state
    val initialVersion = transactionLog.listFiles().length
    val initialFilePaths = transactionLog.listFiles().map(_.path).toSet
    logger.info(s"Initial state: $initialVersion files: ${initialFilePaths.mkString(", ")}")
    
    // Execute MERGE SPLITS with 200MB target (should merge 2 files per group)
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val command = sqlParser.parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${200 * 1024 * 1024}")
      .asInstanceOf[MergeSplitsCommand]
    
    val result = command.run(spark)
    
    // Validate transaction log changes
    val finalFiles = transactionLog.listFiles()
    val finalFilePaths = finalFiles.map(_.path).toSet
    
    logger.info(s"Final state: ${finalFiles.length} files: ${finalFilePaths.mkString(", ")}")
    
    // Should have fewer files after merge
    assert(finalFiles.length < initialFiles.length, 
      s"Should have fewer files after merge: ${finalFiles.length} vs ${initialFiles.length}")
    
    // Check atomic replacement: only unmerged files should remain from initial state
    // In this test, split5.split (95MB) should remain since it forms a single-file group
    val expectedUnmergedFiles = Set("split5.split")
    val actualUnmergedFiles = initialFilePaths.intersect(finalFilePaths)
    assert(actualUnmergedFiles == expectedUnmergedFiles, 
      s"Only unmerged files should remain unchanged: expected ${expectedUnmergedFiles}, got ${actualUnmergedFiles}")
    
    // Verify merged files have appropriate tags
    val mergedFiles = finalFiles.filter(_.tags.exists(_.get("operation").contains("optimize")))
    assert(mergedFiles.nonEmpty, "Should have files tagged as merged")
    
    logger.info(s"✓ Transaction log properly updated with ${mergedFiles.length} merged files")
  }

  test("Statistics should be properly merged without reading file contents") {
    // Create files with known statistics
    val file1 = createMockAddAction("file1.split", size = 50 * 1024 * 1024,
      minValues = Map("id" -> "1", "score" -> "10.5"),
      maxValues = Map("id" -> "100", "score" -> "95.7"),
      numRecords = 1000L)
    
    val file2 = createMockAddAction("file2.split", size = 75 * 1024 * 1024, 
      minValues = Map("id" -> "101", "score" -> "8.2"),
      maxValues = Map("id" -> "200", "score" -> "98.1"),
      numRecords = 1500L)
    
    val file3 = createMockAddAction("file3.split", size = 60 * 1024 * 1024,
      minValues = Map("id" -> "201", "score" -> "12.0"),
      maxValues = Map("id" -> "300", "score" -> "89.3"),
      numRecords = 1200L)
    
    // Initialize table and add files
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("score", StringType, nullable = true) // Stored as string for min/max
    ))
    transactionLog.initialize(schema)
    
    Seq(file1, file2, file3).foreach(transactionLog.addFile)
    
    // Execute merge with target size that will merge all files (200MB target)
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val command = sqlParser.parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${200 * 1024 * 1024}")
      .asInstanceOf[MergeSplitsCommand]
    
    command.run(spark)
    
    // Verify merged statistics
    val mergedFiles = transactionLog.listFiles().filter(_.tags.exists(_.get("operation").contains("optimize")))
    assert(mergedFiles.nonEmpty, "Should have merged files")
    
    val mergedFile = mergedFiles.head
    
    // Check aggregated statistics
    mergedFile.minValues match {
      case Some(mins: Map[String, String]) =>
        assert(mins.get("id") == Some("1"), s"Min ID should be '1', got: ${mins.get("id")}")
        assert(mins.get("score") == Some("8.2"), s"Min score should be '8.2', got: ${mins.get("score")}")
      case None => fail("Merged file should have min values")
    }
    
    mergedFile.maxValues match {
      case Some(maxs: Map[String, String]) =>
        assert(maxs.get("id") == Some("300"), s"Max ID should be '300', got: ${maxs.get("id")}")
        assert(maxs.get("score") == Some("98.1"), s"Max score should be '98.1', got: ${maxs.get("score")}")
      case None => fail("Merged file should have max values")
    }
    
    // Check aggregated record count
    assert(mergedFile.numRecords.contains(3700L), 
      s"Merged file should have 3700 records (1000+1500+1200), got: ${mergedFile.numRecords}")
    
    logger.info("✓ Statistics properly merged without reading file contents")
  }

  test("WHERE predicate should filter partitions correctly") {
    // Create files in different partitions
    val partition2023Q1 = Map("year" -> "2023", "quarter" -> "Q1")
    val partition2023Q2 = Map("year" -> "2023", "quarter" -> "Q2") 
    val partition2022Q4 = Map("year" -> "2022", "quarter" -> "Q4")
    
    val files2023Q1 = createMockSplitFiles(3, 50 * 1024 * 1024, partition2023Q1)
    val files2023Q2 = createMockSplitFiles(3, 50 * 1024 * 1024, partition2023Q2)
    val files2022Q4 = createMockSplitFiles(2, 50 * 1024 * 1024, partition2022Q4)
    
    // Initialize transaction log
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("data", StringType, nullable = true),
      StructField("year", StringType, nullable = true),
      StructField("quarter", StringType, nullable = true)
    ))
    transactionLog.initialize(schema, Seq("year", "quarter"))
    
    // Add all files
    (files2023Q1 ++ files2023Q2 ++ files2022Q4).foreach(transactionLog.addFile)
    
    val initialFileCount = transactionLog.listFiles().length
    logger.info(s"Initial files: $initialFileCount across 3 partitions")
    
    // Execute MERGE SPLITS with WHERE predicate for 2023 data only
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val command = sqlParser.parsePlan(
      s"MERGE SPLITS '$tempTablePath' WHERE year = '2023' TARGET SIZE ${100 * 1024 * 1024}"
    ).asInstanceOf[MergeSplitsCommand]
    
    command.run(spark)
    
    // Verify only 2023 partitions were affected
    val finalFiles = transactionLog.listFiles()
    val files2022Remaining = finalFiles.filter(_.partitionValues.get("year").contains("2022"))
    val files2023Final = finalFiles.filter(_.partitionValues.get("year").contains("2023"))
    
    // 2022 files should be unchanged
    assert(files2022Remaining.length == 2, 
      s"2022 files should be unchanged: expected 2, got ${files2022Remaining.length}")
    
    // 2023 files should be merged (6 original files should become fewer)  
    assert(files2023Final.length < 6,
      s"2023 files should be merged: expected < 6, got ${files2023Final.length}")
    
    // Verify merged files are tagged appropriately
    val mergedFiles = finalFiles.filter(_.tags.exists(_.get("operation").contains("optimize")))
    assert(mergedFiles.nonEmpty, "Should have merged files")
    assert(mergedFiles.forall(_.partitionValues.get("year").contains("2023")), 
      "All merged files should be from 2023 partitions")
    
    logger.info(s"✓ WHERE predicate correctly filtered partitions: ${mergedFiles.length} merged files from 2023")
  }

  test("Reads should access merged splits not original constituent splits") {
    // This test validates the core requirement: after merge, queries should only read
    // the merged splits and not the original files that were merged together
    
    // Create initial files with tracking information
    val originalFiles = (1 to 4).map { i =>
      createMockAddAction(
        path = s"original_split_$i.split",
        size = 40 * 1024 * 1024, // 40MB each
        minValues = Map("id" -> (i * 1000).toString),
        maxValues = Map("id" -> ((i + 1) * 1000 - 1).toString),
        numRecords = 1000L,
        partitionValues = Map("region" -> "us-west")
      )
    }
    
    // Initialize transaction log
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("data", StringType, nullable = true),
      StructField("region", StringType, nullable = true)
    ))
    transactionLog.initialize(schema, Seq("region"))
    
    originalFiles.foreach(transactionLog.addFile)
    
    // Record original file paths
    val originalPaths = originalFiles.map(_.path).toSet
    logger.info(s"Original files: ${originalPaths.mkString(", ")}")
    
    // Execute MERGE SPLITS with target size that will merge all files (200MB)
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val command = sqlParser.parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${200 * 1024 * 1024}")
      .asInstanceOf[MergeSplitsCommand]
    
    val result = command.run(spark)
    
    // Get final file list from transaction log
    val finalFiles = transactionLog.listFiles()
    val finalPaths = finalFiles.map(_.path).toSet
    
    logger.info(s"Final files: ${finalPaths.mkString(", ")}")
    
    // Critical validation: NO overlap between original and final file paths
    val overlappingPaths = originalPaths.intersect(finalPaths)
    assert(overlappingPaths.isEmpty,
      s"NO files should exist in both original and final state. Found overlapping: ${overlappingPaths}")
    
    // Should have fewer files (merged)
    assert(finalFiles.length < originalFiles.length,
      s"Should have fewer files after merge: ${finalFiles.length} vs ${originalFiles.length}")
    
    // All final files should be merged files (have merge operation tag)
    val mergedFiles = finalFiles.filter(_.tags.exists(_.get("operation").contains("optimize")))
    assert(mergedFiles.length == finalFiles.length,
      "All remaining files should be marked as merged files")
    
    // Validate statistics consolidation
    val totalOriginalRecords = originalFiles.flatMap(_.numRecords).sum
    val totalMergedRecords = mergedFiles.flatMap(_.numRecords).sum
    assert(totalMergedRecords == totalOriginalRecords,
      s"Total record count should be preserved: original=$totalOriginalRecords, merged=$totalMergedRecords")
    
    // Validate min/max ranges are preserved
    val originalMinId = originalFiles.flatMap(_.minValues.flatMap(_.get("id"))).map(_.toInt).min
    val originalMaxId = originalFiles.flatMap(_.maxValues.flatMap(_.get("id"))).map(_.toInt).max
    
    val mergedMinId = mergedFiles.flatMap(_.minValues.flatMap(_.get("id"))).map(_.toInt).min
    val mergedMaxId = mergedFiles.flatMap(_.maxValues.flatMap(_.get("id"))).map(_.toInt).max
    
    assert(mergedMinId == originalMinId, s"Min ID should be preserved: $originalMinId vs $mergedMinId")
    assert(mergedMaxId == originalMaxId, s"Max ID should be preserved: $originalMaxId vs $mergedMaxId")
    
    logger.info("✓ CRITICAL: Reads will access merged splits only - original splits are atomically replaced")
    logger.info(s"  Original files: ${originalPaths.size} files")
    logger.info(s"  Merged files: ${finalPaths.size} files")
    logger.info(s"  No file path overlap: ${overlappingPaths.isEmpty}")
    logger.info(s"  Statistics preserved: ${totalMergedRecords} records, ID range $mergedMinId-$mergedMaxId")
  }

  test("Bin packing algorithm should respect target size boundaries") {
    // Create files with specific sizes to test bin packing logic
    val targetSize = 100 * 1024 * 1024 // 100MB target
    
    val testFiles = Seq(
      createMockAddAction("small1.split", 20 * 1024 * 1024), // 20MB
      createMockAddAction("small2.split", 30 * 1024 * 1024), // 30MB  
      createMockAddAction("small3.split", 40 * 1024 * 1024), // 40MB
      createMockAddAction("small4.split", 25 * 1024 * 1024), // 25MB
      createMockAddAction("large1.split", 120 * 1024 * 1024), // 120MB (larger than target)
      createMockAddAction("medium1.split", 80 * 1024 * 1024), // 80MB
    )
    
    // Initialize table
    val schema = StructType(Seq(StructField("data", StringType, nullable = true)))
    transactionLog.initialize(schema)
    
    testFiles.foreach(transactionLog.addFile)
    
    logger.info(s"Test files created with sizes: ${testFiles.map(f => s"${f.path}(${f.size/1024/1024}MB)").mkString(", ")}")
    
    // Execute merge
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser) 
    val command = sqlParser.parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE $targetSize")
      .asInstanceOf[MergeSplitsCommand]
    
    command.run(spark)
    
    val finalFiles = transactionLog.listFiles()
    logger.info(s"Final files: ${finalFiles.map(f => s"${f.path}(${f.size/1024/1024}MB)").mkString(", ")}")
    
    // Validate bin packing results:
    // 1. Files larger than target should be left alone
    val largeFiles = finalFiles.filter(_.size > targetSize)
    assert(largeFiles.exists(_.path.contains("large1")), "Files larger than target should remain")
    
    // 2. Small files should be merged into groups that don't exceed target
    val mergedFiles = finalFiles.filter(_.tags.exists(_.get("operation").contains("optimize")))
    mergedFiles.foreach { file =>
      assert(file.size <= targetSize, 
        s"Merged file ${file.path} should not exceed target size: ${file.size} > $targetSize")
    }
    
    // 3. Total size should be preserved
    val originalTotalSize = testFiles.map(_.size).sum
    val finalTotalSize = finalFiles.map(_.size).sum
    assert(originalTotalSize == finalTotalSize, 
      s"Total size should be preserved: $originalTotalSize vs $finalTotalSize")
    
    logger.info("✓ Bin packing algorithm correctly grouped files within target size boundaries")
  }

  // Helper methods

  private def createMockSplitFiles(
      count: Int, 
      avgSize: Long, 
      partitionValues: Map[String, String] = Map.empty
  ): Seq[AddAction] = {
    (1 to count).map { i =>
      val sizeVariation = (Random.nextDouble() - 0.5) * 0.2 // ±10% size variation
      val actualSize = (avgSize * (1 + sizeVariation)).toLong
      
      createMockAddAction(
        path = s"split_${partitionValues.values.mkString("_")}_$i.split",
        size = actualSize,
        partitionValues = partitionValues
      )
    }
  }

  private def createMockAddAction(
      path: String,
      size: Long,
      minValues: Map[String, String] = Map.empty,
      maxValues: Map[String, String] = Map.empty, 
      numRecords: Long = 1000L,
      partitionValues: Map[String, String] = Map.empty
  ): AddAction = {
    AddAction(
      path = path,
      partitionValues = partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = None,
      tags = Some(Map("created_by" -> "test")),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None,
      numRecords = Some(numRecords)
    )
  }
}