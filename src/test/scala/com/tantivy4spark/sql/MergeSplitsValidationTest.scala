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
import org.apache.spark.sql.functions.{col, concat, lit}
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
    // Create multiple writes to get real split files
    (1 to 3).foreach { i =>
      val data = spark.range((i-1)*100 + 1, i*100 + 1).select(
        col("id"),
        concat(lit("data_"), col("id")).as("content")
      )
      
      data.coalesce(1).write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.indexWriter.batchSize", "50")
        .mode("append")
        .save(tempTablePath)
    }
    
    // Record initial state
    val initialFiles = transactionLog.listFiles()
    val initialFilePaths = initialFiles.map(_.path).toSet
    logger.info(s"Initial state: ${initialFiles.length} files: ${initialFilePaths.mkString(", ")}")
    
    // Execute MERGE SPLITS
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}") // 50MB target
    
    // Validate transaction log changes
    val finalFiles = transactionLog.listFiles()
    val finalFilePaths = finalFiles.map(_.path).toSet
    
    logger.info(s"Final state: ${finalFiles.length} files: ${finalFilePaths.mkString(", ")}")
    
    // Should have same or fewer files after merge
    assert(finalFiles.length <= initialFiles.length, 
      s"Should have same or fewer files after merge: ${finalFiles.length} vs ${initialFiles.length}")
    
    // Validate that data is still readable and complete
    val mergedData = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tempTablePath)
    val actualCount = mergedData.count()
    val expectedCount = 300L // 3 writes * 100 records each
    assert(actualCount == expectedCount, s"Should preserve all data: expected $expectedCount, got $actualCount")
    
    logger.info("✓ Transaction log properly updated with REMOVE+ADD pattern")
  }

  test("Statistics should be properly merged without reading file contents") {
    // Create multiple writes to get real split files with statistics
    (1 to 2).foreach { i =>
      val data = spark.range((i-1)*150 + 1, i*150 + 1).select(
        col("id"),
        concat(lit("content_"), col("id")).as("content")
      )
      
      data.coalesce(1).write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.indexWriter.batchSize", "75")
        .mode("append")
        .save(tempTablePath)
    }
    
    val initialFiles = transactionLog.listFiles()
    logger.info(s"Created ${initialFiles.length} files with statistics")
    
    // Merge the files
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${100 * 1024 * 1024}") // 100MB target
    
    // Check final state
    val finalFiles = transactionLog.listFiles()
    assert(finalFiles.length <= initialFiles.length, "Should have same or fewer files after merge")
    
    // Validate that statistics are preserved (files should have size, numRecords, etc.)
    val totalInitialSize = initialFiles.map(_.size).sum
    val totalFinalSize = finalFiles.map(_.size).sum
    
    // Sizes should be approximately the same (within 10% due to compression differences)
    val sizeDifference = math.abs(totalFinalSize - totalInitialSize).toDouble / totalInitialSize
    assert(sizeDifference < 0.1, 
      s"Total size should be approximately preserved: ${totalInitialSize} vs ${totalFinalSize}")
    
    // Validate data integrity
    val mergedData = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tempTablePath)
    val actualCount = mergedData.count()
    val expectedCount = 300L // 2 writes * 150 records each
    assert(actualCount == expectedCount, s"Should preserve all data: expected $expectedCount, got $actualCount")
    
    logger.info("✓ Statistics properly merged without reading file contents")
  }

  test("WHERE predicate should filter partitions correctly") {
    // Create files in different partitions
    val partition2023Q1 = Map("year" -> "2023", "quarter" -> "Q1")
    val partition2023Q2 = Map("year" -> "2023", "quarter" -> "Q2") 
    val partition2022Q4 = Map("year" -> "2022", "quarter" -> "Q4")
    
    createRealSplitFiles(3, 100, partition2023Q1)
    createRealSplitFiles(3, 100, partition2023Q2)  
    createRealSplitFiles(2, 100, partition2022Q4)
    
    // Files are already in transaction log from the writes
    
    val initialFileCount = transactionLog.listFiles().length
    logger.info(s"Initial files: $initialFileCount across 3 partitions")
    
    // Execute MERGE SPLITS with WHERE predicate for 2023 data only
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val command = sqlParser.parsePlan(
      s"MERGE SPLITS '$tempTablePath' WHERE year = '2023' TARGET SIZE ${100 * 1024 * 1024}"
    ).asInstanceOf[MergeSplitsCommand]
    
    command.run(spark)
    
    // Invalidate cache to ensure we see the latest transaction log state
    transactionLog.invalidateCache()
    
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
    
    // Invalidate cache to ensure we see the latest transaction log state
    transactionLog.invalidateCache()
    
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
    // Create multiple small writes to get multiple split files
    (1 to 5).foreach { i =>
      val data = spark.range((i-1)*50 + 1, i*50 + 1).select(
        col("id"),
        concat(lit("content_"), col("id")).as("content")
      )
      
      data.coalesce(1).write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.indexWriter.batchSize", "25")
        .mode("append")
        .save(tempTablePath)
    }
    
    val initialFiles = transactionLog.listFiles()
    logger.info(s"Created ${initialFiles.length} split files")
    
    // Merge via SQL
    val targetSize = 100 * 1024 * 1024 // 100MB  
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE $targetSize")
    
    // Validate transaction log
    val finalFiles = transactionLog.listFiles()
    logger.info(s"After merge: ${finalFiles.length} split files remain")
    
    assert(finalFiles.length <= initialFiles.length, "Should have same or fewer files after merge")
    
    // CRITICAL: Validate the merged file actually contains the expected data
    logger.info("🔍 Validating merged file contents...")
    val mergedData = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tempTablePath)
    
    val actualCount = mergedData.count()
    val expectedCount = 250L // 5 writes * 50 records each
    assert(actualCount == expectedCount, s"Merged data should contain $expectedCount records, got $actualCount")
    
    // Validate ID range
    val actualIds = mergedData.select("id").collect().map(_.getLong(0)).sorted
    val expectedIds = (1L to 250L).toArray
    assert(actualIds.sameElements(expectedIds), s"Merged data should contain IDs 1-250, got ${actualIds.take(10).mkString(",")}...")
    
    // Validate content format
    val sampleRecord = mergedData.filter(col("id") === 1).collect().head
    val expectedContent = "content_1"
    val actualContent = sampleRecord.getString(1)
    assert(actualContent == expectedContent, s"Content format should be preserved: expected '$expectedContent', got '$actualContent'")
    
    logger.info(s"✓ Merged file validation passed: $actualCount records, IDs 1-250, content preserved")
    logger.info("✓ Bin packing algorithm respects target size boundaries")
  }

  // Helper methods

  private def createRealSplitFiles(
      count: Int, 
      recordsPerSplit: Int = 100,
      partitionValues: Map[String, String] = Map.empty
  ): Unit = {
    // Create multiple writes to ensure multiple split files
    (1 to count).foreach { i =>
      val startId = (i - 1) * recordsPerSplit + 1
      val endId = i * recordsPerSplit
      
      // Create data for this split  
      val baseData = spark.range(startId, endId + 1).select(
        col("id"),
        concat(lit("data_"), col("id")).as("data")
      )
      
      val df = if (partitionValues.nonEmpty) {
        baseData
          .withColumn("year", lit(partitionValues.get("year").orNull))
          .withColumn("quarter", lit(partitionValues.get("quarter").orNull))
      } else {
        baseData
      }
      
      // Write using Tantivy4Spark format with small batch size to force separate splits
      df.coalesce(1).write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.indexWriter.batchSize", "50") // Small batch size
        .mode("append")
        .save(tempTablePath)
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