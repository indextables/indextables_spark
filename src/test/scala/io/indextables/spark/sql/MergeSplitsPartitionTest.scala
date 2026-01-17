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

import org.apache.spark.sql.functions.{col, lit}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

/**
 * Tests for partition-aware merge functionality. Validates that MERGE SPLITS never attempts to merge files from
 * different partitions.
 */
class MergeSplitsPartitionTest extends TestBase with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsPartitionTest])

  var tempTablePath: String          = _
  var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempTablePath = Files.createTempDirectory("partition_merge_test_").toFile.getAbsolutePath
    transactionLog = TransactionLogFactory.create(new Path(tempTablePath), spark)
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

  test("MERGE SPLITS should never attempt to merge files from different partitions") {
    logger.info("Testing partition-aware merge grouping logic")

    // Create partitioned data with multiple partitions and multiple files per partition
    val testData = Seq(
      ("2023", "Q1", 1 to 100),
      ("2023", "Q2", 101 to 200),
      ("2024", "Q1", 201 to 300),
      ("2024", "Q2", 301 to 400)
    )

    // Write data for each partition separately to create multiple files per partition
    testData.foreach {
      case (year, quarter, idRange) =>
        logger.info(s"Creating data for partition year=$year, quarter=$quarter")

        // Create 2 separate writes per partition to ensure multiple files
        val midPoint = idRange.start + (idRange.end - idRange.start) / 2

        // First file for this partition
        val data1 = spark
          .range(idRange.start, midPoint + 1)
          .select(
            col("id"),
            lit(s"content_${year}_${quarter}_").as("content"),
            lit(year).as("year"),
            lit(quarter).as("quarter")
          )

        data1
          .coalesce(1)
          .write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.indexWriter.batchSize", "25")
          .partitionBy("year", "quarter")
          .mode("append")
          .save(tempTablePath)

        // Second file for this partition
        val data2 = spark
          .range(midPoint + 1, idRange.end + 1)
          .select(
            col("id"),
            lit(s"content_${year}_${quarter}_").as("content"),
            lit(year).as("year"),
            lit(quarter).as("quarter")
          )

        data2
          .coalesce(1)
          .write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.indexWriter.batchSize", "25")
          .partitionBy("year", "quarter")
          .mode("append")
          .save(tempTablePath)
    }

    // Verify the initial state - should have multiple files across multiple partitions
    val initialFiles = transactionLog.listFiles()
    logger.info(s"Created ${initialFiles.length} files across partitions")

    // Group files by partition to verify setup
    val filesByPartition = initialFiles.groupBy(_.partitionValues)
    logger.info(s"Files grouped by partition:")
    filesByPartition.foreach {
      case (partitionValues, files) =>
        logger.info(s"  Partition $partitionValues: ${files.length} files")
        files.foreach(file => logger.info(s"    - ${file.path} (${file.size} bytes)"))
    }

    // Ensure we have multiple partitions with multiple files each
    assert(filesByPartition.size >= 4, s"Should have at least 4 partitions, got ${filesByPartition.size}")
    filesByPartition.foreach {
      case (partitionValues, files) =>
        assert(
          files.length >= 2,
          s"Each partition should have at least 2 files for merging, partition $partitionValues has ${files.length}"
        )
    }

    // Execute MERGE SPLITS - this should merge files within each partition only
    logger.info("Executing MERGE SPLITS operation")
    spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${50 * 1024 * 1024}") // 50MB target

    // Refresh transaction log to see results
    transactionLog.invalidateCache()
    val finalFiles = transactionLog.listFiles()

    logger.info(s"After merge: ${finalFiles.length} files remain")

    // Verify files by partition after merge
    val finalFilesByPartition = finalFiles.groupBy(_.partitionValues)
    logger.info("Files after merge grouped by partition:")
    finalFilesByPartition.foreach {
      case (partitionValues, files) =>
        logger.info(s"  Partition $partitionValues: ${files.length} files")
        files.foreach(file => logger.info(s"    - ${file.path} (${file.size} bytes)"))
    }

    // CRITICAL VALIDATION: All files should still belong to their correct partitions
    finalFiles.foreach { file =>
      val partitionPath = file.path

      // Verify partition path structure for partitioned files
      if (file.partitionValues.nonEmpty) {
        val year    = file.partitionValues.get("year")
        val quarter = file.partitionValues.get("quarter")

        if (year.isDefined && quarter.isDefined) {
          assert(partitionPath.contains(s"year=${year.get}"), s"File path should contain year partition: $partitionPath")
          assert(
            partitionPath.contains(s"quarter=${quarter.get}"),
            s"File path should contain quarter partition: $partitionPath"
          )
        }
      }
    }

    // Verify merge operation succeeded (fewer or equal files)
    assert(
      finalFiles.length <= initialFiles.length,
      s"Should have same or fewer files after merge: ${finalFiles.length} vs ${initialFiles.length}"
    )

    // Verify no cross-partition contamination by checking data integrity
    val finalData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val totalCount = finalData.count()
    assert(totalCount == 400, s"Should preserve all 400 records, got $totalCount")

    // Verify each partition still has the correct data
    testData.foreach {
      case (year, quarter, idRange) =>
        val partitionData = finalData.filter(
          col("year") === year && col("quarter") === quarter
        )
        val partitionCount = partitionData.count()
        val expectedCount  = idRange.size

        assert(
          partitionCount == expectedCount,
          s"Partition year=$year quarter=$quarter should have $expectedCount records, got $partitionCount"
        )

        // Verify ID ranges are correct
        val actualIds   = partitionData.select("id").collect().map(_.getLong(0)).sorted
        val expectedIds = idRange.toArray
        assert(actualIds.sameElements(expectedIds), s"Partition year=$year quarter=$quarter has incorrect IDs")
    }

    logger.info("✅ Partition-aware merge validation passed")
    logger.info("✅ No cross-partition merging occurred")
    logger.info("✅ All data integrity checks passed")
  }

  test("MergeSplitsExecutor should detect and prevent cross-partition merges in findMergeableGroups") {
    import io.indextables.spark.sql.{MergeSplitsExecutor, MergeGroup}

    logger.info("Testing MergeSplitsExecutor partition validation logic")

    // Create test files with different partition values
    val partition2023Q1 = Map("year" -> "2023", "quarter" -> "Q1")
    val partition2023Q2 = Map("year" -> "2023", "quarter" -> "Q2")
    val partition2024Q1 = Map("year" -> "2024", "quarter" -> "Q1")

    val filesPartition1 = Seq(
      AddAction(
        path = "year=2023/quarter=Q1/file1.split",
        partitionValues = partition2023Q1,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "year=2023/quarter=Q1/file2.split",
        partitionValues = partition2023Q1,
        size = 2000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val filesPartition2 = Seq(
      AddAction(
        path = "year=2023/quarter=Q2/file3.split",
        partitionValues = partition2023Q2,
        size = 1500L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val _filesPartition3 = Seq(
      AddAction(
        path = "year=2024/quarter=Q1/file4.split",
        partitionValues = partition2024Q1,
        size = 1800L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    // Create executor
    val executor = new MergeSplitsExecutor(
      spark,
      transactionLog,
      new Path(tempTablePath),
      Seq.empty,
      5L * 1024L * 1024L * 1024L, // 5GB
      None,                       // maxDestSplits
      None,                       // maxSourceSplitsPerMerge
      false
    )

    // Use reflection to access private findMergeableGroups method
    val findMergeableGroupsMethod = classOf[MergeSplitsExecutor].getDeclaredMethod(
      "findMergeableGroups",
      classOf[Map[String, String]],
      classOf[Seq[AddAction]],
      classOf[Int]
    )
    findMergeableGroupsMethod.setAccessible(true)

    // Test 1: Valid case - all files from same partition
    logger.info("Test 1: Valid case - all files from same partition")
    val defaultMaxSourceSplitsPerMerge: java.lang.Integer = 1000
    val validGroups = findMergeableGroupsMethod
      .invoke(executor, partition2023Q1, filesPartition1, defaultMaxSourceSplitsPerMerge)
      .asInstanceOf[Seq[MergeGroup]]

    assert(validGroups.nonEmpty, "Should create merge groups for same-partition files")
    assert(validGroups.head.files.length == 2, "Should group both files from same partition")
    logger.info("✅ Valid same-partition grouping works correctly")

    // Test 2: Invalid case - files from different partitions should cause error
    logger.info("Test 2: Invalid case - files from different partitions")
    val mixedFiles = filesPartition1 ++ filesPartition2 // Mix files from different partitions

    val exception = intercept[java.lang.reflect.InvocationTargetException] {
      findMergeableGroupsMethod.invoke(executor, partition2023Q1, mixedFiles, defaultMaxSourceSplitsPerMerge)
    }

    val cause = exception.getCause
    assert(
      cause.isInstanceOf[IllegalStateException],
      s"Should throw IllegalStateException, got: ${cause.getClass.getSimpleName}"
    )
    assert(
      cause.getMessage.contains("findMergeableGroups received files from wrong partitions"),
      s"Should detect cross-partition files, got: ${cause.getMessage}"
    )
    logger.info("✅ Cross-partition detection works correctly")

    // Test 3: Edge case - single file (should not create groups)
    logger.info("Test 3: Edge case - single file")
    val singleFileGroups = findMergeableGroupsMethod
      .invoke(executor, partition2023Q2, filesPartition2, defaultMaxSourceSplitsPerMerge)
      .asInstanceOf[Seq[MergeGroup]]

    assert(singleFileGroups.isEmpty, "Should not create groups for single file")
    logger.info("✅ Single file handling works correctly")

    logger.info("✅ All MergeSplitsExecutor partition validation tests passed")
  }

  test("MergeSplitsExecutor should validate partition consistency in createMergedSplit") {
    import io.indextables.spark.sql.{MergeSplitsExecutor, MergeGroup, SerializableAwsConfig, SerializableAzureConfig}

    logger.info("Testing createMergedSplit partition validation")

    // Create test files with different partition values
    val partition2023Q1 = Map("year" -> "2023", "quarter" -> "Q1")
    val partition2023Q2 = Map("year" -> "2023", "quarter" -> "Q2")

    val invalidMergeGroup = MergeGroup(
      partitionValues = partition2023Q1,
      files = Seq(
        AddAction(
          path = "year=2023/quarter=Q1/file1.split",
          partitionValues = partition2023Q1,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        ),
        AddAction(
          path = "year=2023/quarter=Q2/file2.split", // Different partition!
          partitionValues = partition2023Q2,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
      )
    )

    // Create empty configs for test
    val awsConfig = SerializableAwsConfig(
      configs = Map("spark.indextables.aws.region" -> "us-east-1"),
      tablePath = tempTablePath
    )
    val azureConfig = SerializableAzureConfig(None, None, None, None, None, None, None, None)

    // Use reflection to access private createMergedSplitDistributed method from companion object
    val createMergedSplitMethod = MergeSplitsExecutor.getClass.getDeclaredMethod(
      "createMergedSplitDistributed",
      classOf[MergeGroup],
      classOf[String],
      classOf[SerializableAwsConfig],
      classOf[SerializableAzureConfig]
    )
    createMergedSplitMethod.setAccessible(true)

    // This should throw an IllegalStateException due to cross-partition validation
    val exception = intercept[java.lang.reflect.InvocationTargetException] {
      createMergedSplitMethod.invoke(
        MergeSplitsExecutor,
        invalidMergeGroup,
        tempTablePath,
        awsConfig,
        azureConfig
      )
    }

    val cause = exception.getCause
    assert(
      cause.isInstanceOf[IllegalStateException],
      s"Should throw IllegalStateException, got: ${cause.getClass.getSimpleName}"
    )
    assert(
      cause.getMessage.contains("Cross-partition merge detected"),
      s"Should detect cross-partition merge, got: ${cause.getMessage}"
    )

    logger.info("✅ createMergedSplit partition validation works correctly")
  }

  test("MERGE SPLITS with WHERE clause should only process specified partitions") {
    logger.info("Testing MERGE SPLITS with partition filtering")

    // Create data for multiple partitions
    val testData = Seq(
      ("2023", "Q1", 1 to 50),
      ("2023", "Q2", 51 to 100),
      ("2024", "Q1", 101 to 150)
    )

    // Write data for each partition
    testData.foreach {
      case (year, quarter, idRange) =>
        val data = spark
          .range(idRange.start, idRange.end + 1)
          .select(
            col("id"),
            lit(s"content_${year}_$quarter").as("content"),
            lit(year).as("year"),
            lit(quarter).as("quarter")
          )

        // Write multiple small batches to create multiple files per partition
        val chunkSize = idRange.size / 2
        idRange.grouped(chunkSize).foreach { chunk =>
          val chunkData = data.filter(col("id").isin(chunk: _*))
          chunkData
            .coalesce(1)
            .write
            .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
            .option("spark.indextables.indexWriter.batchSize", "20")
            .partitionBy("year", "quarter")
            .mode("append")
            .save(tempTablePath)
        }
    }

    // Get initial state
    val initialFiles            = transactionLog.listFiles()
    val initialFilesByPartition = initialFiles.groupBy(_.partitionValues)

    logger.info(s"Initial state: ${initialFiles.length} files across ${initialFilesByPartition.size} partitions")
    initialFilesByPartition.foreach {
      case (partitionValues, files) =>
        logger.info(s"  Partition $partitionValues: ${files.length} files")
    }

    // Execute MERGE SPLITS with WHERE clause to target only 2023 Q1 partition
    logger.info("Executing MERGE SPLITS with WHERE year = '2023' AND quarter = 'Q1'")
    spark.sql(s"MERGE SPLITS '$tempTablePath' WHERE year = '2023' AND quarter = 'Q1' TARGET SIZE ${10 * 1024 * 1024}")

    // Refresh and check results
    transactionLog.invalidateCache()
    val finalFiles            = transactionLog.listFiles()
    val finalFilesByPartition = finalFiles.groupBy(_.partitionValues)

    logger.info(s"After targeted merge: ${finalFiles.length} files across ${finalFilesByPartition.size} partitions")
    finalFilesByPartition.foreach {
      case (partitionValues, files) =>
        logger.info(s"  Partition $partitionValues: ${files.length} files")
    }

    // Verify that only the targeted partition was affected
    val target2023Q1       = Map("year" -> "2023", "quarter" -> "Q1")
    val initial2023Q1Files = initialFilesByPartition.getOrElse(target2023Q1, Seq.empty).length
    val final2023Q1Files   = finalFilesByPartition.getOrElse(target2023Q1, Seq.empty).length

    assert(
      final2023Q1Files <= initial2023Q1Files,
      s"Target partition should have same or fewer files: $final2023Q1Files vs $initial2023Q1Files"
    )

    // Verify other partitions were not affected
    val otherPartitions = initialFilesByPartition.keys.filterNot(_ == target2023Q1)
    otherPartitions.foreach { partition =>
      val initialCount = initialFilesByPartition(partition).length
      val finalCount   = finalFilesByPartition.getOrElse(partition, Seq.empty).length
      assert(
        finalCount == initialCount,
        s"Non-target partition $partition should be unchanged: $finalCount vs $initialCount"
      )
    }

    // Verify data integrity
    val finalData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val totalCount = finalData.count()
    assert(totalCount == 150, s"Should preserve all 150 records, got $totalCount")

    logger.info("✅ Partition filtering with WHERE clause works correctly")
    logger.info("✅ Non-target partitions remain unchanged")
  }
}
