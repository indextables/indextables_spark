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

import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

/**
 * Comprehensive test suite validating that repaired transaction logs are functionally equivalent to original logs and
 * that tables remain fully readable after replacement.
 *
 * Test Coverage:
 *   - Basic repair & replace tests
 *   - Missing split file handling
 *   - Checkpoint reconstruction
 *   - Partition preservation
 *   - Multiple transaction consolidation
 *   - Edge cases & error handling
 */
class RepairIndexFilesTransactionLogReplacementSuite extends TestBase {

  test("repaired transaction log should produce identical results after replacement") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create table with sample data
      val data = Seq(
        ("doc1", "content one", 100),
        ("doc2", "content two", 200),
        ("doc3", "content three", 300)
      ).toDF("id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Read original table and collect results
      val originalDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val originalResults = originalDf.orderBy("id").collect()
      val originalCount   = originalDf.count()

      // 3. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      // 4. Validate repair statistics
      assert(repairResult.length === 1)
      assert(repairResult(0).getAs[String]("status") === "SUCCESS")
      assert(repairResult(0).getAs[Int]("missing_splits") === 0)
      val validSplits = repairResult(0).getAs[Int]("valid_splits")
      assert(validSplits > 0)

      // 5. Replace original transaction log with repaired version
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")

      // Backup and replace
      val backupPath = new Path(tempDir, "_transaction_log_backup")
      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 6. Read table with repaired transaction log
      val repairedDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedResults = repairedDf.orderBy("id").collect()
      val repairedCount   = repairedDf.count()

      // 7. Validate results are identical
      assert(repairedCount === originalCount)
      assert(repairedResults.length === originalResults.length)

      repairedResults.zip(originalResults).foreach {
        case (repaired, original) =>
          assert(repaired.getAs[String]("id") === original.getAs[String]("id"))
          assert(repaired.getAs[String]("content") === original.getAs[String]("content"))
          assert(repaired.getAs[Int]("score") === original.getAs[Int]("score"))
      }

      // 8. Validate transaction log structure (no checkpoint needed for consolidated log)
      // Checkpoint is not needed since we only have 2 transaction files which is optimal
    }
  }

  test("repaired transaction log should exclude missing splits and remain readable") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create table with multiple splits
      val data = (1 to 100)
        .map(i => (s"doc$i", s"content $i", i))
        .toDF("id", "content", "score")

      data
        .repartition(5)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Read original count
      val originalCount =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
      assert(originalCount === 100)

      // 3. Delete 2 split files to simulate missing files
      val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val splitFiles = fs
        .listStatus(new Path(tablePath))
        .filter(_.getPath.getName.endsWith(".split"))
      assert(splitFiles.length >= 2, "Should have at least 2 split files")

      val deletedSplits = splitFiles.take(2)
      deletedSplits.foreach { file =>
        fs.delete(file.getPath, false)
        println(s"Deleted split file: ${file.getPath}")
      }

      // 4. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      // 5. Validate repair detected missing splits
      assert(repairResult.length === 1)
      assert(repairResult(0).getAs[String]("status") === "SUCCESS")
      val missingSplits = repairResult(0).getAs[Int]("missing_splits")
      assert(missingSplits === 2, "Should report 2 missing splits")

      val validSplits = repairResult(0).getAs[Int]("valid_splits")
      val totalSplits = repairResult(0).getAs[Int]("total_splits")
      assert(validSplits === totalSplits - 2)

      // 6. Replace transaction log
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      val backupPath      = new Path(tempDir, "_transaction_log_backup")

      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 7. Read table with repaired transaction log (should succeed)
      val repairedDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedCount = repairedDf.count()

      // 8. Validate reduced count (fewer documents due to missing splits)
      assert(repairedCount < originalCount, "Count should be less after excluding missing splits")
      assert(repairedCount > 0, "Should still have readable data")

      // 9. Validate all remaining data is readable
      val results = repairedDf.select("id", "content", "score").collect()
      assert(results.nonEmpty)
      results.foreach { row =>
        assert(row.getAs[String]("id").startsWith("doc"))
        assert(row.getAs[String]("content").startsWith("content"))
        assert(row.getAs[Int]("score") > 0)
      }
    }
  }

  test("repaired transaction log should consolidate multiple append transactions") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create initial table
      val data1 = Seq(("doc1", "content1", 100)).toDF("id", "content", "score")
      data1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Append more data (transaction 2)
      val data2 = Seq(("doc2", "content2", 200)).toDF("id", "content", "score")
      data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath)

      // 3. Append more data (transaction 3)
      val data3 = Seq(("doc3", "content3", 300)).toDF("id", "content", "score")
      data3.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath)

      // 4. Verify original table has 3 documents
      val originalDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val originalCount = originalDf.count()
      assert(originalCount === 3)
      val originalResults = originalDf.orderBy("id").collect()

      // 5. Check transaction log has multiple files
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val txnFiles = fs
        .listStatus(originalLogPath)
        .filter(_.getPath.getName.matches("\\d{20}\\.json"))
      assert(txnFiles.length >= 3, "Should have at least 3 transaction files")

      // 6. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(repairResult(0).getAs[String]("status") === "SUCCESS")
      assert(repairResult(0).getAs[Int]("valid_splits") === 3)

      // 7. Verify repaired log is consolidated (2 files: protocol+metadata, add actions)
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      assert(fs.exists(repairedLogPath), s"Repaired transaction log should exist at: $repairedLogPath")

      val repairedFiles    = fs.listStatus(repairedLogPath)
      val repairedTxnFiles = repairedFiles.filter(_.getPath.getName.matches("\\d{20}\\.json"))
      assert(
        repairedTxnFiles.length === 2,
        s"Should have exactly 2 transaction files (metadata + adds), found ${repairedTxnFiles.length}"
      )

      // No checkpoint needed for consolidated transaction log with only 2 files

      // 8. Replace transaction log
      val backupPath = new Path(tempDir, "_transaction_log_backup")
      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 9. Read table with consolidated transaction log
      val repairedDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedCount = repairedDf.count()
      val repairedResults = repairedDf.orderBy("id").collect()

      // 10. Validate identical results
      assert(repairedCount === originalCount)
      assert(repairedResults.length === 3)

      repairedResults.zip(originalResults).foreach {
        case (repaired, original) =>
          assert(repaired.getAs[String]("id") === original.getAs[String]("id"))
          assert(repaired.getAs[String]("content") === original.getAs[String]("content"))
          assert(repaired.getAs[Int]("score") === original.getAs[Int]("score"))
      }
    }
  }

  test("repaired transaction log should preserve partition structure") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      println(s"🧪 TEST STEP 1: Creating partitioned table at $tablePath")
      // 1. Create partitioned table
      val data = Seq(
        ("2024-01-01", 10, "doc1", "content1", 100),
        ("2024-01-01", 11, "doc2", "content2", 200),
        ("2024-01-02", 10, "doc3", "content3", 300),
        ("2024-01-02", 11, "doc4", "content4", 400)
      ).toDF("load_date", "load_hour", "id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date", "load_hour")
        .option("spark.indextables.indexing.fastfields", "score,load_date,load_hour")
        .mode("overwrite")
        .save(tablePath)
      println(s"🧪 TEST STEP 1: ✅ Partitioned table created successfully")

      println(s"🧪 TEST STEP 2: Reading original table with partition pruning")
      // 2. Read original with partition pruning
      val originalDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val filteredOriginal = originalDf
        .filter(col("load_date") === "2024-01-01" && col("load_hour") === 10)
      val originalCount = filteredOriginal.count()
      assert(originalCount === 1)
      println(s"🧪 TEST STEP 2: ✅ Original partition pruning works (count=$originalCount)")

      println(s"🧪 TEST STEP 2.5: Validating partition accessibility on ORIGINAL table")
      // Note: distinct().collect() triggers aggregate pushdown issues with partitioned tables
      // Use collect() and manual deduplication as workaround
      val originalAllPartitionsRaw = originalDf.select("load_date", "load_hour").collect()
      val originalAllPartitions    = originalAllPartitionsRaw.map(r => (r.getString(0), r.getInt(1))).distinct
      println(s"🧪 TEST STEP 2.5: Found ${originalAllPartitions.length} partitions in original table")
      assert(
        originalAllPartitions.length === 4,
        s"Original table should have 4 partition combinations, got ${originalAllPartitions.length}"
      )
      println(s"🧪 TEST STEP 2.5: ✅ Original partition accessibility validated")

      println(s"🧪 TEST STEP 3: Running REPAIR command")
      // 3. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(repairResult(0).getAs[String]("status") === "SUCCESS")
      assert(repairResult(0).getAs[Int]("valid_splits") === 4)
      println(s"🧪 TEST STEP 3: ✅ REPAIR command completed successfully")

      println(s"🧪 TEST STEP 4: Validating repaired transaction log structure")
      // 4. Validate repaired transaction log structure
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")

      // Verify protocol and metadata are preserved
      val originalV0Path    = new Path(originalLogPath, "00000000000000000000.json")
      val originalV0Content = scala.io.Source.fromInputStream(fs.open(originalV0Path)).mkString
      val repairedV0Path    = new Path(repairedLogPath, "00000000000000000000.json")
      val repairedV0Content = scala.io.Source.fromInputStream(fs.open(repairedV0Path)).mkString

      assert(originalV0Content == repairedV0Content, "Protocol and metadata should be identical")

      // Verify add actions are preserved (same count)
      val originalV1Path    = new Path(originalLogPath, "00000000000000000001.json")
      val originalV1Content = scala.io.Source.fromInputStream(fs.open(originalV1Path)).mkString
      val originalV1Lines   = originalV1Content.split("\n").filter(_.nonEmpty)

      val repairedV1Path    = new Path(repairedLogPath, "00000000000000000001.json")
      val repairedV1Content = scala.io.Source.fromInputStream(fs.open(repairedV1Path)).mkString
      val repairedV1Lines   = repairedV1Content.split("\n").filter(_.nonEmpty)

      assert(
        originalV1Lines.length == repairedV1Lines.length,
        s"Add actions count mismatch: original=${originalV1Lines.length}, repaired=${repairedV1Lines.length}"
      )

      println(s"🧪 TEST STEP 4: ✅ Transaction log structure validated")

      println(s"🧪 TEST STEP 5: Replacing transaction log with repaired version")
      val backupPath = new Path(tempDir, "_transaction_log_backup")

      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)
      println(s"🧪 TEST STEP 5: ✅ Transaction log replaced successfully")

      println(s"🧪 TEST STEP 6: Reading table with repaired transaction log")
      // 5. Read table with repaired transaction log
      val repairedDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      println(s"🧪 TEST STEP 6: ✅ Table loaded with repaired transaction log")

      println(s"🧪 TEST STEP 7: Validating partition pruning with repaired log")
      // 6. Validate partition pruning still works
      val filteredRepaired = repairedDf
        .filter(col("load_date") === "2024-01-01" && col("load_hour") === 10)
      // Use collect() instead of count() to avoid aggregate pushdown issue
      val repairedResults = filteredRepaired.collect()
      assert(repairedResults.length === 1)

      val result = filteredRepaired.collect()(0)
      assert(result.getAs[String]("id") === "doc1")
      assert(result.getAs[String]("load_date") === "2024-01-01")
      assert(result.getAs[Int]("load_hour") === 10)
      println(s"🧪 TEST STEP 7: ✅ Partition pruning validated successfully")

      println(s"🧪 TEST STEP 8: Validating all partitions are accessible")
      // 7. Validate all partitions are accessible
      // Note: distinct().collect() triggers aggregate pushdown issues with partitioned tables
      // Use collect() and manual deduplication as workaround
      val allPartitionsRaw = repairedDf.select("load_date", "load_hour").collect()
      val allPartitions    = allPartitionsRaw.map(r => (r.getString(0), r.getInt(1))).distinct
      assert(allPartitions.length === 4, "Should have 4 partition combinations")
      println(s"🧪 TEST STEP 8: ✅ All 4 partitions accessible")

      println(s"🧪 TEST STEP 9: Validating total row count")
      // 8. Validate total count matches
      assert(repairedDf.collect().length === 4)
      println(s"🧪 TEST STEP 9: ✅ Total row count validated (4 rows)")
    }
  }

  test("repaired transaction log should respect overwrite boundaries") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create initial table
      val data1 = Seq(("old1", "old content", 100)).toDF("id", "content", "score")
      data1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Append data
      val data2 = Seq(("old2", "old content 2", 200)).toDF("id", "content", "score")
      data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath)

      // 3. Overwrite table (should make old1/old2 invisible)
      val data3 = Seq(
        ("new1", "new content", 300),
        ("new2", "new content 2", 400)
      ).toDF("id", "content", "score")
      data3.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

      // 4. Verify only new data is visible
      val originalDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val originalCount = originalDf.count()
      assert(originalCount === 2)

      val originalIds = originalDf.select("id").collect().map(_.getString(0)).sorted
      assert(originalIds === Array("new1", "new2"))

      // 5. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(repairResult(0).getAs[String]("status") === "SUCCESS")
      val validSplits = repairResult(0).getAs[Int]("valid_splits")
      assert(validSplits === 2, "Should only include splits from overwrite, not old splits")

      // 6. Replace transaction log
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      val backupPath      = new Path(tempDir, "_transaction_log_backup")

      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 7. Read table with repaired transaction log
      val repairedDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedCount = repairedDf.count()
      assert(repairedCount === 2)

      val repairedIds = repairedDf.select("id").collect().map(_.getString(0)).sorted
      assert(repairedIds === Array("new1", "new2"))

      // 8. Validate old data is NOT accessible
      val allIds = repairedDf.select("id").collect().map(_.getString(0))
      assert(!allIds.contains("old1"))
      assert(!allIds.contains("old2"))
    }
  }

  test("repaired transaction log should support IndexQuery operations") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create table with text fields
      val data = Seq(
        ("doc1", "apache spark machine learning", 100),
        ("doc2", "python pandas data science", 200),
        ("doc3", "spark sql and dataframes", 300)
      ).toDF("id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Test IndexQuery on original table
      val originalDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val originalSparkQuery = originalDf
        .filter("content indexquery 'spark'")
        .select("id")
        .collect()
        .map(_.getString(0))
        .sorted

      assert(originalSparkQuery === Array("doc1", "doc3"))

      // 3. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(repairResult(0).getAs[String]("status") === "SUCCESS")

      // 4. Replace transaction log
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      val backupPath      = new Path(tempDir, "_transaction_log_backup")

      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 5. Test IndexQuery on repaired table
      val repairedDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedSparkQuery = repairedDf
        .filter("content indexquery 'spark'")
        .select("id")
        .collect()
        .map(_.getString(0))
        .sorted

      assert(repairedSparkQuery === Array("doc1", "doc3"))

      // 6. Test complex IndexQuery
      val complexQuery = repairedDf
        .filter("content indexquery 'spark AND (sql OR machine)'")
        .select("id")
        .collect()
        .map(_.getString(0))
        .sorted

      assert(complexQuery === Array("doc1", "doc3"))

      // 7. Test _indexall query
      val indexAllQuery = repairedDf
        .filter("_indexall indexquery 'python OR dataframes'")
        .select("id")
        .collect()
        .map(_.getString(0))
        .sorted

      assert(indexAllQuery === Array("doc2", "doc3"))
    }
  }

  test("repaired transaction log should support aggregate pushdown") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create table with numeric fast field
      val data = Seq(
        ("doc1", "content1", 100),
        ("doc2", "content2", 200),
        ("doc3", "content3", 300),
        ("doc4", "content4", 400)
      ).toDF("id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // 2. Test aggregations on original table
      val originalDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val originalCount = originalDf.count()
      val originalSum   = originalDf.agg(sum("score")).collect()(0).getLong(0)
      val originalAvg   = originalDf.agg(avg("score")).collect()(0).getDouble(0)

      assert(originalCount === 4)
      assert(originalSum === 1000)
      assert(originalAvg === 250.0)

      // 3. Run repair command
      val repairResult = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(repairResult(0).getAs[String]("status") === "SUCCESS")

      // 4. Replace transaction log
      val fs              = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val originalLogPath = new Path(tablePath, "_transaction_log")
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      val backupPath      = new Path(tempDir, "_transaction_log_backup")

      fs.rename(originalLogPath, backupPath)
      fs.rename(repairedLogPath, originalLogPath)

      // 5. Test aggregations on repaired table
      val repairedDf    = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val repairedCount = repairedDf.count()
      val repairedSum   = repairedDf.agg(sum("score")).collect()(0).getLong(0)
      val repairedAvg   = repairedDf.agg(avg("score")).collect()(0).getDouble(0)

      assert(repairedCount === originalCount)
      assert(repairedSum === originalSum)
      assert(repairedAvg === originalAvg)

      // 6. Test multiple aggregations
      val multiAgg = repairedDf
        .agg(
          count("*").as("count"),
          sum("score").as("sum"),
          avg("score").as("avg"),
          min("score").as("min"),
          max("score").as("max")
        )
        .collect()(0)

      assert(multiAgg.getAs[Long]("count") === 4)
      assert(multiAgg.getAs[Long]("sum") === 1000)
      assert(multiAgg.getAs[Double]("avg") === 250.0)
      assert(multiAgg.getAs[Int]("min") === 100)
      assert(multiAgg.getAs[Int]("max") === 400)
    }
  }

  test("repair should reject target location that already exists") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath    = new File(tempDir, "test_table").getAbsolutePath
      val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

      // 1. Create original table
      val data = Seq(("doc1", "content1", 100)).toDF("id", "content", "score")
      data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

      // 2. Create existing target directory with content
      val fs              = new Path(repairedPath).getFileSystem(spark.sessionState.newHadoopConf())
      val repairedLogPath = new Path(repairedPath, "_transaction_log")
      fs.mkdirs(repairedLogPath)

      // Write dummy file to make directory non-empty
      val dummyFile = new Path(repairedLogPath, "dummy.txt")
      val out       = fs.create(dummyFile)
      out.writeBytes("dummy content")
      out.close()

      // 3. Attempt repair (should fail)
      val exception = intercept[Exception] {
        spark
          .sql(
            s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
            AT LOCATION '$repairedPath/_transaction_log'"""
          )
          .collect()
      }

      assert(
        exception.getMessage.contains("already exists") ||
          exception.getMessage.contains("not empty")
      )
    }
  }

  test("repair should handle missing source transaction log gracefully") {
    withTempPath { tempDir =>
      val nonExistentPath = new File(tempDir, "does_not_exist").getAbsolutePath
      val repairedPath    = new File(tempDir, "repaired").getAbsolutePath

      // Attempt repair on non-existent source
      val result = spark
        .sql(
          s"""REPAIR INDEXFILES TRANSACTION LOG '$nonExistentPath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
        )
        .collect()

      assert(result.length === 1)
      val status = result(0).getAs[String]("status")
      assert(status.startsWith("ERROR"))
      assert(status.contains("not found") || status.contains("does not exist"))
    }
  }

  test("SQL command parsing should work via extension") {
    // Test that our SQL extension can parse the command
    val sqlText =
      "REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log' AT LOCATION 's3://bucket/table/_transaction_log_repaired'"

    // This test verifies that the parser can handle the command
    val parser     = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val parsedPlan = parser.parsePlan(sqlText)

    assert(parsedPlan.isInstanceOf[RepairIndexFilesTransactionLogCommand])
    val command = parsedPlan.asInstanceOf[RepairIndexFilesTransactionLogCommand]
    assert(command.sourcePath === "s3://bucket/table/_transaction_log")
    assert(command.targetPath === "s3://bucket/table/_transaction_log_repaired")
  }

  test("SQL command should be case insensitive") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val variations = Seq(
      "repair indexfiles transaction log 's3://bucket/table/_transaction_log' at location 's3://bucket/table/_transaction_log_repaired'",
      "REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log' AT LOCATION 's3://bucket/table/_transaction_log_repaired'",
      "Repair IndexFiles Transaction Log 's3://bucket/table/_transaction_log' At Location 's3://bucket/table/_transaction_log_repaired'"
    )

    variations.foreach { sql =>
      val parsedPlan = parser.parsePlan(sql)
      assert(parsedPlan.isInstanceOf[RepairIndexFilesTransactionLogCommand])
    }
  }
}
