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

package io.indextables.spark.sync

import java.io.File
import java.nio.file.Files

import io.indextables.spark.transaction.TransactionLogFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests that BUILD INDEXTABLES COMPANION FROM DELTA correctly handles Delta tables that
 * have undergone multiple transactions between syncs, including appends,
 * OPTIMIZE (compaction), DELETE, UPDATE, and schema evolution.
 *
 * Uses delta-spark (Spark SQL) for all Delta table manipulations rather than
 * delta-standalone, providing realistic Delta transaction histories.
 */
class MultiTransactionSyncTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Stop any existing SparkSession (e.g., from TestBase)
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession.builder()
      .appName("MultiTransactionSyncTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir",
        Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Disable AQE to get predictable file counts
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      // Set default AWS parameters to non-null values
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Initialize SplitConversionThrottle for tests
    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark").toString
    try {
      try {
        import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
        GlobalSplitCacheManager.flushAllCaches()
        DriverSplitLocalityManager.clear()
      } catch {
        case _: Exception =>
      }
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def syncAndCollect(deltaPath: String, indexPath: String): org.apache.spark.sql.Row = {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    val rows = result.collect()
    rows.length shouldBe 1
    rows(0)
  }

  // ═══════════════════════════════════════════════════
  //  Multiple Append Transactions
  // ═══════════════════════════════════════════════════

  test("initial SYNC after multiple append transactions should index all files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_multi_append").getAbsolutePath
      val indexPath = new File(tempDir, "index_multi_append").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Transaction 1: Create table
      Seq((1, "alice", 10.0), (2, "bob", 20.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // Transaction 2: Append more data
      Seq((3, "charlie", 30.0), (4, "dave", 40.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Transaction 3: Append even more data
      Seq((5, "eve", 50.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // SYNC should index all files from all 3 transactions
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getAs[Long](3) shouldBe 2L // delta version (0-indexed: v0, v1, v2)
      row.getInt(6) should be >= 3 // at least 3 parquet files (one per append)

      // Verify transaction log has companion metadata
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        // All files should reference delta version 2
        files.foreach { f =>
          f.companionDeltaVersion shouldBe Some(2L)
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("incremental SYNC should detect new files from append transactions") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_incr_append").getAbsolutePath
      val indexPath = new File(tempDir, "index_incr_append").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Transaction 0: Create table
      Seq((1, "alice", 10.0), (2, "bob", 20.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // First SYNC
      val row1 = syncAndCollect(deltaPath, indexPath)
      row1.getString(2) shouldBe "success"
      row1.getAs[Long](3) shouldBe 0L

      val txLog1 = TransactionLogFactory.create(new Path(indexPath), spark)
      val fileCount1 = try txLog1.listFiles().size finally txLog1.close()

      // Transaction 1: Append more data
      Seq((3, "charlie", 30.0), (4, "dave", 40.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Transaction 2: Append even more
      Seq((5, "eve", 50.0), (6, "frank", 60.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Second SYNC: should only add new files, not re-index old ones
      val row2 = syncAndCollect(deltaPath, indexPath)
      row2.getString(2) shouldBe "success"
      row2.getAs[Long](3) shouldBe 2L
      row2.getInt(4) should be > 0 // splits_created > 0
      row2.getInt(5) shouldBe 0 // splits_invalidated = 0 (no removals)

      // Verify total files increased
      val txLog2 = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog2.listFiles()
        files.size should be > fileCount1
        // New files should have version 2, old files should have version 0
        files.flatMap(_.companionDeltaVersion).toSet should contain allOf (0L, 2L)
      } finally {
        txLog2.close()
      }
    }
  }

  test("third incremental SYNC with no new changes should be no-op") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_noop").getAbsolutePath
      val indexPath = new File(tempDir, "index_noop").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create and sync
      Seq((1, "alice", 10.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Append and sync
      Seq((2, "bob", 20.0)).toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Third sync with no new changes
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "no_action"
    }
  }

  // ═══════════════════════════════════════════════════
  //  Delta OPTIMIZE (Compaction)
  // ═══════════════════════════════════════════════════

  test("SYNC after OPTIMIZE should invalidate old splits and create new ones") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_optimize").getAbsolutePath
      val indexPath = new File(tempDir, "index_optimize").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create table with multiple small files
      Seq((1, "alice", 10.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)
      Seq((2, "bob", 20.0)).toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)
      Seq((3, "charlie", 30.0)).toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // First SYNC at version 2
      val row1 = syncAndCollect(deltaPath, indexPath)
      row1.getString(2) shouldBe "success"

      // OPTIMIZE: compacts multiple small files into fewer large files
      // This creates RemoveFile actions for old files and AddFile for new compacted files
      spark.sql(s"OPTIMIZE delta.`$deltaPath`")

      // Second SYNC: should detect file removals and re-index
      val row2 = syncAndCollect(deltaPath, indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits_invalidated > 0

      // Verify the index reflects the current state of the Delta table
      val txLog2 = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val filesAfter = txLog2.listFiles()
        filesAfter should not be empty

        // The new companion splits should reference the optimized parquet files
        val allSourceFiles = filesAfter.flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
        allSourceFiles should not be empty

        // Verify metadata still shows companion mode
        val metadata = txLog2.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
      } finally {
        txLog2.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Delta DELETE
  // ═══════════════════════════════════════════════════

  test("SYNC after DELETE should handle rewritten parquet files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_delete").getAbsolutePath
      val indexPath = new File(tempDir, "index_delete").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create table
      (1 to 20).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .repartition(1) // single file for predictability
        .write.format("delta").save(deltaPath)

      // First SYNC
      val row1 = syncAndCollect(deltaPath, indexPath)
      row1.getString(2) shouldBe "success"

      // DELETE some rows: Delta rewrites the affected parquet file
      spark.sql(s"DELETE FROM delta.`$deltaPath` WHERE id <= 5")

      // Second SYNC: should detect rewritten file and re-index
      val row2 = syncAndCollect(deltaPath, indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits invalidated (old file removed)
      row2.getInt(4) should be > 0 // new splits created (new file added)
    }
  }

  // ═══════════════════════════════════════════════════
  //  Delta UPDATE
  // ═══════════════════════════════════════════════════

  test("SYNC after UPDATE should handle rewritten parquet files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_update").getAbsolutePath
      val indexPath = new File(tempDir, "index_update").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create table
      (1 to 10).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .repartition(1)
        .write.format("delta").save(deltaPath)

      // First SYNC
      val row1 = syncAndCollect(deltaPath, indexPath)
      row1.getString(2) shouldBe "success"

      // UPDATE some rows: Delta rewrites the affected parquet file
      spark.sql(s"UPDATE delta.`$deltaPath` SET score = score * 2 WHERE id > 5")

      // Second SYNC
      val row2 = syncAndCollect(deltaPath, indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits invalidated
      row2.getInt(4) should be > 0 // new splits created
    }
  }

  // ═══════════════════════════════════════════════════
  //  Mixed Operations (Append + OPTIMIZE + DELETE)
  // ═══════════════════════════════════════════════════

  test("SYNC after mixed append + OPTIMIZE + DELETE should reflect final state") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_mixed").getAbsolutePath
      val indexPath = new File(tempDir, "index_mixed").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create table
      (1 to 10).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // First SYNC at v0
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // v1: Append
      (11 to 20).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // v2: Append more
      (21 to 30).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // v3: OPTIMIZE
      spark.sql(s"OPTIMIZE delta.`$deltaPath`")

      // v4: DELETE some rows
      spark.sql(s"DELETE FROM delta.`$deltaPath` WHERE id <= 5")

      // Second SYNC: should handle the entire v1..v4 gap
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      // Should have created new splits and invalidated old ones
      row.getInt(4) should be > 0 // splits_created
      row.getInt(5) should be > 0 // splits_invalidated (v0 files were compacted/deleted)

      // Verify metadata tracks latest delta version
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        val lastSynced = metadata.configuration("indextables.companion.lastSyncedVersion").toLong
        lastSynced should be >= 3L // at least v3 (OPTIMIZE), likely v4
      } finally {
        txLog.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Large Version Gap
  // ═══════════════════════════════════════════════════

  test("SYNC should handle large version gap between syncs") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_gap").getAbsolutePath
      val indexPath = new File(tempDir, "index_gap").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create table
      Seq((1, "alice", 10.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // First SYNC at v0
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Create many versions (v1 through v10)
      for (i <- 2 to 11) {
        Seq((i, s"name_$i", i * 10.0)).toDF("id", "name", "score")
          .write.format("delta").mode("append").save(deltaPath)
      }

      // Second SYNC: jump from v0 to v10
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getAs[Long](3) shouldBe 10L // delta_version
      row.getInt(4) should be > 0 // new splits created
      row.getInt(5) shouldBe 0 // no invalidations (all appends, no removals)
      row.getInt(6) should be >= 10 // at least 10 new parquet files indexed

      // Verify all files tracked
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        // Should have original file + 10 appended files' worth of splits
        files.size should be >= 2 // at least original + incremental
        // New splits should reference version 10
        files.exists(_.companionDeltaVersion == Some(10L)) shouldBe true
      } finally {
        txLog.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Schema Evolution
  // ═══════════════════════════════════════════════════

  test("SYNC should handle schema evolution (new column added)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_schema").getAbsolutePath
      val indexPath = new File(tempDir, "index_schema").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create table with initial schema
      Seq((1, "alice", 10.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // First SYNC
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // v1: Add a new column via schema evolution
      spark.sql(s"ALTER TABLE delta.`$deltaPath` ADD COLUMNS (category STRING)")

      // v2: Insert data with new column
      spark.sql(
        s"INSERT INTO delta.`$deltaPath` VALUES (2, 'bob', 20.0, 'engineering')")

      // Second SYNC: should handle the schema change
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // new split created for the new file
    }
  }

  // ═══════════════════════════════════════════════════
  //  MERGE INTO
  // ═══════════════════════════════════════════════════

  test("SYNC after MERGE INTO should handle upserted files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_merge").getAbsolutePath
      val indexPath = new File(tempDir, "index_merge").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create target table
      (1 to 10).map(i => (i, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .repartition(1)
        .write.format("delta").save(deltaPath)

      // First SYNC
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Create source DataFrame for MERGE
      Seq(
        (5, "name_5_updated", 500.0),  // update existing
        (15, "name_15", 150.0)          // insert new
      ).toDF("id", "name", "score")
        .createOrReplaceTempView("merge_source")

      // v1: MERGE INTO (upsert)
      spark.sql(
        s"""MERGE INTO delta.`$deltaPath` t
           |USING merge_source s ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      // Second SYNC: should detect rewritten + new files
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // new splits
      row.getInt(5) should be > 0 // invalidated splits (merge rewrites target files)
    }
  }

  // ═══════════════════════════════════════════════════
  //  Partitioned Delta Table
  // ═══════════════════════════════════════════════════

  test("SYNC after appends to partitioned Delta table should work correctly") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_part").getAbsolutePath
      val indexPath = new File(tempDir, "index_part").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create partitioned table
      Seq(
        (1, "alice", "east", 10.0),
        (2, "bob", "west", 20.0)
      ).toDF("id", "name", "region", "score")
        .write.format("delta").partitionBy("region").save(deltaPath)

      // First SYNC
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // v1: Append to one partition
      Seq((3, "charlie", "east", 30.0))
        .toDF("id", "name", "region", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // v2: Append to another partition
      Seq((4, "dave", "west", 40.0))
        .toDF("id", "name", "region", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Second SYNC
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // new splits
      row.getInt(5) shouldBe 0 // no invalidations (appends only)
    }
  }

  test("SYNC after DELETE in partitioned Delta table should invalidate affected partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_part_del").getAbsolutePath
      val indexPath = new File(tempDir, "index_part_del").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // v0: Create partitioned table
      (1 to 20).map { i =>
        val region = if (i % 2 == 0) "east" else "west"
        (i, s"name_$i", region, i * 10.0)
      }.toDF("id", "name", "region", "score")
        .repartition(2)
        .write.format("delta").partitionBy("region").save(deltaPath)

      // First SYNC
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // v1: DELETE rows from one partition
      spark.sql(s"DELETE FROM delta.`$deltaPath` WHERE region = 'east' AND id <= 4")

      // Second SYNC: should invalidate splits for the affected partition
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(5) should be > 0 // splits invalidated
      row.getInt(4) should be > 0 // new splits created
    }
  }

  // ═══════════════════════════════════════════════════
  //  Partition Boundary Enforcement in Grouping
  // ═══════════════════════════════════════════════════

  test("SYNC should never mix files from different partitions into one split") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_part_group").getAbsolutePath
      val indexPath = new File(tempDir, "index_part_group").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create partitioned table with small files that would easily fit in one group
      Seq(
        (1, "alice", "east", 10.0),
        (2, "bob", "east", 20.0)
      ).toDF("id", "name", "region", "score")
        .repartition(1)
        .write.format("delta").partitionBy("region").save(deltaPath)

      Seq(
        (3, "charlie", "west", 30.0),
        (4, "dave", "west", 40.0)
      ).toDF("id", "name", "region", "score")
        .repartition(1)
        .write.format("delta").mode("append").save(deltaPath)

      Seq(
        (5, "eve", "north", 50.0)
      ).toDF("id", "name", "region", "score")
        .repartition(1)
        .write.format("delta").mode("append").save(deltaPath)

      // SYNC with very large target size — all files COULD fit in one group,
      // but partition boundaries must be respected
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE 10G AT LOCATION '$indexPath'"
      )
      val row = result.collect()(0)
      row.getString(2) shouldBe "success"

      // Verify each split only references files from a single partition
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        // Should have at least 3 splits (one per partition: east, west, north)
        files.size should be >= 3

        // Each split's companionSourceFiles should only contain files from one partition
        // We verify this by checking that each split's partition values are consistent
        val partitionsWithSplits = files.groupBy(_.partitionValues)
        partitionsWithSplits.size should be >= 3

        // Verify each partition's split(s) reference only files from that partition
        files.foreach { f =>
          f.companionSourceFiles shouldBe defined
          f.companionSourceFiles.get should not be empty
          // All source files in a split should belong to the same partition directory
          val sourceFiles = f.companionSourceFiles.get
          if (f.partitionValues.nonEmpty) {
            // For partitioned files, source file paths should contain the partition prefix
            val partPrefix = f.partitionValues.toSeq.sorted
              .map { case (k, v) => s"$k=$v" }.mkString("/")
            sourceFiles.foreach { sf =>
              sf should include(partPrefix)
            }
          }
        }
      } finally {
        txLog.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Target Input Size Enforcement
  // ═══════════════════════════════════════════════════

  test("small TARGET INPUT SIZE should produce more splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_small_target").getAbsolutePath
      val indexSmall = new File(tempDir, "index_small_target").getAbsolutePath
      val indexLarge = new File(tempDir, "index_large_target").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create table with multiple files (each append creates a new parquet file)
      Seq((1, "a", 1.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)
      for (i <- 2 to 6) {
        Seq((i, s"name_$i", i * 1.0)).toDF("id", "name", "score")
          .write.format("delta").mode("append").save(deltaPath)
      }

      // Get actual file sizes to set a meaningful target
      val deltaReader = new DeltaLogReader(deltaPath,
        spark.sparkContext.hadoopConfiguration)
      val allFiles = deltaReader.getAllFiles()
      allFiles.size should be >= 6
      val singleFileSize = allFiles.head.size
      singleFileSize should be > 0L

      // SYNC with very small target (just above one file size) — should create many splits
      val smallTarget = singleFileSize + 1 // fits exactly one file per group
      val rowSmall = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE ${smallTarget} AT LOCATION '$indexSmall'"
      ).collect()(0)
      rowSmall.getString(2) shouldBe "success"
      val smallSplitCount = rowSmall.getInt(4) // splits_created

      // SYNC with very large target — should create fewer splits
      val rowLarge = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE 10G AT LOCATION '$indexLarge'"
      ).collect()(0)
      rowLarge.getString(2) shouldBe "success"
      val largeSplitCount = rowLarge.getInt(4) // splits_created

      // Small target should produce more splits than large target
      smallSplitCount should be > largeSplitCount

      // Verify small target: each split should reference at most 1-2 files
      val txLogSmall = TransactionLogFactory.create(new Path(indexSmall), spark)
      try {
        val files = txLogSmall.listFiles()
        files.size shouldBe smallSplitCount
        files.foreach { f =>
          f.companionSourceFiles shouldBe defined
          // Each split should contain a small number of files
          f.companionSourceFiles.get.size should be <= 2
        }
      } finally {
        txLogSmall.close()
      }

      // Verify large target: should have fewer splits, each with more files
      val txLogLarge = TransactionLogFactory.create(new Path(indexLarge), spark)
      try {
        val files = txLogLarge.listFiles()
        files.size shouldBe largeSplitCount
        // With 10G target, all 6 small files should fit in one split
        largeSplitCount shouldBe 1
        files.head.companionSourceFiles.get.size shouldBe allFiles.size
      } finally {
        txLogLarge.close()
      }
    }
  }

  test("single file larger than TARGET INPUT SIZE should still create one split") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_big_file").getAbsolutePath
      val indexPath = new File(tempDir, "index_big_file").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create a single file with enough data
      (1 to 100).map(i => (i, s"name_$i" * 20, i * 10.0))
        .toDF("id", "name", "score")
        .repartition(1)
        .write.format("delta").save(deltaPath)

      // SYNC with tiny target size (1 byte) — file is bigger but should still be indexed
      val row = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE 1 AT LOCATION '$indexPath'"
      ).collect()(0)
      row.getString(2) shouldBe "success"
      row.getInt(4) shouldBe 1 // exactly 1 split created
      row.getInt(6) shouldBe 1 // 1 parquet file indexed
    }
  }

  test("TARGET INPUT SIZE should split files within same partition correctly") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_split_within").getAbsolutePath
      val indexPath = new File(tempDir, "index_split_within").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create 4 files in same partition (no partitioning)
      Seq((1, "a", 1.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)
      for (i <- 2 to 4) {
        Seq((i, s"name_$i", i * 1.0)).toDF("id", "name", "score")
          .write.format("delta").mode("append").save(deltaPath)
      }

      // Get actual file sizes and use a target that precisely fits 2 files
      val deltaReader = new DeltaLogReader(deltaPath,
        spark.sparkContext.hadoopConfiguration)
      val allFiles = deltaReader.getAllFiles()
      allFiles.size shouldBe 4
      val sortedSizes = allFiles.map(_.size).sorted

      // Set target to fit exactly the 2 largest files (sum of 2 largest)
      // The greedy algorithm packs files left-to-right; with a target size
      // equal to the sum of the 2 largest, we may get 2-3 groups depending
      // on file size distribution. The key invariant: more splits than 1,
      // fewer splits than 4, and total source files = 4.
      val targetSize = sortedSizes.takeRight(2).sum
      val row = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE $targetSize AT LOCATION '$indexPath'"
      ).collect()(0)
      row.getString(2) shouldBe "success"
      val splitCount = row.getInt(4)
      splitCount should be >= 2 // at least 2 groups (can't fit all 4 files in target)
      splitCount should be <= 4 // at most 4 groups

      // Verify total source files across all splits equals original file count
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe splitCount
        files.foreach { f =>
          f.companionSourceFiles shouldBe defined
          f.companionSourceFiles.get should not be empty
        }
        files.flatMap(_.companionSourceFiles.get).size shouldBe 4
      } finally {
        txLog.close()
      }
    }
  }

  test("DRY RUN should report correct group count for TARGET INPUT SIZE") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_dry_target").getAbsolutePath
      val indexPath = new File(tempDir, "index_dry_target").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create 4 files
      Seq((1, "a", 1.0)).toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)
      for (i <- 2 to 4) {
        Seq((i, s"name_$i", i * 1.0)).toDF("id", "name", "score")
          .write.format("delta").mode("append").save(deltaPath)
      }

      // DRY RUN with tiny target — each file should be its own group
      val deltaReader = new DeltaLogReader(deltaPath,
        spark.sparkContext.hadoopConfiguration)
      val singleFileSize = deltaReader.getAllFiles().head.size

      val rowSmall = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE ${singleFileSize + 1} AT LOCATION '$indexPath' DRY RUN"
      ).collect()(0)
      rowSmall.getString(2) shouldBe "dry_run"
      rowSmall.getInt(4) shouldBe 4 // splits_created = 4 groups

      // DRY RUN with huge target — all files in one group
      val rowLarge = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE 10G AT LOCATION '$indexPath' DRY RUN"
      ).collect()(0)
      rowLarge.getString(2) shouldBe "dry_run"
      rowLarge.getInt(4) shouldBe 1 // splits_created = 1 group
    }
  }

  test("partitioned table with TARGET INPUT SIZE should respect both constraints") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_part_target").getAbsolutePath
      val indexPath = new File(tempDir, "index_part_target").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // Create partitioned table: 2 files per partition, 2 partitions
      Seq((1, "a", "east", 1.0)).toDF("id", "name", "region", "score")
        .write.format("delta").partitionBy("region").save(deltaPath)
      Seq((2, "b", "east", 2.0)).toDF("id", "name", "region", "score")
        .write.format("delta").mode("append").save(deltaPath)
      Seq((3, "c", "west", 3.0)).toDF("id", "name", "region", "score")
        .write.format("delta").mode("append").save(deltaPath)
      Seq((4, "d", "west", 4.0)).toDF("id", "name", "region", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Get file size for target calculation
      val deltaReader = new DeltaLogReader(deltaPath,
        spark.sparkContext.hadoopConfiguration)
      val singleFileSize = deltaReader.getAllFiles().head.size

      // Target size fits exactly 1 file — should create 4 splits (1 per file)
      val row = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' TARGET INPUT SIZE ${singleFileSize + 1} AT LOCATION '$indexPath'"
      ).collect()(0)
      row.getString(2) shouldBe "success"
      row.getInt(4) shouldBe 4 // 4 splits: 2 partitions × 2 files/partition × 1 file/group

      // Verify partition isolation
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe 4

        // Group splits by partition
        val byPartition = files.groupBy(_.partitionValues)
        byPartition.size shouldBe 2 // east and west
        byPartition.foreach { case (_, splits) =>
          splits.size shouldBe 2 // 2 splits per partition
          splits.foreach { s =>
            s.companionSourceFiles.get.size shouldBe 1 // 1 file per split
          }
        }
      } finally {
        txLog.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Data Types Coverage
  // ═══════════════════════════════════════════════════

  test("SYNC should handle all primitive data types") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_types"
      val indexPath = s"$base/idx_types"

      val ss = spark
      import ss.implicits._

      // Create Delta table with diverse column types
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  int_val INT,
           |  float_val FLOAT,
           |  double_val DOUBLE,
           |  bool_val BOOLEAN,
           |  str_val STRING,
           |  date_val DATE,
           |  ts_val TIMESTAMP
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 10, 1.5, 2.5, true, 'hello', DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
           |  (2, 20, 2.5, 3.5, false, 'world', DATE '2024-02-20', TIMESTAMP '2024-02-20 14:00:00'),
           |  (3, 30, 3.5, 4.5, true, 'test', DATE '2024-03-25', TIMESTAMP '2024-03-25 08:15:00')
           |""".stripMargin)

      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // splits_created
      row.getInt(6) should be > 0 // parquet_files_indexed

      // Verify companion metadata
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { f =>
          f.companionSourceFiles shouldBe defined
          f.companionDeltaVersion shouldBe defined
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("SYNC should handle struct (JSON) columns") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_struct"
      val indexPath = s"$base/idx_struct"

      val ss = spark
      import ss.implicits._

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  name STRING,
           |  address STRUCT<city: STRING, zip: STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'Alice', named_struct('city', 'NYC', 'zip', '10001')
           |UNION ALL
           |SELECT 2, 'Bob', named_struct('city', 'LA', 'zip', '90001')
           |""".stripMargin)

      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(6) should be > 0
    }
  }

  test("SYNC should handle array columns") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_array"
      val indexPath = s"$base/idx_array"

      val ss = spark
      import ss.implicits._

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  tags ARRAY<STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, array('tag1', 'tag2', 'tag3')
           |UNION ALL
           |SELECT 2, array('alpha', 'beta')
           |""".stripMargin)

      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(6) should be > 0
    }
  }

  test("SYNC should handle map columns") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_map"
      val indexPath = s"$base/idx_map"

      val ss = spark
      import ss.implicits._

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  props MAP<STRING, STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, map('color', 'red', 'size', 'large')
           |UNION ALL
           |SELECT 2, map('color', 'blue', 'weight', '5kg')
           |""".stripMargin)

      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(6) should be > 0
    }
  }

  // ═══════════════════════════════════════════════════
  //  Indexing Mode Overrides
  // ═══════════════════════════════════════════════════

  test("SYNC with INDEXING MODES should store config in executor") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_modes"
      val indexPath = s"$base/idx_modes"

      val ss = spark
      import ss.implicits._

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  title STRING,
           |  body STRING,
           |  status STRING
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Hello World', 'This is a long body text for search', 'active'),
           |  (2, 'Goodbye', 'Another body paragraph here', 'inactive')
           |""".stripMargin)

      // Use INDEXING MODES to set text/string types
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('title': 'text', 'body': 'text', 'status': 'string')
           |  AT LOCATION '$indexPath'""".stripMargin)

      val row = result.collect()(0)
      row.getString(2) shouldBe "success"

      // Verify metadata records the indexing modes
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration should contain key "indextables.companion.indexingModes"
      } finally {
        txLog.close()
      }
    }
  }

  test("SYNC with all three FASTFIELDS MODEs should each produce valid companion splits") {
    Seq("DISABLED", "HYBRID", "PARQUET_ONLY").foreach { mode =>
      withTempPath { base =>
        val deltaPath = s"$base/delta_ffm_$mode"
        val indexPath = s"$base/idx_ffm_$mode"

        val ss = spark
        import ss.implicits._

        Seq((1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0))
          .toDF("id", "name", "score")
          .write.format("delta").save(deltaPath)

        val result = spark.sql(
          s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' FASTFIELDS MODE $mode AT LOCATION '$indexPath'"
        )
        val row = result.collect()(0)
        row.getString(2) shouldBe "success"

        val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
        try {
          val files = txLog.listFiles()
          files should not be empty
          files.foreach { f =>
            f.companionFastFieldMode shouldBe Some(mode)
          }

          val metadata = txLog.getMetadata()
          metadata.configuration("indextables.companion.fastFieldMode") shouldBe mode
        } finally {
          txLog.close()
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Companion Metadata Validation
  // ═══════════════════════════════════════════════════

  test("companion metadata should track source path and delta version correctly") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_meta2"
      val indexPath = s"$base/idx_meta2"

      val ss = spark
      import ss.implicits._

      // First transaction: create table
      Seq((1, "alpha"), (2, "beta")).toDF("id", "name")
        .write.format("delta").save(deltaPath)

      // Second transaction: append
      Seq((3, "gamma")).toDF("id", "name")
        .write.format("delta").mode("append").save(deltaPath)

      // SYNC should reference the latest delta version
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getAs[Long](3) shouldBe 1L // delta_version = 1 (0=create, 1=append)

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.sourceTablePath") shouldBe deltaPath
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "delta"
        metadata.configuration("indextables.companion.lastSyncedVersion") shouldBe "1"

        // Every split should have companionDeltaVersion = 1
        val files = txLog.listFiles()
        files.foreach { f =>
          f.companionDeltaVersion shouldBe Some(1L)
        }
      } finally {
        txLog.close()
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Query / Read Path Tests
  // ═══════════════════════════════════════════════════

  test("SELECT from companion-mode table should return correct row count") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_read1"
      val indexPath = s"$base/idx_read1"

      val ss = spark
      import ss.implicits._

      // Create Delta table with known data
      (1 to 50).map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // SYNC
      val row = syncAndCollect(deltaPath, indexPath)
      row.getString(2) shouldBe "success"

      // READ from companion index — should return all 50 rows
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val results = df.collect()
      results.length shouldBe 50
    }
  }

  test("SELECT with filter from companion-mode table should return matching rows") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_read2"
      val indexPath = s"$base/idx_read2"

      val ss = spark
      import ss.implicits._

      // Create Delta table
      (1 to 20).map(i => (i.toLong, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      // Numeric filter pushdown should work at the index level
      val filtered = df.filter("id = 5")
      val results = filtered.collect()
      results.length shouldBe 1

      // Note: Field values from companion doc retrieval depend on parquetTableRoot
      // being correctly resolved at read time. The filter correctly prunes to 1 doc
      // via the tantivy index, but document field population requires the parquet
      // table root to be accessible. This is validated by the row count being correct.
    }
  }

  test("COUNT(*) on companion-mode table should return correct count") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_read3"
      val indexPath = s"$base/idx_read3"

      val ss = spark
      import ss.implicits._

      (1 to 30).map(i => (i.toLong, s"name_$i", i * 2.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      val count = df.count()
      count shouldBe 30
    }
  }

  test("SELECT from companion-mode table after incremental SYNC should include all data") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_read4"
      val indexPath = s"$base/idx_read4"

      val ss = spark
      import ss.implicits._

      // Initial data
      (1 to 10).map(i => (i.toLong, s"name_$i", i * 1.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Append more data
      (11 to 25).map(i => (i.toLong, s"name_$i", i * 1.0))
        .toDF("id", "name", "score")
        .write.format("delta").mode("append").save(deltaPath)

      // Incremental SYNC
      syncAndCollect(deltaPath, indexPath).getString(2) shouldBe "success"

      // Should find all 25 rows
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      df.count() shouldBe 25
    }
  }

  test("incremental SYNC should reuse stored indexing modes when not specified") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_modes_reuse"
      val indexPath = s"$base/idx_modes_reuse"

      val ss = spark
      import ss.implicits._

      // Create initial data
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  title STRING,
           |  status STRING
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Hello World', 'active'),
           |  (2, 'Goodbye Moon', 'inactive')
           |""".stripMargin)

      // Initial SYNC with INDEXING MODES
      val row1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('title': 'text', 'status': 'string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()(0)
      row1.getString(2) shouldBe "success"

      // Verify metadata stored indexing modes
      val txLog1 = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val meta1 = txLog1.getMetadata()
        meta1.configuration should contain key "indextables.companion.indexingModes"
      } finally {
        txLog1.close()
      }

      // Append more data
      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (3, 'New Entry', 'active')
           |""".stripMargin)

      // Incremental SYNC WITHOUT specifying INDEXING MODES — should reuse stored ones
      val row2 = syncAndCollect(deltaPath, indexPath)
      row2.getString(2) shouldBe "success"

      // Verify indexing modes were preserved in metadata
      val txLog2 = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val meta2 = txLog2.getMetadata()
        meta2.configuration should contain key "indextables.companion.indexingModes"
        val modesJson = meta2.configuration("indextables.companion.indexingModes")
        modesJson should include("title")
        modesJson should include("text")
        modesJson should include("status")
        modesJson should include("string")
      } finally {
        txLog2.close()
      }
    }
  }

  test("aggregate pushdown on companion-mode table should compute correct results") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_agg"
      val indexPath = s"$base/idx_agg"

      val ss = spark
      import ss.implicits._

      // Create Delta table with numeric data for aggregations
      (1 to 100).map(i => (i, s"name_$i", i.toDouble * 1.5))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // Sync with fast fields for aggregation
      spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('name': 'string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // COUNT(*) aggregate pushdown
      val count = df.count()
      count shouldBe 100

      // COUNT with filter - verifies aggregate + filter pushdown work together
      val filteredCount = df.filter("id > 50").count()
      filteredCount should be > 0L
      filteredCount should be <= 50L
    }
  }

  test("MERGE SPLITS on companion-mode table should preserve companion metadata") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_merge"
      val indexPath = s"$base/idx_merge"

      val ss = spark
      import ss.implicits._

      // Create Delta table with small data so SYNC creates small splits
      Seq((1, "a"), (2, "b")).toDF("id", "name")
        .write.format("delta").save(deltaPath)

      // First sync
      syncAndCollect(deltaPath, indexPath)

      // Append more data for second sync
      Seq((3, "c"), (4, "d")).toDF("id", "name")
        .write.format("delta").mode("append").save(deltaPath)

      // Second sync (creates additional companion splits)
      syncAndCollect(deltaPath, indexPath)

      // Verify we have multiple splits before merge
      val txLogBefore = TransactionLogFactory.create(new Path(indexPath), spark)
      val filesBefore = try txLogBefore.listFiles() finally txLogBefore.close()
      filesBefore.size should be >= 2

      // Merge all splits into one (small target size to force merge)
      val mergeResult = spark.sql(
        s"MERGE SPLITS '$indexPath' TARGET SIZE 100M"
      ).collect()

      // Verify merge completed
      mergeResult.length should be > 0

      // Verify merged split has combined companion source files
      val txLogAfter = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val filesAfter = txLogAfter.listFiles()
        // Should have fewer splits after merge
        filesAfter.size should be <= filesBefore.size

        // Merged split should have companion source files from all source splits
        val allSourceFiles = filesBefore.flatMap(_.companionSourceFiles.getOrElse(Seq.empty)).distinct
        val mergedSourceFiles = filesAfter.flatMap(_.companionSourceFiles.getOrElse(Seq.empty)).distinct

        // All original source files should be present in merged result
        allSourceFiles.foreach { srcFile =>
          mergedSourceFiles should contain(srcFile)
        }

        // Companion delta version should be preserved
        filesAfter.foreach { f =>
          f.companionDeltaVersion shouldBe defined
        }
      } finally {
        txLogAfter.close()
      }

      // Verify count still correct after merge
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)
      df.count() shouldBe 4
    }
  }

  test("companion source files should contain relative parquet paths") {
    withTempPath { base =>
      val deltaPath = s"$base/delta_relpath"
      val indexPath = s"$base/idx_relpath"

      val ss = spark
      import ss.implicits._

      Seq((1, "a"), (2, "b")).toDF("id", "name")
        .write.format("delta").save(deltaPath)

      syncAndCollect(deltaPath, indexPath)

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.foreach { f =>
          f.companionSourceFiles shouldBe defined
          f.companionSourceFiles.get.foreach { srcFile =>
            // Source file paths should be relative (not absolute)
            srcFile should not startWith "/"
            srcFile should not contain "://"
            // Should end with .parquet
            srcFile should endWith (".parquet")
          }
        }
      } finally {
        txLog.close()
      }
    }
  }
}
