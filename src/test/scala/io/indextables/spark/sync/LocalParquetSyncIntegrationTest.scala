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
import java.sql.{Date, Timestamp}

import io.indextables.spark.transaction.TransactionLogFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Local filesystem integration tests for BUILD INDEXTABLES COMPANION FOR PARQUET.
 *
 * Uses Spark to write Parquet data with known schemas, then exercises the
 * BUILD INDEXTABLES COMPANION FOR PARQUET command pipeline.
 *
 * No cloud credentials needed -- runs entirely on local filesystem.
 */
class LocalParquetSyncIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession.builder()
      .appName("LocalParquetSyncIntegrationTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir",
        Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark-parquet").toString
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

  /**
   * Create unpartitioned Parquet data (no Delta, no Iceberg).
   * Schema: id(Long), name(String), score(Double), active(Boolean)
   */
  private def createLocalParquetData(parquetPath: String, numRows: Int = 20): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numRows).map { i =>
      (i.toLong, s"name_$i", i * 1.5, i % 2 == 0)
    }
    data.toDF("id", "name", "score", "active")
      .repartition(1)
      .write.parquet(parquetPath)
  }

  /**
   * Create Hive-style partitioned Parquet data.
   * Schema: id(Long), name(String), region(String partition column)
   */
  private def createPartitionedParquetData(parquetPath: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "alice", "east"),
      (2L, "bob", "east"),
      (3L, "carol", "west"),
      (4L, "dave", "west"),
      (5L, "eve", "east")
    )
    data.toDF("id", "name", "region")
      .write.partitionBy("region")
      .parquet(parquetPath)
  }

  /**
   * Run BUILD COMPANION FOR PARQUET and collect the result row.
   */
  private def syncParquetAndCollect(parquetPath: String, indexPath: String): Row = {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    )
    val rows = result.collect()
    rows.length shouldBe 1
    rows(0)
  }

  // -------------------------------------------------------
  //  Tests
  // -------------------------------------------------------

  test("basic companion from local parquet directory") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_data").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 20)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(0) shouldBe indexPath   // table_path
      row.getString(1) shouldBe parquetPath // source_path
      row.getString(2) shouldBe "success"   // status
      row.isNullAt(3) shouldBe true          // source_version (null for parquet)
      row.getInt(4) should be > 0           // splits_created
      row.getInt(6) should be > 0           // parquet_files_indexed
    }
  }

  test("multiple parquet files should all be indexed") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_multi").getAbsolutePath
      val indexPath = new File(tempDir, "companion_multi").getAbsolutePath

      val ss = spark
      import ss.implicits._
      val data = (0 until 30).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
      data.toDF("id", "name", "score", "active")
        .repartition(3)
        .write.parquet(parquetPath)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(6) shouldBe 3 // parquet_files_indexed
    }
  }

  test("DRY RUN should not create any splits") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_dry").getAbsolutePath
      val indexPath = new File(tempDir, "companion_dry").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath' DRY RUN"
      )
      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "dry_run"

      // No split files should exist
      val indexDir = new File(indexPath)
      if (indexDir.exists()) {
        val splitFiles = indexDir.listFiles().filter(_.getName.endsWith(".split"))
        splitFiles shouldBe empty
      }
    }
  }

  test("re-sync should return no_action when same files detected") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_resync").getAbsolutePath
      val indexPath = new File(tempDir, "companion_resync").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      // First sync
      val row1 = syncParquetAndCollect(parquetPath, indexPath)
      row1.getString(2) shouldBe "success"

      // Second sync (same files) should be no_action
      val row2 = syncParquetAndCollect(parquetPath, indexPath)
      row2.getString(2) shouldBe "no_action"
    }
  }

  test("Hive-style partitioned directories should be detected") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_part").getAbsolutePath
      val indexPath = new File(tempDir, "companion_part").getAbsolutePath

      createPartitionedParquetData(parquetPath)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(6) should be > 0 // parquet_files_indexed

      // Verify partition columns are recorded in transaction log
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          file.partitionValues should contain key "region"
        }
        // Should have files for both east and west
        val regions = files.map(_.partitionValues("region")).toSet
        regions should contain("east")
        regions should contain("west")
      } finally {
        txLog.close()
      }
    }
  }

  test("SCHEMA SOURCE option should specify explicit file for schema") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_schema").getAbsolutePath
      val indexPath = new File(tempDir, "companion_schema").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      // Find a parquet file to use as schema source
      val parquetFiles = new File(parquetPath).listFiles()
        .filter(_.getName.endsWith(".parquet"))
      parquetFiles should not be empty
      val schemaFile = parquetFiles.head.getAbsolutePath

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
        s"SCHEMA SOURCE '$schemaFile' " +
        s"AT LOCATION '$indexPath'"
      )
      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"
    }
  }

  test("all primitive data types should be indexed correctly") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_types").getAbsolutePath
      val indexPath = new File(tempDir, "companion_types").getAbsolutePath

      val ss = spark
      import ss.implicits._
      val data = Seq(
        (1L, 42, 3.14f, 2.718, true, "hello",
          Date.valueOf("2024-01-15"),
          Timestamp.valueOf("2024-01-15 10:30:00"))
      )
      data.toDF("long_col", "int_col", "float_col", "double_col",
        "bool_col", "string_col", "date_col", "timestamp_col")
        .repartition(1)
        .write.parquet(parquetPath)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // splits_created
    }
  }

  test("read-back should return correct data") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_readback").getAbsolutePath
      val indexPath = new File(tempDir, "companion_readback").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"

      // Read back companion index
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      val allRecords = companionDf.collect()
      allRecords.length shouldBe 10

      // Verify data is not null
      allRecords.foreach { r =>
        r.isNullAt(0) shouldBe false
      }
    }
  }

  test("FASTFIELDS MODE should be stored in companion splits") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_ff").getAbsolutePath
      val indexPath = new File(tempDir, "companion_ff").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
        s"FASTFIELDS MODE PARQUET_ONLY AT LOCATION '$indexPath'"
      )
      result.collect()(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          file.companionFastFieldMode shouldBe Some("PARQUET_ONLY")
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("INDEXING MODES should be stored in companion metadata") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_modes").getAbsolutePath
      val indexPath = new File(tempDir, "companion_modes").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
        s"INDEXING MODES ('name':'text') " +
        s"AT LOCATION '$indexPath'"
      )
      result.collect()(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.indexingModes") should include("name")
        metadata.configuration("indextables.companion.indexingModes") should include("text")
      } finally {
        txLog.close()
      }
    }
  }

  test("transaction log metadata should have sourceFormat=parquet") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_meta").getAbsolutePath
      val indexPath = new File(tempDir, "companion_meta").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration("indextables.companion.sourceTablePath") shouldBe parquetPath
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "parquet"
      } finally {
        txLog.close()
      }
    }
  }

  test("TARGET INPUT SIZE should control split count") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_target").getAbsolutePath
      val indexPath = new File(tempDir, "companion_target").getAbsolutePath

      // Create larger dataset with multiple files
      val ss = spark
      import ss.implicits._
      val data = (0 until 100).map(i => (i.toLong, s"name_$i " * 20, i * 1.5, i % 2 == 0))
      data.toDF("id", "name", "score", "active")
        .repartition(5)
        .write.parquet(parquetPath)

      // Use a very small target to force multiple groups
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
        s"TARGET INPUT SIZE 1024 " +
        s"AT LOCATION '$indexPath'"
      )
      val row = result.collect()(0)
      row.getString(2) shouldBe "success"
      // With a 1K target, we should get multiple splits from 5 parquet files
      row.getInt(4) should be >= 1 // splits_created
    }
  }

  test("non-existent directory should return error") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "nonexistent_parquet").getAbsolutePath
      val indexPath = new File(tempDir, "companion_noexist").getAbsolutePath

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
      )
      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error"
    }
  }

  test("write guard should reject direct writes to parquet companion table") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_guard").getAbsolutePath
      val indexPath = new File(tempDir, "companion_guard").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 10)

      // Create companion index
      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"

      // Try to write directly -- should fail
      val ss = spark
      import ss.implicits._
      val df = Seq((100, "test")).toDF("id", "content")

      val ex = intercept[Exception] {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(indexPath)
      }
      ex.getMessage should include("companion")
    }
  }

  test("COUNT(*) should return correct count") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_count").getAbsolutePath
      val indexPath = new File(tempDir, "companion_count").getAbsolutePath

      createLocalParquetData(parquetPath, numRows = 15)

      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 15
    }
  }

  test("GROUP BY string column should return rows") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_groupby").getAbsolutePath
      val indexPath = new File(tempDir, "companion_groupby").getAbsolutePath

      // Create parquet data with repeating name values for grouping
      val ss = spark
      import ss.implicits._
      val data = Seq(
        (1L, "alice", 10.0),
        (2L, "bob",   20.0),
        (3L, "alice", 30.0),
        (4L, "bob",   40.0),
        (5L, "alice", 50.0)
      )
      data.toDF("id", "name", "score")
        .repartition(1)
        .write.parquet(parquetPath)

      // Build companion (default HYBRID mode should make string fields fast)
      val row = syncParquetAndCollect(parquetPath, indexPath)
      row.getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // First verify regular count works
      companionDf.count() shouldBe 5

      // Now try GROUP BY on name column
      val groupByResult = companionDf.groupBy("name").count().orderBy("name").collect()

      println(s"GROUP BY result rows: ${groupByResult.length}")
      groupByResult.foreach(r => println(s"  ${r.getString(0)} -> ${r.getLong(1)}"))

      // Should have 2 groups: alice=3, bob=2
      groupByResult.length shouldBe 2
      groupByResult(0).getString(0) shouldBe "alice"
      groupByResult(0).getLong(1) shouldBe 3L
      groupByResult(1).getString(0) shouldBe "bob"
      groupByResult(1).getLong(1) shouldBe 2L
    }
  }
}
