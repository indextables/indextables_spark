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
 * Local filesystem integration tests for BUILD INDEXTABLES COMPANION FROM DELTA.
 *
 * Uses delta-spark to create Delta tables with known schemas, then exercises
 * the BUILD INDEXTABLES COMPANION command pipeline.
 *
 * No cloud credentials needed — runs entirely on local filesystem.
 */
class LocalSyncIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession.builder()
      .appName("LocalSyncIntegrationTest")
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

  /**
   * Create a Delta table using delta-spark's DataFrame write API.
   * Schema: id(Long), name(String), score(Double), active(Boolean)
   */
  private def createLocalDeltaTable(
    deltaPath: String,
    numRows: Int = 20
  ): Unit = {
    val ss = spark
    import ss.implicits._

    val data = (0 until numRows).map { i =>
      (i.toLong, s"name_$i", i * 1.5, i % 2 == 0)
    }
    data.toDF("id", "name", "score", "active")
      .repartition(1)
      .write.format("delta").save(deltaPath)
  }

  /**
   * Create a Delta table with a specific number of parquet files.
   */
  private def createLocalDeltaTableWithFiles(
    deltaPath: String,
    numFiles: Int,
    rowsPerFile: Int = 10
  ): Unit = {
    val ss = spark
    import ss.implicits._

    val data = (0 until numFiles * rowsPerFile).map { i =>
      (i.toLong, s"name_$i", i * 1.5, i % 2 == 0)
    }
    data.toDF("id", "name", "score", "active")
      .repartition(numFiles)
      .write.format("delta").save(deltaPath)
  }

  // ═══════════════════════════════════════════
  //  Full pipeline tests (local filesystem)
  // ═══════════════════════════════════════════

  test("SYNC should create companion index from local Delta table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 20)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1

      val row = rows(0)
      row.getString(0) shouldBe indexPath // table_path
      row.getString(1) shouldBe deltaPath // source_path
      row.getString(2) shouldBe "success" // status
      row.getAs[Long](3) shouldBe 0L // delta_version
      row.getInt(4) should be > 0 // splits_created
      row.getInt(6) should be > 0 // parquet_files_indexed
    }
  }

  test("SYNC with multiple parquet files should index all files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_multi").getAbsolutePath
      val indexPath = new File(tempDir, "companion_multi").getAbsolutePath

      createLocalDeltaTableWithFiles(deltaPath, numFiles = 3, rowsPerFile = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"
      rows(0).getInt(6) shouldBe 3 // parquet_files_indexed
    }
  }

  test("SYNC DRY RUN should not create any splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_dryrun").getAbsolutePath
      val indexPath = new File(tempDir, "companion_dryrun").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath' DRY RUN"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "dry_run"

      // DRY RUN should not create any split files (tx log dir may be created)
      val indexDir = new File(indexPath)
      if (indexDir.exists()) {
        val splitFiles = indexDir.listFiles().filter(_.getName.endsWith(".split"))
        splitFiles shouldBe empty
      }
    }
  }

  test("incremental SYNC should detect no-op when already synced") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_incr").getAbsolutePath
      val indexPath = new File(tempDir, "companion_incr").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      // First sync
      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result1.collect()(0).getString(2) shouldBe "success"

      // Second sync (same delta version) - should be no_action
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result2.collect()(0).getString(2) shouldBe "no_action"
    }
  }

  test("SYNC with FASTFIELDS MODE DISABLED should store mode in companion splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_ff").getAbsolutePath
      val indexPath = new File(tempDir, "companion_ff").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' FASTFIELDS MODE DISABLED AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          file.companionFastFieldMode shouldBe Some("DISABLED")
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("SYNC with FASTFIELDS MODE HYBRID should store mode in companion splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_hybrid").getAbsolutePath
      val indexPath = new File(tempDir, "companion_hybrid").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          file.companionFastFieldMode shouldBe Some("HYBRID")
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("SYNC with FASTFIELDS MODE PARQUET_ONLY should store mode in companion splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_pq").getAbsolutePath
      val indexPath = new File(tempDir, "companion_pq").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"

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

  test("SYNC should record companion metadata in transaction log") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_meta").getAbsolutePath
      val indexPath = new File(tempDir, "companion_meta").getAbsolutePath

      createLocalDeltaTableWithFiles(deltaPath, numFiles = 2, rowsPerFile = 15)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result.collect()(0).getString(2) shouldBe "success"

      // Verify transaction log metadata
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration("indextables.companion.sourceTablePath") shouldBe deltaPath
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "delta"
        metadata.configuration should contain key "indextables.companion.lastSyncedVersion"

        // Verify companion fields on AddActions
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          file.companionDeltaVersion shouldBe defined
          file.companionSourceFiles shouldBe defined
          file.companionSourceFiles.get should not be empty
        }
      } finally {
        txLog.close()
      }
    }
  }

  test("SYNC should report metrics in result row") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_metrics").getAbsolutePath
      val indexPath = new File(tempDir, "companion_metrics").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 20)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val row = result.collect()(0)
      row.getString(2) shouldBe "success"
      row.getAs[Long](7) should be > 0L // parquet_bytes_downloaded
      row.getAs[Long](8) should be > 0L // split_bytes_uploaded
      row.getAs[Long](9) should be > 0L // duration_ms
    }
  }

  test("SYNC should return error for non-existent Delta table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "nonexistent_delta").getAbsolutePath
      val indexPath = new File(tempDir, "nonexistent_index").getAbsolutePath

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error"
    }
  }

  test("companion read-back should return correct data via docBatchProjected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_readback").getAbsolutePath
      val indexPath = new File(tempDir, "companion_readback").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      // Build companion index
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result.collect()(0).getString(2) shouldBe "success"

      // Read back companion index — this exercises the full docBatchProjected path
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      val allRecords = companionDf.collect()
      allRecords.length shouldBe 10

      // Verify data is not null (the old doc() path returned nulls for companion splits)
      allRecords.foreach { row =>
        row.get(0) should not be (null: Any) // id
      }
    }
  }

  test("write guard should reject normal writes to companion-mode table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_guard").getAbsolutePath
      val indexPath = new File(tempDir, "companion_guard").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 10)

      // Create companion index
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Try to write directly — should fail
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
}
