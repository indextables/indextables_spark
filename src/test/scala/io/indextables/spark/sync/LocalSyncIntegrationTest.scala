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

import io.delta.standalone.{DeltaLog => StandaloneDeltaLog}
import io.delta.standalone.actions.{AddFile => StandaloneAddFile, Metadata => StandaloneMetadata}
import io.delta.standalone.types.{DoubleType => StandaloneDoubleType, LongType => StandaloneLongType,
  StringType => StandaloneStringType, StructField => StandaloneStructField,
  StructType => StandaloneStructType, BooleanType => StandaloneBooleanType}
import io.indextables.spark.TestBase
import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

/**
 * Local filesystem integration tests for SYNC INDEXTABLES TO DELTA.
 *
 * Uses tantivy4java's nativeWriteTestParquet to create parquet files that are
 * guaranteed to be compatible with createFromParquet, then creates a Delta
 * transaction log pointing at those files using delta-standalone.
 *
 * No cloud credentials needed — runs entirely on local filesystem.
 */
class LocalSyncIntegrationTest extends TestBase {

  /**
   * Create a Delta table on the local filesystem using delta-standalone.
   * Writes parquet files using nativeWriteTestParquet (compatible with createFromParquet),
   * then creates a Delta log referencing those files.
   *
   * nativeWriteTestParquet produces: id(i64), name(utf8), score(f64), active(bool)
   */
  private def createLocalDeltaTable(
    deltaPath: String,
    numFiles: Int = 1,
    rowsPerFile: Int = 20
  ): Unit = {
    val deltaDir = new File(deltaPath)
    deltaDir.mkdirs()

    // Schema matching nativeWriteTestParquet output
    val schema = new StandaloneStructType(Array(
      new StandaloneStructField("id", new StandaloneLongType, false),
      new StandaloneStructField("name", new StandaloneStringType, true),
      new StandaloneStructField("score", new StandaloneDoubleType, true),
      new StandaloneStructField("active", new StandaloneBooleanType, true)
    ))

    // Write parquet files using tantivy4java's native writer
    val addFiles = (0 until numFiles).map { i =>
      val parquetFileName = s"part-0000$i.parquet"
      val parquetPath = new File(deltaDir, parquetFileName).getAbsolutePath

      QuickwitSplit.nativeWriteTestParquet(parquetPath, rowsPerFile, i * rowsPerFile)

      val fileSize = new File(parquetPath).length()
      new StandaloneAddFile(
        parquetFileName,
        java.util.Collections.emptyMap(),
        fileSize,
        System.currentTimeMillis(),
        true, // dataChange
        null, // stats
        null  // tags
      )
    }

    // Create Delta log using delta-standalone
    val hadoopConf = new Configuration()
    val deltaLog = StandaloneDeltaLog.forTable(hadoopConf, deltaPath)

    val metadata = StandaloneMetadata.builder()
      .schema(schema)
      .format(new io.delta.standalone.actions.Format("parquet", java.util.Collections.emptyMap()))
      .build()

    val txn = deltaLog.startTransaction()
    val actions = new java.util.ArrayList[io.delta.standalone.actions.Action]()
    actions.add(metadata)
    addFiles.foreach(actions.add)
    txn.commit(actions, new io.delta.standalone.Operation(
      io.delta.standalone.Operation.Name.CREATE_TABLE), "test")
  }

  // ═══════════════════════════════════════════
  //  Full pipeline tests (local filesystem)
  // ═══════════════════════════════════════════

  test("SYNC should create companion index from local Delta table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 20)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
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

      createLocalDeltaTable(deltaPath, numFiles = 3, rowsPerFile = 10)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
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

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath' DRY RUN"
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

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      // First sync
      val result1 = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result1.collect()(0).getString(2) shouldBe "success"

      // Second sync (same delta version) - should be no_action
      val result2 = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      result2.collect()(0).getString(2) shouldBe "no_action"
    }
  }

  test("SYNC with FASTFIELDS MODE DISABLED should store mode in companion splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_ff").getAbsolutePath
      val indexPath = new File(tempDir, "companion_ff").getAbsolutePath

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' FASTFIELDS MODE DISABLED AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"

      // Verify the companion splits have the correct fast field mode
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

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
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

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '$indexPath'"
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

      createLocalDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 15)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
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

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 20)

      val result = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
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
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error"
    }
  }

  test("write guard should reject normal writes to companion-mode table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_guard").getAbsolutePath
      val indexPath = new File(tempDir, "companion_guard").getAbsolutePath

      createLocalDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      // Create companion index
      val syncResult = spark.sql(
        s"SYNC INDEXTABLES TO DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Try to write directly — should fail
      val _spark = spark
      import _spark.implicits._
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
