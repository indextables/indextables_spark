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

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import scala.jdk.CollectionConverters._

/**
 * End-to-end integration tests for distributed log read in BUILD INDEXTABLES COMPANION.
 * Tests the full pipeline with distributed enabled vs disabled, verifying identical results.
 */
class DistributedDeltaSyncIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("DistributedDeltaSyncIntegrationTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-access-key")
      .config("spark.indextables.aws.secretKey", "test-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-session-token")
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
    val path = Files.createTempDirectory("dist-sync-test").toString
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

  // ─── End-to-End: Distributed Enabled (default) ───

  test("BUILD COMPANION with distributed enabled should succeed") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )

      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"
      rows(0).getInt(4) should be > 0 // splits_created
      rows(0).getInt(6) shouldBe 2    // parquet_files_indexed
    }
  }

  test("BUILD COMPANION with distributed enabled should record companion metadata") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_meta").getAbsolutePath
      val indexPath = new File(tempDir, "companion_meta").getAbsolutePath

      createDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()

      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration should contain key "indextables.companion.enabled"
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration should contain key "indextables.companion.sourceFormat"
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "delta"

        val files = txLog.listFiles()
        files should not be empty
        files.head.companionSourceFiles shouldBe defined
        files.head.companionDeltaVersion shouldBe defined
      } finally
        txLog.close()
    }
  }

  test("incremental sync with distributed should detect new files") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_incr").getAbsolutePath
      val indexPath = new File(tempDir, "companion_incr").getAbsolutePath

      // Initial write: 2 files
      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 10)
      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result1(0).getString(2) shouldBe "success"
      val initialFiles = result1(0).getInt(6) // parquet_files_indexed

      // Append more data
      val ss = spark
      import ss.implicits._
      val newData = (100 until 110).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
      newData
        .toDF("id", "name", "score", "active")
        .repartition(1)
        .write
        .format("delta")
        .mode("append")
        .save(deltaPath)

      // Incremental sync: should detect the new file
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()

      result2(0).getString(2) shouldBe "success"
      result2(0).getInt(6) shouldBe 1 // only the new file
    }
  }

  test("re-sync with no changes should return no_action") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_noop").getAbsolutePath
      val indexPath = new File(tempDir, "companion_noop").getAbsolutePath

      createDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 10)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()

      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "no_action"
    }
  }

  // ─── Distributed Disabled (fallback) ───

  test("BUILD COMPANION with distributed disabled should succeed") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_fallback").getAbsolutePath
      val indexPath = new File(tempDir, "companion_fallback").getAbsolutePath

      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 10)

      // Disable distributed log read
      spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
      try {
        val result = spark.sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
        ).collect()

        result(0).getString(2) shouldBe "success"
        result(0).getInt(6) shouldBe 2
      } finally {
        spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
      }
    }
  }

  // ─── Both Paths: Identical Results ───

  test("distributed and non-distributed paths should produce identical companion indexes") {
    withTempPath { tempDir =>
      val deltaPath    = new File(tempDir, "delta_compare").getAbsolutePath
      val distPath     = new File(tempDir, "companion_dist").getAbsolutePath
      val nonDistPath  = new File(tempDir, "companion_nondist").getAbsolutePath

      createDeltaTable(deltaPath, numFiles = 3, rowsPerFile = 10)

      // Build with distributed enabled (default)
      val distResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$distPath'"
      ).collect()

      // Build with distributed disabled
      spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
      try {
        val nonDistResult = spark.sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$nonDistPath'"
        ).collect()

        // Both should succeed with same file count
        distResult(0).getString(2) shouldBe "success"
        nonDistResult(0).getString(2) shouldBe "success"
        distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6) // same parquet_files_indexed
      } finally {
        spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
      }
    }
  }

  // ─── Parquet (distributed) ───

  test("BUILD COMPANION for Parquet with distributed should succeed") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_data").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_parquet").getAbsolutePath

      createParquetData(parquetPath, numFiles = 2)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      result(0).getInt(6) shouldBe 2
    }
  }

  // ─── WHERE-Scoped Invalidation ───

  test("WHERE-scoped invalidation should not invalidate splits outside WHERE range") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_scoped").getAbsolutePath
      val indexPath = new File(tempDir, "companion_scoped").getAbsolutePath

      // Create partitioned Delta table with 3 dates
      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      // Initial sync (all partitions)
      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Verify all partitions are indexed
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      val initialSplitCount = txLog.listFiles().size
      initialSplitCount should be >= 3
      txLog.close()

      // Delete source files for date=2024-01-01 (outside WHERE range)
      deleteRecursively(new File(deltaPath))
      createPartitionedDeltaTable(deltaPath, Seq("2024-02-01", "2024-03-01"))

      // Incremental sync with WHERE date >= '2024-02-01' (default: scoped invalidation)
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date >= '2024-02-01' AT LOCATION '$indexPath'"
      ).collect()

      // Verify: splits for date=2024-01-01 should NOT be invalidated
      val txLog2 = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val remainingFiles = txLog2.listFiles()
        // Splits for 2024-01-01 should still be present (not invalidated)
        val jan01Splits = remainingFiles.filter(_.partitionValues.get("date").contains("2024-01-01"))
        jan01Splits should not be empty
      } finally
        txLog2.close()
    }
  }

  test("INVALIDATE ALL PARTITIONS should invalidate all gone splits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_invalidate_all").getAbsolutePath
      val indexPath = new File(tempDir, "companion_invalidate_all").getAbsolutePath

      // Create partitioned Delta table with 3 dates
      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      // Initial sync (all partitions)
      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Delete source files for date=2024-01-01 and recreate without that partition
      deleteRecursively(new File(deltaPath))
      createPartitionedDeltaTable(deltaPath, Seq("2024-02-01", "2024-03-01"))

      // Incremental sync with WHERE + INVALIDATE ALL PARTITIONS
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date >= '2024-02-01' INVALIDATE ALL PARTITIONS AT LOCATION '$indexPath'"
      ).collect()

      // Verify: splits for date=2024-01-01 SHOULD be invalidated
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val remainingFiles = txLog.listFiles()
        val jan01Splits = remainingFiles.filter(_.partitionValues.get("date").contains("2024-01-01"))
        jan01Splits shouldBe empty
      } finally
        txLog.close()
    }
  }

  test("WHERE-scoped invalidation detects gone files within WHERE range") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_within_scope").getAbsolutePath
      val indexPath = new File(tempDir, "companion_within_scope").getAbsolutePath

      // Create partitioned Delta table with 3 dates
      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      // Initial sync (all partitions)
      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Delete source files for date=2024-02-01 (within WHERE range) and recreate without it
      deleteRecursively(new File(deltaPath))
      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-03-01"))

      // Incremental sync with WHERE date >= '2024-02-01' (scoped invalidation)
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date >= '2024-02-01' AT LOCATION '$indexPath'"
      ).collect()

      // Verify: splits for date=2024-02-01 SHOULD be invalidated (within scope)
      // Verify: splits for date=2024-01-01 should NOT be invalidated (outside scope)
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val remainingFiles = txLog.listFiles()
        val feb01Splits = remainingFiles.filter(_.partitionValues.get("date").contains("2024-02-01"))
        feb01Splits shouldBe empty // invalidated (within WHERE scope)

        val jan01Splits = remainingFiles.filter(_.partitionValues.get("date").contains("2024-01-01"))
        jan01Splits should not be empty // untouched (outside WHERE scope)
      } finally
        txLog.close()
    }
  }

  // ─── Native PartitionFilter Pushdown with Varied Predicates ───

  test("WHERE with equality filter should only index matching partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_eq_filter").getAbsolutePath
      val indexPath = new File(tempDir, "companion_eq_filter").getAbsolutePath

      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date = '2024-02-01' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      // Only files from the date=2024-02-01 partition should be indexed
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { f =>
          f.partitionValues.get("date") shouldBe Some("2024-02-01")
        }
      } finally txLog.close()
    }
  }

  test("WHERE with IN filter should index matching partitions only") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_in_filter").getAbsolutePath
      val indexPath = new File(tempDir, "companion_in_filter").getAbsolutePath

      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date IN ('2024-01-01', '2024-03-01') AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files    = txLog.listFiles()
        val dateVals = files.flatMap(_.partitionValues.get("date")).toSet
        dateVals should contain("2024-01-01")
        dateVals should contain("2024-03-01")
        dateVals should not contain "2024-02-01"
      } finally txLog.close()
    }
  }

  test("WHERE with range filter on integer partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_range_int").getAbsolutePath
      val indexPath = new File(tempDir, "companion_range_int").getAbsolutePath

      createMultiPartDeltaTable(deltaPath)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE year >= 2024 AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files     = txLog.listFiles()
        val yearVals  = files.flatMap(_.partitionValues.get("year")).map(_.toInt).toSet
        yearVals.foreach(_ should be >= 2024)
        yearVals should not contain 2023  // assert exclusion
      } finally txLog.close()
    }
  }

  test("WHERE with compound AND filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_and_filter").getAbsolutePath
      val indexPath = new File(tempDir, "companion_and_filter").getAbsolutePath

      createMultiPartDeltaTable(deltaPath)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE year = 2024 AND region = 'east' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { f =>
          f.partitionValues.get("year") shouldBe Some("2024")
          f.partitionValues.get("region") shouldBe Some("east")
        }
      } finally txLog.close()
    }
  }

  test("WHERE with OR filter should index files from either partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_or_filter").getAbsolutePath
      val indexPath = new File(tempDir, "companion_or_filter").getAbsolutePath

      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date = '2024-01-01' OR date = '2024-03-01' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val dateVals = txLog.listFiles().flatMap(_.partitionValues.get("date")).toSet
        dateVals should contain("2024-01-01")
        dateVals should contain("2024-03-01")
        dateVals should not contain "2024-02-01"
      } finally txLog.close()
    }
  }

  test("WHERE with strict greater-than should exclude boundary value") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_strict_gt").getAbsolutePath
      val indexPath = new File(tempDir, "companion_strict_gt").getAbsolutePath

      createMultiPartDeltaTable(deltaPath)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE year > 2023 AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files    = txLog.listFiles()
        val yearVals = files.flatMap(_.partitionValues.get("year")).map(_.toInt).toSet
        yearVals.foreach(_ should be > 2023)
        yearVals should not contain 2023  // strictly excluded
        yearVals should contain(2024)
        yearVals should contain(2025)
      } finally txLog.close()
    }
  }

  test("WHERE with BETWEEN-equivalent range filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_between").getAbsolutePath
      val indexPath = new File(tempDir, "companion_between").getAbsolutePath

      createMultiPartDeltaTable(deltaPath)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE year >= 2024 AND year <= 2024 AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files    = txLog.listFiles()
        val yearVals = files.flatMap(_.partitionValues.get("year")).map(_.toInt).toSet
        yearVals shouldBe Set(2024) // only year=2024
      } finally txLog.close()
    }
  }

  test("WHERE with date-format partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_date_fmt").getAbsolutePath
      val indexPath = new File(tempDir, "companion_date_fmt").getAbsolutePath

      createDatePartitionedDeltaTable(deltaPath)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE dt >= '2025-01-01' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val files  = txLog.listFiles()
        val dtVals = files.flatMap(_.partitionValues.get("dt")).toSet
        dtVals.foreach(_ should be >= "2025-01-01")
        dtVals should not contain "2024-06-15"
        dtVals should not contain "2024-12-31"
      } finally txLog.close()
    }
  }

  test("WHERE with != filter should exclude matching partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_neq_filter").getAbsolutePath
      val indexPath = new File(tempDir, "companion_neq_filter").getAbsolutePath

      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date != '2024-02-01' AT LOCATION '$indexPath'"
      ).collect()

      result(0).getString(2) shouldBe "success"
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
      try {
        val dateVals = txLog.listFiles().flatMap(_.partitionValues.get("date")).toSet
        dateVals should contain("2024-01-01")
        dateVals should contain("2024-03-01")
        dateVals should not contain "2024-02-01"
      } finally txLog.close()
    }
  }

  // ─── Arrow FFI vs TANT Path Comparison ───

  test("Arrow FFI disabled should produce same results as Arrow FFI enabled") {
    withTempPath { tempDir =>
      val deltaPath    = new File(tempDir, "delta_ffi_compare").getAbsolutePath
      val ffiPath      = new File(tempDir, "companion_ffi").getAbsolutePath
      val tantPath     = new File(tempDir, "companion_tant").getAbsolutePath

      createPartitionedDeltaTable(deltaPath, Seq("2024-01-01", "2024-02-01", "2024-03-01"))

      // Build with Arrow FFI enabled (default)
      val ffiResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date >= '2024-02-01' AT LOCATION '$ffiPath'"
      ).collect()

      // Build with Arrow FFI disabled
      spark.conf.set("spark.indextables.companion.sync.arrowFfi.enabled", "false")
      try {
        val tantResult = spark.sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WHERE date >= '2024-02-01' AT LOCATION '$tantPath'"
        ).collect()

        // Both should succeed with same file count
        ffiResult(0).getString(2) shouldBe "success"
        tantResult(0).getString(2) shouldBe "success"
        ffiResult(0).getInt(6) shouldBe tantResult(0).getInt(6) // same parquet_files_indexed
      } finally {
        spark.conf.unset("spark.indextables.companion.sync.arrowFfi.enabled")
      }
    }
  }

  // ─── Helpers ───

  private def createPartitionedDeltaTable(path: String, dates: Seq[String]): Unit = {
    val ss = spark
    import ss.implicits._
    val data = dates.flatMap { date =>
      (0 until 5).map(i => (i.toLong, s"name_$i", date))
    }
    data
      .toDF("id", "name", "date")
      .repartition(dates.size)
      .write
      .format("delta")
      .partitionBy("date")
      .save(path)
  }

  private def createMultiPartDeltaTable(path: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "alice", 2023, "east"),
      (2L, "bob", 2023, "west"),
      (3L, "carol", 2024, "east"),
      (4L, "dave", 2024, "west"),
      (5L, "eve", 2025, "east")
    )
    data
      .toDF("id", "name", "year", "region")
      .write
      .format("delta")
      .partitionBy("year", "region")
      .save(path)
  }

  private def createDatePartitionedDeltaTable(path: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "alice", "2024-06-15"),
      (2L, "bob", "2024-12-31"),
      (3L, "carol", "2025-01-01"),
      (4L, "dave", "2025-06-15"),
      (5L, "eve", "2025-12-31")
    )
    data
      .toDF("id", "name", "dt")
      .write
      .format("delta")
      .partitionBy("dt")
      .save(path)
  }

  private def createDeltaTable(path: String, numFiles: Int, rowsPerFile: Int): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numFiles * rowsPerFile).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
    data
      .toDF("id", "name", "score", "active")
      .repartition(numFiles)
      .write
      .format("delta")
      .save(path)
  }

  private def createParquetData(path: String, numFiles: Int): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numFiles * 10).map(i => (i.toLong, s"name_$i", i * 1.5))
    data
      .toDF("id", "name", "score")
      .repartition(numFiles)
      .write
      .parquet(path)
  }
}
