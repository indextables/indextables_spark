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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, max, min, sum}
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for MERGE SPLITS on companion tables.
 *
 * Verifies that companion metadata fields (companionSourceFiles, companionDeltaVersion, companionFastFieldMode) are
 * correctly preserved and merged during split consolidation, and that all data remains accessible after merge.
 *
 * Critical code path: MergeSplitsCommand.scala lines 1092-1133.
 */
class CompanionMergeSplitsTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionMergeSplitsTest")
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
    if (spark != null) spark.stop()

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-merge-splits").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("department", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val batch1Rows = Seq(
    Row(1L, "alice", "engineering", 100.0),
    Row(2L, "bob", "marketing", 150.0),
    Row(3L, "carol", "engineering", 80.0),
    Row(4L, "dave", "sales", 50.0)
  )

  private val batch2Rows = Seq(
    Row(5L, "eve", "engineering", 200.0),
    Row(6L, "frank", "sales", 120.0),
    Row(7L, "grace", "marketing", 90.0),
    Row(8L, "heidi", "engineering", 160.0)
  )

  // Expected totals for all 8 rows
  private val AllRows    = 8L
  private val TotalScore = 950.0 // 100+150+80+50+200+120+90+160
  private val MinScore   = 50.0
  private val MaxScore   = 200.0

  /**
   * Creates a Delta table with two writes, builds companion twice to create 2+ splits, then merges them. Returns the
   * index path so the caller can inspect the result.
   */
  private def buildAndMergeDeltaCompanion(tempDir: String): String = {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    val ss = spark
    import ss.implicits._

    // Write batch 1 to Delta
    spark
      .createDataFrame(spark.sparkContext.parallelize(batch1Rows), sparkSchema)
      .write
      .format("delta")
      .save(deltaPath)

    // Build companion (creates split 1)
    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    // Append batch 2 to Delta
    spark
      .createDataFrame(spark.sparkContext.parallelize(batch2Rows), sparkSchema)
      .write
      .format("delta")
      .mode("append")
      .save(deltaPath)

    flushCaches()

    // Re-sync (creates split 2 for new files)
    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    flushCaches()

    // Verify we have multiple splits before merge
    val txLogBefore = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val splitCount = txLogBefore.listFiles().size
      splitCount should be >= 2
    } finally
      txLogBefore.close()

    // Merge
    spark.sql(s"MERGE SPLITS '$indexPath' TARGET SIZE 100M").collect()

    flushCaches()

    indexPath
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion merge tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("MERGE SPLITS preserves companionSourceFiles") {
    withTempPath { tempDir =>
      val indexPath = buildAndMergeDeltaCompanion(tempDir)

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe 1

        val merged = files.head
        merged.companionSourceFiles shouldBe defined

        // The merged split should contain source files from BOTH original splits
        val sourceFiles = merged.companionSourceFiles.get
        sourceFiles.size should be >= 2
      } finally
        txLog.close()
    }
  }

  test("MERGE SPLITS takes max companionDeltaVersion") {
    withTempPath { tempDir =>
      val indexPath = buildAndMergeDeltaCompanion(tempDir)

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe 1

        val merged = files.head
        merged.companionDeltaVersion shouldBe defined

        merged.companionDeltaVersion.get shouldBe 1L
      } finally
        txLog.close()
    }
  }

  test("MERGE SPLITS preserves companionFastFieldMode") {
    withTempPath { tempDir =>
      val indexPath = buildAndMergeDeltaCompanion(tempDir)

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe 1

        val merged = files.head
        merged.companionFastFieldMode shouldBe Some("HYBRID")
      } finally
        txLog.close()
    }
  }

  test("all data accessible after merge on Delta companion") {
    withTempPath { tempDir =>
      val indexPath = buildAndMergeDeltaCompanion(tempDir)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      df.count() shouldBe AllRows

      // Verify all rows present by checking IDs
      val ids = df.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)
    }
  }

  test("aggregations correct after merge on Delta companion") {
    withTempPath { tempDir =>
      val indexPath = buildAndMergeDeltaCompanion(tempDir)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val row = df
        .agg(
          count("*"),
          sum("score"),
          min("score"),
          max("score")
        )
        .collect()(0)

      row.getLong(0) shouldBe AllRows
      row.getDouble(1) shouldBe TotalScore
      row.getDouble(2) shouldBe MinScore
      row.getDouble(3) shouldBe MaxScore
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion merge tests
  // ═══════════════════════════════════════════════════════════════════════════

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "department", Types.StringType.get()),
    Types.NestedField.required(4, "score", Types.DoubleType.get())
  )

  private def appendIcebergSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    rootDir: File,
    batchId: Int
  ): Unit = {
    // Write parquet to a staging directory (Spark needs its own output directory)
    val stagingDir = new File(rootDir, s"staging-$batchId")
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), sparkSchema)
    df.coalesce(1).write.parquet(s"file://${stagingDir.getAbsolutePath}")

    // Move parquet files to a common data directory so all files share
    // the same parent — extractTableBasePath() computes the storage root
    // from the first file's path, and companion reads resolve all source
    // files relative to that root.
    val dataDir = new File(rootDir, "data")
    dataDir.mkdirs()

    val parquetFiles = stagingDir
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)
      .map { src =>
        val dest = new File(dataDir, src.getName)
        java.nio.file.Files.move(src.toPath, dest.toPath)
        dest
      }

    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    parquetFiles.foreach { pf =>
      appendOp.appendFile(
        DataFiles
          .builder(table.spec())
          .withPath(s"file://${pf.getAbsolutePath}")
          .withFileSizeInBytes(pf.length())
          .withRecordCount(rows.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .build()
      )
    }
    appendOp.commit()
  }

  private def withIcebergMerge(f: (String, DataFrame) => Unit): Unit = {
    val root         = Files.createTempDirectory("iceberg-merge-splits").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "merge_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Snapshot 1: batch 1
      appendIcebergSnapshot(server, tableId, batch1Rows, root, 1)

      // Use tiny TARGET INPUT SIZE to force one split per file
      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.merge_test' FASTFIELDS MODE HYBRID TARGET INPUT SIZE 1024 AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      // Snapshot 2: batch 2
      appendIcebergSnapshot(server, tableId, batch2Rows, root, 2)

      flushCaches()

      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.merge_test' FASTFIELDS MODE HYBRID TARGET INPUT SIZE 1024 AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      flushCaches()

      // Verify multiple splits (small target size should force at least 2)
      val txLogBefore = TransactionLogFactory.create(new Path(indexPath), spark)
      val splitCountBefore =
        try txLogBefore.listFiles().size
        finally txLogBefore.close()
      splitCountBefore should be >= 2

      // Merge
      spark.sql(s"MERGE SPLITS '$indexPath' TARGET SIZE 100M").collect()
      flushCaches()

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      f(indexPath, df)
    } finally {
      try spark.conf.unset("spark.indextables.iceberg.catalogType")
      catch { case _: Exception => }
      try spark.conf.unset("spark.indextables.iceberg.uri")
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  test("MERGE on Iceberg companion preserves companion fields") {
    withIcebergMerge { (indexPath, _) =>
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files.size shouldBe 1

        val merged = files.head
        merged.companionSourceFiles shouldBe defined
        merged.companionSourceFiles.get.size should be >= 2
        merged.companionFastFieldMode shouldBe Some("HYBRID")
      } finally
        txLog.close()
    }
  }

  test("all data accessible after merge on Iceberg companion") {
    withIcebergMerge { (_, df) =>
      val rows = df.collect()
      rows.length shouldBe AllRows.toInt
      val ids = rows.map(_.getLong(rows(0).fieldIndex("id"))).sorted
      ids shouldBe Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)
    }
  }

  test("aggregations correct after merge on Iceberg companion") {
    withIcebergMerge { (_, df) =>
      val row = df
        .agg(count("*"), sum("score"), min("score"), max("score"))
        .collect()(0)

      row.getLong(0) shouldBe AllRows
      row.getDouble(1) shouldBe TotalScore
      row.getDouble(2) shouldBe MinScore
      row.getDouble(3) shouldBe MaxScore
    }
  }

  test("filters correct after merge on Iceberg companion") {
    withIcebergMerge { (_, df) =>
      // Filter pushdown with count uses tantivy index, not parquet reads
      df.filter(col("name") === "alice").count() shouldBe 1
      df.filter(col("score") > 100.0).count() shouldBe 4 // 150,200,120,160
      df.filter(col("department").isin("engineering", "sales")).count() shouldBe 6
    }
  }
}
