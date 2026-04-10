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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, max, min, sum}
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
 * Tests for TRUNCATE INDEXTABLES TIME TRAVEL on companion tables built from Delta and Iceberg.
 *
 * Verifies that truncation removes old transaction log versions while preserving data integrity and companion metadata,
 * and that DRY RUN mode previews without deleting.
 */
class CompanionTruncateTimeTravelTest
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
      .appName("CompanionTruncateTimeTravelTest")
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
      .config("spark.indextables.state.format", "json")
      .config("spark.indextables.checkpoint.interval", "5")
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

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-truncate-tt").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private val sparkSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private def makeBatch(startId: Int, count: Int): Seq[Row] =
    (startId until startId + count).map(i => Row(i, s"user_$i", i * 1.5))

  /** Count transaction log files. Returns (versionFileCount, checkpointFileCount). */
  private def countTransactionLogFiles(tablePath: String): (Int, Int) = {
    val txLogDir = new java.io.File(tablePath, "_transaction_log")
    if (!txLogDir.exists()) return (0, 0)
    val files = txLogDir.listFiles()
    if (files == null) return (0, 0)
    val versionPattern    = """^\d{20}\.json$""".r
    val checkpointPattern = """^\d{20}\.checkpoint""".r
    val versionCount      = files.count(f => versionPattern.findFirstIn(f.getName).isDefined)
    val checkpointCount   = files.count(f => checkpointPattern.findFirstIn(f.getName).isDefined)
    (versionCount, checkpointCount)
  }

  private def readCompanion(indexPath: String) =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)

  /**
   * Write initial Delta data (5 rows), build companion, then append 1 row and re-sync `extraSyncs` times to create
   * multiple tx log versions.
   *
   * Returns (deltaPath, indexPath, totalExpectedRows).
   */
  private def buildDeltaCompanionWithManySyncs(
    tempDir: String,
    extraSyncs: Int
  ): (String, String, Int) = {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    // Write initial batch of 5 rows
    val initialBatch = makeBatch(1, 5)
    spark
      .createDataFrame(spark.sparkContext.parallelize(initialBatch), sparkSchema)
      .write
      .format("delta")
      .save(deltaPath)

    // Build companion (version 0)
    val buildResult = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    buildResult(0).getString(2) shouldBe "success"

    flushCaches()

    // Repeatedly append 1 row to Delta and re-sync to create more versions
    var totalRows = 5
    for (i <- 1 to extraSyncs) {
      val newRow = makeBatch(5 + i, 1)
      spark
        .createDataFrame(spark.sparkContext.parallelize(newRow), sparkSchema)
        .write
        .format("delta")
        .mode("append")
        .save(deltaPath)

      flushCaches()

      val syncResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      syncResult(0).getString(2) shouldBe "success"

      flushCaches()
      totalRows += 1
    }

    (deltaPath, indexPath, totalRows)
  }

  // ── Iceberg helpers ──────────────────────────────────────────────────────────

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get())
  )

  private def appendIcebergSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    rootDir: File,
    batchId: Int
  ): Unit = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId").getAbsolutePath
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), sparkSchema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

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

  /**
   * Create an Iceberg companion with multiple snapshots/syncs. Calls `f` with (indexPath, icebergTableName, server,
   * rootDir, totalExpectedRows).
   */
  private def withIcebergCompanionManySyncs(
    extraSyncs: Int
  )(
    f: (String, String, EmbeddedIcebergRestServer, File, Int) => Unit
  ): Unit = {
    val root         = Files.createTempDirectory("iceberg-truncate-tt").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "truncate_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Snapshot 1: initial batch of 5 rows
      val initialBatch = makeBatch(1, 5)
      appendIcebergSnapshot(server, tableId, initialBatch, root, 0)

      // Build companion (version 0)
      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.truncate_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"

      flushCaches()

      // Repeatedly append 1 row as new snapshot and re-sync
      var totalRows = 5
      for (i <- 1 to extraSyncs) {
        val newRow = makeBatch(5 + i, 1)
        appendIcebergSnapshot(server, tableId, newRow, root, i)

        flushCaches()

        val syncResult = spark
          .sql(
            s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.truncate_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
          )
          .collect()
        syncResult(0).getString(2) shouldBe "success"

        flushCaches()
        totalRows += 1
      }

      f(indexPath, "default.truncate_test", server, root, totalRows)
    } finally {
      try spark.conf.unset("spark.indextables.iceberg.catalogType")
      catch { case _: Exception => }
      try spark.conf.unset("spark.indextables.iceberg.uri")
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion truncate tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("TRUNCATE on Delta companion with many syncs should delete old versions") {
    withTempPath { tempDir =>
      val (deltaPath, indexPath, totalRows) = buildDeltaCompanionWithManySyncs(tempDir, 12)

      // Verify we have many version files before truncation
      val (versionsBefore, _) = countTransactionLogFiles(indexPath)
      versionsBefore should be >= 12

      // Truncate
      val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'")
      val row    = result.collect().head

      row.getString(1) shouldBe "SUCCESS"
      row.getLong(3) should be >= 0L // versions_deleted

      // Verify version count decreased
      val (versionsAfter, _) = countTransactionLogFiles(indexPath)
      versionsAfter should be < versionsBefore

      // Verify data still readable
      flushCaches()
      val df = readCompanion(indexPath)
      df.count() shouldBe totalRows
    }
  }

  test("TRUNCATE should create checkpoint if none exists on Delta companion") {
    withTempPath { tempDir =>
      // 6 syncs — above checkpoint interval of 5, so truncate will create a checkpoint
      val (_, indexPath, totalRows) = buildDeltaCompanionWithManySyncs(tempDir, 6)

      // Truncate should create a checkpoint first
      val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'")
      val row    = result.collect().head

      row.getString(1) shouldBe "SUCCESS"
      row.getLong(2) should be >= 0L // checkpoint_version created

      // Verify checkpoint was created (check for checkpoint files or _last_checkpoint)
      val txLogDir      = new java.io.File(indexPath, "_transaction_log")
      val allFiles      = Option(txLogDir.listFiles()).getOrElse(Array.empty)
      val hasCheckpoint = allFiles.exists(f => f.getName.contains("checkpoint") || f.getName == "_last_checkpoint")
      hasCheckpoint shouldBe true

      // Verify data still readable
      flushCaches()
      val df = readCompanion(indexPath)
      df.count() shouldBe totalRows
    }
  }

  test("TRUNCATE DRY RUN on Delta companion should preview without deleting") {
    withTempPath { tempDir =>
      val (_, indexPath, _) = buildDeltaCompanionWithManySyncs(tempDir, 10)

      val (versionsBefore, _) = countTransactionLogFiles(indexPath)

      // DRY RUN
      val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath' DRY RUN")
      val row    = result.collect().head

      row.getString(1) shouldBe "DRY_RUN"
      row.getLong(3) should be >= 0L // versions that would be deleted

      // Verify NO files were actually deleted
      val (versionsAfter, _) = countTransactionLogFiles(indexPath)
      versionsAfter shouldBe versionsBefore
    }
  }

  test("Data integrity preserved after truncation on Delta companion") {
    withTempPath { tempDir =>
      val (_, indexPath, totalRows) = buildDeltaCompanionWithManySyncs(tempDir, 8)

      // Compute aggregations before truncation
      flushCaches()
      val dfBefore    = readCompanion(indexPath)
      val countBefore = dfBefore.count()
      val aggBefore = dfBefore
        .agg(sum("score"), min("score"), max("score"))
        .collect()(0)
      val sumBefore = aggBefore.getDouble(0)
      val minBefore = aggBefore.getDouble(1)
      val maxBefore = aggBefore.getDouble(2)

      countBefore shouldBe totalRows

      // Truncate
      spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'").collect()

      // Verify exact same results after truncation
      flushCaches()
      val dfAfter    = readCompanion(indexPath)
      val countAfter = dfAfter.count()
      val aggAfter = dfAfter
        .agg(sum("score"), min("score"), max("score"))
        .collect()(0)

      countAfter shouldBe countBefore
      aggAfter.getDouble(0) shouldBe sumBefore
      aggAfter.getDouble(1) shouldBe minBefore
      aggAfter.getDouble(2) shouldBe maxBefore

      // Verify all IDs present
      val ids = dfAfter.select("id").collect().map(_.getInt(0)).sorted
      ids shouldBe (1 to totalRows).toArray
    }
  }

  test("Re-sync after truncation should succeed on Delta companion") {
    withTempPath { tempDir =>
      val (deltaPath, indexPath, totalRows) = buildDeltaCompanionWithManySyncs(tempDir, 6)

      // Truncate
      spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'").collect()

      // Append new data to Delta
      val newBatch = makeBatch(totalRows + 1, 5)
      spark
        .createDataFrame(spark.sparkContext.parallelize(newBatch), sparkSchema)
        .write
        .format("delta")
        .mode("append")
        .save(deltaPath)

      flushCaches()

      // Re-sync after truncation
      val syncResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      syncResult(0).getString(2) shouldBe "success"

      // Verify new data appears
      flushCaches()
      val df = readCompanion(indexPath)
      df.count() shouldBe (totalRows + 5)

      // Verify new rows are present
      val maxId = df.agg(max("id")).collect()(0).getInt(0)
      maxId shouldBe (totalRows + 5)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion truncate tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("TRUNCATE on Iceberg companion with many syncs should delete old versions and preserve data") {
    withIcebergCompanionManySyncs(12) {
      (
        indexPath,
        _,
        _,
        _,
        totalRows
      ) =>
        // Verify we have many version files before truncation
        val (versionsBefore, _) = countTransactionLogFiles(indexPath)
        versionsBefore should be >= 12

        // Truncate
        val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'")
        val row    = result.collect().head

        row.getString(1) shouldBe "SUCCESS"

        // Verify version count decreased
        val (versionsAfter, _) = countTransactionLogFiles(indexPath)
        versionsAfter should be < versionsBefore

        // Verify data still readable via count (uses tantivy fast fields)
        flushCaches()
        val df = readCompanion(indexPath)
        df.count() shouldBe totalRows
    }
  }

  test("Data integrity preserved after truncation on Iceberg companion") {
    withIcebergCompanionManySyncs(8) {
      (
        indexPath,
        _,
        _,
        _,
        totalRows
      ) =>
        // Compute aggregations before truncation
        flushCaches()
        val dfBefore    = readCompanion(indexPath)
        val countBefore = dfBefore.count()
        val aggBefore = dfBefore
          .agg(sum("score"), min("score"), max("score"))
          .collect()(0)
        val sumBefore = aggBefore.getDouble(0)
        val minBefore = aggBefore.getDouble(1)
        val maxBefore = aggBefore.getDouble(2)

        countBefore shouldBe totalRows

        // Truncate
        spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'").collect()

        // Verify exact same aggregations after truncation
        flushCaches()
        val dfAfter    = readCompanion(indexPath)
        val countAfter = dfAfter.count()
        val aggAfter = dfAfter
          .agg(sum("score"), min("score"), max("score"))
          .collect()(0)

        countAfter shouldBe countBefore
        aggAfter.getDouble(0) shouldBe sumBefore
        aggAfter.getDouble(1) shouldBe minBefore
        aggAfter.getDouble(2) shouldBe maxBefore
    }
  }

  test("Companion metadata preserved after truncation on Iceberg companion") {
    withIcebergCompanionManySyncs(8) {
      (
        indexPath,
        _,
        _,
        _,
        _
      ) =>
        // Verify companion config exists before truncation
        val txLogBefore = TransactionLogFactory.create(new Path(indexPath), spark)
        try {
          val metadataBefore = txLogBefore.getMetadata()
          metadataBefore.configuration.get("indextables.companion.enabled") shouldBe Some("true")
        } finally txLogBefore.close()

        // Truncate
        spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'").collect()

        // Verify companion config still present after truncation
        flushCaches()
        val txLogAfter = TransactionLogFactory.create(new Path(indexPath), spark)
        try {
          val metadataAfter = txLogAfter.getMetadata()
          metadataAfter.configuration.get("indextables.companion.enabled") shouldBe Some("true")
        } finally txLogAfter.close()
    }
  }

  test("Re-sync after truncation on Iceberg companion should succeed") {
    withIcebergCompanionManySyncs(6) {
      (
        indexPath,
        tableName,
        server,
        root,
        totalRows
      ) =>
        // Truncate
        spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$indexPath'").collect()

        // Append new snapshot to Iceberg
        val ns       = Namespace.of("default")
        val tableId  = TableIdentifier.of(ns, "truncate_test")
        val newBatch = makeBatch(totalRows + 1, 5)
        appendIcebergSnapshot(server, tableId, newBatch, root, 100)

        flushCaches()

        // Re-sync after truncation
        val syncResult = spark
          .sql(
            s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$tableName' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
          )
          .collect()
        syncResult(0).getString(2) shouldBe "success"

        // Verify new data appears via count
        flushCaches()
        val df = readCompanion(indexPath)
        df.count() shouldBe (totalRows + 5)
    }
  }
}
