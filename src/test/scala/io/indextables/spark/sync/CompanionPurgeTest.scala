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
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for PURGE INDEXTABLE on companion tables built from Delta and Iceberg sources.
 *
 * Validates that orphaned split files are correctly identified and deleted, that valid splits referenced in the
 * transaction log are preserved, and that retention periods are respected.
 */
class CompanionPurgeTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach
    with io.indextables.spark.testutils.FileCleanupHelper {

  var spark: SparkSession = _
  var tempDir: String     = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("CompanionPurgeTest")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.indextables.purge.retentionCheckEnabled", "false")
      .getOrCreate()

    tempDir = Files.createTempDirectory("companion_purge_test").toString

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null) deleteRecursively(new File(tempDir))
  }

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  // ── Shared schema & data ────────────────────────────────────────────────────

  private val sparkSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val testRows: Seq[Row] = Seq(
    Row(1, "alice", 85.0),
    Row(2, "bob", 92.5),
    Row(3, "charlie", 78.0),
    Row(4, "dave", 95.1),
    Row(5, "eve", 88.3)
  )

  // ── Delta helpers ───────────────────────────────────────────────────────────

  private def createDeltaSource(deltaPath: String): Unit = {
    spark
      .createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
      .write
      .format("delta")
      .save(deltaPath)
  }

  private def buildDeltaCompanion(deltaPath: String, indexPath: String): Unit = {
    val result = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    result(0).getString(2) shouldBe "success"
    flushCaches()
  }

  private def readCompanion(indexPath: String): org.apache.spark.sql.DataFrame = {
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
  }

  private def createOrphanFile(indexPath: String, name: String, ageHoursAgo: Long): File = {
    val orphanFile = new File(indexPath, name)
    orphanFile.createNewFile()
    orphanFile.setLastModified(System.currentTimeMillis() - (ageHoursAgo * 60 * 60 * 1000))
    orphanFile
  }

  private def setAllSplitTimestamps(indexPath: String, ageHoursAgo: Long): Unit = {
    new File(indexPath).listFiles().filter(_.getName.endsWith(".split")).foreach { f =>
      f.setLastModified(System.currentTimeMillis() - (ageHoursAgo * 60 * 60 * 1000))
    }
  }

  // ── Iceberg helpers ─────────────────────────────────────────────────────────

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
   * Runs body with an Iceberg companion index built from the test data. Cleans up Iceberg catalog config in finally
   * block.
   *
   * @param f
   *   receives (root directory, index path, server, table identifier)
   */
  private def withIcebergCompanion(f: (File, String, EmbeddedIcebergRestServer, TableIdentifier) => Unit): Unit = {
    val root         = Files.createTempDirectory("iceberg-companion-purge").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "purge_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Append test data
      appendIcebergSnapshot(server, tableId, testRows, root, 1)

      // Build companion
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.purge_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"
      flushCaches()

      f(root, indexPath, server, tableId)
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion purge tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("PURGE on Delta companion should delete orphaned split files") {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    createDeltaSource(deltaPath)
    buildDeltaCompanion(deltaPath, indexPath)

    // Count valid splits before adding orphans
    val validSplitsBefore = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
    validSplitsBefore should be > 0

    // Create orphaned .split files aged 25 hours
    val orphan1 = createOrphanFile(indexPath, "orphan-12345678-1234-1234-1234-123456789abc.split", 25)
    val orphan2 = createOrphanFile(indexPath, "orphan-87654321-4321-4321-4321-cba987654321.split", 25)

    orphan1.exists() shouldBe true
    orphan2.exists() shouldBe true

    // Purge with 0-hour retention
    val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
    val metrics = result(0).getStruct(1)

    metrics.getString(0) shouldBe "SUCCESS"
    metrics.getLong(1) should be >= 2L
    metrics.getLong(2) should be >= 2L

    // Verify orphans deleted
    orphan1.exists() shouldBe false
    orphan2.exists() shouldBe false

    // Verify valid splits preserved
    val validSplitsAfter = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
    validSplitsAfter shouldBe validSplitsBefore

    // Verify data still readable
    val df = readCompanion(indexPath)
    df.count() shouldBe 5
  }

  test("PURGE should not delete valid Delta companion splits") {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    createDeltaSource(deltaPath)
    buildDeltaCompanion(deltaPath, indexPath)

    // Set old timestamps on ALL files (valid + will-be-orphaned)
    setAllSplitTimestamps(indexPath, 25)

    // Create orphaned .split files also aged 25 hours
    val orphan = createOrphanFile(indexPath, "orphan-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.split", 25)

    val validSplitsBefore = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
    val metrics = result(0).getStruct(1)

    metrics.getString(0) shouldBe "SUCCESS"

    // Only the orphan should be deleted, not the valid splits
    orphan.exists() shouldBe false

    val validSplitsAfter = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
    validSplitsAfter shouldBe (validSplitsBefore - 1)

    // Verify table is still readable
    val df = readCompanion(indexPath)
    df.count() shouldBe 5
  }

  test("PURGE DRY RUN on Delta companion") {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    createDeltaSource(deltaPath)
    buildDeltaCompanion(deltaPath, indexPath)

    // Create orphaned .split files aged 25 hours
    val orphan1 = createOrphanFile(indexPath, "orphan-11111111-2222-3333-4444-555555555555.split", 25)
    val orphan2 = createOrphanFile(indexPath, "orphan-66666666-7777-8888-9999-aaaaaaaaaaaa.split", 25)

    val totalFilesBefore = new File(indexPath).listFiles().length

    // DRY RUN
    val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    metrics.getString(0) shouldBe "DRY_RUN"
    metrics.getLong(1) should be >= 2L
    metrics.getLong(2) shouldBe 0L
    metrics.getBoolean(8) shouldBe true

    // Files should still exist
    orphan1.exists() shouldBe true
    orphan2.exists() shouldBe true

    val totalFilesAfter = new File(indexPath).listFiles().length
    totalFilesAfter shouldBe totalFilesBefore
  }

  test("PURGE with retention respects retention period") {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    createDeltaSource(deltaPath)
    buildDeltaCompanion(deltaPath, indexPath)

    // Create orphans at different ages
    val recentOrphan = createOrphanFile(indexPath, "orphan-recent-1234-1234-1234-123456789abc.split", 2) // 2 hours ago
    val oldOrphan    = createOrphanFile(indexPath, "orphan-old-5678-5678-5678-567856785678.split", 25)   // 25 hours ago

    // Purge with 24-hour retention
    val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 24 HOURS").collect()
    val metrics = result(0).getStruct(1)

    metrics.getString(0) shouldBe "SUCCESS"

    // Only the 25h-old orphan should be deleted
    oldOrphan.exists() shouldBe false
    recentOrphan.exists() shouldBe true
  }

  test("PURGE on partitioned Delta companion") {
    val deltaPath = new File(tempDir, "delta").getAbsolutePath
    val indexPath = new File(tempDir, "index").getAbsolutePath

    // Create partitioned Delta source
    val partitionedSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("score", DoubleType, nullable = false),
        StructField("region", StringType, nullable = false)
      )
    )

    val partitionedRows = Seq(
      Row(1, "alice", 85.0, "us-east"),
      Row(2, "bob", 92.5, "us-west"),
      Row(3, "charlie", 78.0, "eu-west"),
      Row(4, "dave", 95.1, "us-east"),
      Row(5, "eve", 88.3, "eu-west")
    )

    spark
      .createDataFrame(spark.sparkContext.parallelize(partitionedRows), partitionedSchema)
      .write
      .format("delta")
      .partitionBy("region")
      .save(deltaPath)

    // Build companion
    val buildResult = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    buildResult(0).getString(2) shouldBe "success"
    flushCaches()

    val validSplitsBefore = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))

    // Create orphaned files in the index directory
    val orphan1 = createOrphanFile(indexPath, "orphan-part-aaaa-bbbb-cccc-dddddddddddd.split", 25)
    val orphan2 = createOrphanFile(indexPath, "orphan-part-eeee-ffff-0000-111111111111.split", 25)

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
    val metrics = result(0).getStruct(1)

    metrics.getString(0) shouldBe "SUCCESS"
    metrics.getLong(2) should be >= 2L

    // Verify orphans deleted
    orphan1.exists() shouldBe false
    orphan2.exists() shouldBe false

    // Verify valid splits preserved and data readable
    val validSplitsAfter = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
    validSplitsAfter shouldBe validSplitsBefore

    val df = readCompanion(indexPath)
    df.count() shouldBe 5
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion purge tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("PURGE on Iceberg companion should delete orphaned splits") {
    withIcebergCompanion { (_, indexPath, _, _) =>
      val validSplitsBefore = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
      validSplitsBefore should be > 0

      // Create orphaned .split files aged 25 hours
      val orphan1 = createOrphanFile(indexPath, "orphan-ice-1111-2222-3333-444444444444.split", 25)
      val orphan2 = createOrphanFile(indexPath, "orphan-ice-5555-6666-7777-888888888888.split", 25)

      // Purge
      val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
      val metrics = result(0).getStruct(1)

      metrics.getString(0) shouldBe "SUCCESS"
      metrics.getLong(1) should be >= 2L
      metrics.getLong(2) should be >= 2L

      // Verify orphans deleted
      orphan1.exists() shouldBe false
      orphan2.exists() shouldBe false

      // Verify valid splits preserved
      val validSplitsAfter = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
      validSplitsAfter shouldBe validSplitsBefore

      // Verify data still readable
      val df = readCompanion(indexPath)
      df.count() shouldBe 5
    }
  }

  test("PURGE should not delete valid Iceberg companion splits") {
    withIcebergCompanion { (_, indexPath, _, _) =>
      // Set old timestamps on ALL files (valid + will-be-orphaned)
      setAllSplitTimestamps(indexPath, 25)

      // Create orphaned .split file also aged 25 hours
      val orphan = createOrphanFile(indexPath, "orphan-ice-aaaa-bbbb-cccc-dddddddddddd.split", 25)

      val validSplitsBefore = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))

      // Purge
      val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
      val metrics = result(0).getStruct(1)

      metrics.getString(0) shouldBe "SUCCESS"

      // Only the orphan should be deleted
      orphan.exists() shouldBe false

      val validSplitsAfter = new File(indexPath).listFiles().count(_.getName.endsWith(".split"))
      validSplitsAfter shouldBe (validSplitsBefore - 1)

      // Verify table is still readable
      val df = readCompanion(indexPath)
      df.count() shouldBe 5
    }
  }

  test("PURGE on Iceberg companion preserves data integrity") {
    withIcebergCompanion { (_, indexPath, _, _) =>
      // Create orphans and purge
      createOrphanFile(indexPath, "orphan-integrity-1111-2222-3333-444444444444.split", 25)
      createOrphanFile(indexPath, "orphan-integrity-5555-6666-7777-888888888888.split", 25)

      val result  = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
      val metrics = result(0).getStruct(1)
      metrics.getString(0) shouldBe "SUCCESS"

      // Verify all rows are still correct
      val df   = readCompanion(indexPath)
      val rows = df.collect()
      rows.length shouldBe 5

      val rowsById = rows.map(r => r.getAs[Int]("id") -> r).toMap
      rowsById.size shouldBe 5

      rowsById(1).getAs[String]("name") shouldBe "alice"
      rowsById(2).getAs[String]("name") shouldBe "bob"
      rowsById(3).getAs[String]("name") shouldBe "charlie"
      rowsById(4).getAs[String]("name") shouldBe "dave"
      rowsById(5).getAs[String]("name") shouldBe "eve"

      rowsById(1).getAs[Double]("score") shouldBe 85.0 +- 0.01
      rowsById(2).getAs[Double]("score") shouldBe 92.5 +- 0.01
      rowsById(3).getAs[Double]("score") shouldBe 78.0 +- 0.01
      rowsById(4).getAs[Double]("score") shouldBe 95.1 +- 0.01
      rowsById(5).getAs[Double]("score") shouldBe 88.3 +- 0.01
    }
  }

  test("Re-sync after purge on Iceberg companion") {
    withIcebergCompanion { (root, indexPath, server, tableId) =>
      // Create orphans and purge
      createOrphanFile(indexPath, "orphan-resync-1111-2222-3333-444444444444.split", 25)

      val purgeResult = spark.sql(s"PURGE INDEXTABLE '$indexPath' OLDER THAN 0 HOURS").collect()
      val purgeMetrics = purgeResult(0).getStruct(1)
      purgeMetrics.getString(0) shouldBe "SUCCESS"

      // Append new data to Iceberg
      val newRows = Seq(
        Row(6, "frank", 91.0),
        Row(7, "grace", 87.5)
      )
      appendIcebergSnapshot(server, tableId, newRows, root, 2)

      flushCaches()

      // Re-sync should succeed
      val resyncResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.purge_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      resyncResult(0).getString(2) shouldBe "success"

      flushCaches()

      // Verify all rows (original + new) are present via count
      // (Iceberg companion file:// paths may not support .collect() reads)
      val df = readCompanion(indexPath)
      df.count() shouldBe 7
    }
  }
}
