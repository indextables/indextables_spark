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
import java.util.Collections

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import io.indextables.spark.transaction.TransactionLogFactory

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests that BUILD INDEXTABLES COMPANION FOR ICEBERG correctly handles Iceberg tables that have undergone data
 * mutations between syncs: appends, file deletions, overwrites, and mixed operations.
 *
 * Mirrors the Delta mutation tests in MultiTransactionSyncTest but uses the Iceberg Java API (DataFiles, DeleteFiles,
 * OverwriteFiles) against an EmbeddedIcebergRestServer. No Docker required.
 */
class IcebergMutationSyncTest
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
      .appName("IcebergMutationSyncTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
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

  // ── Schema ───────────────────────────────────────────────────────────────────

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("department", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "department", Types.StringType.get()),
    Types.NestedField.required(4, "score", Types.DoubleType.get())
  )

  private val partitionedIcebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get()),
    Types.NestedField.required(4, "region", Types.StringType.get())
  )

  private val partitionedSparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("region", StringType, nullable = false)
    )
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /** Write parquet rows and register as an Iceberg snapshot. Returns paths of registered data files. */
  private def appendSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    schema: StructType,
    rootDir: File,
    batchId: Int
  ): Seq[String] = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId").getAbsolutePath
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    val paths    = parquetFiles.map { pf =>
      val path = s"file://${pf.getAbsolutePath}"
      appendOp.appendFile(
        DataFiles
          .builder(table.spec())
          .withPath(path)
          .withFileSizeInBytes(pf.length())
          .withRecordCount(rows.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .build()
      )
      path
    }
    appendOp.commit()
    paths.toSeq
  }

  /** Delete data files from an Iceberg table by path (creates new snapshot). */
  private def deleteFiles(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    filePaths: Seq[String]
  ): Unit = {
    val table    = server.catalog.loadTable(tableId)
    val deleteOp = table.newDelete()
    filePaths.foreach(deleteOp.deleteFile)
    deleteOp.commit()
  }

  /** Overwrite: atomically remove old files and add new ones (simulates compaction). */
  private def overwriteFiles(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    oldPaths: Seq[String],
    newRows: Seq[Row],
    schema: StructType,
    rootDir: File,
    batchId: Int
  ): Seq[String] = {
    // Write replacement parquet
    val parquetDir = new File(rootDir, s"parquet-data/overwrite-$batchId").getAbsolutePath
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(newRows), schema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    val table       = server.catalog.loadTable(tableId)
    val overwriteOp = table.newOverwrite()

    oldPaths.foreach { path =>
      overwriteOp.deleteFile(
        DataFiles.builder(table.spec()).withPath(path).withFileSizeInBytes(1L).withRecordCount(1L).withFormat(FileFormat.PARQUET).build()
      )
    }

    val newPaths = parquetFiles.map { pf =>
      val path = s"file://${pf.getAbsolutePath}"
      overwriteOp.addFile(
        DataFiles
          .builder(table.spec())
          .withPath(path)
          .withFileSizeInBytes(pf.length())
          .withRecordCount(newRows.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .build()
      )
      path
    }
    overwriteOp.commit()
    newPaths.toSeq
  }

  private def syncIceberg(tableName: String, indexPath: String): Row = {
    val result = spark
      .sql(s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.$tableName' AT LOCATION '$indexPath'")
      .collect()
    result.length shouldBe 1
    result(0)
  }

  private def readCompanion(indexPath: String): org.apache.spark.sql.DataFrame = {
    flushCaches()
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "10000")
      .load(indexPath)
  }

  /** Sets up an Iceberg table with EmbeddedIcebergRestServer and runs the test body. */
  private def withIcebergTable(tableName: String)(
    f: (EmbeddedIcebergRestServer, TableIdentifier, File, String) => Unit
  ): Unit = {
    val root         = Files.createTempDirectory("iceberg-mutation-sync").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, Collections.emptyMap())

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      f(server, tableId, root, indexPath)
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  private def makeBatch(startId: Long, count: Int): Seq[Row] =
    (startId until startId + count).map(i => Row(i, s"user_$i", "engineering", i * 10.0))

  // ═══════════════════════════════════════════════════════════════════════════
  //  Append & Incremental Sync
  // ═══════════════════════════════════════════════════════════════════════════

  test("initial sync after multiple snapshots should index all files") {
    withIcebergTable("multi_snap") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      appendSnapshot(server, tableId, makeBatch(1, 3), sparkSchema, root, 1)
      appendSnapshot(server, tableId, makeBatch(4, 3), sparkSchema, root, 2)
      appendSnapshot(server, tableId, makeBatch(7, 4), sparkSchema, root, 3)

      val row = syncIceberg("multi_snap", indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0
      row.getInt(6) shouldBe 3 // exactly 3 files (1 per snapshot, each coalesced to 1)

      readCompanion(indexPath).count() shouldBe 10 // 3+3+4 rows
    }
  }

  test("incremental sync should detect new files from new snapshot") {
    withIcebergTable("incr_sync") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)

      val row1 = syncIceberg("incr_sync", indexPath)
      row1.getString(2) shouldBe "success"

      // Append new snapshot
      appendSnapshot(server, tableId, makeBatch(6, 5), sparkSchema, root, 2)
      flushCaches()

      val row2 = syncIceberg("incr_sync", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(4) should be > 0
      row2.getInt(6) shouldBe 2 // re-indexes existing + 1 new file

      readCompanion(indexPath).count() shouldBe 10 // 5 original + 5 new
    }
  }

  test("re-sync with no new snapshot should be no-op") {
    withIcebergTable("no_op") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)

      syncIceberg("no_op", indexPath).getString(2) shouldBe "success"
      flushCaches()
      syncIceberg("no_op", indexPath).getString(2) shouldBe "no_action"
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  File Deletion (equivalent to Delta DELETE)
  // ═══════════════════════════════════════════════════════════════════════════

  test("sync after file deletion should invalidate old splits") {
    withIcebergTable("delete_files") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      val batch1Paths = appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)
      appendSnapshot(server, tableId, makeBatch(6, 5), sparkSchema, root, 2)

      val row1 = syncIceberg("delete_files", indexPath)
      row1.getString(2) shouldBe "success"

      // Delete the first batch's files
      deleteFiles(server, tableId, batch1Paths)
      flushCaches()

      val row2 = syncIceberg("delete_files", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits_invalidated

      readCompanion(indexPath).count() shouldBe 5 // only batch2 remains
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  File Overwrite (equivalent to Delta OPTIMIZE / compaction)
  // ═══════════════════════════════════════════════════════════════════════════

  test("sync after overwrite should handle replaced files") {
    withIcebergTable("overwrite_files") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      val batch1Paths = appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)
      appendSnapshot(server, tableId, makeBatch(6, 5), sparkSchema, root, 2)

      val row1 = syncIceberg("overwrite_files", indexPath)
      row1.getString(2) shouldBe "success"

      // Overwrite batch 1 files with compacted version (simulates OPTIMIZE)
      overwriteFiles(server, tableId, batch1Paths, makeBatch(1, 5), sparkSchema, root, 3)
      flushCaches()

      val row2 = syncIceberg("overwrite_files", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits_invalidated (old files gone)
      row2.getInt(4) should be > 0 // splits_created (new compacted file)

      readCompanion(indexPath).count() shouldBe 10 // all rows present, just rewritten
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Mixed Operations
  // ═══════════════════════════════════════════════════════════════════════════

  test("sync after mixed append + delete should reflect final state") {
    withIcebergTable("mixed_ops") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      val batch1Paths = appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)

      syncIceberg("mixed_ops", indexPath).getString(2) shouldBe "success"

      // Multiple mutations without sync in between:
      // 1. Append new data
      appendSnapshot(server, tableId, makeBatch(6, 5), sparkSchema, root, 2)
      // 2. Delete old data
      deleteFiles(server, tableId, batch1Paths)
      // 3. Append more data
      appendSnapshot(server, tableId, makeBatch(11, 5), sparkSchema, root, 4)

      flushCaches()

      val row = syncIceberg("mixed_ops", indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // splits_created (new files)
      row.getInt(5) should be > 0 // splits_invalidated (deleted files)

      readCompanion(indexPath).count() shouldBe 10 // batch1 deleted (5), batch2 (5) + batch3 (5)
    }
  }

  test("sync should handle large snapshot gap") {
    withIcebergTable("large_gap") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, makeBatch(1, 2), sparkSchema, root, 1)

      syncIceberg("large_gap", indexPath).getString(2) shouldBe "success"

      // Create 10 more snapshots
      for (i <- 2 to 11)
        appendSnapshot(server, tableId, Seq(Row(i.toLong, s"user_$i", "engineering", i * 10.0)), sparkSchema, root, i)

      flushCaches()

      val row = syncIceberg("large_gap", indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0
      row.getInt(6) shouldBe 11 // 1 existing re-indexed + 10 new (1 per snapshot)

      readCompanion(indexPath).count() shouldBe 12 // 2 original + 10 new
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Metadata Tracking
  // ═══════════════════════════════════════════════════════════════════════════

  test("companion metadata should track snapshot ID correctly") {
    withIcebergTable("snapshot_meta") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, makeBatch(1, 5), sparkSchema, root, 1)

      val expectedSnapshotId = server.catalog.loadTable(tableId).currentSnapshot().snapshotId()

      val row = syncIceberg("snapshot_meta", indexPath)
      row.getString(2) shouldBe "success"

      val snapshotId = row.getAs[Long](3)
      snapshotId shouldBe expectedSnapshotId

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      } finally
        txLog.close()
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Partitioned Tables
  // ═══════════════════════════════════════════════════════════════════════════

  test("sync after append to partitioned Iceberg table") {
    withIcebergTable("part_append") { (server, tableId, root, indexPath) =>
      val spec = PartitionSpec.builderFor(partitionedIcebergSchema).identity("region").build()
      server.catalog.buildTable(tableId, partitionedIcebergSchema).withPartitionSpec(spec).create()

      // Append us-east and us-west partitions
      appendPartitioned(server, tableId, root, 1, "us-east",
        Seq(Row(1L, "alice", 85.0, "us-east"), Row(2L, "bob", 90.0, "us-east")))
      appendPartitioned(server, tableId, root, 2, "us-west",
        Seq(Row(3L, "carol", 75.0, "us-west"), Row(4L, "dave", 80.0, "us-west")))

      syncIceberg("part_append", indexPath).getString(2) shouldBe "success"

      // Append new eu-west partition
      appendPartitioned(server, tableId, root, 3, "eu-west",
        Seq(Row(5L, "eve", 95.0, "eu-west"), Row(6L, "frank", 70.0, "eu-west")))

      flushCaches()

      val row2 = syncIceberg("part_append", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(4) should be > 0

      // Verify eu-west partition exists in companion splits
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val regionVals = txLog.listFiles().flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("eu-west")
        regionVals should contain("us-east")
        regionVals should contain("us-west")
      } finally txLog.close()

      readCompanion(indexPath).count() shouldBe 6 // 2+2+2
    }
  }

  test("sync after delete in partitioned Iceberg table should invalidate affected partition") {
    withIcebergTable("part_delete") { (server, tableId, root, indexPath) =>
      val spec = PartitionSpec.builderFor(partitionedIcebergSchema).identity("region").build()
      server.catalog.buildTable(tableId, partitionedIcebergSchema).withPartitionSpec(spec).create()

      val eastPaths = appendPartitioned(server, tableId, root, 1, "us-east",
        Seq(Row(1L, "alice", 85.0, "us-east"), Row(2L, "bob", 90.0, "us-east")))
      appendPartitioned(server, tableId, root, 2, "us-west",
        Seq(Row(3L, "carol", 75.0, "us-west"), Row(4L, "dave", 80.0, "us-west")))

      syncIceberg("part_delete", indexPath).getString(2) shouldBe "success"

      // Delete us-east partition files
      deleteFiles(server, tableId, eastPaths)
      flushCaches()

      val row2 = syncIceberg("part_delete", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(5) should be > 0 // splits_invalidated

      // Verify us-east partition is gone, us-west remains
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val regionVals = txLog.listFiles().flatMap(_.partitionValues.get("region")).toSet
        regionVals should not contain "us-east"
        regionVals should contain("us-west")
      } finally txLog.close()

      readCompanion(indexPath).count() shouldBe 2 // only us-west rows
    }
  }

  /** Append rows to a partitioned Iceberg table. Writes parquet to a Hive-style partition directory. */
  private def appendPartitioned(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rootDir: File,
    batchId: Int,
    region: String,
    rows: Seq[Row]
  ): Seq[String] = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId-$region").getAbsolutePath
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), partitionedSparkSchema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    val paths = parquetFiles.map { pf =>
      val path = s"file://${pf.getAbsolutePath}"
      appendOp.appendFile(
        DataFiles
          .builder(table.spec())
          .withPath(path)
          .withFileSizeInBytes(pf.length())
          .withRecordCount(rows.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .withPartitionPath(s"region=$region")
          .build()
      )
      path
    }
    appendOp.commit()
    paths.toSeq
  }
}
