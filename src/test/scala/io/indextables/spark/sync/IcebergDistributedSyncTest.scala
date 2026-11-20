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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
 * End-to-end integration tests for distributed sync and WHERE clause partition filtering for Iceberg companion tables.
 *
 * Tests the BUILD INDEXTABLES COMPANION FOR ICEBERG command with partition-scoped WHERE clauses (equality, IN, !=),
 * scoped invalidation, INVALIDATE ALL PARTITIONS, and TARGET INPUT SIZE. Uses EmbeddedIcebergRestServer with
 * partitioned Iceberg tables (identity partitioning on "region").
 */
class IcebergDistributedSyncTest
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
      .appName("IcebergDistributedSyncTest")
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
      StructField("score", DoubleType, nullable = false),
      StructField("region", StringType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get()),
    Types.NestedField.required(4, "region", Types.StringType.get())
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /** Write parquet rows for a specific partition and register via DataFiles with withPartitionPath. Returns file paths. */
  private def appendPartitionedSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rootDir: File,
    batchId: Int,
    region: String,
    rows: Seq[Row]
  ): Seq[String] = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId-$region").getAbsolutePath
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), sparkSchema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    val table = server.catalog.loadTable(tableId)
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

  /** Delete data files from an Iceberg table by path (creates new snapshot). */
  private def deleteFiles(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    filePaths: Seq[String]
  ): Unit = {
    val table = server.catalog.loadTable(tableId)
    val deleteOp = table.newDelete()
    filePaths.foreach(deleteOp.deleteFile)
    deleteOp.commit()
  }

  /** Sync helper that runs BUILD INDEXTABLES COMPANION FOR ICEBERG with optional extra clauses. */
  private def syncIceberg(tableName: String, indexPath: String, extraClauses: String = ""): Row = {
    val sql = s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.$tableName' $extraClauses AT LOCATION '$indexPath'"
    val result = spark.sql(sql).collect()
    result.length shouldBe 1
    result(0)
  }

  /** Open a TransactionLog for inspection, returning the log instance. Caller must close. */
  private def openTxLog(indexPath: String): io.indextables.spark.transaction.TransactionLogInterface =
    TransactionLogFactory.create(
      new Path(indexPath),
      spark,
      new CaseInsensitiveStringMap(
        Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
      )
    )

  /**
   * Sets up a partitioned Iceberg table with 3 regions (us-east, us-west, eu-west), each with 4 rows.
   * Returns file paths per region so tests can selectively delete them.
   */
  private def withPartitionedIcebergTable(tableName: String)(
    f: (EmbeddedIcebergRestServer, TableIdentifier, File, String, Map[String, Seq[String]]) => Unit
  ): Unit = {
    val root = Files.createTempDirectory("iceberg-distributed-sync").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()
      val ns = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, Collections.emptyMap())

      val spec = PartitionSpec.builderFor(icebergSchema).identity("region").build()
      server.catalog.buildTable(tableId, icebergSchema).withPartitionSpec(spec).create()

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Seed 3 regions: us-east (4 rows), us-west (4 rows), eu-west (4 rows)
      val usEastPaths = appendPartitionedSnapshot(server, tableId, root, 1, "us-east",
        Seq(Row(1L, "alice", 85.0, "us-east"), Row(2L, "bob", 90.0, "us-east"),
            Row(3L, "carol", 75.0, "us-east"), Row(4L, "dave", 80.0, "us-east")))
      val usWestPaths = appendPartitionedSnapshot(server, tableId, root, 2, "us-west",
        Seq(Row(5L, "eve", 95.0, "us-west"), Row(6L, "frank", 70.0, "us-west"),
            Row(7L, "grace", 88.0, "us-west"), Row(8L, "heidi", 92.0, "us-west")))
      val euWestPaths = appendPartitionedSnapshot(server, tableId, root, 3, "eu-west",
        Seq(Row(9L, "ivan", 78.0, "eu-west"), Row(10L, "judy", 83.0, "eu-west"),
            Row(11L, "karl", 91.0, "eu-west"), Row(12L, "lisa", 87.0, "eu-west")))

      val pathsByRegion = Map(
        "us-east" -> usEastPaths,
        "us-west" -> usWestPaths,
        "eu-west" -> euWestPaths
      )

      f(server, tableId, root, indexPath, pathsByRegion)
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
  //  Basic Build & Sync
  // ═══════════════════════════════════════════════════════════════════════════

  test("BUILD COMPANION for Iceberg should succeed") {
    withPartitionedIcebergTable("basic_build") { (_, _, _, indexPath, _) =>
      val row = syncIceberg("basic_build", indexPath)
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0   // splits_created
      row.getInt(6) should be >= 3  // parquet_files_indexed (at least 3 files from 3 partitions)
    }
  }

  test("BUILD COMPANION for Iceberg should record companion metadata") {
    withPartitionedIcebergTable("metadata_check") { (_, _, _, indexPath, _) =>
      syncIceberg("metadata_check", indexPath)

      val txLog = openTxLog(indexPath)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration should contain key "indextables.companion.enabled"
        metadata.configuration("indextables.companion.enabled") shouldBe "true"
        metadata.configuration should contain key "indextables.companion.sourceFormat"
        metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      } finally
        txLog.close()
    }
  }

  test("incremental sync should detect new Iceberg snapshot") {
    withPartitionedIcebergTable("incr_sync") { (server, tableId, root, indexPath, _) =>
      val row1 = syncIceberg("incr_sync", indexPath)
      row1.getString(2) shouldBe "success"

      // Append a new partition
      appendPartitionedSnapshot(server, tableId, root, 4, "ap-south",
        Seq(Row(13L, "mike", 77.0, "ap-south"), Row(14L, "nina", 82.0, "ap-south")))
      flushCaches()

      val row2 = syncIceberg("incr_sync", indexPath)
      row2.getString(2) shouldBe "success"
      row2.getInt(4) should be > 0 // splits_created for new partition
    }
  }

  test("re-sync with no changes should return no_action") {
    withPartitionedIcebergTable("no_action") { (_, _, _, indexPath, _) =>
      syncIceberg("no_action", indexPath).getString(2) shouldBe "success"
      flushCaches()
      syncIceberg("no_action", indexPath).getString(2) shouldBe "no_action"
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WHERE Clause Partition Filtering
  // ═══════════════════════════════════════════════════════════════════════════

  test("WHERE with equality filter should only index matching partition") {
    withPartitionedIcebergTable("where_eq") { (_, _, _, indexPath, _) =>
      val row = syncIceberg("where_eq", indexPath, "WHERE region = 'us-east'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach(f => f.partitionValues.get("region") shouldBe Some("us-east"))
      } finally txLog.close()
    }
  }

  test("WHERE with IN filter should index matching partitions only") {
    withPartitionedIcebergTable("where_in") { (_, _, _, indexPath, _) =>
      val row = syncIceberg("where_in", indexPath, "WHERE region IN ('us-east', 'eu-west')")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("eu-west")
        regionVals should not contain "us-west"
      } finally txLog.close()
    }
  }

  test("WHERE with != filter should exclude matching partition") {
    withPartitionedIcebergTable("where_neq") { (_, _, _, indexPath, _) =>
      val row = syncIceberg("where_neq", indexPath, "WHERE region != 'us-east'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-west")
        regionVals should contain("eu-west")
        regionVals should not contain "us-east"
      } finally txLog.close()
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WHERE-Scoped Invalidation
  // ═══════════════════════════════════════════════════════════════════════════

  test("WHERE-scoped invalidation should not invalidate splits outside WHERE range") {
    withPartitionedIcebergTable("scoped_outside") { (server, tableId, _, indexPath, pathsByRegion) =>
      // Initial full sync (all 3 regions)
      val row1 = syncIceberg("scoped_outside", indexPath)
      row1.getString(2) shouldBe "success"

      // Delete us-east files from the Iceberg table
      deleteFiles(server, tableId, pathsByRegion("us-east"))
      flushCaches()

      // Re-sync with WHERE region = 'us-west' — us-east is outside scope
      syncIceberg("scoped_outside", indexPath, "WHERE region = 'us-west'")

      // Verify: splits for us-east should still exist (not invalidated, outside WHERE scope)
      val txLog = openTxLog(indexPath)
      try {
        val remainingFiles = txLog.listFiles()
        val usEastSplits = remainingFiles.filter(_.partitionValues.get("region").contains("us-east"))
        usEastSplits should not be empty
      } finally txLog.close()
    }
  }

  // Known bug: Iceberg scoped invalidation does not detect deleted files within WHERE range.
  // The anti-join doesn't detect file removals via manifest-based comparison.
  test("WHERE-scoped invalidation detects gone files within WHERE range") {
    withPartitionedIcebergTable("scoped_within") { (server, tableId, _, indexPath, pathsByRegion) =>
      val row1 = syncIceberg("scoped_within", indexPath)
      row1.getString(2) shouldBe "success"

      deleteFiles(server, tableId, pathsByRegion("us-west"))
      flushCaches()

      val row2 = syncIceberg("scoped_within", indexPath, "WHERE region = 'us-west'")

      val txLog = openTxLog(indexPath)
      try {
        val remainingFiles = txLog.listFiles()
        val usWestSplits = remainingFiles.filter(_.partitionValues.get("region").contains("us-west"))
        usWestSplits shouldBe empty
      } finally txLog.close()
    }
  }

  test("INVALIDATE ALL PARTITIONS should invalidate all gone splits") {
    withPartitionedIcebergTable("invalidate_all") { (server, tableId, _, indexPath, pathsByRegion) =>
      // Initial full sync (all 3 regions)
      val row1 = syncIceberg("invalidate_all", indexPath)
      row1.getString(2) shouldBe "success"

      // Delete us-east files from the Iceberg table
      deleteFiles(server, tableId, pathsByRegion("us-east"))
      flushCaches()

      // Re-sync with WHERE that excludes us-east BUT with INVALIDATE ALL PARTITIONS
      syncIceberg("invalidate_all", indexPath, "WHERE region != 'us-east' INVALIDATE ALL PARTITIONS")

      // Verify: splits for us-east SHOULD be invalidated (INVALIDATE ALL PARTITIONS overrides scope)
      val txLog = openTxLog(indexPath)
      try {
        val remainingFiles = txLog.listFiles()
        val usEastSplits = remainingFiles.filter(_.partitionValues.get("region").contains("us-east"))
        usEastSplits shouldBe empty
      } finally txLog.close()
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TARGET INPUT SIZE
  // ═══════════════════════════════════════════════════════════════════════════

  test("TARGET INPUT SIZE should control split sizing for Iceberg") {
    withPartitionedIcebergTable("target_size") { (_, _, _, indexPath, _) =>
      val row = syncIceberg("target_size", indexPath, "TARGET INPUT SIZE 1M")
      row.getString(2) shouldBe "success"
      row.getInt(4) should be > 0 // splits_created
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Partition Column Detection
  // ═══════════════════════════════════════════════════════════════════════════

  test("partition columns detected from Iceberg partition spec") {
    withPartitionedIcebergTable("partition_detect") { (_, _, _, indexPath, _) =>
      syncIceberg("partition_detect", indexPath).getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        // Every split should have a "region" partition value
        files.foreach { f =>
          f.partitionValues should contain key "region"
        }
        // Verify all 3 regions are represented
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("us-west")
        regionVals should contain("eu-west")
      } finally txLog.close()
    }
  }
}
