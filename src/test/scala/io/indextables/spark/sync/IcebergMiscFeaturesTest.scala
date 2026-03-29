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

import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import io.indextables.spark.transaction.TransactionLogFactory

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests error handling, write guard, fast field modes, and FROM SNAPSHOT for Iceberg companion.
 */
class IcebergMiscFeaturesTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with io.indextables.spark.testutils.FileCleanupHelper
    with IcebergSnapshotHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("IcebergMiscFeaturesTest")
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

  // -- Schema ----------------------------------------------------------------

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get())
  )

  // -- Test data -------------------------------------------------------------

  private val testRows: Seq[Row] = Seq(
    Row(1L, "alice", 85.0),
    Row(2L, "bob", 90.5),
    Row(3L, "carol", 75.0),
    Row(4L, "dave", 80.0),
    Row(5L, "eve", 88.3)
  )

  // -- Helpers ---------------------------------------------------------------

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
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
    val root         = Files.createTempDirectory("iceberg-misc-features").toFile
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

  // =========================================================================
  //  F13 -- Error Handling
  // =========================================================================

  test("BUILD COMPANION should return error for non-existent Iceberg table") {
    withIcebergTable("error_nonexistent") { (server, _, _, indexPath) =>
      // Configure spark for embedded catalog (already done by withIcebergTable)
      // Don't create the table -- just try to build
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.nonexistent_table' AT LOCATION '$indexPath'"
      ).collect()
      result(0).getString(2) shouldBe "error"
    }
  }

  test("BUILD COMPANION should return error for unreachable catalog") {
    val root      = Files.createTempDirectory("iceberg-unreachable").toFile
    val indexPath = new File(root, "index").getAbsolutePath
    try {
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", "http://localhost:99999") // bad port

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.some_table' AT LOCATION '$indexPath'"
      ).collect()
      result(0).getString(2) shouldBe "error"
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      deleteRecursively(root)
    }
  }

  // =========================================================================
  //  F14 -- Write Guard
  // =========================================================================

  test("write to Iceberg companion table should be rejected") {
    withIcebergTable("write_guard") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendIcebergSnapshot(server, tableId, testRows, sparkSchema, root, 1)

      // Build companion
      syncIceberg("write_guard", indexPath).getString(2) shouldBe "success"

      // Try to write directly to the companion index
      val ex = intercept[Exception] {
        spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
          .write
          .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
          .mode("append")
          .save(indexPath)
      }
      val msg = Option(ex.getMessage).getOrElse("") +
        Option(ex.getCause).map(c => " " + c.getMessage).getOrElse("")
      msg.toLowerCase should (include("companion") or include("cannot write"))
    }
  }

  // =========================================================================
  //  F15 -- Fast Field Modes
  // =========================================================================

  test("FASTFIELDS MODE DISABLED should store mode in companion splits") {
    withIcebergTable("ff_disabled") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendIcebergSnapshot(server, tableId, testRows, sparkSchema, root, 1)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.ff_disabled' FASTFIELDS MODE DISABLED AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        txLog.listFiles().foreach(_.companionFastFieldMode shouldBe Some("DISABLED"))
      } finally txLog.close()
    }
  }

  test("FASTFIELDS MODE PARQUET_ONLY should store mode in companion splits") {
    withIcebergTable("ff_parquet_only") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendIcebergSnapshot(server, tableId, testRows, sparkSchema, root, 1)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.ff_parquet_only' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        txLog.listFiles().foreach(_.companionFastFieldMode shouldBe Some("PARQUET_ONLY"))
      } finally txLog.close()
    }
  }

  test("all three fast field modes should produce readable companion") {
    for (mode <- Seq("HYBRID", "DISABLED", "PARQUET_ONLY")) {
      withIcebergTable(s"ff_$mode") { (server, tableId, root, indexPath) =>
        server.catalog.buildTable(tableId, icebergSchema).create()
        appendIcebergSnapshot(server, tableId, testRows, sparkSchema, root, 1)

        spark.sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.ff_$mode' FASTFIELDS MODE $mode AT LOCATION '$indexPath'"
        ).collect()(0).getString(2) shouldBe "success"

        readCompanion(indexPath).count() shouldBe 5
      }
    }
  }

  // =========================================================================
  //  F17 -- FROM SNAPSHOT
  // =========================================================================

  test("FROM SNAPSHOT should sync specific snapshot, not current") {
    withIcebergTable("from_snapshot") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Snapshot 1: 3 rows
      val rows1 = Seq(
        Row(1L, "alice", 85.0),
        Row(2L, "bob", 90.5),
        Row(3L, "carol", 75.0)
      )
      appendIcebergSnapshot(server, tableId, rows1, sparkSchema, root, 1)
      val snap1Id = server.catalog.loadTable(tableId).currentSnapshot().snapshotId()

      // Snapshot 2: 2 more rows
      val rows2 = Seq(
        Row(4L, "dave", 80.0),
        Row(5L, "eve", 88.3)
      )
      appendIcebergSnapshot(server, tableId, rows2, sparkSchema, root, 2)
      val snap2Id = server.catalog.loadTable(tableId).currentSnapshot().snapshotId()
      snap2Id should not equal snap1Id

      // Build from snapshot 1 only
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.from_snapshot' FROM SNAPSHOT $snap1Id AT LOCATION '$indexPath'"
      ).collect()
      result(0).getString(2) shouldBe "success"
      result(0).getAs[Long](3) shouldBe snap1Id

      // Should only have snapshot 1 files (1 file, 3 rows)
      flushCaches()
      readCompanion(indexPath).count() shouldBe 3
    }
  }

  test("FROM SNAPSHOT with non-existent snapshot should return error") {
    withIcebergTable("bad_snapshot") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendIcebergSnapshot(server, tableId, testRows, sparkSchema, root, 1)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.bad_snapshot' FROM SNAPSHOT 9999999999999 AT LOCATION '$indexPath'"
      ).collect()
      result(0).getString(2) shouldBe "error"
    }
  }
}
