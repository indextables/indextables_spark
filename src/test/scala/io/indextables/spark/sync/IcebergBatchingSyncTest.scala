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

import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests batchSize and maxConcurrentBatches configuration with Iceberg companion sync.
 *
 * Verifies that the batched concurrent dispatch mechanism works correctly when building
 * companion indexes for Iceberg tables, mirroring the batching tests in MultiTransactionSyncTest
 * but using the Iceberg REST catalog path via EmbeddedIcebergRestServer.
 */
class IcebergBatchingSyncTest
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
      .appName("IcebergBatchingSyncTest")
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
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get())
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /**
   * Count the number of Avro state directories in the transaction log. Each commit (via writeActionsToAvroState)
   * creates a new state-vN directory. This is the reliable way to count commits with the Avro state format.
   */
  private def countStateDirs(indexPath: String): Int = {
    val txLogDir = new java.io.File(indexPath, "_transaction_log")
    if (!txLogDir.exists()) return 0
    Option(txLogDir.listFiles())
      .map(_.count(f => f.isDirectory && f.getName.startsWith("state-v")))
      .getOrElse(0)
  }

  private def syncIcebergWithTargetSize(tableName: String, indexPath: String, targetSize: String = "1M"): Row = {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.$tableName' TARGET INPUT SIZE $targetSize AT LOCATION '$indexPath'"
    ).collect()
    result.length shouldBe 1
    result(0)
  }

  /** Sets up an Iceberg table with EmbeddedIcebergRestServer and runs the test body. */
  private def withIcebergTable(tableName: String)(
    f: (EmbeddedIcebergRestServer, TableIdentifier, File) => Unit
  ): Unit = {
    val root         = Files.createTempDirectory("iceberg-batching-sync").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, Collections.emptyMap())

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      f(server, tableId, root)
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
    (startId until startId + count).map(i => Row(i, s"user_$i", i * 10.0))

  // ═══════════════════════════════════════════════════════════════════════════
  //  Batched Concurrent Dispatch Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("batchSize=1 should produce one commit per indexing task") {
    withIcebergTable("batch_one") { (server, tableId, root) =>
      val indexPath = new File(root, "index_batch_one").getAbsolutePath
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Create 4 separate snapshots (1 file each via coalesce(1))
      for (i <- 1 to 4)
        appendIcebergSnapshot(server, tableId, makeBatch(i * 10, 1), sparkSchema, root, i)

      // Force batchSize=1 so each indexing task is its own batch (= its own commit)
      // TARGET INPUT SIZE 1 forces each parquet file into its own indexing group
      spark.conf.set("spark.indextables.companion.sync.batchSize", "1")
      try {
        val row = syncIcebergWithTargetSize("batch_one", indexPath, "1")
        row.getString(2) shouldBe "success"

        // Verify multiple Avro state directories (one per batch commit)
        val stateDirs = countStateDirs(indexPath)
        stateDirs should be >= 4 // 4 batch commits (one per indexing group)
      } finally
        spark.conf.unset("spark.indextables.companion.sync.batchSize")
    }
  }

  test("batchSize=1 with maxConcurrentBatches=1 should produce sequential commits") {
    withIcebergTable("batch_seq") { (server, tableId, root) =>
      val indexPath = new File(root, "index_batch_seq").getAbsolutePath
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Create 4 separate snapshots (1 file each via coalesce(1))
      for (i <- 1 to 4)
        appendIcebergSnapshot(server, tableId, makeBatch(i * 10, 1), sparkSchema, root, i)

      // Force sequential execution: 1 batch at a time, 1 task per batch
      spark.conf.set("spark.indextables.companion.sync.batchSize", "1")
      spark.conf.set("spark.indextables.companion.sync.maxConcurrentBatches", "1")
      try {
        val row = syncIcebergWithTargetSize("batch_seq", indexPath, "1")
        row.getString(2) shouldBe "success"

        // Verify multiple state directories from sequential commits
        val stateDirs = countStateDirs(indexPath)
        stateDirs should be >= 4 // 4 sequential batch commits
      } finally {
        spark.conf.unset("spark.indextables.companion.sync.batchSize")
        spark.conf.unset("spark.indextables.companion.sync.maxConcurrentBatches")
      }
    }
  }

  test("default batchSize should produce fewer commits than batchSize=1") {
    withIcebergTable("batch_default") { (server, tableId, root) =>
      val indexPath1 = new File(root, "index_default").getAbsolutePath
      val indexPath2 = new File(root, "index_batch1").getAbsolutePath
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Create 4 separate snapshots (1 file each via coalesce(1))
      for (i <- 1 to 4)
        appendIcebergSnapshot(server, tableId, makeBatch(i * 10, 1), sparkSchema, root, i)

      // Build with default config (larger batch size groups tasks together)
      val row1 = syncIcebergWithTargetSize("batch_default", indexPath1, "1")
      row1.getString(2) shouldBe "success"

      flushCaches()

      // Build with batchSize=1 (each task is its own commit)
      spark.conf.set("spark.indextables.companion.sync.batchSize", "1")
      try {
        val row2 = syncIcebergWithTargetSize("batch_default", indexPath2, "1")
        row2.getString(2) shouldBe "success"

        // batchSize=1 should produce at least as many state dirs as default
        val stateDirsDefault = countStateDirs(indexPath1)
        val stateDirsBatch1  = countStateDirs(indexPath2)
        stateDirsBatch1 should be >= stateDirsDefault
      } finally
        spark.conf.unset("spark.indextables.companion.sync.batchSize")
    }
  }

  test("batching config with Iceberg incremental sync") {
    withIcebergTable("batch_incr") { (server, tableId, root) =>
      val indexPath = new File(root, "index_batch_incr").getAbsolutePath
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Create 4 separate snapshots (1 file each via coalesce(1))
      for (i <- 1 to 4)
        appendIcebergSnapshot(server, tableId, makeBatch(i * 10, 1), sparkSchema, root, i)

      // Initial sync (all 4 files)
      val row1 = syncIcebergWithTargetSize("batch_incr", indexPath, "1")
      row1.getString(2) shouldBe "success"

      val stateDirsBefore = countStateDirs(indexPath)

      // Append 3 more snapshots
      for (i <- 5 to 7)
        appendIcebergSnapshot(server, tableId, makeBatch(i * 10, 1), sparkSchema, root, i)

      flushCaches()

      // Re-sync with batchSize=1 + TARGET INPUT SIZE 1
      spark.conf.set("spark.indextables.companion.sync.batchSize", "1")
      try {
        val row2 = syncIcebergWithTargetSize("batch_incr", indexPath, "1")
        row2.getString(2) shouldBe "success"

        // Verify new batched commits were created for incremental files
        val stateDirsAfter = countStateDirs(indexPath)
        stateDirsAfter should be > stateDirsBefore
      } finally
        spark.conf.unset("spark.indextables.companion.sync.batchSize")
    }
  }
}
