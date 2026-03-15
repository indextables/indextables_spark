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

package io.indextables.spark.sql

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import io.indextables.spark.sync.EmbeddedIcebergRestServer
import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * End-to-end streaming tests for Iceberg companion sync using an embedded REST catalog server.
 *
 * <p>No Docker, no MinIO, no external infrastructure required. An {@link EmbeddedIcebergRestServer} spins up an
 * in-process Iceberg REST catalog backed by the local filesystem. Parquet data files are written via Spark and then
 * registered with the catalog using the Iceberg Java API, creating new snapshots that the streaming manager detects via
 * the cheap snapshot probe.
 *
 * <p>The {@code file://} scheme is supported by the {@code iceberg = "0.8"} Rust crate's default {@code storage-fs}
 * feature, so all manifest reads and Parquet reads happen over the local filesystem with no credentials required.
 *
 * <p>Scenario covered: <ol> <li>Initial full sync populates the companion index (2 rows).</li> <li>A new snapshot
 * appended while the stream is running is picked up in the next cycle.</li> <li>A no-changes poll leaves the companion
 * unmodified.</li> <li>A second new snapshot is picked up by a subsequent cycle.</li> <li>A restart resumes from the
 * last synced snapshot without re-indexing existing data.</li> </ol>
 */
class StreamingCompanionIcebergEndToEndTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("StreamingCompanionIcebergEndToEndTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    )
  )

  private def withTempDirs(f: (String, String, String) => Unit): Unit = {
    val root = Files.createTempDirectory("iceberg-streaming-e2e").toFile
    try {
      val warehouseDir = new File(root, "warehouse").getAbsolutePath
      val indexPath    = new File(root, "index").getAbsolutePath
      new File(warehouseDir).mkdirs()
      flushCaches()
      f(warehouseDir, indexPath, root.getAbsolutePath)
    } finally
      deleteRecursively(root)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def countCompanionRows(indexPath: String): Long = {
    flushCaches()
    try
      spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "100000")
        .load(indexPath)
        .count()
    catch {
      case _: Exception => 0L
    }
  }

  private def waitUntil(timeoutMs: Long, pollMs: Long = 500)(condition: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      if (condition) return true
      Thread.sleep(pollMs)
    }
    false
  }

  /**
   * Write rows as a Parquet file via Spark, then register the resulting files as a new Iceberg snapshot in the given
   * table. Returns the number of actual Parquet files registered.
   */
  private def appendIcebergSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    batchId: Int
  ): Unit = {
    // Write to a sibling directory outside the warehouse to avoid conflicts with Iceberg's
    // own directory creation (which pre-creates the warehouse subtree).
    val tableLocalDir = s"${server.warehouseDir}/../parquet-data/batch-$batchId"

    // Write Parquet data via Spark to a unique batch directory (Spark creates the directory).
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), sparkSchema)
    df.coalesce(1).write.parquet(s"file://$tableLocalDir")

    // Collect the actual part files Spark wrote (skip _SUCCESS, .crc, etc.).
    val parquetFiles = new File(tableLocalDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    require(parquetFiles.nonEmpty, s"No Parquet files written to $tableLocalDir")

    // Register each part file as a DataFile in the Iceberg table.
    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    parquetFiles.foreach { f =>
      appendOp.appendFile(
        DataFiles
          .builder(table.spec())
          .withPath(s"file://${f.getAbsolutePath}")
          .withFileSizeInBytes(f.length())
          .withRecordCount(rows.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .build()
      )
    }
    appendOp.commit()
  }

  /** Create the default namespace + test_table in the embedded catalog. */
  private def createTestTable(server: EmbeddedIcebergRestServer): TableIdentifier = {
    val ns      = Namespace.of("default")
    val tableId = TableIdentifier.of(ns, "test_table")
    server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
    val schema = new IcebergSchema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "name", Types.StringType.get())
    )
    server.catalog
      .buildTable(tableId, schema)
      .create()
    tableId
  }

  /** Configure the active Spark session to point at the embedded catalog. */
  private def configureSparkForEmbeddedCatalog(server: EmbeddedIcebergRestServer): Unit = {
    spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
    spark.conf.set("spark.indextables.iceberg.uri", server.restUri)
  }

  private def clearSparkIcebergConfig(): Unit =
    Seq(
      "spark.indextables.iceberg.catalogType",
      "spark.indextables.iceberg.uri"
    ).foreach { k =>
      try spark.conf.unset(k)
      catch { case _: Exception => }
    }

  private def makeIcebergCommand(indexPath: String): SyncToExternalCommand =
    SyncToExternalCommand(
      sourceFormat = "iceberg",
      sourcePath = "default.test_table",
      destPath = indexPath,
      indexingModes = Map.empty,
      fastFieldMode = "HYBRID",
      targetInputSize = None,
      catalogType = Some("rest"),
      dryRun = false
    )

  // ── Tests ─────────────────────────────────────────────────────────────────────

  test("streaming Iceberg companion: initial sync then two incremental snapshots are indexed") {
    withTempDirs {
      (
        warehouseDir,
        indexPath,
        _
      ) =>
        val server = new EmbeddedIcebergRestServer(warehouseDir)
        try {
          val tableId = createTestTable(server)

          // Snapshot 1: 2 rows.
          appendIcebergSnapshot(server, tableId, Seq(Row(1L, "alice"), Row(2L, "bob")), batchId = 1)

          configureSparkForEmbeddedCatalog(server)

          val command = makeIcebergCommand(indexPath).copy(streamingPollIntervalMs = Some(2000L))
          val thread  = new Thread(() => command.run(spark))
          thread.setDaemon(true)
          thread.start()

          try {
            // Wait for initial full sync (2 rows).
            val initialSynced = waitUntil(30000)(countCompanionRows(indexPath) == 2)
            withClue("initial full sync should complete within 30 s") {
              initialSynced shouldBe true
            }

            // Snapshot 2: 2 more rows appended while the stream is running.
            appendIcebergSnapshot(server, tableId, Seq(Row(3L, "charlie"), Row(4L, "dave")), batchId = 2)

            // Stream should detect the new snapshot and index it incrementally.
            val secondSynced = waitUntil(30000)(countCompanionRows(indexPath) == 4)
            withClue("streaming should pick up snapshot 2 (4 total rows) within 30 s") {
              secondSynced shouldBe true
            }

            // Idle period: 3 poll cycles (6 s) — row count must remain stable.
            Thread.sleep(6000)
            withClue("row count should remain 4 after a no-changes idle period") {
              countCompanionRows(indexPath) shouldBe 4
            }

            // Snapshot 3: 1 more row.
            appendIcebergSnapshot(server, tableId, Seq(Row(5L, "eve")), batchId = 3)

            val thirdSynced = waitUntil(30000)(countCompanionRows(indexPath) == 5)
            withClue("streaming should pick up snapshot 3 (5 total rows) within 30 s") {
              thirdSynced shouldBe true
            }

          } finally {
            thread.interrupt()
            thread.join(5000)
          }
        } finally {
          clearSparkIcebergConfig()
          server.close()
        }
    }
  }

  test("streaming Iceberg companion: restart resumes from last synced snapshot") {
    withTempDirs {
      (
        warehouseDir,
        indexPath,
        _
      ) =>
        val server = new EmbeddedIcebergRestServer(warehouseDir)
        try {
          val tableId = createTestTable(server)

          // Snapshot 1: 2 rows.
          appendIcebergSnapshot(server, tableId, Seq(Row(1L, "alice"), Row(2L, "bob")), batchId = 1)

          configureSparkForEmbeddedCatalog(server)

          val command = makeIcebergCommand(indexPath)

          // Run one direct sync to establish a known 2-row baseline with snapshot 1 persisted.
          command.run(spark)
          countCompanionRows(indexPath) shouldBe 2

          // Snapshot 2: 1 more row — NOT covered by the direct sync above.
          appendIcebergSnapshot(server, tableId, Seq(Row(3L, "charlie")), batchId = 2)

          // Start streaming: it reads the persisted lastSyncedVersion (snapshot 1) and runs an
          // incremental cycle for snapshot 2 instead of re-indexing all data.
          val streamingCommand = command.copy(streamingPollIntervalMs = Some(2000L))
          val thread           = new Thread(() => streamingCommand.run(spark))
          thread.setDaemon(true)
          thread.start()

          try {
            val resumeSynced = waitUntil(30000)(countCompanionRows(indexPath) == 3)
            withClue("streaming resume should index the missing snapshot (charlie) within 30 s") {
              resumeSynced shouldBe true
            }
          } finally {
            thread.interrupt()
            thread.join(5000)
          }
        } finally {
          clearSparkIcebergConfig()
          server.close()
        }
    }
  }
}
