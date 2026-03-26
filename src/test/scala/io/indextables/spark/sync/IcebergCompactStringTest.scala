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

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests compact string indexing modes with Iceberg companion.
 *
 * Exercises the `exact_only`, `text_uuid_exactonly`, `text_uuid_strip`, and combined modes via BUILD INDEXTABLES
 * COMPANION FOR ICEBERG with INDEXING MODES syntax.
 *
 * No cloud credentials needed -- runs entirely on local filesystem with EmbeddedIcebergRestServer.
 */
class IcebergCompactStringTest
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
      .appName("IcebergCompactStringTest")
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

  // -- Schema -----------------------------------------------------------------

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("trace_id", StringType, nullable = false),
      StructField("message", StringType, nullable = false),
      StructField("audit_log", StringType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "trace_id", Types.StringType.get()),
    Types.NestedField.required(3, "message", Types.StringType.get()),
    Types.NestedField.required(4, "audit_log", Types.StringType.get())
  )

  // -- Data (5 rows) ----------------------------------------------------------

  private val rows: Seq[Row] = Seq(
    Row(1L, "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "Processing request a1b2c3d4-e5f6-7890-abcd-ef1234567890 for user alice",
      "Order ORD-00000001 processed successfully"),
    Row(2L, "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "Processing request b2c3d4e5-f6a7-8901-bcde-f12345678901 for user bob",
      "Order ORD-00000002 processed successfully"),
    Row(3L, "c3d4e5f6-a7b8-9012-cdef-123456789012",
      "Processing request c3d4e5f6-a7b8-9012-cdef-123456789012 for user carol",
      "Order ORD-00000003 processed successfully"),
    Row(4L, "d4e5f6a7-b8c9-0123-defa-234567890123",
      "Handling error d4e5f6a7-b8c9-0123-defa-234567890123 for user dave",
      "Order ORD-00000004 failed validation"),
    Row(5L, "e5f6a7b8-c9d0-1234-efab-345678901234",
      "Processing request e5f6a7b8-c9d0-1234-efab-345678901234 for user eve",
      "Order ORD-00000005 processed successfully")
  )

  // -- Helpers ----------------------------------------------------------------

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /** Write parquet rows and register as an Iceberg snapshot. */
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

  private def syncIceberg(tableName: String, indexPath: String, indexingModes: String = ""): Row = {
    val modeClause = if (indexingModes.nonEmpty) s" INDEXING MODES ($indexingModes)" else ""
    val result = spark
      .sql(s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.$tableName'$modeClause AT LOCATION '$indexPath'")
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
    val root         = Files.createTempDirectory("iceberg-compact-string").toFile
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

  // ===========================================================================
  //  Tests
  // ===========================================================================

  test("exact_only mode should support equality filter on Iceberg companion") {
    withIcebergTable("exact_only_ice") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val row = syncIceberg("exact_only_ice", indexPath, "'trace_id':'exact_only'")
      row.getString(2) shouldBe "success"

      val companionDf = readCompanion(indexPath)
      companionDf.count() shouldBe 5

      // Equality filter on exact_only field should return exactly 1 row
      val filtered = companionDf.filter(
        companionDf("trace_id") === "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
      ).collect()
      filtered.length shouldBe 1
    }
  }

  test("text_uuid_exactonly mode should index text and preserve UUIDs") {
    withIcebergTable("text_uuid_exactonly_ice") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val row = syncIceberg("text_uuid_exactonly_ice", indexPath, "'message':'text_uuid_exactonly'")
      row.getString(2) shouldBe "success"

      val companionDf = readCompanion(indexPath)
      companionDf.createOrReplaceTempView("text_uuid_exactonly_view")

      // Text search for "processing" should return rows (4 out of 5 contain "Processing request")
      val textResults = spark
        .sql("SELECT * FROM text_uuid_exactonly_view WHERE message indexquery 'processing'")
        .collect()
      textResults.length should be > 0

      // Exact phrase query for a specific UUID should return exactly 1 row (UUID preserved)
      val uuidResults = spark
        .sql(
          """SELECT * FROM text_uuid_exactonly_view WHERE message indexquery '"a1b2c3d4-e5f6-7890-abcd-ef1234567890"'"""
        )
        .collect()
      uuidResults.length shouldBe 1
    }
  }

  test("text_uuid_strip mode should index text but strip UUIDs") {
    withIcebergTable("text_uuid_strip_ice") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val row = syncIceberg("text_uuid_strip_ice", indexPath, "'message':'text_uuid_strip'")
      row.getString(2) shouldBe "success"

      val companionDf = readCompanion(indexPath)
      companionDf.createOrReplaceTempView("text_uuid_strip_view")

      // Text search for "processing" should still return rows
      val textResults = spark
        .sql("SELECT * FROM text_uuid_strip_view WHERE message indexquery 'processing'")
        .collect()
      textResults.length should be > 0

      // Exact phrase query for a specific UUID should return 0 rows (UUID stripped)
      val uuidResults = spark
        .sql(
          """SELECT * FROM text_uuid_strip_view WHERE message indexquery '"a1b2c3d4-e5f6-7890-abcd-ef1234567890"'"""
        )
        .collect()
      uuidResults.length shouldBe 0
    }
  }

  test("multiple indexing modes on Iceberg companion") {
    withIcebergTable("multi_mode_ice") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, icebergSchema).create()
      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val row = syncIceberg(
        "multi_mode_ice",
        indexPath,
        "'trace_id':'exact_only', 'message':'text_uuid_exactonly'"
      )
      row.getString(2) shouldBe "success"

      val companionDf = readCompanion(indexPath)

      // Verify exact_only mode works on trace_id
      val exactFiltered = companionDf.filter(
        companionDf("trace_id") === "b2c3d4e5-f6a7-8901-bcde-f12345678901"
      ).collect()
      exactFiltered.length shouldBe 1

      // Verify text_uuid_exactonly mode works on message
      companionDf.createOrReplaceTempView("multi_mode_view")
      val textResults = spark
        .sql("SELECT * FROM multi_mode_view WHERE message indexquery 'processing'")
        .collect()
      textResults.length should be > 0
    }
  }
}
