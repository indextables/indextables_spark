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
 * Tests that BUILD INDEXTABLES COMPANION FOR ICEBERG correctly handles Iceberg tables that have undergone schema
 * evolution operations: adding columns, dropping columns, renaming columns.
 *
 * Uses the Iceberg Java API for schema evolution against an EmbeddedIcebergRestServer. No Docker required.
 */
class IcebergSchemaEvolutionSyncTest
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
      .appName("IcebergSchemaEvolutionSyncTest")
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

  // -- Initial Schema (id: Long, name: String, score: Double) ----------------

  private val initialSparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val initialIcebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get())
  )

  // -- Initial Data (5 rows) -------------------------------------------------

  private val initialRows: Seq[Row] = Seq(
    Row(1L, "alice", 85.0),
    Row(2L, "bob", 90.5),
    Row(3L, "carol", 78.2),
    Row(4L, "dave", 92.1),
    Row(5L, "eve", 88.3)
  )

  // -- Helpers ----------------------------------------------------------------

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
    val root         = Files.createTempDirectory("iceberg-schema-evo-sync").toFile
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
  //  Schema Evolution Tests
  // ===========================================================================

  test("sync after adding column to Iceberg table") {
    withIcebergTable("add_col") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, initialIcebergSchema).create()

      // Initial data with 3 columns
      appendSnapshot(server, tableId, initialRows, initialSparkSchema, root, 1)

      // Build companion from initial schema
      val row1 = syncIceberg("add_col", indexPath)
      row1.getString(2) shouldBe "success"

      // Evolve schema: add "department" column
      val table = server.catalog.loadTable(tableId)
      table.updateSchema().addColumn("department", Types.StringType.get()).commit()

      // Append new data with the evolved 4-column schema
      val evolvedSparkSchema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("score", DoubleType, nullable = false),
          StructField("department", StringType, nullable = true)
        )
      )
      val newRows = Seq(
        Row(6L, "frank", 77.0, "engineering"),
        Row(7L, "grace", 91.5, "marketing"),
        Row(8L, "heidi", 84.3, "engineering")
      )
      appendSnapshot(server, tableId, newRows, evolvedSparkSchema, root, 2)
      flushCaches()

      // Re-sync should succeed after schema evolution
      val row2 = syncIceberg("add_col", indexPath)
      row2.getString(2) shouldBe "success"

      val companion = readCompanion(indexPath)
      companion.count() shouldBe 8 // 5 original + 3 new

      // Verify the new column is queryable
      companion.filter("department = 'engineering'").count() shouldBe 1
      companion.filter("department = 'marketing'").count() shouldBe 1
    }
  }

  test("sync after dropping column from Iceberg table") {
    withIcebergTable("drop_col") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, initialIcebergSchema).create()

      // Initial data with 3 columns
      appendSnapshot(server, tableId, initialRows, initialSparkSchema, root, 1)

      // Build companion from initial schema
      val row1 = syncIceberg("drop_col", indexPath)
      row1.getString(2) shouldBe "success"

      // Evolve schema: drop "score" column
      val table = server.catalog.loadTable(tableId)
      table.updateSchema().deleteColumn("score").commit()

      // Append new data WITHOUT the score column
      val evolvedSparkSchema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType, nullable = false)
        )
      )
      val newRows = Seq(
        Row(6L, "frank"),
        Row(7L, "grace"),
        Row(8L, "heidi")
      )
      appendSnapshot(server, tableId, newRows, evolvedSparkSchema, root, 2)
      flushCaches()

      // Re-sync should succeed after column drop
      val row2 = syncIceberg("drop_col", indexPath)
      row2.getString(2) shouldBe "success"

      val companion = readCompanion(indexPath)
      companion.count() shouldBe 8 // 5 original + 3 new

      // Verify remaining columns are still queryable
      companion.filter("name = 'alice'").count() shouldBe 1
      companion.filter("name = 'frank'").count() shouldBe 1
    }
  }

  test("companion should index new column after schema evolution") {
    withIcebergTable("index_new_col") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, initialIcebergSchema).create()

      // Evolve schema first: add "department" column
      val table = server.catalog.loadTable(tableId)
      table.updateSchema().addColumn("department", Types.StringType.get()).commit()

      // Append data with department values using evolved schema
      val evolvedSparkSchema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("score", DoubleType, nullable = false),
          StructField("department", StringType, nullable = true)
        )
      )
      val rowsWithDept = Seq(
        Row(1L, "alice", 85.0, "engineering"),
        Row(2L, "bob", 90.5, "marketing"),
        Row(3L, "carol", 78.2, "engineering"),
        Row(4L, "dave", 92.1, "sales"),
        Row(5L, "eve", 88.3, "marketing")
      )
      appendSnapshot(server, tableId, rowsWithDept, evolvedSparkSchema, root, 1)

      // Sync
      val row = syncIceberg("index_new_col", indexPath)
      row.getString(2) shouldBe "success"

      // Read companion and verify department column exists with correct values
      val companion = readCompanion(indexPath)
      companion.columns should contain("department")
      companion.count() shouldBe 5

      // Verify department values via count-based verification
      val engCount = companion.filter("department = 'engineering'").count()
      engCount shouldBe 2

      val mktCount = companion.filter("department = 'marketing'").count()
      mktCount shouldBe 2
    }
  }

  test("sync should handle renamed column") {
    withIcebergTable("rename_col") { (server, tableId, root, indexPath) =>
      server.catalog.buildTable(tableId, initialIcebergSchema).create()

      // Initial data with 3 columns
      appendSnapshot(server, tableId, initialRows, initialSparkSchema, root, 1)

      // Build companion from initial schema
      val row1 = syncIceberg("rename_col", indexPath)
      row1.getString(2) shouldBe "success"

      // Evolve schema: rename "name" to "full_name"
      val table = server.catalog.loadTable(tableId)
      table.updateSchema().renameColumn("name", "full_name").commit()

      // Append data using the new schema with renamed column
      val evolvedSparkSchema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("full_name", StringType, nullable = false),
          StructField("score", DoubleType, nullable = false)
        )
      )
      val newRows = Seq(
        Row(6L, "frank", 77.0),
        Row(7L, "grace", 91.5),
        Row(8L, "heidi", 84.3)
      )
      appendSnapshot(server, tableId, newRows, evolvedSparkSchema, root, 2)
      flushCaches()

      // Re-sync should succeed after column rename
      val row2 = syncIceberg("rename_col", indexPath)
      row2.getString(2) shouldBe "success"

      val companion = readCompanion(indexPath)
      companion.count() shouldBe 8 // 5 original + 3 new

      // Verify the renamed column is queryable
      companion.filter("full_name = 'frank'").count() shouldBe 1
      companion.filter("full_name = 'alice'").count() shouldBe 1
    }
  }
}
