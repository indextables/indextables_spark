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
import java.sql.{Date, Timestamp}
import java.util.Collections

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Round-trip validation tests for Iceberg companion indexes.
 *
 * Each test writes known data to an Iceberg table (via Java API + parquet), builds a companion index, reads ALL rows
 * back through the companion, and asserts that every column of every row matches the original source. This validates
 * data type fidelity across the Iceberg -> companion -> read-back pipeline.
 */
class IcebergRoundTripTest
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
      .appName("IcebergRoundTripTest")
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

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /**
   * Compare two sets of rows column-by-column, ignoring row order. Rows are keyed by the given column name for
   * deterministic matching.
   */
  private def assertRowsMatch(
    source: Array[Row],
    companion: Array[Row],
    schema: StructType,
    keyColumn: String
  ): Unit = {
    source.length shouldBe companion.length

    val keyIdx = schema.fieldIndex(keyColumn)

    val sourceByKey    = source.map(r => String.valueOf(r.get(keyIdx)) -> r).toMap
    val companionByKey = companion.map(r => String.valueOf(r.get(keyIdx)) -> r).toMap

    sourceByKey.keySet shouldBe companionByKey.keySet

    for ((key, sourceRow) <- sourceByKey) {
      val companionRow = companionByKey(key)
      for (field <- schema.fields) {
        val idx          = schema.fieldIndex(field.name)
        val sourceVal    = sourceRow.get(idx)
        val companionVal = companionRow.get(idx)

        field.dataType match {
          case FloatType =>
            if (sourceVal == null) assert(companionVal == null)
            else companionVal.asInstanceOf[Float] shouldBe sourceVal.asInstanceOf[Float] +- 0.001f
          case DoubleType =>
            if (sourceVal == null) assert(companionVal == null)
            else companionVal.asInstanceOf[Double] shouldBe sourceVal.asInstanceOf[Double] +- 0.001
          case _ =>
            withClue(s"Mismatch at key=$key, column=${field.name}: ") {
              companionVal shouldBe sourceVal
            }
        }
      }
    }
  }

  /** Write parquet rows and register as an Iceberg snapshot. Returns paths of registered data files. */
  private def appendSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    schema: StructType,
    rootDir: File,
    batchId: Int,
    partitionPath: Option[String] = None
  ): Seq[String] = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId").getAbsolutePath
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.coalesce(1).write.parquet(s"file://$parquetDir")

    val parquetFiles = new File(parquetDir)
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    val paths = parquetFiles.map { pf =>
      val path = s"file://${pf.getAbsolutePath}"
      val builder = DataFiles
        .builder(table.spec())
        .withPath(path)
        .withFileSizeInBytes(pf.length())
        .withRecordCount(rows.size.toLong)
        .withFormat(FileFormat.PARQUET)
      partitionPath.foreach(builder.withPartitionPath)
      appendOp.appendFile(builder.build())
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

  private def readCompanion(indexPath: String): DataFrame = {
    flushCaches()
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "10000")
      .option("spark.indextables.read.columnar.enabled", "false")
      .load(indexPath)
  }

  /** Sets up an Iceberg table with EmbeddedIcebergRestServer and runs the test body. */
  private def withIcebergTable(
    tableName: String,
    icebergSchema: IcebergSchema,
    partitionSpec: Option[PartitionSpec] = None
  )(
    f: (EmbeddedIcebergRestServer, TableIdentifier, File, String) => Unit
  ): Unit = {
    val root         = Files.createTempDirectory("iceberg-roundtrip").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, Collections.emptyMap())

      val tableBuilder = server.catalog.buildTable(tableId, icebergSchema)
      partitionSpec.foreach(tableBuilder.withPartitionSpec)
      tableBuilder.create()

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

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: scalar columns
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: scalar columns (Int, Long, Double, String, Boolean)") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "big_id", Types.LongType.get()),
      Types.NestedField.required(4, "score", Types.DoubleType.get()),
      Types.NestedField.required(5, "active", Types.BooleanType.get()),
      Types.NestedField.required(6, "label", Types.StringType.get())
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("big_id", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("label", StringType, nullable = false)
    ))

    withIcebergTable("scalar_cols", icebergSchema) { (server, tableId, root, indexPath) =>
      val rows = Seq(
        Row(1, "alice",   100L, 1.5,   true,  "alpha"),
        Row(2, "bob",     200L, 2.7,   false, "beta"),
        Row(3, "charlie", 300L, 3.14,  true,  "gamma"),
        Row(4, "dave",    400L, 0.0,   false, "delta"),
        Row(5, "eve",     500L, 99.9,  true,  "epsilon")
      )

      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val syncRow = syncIceberg("scalar_cols", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      assertRowsMatch(rows.toArray, companionRows, sparkSchema, "id")
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: Date and Timestamp columns
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: Date and Timestamp columns") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "event_date", Types.DateType.get()),
      Types.NestedField.required(3, "event_ts", Types.TimestampType.withZone())
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("event_date", DateType, nullable = false),
      StructField("event_ts", TimestampType, nullable = false)
    ))

    withIcebergTable("date_ts_cols", icebergSchema) { (server, tableId, root, indexPath) =>
      val rows = Seq(
        Row(1, Date.valueOf("2024-01-15"), Timestamp.valueOf("2024-01-15 10:30:00")),
        Row(2, Date.valueOf("2024-06-20"), Timestamp.valueOf("2024-06-20 14:00:00")),
        Row(3, Date.valueOf("2024-12-25"), Timestamp.valueOf("2024-12-25 23:59:59"))
      )

      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val syncRow = syncIceberg("date_ts_cols", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      assertRowsMatch(rows.toArray, companionRows, sparkSchema, "id")
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: Array column
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: Array column") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "tags", Types.ListType.ofRequired(10, Types.StringType.get()))
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("tags", ArrayType(StringType, containsNull = false), nullable = false)
    ))

    withIcebergTable("array_col", icebergSchema) { (server, tableId, root, indexPath) =>
      val rows = Seq(
        Row(1, "alice",   Seq("scala", "java")),
        Row(2, "bob",     Seq("python")),
        Row(3, "charlie", Seq("rust", "go", "c"))
      )

      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val syncRow = syncIceberg("array_col", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      companionRows.length shouldBe 3

      val sourceById    = rows.map(r => r.getInt(0) -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id name: ") {
          comp.getAs[String]("name") shouldBe src.getString(1)
        }
        withClue(s"id=$id tags: ") {
          comp.getAs[Seq[String]]("tags") shouldBe src.getAs[Seq[String]](2)
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: Struct column
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: Struct column") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "info", Types.StructType.of(
        Types.NestedField.required(10, "first_name", Types.StringType.get()),
        Types.NestedField.required(11, "age", Types.IntegerType.get())
      ))
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("first_name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )), nullable = false)
    ))

    withIcebergTable("struct_col", icebergSchema) { (server, tableId, root, indexPath) =>
      val rows = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35))
      )

      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val syncRow = syncIceberg("struct_col", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      companionRows.length shouldBe 3

      val sourceById    = rows.map(r => r.getInt(0) -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp     = companionById(id)
        val srcInfo  = src.getAs[Row](1)
        val compInfo = comp.getAs[Row]("info")
        withClue(s"id=$id first_name: ") {
          compInfo.getAs[String]("first_name") shouldBe srcInfo.getString(0)
        }
        withClue(s"id=$id age: ") {
          compInfo.getAs[Int]("age") shouldBe srcInfo.getInt(1)
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: Map column
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: Map column") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "attrs", Types.MapType.ofRequired(10, 11, Types.StringType.get(), Types.StringType.get()))
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("attrs", MapType(StringType, StringType, valueContainsNull = false), nullable = false)
    ))

    withIcebergTable("map_col", icebergSchema) { (server, tableId, root, indexPath) =>
      val rows = Seq(
        Row(1, "alice",   Map("color" -> "red", "size" -> "small")),
        Row(2, "bob",     Map("color" -> "blue")),
        Row(3, "charlie", Map("color" -> "green", "size" -> "large", "weight" -> "heavy"))
      )

      appendSnapshot(server, tableId, rows, sparkSchema, root, 1)

      val syncRow = syncIceberg("map_col", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      companionRows.length shouldBe 3

      val sourceById    = rows.map(r => r.getInt(0) -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id name: ") {
          comp.getAs[String]("name") shouldBe src.getString(1)
        }
        withClue(s"id=$id attrs: ") {
          comp.getAs[Map[String, String]]("attrs") shouldBe src.getAs[Map[String, String]](2)
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Round-trip: partitioned table with mixed types
  // ═══════════════════════════════════════════════════════════════════════════

  test("round-trip: partitioned table with mixed types") {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "score", Types.DoubleType.get()),
      Types.NestedField.required(4, "active", Types.BooleanType.get()),
      Types.NestedField.required(5, "region", Types.StringType.get())
    )

    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("region", StringType, nullable = false)
    ))

    val partitionSpec = PartitionSpec.builderFor(icebergSchema).identity("region").build()

    withIcebergTable("partitioned_mixed", icebergSchema, Some(partitionSpec)) { (server, tableId, root, indexPath) =>
      // us-east rows
      val usEastRows = Seq(
        Row(1, "alice", 85.5, true,  "us-east"),
        Row(2, "bob",   92.0, false, "us-east")
      )
      appendSnapshot(server, tableId, usEastRows, sparkSchema, root, 1, partitionPath = Some("region=us-east"))

      // us-west rows
      val usWestRows = Seq(
        Row(3, "charlie", 78.3,  true,  "us-west"),
        Row(4, "dave",    95.1,  false, "us-west")
      )
      appendSnapshot(server, tableId, usWestRows, sparkSchema, root, 2, partitionPath = Some("region=us-west"))

      // eu-west rows
      val euWestRows = Seq(
        Row(5, "eve",   88.8, true,  "eu-west"),
        Row(6, "frank", 70.0, false, "eu-west")
      )
      appendSnapshot(server, tableId, euWestRows, sparkSchema, root, 3, partitionPath = Some("region=eu-west"))

      val syncRow = syncIceberg("partitioned_mixed", indexPath)
      syncRow.getString(2) shouldBe "success"

      val companionDf   = readCompanion(indexPath)
      val companionRows = companionDf.collect()

      val allSourceRows = (usEastRows ++ usWestRows ++ euWestRows).toArray

      assertRowsMatch(allSourceRows, companionRows, sparkSchema, "id")
    }
  }
}
