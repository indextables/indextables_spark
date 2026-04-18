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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.transaction.TransactionLogFactory

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec, Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

/**
 * Iceberg-format implementation of the companion INCLUDE/EXCLUDE COLUMNS test suite.
 *
 * <p>Uses {@link EmbeddedIcebergRestServer} for Docker-free testing. All table creation and data
 * ingestion happen via the Iceberg Java API against an in-process REST catalog backed by the local
 * filesystem.
 */
class IcebergCompanionColumnsTest extends CompanionColumnsTestBase {

  override def formatName: String = "iceberg"

  private var _spark: SparkSession = _
  override def spark: SparkSession = _spark

  private var server: EmbeddedIcebergRestServer = _
  private var warehouseDir: String = _
  private var batchCounter: AtomicInteger = _

  // ─────────────────────────────────────────────────────────────────────
  //  Lifecycle
  // ─────────────────────────────────────────────────────────────────────

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    warehouseDir = Files.createTempDirectory("iceberg-warehouse").toString

    server = new EmbeddedIcebergRestServer(warehouseDir)
    server.catalog.createNamespace(Namespace.of("default"), java.util.Collections.emptyMap())

    _spark = SparkSession
      .builder()
      .appName("IcebergCompanionColumnsTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.iceberg.catalogType", "rest")
      .config("spark.indextables.iceberg.uri", server.restUri)
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    batchCounter = new AtomicInteger(0)

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit = {
    cleanupSharedTables()
    if (server != null) server.close()
    if (_spark != null) _spark.stop()
    if (warehouseDir != null) deleteRecursively(new File(warehouseDir))
  }

  // ─────────────────────────────────────────────────────────────────────
  //  Abstract method implementations
  // ─────────────────────────────────────────────────────────────────────

  override def newTableId(tempDir: String, name: String): String =
    s"default.test_${name}_${java.util.UUID.randomUUID().toString.take(8)}"

  override def createPartitionedTable(tableId: String): Unit = {
    val icebergSchema = new IcebergSchema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "name", Types.StringType.get()),
      Types.NestedField.optional(3, "score", Types.DoubleType.get()),
      Types.NestedField.optional(4, "region", Types.StringType.get())
    )
    val spec = PartitionSpec.builderFor(icebergSchema).identity("region").build()

    val icebergTableId = parseTableId(tableId)
    server.catalog.buildTable(icebergTableId, icebergSchema).withPartitionSpec(spec).create()

    val schema = partitionedSparkSchema
    val regionAData = Seq(
      Row(1, "alice", 100.0, "region_a"),
      Row(3, "charlie", 300.75, "region_a")
    )
    val regionBData = Seq(
      Row(2, "bob", 200.5, "region_b")
    )
    appendPartitionedData(tableId, schema, regionAData, "region=region_a")
    appendPartitionedData(tableId, schema, regionBData, "region=region_b")
  }

  override def createSimpleTable(tableId: String, schema: StructType, data: Seq[Row]): Unit = {
    val icebergSchema = sparkToIcebergSchema(schema)
    val icebergTableId = parseTableId(tableId)
    server.catalog.buildTable(icebergTableId, icebergSchema).create()
    appendIcebergData(tableId, schema, data)
  }

  override def recreateTable(tableId: String, schema: StructType, data: Seq[Row]): Unit = {
    val icebergTableId = parseTableId(tableId)
    server.catalog.dropTable(icebergTableId, true)
    createSimpleTable(tableId, schema, data)
  }

  override def appendData(tableId: String, schema: StructType, data: Seq[Row]): Unit =
    appendIcebergData(tableId, schema, data)

  override def buildCompanionSql(tableId: String, clauses: String, indexPath: String): String = {
    val c = if (clauses.nonEmpty) s" $clauses" else ""
    s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$tableId'$c AT LOCATION '$indexPath'"
  }

  // ─────────────────────────────────────────────────────────────────────
  //  Helpers
  // ─────────────────────────────────────────────────────────────────────

  private def parseTableId(tableId: String): TableIdentifier = {
    val parts = tableId.split("\\.", 2)
    TableIdentifier.of(Namespace.of(parts(0)), parts(1))
  }

  private val partitionedSparkSchema: StructType = StructType(Seq(
    StructField("id", IntegerType), StructField("name", StringType),
    StructField("score", DoubleType), StructField("region", StringType)
  ))

  private def sparkToIcebergSchema(sparkSchema: StructType): IcebergSchema = {
    val fields = sparkSchema.fields.zipWithIndex.map { case (f, idx) =>
      val icebergType = f.dataType match {
        case IntegerType  => Types.IntegerType.get()
        case LongType     => Types.LongType.get()
        case DoubleType   => Types.DoubleType.get()
        case FloatType    => Types.FloatType.get()
        case StringType   => Types.StringType.get()
        case BooleanType  => Types.BooleanType.get()
        case BinaryType   => Types.BinaryType.get()
        case dt: DecimalType => Types.DecimalType.of(dt.precision, dt.scale)
        case other => throw new UnsupportedOperationException(s"Unsupported Spark type: $other")
      }
      Types.NestedField.optional(idx + 1, f.name, icebergType)
    }
    new IcebergSchema(fields: _*)
  }

  /**
   * Write rows as Parquet via Spark, then register the resulting files as a new Iceberg snapshot.
   * Follows the same pattern as [[StreamingCompanionIcebergEndToEndTest.appendIcebergSnapshot]].
   */
  private def appendIcebergData(tableId: String, schema: StructType, data: Seq[Row]): Unit = {
    val batchId = batchCounter.incrementAndGet()
    val batchDir = s"$warehouseDir/parquet-data/batch-$batchId"

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .coalesce(1).write.parquet(s"file://$batchDir")

    val parquetFiles = new File(batchDir).listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)
    require(parquetFiles.nonEmpty, s"No Parquet files written to $batchDir")

    val icebergTableId = parseTableId(tableId)
    val table = server.catalog.loadTable(icebergTableId)
    val appendOp = table.newAppend()
    parquetFiles.foreach { f =>
      appendOp.appendFile(
        DataFiles.builder(table.spec())
          .withPath(s"file://${f.getAbsolutePath}")
          .withFileSizeInBytes(f.length())
          .withRecordCount(data.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .build()
      )
    }
    appendOp.commit()
  }

  // ─────────────────────────────────────────────────────────────────────
  //  Iceberg-specific tests (not in shared base trait)
  // ─────────────────────────────────────────────────────────────────────

  test("explicit CATALOG TYPE WAREHOUSE SQL clause builds companion end-to-end") {
    // Scott noted that all other Iceberg tests configure the catalog via SparkConf,
    // so the grammar-to-runtime path for CATALOG '...' TYPE '...' WAREHOUSE '...'
    // SQL syntax is never exercised. This test does exactly that: it issues a
    // BUILD COMPANION SQL with an explicit catalog clause and verifies both
    // the build succeeds AND the SQL-supplied catalog name flows through to
    // the persisted companion metadata.
    withTempPath { tempDir =>
      val indexPath = new File(tempDir, "explicit_catalog_idx").getAbsolutePath
      val tableId = newTableId(tempDir, "explicit_catalog")
      createSimpleTable(tableId, sharedTableSchema, sharedTableData)

      // The SparkConf provides URI + catalogType=rest already, but the
      // catalogName 'my_explicit_catalog' is supplied ONLY via the SQL clause
      // — if it shows up in the persisted metadata, the SQL parse-and-route
      // path is provably exercised (the SparkConf fallback would write 'default').
      //
      // Grammar order (IndexTables4SparkSqlBase.g4): CATALOG → WAREHOUSE → INCLUDE COLUMNS.
      val sql = s"""BUILD INDEXTABLES COMPANION FOR ICEBERG '$tableId'
                   |  CATALOG 'my_explicit_catalog' TYPE 'rest'
                   |  WAREHOUSE 'file://$warehouseDir'
                   |  INCLUDE COLUMNS ('id', 'name')
                   |  AT LOCATION '$indexPath'""".stripMargin

      val result = spark.sql(sql).collect()
      withClue(s"build status: ${result(0).getString(2)}, message: ${result(0).getString(10)}") {
        result(0).getString(2) shouldBe "success"
      }

      // Verify the catalog name from the SQL clause was persisted in the
      // companion metadata (not the default "default" fallback).
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
      )
      try {
        txLog.invalidateCache()
        val config = txLog.getMetadata().configuration
        config.get("indextables.companion.icebergCatalog") shouldBe Some("my_explicit_catalog")
      } finally
        txLog.close()

      // Sanity: the companion is queryable.
      val df = readCompanion(indexPath)
      df.filter(col("name") === "alice").count() shouldBe 1L
      df.count() shouldBe 3L
    }
  }

  test("incremental sync restores CATALOG / WAREHOUSE from companion metadata") {
    // Regression for the previously-missing restore path: the initial sync
    // provides CATALOG/TYPE/WAREHOUSE via the SQL clause, then a subsequent
    // incremental sync omits them entirely. Before the Item 7 fix, the
    // second sync fell back to "default" catalog and silently clobbered the
    // stored icebergCatalog metadata key with "default". After the fix, the
    // stored value survives and the sync succeeds using the restored config.
    withTempPath { tempDir =>
      val indexPath = new File(tempDir, "catalog_restore_idx").getAbsolutePath
      val tableId = newTableId(tempDir, "catalog_restore")
      createSimpleTable(tableId, sharedTableSchema, sharedTableData)

      // Initial sync with explicit CATALOG/WAREHOUSE and an unusual catalog
      // name that could not come from the SparkConf fallback ("rest" + URI
      // are the only SparkConf values — catalogName is SQL-only).
      val initialSql = s"""BUILD INDEXTABLES COMPANION FOR ICEBERG '$tableId'
                          |  CATALOG 'restore_test_catalog' TYPE 'rest'
                          |  WAREHOUSE 'file://$warehouseDir'
                          |  INCLUDE COLUMNS ('id', 'name')
                          |  AT LOCATION '$indexPath'""".stripMargin
      val initialResult = spark.sql(initialSql).collect()
      withClue(s"initial build: ${initialResult(0).getString(2)}, ${initialResult(0).getString(10)}") {
        initialResult(0).getString(2) shouldBe "success"
      }

      // Append more data so the incremental sync has something to do.
      appendData(tableId, sharedTableSchema, Seq(
        Row(4, "dave", 400.0, 4000L, true, "more data", "10.0.0.4", "cat_c",
          new java.math.BigDecimal("400.50"), new java.math.BigDecimal("400.50"),
          new java.math.BigDecimal("400.50000"), 400, 4, "drop_d")
      ))

      // Incremental sync WITHOUT re-specifying CATALOG/TYPE/WAREHOUSE —
      // these must be restored from companion metadata.
      val incrementalSql = s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$tableId' AT LOCATION '$indexPath'"
      val incrementalResult = spark.sql(incrementalSql).collect()
      withClue(s"incremental build: ${incrementalResult(0).getString(2)}, ${incrementalResult(0).getString(10)}") {
        incrementalResult(0).getString(2) shouldBe "success"
      }

      // Verify the stored catalog name was NOT clobbered with "default".
      val txLog = TransactionLogFactory.create(
        new Path(indexPath), spark,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
      )
      try {
        txLog.invalidateCache()
        val config = txLog.getMetadata().configuration
        config.get("indextables.companion.icebergCatalog") shouldBe Some("restore_test_catalog")
        config.get("indextables.companion.icebergCatalogType") shouldBe Some("rest")
        config.get("indextables.companion.icebergWarehouse") shouldBe Some(s"file://$warehouseDir")
      } finally
        txLog.close()

      // Companion should have indexed the new row too.
      readCompanion(indexPath).count() shouldBe 4L
    }
  }

  /**
   * Write rows as Parquet via Spark, then register them as an Iceberg snapshot with a specific
   * partition path. Used for identity-partitioned tables where each DataFile must declare its
   * partition value.
   */
  private def appendPartitionedData(
    tableId: String,
    schema: StructType,
    data: Seq[Row],
    partitionPath: String
  ): Unit = {
    val batchId = batchCounter.incrementAndGet()
    val batchDir = s"$warehouseDir/parquet-data/batch-$batchId"

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .coalesce(1).write.parquet(s"file://$batchDir")

    val parquetFiles = new File(batchDir).listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)
    require(parquetFiles.nonEmpty, s"No Parquet files written to $batchDir")

    val icebergTableId = parseTableId(tableId)
    val table = server.catalog.loadTable(icebergTableId)
    val appendOp = table.newAppend()
    parquetFiles.foreach { f =>
      appendOp.appendFile(
        DataFiles.builder(table.spec())
          .withPath(s"file://${f.getAbsolutePath}")
          .withFileSizeInBytes(f.length())
          .withRecordCount(data.size.toLong)
          .withFormat(FileFormat.PARQUET)
          .withPartitionPath(partitionPath)
          .build()
      )
    }
    appendOp.commit()
  }
}
