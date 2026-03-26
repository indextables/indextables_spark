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
 * Tests Iceberg partition spec evolution with companion sync.
 *
 * Verifies that BUILD INDEXTABLES COMPANION FOR ICEBERG correctly handles tables that evolve from unpartitioned to
 * partitioned, add additional partition columns, or contain files from mixed partition specs.
 *
 * Uses the Iceberg Java API for partition evolution against an EmbeddedIcebergRestServer. No Docker required.
 */
class IcebergPartitionEvolutionTest
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
      .appName("IcebergPartitionEvolutionTest")
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

  // -- Schema (4-field: id, name, score, region) ------------------------------

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

  // -- Schema (5-field: id, name, score, region, year) for test 2 -------------

  private val sparkSchemaWithYear = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("region", StringType, nullable = false),
      StructField("year", StringType, nullable = false)
    )
  )

  private val icebergSchemaWithYear = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get()),
    Types.NestedField.required(4, "region", Types.StringType.get()),
    Types.NestedField.required(5, "year", Types.StringType.get())
  )

  // -- Helpers ----------------------------------------------------------------

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /** Write parquet rows and register as an unpartitioned Iceberg snapshot. */
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

  /** Write parquet rows and register as a partitioned Iceberg snapshot with a single partition path. */
  private def appendPartitioned(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rootDir: File,
    batchId: Int,
    partitionPath: String,
    rows: Seq[Row],
    schema: StructType
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
          .withPartitionPath(partitionPath)
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
    val root         = Files.createTempDirectory("iceberg-part-evo").toFile
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

  test("companion should handle table evolving from non-partitioned to partitioned") {
    withIcebergTable("unpart_to_part") { (server, tableId, root, indexPath) =>
      // Create unpartitioned table
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Append 3 rows (unpartitioned)
      val unpartRows = Seq(
        Row(1L, "alice", 85.0, "us-east"),
        Row(2L, "bob", 90.5, "us-west"),
        Row(3L, "carol", 78.2, "eu-west")
      )
      appendSnapshot(server, tableId, unpartRows, sparkSchema, root, 1)

      // First sync
      val row1 = syncIceberg("unpart_to_part", indexPath)
      row1.getString(2) shouldBe "success"

      // Evolve partition spec: add "region" as identity partition
      val table = server.catalog.loadTable(tableId)
      table.updateSpec().addField("region").commit()

      // Append 3 more rows WITH partition paths
      appendPartitioned(server, tableId, root, 2, "region=us-east",
        Seq(Row(4L, "dave", 92.1, "us-east"), Row(5L, "eve", 88.3, "us-east")), sparkSchema)
      appendPartitioned(server, tableId, root, 3, "region=us-west",
        Seq(Row(6L, "frank", 77.0, "us-west")), sparkSchema)

      flushCaches()

      // Re-sync
      val row2 = syncIceberg("unpart_to_part", indexPath)
      row2.getString(2) shouldBe "success"

      // Verify all 6 rows are indexed
      val companion = readCompanion(indexPath)
      companion.count() shouldBe 6L

      // Verify partition column "region" is detected in companion splits
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val partCols = txLog.listFiles().flatMap(_.partitionValues.keys).toSet
        partCols should contain("region")
      } finally txLog.close()
    }
  }

  test("companion should handle adding second partition column") {
    withIcebergTable("add_second_part") { (server, tableId, root, indexPath) =>
      // Create table with initial partition on region only (using 5-field schema)
      val spec = PartitionSpec.builderFor(icebergSchemaWithYear).identity("region").build()
      server.catalog.buildTable(tableId, icebergSchemaWithYear).withPartitionSpec(spec).create()

      // Append data partitioned by region
      appendPartitioned(server, tableId, root, 1, "region=us-east",
        Seq(
          Row(1L, "alice", 85.0, "us-east", "2025"),
          Row(2L, "bob", 90.5, "us-east", "2025")
        ), sparkSchemaWithYear)
      appendPartitioned(server, tableId, root, 2, "region=us-west",
        Seq(
          Row(3L, "carol", 78.2, "us-west", "2025")
        ), sparkSchemaWithYear)

      // First sync
      val row1 = syncIceberg("add_second_part", indexPath)
      row1.getString(2) shouldBe "success"

      // Evolve partition spec: add "year" as second partition column
      val table = server.catalog.loadTable(tableId)
      table.updateSpec().addField("year").commit()

      // Append data with both partitions
      appendPartitioned(server, tableId, root, 3, "region=us-east/year=2026",
        Seq(
          Row(4L, "dave", 92.1, "us-east", "2026"),
          Row(5L, "eve", 88.3, "us-east", "2026")
        ), sparkSchemaWithYear)
      appendPartitioned(server, tableId, root, 4, "region=eu-west/year=2026",
        Seq(
          Row(6L, "frank", 77.0, "eu-west", "2026")
        ), sparkSchemaWithYear)

      flushCaches()

      // Re-sync
      val row2 = syncIceberg("add_second_part", indexPath)
      row2.getString(2) shouldBe "success"

      // Verify all 6 rows are indexed
      readCompanion(indexPath).count() shouldBe 6L

      // Verify both partition columns are present in companion splits
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val allPartKeys = txLog.listFiles().flatMap(_.partitionValues.keys).toSet
        allPartKeys should contain("region")
        allPartKeys should contain("year")
      } finally txLog.close()
    }
  }

  test("companion should handle files from mixed partition specs") {
    withIcebergTable("mixed_specs") { (server, tableId, root, indexPath) =>
      // Create unpartitioned table
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Append 3 rows (no partition)
      val unpartRows = Seq(
        Row(1L, "alice", 85.0, "us-east"),
        Row(2L, "bob", 90.5, "us-west"),
        Row(3L, "carol", 78.2, "eu-west")
      )
      appendSnapshot(server, tableId, unpartRows, sparkSchema, root, 1)

      // Evolve spec to add "region" partition
      val table = server.catalog.loadTable(tableId)
      table.updateSpec().addField("region").commit()

      // Append 3 more rows (with region partition)
      appendPartitioned(server, tableId, root, 2, "region=us-east",
        Seq(Row(4L, "dave", 92.1, "us-east"), Row(5L, "eve", 88.3, "us-east")), sparkSchema)
      appendPartitioned(server, tableId, root, 3, "region=eu-west",
        Seq(Row(6L, "frank", 77.0, "eu-west")), sparkSchema)

      // Sync ONCE (covers files from both partition specs)
      val row = syncIceberg("mixed_specs", indexPath)
      row.getString(2) shouldBe "success"

      // Verify all 6 rows indexed
      readCompanion(indexPath).count() shouldBe 6

      // Verify partition values are present for the partitioned files
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(
          Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
        )
      )
      try {
        val regionVals = txLog.listFiles().flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("eu-west")
      } finally txLog.close()
    }
  }
}
