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

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.TransactionLogFactory

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Extended WHERE clause predicate tests for Iceberg companion BUILD.
 *
 * Covers predicates not tested by IcebergDistributedSyncTest (which covers =, IN, !=):
 * range filters (>=, >), compound AND, OR, and BETWEEN-equivalent ranges.
 * Uses a DUAL-partitioned Iceberg table (region + year) with 12 rows across 6 partition combinations.
 */
class IcebergWhereClauseExtendedTest
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
      .appName("IcebergWhereClauseExtendedTest")
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
      StructField("region", StringType, nullable = false),
      StructField("year", StringType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get()),
    Types.NestedField.required(4, "region", Types.StringType.get()),
    Types.NestedField.required(5, "year", Types.StringType.get())
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /** Write parquet rows for a specific partition and register via DataFiles with withPartitionPath. Returns file paths. */
  private def appendPartition(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rootDir: File,
    batchId: Int,
    region: String,
    year: String,
    rows: Seq[Row]
  ): Seq[String] = {
    val parquetDir = new File(rootDir, s"parquet-data/batch-$batchId-$region-$year").getAbsolutePath
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
          .withPartitionPath(s"region=$region/year=$year")
          .build()
      )
      path
    }
    appendOp.commit()
    paths.toSeq
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
   * Sets up a dual-partitioned Iceberg table with 6 partition combinations
   * (3 regions x 2 years), each with 2 rows (12 rows total).
   * Handles Iceberg spark config cleanup in finally blocks.
   */
  private def withDualPartitionedTable(tableName: String)(
    f: (EmbeddedIcebergRestServer, TableIdentifier, File, String) => Unit
  ): Unit = {
    val root = Files.createTempDirectory("iceberg-where-extended").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()
      val ns = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, Collections.emptyMap())

      val spec = PartitionSpec.builderFor(icebergSchema).identity("region").identity("year").build()
      server.catalog.buildTable(tableId, icebergSchema).withPartitionSpec(spec).create()

      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Seed 6 partition combinations (3 regions x 2 years), 2 rows each = 12 rows total
      var batchId = 0

      batchId += 1
      appendPartition(server, tableId, root, batchId, "us-east", "2023",
        Seq(Row(1L, "alice", 85.0, "us-east", "2023"), Row(2L, "bob", 90.0, "us-east", "2023")))

      batchId += 1
      appendPartition(server, tableId, root, batchId, "us-east", "2024",
        Seq(Row(3L, "carol", 75.0, "us-east", "2024"), Row(4L, "dave", 80.0, "us-east", "2024")))

      batchId += 1
      appendPartition(server, tableId, root, batchId, "us-west", "2023",
        Seq(Row(5L, "eve", 95.0, "us-west", "2023"), Row(6L, "frank", 70.0, "us-west", "2023")))

      batchId += 1
      appendPartition(server, tableId, root, batchId, "us-west", "2024",
        Seq(Row(7L, "grace", 88.0, "us-west", "2024"), Row(8L, "heidi", 92.0, "us-west", "2024")))

      batchId += 1
      appendPartition(server, tableId, root, batchId, "eu-west", "2023",
        Seq(Row(9L, "ivan", 78.0, "eu-west", "2023"), Row(10L, "judy", 83.0, "eu-west", "2023")))

      batchId += 1
      appendPartition(server, tableId, root, batchId, "eu-west", "2024",
        Seq(Row(11L, "karl", 91.0, "eu-west", "2024"), Row(12L, "lisa", 87.0, "eu-west", "2024")))

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
  //  WHERE Clause Extended Predicate Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("WHERE with range filter on string partition") {
    withDualPartitionedTable("where_gte") { (_, _, _, indexPath) =>
      // Lexicographic ordering: eu-west < us-east < us-west
      // region >= 'us-east' should include us-east and us-west, but NOT eu-west
      val row = syncIceberg("where_gte", indexPath, "WHERE region >= 'us-east'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("us-west")
        regionVals should not contain "eu-west"
      } finally txLog.close()
    }
  }

  test("WHERE with compound AND filter") {
    withDualPartitionedTable("where_and") { (_, _, _, indexPath) =>
      // region = 'us-east' AND year = '2024' should yield only that single partition combination
      val row = syncIceberg("where_and", indexPath, "WHERE region = 'us-east' AND year = '2024'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        // Every split should have region=us-east AND year=2024
        files.foreach { f =>
          f.partitionValues.get("region") shouldBe Some("us-east")
          f.partitionValues.get("year") shouldBe Some("2024")
        }
      } finally txLog.close()
    }
  }

  test("WHERE with OR filter") {
    withDualPartitionedTable("where_or") { (_, _, _, indexPath) =>
      // region = 'us-east' OR region = 'eu-west' should include both but NOT us-west
      val row = syncIceberg("where_or", indexPath, "WHERE region = 'us-east' OR region = 'eu-west'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("eu-west")
        regionVals should not contain "us-west"
      } finally txLog.close()
    }
  }

  test("WHERE with strict greater-than") {
    withDualPartitionedTable("where_gt") { (_, _, _, indexPath) =>
      // Lexicographic ordering: eu-west < us-east < us-west
      // region > 'eu-west' should include us-east and us-west, but NOT eu-west
      val row = syncIceberg("where_gt", indexPath, "WHERE region > 'eu-west'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        val regionVals = files.flatMap(_.partitionValues.get("region")).toSet
        regionVals should contain("us-east")
        regionVals should contain("us-west")
        regionVals should not contain "eu-west"
      } finally txLog.close()
    }
  }

  test("WHERE with BETWEEN-equivalent range") {
    withDualPartitionedTable("where_between") { (_, _, _, indexPath) =>
      // year >= '2024' AND year <= '2024' is equivalent to year = '2024'
      // Should only include partitions with year=2024
      val row = syncIceberg("where_between", indexPath, "WHERE year >= '2024' AND year <= '2024'")
      row.getString(2) shouldBe "success"

      val txLog = openTxLog(indexPath)
      try {
        val files = txLog.listFiles()
        files should not be empty
        val yearVals = files.flatMap(_.partitionValues.get("year")).toSet
        yearVals shouldBe Set("2024")
      } finally txLog.close()
    }
  }
}
