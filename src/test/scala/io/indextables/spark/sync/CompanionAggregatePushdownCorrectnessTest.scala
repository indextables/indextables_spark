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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, max, min, sum}
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for aggregate pushdown correctness on companion tables built from Delta and Iceberg sources.
 *
 * Unlike CompanionAggregateGuardTest (which tests the guard mechanism), this suite verifies that actual computed values
 * (COUNT, SUM, AVG, MIN, MAX) are correct when aggregate pushdown succeeds on companion tables with fast fields
 * enabled.
 *
 * Uses a deterministic 20-row dataset with pre-calculated expected values for all aggregations.
 */
class CompanionAggregatePushdownCorrectnessTest
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
      .appName("CompanionAggregatePushdownCorrectnessTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

  // ── Test data ────────────────────────────────────────────────────────────────
  //
  // 20 rows, 3 departments, 3 regions. All expected values hand-calculated.
  //
  // department  | rows | SUM(score) | MIN(score) | MAX(score) | AVG(score)
  // engineering |   8  |   780.0    |   50.0     |   150.0    |   97.5
  // marketing   |   6  |   510.0    |   60.0     |   120.0    |   85.0
  // sales       |   6  |   510.0    |   55.0     |   130.0    |   85.0
  // ────────────+──────+────────────+────────────+────────────+──────────
  // TOTAL       |  20  |  1800.0    |   50.0     |   150.0    |   90.0

  private val sparkSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("department", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("region", StringType, nullable = false)
    )
  )

  private val testRows: Seq[Row] = Seq(
    // engineering (8 rows): scores 50,70,80,90,100,110,130,150 = 780
    Row(1, "alice", "engineering", 100.0, 10, "us-east"),
    Row(2, "bob", "engineering", 150.0, 20, "us-west"),
    Row(3, "carol", "engineering", 80.0, 15, "eu-west"),
    Row(4, "dave", "engineering", 130.0, 25, "us-east"),
    Row(5, "eve", "engineering", 50.0, 5, "us-west"),
    Row(6, "frank", "engineering", 90.0, 12, "eu-west"),
    Row(7, "grace", "engineering", 110.0, 18, "us-east"),
    Row(8, "heidi", "engineering", 70.0, 8, "us-west"),
    // marketing (6 rows): scores 60,75,85,95,105,90 = 510
    Row(9, "ivan", "marketing", 85.0, 14, "us-east"),
    Row(10, "judy", "marketing", 95.0, 22, "us-west"),
    Row(11, "karl", "marketing", 60.0, 6, "eu-west"),
    Row(12, "lisa", "marketing", 105.0, 16, "us-east"),
    Row(13, "mike", "marketing", 75.0, 9, "us-west"),
    Row(14, "nancy", "marketing", 90.0, 11, "eu-west"),
    // sales (6 rows): scores 55,65,85,110,120,75 = 510
    Row(15, "oscar", "sales", 85.0, 13, "us-east"),
    Row(16, "pat", "sales", 120.0, 24, "us-west"),
    Row(17, "quinn", "sales", 55.0, 7, "eu-west"),
    Row(18, "rachel", "sales", 110.0, 19, "us-east"),
    Row(19, "sam", "sales", 65.0, 10, "us-west"),
    Row(20, "tina", "sales", 75.0, 8, "eu-west")
  )

  // Expected totals
  private val TotalRows  = 20L
  private val TotalScore = 1800.0
  private val AvgScore   = 90.0
  private val MinScore   = 50.0
  private val MaxScore   = 150.0

  private val DeptExpected: Map[String, (Long, Double, Double, Double, Double)] = Map(
    "engineering" -> (8L, 780.0, 97.5, 50.0, 150.0),
    "marketing"   -> (6L, 510.0, 85.0, 60.0, 105.0),
    "sales"       -> (6L, 510.0, 85.0, 55.0, 120.0)
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-agg-pushdown").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def createDeltaSource(deltaPath: String): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
    df.write.format("delta").partitionBy("region").save(deltaPath)
  }

  private def buildDeltaCompanion(deltaPath: String, indexPath: String): DataFrame = {
    val result = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    result(0).getString(2) shouldBe "success"

    flushCaches()

    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
  }

  // ── Iceberg helpers ──────────────────────────────────────────────────────────

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "department", Types.StringType.get()),
    Types.NestedField.required(4, "score", Types.DoubleType.get()),
    Types.NestedField.required(5, "quantity", Types.IntegerType.get()),
    Types.NestedField.required(6, "region", Types.StringType.get())
  )

  private def withIcebergCompanion(f: DataFrame => Unit): Unit = {
    val root         = Files.createTempDirectory("iceberg-agg-pushdown").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      // Create Iceberg table
      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "agg_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Write parquet data and register as snapshot
      val parquetDir = new File(root, "parquet-data").getAbsolutePath
      val df         = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
      df.coalesce(1).write.parquet(s"file://$parquetDir")

      val parquetFiles = new File(parquetDir)
        .listFiles()
        .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

      val table    = server.catalog.loadTable(tableId)
      val appendOp = table.newAppend()
      parquetFiles.foreach { pf =>
        appendOp.appendFile(
          DataFiles
            .builder(table.spec())
            .withPath(s"file://${pf.getAbsolutePath}")
            .withFileSizeInBytes(pf.length())
            .withRecordCount(testRows.size.toLong)
            .withFormat(FileFormat.PARQUET)
            .build()
        )
      }
      appendOp.commit()

      // Configure Spark for embedded catalog
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Build companion
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.agg_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"

      flushCaches()

      val companionDf = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      f(companionDf)
    } finally {
      try spark.conf.unset("spark.indextables.iceberg.catalogType")
      catch { case _: Exception => }
      try spark.conf.unset("spark.indextables.iceberg.uri")
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion aggregate pushdown tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("COUNT(*) on Delta companion returns exact row count") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.count() shouldBe TotalRows
    }
  }

  test("SUM on Delta companion returns correct total") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.agg(sum("score")).collect()(0).getDouble(0) shouldBe TotalScore
    }
  }

  test("AVG on Delta companion returns correct average") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.agg(avg("score")).collect()(0).getDouble(0) shouldBe AvgScore +- 0.01
    }
  }

  test("MIN and MAX on Delta companion return correct extremes") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.agg(min("score")).collect()(0).getDouble(0) shouldBe MinScore
      df.agg(max("score")).collect()(0).getDouble(0) shouldBe MaxScore
    }
  }

  test("multiple aggregations in single query on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      val row = df
        .agg(
          count("*"),
          sum("score"),
          avg("score"),
          min("score"),
          max("score")
        )
        .collect()(0)

      row.getLong(0) shouldBe TotalRows
      row.getDouble(1) shouldBe TotalScore
      row.getDouble(2) shouldBe AvgScore +- 0.01
      row.getDouble(3) shouldBe MinScore
      row.getDouble(4) shouldBe MaxScore
    }
  }

  test("GROUP BY with aggregations on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      val grouped = df
        .groupBy("department")
        .agg(
          count("*").as("cnt"),
          sum("score").as("total"),
          avg("score").as("average"),
          min("score").as("lo"),
          max("score").as("hi")
        )
        .orderBy("department")
        .collect()

      grouped.length shouldBe 3

      for (row <- grouped) {
        val dept                                     = row.getString(0)
        val (expCnt, expSum, expAvg, expMin, expMax) = DeptExpected(dept)

        withClue(s"department=$dept: ") {
          row.getLong(1) shouldBe expCnt
          row.getDouble(2) shouldBe expSum
          row.getDouble(3) shouldBe expAvg +- 0.01
          row.getDouble(4) shouldBe expMin
          row.getDouble(5) shouldBe expMax
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion aggregate pushdown tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("COUNT(*) on Iceberg companion returns exact row count") {
    withIcebergCompanion(df => df.count() shouldBe TotalRows)
  }

  test("SUM and AVG on Iceberg companion return correct values") {
    withIcebergCompanion { df =>
      df.agg(sum("score")).collect()(0).getDouble(0) shouldBe TotalScore
      df.agg(avg("score")).collect()(0).getDouble(0) shouldBe AvgScore +- 0.01
    }
  }

  test("MIN and MAX on Iceberg companion return correct extremes") {
    withIcebergCompanion { df =>
      df.agg(min("score")).collect()(0).getDouble(0) shouldBe MinScore
      df.agg(max("score")).collect()(0).getDouble(0) shouldBe MaxScore
    }
  }

  test("GROUP BY with aggregations on Iceberg companion") {
    withIcebergCompanion { df =>
      val grouped = df
        .groupBy("department")
        .agg(
          count("*").as("cnt"),
          sum("score").as("total"),
          avg("score").as("average"),
          min("score").as("lo"),
          max("score").as("hi")
        )
        .orderBy("department")
        .collect()

      grouped.length shouldBe 3

      for (row <- grouped) {
        val dept                                     = row.getString(0)
        val (expCnt, expSum, expAvg, expMin, expMax) = DeptExpected(dept)

        withClue(s"department=$dept: ") {
          row.getLong(1) shouldBe expCnt
          row.getDouble(2) shouldBe expSum
          row.getDouble(3) shouldBe expAvg +- 0.01
          row.getDouble(4) shouldBe expMin
          row.getDouble(5) shouldBe expMax
        }
      }
    }
  }

  test("filtered aggregation on Iceberg companion") {
    withIcebergCompanion { df =>
      // Filter to us-east region: rows 1,4,7,9,12,15,18 = 7 rows
      // scores: 100,130,110,85,105,85,110 = 725
      val filtered = df.filter(col("region") === "us-east")
      filtered.count() shouldBe 7L
      filtered.agg(sum("score")).collect()(0).getDouble(0) shouldBe 725.0
    }
  }
}
