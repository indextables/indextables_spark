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
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for DROP INDEXTABLES PARTITIONS on companion tables built from Delta and Iceberg sources.
 *
 * Verifies that partition drop operations correctly remove data from companion indexes while preserving remaining
 * partitions, and that re-sync from the source table restores dropped partitions.
 */
class CompanionDropPartitionsTest
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
      .appName("CompanionDropPartitionsTest")
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

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-drop-partitions").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def readCompanion(indexPath: String): DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)

  // ── Test data (10 rows, partitioned by region) ─────────────────────────────
  //
  // region   | rows | ids
  // us-east  |   4  | 1, 4, 7, 10
  // us-west  |   3  | 2, 5, 8
  // eu-west  |   3  | 3, 6, 9

  private val regionSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("region", StringType, nullable = false)
    )
  )

  private val regionRows: Seq[Row] = Seq(
    Row(1, "alice", 85.0, "us-east"),
    Row(2, "bob", 92.0, "us-west"),
    Row(3, "carol", 78.0, "eu-west"),
    Row(4, "dave", 95.0, "us-east"),
    Row(5, "eve", 88.0, "us-west"),
    Row(6, "frank", 70.0, "eu-west"),
    Row(7, "grace", 91.0, "us-east"),
    Row(8, "heidi", 83.0, "us-west"),
    Row(9, "ivan", 76.0, "eu-west"),
    Row(10, "judy", 99.0, "us-east")
  )

  private def createRegionDelta(deltaPath: String): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(regionRows), regionSchema)
    df.write.format("delta").partitionBy("region").save(deltaPath)
  }

  private def buildDeltaCompanion(deltaPath: String, indexPath: String): Unit = {
    val result = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    result(0).getString(2) shouldBe "success"
    flushCaches()
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion: DROP PARTITIONS tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("DROP PARTITIONS on Delta companion should logically remove partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createRegionDelta(deltaPath)
      buildDeltaCompanion(deltaPath, indexPath)

      // Verify all 10 rows present before drop
      readCompanion(indexPath).count() shouldBe 10

      // Drop us-east partition (4 rows)
      val result = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      result(0).getString(1) shouldBe "success"

      flushCaches()

      // Verify 6 rows remain (us-west: 3 + eu-west: 3)
      val afterDrop = readCompanion(indexPath)
      afterDrop.count() shouldBe 6

      // Verify no us-east rows remain
      val ss = spark; import ss.implicits._
      afterDrop.filter($"region" === "us-east").count() shouldBe 0
      afterDrop.filter($"region" === "us-west").count() shouldBe 3
      afterDrop.filter($"region" === "eu-west").count() shouldBe 3
    }
  }

  test("DROP PARTITIONS with range predicate on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create year-partitioned data: 2020-2024, 2 rows per year = 10 rows
      val yearSchema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("score", DoubleType, nullable = false),
          StructField("year", StringType, nullable = false)
        )
      )
      val yearRows = Seq(
        Row(1, "a", 10.0, "2020"),
        Row(2, "b", 20.0, "2020"),
        Row(3, "c", 30.0, "2021"),
        Row(4, "d", 40.0, "2021"),
        Row(5, "e", 50.0, "2022"),
        Row(6, "f", 60.0, "2022"),
        Row(7, "g", 70.0, "2023"),
        Row(8, "h", 80.0, "2023"),
        Row(9, "i", 90.0, "2024"),
        Row(10, "j", 100.0, "2024")
      )

      spark
        .createDataFrame(spark.sparkContext.parallelize(yearRows), yearSchema)
        .write
        .format("delta")
        .partitionBy("year")
        .save(deltaPath)

      // Build companion
      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      readCompanion(indexPath).count() shouldBe 10

      // Drop years before 2022
      val dropResult = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE year < '2022'")
        .collect()
      dropResult(0).getString(1) shouldBe "success"

      flushCaches()

      // Verify only 2022-2024 remain (6 rows)
      val afterDrop = readCompanion(indexPath)
      afterDrop.count() shouldBe 6

      val ss = spark; import ss.implicits._
      afterDrop.filter($"year" === "2020").count() shouldBe 0
      afterDrop.filter($"year" === "2021").count() shouldBe 0
      afterDrop.filter($"year" >= "2022").count() shouldBe 6
    }
  }

  test("DROP PARTITIONS with compound AND predicate on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create (region, year) partitioned data
      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  score DOUBLE,
                   |  region STRING,
                   |  year STRING
                   |) USING DELTA
                   |PARTITIONED BY (region, year)""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'alice',   85.0, 'us-east', '2023'),
                   |  (2, 'bob',     92.0, 'us-east', '2024'),
                   |  (3, 'carol',   78.0, 'us-west', '2023'),
                   |  (4, 'dave',    95.0, 'us-west', '2024'),
                   |  (5, 'eve',     88.0, 'eu-west', '2023'),
                   |  (6, 'frank',   70.0, 'eu-west', '2024')
                   |""".stripMargin)

      // Build companion
      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      readCompanion(indexPath).count() shouldBe 6

      // Drop only us-east/2023 partition
      val dropResult = spark
        .sql(
          s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east' AND year = '2023'"
        )
        .collect()
      dropResult(0).getString(1) shouldBe "success"

      flushCaches()

      // Verify 5 rows remain (dropped 1 row: alice)
      val afterDrop = readCompanion(indexPath)
      afterDrop.count() shouldBe 5

      val ss = spark; import ss.implicits._
      // us-east/2023 gone, but us-east/2024 still there
      afterDrop.filter($"region" === "us-east" && $"year" === "2023").count() shouldBe 0
      afterDrop.filter($"region" === "us-east" && $"year" === "2024").count() shouldBe 1
      afterDrop.filter($"region" === "us-west").count() shouldBe 2
      afterDrop.filter($"region" === "eu-west").count() shouldBe 2
    }
  }

  test("Multiple cumulative drops on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createRegionDelta(deltaPath)
      buildDeltaCompanion(deltaPath, indexPath)

      val ss = spark; import ss.implicits._

      // Initial state: 10 rows
      readCompanion(indexPath).count() shouldBe 10

      // First drop: us-east (4 rows)
      val drop1 = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      drop1(0).getString(1) shouldBe "success"
      flushCaches()

      val afterDrop1 = readCompanion(indexPath)
      afterDrop1.count() shouldBe 6
      afterDrop1.filter($"region" === "us-east").count() shouldBe 0

      // Second drop: eu-west (3 rows)
      val drop2 = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'eu-west'")
        .collect()
      drop2(0).getString(1) shouldBe "success"
      flushCaches()

      val afterDrop2 = readCompanion(indexPath)
      afterDrop2.count() shouldBe 3
      afterDrop2.filter($"region" === "us-east").count() shouldBe 0
      afterDrop2.filter($"region" === "eu-west").count() shouldBe 0
      afterDrop2.filter($"region" === "us-west").count() shouldBe 3

      // Verify remaining IDs are exactly the us-west rows
      val remainingIds = afterDrop2.select("id").collect().map(_.getInt(0)).sorted
      remainingIds shouldBe Array(2, 5, 8)
    }
  }

  test("Re-sync after drop should re-add dropped partition data on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createRegionDelta(deltaPath)
      buildDeltaCompanion(deltaPath, indexPath)

      val ss = spark; import ss.implicits._

      // Drop us-east
      val drop = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      drop(0).getString(1) shouldBe "success"
      flushCaches()

      readCompanion(indexPath).count() shouldBe 6

      // Re-sync from Delta source — should restore dropped partition
      val resyncResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      resyncResult(0).getString(2) shouldBe "success"
      flushCaches()

      // All 10 rows should be back
      val afterResync = readCompanion(indexPath)
      afterResync.count() shouldBe 10
      afterResync.filter($"region" === "us-east").count() shouldBe 4
      afterResync.filter($"region" === "us-west").count() shouldBe 3
      afterResync.filter($"region" === "eu-west").count() shouldBe 3
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion: DROP PARTITIONS tests
  // ═══════════════════════════════════════════════════════════════════════════

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get()),
    Types.NestedField.required(4, "region", Types.StringType.get())
  )

  /**
   * Helper that creates an Iceberg table with partitioned parquet data, builds a companion, and runs a test function.
   * The Iceberg table uses identity partitioning on the region column so that the companion index inherits partitions.
   */
  private def withIcebergCompanion(tableName: String)(f: (String, EmbeddedIcebergRestServer) => Unit): Unit = {
    val root         = Files.createTempDirectory("iceberg-drop-partitions").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, tableName)
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())

      // Create partitioned Iceberg table
      import org.apache.iceberg.PartitionSpec
      val spec = PartitionSpec.builderFor(icebergSchema).identity("region").build()
      server.catalog.buildTable(tableId, icebergSchema).withPartitionSpec(spec).create()

      // Write parquet data per partition and register as DataFiles
      val regions = Map(
        "us-east" -> regionRows.filter(_.getString(3) == "us-east"),
        "us-west" -> regionRows.filter(_.getString(3) == "us-west"),
        "eu-west" -> regionRows.filter(_.getString(3) == "eu-west")
      )

      val table    = server.catalog.loadTable(tableId)
      val appendOp = table.newAppend()

      for ((region, rows) <- regions) {
        val parquetDir = new File(root, s"parquet-data/$region").getAbsolutePath
        val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), regionSchema)
        df.coalesce(1).write.parquet(s"file://$parquetDir")

        val parquetFiles = new File(parquetDir)
          .listFiles()
          .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

        parquetFiles.foreach { pf =>
          appendOp.appendFile(
            DataFiles
              .builder(table.spec())
              .withPath(s"file://${pf.getAbsolutePath}")
              .withFileSizeInBytes(pf.length())
              .withRecordCount(rows.size.toLong)
              .withFormat(FileFormat.PARQUET)
              .withPartitionPath(s"region=$region")
              .build()
          )
        }
      }
      appendOp.commit()

      // Configure Spark for embedded catalog
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      f(root.getAbsolutePath, server)
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  test("DROP PARTITIONS on Iceberg companion should logically remove partition") {
    withIcebergCompanion("drop_basic") { (rootDir, _) =>
      val indexPath = new File(rootDir, "index").getAbsolutePath

      // Build companion
      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.drop_basic' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      // Verify all 10 rows
      readCompanion(indexPath).count() shouldBe 10

      // Drop us-east partition
      val dropResult = spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      dropResult(0).getString(1) shouldBe "success"
      flushCaches()

      // Verify 6 rows remain
      val afterDrop = readCompanion(indexPath)
      afterDrop.count() shouldBe 6

      val ss = spark; import ss.implicits._
      afterDrop.filter($"region" === "us-east").count() shouldBe 0
      afterDrop.filter($"region" === "us-west").count() shouldBe 3
      afterDrop.filter($"region" === "eu-west").count() shouldBe 3
    }
  }

  test("Remaining data correctness after drop on Iceberg companion") {
    withIcebergCompanion("drop_verify") { (rootDir, _) =>
      val indexPath = new File(rootDir, "index").getAbsolutePath

      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.drop_verify' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      // Drop us-east
      spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      flushCaches()

      val ss = spark; import ss.implicits._

      val afterDrop = readCompanion(indexPath)
      afterDrop.count() shouldBe 6

      // Verify partition filter works correctly after drop
      afterDrop.filter($"region" === "us-east").count() shouldBe 0
      afterDrop.filter($"region" === "us-west").count() shouldBe 3
      afterDrop.filter($"region" === "eu-west").count() shouldBe 3
    }
  }

  test("Re-sync after drop on Iceberg companion") {
    withIcebergCompanion("drop_resync") { (rootDir, _) =>
      val indexPath = new File(rootDir, "index").getAbsolutePath

      // Build
      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.drop_resync' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      readCompanion(indexPath).count() shouldBe 10

      // Drop us-west
      spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-west'")
        .collect()
      flushCaches()

      readCompanion(indexPath).count() shouldBe 7

      // Re-sync from Iceberg source — should restore dropped partition
      val resyncResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.drop_resync' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      resyncResult(0).getString(2) shouldBe "success"
      flushCaches()

      // All 10 rows should be back
      val afterResync = readCompanion(indexPath)
      afterResync.count() shouldBe 10

      val ss = spark; import ss.implicits._
      afterResync.filter($"region" === "us-west").count() shouldBe 3
      afterResync.filter($"region" === "us-east").count() shouldBe 4
      afterResync.filter($"region" === "eu-west").count() shouldBe 3
    }
  }

  test("Multiple drops on Iceberg companion") {
    withIcebergCompanion("drop_multi") { (rootDir, _) =>
      val indexPath = new File(rootDir, "index").getAbsolutePath

      val buildResult = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.drop_multi' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      buildResult(0).getString(2) shouldBe "success"
      flushCaches()

      val ss = spark; import ss.implicits._

      // Initial: 10 rows
      readCompanion(indexPath).count() shouldBe 10

      // Drop us-east (4 rows)
      spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'us-east'")
        .collect()
      flushCaches()

      val afterDrop1 = readCompanion(indexPath)
      afterDrop1.count() shouldBe 6
      afterDrop1.filter($"region" === "us-east").count() shouldBe 0

      // Drop eu-west (3 rows)
      spark
        .sql(s"DROP INDEXTABLES PARTITIONS FROM '$indexPath' WHERE region = 'eu-west'")
        .collect()
      flushCaches()

      val afterDrop2 = readCompanion(indexPath)
      afterDrop2.count() shouldBe 3
      afterDrop2.filter($"region" === "us-east").count() shouldBe 0
      afterDrop2.filter($"region" === "eu-west").count() shouldBe 0
      afterDrop2.filter($"region" === "us-west").count() shouldBe 3

      // Verify only us-west remains (3 rows)
      afterDrop2.filter($"region" === "us-west").count() shouldBe 3
    }
  }
}
