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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Round-trip validation tests for companion indexes.
 *
 * Each test writes known data to a Delta table, builds a companion index, reads ALL rows back through the companion,
 * and asserts that every column of every row matches the original source.
 */
class CompanionRoundTripTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionRoundTripTest")
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
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark-roundtrip").toString
    try {
      try {
        import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
        GlobalSplitCacheManager.flushAllCaches()
        DriverSplitLocalityManager.clear()
      } catch {
        case _: Exception =>
      }
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def buildAndReadCompanion(deltaPath: String, indexPath: String): DataFrame = {
    val syncResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    syncResult.collect()(0).getString(2) shouldBe "success"

    spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
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

  // ═══════════════════════════════════════════════════════════════════
  //  Round-trip: non-partitioned tables
  // ═══════════════════════════════════════════════════════════════════

  test("round-trip: scalar columns (String, Int, Long, Double, Boolean)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      val sourceData = Seq(
        (1, "alice", 100L, 1.5, true),
        (2, "bob", 200L, 2.7, false),
        (3, "charlie", 300L, 3.14, true),
        (4, "dave", 400L, 0.0, false),
        (5, "eve", 500L, 99.9, true)
      ).toDF("id", "name", "big_id", "score", "active")

      sourceData.write.format("delta").save(deltaPath)

      val companionDf = buildAndReadCompanion(deltaPath, indexPath)

      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()
      val schema        = sourceData.schema

      assertRowsMatch(sourceRows, companionRows, schema, "id")
    }
  }

  test("round-trip: Date and Timestamp columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  event_date DATE,
                   |  event_ts TIMESTAMP
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
                   |  (2, DATE '2024-06-20', TIMESTAMP '2024-06-20 14:00:00'),
                   |  (3, DATE '2024-12-25', TIMESTAMP '2024-12-25 23:59:59')
                   |""".stripMargin)

      val companionDf = buildAndReadCompanion(deltaPath, indexPath)

      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe companionRows.length

      // Key by id and compare date/timestamp values
      val sourceById    = sourceRows.map(r => r.getInt(0) -> r).toMap
      val companionById = companionRows.map(r => r.getInt(0) -> r).toMap

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id date: ") {
          comp.getAs[Date]("event_date") shouldBe src.getAs[Date]("event_date")
        }
        withClue(s"id=$id timestamp: ") {
          comp.getAs[Timestamp]("event_ts") shouldBe src.getAs[Timestamp]("event_ts")
        }
      }
    }
  }

  test("round-trip: Float column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, 1.5f), (2, 2.7f), (3, -0.5f), (4, 0.0f))
        .toDF("id", "value")
        .write
        .format("delta")
        .save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()
      val schema        = StructType(Seq(StructField("id", IntegerType), StructField("value", FloatType)))

      assertRowsMatch(sourceRows, companionRows, schema, "id")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Round-trip: partitioned tables
  // ═══════════════════════════════════════════════════════════════════

  test("round-trip: single String partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      val sourceData = Seq(
        (1, "alice", 85.0, "us-east"),
        (2, "bob", 92.0, "us-west"),
        (3, "charlie", 78.0, "eu-west"),
        (4, "dave", 95.0, "us-east"),
        (5, "eve", 88.0, "eu-west")
      ).toDF("id", "name", "score", "region")

      sourceData.write.format("delta").partitionBy("region").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()
      val schema        = sourceData.schema

      assertRowsMatch(sourceRows, companionRows, schema, "id")
    }
  }

  test("round-trip: Int partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "jan-report", 2023),
        (2, "feb-report", 2023),
        (3, "mar-report", 2024),
        (4, "apr-report", 2024),
        (5, "may-report", 2025)
      ).toDF("id", "name", "year").write.format("delta").partitionBy("year").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("year", IntegerType)
        )
      )

      assertRowsMatch(sourceRows, companionRows, schema, "id")
    }
  }

  test("round-trip: Date partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  event_date DATE
                   |) USING DELTA
                   |PARTITIONED BY (event_date)""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   DATE '2024-01-15'),
                   |  (2, 'Bob',     DATE '2024-06-20'),
                   |  (3, 'Charlie', DATE '2024-12-25')
                   |""".stripMargin)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[String]("name") shouldBe src.getAs[String]("name")
        comp.getAs[Date]("event_date") shouldBe src.getAs[Date]("event_date")
      }
    }
  }

  test("round-trip: multiple partition columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  region STRING,
                   |  year INT
                   |) USING DELTA
                   |PARTITIONED BY (region, year)""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   'us', 2023),
                   |  (2, 'Bob',     'us', 2024),
                   |  (3, 'Charlie', 'eu', 2023),
                   |  (4, 'Dave',    'eu', 2024),
                   |  (5, 'Eve',     'ap', 2025)
                   |""".stripMargin)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[String]("name") shouldBe src.getAs[String]("name")
        comp.getAs[String]("region") shouldBe src.getAs[String]("region")
        comp.getAs[Int]("year") shouldBe src.getAs[Int]("year")
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Round-trip: complex types
  // ═══════════════════════════════════════════════════════════════════

  test("round-trip: Array[String] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", Seq("scala", "java")),
        (2, "bob", Seq("python")),
        (3, "charlie", Seq("rust", "go", "c"))
      ).toDF("id", "name", "tags").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[String]("name") shouldBe src.getAs[String]("name")
        comp.getAs[Seq[String]]("tags") shouldBe src.getAs[Seq[String]]("tags")
      }
    }
  }

  test("round-trip: Struct column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField(
            "info",
            StructType(
              Seq(
                StructField("first_name", StringType),
                StructField("age", IntegerType)
              )
            )
          )
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema).write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp     = companionById(id)
        val srcInfo  = src.getAs[Row]("info")
        val compInfo = comp.getAs[Row]("info")
        compInfo.getAs[String]("first_name") shouldBe srcInfo.getAs[String]("first_name")
        compInfo.getAs[Int]("age") shouldBe srcInfo.getAs[Int]("age")
      }
    }
  }

  test("round-trip: Map[String, String] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", Map("color" -> "red", "size" -> "small")),
        (2, "bob", Map("color" -> "blue")),
        (3, "charlie", Map("color" -> "green", "size" -> "large", "weight" -> "heavy"))
      ).toDF("id", "name", "attrs").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[String]("name") shouldBe src.getAs[String]("name")
        comp.getAs[Map[String, String]]("attrs") shouldBe src.getAs[Map[String, String]]("attrs")
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Round-trip: mixed scenario (all column types + partitioning)
  // ═══════════════════════════════════════════════════════════════════

  test("round-trip: mixed types with partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  score DOUBLE,
                   |  active BOOLEAN,
                   |  event_date DATE,
                   |  region STRING
                   |) USING DELTA
                   |PARTITIONED BY (region)""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   85.5,  true,  DATE '2024-01-15', 'us-east'),
                   |  (2, 'Bob',     92.0,  false, DATE '2024-03-20', 'us-west'),
                   |  (3, 'Charlie', 78.3,  true,  DATE '2024-06-10', 'eu-west'),
                   |  (4, 'Dave',    95.1,  false, DATE '2024-09-05', 'us-east'),
                   |  (5, 'Eve',     88.8,  true,  DATE '2024-12-25', 'eu-west'),
                   |  (6, 'Frank',   70.0,  false, DATE '2024-02-14', 'us-west')
                   |""".stripMargin)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe 6
      companionRows.length shouldBe 6

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id: ") {
          comp.getAs[String]("name") shouldBe src.getAs[String]("name")
          comp.getAs[Double]("score") shouldBe src.getAs[Double]("score") +- 0.01
          comp.getAs[Boolean]("active") shouldBe src.getAs[Boolean]("active")
          comp.getAs[Date]("event_date") shouldBe src.getAs[Date]("event_date")
          comp.getAs[String]("region") shouldBe src.getAs[String]("region")
        }
      }
    }
  }

  test("round-trip: larger dataset (50 rows) with partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss      = spark; import ss.implicits._
      val regions = Seq("us-east", "us-west", "eu-west", "ap-south")
      val sourceData = (1 to 50)
        .map(i => (i, s"user_$i", i * 1.1, i % 2 == 0, regions(i % regions.size)))
        .toDF("id", "name", "score", "active", "region")

      sourceData.write.format("delta").partitionBy("region").save(deltaPath)

      val companionDf   = buildAndReadCompanion(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe 50
      companionRows.length shouldBe 50

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id: ") {
          comp.getAs[String]("name") shouldBe src.getAs[String]("name")
          comp.getAs[Double]("score") shouldBe src.getAs[Double]("score") +- 0.001
          comp.getAs[Boolean]("active") shouldBe src.getAs[Boolean]("active")
          comp.getAs[String]("region") shouldBe src.getAs[String]("region")
        }
      }
    }
  }
}
