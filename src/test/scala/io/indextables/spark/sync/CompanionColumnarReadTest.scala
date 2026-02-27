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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests for the Arrow FFI columnar read path (CompanionColumnarPartitionReader).
 *
 * Tests verify that companion reads with `spark.indextables.read.columnar.enabled=true` produce identical results to
 * the row-based path. Coverage includes:
 *   - All scalar types (String, Int, Long, Float, Double, Boolean, Date, Timestamp)
 *   - Complex types (Array, Struct, Map)
 *   - Partition column injection (String, Int, Date, multiple partition columns)
 *   - Filter pushdown (equality, range, boolean)
 *   - Limit handling (single split and multi-split)
 *   - Schema projection (subset of columns)
 *   - Row vs columnar equivalence
 */
class CompanionColumnarReadTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionColumnarReadTest")
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
    val path = Files.createTempDirectory("tantivy4spark-columnar").toString
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

  /** Build companion index and read it with columnar reads enabled. */
  private def buildAndReadColumnar(deltaPath: String, indexPath: String): DataFrame = {
    val syncResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    syncResult.collect()(0).getString(2) shouldBe "success"

    spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.defaultLimit", "1000")
      .option("spark.indextables.read.columnar.enabled", "true")
      .load(indexPath)
  }

  /** Build companion index and read it with the default row-based path. */
  private def buildAndReadRow(deltaPath: String, indexPath: String): DataFrame =
    spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.defaultLimit", "1000")
      .option("spark.indextables.read.columnar.enabled", "false")
      .load(indexPath)

  /** Compare two sets of rows column-by-column, ignoring order. Keyed by given column. */
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
  //  Scalar types: basic round-trip through columnar path
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: scalar columns (String, Int, Long, Double, Boolean)") {
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

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      assertRowsMatch(sourceRows, companionRows, sourceData.schema, "id")
    }
  }

  test("columnar: Float column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, 1.5f), (2, 2.7f), (3, -0.5f), (4, 0.0f))
        .toDF("id", "value")
        .write
        .format("delta")
        .save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()
      val schema        = StructType(Seq(StructField("id", IntegerType), StructField("value", FloatType)))

      assertRowsMatch(sourceRows, companionRows, schema, "id")
    }
  }

  test("columnar: Date column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  event_date DATE
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice', DATE '2024-01-15'),
                   |  (2, 'Bob',   DATE '2024-06-20'),
                   |  (3, 'Charlie', DATE '2024-12-25')
                   |""".stripMargin)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe companionRows.length

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[String]("name") shouldBe src.getAs[String]("name")
        withClue(s"id=$id date: ") {
          comp.getAs[Date]("event_date") shouldBe src.getAs[Date]("event_date")
        }
      }
    }
  }

  test("columnar: Timestamp column round-trip") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  event_ts TIMESTAMP
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, TIMESTAMP '2024-01-15 10:30:00'),
                   |  (2, TIMESTAMP '2024-06-20 14:00:00')
                   |""".stripMargin)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      companionRows.length shouldBe sourceRows.length

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id timestamp: ") {
          comp.getAs[java.sql.Timestamp]("event_ts") shouldBe src.getAs[java.sql.Timestamp]("event_ts")
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Complex types: Array, Struct, Map
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: Array[String] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", Seq("scala", "java")),
        (2, "bob", Seq("python")),
        (3, "charlie", Seq("rust", "go", "c"))
      ).toDF("id", "name", "tags").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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

  test("columnar: Struct column") {
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

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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

  test("columnar: Map[String, String] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", Map("color" -> "red", "size" -> "small")),
        (2, "bob", Map("color" -> "blue")),
        (3, "charlie", Map("color" -> "green", "size" -> "large", "weight" -> "heavy"))
      ).toDF("id", "name", "attrs").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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

  test("columnar: Array[Int] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, Seq(10, 20, 30)),
        (2, Seq(40)),
        (3, Seq(50, 60))
      ).toDF("id", "scores").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[Seq[Int]]("scores") shouldBe src.getAs[Seq[Int]]("scores")
      }
    }
  }

  test("columnar: nested Struct (Struct containing Struct)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField(
            "profile",
            StructType(
              Seq(
                StructField("name", StringType),
                StructField(
                  "address",
                  StructType(
                    Seq(
                      StructField("city", StringType),
                      StructField("zip", StringType)
                    )
                  )
                )
              )
            )
          )
        )
      )

      val data = Seq(
        Row(1, Row("Alice", Row("NYC", "10001"))),
        Row(2, Row("Bob", Row("LA", "90001"))),
        Row(3, Row("Charlie", Row("Chicago", "60601")))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema).write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp        = companionById(id)
        val srcProfile  = src.getAs[Row]("profile")
        val compProfile = comp.getAs[Row]("profile")
        compProfile.getAs[String]("name") shouldBe srcProfile.getAs[String]("name")
        val srcAddr  = srcProfile.getAs[Row]("address")
        val compAddr = compProfile.getAs[Row]("address")
        compAddr.getAs[String]("city") shouldBe srcAddr.getAs[String]("city")
        compAddr.getAs[String]("zip") shouldBe srcAddr.getAs[String]("zip")
      }
    }
  }

  test("columnar: Array[Struct] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField(
            "contacts",
            ArrayType(
              StructType(
                Seq(
                  StructField("name", StringType),
                  StructField("phone", StringType)
                )
              )
            )
          )
        )
      )

      val data = Seq(
        Row(1, Seq(Row("Alice", "111"), Row("Bob", "222"))),
        Row(2, Seq(Row("Charlie", "333"))),
        Row(3, Seq(Row("Dave", "444"), Row("Eve", "555"), Row("Frank", "666")))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema).write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe companionRows.length

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp           = companionById(id)
        val srcContacts    = src.getAs[Seq[Row]]("contacts")
        val compContacts   = comp.getAs[Seq[Row]]("contacts")
        srcContacts.length shouldBe compContacts.length
        srcContacts.zip(compContacts).foreach { case (s, c) =>
          c.getAs[String]("name") shouldBe s.getAs[String]("name")
          c.getAs[String]("phone") shouldBe s.getAs[String]("phone")
        }
      }
    }
  }

  test("columnar: Map[String, Int] column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, Map("score" -> 100, "rank" -> 1)),
        (2, Map("score" -> 85)),
        (3, Map("score" -> 92, "rank" -> 2, "bonus" -> 10))
      ).toDF("id", "metrics").write.format("delta").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      sourceById.keySet shouldBe companionById.keySet

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        comp.getAs[Map[String, Int]]("metrics") shouldBe src.getAs[Map[String, Int]]("metrics")
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Partition column injection
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: single String partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 85.0, "us-east"),
        (2, "bob", 92.0, "us-west"),
        (3, "charlie", 78.0, "eu-west"),
        (4, "dave", 95.0, "us-east"),
        (5, "eve", 88.0, "eu-west")
      ).toDF("id", "name", "score", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
      val sourceRows    = spark.read.format("delta").load(deltaPath).collect()
      val companionRows = companionDf.collect()

      sourceRows.length shouldBe companionRows.length

      val sourceById    = sourceRows.map(r => r.getAs[Int]("id") -> r).toMap
      val companionById = companionRows.map(r => r.getAs[Int]("id") -> r).toMap

      for ((id, src) <- sourceById) {
        val comp = companionById(id)
        withClue(s"id=$id: ") {
          comp.getAs[String]("name") shouldBe src.getAs[String]("name")
          comp.getAs[Double]("score") shouldBe src.getAs[Double]("score") +- 0.001
          comp.getAs[String]("region") shouldBe src.getAs[String]("region")
        }
      }
    }
  }

  test("columnar: Int partition column") {
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

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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

  test("columnar: Date partition column") {
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

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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

  test("columnar: multiple partition columns") {
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

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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
  //  Filter pushdown through columnar path
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: equality filter on String") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("alice", 1), ("bob", 2), ("charlie", 3))
        .toDF("name", "id")
        .write
        .format("delta")
        .save(deltaPath)

      val df       = buildAndReadColumnar(deltaPath, indexPath)
      val filtered = df.filter(col("name") === "bob").collect()
      filtered.length shouldBe 1
      filtered.head.getAs[String]("name") shouldBe "bob"
    }
  }

  test("columnar: equality and range filters on Int") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("a", 10), ("b", 20), ("c", 30))
        .toDF("name", "value")
        .write
        .format("delta")
        .save(deltaPath)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val eqResult = df.filter(col("value") === 20).collect()
      eqResult.length shouldBe 1

      val rangeResult = df.filter(col("value") > 15).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("columnar: range filter on Long") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((100L, "x"), (200L, "y"), (300L, "z"))
        .toDF("big_id", "label")
        .write
        .format("delta")
        .save(deltaPath)

      val df          = buildAndReadColumnar(deltaPath, indexPath)
      val rangeResult = df.filter(col("big_id") >= 200L).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("columnar: range filter on Double") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((10.5, "a"), (20.5, "b"), (30.5, "c"))
        .toDF("amount", "label")
        .write
        .format("delta")
        .save(deltaPath)

      val df          = buildAndReadColumnar(deltaPath, indexPath)
      val rangeResult = df.filter(col("amount") < 25.0).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("columnar: Boolean filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, true), (2, false), (3, true), (4, false))
        .toDF("id", "active")
        .write
        .format("delta")
        .save(deltaPath)

      val df       = buildAndReadColumnar(deltaPath, indexPath)
      val filtered = df.filter(col("active") === true).collect()
      filtered.length shouldBe 2
    }
  }

  test("columnar: partition filter (not sent to tantivy)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", "us"),
        (2, "bob", "eu"),
        (3, "charlie", "us"),
        (4, "dave", "ap")
      ).toDF("id", "name", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val df       = buildAndReadColumnar(deltaPath, indexPath)
      val filtered = df.filter(col("region") === "us").collect()
      filtered.length shouldBe 2
      filtered.map(_.getAs[String]("name")).toSet shouldBe Set("alice", "charlie")
    }
  }

  test("columnar: combined partition + data filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 85.0, "us"),
        (2, "bob", 92.0, "us"),
        (3, "charlie", 78.0, "eu"),
        (4, "dave", 95.0, "eu")
      ).toDF("id", "name", "score", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val df       = buildAndReadColumnar(deltaPath, indexPath)
      val filtered = df.filter(col("region") === "us" && col("score") > 90.0).collect()
      filtered.length shouldBe 1
      filtered.head.getAs[String]("name") shouldBe "bob"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Limit handling
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: pushed SQL LIMIT restricts result count") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      (1 to 50).map(i => (i, s"name_$i")).toDF("id", "name").write.format("delta").save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      // SQL LIMIT is pushed down to the reader via SupportsPushDownLimit
      df.createOrReplaceTempView("columnar_limit_test")
      val limited = spark.sql("SELECT * FROM columnar_limit_test LIMIT 10").collect()
      limited.length shouldBe 10
    }
  }

  test("columnar: SQL LIMIT") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      (1 to 30).map(i => (i, s"name_$i")).toDF("id", "name").write.format("delta").save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      df.createOrReplaceTempView("columnar_test")
      val limited = spark.sql("SELECT * FROM columnar_test LIMIT 5").collect()
      limited.length shouldBe 5
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Schema projection (reading subset of columns)
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: select subset of columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 100L, 1.5, true),
        (2, "bob", 200L, 2.7, false),
        (3, "charlie", 300L, 3.14, true)
      ).toDF("id", "name", "big_id", "score", "active").write.format("delta").save(deltaPath)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      // Project only 2 of 5 columns
      val projected = df.select("id", "name").collect()
      projected.length shouldBe 3
      projected.head.schema.fieldNames should contain theSameElementsAs Array("id", "name")
    }
  }

  test("columnar: select only partition column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", "us"),
        (2, "bob", "eu"),
        (3, "charlie", "us")
      ).toDF("id", "name", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      // Select only the partition column
      val projected = df.select("region").collect()
      projected.length shouldBe 3
      projected.map(_.getString(0)).toSet shouldBe Set("us", "eu")
    }
  }

  test("columnar: select mix of data and partition columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 85.0, "us"),
        (2, "bob", 92.0, "eu"),
        (3, "charlie", 78.0, "us")
      ).toDF("id", "name", "score", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      // Select one data column and the partition column
      val projected = df.select("name", "region").collect()
      projected.length shouldBe 3
      projected.head.schema.fieldNames should contain theSameElementsAs Array("name", "region")

      val nameRegionPairs = projected.map(r => (r.getString(0), r.getString(1))).toSet
      nameRegionPairs should contain(("alice", "us"))
      nameRegionPairs should contain(("bob", "eu"))
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Row vs columnar equivalence
  // ═══════════════════════════════════════════════════════════════════

  test("equivalence: columnar and row paths produce identical results for scalars") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 100L, 1.5, true),
        (2, "bob", 200L, 2.7, false),
        (3, "charlie", 300L, 3.14, true),
        (4, "dave", 400L, 0.0, false),
        (5, "eve", 500L, 99.9, true)
      ).toDF("id", "name", "big_id", "score", "active").write.format("delta").save(deltaPath)

      // Build companion once
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Read with columnar
      val columnarRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
        .collect()

      // Read with row
      val rowRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "false")
        .load(indexPath)
        .collect()

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("big_id", LongType),
          StructField("score", DoubleType),
          StructField("active", BooleanType)
        )
      )

      assertRowsMatch(columnarRows, rowRows, schema, "id")
    }
  }

  test("equivalence: columnar and row paths produce identical results with partition columns") {
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

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val columnarRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
        .collect()

      val rowRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "false")
        .load(indexPath)
        .collect()

      columnarRows.length shouldBe rowRows.length
      columnarRows.length shouldBe 6

      val columnarById = columnarRows.map(r => r.getAs[Int]("id") -> r).toMap
      val rowById      = rowRows.map(r => r.getAs[Int]("id") -> r).toMap

      for ((id, rowRow) <- rowById) {
        val colRow = columnarById(id)
        withClue(s"id=$id: ") {
          colRow.getAs[String]("name") shouldBe rowRow.getAs[String]("name")
          colRow.getAs[Double]("score") shouldBe rowRow.getAs[Double]("score") +- 0.01
          colRow.getAs[Boolean]("active") shouldBe rowRow.getAs[Boolean]("active")
          colRow.getAs[Date]("event_date") shouldBe rowRow.getAs[Date]("event_date")
          colRow.getAs[String]("region") shouldBe rowRow.getAs[String]("region")
        }
      }
    }
  }

  test("equivalence: columnar and row paths produce identical results with filters") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", 85.0, "us"),
        (2, "bob", 92.0, "us"),
        (3, "charlie", 78.0, "eu"),
        (4, "dave", 95.0, "eu"),
        (5, "eve", 88.0, "ap")
      ).toDF("id", "name", "score", "region").write.format("delta").partitionBy("region").save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Filtered read — columnar
      val columnarFiltered = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
        .filter(col("score") > 85.0)
        .collect()

      // Filtered read — row
      val rowFiltered = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "false")
        .load(indexPath)
        .filter(col("score") > 85.0)
        .collect()

      columnarFiltered.length shouldBe rowFiltered.length

      val columnarNames = columnarFiltered.map(_.getAs[String]("name")).toSet
      val rowNames      = rowFiltered.map(_.getAs[String]("name")).toSet
      columnarNames shouldBe rowNames
    }
  }

  test("equivalence: columnar and row paths for complex types") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", Seq("scala", "java"), Map("color" -> "red")),
        (2, "bob", Seq("python"), Map("color" -> "blue", "size" -> "large")),
        (3, "charlie", Seq("rust", "go"), Map("color" -> "green"))
      ).toDF("id", "name", "tags", "attrs").write.format("delta").save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val columnarRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
        .collect()

      val rowRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "false")
        .load(indexPath)
        .collect()

      columnarRows.length shouldBe rowRows.length

      val columnarById = columnarRows.map(r => r.getAs[Int]("id") -> r).toMap
      val rowById      = rowRows.map(r => r.getAs[Int]("id") -> r).toMap

      for ((id, rowRow) <- rowById) {
        val colRow = columnarById(id)
        withClue(s"id=$id: ") {
          colRow.getAs[String]("name") shouldBe rowRow.getAs[String]("name")
          colRow.getAs[Seq[String]]("tags") shouldBe rowRow.getAs[Seq[String]]("tags")
          colRow.getAs[Map[String, String]]("attrs") shouldBe rowRow.getAs[Map[String, String]]("attrs")
        }
      }
    }
  }

  test("equivalence: columnar and row paths produce identical results for Timestamp columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  created_at TIMESTAMP
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   TIMESTAMP '2024-01-15 10:30:00'),
                   |  (2, 'Bob',     TIMESTAMP '2024-06-15 14:00:00'),
                   |  (3, 'Charlie', TIMESTAMP '2024-09-01 09:00:00'),
                   |  (4, 'Dave',    TIMESTAMP '2024-12-25 23:59:59')
                   |""".stripMargin)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val columnarRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
        .collect()

      val rowRows = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "false")
        .load(indexPath)
        .collect()

      columnarRows.length shouldBe rowRows.length
      columnarRows.length shouldBe 4

      val columnarById = columnarRows.map(r => r.getAs[Int]("id") -> r).toMap
      val rowById      = rowRows.map(r => r.getAs[Int]("id") -> r).toMap

      for ((id, rowRow) <- rowById) {
        val colRow = columnarById(id)
        withClue(s"id=$id: ") {
          colRow.getAs[String]("name") shouldBe rowRow.getAs[String]("name")
          colRow.getAs[Timestamp]("created_at") shouldBe rowRow.getAs[Timestamp]("created_at")
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Mixed scenario: all types + partitioning + filter
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: mixed types with partition column and filter") {
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

      val companionDf = buildAndReadColumnar(deltaPath, indexPath)

      // Unfiltered — all 6 rows
      val all = companionDf.collect()
      all.length shouldBe 6

      // Partition filter
      val usEast = companionDf.filter(col("region") === "us-east").collect()
      usEast.length shouldBe 2

      // Data filter
      val highScore = companionDf.filter(col("score") > 90.0).collect()
      highScore.length shouldBe 2
      highScore.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Dave")

      // Combined filter
      val euActive = companionDf.filter(col("region") === "eu-west" && col("active") === true).collect()
      euActive.length shouldBe 2
      euActive.map(_.getAs[String]("name")).toSet shouldBe Set("Charlie", "Eve")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Temporal range queries through columnar path
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: Date range filter greater-than") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  event_date DATE
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   DATE '2024-01-15'),
                   |  (2, 'Bob',     DATE '2024-06-15'),
                   |  (3, 'Charlie', DATE '2024-09-01'),
                   |  (4, 'Dave',    DATE '2024-12-25')
                   |""".stripMargin)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val afterJune = df.filter(col("event_date") > java.sql.Date.valueOf("2024-06-01")).collect()
      afterJune.length shouldBe 3
      afterJune.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Charlie", "Dave")
    }
  }

  test("columnar: Date range filter between") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  event_date DATE
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   DATE '2024-01-15'),
                   |  (2, 'Bob',     DATE '2024-06-15'),
                   |  (3, 'Charlie', DATE '2024-09-01'),
                   |  (4, 'Dave',    DATE '2024-12-25')
                   |""".stripMargin)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val midYear = df.filter(
        col("event_date") >= java.sql.Date.valueOf("2024-06-01") &&
          col("event_date") < java.sql.Date.valueOf("2024-10-01")
      ).collect()
      midYear.length shouldBe 2
      midYear.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Charlie")
    }
  }

  test("columnar: Date partition column range filter") {
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
                   |  (2, 'Bob',     DATE '2024-06-15'),
                   |  (3, 'Charlie', DATE '2024-09-01'),
                   |  (4, 'Dave',    DATE '2024-12-25')
                   |""".stripMargin)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val afterJune = df.filter(col("event_date") > java.sql.Date.valueOf("2024-06-01")).collect()
      afterJune.length shouldBe 3
      afterJune.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Charlie", "Dave")

      val q3q4 = df.filter(
        col("event_date") >= java.sql.Date.valueOf("2024-07-01") &&
          col("event_date") <= java.sql.Date.valueOf("2024-12-31")
      ).collect()
      q3q4.length shouldBe 2
      q3q4.map(_.getAs[String]("name")).toSet shouldBe Set("Charlie", "Dave")
    }
  }

  test("columnar: Timestamp range filter greater-than") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  created_at TIMESTAMP
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   TIMESTAMP '2024-01-15 10:00:00'),
                   |  (2, 'Bob',     TIMESTAMP '2024-06-15 14:30:00'),
                   |  (3, 'Charlie', TIMESTAMP '2024-09-01 09:00:00'),
                   |  (4, 'Dave',    TIMESTAMP '2024-12-25 23:59:59')
                   |""".stripMargin)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val afterJune = df.filter(col("created_at") > java.sql.Timestamp.valueOf("2024-06-01 00:00:00")).collect()
      afterJune.length shouldBe 3
      afterJune.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Charlie", "Dave")
    }
  }

  test("columnar: Timestamp range filter between") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  name STRING,
                   |  created_at TIMESTAMP
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'Alice',   TIMESTAMP '2024-01-15 10:00:00'),
                   |  (2, 'Bob',     TIMESTAMP '2024-06-15 14:30:00'),
                   |  (3, 'Charlie', TIMESTAMP '2024-09-01 09:00:00'),
                   |  (4, 'Dave',    TIMESTAMP '2024-12-25 23:59:59')
                   |""".stripMargin)

      val df = buildAndReadColumnar(deltaPath, indexPath)

      val midYear = df.filter(
        col("created_at") >= java.sql.Timestamp.valueOf("2024-06-01 00:00:00") &&
          col("created_at") < java.sql.Timestamp.valueOf("2024-10-01 00:00:00")
      ).collect()
      midYear.length shouldBe 2
      midYear.map(_.getAs[String]("name")).toSet shouldBe Set("Bob", "Charlie")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Mixed scenario: all types + partitioning + filter
  // ═══════════════════════════════════════════════════════════════════

  test("columnar: larger dataset (50 rows) with partition") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss      = spark; import ss.implicits._
      val regions = Seq("us-east", "us-west", "eu-west", "ap-south")
      val sourceData = (1 to 50)
        .map(i => (i, s"user_$i", i * 1.1, i % 2 == 0, regions(i % regions.size)))
        .toDF("id", "name", "score", "active", "region")

      sourceData.write.format("delta").partitionBy("region").save(deltaPath)

      val companionDf   = buildAndReadColumnar(deltaPath, indexPath)
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
