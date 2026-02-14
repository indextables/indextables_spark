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
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Comprehensive type tests for companion splits.
 *
 * Tests every supported Spark type as BOTH partition and non-partition columns,
 * verifying:
 *   - Unfiltered reads return correct data
 *   - Equality filters work on non-partition columns (pushed down to tantivy)
 *   - Range filters work on non-partition numeric columns
 *   - Partition column values are correctly injected
 *   - Complex types (Array, Struct, Map) are correctly deserialized
 */
class CompanionSplitTypeComprehensiveTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession.builder()
      .appName("CompanionSplitTypeComprehensiveTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir",
        Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    val path = Files.createTempDirectory("tantivy4spark").toString
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

  /** Build companion index from a Delta table and return a DataFrame reading it. */
  private def buildAndReadCompanion(deltaPath: String, indexPath: String): DataFrame = {
    val syncResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    syncResult.collect()(0).getString(2) shouldBe "success"

    spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Non-partition column tests: verify data retrieval and filtering
  // ═══════════════════════════════════════════════════════════════════

  test("Non-partition String column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("alice", 1), ("bob", 2), ("charlie", 3))
        .toDF("name", "id")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      df.collect().length shouldBe 3

      // Equality filter on string
      val filtered = df.filter(col("name") === "bob").collect()
      filtered.length shouldBe 1
      filtered.head.getAs[String]("name") shouldBe "bob"
    }
  }

  test("Non-partition Int column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("a", 10), ("b", 20), ("c", 30))
        .toDF("name", "value")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      // Equality filter on int
      val eqResult = df.filter(col("value") === 20).collect()
      eqResult.length shouldBe 1

      // Range filter on int
      val rangeResult = df.filter(col("value") > 15).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("Non-partition Long column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((100L, "x"), (200L, "y"), (300L, "z"))
        .toDF("big_id", "label")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      val filtered = df.filter(col("big_id") === 200L).collect()
      filtered.length shouldBe 1

      val rangeResult = df.filter(col("big_id") >= 200L).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("Non-partition Float column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1.5f, "a"), (2.5f, "b"), (3.5f, "c"))
        .toDF("score", "label")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      df.collect().length shouldBe 3

      val filtered = df.filter(col("score") > 2.0f).collect()
      filtered.length shouldBe 2
    }
  }

  test("Non-partition Double column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((10.5, "a"), (20.5, "b"), (30.5, "c"))
        .toDF("amount", "label")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      val filtered = df.filter(col("amount") === 20.5).collect()
      filtered.length shouldBe 1

      val rangeResult = df.filter(col("amount") < 25.0).collect()
      rangeResult.length shouldBe 2
    }
  }

  test("Non-partition Boolean column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, true), (2, false), (3, true), (4, false))
        .toDF("id", "active")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      df.collect().length shouldBe 4

      val trueRows = df.filter(col("active") === true).collect()
      trueRows.length shouldBe 2

      val falseRows = df.filter(col("active") === false).collect()
      falseRows.length shouldBe 2
    }
  }

  test("Non-partition Date column: read and filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  event_date DATE
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, DATE '2024-01-15'),
           |  (2, DATE '2024-06-15'),
           |  (3, DATE '2024-12-25')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      // Verify date values are not null and correct
      val dates = df.select("event_date").collect().map(_.getDate(0))
      dates.foreach(d => assert(d != null, "Date should not be null"))
      dates.map(_.toString).toSet shouldBe Set("2024-01-15", "2024-06-15", "2024-12-25")
    }
  }

  test("Non-partition Timestamp column: read") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  created_at TIMESTAMP
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, TIMESTAMP '2024-01-15 10:30:00'),
           |  (2, TIMESTAMP '2024-06-15 14:00:00'),
           |  (3, TIMESTAMP '2024-12-25 08:00:00')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val timestamps = df.select("created_at").collect().map(_.getTimestamp(0))
      timestamps.foreach(t => assert(t != null, "Timestamp should not be null"))
    }
  }

  test("Non-partition Array[String] column: read") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  tags ARRAY<STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, array('a', 'b', 'c')
           |UNION ALL
           |SELECT 2, array('x', 'y')
           |UNION ALL
           |SELECT 3, array('single')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      // Verify arrays are not null and contain expected elements
      results.foreach { row =>
        val tags = row.getAs[Seq[String]]("tags")
        assert(tags != null, s"Array field should not be null for row: $row")
        assert(tags.nonEmpty, s"Array should not be empty for row: $row")
      }
    }
  }

  test("Non-partition Struct column: read") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  info STRUCT<first_name: STRING, age: INT>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, named_struct('first_name', 'Alice', 'age', 30)
           |UNION ALL
           |SELECT 2, named_struct('first_name', 'Bob', 'age', 25)
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 2

      results.foreach { row =>
        val info = row.getAs[Row]("info")
        assert(info != null, s"Struct field should not be null for row: $row")
        assert(info.getAs[String]("first_name") != null, "Struct first_name should not be null")
      }
    }
  }

  test("Non-partition Map column: read") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  attrs MAP<STRING, STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, map('color', 'red', 'size', 'large')
           |UNION ALL
           |SELECT 2, map('color', 'blue')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 2

      results.foreach { row =>
        val attrs = row.getAs[Map[String, String]]("attrs")
        assert(attrs != null, s"Map field should not be null for row: $row")
        assert(attrs.nonEmpty, s"Map should not be empty for row: $row")
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Partition column tests: verify partition value injection
  // ═══════════════════════════════════════════════════════════════════

  test("String partition column: values correctly injected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, "us-east"), (2, "us-west"), (3, "eu-west"))
        .toDF("id", "region")
        .write.format("delta").partitionBy("region").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val regions = df.select("region").collect().map(_.getString(0)).toSet
      regions shouldBe Set("us-east", "us-west", "eu-west")
    }
  }

  test("Int partition column: values correctly injected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("a", 2023), ("b", 2024), ("c", 2025))
        .toDF("name", "year")
        .write.format("delta").partitionBy("year").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val years = df.select("year").collect().map(_.getInt(0)).toSet
      years shouldBe Set(2023, 2024, 2025)
    }
  }

  test("Long partition column: values correctly injected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  name STRING,
           |  epoch LONG
           |) USING DELTA
           |PARTITIONED BY (epoch)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  ('a', 1000000000),
           |  ('b', 2000000000),
           |  ('c', 3000000000)
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val epochs = df.select("epoch").collect().map(_.getLong(0)).toSet
      epochs shouldBe Set(1000000000L, 2000000000L, 3000000000L)
    }
  }

  test("Boolean partition column: values correctly injected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(("active-1", true), ("active-2", true), ("inactive-1", false))
        .toDF("name", "is_active")
        .write.format("delta").partitionBy("is_active").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val activeCount = df.select("is_active").collect().count(_.getBoolean(0))
      activeCount shouldBe 2
    }
  }

  test("Date partition column: values correctly injected (Bug #2 regression)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  event_date DATE
           |) USING DELTA
           |PARTITIONED BY (event_date)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Alice', DATE '2024-01-15'),
           |  (2, 'Bob', DATE '2024-06-15'),
           |  (3, 'Charlie', DATE '2024-12-25')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val dates = df.select("event_date").collect().map(_.getDate(0))
      dates.foreach(d => assert(d != null, "Date partition value should not be null"))
      dates.map(_.toString).toSet shouldBe Set("2024-01-15", "2024-06-15", "2024-12-25")
    }
  }

  test("Double partition column: values correctly injected") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  version DOUBLE
           |) USING DELTA
           |PARTITIONED BY (version)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'a', 1.0),
           |  (2, 'b', 2.0),
           |  (3, 'c', 3.0)
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      val versions = df.select("version").collect().map(_.getDouble(0)).toSet
      versions shouldBe Set(1.0, 2.0, 3.0)
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Mixed: partition + non-partition with filtering
  // ═══════════════════════════════════════════════════════════════════

  test("Filter on non-partition column with partition column present") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  score DOUBLE,
           |  region STRING
           |) USING DELTA
           |PARTITIONED BY (region)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Alice', 85.0, 'us-east'),
           |  (2, 'Bob', 92.0, 'us-east'),
           |  (3, 'Charlie', 78.0, 'eu-west'),
           |  (4, 'Dave', 95.0, 'eu-west'),
           |  (5, 'Eve', 88.0, 'us-west')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      // Filter on non-partition string column
      val nameResult = df.filter(col("name") === "Alice").collect()
      nameResult.length shouldBe 1
      nameResult.head.getAs[String]("region") shouldBe "us-east"

      // Filter on non-partition numeric column
      val scoreResult = df.filter(col("score") > 90.0).collect()
      scoreResult.length shouldBe 2
    }
  }

  test("Filter on partition column with partition pruning") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  category STRING
           |) USING DELTA
           |PARTITIONED BY (category)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Item1', 'electronics'),
           |  (2, 'Item2', 'electronics'),
           |  (3, 'Item3', 'clothing'),
           |  (4, 'Item4', 'food')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      // Filter on partition column — handled by partition pruning, not tantivy
      val catResult = df.filter(col("category") === "electronics").collect()
      catResult.length shouldBe 2
    }
  }

  test("Combined partition and non-partition filters") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  score INT,
           |  dept STRING
           |) USING DELTA
           |PARTITIONED BY (dept)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Alice', 85, 'engineering'),
           |  (2, 'Bob', 92, 'engineering'),
           |  (3, 'Charlie', 78, 'marketing'),
           |  (4, 'Dave', 95, 'marketing'),
           |  (5, 'Eve', 60, 'engineering')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      // Combined: partition filter + non-partition filter
      val result = df
        .filter(col("dept") === "engineering")
        .filter(col("score") > 80)
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).toSet shouldBe Set("Alice", "Bob")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  All types in a single table (non-partitioned)
  // ═══════════════════════════════════════════════════════════════════

  test("All scalar types in single non-partitioned table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  str_col STRING,
           |  int_col INT,
           |  long_col LONG,
           |  float_col FLOAT,
           |  double_col DOUBLE,
           |  bool_col BOOLEAN,
           |  date_col DATE,
           |  ts_col TIMESTAMP
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  ('hello', 1, 100, 1.5, 10.5, true,  DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00'),
           |  ('world', 2, 200, 2.5, 20.5, false, DATE '2024-06-15', TIMESTAMP '2024-06-15 18:30:00'),
           |  ('test',  3, 300, 3.5, 30.5, true,  DATE '2024-12-25', TIMESTAMP '2024-12-25 06:00:00')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      // Verify each type is readable and not null
      results.foreach { row =>
        assert(row.getAs[String]("str_col") != null, "str_col should not be null")
        // int_col, long_col, float_col, double_col, bool_col are primitives — no null check needed
        assert(row.getAs[java.sql.Date]("date_col") != null, "date_col should not be null")
        assert(row.getAs[java.sql.Timestamp]("ts_col") != null, "ts_col should not be null")
      }

      // String filter
      df.filter(col("str_col") === "hello").collect().length shouldBe 1

      // Int filter
      df.filter(col("int_col") === 2).collect().length shouldBe 1

      // Long range filter
      df.filter(col("long_col") >= 200L).collect().length shouldBe 2

      // Boolean filter
      df.filter(col("bool_col") === true).collect().length shouldBe 2
    }
  }

  test("All types including complex in single non-partitioned table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  tags ARRAY<STRING>,
           |  info STRUCT<city: STRING, zip: INT>,
           |  attrs MAP<STRING, STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'Alice', array('dev', 'scala'),
           |       named_struct('city', 'NYC', 'zip', 10001),
           |       map('role', 'engineer')
           |UNION ALL
           |SELECT 2, 'Bob', array('qa'),
           |       named_struct('city', 'LA', 'zip', 90001),
           |       map('role', 'manager')
           |UNION ALL
           |SELECT 3, 'Charlie', array('dev', 'python', 'rust'),
           |       named_struct('city', 'SF', 'zip', 94101),
           |       map('role', 'engineer', 'level', 'senior')
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      // Verify complex types
      results.foreach { row =>
        assert(row.getAs[Seq[String]]("tags") != null, "tags should not be null")
        assert(row.getAs[Row]("info") != null, "info should not be null")
        assert(row.getAs[Map[String, String]]("attrs") != null, "attrs should not be null")
      }

      // Filter on string column while complex types are present
      val nameResult = df.filter(col("name") === "Bob").collect()
      nameResult.length shouldBe 1

      // Filter on id (int) column
      val idResult = df.filter(col("id") > 1).collect()
      idResult.length shouldBe 2
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Edge cases
  // ═══════════════════════════════════════════════════════════════════

  test("Multiple partition columns with mixed types") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  region STRING,
           |  year INT
           |) USING DELTA
           |PARTITIONED BY (region, year)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Alice', 'us', 2023),
           |  (2, 'Bob', 'us', 2024),
           |  (3, 'Charlie', 'eu', 2023),
           |  (4, 'Dave', 'eu', 2024)
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 4

      // Both partition columns should be injected correctly
      val regionYears = results.map(r => (r.getAs[String]("region"), r.getAs[Int]("year"))).toSet
      regionYears shouldBe Set(("us", 2023), ("us", 2024), ("eu", 2023), ("eu", 2024))

      // Filter on one partition column
      val usResult = df.filter(col("region") === "us").collect()
      usResult.length shouldBe 2

      // Filter on non-partition column within partitioned table
      val nameResult = df.filter(col("name") === "Charlie").collect()
      nameResult.length shouldBe 1
      nameResult.head.getAs[String]("region") shouldBe "eu"
      nameResult.head.getAs[Int]("year") shouldBe 2023
    }
  }

  test("Date partition + complex non-partition columns together") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  tags ARRAY<STRING>,
           |  event_date DATE
           |) USING DELTA
           |PARTITIONED BY (event_date)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'Alice', array('a', 'b'), DATE '2024-01-15'
           |UNION ALL
           |SELECT 2, 'Bob', array('c'), DATE '2024-06-15'
           |UNION ALL
           |SELECT 3, 'Charlie', array('d', 'e', 'f'), DATE '2024-12-25'
           |""".stripMargin)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      val results = df.collect()
      results.length shouldBe 3

      // Verify date partition values injected
      val dates = df.select("event_date").collect().map(_.getDate(0))
      dates.foreach(d => assert(d != null, "Date should not be null"))
      dates.map(_.toString).toSet shouldBe Set("2024-01-15", "2024-06-15", "2024-12-25")

      // Verify array values deserialized
      results.foreach { row =>
        assert(row.getAs[Seq[String]]("tags") != null, "tags should not be null")
      }

      // Filter on name (non-partition) — tests Bug #3 fix with complex types present
      val nameResult = df.filter(col("name") === "Bob").collect()
      nameResult.length shouldBe 1
      nameResult.head.getAs[java.sql.Date]("event_date").toString shouldBe "2024-06-15"
    }
  }

  test("Empty partition: no rows for a partition value") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq((1, "a"), (2, "a"), (3, "b"))
        .toDF("id", "group")
        .write.format("delta").partitionBy("group").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)

      // Filter for non-existent partition — should return 0 rows
      val noMatch = df.filter(col("group") === "nonexistent").collect()
      noMatch.length shouldBe 0

      // Filter for non-existent value in non-partition column
      val noMatchName = df.filter(col("id") === 999).collect()
      noMatchName.length shouldBe 0
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  INDEXING MODES: verify TEXT mode enables full-text search
  // ═══════════════════════════════════════════════════════════════════

  test("INDEXING MODES text field supports indexquery full-text search") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create Delta table with a message column (STRING in Delta, but we want TEXT indexing)
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  status STRING,
           |  message STRING
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'open', 'the quick brown fox jumps over the lazy dog'),
           |  (2, 'closed', 'machine learning is transforming the world'),
           |  (3, 'open', 'apache spark processes big data efficiently'),
           |  (4, 'closed', 'the lazy cat sleeps all day long')
           |""".stripMargin)

      // Build companion with INDEXING MODES: message as TEXT
      val syncResult = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
      df.createOrReplaceTempView("indexing_modes_test")

      // indexquery on TEXT field should find tokenized matches
      val iqResults = spark.sql(
        "SELECT id, message FROM indexing_modes_test WHERE message indexquery 'lazy'"
      ).collect()
      iqResults.length shouldBe 2
      iqResults.map(_.getInt(0)).toSet shouldBe Set(1, 4)

      // indexquery with multiple terms
      val iqMulti = spark.sql(
        "SELECT id FROM indexing_modes_test WHERE message indexquery 'machine learning'"
      ).collect()
      iqMulti.length shouldBe 1
      iqMulti.head.getInt(0) shouldBe 2

      // Exact match on non-text field (status) should still work via pushdown
      val statusResult = df.filter(col("status") === "open").collect()
      statusResult.length shouldBe 2
      statusResult.map(_.getAs[Int]("id")).toSet shouldBe Set(1, 3)
    }
  }

  test("INDEXING MODES ipaddress field supports IP indexquery") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create Delta table with a STRING column holding IP addresses
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  name STRING,
           |  ip STRING
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'server1', '192.168.1.1'),
           |  (2, 'server2', '192.168.1.2'),
           |  (3, 'server3', '192.168.1.10'),
           |  (4, 'server4', '10.0.0.1'),
           |  (5, 'server5', '10.0.0.2')
           |""".stripMargin)

      // Build companion with INDEXING MODES: ip as ipaddress
      val syncResult = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('ip': 'ipaddress')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
      df.createOrReplaceTempView("ip_modes_test")

      // Exact IP match via indexquery
      val exact = spark.sql(
        "SELECT id FROM ip_modes_test WHERE ip indexquery '192.168.1.1'"
      ).collect()
      exact.length shouldBe 1
      exact.head.getInt(0) shouldBe 1

      // IP range query via indexquery
      val range = spark.sql(
        "SELECT id FROM ip_modes_test WHERE ip indexquery '[192.168.1.0 TO 192.168.1.5]'"
      ).collect()
      range.length shouldBe 2
      range.map(_.getInt(0)).toSet shouldBe Set(1, 2)

      // Exact match on non-IP field should still work
      val nameResult = df.filter(col("name") === "server4").collect()
      nameResult.length shouldBe 1
      nameResult.head.getAs[Int]("id") shouldBe 4
    }
  }

  test("INDEXING MODES json field supports nested field filter pushdown") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create Delta table with a Struct column (auto-detected as JSON)
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  label STRING,
           |  metadata STRUCT<color: STRING, size: INT>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'item-a', named_struct('color', 'red', 'size', 10)
           |UNION ALL
           |SELECT 2, 'item-b', named_struct('color', 'blue', 'size', 20)
           |UNION ALL
           |SELECT 3, 'item-c', named_struct('color', 'red', 'size', 30)
           |""".stripMargin)

      // Build companion — Struct fields are auto-detected as JSON
      val syncResult = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      // All rows present
      df.collect().length shouldBe 3

      // Nested field filter pushdown on struct (equality)
      val colorResult = df.filter(col("metadata.color") === "red").collect()
      colorResult.length shouldBe 2
      colorResult.map(_.getAs[Int]("id")).toSet shouldBe Set(1, 3)

      // Equality filter on nested numeric field
      val sizeResult = df.filter(col("metadata.size") === 20).collect()
      sizeResult.length shouldBe 1
      sizeResult.head.getAs[Int]("id") shouldBe 2

      // Label (non-struct) filter still works
      val labelResult = df.filter(col("label") === "item-b").collect()
      labelResult.length shouldBe 1
      labelResult.head.getAs[Int]("id") shouldBe 2
    }
  }

  test("INDEXING MODES json on STRING column indexes raw JSON for nested queries") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create Delta table with a plain STRING column containing raw JSON strings
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id INT,
           |  label STRING,
           |  payload STRING
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'first',  '{"user":"alice","score":42}'),
           |  (2, 'second', '{"user":"bob","score":99}'),
           |  (3, 'third',  '{"user":"alice","score":7}')
           |""".stripMargin)

      // Build companion with INDEXING MODES: payload as JSON
      val syncResult = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath'
           |  INDEXING MODES ('payload': 'json')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
      df.createOrReplaceTempView("json_string_test")

      // All rows present
      df.collect().length shouldBe 3

      // Nested field query via indexquery (tantivy JSON path syntax: field.key:value)
      val aliceResults = spark.sql(
        "SELECT id FROM json_string_test WHERE payload indexquery 'payload.user:alice'"
      ).collect()
      aliceResults.length shouldBe 2
      aliceResults.map(_.getInt(0)).toSet shouldBe Set(1, 3)

      // Another nested field query
      val bobResults = spark.sql(
        "SELECT id FROM json_string_test WHERE payload indexquery 'payload.user:bob'"
      ).collect()
      bobResults.length shouldBe 1
      bobResults.head.getInt(0) shouldBe 2

      // Non-JSON field filter still works
      val labelResult = df.filter(col("label") === "first").collect()
      labelResult.length shouldBe 1
      labelResult.head.getAs[Int]("id") shouldBe 1
    }
  }

  test("Mixed-case column names should work in SQL query predicates") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark; import ss.implicits._
      Seq(
        (1, "alice", "Engineering"),
        (2, "bob", "Marketing"),
        (3, "charlie", "Engineering")
      ).toDF("userId", "userName", "deptName")
        .write.format("delta").save(deltaPath)

      val df = buildAndReadCompanion(deltaPath, indexPath)
      df.createOrReplaceTempView("mixed_case_test")

      // Verify all rows readable via SQL
      val allRows = spark.sql("SELECT * FROM mixed_case_test").collect()
      allRows.length shouldBe 3

      // Filter on mixed-case column via SQL (this is where the user sees the error)
      val result = spark.sql("SELECT * FROM mixed_case_test WHERE userName = 'bob'").collect()
      result.length shouldBe 1
      result.head.getAs[String]("userName") shouldBe "bob"

      // Filter on another mixed-case column via SQL
      val deptResult = spark.sql("SELECT * FROM mixed_case_test WHERE deptName = 'Engineering'").collect()
      deptResult.length shouldBe 2
    }
  }
}
