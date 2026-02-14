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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Reproduction tests for companion split bugs:
 *   1. Array[String] field returns null with "Cannot deserialize value of type LinkedHashMap" error
 *   2. Date field throws ClassCastException
 *   3. Any predicate returns zero rows
 */
class CompanionSplitBugReproductionTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession.builder()
      .appName("CompanionSplitBugReproductionTest")
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

  // ═══════════════════════════════════════════════════
  //  Bug #1: Array[String] field deserialization error
  // ═══════════════════════════════════════════════════

  test("BUG #1: Array[String] field should be returned correctly from companion split") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_array_bug").getAbsolutePath
      val indexPath = new File(tempDir, "companion_array_bug").getAbsolutePath

      // Create Delta table with Array[String] column
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  name STRING,
           |  tags ARRAY<STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'Alice', array('tag1', 'tag2', 'tag3')
           |UNION ALL
           |SELECT 2, 'Bob', array('alpha', 'beta')
           |UNION ALL
           |SELECT 3, 'Charlie', array('x', 'y', 'z')
           |""".stripMargin)

      // Build companion index
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Read back companion index
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      // This should NOT throw "Cannot deserialize value of type LinkedHashMap"
      // and tags should NOT be null
      val results = df.collect()
      results.length shouldBe 3

      // Verify array fields are not null
      results.foreach { row =>
        val tagsIdx = row.fieldIndex("tags")
        val tags = row.get(tagsIdx)
        withClue(s"tags field should not be null for row: $row") {
          assert(tags != null, "tags should not be null")
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════
  //  Bug #2: Date field ClassCastException
  // ═══════════════════════════════════════════════════

  test("BUG #2: Date PARTITION column should not throw ClassCastException from companion split") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_date_part_bug").getAbsolutePath
      val indexPath = new File(tempDir, "companion_date_part_bug").getAbsolutePath

      // Create Delta table partitioned by Date column — this is the scenario that triggers
      // ClassCastException because convertPartitionValue doesn't handle DateType
      val ss = spark
      import ss.implicits._

      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  name STRING,
           |  score DOUBLE,
           |  event_date DATE
           |) USING DELTA
           |PARTITIONED BY (event_date)""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath` VALUES
           |  (1, 'Alice', 10.5, DATE '2024-01-15'),
           |  (2, 'Bob', 20.5, DATE '2024-02-20'),
           |  (3, 'Charlie', 30.5, DATE '2024-03-25')
           |""".stripMargin)

      // Build companion index
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Read back companion index — ClassCastException is thrown here because
      // the partition value for event_date is a string "2024-01-15" but Spark's
      // DateType expects Int (days since epoch). convertPartitionValue falls through
      // to the default case and returns UTF8String instead of Int.
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      // This is where the ClassCastException would be thrown
      val results = df.collect()
      results.length shouldBe 3

      // Verify date partition fields are not null and contain valid dates
      val dateValues = df.select("event_date").collect().map(_.getDate(0))
      dateValues.length shouldBe 3
      dateValues.foreach { d =>
        assert(d != null, s"Date partition value should not be null")
      }

      // Verify specific date values
      val expectedDates = Set("2024-01-15", "2024-02-20", "2024-03-25")
      val actualDates = dateValues.map(_.toString).toSet
      actualDates shouldBe expectedDates
    }
  }

  // ═══════════════════════════════════════════════════
  //  Bug #3: Any predicate returns zero rows
  // ═══════════════════════════════════════════════════

  test("BUG #3: Simple equality predicate should return rows from companion split") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_pred_bug").getAbsolutePath
      val indexPath = new File(tempDir, "companion_pred_bug").getAbsolutePath

      val ss = spark
      import ss.implicits._

      // Create a simple Delta table with basic types only
      (1 to 20).map(i => (i.toLong, s"name_$i", i * 10.0))
        .toDF("id", "name", "score")
        .write.format("delta").save(deltaPath)

      // Build companion index
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Read without filter using COLLECT (not count) to verify actual tantivy search
      val dfNoFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val noFilterResults = dfNoFilter.collect()
      noFilterResults.length shouldBe 20

      // Read WITH equality filter on string field — should return matching rows
      val dfStringFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
        .filter(col("name") === "name_5")

      val stringFilterResults = dfStringFilter.collect()
      withClue("String equality filter should return 1 row but returned 0") {
        stringFilterResults.length shouldBe 1
      }

      // Read WITH equality filter on numeric field
      val dfNumericFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
        .filter(col("id") === 5L)

      val numericFilterResults = dfNumericFilter.collect()
      withClue("Numeric equality filter should return 1 row but returned 0") {
        numericFilterResults.length shouldBe 1
      }

      // Read WITH range filter
      val dfRangeFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
        .filter(col("id") > 15L)

      val rangeFilterResults = dfRangeFilter.collect()
      withClue("Range filter (id > 15) should return 5 rows but returned 0") {
        rangeFilterResults.length shouldBe 5
      }
    }
  }

  test("BUG #3b: Predicate on companion split with complex types should still work") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_pred_complex").getAbsolutePath
      val indexPath = new File(tempDir, "companion_pred_complex").getAbsolutePath

      // Create Delta table with all types
      spark.sql(
        s"""CREATE TABLE delta.`$deltaPath` (
           |  id LONG,
           |  name STRING,
           |  score DOUBLE,
           |  active BOOLEAN,
           |  event_date DATE,
           |  tags ARRAY<STRING>
           |) USING DELTA""".stripMargin)

      spark.sql(
        s"""INSERT INTO delta.`$deltaPath`
           |SELECT 1, 'Alice', 10.5, true, DATE '2024-01-15', array('tag1', 'tag2')
           |UNION ALL
           |SELECT 2, 'Bob', 20.5, false, DATE '2024-02-20', array('alpha', 'beta')
           |UNION ALL
           |SELECT 3, 'Charlie', 30.5, true, DATE '2024-03-25', array('x')
           |UNION ALL
           |SELECT 4, 'Dave', 40.5, false, DATE '2024-04-10', array('y', 'z')
           |UNION ALL
           |SELECT 5, 'Eve', 50.5, true, DATE '2024-05-05', array('tag1')
           |""".stripMargin)

      // Build companion index
      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FROM DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      // Read without filter — baseline
      val dfNoFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val noFilterCount = dfNoFilter.count()
      noFilterCount shouldBe 5

      // Filter on name (string field)
      val dfNameFilter = dfNoFilter.filter(col("name") === "Alice")
      val nameResults = dfNameFilter.collect()
      withClue("Filter name='Alice' should return 1 row") {
        nameResults.length shouldBe 1
      }

      // Filter on id (long field)
      val dfIdFilter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
        .filter(col("id") === 3L)

      val idResults = dfIdFilter.collect()
      withClue("Filter id=3 should return 1 row") {
        idResults.length shouldBe 1
      }
    }
  }
}
