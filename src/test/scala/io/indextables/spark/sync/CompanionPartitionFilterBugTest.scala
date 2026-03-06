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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Regression test: partition column filters must be excluded from Tantivy queries in companion splits.
 *
 * Partition columns only exist in directory paths and AddAction metadata — they are NOT indexed in Tantivy. When Spark
 * pushes down a partition filter (e.g., `category = 'animals'`), the regular read path, simple aggregate path, and
 * group-by aggregate path must all exclude it before converting to a Tantivy query. Otherwise the query returns
 * incorrect results (zero or wrong).
 */
class CompanionPartitionFilterBugTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionPartitionFilterBugTest")
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
    if (spark != null) {
      spark.stop()
    }

  private def flushCaches(): Unit =
    try {
      import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch { case _: Exception => }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  /**
   * Creates a partitioned parquet dataset and builds a companion index.
   *
   * Schema: id: Long, name: String, score: Double, category: String (partition column)
   *
   * Data: category=animals: (1, "dog", 10.0), (2, "cat", 20.0), (3, "bird", 30.0) category=tech: (4, "laptop", 40.0),
   * (5, "phone", 50.0)
   *
   * Total: 5 rows, animals=3, tech=2
   */
  private def withPartitionedCompanion(f: (DataFrame, String) => Unit): Unit = {
    val tempDir = Files.createTempDirectory("tantivy4spark-partition-filter-bug").toString
    try {
      flushCaches()

      val parquetPath = new File(tempDir, "parquet").getAbsolutePath
      val indexPath   = new File(tempDir, "companion").getAbsolutePath

      val ss = spark
      import ss.implicits._

      Seq(
        (1L, "dog", 10.0, "animals"),
        (2L, "cat", 20.0, "animals"),
        (3L, "bird", 30.0, "animals"),
        (4L, "laptop", 40.0, "tech"),
        (5L, "phone", 50.0, "tech")
      ).toDF("id", "name", "score", "category")
        .repartition(1, col("category"))
        .write
        .partitionBy("category")
        .parquet(parquetPath)

      // Build companion index
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"AT LOCATION '$indexPath'"
      )
      val rows = result.collect()
      rows.length should be >= 1
      rows(0).getString(2) shouldBe "success"

      // Read companion
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      f(companionDf, indexPath)
    } finally
      deleteRecursively(new File(tempDir))
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Regular read path: partition filter + data filter
  // ═══════════════════════════════════════════════════════════════════

  test("regular read: partition filter with data filter should return correct results") {
    withPartitionedCompanion { (df, _) =>
      // Query with partition filter AND data filter
      val results = df.filter(col("category") === "animals" && col("name") === "dog").collect()
      results.length shouldBe 1
      results(0).getLong(results(0).fieldIndex("id")) shouldBe 1L
    }
  }

  test("regular read: partition filter only should return correct results") {
    withPartitionedCompanion { (df, _) =>
      // Query with only a partition filter
      val results = df.filter(col("category") === "animals").collect()
      results.length shouldBe 3
    }
  }

  test("regular read: partition filter with IN clause should return correct results") {
    withPartitionedCompanion { (df, _) =>
      val results = df.filter(col("category").isin("animals", "tech")).collect()
      results.length shouldBe 5
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Simple aggregate path: COUNT(*) with partition filter
  // ═══════════════════════════════════════════════════════════════════

  test("simple aggregate: COUNT(*) with partition filter should return correct count") {
    withPartitionedCompanion { (df, _) =>
      df.createOrReplaceTempView("partition_bug_test")

      val result = spark
        .sql(
          "SELECT count(*) as cnt FROM partition_bug_test WHERE category = 'animals'"
        )
        .collect()
      result.length shouldBe 1
      result(0).getLong(0) shouldBe 3L
    }
  }

  test("simple aggregate: SUM with partition filter should return correct sum") {
    withPartitionedCompanion { (df, _) =>
      df.createOrReplaceTempView("partition_bug_test_sum")

      val result = spark
        .sql(
          "SELECT sum(score) as total FROM partition_bug_test_sum WHERE category = 'animals'"
        )
        .collect()
      result.length shouldBe 1
      result(0).getDouble(0) shouldBe 60.0
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Group-by aggregate path: GROUP BY with partition filter
  // ═══════════════════════════════════════════════════════════════════

  test("group by: GROUP BY with partition filter should return correct groups") {
    withPartitionedCompanion { (df, _) =>
      df.createOrReplaceTempView("partition_bug_test_groupby")

      val result = spark
        .sql(
          "SELECT name, count(*) as cnt FROM partition_bug_test_groupby WHERE category = 'animals' GROUP BY name"
        )
        .collect()
      result.length shouldBe 3

      val nameCountMap = result.map(r => (r.getString(0), r.getLong(1))).toMap
      nameCountMap("dog") shouldBe 1L
      nameCountMap("cat") shouldBe 1L
      nameCountMap("bird") shouldBe 1L
    }
  }

  test("group by: GROUP BY partition column itself should return correct groups") {
    withPartitionedCompanion { (df, _) =>
      df.createOrReplaceTempView("partition_bug_test_groupby_part")

      val result = spark
        .sql(
          "SELECT category, count(*) as cnt FROM partition_bug_test_groupby_part GROUP BY category"
        )
        .collect()
      result.length shouldBe 2

      val catCountMap = result.map(r => (r.getString(0), r.getLong(1))).toMap
      catCountMap("animals") shouldBe 3L
      catCountMap("tech") shouldBe 2L
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Combined: partition filter + data filter + aggregate
  // ═══════════════════════════════════════════════════════════════════

  test("combined: aggregate with partition filter and data filter should return correct result") {
    withPartitionedCompanion { (df, _) =>
      df.createOrReplaceTempView("partition_bug_test_combined")

      val result = spark
        .sql(
          "SELECT count(*) as cnt FROM partition_bug_test_combined WHERE category = 'animals' AND name = 'dog'"
        )
        .collect()
      result.length shouldBe 1
      result(0).getLong(0) shouldBe 1L
    }
  }
}
