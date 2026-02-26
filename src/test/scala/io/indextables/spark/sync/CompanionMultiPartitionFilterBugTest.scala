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
import org.apache.spark.sql.functions.col

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Reproduction test: when a companion table has two partition columns (year, month) and the user pre-filters on the
 * first partition column via the DataFrame API, creates a temp view, then queries the view with the second partition
 * column + an indexquery condition on a text field, all rows in the first partition are returned instead of only the
 * matching rows.
 */
class CompanionMultiPartitionFilterBugTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionMultiPartitionFilterBugTest")
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
   * Creates a Delta table with two partition columns (year, month), a text content field, and several other data
   * fields. Builds a companion index with text indexing mode on the content field.
   *
   * Schema: id INT, title STRING, content STRING, score DOUBLE, year STRING, month STRING Partitioned by: (year, month)
   *
   * Data (12 rows across 4 partition combos): year=2024, month=10: rows 1-3 (content: "machine learning ...", "deep
   * learning ...", "natural language ...") year=2024, month=11: rows 4-6 (content: "computer vision ...",
   * "reinforcement ...", "neural network ...") year=2025, month=01: rows 7-9 (content: "data engineering ...", "cloud
   * computing ...", "machine learning ...") year=2025, month=02: rows 10-12 (content: "quantum computing ...", "edge
   * computing ...", "machine learning ...")
   */
  private def withMultiPartitionCompanion(f: (DataFrame, String) => Unit): Unit = {
    val tempDir = Files.createTempDirectory("tantivy4spark-multi-partition-bug").toString
    try {
      flushCaches()

      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "companion").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id INT,
                   |  title STRING,
                   |  content STRING,
                   |  score DOUBLE,
                   |  year STRING,
                   |  month STRING
                   |) USING DELTA
                   |PARTITIONED BY (year, month)""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1,  'ML Intro',       'machine learning algorithms and models',         85.5,  '2024', '10'),
                   |  (2,  'DL Guide',       'deep learning with neural networks',             92.0,  '2024', '10'),
                   |  (3,  'NLP Basics',     'natural language processing techniques',         78.3,  '2024', '10'),
                   |  (4,  'CV Tutorial',    'computer vision image recognition systems',      95.1,  '2024', '11'),
                   |  (5,  'RL Paper',       'reinforcement learning reward optimization',     88.8,  '2024', '11'),
                   |  (6,  'NN Architecture','neural network architecture design patterns',    70.0,  '2024', '11'),
                   |  (7,  'DE Handbook',    'data engineering pipeline best practices',       91.2,  '2025', '01'),
                   |  (8,  'Cloud Guide',    'cloud computing infrastructure and services',    67.5,  '2025', '01'),
                   |  (9,  'ML Advanced',    'machine learning feature engineering at scale',  84.0,  '2025', '01'),
                   |  (10, 'Quantum Intro',  'quantum computing qubits and circuits',          76.9,  '2025', '02'),
                   |  (11, 'Edge Systems',   'edge computing distributed sensor networks',     93.3,  '2025', '02'),
                   |  (12, 'ML Ops',         'machine learning deployment and monitoring',     81.0,  '2025', '02')
                   |""".stripMargin)

      // Build companion index with content as a text field for indexquery
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' " +
          s"INDEXING MODES ('content':'text') " +
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

  // ===================================================================
  //  Bug reproduction: pre-filter first partition -> temp view ->
  //  SQL query with second partition + indexquery
  // ===================================================================

  test("BUG: pre-filter year, temp view, query with month + indexquery returns only matching rows") {
    withMultiPartitionCompanion { (df, _) =>
      // Step 1: Pre-filter on first partition column and create temp view
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024")

      // Step 2: Query temp view with second partition + indexquery
      // Expected: rows from year=2024, month=11 where content matches "neural"
      //   -> id=6 "neural network architecture design patterns"
      // Bug: returns ALL rows where year=2024 (6 rows)
      val results = spark
        .sql(
          "SELECT * FROM year_2024 WHERE month = '11' AND content indexquery 'neural'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=6, NN Architecture) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 6
      results(0).getAs[String]("title") shouldBe "NN Architecture"
    }
  }

  test("BUG: pre-filter year, temp view, query with month + indexquery matching multiple rows") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v2")

      // "learning" appears in month=10 (rows 1,2) and month=11 (rows 5,6 has "learning")
      // Filtering to month=11 should give only rows 4 (no), 5 (yes - reinforcement learning), 6 (no - neural network)
      // Actually: row 5 has "reinforcement learning reward optimization" -> matches "learning"
      // Expected: 1 row (id=5)
      val results = spark
        .sql(
          "SELECT * FROM year_2024_v2 WHERE month = '11' AND content indexquery 'learning'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=5, RL Paper) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 5
    }
  }

  test("BUG: pre-filter year, temp view, query with month + string filter (no indexquery)") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v3")

      // Filter on second partition + data field (title) without indexquery
      // Expected: 1 row (id=4, CV Tutorial, year=2024, month=11)
      val results = spark
        .sql(
          "SELECT * FROM year_2024_v3 WHERE month = '11' AND title = 'CV Tutorial'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=4, CV Tutorial) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 4
    }
  }

  test("BUG: pre-filter year, temp view, query with month only") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v4")

      // Only second partition filter on the temp view
      // Expected: 3 rows (ids 4,5,6 - year=2024, month=11)
      val results = spark
        .sql(
          "SELECT * FROM year_2024_v4 WHERE month = '11'"
        )
        .collect()

      withClue(
        s"Expected 3 rows (month=11) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 3
      }
      val ids = results.map(_.getAs[Int]("id")).sorted
      ids shouldBe Array(4, 5, 6)
    }
  }

  test("BUG: pre-filter year, temp view, query with indexquery only (no second partition)") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v5")

      // indexquery on temp view without second partition filter
      // "machine" appears in year=2024 month=10 row 1 only
      // Expected: 1 row (id=1, ML Intro)
      val results = spark
        .sql(
          "SELECT * FROM year_2024_v5 WHERE content indexquery 'machine'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=1, ML Intro) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 1
    }
  }

  test("BUG: pre-filter year, temp view, multiple ANDs with second partition key LAST") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v6")

      // Multiple conditions AND'ed together, second partition key as the FINAL condition
      // Expected: id=6 "neural network architecture design patterns" (year=2024, month=11, score > 60)
      val results = spark
        .sql(
          "SELECT * FROM year_2024_v6 WHERE content indexquery 'neural' AND score > 60.0 AND month = '11'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=6, NN Architecture) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")}, score=${r.getAs[Double]("score")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 6
    }
  }

  test("BUG: pre-filter year, temp view, indexquery + title filter + second partition LAST") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2025").createOrReplaceTempView("year_2025")

      // indexquery + string equality + second partition key last
      // "machine" in year=2025: id=9 (month=01), id=12 (month=02)
      // Filtering to month='02' should give only id=12
      val results = spark
        .sql(
          "SELECT * FROM year_2025 WHERE content indexquery 'machine' AND month = '02'"
        )
        .collect()

      withClue(
        s"Expected 1 row (id=12, ML Ops) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 12
    }
  }

  test("BUG: pre-filter year, temp view, three data conditions + second partition LAST") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_v7")

      // Several data conditions AND'ed, with the partition key at the very end
      // Expected: id=5 (RL Paper, score=88.8, year=2024, month=11)
      val results = spark
        .sql(
          """SELECT * FROM year_2024_v7
            |WHERE content indexquery 'learning'
            |  AND score > 80.0
            |  AND score < 95.0
            |  AND month = '11'""".stripMargin
        )
        .collect()

      withClue(
        s"Expected 1 row (id=5, RL Paper) but got ${results.length} rows: " +
          s"${results
              .map(r =>
                s"(id=${r.getAs[Int]("id")}, title=${r.getAs[String]("title")}, " +
                  s"month=${r.getAs[String]("month")}, score=${r.getAs[Double]("score")})"
              )
              .mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getAs[Int]("id") shouldBe 5
    }
  }

  // ===================================================================
  //  Aggregation impact: does the bug also affect COUNT/SUM/AVG?
  // ===================================================================

  test("AGG BUG: pre-filter year, temp view, COUNT(*) with month + indexquery") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_agg1")

      // "neural" matches only id=6 in month=11. Expected count=1
      // If bug hits aggregation path, count will be 3 (all month=11 rows)
      val results = spark
        .sql(
          "SELECT count(*) as cnt FROM year_2024_agg1 WHERE month = '11' AND content indexquery 'neural'"
        )
        .collect()

      withClue(s"Expected count=1 but got ${results(0).getLong(0)}: ") {
        results(0).getLong(0) shouldBe 1L
      }
    }
  }

  test("AGG BUG: pre-filter year, temp view, COUNT(*) with month only (no indexquery)") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_agg2")

      // month=11 in year=2024 has 3 rows (ids 4,5,6). Expected count=3
      val results = spark
        .sql(
          "SELECT count(*) as cnt FROM year_2024_agg2 WHERE month = '11'"
        )
        .collect()

      withClue(s"Expected count=3 but got ${results(0).getLong(0)}: ") {
        results(0).getLong(0) shouldBe 3L
      }
    }
  }

  test("AGG BUG: pre-filter year, temp view, SUM(score) with month + indexquery") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_agg3")

      // "neural" matches only id=6 (score=70.0) in month=11. Expected sum=70.0
      val results = spark
        .sql(
          "SELECT sum(score) as total FROM year_2024_agg3 WHERE month = '11' AND content indexquery 'neural'"
        )
        .collect()

      withClue(s"Expected sum=70.0 but got ${results(0).getDouble(0)}: ") {
        results(0).getDouble(0) shouldBe 70.0 +- 0.01
      }
    }
  }

  test("AGG BUG: pre-filter year, temp view, AVG(score) with month + indexquery") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2025").createOrReplaceTempView("year_2025_agg4")

      // "machine" in year=2025, month=02: only id=12 (score=81.0). Expected avg=81.0
      val results = spark
        .sql(
          "SELECT avg(score) as avg_score FROM year_2025_agg4 WHERE month = '02' AND content indexquery 'machine'"
        )
        .collect()

      withClue(s"Expected avg=81.0 but got ${results(0).getDouble(0)}: ") {
        results(0).getDouble(0) shouldBe 81.0 +- 0.01
      }
    }
  }

  test("AGG BUG: pre-filter year, temp view, GROUP BY with month + indexquery") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_agg5")

      // "learning" in year=2024: id=1 (month=10), id=2 (month=10 deep learning), id=5 (month=11)
      // With month='11': only id=5. GROUP BY title should yield 1 group.
      val results = spark
        .sql(
          """SELECT title, count(*) as cnt
            |FROM year_2024_agg5
            |WHERE month = '11' AND content indexquery 'learning'
            |GROUP BY title""".stripMargin
        )
        .collect()

      withClue(
        s"Expected 1 group but got ${results.length} groups: " +
          s"${results.map(r => s"(title=${r.getString(0)}, cnt=${r.getLong(1)})").mkString(", ")}: "
      ) {
        results.length shouldBe 1
      }
      results(0).getString(0) shouldBe "RL Paper"
      results(0).getLong(1) shouldBe 1L
    }
  }

  test("AGG BUG: pre-filter year, temp view, multiple ANDs + aggregation + second partition LAST") {
    withMultiPartitionCompanion { (df, _) =>
      df.filter(col("year") === "2024").createOrReplaceTempView("year_2024_agg6")

      // indexquery + score filter + partition last, with COUNT
      // "neural" in month=11: id=6 (score=70.0). score > 60 -> matches. Expected count=1
      val results = spark
        .sql(
          "SELECT count(*) as cnt FROM year_2024_agg6 WHERE content indexquery 'neural' AND score > 60.0 AND month = '11'"
        )
        .collect()

      withClue(s"Expected count=1 but got ${results(0).getLong(0)}: ") {
        results(0).getLong(0) shouldBe 1L
      }
    }
  }
}
