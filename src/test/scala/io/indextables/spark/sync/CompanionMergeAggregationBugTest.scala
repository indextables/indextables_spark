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
import org.apache.spark.sql.functions.{avg, col, count, max, min, sum}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Regression test: comprehensive query battery on companion splits after MERGE SPLITS.
 *
 * With tantivy4java 0.30.4, aggregations worked on companion splits but broke after merge because the merge path
 * dropped string fast fields. Fixed in tantivy4java 0.30.5.
 *
 * This suite exercises every supported query type against a merged companion split to ensure no regressions: filters
 * (EqualTo, range, In, Not, And, Or, StringStartsWith, StringContains, StringEndsWith), aggregations (COUNT, SUM, AVG,
 * MIN, MAX), GROUP BY, and combined filter + aggregation patterns.
 */
class CompanionMergeAggregationBugTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionMergeAggregationBugTest")
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
   * Builds a companion index from two parquet batches, merges the resulting splits, and runs the provided test function
   * against the merged DataFrame.
   *
   * Schema: id: Long, name: String, department: String, score: Double, content: String
   *
   * Data (8 rows across 2 batches): Batch 1: alice/engineering/100, alice/engineering/200, bob/marketing/150,
   * dave/engineering/50 Batch 2: alice/engineering/300, bob/sales/250, carol/engineering/175, carol/sales/125
   *
   * Expected: name counts: alice=3, bob=2, carol=2, dave=1 department counts: engineering=5, marketing=1, sales=2 total
   * score: 1350
   */
  private def withMergedCompanion(indexingModes: String = "")(f: DataFrame => Unit): Unit = {
    val tempDir = Files.createTempDirectory("tantivy4spark-merge-agg-bug").toString
    try {
      flushCaches()

      val batch1Path   = new File(tempDir, "batch1").getAbsolutePath
      val batch2Path   = new File(tempDir, "batch2").getAbsolutePath
      val combinedPath = new File(tempDir, "combined").getAbsolutePath
      val indexPath    = new File(tempDir, "companion").getAbsolutePath

      val ss = spark
      import ss.implicits._

      Seq(
        (1L, "alice", "engineering", 100.0, "Designing the new search backend"),
        (2L, "alice", "engineering", 200.0, "Reviewing pull requests for search"),
        (3L, "bob", "marketing", 150.0, "Creating campaign for product launch"),
        (4L, "dave", "engineering", 50.0, "Setting up development environment")
      ).toDF("id", "name", "department", "score", "content")
        .repartition(1)
        .write
        .parquet(batch1Path)

      Seq(
        (5L, "alice", "engineering", 300.0, "Deploying search service to production"),
        (6L, "bob", "sales", 250.0, "Closing deal with enterprise customer"),
        (7L, "carol", "engineering", 175.0, "Writing integration tests for search"),
        (8L, "carol", "sales", 125.0, "Preparing sales report for quarter")
      ).toDF("id", "name", "department", "score", "content")
        .repartition(1)
        .write
        .parquet(batch2Path)

      // Build companion from batch1 (creates first split)
      val modesClause = if (indexingModes.nonEmpty) s"INDEXING MODES ($indexingModes) " else ""
      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR PARQUET '$batch1Path' ${modesClause}AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      // Combine and re-sync to get a second split
      new File(combinedPath).mkdirs()
      new File(batch1Path).listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
        java.nio.file.Files.copy(f.toPath, new File(combinedPath, f.getName).toPath)
      }
      new File(batch2Path).listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
        java.nio.file.Files.copy(f.toPath, new File(combinedPath, "batch2_" + f.getName).toPath)
      }

      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR PARQUET '$combinedPath' ${modesClause}AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      // Verify multiple splits before merge
      val txLogBefore = TransactionLogFactory.create(new Path(indexPath), spark)
      val splitCountBefore =
        try txLogBefore.listFiles().size
        finally txLogBefore.close()
      splitCountBefore should be >= 2

      flushCaches()

      // Merge
      spark.sql(s"MERGE SPLITS '$indexPath' TARGET SIZE 100M").collect()

      // Verify merge reduced split count
      val txLogAfter = TransactionLogFactory.create(new Path(indexPath), spark)
      try
        txLogAfter.listFiles().size should be < splitCountBefore
      finally
        txLogAfter.close()

      flushCaches()

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      f(df)
    } finally
      deleteRecursively(new File(tempDir))
  }

  // ==========================================================================
  //  COUNT / basic aggregations
  // ==========================================================================

  test("COUNT(*) after merge") {
    withMergedCompanion()(df => df.count() shouldBe 8)
  }

  test("COUNT(string_field) after merge") {
    withMergedCompanion() { df =>
      df.agg(count("name")).collect()(0).getLong(0) shouldBe 8L
      df.agg(count("department")).collect()(0).getLong(0) shouldBe 8L
    }
  }

  test("COUNT(numeric_field) after merge") {
    withMergedCompanion() { df =>
      df.agg(count("score")).collect()(0).getLong(0) shouldBe 8L
      df.agg(count("id")).collect()(0).getLong(0) shouldBe 8L
    }
  }

  test("SUM after merge") {
    withMergedCompanion() { df =>
      // 100+200+150+50+300+250+175+125 = 1350
      df.agg(sum("score")).collect()(0).getDouble(0) shouldBe 1350.0
    }
  }

  test("AVG after merge") {
    withMergedCompanion()(df => df.agg(avg("score")).collect()(0).getDouble(0) shouldBe (1350.0 / 8.0) +- 0.01)
  }

  test("MIN and MAX on numeric field after merge") {
    withMergedCompanion() { df =>
      df.agg(min("score")).collect()(0).getDouble(0) shouldBe 50.0
      df.agg(max("score")).collect()(0).getDouble(0) shouldBe 300.0
    }
  }

  test("MIN and MAX on Long field after merge") {
    withMergedCompanion() { df =>
      df.agg(min("id")).collect()(0).getLong(0) shouldBe 1L
      df.agg(max("id")).collect()(0).getLong(0) shouldBe 8L
    }
  }

  test("multiple aggregations in single query after merge") {
    withMergedCompanion() { df =>
      val row = df
        .agg(
          count("*"),
          sum("score"),
          avg("score"),
          min("score"),
          max("score")
        )
        .collect()(0)

      row.getLong(0) shouldBe 8L
      row.getDouble(1) shouldBe 1350.0
      row.getDouble(2) shouldBe (1350.0 / 8.0) +- 0.01
      row.getDouble(3) shouldBe 50.0
      row.getDouble(4) shouldBe 300.0
    }
  }

  // ==========================================================================
  //  GROUP BY
  // ==========================================================================

  test("GROUP BY string column + COUNT after merge") {
    withMergedCompanion() { df =>
      val grouped = df.groupBy("name").agg(count("*").as("cnt")).orderBy("name").collect()
      grouped.length shouldBe 4
      val m = grouped.map(r => r.getString(0) -> r.getLong(1)).toMap
      m("alice") shouldBe 3L
      m("bob") shouldBe 2L
      m("carol") shouldBe 2L
      m("dave") shouldBe 1L
    }
  }

  test("GROUP BY string column + SUM after merge") {
    withMergedCompanion() { df =>
      // alice: 100+200+300=600, bob: 150+250=400, carol: 175+125=300, dave: 50
      val grouped = df.groupBy("name").agg(sum("score").as("total")).orderBy("name").collect()
      val m       = grouped.map(r => r.getString(0) -> r.getDouble(1)).toMap
      m("alice") shouldBe 600.0
      m("bob") shouldBe 400.0
      m("carol") shouldBe 300.0
      m("dave") shouldBe 50.0
    }
  }

  test("GROUP BY string column + AVG after merge") {
    withMergedCompanion() { df =>
      val grouped = df.groupBy("name").agg(avg("score").as("average")).orderBy("name").collect()
      val m       = grouped.map(r => r.getString(0) -> r.getDouble(1)).toMap
      m("alice") shouldBe 200.0 +- 0.01
      m("bob") shouldBe 200.0 +- 0.01
      m("carol") shouldBe 150.0 +- 0.01
      m("dave") shouldBe 50.0 +- 0.01
    }
  }

  test("GROUP BY string column + MIN/MAX after merge") {
    withMergedCompanion() { df =>
      val grouped = df
        .groupBy("name")
        .agg(
          min("score").as("min_score"),
          max("score").as("max_score")
        )
        .orderBy("name")
        .collect()
      val m = grouped.map(r => r.getString(0) -> (r.getDouble(1), r.getDouble(2))).toMap
      m("alice") shouldBe (100.0, 300.0)
      m("bob") shouldBe (150.0, 250.0)
      m("carol") shouldBe (125.0, 175.0)
      m("dave") shouldBe (50.0, 50.0)
    }
  }

  test("GROUP BY string + multiple aggregations after merge") {
    withMergedCompanion() { df =>
      val grouped = df
        .groupBy("name")
        .agg(
          count("*").as("cnt"),
          sum("score").as("total"),
          avg("score").as("average"),
          min("score").as("lo"),
          max("score").as("hi")
        )
        .orderBy("name")
        .collect()

      grouped.length shouldBe 4

      val alice = grouped(0)
      alice.getString(0) shouldBe "alice"
      alice.getLong(1) shouldBe 3L
      alice.getDouble(2) shouldBe 600.0
      alice.getDouble(3) shouldBe 200.0 +- 0.01
      alice.getDouble(4) shouldBe 100.0
      alice.getDouble(5) shouldBe 300.0
    }
  }

  test("GROUP BY second string column (department) after merge") {
    withMergedCompanion() { df =>
      // engineering: 5 rows (alice x3, dave x1, carol x1), marketing: 1 (bob), sales: 2 (bob, carol)
      val grouped = df.groupBy("department").agg(count("*").as("cnt")).orderBy("department").collect()
      grouped.length shouldBe 3
      val m = grouped.map(r => r.getString(0) -> r.getLong(1)).toMap
      m("engineering") shouldBe 5L
      m("marketing") shouldBe 1L
      m("sales") shouldBe 2L
    }
  }

  test("GROUP BY department + SUM(score) after merge") {
    withMergedCompanion() { df =>
      // engineering: 100+200+50+300+175=825, marketing: 150, sales: 250+125=375
      val grouped = df.groupBy("department").agg(sum("score").as("total")).orderBy("department").collect()
      val m       = grouped.map(r => r.getString(0) -> r.getDouble(1)).toMap
      m("engineering") shouldBe 825.0
      m("marketing") shouldBe 150.0
      m("sales") shouldBe 375.0
    }
  }

  // ==========================================================================
  //  Filters on string fields
  // ==========================================================================

  test("EqualTo on string field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("name") === "alice").collect()
      result.length shouldBe 3
      result.foreach(_.getString(result(0).fieldIndex("name")) shouldBe "alice")
    }
  }

  test("EqualTo with no match after merge") {
    withMergedCompanion()(df => df.filter(col("name") === "zzzz").collect().length shouldBe 0)
  }

  test("In filter on string field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("name").isin("alice", "carol")).collect()
      result.length shouldBe 5 // alice=3, carol=2
    }
  }

  test("Not EqualTo on string field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("name") =!= "alice").collect()
      result.length shouldBe 5 // bob=2, carol=2, dave=1
    }
  }

  test("And filter on two string fields after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("name") === "bob" && col("department") === "sales").collect()
      result.length shouldBe 1
      result(0).getDouble(result(0).fieldIndex("score")) shouldBe 250.0
    }
  }

  test("Or filter on string field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("name") === "dave" || col("name") === "carol").collect()
      result.length shouldBe 3 // dave=1, carol=2
    }
  }

  test("StringStartsWith after merge") {
    withMergedCompanion() { df =>
      // "alice", "bob", "carol", "dave" — names starting with 'a' or 'b': alice=3, bob=2
      val result = df.filter(col("name").startsWith("a")).collect()
      result.length shouldBe 3
    }
  }

  test("StringContains after merge") {
    withMergedCompanion() { df =>
      // "engineering", "marketing", "sales" — contains "ing": engineering=5, marketing=1
      val result = df.filter(col("department").contains("ing")).collect()
      result.length shouldBe 6
    }
  }

  test("StringEndsWith after merge") {
    withMergedCompanion() { df =>
      // names ending with 'e': alice=3, dave=1
      val result = df.filter(col("name").endsWith("e")).collect()
      result.length shouldBe 4
    }
  }

  // ==========================================================================
  //  Filters on numeric fields
  // ==========================================================================

  test("EqualTo on Long field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("id") === 5L).collect()
      result.length shouldBe 1
      result(0).getString(result(0).fieldIndex("name")) shouldBe "alice"
    }
  }

  test("GreaterThan on numeric field after merge") {
    withMergedCompanion() { df =>
      // score > 200: 250, 300, 175 is not > 200. Actually: 250, 300 = 2 rows
      // Wait: 100,200,150,50,300,250,175,125. > 200: 300, 250 = 2
      val result = df.filter(col("score") > 200.0).collect()
      result.length shouldBe 2
    }
  }

  test("GreaterThanOrEqual on numeric field after merge") {
    withMergedCompanion() { df =>
      // >= 200: 200, 300, 250 = 3
      val result = df.filter(col("score") >= 200.0).collect()
      result.length shouldBe 3
    }
  }

  test("LessThan on numeric field after merge") {
    withMergedCompanion() { df =>
      // < 125: 100, 50 = 2
      val result = df.filter(col("score") < 125.0).collect()
      result.length shouldBe 2
    }
  }

  test("LessThanOrEqual on numeric field after merge") {
    withMergedCompanion() { df =>
      // <= 125: 100, 50, 125 = 3
      val result = df.filter(col("score") <= 125.0).collect()
      result.length shouldBe 3
    }
  }

  test("range filter on Long field after merge") {
    withMergedCompanion() { df =>
      // id >= 3 AND id <= 6: rows 3,4,5,6
      val result = df.filter(col("id") >= 3L && col("id") <= 6L).collect()
      result.length shouldBe 4
    }
  }

  test("In filter on numeric field after merge") {
    withMergedCompanion() { df =>
      val result = df.filter(col("id").isin(1L, 4L, 7L)).collect()
      result.length shouldBe 3
    }
  }

  // ==========================================================================
  //  Combined filters
  // ==========================================================================

  test("string EqualTo + numeric range after merge") {
    withMergedCompanion() { df =>
      // alice with score > 150: rows with score 200, 300
      val result = df.filter(col("name") === "alice" && col("score") > 150.0).collect()
      result.length shouldBe 2
    }
  }

  test("In on string + GreaterThan on numeric after merge") {
    withMergedCompanion() { df =>
      // name in (bob, carol) AND score >= 200: bob/250, carol has 175 and 125 — only bob/250
      val result = df.filter(col("name").isin("bob", "carol") && col("score") >= 200.0).collect()
      result.length shouldBe 1
      result(0).getString(result(0).fieldIndex("name")) shouldBe "bob"
    }
  }

  test("Or across different field types after merge") {
    withMergedCompanion() { df =>
      // name == 'dave' OR score > 250: dave(50) + alice(300) = 2
      val result = df.filter(col("name") === "dave" || col("score") > 250.0).collect()
      result.length shouldBe 2
    }
  }

  // ==========================================================================
  //  Filter + aggregation
  // ==========================================================================

  test("filtered COUNT(*) after merge") {
    withMergedCompanion() { df =>
      df.filter(col("name") === "alice").count() shouldBe 3
      df.filter(col("department") === "engineering").count() shouldBe 5
    }
  }

  test("filtered SUM after merge") {
    withMergedCompanion() { df =>
      // alice: 100+200+300=600
      val result = df.filter(col("name") === "alice").agg(sum("score")).collect()(0).getDouble(0)
      result shouldBe 600.0
    }
  }

  test("filtered GROUP BY after merge") {
    withMergedCompanion() { df =>
      // engineering only: alice=3, dave=1, carol=1
      val grouped = df
        .filter(col("department") === "engineering")
        .groupBy("name")
        .agg(count("*").as("cnt"))
        .orderBy("name")
        .collect()
      grouped.length shouldBe 3
      val m = grouped.map(r => r.getString(0) -> r.getLong(1)).toMap
      m("alice") shouldBe 3L
      m("carol") shouldBe 1L
      m("dave") shouldBe 1L
    }
  }

  test("numeric range filter + GROUP BY + SUM after merge") {
    withMergedCompanion() { df =>
      // score >= 150: alice(200,300), bob(150,250), carol(175), dave excluded
      val grouped = df
        .filter(col("score") >= 150.0)
        .groupBy("name")
        .agg(sum("score").as("total"))
        .orderBy("name")
        .collect()
      val m = grouped.map(r => r.getString(0) -> r.getDouble(1)).toMap
      m("alice") shouldBe 500.0 // 200+300
      m("bob") shouldBe 400.0   // 150+250
      m("carol") shouldBe 175.0
      m.get("dave") shouldBe None
    }
  }

  // ==========================================================================
  //  IndexQuery (text search) after merge
  // ==========================================================================

  test("IndexQuery text search after merge") {
    withMergedCompanion("'content':'text'") { df =>
      df.createOrReplaceTempView("merged_companion_iq")

      // "search" appears in: row 1 (search backend), row 2 (pull requests for search),
      // row 5 (search service), row 7 (tests for search) = 4 rows
      val result = spark
        .sql(
          "SELECT * FROM merged_companion_iq WHERE content indexquery 'search'"
        )
        .collect()
      result.length shouldBe 4
    }
  }

  test("IndexQuery phrase search after merge") {
    withMergedCompanion("'content':'text'") { df =>
      df.createOrReplaceTempView("merged_companion_phrase")

      val result = spark
        .sql(
          "SELECT * FROM merged_companion_phrase WHERE content indexquery '\"pull requests\"'"
        )
        .collect()
      result.length shouldBe 1
    }
  }

  test("IndexQuery with no match after merge") {
    withMergedCompanion("'content':'text'") { df =>
      df.createOrReplaceTempView("merged_companion_nomatch")

      val result = spark
        .sql(
          "SELECT * FROM merged_companion_nomatch WHERE content indexquery 'zzzznotfound'"
        )
        .collect()
      result.length shouldBe 0
    }
  }

  test("IndexQuery combined with EqualTo filter after merge") {
    withMergedCompanion("'content':'text'") { df =>
      df.createOrReplaceTempView("merged_companion_combined_iq")

      // "search" in content AND name = alice: rows 1, 2, 5 (alice with search)
      val result = spark
        .sql(
          "SELECT * FROM merged_companion_combined_iq WHERE content indexquery 'search' AND name = 'alice'"
        )
        .collect()
      result.length shouldBe 3
    }
  }

  // ==========================================================================
  //  Full data readback
  // ==========================================================================

  test("full table scan returns all rows with correct values after merge") {
    withMergedCompanion() { df =>
      val rows = df.orderBy("id").collect()
      rows.length shouldBe 8

      rows(0).getLong(rows(0).fieldIndex("id")) shouldBe 1L
      rows(0).getString(rows(0).fieldIndex("name")) shouldBe "alice"
      rows(0).getDouble(rows(0).fieldIndex("score")) shouldBe 100.0

      rows(7).getLong(rows(7).fieldIndex("id")) shouldBe 8L
      rows(7).getString(rows(7).fieldIndex("name")) shouldBe "carol"
      rows(7).getDouble(rows(7).fieldIndex("score")) shouldBe 125.0
    }
  }

  test("select specific columns after merge") {
    withMergedCompanion() { df =>
      val rows = df.select("name", "score").orderBy("score").collect()
      rows.length shouldBe 8
      rows(0).getString(0) shouldBe "dave"
      rows(0).getDouble(1) shouldBe 50.0
      rows(7).getString(0) shouldBe "alice"
      rows(7).getDouble(1) shouldBe 300.0
    }
  }
}
