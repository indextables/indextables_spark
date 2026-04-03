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
import org.apache.spark.sql.functions._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests for the text_and_string indexing mode in companion splits.
 *
 * Validates that text_and_string creates dual tantivy fields (raw string + tokenized text):
 *   - TEXTSEARCH/indexquery auto-routes to __text field
 *   - Exact match uses raw string field
 *   - Aggregations use raw string field
 *   - SELECT * returns single column (no __text exposed)
 *   - Edge cases: empty strings, long strings, special characters, multiple columns
 */
class CompanionTextAndStringTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionTextAndStringTest")
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
    val path = Files.createTempDirectory("tantivy4spark-text-and-string").toString
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

  private val longString = "word " * 2000 // ~10KB

  private def createTestData(deltaPath: String): Unit = {
    val ss = spark
    import ss.implicits._
    Seq(
      (1L, "the quick brown fox jumps over the lazy dog", "server started successfully", 100.0),
      (2L, "hello world from indextables", "connection timeout on port 8080", 200.0),
      (3L, "error: connection timeout on port 8080", "server started successfully", 300.0),
      (4L, "", "empty message test", 400.0),
      (5L, longString.trim, "long message test", 500.0),
      (6L, "special: \"quotes\" and\nnewlines and emojis", "unicode test", 600.0)
    ).toDF("id", "message", "description", "score")
      .write.format("delta").mode("overwrite").save(deltaPath)
  }

  private def buildCompanion(deltaPath: String, indexPath: String, extraClauses: String = ""): Unit = {
    val result = spark.sql(
      s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
         |  INDEXING MODES ('message': 'text_and_string', 'description': 'text_and_string')
         |  $extraClauses
         |  AT LOCATION '$indexPath'""".stripMargin
    ).collect()
    result(0).getString(2) shouldBe "success"
  }

  private def readCompanion(indexPath: String): DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .option("spark.indextables.read.columnar.enabled", "true")
      .load(indexPath)

  // ═══════════════════════════════════════════════════════════════════
  //  Core functionality
  // ═══════════════════════════════════════════════════════════════════

  test("TEXTSEARCH finds results via tokenized text field") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_1")

      val results = spark.sql("SELECT id FROM tas_test_1 WHERE message indexquery 'quick brown'").collect()
      results.length shouldBe 1
    }
  }

  test("exact match finds results via raw string field") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      val results = df.filter(col("message") === "hello world from indextables").collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 2L
    }
  }

  test("COUNT(*) works") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      readCompanion(indexPath).count() shouldBe 6
    }
  }

  test("SELECT * returns single message column, no __text") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val columns = readCompanion(indexPath).columns
      columns should contain("message")
      columns should not contain "message__text"
      columns should contain("description")
      columns should not contain "description__text"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Functional tests
  // ═══════════════════════════════════════════════════════════════════

  test("* TEXTSEARCH with text_and_string does not return duplicate hits") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_star")

      // "timeout" appears in both message (row 3) and description (row 2)
      // Should not get duplicate rows from raw + __text fields
      val results = spark.sql("SELECT id FROM tas_test_star WHERE indexqueryall('timeout')").collect()
      val ids = results.map(_.getLong(0)).toSet
      ids should contain(2L) // description has "connection timeout"
      ids should contain(3L) // message has "connection timeout"
      // Each row should appear at most once
      results.length shouldBe ids.size
    }
  }

  test("multiple text_and_string columns work independently") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_multi")

      // Search message field
      val msgResults = spark.sql("SELECT id FROM tas_test_multi WHERE message indexquery 'quick'").collect()
      msgResults.length shouldBe 1
      msgResults(0).getLong(0) shouldBe 1L

      // Search description field
      val descResults = spark.sql("SELECT id FROM tas_test_multi WHERE description indexquery 'started'").collect()
      descResults.length shouldBe 2 // rows 1 and 3
    }
  }

  test("text_and_string with INCLUDE COLUMNS") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'message')
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_incl")

      // TEXTSEARCH on included text_and_string column should work
      val results = spark.sql("SELECT id FROM tas_test_incl WHERE message indexquery 'quick'").collect()
      results.length shouldBe 1

      // Exact match on included column should work
      val exactResults = df.filter(col("message") === "hello world from indextables").collect()
      exactResults.length shouldBe 1
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Edge cases
  // ═══════════════════════════════════════════════════════════════════

  test("empty string values do not crash dual-field indexing") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      // Row 4 has empty message — should be indexed without crash
      val emptyMatch = df.filter(col("message") === "").collect()
      emptyMatch.length shouldBe 1
      emptyMatch(0).getLong(0) shouldBe 4L
    }
  }

  test("very long string values: tokenized search works") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_long")

      // TEXTSEARCH on long string — tokenized "word" appears many times
      val textResults = spark.sql("SELECT id FROM tas_test_long WHERE message indexquery 'word'").collect()
      textResults.length shouldBe 1
      textResults(0).getLong(0) shouldBe 5L

      // Note: exact match on 10KB strings exceeds tantivy's raw tokenizer max token length (255 bytes),
      // so exact match won't find it. This is expected tantivy behavior, not a text_and_string bug.
      df.count() shouldBe 6
    }
  }

  test("special characters in strings work for both search paths") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_special")

      // TEXTSEARCH on word within special-character string
      val results = spark.sql("SELECT id FROM tas_test_special WHERE message indexquery 'special'").collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 6L

      // Exact match on the full special-character string
      val exactResults = df.filter(col("message") === "special: \"quotes\" and\nnewlines and emojis").collect()
      exactResults.length shouldBe 1
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Query correctness
  // ═══════════════════════════════════════════════════════════════════

  test("tokenized vs exact semantics: TEXTSEARCH matches tokens, exact match requires full string") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_semantics")

      // TEXTSEARCH for "quick" finds row 1 (tokenized search matches single word)
      val textResults = spark.sql("SELECT id FROM tas_test_semantics WHERE message indexquery 'quick'").collect()
      textResults.length shouldBe 1
      textResults(0).getLong(0) shouldBe 1L

      // Exact match for "quick" does NOT find row 1 (requires full string match)
      val exactResults = df.filter(col("message") === "quick").collect()
      exactResults.length shouldBe 0
    }
  }

  test("phrase query via indexquery matches via __text field") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_phrase")

      // Phrase query: "quick brown" should match row 1
      val results = spark.sql("""SELECT id FROM tas_test_phrase WHERE message indexquery '"quick brown"'""").collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 1L

      // Phrase query: "brown quick" should NOT match (wrong order)
      val wrongOrder = spark.sql("""SELECT id FROM tas_test_phrase WHERE message indexquery '"brown quick"'""").collect()
      wrongOrder.length shouldBe 0
    }
  }

  test("aggregation uses string field: GROUP BY returns full raw strings") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create data with duplicate descriptions for grouping
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)

      // GROUP BY description — "server started successfully" appears twice (rows 1, 3)
      val grouped = df.groupBy("description").count().collect()
      val serverStarted = grouped.find(_.getString(0) == "server started successfully")
      serverStarted shouldBe defined
      serverStarted.get.getLong(1) shouldBe 2
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Collision and feature combination tests
  // ═══════════════════════════════════════════════════════════════════

  test("text_and_string rejects column name collision with __text") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create Delta table with a column that collides with the __text companion field name
      val ss = spark
      import ss.implicits._
      Seq(
        ("hello", "world"),
        ("foo", "bar")
      ).toDF("message", "message__text")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("message__text")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Mixed Catalyst predicate tests — single-pass pushdown validation
  // ═══════════════════════════════════════════════════════════════════

  test("mixed exact match OR indexquery on same text_and_string column in single query") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_mixed_or")

      // OR: exact match on raw field (row 2) OR tokenized search on __text (row 1)
      val orResults = spark.sql(
        """SELECT id FROM tas_mixed_or
          |WHERE message = 'hello world from indextables' OR message indexquery 'quick brown'""".stripMargin
      ).collect()
      val orIds = orResults.map(_.getLong(0)).toSet
      orIds should contain(1L) // "quick brown" matches via __text
      orIds should contain(2L) // exact match via raw field
      orResults.length shouldBe 2

      // Dedup: row 1 matches BOTH branches (exact match on full string + tokenized "quick brown")
      // Should appear exactly once, not duplicated
      val dedupResults = spark.sql(
        """SELECT id FROM tas_mixed_or
          |WHERE message = 'the quick brown fox jumps over the lazy dog' OR message indexquery 'quick brown'""".stripMargin
      ).collect()
      dedupResults.length shouldBe 1 // exactly one row, no duplicates, no spurious matches
      dedupResults(0).getLong(0) shouldBe 1L
    }
  }

  test("mixed exact match AND indexquery on same text_and_string column in single query") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_mixed_and")

      // AND: exact match on raw field AND tokenized search on __text — both must match same row
      val andResults = spark.sql(
        """SELECT id FROM tas_mixed_and
          |WHERE message = 'hello world from indextables' AND message indexquery 'hello world'""".stripMargin
      ).collect()
      andResults.length shouldBe 1
      andResults(0).getLong(0) shouldBe 2L

      // AND with contradicting predicates: exact match won't match tokenized-only query
      val emptyResults = spark.sql(
        """SELECT id FROM tas_mixed_and
          |WHERE message = 'hello world from indextables' AND message indexquery 'quick brown'""".stripMargin
      ).collect()
      emptyResults.length shouldBe 0
    }
  }

  test("aggregation after indexquery filter on text_and_string column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath, "HASHED FASTFIELDS INCLUDE ('description')")

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_mixed_agg")

      // indexquery filters via __text, then GROUP BY uses raw field
      val results = spark.sql(
        """SELECT description, COUNT(*) as cnt FROM tas_mixed_agg
          |WHERE description indexquery 'server started'
          |GROUP BY description""".stripMargin
      ).collect()
      results.length shouldBe 1
      results(0).getString(0) shouldBe "server started successfully"
      results(0).getLong(1) shouldBe 2 // rows 1 and 3
    }
  }

  test("mixed predicates across multiple text_and_string columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_mixed_cross")

      // indexquery on message.__text AND exact match on description raw field
      val results = spark.sql(
        """SELECT id FROM tas_mixed_cross
          |WHERE message indexquery 'quick' AND description = 'server started successfully'""".stripMargin
      ).collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 1L // row 1: message has "quick", description is "server started successfully"

      // Reverse: exact match on message AND indexquery on description
      val reverseResults = spark.sql(
        """SELECT id FROM tas_mixed_cross
          |WHERE message = 'hello world from indextables' AND description indexquery 'timeout'""".stripMargin
      ).collect()
      reverseResults.length shouldBe 1
      reverseResults(0).getLong(0) shouldBe 2L // row 2: message exact, description has "timeout"

      // Cross-column OR: exact match on message OR exact match on description
      val crossOrResults = spark.sql(
        """SELECT id FROM tas_mixed_cross
          |WHERE message = 'hello world from indextables' OR description = 'server started successfully'""".stripMargin
      ).collect()
      val crossOrIds = crossOrResults.map(_.getLong(0)).toSet
      crossOrIds should contain(1L) // description = "server started successfully"
      crossOrIds should contain(2L) // message = "hello world from indextables"
      crossOrIds should contain(3L) // description = "server started successfully"
      crossOrResults.length shouldBe crossOrIds.size
    }
  }

  test("mixed predicates do not split into multiple scan stages") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_plan_check")

      val filtered = spark.sql(
        """SELECT id FROM tas_plan_check
          |WHERE message = 'hello world from indextables' AND message indexquery 'hello world'""".stripMargin
      )

      val planString = filtered.queryExecution.executedPlan.toString

      // Verify single BatchScan — no subquery/union splitting the query into multiple scans
      val batchScanCount = "BatchScan ".r.findAllIn(planString).length
      withClue(s"Expected single BatchScan but found $batchScanCount in plan:\n$planString\n") {
        batchScanCount shouldBe 1
      }

      // Verify no FilterExec above the scan — filters are pushed down, not post-filtered
      val hasFilterExec = planString.contains("Filter ")
      withClue(s"FilterExec found above BatchScan — filters not fully pushed down:\n$planString\n") {
        hasFilterExec shouldBe false
      }

      // Verify the query still returns correct results
      val results = filtered.collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 2L
    }
  }

  test("text_and_string with HASHED FASTFIELDS enables aggregation and text search") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create data with repeated message values for grouping
      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "the quick brown fox", 100.0),
        (2L, "hello world from indextables", 200.0),
        (3L, "the quick brown fox", 300.0),
        (4L, "error connection timeout", 400.0),
        (5L, "hello world from indextables", 500.0)
      ).toDF("id", "message", "score")
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Build companion with both HASHED FASTFIELDS and text_and_string
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  HASHED FASTFIELDS INCLUDE ('message')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_test_hashed")

      // 1. TEXTSEARCH finds results (tokenized search via __text)
      val textResults = spark.sql("SELECT id FROM tas_test_hashed WHERE message indexquery 'quick brown'").collect()
      textResults.length shouldBe 2
      val textIds = textResults.map(_.getLong(0)).toSet
      textIds should contain(1L)
      textIds should contain(3L)

      // 2. GROUP BY / COUNT aggregation works (via hashed fast field on raw field)
      val grouped = df.groupBy("message").count().collect()
      grouped.length shouldBe 3
      val groupMap = grouped.map(r => r.getString(0) -> r.getLong(1)).toMap
      groupMap("the quick brown fox") shouldBe 2L
      groupMap("hello world from indextables") shouldBe 2L
      groupMap("error connection timeout") shouldBe 1L
    }
  }
}
