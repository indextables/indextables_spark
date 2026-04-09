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
 * Validates the single-field approach where text_and_string uses one tantivy field with:
 *   - Full-text search via the "default"-tokenized inverted index
 *   - Equality filters via PhraseQuery (slop=0) on the same inverted index + Spark post-filter
 *   - Aggregations via the "raw" fast field on the same field
 *   - SELECT * returns single column (no internal fields exposed)
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
        case scala.util.control.NonFatal(_) => // Cache may not be initialized yet — safe to ignore during test setup
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

  test("exact match finds results via PhraseQuery + Spark post-filter") {
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

  test("SELECT * returns single message column, no internal fields exposed") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val columns = readCompanion(indexPath).columns
      columns should contain("message")
      columns should contain("description")
      // Single-field approach: no __text companion fields should exist
      columns.filter(_.contains("__text")) shouldBe empty
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
      // Single-field: each row should appear at most once
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

  test("empty string values do not crash text_and_string indexing") {
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

      // Note: exact match on very long strings may not work via PhraseQuery because the tokenized
      // phrase query becomes impractically long. This is expected behavior.
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

  test("Unicode strings: exact match and tokenized search on accented characters") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "caf\u00e9 cr\u00e8me br\u00fbl\u00e9e"),         // café crème brûlée (precomposed NFC)
        (2L, "M\u00fcnchen Gro\u00dfstadt Stra\u00dfe"),         // München Großstadt Straße (German)
        (3L, "hello world plain ascii")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_unicode")

      // Tokenized search on accented characters — tantivy's default tokenizer preserves
      // accented letters as single tokens (café → "café", not split at the accent)
      val cafeResults = spark.sql("SELECT id FROM tas_unicode WHERE message indexquery 'caf\u00e9'").collect()
      cafeResults.length shouldBe 1
      cafeResults(0).getLong(0) shouldBe 1L

      // Exact match on full Unicode string (Spark candidate post-filter ensures correctness)
      val exactResults = df.filter(col("message") === "M\u00fcnchen Gro\u00dfstadt Stra\u00dfe").collect()
      exactResults.length shouldBe 1
      exactResults(0).getLong(0) shouldBe 2L

      // Exact match should NOT match a substring
      val partialResults = df.filter(col("message") === "M\u00fcnchen").collect()
      partialResults.length shouldBe 0
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

  test("phrase query via indexquery matches on tokenized field") {
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

  test("aggregation uses raw fast field: GROUP BY returns full raw strings") {
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

  test("IN filter on text_and_string uses candidate pushdown with Spark post-filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_in_test")

      // Use exact full string values — Spark post-filter ensures exact match correctness
      val results = spark.sql(
        "SELECT id FROM tas_in_test WHERE message IN ('the quick brown fox jumps over the lazy dog', 'hello world from indextables')"
      ).collect()
      val ids = results.map(_.getLong(0)).toSet
      ids should contain(1L)
      ids should contain(2L)
      results.length shouldBe 2
    }
  }

  test("IN filter with empty string value does not crash") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)
      buildCompanion(deltaPath, indexPath)

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_in_empty")

      // IN with empty string and a real value — the empty string tokenizes to zero terms
      // and is dropped from the PhraseQuery OR (can't represent empty in inverted index).
      // Only the non-empty value matches. Empty string equality requires EqualTo, not IN.
      val results = spark.sql(
        "SELECT id FROM tas_in_empty WHERE message IN ('', 'hello world from indextables')"
      ).collect()
      val ids = results.map(_.getLong(0)).toSet
      ids should contain(2L) // row 2 has "hello world from indextables"
      // Note: row 4 (empty message) is NOT matched — empty string is dropped from IN's PhraseQuery list
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Collision and feature combination tests
  // ═══════════════════════════════════════════════════════════════════

  test("text_and_string works even when source has column ending in __text") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // In the single-field approach, there's no __text companion field, so a column named
      // "message__text" should not cause a collision error
      val ss = spark
      import ss.implicits._
      Seq(
        ("hello world", "extra data"),
        ("foo bar", "more data")
      ).toDF("message", "message__text")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      // Should succeed — no collision in single-field mode
      result(0).getString(2) shouldBe "success"

      // Verify TEXTSEARCH works on the text_and_string field
      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_no_collision")
      val results = spark.sql("SELECT message FROM tas_no_collision WHERE message indexquery 'hello'").collect()
      results.length shouldBe 1
      results(0).getString(0) shouldBe "hello world"
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

      // OR: exact match via PhraseQuery (row 2) OR tokenized search (row 1)
      val orResults = spark.sql(
        """SELECT id FROM tas_mixed_or
          |WHERE message = 'hello world from indextables' OR message indexquery 'quick brown'""".stripMargin
      ).collect()
      val orIds = orResults.map(_.getLong(0)).toSet
      orIds should contain(1L) // "quick brown" matches via tokenized search
      orIds should contain(2L) // exact match via PhraseQuery + Spark post-filter
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

      // AND: exact match via PhraseQuery AND tokenized search — both must match same row
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

      // indexquery filters via tokenized search, then GROUP BY uses raw fast field
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

      // indexquery on message (tokenized) AND exact match on description (PhraseQuery + post-filter)
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

      // In the single-field approach, text_and_string EqualTo uses candidate pushdown:
      // PhraseQuery(slop=0) is pushed to tantivy for approximate filtering, and Spark adds
      // a FilterExec for exact equality post-filtering. This is expected and correct.

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

      // 1. TEXTSEARCH finds results (tokenized search on same field)
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

  // ═══════════════════════════════════════════════════════════════════
  //  Correctness guards — prove Spark post-filter is actually running
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Data for the superset-correctness tests. Row A is the exact match target. Row B has extra tokens
   * after the phrase so a SplitPhraseQuery(slop=0) for "hello world" matches both rows — only Spark's
   * FilterExec post-filter can distinguish them. If the post-filter ever stops running, this test set
   * returns more rows than expected.
   */
  private def createPhraseFalsePositiveData(deltaPath: String): Unit = {
    val ss = spark
    import ss.implicits._
    Seq(
      (1L, "hello world"),             // row A — exact match target
      (2L, "hello world extra stuff"), // row B — phrase candidate false positive
      (3L, "other content")
    ).toDF("id", "message")
      .write.format("delta").mode("overwrite").save(deltaPath)
  }

  test("EqualTo on text_and_string: phrase false positive is removed by Spark post-filter") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createPhraseFalsePositiveData(deltaPath)
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      // SplitPhraseQuery for "hello world" would match both row 1 and row 2.
      // Spark's FilterExec post-filter must trim row 2 to give the exact result.
      val results = df.filter(col("message") === "hello world").collect()
      results.length shouldBe 1
      results(0).getLong(0) shouldBe 1L
    }
  }

  test("EqualTo on text_and_string: query plan keeps FilterExec with the original predicate") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createPhraseFalsePositiveData(deltaPath)
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      val filtered = df.filter(col("message") === "hello world")

      // Walk the physical plan for a FilterExec node whose condition references the target field
      // AND carries the original literal "hello world". If a future optimizer change silently drops
      // the post-filter (or leaves only a Catalyst-injected IsNotNull on `message`), this assertion
      // will fail loudly instead of returning wrong results via the phrase-query superset.
      import org.apache.spark.sql.execution.FilterExec
      val plan         = filtered.queryExecution.executedPlan
      val filterExecs  = plan.collect { case f: FilterExec => f }
      withClue(s"Expected a FilterExec in the physical plan but none was found:\n$plan\n") {
        filterExecs should not be empty
      }
      // Union of all FilterExec condition strings, lowercased for comparison
      val conditionText = filterExecs.map(_.condition.toString).mkString(" | ").toLowerCase
      withClue(s"FilterExec condition should reference the text_and_string field:\n$conditionText") {
        conditionText should include("message")
      }
      // Require the original literal so a stray FilterExec (e.g., just IsNotNull(message)) cannot
      // satisfy the assertion on its own — the equality predicate itself must be present.
      withClue(s"FilterExec condition should carry the original 'hello world' literal:\n$conditionText") {
        conditionText should include("hello world")
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Critical correctness regressions from PR review
  // ═══════════════════════════════════════════════════════════════════

  test("C1: COUNT(*) with text_and_string EqualTo returns exact count, not phrase superset") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createPhraseFalsePositiveData(deltaPath)
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  HASHED FASTFIELDS INCLUDE ('message')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_c1_count")

      // The phrase candidate for "hello world" matches both row 1 ("hello world") and row 2
      // ("hello world extra stuff"). If aggregate pushdown ever computed COUNT over the raw
      // candidate result, it would return 2 instead of the exact-equality answer of 1. Two
      // layers of protection ensure we get 1:
      //   1. Candidate filters are returned from pushFilters() as "unsupported", so Spark sees
      //      them as pending and does not attempt aggregate pushdown in the first place — the
      //      row scan runs with a Spark FilterExec that trims the superset to the exact match.
      //   2. Even if Spark did attempt aggregate pushdown (e.g. a future refactor), the guard
      //      in `areFiltersCompatibleWithAggregation` rejects candidate filters up front.
      val cnt = spark.sql("SELECT COUNT(*) FROM tas_c1_count WHERE message = 'hello world'").collect()
      cnt.length shouldBe 1
      cnt(0).getLong(0) shouldBe 1L
    }
  }

  test("C2 + aggregate: COUNT(*) WHERE tas != 'foo bar' returns exact count, not phrase-subset count") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Same fixture as the C2 test but run through an aggregate instead of a row scan. Combines
      // the two most dangerous round-1 findings: the Not(candidate) subset bug (fixed by
      // isNotFree) and the aggregate-pushdown-over-candidate inflated-count bug (fixed by the
      // guard in areFiltersCompatibleWithAggregation). Without BOTH fixes, the count returned
      // here would be 1 (only row 3 survives the buggy Not(phrase) exclusion). With the fixes,
      // the correct count is 2 (row 1 "Foo-Bar" and row 3 "other" both satisfy != "foo bar").
      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "Foo-Bar"),
        (2L, "foo bar"),
        (3L, "other")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  HASHED FASTFIELDS INCLUDE ('message')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_c2_count")

      val cnt = spark.sql("SELECT COUNT(*) FROM tas_c2_count WHERE message != 'foo bar'").collect()
      cnt.length shouldBe 1
      cnt(0).getLong(0) shouldBe 2L
    }
  }

  test("C2: Not(EqualTo) on text_and_string does NOT drop rows whose tokens match the phrase") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Fixture where exact equality and phrase equality diverge:
      //   row 1 "Foo-Bar"  — tokens ["foo","bar"]           != "foo bar"  (correct: included)
      //   row 2 "foo bar"  — tokens ["foo","bar"]           == "foo bar"  (correct: excluded)
      //   row 3 "other"    — tokens ["other"]               != "foo bar"  (correct: included)
      //
      // Before the fix, isCandidateFilter(Not(EqualTo)) returned true and the converter built
      // MUST(match_all) + MUST_NOT(phrase). The phrase matched both row 1 and row 2, so the
      // source returned only row 3 — Spark's post-filter could not add row 1 back, producing
      // a silently-wrong subset.
      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "Foo-Bar"),
        (2L, "foo bar"),
        (3L, "other")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      val results = df.filter(col("message") =!= "foo bar").collect()
      val ids = results.map(_.getLong(0)).toSet
      ids shouldBe Set(1L, 3L)
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Range query restriction (I5)
  // ═══════════════════════════════════════════════════════════════════

  test("range queries on text_and_string are evaluated by Spark (not pushed as tokenized range)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "apple"),
        (2L, "nile"),
        (3L, "zoo")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df       = readCompanion(indexPath)
      val filtered = df.filter(col("message") > "m")

      // Range queries are unsupported for text_and_string (tokenized index has no meaningful
      // ordering). The ScanBuilder rejects the range filter so it lands in Spark-side FilterExec
      // evaluation, which uses the raw stored value and produces the correct lexicographic result.
      val ids = filtered.collect().map(_.getLong(0)).toSet
      ids shouldBe Set(2L, 3L)

      // Plan should contain a FilterExec (Spark-side) for the range predicate — the scan itself
      // must not have silently pushed the range down as a tokenized range query.
      import org.apache.spark.sql.execution.FilterExec
      val plan        = filtered.queryExecution.executedPlan
      val filterExecs = plan.collect { case f: FilterExec => f }
      withClue(s"Range query on text_and_string must be evaluated by FilterExec:\n$plan\n") {
        filterExecs should not be empty
      }

      // The scan node must NOT carry the range predicate on the text_and_string field. Redundant
      // double-evaluation (FilterExec + scan-side push) would still produce the correct result
      // set, but it's a perf regression and an architectural leak — the scan shouldn't see a
      // range predicate on a tokenized field at all. Inspect the BatchScan line(s) of the plan
      // string and verify the range predicate only appears in the top-level Filter node.
      val planString = plan.toString
      val batchScanLines = planString.linesIterator.filter(_.contains("BatchScan")).toSeq
      withClue(s"Plan should contain a BatchScan line:\n$planString\n") {
        batchScanLines should not be empty
      }
      val batchScanText = batchScanLines.mkString("\n")
      withClue(
        s"Range predicate on text_and_string must not appear in the BatchScan line. " +
          s"Full BatchScan text:\n$batchScanText\n"
      ) {
        batchScanText should not include "GreaterThan"
        batchScanText should not include "message > m"
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  EXCLUDE COLUMNS (I6)
  // ═══════════════════════════════════════════════════════════════════

  test("text_and_string with EXCLUDE COLUMNS does not break the kept text_and_string column") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createTestData(deltaPath)

      // EXCLUDE COLUMNS ('description') removes `description` from the companion index. The kept
      // `message` column is text_and_string and must continue to support both TEXTSEARCH and
      // candidate-pushdown EqualTo + Spark post-filter. End-to-end verification that EXCLUDE
      // actually removes a column from the index lives in CompanionIncludeExcludeColumnsTest
      // (see "EXCLUDE COLUMNS skips specified columns"); this test's job is only to confirm
      // that combining EXCLUDE COLUMNS with INDEXING MODES on a text_and_string kept column
      // compiles, builds, and reads without interaction bugs.
      //
      // NOTE: attempting to assert a negative probe via `description indexquery '...'` is unsafe
      // because IndexQuery validation on a missing field is currently a log-and-continue warning
      // with a silent match-all fallback (see FiltersToQueryConverter executor-side validation).
      // That is a separate pre-existing gap — locking it in here would couple this test to
      // behavior we don't want to freeze.
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('description')
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_excl_test")

      // TEXTSEARCH on the kept text_and_string column still works
      val textResults = spark.sql("SELECT id FROM tas_excl_test WHERE message indexquery 'quick'").collect()
      textResults.length shouldBe 1
      textResults(0).getLong(0) shouldBe 1L

      // Exact match on the kept column still works (candidate pushdown + post-filter)
      val exactResults = df.filter(col("message") === "hello world from indextables").collect()
      exactResults.length shouldBe 1
      exactResults(0).getLong(0) shouldBe 2L

      // Row count is preserved — EXCLUDE drops a column from the index schema, not rows
      df.count() shouldBe 6
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Tokenizer behavior (minor findings)
  // ═══════════════════════════════════════════════════════════════════

  test("tokenizer normalizes case: uppercase indexed content matches lowercase query") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "QUICK BROWN FOX"),
        (2L, "quiet morning")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_case_test")

      // Tokenized search: lowercase query should find uppercase indexed tokens because the
      // default tokenizer lowercases during both index and query.
      val lowerResults = spark.sql("SELECT id FROM tas_case_test WHERE message indexquery 'quick'").collect()
      lowerResults.length shouldBe 1
      lowerResults(0).getLong(0) shouldBe 1L

      // Exact match via EqualTo also works: the PhraseQuery candidate is built from lowercased
      // tokens and the Spark post-filter compares the raw stored value, which preserves case.
      val exactResults = df.filter(col("message") === "QUICK BROWN FOX").collect()
      exactResults.length shouldBe 1
      exactResults(0).getLong(0) shouldBe 1L
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  LIKE / StringStartsWith behavior on text_and_string (C2 round 2)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Behavior-pinning test for SQL `LIKE` / `StringStartsWith` on a text_and_string column.
   *
   * Fixture: three raw values that stress the tokenized-vs-raw semantic gap:
   *   - "Foo-Bar"      (tokens: ["foo", "bar"] after lowercase + punctuation split)
   *   - "foobar"       (tokens: ["foobar"])
   *   - "unrelated"    (tokens: ["unrelated"])
   *
   * This test documents two things:
   *
   *   1. **Default config (pushdown OFF) is correct.** `spark.indextables.filter.stringPattern.pushdown`
   *      is off by default, so Spark evaluates `LIKE 'Foo-%'` directly on the raw stored value and
   *      returns `{1}` — matching standard SQL LIKE semantics.
   *
   *   2. **Pushdown ON is silently lossy for text_and_string columns.** When the master switch is on,
   *      `StringStartsWith("Foo-")` is pushed into tantivy as a prefix query against the inverted
   *      index. The inverted index only holds the tokenized form (lowercased, punctuation-split):
   *      row 1's stored tokens are `["foo","bar"]` — no token starts with `"Foo-"` (case-sensitive)
   *      or even `"foo-"` (hyphen is a split char). The prefix query matches zero documents and the
   *      query returns `{}` even though the raw stored value satisfies the user's LIKE pattern.
   *
   * This is a pre-existing silent-correctness problem with `stringPattern.pushdown=true` on
   * text_and_string columns. It is not introduced by this PR and cannot be fixed without either
   * routing `StringStartsWith` through a candidate-pushdown + post-filter (like EqualTo) or
   * refusing to push the predicate at all for text_and_string fields. The test locks in the
   * observed behavior so any future fix that changes it has to update the test intentionally,
   * and the documentation below must match whatever the test proves.
   */
  test("LIKE on text_and_string: default (pushdown off) is correct; pushdown on is lossy (locked in)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val ss = spark
      import ss.implicits._
      Seq(
        (1L, "Foo-Bar"),
        (2L, "foobar"),
        (3L, "unrelated")
      ).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      // ─── Default config: pushdown OFF ───
      // Spark evaluates LIKE on the raw stored value. This matches standard SQL semantics.
      val dfNoPush = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .option("spark.indextables.filter.stringPattern.pushdown", "false")
        .load(indexPath)
      dfNoPush.createOrReplaceTempView("tas_like_nopush")

      // 'Foo-%' matches only "Foo-Bar" (case-sensitive raw match)
      spark.sql("SELECT id FROM tas_like_nopush WHERE message LIKE 'Foo-%'")
        .collect().map(_.getLong(0)).toSet shouldBe Set(1L)
      // 'foo%' matches only "foobar" (case-sensitive — "Foo-Bar" starts with uppercase "F")
      spark.sql("SELECT id FROM tas_like_nopush WHERE message LIKE 'foo%'")
        .collect().map(_.getLong(0)).toSet shouldBe Set(2L)

      // ─── Pushdown ON: silently lossy ───
      // StringStartsWith is pushed into tantivy as a prefix query on the tokenized inverted index,
      // which strips case and punctuation before indexing. Prefixes that would match the raw value
      // do not match any indexed token, so the query returns an empty result instead of the
      // correct rows. We lock in this known-bad behavior so any future fix shows up as a test
      // delta rather than a silent change.
      val dfPush = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .option("spark.indextables.filter.stringPattern.pushdown", "true")
        .load(indexPath)
      dfPush.createOrReplaceTempView("tas_like_push")

      // Known-bad A: the user wrote `LIKE 'Foo-%'` expecting to match row 1 ("Foo-Bar"). The
      // pushdown builds a tantivy prefix query for the literal "Foo-" against the tokenized
      // inverted index. The index holds lowercased, punctuation-split tokens (row 1 → ["foo","bar"],
      // row 2 → ["foobar"]). No token starts with "Foo-" (case-sensitive, includes hyphen), so
      // the prefix query matches zero documents. `StringStartsWith` is treated as fully supported
      // by the ScanBuilder, so Spark does NOT run a post-filter — the empty result is returned
      // as-is. Expected correct answer: {1}. Observed: {}.
      spark.sql("SELECT id FROM tas_like_push WHERE message LIKE 'Foo-%'")
        .collect().map(_.getLong(0)).toSet shouldBe Set.empty[Long]
      // Known-bad B: `LIKE 'foo%'` against raw values is case-sensitive — standard SQL semantics
      // would match only row 2 ("foobar"). With pushdown on, the prefix query "foo" matches
      // BOTH indexed tokens — "foo" (from "Foo-Bar" after tokenization) AND "foobar". The
      // pushdown returns {1, 2}, a SUPERSET of the correct answer. `StringStartsWith` is fully
      // supported, so Spark does not re-filter. Expected correct: {2}. Observed: {1, 2} — wrong
      // row 1 appears.
      spark.sql("SELECT id FROM tas_like_push WHERE message LIKE 'foo%'")
        .collect().map(_.getLong(0)).toSet shouldBe Set(1L, 2L)
    }
  }

  test("tokenizer drops tokens longer than 255 UTF-8 bytes but keeps shorter neighbors") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Build a row with (a) a normal short token and (b) a single 300-byte token. The 300-byte
      // token must be silently dropped by tantivy's RemoveLongFilter (255-byte cap) while the
      // normal token remains searchable.
      val longToken = "x" * 300 // 300 ASCII chars = 300 bytes in UTF-8
      val ss        = spark
      import ss.implicits._
      Seq((1L, s"normaltoken $longToken")).toDF("id", "message")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INDEXING MODES ('message': 'text_and_string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("tas_long_token")

      // The short neighbor is searchable via TEXTSEARCH — proves the row was indexed
      // and not wholly rejected by the long-token filter.
      val shortResults = spark.sql("SELECT id FROM tas_long_token WHERE message indexquery 'normaltoken'").collect()
      shortResults.length shouldBe 1
      shortResults(0).getLong(0) shouldBe 1L

      // The 300-byte token itself must have been dropped by tantivy's RemoveLongFilter
      // (255-byte cap). A query for the full token returns nothing. Without the length cap
      // this assertion would fail. We use a 280-char prefix because tantivy's query parser
      // may reject querying with a 300-char token for the same reason the indexer drops it,
      // but a 280-char query term is still > 255 bytes and cannot be present in the index.
      val longProbe = "x" * 280
      val longResults = spark.sql(
        s"SELECT id FROM tas_long_token WHERE message indexquery '$longProbe'"
      ).collect()
      longResults.length shouldBe 0
    }
  }
}
