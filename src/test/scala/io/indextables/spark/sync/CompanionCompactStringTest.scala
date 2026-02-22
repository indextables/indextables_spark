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
import java.util.UUID

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, count}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for compact string indexing modes in companion splits.
 *
 * Exercises the `exact_only`, `text_uuid_exactonly`, `text_uuid_strip`,
 * and `text_custom_exactonly` modes via BUILD INDEXTABLES COMPANION FOR PARQUET.
 *
 * No cloud credentials needed — runs entirely on local filesystem.
 */
class CompanionCompactStringTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionCompactStringTest")
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

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark-compact-string").toString
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

  /** Create parquet data with UUID columns. */
  private def createUuidParquetData(parquetPath: String, numRows: Int = 20): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numRows).map { i =>
      val traceId = UUID.randomUUID().toString
      val requestId = UUID.randomUUID().toString
      val message = s"Processing request $requestId for user_$i with trace $traceId"
      (i.toLong, traceId, requestId, message, s"name_$i", i * 1.5)
    }
    data
      .toDF("id", "trace_id", "request_id", "message", "name", "score")
      .repartition(1)
      .write
      .parquet(parquetPath)
  }

  /** Create parquet data with custom-pattern columns. */
  private def createCustomPatternParquetData(parquetPath: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "Processing order ORD-00000001 completed", "ORD-00000001"),
      (2L, "Processing order ORD-00000002 failed", "ORD-00000002"),
      (3L, "Processing order ORD-00000003 completed", "ORD-00000003"),
      (4L, "System health check passed", "SYS-00000001"),
      (5L, "Processing order ORD-00000004 completed", "ORD-00000004")
    )
    data
      .toDF("id", "audit_log", "order_id")
      .repartition(1)
      .write
      .parquet(parquetPath)
  }

  // -------------------------------------------------------
  //  Tests
  // -------------------------------------------------------

  test("exact_only mode should build companion and support EqualTo filter") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_exact").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_exact").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 10)

      // Read back one trace_id for later filtering
      val sourceData = spark.read.parquet(parquetPath).collect()
      val targetTraceId = sourceData(0).getString(sourceData(0).fieldIndex("trace_id"))

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'exact_only', 'request_id':'exact_only') " +
          s"AT LOCATION '$indexPath'"
      )
      val row = result.collect()
      row.length shouldBe 1
      row(0).getString(2) shouldBe "success"

      // Read companion and verify EqualTo filter works
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      val filtered = companionDf.filter(col("trace_id") === targetTraceId).collect()
      filtered.length shouldBe 1
      filtered(0).getString(filtered(0).fieldIndex("trace_id")) shouldBe targetTraceId

      // Verify total count is correct
      companionDf.count() shouldBe 10
    }
  }

  test("text_uuid_exactonly mode should build companion and support text search") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_text_uuid").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_text_uuid").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('message':'text_uuid_exactonly') " +
          s"AT LOCATION '$indexPath'"
      )
      val row = result.collect()
      row.length shouldBe 1
      row(0).getString(2) shouldBe "success"

      // Read companion and verify count is correct
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 10

      // Verify text search works on stripped text via IndexQuery
      companionDf.createOrReplaceTempView("text_uuid_test")
      val textResults = spark.sql(
        "SELECT * FROM text_uuid_test WHERE message indexquery 'processing'"
      ).collect()
      // All messages contain "Processing" — should match all rows
      textResults.length shouldBe 10
    }
  }

  test("text_uuid_strip mode should build companion and support text search") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_strip").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_strip").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 10)

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('message':'text_uuid_strip') " +
          s"AT LOCATION '$indexPath'"
      )
      val row = result.collect()
      row.length shouldBe 1
      row(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 10

      // Verify text search works on stripped text
      companionDf.createOrReplaceTempView("strip_test")
      val textResults = spark.sql(
        "SELECT * FROM strip_test WHERE message indexquery 'processing'"
      ).collect()
      textResults.length shouldBe 10
    }
  }

  test("text_custom_exactonly mode should build companion with custom regex") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_custom").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_custom").getAbsolutePath

      createCustomPatternParquetData(parquetPath)

      // Note: Spark SQL treats \ as escape in string literals, so \d requires \\d in SQL.
      // From Scala regular string: "\\\\d" → actual chars "\\d" → SQL parser → "\d"
      val result = spark.sql(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '" + parquetPath + "' " +
          "INDEXING MODES ('audit_log':'text_custom_exactonly:ORD-\\\\d{8}') " +
          "AT LOCATION '" + indexPath + "'"
      )
      val row = result.collect()
      row.length shouldBe 1
      row(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 5

      // Verify text search works on non-pattern content
      companionDf.createOrReplaceTempView("custom_test")
      val textResults = spark.sql(
        "SELECT * FROM custom_test WHERE audit_log indexquery 'processing'"
      ).collect()
      // 4 out of 5 messages contain "Processing order"
      textResults.length shouldBe 4

      // Verify the regex-matched pattern was stripped from indexed TEXT.
      // "00000001" is a token produced by the default tokenizer from "ORD-00000001".
      // After stripping, this token should NOT exist in the text index.
      // Note: we can't use "ORD-00000001" because the hash_field_rewriter
      // redirects regex-matching queries to the companion U64 hash field (expected).
      val tokenResults = spark.sql(
        "SELECT * FROM custom_test WHERE audit_log indexquery '00000001'"
      ).collect()
      tokenResults.length shouldBe 0

      // Term "ORD" should also return 0 — "ord" token only exists if text wasn't stripped.
      // (The default tokenizer splits "ORD-00000001" into tokens "ord" and "00000001".)
      // Note: can't use "OR*" because it also matches "order" from "Processing order...".
      val ordResults = spark.sql(
        "SELECT * FROM custom_test WHERE audit_log indexquery 'ORD'"
      ).collect()
      ordResults.length shouldBe 0

      // Wildcard "00*" should return 0 — tokens starting with "00" (e.g., "00000001")
      // only exist if the ORD-XXXXXXXX pattern was not stripped from the text.
      val zeroWildcardResults = spark.sql(
        "SELECT * FROM custom_test WHERE audit_log indexquery '00*'"
      ).collect()
      zeroWildcardResults.length shouldBe 0

      // Verify the extracted pattern IS still queryable via the companion hash redirect.
      // This is the key difference from text_custom_strip: exactonly preserves exact lookups.
      val exactResults = spark.sql(
        "SELECT * FROM custom_test WHERE audit_log indexquery '\"ORD-00000001\"'"
      ).collect()
      exactResults.length shouldBe 1
    }
  }

  test("text_custom_strip mode should build companion with custom regex stripped") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_custom_strip").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_custom_strip").getAbsolutePath

      createCustomPatternParquetData(parquetPath)

      // Note: Spark SQL treats \ as escape in string literals, so \d requires \\d in SQL.
      // From Scala regular string: "\\\\d" → actual chars "\\d" → SQL parser → "\d"
      val result = spark.sql(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '" + parquetPath + "' " +
          "INDEXING MODES ('audit_log':'text_custom_strip:ORD-\\\\d{8}') " +
          "AT LOCATION '" + indexPath + "'"
      )
      val row = result.collect()
      row.length shouldBe 1
      row(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 5

      // Verify text search works — "processing" should match even after ORD-* patterns stripped
      companionDf.createOrReplaceTempView("custom_strip_test")
      val textResults = spark.sql(
        "SELECT * FROM custom_strip_test WHERE audit_log indexquery 'processing'"
      ).collect()
      // 4 out of 5 messages contain "Processing order"
      textResults.length shouldBe 4

      // Verify stripped pattern is NOT searchable via phrase query on audit_log.
      // text_custom_strip should have removed ORD-XXXXXXXX from indexed text,
      // so a phrase query for "ORD-00000001" on audit_log should return 0.
      val strippedResults = spark.sql(
        "SELECT * FROM custom_strip_test WHERE audit_log indexquery '\"ORD-00000001\"'"
      ).collect()
      strippedResults.length shouldBe 0
    }
  }

  test("incremental sync should preserve compact modes from metadata") {
    withTempPath { tempDir =>
      val parquetPath  = new File(tempDir, "parquet_inc").getAbsolutePath
      val parquetPath2 = new File(tempDir, "parquet_inc2").getAbsolutePath
      val indexPath    = new File(tempDir, "companion_inc").getAbsolutePath

      // Create initial data and sync with compact modes
      val ss = spark
      import ss.implicits._
      val data1 = (0 until 5).map(i => (i.toLong, UUID.randomUUID().toString, s"name_$i"))
      data1.toDF("id", "trace_id", "name").repartition(1).write.parquet(parquetPath)

      val result1 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'exact_only') " +
          s"AT LOCATION '$indexPath'"
      )
      result1.collect()(0).getString(2) shouldBe "success"

      // Verify modes are stored in metadata
      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        metadata.configuration("indextables.companion.indexingModes") should include("exact_only")
      } finally
        txLog.close()

      // Add more data to a second parquet directory and re-sync WITHOUT specifying modes
      val data2 = (5 until 10).map(i => (i.toLong, UUID.randomUUID().toString, s"name_$i"))
      data2.toDF("id", "trace_id", "name").repartition(1).write.parquet(parquetPath2)

      // Create a combined parquet directory
      val combinedPath = new File(tempDir, "parquet_combined").getAbsolutePath
      new File(combinedPath).mkdirs()
      // Copy files from both directories
      new File(parquetPath).listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
        java.nio.file.Files.copy(f.toPath, new File(combinedPath, f.getName).toPath)
      }
      new File(parquetPath2).listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
        java.nio.file.Files.copy(f.toPath, new File(combinedPath, "batch2_" + f.getName).toPath)
      }

      // Re-sync from combined path (modes not specified — should pick up from metadata)
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$combinedPath' " +
          s"AT LOCATION '$indexPath'"
      )
      val row2 = result2.collect()(0)
      // Should succeed (not error) — modes were inherited from metadata
      row2.getString(2) shouldBe "success"
    }
  }

  test("exact_only field should support EqualTo pushdown") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_pushdown").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_pushdown").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 10)

      // Get a known trace_id
      val sourceData = spark.read.parquet(parquetPath).collect()
      val targetTraceId = sourceData(3).getString(sourceData(3).fieldIndex("trace_id"))

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'exact_only') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // EqualTo on exact_only field should be pushed down and return correct result
      val result = companionDf.filter(col("trace_id") === targetTraceId).collect()
      result.length shouldBe 1
      result(0).getString(result(0).fieldIndex("trace_id")) shouldBe targetTraceId
    }
  }

  test("text_uuid_exactonly field should not push down EqualTo (deferred to Spark)") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_nopush").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_nopush").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 10)

      // Get a known message for filtering
      val sourceData = spark.read.parquet(parquetPath).collect()
      val targetMessage = sourceData(0).getString(sourceData(0).fieldIndex("message"))

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('message':'text_uuid_exactonly') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // EqualTo on text_uuid_exactonly should work (deferred to Spark post-filter)
      val result = companionDf.filter(col("message") === targetMessage).collect()
      result.length shouldBe 1
      result(0).getString(result(0).fieldIndex("message")) shouldBe targetMessage
    }
  }

  test("COUNT(*) and COUNT(field) should work on exact_only companion") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_agg").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_agg").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 15)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'exact_only') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // COUNT(*) should return total rows
      companionDf.count() shouldBe 15

      // COUNT(trace_id) should return non-null count
      val result = companionDf.agg(count("trace_id")).collect()
      result(0).getLong(0) shouldBe 15L
    }
  }

  test("invalid indexing mode should return error with descriptive message") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_invalid").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_invalid").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      // The command catches exceptions and returns an error row
      val rows = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'bogus_mode') " +
          s"AT LOCATION '$indexPath'"
      ).collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error" // status
      rows(0).getString(10) should include("Unrecognized indexing mode")
      rows(0).getString(10) should include("bogus_mode")
    }
  }

  test("invalid field name in INDEXING MODES should return error") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_badfield").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_badfield").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      // Reference a field that doesn't exist in the parquet schema
      val rows = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('nonexistent_field':'exact_only') " +
          s"AT LOCATION '$indexPath'"
      ).collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error" // status
      rows(0).getString(10) should include("does not exist in source schema")
      rows(0).getString(10) should include("nonexistent_field")
    }
  }

  test("case-insensitive mode names should work (regression for parser .toLowerCase removal)") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_case").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_case").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      val sourceData = spark.read.parquet(parquetPath).collect()
      val targetTraceId = sourceData(0).getString(sourceData(0).fieldIndex("trace_id"))

      // Use UPPER CASE mode name — should be recognized by IndexingModes.isRecognized
      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'EXACT_ONLY') " +
          s"AT LOCATION '$indexPath'"
      )
      result.collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.count() shouldBe 5

      // EqualTo filter should still work
      val filtered = companionDf.filter(col("trace_id") === targetTraceId).collect()
      filtered.length shouldBe 1
      filtered(0).getString(filtered(0).fieldIndex("trace_id")) shouldBe targetTraceId
    }
  }

  test("empty regex in custom mode should return error") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_empty_regex").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_empty_regex").getAbsolutePath

      createCustomPatternParquetData(parquetPath)

      val rows = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('audit_log':'text_custom_strip:') " +
          s"AT LOCATION '$indexPath'"
      ).collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "error"
      rows(0).getString(10) should include("empty regex")
    }
  }

  test("text_uuid_strip should actually strip UUIDs from indexed text") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_strip_verify").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_strip_verify").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      // Get a known UUID from the source data
      val sourceData = spark.read.parquet(parquetPath).collect()
      val knownTraceId = sourceData(0).getString(sourceData(0).fieldIndex("trace_id"))
      // Extract first 8 hex chars from UUID for a targeted search
      val uuidPrefix = knownTraceId.split("-")(0) // e.g., "550e8400"

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('message':'text_uuid_strip') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      // Text search for non-UUID content should still work
      companionDf.createOrReplaceTempView("uuid_strip_verify")
      val textResults = spark.sql(
        "SELECT * FROM uuid_strip_verify WHERE message indexquery 'processing'"
      ).collect()
      textResults.length shouldBe 5

      // UUID prefix should NOT be searchable — it was stripped before indexing
      val uuidResults = spark.sql(
        s"SELECT * FROM uuid_strip_verify WHERE message indexquery '$uuidPrefix'"
      ).collect()
      uuidResults.length shouldBe 0
    }
  }

  test("text_uuid_exactonly should strip UUIDs from text but preserve them via parquet readback") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_exactonly_verify").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_exactonly_verify").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      // Get a known UUID from the source data
      val sourceData = spark.read.parquet(parquetPath).collect()
      val knownMessage = sourceData(0).getString(sourceData(0).fieldIndex("message"))
      val knownTraceId = sourceData(0).getString(sourceData(0).fieldIndex("trace_id"))
      val uuidPrefix = knownTraceId.split("-")(0)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('message':'text_uuid_exactonly') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(indexPath)

      companionDf.createOrReplaceTempView("uuid_exactonly_verify")

      // UUID prefix should NOT be searchable via IndexQuery — stripped from text
      val uuidResults = spark.sql(
        s"SELECT * FROM uuid_exactonly_verify WHERE message indexquery '$uuidPrefix'"
      ).collect()
      uuidResults.length shouldBe 0

      // Wildcard on UUID prefix should also NOT match — hex chars were fully removed
      val wildcardResults = spark.sql(
        s"SELECT * FROM uuid_exactonly_verify WHERE message indexquery '$uuidPrefix*'"
      ).collect()
      wildcardResults.length shouldBe 0

      // But EqualTo on message should still work — parquet has the original unstripped value
      val equalToResults = companionDf.filter(col("message") === knownMessage).collect()
      equalToResults.length shouldBe 1
      equalToResults(0).getString(equalToResults(0).fieldIndex("message")) shouldBe knownMessage
    }
  }

  test("compact string modes should be stored in companion metadata") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_meta_compact").getAbsolutePath
      val indexPath   = new File(tempDir, "companion_meta_compact").getAbsolutePath

      createUuidParquetData(parquetPath, numRows = 5)

      spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
          s"INDEXING MODES ('trace_id':'exact_only', 'message':'text_uuid_exactonly') " +
          s"AT LOCATION '$indexPath'"
      ).collect()(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val metadata = txLog.getMetadata()
        val modesJson = metadata.configuration("indextables.companion.indexingModes")
        modesJson should include("exact_only")
        modesJson should include("text_uuid_exactonly")
        modesJson should include("trace_id")
        modesJson should include("message")
      } finally
        txLog.close()
    }
  }
}
