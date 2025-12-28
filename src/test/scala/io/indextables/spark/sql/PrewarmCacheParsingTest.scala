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

package io.indextables.spark.sql

import org.apache.spark.sql.SparkSession

import io.indextables.spark.prewarm.IndexComponentMapping
import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Parsing tests for PREWARM INDEXTABLES CACHE command.
 *
 * Tests SQL syntax variations and parameter extraction.
 */
class PrewarmCacheParsingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit =
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrewarmCacheParsingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  override def afterEach(): Unit =
    if (spark != null) {
      spark.stop()
      spark = null
    }

  // === Basic Syntax Tests ===

  test("PREWARM INDEXTABLES CACHE should parse with path only") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.segments.isEmpty) // Use defaults
    assert(cmd.fields.isEmpty)   // All fields
    assert(cmd.splitsPerTask == 2) // Default
    assert(cmd.wherePredicates.isEmpty)
  }

  test("PREWARM INDEXTABLES CACHE should parse with S3 path") {
    val sql  = "PREWARM INDEXTABLES CACHE 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.tablePath == "s3://bucket/table")
  }

  test("PREWARM TANTIVY4SPARK CACHE should also work") {
    val sql  = "PREWARM TANTIVY4SPARK CACHE '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])
  }

  test("PREWARM should parse case-insensitive keywords") {
    val sql  = "prewarm indextables cache '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])
  }

  test("PREWARM INDEXTABLES CACHE should parse with table identifier") {
    val sql  = "PREWARM INDEXTABLES CACHE my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.tablePath == "my_table")
  }

  test("PREWARM INDEXTABLES CACHE should parse with qualified table identifier") {
    val sql  = "PREWARM INDEXTABLES CACHE my_db.my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.tablePath == "my_db.my_table")
  }

  // === FOR SEGMENTS Tests ===

  test("PREWARM should parse with single segment") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' FOR SEGMENTS (TERM_DICT)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.segments.size == 1)
    assert(cmd.segments.contains("TERM_DICT"))
  }

  test("PREWARM should parse with multiple segments") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.segments.size == 3)
    assert(cmd.segments.contains("TERM_DICT"))
    assert(cmd.segments.contains("FAST_FIELD"))
    assert(cmd.segments.contains("POSTINGS"))
  }

  test("PREWARM should parse all segment aliases") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' FOR SEGMENTS (TERM, FASTFIELD, POSTING_LISTS, FIELDNORM, DOC_STORE)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.segments.size == 5)
  }

  // === ON FIELDS Tests ===

  test("PREWARM should parse with single field") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' ON FIELDS (title)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.fields.isDefined)
    assert(cmd.fields.get.size == 1)
    assert(cmd.fields.get.contains("title"))
  }

  test("PREWARM should parse with multiple fields") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' ON FIELDS (title, content, author)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.fields.isDefined)
    assert(cmd.fields.get.size == 3)
    assert(cmd.fields.get.contains("title"))
    assert(cmd.fields.get.contains("content"))
    assert(cmd.fields.get.contains("author"))
  }

  // === WITH PERWORKER PARALLELISM Tests ===

  test("PREWARM should parse with parallelism") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' WITH PERWORKER PARALLELISM OF 5"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.splitsPerTask == 5)
  }

  test("PREWARM should reject non-positive parallelism") {
    val sql = "PREWARM INDEXTABLES CACHE '/tmp/test' WITH PERWORKER PARALLELISM OF 0"
    val ex = intercept[IllegalArgumentException] {
      val plan = spark.sessionState.sqlParser.parsePlan(sql)
      // The command validates at parse time
      plan.asInstanceOf[PrewarmCacheCommand].run(spark)
    }
    assert(ex.getMessage.contains("positive") || ex.getMessage.contains("PARALLELISM"))
  }

  // === WHERE Clause Tests ===

  test("PREWARM should parse with simple WHERE clause") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' WHERE year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("year"))
  }

  test("PREWARM should parse with compound WHERE clause") {
    val sql  = "PREWARM INDEXTABLES CACHE '/tmp/test' WHERE year = '2024' AND region = 'us-east'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.wherePredicates.nonEmpty)
  }

  // === Combined Clause Tests ===

  test("PREWARM should parse with segments and fields") {
    val sql = "PREWARM INDEXTABLES CACHE '/tmp/test' FOR SEGMENTS (TERM_DICT, POSTINGS) ON FIELDS (title, content)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.segments.size == 2)
    assert(cmd.fields.isDefined)
    assert(cmd.fields.get.size == 2)
  }

  test("PREWARM should parse with all clauses") {
    val sql = """
      PREWARM INDEXTABLES CACHE 's3://bucket/table'
        FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM)
        ON FIELDS (title, content, timestamp)
        WITH PERWORKER PARALLELISM OF 4
        WHERE date >= '2024-01-01'
    """.stripMargin.replaceAll("\n", " ")

    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmCacheCommand])

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    assert(cmd.tablePath == "s3://bucket/table")
    assert(cmd.segments.size == 4)
    assert(cmd.fields.isDefined)
    assert(cmd.fields.get.size == 3)
    assert(cmd.splitsPerTask == 4)
    assert(cmd.wherePredicates.nonEmpty)
  }

  // === IndexComponentMapping Tests ===

  test("IndexComponentMapping should map all segment aliases to components") {
    val aliasToExpected = Map(
      "TERM"            -> IndexComponent.TERM,
      "TERM_DICT"       -> IndexComponent.TERM,
      "TERM_DICTIONARY" -> IndexComponent.TERM,
      "FASTFIELD"       -> IndexComponent.FASTFIELD,
      "FAST_FIELD"      -> IndexComponent.FASTFIELD,
      "POSTINGS"        -> IndexComponent.POSTINGS,
      "POSTING_LISTS"   -> IndexComponent.POSTINGS,
      "FIELDNORM"       -> IndexComponent.FIELDNORM,
      "FIELD_NORM"      -> IndexComponent.FIELDNORM,
      "STORE"           -> IndexComponent.STORE,
      "DOC_STORE"       -> IndexComponent.STORE
    )

    aliasToExpected.foreach {
      case (alias, expected) =>
        val result = IndexComponentMapping.aliasToComponent.get(alias)
        assert(result.isDefined, s"Alias '$alias' should be mapped")
        assert(result.get == expected, s"Alias '$alias' should map to $expected, got ${result.get}")
    }
  }

  test("IndexComponentMapping should have correct default components") {
    val defaults = IndexComponentMapping.defaultComponents

    assert(defaults.contains(IndexComponent.TERM))
    assert(defaults.contains(IndexComponent.FASTFIELD))
    assert(defaults.contains(IndexComponent.POSTINGS))
    assert(defaults.contains(IndexComponent.FIELDNORM))
    assert(!defaults.contains(IndexComponent.STORE), "DOC_STORE should not be in defaults")
    assert(defaults.size == 4)
  }

  test("IndexComponentMapping.parseSegments should handle comma-separated input") {
    val result = IndexComponentMapping.parseSegments("TERM_DICT, FAST_FIELD, POSTINGS")

    assert(result.size == 3)
    assert(result.contains(IndexComponent.TERM))
    assert(result.contains(IndexComponent.FASTFIELD))
    assert(result.contains(IndexComponent.POSTINGS))
  }

  test("IndexComponentMapping.parseSegments should return defaults for empty input") {
    val result = IndexComponentMapping.parseSegments("")

    assert(result == IndexComponentMapping.defaultComponents)
  }

  test("IndexComponentMapping.parseSegments should throw for unknown segment") {
    val ex = intercept[IllegalArgumentException] {
      IndexComponentMapping.parseSegments("UNKNOWN_SEGMENT")
    }
    assert(ex.getMessage.contains("Unknown segment type"))
  }
}
