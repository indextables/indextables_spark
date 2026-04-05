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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.TestBase
import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression, SearchType}

/**
 * Parameterized test suite for all search operator variants: TEXTSEARCH, FIELDMATCH, indexquery,
 * `*` TEXTSEARCH, `*` FIELDMATCH, `*` indexquery, and indexqueryall.
 *
 * Test conditions are defined once in [[SingleFieldSearchBehaviors]] and [[AllFieldsSearchBehaviors]],
 * then executed for every operator via specs. Adding a new condition to the traits automatically
 * covers all operators — no per-operator test duplication.
 */
class SearchOperatorTest
    extends TestBase
    with SingleFieldSearchBehaviors
    with AllFieldsSearchBehaviors {

  // --- Shared mixed-type fixture table ---

  // Written once, used by ~90% of tests. Contains:
  //   content (text/tokenized), status (string/non-tokenized), category (string/non-tokenized)
  //
  // Data:
  //   (1, "apache spark documentation", "active",   "technology")  -- content matches "spark"; status matches "active"
  //   (2, "machine learning algorithms", "inactive", "ai")         -- content matches "learning"
  //   (3, "spark streaming tutorial",    "active",   "technology")  -- content matches "spark"; status matches "active"
  //   (4, "deep learning neural networks","pending", "ai")         -- content matches "learning"
  //   (5, "big data processing",         "active",   "data")       -- status matches "active"
  private var sharedTablePathVar: String = _
  private var scaleTablePathVar: String = _
  private var edgeCaseTablePathVar: String = _
  private var colKeywordTablePathVar: String = _
  private var allTextTablePathVar: String = _
  private var allStringTablePathVar: String = _
  private var intOnlyTablePathVar: String = _

  override protected lazy val sharedTablePath: String = sharedTablePathVar
  override protected lazy val scalePath: String = scaleTablePathVar
  override protected lazy val edgeCasePath: String = edgeCaseTablePathVar
  override protected lazy val colKeywordPath: String = colKeywordTablePathVar
  override protected lazy val allTextPath: String = allTextTablePathVar
  override protected lazy val allStringPath: String = allStringTablePathVar
  override protected lazy val intOnlyPath: String = intOnlyTablePathVar

  private def flushCaches(): Unit =
    try {
      import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch { case _: Exception => }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val _spark = spark
    import _spark.implicits._

    // --- 5-row shared fixture ---
    val path = Files.createTempDirectory("search-op-shared").toString
    sharedTablePathVar = path

    Seq(
      (1, "apache spark documentation", "active", "technology"),
      (2, "machine learning algorithms", "inactive", "ai"),
      (3, "spark streaming tutorial", "active", "technology"),
      (4, "deep learning neural networks", "pending", "ai"),
      (5, "big data processing", "active", "data")
    ).toDF("id", "content", "status", "category").write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.status", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(path)

    // View registered per-test via freshSharedView()/freshView() to avoid ThreadLocal leakage

    // --- Edge-case fixture (unicode + special chars, one text column + one string column) ---
    val edgePath = Files.createTempDirectory("search-op-edge").toString
    edgeCaseTablePathVar = edgePath

    Seq(
      (1, "\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8", "\u65E5\u672C\u8A9E"),
      (2, "\u4E2D\u6587\u6D4B\u8BD5", "\u4E2D\u6587"),
      (3, "user@example.com sent a message", "user@example.com"),
      (4, "admin@test.org replied", "admin@test.org"),
      (5, "user@example.com sent another message", "user@example.com")
    ).toDF("id", "content", "label").write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.label", "string")
      .mode("overwrite")
      .save(edgePath)

    // --- Column-as-keyword fixture ---
    // Columns named after operators: textsearch (text), fieldmatch (string), indexquery (text)
    val colKwPath = Files.createTempDirectory("search-op-colkw").toString
    colKeywordTablePathVar = colKwPath

    Seq(
      (1, "apache spark documentation", "active", "apache spark documentation"),
      (2, "machine learning algorithms", "inactive", "machine learning algorithms"),
      (3, "big data processing", "active", "big data processing")
    ).toDF("id", "textsearch", "fieldmatch", "indexquery").write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.textsearch", "text")
      .option("spark.indextables.indexing.typemap.fieldmatch", "string")
      .option("spark.indextables.indexing.typemap.indexquery", "text")
      .mode("overwrite")
      .save(colKwPath)

    // --- All-text uniform fixture ---
    val allTextPath = Files.createTempDirectory("search-op-alltext").toString
    allTextTablePathVar = allTextPath

    Seq(
      (1, "apache spark documentation", "full text search engine"),
      (2, "machine learning algorithms", "distributed processing framework"),
      (3, "big data processing", "cloud infrastructure automation")
    ).toDF("id", "content", "description").write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.description", "text")
      .mode("overwrite")
      .save(allTextPath)

    // View registered per-test via freshView() to avoid ThreadLocal leakage

    // --- All-string uniform fixture ---
    val allStringPath = Files.createTempDirectory("search-op-allstring").toString
    allStringTablePathVar = allStringPath

    Seq(
      (1, "active", "technology"),
      (2, "inactive", "science"),
      (3, "active", "engineering")
    ).toDF("id", "status", "category").write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.status", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(allStringPath)

    // View registered per-test via freshView()

    // --- Integer-only fixture ---
    val intOnlyPath = Files.createTempDirectory("search-op-intonly").toString
    intOnlyTablePathVar = intOnlyPath

    Seq((1, 100, 200), (2, 300, 400)).toDF("id", "score", "rating").write
      .format(INDEXTABLES_FORMAT)
      .mode("overwrite")
      .save(intOnlyPath)

    // View registered per-test via freshView()

    // --- 100k-row scale fixture ---
    // content: 10 titles cycling (1 in 10 contains "spark" -> 10,000 matches)
    // status: 5 values cycling (1 in 5 is "active" -> 20,000 matches)
    val scalePath = Files.createTempDirectory("search-op-scale").toString
    scaleTablePathVar = scalePath

    val titles = Array(
      "apache spark documentation",
      "machine learning algorithms",
      "big data processing pipeline",
      "natural language processing with transformers",
      "distributed computing fundamentals",
      "deep learning neural networks",
      "real-time stream processing",
      "graph database optimization",
      "kubernetes cluster management",
      "cloud infrastructure automation"
    )
    val statuses = Array("active", "inactive", "pending", "archived", "disabled")

    val scaleRows = (1 to 100000).map { i =>
      (i, titles(i % titles.length), statuses(i % statuses.length))
    }
    scaleRows.toDF("id", "content", "status").repartition(1).write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.status", "string")
      .mode("overwrite")
      .save(scalePath)

  }

  override def afterAll(): Unit = {
    Seq(sharedTablePathVar, edgeCaseTablePathVar, colKeywordTablePathVar,
      allTextTablePathVar, allStringTablePathVar, intOnlyTablePathVar, scaleTablePathVar)
      .filter(_ != null).foreach(p => deleteRecursively(new File(p)))
    super.afterAll()
  }

  // --- Spec instances ---

  // TEXTSEARCH "spark" on content -> ids 1, 3 (2 rows)
  // TEXTSEARCH "learning" on content -> ids 2, 4
  // TEXTSEARCH "spark" AND category='technology' -> ids 1, 3
  // TEXTSEARCH "spark" OR "learning" -> ids 1, 2, 3, 4
  //
  // FIELDMATCH "active" on status -> ids 1, 3, 5 (3 rows)
  // FIELDMATCH "pending" on status -> id 4
  // FIELDMATCH "active" AND category='technology' -> ids 1, 3
  // FIELDMATCH "active" OR "pending" -> ids 1, 3, 4, 5
  //
  // indexquery "spark" on content -> ids 1, 3 (same as TEXTSEARCH)

  private val singleFieldSpecs = Seq(
    SingleFieldSpec(
      name = "TEXTSEARCH",
      keyword = "TEXTSEARCH",
      searchType = SearchType.TextSearch,
      requiresTokenized = Some(true),
      matchingColumn = "content",
      wrongColumn = "status",
      sampleQuery = "spark",
      sampleQuery2 = "learning",
      expectedCount = 2,
      expectedIds = Seq(1, 3),
      expectedAndIds = Seq(1, 3),
      expectedOrIds = Seq(1, 2, 3, 4),
      errorSubstrings = Seq("not tokenized", "FIELDMATCH"),
      scaleExpectedCount = 10000 // 1 in 10 titles contains "spark"
    ),
    SingleFieldSpec(
      name = "FIELDMATCH",
      keyword = "FIELDMATCH",
      searchType = SearchType.FieldMatch,
      requiresTokenized = Some(false),
      matchingColumn = "status",
      wrongColumn = "content",
      sampleQuery = "active",
      sampleQuery2 = "pending",
      expectedCount = 3,
      expectedIds = Seq(1, 3, 5),
      expectedAndIds = Seq(1, 3),
      expectedOrIds = Seq(1, 3, 4, 5),
      errorSubstrings = Seq("tokenized", "TEXTSEARCH"),
      scaleExpectedCount = 20000 // 1 in 5 statuses is "active"
    ),
    SingleFieldSpec(
      name = "indexquery",
      keyword = "indexquery",
      searchType = SearchType.IndexQuery,
      requiresTokenized = None,
      matchingColumn = "content",
      wrongColumn = "status",
      sampleQuery = "spark",
      sampleQuery2 = "learning",
      expectedCount = 2,
      expectedIds = Seq(1, 3),
      expectedAndIds = Seq(1, 3),
      expectedOrIds = Seq(1, 2, 3, 4),
      scaleExpectedCount = 10000 // same as TEXTSEARCH — queries content column
    )
  )

  private val allFieldsSpecs = Seq(
    AllFieldsSpec(
      name = "* TEXTSEARCH",
      keyword = Some("TEXTSEARCH"),
      sqlTemplate = q => s"* TEXTSEARCH '$q'",
      searchType = SearchType.TextSearch,
      requiresTokenized = Some(true)
    ),
    AllFieldsSpec(
      name = "* FIELDMATCH",
      keyword = Some("FIELDMATCH"),
      sqlTemplate = q => s"* FIELDMATCH '$q'",
      searchType = SearchType.FieldMatch,
      requiresTokenized = Some(false)
    ),
    AllFieldsSpec(
      name = "* indexquery",
      keyword = Some("indexquery"),
      sqlTemplate = q => s"* indexquery '$q'",
      searchType = SearchType.IndexQuery,
      requiresTokenized = None
    ),
    AllFieldsSpec(
      name = "indexqueryall",
      keyword = None,
      sqlTemplate = q => s"indexqueryall('$q')",
      searchType = SearchType.IndexQueryAll,
      requiresTokenized = None,
      isFunctionSyntax = true
    )
  )

  // --- Register parameterized tests ---

  registerSingleFieldTests(singleFieldSpecs)
  registerAllFieldsTests(allFieldsSpecs)
  registerSingleFieldScaleTests(singleFieldSpecs) // registered last so scale tests run after everything else

  // ========================================================================
  // Non-parameterized standalone tests
  // ========================================================================

  // --- Cross-checks ---

  test("cross-check: TEXTSEARCH and indexquery return identical rows") {
    val view = freshView("standalone", sharedTablePath)
    val textsearchIds = spark.sql(
      s"SELECT id FROM $view WHERE content TEXTSEARCH 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    val indexqueryIds = spark.sql(
      s"SELECT id FROM $view WHERE content indexquery 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    assert(textsearchIds == indexqueryIds, "TEXTSEARCH and indexquery must return identical rows")
    assert(textsearchIds == Seq(1, 3), s"Expected Seq(1, 3), got $textsearchIds")
  }

  test("cross-check: * indexquery and indexqueryall return identical rows") {
    val view = freshView("standalone", sharedTablePath)
    val starIds = spark.sql(
      s"SELECT id FROM $view WHERE * indexquery 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    val allIds = spark.sql(
      s"SELECT id FROM $view WHERE indexqueryall('spark') ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    assert(starIds == allIds, "* indexquery and indexqueryall must return identical rows")
    assert(starIds.nonEmpty, "Cross-check queries should return results")
    // Verify not returning all rows — operator must actually filter
    val totalRows = spark.sql(s"SELECT count(*) FROM $view").collect()(0).getLong(0)
    assert(starIds.length < totalRows, s"* indexquery returned all $totalRows rows — not filtering")
  }

  test("cross-check: * TEXTSEARCH and indexqueryall return identical rows on all-text table") {
    val view1 = freshView("xcheck_ts", allTextPath)
    val textsearchIds = spark.sql(
      s"SELECT id FROM $view1 WHERE * TEXTSEARCH 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    val view2 = freshView("xcheck_iqa", allTextPath)
    val indexqueryallIds = spark.sql(
      s"SELECT id FROM $view2 WHERE indexqueryall('spark') ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    assert(textsearchIds == indexqueryallIds, "* TEXTSEARCH and indexqueryall must match on all-text table")
  }

  test("cross-check: * FIELDMATCH and indexqueryall return identical rows on all-string table") {
    val view1 = freshView("xcheck_fm", allStringPath)
    val fieldmatchIds = spark.sql(
      s"SELECT id FROM $view1 WHERE * FIELDMATCH 'active' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    val view2 = freshView("xcheck_iqa2", allStringPath)
    val indexqueryallIds = spark.sql(
      s"SELECT id FROM $view2 WHERE indexqueryall('active') ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    assert(fieldmatchIds == indexqueryallIds, "* FIELDMATCH and indexqueryall must match on all-string table")
  }

  // --- Preprocessor ---

  test("preprocessor: already-converted function calls are not double-processed") {
    val view1 = freshView("idem_ts", allTextPath)
    val textsearchResults = spark.sql(
      s"SELECT id FROM $view1 WHERE tantivy4spark_textsearch('content', 'spark') ORDER BY id"
    ).collect()
    val view2 = freshView("idem_iq", allTextPath)
    val indexqueryResults = spark.sql(
      s"SELECT id FROM $view2 WHERE tantivy4spark_indexquery('content', 'spark') ORDER BY id"
    ).collect()
    val view3 = freshView("idem_iqa", allTextPath)
    val indexqueryallResults = spark.sql(
      s"SELECT id FROM $view3 WHERE tantivy4spark_indexqueryall('spark') ORDER BY id"
    ).collect()

    val ids1 = textsearchResults.map(_.getInt(0)).toSeq
    val ids2 = indexqueryResults.map(_.getInt(0)).toSeq
    val ids3 = indexqueryallResults.map(_.getInt(0)).toSeq
    assert(ids1 == ids2, "tantivy4spark_textsearch and tantivy4spark_indexquery should match")
    assert(ids1 == ids3, "tantivy4spark_textsearch and tantivy4spark_indexqueryall should match")
    assert(ids1.nonEmpty, "Function call forms should return results")
  }

  // --- Mixed operators ---

  test("mixed operators: TEXTSEARCH AND FIELDMATCH in same WHERE") {
    val view = freshView("standalone", sharedTablePath)
    val results = spark.sql(
      s"""SELECT id, content, status FROM $view
         |WHERE content TEXTSEARCH 'spark' AND status FIELDMATCH 'active'
         |ORDER BY id""".stripMargin
    ).collect()
    val resultIds = results.map(_.getInt(0)).toSeq
    assert(resultIds == Seq(1, 3), s"Expected Seq(1, 3), got $resultIds")
    assert(results.forall(r => r.getString(2) == "active"), "All results should have status=active")
    assert(
      results.forall(r => r.getString(1).toLowerCase.contains("spark")),
      "All results should contain 'spark' in content"
    )
  }

  // --- Cross-operator column ambiguity ---

  test("edge case: column named 'textsearch' with FIELDMATCH operator") {
    // colKeywordViewName has textsearch (text) column — but FIELDMATCH requires string type
    // This tests the parser resolves 'textsearch' as column name, not as operator keyword
    // Note: this will throw type validation since textsearch column is text, not string
    // Use the fieldmatch column (which is string) with TEXTSEARCH operator instead for cross-test
    val view = freshView("colkw_standalone", colKeywordPath)
    val results = spark.sql(
      s"SELECT id FROM $view WHERE fieldmatch FIELDMATCH 'active' ORDER BY id"
    ).collect()
    assert(results.length == 2, s"Expected 2 rows, got ${results.length}")
  }

  test("edge case: column named 'fieldmatch' with TEXTSEARCH operator") {
    // colKeywordViewName has fieldmatch (string) column — TEXTSEARCH on string should throw
    // This verifies parser resolves 'fieldmatch' as column name even when TEXTSEARCH is the operator
    val view = freshView("colkw_standalone", colKeywordPath)
    val ex = intercept[IllegalArgumentException] {
      spark.sql(s"SELECT id FROM $view WHERE fieldmatch TEXTSEARCH 'active'").collect()
    }
    assert(ex.getMessage.contains("Cannot use TEXTSEARCH"), s"Expected type rejection, got: ${ex.getMessage}")
  }

  // ========================================================================
  // Restored tests from old file — distinct code paths not in the matrix
  // ========================================================================

  // --- Invalid searchType constructor validation ---

  test("invalid searchType should throw IllegalArgumentException on IndexQueryExpression") {
    val column = org.apache.spark.sql.catalyst.expressions.AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("test"), StringType)
    val ex = intercept[IllegalArgumentException] {
      IndexQueryExpression(column, query, "invalid_type")
    }
    assert(ex.getMessage.contains("invalid_type") || ex.getMessage.contains("requirement failed"))
  }

  test("invalid searchType should throw IllegalArgumentException on IndexQueryAllExpression") {
    val query = Literal(UTF8String.fromString("test"), StringType)
    val ex = intercept[IllegalArgumentException] {
      IndexQueryAllExpression(query, "invalid_type")
    }
    assert(ex.getMessage.contains("invalid_type") || ex.getMessage.contains("requirement failed"))
  }

  // --- DocMapping-based validation (read without typemap options) ---

  test("docMapping-based validation: type info persists from write to read without typemap options") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive")
      ).toDF("id", "content", "status").write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      // Read WITHOUT typemap options — docMapping in transaction log provides field types
      val df = spark.read.format(INDEXTABLES_FORMAT).load(tempPath)
      df.createOrReplaceTempView("docmapping_validation_test")

      // TEXTSEARCH on text field should succeed
      val results = spark.sql(
        "SELECT id FROM docmapping_validation_test WHERE content TEXTSEARCH 'spark'"
      ).collect()
      assert(results.length == 1, s"Expected 1 row, got ${results.length}")

      // FIELDMATCH on string field should succeed
      val fmResults = spark.sql(
        "SELECT id FROM docmapping_validation_test WHERE status FIELDMATCH 'active'"
      ).collect()
      assert(fmResults.length == 1, s"Expected 1 row, got ${fmResults.length}")

      // TEXTSEARCH on string field should throw
      val ex = intercept[IllegalArgumentException] {
        spark.sql("SELECT id FROM docmapping_validation_test WHERE status TEXTSEARCH 'active'").collect()
      }
      assert(ex.getMessage.contains("Cannot use TEXTSEARCH"))
    }
  }

  // --- Non-existent field validation ---

  test("TEXTSEARCH on non-existent field should throw with field name in error") {
    val view = freshView("standalone", sharedTablePath)
    val ex = intercept[IllegalArgumentException] {
      spark.sql(s"SELECT id FROM $view WHERE nonexistent TEXTSEARCH 'spark'").collect()
    }
    assert(ex.getMessage.contains("nonexistent"), s"Expected field name in error, got: ${ex.getMessage}")
  }

  test("FIELDMATCH on non-existent field should throw with field name in error") {
    val view = freshView("standalone", sharedTablePath)
    val ex = intercept[IllegalArgumentException] {
      spark.sql(s"SELECT id FROM $view WHERE nonexistent FIELDMATCH 'active'").collect()
    }
    assert(ex.getMessage.contains("nonexistent"), s"Expected field name in error, got: ${ex.getMessage}")
  }

  // --- Custom tokenizer type variants ---

  test("TEXTSEARCH on text_uuid_exactonly field should succeed") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      Seq(
        (1, "550e8400-e29b-41d4-a716-446655440000"),
        (2, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
      ).toDF("id", "uuid_field").write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.uuid_field", "text_uuid_exactonly")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read.format(INDEXTABLES_FORMAT).load(tempPath)
      df.createOrReplaceTempView("text_uuid_exactonly_test")

      val results = spark.sql(
        "SELECT id FROM text_uuid_exactonly_test WHERE uuid_field TEXTSEARCH '550e8400-e29b-41d4-a716-446655440000'"
      ).collect()
      assert(results.length == 1, s"Expected 1 row, got ${results.length}")
    }
  }

  test("FIELDMATCH on text_uuid_exactonly field should throw") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      Seq(
        (1, "550e8400-e29b-41d4-a716-446655440000"),
        (2, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
      ).toDF("id", "uuid_field").write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.uuid_field", "text_uuid_exactonly")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read.format(INDEXTABLES_FORMAT).load(tempPath)
      df.createOrReplaceTempView("text_uuid_exactonly_fm_test")

      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          "SELECT id FROM text_uuid_exactonly_fm_test WHERE uuid_field FIELDMATCH '550e8400-e29b-41d4-a716-446655440000'"
        ).collect()
      }
      assert(ex.getMessage.contains("Cannot use FIELDMATCH"), s"Expected FIELDMATCH rejection, got: ${ex.getMessage}")
    }
  }

  // --- _indexall synthetic column cross-check ---

  test("* indexquery returns same results as _indexall indexquery") {
    val view = freshView("standalone", sharedTablePath)
    val starResults = spark.sql(
      s"SELECT id FROM $view WHERE * indexquery 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    val indexallResults = spark.sql(
      s"SELECT id FROM $view WHERE _indexall indexquery 'spark' ORDER BY id"
    ).collect().map(_.getInt(0)).toSeq

    assert(starResults == indexallResults,
      s"* indexquery $starResults and _indexall indexquery $indexallResults should return identical results")
    assert(starResults.nonEmpty, "Both should return results")
  }
}
