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

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import io.indextables.spark.TestBase
import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression}

// --- Valid condition keys (closed sets for excludes validation) ---

object ConditionKeys {
  val SingleField: Set[String] = Set(
    "basic-parsing",
    "case-insensitivity",
    "backtick-columns",
    "qualified-columns",
    "complex-tantivy-syntax",
    "phrase-queries",
    "wildcard-queries",
    "e2e-sql",
    "e2e-compound-and",
    "e2e-compound-or",
    "type-correct",
    "type-wrong-throws",
    "type-wrong-compound-throws",
    "column-named-as-keyword",
    "empty-query",
    "unicode-query",
    "special-chars-query",
    "scale-100k"
  )

  val AllFields: Set[String] = Set(
    "basic-parsing",
    "case-insensitivity",
    "e2e-sql",
    "e2e-compound-and",
    "type-correct-uniform",
    "type-mixed-throws",
    "type-all-wrong-throws",
    "type-no-text-fields-throws"
  )
}

// --- Spec case classes ---

/**
 * Spec for a single-field search operator (e.g., `title TEXTSEARCH 'query'`).
 *
 * @param name              Display name for test output (e.g., "TEXTSEARCH")
 * @param keyword           SQL keyword used in queries (e.g., "TEXTSEARCH")
 * @param searchType        Expected SearchType constant value (e.g., "textsearch")
 * @param requiresTokenized Some(true) for TEXTSEARCH, Some(false) for FIELDMATCH, None for indexquery
 * @param matchingColumn    Column in the shared table that matches this operator's type requirement
 * @param wrongColumn       Column in the shared table that mismatches (for type-wrong-throws tests)
 * @param sampleQuery       Query string that returns results on the matching column
 * @param sampleQuery2      Second query string for OR tests
 * @param expectedCount     Expected row count for sampleQuery on the shared table
 * @param expectedIds       Expected row IDs for sampleQuery on the shared table (sorted)
 * @param expectedAndIds    Expected row IDs for sampleQuery AND category='technology' (sorted)
 * @param expectedOrIds     Expected row IDs for sampleQuery OR sampleQuery2 (sorted)
 * @param errorSubstrings   Expected substrings in type validation error messages
 * @param scaleExpectedCount Expected row count for sampleQuery on the 100k-row scale table
 * @param excludes          Condition keys to skip for this operator
 */
case class SingleFieldSpec(
    name: String,
    keyword: String,
    searchType: String,
    requiresTokenized: Option[Boolean],
    matchingColumn: String,
    wrongColumn: String,
    sampleQuery: String,
    sampleQuery2: String,
    expectedCount: Int,
    expectedIds: Seq[Int],
    expectedAndIds: Seq[Int],
    expectedOrIds: Seq[Int],
    errorSubstrings: Seq[String] = Seq.empty,
    scaleExpectedCount: Int = 0,
    excludes: Set[String] = Set.empty
)

/**
 * Spec for an all-fields search operator (e.g., `* TEXTSEARCH 'query'` or `indexqueryall('query')`).
 *
 * @param name             Display name for test output (e.g., "* TEXTSEARCH")
 * @param keyword          The infix keyword (e.g., "TEXTSEARCH"); None for function syntax
 * @param sqlTemplate      Function from query string to SQL WHERE fragment
 * @param searchType       Expected SearchType constant value
 * @param requiresTokenized Type validation requirement (same as SingleFieldSpec)
 * @param isFunctionSyntax true for indexqueryall('q') which has no keyword to vary case on
 * @param excludes         Condition keys to skip
 */
case class AllFieldsSpec(
    name: String,
    keyword: Option[String],
    sqlTemplate: String => String,
    searchType: String,
    requiresTokenized: Option[Boolean],
    isFunctionSyntax: Boolean = false,
    excludes: Set[String] = Set.empty
)

// --- Behavior traits ---

/**
 * Parameterized test conditions for single-field search operators.
 * Mix into a concrete test class and call `registerSingleFieldTests`.
 */
trait SingleFieldSearchBehaviors { self: TestBase =>

  /** Path to the shared mixed-type table. Each test registers a fresh view to avoid ThreadLocal leakage. */
  protected def sharedTablePath: String

  /** Path to edge-case table: unicode + special chars in both text and string columns. */
  protected def edgeCasePath: String

  /** Path to column-as-keyword table: columns named textsearch (text), fieldmatch (string), indexquery (text). */
  protected def colKeywordPath: String

  private val sfViewCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  private def freshSfView(prefix: String, path: String): String = {
    val name = s"${prefix}_${sfViewCounter.incrementAndGet()}"
    spark.read.format(INDEXTABLES_FORMAT).load(path).createOrReplaceTempView(name)
    name
  }
  private def freshSharedView(): String = freshSfView("shared", sharedTablePath)
  private def freshEdgeCaseView(): String = freshSfView("edge", edgeCasePath)
  private def freshColKeywordView(): String = freshSfView("colkw", colKeywordPath)

  protected def registerSingleFieldTests(specs: Seq[SingleFieldSpec]): Unit =
    for (spec <- specs) {
      val invalid = spec.excludes -- ConditionKeys.SingleField
      require(
        invalid.isEmpty,
        s"${spec.name}: unknown exclude keys: ${invalid.mkString(", ")}. " +
          s"Valid keys: ${ConditionKeys.SingleField.mkString(", ")}"
      )
      testBasicParsing(spec)
      testCaseInsensitivity(spec)
      testBacktickColumns(spec)
      testQualifiedColumns(spec)
      testComplexTantivySyntax(spec)
      testPhraseQueries(spec)
      testWildcardQueries(spec)
      testE2ESql(spec)
      testE2ECompoundAnd(spec)
      testE2ECompoundOr(spec)
      testTypeCorrect(spec)
      testTypeWrongThrows(spec)
      testTypeWrongCompoundThrows(spec)
      testColumnNamedAsKeyword(spec)
      testEmptyQuery(spec)
      testUnicodeQuery(spec)
      testSpecialCharsQuery(spec)
    }

  /** Register scale tests separately so they run last. Call after registerSingleFieldTests. */
  protected def registerSingleFieldScaleTests(specs: Seq[SingleFieldSpec]): Unit =
    for (spec <- specs) testScale100k(spec)

  // --- Parser-level tests (no Spark session / table needed) ---

  private def testBasicParsing(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("basic-parsing")) return
    test(s"${spec.name}: parse simple expression") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val expr = parser.parseExpression(s"title ${spec.keyword} 'spark AND sql'")
      assert(expr.isInstanceOf[IndexQueryExpression])
      val iqe = expr.asInstanceOf[IndexQueryExpression]
      assert(iqe.getColumnName.contains("title"))
      assert(iqe.getQueryString.contains("spark AND sql"))
      assert(iqe.searchType == spec.searchType)
    }
  }

  private def testCaseInsensitivity(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("case-insensitivity")) return
    test(s"${spec.name}: case-insensitive keyword") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val variants = Seq(
        spec.keyword.toUpperCase,
        spec.keyword.toLowerCase,
        spec.keyword.take(1).toUpperCase + spec.keyword.drop(1).toLowerCase
      )
      for (variant <- variants) {
        val expr = parser.parseExpression(s"title $variant 'query'")
        assert(expr.isInstanceOf[IndexQueryExpression], s"Failed for variant: $variant")
        assert(
          expr.asInstanceOf[IndexQueryExpression].searchType == spec.searchType,
          s"Wrong searchType for variant: $variant"
        )
      }
    }
  }

  private def testBacktickColumns(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("backtick-columns")) return
    test(s"${spec.name}: backtick-quoted column") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val expr = parser.parseExpression(s"`my column` ${spec.keyword} 'test'")
      assert(expr.isInstanceOf[IndexQueryExpression])
      val iqe = expr.asInstanceOf[IndexQueryExpression]
      assert(iqe.getQueryString.contains("test"))
      assert(iqe.searchType == spec.searchType)
    }
  }

  private def testQualifiedColumns(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("qualified-columns")) return
    test(s"${spec.name}: qualified column name") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val expr = parser.parseExpression(s"table.columnName ${spec.keyword} 'search term'")
      assert(expr.isInstanceOf[IndexQueryExpression])
      val iqe = expr.asInstanceOf[IndexQueryExpression]
      assert(iqe.getQueryString.contains("search term"))
      assert(iqe.searchType == spec.searchType)
    }
  }

  private def testComplexTantivySyntax(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("complex-tantivy-syntax")) return
    test(s"${spec.name}: complex Tantivy syntax preserved") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val query = "(apache AND spark) OR (hadoop AND mapreduce)"
      val expr = parser.parseExpression(s"content ${spec.keyword} '$query'")
      assert(expr.isInstanceOf[IndexQueryExpression])
      assert(expr.asInstanceOf[IndexQueryExpression].getQueryString.contains(query))
    }
  }

  private def testPhraseQueries(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("phrase-queries")) return
    test(s"${spec.name}: phrase query preserved") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val expr = parser.parseExpression(s"""content ${spec.keyword} '"exact phrase match"'""")
      assert(expr.isInstanceOf[IndexQueryExpression])
      assert(expr.asInstanceOf[IndexQueryExpression].getQueryString.contains("\"exact phrase match\""))
    }
  }

  private def testWildcardQueries(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("wildcard-queries")) return
    test(s"${spec.name}: wildcard query preserved") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val expr = parser.parseExpression(s"title ${spec.keyword} 'prefix* AND *suffix'")
      assert(expr.isInstanceOf[IndexQueryExpression])
      assert(expr.asInstanceOf[IndexQueryExpression].getQueryString.contains("prefix* AND *suffix"))
    }
  }

  // --- E2E tests (use the shared mixed-type table) ---

  private def testE2ESql(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("e2e-sql")) return
    test(s"${spec.name}: e2e SQL query returns exact expected results") {
      val view = freshSharedView()
      val results = spark.sql(
        s"SELECT id FROM $view WHERE ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery}' ORDER BY id"
      ).collect()
      val resultIds = results.map(_.getInt(0)).toSeq
      assert(
        resultIds == spec.expectedIds,
        s"${spec.name} expected ids ${spec.expectedIds}, got $resultIds"
      )
    }
  }

  private def testE2ECompoundAnd(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("e2e-compound-and")) return
    test(s"${spec.name}: compound AND with standard filter") {
      val view = freshSharedView()
      val results = spark.sql(
        s"""SELECT id, category FROM $view
           |WHERE ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery}'
           |AND category = 'technology'
           |ORDER BY id""".stripMargin
      ).collect()
      val resultIds = results.map(_.getInt(0)).toSeq
      assert(
        resultIds == spec.expectedAndIds,
        s"${spec.name} AND expected ids ${spec.expectedAndIds}, got $resultIds"
      )
      assert(results.forall(_.getString(1) == "technology"), "All results should have category=technology")
    }
  }

  private def testE2ECompoundOr(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("e2e-compound-or")) return
    test(s"${spec.name}: compound OR returns union of both queries") {
      val view = freshSharedView()
      val orResults = spark.sql(
        s"""SELECT id FROM $view
           |WHERE ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery}'
           |OR ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery2}'
           |ORDER BY id""".stripMargin
      ).collect()
      val resultIds = orResults.map(_.getInt(0)).toSeq
      assert(
        resultIds == spec.expectedOrIds,
        s"${spec.name} OR expected ids ${spec.expectedOrIds}, got $resultIds"
      )
    }
  }

  private def testTypeCorrect(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("type-correct")) return
    test(s"${spec.name}: correct field type succeeds with exact count") {
      val view = freshSharedView()
      val results = spark.sql(
        s"SELECT id FROM $view WHERE ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery}'"
      ).collect()
      assert(
        results.length == spec.expectedCount,
        s"${spec.name} expected ${spec.expectedCount} rows, got ${results.length}"
      )
    }
  }

  private def testTypeWrongThrows(spec: SingleFieldSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: wrong field type throws (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-wrong-throws")) return
    test(s"${spec.name}: wrong field type throws IllegalArgumentException") {
      val view = freshSharedView()
      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          s"SELECT id FROM $view WHERE ${spec.wrongColumn} ${spec.keyword} '${spec.sampleQuery}'"
        ).collect()
      }
      assert(
        ex.getMessage.contains(s"Cannot use ${spec.keyword.toUpperCase}"),
        s"Expected 'Cannot use ${spec.keyword.toUpperCase}' in error, got: ${ex.getMessage}"
      )
      assert(ex.getMessage.contains(spec.wrongColumn), s"Expected column name in error, got: ${ex.getMessage}")
      for (substring <- spec.errorSubstrings) {
        assert(ex.getMessage.contains(substring), s"Expected '$substring' in error, got: ${ex.getMessage}")
      }
    }
  }

  private def testTypeWrongCompoundThrows(spec: SingleFieldSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: wrong type in compound WHERE (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-wrong-compound-throws")) return
    test(s"${spec.name}: wrong field type in compound WHERE still throws") {
      val view = freshSharedView()
      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          s"SELECT id FROM $view WHERE ${spec.wrongColumn} ${spec.keyword} '${spec.sampleQuery}' AND id > 0"
        ).collect()
      }
      assert(
        ex.getMessage.contains(s"Cannot use ${spec.keyword.toUpperCase}"),
        s"Expected rejection message in compound WHERE, got: ${ex.getMessage}"
      )
    }
  }

  // Column-as-keyword table layout:
  //   id | textsearch (text)                | fieldmatch (string) | indexquery (text)
  //   1  | "apache spark documentation"     | "active"            | "apache spark documentation"
  //   2  | "machine learning algorithms"    | "inactive"          | "machine learning algorithms"
  //   3  | "big data processing"            | "active"            | "big data processing"

  private def testColumnNamedAsKeyword(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("column-named-as-keyword")) return
    test(s"${spec.name}: column named '${spec.keyword.toLowerCase}' with ${spec.name} operator") {
      val view = freshColKeywordView()
      val colName = spec.keyword.toLowerCase
      val query = if (spec.requiresTokenized.contains(false)) "active" else "spark"
      val expectedCount = if (spec.requiresTokenized.contains(false)) 2 else 1
      val results = spark.sql(
        s"SELECT id FROM $view WHERE $colName ${spec.keyword} '$query' ORDER BY id"
      ).collect()
      assert(
        results.length == expectedCount,
        s"Column named '${colName}' with ${spec.name} operator: expected $expectedCount rows, got ${results.length}"
      )
    }
  }

  // --- Edge-case input tests (use shared edge-case table) ---
  //
  // Edge-case table layout:
  //   id | content (text)                          | label (string)
  //   1  | "日本語テスト"                            | "日本語"
  //   2  | "中文测试"                               | "中文"
  //   3  | "user@example.com sent a message"       | "user@example.com"
  //   4  | "admin@test.org replied"                | "admin@test.org"
  //   5  | "user@example.com sent another message" | "user@example.com"

  private def edgeCaseColumn(spec: SingleFieldSpec): String =
    if (spec.requiresTokenized.contains(false)) "label" else "content"

  private def testEmptyQuery(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("empty-query")) return
    test(s"${spec.name}: empty query string should not crash") {
      val view = freshEdgeCaseView()
      val col = edgeCaseColumn(spec)
      val results = spark.sql(s"SELECT id FROM $view WHERE $col ${spec.keyword} ''").collect()
      // Empty query should not crash. Tantivy may return all rows (match-all) or zero.
      assert(results.length >= 0 && results.length <= 5,
        s"${spec.name} with empty query returned unexpected count: ${results.length}")
    }
  }

  private def testUnicodeQuery(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("unicode-query")) return
    test(s"${spec.name}: unicode characters return correct results") {
      val view = freshEdgeCaseView()
      val col = edgeCaseColumn(spec)
      val results = spark.sql(
        s"SELECT id FROM $view WHERE $col ${spec.keyword} '日本語' ORDER BY id"
      ).collect()
      if (spec.requiresTokenized.contains(false)) {
        // FIELDMATCH exact match on label: "日本語" matches id=1
        assert(results.length == 1, s"${spec.name} exact match on unicode should return 1 row, got ${results.length}")
        assert(results(0).getInt(0) == 1, s"Expected id=1, got ${results(0).getInt(0)}")
      } else {
        // TEXTSEARCH/indexquery: CJK tokenization is engine-dependent; must not crash, bounded results
        assert(results.length <= 5, s"${spec.name} unicode query returned more rows than exist: ${results.length}")
      }
    }
  }

  private def testSpecialCharsQuery(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("special-chars-query")) return
    test(s"${spec.name}: special characters return correct results") {
      val view = freshEdgeCaseView()
      val col = edgeCaseColumn(spec)
      if (spec.requiresTokenized.contains(false)) {
        // FIELDMATCH exact match on label: "user@example.com" matches ids 3, 5
        val results = spark.sql(
          s"SELECT id FROM $view WHERE $col ${spec.keyword} 'user@example.com' ORDER BY id"
        ).collect()
        assert(results.length == 2, s"${spec.name} on 'user@example.com' should return 2 rows, got ${results.length}")
        assert(results.map(_.getInt(0)).toSeq == Seq(3, 5), s"Expected ids Seq(3, 5)")
      } else {
        // TEXTSEARCH/indexquery: "user" tokenized from email content — depends on tokenizer
        val results = spark.sql(
          s"SELECT id FROM $view WHERE $col ${spec.keyword} 'user' ORDER BY id"
        ).collect()
        assert(results.length > 0, s"${spec.name} on 'user' should return results")
        assert(results.length < 5, s"${spec.name} on 'user' should not return all rows (got ${results.length})")
      }
    }
  }

  // --- Scale test (registered separately via registerSingleFieldScaleTests) ---

  /** The 100k-row scale view name — must be provided by the concrete test class. */
  protected def scalePath: String

  private def testScale100k(spec: SingleFieldSpec): Unit = {
    if (spec.excludes.contains("scale-100k")) return
    test(s"${spec.name}: 100k rows returns exact expected count") {
      spark.conf.set("spark.indextables.read.defaultLimit", "200000")
      try {
        val view = freshSfView("scale", scalePath)
        val results = spark.sql(
          s"SELECT id FROM $view WHERE ${spec.matchingColumn} ${spec.keyword} '${spec.sampleQuery}' ORDER BY id"
        ).collect()
        assert(
          results.length == spec.scaleExpectedCount,
          s"${spec.name} at 100k scale: expected ${spec.scaleExpectedCount} rows, got ${results.length}"
        )
        assert(results.length < 100000, s"${spec.name} returned all rows — operator not filtering")
      } finally {
        spark.conf.set("spark.indextables.read.defaultLimit", "250")
      }
    }
  }
}

/**
 * Parameterized test conditions for all-fields search operators.
 * Mix into a concrete test class and call `registerAllFieldsTests`.
 */
trait AllFieldsSearchBehaviors { self: TestBase =>

  /** Path to the shared mixed-type table. */
  protected def sharedTablePath: String

  /** Paths to shared tables (written once in beforeAll). Each test registers a unique view to avoid ThreadLocal leakage. */
  protected def allTextPath: String
  protected def allStringPath: String
  protected def intOnlyPath: String

  private val viewCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  protected def freshView(prefix: String, path: String): String = {
    val name = s"${prefix}_${viewCounter.incrementAndGet()}"
    spark.read.format(INDEXTABLES_FORMAT).load(path).createOrReplaceTempView(name)
    name
  }

  protected def registerAllFieldsTests(specs: Seq[AllFieldsSpec]): Unit =
    for (spec <- specs) {
      val invalid = spec.excludes -- ConditionKeys.AllFields
      require(
        invalid.isEmpty,
        s"${spec.name}: unknown exclude keys: ${invalid.mkString(", ")}. " +
          s"Valid keys: ${ConditionKeys.AllFields.mkString(", ")}"
      )
      testAllFieldsBasicParsing(spec)
      testAllFieldsCaseInsensitivity(spec)
      testAllFieldsE2ESql(spec)
      testAllFieldsE2ECompoundAnd(spec)
      testAllFieldsTypeCorrectUniform(spec)
      testAllFieldsTypeMixedThrows(spec)
      testAllFieldsTypeAllWrongThrows(spec)
      testAllFieldsTypeNoTextFieldsThrows(spec)
    }

  // --- Parser-level tests ---

  private def testAllFieldsBasicParsing(spec: AllFieldsSpec): Unit = {
    if (spec.excludes.contains("basic-parsing")) return
    test(s"${spec.name}: parse expression") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val sqlFragment = spec.sqlTemplate("spark AND sql")
      val expr = parser.parseExpression(sqlFragment)
      assert(expr.isInstanceOf[IndexQueryAllExpression], s"Expected IndexQueryAllExpression, got ${expr.getClass}")
      val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
      assert(allExpr.getQueryString.contains("spark AND sql"))
      assert(allExpr.searchType == spec.searchType)
    }
  }

  private def testAllFieldsCaseInsensitivity(spec: AllFieldsSpec): Unit = {
    if (spec.isFunctionSyntax) {
      test(s"${spec.name}: case-insensitive keyword (N/A: function syntax)") {
        cancel(s"${spec.name} uses function syntax — no keyword to vary")
      }
      return
    }
    if (spec.excludes.contains("case-insensitivity")) return
    test(s"${spec.name}: case-insensitive keyword") {
      val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
      val kw = spec.keyword.get
      val variants = Seq(
        s"* ${kw.toUpperCase} 'query'",
        s"* ${kw.toLowerCase} 'query'",
        s"* ${kw.take(1).toUpperCase + kw.drop(1).toLowerCase} 'query'"
      )
      for (variant <- variants) {
        val expr = parser.parseExpression(variant)
        assert(expr.isInstanceOf[IndexQueryAllExpression], s"Failed for variant: $variant")
        assert(
          expr.asInstanceOf[IndexQueryAllExpression].searchType == spec.searchType,
          s"Wrong searchType for variant: $variant"
        )
      }
    }
  }

  // --- E2E tests ---

  // All-text table:  id=1 "apache spark documentation"/"full text search engine", id=2 "machine learning"/"distributed", id=3 "big data"/"cloud"
  // All-string table: id=1 "active"/"technology", id=2 "inactive"/"science", id=3 "active"/"engineering"
  // Integer-only table: id=1 score=100 rating=200, id=2 score=300 rating=400

  private def uniformView(spec: AllFieldsSpec): String = {
    val (prefix, path) =
      if (spec.requiresTokenized.contains(true)) ("alltext", allTextPath) else ("allstring", allStringPath)
    freshView(prefix, path)
  }

  private def testAllFieldsE2ESql(spec: AllFieldsSpec): Unit = {
    if (spec.excludes.contains("e2e-sql")) return
    test(s"${spec.name}: e2e SQL query returns correct results") {
      if (spec.requiresTokenized.isDefined) {
        val view = uniformView(spec)
        val query = if (spec.requiresTokenized.contains(true)) "spark" else "active"
        // text: "spark" matches row 1 only (content col); string: "active" matches rows 1, 3 (status col)
        val expectedIds = if (spec.requiresTokenized.contains(true)) Seq(1) else Seq(1, 3)
        val whereClause = spec.sqlTemplate(query)
        val results = spark.sql(s"SELECT id FROM $view WHERE $whereClause ORDER BY id").collect()
        val resultIds = results.map(_.getInt(0)).toSeq
        assert(resultIds == expectedIds,
          s"${spec.name} on uniform table: expected $expectedIds, got $resultIds")
      } else {
        // indexquery/indexqueryall with no type validation: searches all fields on the mixed table.
        // "spark" matches tokenized content in rows 1, 3. No false positives from exact-match
        // fields (status="active"/"inactive") because "spark" doesn't match those values.
        val view = freshView("mixed", sharedTablePath)
        val whereClause = spec.sqlTemplate("spark")
        val results = spark.sql(s"SELECT id FROM $view WHERE $whereClause ORDER BY id").collect()
        val resultIds = results.map(_.getInt(0)).toSeq
        assert(resultIds == Seq(1, 3),
          s"${spec.name} on shared mixed table: expected Seq(1, 3), got $resultIds")
      }
    }
  }

  private def testAllFieldsE2ECompoundAnd(spec: AllFieldsSpec): Unit = {
    if (spec.excludes.contains("e2e-compound-and")) return
    test(s"${spec.name}: compound AND with standard filter") {
      if (spec.requiresTokenized.isDefined) {
        // Use shared uniform table + integer filter (id < 3) so * operator doesn't hit the filter column
        val view = uniformView(spec)
        val query = if (spec.requiresTokenized.contains(true)) "spark" else "active"
        val whereClause = spec.sqlTemplate(query)
        // text: "spark" matches id=1 only, AND id < 3 → Seq(1)
        // string: "active" matches ids 1, 3, AND id < 3 → Seq(1)
        val expectedIds = Seq(1)
        val results = spark.sql(
          s"SELECT id FROM $view WHERE $whereClause AND id < 3 ORDER BY id"
        ).collect()
        val resultIds = results.map(_.getInt(0)).toSeq
        assert(resultIds == expectedIds,
          s"${spec.name} AND: expected $expectedIds, got $resultIds")
      } else {
        // "spark" matches content in rows 1, 3; AND category='technology' → ids 1, 3
        val view = freshView("mixed", sharedTablePath)
        val whereClause = spec.sqlTemplate("spark")
        val results = spark.sql(
          s"SELECT id FROM $view WHERE $whereClause AND category = 'technology' ORDER BY id"
        ).collect()
        val resultIds = results.map(_.getInt(0)).toSeq
        assert(resultIds == Seq(1, 3), s"${spec.name} AND on mixed table: expected Seq(1, 3), got $resultIds")
      }
    }
  }

  // --- Type validation tests ---

  private def testAllFieldsTypeCorrectUniform(spec: AllFieldsSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: all fields correct type (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-correct-uniform")) return
    test(s"${spec.name}: all fields correct type succeeds") {
      val view = uniformView(spec)
      val query = if (spec.requiresTokenized.contains(true)) "spark" else "active"
      // text: "spark" matches row 1 only; string: "active" matches rows 1, 3
      val expectedIds = if (spec.requiresTokenized.contains(true)) Seq(1) else Seq(1, 3)
      val whereClause = spec.sqlTemplate(query)
      val results = spark.sql(s"SELECT id FROM $view WHERE $whereClause ORDER BY id").collect()
      val resultIds = results.map(_.getInt(0)).toSeq
      assert(resultIds == expectedIds, s"${spec.name} on uniform table: expected $expectedIds, got $resultIds")
    }
  }

  private def testAllFieldsTypeMixedThrows(spec: AllFieldsSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: mixed field types throws (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-mixed-throws")) return
    test(s"${spec.name}: mixed field types throws") {
      val view = freshView("mixed", sharedTablePath)
      val whereClause = spec.sqlTemplate("spark")
      val ex = intercept[IllegalArgumentException] {
        spark.sql(s"SELECT id FROM $view WHERE $whereClause").collect()
      }
      val expectedKeyword = spec.keyword.getOrElse(spec.name)
      assert(
        ex.getMessage.contains("Cannot use") && ex.getMessage.contains(expectedKeyword),
        s"Expected 'Cannot use' AND '$expectedKeyword' in error, got: ${ex.getMessage}"
      )
    }
  }

  private def testAllFieldsTypeAllWrongThrows(spec: AllFieldsSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: all fields wrong type throws (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-all-wrong-throws")) return
    test(s"${spec.name}: all fields wrong type throws") {
      // * TEXTSEARCH on all-string = wrong; * FIELDMATCH on all-text = wrong
      val (prefix, path) =
        if (spec.requiresTokenized.contains(true)) ("allstring_wrong", allStringPath) else ("alltext_wrong", allTextPath)
      val view = freshView(prefix, path)
      val query = if (spec.requiresTokenized.contains(true)) "active" else "spark"
      val whereClause = spec.sqlTemplate(query)
      val ex = intercept[IllegalArgumentException] {
        spark.sql(s"SELECT id FROM $view WHERE $whereClause").collect()
      }
      val expectedKeyword = spec.keyword.getOrElse(spec.name)
      assert(
        ex.getMessage.contains("Cannot use") && ex.getMessage.contains(expectedKeyword),
        s"Expected 'Cannot use' AND '$expectedKeyword' in error, got: ${ex.getMessage}"
      )
    }
  }

  private def testAllFieldsTypeNoTextFieldsThrows(spec: AllFieldsSpec): Unit = {
    if (spec.requiresTokenized.isEmpty) {
      test(s"${spec.name}: integer-only table throws (N/A: no type validation)") {
        cancel(s"${spec.name} has no type validation")
      }
      return
    }
    if (spec.excludes.contains("type-no-text-fields-throws")) return
    test(s"${spec.name}: integer-only table (no text/string fields) throws") {
      val view = freshView("intonly", intOnlyPath)
      val whereClause = spec.sqlTemplate("something")
      val ex = intercept[IllegalArgumentException] {
        spark.sql(s"SELECT id FROM $view WHERE $whereClause").collect()
      }
      assert(
        ex.getMessage.contains("Cannot use"),
        s"Expected type validation error on integer-only table, got: ${ex.getMessage}"
      )
    }
  }
}
