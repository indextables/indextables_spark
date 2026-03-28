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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression}
import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite

class TextSearchFieldMatchParserTest extends AnyFunSuite with TestBase {

  // --- TEXTSEARCH basic parsing ---

  test("TEXTSEARCH should parse simple expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("title TEXTSEARCH 'spark AND sql'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("spark AND sql"))
    assert(indexQueryExpr.searchType == "textsearch")
  }

  test("TEXTSEARCH should be case-insensitive") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr1 = parser.parseExpression("title TEXTSEARCH 'query'")
    val expr2 = parser.parseExpression("title textsearch 'query'")
    val expr3 = parser.parseExpression("title TextSearch 'query'")

    assert(expr1.isInstanceOf[IndexQueryExpression])
    assert(expr2.isInstanceOf[IndexQueryExpression])
    assert(expr3.isInstanceOf[IndexQueryExpression])

    assert(expr1.asInstanceOf[IndexQueryExpression].searchType == "textsearch")
    assert(expr2.asInstanceOf[IndexQueryExpression].searchType == "textsearch")
    assert(expr3.asInstanceOf[IndexQueryExpression].searchType == "textsearch")
  }

  test("TEXTSEARCH should handle backtick columns") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("`my column` TEXTSEARCH 'test'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("test"))
    assert(indexQueryExpr.searchType == "textsearch")
  }

  test("TEXTSEARCH should handle complex Tantivy syntax") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("content TEXTSEARCH '(apache AND spark) OR (hadoop AND mapreduce)'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("content"))
    assert(indexQueryExpr.getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
  }

  test("TEXTSEARCH should handle qualified column names") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("table.columnName TEXTSEARCH 'search term'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("search term"))
  }

  // --- FIELDMATCH basic parsing ---

  test("FIELDMATCH should parse simple expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("status FIELDMATCH 'active'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("status"))
    assert(indexQueryExpr.getQueryString.contains("active"))
    assert(indexQueryExpr.searchType == "fieldmatch")
  }

  test("FIELDMATCH should be case-insensitive") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr1 = parser.parseExpression("status FIELDMATCH 'active'")
    val expr2 = parser.parseExpression("status fieldmatch 'active'")
    val expr3 = parser.parseExpression("status FieldMatch 'active'")

    assert(expr1.isInstanceOf[IndexQueryExpression])
    assert(expr2.isInstanceOf[IndexQueryExpression])
    assert(expr3.isInstanceOf[IndexQueryExpression])

    assert(expr1.asInstanceOf[IndexQueryExpression].searchType == "fieldmatch")
    assert(expr2.asInstanceOf[IndexQueryExpression].searchType == "fieldmatch")
    assert(expr3.asInstanceOf[IndexQueryExpression].searchType == "fieldmatch")
  }

  test("FIELDMATCH should handle backtick columns") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("`my.field` FIELDMATCH 'value'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("value"))
    assert(indexQueryExpr.searchType == "fieldmatch")
  }

  test("FIELDMATCH should handle qualified column names") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("table.columnName FIELDMATCH 'exact value'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("exact value"))
    assert(indexQueryExpr.searchType == "fieldmatch")
  }

  // --- * TEXTSEARCH and * FIELDMATCH parsing ---

  test("* TEXTSEARCH should parse all-fields expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("* TEXTSEARCH 'error timeout'")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("error timeout"))
    assert(allExpr.searchType == "textsearch")
  }

  test("* TEXTSEARCH should be case-insensitive") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr1 = parser.parseExpression("* TEXTSEARCH 'query'")
    val expr2 = parser.parseExpression("* textsearch 'query'")
    val expr3 = parser.parseExpression("* TextSearch 'query'")

    assert(expr1.isInstanceOf[IndexQueryAllExpression])
    assert(expr2.isInstanceOf[IndexQueryAllExpression])
    assert(expr3.isInstanceOf[IndexQueryAllExpression])

    assert(expr1.asInstanceOf[IndexQueryAllExpression].searchType == "textsearch")
    assert(expr2.asInstanceOf[IndexQueryAllExpression].searchType == "textsearch")
    assert(expr3.asInstanceOf[IndexQueryAllExpression].searchType == "textsearch")
  }

  test("* FIELDMATCH should parse all-fields expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("* FIELDMATCH 'exact value'")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("exact value"))
    assert(allExpr.searchType == "fieldmatch")
  }

  // --- Backward compatibility ---

  test("Legacy indexquery syntax should still work") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("title indexquery 'spark AND sql'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("spark AND sql"))
    assert(indexQueryExpr.searchType == "indexquery")
  }

  test("Legacy indexqueryall syntax should still work") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("indexqueryall('spark AND sql')")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("spark AND sql"))
    assert(allExpr.searchType == "indexqueryall")
  }

  // --- End-to-end SQL tests ---

  test("End-to-end: WHERE col TEXTSEARCH 'query' via spark.sql") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms"),
        (3, "big data processing")
      ).toDF("id", "title")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("textsearch_test_docs")

      val results = spark.sql("""
        SELECT id, title
        FROM textsearch_test_docs
        WHERE title TEXTSEARCH 'spark'
        ORDER BY id
      """).collect()

      assert(results.length > 0, "TEXTSEARCH query should return results")
      assert(
        results.exists(_.getString(1).toLowerCase.contains("spark")),
        "TEXTSEARCH should return documents containing the search term"
      )
    }
  }

  test("End-to-end: WHERE * TEXTSEARCH 'query' via spark.sql") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms"),
        (3, "big data processing")
      ).toDF("id", "title")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("star_textsearch_test_docs")

      val results = spark.sql("""
        SELECT id, title
        FROM star_textsearch_test_docs
        WHERE * TEXTSEARCH 'spark'
        ORDER BY id
      """).collect()

      assert(results.length > 0, "* TEXTSEARCH query should return results")
      assert(
        results.exists(_.getString(1).toLowerCase.contains("spark")),
        "* TEXTSEARCH should return documents containing the search term"
      )
    }
  }

  // --- Field type validation tests ---

  test("TEXTSEARCH on text field should succeed") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms")
      ).toDF("id", "content")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("textsearch_text_field")

      val results = spark.sql("SELECT id FROM textsearch_text_field WHERE content TEXTSEARCH 'spark'").collect()
      assert(results.length > 0, "TEXTSEARCH on text field should succeed and return results")
    }
  }

  test("TEXTSEARCH on string field should throw error") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive")
      ).toDF("id", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .load(tempPath)
      df.createOrReplaceTempView("textsearch_string_field")

      val ex = intercept[IllegalArgumentException] {
        spark.sql("SELECT id FROM textsearch_string_field WHERE status TEXTSEARCH 'active'").collect()
      }
      assert(ex.getMessage.contains("Cannot use TEXTSEARCH"))
      assert(ex.getMessage.contains("status"))
      assert(ex.getMessage.contains("not tokenized"))
      assert(ex.getMessage.contains("FIELDMATCH"))
    }
  }

  test("FIELDMATCH on string field should succeed") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive")
      ).toDF("id", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_string_field")

      val results = spark.sql("SELECT id FROM fieldmatch_string_field WHERE status FIELDMATCH 'active'").collect()
      assert(results.length > 0, "FIELDMATCH on string field should succeed and return results")
    }
  }

  test("FIELDMATCH on text field should throw error") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms")
      ).toDF("id", "content")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_text_field")

      val ex = intercept[IllegalArgumentException] {
        spark.sql("SELECT id FROM fieldmatch_text_field WHERE content FIELDMATCH 'spark'").collect()
      }
      assert(ex.getMessage.contains("Cannot use FIELDMATCH"))
      assert(ex.getMessage.contains("content"))
      assert(ex.getMessage.contains("tokenized"))
      assert(ex.getMessage.contains("TEXTSEARCH"))
    }
  }

  test("indexquery on any field should succeed without type validation") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms")
      ).toDF("id", "content")

      // Write with text field type
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("indexquery_any_field")

      // indexquery should work on text fields without type validation
      val results = spark.sql("SELECT id FROM indexquery_any_field WHERE content indexquery 'spark'").collect()
      assert(results.length > 0, "indexquery should work on text fields without type validation")
    }
  }

  // --- Comprehensive end-to-end: all syntax variants ---

  test("End-to-end 100k rows: all syntax variants return identical results") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

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
      val categories = Array("technology", "ai", "data", "infrastructure", "devops")

      val rows = (1 to 100000).map { i =>
        (i, titles(i % titles.length), categories(i % categories.length))
      }
      val testData = rows.toDF("id", "title", "category")

      testData.repartition(1).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      spark.conf.set("spark.indextables.read.defaultLimit", "200000")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("large_syntax_test")

      val totalCount = spark.sql("SELECT count(*) FROM large_syntax_test").collect()(0).getLong(0)
      assert(totalCount == 100000, s"Expected 100000 rows, got $totalCount")

      // TEXTSEARCH (new preferred), indexquery (legacy), and * TEXTSEARCH / indexqueryall
      val textsearchRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title TEXTSEARCH 'spark' ORDER BY id"
      ).collect()

      val starTextsearchRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE * TEXTSEARCH 'spark' ORDER BY id"
      ).collect()

      val indexqueryRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title indexquery 'spark' ORDER BY id"
      ).collect()

      val indexqueryallRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE indexqueryall('spark') ORDER BY id"
      ).collect()

      val textsearchIds = textsearchRows.map(_.getInt(0)).sorted.toSeq
      val starTextsearchIds = starTextsearchRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryIds = indexqueryRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryallIds = indexqueryallRows.map(_.getInt(0)).sorted.toSeq

      assert(textsearchIds == indexqueryIds, "TEXTSEARCH and indexquery must return identical rows")
      assert(starTextsearchIds == indexqueryallIds, "* TEXTSEARCH and indexqueryall must return identical rows")
      assert(textsearchRows.length == 10000, s"Expected 10000 rows for 'spark', got ${textsearchRows.length}")
      assert(textsearchRows.forall(_.getString(1).contains("spark")), "All TEXTSEARCH results should contain 'spark'")

      // Boolean query
      val boolTextsearchRows = spark.sql(
        "SELECT id FROM large_syntax_test WHERE title TEXTSEARCH 'learning AND neural' ORDER BY id"
      ).collect()
      val boolIndexqueryRows = spark.sql(
        "SELECT id FROM large_syntax_test WHERE title indexquery 'learning AND neural' ORDER BY id"
      ).collect()
      assert(
        boolTextsearchRows.map(_.getInt(0)).toSeq == boolIndexqueryRows.map(_.getInt(0)).toSeq,
        "Boolean query must return identical rows across syntaxes"
      )

      // Phrase query
      val phraseTextsearchRows = spark.sql(
        """SELECT id FROM large_syntax_test WHERE title TEXTSEARCH '"machine learning"' ORDER BY id"""
      ).collect()
      val phraseIndexqueryRows = spark.sql(
        """SELECT id FROM large_syntax_test WHERE title indexquery '"machine learning"' ORDER BY id"""
      ).collect()
      assert(
        phraseTextsearchRows.map(_.getInt(0)).toSeq == phraseIndexqueryRows.map(_.getInt(0)).toSeq,
        "Phrase query must return identical rows across syntaxes"
      )
      assert(phraseTextsearchRows.length == 10000, s"Expected 10000 rows for phrase 'machine learning', got ${phraseTextsearchRows.length}")

      spark.conf.set("spark.indextables.read.defaultLimit", "250")
    }
  }

  // --- Compound WHERE clause tests ---

  test("End-to-end: TEXTSEARCH in compound WHERE clauses") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "technology"),
        (2, "machine learning algorithms", "ai"),
        (3, "spark streaming tutorial", "technology"),
        (4, "deep learning neural networks", "ai"),
        (5, "big data processing", "technology")
      ).toDF("id", "title", "category")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("compound_test")

      // 1. TEXTSEARCH AND standard filter
      val andResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE title TEXTSEARCH 'spark' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(andResults.length > 0, "TEXTSEARCH AND filter should return results")
      assert(andResults.forall(_.getString(2) == "technology"), "All results should have category=technology")
      assert(andResults.forall(_.getString(1).toLowerCase.contains("spark")), "All results should match 'spark'")

      // Verify legacy indexquery AND returns same results
      val andLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE title indexquery 'spark' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(
        andResults.map(_.getInt(0)).toSeq == andLegacy.map(_.getInt(0)).toSeq,
        "TEXTSEARCH AND and indexquery AND should return the same rows"
      )

      // 2. TEXTSEARCH OR TEXTSEARCH
      val orResults = spark.sql(
        "SELECT id, title FROM compound_test WHERE title TEXTSEARCH 'spark' OR title TEXTSEARCH 'learning' ORDER BY id"
      ).collect()
      assert(orResults.length > 0, "TEXTSEARCH OR TEXTSEARCH should return results")
      assert(
        orResults.forall(r => r.getString(1).toLowerCase.contains("spark") || r.getString(1).toLowerCase.contains("learning")),
        "All results should match 'spark' or 'learning'"
      )

      // 3. * TEXTSEARCH AND standard filter
      val starAndResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * TEXTSEARCH 'learning' AND category = 'ai' ORDER BY id"
      ).collect()
      assert(starAndResults.length > 0, "* TEXTSEARCH AND filter should return results")
      assert(starAndResults.forall(_.getString(2) == "ai"), "All results should have category=ai")

      // 4. TEXTSEARCH AND TEXTSEARCH
      val andAndResults = spark.sql(
        "SELECT id, title FROM compound_test WHERE title TEXTSEARCH 'spark' AND title TEXTSEARCH 'documentation' ORDER BY id"
      ).collect()
      assert(andAndResults.length == 1, s"Expected 1 row (only row 1 has both terms), got ${andAndResults.length}")
      assert(andAndResults(0).getInt(0) == 1, "Expected row id=1")

      // 5. TEXTSEARCH OR standard filter
      val textsearchOrResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE title TEXTSEARCH 'spark' OR category = 'ai' ORDER BY id"
      ).collect()
      assert(textsearchOrResults.length == 4, s"Expected 4 rows (ids 1,2,3,4), got ${textsearchOrResults.length}")
      assert(textsearchOrResults.map(_.getInt(0)).toSeq == Seq(1, 2, 3, 4), "Expected ids 1, 2, 3, 4")

      // 6. * TEXTSEARCH OR standard filter
      val starOrResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * TEXTSEARCH 'neural' OR category = 'technology' ORDER BY id"
      ).collect()
      assert(starOrResults.length == 4, s"Expected 4 rows (ids 1,3,4,5), got ${starOrResults.length}")
      assert(starOrResults.map(_.getInt(0)).toSeq == Seq(1, 3, 4, 5), "Expected ids 1, 3, 4, 5")
    }
  }

  test("preprocessor should not double-process already-converted function calls") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms"),
        (3, "big data processing")
      ).toDF("id", "title")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("idempotency_test")

      // 1. tantivy4spark_textsearch — if pass 2 re-matched, it would mangle the function
      val textsearchResults = spark.sql(
        "SELECT id, title FROM idempotency_test WHERE tantivy4spark_textsearch('title', 'spark') ORDER BY id"
      ).collect()
      assert(textsearchResults.length > 0, "tantivy4spark_textsearch should return results without being double-processed")
      assert(textsearchResults.forall(_.getString(1).toLowerCase.contains("spark")))

      // 2. tantivy4spark_indexqueryall — the negative lookbehind on pass 3 must prevent re-matching
      val indexqueryallResults = spark.sql(
        "SELECT id, title FROM idempotency_test WHERE tantivy4spark_indexqueryall('spark') ORDER BY id"
      ).collect()
      assert(indexqueryallResults.length > 0, "tantivy4spark_indexqueryall should return results without being double-processed")
      assert(indexqueryallResults.forall(_.getString(1).toLowerCase.contains("spark")))

      // 3. tantivy4spark_indexquery — legacy form must also pass through unchanged
      val indexqueryResults = spark.sql(
        "SELECT id, title FROM idempotency_test WHERE tantivy4spark_indexquery('title', 'spark') ORDER BY id"
      ).collect()
      assert(indexqueryResults.length > 0, "tantivy4spark_indexquery should return results without being double-processed")
      assert(indexqueryResults.forall(_.getString(1).toLowerCase.contains("spark")))

      // Cross-check: all three should return the same rows
      val ids1 = textsearchResults.map(_.getInt(0)).toSeq
      val ids2 = indexqueryallResults.map(_.getInt(0)).toSeq
      val ids3 = indexqueryResults.map(_.getInt(0)).toSeq
      assert(ids1 == ids3, "tantivy4spark_textsearch and tantivy4spark_indexquery should return identical rows")
      assert(ids1 == ids2, "tantivy4spark_textsearch and tantivy4spark_indexqueryall should return identical rows")
    }
  }

  test("End-to-end: all syntax variants (TEXTSEARCH, * TEXTSEARCH, indexquery, indexqueryall)") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "technology"),
        (2, "machine learning algorithms", "ai"),
        (3, "big data processing", "technology")
      ).toDF("id", "title", "category")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("all_syntax_test")

      // 1. TEXTSEARCH (new preferred syntax)
      val textsearchResults = spark.sql(
        "SELECT id, title FROM all_syntax_test WHERE title TEXTSEARCH 'spark' ORDER BY id"
      ).collect()
      assert(textsearchResults.length > 0, "TEXTSEARCH should return results")

      // 2. * TEXTSEARCH (new all-fields syntax)
      val starTextsearchResults = spark.sql(
        "SELECT id, title FROM all_syntax_test WHERE * TEXTSEARCH 'spark' ORDER BY id"
      ).collect()
      assert(starTextsearchResults.length > 0, "* TEXTSEARCH should return results")

      // 3. indexquery (legacy single-column syntax)
      val indexqueryResults = spark.sql(
        "SELECT id, title FROM all_syntax_test WHERE title indexquery 'spark' ORDER BY id"
      ).collect()
      assert(indexqueryResults.length > 0, "indexquery should return results")

      // 4. indexqueryall (legacy all-fields syntax)
      val indexqueryallResults = spark.sql(
        "SELECT id, title FROM all_syntax_test WHERE indexqueryall('spark') ORDER BY id"
      ).collect()
      assert(indexqueryallResults.length > 0, "indexqueryall should return results")

      // Cross-check: single-column variants return same results
      val textsearchIds = textsearchResults.map(_.getInt(0)).toSet
      val indexqueryIds = indexqueryResults.map(_.getInt(0)).toSet
      assert(textsearchIds == indexqueryIds, "TEXTSEARCH and indexquery should return the same rows")

      // Cross-check: all-fields variants return same results
      val starTextsearchIds = starTextsearchResults.map(_.getInt(0)).toSet
      val indexqueryallIds = indexqueryallResults.map(_.getInt(0)).toSet
      assert(starTextsearchIds == indexqueryallIds, "* TEXTSEARCH and indexqueryall should return the same rows")
    }
  }

  // --- * TEXTSEARCH / * FIELDMATCH field type mismatch warning ---

  test("* TEXTSEARCH should warn about non-tokenized fields in mixed-type table") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive"),
        (3, "big data processing", "active")
      ).toDF("id", "content", "status")

      // Write with mixed types: content=text (tokenized), status=string (non-tokenized)
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .load(tempPath)
      df.createOrReplaceTempView("mixed_type_warn_test")

      // Capture log output
      import org.apache.logging.log4j.LogManager
      import org.apache.logging.log4j.core.{LoggerContext, Logger => Log4jLogger}
      import org.apache.logging.log4j.core.appender.WriterAppender
      import org.apache.logging.log4j.core.layout.PatternLayout
      import org.apache.logging.log4j.Level

      val logCtx      = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val log4jLogger = logCtx.getLogger("io.indextables.spark.core.IndexTables4SparkScanBuilder").asInstanceOf[Log4jLogger]
      val origLevel   = log4jLogger.getLevel
      log4jLogger.setLevel(Level.WARN)

      val logOutput = new java.io.StringWriter()
      val layout    = PatternLayout.newBuilder().withPattern("%msg%n").build()
      val appender  = WriterAppender.createAppender(layout, null, logOutput, "test-warn-capture", false, true)
      appender.start()
      log4jLogger.addAppender(appender)

      try {
        // * TEXTSEARCH searches all fields — should warn about non-tokenized 'status'
        val results = spark.sql(
          "SELECT id, content FROM mixed_type_warn_test WHERE * TEXTSEARCH 'spark'"
        ).collect()

        val logStr = logOutput.toString
        assert(logStr.contains("TEXTSEARCH"), s"Expected TEXTSEARCH warning in log output but got: $logStr")
        assert(logStr.contains("non-tokenized"), s"Expected 'non-tokenized' in warning but got: $logStr")
        assert(logStr.contains("status"), s"Expected field name 'status' in warning but got: $logStr")

        // Query should still execute and return results (no behavioral change)
        assert(results.length > 0, "* TEXTSEARCH should still return results despite warning")
      } finally {
        log4jLogger.removeAppender(appender)
        appender.stop()
        log4jLogger.setLevel(origLevel)
      }
    }
  }

  test("* FIELDMATCH should warn about tokenized fields in mixed-type table") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive"),
        (3, "big data processing", "active")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .load(tempPath)
      df.createOrReplaceTempView("mixed_type_fieldmatch_warn_test")

      import org.apache.logging.log4j.LogManager
      import org.apache.logging.log4j.core.{LoggerContext, Logger => Log4jLogger}
      import org.apache.logging.log4j.core.appender.WriterAppender
      import org.apache.logging.log4j.core.layout.PatternLayout
      import org.apache.logging.log4j.Level

      val logCtx      = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val log4jLogger = logCtx.getLogger("io.indextables.spark.core.IndexTables4SparkScanBuilder").asInstanceOf[Log4jLogger]
      val origLevel   = log4jLogger.getLevel
      log4jLogger.setLevel(Level.WARN)

      val logOutput = new java.io.StringWriter()
      val layout    = PatternLayout.newBuilder().withPattern("%msg%n").build()
      val appender  = WriterAppender.createAppender(layout, null, logOutput, "test-fieldmatch-warn-capture", false, true)
      appender.start()
      log4jLogger.addAppender(appender)

      try {
        // * FIELDMATCH searches all fields — should warn about tokenized 'content'
        val results = spark.sql(
          "SELECT id, content FROM mixed_type_fieldmatch_warn_test WHERE * FIELDMATCH 'active'"
        ).collect()

        val logStr = logOutput.toString
        assert(logStr.contains("FIELDMATCH"), s"Expected FIELDMATCH warning in log output but got: $logStr")
        assert(logStr.contains("tokenized"), s"Expected 'tokenized' in warning but got: $logStr")
        assert(logStr.contains("content"), s"Expected field name 'content' in warning but got: $logStr")

        // Query should still execute (no behavioral change)
      } finally {
        log4jLogger.removeAppender(appender)
        appender.stop()
        log4jLogger.setLevel(origLevel)
      }
    }
  }

  // --- FIELDMATCH end-to-end SQL tests ---

  test("End-to-end: WHERE col FIELDMATCH 'query' via spark.sql") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive"),
        (3, "pending"),
        (4, "active"),
        (5, "inactive")
      ).toDF("id", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("fieldmatch_test_docs")

      val results = spark.sql("""
        SELECT id
        FROM fieldmatch_test_docs
        WHERE status FIELDMATCH 'active'
        ORDER BY id
      """).collect()

      assert(results.length == 2, s"FIELDMATCH query should return 2 results, got ${results.length}")
      assert(results.map(_.getInt(0)).toSeq == Seq(1, 4), "Should return ids 1 and 4")
    }
  }

  test("End-to-end: WHERE * FIELDMATCH 'query' via spark.sql") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive"),
        (3, "pending"),
        (4, "active"),
        (5, "inactive")
      ).toDF("id", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("star_fieldmatch_test_docs")

      val results = spark.sql("""
        SELECT id
        FROM star_fieldmatch_test_docs
        WHERE * FIELDMATCH 'active'
        ORDER BY id
      """).collect()

      assert(results.length > 0, "* FIELDMATCH query should return results")
      assert(results.map(_.getInt(0)).toSeq.contains(1), "Should include id 1 which has status=active")
    }
  }

  test("End-to-end: FIELDMATCH in compound WHERE clauses") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active", "technology"),
        (2, "machine learning algorithms", "inactive", "ai"),
        (3, "spark streaming tutorial", "active", "technology"),
        (4, "deep learning neural networks", "pending", "ai"),
        (5, "big data processing", "active", "data")
      ).toDF("id", "title", "status", "category")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_compound_test")

      // 1. FIELDMATCH AND standard filter
      val andResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE status FIELDMATCH 'active' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(andResults.length == 2, s"Expected 2 rows (ids 1,3), got ${andResults.length}")
      assert(andResults.map(_.getInt(0)).toSeq == Seq(1, 3), "Expected ids 1, 3")

      // 2. FIELDMATCH OR FIELDMATCH
      val orResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE status FIELDMATCH 'active' OR status FIELDMATCH 'pending' ORDER BY id"
      ).collect()
      assert(orResults.length == 4, s"Expected 4 rows (ids 1,3,4,5), got ${orResults.length}")
      assert(orResults.map(_.getInt(0)).toSeq == Seq(1, 3, 4, 5), "Expected ids 1, 3, 4, 5")

      // 3. * FIELDMATCH AND standard filter
      val starAndResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE * FIELDMATCH 'active' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(starAndResults.length > 0, "* FIELDMATCH AND filter should return results")
      assert(starAndResults.map(_.getInt(0)).toSeq == Seq(1, 3), "Expected ids 1, 3")

      // 4. Mixed TEXTSEARCH + FIELDMATCH
      val mixedResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE title TEXTSEARCH 'spark' AND status FIELDMATCH 'active' ORDER BY id"
      ).collect()
      assert(mixedResults.length == 2, s"Expected 2 rows (ids 1,3), got ${mixedResults.length}")
      assert(mixedResults.map(_.getInt(0)).toSeq == Seq(1, 3), "Expected ids 1, 3")
    }
  }

  test("FIELDMATCH should warn when no typemap configuration found at read time") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive"),
        (3, "active")
      ).toDF("id", "status")

      // Write WITH typemap
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      // Read WITHOUT typemap
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_no_typemap_test")

      import org.apache.logging.log4j.LogManager
      import org.apache.logging.log4j.core.{LoggerContext, Logger => Log4jLogger}
      import org.apache.logging.log4j.core.appender.WriterAppender
      import org.apache.logging.log4j.core.layout.PatternLayout
      import org.apache.logging.log4j.Level

      val logCtx      = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val log4jLogger = logCtx.getLogger("io.indextables.spark.core.IndexTables4SparkScanBuilder").asInstanceOf[Log4jLogger]
      val origLevel   = log4jLogger.getLevel
      log4jLogger.setLevel(Level.WARN)

      val logOutput = new java.io.StringWriter()
      val layout    = PatternLayout.newBuilder().withPattern("%msg%n").build()
      val appender  = WriterAppender.createAppender(layout, null, logOutput, "test-fieldmatch-no-typemap-capture", false, true)
      appender.start()
      log4jLogger.addAppender(appender)

      try {
        val results = spark.sql(
          "SELECT id FROM fieldmatch_no_typemap_test WHERE status FIELDMATCH 'active'"
        ).collect()

        val logStr = logOutput.toString
        assert(logStr.contains("Cannot validate FIELDMATCH"), s"Expected 'Cannot validate FIELDMATCH' in log but got: $logStr")
        assert(logStr.contains("no typemap configuration found"), s"Expected 'no typemap configuration found' in log but got: $logStr")

        // Query should still execute without throwing
        assert(results.length > 0, "FIELDMATCH should still return results even without typemap at read time")
      } finally {
        log4jLogger.removeAppender(appender)
        appender.stop()
        log4jLogger.setLevel(origLevel)
      }
    }
  }

  test("End-to-end 100k rows: FIELDMATCH returns correct results and matches indexquery") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

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
      val statuses = Array("active", "inactive", "pending", "archived", "draft")
      val categories = Array("technology", "ai", "data", "infrastructure", "devops")

      val rows = (1 to 100000).map { i =>
        (i, titles(i % titles.length), statuses(i % statuses.length), categories(i % categories.length))
      }
      val testData = rows.toDF("id", "title", "status", "category")

      testData.repartition(1).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      spark.conf.set("spark.indextables.read.defaultLimit", "200000")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("large_fieldmatch_test")

      val totalCount = spark.sql("SELECT count(*) FROM large_fieldmatch_test").collect()(0).getLong(0)
      assert(totalCount == 100000, s"Expected 100000 rows, got $totalCount")

      // FIELDMATCH for 'active' — should return 20000 rows (100k / 5 statuses)
      val fieldmatchRows = spark.sql(
        "SELECT id FROM large_fieldmatch_test WHERE status FIELDMATCH 'active' ORDER BY id"
      ).collect()
      assert(fieldmatchRows.length == 20000, s"Expected 20000 rows for FIELDMATCH 'active', got ${fieldmatchRows.length}")

      // Cross-check with indexquery — should return identical row ids
      val indexqueryRows = spark.sql(
        "SELECT id FROM large_fieldmatch_test WHERE status indexquery 'active' ORDER BY id"
      ).collect()
      val fieldmatchIds = fieldmatchRows.map(_.getInt(0)).toSeq
      val indexqueryIds = indexqueryRows.map(_.getInt(0)).toSeq
      assert(fieldmatchIds == indexqueryIds, "FIELDMATCH and indexquery must return identical rows")

      spark.conf.set("spark.indextables.read.defaultLimit", "250")
    }
  }

  test("* indexqueryall should NOT warn about field type mismatches") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .load(tempPath)
      df.createOrReplaceTempView("indexqueryall_no_warn_test")

      import org.apache.logging.log4j.LogManager
      import org.apache.logging.log4j.core.{LoggerContext, Logger => Log4jLogger}
      import org.apache.logging.log4j.core.appender.WriterAppender
      import org.apache.logging.log4j.core.layout.PatternLayout
      import org.apache.logging.log4j.Level

      val logCtx      = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val log4jLogger = logCtx.getLogger("io.indextables.spark.core.IndexTables4SparkScanBuilder").asInstanceOf[Log4jLogger]
      val origLevel   = log4jLogger.getLevel
      log4jLogger.setLevel(Level.WARN)

      val logOutput = new java.io.StringWriter()
      val layout    = PatternLayout.newBuilder().withPattern("%msg%n").build()
      val appender  = WriterAppender.createAppender(layout, null, logOutput, "test-no-warn-capture", false, true)
      appender.start()
      log4jLogger.addAppender(appender)

      try {
        // indexqueryall (legacy) should NOT trigger the warning
        val results = spark.sql(
          "SELECT id FROM indexqueryall_no_warn_test WHERE indexqueryall('spark')"
        ).collect()

        val logStr = logOutput.toString
        assert(
          !logStr.contains("non-tokenized") && !logStr.contains("TEXTSEARCH query will search"),
          s"indexqueryall should not trigger field type mismatch warning, but got: $logStr"
        )
      } finally {
        log4jLogger.removeAppender(appender)
        appender.stop()
        log4jLogger.setLevel(origLevel)
      }
    }
  }
}
