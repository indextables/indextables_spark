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

      // Read WITHOUT typemap options — field type comes from docMapping in transaction log
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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

      // Read WITHOUT typemap options — field type comes from docMapping in transaction log
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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

      // TEXTSEARCH (new preferred), indexquery (legacy), and * indexquery / indexqueryall
      // Note: * TEXTSEARCH is not used here because this table has mixed types (title=text, category=string)
      // and * TEXTSEARCH now throws on tables with non-tokenized fields.
      val textsearchRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title TEXTSEARCH 'spark' ORDER BY id"
      ).collect()

      val starIndexqueryRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE * indexquery 'spark' ORDER BY id"
      ).collect()

      val indexqueryRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title indexquery 'spark' ORDER BY id"
      ).collect()

      val indexqueryallRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE indexqueryall('spark') ORDER BY id"
      ).collect()

      val textsearchIds = textsearchRows.map(_.getInt(0)).sorted.toSeq
      val starIndexqueryIds = starIndexqueryRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryIds = indexqueryRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryallIds = indexqueryallRows.map(_.getInt(0)).sorted.toSeq

      assert(textsearchIds == indexqueryIds, "TEXTSEARCH and indexquery must return identical rows")
      assert(starIndexqueryIds == indexqueryallIds, "* indexquery and indexqueryall must return identical rows")
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

      // 3. * indexquery AND standard filter (use * indexquery since table has mixed types)
      val starAndResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * indexquery 'learning' AND category = 'ai' ORDER BY id"
      ).collect()
      assert(starAndResults.length > 0, "* indexquery AND filter should return results")
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

      // 6. * indexquery OR standard filter (use * indexquery since table has mixed types)
      val starOrResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * indexquery 'neural' OR category = 'technology' ORDER BY id"
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

  test("End-to-end: all syntax variants (TEXTSEARCH, * indexquery, indexquery, indexqueryall)") {
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

      // 2. * indexquery (all-fields without type validation, since table has mixed types)
      val starIndexqueryResults = spark.sql(
        "SELECT id, title FROM all_syntax_test WHERE * indexquery 'spark' ORDER BY id"
      ).collect()
      assert(starIndexqueryResults.length > 0, "* indexquery should return results")

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
      val starIndexqueryIds = starIndexqueryResults.map(_.getInt(0)).toSet
      val indexqueryallIds = indexqueryallResults.map(_.getInt(0)).toSet
      assert(starIndexqueryIds == indexqueryallIds, "* indexquery and indexqueryall should return the same rows")
    }
  }

  // --- * TEXTSEARCH / * FIELDMATCH field type mismatch warning ---

  test("* TEXTSEARCH should throw on non-tokenized fields in mixed-type table") {
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

      // Read WITHOUT typemap — docMapping provides field types
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("mixed_type_textsearch_throw_test")

      // * TEXTSEARCH on table with non-tokenized fields should throw
      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          "SELECT id, content FROM mixed_type_textsearch_throw_test WHERE * TEXTSEARCH 'spark'"
        ).collect()
      }
      assert(ex.getMessage.contains("TEXTSEARCH"), s"Expected TEXTSEARCH in error but got: ${ex.getMessage}")
      assert(ex.getMessage.contains("non-tokenized"), s"Expected 'non-tokenized' in error but got: ${ex.getMessage}")
    }
  }

  test("* FIELDMATCH should throw on tokenized fields in mixed-type table") {
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

      // Read WITHOUT typemap — docMapping provides field types
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("mixed_type_fieldmatch_throw_test")

      // * FIELDMATCH on table with tokenized fields should throw
      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          "SELECT id, content FROM mixed_type_fieldmatch_throw_test WHERE * FIELDMATCH 'active'"
        ).collect()
      }
      assert(ex.getMessage.contains("FIELDMATCH"), s"Expected FIELDMATCH in error but got: ${ex.getMessage}")
      assert(ex.getMessage.contains("tokenized"), s"Expected 'tokenized' in error but got: ${ex.getMessage}")
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

      // 3. * indexquery AND standard filter (use * indexquery since table has mixed types)
      val starAndResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE * indexquery 'active' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(starAndResults.length > 0, "* indexquery AND filter should return results")
      assert(starAndResults.map(_.getInt(0)).toSeq == Seq(1, 3), "Expected ids 1, 3")

      // 4. Mixed TEXTSEARCH + FIELDMATCH
      val mixedResults = spark.sql(
        "SELECT id FROM fieldmatch_compound_test WHERE title TEXTSEARCH 'spark' AND status FIELDMATCH 'active' ORDER BY id"
      ).collect()
      assert(mixedResults.length == 2, s"Expected 2 rows (ids 1,3), got ${mixedResults.length}")
      assert(mixedResults.map(_.getInt(0)).toSeq == Seq(1, 3), "Expected ids 1, 3")
    }
  }

  // --- MixedBooleanFilter failure path tests ---

  test("TEXTSEARCH on string field inside compound WHERE should throw") {
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
        .load(tempPath)
      df.createOrReplaceTempView("mixed_filter_fail_test")

      // status is a string (non-tokenized) field — TEXTSEARCH should throw even inside compound WHERE
      val ex = intercept[IllegalArgumentException] {
        spark.sql("SELECT id FROM mixed_filter_fail_test WHERE status TEXTSEARCH 'active' AND id > 0").collect()
      }
      assert(ex.getMessage.contains("Cannot use TEXTSEARCH"), s"Expected TEXTSEARCH rejection, got: ${ex.getMessage}")
      assert(ex.getMessage.contains("status"), s"Expected field name 'status' in error, got: ${ex.getMessage}")
    }
  }

  test("FIELDMATCH on text field inside compound WHERE should throw") {
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
        .load(tempPath)
      df.createOrReplaceTempView("mixed_filter_fail_test_2")

      // content is a text (tokenized) field — FIELDMATCH should throw even inside compound WHERE
      val ex = intercept[IllegalArgumentException] {
        spark.sql("SELECT id FROM mixed_filter_fail_test_2 WHERE content FIELDMATCH 'spark' AND id > 0").collect()
      }
      assert(ex.getMessage.contains("Cannot use FIELDMATCH"), s"Expected FIELDMATCH rejection, got: ${ex.getMessage}")
      assert(ex.getMessage.contains("content"), s"Expected field name 'content' in error, got: ${ex.getMessage}")
    }
  }

  test("FIELDMATCH should succeed without read-time typemap options (docMapping provides field types)") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "active"),
        (2, "inactive"),
        (3, "active")
      ).toDF("id", "status")

      // Write WITH typemap — native layer captures field type in docMapping
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      // Read WITHOUT typemap — docMapping in transaction log provides field type info
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_no_typemap_test")

      // FIELDMATCH on string field should succeed silently (docMapping knows it's non-tokenized)
      val results = spark.sql(
        "SELECT id FROM fieldmatch_no_typemap_test WHERE status FIELDMATCH 'active'"
      ).collect()

      assert(results.length > 0, "FIELDMATCH should return results when docMapping identifies field as non-tokenized")
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

  // --- * indexquery syntax ---

  test("* indexquery should parse as all-fields expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("* indexquery 'spark AND sql'")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("spark AND sql"))
    assert(allExpr.searchType == "indexquery")
  }

  test("* indexquery preprocessor should convert to tantivy4spark_indexqueryall") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation"),
        (2, "machine learning algorithms"),
        (3, "distributed computing systems")
      ).toDF("id", "content")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("star_indexquery_test")

      val starResults = spark.sql(
        "SELECT id FROM star_indexquery_test WHERE * indexquery 'spark'"
      ).collect().map(_.getInt(0)).sorted

      val indexqueryallResults = spark.sql(
        "SELECT id FROM star_indexquery_test WHERE indexqueryall('spark')"
      ).collect().map(_.getInt(0)).sorted

      assert(starResults.nonEmpty, "* indexquery should return results")
      assert(starResults.sameElements(indexqueryallResults),
        s"* indexquery and indexqueryall should return identical results")
    }
  }

  test("* indexquery end-to-end: returns same results as _indexall indexquery") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive"),
        (3, "spark streaming tutorial", "active")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      // Read WITHOUT typemap — * indexquery skips type validation
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("star_indexquery_e2e_test")

      val starResults = spark.sql(
        "SELECT id FROM star_indexquery_e2e_test WHERE * indexquery 'spark'"
      ).collect().map(_.getInt(0)).sorted

      val indexallResults = spark.sql(
        "SELECT id FROM star_indexquery_e2e_test WHERE _indexall indexquery 'spark'"
      ).collect().map(_.getInt(0)).sorted

      assert(starResults.nonEmpty, "* indexquery should return results")
      assert(starResults.sameElements(indexallResults),
        s"* indexquery [${starResults.mkString(",")}] and _indexall indexquery [${indexallResults.mkString(",")}] should return identical results")
    }
  }

  // --- Type validation for text_uuid and text_custom modes ---

  test("TEXTSEARCH on text_uuid_exactonly field should succeed") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "log entry with uuid 550e8400-e29b-41d4-a716-446655440000"),
        (2, "another log entry with uuid 6ba7b810-9dad-11d1-80b4-00c04fd430c8")
      ).toDF("id", "message")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_exactonly")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_exactonly")
        .load(tempPath)
      df.createOrReplaceTempView("textsearch_uuid_test")

      val results = spark.sql(
        "SELECT id FROM textsearch_uuid_test WHERE message TEXTSEARCH 'log'"
      ).collect()
      assert(results.length >= 1, "TEXTSEARCH on text_uuid_exactonly field should return results")
    }
  }

  test("FIELDMATCH on text_uuid_exactonly field should throw error") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "log entry with uuid"),
        (2, "another log entry")
      ).toDF("id", "message")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_exactonly")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_exactonly")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_uuid_test")

      val error = intercept[Exception] {
        spark.sql(
          "SELECT id FROM fieldmatch_uuid_test WHERE message FIELDMATCH 'log'"
        ).collect()
      }
      assert(error.getMessage.contains("Cannot use FIELDMATCH") ||
        error.getCause != null && error.getCause.getMessage.contains("Cannot use FIELDMATCH"),
        s"Expected FIELDMATCH rejection error for text_uuid_exactonly, got: ${error.getMessage}")
    }
  }

  test("TEXTSEARCH on text_uuid_strip field should succeed") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "log entry with uuid 550e8400-e29b-41d4-a716-446655440000"),
        (2, "another log entry")
      ).toDF("id", "message")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_strip")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_uuid_strip")
        .load(tempPath)
      df.createOrReplaceTempView("textsearch_uuid_strip_test")

      val results = spark.sql(
        "SELECT id FROM textsearch_uuid_strip_test WHERE message TEXTSEARCH 'log'"
      ).collect()
      assert(results.length >= 1, "TEXTSEARCH on text_uuid_strip field should return results")
    }
  }

  test("FIELDMATCH on text_custom_exactonly field should throw error") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "user logged in from 192.168.1.1"),
        (2, "request from 10.0.0.1 failed")
      ).toDF("id", "message")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_custom_exactonly:\\d+\\.\\d+\\.\\d+\\.\\d+")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message", "text_custom_exactonly:\\d+\\.\\d+\\.\\d+\\.\\d+")
        .load(tempPath)
      df.createOrReplaceTempView("fieldmatch_custom_test")

      val error = intercept[Exception] {
        spark.sql(
          "SELECT id FROM fieldmatch_custom_test WHERE message FIELDMATCH 'user'"
        ).collect()
      }
      assert(error.getMessage.contains("Cannot use FIELDMATCH") ||
        error.getCause != null && error.getCause.getMessage.contains("Cannot use FIELDMATCH"),
        s"Expected FIELDMATCH rejection error for text_custom_exactonly, got: ${error.getMessage}")
    }
  }

  // --- DocMapping-based type validation (read without typemap options) ---

  test("DocMapping-based type validation should work at read time without typemap options") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        (1, "apache spark documentation", "active"),
        (2, "machine learning algorithms", "inactive")
      ).toDF("id", "content", "status")

      // Write with typemap options — native layer captures field types in docMapping
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.status", "string")
        .mode("overwrite")
        .save(tempPath)

      // Read WITHOUT specifying typemap options — docMapping provides field type info
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("docmapping_validate_test")

      // TEXTSEARCH on text field should succeed (docMapping knows it's tokenized)
      val results = spark.sql(
        "SELECT id FROM docmapping_validate_test WHERE content TEXTSEARCH 'spark'"
      ).collect()
      assert(results.length >= 1, "TEXTSEARCH should work with field type from docMapping")

      // TEXTSEARCH on string field should throw (docMapping knows it's not tokenized)
      val error = intercept[Exception] {
        spark.sql(
          "SELECT id FROM docmapping_validate_test WHERE status TEXTSEARCH 'active'"
        ).collect()
      }
      assert(error.getMessage.contains("Cannot use TEXTSEARCH") ||
        error.getCause != null && error.getCause.getMessage.contains("Cannot use TEXTSEARCH"),
        s"Expected TEXTSEARCH rejection for string field (from docMapping), got: ${error.getMessage}")
    }
  }

  test("DocMapping-based validation: FIELDMATCH on string field succeeds, TEXTSEARCH throws") {
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

      // Read WITHOUT specifying typemap options — docMapping in splits provides field types
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("docmapping_fieldmatch_test")

      // FIELDMATCH on string field should succeed (docMapping identifies it as non-tokenized)
      val results = spark.sql(
        "SELECT id FROM docmapping_fieldmatch_test WHERE status FIELDMATCH 'active'"
      ).collect()
      assert(results.length >= 1, "FIELDMATCH on string field should work via docMapping")

      // FIELDMATCH on text field should throw (docMapping identifies it as tokenized)
      val error = intercept[Exception] {
        spark.sql(
          "SELECT id FROM docmapping_fieldmatch_test WHERE content FIELDMATCH 'spark'"
        ).collect()
      }
      assert(error.getMessage.contains("Cannot use FIELDMATCH") ||
        error.getCause != null && error.getCause.getMessage.contains("Cannot use FIELDMATCH"),
        s"Expected FIELDMATCH rejection for text field (from docMapping), got: ${error.getMessage}")
    }
  }

  // --- SearchType require() validation ---

  test("Invalid searchType should throw IllegalArgumentException") {
    import org.apache.spark.sql.catalyst.expressions._
    import org.apache.spark.sql.types.StringType
    import org.apache.spark.unsafe.types.UTF8String

    val column = AttributeReference("title", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("test"), StringType)

    // Valid searchTypes should work
    IndexQueryExpression(column, query, "indexquery")
    IndexQueryExpression(column, query, "textsearch")
    IndexQueryExpression(column, query, "fieldmatch")

    // Invalid searchType should throw
    intercept[IllegalArgumentException] {
      IndexQueryExpression(column, query, "textSearch")
    }
    intercept[IllegalArgumentException] {
      IndexQueryExpression(column, query, "field_match")
    }
    intercept[IllegalArgumentException] {
      IndexQueryExpression(column, query, "indexqueryall")
    }
  }

  test("Invalid searchType on IndexQueryAllExpression should throw IllegalArgumentException") {
    import org.apache.spark.sql.catalyst.expressions._
    import org.apache.spark.sql.types.StringType
    import org.apache.spark.unsafe.types.UTF8String

    val query = Literal(UTF8String.fromString("test"), StringType)

    // Valid searchTypes should work
    IndexQueryAllExpression(query, "indexqueryall")
    IndexQueryAllExpression(query, "textsearch")
    IndexQueryAllExpression(query, "fieldmatch")

    // indexquery is valid for all-fields (used by * indexquery syntax)
    IndexQueryAllExpression(query, "indexquery")

    // Invalid searchType should throw
    intercept[IllegalArgumentException] {
      IndexQueryAllExpression(query, "textSearch")
    }
    intercept[IllegalArgumentException] {
      IndexQueryAllExpression(query, "invalid_type")
    }
  }
}
