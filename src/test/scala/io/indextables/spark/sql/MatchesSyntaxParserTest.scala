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

class MatchesSyntaxParserTest extends AnyFunSuite with TestBase {

  // --- MATCHES basic parsing ---

  test("MATCHES should parse simple expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("title MATCHES 'spark AND sql'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("spark AND sql"))
  }

  test("MATCHES should be case-insensitive") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr1 = parser.parseExpression("title MATCHES 'query'")
    val expr2 = parser.parseExpression("title matches 'query'")
    val expr3 = parser.parseExpression("title Matches 'query'")

    assert(expr1.isInstanceOf[IndexQueryExpression])
    assert(expr2.isInstanceOf[IndexQueryExpression])
    assert(expr3.isInstanceOf[IndexQueryExpression])

    assert(expr1.asInstanceOf[IndexQueryExpression].getQueryString.contains("query"))
    assert(expr2.asInstanceOf[IndexQueryExpression].getQueryString.contains("query"))
    assert(expr3.asInstanceOf[IndexQueryExpression].getQueryString.contains("query"))
  }

  test("MATCHES should handle backtick columns") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("`my column` MATCHES 'test'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("test"))
  }

  test("MATCHES should handle complex Tantivy syntax") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("content MATCHES '(apache AND spark) OR (hadoop AND mapreduce)'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("content"))
    assert(indexQueryExpr.getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
  }

  test("MATCHES should handle qualified column names") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("table.columnName MATCHES 'search term'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    assert(indexQueryExpr.getQueryString.contains("search term"))
  }

  // --- * MATCHES parsing ---

  test("* MATCHES should parse all-fields expression") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("* MATCHES 'error timeout'")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("error timeout"))
  }

  test("* MATCHES should be case-insensitive") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr1 = parser.parseExpression("* MATCHES 'query'")
    val expr2 = parser.parseExpression("* matches 'query'")
    val expr3 = parser.parseExpression("* Matches 'query'")

    assert(expr1.isInstanceOf[IndexQueryAllExpression])
    assert(expr2.isInstanceOf[IndexQueryAllExpression])
    assert(expr3.isInstanceOf[IndexQueryAllExpression])

    assert(expr1.asInstanceOf[IndexQueryAllExpression].getQueryString.contains("query"))
    assert(expr2.asInstanceOf[IndexQueryAllExpression].getQueryString.contains("query"))
    assert(expr3.asInstanceOf[IndexQueryAllExpression].getQueryString.contains("query"))
  }

  // --- End-to-end SQL tests ---

  test("End-to-end: WHERE col MATCHES 'query' via spark.sql") {
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

      df.createOrReplaceTempView("matches_test_docs")

      val results = spark.sql("""
        SELECT id, title
        FROM matches_test_docs
        WHERE title MATCHES 'spark'
        ORDER BY id
      """).collect()

      assert(results.length > 0, "MATCHES query should return results")
      assert(
        results.exists(_.getString(1).toLowerCase.contains("spark")),
        "MATCHES should return documents containing the search term"
      )
    }
  }

  test("End-to-end: WHERE * MATCHES 'query' via spark.sql") {
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

      df.createOrReplaceTempView("star_matches_test_docs")

      val results = spark.sql("""
        SELECT id, title
        FROM star_matches_test_docs
        WHERE * MATCHES 'spark'
        ORDER BY id
      """).collect()

      assert(results.length > 0, "* MATCHES query should return results")
      assert(
        results.exists(_.getString(1).toLowerCase.contains("spark")),
        "* MATCHES should return documents containing the search term"
      )
    }
  }

  // --- Backward compatibility ---

  test("Legacy indexquery syntax should still work") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("title indexquery 'spark AND sql'")

    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]

    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("spark AND sql"))
  }

  test("Legacy indexqueryall syntax should still work") {
    val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

    val expr = parser.parseExpression("indexqueryall('spark AND sql')")

    assert(expr.isInstanceOf[IndexQueryAllExpression])
    val allExpr = expr.asInstanceOf[IndexQueryAllExpression]
    assert(allExpr.getQueryString.contains("spark AND sql"))
  }

  // --- Comprehensive end-to-end: all 4 syntaxes against same dataset ---

  test("End-to-end 100k rows: all syntax variants return identical results") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Generate 100k rows with varied content so search is selective
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

      // Write with repartition(1) so all rows land in a single split
      testData.repartition(1).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      // Set a high read limit so we get all matching rows back (default is 250 per split)
      spark.conf.set("spark.indextables.read.defaultLimit", "200000")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      df.createOrReplaceTempView("large_syntax_test")

      val totalCount = spark.sql("SELECT count(*) FROM large_syntax_test").collect()(0).getLong(0)
      println(s"\n========== DATASET: $totalCount rows ==========")
      assert(totalCount == 100000, s"Expected 100000 rows, got $totalCount")

      // --- Search 1: selective term "spark" (appears in 1 out of 10 titles = 10k rows) ---
      println("\n--- Search term: 'spark' (expected: 10k rows) ---")

      val t1 = System.currentTimeMillis()
      val matchesRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title MATCHES 'spark' ORDER BY id"
      ).collect()
      val t2 = System.currentTimeMillis()

      val starMatchesRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE * MATCHES 'spark' ORDER BY id"
      ).collect()
      val t3 = System.currentTimeMillis()

      val indexqueryRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE title indexquery 'spark' ORDER BY id"
      ).collect()
      val t4 = System.currentTimeMillis()

      val indexqueryallRows = spark.sql(
        "SELECT id, title FROM large_syntax_test WHERE indexqueryall('spark') ORDER BY id"
      ).collect()
      val t5 = System.currentTimeMillis()

      println(s"  MATCHES:       ${matchesRows.length} rows (${t2 - t1}ms)")
      println(s"  * MATCHES:     ${starMatchesRows.length} rows (${t3 - t2}ms)")
      println(s"  indexquery:    ${indexqueryRows.length} rows (${t4 - t3}ms)")
      println(s"  indexqueryall: ${indexqueryallRows.length} rows (${t5 - t4}ms)")

      // Sample first 5 rows from MATCHES
      println(s"  Sample rows (MATCHES, first 5):")
      matchesRows.take(5).foreach(r => println(s"    id=${r.getInt(0)}, title='${r.getString(1)}'"))

      val matchesIds = matchesRows.map(_.getInt(0)).sorted.toSeq
      val starMatchesIds = starMatchesRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryIds = indexqueryRows.map(_.getInt(0)).sorted.toSeq
      val indexqueryallIds = indexqueryallRows.map(_.getInt(0)).sorted.toSeq

      println(s"  MATCHES == indexquery:      ${matchesIds == indexqueryIds}")
      println(s"  * MATCHES == indexqueryall: ${starMatchesIds == indexqueryallIds}")

      assert(matchesIds == indexqueryIds, "MATCHES and indexquery must return identical rows")
      assert(starMatchesIds == indexqueryallIds, "* MATCHES and indexqueryall must return identical rows")
      assert(matchesRows.length == 10000, s"Expected 10000 rows for 'spark', got ${matchesRows.length}")
      assert(matchesRows.forall(_.getString(1).contains("spark")), "All MATCHES results should contain 'spark'")

      // --- Search 2: boolean query "learning AND neural" (no single title has both) ---
      println("\n--- Search term: 'learning AND neural' (cross-term boolean) ---")

      val boolMatchesRows = spark.sql(
        "SELECT id FROM large_syntax_test WHERE title MATCHES 'learning AND neural' ORDER BY id"
      ).collect()
      val boolIndexqueryRows = spark.sql(
        "SELECT id FROM large_syntax_test WHERE title indexquery 'learning AND neural' ORDER BY id"
      ).collect()

      println(s"  MATCHES:    ${boolMatchesRows.length} rows")
      println(s"  indexquery: ${boolIndexqueryRows.length} rows")
      println(s"  Equal:      ${boolMatchesRows.map(_.getInt(0)).toSeq == boolIndexqueryRows.map(_.getInt(0)).toSeq}")

      assert(
        boolMatchesRows.map(_.getInt(0)).toSeq == boolIndexqueryRows.map(_.getInt(0)).toSeq,
        "Boolean query must return identical rows across syntaxes"
      )

      // --- Search 3: phrase query ---
      println("\n--- Search term: '\"machine learning\"' (phrase) ---")

      val phraseMatchesRows = spark.sql(
        """SELECT id FROM large_syntax_test WHERE title MATCHES '"machine learning"' ORDER BY id"""
      ).collect()
      val phraseIndexqueryRows = spark.sql(
        """SELECT id FROM large_syntax_test WHERE title indexquery '"machine learning"' ORDER BY id"""
      ).collect()

      println(s"  MATCHES:    ${phraseMatchesRows.length} rows")
      println(s"  indexquery: ${phraseIndexqueryRows.length} rows")
      println(s"  Equal:      ${phraseMatchesRows.map(_.getInt(0)).toSeq == phraseIndexqueryRows.map(_.getInt(0)).toSeq}")

      assert(
        phraseMatchesRows.map(_.getInt(0)).toSeq == phraseIndexqueryRows.map(_.getInt(0)).toSeq,
        "Phrase query must return identical rows across syntaxes"
      )
      assert(phraseMatchesRows.length == 10000, s"Expected 10000 rows for phrase 'machine learning', got ${phraseMatchesRows.length}")

      println("\n========== ALL CHECKS PASSED ==========\n")

      // Reset default limit to avoid leaking to other tests
      spark.conf.set("spark.indextables.read.defaultLimit", "250")
    }
  }

  // --- Compound WHERE clause tests ---

  test("End-to-end: MATCHES in compound WHERE clauses") {
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

      // 1. MATCHES AND standard filter
      println("\n========== COMPOUND WHERE CLAUSES ==========")
      println("\n--- MATCHES AND category = 'technology' ---")
      val andResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE title MATCHES 'spark' AND category = 'technology' ORDER BY id"
      ).collect()
      andResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}', category='${r.getString(2)}'"))
      println(s"  Rows returned: ${andResults.length}")
      assert(andResults.length > 0, "MATCHES AND filter should return results")
      assert(andResults.forall(_.getString(2) == "technology"), "All results should have category=technology")
      assert(andResults.forall(_.getString(1).toLowerCase.contains("spark")), "All results should match 'spark'")

      // Verify legacy indexquery AND returns same results
      val andLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE title indexquery 'spark' AND category = 'technology' ORDER BY id"
      ).collect()
      assert(
        andResults.map(_.getInt(0)).toSeq == andLegacy.map(_.getInt(0)).toSeq,
        "MATCHES AND and indexquery AND should return the same rows"
      )
      println(s"  MATCHES AND == indexquery AND: true")

      // 2. MATCHES OR MATCHES
      println("\n--- MATCHES OR MATCHES ---")
      val orResults = spark.sql(
        "SELECT id, title FROM compound_test WHERE title MATCHES 'spark' OR title MATCHES 'learning' ORDER BY id"
      ).collect()
      orResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${orResults.length}")
      assert(orResults.length > 0, "MATCHES OR MATCHES should return results")
      assert(
        orResults.forall(r => r.getString(1).toLowerCase.contains("spark") || r.getString(1).toLowerCase.contains("learning")),
        "All results should match 'spark' or 'learning'"
      )

      // Verify legacy indexquery OR returns same results
      val orLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE title indexquery 'spark' OR title indexquery 'learning' ORDER BY id"
      ).collect()
      assert(
        orResults.map(_.getInt(0)).toSeq == orLegacy.map(_.getInt(0)).toSeq,
        "MATCHES OR and indexquery OR should return the same rows"
      )
      println(s"  MATCHES OR == indexquery OR: true")

      // 3. * MATCHES AND standard filter
      println("\n--- * MATCHES AND category = 'ai' ---")
      val starAndResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * MATCHES 'learning' AND category = 'ai' ORDER BY id"
      ).collect()
      starAndResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}', category='${r.getString(2)}'"))
      println(s"  Rows returned: ${starAndResults.length}")
      assert(starAndResults.length > 0, "* MATCHES AND filter should return results")
      assert(starAndResults.forall(_.getString(2) == "ai"), "All results should have category=ai")

      // Verify legacy indexqueryall AND returns same results
      val starAndLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE indexqueryall('learning') AND category = 'ai' ORDER BY id"
      ).collect()
      assert(
        starAndResults.map(_.getInt(0)).toSeq == starAndLegacy.map(_.getInt(0)).toSeq,
        "* MATCHES AND and indexqueryall AND should return the same rows"
      )
      println(s"  * MATCHES AND == indexqueryall AND: true")

      // 4. MATCHES AND MATCHES (two different text searches combined)
      println("\n--- MATCHES AND MATCHES ---")
      val andAndResults = spark.sql(
        "SELECT id, title FROM compound_test WHERE title MATCHES 'spark' AND title MATCHES 'documentation' ORDER BY id"
      ).collect()
      andAndResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${andAndResults.length}")
      assert(andAndResults.length == 1, s"Expected 1 row (only row 1 has both terms), got ${andAndResults.length}")
      assert(andAndResults(0).getInt(0) == 1, "Expected row id=1")
      assert(andAndResults(0).getString(1).contains("spark") && andAndResults(0).getString(1).contains("documentation"),
        "Result should contain both 'spark' and 'documentation'")

      // Verify legacy indexquery AND returns same results
      val andAndLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE title indexquery 'spark' AND title indexquery 'documentation' ORDER BY id"
      ).collect()
      assert(
        andAndResults.map(_.getInt(0)).toSeq == andAndLegacy.map(_.getInt(0)).toSeq,
        "MATCHES AND MATCHES and indexquery AND indexquery should return the same rows"
      )
      println(s"  MATCHES AND MATCHES == indexquery AND indexquery: true")

      // 5. MATCHES OR standard filter
      println("\n--- MATCHES OR standard filter ---")
      val matchesOrResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE title MATCHES 'spark' OR category = 'ai' ORDER BY id"
      ).collect()
      matchesOrResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}', category='${r.getString(2)}'"))
      println(s"  Rows returned: ${matchesOrResults.length}")
      assert(matchesOrResults.length == 4, s"Expected 4 rows (ids 1,2,3,4), got ${matchesOrResults.length}")
      assert(matchesOrResults.map(_.getInt(0)).toSeq == Seq(1, 2, 3, 4), "Expected ids 1, 2, 3, 4")
      assert(
        matchesOrResults.forall(r => r.getString(1).toLowerCase.contains("spark") || r.getString(2) == "ai"),
        "All results should match 'spark' or have category=ai"
      )

      // Verify legacy indexquery OR returns same results
      val matchesOrLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE title indexquery 'spark' OR category = 'ai' ORDER BY id"
      ).collect()
      assert(
        matchesOrResults.map(_.getInt(0)).toSeq == matchesOrLegacy.map(_.getInt(0)).toSeq,
        "MATCHES OR filter and indexquery OR filter should return the same rows"
      )
      println(s"  MATCHES OR filter == indexquery OR filter: true")

      // 6. * MATCHES OR standard filter
      println("\n--- * MATCHES OR standard filter ---")
      val starOrResults = spark.sql(
        "SELECT id, title, category FROM compound_test WHERE * MATCHES 'neural' OR category = 'technology' ORDER BY id"
      ).collect()
      starOrResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}', category='${r.getString(2)}'"))
      println(s"  Rows returned: ${starOrResults.length}")
      assert(starOrResults.length == 4, s"Expected 4 rows (ids 1,3,4,5), got ${starOrResults.length}")
      assert(starOrResults.map(_.getInt(0)).toSeq == Seq(1, 3, 4, 5), "Expected ids 1, 3, 4, 5")
      assert(
        starOrResults.forall(r => r.getString(1).toLowerCase.contains("neural") || r.getString(2) == "technology"),
        "All results should match 'neural' or have category=technology"
      )

      // Verify legacy indexqueryall OR returns same results
      val starOrLegacy = spark.sql(
        "SELECT id FROM compound_test WHERE indexqueryall('neural') OR category = 'technology' ORDER BY id"
      ).collect()
      assert(
        starOrResults.map(_.getInt(0)).toSeq == starOrLegacy.map(_.getInt(0)).toSeq,
        "* MATCHES OR filter and indexqueryall OR filter should return the same rows"
      )
      println(s"  * MATCHES OR filter == indexqueryall OR filter: true")

      println("\n========== COMPOUND CHECKS PASSED ==========\n")
    }
  }

  test("End-to-end: all syntax variants (MATCHES, * MATCHES, indexquery, indexqueryall)") {
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

      println("\n========== TEST DATA ==========")
      val allRows = spark.sql("SELECT id, title, category FROM all_syntax_test ORDER BY id").collect()
      allRows.foreach(r => println(s"  id=${r.getInt(0)}, title='${r.getString(1)}', category='${r.getString(2)}'"))
      println(s"  Total rows: ${allRows.length}")

      // 1. MATCHES (new single-column syntax)
      println("\n========== 1. title MATCHES 'spark' ==========")
      val matchesSql = "SELECT id, title FROM all_syntax_test WHERE title MATCHES 'spark' ORDER BY id"
      println(s"  SQL: $matchesSql")
      val matchesResults = spark.sql(matchesSql).collect()
      matchesResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${matchesResults.length}")
      assert(matchesResults.length > 0, "MATCHES should return results")
      assert(
        matchesResults.exists(_.getString(1).toLowerCase.contains("spark")),
        "MATCHES should find 'spark'"
      )

      // 2. * MATCHES (new all-fields syntax)
      println("\n========== 2. * MATCHES 'spark' ==========")
      val starMatchesSql = "SELECT id, title FROM all_syntax_test WHERE * MATCHES 'spark' ORDER BY id"
      println(s"  SQL: $starMatchesSql")
      val starMatchesResults = spark.sql(starMatchesSql).collect()
      starMatchesResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${starMatchesResults.length}")
      assert(starMatchesResults.length > 0, "* MATCHES should return results")
      assert(
        starMatchesResults.exists(_.getString(1).toLowerCase.contains("spark")),
        "* MATCHES should find 'spark'"
      )

      // 3. indexquery (legacy single-column syntax)
      println("\n========== 3. title indexquery 'spark' ==========")
      val indexquerySql = "SELECT id, title FROM all_syntax_test WHERE title indexquery 'spark' ORDER BY id"
      println(s"  SQL: $indexquerySql")
      val indexqueryResults = spark.sql(indexquerySql).collect()
      indexqueryResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${indexqueryResults.length}")
      assert(indexqueryResults.length > 0, "indexquery should return results")
      assert(
        indexqueryResults.exists(_.getString(1).toLowerCase.contains("spark")),
        "indexquery should find 'spark'"
      )

      // 4. indexqueryall (legacy all-fields syntax)
      println("\n========== 4. indexqueryall('spark') ==========")
      val indexqueryallSql = "SELECT id, title FROM all_syntax_test WHERE indexqueryall('spark') ORDER BY id"
      println(s"  SQL: $indexqueryallSql")
      val indexqueryallResults = spark.sql(indexqueryallSql).collect()
      indexqueryallResults.foreach(r => println(s"  => id=${r.getInt(0)}, title='${r.getString(1)}'"))
      println(s"  Rows returned: ${indexqueryallResults.length}")
      assert(indexqueryallResults.length > 0, "indexqueryall should return results")
      assert(
        indexqueryallResults.exists(_.getString(1).toLowerCase.contains("spark")),
        "indexqueryall should find 'spark'"
      )

      // Cross-check: single-column variants return same results
      val matchesIds = matchesResults.map(_.getInt(0)).toSet
      val indexqueryIds = indexqueryResults.map(_.getInt(0)).toSet
      println(s"\n========== CROSS-CHECK ==========")
      println(s"  MATCHES ids:       $matchesIds")
      println(s"  indexquery ids:     $indexqueryIds")
      println(s"  Single-col match:  ${matchesIds == indexqueryIds}")
      assert(matchesIds == indexqueryIds, "MATCHES and indexquery should return the same rows")

      // Cross-check: all-fields variants return same results
      val starMatchesIds = starMatchesResults.map(_.getInt(0)).toSet
      val indexqueryallIds = indexqueryallResults.map(_.getInt(0)).toSet
      println(s"  * MATCHES ids:     $starMatchesIds")
      println(s"  indexqueryall ids: $indexqueryallIds")
      println(s"  All-fields match:  ${starMatchesIds == indexqueryallIds}")
      assert(starMatchesIds == indexqueryallIds, "* MATCHES and indexqueryall should return the same rows")
      println()
    }
  }
}
