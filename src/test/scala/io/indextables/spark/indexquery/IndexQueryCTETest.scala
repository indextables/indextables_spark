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

package io.indextables.spark.indexquery

import io.indextables.spark.TestBase

/**
 * Test cases for IndexQuery operator with Common Table Expressions (CTEs).
 *
 * These tests verify that the indexquery operator works correctly when used
 * within CTE (WITH ... AS) SQL constructs, both in the inner CTE definition
 * and in the outer query that references the CTE.
 */
class IndexQueryCTETest extends TestBase {

  private val format = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("IndexQuery in outer query of CTE - filter CTE results with indexquery") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_outer_indexquery_test"

    // Create test data with a text field for full-text search
    val testData = Seq(
      ("doc1", "apache spark documentation and tutorials", "technology"),
      ("doc2", "machine learning algorithms with spark", "ai"),
      ("doc3", "big data processing frameworks", "technology"),
      ("doc4", "natural language processing basics", "ai"),
      ("doc5", "distributed spark computing systems", "technology")
    ).toDF("id", "content", "category")

    // Write with content as text field for tokenized search
    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    // Read and create temp view
    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("documents")

    // CTE with regular filter, indexquery in outer query
    val sql = """
      WITH filtered_docs AS (
        SELECT * FROM documents
        WHERE category = 'technology'
      )
      SELECT id, content, category
      FROM filtered_docs
      WHERE content indexquery 'spark'
      ORDER BY id
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE outer indexquery results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}'"))

    // Should find documents that are both technology category AND contain spark
    assert(rows.length >= 1, "Should return at least one result")
    rows.foreach { row =>
      assert(row.getString(2) == "technology", "All results should be technology category")
      assert(row.getString(1).toLowerCase.contains("spark"), "All results should contain 'spark'")
    }
  }

  test("IndexQuery in inner CTE definition - apply indexquery within CTE") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_inner_indexquery_test"

    // Create test data
    val testData = Seq(
      ("doc1", "apache spark documentation and tutorials", "technology"),
      ("doc2", "machine learning algorithms with spark", "ai"),
      ("doc3", "big data processing frameworks", "technology"),
      ("doc4", "natural language processing basics", "ai"),
      ("doc5", "distributed spark computing systems", "technology")
    ).toDF("id", "content", "category")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("documents_inner")

    // CTE with indexquery in inner definition, regular filter in outer query
    val sql = """
      WITH spark_docs AS (
        SELECT * FROM documents_inner
        WHERE content indexquery 'spark'
      )
      SELECT id, content, category
      FROM spark_docs
      WHERE category = 'technology'
      ORDER BY id
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE inner indexquery results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}'"))

    // Should find documents that contain spark AND are technology category
    assert(rows.length >= 1, "Should return at least one result")
    rows.foreach { row =>
      assert(row.getString(2) == "technology", "All results should be technology category")
      assert(row.getString(1).toLowerCase.contains("spark"), "All results should contain 'spark'")
    }
  }

  test("IndexQuery in both CTE and outer query") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_dual_indexquery_test"

    // Create test data with two text fields
    val testData = Seq(
      ("doc1", "apache spark tutorials", "beginner guide for developers"),
      ("doc2", "spark machine learning", "advanced algorithms tutorial"),
      ("doc3", "hadoop big data", "enterprise guide"),
      ("doc4", "spark streaming guide", "real-time processing tutorial"),
      ("doc5", "python programming", "beginner tutorial")
    ).toDF("id", "title", "description")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.description", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("articles")

    // IndexQuery in both CTE and outer query on different fields
    val sql = """
      WITH spark_articles AS (
        SELECT * FROM articles
        WHERE title indexquery 'spark'
      )
      SELECT id, title, description
      FROM spark_articles
      WHERE description indexquery 'tutorial'
      ORDER BY id
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE dual indexquery results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, title='${r.getString(1)}', desc='${r.getString(2)}'"))

    // Should find documents with spark in title AND tutorial in description
    rows.foreach { row =>
      assert(row.getString(1).toLowerCase.contains("spark"), "Title should contain 'spark'")
      assert(row.getString(2).toLowerCase.contains("tutorial"), "Description should contain 'tutorial'")
    }
  }

  test("Multiple CTEs with IndexQuery") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_multiple_test"

    val testData = Seq(
      ("doc1", "apache spark documentation", "tech", 100),
      ("doc2", "machine learning spark", "ai", 200),
      ("doc3", "big data frameworks", "tech", 150),
      ("doc4", "spark streaming real-time", "tech", 300),
      ("doc5", "python basics", "programming", 50)
    ).toDF("id", "content", "category", "views")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("articles_multi")

    // Multiple CTEs, one with indexquery
    val sql = """
      WITH spark_docs AS (
        SELECT * FROM articles_multi
        WHERE content indexquery 'spark'
      ),
      popular_docs AS (
        SELECT * FROM spark_docs
        WHERE views > 100
      )
      SELECT id, content, category, views
      FROM popular_docs
      ORDER BY views DESC
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"Multiple CTEs results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}', views=${r.getInt(3)}"))

    // Should find spark docs with views > 100
    rows.foreach { row =>
      assert(row.getString(1).toLowerCase.contains("spark"), "Content should contain 'spark'")
      assert(row.getInt(3) > 100, "Views should be > 100")
    }
  }

  test("CTE with IndexQuery and aggregation") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_agg_test"

    val testData = Seq(
      ("doc1", "apache spark documentation", "tech"),
      ("doc2", "machine learning spark", "ai"),
      ("doc3", "spark streaming", "tech"),
      ("doc4", "spark sql guide", "tech"),
      ("doc5", "python basics", "programming")
    ).toDF("id", "content", "category")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("articles_agg")

    // CTE with indexquery, aggregation in outer query
    val sql = """
      WITH spark_docs AS (
        SELECT * FROM articles_agg
        WHERE content indexquery 'spark'
      )
      SELECT category, COUNT(*) as doc_count
      FROM spark_docs
      GROUP BY category
      ORDER BY doc_count DESC
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE aggregation results: ${rows.length}")
    rows.foreach(r => println(s"  category=${r.getString(0)}, count=${r.getLong(1)}"))

    // Should have grouped results
    assert(rows.length >= 1, "Should have at least one category")
    val totalCount = rows.map(_.getLong(1)).sum
    assert(totalCount >= 1, "Should have counted at least one spark document")
  }

  test("Nested CTE references with IndexQuery") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_nested_test"

    val testData = Seq(
      ("doc1", "apache spark machine learning", "tech", 100),
      ("doc2", "spark deep learning", "ai", 200),
      ("doc3", "hadoop mapreduce", "tech", 150),
      ("doc4", "spark nlp processing", "ai", 300),
      ("doc5", "tensorflow basics", "ai", 50)
    ).toDF("id", "content", "category", "score")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("articles_nested")

    // Nested CTEs: first filters by indexquery, second references first
    val sql = """
      WITH spark_docs AS (
        SELECT * FROM articles_nested
        WHERE content indexquery 'spark'
      ),
      ai_spark_docs AS (
        SELECT * FROM spark_docs
        WHERE category = 'ai'
      )
      SELECT id, content, score
      FROM ai_spark_docs
      ORDER BY score DESC
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"Nested CTE results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}', score=${r.getInt(2)}"))

    // Results should have spark in content (from first CTE)
    rows.foreach { row =>
      assert(row.getString(1).toLowerCase.contains("spark"), "Content should contain 'spark'")
    }
  }

  test("CTE with IndexQuery and JOIN") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_join_test"

    // Create articles table
    val articles = Seq(
      ("doc1", "apache spark tutorials", "auth1"),
      ("doc2", "spark machine learning", "auth2"),
      ("doc3", "hadoop basics", "auth1"),
      ("doc4", "spark streaming", "auth3")
    ).toDF("id", "content", "author_id")

    articles.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    // Create authors in-memory
    val authors = Seq(
      ("auth1", "John Smith"),
      ("auth2", "Jane Doe"),
      ("auth3", "Bob Wilson")
    ).toDF("author_id", "author_name")
    authors.createOrReplaceTempView("authors")

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("articles_join")

    // CTE with indexquery, then JOIN in outer query
    val sql = """
      WITH spark_articles AS (
        SELECT * FROM articles_join
        WHERE content indexquery 'spark'
      )
      SELECT s.id, s.content, a.author_name
      FROM spark_articles s
      JOIN authors a ON s.author_id = a.author_id
      ORDER BY s.id
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE with JOIN results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}', author='${r.getString(2)}'"))

    // Should have joined results for spark articles
    assert(rows.length >= 1, "Should have at least one joined result")
    rows.foreach { row =>
      assert(row.getString(1).toLowerCase.contains("spark"), "Content should contain 'spark'")
      assert(row.getString(2) != null, "Author name should be present")
    }
  }

  test("CTE with IndexQuery phrase search") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/cte_phrase_test"

    val testData = Seq(
      ("doc1", "apache spark machine learning tutorial", "tech"),
      ("doc2", "machine learning with tensorflow", "ai"),
      ("doc3", "deep learning and machine learning", "ai"),
      ("doc4", "spark sql machine learning pipeline", "tech")
    ).toDF("id", "content", "category")

    testData.write
      .format(format)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read.format(format).load(testPath)
    df.createOrReplaceTempView("documents_phrase")

    // CTE with phrase search in indexquery
    val sql = """
      WITH ml_docs AS (
        SELECT * FROM documents_phrase
        WHERE content indexquery '"machine learning"'
      )
      SELECT id, content, category
      FROM ml_docs
      ORDER BY id
    """

    val result = spark.sql(sql)
    val rows = result.collect()

    println(s"CTE phrase search results: ${rows.length}")
    rows.foreach(r => println(s"  id=${r.getString(0)}, content='${r.getString(1)}'"))

    // Should find documents with "machine learning" phrase
    assert(rows.length >= 1, "Should find documents with 'machine learning' phrase")
    rows.foreach { row =>
      assert(row.getString(1).toLowerCase.contains("machine learning"),
        "Content should contain 'machine learning'")
    }
  }
}
