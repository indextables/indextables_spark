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

package io.indextables.spark.filters

import org.scalatest.funsuite.AnyFunSuite

import io.indextables.spark.TestBase

/**
 * Tests for IndexQuery boolean combinations (OR, AND, NOT).
 *
 * These tests verify that the fix for the bug where SQL OR combining multiple
 * indexquery expressions returned no results is working correctly.
 *
 * The bug was:
 *   - Working: `url indexquery 'community OR curl'` (Tantivy-native OR)
 *   - Broken: `(url indexquery 'community') OR (url indexquery 'curl')` (SQL OR)
 *
 * The fix introduces MixedBooleanFilter types that preserve the boolean structure
 * instead of flattening IndexQuery expressions with AND semantics.
 */
class IndexQueryBooleanTest extends AnyFunSuite with TestBase {

  test("MixedBooleanFilter types should have correct references") {
    // Test MixedIndexQuery references
    val indexQueryFilter = IndexQueryFilter("url", "test query")
    val mixedIndexQuery = MixedIndexQuery(indexQueryFilter)
    assert(mixedIndexQuery.references().toSet == Set("url"))

    // Test MixedIndexQueryAll references (should be empty)
    val indexQueryAllFilter = IndexQueryAllFilter("test query")
    val mixedIndexQueryAll = MixedIndexQueryAll(indexQueryAllFilter)
    assert(mixedIndexQueryAll.references().isEmpty)

    // Test MixedSparkFilter references
    import org.apache.spark.sql.sources.EqualTo
    val sparkFilter = EqualTo("status", "active")
    val mixedSparkFilter = MixedSparkFilter(sparkFilter)
    assert(mixedSparkFilter.references().toSet == Set("status"))

    // Test MixedOrFilter references (combined from both sides)
    val orFilter = MixedOrFilter(mixedIndexQuery, mixedSparkFilter)
    assert(orFilter.references().toSet == Set("url", "status"))

    // Test MixedAndFilter references (combined from both sides)
    val andFilter = MixedAndFilter(mixedIndexQuery, mixedSparkFilter)
    assert(andFilter.references().toSet == Set("url", "status"))

    // Test MixedNotFilter references (from child)
    val notFilter = MixedNotFilter(mixedIndexQuery)
    assert(notFilter.references().toSet == Set("url"))
  }

  test("MixedSparkFilter.extractReferences should handle all Spark filter types") {
    import org.apache.spark.sql.sources._

    // Simple attribute filters
    assert(MixedSparkFilter.extractReferences(EqualTo("col1", "value")).toSet == Set("col1"))
    assert(MixedSparkFilter.extractReferences(EqualNullSafe("col2", "value")).toSet == Set("col2"))
    assert(MixedSparkFilter.extractReferences(GreaterThan("col3", 10)).toSet == Set("col3"))
    assert(MixedSparkFilter.extractReferences(GreaterThanOrEqual("col4", 10)).toSet == Set("col4"))
    assert(MixedSparkFilter.extractReferences(LessThan("col5", 10)).toSet == Set("col5"))
    assert(MixedSparkFilter.extractReferences(LessThanOrEqual("col6", 10)).toSet == Set("col6"))
    assert(MixedSparkFilter.extractReferences(In("col7", Array("a", "b"))).toSet == Set("col7"))
    assert(MixedSparkFilter.extractReferences(IsNull("col8")).toSet == Set("col8"))
    assert(MixedSparkFilter.extractReferences(IsNotNull("col9")).toSet == Set("col9"))
    assert(MixedSparkFilter.extractReferences(StringStartsWith("col10", "prefix")).toSet == Set("col10"))
    assert(MixedSparkFilter.extractReferences(StringEndsWith("col11", "suffix")).toSet == Set("col11"))
    assert(MixedSparkFilter.extractReferences(StringContains("col12", "sub")).toSet == Set("col12"))

    // Compound filters
    val andFilter = And(EqualTo("col1", "a"), EqualTo("col2", "b"))
    assert(MixedSparkFilter.extractReferences(andFilter).toSet == Set("col1", "col2"))

    val orFilter = Or(EqualTo("col1", "a"), EqualTo("col2", "b"))
    assert(MixedSparkFilter.extractReferences(orFilter).toSet == Set("col1", "col2"))

    val notFilter = Not(EqualTo("col1", "a"))
    assert(MixedSparkFilter.extractReferences(notFilter).toSet == Set("col1"))

    // Nested compound filters
    val nestedFilter = And(
      Or(EqualTo("a", 1), EqualTo("b", 2)),
      Not(EqualTo("c", 3))
    )
    assert(MixedSparkFilter.extractReferences(nestedFilter).toSet == Set("a", "b", "c"))
  }

  test("OR combining multiple indexquery expressions should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with urls containing different terms
      val testData = Seq(
        (1, "user1", "https://community.example.com/page"),
        (2, "user2", "https://curl.example.com/download"),
        (3, "user3", "https://other.example.com/page"),
        (4, "user1", "https://community.example.com/forum")
      ).toDF("id", "uid", "url")

      // Write test data - url as text type for IndexQuery support
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.url", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_urls")

      // Test 1: Native Tantivy OR (should work)
      val nativeOrResult = spark.sql("""
        SELECT id, url FROM test_urls
        WHERE url indexquery 'community OR curl'
        ORDER BY id
      """).collect()

      println(s"Native OR result count: ${nativeOrResult.length}")
      nativeOrResult.foreach(r => println(s"  id=${r.getInt(0)}, url='${r.getString(1)}'"))

      // Test 2: SQL OR combining multiple IndexQuery expressions (was broken, now fixed)
      val sqlOrResult = spark.sql("""
        SELECT id, url FROM test_urls
        WHERE (url indexquery 'community') OR (url indexquery 'curl')
        ORDER BY id
      """).collect()

      println(s"SQL OR result count: ${sqlOrResult.length}")
      sqlOrResult.foreach(r => println(s"  id=${r.getInt(0)}, url='${r.getString(1)}'"))

      // Both should return the same number of results (documents with community OR curl)
      assert(nativeOrResult.length > 0, "Native OR should return results")
      assert(sqlOrResult.length > 0, "SQL OR should return results (was broken before fix)")
      assert(
        sqlOrResult.length == nativeOrResult.length,
        s"SQL OR should match native OR: SQL=${sqlOrResult.length}, native=${nativeOrResult.length}"
      )

      // Verify correct documents are returned
      val sqlIds = sqlOrResult.map(_.getInt(0)).toSet
      assert(sqlIds.contains(1), "Should include id=1 (community)")
      assert(sqlIds.contains(2), "Should include id=2 (curl)")
      assert(sqlIds.contains(4), "Should include id=4 (community)")
      assert(!sqlIds.contains(3), "Should NOT include id=3 (neither community nor curl)")
    }
  }

  test("mixed OR with regular predicates and indexquery") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with urgency levels and urls
      val testData = Seq(
        (1, "user1", "CRITICAL", "https://one.example.com/page"),
        (2, "user1", "CLASS2", "https://two.example.com/download"),
        (3, "user1", "CLASS1", "https://two.example.com/page"),
        (4, "user1", "CLASS2", "https://one.example.com/forum"),
        (5, "user2", "CRITICAL", "https://three.example.com/page")
      ).toDF("id", "uid", "urgency", "url")

      // Write test data
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.url", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_mixed")

      // Complex mixed predicate:
      // (urgency = 'CRITICAL' AND url indexquery 'one') OR (urgency <> 'CLASS2' AND url indexquery 'two')
      val result = spark.sql("""
        SELECT id, urgency, url FROM test_mixed
        WHERE uid = 'user1'
          AND ((urgency = 'CRITICAL' AND url indexquery 'one')
               OR (urgency <> 'CLASS2' AND url indexquery 'two'))
        ORDER BY id
      """).collect()

      println(s"Mixed predicate result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, urgency='${r.getString(1)}', url='${r.getString(2)}'"))

      // Expected:
      // - id=1: CRITICAL AND 'one' matches -> YES
      // - id=2: CLASS2 AND 'two' -> NO (urgency IS CLASS2)
      // - id=3: CLASS1 (not CLASS2) AND 'two' -> YES
      // - id=4: CLASS2 AND 'one' -> NO (urgency IS CLASS2, doesn't match either branch)
      // - id=5: user2, filtered out

      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1 (CRITICAL AND one)")
      assert(resultIds.contains(3), "Should include id=3 (CLASS1 AND two)")
    }
  }

  test("nested OR/AND combinations work correctly") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "term1", "active", 10),
        (2, "term2", "inactive", 5),
        (3, "term1", "inactive", 8),
        (4, "term2", "active", 3),
        (5, "other", "active", 15)
      ).toDF("id", "content", "status", "priority")

      // Write test data
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_nested")

      // Test: (A OR B) AND (C OR D)
      // (content indexquery 'term1' OR content indexquery 'term2') AND (status = 'active' OR priority > 5)
      val result = spark.sql("""
        SELECT id, content, status, priority FROM test_nested
        WHERE ((content indexquery 'term1') OR (content indexquery 'term2'))
          AND (status = 'active' OR priority > 5)
        ORDER BY id
      """).collect()

      println(s"Nested OR/AND result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}', priority=${r.getInt(3)}"))

      // Expected:
      // - id=1: term1, active, 10 -> (term1 OR term2) AND (active OR 10>5) -> YES
      // - id=2: term2, inactive, 5 -> (term1 OR term2) AND (inactive OR 5>5=false) -> NO
      // - id=3: term1, inactive, 8 -> (term1 OR term2) AND (inactive OR 8>5) -> YES
      // - id=4: term2, active, 3 -> (term1 OR term2) AND (active OR 3>5=false) -> YES
      // - id=5: other, active, 15 -> (other=false) -> NO

      assert(result.length == 3, s"Should return 3 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1")
      assert(resultIds.contains(3), "Should include id=3")
      assert(resultIds.contains(4), "Should include id=4")
    }
  }

  test("NOT with indexquery") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "excluded term here", "active"),
        (2, "good content", "active"),
        (3, "excluded term also", "inactive"),
        (4, "another good one", "active")
      ).toDF("id", "content", "status")

      // Write test data
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_not")

      // Test: NOT indexquery combined with other predicate
      val result = spark.sql("""
        SELECT id, content, status FROM test_not
        WHERE NOT (content indexquery 'excluded') AND status = 'active'
        ORDER BY id
      """).collect()

      println(s"NOT indexquery result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}'"))

      // Expected:
      // - id=1: excluded, active -> NO (has excluded)
      // - id=2: good, active -> YES
      // - id=3: excluded, inactive -> NO (inactive)
      // - id=4: good, active -> YES

      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(2), "Should include id=2")
      assert(resultIds.contains(4), "Should include id=4")
      assert(!resultIds.contains(1), "Should NOT include id=1 (has excluded)")
      assert(!resultIds.contains(3), "Should NOT include id=3 (inactive)")
    }
  }

  test("additional OR indexquery test using SQL") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "community forum post"),
        (2, "curl command example"),
        (3, "unrelated content"),
        (4, "community discussion")
      ).toDF("id", "url")

      // Write test data
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.url", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_sql_or")

      // Test using SQL with OR operator
      val result = spark.sql("""
        SELECT id, url FROM test_sql_or
        WHERE (url indexquery 'community') OR (url indexquery 'curl')
        ORDER BY id
      """).collect()

      println(s"SQL OR result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, url='${r.getString(1)}'"))

      assert(result.length == 3, s"Should return 3 rows (community x2 + curl x1), got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1 (community)")
      assert(resultIds.contains(2), "Should include id=2 (curl)")
      assert(resultIds.contains(4), "Should include id=4 (community)")
      assert(!resultIds.contains(3), "Should NOT include id=3")
    }
  }

  test("IndexQueryAll should search across all text fields") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with multiple text fields
      val testData = Seq(
        ("doc1", "apache spark", "distributed"),
        ("doc2", "machine learning", "spark"),
        ("doc3", "data engineering", "pipelines"),
        ("doc4", "streaming", "spark applications")
      ).toDF("id", "content", "tags")

      // Write test data with both content and tags as text fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.tags", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_indexall")

      // Test IndexQueryAll - should search across all text fields (content AND tags)
      val result = spark.sql("""
        SELECT id, content, tags FROM test_indexall
        WHERE _indexall indexquery 'spark'
        ORDER BY id
      """).collect()

      println(s"IndexQueryAll result count: ${result.length}")
      result.foreach(r => println(s"  id='${r.getString(0)}', content='${r.getString(1)}', tags='${r.getString(2)}'"))

      // Should find 3 documents containing "spark" in any text field:
      // - doc1: content="apache spark" -> matches
      // - doc2: tags="spark" -> matches
      // - doc4: tags="spark applications" -> matches
      assert(result.length == 3, s"Expected 3 documents with 'spark' in any field, got ${result.length}")
      val resultIds = result.map(_.getString(0)).toSet
      assert(resultIds.contains("doc1"), "Should include doc1 (spark in content)")
      assert(resultIds.contains("doc2"), "Should include doc2 (spark in tags)")
      assert(resultIds.contains("doc4"), "Should include doc4 (spark in tags)")
      assert(!resultIds.contains("doc3"), "Should NOT include doc3 (no spark)")
    }
  }

  test("IndexQuery on non-existent field should throw error") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "test content", "active")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_nonexistent")

      // Test: IndexQuery with a field that doesn't exist should throw an error
      val exception = intercept[Exception] {
        spark.sql("""
          SELECT id, content FROM test_nonexistent
          WHERE fake_field indexquery 'something'
        """).collect()
      }

      val errorMessage = exception.getMessage + Option(exception.getCause).map(_.getMessage).getOrElse("")
      assert(
        errorMessage.contains("fake_field") || errorMessage.contains("non-existent"),
        s"Error message should mention the non-existent field. Actual message: $errorMessage"
      )
    }
  }
}
