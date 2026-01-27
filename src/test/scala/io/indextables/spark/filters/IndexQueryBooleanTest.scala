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

  // ============================================================================
  // CRITICAL REGRESSION TESTS: Partition predicate preservation
  // These tests verify the fix for the bug where partition predicates were
  // incorrectly replaced with Literal(true), causing queries to scan ALL
  // partitions instead of pruning.
  // ============================================================================

  test("IndexQuery combined with partition predicate should preserve partition pruning") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create partitioned test data
      val testData = Seq(
        (1, "2024-01-01", "error message one"),
        (2, "2024-01-01", "warning message two"),
        (3, "2024-01-02", "error message three"),
        (4, "2024-01-02", "info message four"),
        (5, "2024-01-01", "debug message five")
      ).toDF("id", "load_date", "message")

      // Write as partitioned table
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date")
        .option("spark.indextables.indexing.typemap.message", "text")
        .mode("overwrite")
        .save(tempPath)

      // Read back
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("partitioned_data")

      // Query with partition predicate AND indexquery
      // CRITICAL: This must return ONLY rows from 2024-01-01 partition
      val result = spark.sql("""
        SELECT id, load_date, message FROM partitioned_data
        WHERE load_date = '2024-01-01' AND message indexquery 'error OR warning'
        ORDER BY id
      """).collect()

      println(s"Partition + IndexQuery result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, load_date='${r.getString(1)}', message='${r.getString(2)}'"))

      // Verify all results are from the target partition
      val resultDates = result.map(_.getString(1)).toSet
      assert(resultDates.size == 1, s"All results should be from one partition, got: $resultDates")
      assert(resultDates.head == "2024-01-01", s"Results should be from 2024-01-01, got: ${resultDates.head}")

      // Verify correct rows are returned
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1 (error on 2024-01-01)")
      assert(resultIds.contains(2), "Should include id=2 (warning on 2024-01-01)")
      assert(!resultIds.contains(3), "Should NOT include id=3 (wrong partition)")
      assert(!resultIds.contains(4), "Should NOT include id=4 (wrong partition and no match)")
      assert(!resultIds.contains(5), "Should NOT include id=5 (no match)")
    }
  }

  test("OR with IndexQuery should preserve partition predicates in parent AND") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create partitioned test data
      val testData = Seq(
        (1, "2024-01-01", "term_a content"),
        (2, "2024-01-01", "term_b content"),
        (3, "2024-01-02", "term_a content"),
        (4, "2024-01-01", "other content")
      ).toDF("id", "partition_col", "content")

      // Write as partitioned table
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("partition_col")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("partition_or_test")

      // Query: partition_col = 'x' AND (indexquery 'a' OR indexquery 'b')
      // This MUST preserve partition pruning
      val result = spark.sql("""
        SELECT id, partition_col, content FROM partition_or_test
        WHERE partition_col = '2024-01-01'
          AND ((content indexquery 'term_a') OR (content indexquery 'term_b'))
        ORDER BY id
      """).collect()

      println(s"Partition + OR IndexQuery result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, partition='${r.getString(1)}', content='${r.getString(2)}'"))

      // ALL results must be from the specified partition
      val resultPartitions = result.map(_.getString(1)).distinct
      assert(resultPartitions.length == 1 && resultPartitions.head == "2024-01-01",
        s"All results should be from 2024-01-01, got: ${resultPartitions.mkString(", ")}")

      // Should get id=1 and id=2 (matching terms in correct partition)
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1")
      assert(resultIds.contains(2), "Should include id=2")
      assert(!resultIds.contains(3), "Should NOT include id=3 (wrong partition)")
      assert(!resultIds.contains(4), "Should NOT include id=4 (no match)")
    }
  }

  test("complex mixed predicates with partition should preserve partition pruning") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create partitioned test data with multiple fields
      val testData = Seq(
        (1, "2024-01-01", "CRITICAL", "term_one content"),
        (2, "2024-01-01", "LOW", "term_two content"),
        (3, "2024-01-01", "CRITICAL", "term_two content"),
        (4, "2024-01-02", "CRITICAL", "term_one content"),
        (5, "2024-01-01", "MEDIUM", "term_one content")
      ).toDF("id", "date_partition", "priority", "content")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date_partition")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("complex_partition_test")

      // Complex query: partition filter AND ((spark_filter AND indexquery) OR (spark_filter AND indexquery))
      val result = spark.sql("""
        SELECT id, date_partition, priority, content FROM complex_partition_test
        WHERE date_partition = '2024-01-01'
          AND ((priority = 'CRITICAL' AND content indexquery 'term_one')
               OR (priority <> 'CRITICAL' AND content indexquery 'term_two'))
        ORDER BY id
      """).collect()

      println(s"Complex partition query result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, partition='${r.getString(1)}', priority='${r.getString(2)}', content='${r.getString(3)}'"))

      // All results MUST be from 2024-01-01
      val resultPartitions = result.map(_.getString(1)).distinct
      assert(resultPartitions.length == 1 && resultPartitions.head == "2024-01-01",
        s"All results should be from 2024-01-01, got: ${resultPartitions.mkString(", ")}")

      // Expected results:
      // - id=1: 2024-01-01, CRITICAL, term_one -> YES (first OR branch)
      // - id=2: 2024-01-01, LOW (not CRITICAL), term_two -> YES (second OR branch)
      // - id=3: 2024-01-01, CRITICAL, term_two -> NO (CRITICAL but term_two doesn't match first branch, CRITICAL doesn't match second)
      // - id=4: 2024-01-02 -> NO (wrong partition)
      // - id=5: 2024-01-01, MEDIUM (not CRITICAL), term_one -> NO (not CRITICAL AND term_one doesn't match second branch)

      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds.contains(1), "Should include id=1 (CRITICAL + term_one)")
      assert(resultIds.contains(2), "Should include id=2 (not CRITICAL + term_two)")
      assert(!resultIds.contains(3), "Should NOT include id=3 (CRITICAL but term_two)")
      assert(!resultIds.contains(4), "Should NOT include id=4 (wrong partition)")
      assert(!resultIds.contains(5), "Should NOT include id=5 (not CRITICAL but term_one)")
    }
  }

  test("MixedBooleanFilter.extractIndexQueryFilters should work correctly") {
    // Unit test for the extraction helper methods
    val indexQuery1 = MixedIndexQuery(IndexQueryFilter("field1", "query1"))
    val indexQuery2 = MixedIndexQuery(IndexQueryFilter("field2", "query2"))
    val indexQueryAll = MixedIndexQueryAll(IndexQueryAllFilter("query_all"))
    val sparkFilter = MixedSparkFilter(org.apache.spark.sql.sources.EqualTo("col", "val"))

    // Test extraction from nested structure
    val complexTree = MixedAndFilter(
      MixedOrFilter(indexQuery1, indexQuery2),
      MixedOrFilter(sparkFilter, MixedNotFilter(indexQueryAll))
    )

    val extractedIndexQueries = MixedBooleanFilter.extractIndexQueryFilters(complexTree)
    assert(extractedIndexQueries.length == 2, s"Should extract 2 IndexQueryFilters, got ${extractedIndexQueries.length}")
    assert(extractedIndexQueries.exists(_.columnName == "field1"), "Should include field1")
    assert(extractedIndexQueries.exists(_.columnName == "field2"), "Should include field2")

    val extractedIndexQueryAll = MixedBooleanFilter.extractIndexQueryAllFilters(complexTree)
    assert(extractedIndexQueryAll.length == 1, s"Should extract 1 IndexQueryAllFilter, got ${extractedIndexQueryAll.length}")
    assert(extractedIndexQueryAll.head.queryString == "query_all", "Should have correct query string")
  }

  test("multiple IndexQueries on same field with OR should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data - test that multiple OR'd IndexQueries on the same field work
      val testData = Seq(
        (1, "apache spark tutorial"),
        (2, "machine learning basics"),
        (3, "data engineering with spark"),
        (4, "python programming"),
        (5, "spark streaming guide")
      ).toDF("id", "title")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("multi_or_test")

      // Multiple OR'd IndexQueries on the same field
      val result = spark.sql("""
        SELECT id, title FROM multi_or_test
        WHERE (title indexquery 'apache') OR (title indexquery 'streaming') OR (title indexquery 'machine')
        ORDER BY id
      """).collect()

      println(s"Multiple OR IndexQuery result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, title='${r.getString(1)}'"))

      // Expected:
      // - id=1: apache -> YES
      // - id=2: machine -> YES
      // - id=5: streaming -> YES
      assert(result.length == 3, s"Should return 3 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 2, 5), s"Should include ids 1, 2, 5, got: $resultIds")
    }
  }

  // ============================================================================
  // CRITICAL: Complex NOT with mixed predicates tests
  // These verify that NOT wrapping mixed IndexQuery AND Spark predicates
  // doesn't produce incorrect results.
  // ============================================================================

  test("NOT with mixed IndexQuery AND Spark predicate should work correctly") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "term_match content", "12345"),  // matches: NOT(term_match AND id=12345) = NOT(true AND true) = false
        (2, "term_match content", "99999"),  // matches: NOT(term_match AND id=12345) = NOT(true AND false) = true
        (3, "other content", "12345"),       // matches: NOT(term_match AND id=12345) = NOT(false AND true) = true
        (4, "other content", "99999")        // matches: NOT(term_match AND id=12345) = NOT(false AND false) = true
      ).toDF("id", "content", "code")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("not_mixed_test")

      // Test: NOT (content indexquery 'term_match' AND code = '12345')
      // This should use De Morgan's law: NOT(A AND B) = NOT A OR NOT B
      // So documents matching (term_match AND code=12345) are EXCLUDED
      val result = spark.sql("""
        SELECT id, content, code FROM not_mixed_test
        WHERE NOT (content indexquery 'term_match' AND code = '12345')
        ORDER BY id
      """).collect()

      println(s"NOT mixed predicate result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', code='${r.getString(2)}'"))

      // Expected: id=2, 3, 4 (everything EXCEPT id=1 which matches both conditions)
      assert(result.length == 3, s"Should return 3 rows (excluding id=1), got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(!resultIds.contains(1), "Should NOT include id=1 (matches both conditions)")
      assert(resultIds.contains(2), "Should include id=2 (term_match but wrong code)")
      assert(resultIds.contains(3), "Should include id=3 (wrong content but right code)")
      assert(resultIds.contains(4), "Should include id=4 (both conditions fail)")
    }
  }

  test("NOT with complex OR of mixed predicates should work correctly") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "term_a content", "field2_b value", "12345", "99"),  // matches (A AND id) -> excluded
        (2, "term_a content", "other", "12345", "99"),           // matches (A AND id) -> excluded
        (3, "other", "field2_b value", "12345", "99"),           // matches (B AND code) -> excluded
        (4, "other", "field2_b value", "other", "99"),           // matches (B AND code) -> excluded
        (5, "term_a content", "field2_b value", "other", "other"), // matches A but not (A AND id), matches B but not (B AND code) -> included
        (6, "other", "other", "12345", "99")                     // no match -> included
      ).toDF("id", "content1", "content2", "idcol", "codeval")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content1", "text")
        .option("spark.indextables.indexing.typemap.content2", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("not_complex_or_test")

      // Test: NOT ((content1 indexquery 'term_a' AND idcol = '12345') OR (content2 indexquery 'field2_b' AND codeval = '99'))
      // This excludes documents where EITHER:
      // - content1 has 'term_a' AND idcol is '12345'
      // - content2 has 'field2_b' AND codeval is '99'
      val result = spark.sql("""
        SELECT id FROM not_complex_or_test
        WHERE NOT (
          (content1 indexquery 'term_a' AND idcol = '12345')
          OR
          (content2 indexquery 'field2_b' AND codeval = '99')
        )
        ORDER BY id
      """).collect()

      println(s"NOT complex OR result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}"))

      // Expected:
      // - id=1: (term_a AND 12345)=true OR (field2_b AND 99)=true -> NOT(true OR true) = false -> EXCLUDED
      // - id=2: (term_a AND 12345)=true OR (other AND 99)=false -> NOT(true OR false) = false -> EXCLUDED
      // - id=3: (other AND 12345)=false OR (field2_b AND 99)=true -> NOT(false OR true) = false -> EXCLUDED
      // - id=4: (other AND other)=false OR (field2_b AND 99)=true -> NOT(false OR true) = false -> EXCLUDED
      // - id=5: (term_a AND other)=false OR (field2_b AND other)=false -> NOT(false OR false) = true -> INCLUDED
      // - id=6: (other AND 12345)=false OR (other AND 99)=false -> NOT(false OR false) = true -> INCLUDED

      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(5, 6), s"Should include only ids 5, 6, got: $resultIds")
    }
  }

  test("double NOT should cancel out correctly") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "target content", "active"),
        (2, "other content", "active"),
        (3, "target content", "inactive")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("double_not_test")

      // Test: NOT NOT (content indexquery 'target') AND status = 'active'
      // Double NOT should cancel, equivalent to: content indexquery 'target' AND status = 'active'
      val result = spark.sql("""
        SELECT id, content, status FROM double_not_test
        WHERE NOT (NOT (content indexquery 'target')) AND status = 'active'
        ORDER BY id
      """).collect()

      println(s"Double NOT result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}'"))

      // Expected: id=1 (target AND active)
      assert(result.length == 1, s"Should return 1 row, got ${result.length}")
      assert(result(0).getInt(0) == 1, "Should include id=1")
    }
  }

  // ============================================================================
  // Additional coverage: Multiple fields, IndexQueryAll combinations, range predicates
  // ============================================================================

  test("IndexQuery on multiple DIFFERENT fields with AND should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with multiple text fields
      val testData = Seq(
        (1, "alpha content", "beta tags"),      // both match -> YES
        (2, "alpha content", "gamma tags"),     // only field1 matches -> NO
        (3, "delta content", "beta tags"),      // only field2 matches -> NO
        (4, "delta content", "gamma tags"),     // neither matches -> NO
        (5, "alpha stuff", "beta description")  // both match -> YES
      ).toDF("id", "field1", "field2")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.field1", "text")
        .option("spark.indextables.indexing.typemap.field2", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("multi_field_and_test")

      // Test: IndexQuery on different fields with AND
      val result = spark.sql("""
        SELECT id, field1, field2 FROM multi_field_and_test
        WHERE (field1 indexquery 'alpha') AND (field2 indexquery 'beta')
        ORDER BY id
      """).collect()

      println(s"Multi-field AND result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, field1='${r.getString(1)}', field2='${r.getString(2)}'"))

      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 5), s"Should include ids 1, 5, got: $resultIds")
    }
  }

  test("IndexQuery on multiple DIFFERENT fields with OR should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with multiple text fields
      val testData = Seq(
        (1, "alpha content", "other tags"),     // field1 matches -> YES
        (2, "other content", "beta tags"),      // field2 matches -> YES
        (3, "alpha content", "beta tags"),      // both match -> YES
        (4, "other content", "other tags")      // neither matches -> NO
      ).toDF("id", "field1", "field2")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.field1", "text")
        .option("spark.indextables.indexing.typemap.field2", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("multi_field_or_test")

      // Test: IndexQuery on different fields with OR
      val result = spark.sql("""
        SELECT id, field1, field2 FROM multi_field_or_test
        WHERE (field1 indexquery 'alpha') OR (field2 indexquery 'beta')
        ORDER BY id
      """).collect()

      println(s"Multi-field OR result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, field1='${r.getString(1)}', field2='${r.getString(2)}'"))

      assert(result.length == 3, s"Should return 3 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 2, 3), s"Should include ids 1, 2, 3, got: $resultIds")
    }
  }

  test("IndexQueryAll combined with regular filter should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        ("doc1", "spark tutorial", "distributed computing", "active"),
        ("doc2", "machine learning", "spark", "active"),
        ("doc3", "data pipelines", "spark streaming", "inactive"),
        ("doc4", "web development", "javascript", "active")
      ).toDF("id", "title", "tags", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.tags", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("indexall_combined_test")

      // Test: _indexall combined with status filter
      val result = spark.sql("""
        SELECT id, title, tags, status FROM indexall_combined_test
        WHERE _indexall indexquery 'spark' AND status = 'active'
        ORDER BY id
      """).collect()

      println(s"IndexQueryAll + filter result count: ${result.length}")
      result.foreach(r => println(s"  id='${r.getString(0)}', title='${r.getString(1)}', status='${r.getString(3)}'"))

      // Expected: doc1 and doc2 (both have 'spark' and are 'active')
      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getString(0)).toSet
      assert(resultIds == Set("doc1", "doc2"), s"Should include doc1 and doc2, got: $resultIds")
    }
  }

  test("IndexQueryAll OR specific field IndexQuery should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        ("doc1", "spark tutorial", "computing"),
        ("doc2", "machine learning", "ai"),
        ("doc3", "web dev", "javascript"),
        ("doc4", "data science", "python")
      ).toDF("id", "title", "category")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.category", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("indexall_or_specific_test")

      // Test: _indexall for 'spark' OR specific field for 'python'
      val result = spark.sql("""
        SELECT id, title, category FROM indexall_or_specific_test
        WHERE (_indexall indexquery 'spark') OR (category indexquery 'python')
        ORDER BY id
      """).collect()

      println(s"IndexQueryAll OR specific result count: ${result.length}")
      result.foreach(r => println(s"  id='${r.getString(0)}', title='${r.getString(1)}', category='${r.getString(2)}'"))

      // Expected: doc1 (spark) and doc4 (python)
      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getString(0)).toSet
      assert(resultIds == Set("doc1", "doc4"), s"Should include doc1 and doc4, got: $resultIds")
    }
  }

  test("range predicates with IndexQuery should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "target content", 10),
        (2, "target content", 5),
        (3, "other content", 10),
        (4, "target content", 3),
        (5, "target content", 8)
      ).toDF("id", "content", "priority")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("range_pred_test")

      // Test: IndexQuery with range predicate
      val result = spark.sql("""
        SELECT id, content, priority FROM range_pred_test
        WHERE content indexquery 'target' AND priority > 5
        ORDER BY id
      """).collect()

      println(s"Range predicate result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', priority=${r.getInt(2)}"))

      // Expected: id=1 (priority 10 > 5), id=5 (priority 8 > 5)
      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 5), s"Should include ids 1, 5, got: $resultIds")
    }
  }

  test("IN clause with IndexQuery should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "target content", "active"),
        (2, "target content", "pending"),
        (3, "target content", "archived"),
        (4, "other content", "active"),
        (5, "target content", "deleted")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("in_clause_test")

      // Test: IndexQuery with IN clause
      val result = spark.sql("""
        SELECT id, content, status FROM in_clause_test
        WHERE content indexquery 'target' AND status IN ('active', 'pending')
        ORDER BY id
      """).collect()

      println(s"IN clause result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}'"))

      // Expected: id=1 (active), id=2 (pending)
      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 2), s"Should include ids 1, 2, got: $resultIds")
    }
  }

  test("string equality with IndexQuery should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "target content", "active"),
        (2, "target content", "inactive"),
        (3, "other content", "active"),
        (4, "target content", "pending"),
        (5, "target content", "active")
      ).toDF("id", "content", "status")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("string_eq_test")

      // Test: IndexQuery with string equality
      val result = spark.sql("""
        SELECT id, content, status FROM string_eq_test
        WHERE content indexquery 'target' AND status = 'active'
        ORDER BY id
      """).collect()

      println(s"String equality result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}'"))

      // Expected: id=1 and id=5 (target content and active status)
      assert(result.length == 2, s"Should return 2 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 5), s"Should include ids 1, 5, got: $resultIds")
    }
  }

  test("deeply nested boolean expressions should work") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "alpha content", "cat_a", "active", 10),
        (2, "beta content", "cat_b", "active", 5),
        (3, "alpha content", "cat_a", "inactive", 10),
        (4, "gamma content", "cat_c", "active", 15),
        (5, "alpha content", "cat_b", "active", 8)
      ).toDF("id", "content", "category", "status", "score")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("deep_nested_test")

      // Test: ((A OR B) AND C) OR D
      // ((alpha OR beta) AND active) OR score > 12
      val result = spark.sql("""
        SELECT id, content, status, score FROM deep_nested_test
        WHERE (((content indexquery 'alpha') OR (content indexquery 'beta')) AND status = 'active')
              OR score > 12
        ORDER BY id
      """).collect()

      println(s"Deep nested result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, content='${r.getString(1)}', status='${r.getString(2)}', score=${r.getInt(3)}"))

      // Expected:
      // - id=1: alpha, active -> YES (first part)
      // - id=2: beta, active -> YES (first part)
      // - id=3: alpha, inactive -> NO (fails active check, score=10)
      // - id=4: gamma, active, score=15 -> YES (score > 12)
      // - id=5: alpha, active -> YES (first part)
      assert(result.length == 4, s"Should return 4 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 2, 4, 5), s"Should include ids 1, 2, 4, 5, got: $resultIds")
    }
  }

  test("partition predicate in OR with IndexQuery should work correctly") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create partitioned test data
      val testData = Seq(
        (1, "2024-01-01", "target content"),
        (2, "2024-01-02", "target content"),
        (3, "2024-01-01", "other content"),
        (4, "2024-01-03", "different content")
      ).toDF("id", "date_part", "content")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date_part")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("partition_or_indexquery_test")

      // Test: partition_col = 'x' OR (indexquery AND spark_pred)
      // Note: This is tricky - partition filter in OR means we need to scan multiple partitions
      val result = spark.sql("""
        SELECT id, date_part, content FROM partition_or_indexquery_test
        WHERE date_part = '2024-01-01' OR (content indexquery 'target' AND date_part = '2024-01-02')
        ORDER BY id
      """).collect()

      println(s"Partition OR IndexQuery result count: ${result.length}")
      result.foreach(r => println(s"  id=${r.getInt(0)}, date_part='${r.getString(1)}', content='${r.getString(2)}'"))

      // Expected:
      // - id=1: 2024-01-01 -> YES (first OR branch)
      // - id=2: target AND 2024-01-02 -> YES (second OR branch)
      // - id=3: 2024-01-01 -> YES (first OR branch)
      // - id=4: 2024-01-03 -> NO (neither branch)
      assert(result.length == 3, s"Should return 3 rows, got ${result.length}")
      val resultIds = result.map(_.getInt(0)).toSet
      assert(resultIds == Set(1, 2, 3), s"Should include ids 1, 2, 3, got: $resultIds")
    }
  }
}
