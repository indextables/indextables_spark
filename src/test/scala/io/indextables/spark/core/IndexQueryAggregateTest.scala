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

package io.indextables.spark.core

import io.indextables.spark.TestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

/**
 * Integration tests for IndexQuery expressions with aggregate operations.
 * Validates that IndexQuery filters are properly passed to aggregate scans
 * (both SimpleAggregateScan and GroupByAggregateScan).
 */
class IndexQueryAggregateTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
      true
    } catch {
      case _: Exception => false
    }

  // Helper to create test data with text fields for IndexQuery
  private def createTestData() = {
    val data = Seq(
      ("doc1", "apache spark is awesome", "technology", 100),
      ("doc2", "machine learning with spark", "technology", 200),
      ("doc3", "data engineering pipelines", "technology", 150),
      ("doc4", "spark streaming applications", "technology", 300),
      ("doc5", "business intelligence reports", "business", 250)
    )
    spark.createDataFrame(data).toDF("id", "content", "category", "score")
  }

  test("SimpleAggregateScan should work with IndexQuery filters - COUNT") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      // Write with text field for IndexQuery
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test COUNT with IndexQuery filter - should find 3 docs containing "spark"
      val result = spark.sql("SELECT COUNT(*) as count FROM test_docs WHERE content indexquery 'spark'").collect()

      val count = result(0).getLong(0)
      assert(count == 3, s"Expected 3 documents with 'spark', got $count")

      println(s"✅ SimpleAggregateScan with IndexQuery COUNT test passed: count=$count")
    }
  }

  test("SimpleAggregateScan should work with IndexQuery filters - SUM") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test SUM with IndexQuery filter - docs with "spark" have scores: 100, 200, 300
      val result = spark.sql("SELECT SUM(score) as total FROM test_docs WHERE content indexquery 'spark'").collect()

      val total = result(0).getLong(0)
      assert(total == 600, s"Expected sum=600 (100+200+300), got $total")

      println(s"✅ SimpleAggregateScan with IndexQuery SUM test passed: total=$total")
    }
  }

  test("SimpleAggregateScan should work with IndexQuery filters - Multiple aggregations") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test multiple aggregations with IndexQuery
      val result = spark.sql("""
        SELECT COUNT(*) as count, SUM(score) as total, AVG(score) as avg, MIN(score) as min, MAX(score) as max
        FROM test_docs
        WHERE content indexquery 'spark'
      """).collect()

      val row = result(0)
      val count = row.getLong(0)
      val total = row.getLong(1)
      val avg = row.getDouble(2)
      val min = row.getInt(3)
      val max = row.getInt(4)

      assert(count == 3, s"Expected count=3, got $count")
      assert(total == 600, s"Expected total=600, got $total")
      assert(avg == 200.0, s"Expected avg=200.0, got $avg")
      assert(min == 100, s"Expected min=100, got $min")
      assert(max == 300, s"Expected max=300, got $max")

      println(s"✅ SimpleAggregateScan with IndexQuery multiple aggregations test passed: count=$count, total=$total, avg=$avg, min=$min, max=$max")
    }
  }

  test("SimpleAggregateScan should work with IndexQueryAll filters") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test with IndexQueryAll - search across all fields
      val result = spark.sql("SELECT COUNT(*) as count FROM test_docs WHERE _indexall indexquery 'business'").collect()

      val count = result(0).getLong(0)
      assert(count == 1, s"Expected 1 document with 'business', got $count")

      println(s"✅ SimpleAggregateScan with IndexQueryAll test passed: count=$count")
    }
  }

  test("GroupByAggregateScan should work with IndexQuery filters") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score,category")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test GROUP BY with IndexQuery filter
      // Only docs with "spark" (all in "technology" category): 100, 200, 300
      val result = spark.sql("""
        SELECT category, COUNT(*) as count, SUM(score) as total
        FROM test_docs
        WHERE content indexquery 'spark'
        GROUP BY category
      """).collect()

      assert(result.length == 1, s"Expected 1 group (technology), got ${result.length}")

      val row = result(0)
      val category = row.getString(0)
      val count = row.getLong(1)
      val total = row.getLong(2)

      assert(category == "technology", s"Expected category='technology', got '$category'")
      assert(count == 3, s"Expected count=3, got $count")
      assert(total == 600, s"Expected total=600, got $total")

      println(s"✅ GroupByAggregateScan with IndexQuery test passed: category=$category, count=$count, total=$total")
    }
  }

  test("GroupByAggregateScan should work with IndexQuery and multiple groups") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      // Create data with multiple categories
      val data = Seq(
        ("doc1", "spark technology news", "tech", 100),
        ("doc2", "spark business insights", "business", 200),
        ("doc3", "spark data science", "tech", 150),
        ("doc4", "unrelated content", "other", 50)
      )
      val testData = spark.createDataFrame(data).toDF("id", "content", "category", "score")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score,category")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test GROUP BY with IndexQuery - should find docs with "spark" in 2 categories
      val result = spark.sql("""
        SELECT category, COUNT(*) as count, SUM(score) as total
        FROM test_docs
        WHERE content indexquery 'spark'
        GROUP BY category
        ORDER BY category
      """).collect()

      assert(result.length == 2, s"Expected 2 groups (business, tech), got ${result.length}")

      // Business group
      val businessRow = result(0)
      assert(businessRow.getString(0) == "business")
      assert(businessRow.getLong(1) == 1)
      assert(businessRow.getLong(2) == 200)

      // Tech group
      val techRow = result(1)
      assert(techRow.getString(0) == "tech")
      assert(techRow.getLong(1) == 2)
      assert(techRow.getLong(2) == 250)

      println(s"✅ GroupByAggregateScan with IndexQuery and multiple groups test passed")
    }
  }

  test("Aggregates with IndexQuery should skip non-matching splits") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      // Write with multiple partitions to create multiple splits
      testData.repartition(2).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test that data skipping works with IndexQuery in aggregates
      val result = spark.sql("SELECT COUNT(*) as count FROM test_docs WHERE content indexquery 'nonexistent'").collect()

      val count = result(0).getLong(0)
      assert(count == 0, s"Expected 0 documents with 'nonexistent', got $count")

      println(s"✅ Aggregate with IndexQuery data skipping test passed: count=$count")
    }
  }

  test("SimpleAggregateScan with IndexQuery + equality + range filters combined") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test combining IndexQuery + equality filter + range filter
      // Expected: docs with "spark" AND category="technology" AND score >= 150 AND score <= 300
      // Should match: doc2 (score=200), doc4 (score=300)
      val result = spark.sql("""
        SELECT COUNT(*) as count, SUM(score) as total, AVG(score) as avg
        FROM test_docs
        WHERE content indexquery 'spark'
          AND category = 'technology'
          AND score >= 150
          AND score <= 300
      """).collect()

      val row = result(0)
      val count = row.getLong(0)
      val total = row.getLong(1)
      val avg = row.getDouble(2)

      assert(count == 2, s"Expected count=2 (doc2, doc4), got $count")
      assert(total == 500, s"Expected total=500 (200+300), got $total")
      assert(avg == 250.0, s"Expected avg=250.0, got $avg")

      println(s"✅ SimpleAggregateScan with combined filters test passed: count=$count, total=$total, avg=$avg")
    }
  }

  test("GroupByAggregateScan with IndexQuery + equality + range filters combined") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      // Create data with varied categories and scores
      val data = Seq(
        ("doc1", "spark technology news", "tech", 100),
        ("doc2", "spark business insights", "business", 200),
        ("doc3", "spark data science", "tech", 150),
        ("doc4", "spark machine learning", "tech", 300),
        ("doc5", "unrelated content", "tech", 250),
        ("doc6", "spark enterprise solutions", "business", 180)
      )
      val testData = spark.createDataFrame(data).toDF("id", "content", "category", "score")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score,category")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test GROUP BY with IndexQuery + range filter
      // Expected: docs with "spark" AND score >= 150 AND score <= 300, grouped by category
      // tech: doc3 (150), doc4 (300) = 2 docs, sum=450
      // business: doc2 (200), doc6 (180) = 2 docs, sum=380
      val result = spark.sql("""
        SELECT category, COUNT(*) as count, SUM(score) as total
        FROM test_docs
        WHERE content indexquery 'spark'
          AND score >= 150
          AND score <= 300
        GROUP BY category
        ORDER BY category
      """).collect()

      assert(result.length == 2, s"Expected 2 groups (business, tech), got ${result.length}")

      // Business group
      val businessRow = result(0)
      assert(businessRow.getString(0) == "business", s"Expected 'business', got '${businessRow.getString(0)}'")
      assert(businessRow.getLong(1) == 2, s"Expected count=2 for business, got ${businessRow.getLong(1)}")
      assert(businessRow.getLong(2) == 380, s"Expected total=380 for business, got ${businessRow.getLong(2)}")

      // Tech group
      val techRow = result(1)
      assert(techRow.getString(0) == "tech", s"Expected 'tech', got '${techRow.getString(0)}'")
      assert(techRow.getLong(1) == 2, s"Expected count=2 for tech, got ${techRow.getLong(1)}")
      assert(techRow.getLong(2) == 450, s"Expected total=450 for tech, got ${techRow.getLong(2)}")

      println(s"✅ GroupByAggregateScan with combined filters test passed")
    }
  }

  test("Regular scan with IndexQuery + equality + range filters combined") {
    assume(isNativeLibraryAvailable(), "Native library not available")

    withTempPath { tempPath =>
      val testData = createTestData()

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      df.createOrReplaceTempView("test_docs")

      // Test regular query (not aggregation) with combined filters
      // Expected: docs with "spark" AND category="technology" AND score >= 150 AND score <= 300
      // Should match: doc2 (score=200, content="machine learning with spark"), doc4 (score=300, content="spark streaming applications")
      val result = spark.sql("""
        SELECT id, content, category, score
        FROM test_docs
        WHERE content indexquery 'spark'
          AND category = 'technology'
          AND score >= 150
          AND score <= 300
        ORDER BY score
      """).collect()

      assert(result.length == 2, s"Expected 2 documents, got ${result.length}")

      // First doc (score=200)
      val doc1 = result(0)
      assert(doc1.getString(0) == "doc2", s"Expected id='doc2', got '${doc1.getString(0)}'")
      assert(doc1.getInt(3) == 200, s"Expected score=200, got ${doc1.getInt(3)}")
      assert(doc1.getString(1).contains("spark"), s"Expected content to contain 'spark'")

      // Second doc (score=300)
      val doc2 = result(1)
      assert(doc2.getString(0) == "doc4", s"Expected id='doc4', got '${doc2.getString(0)}'")
      assert(doc2.getInt(3) == 300, s"Expected score=300, got ${doc2.getInt(3)}")
      assert(doc2.getString(1).contains("spark"), s"Expected content to contain 'spark'")

      println(s"✅ Regular scan with combined filters test passed: ${result.length} documents returned")
    }
  }
}
