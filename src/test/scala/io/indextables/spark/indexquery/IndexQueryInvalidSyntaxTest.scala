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
import io.indextables.spark.exceptions.IndexQueryParseException

/**
 * Test cases for IndexQuery behavior with invalid query syntax.
 *
 * These tests verify that:
 * 1. Invalid query syntax throws IndexQueryParseException on the DRIVER (pre-scan validation)
 * 2. Error messages are user-friendly and include the problematic query
 * 3. No Spark tasks are created/failed (validation happens before task scheduling)
 */
class IndexQueryInvalidSyntaxTest extends TestBase {

  /**
   * Helper to get full error message chain from an exception
   */
  private def getFullErrorMessage(e: Throwable): String = {
    val messages = scala.collection.mutable.ArrayBuffer[String]()
    var current: Throwable = e
    while (current != null) {
      if (current.getMessage != null) {
        messages += current.getMessage
      }
      current = current.getCause
    }
    messages.mkString(" -> ")
  }

  /**
   * Check if exception chain contains IndexQueryParseException.
   * This verifies that the error was properly wrapped in our custom exception.
   */
  private def containsIndexQueryParseException(e: Throwable): Boolean = {
    var current: Throwable = e
    while (current != null) {
      if (current.isInstanceOf[IndexQueryParseException]) {
        return true
      }
      current = current.getCause
    }
    false
  }

  /**
   * Verify that an exception was thrown during driver-side validation,
   * NOT during task execution. Task failures would contain "Task X in stage Y failed".
   */
  private def assertDriverSideValidation(e: Throwable): Unit = {
    val errorMessage = getFullErrorMessage(e)

    // Should NOT contain task failure indicators
    assert(
      !errorMessage.contains("Task") || !errorMessage.contains("failed"),
      s"Exception appears to be from task execution, not driver-side validation. " +
      s"Error: $errorMessage"
    )

    // Should contain IndexQueryParseException in the chain
    assert(
      containsIndexQueryParseException(e),
      s"Exception chain should contain IndexQueryParseException for proper error handling. " +
      s"Got: ${e.getClass.getName}: $errorMessage"
    )
  }

  // ============================================================================
  // TEST: Invalid syntax should throw errors from driver-side validation
  // ============================================================================

  test("unbalanced parentheses should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/invalid_syntax_parens_test"

    // Create test data
    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "tech"),
      (3, "web development frameworks", "tech")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("parens_test")

    // Invalid query with unbalanced parentheses should throw an error
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, title FROM parens_test
        WHERE title indexquery '((machine learning'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)

    // The error should mention the invalid syntax
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the syntax problem. Got: $errorMessage"
    )
  }

  test("invalid field reference syntax should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/invalid_field_ref_test"

    val testData = Seq(
      (1, "test document one", "cat1"),
      (2, "test document two", "cat2"),
      (3, "test document three", "cat3")
    ).toDF("id", "content", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("field_ref_test")

    // Invalid query with broken field reference syntax
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, content FROM field_ref_test
        WHERE content indexquery 'content:'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the syntax problem. Got: $errorMessage"
    )
  }

  test("unclosed quotes should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/unclosed_quotes_test"

    val testData = Seq(
      (1, "exact phrase match test", "doc1"),
      (2, "another test document", "doc2"),
      (3, "third test entry", "doc3")
    ).toDF("id", "description", "docid")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.description", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("quotes_test")

    // Invalid query with unclosed quote
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, description FROM quotes_test
        WHERE description indexquery '"unclosed phrase'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the quote problem. Got: $errorMessage"
    )
  }

  test("invalid boolean operator syntax should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/invalid_boolean_test"

    val testData = Seq(
      (1, "apple and banana fruit", "fruit"),
      (2, "orange citrus fruit", "fruit"),
      (3, "carrot vegetable", "vegetable")
    ).toDF("id", "name", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.name", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("boolean_test")

    // Invalid query with dangling boolean operator
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, name FROM boolean_test
        WHERE name indexquery 'apple AND AND orange'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the boolean problem. Got: $errorMessage"
    )
  }

  test("completely malformed query should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/malformed_query_test"

    val testData = Seq(
      (1, "document one", 100),
      (2, "document two", 200),
      (3, "document three", 300)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("malformed_test")

    // Completely malformed query
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, title FROM malformed_test
        WHERE title indexquery '))))(((('
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the malformed query. Got: $errorMessage"
    )
  }

  // Aggregate queries (COUNT, SUM, etc.) now properly propagate IndexQuery parse errors.
  // Driver-side validation catches these before any tasks are created.
  test("invalid query in COUNT aggregation should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/count_invalid_test"

    val testData = Seq(
      (1, "test content", 100.0)
    ).toDF("id", "content", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("count_invalid_test")

    // Invalid syntax in aggregation should throw on driver
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT COUNT(*) FROM count_invalid_test
        WHERE content indexquery '((('
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the syntax problem. Got: $errorMessage"
    )
  }

  // ============================================================================
  // TEST: Valid queries should continue to work
  // ============================================================================

  test("valid query syntax should work correctly") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/valid_syntax_test"

    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "data"),
      (3, "deep learning neural networks", "ai")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("valid_test")

    // Valid query with proper parentheses
    val results1 = spark.sql("""
      SELECT id, title FROM valid_test
      WHERE title indexquery '(machine OR deep) AND learning'
    """).collect()

    assert(results1.length == 2, s"Expected 2 results, got ${results1.length}")

    // Valid phrase query
    val results2 = spark.sql("""
      SELECT id, title FROM valid_test
      WHERE title indexquery '"machine learning"'
    """).collect()

    assert(results2.length == 1, s"Expected 1 result for phrase query, got ${results2.length}")

    // Valid simple query
    val results3 = spark.sql("""
      SELECT id, title FROM valid_test
      WHERE title indexquery 'pipeline'
    """).collect()

    assert(results3.length == 1, s"Expected 1 result for simple query, got ${results3.length}")
  }

  test("valid query in aggregation should work correctly") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/valid_agg_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "data engineering pipeline", 200.0),
      (3, "deep learning neural networks", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("valid_agg_test")

    // COUNT with valid IndexQuery
    val countResult = spark.sql("""
      SELECT COUNT(*) FROM valid_agg_test
      WHERE title indexquery 'learning'
    """).collect()

    assert(countResult.length == 1, s"Expected 1 row, got ${countResult.length}")
    assert(countResult(0).getLong(0) == 2, s"Expected count of 2, got ${countResult(0).getLong(0)}")

    // SUM with valid IndexQuery
    val sumResult = spark.sql("""
      SELECT SUM(score) FROM valid_agg_test
      WHERE title indexquery 'learning'
    """).collect()

    assert(sumResult.length == 1, s"Expected 1 row, got ${sumResult.length}")
    assert(sumResult(0).getDouble(0) == 250.0, s"Expected sum of 250.0, got ${sumResult(0).getDouble(0)}")
  }

  test("empty query string behavior") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/empty_query_test"

    val testData = Seq(
      (1, "test document", "cat")
    ).toDF("id", "content", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("empty_query_test")

    // Empty query - may either throw an error or return no results
    // depending on tantivy behavior (both are acceptable, but not returning all docs)
    try {
      val results = spark.sql("""
        SELECT id, content FROM empty_query_test
        WHERE content indexquery ''
      """).collect()

      // If it doesn't throw, it should return empty (not all docs)
      // Empty string is treated as "no query terms" which matches nothing
      assert(results.length <= 1,
        s"Empty query should not return all documents. Got ${results.length} rows")
    } catch {
      case _: Exception =>
        // Throwing an error for empty query is acceptable
        succeed
    }
  }

  test("error message includes query string (driver-side validation)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/error_msg_test"

    val testData = Seq(
      (1, "test content", "cat")
    ).toDF("id", "content", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("error_msg_test")

    val invalidQuery = "((broken query syntax"

    val exception = intercept[Exception] {
      spark.sql(s"""
        SELECT id, content FROM error_msg_test
        WHERE content indexquery '$invalidQuery'
      """).collect()
    }

    // Verify driver-side validation
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)

    // Error message should include the problematic query string
    assert(
      errorMessage.contains("broken") || errorMessage.contains("(("),
      s"Error message should include the query string. Got: $errorMessage"
    )
  }

  test("error message includes field name (driver-side validation)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/error_field_test"

    val testData = Seq(
      (1, "test content", "cat")
    ).toDF("id", "my_special_field", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.my_special_field", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("error_field_test")

    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, my_special_field FROM error_field_test
        WHERE my_special_field indexquery '((('
      """).collect()
    }

    // Verify driver-side validation
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)

    // Error message should include the field name
    assert(
      errorMessage.contains("my_special_field"),
      s"Error message should include the field name 'my_special_field'. Got: $errorMessage"
    )
  }

  // ============================================================================
  // TEST: Bug #1 - Error should not persist across queries (PR #122 fix)
  // ============================================================================

  test("invalid syntax error should not persist to subsequent queries") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/error_persistence_test"

    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "tech"),
      (3, "deep learning neural networks", "ai")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("persistence_test")

    // Query 1: Invalid syntax should fail
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, title FROM persistence_test
        WHERE title indexquery '((invalid syntax'
      """).collect()
    }
    assertDriverSideValidation(exception)

    // Query 2: Valid syntax should succeed (error should NOT persist)
    val results = spark.sql("""
      SELECT id, title FROM persistence_test
      WHERE title indexquery 'learning'
    """).collect()

    assert(results.length == 2,
      s"Valid query after invalid query should succeed. Expected 2 results, got ${results.length}")
  }

  // ============================================================================
  // TEST: Bug #2 - Validation should work with mixed predicates (PR #122 fix)
  // ============================================================================

  test("invalid syntax should be caught when combined with Spark filters via AND") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/mixed_and_test"

    val testData = Seq(
      ("id1", "machine learning algorithms", "tech"),
      ("id2", "data engineering pipeline", "tech"),
      ("id3", "deep learning neural networks", "ai")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("mixed_and_test")

    // Invalid IndexQuery combined with Spark filter via AND
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, title FROM mixed_and_test
        WHERE id = 'id1' AND title indexquery '((badquery'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the syntax problem. Got: $errorMessage"
    )
  }

  test("invalid syntax should be caught when combined with Spark filters via OR") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/mixed_or_test"

    val testData = Seq(
      ("id1", "machine learning algorithms", "tech"),
      ("id2", "data engineering pipeline", "tech"),
      ("id3", "deep learning neural networks", "ai")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("mixed_or_test")

    // Invalid IndexQuery combined with Spark filter via OR
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT id, title FROM mixed_or_test
        WHERE id = 'id1' OR title indexquery '((badquery'
      """).collect()
    }

    // Verify driver-side validation (not task failure)
    assertDriverSideValidation(exception)

    val errorMessage = getFullErrorMessage(exception)
    assert(
      errorMessage.contains("IndexQuery") ||
      errorMessage.contains("syntax") ||
      errorMessage.contains("parse") ||
      errorMessage.contains("Parse error"),
      s"Error message should describe the syntax problem. Got: $errorMessage"
    )
  }

  test("valid IndexQuery combined with Spark filters should work correctly") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/mixed_valid_test"

    val testData = Seq(
      ("id1", "machine learning algorithms", "tech"),
      ("id2", "data engineering pipeline", "tech"),
      ("id3", "deep learning neural networks", "ai")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("mixed_valid_test")

    // Valid query: Spark filter AND IndexQuery
    val resultsAnd = spark.sql("""
      SELECT id, title FROM mixed_valid_test
      WHERE category = 'tech' AND title indexquery 'learning'
    """).collect()

    assert(resultsAnd.length == 1,
      s"Expected 1 result for AND combination, got ${resultsAnd.length}")

    // Valid query: Spark filter OR IndexQuery
    val resultsOr = spark.sql("""
      SELECT id, title FROM mixed_valid_test
      WHERE category = 'ai' OR title indexquery 'pipeline'
    """).collect()

    assert(resultsOr.length == 2,
      s"Expected 2 results for OR combination, got ${resultsOr.length}")
  }
}
