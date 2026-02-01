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

import io.indextables.spark.exceptions.IndexQueryParseException
import io.indextables.spark.TestBase

/**
 * Test cases for IndexQuery behavior with invalid query syntax in aggregate operations.
 *
 * These tests verify that:
 *   1. Invalid query syntax throws IndexQueryParseException on the DRIVER (pre-scan validation) 2. Error messages are
 *      user-friendly 3. No Spark tasks are created/failed (validation happens before task scheduling) 4. Both
 *      SimpleAggregateScan and GroupByAggregateScan paths are covered
 */
class IndexQueryAggregateInvalidSyntaxTest extends TestBase {

  /** Helper to get full error message chain from an exception */
  private def getFullErrorMessage(e: Throwable): String = {
    val messages           = scala.collection.mutable.ArrayBuffer[String]()
    var current: Throwable = e
    while (current != null) {
      if (current.getMessage != null) {
        messages += current.getMessage
      }
      current = current.getCause
    }
    messages.mkString(" -> ")
  }

  /** Check if exception chain contains IndexQueryParseException. */
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

  /** Verify that an exception was thrown during driver-side validation, NOT during task execution. */
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
      s"Exception chain should contain IndexQueryParseException. " +
        s"Got: ${e.getClass.getName}: $errorMessage"
    )
  }

  test("DEBUG: verify IndexQuery filter extraction for COUNT aggregation") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/debug_count_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "deep learning networks", 200.0)
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

    df.createOrReplaceTempView("debug_count_test")

    // First test with a VALID query to see if it works
    println("=== TEST 1: Valid IndexQuery with COUNT ===")
    val validResult = spark
      .sql("""
      SELECT COUNT(*) as cnt FROM debug_count_test
      WHERE content indexquery 'learning'
    """)
      .collect()
    println(s"Valid query result: count = ${validResult(0).getLong(0)}")
    assert(validResult(0).getLong(0) == 2, "Valid query should return 2 documents")

    // Now test with an INVALID query
    println("\n=== TEST 2: Invalid IndexQuery with COUNT ===")
    try {
      val invalidResult = spark
        .sql("""
        SELECT COUNT(*) as cnt FROM debug_count_test
        WHERE content indexquery '((('
      """)
        .collect()
      println(s"Invalid query result: count = ${invalidResult(0).getLong(0)}")
      println("WARNING: No exception was thrown for invalid syntax!")
      fail(s"Expected exception for invalid syntax, but got count = ${invalidResult(0).getLong(0)}")
    } catch {
      case e: Exception =>
        val errorMessage = getFullErrorMessage(e)
        println(s"Exception caught: $errorMessage")
        assert(
          errorMessage.contains("IndexQuery") ||
            errorMessage.contains("syntax") ||
            errorMessage.contains("parse") ||
            errorMessage.contains("Parse error"),
          s"Error message should describe the syntax problem. Got: $errorMessage"
        )
    }
  }

  test("invalid query in simple COUNT aggregation should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/simple_count_invalid_test"

    val testData = Seq(
      (1, "test content one", 100.0),
      (2, "test content two", 200.0)
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

    df.createOrReplaceTempView("simple_count_invalid_test")

    // Invalid syntax in simple COUNT aggregation should throw on driver
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT COUNT(*) FROM simple_count_invalid_test
        WHERE content indexquery '((('
      """)
        .collect()
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

  test("invalid query in SUM aggregation should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/sum_invalid_test"

    val testData = Seq(
      (1, "test content one", 100.0),
      (2, "test content two", 200.0)
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

    df.createOrReplaceTempView("sum_invalid_test")

    // Invalid syntax in SUM aggregation should throw on driver
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT SUM(score) FROM sum_invalid_test
        WHERE content indexquery '((('
      """)
        .collect()
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

  test("invalid query in GROUP BY aggregation should throw IndexQueryParseException on driver") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/groupby_invalid_test"

    val testData = Seq(
      (1, "test content one", "cat1", 100.0),
      (2, "test content two", "cat2", 200.0)
    ).toDF("id", "content", "category", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score,category")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("groupby_invalid_test")

    // Invalid syntax in GROUP BY aggregation should throw on driver
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT category, COUNT(*) FROM groupby_invalid_test
        WHERE content indexquery '((('
        GROUP BY category
      """)
        .collect()
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

  test("valid IndexQuery with aggregation should still work") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/valid_agg_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "deep learning networks", 200.0),
      (3, "data engineering", 150.0)
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

    df.createOrReplaceTempView("valid_agg_test")

    // Valid IndexQuery with COUNT
    val countResult = spark
      .sql("""
      SELECT COUNT(*) FROM valid_agg_test
      WHERE content indexquery 'learning'
    """)
      .collect()
    assert(countResult(0).getLong(0) == 2, s"Expected 2, got ${countResult(0).getLong(0)}")

    // Valid IndexQuery with SUM
    val sumResult = spark
      .sql("""
      SELECT SUM(score) FROM valid_agg_test
      WHERE content indexquery 'learning'
    """)
      .collect()
    assert(sumResult(0).getDouble(0) == 300.0, s"Expected 300.0, got ${sumResult(0).getDouble(0)}")
  }
}
