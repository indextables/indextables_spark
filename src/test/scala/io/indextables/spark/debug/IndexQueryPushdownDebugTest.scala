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

package io.indextables.spark.debug

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.Column
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.expressions.IndexQueryExpression
import io.indextables.spark.util.ExpressionUtils
import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class IndexQueryPushdownDebugTest extends AnyFunSuite with TestBase with BeforeAndAfterEach {

  private var testDataPath: String = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testDataPath = Files.createTempDirectory("indexquery_pushdown_debug_").toString
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (testDataPath != null && Files.exists(Paths.get(testDataPath))) {
      val dir = new File(testDataPath)
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  test("Reproduce indexquery pushdown issue - should filter but returns all results") {
    // Create test data similar to user's example
    val testData = Seq(
      (1, "The quick brown fox", "A classic sentence about animals"),
      (2, "Spark is a unified analytics engine", "Apache Spark provides fast data processing"),
      (3, "Machine learning with Python", "Data science and ML tutorials"),
      (4, "Database optimization techniques", "Performance tuning for large datasets")
    )

    val df = spark.createDataFrame(testData).toDF("id", "review_text", "description")

    println(s"Original test data (${testData.length} rows):")
    df.show(false)

    // Write to IndexTables4Spark format
    val tantivyPath = testDataPath + "/indexquery_debug_data"
    df.write
      .format("tantivy4spark")
      .mode("overwrite")
      .save(tantivyPath)

    // Read back the data
    val tantivyDF = spark.read.format("tantivy4spark").load(tantivyPath)

    println("Data read back from IndexTables4Spark:")
    tantivyDF.show(false)

    // Test 1: Create IndexQueryExpression manually (programmatic way)
    println("\n=== Test 1: Programmatic IndexQueryExpression ===")
    val columnRef      = col("review_text").expr
    val queryLiteral   = Literal(UTF8String.fromString("engine"), StringType)
    val indexQueryExpr = IndexQueryExpression(columnRef, queryLiteral)

    println(s"IndexQueryExpression: $indexQueryExpr")
    println(s"Can push down: ${indexQueryExpr.canPushDown}")
    println(s"Query string: ${indexQueryExpr.getQueryString}")

    // Apply the filter
    val programmaticResult = tantivyDF.filter(new Column(indexQueryExpr))
    println("Results with programmatic IndexQueryExpression:")
    programmaticResult.show(false)

    val programmaticCount = programmaticResult.count()
    println(s"Row count: $programmaticCount")

    // Expected: Should return only 1 row (the one containing "engine")
    // Actual issue: Likely returns all 4 rows

    if (programmaticCount == testData.length) {
      println("❌ ISSUE CONFIRMED: IndexQuery returns all rows instead of filtering!")
      println("❌ Filter pushdown is not working - no filtering is happening")
    } else if (programmaticCount == 1) {
      println("✅ IndexQuery is working correctly - returned filtered results")
    } else {
      println(s"⚠️  Unexpected result count: $programmaticCount")
    }

    // Test 2: Check what happens with standard Spark filters for comparison
    println("\n=== Test 2: Standard Spark Filter for Comparison ===")
    val standardResult = tantivyDF.filter(col("review_text").contains("engine"))
    println("Results with standard contains filter:")
    standardResult.show(false)
    println(s"Standard filter count: ${standardResult.collect().length}")

    // Test 3: Check expression conversion
    println("\n=== Test 3: Expression Conversion ===")
    val convertedFilter = ExpressionUtils.expressionToIndexQueryFilter(indexQueryExpr)
    println(s"Converted filter: $convertedFilter")

    if (convertedFilter.isDefined) {
      val filter = convertedFilter.get
      println(s"Filter column: ${filter.columnName}")
      println(s"Filter query: ${filter.queryString}")
      println(s"Filter valid: ${filter.isValid}")
    } else {
      println("❌ Failed to convert IndexQueryExpression to IndexQueryFilter")
    }

    // Test 4: Check what Spark thinks about our filter
    println("\n=== Test 4: Query Plan Analysis ===")
    programmaticResult.explain(true)
  }

  test("Debug filter pushdown mechanism") {
    // Test the filter pushdown mechanism to understand what's happening
    val testData = Seq((1, "test engine data"), (2, "other content"))
    val df       = spark.createDataFrame(testData).toDF("id", "content")

    val tantivyPath = testDataPath + "/pushdown_debug"
    df.write.format("tantivy4spark").mode("overwrite").save(tantivyPath)

    val tantivyDF = spark.read.format("tantivy4spark").load(tantivyPath)

    // Create IndexQueryExpression
    val expr = IndexQueryExpression(
      col("content").expr,
      Literal(UTF8String.fromString("engine"), StringType)
    )

    // Check if it converts properly
    val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)
    assert(filter.isDefined, "Expression should convert to filter")

    println(s"Converted filter: ${filter.get}")
    println(s"Filter references: ${filter.get.references.mkString(", ")}")

    // Apply and see what happens
    val result = tantivyDF.filter(new Column(expr))
    println(s"Filtered result count: ${result.count()}")
    result.show(false)
  }
}
