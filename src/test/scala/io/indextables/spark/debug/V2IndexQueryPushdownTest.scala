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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import io.indextables.spark.TestBase
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import io.indextables.spark.expressions.IndexQueryExpression
import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Debug test specifically for V2 IndexQuery pushdown issues. This test isolates V2 DataSource API behavior to verify
 * IndexQuery pushdown.
 */
class V2IndexQueryPushdownTest extends AnyFunSuite with TestBase with BeforeAndAfterEach {

  test("V2 DataSource API should push down IndexQuery expressions") {
    val tempDir   = Files.createTempDirectory("tantivy4spark_test_")
    val tablePath = tempDir.toString

    // Create test data with review content
    val testData = Seq(
      (1, "The quick brown fox"),
      (2, "Spark is a unified analytics engine"),
      (3, "Machine learning with Python"),
      (4, "Database optimization techniques")
    )
    val df = spark.createDataFrame(testData).toDF("id", "review_text")

    println("Original test data (4 rows):")
    df.show(truncate = false)

    // Write using V2 DataSource API
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider") // Explicit V2 provider
      .mode("overwrite")
      .option("path", tablePath)
      .save()

    // Read back using V2 DataSource API
    val tantivyDF = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("path", tablePath)
      .load()

    println("Data read back from IndexTables4Spark:")
    tantivyDF.show(truncate = false)

    // Test 1: Programmatic IndexQueryExpression with V2
    println("\n=== Test 1: V2 Programmatic IndexQueryExpression ===")
    val columnRef      = col("review_text").expr
    val queryLiteral   = Literal(UTF8String.fromString("engine"), StringType)
    val indexQueryExpr = IndexQueryExpression(columnRef, queryLiteral)
    println(s"IndexQueryExpression: $indexQueryExpr")
    println(s"Before filter - logical plan:")
    println(tantivyDF.queryExecution.logical.toString)

    val filtered = tantivyDF.filter(new Column(indexQueryExpr))

    println(s"After filter - logical plan:")
    println(filtered.queryExecution.logical.toString)
    println("Results with V2 programmatic IndexQueryExpression:")
    val resultCount = filtered.collect().length
    filtered.show(truncate = false)
    println(s"Row count: $resultCount")

    // Analyze physical plan for V2 pushdown
    println("\n=== Test 2: V2 Physical Plan Analysis ===")
    filtered.explain(true)

    // Check if filters are being pushed down
    val physicalPlan       = filtered.queryExecution.executedPlan.toString
    val hasPushedFilters   = physicalPlan.contains("PushedFilters:")
    val pushedFiltersEmpty = physicalPlan.contains("PushedFilters: []")

    println(s"Physical plan contains PushedFilters: $hasPushedFilters")
    println(s"PushedFilters is empty: $pushedFiltersEmpty")

    if (pushedFiltersEmpty) {
      println("❌ V2 IndexQuery pushdown is NOT working - no filters pushed down")
    } else {
      println("✅ V2 IndexQuery pushdown appears to be working")
    }

    // Test 3: SQL-based IndexQuery with V2
    println("\n=== Test 3: V2 SQL-based IndexQuery ===")
    tantivyDF.createOrReplaceTempView("test_table_v2")

    val sqlResult = spark.sql("SELECT * FROM test_table_v2 WHERE review_text indexquery 'engine'")
    println("SQL IndexQuery results:")
    val sqlCount = sqlResult.collect().length
    sqlResult.show(truncate = false)
    println(s"SQL Row count: $sqlCount")

    sqlResult.explain(true)

    // Verify results are correct
    if (resultCount == 1 && sqlCount == 1) {
      println("✅ V2 IndexQuery is filtering correctly")
    } else {
      println(s"❌ V2 IndexQuery filtering issue - expected 1 row, got programmatic: $resultCount, SQL: $sqlCount")
    }

    // Test 4: V2 IndexQueryAll pushdown
    println("\n=== Test 4: V2 Programmatic IndexQueryAll ===")
    import io.indextables.spark.expressions.IndexQueryAllExpression

    val indexQueryAllExpr = IndexQueryAllExpression(Literal(UTF8String.fromString("engine"), StringType))
    println(s"IndexQueryAllExpression: $indexQueryAllExpr")

    println("Results with V2 programmatic IndexQueryAllExpression:")
    val indexQueryAllResult = tantivyDF.filter(new Column(indexQueryAllExpr))
    val indexQueryAllCount  = indexQueryAllResult.collect().length
    indexQueryAllResult.show(truncate = false)
    println(s"IndexQueryAll row count: $indexQueryAllCount")

    indexQueryAllResult.explain(true)

    // Test 5: V2 SQL-based IndexQueryAll
    println("\n=== Test 5: V2 SQL-based IndexQueryAll ===")
    val sqlIndexQueryAllResult = spark.sql("SELECT * FROM test_table_v2 WHERE indexqueryall('engine')")
    println("SQL IndexQueryAll results:")
    val sqlIndexQueryAllCount = sqlIndexQueryAllResult.collect().length
    sqlIndexQueryAllResult.show(truncate = false)
    println(s"SQL IndexQueryAll row count: $sqlIndexQueryAllCount")

    sqlIndexQueryAllResult.explain(true)

    // Verify IndexQueryAll results are correct (should also return 1 row with "engine")
    if (indexQueryAllCount == 1 && sqlIndexQueryAllCount == 1) {
      println("✅ V2 IndexQueryAll is filtering correctly")
    } else {
      println(s"❌ V2 IndexQueryAll filtering issue - expected 1 row, got programmatic: $indexQueryAllCount, SQL: $sqlIndexQueryAllCount")
    }

    // Cleanup
    try {
      val dir = new File(tablePath)
      if (dir.exists()) {
        dir.listFiles().foreach(_.delete())
        dir.delete()
      }
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }
}
