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

/** Diagnostic test to identify why IndexQuery is returning 0 results */
class IndexQueryDiagnosticTest extends TestBase {

  test("Create test data and validate basic operations") {
    val spark = this.spark
    import spark.implicits._

    // Create simple test data
    val testData = Seq(
      (1, "technology", "Machine Learning Fundamentals", "Alice Johnson"),
      (2, "technology", "Data Engineering with Apache Spark", "Bob Chen"),
      (3, "science", "Quantum Physics Applications", "David Wilson")
    ).toDF("id", "category", "title", "author")

    println("=== Creating Test Data ===")
    testData.show()

    // Write using V2 DataSource API with text fields
    testData.write
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.category", "text")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.author", "string")
      .mode("overwrite")
      .save(s"$tempDir/diagnostic_test")

    println("✅ Data written successfully")

    // Read it back and verify
    val df = spark.read
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .load(s"$tempDir/diagnostic_test")

    println("=== Reading Data Back ===")
    println(s"Total rows: ${df.count()}")
    df.show()

    // Create a view for SQL testing
    df.createOrReplaceTempView("diagnostic_docs")

    // Test 1: Basic SQL query (no IndexQuery)
    println("=== Test 1: Basic SQL Query ===")
    val basicCount = spark.sql("SELECT COUNT(*) as count FROM diagnostic_docs").collect()
    println(s"Basic count: ${basicCount.head.getLong(0)}")

    // Test 2: Regular filter
    println("=== Test 2: Regular Filter ===")
    val regularResults = spark.sql("SELECT id, category FROM diagnostic_docs WHERE category = 'technology'").collect()
    println(s"Regular filter results: ${regularResults.length}")
    regularResults.foreach(row => println(s"  - ${row.getInt(0)}: ${row.getString(1)}"))

    // Test 3: IndexQuery syntax test (operator)
    println("=== Test 3: IndexQuery Operator Syntax Test ===")
    try {
      val indexQueryResults =
        spark.sql("SELECT id, title FROM diagnostic_docs WHERE title indexquery 'machine'").collect()
      println(s"IndexQuery operator results: ${indexQueryResults.length}")
      indexQueryResults.foreach(row => println(s"  - ${row.getInt(0)}: ${row.getString(1)}"))
    } catch {
      case e: Exception =>
        println(s"IndexQuery operator failed: ${e.getMessage}")
        e.printStackTrace()
    }

    // Test 4: IndexQuery function test
    println("=== Test 4: IndexQuery Function Syntax Test ===")
    try {
      val functionResults =
        spark.sql("SELECT id, title FROM diagnostic_docs WHERE tantivy4spark_indexquery(title, 'machine')").collect()
      println(s"IndexQuery function results: ${functionResults.length}")
      functionResults.foreach(row => println(s"  - ${row.getInt(0)}: ${row.getString(1)}"))
    } catch {
      case e: Exception =>
        println(s"IndexQuery function failed: ${e.getMessage}")
        e.printStackTrace()
    }

    // Test 4: Check if extensions are loaded
    println("=== Test 4: Extension Check ===")
    try
      spark.sql("SHOW FUNCTIONS").collect().filter(_.getString(0).contains("tantivy4spark")).foreach { row =>
        println(s"Found function: ${row.getString(0)}")
      }
    catch {
      case e: Exception =>
        println(s"Function check failed: ${e.getMessage}")
    }

    println("✅ Diagnostic test completed")
  }
}
