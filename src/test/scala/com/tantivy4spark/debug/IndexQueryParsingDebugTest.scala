package com.tantivy4spark.debug

import com.tantivy4spark.TestBase

class IndexQueryParsingDebugTest extends TestBase {

  test("should debug IndexQuery parsing and plan structure") {
    withTempPath { testDataPath =>
      // Create simple test data
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      
      val testData = Seq(
        ("doc1", "apache spark framework"),
        ("doc2", "machine learning with spark")
      ).toDF("id", "title")
      
      // Write test data
      testData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(testDataPath)
      
      // Create temp view
      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW debug_table USING com.tantivy4spark.core.Tantivy4SparkTableProvider OPTIONS (path '$testDataPath')")
      
      println("=== Testing IndexQuery SQL Parsing ===")
      
      // Test 1: Parse a simple indexquery expression
      val indexQuerySql = "SELECT id FROM debug_table WHERE title indexquery 'spark'"
      println(s"SQL: $indexQuerySql")
      
      // Parse the plan
      val logicalPlan = spark.sql(indexQuerySql).queryExecution.logical
      println(s"Logical Plan: $logicalPlan")
      
      // Test 2: Parse indexqueryall expression  
      val indexQueryAllSql = "SELECT id FROM debug_table WHERE indexqueryall('spark')"
      println(s"SQL: $indexQueryAllSql")
      
      try {
        val logicalPlanAll = spark.sql(indexQueryAllSql).queryExecution.logical
        println(s"Logical Plan (indexqueryall): $logicalPlanAll")
      } catch {
        case e: Exception => 
          println(s"IndexQueryAll failed: ${e.getMessage}")
      }
      
      println("=== Testing Direct Function Calls ===")
      
      // Test 3: Test direct function calls
      try {
        val directQuery = spark.sql("SELECT tantivy4spark_indexquery('title', 'spark') as result")
        println("Direct tantivy4spark_indexquery function call works")
        val plan = directQuery.queryExecution.logical
        println(s"Function call plan: $plan")
      } catch {
        case e: Exception => 
          println(s"Direct function call failed: ${e.getMessage}")
      }
      
      try {
        val directQueryAll = spark.sql("SELECT tantivy4spark_indexqueryall('spark') as result")
        println("Direct tantivy4spark_indexqueryall function call works")
        val planAll = directQueryAll.queryExecution.logical
        println(s"Function call plan (all): $planAll")
      } catch {
        case e: Exception => 
          println(s"Direct function call (all) failed: ${e.getMessage}")
      }
    }
  }
}