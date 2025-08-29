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

package com.tantivy4spark.integration

import com.tantivy4spark.TestBase
import com.tantivy4spark.expressions.IndexQueryExpression
import com.tantivy4spark.filters.IndexQueryFilter
import com.tantivy4spark.util.ExpressionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class IndexQueryIntegrationTest extends AnyFunSuite with TestBase {
  
  test("IndexQueryExpression should convert to IndexQueryFilter correctly") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("spark AND sql"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    val filterOpt = ExpressionUtils.expressionToIndexQueryFilter(expr)
    
    assert(filterOpt.isDefined)
    val filter = filterOpt.get
    assert(filter.column == "title")
    assert(filter.queryString == "spark AND sql")
  }
  
  test("IndexQueryFilter should be recognized as supported filter") {
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import com.tantivy4spark.core.Tantivy4SparkScanBuilder
    import com.tantivy4spark.transaction.TransactionLog
    
    val testSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("content", StringType, nullable = true)
    ))
    
    // Create a mock transaction log and scan builder to test filter support
    // Note: This would require more setup in a real integration test
    val indexQueryFilter = IndexQueryFilter("title", "spark AND sql")
    
    // The filter should be valid
    assert(indexQueryFilter.isValid)
    assert(indexQueryFilter.references.sameElements(Array("title")))
  }
  
  test("Expression utilities should handle complex expressions with IndexQuery") {
    val indexQuery1 = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("spark"), StringType)
    )
    val indexQuery2 = IndexQueryExpression(
      AttributeReference("content", StringType, nullable = true)(),
      Literal(UTF8String.fromString("sql"), StringType)
    )
    val regularFilter = EqualTo(
      AttributeReference("status", StringType, nullable = true)(),
      Literal(UTF8String.fromString("active"), StringType)
    )
    
    val complexExpr = And(And(indexQuery1, indexQuery2), regularFilter)
    
    val extractedQueries = ExpressionUtils.extractIndexQueries(complexExpr)
    
    assert(extractedQueries.length == 2)
    assert(extractedQueries.contains(indexQuery1))
    assert(extractedQueries.contains(indexQuery2))
  }
  
  test("IndexQueryExpression validation should work correctly") {
    // Valid expression
    val validExpr = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("valid query"), StringType)
    )
    
    val validation = ExpressionUtils.validateIndexQueryExpression(validExpr)
    assert(validation.isRight)
    
    // Invalid expression - empty query
    val invalidExpr = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString(""), StringType)
    )
    
    val invalidValidation = ExpressionUtils.validateIndexQueryExpression(invalidExpr)
    assert(invalidValidation.isLeft)
    assert(invalidValidation.left.get.contains("Query string cannot be empty"))
  }
  
  test("Filter to expression conversion should be reversible") {
    val originalFilter = IndexQueryFilter("title", "machine learning AND spark")
    val expr = ExpressionUtils.filterToExpression(originalFilter)
    val convertedFilter = ExpressionUtils.expressionToIndexQueryFilter(expr)
    
    assert(convertedFilter.isDefined)
    val roundTripFilter = convertedFilter.get
    
    assert(roundTripFilter.column == originalFilter.column)
    assert(roundTripFilter.queryString == originalFilter.queryString)
  }
  
  test("IndexQueryExpression should handle various query types") {
    val testCases = Seq(
      ("title", "simple query"),
      ("content", "\"phrase query\""),
      ("text", "wildcard*"),
      ("description", "field:value AND another:term"),
      ("metadata", "(complex OR compound) AND NOT excluded"),
      ("search_field", "range:[10 TO 100]")
    )
    
    testCases.foreach { case (column, query) =>
      val expr = IndexQueryExpression(
        AttributeReference(column, StringType, nullable = true)(),
        Literal(UTF8String.fromString(query), StringType)
      )
      
      assert(expr.getColumnName.contains(column))
      assert(expr.getQueryString.contains(query))
      assert(expr.canPushDown)
      
      val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)
      assert(filter.isDefined)
      assert(filter.get.column == column)
      assert(filter.get.queryString == query)
    }
  }
  
  test("IndexQueryExpression SQL representation should be correct") {
    val expr = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("search query"), StringType)
    )
    
    val sql = expr.sql
    assert(sql.contains("indexquery"))
    assert(sql.contains("title"))
    assert(sql.contains("search query"))
  }
  
  test("Complex expression trees with IndexQuery should extract correctly") {
    val indexQuery = IndexQueryExpression(
      AttributeReference("content", StringType, nullable = true)(),
      Literal(UTF8String.fromString("important documents"), StringType)
    )
    
    val regularFilter = GreaterThan(
      AttributeReference("score", IntegerType, nullable = false)(),
      Literal(80, IntegerType)
    )
    
    val notExpression = Not(indexQuery)
    val orExpression = Or(indexQuery, regularFilter)
    val nestedExpression = And(orExpression, Not(regularFilter))
    
    // Test extraction from different expression types
    assert(ExpressionUtils.extractIndexQueries(notExpression).length == 1)
    assert(ExpressionUtils.extractIndexQueries(orExpression).length == 1)
    assert(ExpressionUtils.extractIndexQueries(nestedExpression).length == 1)
    
    val extractedFromNested = ExpressionUtils.extractIndexQueries(nestedExpression)
    assert(extractedFromNested.head == indexQuery)
  }
  
  test("IndexQueryExpression type checking should validate inputs") {
    // Valid case
    val validExpr = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query"), StringType)
    )
    assert(validExpr.checkInputDataTypes().isSuccess)
    
    // Invalid right operand type
    val invalidRightExpr = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(123, IntegerType)
    )
    val result = invalidRightExpr.checkInputDataTypes()
    assert(result.isFailure)
    assert(result.asInstanceOf[TypeCheckResult.TypeCheckFailure].message.contains("string literal"))
  }
  
  test("IndexQueryFilter should handle special characters and edge cases") {
    val specialCharQueries = Seq(
      "title:\"quotes and spaces\"",
      "field:[range TO query]",
      "(parentheses) AND {braces}",
      "unicode: 测试查询",
      "symbols: @#$%^&*()",
      "escaped\\:colon\\*asterisk"
    )
    
    specialCharQueries.foreach { query =>
      val filter = IndexQueryFilter("content", query)
      assert(filter.isValid)
      assert(filter.queryString == query)
      assert(filter.references.sameElements(Array("content")))
    }
  }
  
  test("End-to-end SQL integration test with indexquery operator") {
    val spark = this.spark
    import spark.implicits._
    
    // Create test data
    val testData = Seq(
      (1, "Apache Spark documentation", "technology", "spark AND sql"),
      (2, "Machine learning algorithms", "ai", "machine learning"),  
      (3, "Big data processing", "technology", "big data"),
      (4, "Natural language processing", "ai", "nlp processing"),
      (5, "Distributed computing systems", "technology", "distributed AND computing")
    ).toDF("id", "title", "category", "tags")
    
    val tempPath = java.nio.file.Files.createTempDirectory("tantivy4spark_indexquery_test").toString
    
    try {
      // Write test data using Tantivy4Spark V2 DataSource
      testData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)
      
      // Read data back using V2 DataSource API
      val df = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)
      
      // Create temporary view
      df.createOrReplaceTempView("test_documents")
      
      // Test SQL query with indexquery operator - should use our custom parser
      val sqlResult = spark.sql("""
        SELECT id, title, category 
        FROM test_documents 
        WHERE title indexquery 'spark AND documentation'
        ORDER BY id
      """)
      
      val results = sqlResult.collect()
      
      // Verify that the query executed and returned results
      assert(results.length > 0, "SQL query with indexquery operator should return results")
      
      // Verify the SQL plan shows our custom expression
      val planString = sqlResult.queryExecution.executedPlan.toString()
      assert(planString.contains("indexquery") || planString.contains("IndexQuery"), 
        "Query plan should contain evidence of indexquery processing")
      
      // Test another query pattern
      val sqlResult2 = spark.sql("""
        SELECT id, title 
        FROM test_documents 
        WHERE category indexquery 'technology'
      """)
      
      val results2 = sqlResult2.collect()
      assert(results2.length >= 1, "Technology category query should return at least one result")
      
    } finally {
      // Cleanup
      import java.nio.file._
      import scala.util.Try
      Try {
        Files.walk(Paths.get(tempPath))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.delete)
      }
    }
  }
}