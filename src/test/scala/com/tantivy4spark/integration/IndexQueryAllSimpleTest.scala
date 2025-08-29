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

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import com.tantivy4spark.TestBase
import com.tantivy4spark.expressions.IndexQueryAllExpression
import com.tantivy4spark.filters.IndexQueryAllFilter
import com.tantivy4spark.util.ExpressionUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.expressions.Literal

class IndexQueryAllSimpleTest extends AnyFunSuite with TestBase with BeforeAndAfterEach {

  test("IndexQueryAllExpression basic functionality") {
    // Test basic expression creation and validation
    val query = Literal(UTF8String.fromString("Apache AND Spark"), StringType)
    val expr = IndexQueryAllExpression(query)
    
    // Test basic properties
    assert(expr.getQueryString.contains("Apache AND Spark"))
    assert(expr.canPushDown)
    assert(expr.prettyName == "indexqueryall")
    assert(expr.dataType.typeName == "boolean")
    assert(!expr.nullable)
    
    // Test evaluation fallback
    import org.apache.spark.sql.catalyst.expressions.EmptyRow
    assert(expr.eval(EmptyRow) == true)
  }

  test("IndexQueryAllFilter conversion and validation") {
    // Test expression to filter conversion
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr = IndexQueryAllExpression(query)
    
    val filterOpt = ExpressionUtils.expressionToIndexQueryAllFilter(expr)
    assert(filterOpt.isDefined)
    
    val filter = filterOpt.get
    assert(filter.queryString == "machine learning")
    assert(filter.isValid)
    assert(filter.references.isEmpty) // No specific field references for all-fields search
    
    // Test filter back to expression conversion
    val convertedExpr = ExpressionUtils.filterToIndexQueryAllExpression(filter)
    assert(convertedExpr.getQueryString.contains("machine learning"))
  }

  test("IndexQueryAllExpression with complex queries") {
    val complexQueries = Seq(
      "apache AND spark",
      "(machine OR AI) AND learning", 
      "\"natural language processing\"",
      "python OR scala OR java",
      "data AND NOT deprecated"
    )
    
    complexQueries.foreach { queryString =>
      val query = Literal(UTF8String.fromString(queryString), StringType)
      val expr = IndexQueryAllExpression(query)
      
      assert(expr.getQueryString.contains(queryString))
      assert(expr.canPushDown)
      assert(expr.checkInputDataTypes().isSuccess)
      
      // Test conversion to filter
      val filter = ExpressionUtils.expressionToIndexQueryAllFilter(expr)
      assert(filter.isDefined)
      assert(filter.get.queryString == queryString)
    }
  }

  test("IndexQueryAllExpression validation and error handling") {
    // Test valid expression
    val validQuery = Literal(UTF8String.fromString("valid query"), StringType)
    val validExpr = IndexQueryAllExpression(validQuery)
    val validation = ExpressionUtils.validateIndexQueryAllExpression(validExpr)
    assert(validation.isRight)
    
    // Test invalid query type
    val invalidQuery = org.apache.spark.sql.catalyst.expressions.Add(
      Literal(1, org.apache.spark.sql.types.IntegerType), 
      Literal(2, org.apache.spark.sql.types.IntegerType)
    )
    val invalidExpr = IndexQueryAllExpression(invalidQuery)
    assert(!invalidExpr.canPushDown)
    assert(invalidExpr.getQueryString.isEmpty)
    
    val invalidValidation = ExpressionUtils.validateIndexQueryAllExpression(invalidExpr)
    assert(invalidValidation.isLeft)
    
    // Test empty query
    val emptyQuery = Literal(UTF8String.fromString(""), StringType)
    val emptyExpr = IndexQueryAllExpression(emptyQuery)
    val emptyValidation = ExpressionUtils.validateIndexQueryAllExpression(emptyExpr)
    assert(emptyValidation.isLeft)
    assert(emptyValidation.left.get.contains("Query string cannot be empty"))
  }

  test("IndexQueryAllExpression expression tree traversal") {
    import org.apache.spark.sql.catalyst.expressions._
    
    // Create IndexQueryAll expressions
    val query1 = IndexQueryAllExpression(Literal(UTF8String.fromString("apache"), StringType))
    val query2 = IndexQueryAllExpression(Literal(UTF8String.fromString("spark"), StringType))
    
    // Test simple expression
    val simpleQueries = ExpressionUtils.extractIndexQueries(query1)
    assert(simpleQueries.length == 1)
    assert(simpleQueries.head == query1)
    
    // Test complex expression tree
    val otherExpr = EqualTo(
      AttributeReference("status", StringType, nullable = true)(), 
      Literal(UTF8String.fromString("active"), StringType)
    )
    val andExpr = And(query1, otherExpr)
    val complexQueries = ExpressionUtils.extractIndexQueries(andExpr)
    assert(complexQueries.length == 1)
    assert(complexQueries.head == query1)
    
    // Test multiple IndexQueryAll expressions
    val multiExpr = And(query1, query2)
    val multiQueries = ExpressionUtils.extractIndexQueryAllExpressions(multiExpr)
    assert(multiQueries.length == 2)
    assert(multiQueries.contains(query1))
    assert(multiQueries.contains(query2))
  }

  test("IndexQueryAllFilter metadata and utility methods") {
    val filter = IndexQueryAllFilter("complex AND (query OR pattern)")
    
    // Test metadata
    val metadata = filter.metadata
    assert(metadata("type") == "IndexQueryAllFilter")
    assert(metadata("queryString") == "complex AND (query OR pattern)")
    assert(metadata("fieldCount") == "all")
    assert(metadata("hasSpecialSyntax").asInstanceOf[Boolean])
    
    // Test utility methods
    assert(filter.hasSpecialSyntax) // Contains AND, OR, parentheses
    assert(filter.isCompatibleWith(IndexQueryAllFilter("another query")))
    assert(filter.getQueryString == "complex AND (query OR pattern)")
    
    // Test simple query without special syntax
    val simpleFilter = IndexQueryAllFilter("simple")
    assert(!simpleFilter.hasSpecialSyntax)
  }

  test("Integration with ExpressionUtils utility methods") {
    val expr = IndexQueryAllExpression(Literal(UTF8String.fromString("test query"), StringType))
    
    // Test isValidIndexQuery
    assert(ExpressionUtils.isValidIndexQuery(expr))
    
    // Test with invalid expression
    val invalidExpr = org.apache.spark.sql.catalyst.expressions.Add(Literal(1), Literal(2))
    assert(!ExpressionUtils.isValidIndexQuery(invalidExpr))
    
    // Test round-trip conversion
    val filter = ExpressionUtils.expressionToIndexQueryAllFilter(expr).get
    val convertedExpr = ExpressionUtils.filterToIndexQueryAllExpression(filter)
    val finalFilter = ExpressionUtils.expressionToIndexQueryAllFilter(convertedExpr)
    
    assert(finalFilter.isDefined)
    assert(finalFilter.get.queryString == "test query")
  }

  test("SQL string representation") {
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr = IndexQueryAllExpression(query)
    
    val sqlString = expr.sql
    assert(sqlString.contains("indexqueryall"))
    assert(sqlString.contains("machine learning"))
    
    val toStringResult = expr.toString
    assert(toStringResult.contains("indexqueryall"))
    assert(toStringResult.contains("machine learning"))
  }
}