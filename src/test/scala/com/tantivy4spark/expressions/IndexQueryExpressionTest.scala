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

package com.tantivy4spark.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class IndexQueryExpressionTest extends AnyFunSuite {
  
  test("IndexQueryExpression should extract column name correctly from AttributeReference") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("spark AND sql"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.contains("spark AND sql"))
    assert(expr.canPushDown)
  }
  
  test("IndexQueryExpression should extract column name from UnresolvedAttribute") {
    val column = UnresolvedAttribute(Seq("content", "text"))
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.getColumnName.contains("content.text"))
    assert(expr.getQueryString.contains("machine learning"))
    assert(expr.canPushDown)
  }
  
  test("IndexQueryExpression should handle UTF8String literals") {
    val column = AttributeReference("description", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("apache spark"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.getColumnName.contains("description"))
    assert(expr.getQueryString.contains("apache spark"))
    assert(expr.canPushDown)
  }
  
  test("IndexQueryExpression should return None for invalid column references") {
    val invalidColumn = Literal(UTF8String.fromString("not_a_column"), StringType)
    val query = Literal(UTF8String.fromString("test query"), StringType)
    val expr = IndexQueryExpression(invalidColumn, query)
    
    assert(expr.getColumnName.isEmpty)
    assert(expr.getQueryString.contains("test query"))
    assert(!expr.canPushDown)
  }
  
  test("IndexQueryExpression should return None for invalid query literals") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val invalidQuery = Add(Literal(1, IntegerType), Literal(2, IntegerType))
    val expr = IndexQueryExpression(column, invalidQuery)
    
    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.isEmpty)
    assert(!expr.canPushDown)
  }
  
  test("IndexQueryExpression should have correct data type") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.dataType == BooleanType)
    assert(expr.nullable == false) // Predicates are typically not nullable
  }
  
  test("IndexQueryExpression should have correct symbol and pretty name") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.prettyName == "indexquery")
  }
  
  test("IndexQueryExpression should generate correct SQL representation") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    val sql = expr.sql
    assert(sql.contains("indexquery"))
    assert(sql.contains("title"))
    assert(sql.contains("'machine learning'"))
  }
  
  test("IndexQueryExpression should evaluate to true by default") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    // The expression should return true when evaluated (since filtering happens at source)
    val result = expr.eval(EmptyRow)
    assert(result == true)
  }
  
  test("IndexQueryExpression should validate input data types") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val validQuery = Literal(UTF8String.fromString("test query"), StringType)
    val invalidQuery = Literal(123, IntegerType)
    
    val validExpr = IndexQueryExpression(column, validQuery)
    val invalidExpr = IndexQueryExpression(column, invalidQuery)
    
    assert(validExpr.checkInputDataTypes().isSuccess)
    assert(invalidExpr.checkInputDataTypes().isFailure)
    
    val failureResult = invalidExpr.checkInputDataTypes()
    assert(failureResult.isInstanceOf[TypeCheckResult.TypeCheckFailure])
  }
  
  test("IndexQueryExpression should handle complex query strings") {
    val column = AttributeReference("content", StringType, nullable = true)()
    val complexQuery = Literal(UTF8String.fromString("(apache AND spark) OR (hadoop AND mapreduce)"), StringType)
    val expr = IndexQueryExpression(column, complexQuery)
    
    assert(expr.getColumnName.contains("content"))
    assert(expr.getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
    assert(expr.canPushDown)
  }
  
  test("IndexQueryExpression should handle empty query strings") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val emptyQuery = Literal(UTF8String.fromString(""), StringType)
    val expr = IndexQueryExpression(column, emptyQuery)
    
    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.contains(""))
    // Empty query string should still allow pushdown - validation happens at execution time
    assert(expr.canPushDown)
  }
  
  test("IndexQueryExpression should handle null query strings") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val nullQuery = Literal(null, StringType)
    val expr = IndexQueryExpression(column, nullQuery)
    
    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.isEmpty)
    assert(!expr.canPushDown)
  }
  
  test("IndexQueryExpression toString should be human readable") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal(UTF8String.fromString("test query"), StringType)
    val expr = IndexQueryExpression(column, query)
    
    val str = expr.toString
    assert(str.contains("indexquery"))
    assert(str.contains("title"))
    assert(str.contains("test query"))
  }
}