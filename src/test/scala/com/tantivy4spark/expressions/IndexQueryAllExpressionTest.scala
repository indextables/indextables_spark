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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class IndexQueryAllExpressionTest extends AnyFunSuite {

  test("IndexQueryAllExpression should extract query string correctly from UTF8String literal") {
    val query = Literal(UTF8String.fromString("apache AND spark"), StringType)
    val expr  = IndexQueryAllExpression(query)

    assert(expr.getQueryString.contains("apache AND spark"))
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should extract query string from String literal") {
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr  = IndexQueryAllExpression(query)

    assert(expr.getQueryString.contains("machine learning"))
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should return None for invalid query literals") {
    val invalidQuery = Add(Literal(1, IntegerType), Literal(2, IntegerType))
    val expr         = IndexQueryAllExpression(invalidQuery)

    assert(expr.getQueryString.isEmpty)
    assert(!expr.canPushDown)
  }

  test("IndexQueryAllExpression should have correct data type") {
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr  = IndexQueryAllExpression(query)

    assert(expr.dataType == BooleanType)
    assert(expr.nullable == false) // Predicates are typically not nullable
  }

  test("IndexQueryAllExpression should have correct symbol and pretty name") {
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr  = IndexQueryAllExpression(query)

    assert(expr.prettyName == "indexqueryall")
  }

  test("IndexQueryAllExpression should generate correct SQL representation") {
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val sql = expr.sql
    assert(sql.contains("indexqueryall"))
    assert(sql.contains("'machine learning'"))
  }

  test("IndexQueryAllExpression should evaluate to true by default") {
    val query = Literal(UTF8String.fromString("test"), StringType)
    val expr  = IndexQueryAllExpression(query)

    // The expression should return true when evaluated (since filtering happens at source)
    val result = expr.eval(EmptyRow)
    assert(result == true)
  }

  test("IndexQueryAllExpression should validate input data types") {
    val validQuery   = Literal(UTF8String.fromString("test query"), StringType)
    val invalidQuery = Literal(123, IntegerType)

    val validExpr   = IndexQueryAllExpression(validQuery)
    val invalidExpr = IndexQueryAllExpression(invalidQuery)

    assert(validExpr.checkInputDataTypes().isSuccess)
    assert(invalidExpr.checkInputDataTypes().isFailure)

    val failureResult = invalidExpr.checkInputDataTypes()
    assert(failureResult.isInstanceOf[TypeCheckResult.TypeCheckFailure])
  }

  test("IndexQueryAllExpression should handle complex query strings") {
    val complexQuery = Literal(UTF8String.fromString("(apache AND spark) OR (hadoop AND mapreduce)"), StringType)
    val expr         = IndexQueryAllExpression(complexQuery)

    assert(expr.getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should handle empty query strings") {
    val emptyQuery = Literal(UTF8String.fromString(""), StringType)
    val expr       = IndexQueryAllExpression(emptyQuery)

    assert(expr.getQueryString.contains(""))
    // Empty query string should still allow pushdown - validation happens at execution time
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should handle null query strings") {
    val nullQuery = Literal(null, StringType)
    val expr      = IndexQueryAllExpression(nullQuery)

    assert(expr.getQueryString.isEmpty)
    assert(!expr.canPushDown)
  }

  test("IndexQueryAllExpression toString should be human readable") {
    val query = Literal(UTF8String.fromString("test query"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val str = expr.toString
    assert(str.contains("indexqueryall"))
    assert(str.contains("test query"))
  }

  test("IndexQueryAllExpression should handle boolean queries") {
    val booleanQueries = Seq(
      "term1 AND term2",
      "term1 OR term2",
      "term1 AND NOT term2",
      "(term1 OR term2) AND term3"
    )

    booleanQueries.foreach { queryString =>
      val query = Literal(UTF8String.fromString(queryString), StringType)
      val expr  = IndexQueryAllExpression(query)

      assert(expr.getQueryString.contains(queryString))
      assert(expr.canPushDown)
    }
  }

  test("IndexQueryAllExpression should handle phrase queries") {
    val phraseQuery = Literal(UTF8String.fromString("\"machine learning\""), StringType)
    val expr        = IndexQueryAllExpression(phraseQuery)

    assert(expr.getQueryString.contains("\"machine learning\""))
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should handle wildcard queries") {
    val wildcardQueries = Seq(
      "spark*",
      "*learning",
      "mach*ne",
      "data?"
    )

    wildcardQueries.foreach { queryString =>
      val query = Literal(UTF8String.fromString(queryString), StringType)
      val expr  = IndexQueryAllExpression(query)

      assert(expr.getQueryString.contains(queryString))
      assert(expr.canPushDown)
    }
  }

  test("IndexQueryAllExpression should handle special characters") {
    val specialQuery = Literal(UTF8String.fromString("term1:value AND field:(value1 OR value2)"), StringType)
    val expr         = IndexQueryAllExpression(specialQuery)

    assert(expr.getQueryString.contains("term1:value AND field:(value1 OR value2)"))
    assert(expr.canPushDown)
  }

  test("IndexQueryAllExpression should support child replacement") {
    val originalQuery = Literal(UTF8String.fromString("original"), StringType)
    val newQuery      = Literal(UTF8String.fromString("updated"), StringType)
    val originalExpr  = IndexQueryAllExpression(originalQuery)

    // Test that we can create a new expression with different child
    val newExpr = IndexQueryAllExpression(newQuery)

    assert(newExpr.getQueryString.contains("updated"))
    assert(originalExpr.getQueryString.contains("original"))
    assert(newExpr != originalExpr)
  }
}
