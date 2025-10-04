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

package io.indextables.spark.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression}
import io.indextables.spark.filters.{IndexQueryAllFilter, IndexQueryFilter}
import org.scalatest.funsuite.AnyFunSuite

class ExpressionUtilsIndexQueryAllTest extends AnyFunSuite {

  test("expressionToIndexQueryAllFilter should convert valid IndexQueryAllExpression") {
    val query = Literal(UTF8String.fromString("apache AND spark"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val filter = ExpressionUtils.expressionToIndexQueryAllFilter(expr)

    assert(filter.isDefined)
    val indexQueryAllFilter = filter.get
    assert(indexQueryAllFilter.queryString == "apache AND spark")
  }

  test("expressionToIndexQueryAllFilter should handle UTF8String literals") {
    val query = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val filter = ExpressionUtils.expressionToIndexQueryAllFilter(expr)

    assert(filter.isDefined)
    val indexQueryAllFilter = filter.get.asInstanceOf[IndexQueryAllFilter]
    assert(indexQueryAllFilter.queryString == "machine learning")
  }

  test("expressionToIndexQueryAllFilter should return None for invalid expressions") {
    val nonIndexQueryAllExpr = Add(Literal(1), Literal(2))

    val filter = ExpressionUtils.expressionToIndexQueryAllFilter(nonIndexQueryAllExpr)
    assert(filter.isEmpty)
  }

  test("expressionToIndexQueryAllFilter should return None for IndexQueryAllExpression with invalid parts") {
    val invalidQuery = Add(Literal(1), Literal(2))
    val expr         = IndexQueryAllExpression(invalidQuery)

    val filter = ExpressionUtils.expressionToIndexQueryAllFilter(expr)
    assert(filter.isEmpty)
  }

  test("filterToIndexQueryAllExpression should convert IndexQueryAllFilter correctly") {
    val filter = IndexQueryAllFilter("apache AND spark")
    val expr   = ExpressionUtils.filterToIndexQueryAllExpression(filter)

    assert(expr.getQueryString.contains("apache AND spark"))
  }

  test("isValidIndexQuery should return true for valid IndexQueryAllExpression") {
    val query = Literal(UTF8String.fromString("valid query"), StringType)
    val expr  = IndexQueryAllExpression(query)

    assert(ExpressionUtils.isValidIndexQuery(expr))
  }

  test("isValidIndexQuery should return false for invalid IndexQueryAllExpression") {
    val invalidQuery = Add(Literal(1), Literal(2))
    val expr         = IndexQueryAllExpression(invalidQuery)

    assert(!ExpressionUtils.isValidIndexQuery(expr))
  }

  test("isValidIndexQuery should return false for non-IndexQuery expressions") {
    val nonIndexQueryExpr = Add(Literal(1), Literal(2))

    assert(!ExpressionUtils.isValidIndexQuery(nonIndexQueryExpr))
  }

  test("extractIndexQueries should find IndexQueryAllExpression in simple expression") {
    val query = Literal(UTF8String.fromString("test query"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val indexQueries = ExpressionUtils.extractIndexQueries(expr)

    assert(indexQueries.length == 1)
    assert(indexQueries.head == expr)
  }

  test("extractIndexQueries should find IndexQueryAllExpression in And expression") {
    val indexQueryAll = IndexQueryAllExpression(
      Literal(UTF8String.fromString("test query"), StringType)
    )
    val otherExpr = EqualTo(AttributeReference("status", StringType, nullable = true)(), Literal("active"))
    val andExpr   = And(indexQueryAll, otherExpr)

    val indexQueries = ExpressionUtils.extractIndexQueries(andExpr)

    assert(indexQueries.length == 1)
    assert(indexQueries.head == indexQueryAll)
  }

  test("extractIndexQueries should find multiple IndexQuery expressions") {
    val indexQuery = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query1"), StringType)
    )
    val indexQueryAll = IndexQueryAllExpression(
      Literal(UTF8String.fromString("query2"), StringType)
    )
    val andExpr = And(indexQuery, indexQueryAll)

    val indexQueries = ExpressionUtils.extractIndexQueries(andExpr)

    assert(indexQueries.length == 2)
    assert(indexQueries.contains(indexQuery))
    assert(indexQueries.contains(indexQueryAll))
  }

  test("extractIndexQueryAllExpressions should find only IndexQueryAllExpression") {
    val indexQuery = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query1"), StringType)
    )
    val indexQueryAll = IndexQueryAllExpression(
      Literal(UTF8String.fromString("query2"), StringType)
    )
    val andExpr = And(indexQuery, indexQueryAll)

    val indexQueryAlls = ExpressionUtils.extractIndexQueryAllExpressions(andExpr)

    assert(indexQueryAlls.length == 1)
    assert(indexQueryAlls.head == indexQueryAll)
  }

  test("extractIndexQueryExpressions should find only IndexQueryExpression") {
    val indexQuery = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query1"), StringType)
    )
    val indexQueryAll = IndexQueryAllExpression(
      Literal(UTF8String.fromString("query2"), StringType)
    )
    val andExpr = And(indexQuery, indexQueryAll)

    val indexQueries = ExpressionUtils.extractIndexQueryExpressions(andExpr)

    assert(indexQueries.length == 1)
    assert(indexQueries.head == indexQuery)
  }

  test("extractIndexQueries should return empty for expressions without IndexQuery") {
    val simpleExpr = EqualTo(AttributeReference("status", StringType, nullable = true)(), Literal("active"))

    val indexQueries = ExpressionUtils.extractIndexQueries(simpleExpr)
    assert(indexQueries.isEmpty)
  }

  test("validateIndexQueryAllExpression should succeed for valid expression") {
    val query = Literal(UTF8String.fromString("valid query"), StringType)
    val expr  = IndexQueryAllExpression(query)

    val validation = ExpressionUtils.validateIndexQueryAllExpression(expr)
    assert(validation.isRight)
  }

  test("validateIndexQueryAllExpression should fail for empty query") {
    val emptyQuery = Literal(UTF8String.fromString(""), StringType)
    val expr       = IndexQueryAllExpression(emptyQuery)

    val validation = ExpressionUtils.validateIndexQueryAllExpression(expr)
    assert(validation.isLeft)
    assert(validation.left.get.contains("Query string cannot be empty"))
  }

  test("validateIndexQueryAllExpression should fail for invalid query") {
    val invalidQuery = Add(Literal(1), Literal(2))
    val expr         = IndexQueryAllExpression(invalidQuery)

    val validation = ExpressionUtils.validateIndexQueryAllExpression(expr)
    assert(validation.isLeft)
    assert(validation.left.get.contains("Invalid query string"))
  }

  test("extractIndexQueries should handle nested expressions correctly") {
    val indexQueryAll1 = IndexQueryAllExpression(
      Literal(UTF8String.fromString("query1"), StringType)
    )
    val indexQueryAll2 = IndexQueryAllExpression(
      Literal(UTF8String.fromString("query2"), StringType)
    )
    val nestedExpr = Or(
      And(indexQueryAll1, EqualTo(AttributeReference("status", StringType, nullable = true)(), Literal("active"))),
      indexQueryAll2
    )

    val indexQueries = ExpressionUtils.extractIndexQueries(nestedExpr)

    assert(indexQueries.length == 2)
    assert(indexQueries.contains(indexQueryAll1))
    assert(indexQueries.contains(indexQueryAll2))
  }

  test("round-trip conversion should preserve query string") {
    val originalQueryString = "complex AND (query OR pattern)"
    val filter              = IndexQueryAllFilter(originalQueryString)
    val expr                = ExpressionUtils.filterToIndexQueryAllExpression(filter)
    val convertedFilter     = ExpressionUtils.expressionToIndexQueryAllFilter(expr)

    assert(convertedFilter.isDefined)
    assert(convertedFilter.get.queryString == originalQueryString)
  }
}
