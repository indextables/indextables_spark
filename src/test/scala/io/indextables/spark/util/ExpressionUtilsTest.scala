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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.expressions.IndexQueryExpression
import io.indextables.spark.filters.IndexQueryFilter
import org.scalatest.funsuite.AnyFunSuite

class ExpressionUtilsTest extends AnyFunSuite {

  test("expressionToIndexQueryFilter should convert valid IndexQueryExpression") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("spark AND sql"), StringType)
    val expr   = IndexQueryExpression(column, query)

    val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)

    assert(filter.isDefined)
    val indexQueryFilter = filter.get
    assert(indexQueryFilter.columnName == "title")
    assert(indexQueryFilter.queryString == "spark AND sql")
  }

  test("expressionToIndexQueryFilter should handle UnresolvedAttribute") {
    val column = UnresolvedAttribute(Seq("content", "text"))
    val query  = Literal(UTF8String.fromString("machine learning"), StringType)
    val expr   = IndexQueryExpression(column, query)

    val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)

    assert(filter.isDefined)
    val indexQueryFilter = filter.get
    assert(indexQueryFilter.columnName == "content.text")
    assert(indexQueryFilter.queryString == "machine learning")
  }

  test("expressionToIndexQueryFilter should handle UTF8String literals") {
    val column = AttributeReference("description", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("apache spark"), StringType)
    val expr   = IndexQueryExpression(column, query)

    val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)

    assert(filter.isDefined)
    val indexQueryFilter = filter.get.asInstanceOf[IndexQueryFilter]
    assert(indexQueryFilter.columnName == "description")
    assert(indexQueryFilter.queryString == "apache spark")
  }

  test("expressionToIndexQueryFilter should return None for invalid expressions") {
    val nonIndexQueryExpr = Add(Literal(1), Literal(2))

    val filter = ExpressionUtils.expressionToIndexQueryFilter(nonIndexQueryExpr)
    assert(filter.isEmpty)
  }

  test("expressionToIndexQueryFilter should return None for IndexQueryExpression with invalid parts") {
    val invalidColumn = Literal(UTF8String.fromString("not_a_column"), StringType)
    val validQuery    = Literal(UTF8String.fromString("test query"), StringType)
    val expr          = IndexQueryExpression(invalidColumn, validQuery)

    val filter = ExpressionUtils.expressionToIndexQueryFilter(expr)
    assert(filter.isEmpty)
  }

  test("filterToExpression should convert IndexQueryFilter correctly") {
    val filter = IndexQueryFilter("title", "spark AND sql")
    val expr   = ExpressionUtils.filterToExpression(filter)

    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.contains("spark AND sql"))
  }

  test("extractColumnName should handle AttributeReference") {
    val attr       = AttributeReference("title", StringType, nullable = true)()
    val columnName = ExpressionUtils.extractColumnName(attr)

    assert(columnName.contains("title"))
  }

  test("extractColumnName should handle UnresolvedAttribute with single part") {
    val unresolved = UnresolvedAttribute(Seq("content"))
    val columnName = ExpressionUtils.extractColumnName(unresolved)

    assert(columnName.contains("content"))
  }

  test("extractColumnName should handle UnresolvedAttribute with multiple parts") {
    val unresolved = UnresolvedAttribute(Seq("metadata", "author", "name"))
    val columnName = ExpressionUtils.extractColumnName(unresolved)

    assert(columnName.contains("metadata.author.name"))
  }

  test("extractColumnName should return None for invalid expressions") {
    val literal    = Literal(UTF8String.fromString("not_a_column"), StringType)
    val columnName = ExpressionUtils.extractColumnName(literal)

    assert(columnName.isEmpty)
  }

  test("extractStringLiteral should handle String literals") {
    val literal     = Literal(UTF8String.fromString("test query"), StringType)
    val queryString = ExpressionUtils.extractStringLiteral(literal)

    assert(queryString.contains("test query"))
  }

  test("extractStringLiteral should handle UTF8String literals") {
    val literal     = Literal(UTF8String.fromString("utf8 query"), StringType)
    val queryString = ExpressionUtils.extractStringLiteral(literal)

    assert(queryString.contains("utf8 query"))
  }

  test("extractStringLiteral should return None for non-string literals") {
    val intLiteral  = Literal(123)
    val queryString = ExpressionUtils.extractStringLiteral(intLiteral)

    assert(queryString.isEmpty)
  }

  test("isValidIndexQuery should return true for valid IndexQueryExpression") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("valid query"), StringType)
    val expr   = IndexQueryExpression(column, query)

    assert(ExpressionUtils.isValidIndexQuery(expr))
  }

  test("isValidIndexQuery should return false for invalid IndexQueryExpression") {
    val invalidColumn = Literal(UTF8String.fromString("not_a_column"), StringType)
    val query         = Literal(UTF8String.fromString("valid query"), StringType)
    val expr          = IndexQueryExpression(invalidColumn, query)

    assert(!ExpressionUtils.isValidIndexQuery(expr))
  }

  test("isValidIndexQuery should return false for non-IndexQueryExpression") {
    val nonIndexQueryExpr = Add(Literal(1), Literal(2))

    assert(!ExpressionUtils.isValidIndexQuery(nonIndexQueryExpr))
  }

  test("extractIndexQueries should find IndexQueryExpression in simple expression") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("test query"), StringType)
    val expr   = IndexQueryExpression(column, query)

    val indexQueries = ExpressionUtils.extractIndexQueries(expr)

    assert(indexQueries.length == 1)
    assert(indexQueries.head == expr)
  }

  test("extractIndexQueries should find IndexQueryExpression in And expression") {
    val indexQuery = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("test query"), StringType)
    )
    val otherExpr = EqualTo(AttributeReference("status", StringType, nullable = true)(), Literal("active"))
    val andExpr   = And(indexQuery, otherExpr)

    val indexQueries = ExpressionUtils.extractIndexQueries(andExpr)

    assert(indexQueries.length == 1)
    assert(indexQueries.head == indexQuery)
  }

  test("extractIndexQueries should find multiple IndexQueryExpressions") {
    val indexQuery1 = IndexQueryExpression(
      AttributeReference("title", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query1"), StringType)
    )
    val indexQuery2 = IndexQueryExpression(
      AttributeReference("content", StringType, nullable = true)(),
      Literal(UTF8String.fromString("query2"), StringType)
    )
    val andExpr = And(indexQuery1, indexQuery2)

    val indexQueries = ExpressionUtils.extractIndexQueries(andExpr)

    assert(indexQueries.length == 2)
    assert(indexQueries.contains(indexQuery1))
    assert(indexQueries.contains(indexQuery2))
  }

  test("extractIndexQueries should return empty for expressions without IndexQuery") {
    val simpleExpr = EqualTo(AttributeReference("status", StringType, nullable = true)(), Literal("active"))

    val indexQueries = ExpressionUtils.extractIndexQueries(simpleExpr)
    assert(indexQueries.isEmpty)
  }

  test("validateIndexQueryExpression should succeed for valid expression") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query  = Literal(UTF8String.fromString("valid query"), StringType)
    val expr   = IndexQueryExpression(column, query)

    val validation = ExpressionUtils.validateIndexQueryExpression(expr)
    assert(validation.isRight)
  }

  test("validateIndexQueryExpression should fail for invalid column") {
    val invalidColumn = Literal(UTF8String.fromString("not_a_column"), StringType)
    val query         = Literal(UTF8String.fromString("valid query"), StringType)
    val expr          = IndexQueryExpression(invalidColumn, query)

    val validation = ExpressionUtils.validateIndexQueryExpression(expr)
    assert(validation.isLeft)
    assert(validation.left.get.contains("Invalid column reference"))
  }

  test("validateIndexQueryExpression should fail for empty query") {
    val column     = AttributeReference("title", StringType, nullable = true)()
    val emptyQuery = Literal(UTF8String.fromString(""), StringType)
    val expr       = IndexQueryExpression(column, emptyQuery)

    val validation = ExpressionUtils.validateIndexQueryExpression(expr)
    assert(validation.isLeft)
    assert(validation.left.get.contains("Query string cannot be empty"))
  }

  test("validateIndexQueryExpression should fail for invalid query") {
    val column       = AttributeReference("title", StringType, nullable = true)()
    val invalidQuery = Add(Literal(1), Literal(2))
    val expr         = IndexQueryExpression(column, invalidQuery)

    val validation = ExpressionUtils.validateIndexQueryExpression(expr)
    assert(validation.isLeft)
    assert(validation.left.get.contains("Invalid query string"))
  }
}
