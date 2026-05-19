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

package io.indextables.spark.expressions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Compile-time tests for the [[SearchType]] sub-trait hierarchy.
 *
 * The subset invariant that single-field expressions/filters must not accept the `IndexQueryAll` mode is enforced at
 * compile time via the [[SingleFieldSearchType]] and [[AllFieldSearchType]] sub-traits. These tests pin that contract:
 * valid combinations must compile, and invalid combinations must fail to compile.
 *
 * If any of these assertions stops behaving as written, the type-safety guarantee has regressed and needs to be
 * restored (or the test updated to match new intent).
 */
class SearchTypeTypeSafetyTest extends AnyFunSuite with Matchers {

  // ----------------------------------------------------------------------------
  // Positive cases: valid combinations compile cleanly
  // ----------------------------------------------------------------------------

  test("SingleFieldSearchType ascription accepts IndexQuery, TextSearch, FieldMatch") {
    """
      import io.indextables.spark.expressions.{SearchType, SingleFieldSearchType}
      val a: SingleFieldSearchType = SearchType.IndexQuery
      val b: SingleFieldSearchType = SearchType.TextSearch
      val c: SingleFieldSearchType = SearchType.FieldMatch
    """ should compile
  }

  test("AllFieldSearchType ascription accepts all four search types") {
    """
      import io.indextables.spark.expressions.{SearchType, AllFieldSearchType}
      val a: AllFieldSearchType = SearchType.IndexQuery
      val b: AllFieldSearchType = SearchType.IndexQueryAll
      val c: AllFieldSearchType = SearchType.TextSearch
      val d: AllFieldSearchType = SearchType.FieldMatch
    """ should compile
  }

  test("IndexQueryFilter accepts SingleFieldSearchType case objects (IndexQuery, TextSearch, FieldMatch)") {
    """
      import io.indextables.spark.expressions.SearchType
      import io.indextables.spark.filters.IndexQueryFilter
      IndexQueryFilter("col", "q", SearchType.IndexQuery)
      IndexQueryFilter("col", "q", SearchType.TextSearch)
      IndexQueryFilter("col", "q", SearchType.FieldMatch)
    """ should compile
  }

  test("IndexQueryAllFilter accepts all four SearchType case objects") {
    """
      import io.indextables.spark.expressions.SearchType
      import io.indextables.spark.filters.IndexQueryAllFilter
      IndexQueryAllFilter("q", SearchType.IndexQuery)
      IndexQueryAllFilter("q", SearchType.IndexQueryAll)
      IndexQueryAllFilter("q", SearchType.TextSearch)
      IndexQueryAllFilter("q", SearchType.FieldMatch)
    """ should compile
  }

  // ----------------------------------------------------------------------------
  // Negative cases: the type system blocks the nonsensical combinations
  // ----------------------------------------------------------------------------

  test("SingleFieldSearchType ascription rejects IndexQueryAll") {
    """
      import io.indextables.spark.expressions.{SearchType, SingleFieldSearchType}
      val bad: SingleFieldSearchType = SearchType.IndexQueryAll
    """ shouldNot compile
  }

  test("IndexQueryFilter rejects IndexQueryAll at the call site") {
    """
      import io.indextables.spark.expressions.SearchType
      import io.indextables.spark.filters.IndexQueryFilter
      IndexQueryFilter("col", "q", SearchType.IndexQueryAll)
    """ shouldNot compile
  }

  test("IndexQueryExpression rejects IndexQueryAll at the call site") {
    """
      import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
      import org.apache.spark.sql.types.StringType
      import org.apache.spark.unsafe.types.UTF8String
      import io.indextables.spark.expressions.{IndexQueryExpression, SearchType}
      val column = AttributeReference("col", StringType, nullable = true)()
      val query = Literal(UTF8String.fromString("q"), StringType)
      IndexQueryExpression(column, query, SearchType.IndexQueryAll)
    """ shouldNot compile
  }
}
