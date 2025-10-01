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

package com.tantivy4spark.filters

import org.scalatest.funsuite.AnyFunSuite

class IndexQueryFilterTest extends AnyFunSuite {

  test("IndexQueryFilter should store column and query string correctly") {
    val filter = IndexQueryFilter("title", "machine learning AND spark")

    assert(filter.columnName == "title")
    assert(filter.queryString == "machine learning AND spark")
    assert(filter.isValid)
  }

  test("IndexQueryFilter should return correct references") {
    val filter = IndexQueryFilter("content", "apache OR hadoop")

    val refs = filter.references
    assert(refs.length == 1)
    assert(refs.head == "content")
  }

  test("IndexQueryFilter should have meaningful toString") {
    val filter = IndexQueryFilter("description", "search query")

    val str = filter.toString
    assert(str.contains("IndexQuery"))
    assert(str.contains("description"))
    assert(str.contains("search query"))
    assert(str == "IndexQuery(description, 'search query')")
  }

  test("IndexQueryFilter should validate correctly with valid inputs") {
    val validFilter = IndexQueryFilter("title", "spark AND sql")
    assert(validFilter.isValid)
  }

  test("IndexQueryFilter should be invalid with empty column") {
    val invalidFilter = IndexQueryFilter("", "valid query")
    assert(!invalidFilter.isValid)
  }

  test("IndexQueryFilter should be invalid with empty query") {
    val invalidFilter = IndexQueryFilter("title", "")
    assert(!invalidFilter.isValid)
  }

  test("IndexQueryFilter should be invalid with both empty") {
    val invalidFilter = IndexQueryFilter("", "")
    assert(!invalidFilter.isValid)
  }

  test("IndexQueryFilter should normalize query strings") {
    val filterWithWhitespace = IndexQueryFilter("title", "  spark AND sql  ")

    assert(filterWithWhitespace.normalizedQuery == "spark AND sql")
    // Original query string should remain unchanged
    assert(filterWithWhitespace.queryString == "  spark AND sql  ")
  }

  test("IndexQueryFilter should handle complex query strings") {
    val complexQuery = "(machine AND learning) OR (deep AND neural) NOT deprecated"
    val filter       = IndexQueryFilter("content", complexQuery)

    assert(filter.columnName == "content")
    assert(filter.queryString == complexQuery)
    assert(filter.normalizedQuery == complexQuery) // No extra whitespace
    assert(filter.isValid)
  }

  test("IndexQueryFilter should handle special characters in queries") {
    val queryWithSpecialChars = "title:\"Apache Spark\" AND author:smith*"
    val filter                = IndexQueryFilter("metadata", queryWithSpecialChars)

    assert(filter.queryString == queryWithSpecialChars)
    assert(filter.isValid)
  }

  test("IndexQueryFilter should handle wildcard queries") {
    val wildcardQuery = "prefix* AND *suffix AND *middle*"
    val filter        = IndexQueryFilter("text", wildcardQuery)

    assert(filter.queryString == wildcardQuery)
    assert(filter.isValid)
  }

  test("IndexQueryFilter should handle phrase queries") {
    val phraseQuery = "\"exact phrase match\" AND single_term"
    val filter      = IndexQueryFilter("content", phraseQuery)

    assert(filter.queryString == phraseQuery)
    assert(filter.isValid)
  }

  test("IndexQueryFilter should handle range queries") {
    val rangeQuery = "price:[100 TO 500] AND date:[2023-01-01 TO 2023-12-31]"
    val filter     = IndexQueryFilter("metadata", rangeQuery)

    assert(filter.queryString == rangeQuery)
    assert(filter.isValid)
  }

  test("IndexQueryFilter should be equal when content is equal") {
    val filter1 = IndexQueryFilter("title", "spark AND sql")
    val filter2 = IndexQueryFilter("title", "spark AND sql")
    val filter3 = IndexQueryFilter("content", "spark AND sql")
    val filter4 = IndexQueryFilter("title", "different query")

    assert(filter1 == filter2)
    assert(filter1 != filter3)
    assert(filter1 != filter4)
  }

  test("IndexQueryFilter should have consistent hashCode") {
    val filter1 = IndexQueryFilter("title", "spark AND sql")
    val filter2 = IndexQueryFilter("title", "spark AND sql")

    assert(filter1.hashCode == filter2.hashCode)
  }
}
