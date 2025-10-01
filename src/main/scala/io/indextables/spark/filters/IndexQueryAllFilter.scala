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

package io.indextables.spark.filters

/**
 * Custom filter for IndexQueryAll pushdown operations.
 *
 * This filter represents a query that should search across all fields in a Tantivy index without requiring explicit
 * column specification. It's designed to be pushed down to the native Tantivy search engine for optimal performance.
 *
 * Note: This does not extend Spark's sealed Filter class to avoid inheritance restrictions. Instead, it provides the
 * necessary methods for integration with the pushdown system.
 *
 * @param queryString
 *   The Tantivy query string to search across all fields
 */
case class IndexQueryAllFilter(queryString: String) {

  /**
   * Returns an empty array since this filter doesn't reference specific columns. The search operates across all
   * available fields in the index.
   *
   * @return
   *   Empty array indicating no specific column references
   */
  def references: Array[String] = Array.empty

  /**
   * Validates that this filter can be used for pushdown.
   *
   * @return
   *   true if the query string is non-empty and valid for pushdown
   */
  def isValid: Boolean = queryString.nonEmpty

  /**
   * Provides a string representation for debugging and logging.
   *
   * @return
   *   String representation of this filter
   */
  override def toString: String = s"IndexQueryAllFilter($queryString)"

  /**
   * Checks if this filter is compatible with another filter for combining.
   *
   * @param other
   *   Another IndexQueryAllFilter to check compatibility with
   * @return
   *   true if the filters can be logically combined
   */
  def isCompatibleWith(other: IndexQueryAllFilter): Boolean =
    // All IndexQueryAllFilters are compatible since they all search all fields
    true

  /**
   * Returns the query string for native execution.
   *
   * @return
   *   The raw query string to be passed to SplitIndex.parseQuery()
   */
  def getQueryString: String = queryString

  /**
   * Checks if the query string contains special characters that need handling.
   *
   * @return
   *   true if the query contains special Tantivy syntax
   */
  def hasSpecialSyntax: Boolean = {
    val specialChars = Set('(', ')', '"', '*', '?', ':')
    queryString.exists(c => specialChars.contains(c)) ||
    queryString.contains("AND") || queryString.contains("OR") || queryString.contains("NOT")
  }

  /**
   * Provides metadata about this filter for optimization purposes.
   *
   * @return
   *   Map containing filter metadata
   */
  def metadata: Map[String, Any] = Map(
    "type"             -> "IndexQueryAllFilter",
    "queryString"      -> queryString,
    "hasSpecialSyntax" -> hasSpecialSyntax,
    "queryLength"      -> queryString.length,
    "fieldCount"       -> "all"
  )
}
