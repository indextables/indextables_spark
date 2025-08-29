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

import org.apache.spark.sql.sources.Filter

/**
 * Custom filter representing an indexquery operation for pushdown to Tantivy data source.
 * 
 * Since Filter is sealed in Spark, we create this as a wrapper that contains filter information.
 * The actual filter pushdown logic will pattern match on this type.
 * 
 * This filter encapsulates:
 * - The column name to query against
 * - The raw Tantivy query string to execute
 * 
 * Usage: IndexQueryFilter("title", "machine learning AND (spark OR hadoop)")
 * 
 * The query string follows Tantivy's query syntax:
 * - Boolean operations: AND, OR, NOT
 * - Phrase queries: "exact phrase"
 * - Wildcard queries: prefix*, *suffix, *middle*
 * - Field-specific queries: field:value
 * - Range queries: field:[min TO max]
 */
case class IndexQueryFilter(
    column: String,
    queryString: String
) {
  
  override def toString: String = s"IndexQuery($column, '$queryString')"
  
  /**
   * References returns the set of column names that this filter references.
   */
  def references: Array[String] = Array(column)
  
  /**
   * Check if this filter is valid (non-empty column and query).
   */
  def isValid: Boolean = {
    column.nonEmpty && queryString.nonEmpty
  }
  
  /**
   * Get a normalized version of the query string (trimmed).
   */
  def normalizedQuery: String = queryString.trim
}