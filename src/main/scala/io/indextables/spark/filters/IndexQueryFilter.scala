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
 * Custom filter representing an indexquery operation for pushdown to Tantivy data source.
 *
 * Since Filter is sealed in Spark, we create this as a wrapper that contains filter information. The actual filter
 * pushdown logic will pattern match on this type.
 *
 * This filter encapsulates:
 *   - The column name to query against
 *   - The raw Tantivy query string to execute
 *
 * Usage: IndexQueryFilter("title", "machine learning AND (spark OR hadoop)")
 *
 * The query string follows Tantivy's query syntax:
 *   - Boolean operations: AND, OR, NOT
 *   - Phrase queries: "exact phrase"
 *   - Wildcard queries: prefix*, *suffix, *middle*
 *   - Field-specific queries: field:value
 *   - Range queries: field:[min TO max]
 */
case class IndexQueryFilter(
  columnName: String,
  queryString: String) {

  override def toString: String = s"IndexQuery($columnName, '$queryString')"

  /** References returns the set of column names that this filter references. */
  def references: Array[String] = Array(columnName)

  /** Check if this filter is valid (non-empty column and query). */
  def isValid: Boolean =
    columnName.nonEmpty && queryString.nonEmpty

  /** Get a normalized version of the query string (trimmed). */
  def normalizedQuery: String = queryString.trim
}

/**
 * Sealed trait for mixed boolean filter trees that can contain both
 * IndexQuery filters and regular Spark predicates with preserved boolean structure.
 *
 * This enables queries like:
 *   (urgency = 'CRITICAL' AND url indexquery 'one') OR (urgency <> 'CLASS2' AND url indexquery 'two')
 *
 * To be correctly converted to Tantivy boolean queries preserving OR/AND semantics.
 */
sealed trait MixedBooleanFilter {
  def references(): Array[String]
}

/** Wrapper for a simple IndexQuery filter within a boolean tree. */
case class MixedIndexQuery(filter: IndexQueryFilter) extends MixedBooleanFilter {
  override def references(): Array[String] = filter.references
}

/** Wrapper for a simple IndexQueryAll filter within a boolean tree. */
case class MixedIndexQueryAll(filter: IndexQueryAllFilter) extends MixedBooleanFilter {
  override def references(): Array[String] = Array.empty
}

/** Wrapper for a Spark Filter (EqualTo, GreaterThan, etc.) within a boolean tree. */
case class MixedSparkFilter(filter: org.apache.spark.sql.sources.Filter) extends MixedBooleanFilter {
  override def references(): Array[String] = MixedSparkFilter.extractReferences(filter)
}

object MixedSparkFilter {
  def extractReferences(f: org.apache.spark.sql.sources.Filter): Array[String] = {
    import org.apache.spark.sql.sources._
    f match {
      case EqualTo(attr, _)            => Array(attr)
      case EqualNullSafe(attr, _)      => Array(attr)
      case GreaterThan(attr, _)        => Array(attr)
      case GreaterThanOrEqual(attr, _) => Array(attr)
      case LessThan(attr, _)           => Array(attr)
      case LessThanOrEqual(attr, _)    => Array(attr)
      case In(attr, _)                 => Array(attr)
      case IsNull(attr)                => Array(attr)
      case IsNotNull(attr)             => Array(attr)
      case StringStartsWith(attr, _)   => Array(attr)
      case StringEndsWith(attr, _)     => Array(attr)
      case StringContains(attr, _)     => Array(attr)
      case And(l, r)                   => extractReferences(l) ++ extractReferences(r)
      case Or(l, r)                    => extractReferences(l) ++ extractReferences(r)
      case Not(c)                      => extractReferences(c)
      case _                           => Array.empty
    }
  }
}

/** OR combination of mixed filters - uses SHOULD in Tantivy. */
case class MixedOrFilter(left: MixedBooleanFilter, right: MixedBooleanFilter) extends MixedBooleanFilter {
  override def references(): Array[String] = left.references() ++ right.references()
}

/** AND combination of mixed filters - uses MUST in Tantivy. */
case class MixedAndFilter(left: MixedBooleanFilter, right: MixedBooleanFilter) extends MixedBooleanFilter {
  override def references(): Array[String] = left.references() ++ right.references()
}

/** NOT of a mixed filter - uses MUST_NOT in Tantivy. */
case class MixedNotFilter(child: MixedBooleanFilter) extends MixedBooleanFilter {
  override def references(): Array[String] = child.references()
}
