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

/**
 * Type identifier for search expressions and filters.
 *
 * Implemented as a sealed trait with case objects so that:
 *   - Pattern matches on search types can be proven exhaustive at compile time — adding a new search type causes the
 *     compiler to flag every non-exhaustive match site.
 *   - Construction sites are type-checked, eliminating the runtime `require()` guards that previously validated raw
 *     string inputs.
 *   - IDE "find all usages" works per case object instead of conflating literal string occurrences.
 *
 * The `value` field preserves the wire-format string for serialization, SQL display, and external integrations.
 */
sealed trait SearchType {
  def value: String
}

object SearchType {
  case object IndexQuery extends SearchType { val value = "indexquery" }
  case object IndexQueryAll extends SearchType { val value = "indexqueryall" }
  case object TextSearch extends SearchType { val value = "textsearch" }
  case object FieldMatch extends SearchType { val value = "fieldmatch" }

  /**
   * Parse a raw string (typically from the SQL parser or external input) into a typed [[SearchType]].
   *
   * @throws IllegalArgumentException if the input does not match any known search type
   */
  def fromString(s: String): SearchType = s.toLowerCase match {
    case "indexquery"    => IndexQuery
    case "indexqueryall" => IndexQueryAll
    case "textsearch"    => TextSearch
    case "fieldmatch"    => FieldMatch
    case other =>
      throw new IllegalArgumentException(
        s"Unknown search type '$other'. Must be one of: indexquery, indexqueryall, textsearch, fieldmatch"
      )
  }
}
