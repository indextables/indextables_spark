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
 * Implemented as a sealed trait hierarchy with case objects so that:
 *   - Pattern matches on search types can be proven exhaustive at compile time. Adding a new search type causes the
 *     compiler to flag every non-exhaustive match site.
 *   - Construction sites are type-checked, eliminating the runtime `require()` guards that previously validated raw
 *     string inputs.
 *   - The subset invariant (single-field expressions accept only single-field types; all-fields expressions accept
 *     all-fields types) is encoded in the type system via [[SingleFieldSearchType]] and [[AllFieldSearchType]]
 *     sub-traits. No runtime check needed; no nonsensical case branches in pattern matches.
 *   - IDE "find all usages" works per case object instead of conflating literal string occurrences.
 *
 * The `value` field preserves the wire-format string for serialization, SQL display, and external integrations.
 */
sealed trait SearchType {
  def value: String
}

/** Search types valid for single-field expressions and filters (`IndexQueryExpression`, `IndexQueryFilter`). */
sealed trait SingleFieldSearchType extends SearchType

/** Search types valid for all-fields expressions and filters (`IndexQueryAllExpression`, `IndexQueryAllFilter`). */
sealed trait AllFieldSearchType extends SearchType

object SearchType {
  case object IndexQuery extends SingleFieldSearchType with AllFieldSearchType {
    override val value: String = "indexquery"
  }
  case object IndexQueryAll extends AllFieldSearchType {
    override val value: String = "indexqueryall"
  }
  case object TextSearch extends SingleFieldSearchType with AllFieldSearchType {
    override val value: String = "textsearch"
  }
  case object FieldMatch extends SingleFieldSearchType with AllFieldSearchType {
    override val value: String = "fieldmatch"
  }

  /**
   * Parse a raw string (typically from the SQL parser or external input) into a typed [[SearchType]].
   *
   * @throws IllegalArgumentException
   *   if the input does not match any known search type
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
