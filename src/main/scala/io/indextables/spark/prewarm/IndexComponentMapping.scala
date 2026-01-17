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

package io.indextables.spark.prewarm

import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent

/**
 * Mapping utilities for index component aliases used in SQL PREWARM commands.
 *
 * SQL aliases are user-friendly names that map to tantivy4java IndexComponent enum values:
 *
 * | SQL Alias                  | tantivy4java IndexComponent |
 * |:---------------------------|:----------------------------|
 * | TERM_DICT, TERM_DICTIONARY | TERM                        |
 * | FAST_FIELD, FASTFIELD      | FASTFIELD                   |
 * | POSTINGS, POSTING_LISTS    | POSTINGS                    |
 * | POSITIONS, POSITION_LISTS  | POSITIONS                   |
 * | FIELD_NORM, FIELDNORM      | FIELDNORM                   |
 * | DOC_STORE, STORE           | STORE                       |
 *
 * Default components (when no segments specified): TERM, POSTINGS Note: DOC_STORE, FASTFIELD, and FIELDNORM are
 * excluded from defaults as they can be expensive and are not required for basic query operations. Add them explicitly
 * if needed for aggregations.
 */
object IndexComponentMapping {

  /**
   * Mapping from SQL alias (uppercase) to tantivy4java IndexComponent. Supports multiple aliases for each component for
   * user convenience.
   */
  val aliasToComponent: Map[String, IndexComponent] = Map(
    // Term dictionary (FST) aliases
    "TERM"            -> IndexComponent.TERM,
    "TERM_DICT"       -> IndexComponent.TERM,
    "TERM_DICTIONARY" -> IndexComponent.TERM,
    // Fast field aliases
    "FASTFIELD"  -> IndexComponent.FASTFIELD,
    "FAST_FIELD" -> IndexComponent.FASTFIELD,
    // Postings (inverted index) aliases
    "POSTINGS"      -> IndexComponent.POSTINGS,
    "POSTING_LISTS" -> IndexComponent.POSTINGS,
    // Positions (term positions within documents) aliases
    "POSITIONS"      -> IndexComponent.POSITIONS,
    "POSITION_LISTS" -> IndexComponent.POSITIONS,
    // Field norm aliases
    "FIELDNORM"  -> IndexComponent.FIELDNORM,
    "FIELD_NORM" -> IndexComponent.FIELDNORM,
    // Document store aliases
    "STORE"     -> IndexComponent.STORE,
    "DOC_STORE" -> IndexComponent.STORE
  )

  /**
   * Default components to prewarm when no segments are specified. Includes TERM and POSTINGS which are essential for
   * query operations. Add POSITIONS, FASTFIELD, FIELDNORM, or STORE explicitly if needed.
   */
  val defaultComponents: Set[IndexComponent] = Set(
    IndexComponent.TERM,
    IndexComponent.POSTINGS
  )

  /** All available components. */
  val allComponents: Set[IndexComponent] = Set(
    IndexComponent.TERM,
    IndexComponent.FASTFIELD,
    IndexComponent.POSTINGS,
    IndexComponent.POSITIONS,
    IndexComponent.FIELDNORM,
    IndexComponent.STORE
  )

  /**
   * Parse a comma-separated string of segment aliases into a set of IndexComponents.
   *
   * @param segmentString
   *   Comma-separated segment aliases (e.g., "TERM_DICT,FAST_FIELD,POSTINGS")
   * @return
   *   Set of resolved IndexComponent values, or defaultComponents if input is empty
   * @throws IllegalArgumentException
   *   if an unknown segment alias is encountered
   */
  def parseSegments(segmentString: String): Set[IndexComponent] = {
    val trimmed = segmentString.trim
    if (trimmed.isEmpty) {
      defaultComponents
    } else {
      trimmed
        .split(",")
        .map(_.trim.toUpperCase)
        .filter(_.nonEmpty)
        .map { alias =>
          aliasToComponent.getOrElse(
            alias,
            throw new IllegalArgumentException(
              s"Unknown segment type: $alias. Valid types: ${aliasToComponent.keys.toSeq.sorted.mkString(", ")}"
            )
          )
        }
        .toSet
    }
  }

  /**
   * Parse an optional segment string, returning None for defaults.
   *
   * @param segmentStringOpt
   *   Optional segment string
   * @return
   *   Some(Set[IndexComponent]) if specified, or None to use defaults
   */
  def parseSegmentsOption(segmentStringOpt: Option[String]): Option[Set[IndexComponent]] =
    segmentStringOpt.filter(_.trim.nonEmpty).map(parseSegments)

  /** Get the canonical name for a component (for display). */
  def canonicalName(component: IndexComponent): String = component match {
    case IndexComponent.TERM      => "TERM_DICT"
    case IndexComponent.FASTFIELD => "FAST_FIELD"
    case IndexComponent.POSTINGS  => "POSTINGS"
    case IndexComponent.POSITIONS => "POSITIONS"
    case IndexComponent.FIELDNORM => "FIELD_NORM"
    case IndexComponent.STORE     => "DOC_STORE"
    case other                    => other.name()
  }

  /** Convert a set of components to their canonical display names. */
  def toCanonicalNames(components: Set[IndexComponent]): Seq[String] =
    components.map(canonicalName).toSeq.sorted
}
