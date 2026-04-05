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
 * Constants for search type identifiers used by IndexQuery expressions and filters.
 *
 * These constants eliminate magic strings and enable `require()` validation in constructors to catch
 * typos at construction time rather than silently bypassing type validation.
 */
object SearchType {
  val IndexQuery    = "indexquery"
  val IndexQueryAll = "indexqueryall"
  val TextSearch    = "textsearch"
  val FieldMatch    = "fieldmatch"

  /** Valid search types for single-field expressions (IndexQueryExpression, IndexQueryFilter). */
  val validSingleField: Set[String] = Set(IndexQuery, TextSearch, FieldMatch)

  /** Valid search types for all-fields expressions (IndexQueryAllExpression, IndexQueryAllFilter).
    * IndexQuery is included because `* indexquery` via parseExpression creates
    * IndexQueryAllExpression with SearchType.IndexQuery (the preprocessor path uses IndexQueryAll). */
  val validAllFields: Set[String] = Set(IndexQuery, IndexQueryAll, TextSearch, FieldMatch)
}
