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

package io.indextables.spark.exceptions

/**
 * Exception thrown when an IndexQuery string fails to parse.
 *
 * This exception is thrown when the tantivy query parser cannot parse the provided query string due to syntax errors
 * such as:
 *   - Unbalanced parentheses: `((machine learning`
 *   - Unclosed quotes: `"unclosed phrase`
 *   - Invalid boolean operators: `apple AND AND orange`
 *   - Empty field references: `field:`
 *   - Other malformed query syntax
 *
 * @param queryString
 *   The query string that failed to parse
 * @param fieldName
 *   Optional field name if this was a field-specific query
 * @param cause
 *   The underlying parsing exception from tantivy4java
 */
class IndexQueryParseException(
  val queryString: String,
  val fieldName: Option[String],
  cause: Throwable)
    extends RuntimeException(
      IndexQueryParseException.formatMessage(queryString, fieldName, cause),
      cause
    )

object IndexQueryParseException {

  /** Format a user-friendly error message for the parse exception. */
  def formatMessage(
    queryString: String,
    fieldName: Option[String],
    cause: Throwable
  ): String = {
    val fieldPart      = fieldName.map(f => s" on field '$f'").getOrElse("")
    val truncatedQuery = if (queryString.length > 100) queryString.take(100) + "..." else queryString
    val causeMessage   = Option(cause.getMessage).getOrElse("Unknown parse error")
    s"Invalid IndexQuery syntax$fieldPart: '$truncatedQuery'. Parse error: $causeMessage"
  }

  /** Create an IndexQueryParseException for a field-specific query. */
  def forField(
    queryString: String,
    fieldName: String,
    cause: Throwable
  ): IndexQueryParseException =
    new IndexQueryParseException(queryString, Some(fieldName), cause)

  /** Create an IndexQueryParseException for an all-fields query. */
  def forAllFields(queryString: String, cause: Throwable): IndexQueryParseException =
    new IndexQueryParseException(queryString, None, cause)
}
