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

package io.indextables.spark.json

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import io.indextables.tantivy4java.core.Schema
import io.indextables.tantivy4java.query.{Occur, Query}
import org.slf4j.LoggerFactory

/**
 * Translates Spark Catalyst filters to tantivy4java JSON queries.
 *
 * Handles translation of:
 *   - Nested field predicates (e.g., $"user.name" === "Alice")
 *   - Array contains operations
 *   - Range queries on nested fields
 *   - Existence checks (IsNull/IsNotNull)
 *   - Boolean combinations (And/Or/Not)
 */
class JsonPredicateTranslator(
  sparkSchema: StructType,
  schemaMapper: SparkSchemaToTantivyMapper
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Translates a Spark filter to a parseQuery string for use with SplitSearcher.parseQuery().
   *
   * This method generates Tantivy query syntax strings for JSON field queries in splits.
   * Examples:
   *   - Term query: "data.name:\"Alice\""
   *   - Range query: "data.age:[25 TO 30]"
   *   - Boolean query: "data.name:\"Alice\" AND data.age:>25"
   *
   * @param filter Spark Filter to translate
   * @return Some(queryString) if filter can be translated to parseQuery syntax, None otherwise
   */
  def translateFilterToParseQuery(filter: Filter): Option[String] = {
    logger.debug(s"JsonPredicateTranslator.translateFilterToParseQuery called with: $filter")
    filter match {
      // Nested field equality: $"user.name" === "Alice" => data.name:"Alice"
      case EqualTo(attr, value) if isNestedAttribute(attr) =>
        logger.debug(s"JsonPredicateTranslator: Detected nested attribute: $attr")
        val fullPath = attr.replace(".", ".")  // Already in correct format
        val escapedValue = escapeQueryValue(valueToString(value))
        val queryString = s"""$fullPath:"$escapedValue""""
        logger.debug(s"JsonPredicateTranslator: Translating nested equality to parseQuery: $queryString")
        Some(queryString)

      // Nested field range queries
      case GreaterThan(attr, value) if isNestedAttribute(attr) =>
        val fullPath = attr.replace(".", ".")
        val queryString = s"$fullPath:>$value"
        logger.debug(s"Translating nested GT to parseQuery: $queryString")
        Some(queryString)

      case GreaterThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val fullPath = attr.replace(".", ".")
        val queryString = s"$fullPath:>=$value"
        logger.debug(s"Translating nested GTE to parseQuery: $queryString")
        Some(queryString)

      case LessThan(attr, value) if isNestedAttribute(attr) =>
        val fullPath = attr.replace(".", ".")
        val queryString = s"$fullPath:<$value"
        logger.debug(s"Translating nested LT to parseQuery: $queryString")
        Some(queryString)

      case LessThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val fullPath = attr.replace(".", ".")
        val queryString = s"$fullPath:<=$value"
        logger.debug(s"Translating nested LTE to parseQuery: $queryString")
        Some(queryString)

      // Field existence checks - use wildcard syntax
      case IsNotNull(attr) if isNestedAttribute(attr) =>
        val fullPath = attr.replace(".", ".")
        val queryString = s"$fullPath:*"
        logger.debug(s"Translating nested IsNotNull to parseQuery: $queryString")
        Some(queryString)

      case IsNull(attr) if isNestedAttribute(attr) =>
        // IsNull is NOT of exists query
        val fullPath = attr.replace(".", ".")
        val queryString = s"NOT $fullPath:*"
        logger.debug(s"Translating nested IsNull to parseQuery: $queryString")
        Some(queryString)

      // Array contains operations - search array elements
      case StringContains(attr, substring) if isArrayField(attr) =>
        val fullPath = attr.replace(".", ".")
        val escapedValue = escapeQueryValue(substring)
        val queryString = s"""$fullPath:"$escapedValue""""
        logger.debug(s"Translating array contains to parseQuery: $queryString")
        Some(queryString)

      // Boolean combinations
      case And(left, right) =>
        for {
          leftQuery <- translateFilterToParseQuery(left)
          rightQuery <- translateFilterToParseQuery(right)
        } yield {
          val queryString = s"($leftQuery) AND ($rightQuery)"
          logger.debug(s"Translating AND to parseQuery: $queryString")
          queryString
        }

      case Or(left, right) =>
        for {
          leftQuery <- translateFilterToParseQuery(left)
          rightQuery <- translateFilterToParseQuery(right)
        } yield {
          val queryString = s"($leftQuery) OR ($rightQuery)"
          logger.debug(s"Translating OR to parseQuery: $queryString")
          queryString
        }

      case Not(child) =>
        translateFilterToParseQuery(child).map { childQuery =>
          val queryString = s"NOT ($childQuery)"
          logger.debug(s"Translating NOT to parseQuery: $queryString")
          queryString
        }

      case _ =>
        logger.debug(s"Filter not supported for parseQuery translation: $filter")
        None
    }
  }

  /**
   * Escape special characters in query values for parseQuery syntax.
   *
   * @param value Value to escape
   * @return Escaped value safe for use in parseQuery strings
   */
  private def escapeQueryValue(value: String): String = {
    // Escape double quotes and backslashes
    value.replace("\\", "\\\\").replace("\"", "\\\"")
  }

  /**
   * Translates a Spark filter to a tantivy4java Query.
   *
   * @param filter Spark Filter to translate
   * @param tantivySchema tantivy4java Schema
   * @return Some(Query) if filter can be pushed down, None otherwise
   */
  def translateFilter(filter: Filter, tantivySchema: Schema): Option[Query] = {
    filter match {
      // Nested field equality: $"user.name" === "Alice"
      case EqualTo(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested equality: $fieldName.$path = $value")
        Some(Query.jsonTermQuery(tantivySchema, fieldName, path, valueToString(value)))

      // Nested field range queries
      case GreaterThan(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested GT: $fieldName.$path > $value")
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, numValue, null, false, true))

      case GreaterThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested GTE: $fieldName.$path >= $value")
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, numValue, null, true, true))

      case LessThan(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested LT: $fieldName.$path < $value")
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, null, numValue, true, false))

      case LessThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested LTE: $fieldName.$path <= $value")
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, null, numValue, true, true))

      // Field existence checks
      case IsNotNull(attr) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested IsNotNull: $fieldName.$path")
        Some(Query.jsonExistsQuery(tantivySchema, fieldName, path))

      case IsNull(attr) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        logger.debug(s"Translating nested IsNull: $fieldName.$path")
        val existsQuery = Query.jsonExistsQuery(tantivySchema, fieldName, path)
        val occurQueries = java.util.Arrays.asList(
          new Query.OccurQuery(Occur.MUST_NOT, existsQuery)
        )
        Some(Query.booleanQuery(occurQueries))

      // Array contains operations
      // Note: Requires custom filter type, using string matching for now
      case StringContains(attr, substring) if isArrayField(attr) =>
        logger.debug(s"Translating array contains: $attr contains $substring")
        // Use empty path "" to search array elements directly
        Some(Query.jsonTermQuery(tantivySchema, attr, "", substring))

      // Boolean combinations
      case And(left, right) =>
        logger.debug("Translating AND filter")
        for {
          leftQuery <- translateFilter(left, tantivySchema)
          rightQuery <- translateFilter(right, tantivySchema)
        } yield {
          val occurQueries = java.util.Arrays.asList(
            new Query.OccurQuery(Occur.MUST, leftQuery),
            new Query.OccurQuery(Occur.MUST, rightQuery)
          )
          Query.booleanQuery(occurQueries)
        }

      case Or(left, right) =>
        logger.debug("Translating OR filter")
        for {
          leftQuery <- translateFilter(left, tantivySchema)
          rightQuery <- translateFilter(right, tantivySchema)
        } yield {
          val occurQueries = java.util.Arrays.asList(
            new Query.OccurQuery(Occur.SHOULD, leftQuery),
            new Query.OccurQuery(Occur.SHOULD, rightQuery)
          )
          Query.booleanQuery(occurQueries)
        }

      case Not(child) =>
        logger.debug("Translating NOT filter")
        translateFilter(child, tantivySchema).map { childQuery =>
          val occurQueries = java.util.Arrays.asList(
            new Query.OccurQuery(Occur.MUST_NOT, childQuery)
          )
          Query.booleanQuery(occurQueries)
        }

      case _ =>
        logger.debug(s"Filter not supported for pushdown: $filter")
        None
    }
  }

  /**
   * Checks if an attribute references a nested field (contains dot notation).
   *
   * @param attr Attribute name (e.g., "user.name")
   * @return true if attribute is nested
   */
  private def isNestedAttribute(attr: String): Boolean = {
    if (!attr.contains(".")) {
      false
    } else {
      val rootField = attr.split("\\.")(0)
      val field = sparkSchema.find(_.name == rootField)
      field.exists { f =>
        f.dataType.isInstanceOf[StructType] ||
        f.dataType.isInstanceOf[ArrayType] ||
        f.dataType.isInstanceOf[MapType]
      }
    }
  }

  /**
   * Checks if an attribute references an array field.
   *
   * @param attr Attribute name
   * @return true if field is ArrayType
   */
  private def isArrayField(attr: String): Boolean = {
    val rootField = if (attr.contains(".")) attr.split("\\.")(0) else attr
    sparkSchema.find(_.name == rootField).exists(_.dataType.isInstanceOf[ArrayType])
  }

  /**
   * Splits a nested attribute into field name and JSON path.
   *
   * Example: "user.address.city" -> ("user", "address.city")
   *
   * @param attr Nested attribute
   * @return (fieldName, jsonPath)
   */
  private def splitNestedAttribute(attr: String): (String, String) = {
    val parts = attr.split("\\.", 2)
    if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      (parts(0), "")
    }
  }

  /**
   * Gets the Spark field type for an attribute.
   *
   * @param attr Attribute name
   * @return DataType or null if not found
   */
  private def getFieldType(attr: String): DataType = {
    val rootField = if (attr.contains(".")) attr.split("\\.")(0) else attr
    sparkSchema.find(_.name == rootField).map(_.dataType).orNull
  }

  /**
   * Converts a value to String for term queries.
   *
   * @param value Any value
   * @return String representation
   */
  private def valueToString(value: Any): String = {
    value match {
      case null => ""
      case s: String => s
      case _ => value.toString
    }
  }

  /**
   * Converts a value to Long for range queries.
   *
   * @param value Any numeric value
   * @return Long representation
   */
  private def valueToLong(value: Any): java.lang.Long = {
    value match {
      case null => null
      case i: Int => java.lang.Long.valueOf(i.toLong)
      case l: Long => java.lang.Long.valueOf(l)
      case f: Float => java.lang.Long.valueOf(f.toLong)
      case d: Double => java.lang.Long.valueOf(d.toLong)
      case s: String => java.lang.Long.valueOf(s.toLong)
      case _ => java.lang.Long.valueOf(value.toString.toLong)
    }
  }

  /**
   * Determines if a filter can be pushed down to tantivy4java.
   *
   * @param filter Spark Filter
   * @return true if pushdown is supported
   */
  /**
   * Checks if a filter can be pushed down to tantivy4java JSON queries via parseQuery syntax.
   *
   * @param filter Spark Filter to check
   * @return true if filter can be translated to parseQuery syntax
   */
  def canPushDown(filter: Filter): Boolean = {
    // Check if we can generate parseQuery string for this filter
    translateFilterToParseQuery(filter).isDefined
  }
}
