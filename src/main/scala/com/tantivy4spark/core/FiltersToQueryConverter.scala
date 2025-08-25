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


package com.tantivy4spark.core

import org.apache.spark.sql.sources._
import com.tantivy4java.{Query, Schema, Occur, FieldType}
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._

object FiltersToQueryConverter {
  
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Convert Spark filters to a tantivy4java Query object.
   */
  def convertToQuery(filters: Array[Filter], schema: Schema): Query = {
    convertToQuery(filters, schema, None)
  }

  /**
   * Convert Spark filters to a tantivy4java Query object with schema field validation.
   */
  def convertToQuery(filters: Array[Filter], schema: Schema, schemaFieldNames: Option[Set[String]]): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Filter out filters that reference non-existent fields
    val validFilters = schemaFieldNames match {
      case Some(fieldNames) =>
        logger.debug(s"Schema validation enabled with fields: ${fieldNames.mkString(", ")}")
        val valid = filters.filter(filter => isFilterValidForSchema(filter, fieldNames))
        logger.debug(s"Schema validation results: ${valid.length}/${filters.length} filters passed validation")
        valid
      case None =>
        logger.debug("No schema validation - using all filters")
        filters // No schema validation if fieldNames not provided
    }

    if (validFilters.length < filters.length) {
      val skippedCount = filters.length - validFilters.length
      logger.info(s"Schema validation: Skipped $skippedCount filters due to field validation")
    }

    val queries = validFilters.flatMap(filter => Option(convertFilterToQuery(filter, schema))).filter(_ != null)
    
    if (queries.isEmpty) {
      Query.allQuery()
    } else if (queries.length == 1) {
      queries.head
    } else {
      // Combine multiple queries with AND logic
      val occurQueries = queries.map(query => new Query.OccurQuery(Occur.MUST, query)).toList
      Query.booleanQuery(occurQueries.asJava)
    }
  }

  private def isFilterValidForSchema(filter: Filter, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._
    
    def getFilterFieldNames(f: Filter): Set[String] = f match {
      case EqualTo(attribute, _) => Set(attribute)
      case EqualNullSafe(attribute, _) => Set(attribute)
      case GreaterThan(attribute, _) => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _) => Set(attribute)
      case LessThanOrEqual(attribute, _) => Set(attribute)
      case In(attribute, _) => Set(attribute)
      case IsNull(attribute) => Set(attribute)
      case IsNotNull(attribute) => Set(attribute)
      case StringStartsWith(attribute, _) => Set(attribute)
      case StringEndsWith(attribute, _) => Set(attribute)
      case StringContains(attribute, _) => Set(attribute)
      case And(left, right) => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Or(left, right) => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Not(child) => getFilterFieldNames(child)
      case _ => Set.empty
    }
    
    val filterFields = getFilterFieldNames(filter)
    val isValid = filterFields.subsetOf(fieldNames)
    
    if (!isValid) {
      val missingFields = filterFields -- fieldNames
      logger.debug(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }
    
    isValid
  }

  /**
   * Convert a single Spark Filter to a tantivy4java Query.
   */
  private def convertFilterToQuery(filter: Filter, schema: Schema): Query = {
    try {
      filter match {
        case EqualTo(attribute, value) =>
          logger.warn(s"Creating EqualTo query: $attribute = $value")
          val fieldType = getFieldType(schema, attribute)
          val query = if (fieldType == FieldType.TEXT) {
            // For TEXT fields, use phrase query for exact matching
            logger.warn(s"Field '$attribute' is TEXT, using phraseQuery for exact match")
            import scala.jdk.CollectionConverters._
            val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
            Query.phraseQuery(schema, attribute, words)
          } else {
            // For non-TEXT fields, use term query
            logger.warn(s"Field '$attribute' is $fieldType, using termQuery")
            Query.termQuery(schema, attribute, value)
          }
          logger.warn(s"Created Query: ${query.getClass.getSimpleName} for field '$attribute' with value '$value'")
          query
        
        case EqualNullSafe(attribute, value) =>
          if (value == null) {
            logger.debug(s"Creating EqualNullSafe query for null: $attribute IS NULL")
            // For null values, we could return a query that matches no documents
            // or handle this differently based on requirements
            Query.allQuery() // TODO: Implement proper null handling
          } else {
            logger.debug(s"Creating EqualNullSafe query: $attribute = $value")
            Query.termQuery(schema, attribute, value)
          }
        
        case GreaterThan(attribute, value) =>
          logger.debug(s"Creating GreaterThan query: $attribute > $value")
          val fieldType = getFieldType(schema, attribute)
          Query.rangeQuery(schema, attribute, fieldType, value, null, false, true)
        
        case GreaterThanOrEqual(attribute, value) =>
          logger.debug(s"Creating GreaterThanOrEqual query: $attribute >= $value")
          val fieldType = getFieldType(schema, attribute)
          Query.rangeQuery(schema, attribute, fieldType, value, null, true, true)
        
        case LessThan(attribute, value) =>
          logger.debug(s"Creating LessThan query: $attribute < $value")
          val fieldType = getFieldType(schema, attribute)
          Query.rangeQuery(schema, attribute, fieldType, null, value, true, false)
        
        case LessThanOrEqual(attribute, value) =>
          logger.debug(s"Creating LessThanOrEqual query: $attribute <= $value")
          val fieldType = getFieldType(schema, attribute)
          Query.rangeQuery(schema, attribute, fieldType, null, value, true, true)
        
        case In(attribute, values) =>
          logger.debug(s"Creating In query: $attribute IN [${values.mkString(", ")}]")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            // For TEXT fields, create OR query with phrase queries for each value
            logger.warn(s"Field '$attribute' is TEXT, using OR of phraseQueries for IN query")
            import scala.jdk.CollectionConverters._
            val phraseQueries = values.map { value =>
              val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
              Query.phraseQuery(schema, attribute, words)
            }
            val occurQueries = phraseQueries.map(query => new Query.OccurQuery(Occur.SHOULD, query)).toList
            Query.booleanQuery(occurQueries.asJava)
          } else {
            // For non-TEXT fields, use term set query
            logger.warn(s"Field '$attribute' is $fieldType, using termSetQuery")
            val valuesList = values.toList.asJava.asInstanceOf[java.util.List[Object]]
            Query.termSetQuery(schema, attribute, valuesList)
          }
        
        case IsNull(attribute) =>
          logger.debug(s"Creating IsNull query: $attribute IS NULL")
          // For IsNull, we could return a query that matches no documents
          // TODO: Implement proper null handling if needed
          Query.allQuery()
        
        case IsNotNull(attribute) =>
          logger.debug(s"Creating IsNotNull query: $attribute IS NOT NULL")
          // For IsNotNull, we could use a wildcard query to match any value
          try {
            Query.wildcardQuery(schema, attribute, "*", true)
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to create IsNotNull wildcard query for $attribute: ${e.getMessage}")
              Query.allQuery()
          }
        
        case And(left, right) =>
          logger.debug(s"Creating And query: $left AND $right")
          val leftQuery = convertFilterToQuery(left, schema)
          val rightQuery = convertFilterToQuery(right, schema)
          val occurQueries = List(
            new Query.OccurQuery(Occur.MUST, leftQuery),
            new Query.OccurQuery(Occur.MUST, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)
        
        case Or(left, right) =>
          logger.debug(s"Creating Or query: $left OR $right")
          val leftQuery = convertFilterToQuery(left, schema)
          val rightQuery = convertFilterToQuery(right, schema)
          val occurQueries = List(
            new Query.OccurQuery(Occur.SHOULD, leftQuery),
            new Query.OccurQuery(Occur.SHOULD, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)
        
        case Not(child) =>
          logger.debug(s"Creating Not query: NOT $child")
          val childQuery = convertFilterToQuery(child, schema)
          // For NOT queries, we need both MUST (match all) and MUST_NOT (exclude) clauses
          val allQuery = Query.allQuery()
          val occurQueries = java.util.Arrays.asList(
            new Query.OccurQuery(Occur.MUST, allQuery),
            new Query.OccurQuery(Occur.MUST_NOT, childQuery)
          )
          Query.booleanQuery(occurQueries)
        
        case StringStartsWith(attribute, value) =>
          logger.debug(s"Creating StringStartsWith query: $attribute starts with '$value'")
          val pattern = value + "*"
          Query.wildcardQuery(schema, attribute, pattern, true)
        
        case StringEndsWith(attribute, value) =>
          logger.debug(s"Creating StringEndsWith query: $attribute ends with '$value'")
          val pattern = "*" + value
          Query.wildcardQuery(schema, attribute, pattern, true)
        
        case StringContains(attribute, value) =>
          logger.debug(s"Creating StringContains query: $attribute contains '$value'")
          val pattern = "*" + value + "*"
          Query.wildcardQuery(schema, attribute, pattern, true)
        
        case _ =>
          logger.warn(s"Unsupported filter: $filter, falling back to match-all")
          Query.allQuery()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to convert filter $filter to Query: ${e.getMessage}", e)
        Query.allQuery() // Fallback to match-all query
    }
  }
  
  /**
   * Get the field type from the schema for a given field name.
   */
  private def getFieldType(schema: Schema, fieldName: String): FieldType = {
    try {
      val fieldInfo = schema.getFieldInfo(fieldName)
      fieldInfo.getType()
    } catch {
      case e: Exception =>
        logger.warn(s"Could not determine field type for '$fieldName', defaulting to TEXT: ${e.getMessage}")
        FieldType.TEXT
    }
  }
}