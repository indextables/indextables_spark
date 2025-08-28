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
          logger.debug(s"Creating EqualTo query: $attribute = $value")
          val fieldType = getFieldType(schema, attribute)
          logger.debug(s"Field '$attribute' has type: $fieldType, isNumeric: ${isNumericFieldType(fieldType)}")
          val query = if (fieldType == FieldType.TEXT) {
            // For TEXT fields, use phrase query for reliable text matching
            // This handles both single words and phrases properly
            logger.debug(s"Field '$attribute' is TEXT, using phraseQuery for text matching")
            import scala.jdk.CollectionConverters._
            val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
            Query.phraseQuery(schema, attribute, words)
          } else if (isNumericFieldType(fieldType)) {
            // For numeric fields (INTEGER, FLOAT, DATE), use range query for equality
            logger.debug(s"Field '$attribute' is numeric $fieldType, using rangeQuery for equality")
            val convertedValue = convertSparkValueToTantivy(value, fieldType)
            logger.info(s"Creating range query for numeric equality: field='$attribute', fieldType=$fieldType, min=$convertedValue, max=$convertedValue")
            val result = Query.rangeQuery(schema, attribute, fieldType, convertedValue, convertedValue, true, true)
            logger.debug(s"Successfully created range query: ${result.getClass.getSimpleName}")
            result
          } else {
            // For other non-TEXT fields (BOOLEAN, BYTES), use term query with converted value
            logger.debug(s"Field '$attribute' is $fieldType, using termQuery")
            val convertedValue = convertSparkValueToTantivy(value, fieldType)
            logger.info(s"Passing to tantivy4java termQuery: field='$attribute', value=$convertedValue (${convertedValue.getClass.getSimpleName})")
            Query.termQuery(schema, attribute, convertedValue)
          }
          logger.debug(s"Created Query: ${query.getClass.getSimpleName} for field '$attribute' with value '$value'")
          query
        
        case EqualNullSafe(attribute, value) =>
          if (value == null) {
            logger.debug(s"Creating EqualNullSafe query for null: $attribute IS NULL")
            // For null values, we could return a query that matches no documents
            // or handle this differently based on requirements
            Query.allQuery() // TODO: Implement proper null handling
          } else {
            logger.debug(s"Creating EqualNullSafe query: $attribute = $value")
            val fieldType = getFieldType(schema, attribute)
            if (isNumericFieldType(fieldType)) {
              // For numeric fields, use range query for equality
              logger.debug(s"Field '$attribute' is numeric $fieldType, using rangeQuery for equality")
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              Query.rangeQuery(schema, attribute, fieldType, convertedValue, convertedValue, true, true)
            } else {
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              Query.termQuery(schema, attribute, convertedValue)
            }
          }
        
        case GreaterThan(attribute, value) =>
          logger.debug(s"Creating GreaterThan query: $attribute > $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          logger.info(s"Creating GreaterThan range query: field='$attribute', fieldType=$fieldType, min=$convertedValue (exclusive)")
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, false, true)
        
        case GreaterThanOrEqual(attribute, value) =>
          logger.debug(s"Creating GreaterThanOrEqual query: $attribute >= $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, true, true)
        
        case LessThan(attribute, value) =>
          logger.debug(s"Creating LessThan query: $attribute < $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, false)
        
        case LessThanOrEqual(attribute, value) =>
          logger.debug(s"Creating LessThanOrEqual query: $attribute <= $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, true)
        
        case In(attribute, values) =>
          logger.debug(s"Creating In query: $attribute IN [${values.mkString(", ")}]")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            // For TEXT fields, create OR query with phrase queries for each value
            logger.debug(s"Field '$attribute' is TEXT, using OR of phraseQueries for IN query")
            import scala.jdk.CollectionConverters._
            val phraseQueries = values.map { value =>
              val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
              Query.phraseQuery(schema, attribute, words)
            }
            val occurQueries = phraseQueries.map(query => new Query.OccurQuery(Occur.SHOULD, query)).toList
            Query.booleanQuery(occurQueries.asJava)
          } else if (isNumericFieldType(fieldType)) {
            // For numeric fields, create OR query with range queries for each value
            logger.debug(s"Field '$attribute' is numeric $fieldType, using OR of rangeQueries for IN query")
            val rangeQueries = values.map { value =>
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              Query.rangeQuery(schema, attribute, fieldType, convertedValue, convertedValue, true, true)
            }
            val occurQueries = rangeQueries.map(query => new Query.OccurQuery(Occur.SHOULD, query)).toList
            Query.booleanQuery(occurQueries.asJava)
          } else {
            // For other fields (BOOLEAN, BYTES), use term set query with converted values
            logger.debug(s"Field '$attribute' is $fieldType, using termSetQuery")
            val convertedValues = values.map(value => convertSparkValueToTantivy(value, fieldType))
            if (fieldType == FieldType.BOOLEAN) {
              logger.debug(s"Boolean IN query - converted values: ${convertedValues.mkString("[", ", ", "]")}")
              convertedValues.foreach(v => logger.debug(s"  Value: $v (${v.getClass.getSimpleName})"))
            }
            val valuesList = convertedValues.toList.asJava.asInstanceOf[java.util.List[Object]]
            Query.termSetQuery(schema, attribute, valuesList)
          }
        
        case IsNull(attribute) =>
          logger.debug(s"Creating IsNull query: $attribute IS NULL")
          // For IsNull, we could return a query that matches no documents
          // TODO: Implement proper null handling if needed
          Query.allQuery()
        
        case IsNotNull(attribute) =>
          logger.debug(s"Creating IsNotNull query: $attribute IS NOT NULL")
          val fieldType = getFieldType(schema, attribute)
          if (isNumericFieldType(fieldType) || fieldType == FieldType.BOOLEAN) {
            // For numeric and boolean fields, assume non-null (tantivy fields are typically non-nullable)
            // Return allQuery to match all documents since null filtering doesn't apply to these field types
            logger.debug(s"Field '$attribute' is $fieldType, using allQuery for IsNotNull (non-nullable field type)")
            Query.allQuery()
          } else {
            // For TEXT and other fields, use wildcard query to match any value
            logger.debug(s"Field '$attribute' is $fieldType, using wildcardQuery for IsNotNull")
            try {
              Query.wildcardQuery(schema, attribute, "*", true)
            } catch {
              case e: Exception =>
                logger.warn(s"Failed to create IsNotNull wildcard query for $attribute: ${e.getMessage}")
                Query.allQuery()
            }
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
          // TODO: Replace replaceAll with actual Tantivy4Java tokenizer when exposed
          // Convert spaces to wildcards and add trailing wildcard for startsWith
          val pattern = value.replaceAll("\\s+", "*") + "*"
          logger.debug(s"StringStartsWith pattern converted to wildcard: '$pattern'")
          Query.wildcardQuery(schema, attribute, pattern, true)
        
        case StringEndsWith(attribute, value) =>
          logger.debug(s"Creating StringEndsWith query: $attribute ends with '$value'")
          // TODO: Replace replaceAll with actual Tantivy4Java tokenizer when exposed
          // Convert spaces to wildcards and add leading wildcard for endsWith
          val pattern = "*" + value.replaceAll("\\s+", "*")
          logger.debug(s"StringEndsWith pattern converted to wildcard: '$pattern'")
          Query.wildcardQuery(schema, attribute, pattern, true)
        
        case StringContains(attribute, value) =>
          logger.debug(s"Creating StringContains query: $attribute contains '$value'")
          // TODO: Replace replaceAll with actual Tantivy4Java tokenizer when exposed
          // Convert spaces to wildcards and add leading/trailing wildcards for contains
          val pattern = "*" + value.replaceAll("\\s+", "*") + "*"
          logger.debug(s"StringContains pattern converted to wildcard: '$pattern'")
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
  
  /**
   * Check if a field type is numeric (should use range queries instead of term queries for equality)
   */
  private def isNumericFieldType(fieldType: FieldType): Boolean = {
    fieldType match {
      case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE => true
      case _ => false
    }
  }
  
  /**
   * Convert Spark values to tantivy4java compatible values for filtering
   */
  private def convertSparkValueToTantivy(value: Any, fieldType: FieldType): Any = {
    if (value == null) return null
    
    fieldType match {
      case FieldType.DATE =>
        value match {
          case ts: java.sql.Timestamp => ts.getTime // Convert to milliseconds
          case date: java.sql.Date => date.getTime / (24 * 60 * 60 * 1000L) // Convert to days since epoch
          case l: java.lang.Long => l
          case i: java.lang.Integer => i.longValue()
          case other => other
        }
      case FieldType.INTEGER =>
        // Keep original types for range queries - tantivy4java handles type conversion internally
        val result = value match {
          case ts: java.sql.Timestamp => ts.getTime // Convert to milliseconds as Long
          case date: java.sql.Date => date.getTime / (24 * 60 * 60 * 1000L) // Convert to days since epoch as Long
          case i: java.lang.Integer => i // Keep as Integer for range queries
          case l: java.lang.Long => l // Keep as Long 
          case other => other
        }
        logger.debug(s"INTEGER conversion: $value (${value.getClass.getSimpleName}) -> $result (${result.getClass.getSimpleName})")
        result
      case FieldType.BOOLEAN =>
        val booleanResult = value match {
          case b: java.lang.Boolean => b.booleanValue()
          case b: Boolean => b
          case i: java.lang.Integer => i != 0
          case l: java.lang.Long => l != 0
          case s: String => s.toLowerCase == "true" || s == "1"
          case other => throw new IllegalArgumentException(s"Cannot convert $other to Boolean for field type BOOLEAN")
        }
        // Ensure we return a Java Boolean object that tantivy4java expects
        val convertedValue = java.lang.Boolean.valueOf(booleanResult)
        logger.debug(s"Boolean conversion: $value (${value.getClass.getSimpleName}) -> $convertedValue (${convertedValue.getClass.getSimpleName})")
        convertedValue
      case FieldType.FLOAT =>
        value match {
          case f: java.lang.Float => f.floatValue()
          case d: java.lang.Double => d.doubleValue()
          case i: java.lang.Integer => i.doubleValue()
          case l: java.lang.Long => l.doubleValue()
          case s: String => try { s.toDouble } catch { case _: Exception => throw new IllegalArgumentException(s"Cannot convert string '$s' to Float") }
          case other => other
        }
      case _ =>
        // For other types (TEXT, BYTES), pass through as-is
        value
    }
  }
}