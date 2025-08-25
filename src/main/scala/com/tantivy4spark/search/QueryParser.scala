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

package com.tantivy4spark.search

import com.tantivy4java.{Query, Schema, Occur}
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters._

/**
 * Simple query parser for converting string queries to tantivy4java Query objects.
 * 
 * This is a basic implementation that handles common query patterns.
 * For more advanced query parsing, consider using tantivy4java's Index.parseQuery method.
 */
object QueryParser {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Parse a query string into a tantivy4java Query.
   * 
   * Supported formats:
   * - "*" or "" -> match all
   * - "field:value" -> term query on specific field
   * - "value" -> search across all text fields
   * - "field:value*" -> prefix query (implemented as fuzzy with distance 0)
   * - "field:\"phrase query\"" -> phrase query
   * 
   * @param queryString The query string to parse
   * @param schema The Tantivy schema
   * @return A tantivy4java Query object
   */
  def parseQuery(queryString: String, schema: Schema): Query = {
    val trimmed = queryString.trim
    
    if (trimmed.isEmpty || trimmed == "*") {
      // Match all query
      return Query.allQuery()
    }
    
    try {
      // Handle complex boolean queries with OR, AND, etc.
      if (trimmed.contains(" OR ") || trimmed.contains(" AND ") || trimmed.contains("(")) {
        parseBooleanQuery(trimmed, schema)
      } else if (trimmed.contains(":")) {
        parseFieldQuery(trimmed, schema)
      } else {
        // Simple text search across all text fields
        parseTextQuery(trimmed, schema)
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse query '$queryString', falling back to match-all", e)
        Query.allQuery()
    }
  }
  
  private def parseBooleanQuery(query: String, schema: Schema): Query = {
    logger.debug(s"Parsing boolean query: '$query'")
    
    try {
      // Handle NOT queries first
      if (query.trim.startsWith("NOT ")) {
        val innerQuery = query.trim.substring(4).trim.replaceAll("^\\(|\\)$", "").trim
        logger.debug(s"Parsing NOT query with inner: '$innerQuery'")
        val inner = if (innerQuery.contains(":")) {
          parseFieldQuery(innerQuery, schema)
        } else {
          parseTextQuery(innerQuery, schema)
        }
        val notQuery = new Query.OccurQuery(Occur.MUST_NOT, inner)
        return Query.booleanQuery(java.util.Arrays.asList(notQuery))
      }
      
      // For now, handle simple OR queries by splitting on " OR "
      if (query.contains(" OR ")) {
        val parts = query.split(" OR ").map(_.trim.replaceAll("^\\(|\\)$", "").trim)
        
        if (parts.length >= 2) {
          logger.debug(s"Split OR query into ${parts.length} parts: ${parts.mkString(", ")}")
          val queries = parts.map { part =>
            parseSingleQueryPart(part, schema)
          }.toList
          
          // Create OccurQuery objects for OR query (should clauses)
          val occurQueries = queries.map { query =>
            new Query.OccurQuery(Occur.SHOULD, query)
          }
          return Query.booleanQuery(occurQueries.asJava)
        }
      }
      
      // Handle AND queries
      if (query.contains(" AND ")) {
        val parts = query.split(" AND ").map(_.trim.replaceAll("^\\(|\\)$", "").trim)
        
        if (parts.length >= 2) {
          logger.debug(s"Split AND query into ${parts.length} parts: ${parts.mkString(", ")}")
          val queries = parts.map { part =>
            parseSingleQueryPart(part, schema)
          }.toList
          
          // Create OccurQuery objects for AND query (must clauses)
          val occurQueries = queries.map { query =>
            new Query.OccurQuery(Occur.MUST, query)
          }
          return Query.booleanQuery(occurQueries.asJava)
        }
      }
      
      // If we can't parse as boolean, strip parentheses and try as simple query
      val cleaned = query.replaceAll("^\\(|\\)$", "").trim
      parseSingleQueryPart(cleaned, schema)
      
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse boolean query '$query': ${e.getMessage}")
        Query.allQuery()
    }
  }
  
  /**
   * Parse a single query part, handling NOT, field queries, and text queries.
   */
  private def parseSingleQueryPart(part: String, schema: Schema): Query = {
    val trimmed = part.trim
    
    // Handle NOT prefix
    if (trimmed.startsWith("NOT ")) {
      val innerQuery = trimmed.substring(4).trim.replaceAll("^\\(|\\)$", "").trim
      logger.debug(s"Parsing NOT query part with inner: '$innerQuery'")
      val inner = if (innerQuery.contains(":")) {
        parseFieldQuery(innerQuery, schema)
      } else {
        parseTextQuery(innerQuery, schema)
      }
      val notQuery = new Query.OccurQuery(Occur.MUST_NOT, inner)
      return Query.booleanQuery(java.util.Arrays.asList(notQuery))
    }
    
    // Handle field queries and text queries
    if (trimmed.contains(":")) {
      parseFieldQuery(trimmed, schema)
    } else {
      parseTextQuery(trimmed, schema)
    }
  }
  
  private def parseFieldQuery(query: String, schema: Schema): Query = {
    val parts = query.split(":", 2)
    if (parts.length != 2) {
      return Query.allQuery()
    }
    
    val fieldName = parts(0).trim
    val value = parts(1).trim
    
    // Handle range queries first
    if ((value.startsWith("[") && value.endsWith("]")) || (value.startsWith("{") && value.endsWith("}"))) {
      val isInclusive = value.startsWith("[") && value.endsWith("]")
      val rangeContent = value.substring(1, value.length - 1)
      
      if (rangeContent.contains(" TO ")) {
        val rangeParts = rangeContent.split(" TO ", 2)
        if (rangeParts.length == 2) {
          val lowerBound = rangeParts(0).trim
          val upperBound = rangeParts(1).trim
          
          logger.debug(s"Creating range query for field '$fieldName': $lowerBound TO $upperBound (inclusive: $isInclusive)")
          
          try {
            // Get field type from schema
            val fieldInfo = schema.getFieldInfo(fieldName)
            val fieldType = fieldInfo.getType()
            
            // Convert string bounds to appropriate types based on field type
            val lower = if (lowerBound == "*") null else convertValueToFieldType(lowerBound, fieldType)
            val upper = if (upperBound == "*") null else convertValueToFieldType(upperBound, fieldType)
            
            return Query.rangeQuery(schema, fieldName, fieldType, lower, upper, isInclusive, isInclusive)
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to create range query for field '$fieldName': ${e.getMessage}")
              // Fall back to term query if range query fails
              return Query.allQuery()
          }
        }
      }
    }
    
    // Handle quoted phrases - but only for text fields
    if (value.startsWith("\"") && value.endsWith("\"")) {
      val phraseText = value.substring(1, value.length - 1)
      
      // Check if this is a text field before using phrase query
      val isTextField = try {
        import com.tantivy4java.FieldType
        val textFields = schema.getFieldNamesByType(FieldType.TEXT).asScala.toSet
        textFields.contains(fieldName)
      } catch {
        case e: Exception =>
          logger.debug(s"Could not determine field type for '$fieldName', assuming text field: ${e.getMessage}")
          true // Default to true for backward compatibility
      }
      
      if (isTextField) {
        val words = phraseText.split("\\s+").toList.asJava.asInstanceOf[java.util.List[Object]]
        return Query.phraseQuery(schema, fieldName, words)
      } else {
        // For non-text fields (like boolean, integer), use term query with unquoted value
        logger.debug(s"Using term query for non-text field '$fieldName' with value '$phraseText'")
        return Query.termQuery(schema, fieldName, phraseText)
      }
    }
    
    // Handle wildcard queries using the new wildcardQuery method
    if (value.contains("*") || value.contains("?")) {
      logger.debug(s"Creating wildcard query for field '$fieldName' with pattern '$value'")
      try {
        // Use the native wildcard query implementation with lenient mode to avoid errors for missing fields
        return Query.wildcardQuery(schema, fieldName, value, true)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to create wildcard query for pattern '$value' on field '$fieldName': ${e.getMessage}")
          // Fall back to regex query if wildcard query fails
          try {
            val regexPattern = value.replace("*", ".*").replace("?", ".")
            logger.debug(s"Falling back to regex query with pattern '$regexPattern'")
            return Query.regexQuery(schema, fieldName, regexPattern)
          } catch {
            case e2: Exception =>
              logger.warn(s"Failed to create regex query, falling back to match-all: ${e2.getMessage}")
              return Query.allQuery()
          }
      }
    }
    
    // Regular term query
    Query.termQuery(schema, fieldName, value)
  }
  
  private def parseTextQuery(query: String, schema: Schema): Query = {
    // Get all text field names from schema
    val textFields = getTextFields(schema)
    
    if (textFields.isEmpty) {
      logger.warn("No text fields found in schema for text query")
      return Query.allQuery()
    }
    
    // Handle quoted phrases
    if (query.startsWith("\"") && query.endsWith("\"")) {
      val phraseText = query.substring(1, query.length - 1)
      val words = phraseText.split("\\s+").toList.asJava.asInstanceOf[java.util.List[Object]]
      
      // Create phrase query for first text field
      // TODO: Could create boolean query across multiple fields
      return Query.phraseQuery(schema, textFields.head, words)
    }
    
    // Simple term query on first text field
    // TODO: Could create boolean query across multiple fields
    Query.termQuery(schema, textFields.head, query)
  }
  
  private def getTextFields(schema: Schema): List[String] = {
    import scala.jdk.CollectionConverters._
    import com.tantivy4java.FieldType
    
    // Get field names that are of TEXT type
    schema.getFieldNamesByType(FieldType.TEXT).asScala.toList
  }
  
  /**
   * Convert a string value to the appropriate type based on the field type.
   */
  private def convertValueToFieldType(value: String, fieldType: com.tantivy4java.FieldType): Object = {
    import com.tantivy4java.FieldType
    
    try {
      fieldType match {
        case FieldType.INTEGER => 
          java.lang.Integer.valueOf(value.toInt)
        case FieldType.UNSIGNED => 
          java.lang.Long.valueOf(value.toLong) // Unsigned is handled as Long in Java
        case FieldType.FLOAT => 
          java.lang.Float.valueOf(value.toFloat)
        case FieldType.BOOLEAN => 
          java.lang.Boolean.valueOf(value.toBoolean)
        case FieldType.DATE => 
          // DATE fields expect Long values (timestamp in milliseconds)
          java.lang.Long.valueOf(value.toLong)
        case FieldType.TEXT | FieldType.FACET | FieldType.BYTES | FieldType.JSON | FieldType.IP_ADDR => 
          value // String types remain as strings
        case _ => 
          logger.warn(s"Unknown field type: $fieldType, treating as string")
          value
      }
    } catch {
      case e: NumberFormatException =>
        logger.warn(s"Failed to convert '$value' to field type $fieldType: ${e.getMessage}")
        value // Fall back to string if conversion fails
    }
  }
}