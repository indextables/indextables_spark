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

import com.tantivy4java.{Query, Schema}
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
      // Check for field:value pattern
      if (trimmed.contains(":")) {
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
  
  private def parseFieldQuery(query: String, schema: Schema): Query = {
    val parts = query.split(":", 2)
    if (parts.length != 2) {
      return Query.allQuery()
    }
    
    val fieldName = parts(0).trim
    val value = parts(1).trim
    
    // Handle quoted phrases
    if (value.startsWith("\"") && value.endsWith("\"")) {
      val phraseText = value.substring(1, value.length - 1)
      val words = phraseText.split("\\s+").toList.asJava.asInstanceOf[java.util.List[Object]]
      return Query.phraseQuery(schema, fieldName, words)
    }
    
    // Handle prefix queries (ending with *)
    if (value.endsWith("*")) {
      val prefixText = value.substring(0, value.length - 1)
      // Use fuzzy query with distance 0 for prefix-like behavior
      return Query.fuzzyTermQuery(schema, fieldName, prefixText)
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
}