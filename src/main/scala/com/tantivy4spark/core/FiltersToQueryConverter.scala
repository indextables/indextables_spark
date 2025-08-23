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
import org.slf4j.LoggerFactory

object FiltersToQueryConverter {
  
  private val logger = LoggerFactory.getLogger(this.getClass)

  def convert(filters: Array[Filter]): String = {
    convert(filters, None)
  }

  def convert(filters: Array[Filter], schemaFieldNames: Option[Set[String]]): String = {
    if (filters.isEmpty) {
      return ""
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

    val queryParts = validFilters.flatMap(convertFilter).filter(_.nonEmpty)
    
    if (queryParts.isEmpty) {
      ""
    } else if (queryParts.length == 1) {
      queryParts.head
    } else {
      queryParts.mkString("(", ") AND (", ")")
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

  private def convertFilter(filter: Filter): Option[String] = {
    val query = filter match {
      case EqualTo(attribute, value) =>
        s"""$attribute:"${escapeValue(value)}""""
      
      case EqualNullSafe(attribute, value) =>
        if (value == null) {
          s"NOT _exists_:$attribute"
        } else {
          s"""$attribute:"${escapeValue(value)}""""
        }
      
      case GreaterThan(attribute, value) =>
        s"$attribute:{${escapeValue(value)} TO *}"
      
      case GreaterThanOrEqual(attribute, value) =>
        s"$attribute:[${escapeValue(value)} TO *]"
      
      case LessThan(attribute, value) =>
        s"$attribute:{* TO ${escapeValue(value)}}"
      
      case LessThanOrEqual(attribute, value) =>
        s"$attribute:[* TO ${escapeValue(value)}]"
      
      case In(attribute, values) =>
        val valueStrs = values.map(v => s""""${escapeValue(v)}"""").mkString(" OR ")
        s"$attribute:($valueStrs)"
      
      case IsNull(attribute) =>
        s"NOT _exists_:$attribute"
      
      case IsNotNull(attribute) =>
        s"_exists_:$attribute"
      
      case And(left, right) =>
        (convertFilter(left), convertFilter(right)) match {
          case (Some(l), Some(r)) => s"($l) AND ($r)"
          case (Some(l), None) => l
          case (None, Some(r)) => r
          case (None, None) => ""
        }
      
      case Or(left, right) =>
        (convertFilter(left), convertFilter(right)) match {
          case (Some(l), Some(r)) => s"($l) OR ($r)"
          case (Some(l), None) => l
          case (None, Some(r)) => r
          case (None, None) => ""
        }
      
      case Not(child) =>
        convertFilter(child) match {
          case Some(childQuery) => s"NOT ($childQuery)"
          case None => ""
        }
      
      case StringStartsWith(attribute, value) =>
        s"$attribute:${escapeValue(value)}*"
      
      case StringEndsWith(attribute, value) =>
        s"$attribute:*${escapeValue(value)}"
      
      case StringContains(attribute, value) =>
        s"$attribute:*${escapeValue(value)}*"
      
      case _ =>
        logger.warn(s"Unsupported filter: $filter")
        ""
    }

    if (query.nonEmpty) Some(query) else None
  }

  private def escapeValue(value: Any): String = {
    val str = value.toString
    // Escape special characters for Tantivy query syntax
    str.replaceAll("""([\+\-\!\(\)\{\}\[\]\^\~\*\?\:\\"])""", """\\$1""")
  }
}