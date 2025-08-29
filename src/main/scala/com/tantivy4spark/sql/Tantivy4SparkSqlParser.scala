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

package com.tantivy4spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import com.tantivy4spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}

/**
 * Custom SQL parser for Tantivy4Spark that extends the default Spark SQL parser
 * to support additional Tantivy4Spark-specific commands.
 *
 * Supported commands:
 * - FLUSH TANTIVY4SPARK SEARCHER CACHE
 * 
 * Supported operators:
 * - indexquery: column indexquery 'query_string'
 * - indexqueryall: indexqueryall('query_string')
 */
class Tantivy4SparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan = {
    val trimmed = sqlText.trim.toUpperCase
    
    if (trimmed == "FLUSH TANTIVY4SPARK SEARCHER CACHE") {
      FlushTantivyCacheCommand()
    } else {
      // Check if SQL contains indexquery operator and preprocess it
      val preprocessedSql = preprocessIndexQueryOperators(sqlText)
      delegate.parsePlan(preprocessedSql)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    // Check for indexquery operator pattern
    val indexQueryPattern = """(.+?)\s+indexquery\s+(.+)""".r
    
    // Check for indexqueryall function pattern
    val indexQueryAllPattern = """indexqueryall\s*\(\s*(.+)\s*\)""".r
    
    sqlText.trim match {
      case indexQueryPattern(leftExpr, rightExpr) =>
        try {
          val left = delegate.parseExpression(leftExpr.trim)
          val right = delegate.parseExpression(rightExpr.trim)
          IndexQueryExpression(left, right)
        } catch {
          case e: ParseException =>
            // If parsing individual parts fails, delegate to default parser
            delegate.parseExpression(sqlText)
        }
      
      case indexQueryAllPattern(queryExpr) =>
        try {
          val query = delegate.parseExpression(queryExpr.trim)
          IndexQueryAllExpression(query)
        } catch {
          case e: ParseException =>
            // If parsing query fails, delegate to default parser
            delegate.parseExpression(sqlText)
        }
      
      case _ =>
        delegate.parseExpression(sqlText)
    }
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    val trimmed = sqlText.trim.toUpperCase
    
    if (trimmed == "FLUSH TANTIVY4SPARK SEARCHER CACHE") {
      FlushTantivyCacheCommand()
    } else {
      // Check if SQL contains indexquery operator and preprocess it
      val preprocessedSql = preprocessIndexQueryOperators(sqlText)
      delegate.parseQuery(preprocessedSql)
    }
  }
  
  /**
   * Preprocess SQL text to convert indexquery operators and indexqueryall functions 
   * to function calls that Spark can parse. This allows us to inject our custom 
   * expressions into the logical plan.
   */
  private def preprocessIndexQueryOperators(sqlText: String): String = {
    // Pattern to match: column_name indexquery 'query_string'
    val indexQueryPattern = """(\w+)\s+indexquery\s+'([^']*)'""".r
    
    // Pattern to match: indexqueryall('query_string')
    val indexQueryAllPattern = """indexqueryall\s*\(\s*'([^']*)'\s*\)""".r
    
    // First replace indexquery operators
    val afterIndexQuery = indexQueryPattern.replaceAllIn(sqlText, m => {
      val columnName = m.group(1)
      val queryString = m.group(2)
      // Convert to a function call that we can intercept later
      s"tantivy4spark_indexquery('$columnName', '$queryString')"
    })
    
    // Then replace indexqueryall functions
    indexQueryAllPattern.replaceAllIn(afterIndexQuery, m => {
      val queryString = m.group(1)
      // Convert to a function call that we can intercept later
      s"tantivy4spark_indexqueryall('$queryString')"
    })
  }
}