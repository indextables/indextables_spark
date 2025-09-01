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
import com.tantivy4spark.sql.parser.{Tantivy4SparkSqlAstBuilder, Tantivy4SparkSqlBaseParser, Tantivy4SparkSqlBaseLexer}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.atn.PredictionMode
import org.apache.spark.sql.catalyst.parser.ParseErrorListener
import org.slf4j.LoggerFactory

/**
 * Custom SQL parser for Tantivy4Spark that extends the default Spark SQL parser
 * to support additional Tantivy4Spark-specific commands.
 *
 * Supported commands:
 * - FLUSH TANTIVY4SPARK SEARCHER CACHE
 * - MERGE SPLITS <path_or_table> [WHERE predicates] [TARGET SIZE <bytes>] [PRECOMMIT]
 * - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE [FOR <path_or_table>]
 * 
 * Supported operators:
 * - indexquery: column indexquery 'query_string'
 * - indexqueryall: indexqueryall('query_string')
 */
class Tantivy4SparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  private val astBuilder = new Tantivy4SparkSqlAstBuilder()
  private val logger = LoggerFactory.getLogger(getClass)

  override def parsePlan(sqlText: String): LogicalPlan = {
    logger.debug(s"Parsing SQL: $sqlText")
    try {
      parse(sqlText) { parser =>
        val result = astBuilder.visit(parser.singleStatement())
        logger.debug(s"AST Builder result: $result, type: ${if (result != null) result.getClass.getName else "null"}")
        result match {
          case plan: LogicalPlan => 
            logger.debug(s"Successfully parsed Tantivy4Spark command: $plan")
            // Successfully parsed a Tantivy4Spark command
            plan
          case null => 
            logger.debug("ANTLR didn't match any patterns, delegating to Spark parser")
            // ANTLR didn't match any of our patterns, delegate to Spark parser
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)
            delegate.parsePlan(preprocessedSql)
          case _ => 
            logger.debug(s"Unexpected result type: ${result.getClass.getName}, delegating to Spark parser")
            // Unexpected result type, delegate to Spark parser
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)
            delegate.parsePlan(preprocessedSql)
        }
      }
    } catch {
      case e: Exception =>
        logger.debug(s"ANTLR parsing failed with exception: ${e.getMessage}")
        
        // Check for specific MERGE SPLITS error cases before delegating
        val trimmedSql = sqlText.trim
        if (trimmedSql.equals("MERGE SPLITS")) {
          throw new IllegalArgumentException("MERGE SPLITS requires either a path or table identifier")
        } else if (trimmedSql.startsWith("MERGE SPLITS") && trimmedSql.contains("TARGET SIZE")) {
          // Check for invalid TARGET SIZE format - only catch non-numeric values
          val targetSizePattern = "TARGET\\s+SIZE\\s+(\\S+)".r
          targetSizePattern.findFirstMatchIn(trimmedSql) match {
            case Some(m) => 
              val sizeValue = m.group(1)
              // Only throw NumberFormatException for non-numeric values like "invalid"
              // Let negative numbers and other numeric values through to be validated in the command
              try {
                if (!sizeValue.matches("-?\\d+[MG]?")) {
                  throw new NumberFormatException(s"Invalid target size format: $sizeValue")
                }
              } catch {
                case _: NumberFormatException => throw new NumberFormatException(s"Invalid target size format: $sizeValue")
              }
            case None => // No TARGET SIZE found, continue with normal delegation
          }
        }
        
        // If ANTLR parsing fails completely, fall back to delegate parser
        val preprocessedSql = preprocessIndexQueryOperators(sqlText)
        delegate.parsePlan(preprocessedSql)
    }
  }

  /**
   * Parse SQL text using ANTLR (similar to Delta Lake's approach).
   */
  private def parse[T](command: String)(toResult: Tantivy4SparkSqlBaseParser => T): T = {
    val lexer = new Tantivy4SparkSqlBaseLexer(CharStreams.fromString(command))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new Tantivy4SparkSqlBaseParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // First, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // If we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
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
    try {
      parse(sqlText) { parser =>
        astBuilder.visit(parser.singleStatement()) match {
          case plan: LogicalPlan => plan
          case null =>
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)
            delegate.parseQuery(preprocessedSql)
          case _ => 
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)
            delegate.parseQuery(preprocessedSql)
        }
      }
    } catch {
      case _: Exception =>
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
      // Convert to a function call using the registered function name
      s"tantivy4spark_indexqueryall('$queryString')"
    })
  }
}