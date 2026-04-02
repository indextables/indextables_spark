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

package io.indextables.spark.sql

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParseErrorListener
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression}
import io.indextables.spark.sql.parser.{
  IndexTables4SparkSqlAstBuilder,
  IndexTables4SparkSqlBaseLexer,
  IndexTables4SparkSqlBaseParser
}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.slf4j.LoggerFactory

/**
 * Custom SQL parser for IndexTables4Spark that extends the default Spark SQL parser to support additional
 * IndexTables4Spark-specific commands.
 *
 * Supported commands:
 *   - FLUSH TANTIVY4SPARK SEARCHER CACHE
 *   - MERGE SPLITS <path_or_table> [WHERE predicates] [TARGET SIZE <bytes>] [PRECOMMIT]
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE [FOR <path_or_table>]
 *
 * Supported operators:
 *   - TEXTSEARCH: column TEXTSEARCH 'query_string' (tokenized text fields, preferred)
 *   - FIELDMATCH: column FIELDMATCH 'query_string' (non-tokenized fields, preferred)
 *   - * TEXTSEARCH: * TEXTSEARCH 'query_string' (all-fields text search)
 *   - * FIELDMATCH: * FIELDMATCH 'query_string' (all-fields field match)
 *   - indexquery: column indexquery 'query_string' (legacy, no type validation)
 *   - indexqueryall: indexqueryall('query_string') (legacy, no type validation)
 */
class IndexTables4SparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  private val astBuilder = new IndexTables4SparkSqlAstBuilder()
  private val logger     = LoggerFactory.getLogger(getClass)

  override def parsePlan(sqlText: String): LogicalPlan = {
    logger.debug(s"Parsing SQL: $sqlText")
    try
      parse(sqlText) { parser =>
        val result = astBuilder.visit(parser.singleStatement())
        logger.debug(s"AST Builder result: $result, type: ${if (result != null) result.getClass.getName else "null"}")
        result match {
          case plan: LogicalPlan =>
            logger.debug(s"Successfully parsed IndexTables4Spark command: $plan")
            // Successfully parsed a IndexTables4Spark command
            plan
          case null =>
            logger.debug("ANTLR didn't match any patterns, delegating to Spark parser")
            // ANTLR didn't match any of our patterns, delegate to Spark parser
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)

            // Debug: Log the SQL preprocessing
            if (sqlText != preprocessedSql) {
              logger.debug(s"SQL PARSER: Converting indexquery syntax")
              logger.debug(s"SQL PARSER: Original: $sqlText")
              logger.debug(s"SQL PARSER: Preprocessed: $preprocessedSql")
            }

            delegate.parsePlan(preprocessedSql)
          case _ =>
            logger.debug(s"Unexpected result type: ${result.getClass.getName}, delegating to Spark parser")
            // Unexpected result type, delegate to Spark parser
            val preprocessedSql = preprocessIndexQueryOperators(sqlText)
            delegate.parsePlan(preprocessedSql)
        }
      }
    catch {
      case e: IllegalArgumentException =>
        // Re-throw business logic exceptions (these are intended to be thrown)
        throw e
      case e: NumberFormatException =>
        // Re-throw validation exceptions (these are intended to be thrown)
        throw e
      case e: ParseException =>
        // Re-throw ANTLR parse exceptions (these are intended to be thrown)
        throw e
      case e: Exception =>
        logger.debug(s"ANTLR parsing failed for '$sqlText': ${e.getClass.getSimpleName}: ${e.getMessage}")

        // Only delegate to Spark for genuine parsing failures
        val preprocessedSql = preprocessIndexQueryOperators(sqlText)
        delegate.parsePlan(preprocessedSql)
    }
  }

  /** Parse SQL text using ANTLR (similar to Delta Lake's approach). */
  private def parse[T](command: String)(toResult: IndexTables4SparkSqlBaseParser => T): T = {
    val lexer = new IndexTables4SparkSqlBaseLexer(CharStreams.fromString(command))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser      = new IndexTables4SparkSqlBaseParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try
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
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    // Combined pattern: column/star KEYWORD 'query' (case-insensitive)
    // Matches: textsearch, fieldmatch, indexquery
    val operatorPattern = """(?i)(.+?)\s+(textsearch|fieldmatch|indexquery)\s+(.+)""".r

    // Check for indexqueryall function pattern (legacy)
    val indexQueryAllPattern = """indexqueryall\s*\(\s*(.+)\s*\)""".r

    sqlText.trim match {
      case operatorPattern(leftExpr, keyword, rightExpr) =>
        try {
          val trimmedLeft  = leftExpr.trim
          val lowerKeyword = keyword.toLowerCase
          val searchType = lowerKeyword match {
            case "textsearch" => "textsearch"
            case "fieldmatch" => "fieldmatch"
            case _            => "indexquery"
          }
          if (trimmedLeft == "*") {
            val right = delegate.parseExpression(rightExpr.trim)
            IndexQueryAllExpression(right, searchType)
          } else {
            val left  = delegate.parseExpression(trimmedLeft)
            val right = delegate.parseExpression(rightExpr.trim)
            IndexQueryExpression(left, right, searchType)
          }
        } catch {
          case e: ParseException =>
            delegate.parseExpression(sqlText)
        }

      case indexQueryAllPattern(queryExpr) =>
        try {
          val query = delegate.parseExpression(queryExpr.trim)
          IndexQueryAllExpression(query)
        } catch {
          case e: ParseException =>
            delegate.parseExpression(sqlText)
        }

      case _ =>
        delegate.parseExpression(sqlText)
    }
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    try
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
    catch {
      case _: Exception =>
        val preprocessedSql = preprocessIndexQueryOperators(sqlText)
        delegate.parseQuery(preprocessedSql)
    }

  /**
   * Preprocess SQL text to convert search operators to function calls that Spark can parse.
   * Uses a single combined regex pass for all operator variants:
   *   - TEXTSEARCH / FIELDMATCH / indexquery (column and * forms)
   *   - indexqueryall() legacy function
   *   - _indexall indexquery legacy form
   */
  private def preprocessIndexQueryOperators(sqlText: String): String = {
    logger.debug(s"Preprocessing SQL: $sqlText")

    // Combined pattern handles all operator forms in one pass:
    // Group 1: star or column name, Group 2: keyword (textsearch|fieldmatch|indexquery), Group 3: query string
    val operatorPattern = """(?i)(\*|[`]?[\w.]+[`]?)\s+(textsearch|fieldmatch|indexquery)\s+'([^']*)'""".r

    // Legacy indexqueryall('query') but NOT tantivy4spark_indexqueryall
    val indexQueryAllPattern = """(?<!tantivy4spark_)indexqueryall\s*\(\s*'([^']*)'\s*\)""".r

    // Legacy _indexall indexquery 'query'
    val indexAllQueryPattern = """_indexall\s+indexquery\s+'([^']*)'""".r

    // Pass 1: Handle _indexall before combined pattern (since _indexall matches [\w.]+ in the Pass 2 regex).
    // IMPORTANT: This ordering is required — swapping passes would silently break _indexall handling.
    val afterIndexAll = indexAllQueryPattern.replaceAllIn(
      sqlText,
      m => {
        val queryString = m.group(1)
        logger.debug(s"Converting _indexall indexquery '$queryString' to function call")
        s"tantivy4spark_indexqueryall('$queryString')"
      }
    )

    // Pass 2: Combined operator pattern (handles *, column textsearch/fieldmatch/indexquery)
    val afterOperators = operatorPattern.replaceAllIn(
      afterIndexAll,
      m => {
        val target      = m.group(1)
        val keyword     = m.group(2).toLowerCase
        val queryString = m.group(3)

        if (target == "*") {
          // * KEYWORD 'query' -> all-fields function
          val funcName = keyword match {
            case "textsearch" => "tantivy4spark_textsearchall"
            case "fieldmatch" => "tantivy4spark_fieldmatchall"
            case _            => "tantivy4spark_indexqueryall"
          }
          logger.debug(s"Converting * $keyword '$queryString' to $funcName")
          s"$funcName('$queryString')"
        } else {
          // column KEYWORD 'query' -> column-specific function
          val columnName = target.replace("`", "")
          val funcName = keyword match {
            case "textsearch" => "tantivy4spark_textsearch"
            case "fieldmatch" => "tantivy4spark_fieldmatch"
            case _            => "tantivy4spark_indexquery"
          }
          logger.debug(s"Converting $columnName $keyword '$queryString' to $funcName")
          s"$funcName('$columnName', '$queryString')"
        }
      }
    )

    // Pass 3: Legacy indexqueryall() function (not already converted)
    val result = indexQueryAllPattern.replaceAllIn(
      afterOperators,
      m => {
        val queryString = m.group(1)
        logger.debug(s"Converting indexqueryall('$queryString') to function call")
        s"tantivy4spark_indexqueryall('$queryString')"
      }
    )

    if (result != sqlText) {
      logger.debug(s"Preprocessed SQL: $result")
    }

    result
  }
}
