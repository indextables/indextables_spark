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

package com.tantivy4spark.sql.parser

import com.tantivy4spark.sql.{FlushTantivyCacheCommand, MergeSplitsCommand}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.parser.ParserUtils
import com.tantivy4spark.sql.parser.Tantivy4SparkSqlBaseParser._
import org.antlr.v4.runtime.ParserRuleContext
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._

/**
 * Builder that converts ANTLR parse trees into Catalyst logical plans.
 */
class Tantivy4SparkSqlAstBuilder extends Tantivy4SparkSqlBaseBaseVisitor[AnyRef] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def visit(tree: org.antlr.v4.runtime.tree.ParseTree): AnyRef = {
    logger.debug(s"AST Builder visit() called with tree type: ${tree.getClass.getSimpleName}")
    val result = super.visit(tree)
    logger.debug(s"AST Builder visit() result: $result, type: ${if (result != null) result.getClass.getName else "null"}")
    result
  }

  override def visitSingleStatement(ctx: SingleStatementContext): AnyRef = {
    logger.debug("visitSingleStatement called")
    // Visit the statement child directly instead of calling super
    val result = visit(ctx.statement())
    logger.debug(s"visitSingleStatement result: $result, type: ${if (result != null) result.getClass.getName else "null"}")
    result
  }

  override def visitMergeSplitsTable(ctx: MergeSplitsTableContext): LogicalPlan = {
    logger.debug(s"visitMergeSplitsTable called with context: $ctx")
    logger.debug(s"ctx.path = ${ctx.path}, ctx.table = ${ctx.table}")
    
    try {
      // Extract table path or identifier
      val (pathOption, tableIdOption) = if (ctx.path != null) {
        logger.debug(s"Processing path: ${ctx.path.getText}")
        // Quoted path - remove quotes
        val pathStr = ParserUtils.string(ctx.path)
        logger.debug(s"Parsed path: $pathStr")
        (Some(pathStr), None)
      } else if (ctx.table != null) {
        logger.debug(s"Processing table: ${ctx.table.getText}")
        // Table identifier
        val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
        logger.debug(s"Parsed table ID: $tableId")
        val tableIdentifier = if (tableId.length == 1) {
          Some(TableIdentifier(tableId.head))
        } else if (tableId.length == 2) {
          Some(TableIdentifier(tableId(1), Some(tableId.head)))
        } else {
          throw new IllegalArgumentException(s"Invalid table identifier: ${tableId.mkString(".")}")
        }
        (None, tableIdentifier)
      } else {
        logger.error("Neither path nor table found")
        throw new IllegalArgumentException("MERGE SPLITS requires either a path or table identifier")
      }

      // Extract WHERE clause - use Delta Lake approach to preserve original spacing
      val wherePredicates = if (ctx.whereClause != null) {
        val originalText = extractRawText(ctx.whereClause)
        logger.debug(s"Found WHERE clause: $originalText")
        Seq(originalText)
      } else {
        logger.debug("No WHERE clause")
        Seq.empty
      }

      // Extract TARGET SIZE
      val targetSize = if (ctx.targetSize != null) {
        logger.debug(s"Found TARGET SIZE: ${ctx.targetSize.getText}")
        try {
          Some(visitSizeValue(ctx.targetSize).asInstanceOf[Long])
        } catch {
          case _: NumberFormatException =>
            throw new NumberFormatException(s"Invalid target size: ${ctx.targetSize.getText}")
        }
      } else {
        logger.debug("No TARGET SIZE")
        None
      }

      // Extract PRECOMMIT flag
      val preCommit = ctx.PRECOMMIT() != null
      logger.debug(s"PRECOMMIT flag: $preCommit")

      // Create command
      logger.debug(s"Creating MergeSplitsCommand with pathOption=$pathOption, tableIdOption=$tableIdOption")
      val result = MergeSplitsCommand.apply(
        pathOption,
        tableIdOption,
        wherePredicates,
        targetSize,
        preCommit
      )
      logger.debug(s"Created MergeSplitsCommand: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Exception in visitMergeSplitsTable: ${e.getMessage}", e)
        throw e
    }
  }

  override def visitFlushTantivyCache(ctx: FlushTantivyCacheContext): LogicalPlan = {
    FlushTantivyCacheCommand()
  }

  override def visitPassThrough(ctx: PassThroughContext): AnyRef = {
    logger.debug(s"visitPassThrough called with context: $ctx")
    null // This indicates the delegate parser should handle it
  }

  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = {
    ctx.identifier().asScala.map(_.getText).toSeq
  }

  override def visitSizeValue(ctx: SizeValueContext): AnyRef = {
    logger.debug(s"visitSizeValue called with context: $ctx")
    
    // Handle negative sign
    val isNegative = ctx.getText.startsWith("-")
    val baseValue = ctx.INTEGER_VALUE().getText.toLong
    val signedValue = if (isNegative) -baseValue else baseValue
    logger.debug(s"Base integer value: $signedValue")
    
    val multiplier = if (ctx.sizeSuffix() != null) {
      val suffixText = ctx.sizeSuffix().getText
      logger.debug(s"Found size suffix: $suffixText")
      suffixText match {
        case "M" => 1024L * 1024L      // Megabytes
        case "G" => 1024L * 1024L * 1024L  // Gigabytes
        case other => throw new IllegalArgumentException(s"Unsupported size suffix: $other")
      }
    } else {
      logger.debug("No size suffix, using base value")
      1L
    }
    
    val result = signedValue * multiplier
    logger.debug(s"Final size value: $result bytes")
    java.lang.Long.valueOf(result)
  }

  /**
   * Extract raw text from a parser rule context (Delta Lake approach).
   * This preserves the original formatting including spacing.
   */
  private def extractRawText(exprContext: ParserRuleContext): String = {
    import org.antlr.v4.runtime.misc.Interval
    // Extract the raw expression which will be parsed later
    exprContext.getStart.getInputStream.getText(new Interval(
      exprContext.getStart.getStartIndex,
      exprContext.getStop.getStopIndex))
  }

  // No need to override defaultResult() - let ANTLR handle it naturally
}