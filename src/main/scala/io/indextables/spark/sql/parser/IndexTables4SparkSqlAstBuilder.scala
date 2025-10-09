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

package io.indextables.spark.sql.parser

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.TableIdentifier

import io.indextables.spark.sql.{FlushIndexTablesCacheCommand, InvalidateTransactionLogCacheCommand, MergeSplitsCommand, RepairIndexFilesTransactionLogCommand}
import io.indextables.spark.sql.parser.IndexTables4SparkSqlBaseParser._
import org.antlr.v4.runtime.ParserRuleContext
import org.slf4j.LoggerFactory

/** Builder that converts ANTLR parse trees into Catalyst logical plans. */
class IndexTables4SparkSqlAstBuilder extends IndexTables4SparkSqlBaseBaseVisitor[AnyRef] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def visit(tree: org.antlr.v4.runtime.tree.ParseTree): AnyRef = {
    logger.debug(s"AST Builder visit() called with tree type: ${tree.getClass.getSimpleName}")
    val result = super.visit(tree)
    logger.debug(
      s"AST Builder visit() result: $result, type: ${if (result != null) result.getClass.getName else "null"}"
    )
    result
  }

  override def visitSingleStatement(ctx: SingleStatementContext): AnyRef = {
    logger.debug("visitSingleStatement called")
    // Visit the statement child directly instead of calling super
    val result = visit(ctx.statement())
    logger.debug(
      s"visitSingleStatement result: $result, type: ${if (result != null) result.getClass.getName else "null"}"
    )
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
        try
          Some(parseAlphanumericSize(ctx.targetSize.getText))
        catch {
          case _: NumberFormatException =>
            throw new NumberFormatException(s"Invalid target size: ${ctx.targetSize.getText}")
        }
      } else {
        logger.debug("No TARGET SIZE")
        None
      }

      // Extract MAX GROUPS
      val maxGroups = if (ctx.maxGroups != null) {
        logger.debug(s"Found MAX GROUPS: ${ctx.maxGroups.getText}")
        try {
          val maxGroupsValue = parseAlphanumericInt(ctx.maxGroups.getText)
          if (maxGroupsValue <= 0) {
            throw new IllegalArgumentException(s"MAX GROUPS must be positive, got: $maxGroupsValue")
          }
          Some(maxGroupsValue)
        } catch {
          case _: NumberFormatException =>
            throw new NumberFormatException(s"Invalid MAX GROUPS value: ${ctx.maxGroups.getText}")
        }
      } else {
        logger.debug("No MAX GROUPS")
        None
      }

      // Extract PRECOMMIT flag
      val preCommit = ctx.PRECOMMIT() != null
      logger.debug(s"PRECOMMIT flag: $preCommit")

      // Create command
      logger.debug(
        s"Creating MergeSplitsCommand with pathOption=$pathOption, tableIdOption=$tableIdOption, maxGroups=$maxGroups"
      )
      val result = MergeSplitsCommand.apply(
        pathOption,
        tableIdOption,
        wherePredicates,
        targetSize,
        maxGroups,
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

  override def visitFlushIndexTablesCache(ctx: FlushIndexTablesCacheContext): LogicalPlan =
    FlushIndexTablesCacheCommand()

  override def visitInvalidateIndexTablesTransactionLogCache(ctx: InvalidateIndexTablesTransactionLogCacheContext)
    : LogicalPlan = {
    logger.debug(s"visitInvalidateTantivyTransactionLogCache called with context: $ctx")

    // Extract table path or identifier if provided
    val pathOption = if (ctx.path != null) {
      logger.debug(s"Processing path: ${ctx.path.getText}")
      val pathStr = ParserUtils.string(ctx.path)
      logger.debug(s"Parsed path: $pathStr")
      Some(pathStr)
    } else if (ctx.table != null) {
      logger.debug(s"Processing table: ${ctx.table.getText}")
      val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
      logger.debug(s"Parsed table ID: $tableId")
      Some(tableId.mkString("."))
    } else {
      logger.debug("No path or table specified - global cache invalidation")
      None
    }

    val result = InvalidateTransactionLogCacheCommand(pathOption)
    logger.debug(s"Created InvalidateTransactionLogCacheCommand: $result")
    result
  }

  override def visitRepairIndexFilesTransactionLog(ctx: RepairIndexFilesTransactionLogContext): LogicalPlan = {
    logger.debug(s"visitRepairIndexFilesTransactionLog called with context: $ctx")

    // Extract source path
    val sourcePath = if (ctx.sourcePath != null) {
      val pathStr = ParserUtils.string(ctx.sourcePath)
      logger.debug(s"Parsed source path: $pathStr")
      pathStr
    } else {
      throw new IllegalArgumentException("REPAIR INDEXFILES TRANSACTION LOG requires a source path")
    }

    // Extract target path
    val targetPath = if (ctx.targetPath != null) {
      val pathStr = ParserUtils.string(ctx.targetPath)
      logger.debug(s"Parsed target path: $pathStr")
      pathStr
    } else {
      throw new IllegalArgumentException("REPAIR INDEXFILES TRANSACTION LOG requires AT LOCATION target path")
    }

    val result = RepairIndexFilesTransactionLogCommand(sourcePath, targetPath)
    logger.debug(s"Created RepairIndexFilesTransactionLogCommand: $result")
    result
  }

  override def visitPassThrough(ctx: PassThroughContext): AnyRef = {
    logger.debug(s"visitPassThrough called with context: $ctx")
    null // This indicates the delegate parser should handle it
  }

  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] =
    ctx.identifier().asScala.map(_.getText).toSeq

  /** Parse alphanumeric size value (e.g., "100M", "5G", "1024", "-500M") */
  private def parseAlphanumericSize(value: String): Long = {
    val trimmed = value.trim.stripPrefix("'").stripSuffix("'") // Remove quotes if present

    // Handle negative sign
    val isNegative    = trimmed.startsWith("-")
    val absoluteValue = if (isNegative) trimmed.substring(1) else trimmed

    // Extract number and suffix
    val (numStr, suffix) = if (absoluteValue.matches("\\d+[MmGg]")) {
      (absoluteValue.init, absoluteValue.last.toString.toUpperCase)
    } else if (absoluteValue.matches("\\d+")) {
      (absoluteValue, "")
    } else {
      throw new NumberFormatException(s"Invalid size format: $value")
    }

    val baseValue =
      try
        numStr.toLong
      catch {
        case _: NumberFormatException => throw new NumberFormatException(s"Invalid numeric value: $numStr")
      }

    val multiplier = suffix match {
      case ""    => 1L
      case "M"   => 1024L * 1024L         // Megabytes
      case "G"   => 1024L * 1024L * 1024L // Gigabytes
      case other => throw new IllegalArgumentException(s"Unsupported size suffix: $other")
    }

    val result = baseValue * multiplier
    if (isNegative) -result else result
  }

  /** Parse alphanumeric integer value (e.g., "5", "-10") */
  private def parseAlphanumericInt(value: String): Int = {
    val trimmed = value.trim.stripPrefix("'").stripSuffix("'") // Remove quotes if present
    try
      trimmed.toInt
    catch {
      case _: NumberFormatException => throw new NumberFormatException(s"Invalid integer format: $value")
    }
  }

  /**
   * Extract raw text from a parser rule context (Delta Lake approach). This preserves the original formatting including
   * spacing.
   */
  private def extractRawText(exprContext: ParserRuleContext): String = {
    import org.antlr.v4.runtime.misc.Interval
    // Extract the raw expression which will be parsed later
    exprContext.getStart.getInputStream.getText(
      new Interval(exprContext.getStart.getStartIndex, exprContext.getStop.getStopIndex)
    )
  }

  // No need to override defaultResult() - let ANTLR handle it naturally
}
