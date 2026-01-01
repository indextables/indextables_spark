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

import io.indextables.spark.sql.{
  DescribeDiskCacheCommand,
  DescribeEnvironmentCommand,
  DescribeStorageStatsCommand,
  DescribeTransactionLogCommand,
  DropPartitionsCommand,
  FlushDiskCacheCommand,
  FlushIndexTablesCacheCommand,
  InvalidateTransactionLogCacheCommand,
  MergeSplitsCommand,
  PrewarmCacheCommand,
  PurgeOrphanedSplitsCommand,
  RepairIndexFilesTransactionLogCommand
}
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

      // Extract MAX DEST SPLITS (formerly MAX GROUPS)
      val maxDestSplits = if (ctx.maxDestSplits != null) {
        logger.debug(s"Found MAX DEST SPLITS: ${ctx.maxDestSplits.getText}")
        try {
          val maxDestSplitsValue = parseAlphanumericInt(ctx.maxDestSplits.getText)
          if (maxDestSplitsValue <= 0) {
            throw new IllegalArgumentException(s"MAX DEST SPLITS must be positive, got: $maxDestSplitsValue")
          }
          Some(maxDestSplitsValue)
        } catch {
          case _: NumberFormatException =>
            throw new NumberFormatException(s"Invalid MAX DEST SPLITS value: ${ctx.maxDestSplits.getText}")
        }
      } else {
        logger.debug("No MAX DEST SPLITS")
        None
      }

      // Extract MAX SOURCE SPLITS PER MERGE
      val maxSourceSplitsPerMerge = if (ctx.maxSourceSplitsPerMerge != null) {
        logger.debug(s"Found MAX SOURCE SPLITS PER MERGE: ${ctx.maxSourceSplitsPerMerge.getText}")
        try {
          val maxSourceSplitsValue = parseAlphanumericInt(ctx.maxSourceSplitsPerMerge.getText)
          if (maxSourceSplitsValue < 2) {
            throw new IllegalArgumentException(s"MAX SOURCE SPLITS PER MERGE must be at least 2, got: $maxSourceSplitsValue")
          }
          Some(maxSourceSplitsValue)
        } catch {
          case _: NumberFormatException =>
            throw new NumberFormatException(s"Invalid MAX SOURCE SPLITS PER MERGE value: ${ctx.maxSourceSplitsPerMerge.getText}")
        }
      } else {
        logger.debug("No MAX SOURCE SPLITS PER MERGE")
        None
      }

      // Extract PRECOMMIT flag
      val preCommit = ctx.PRECOMMIT() != null
      logger.debug(s"PRECOMMIT flag: $preCommit")

      // Create command
      logger.debug(
        s"Creating MergeSplitsCommand with pathOption=$pathOption, tableIdOption=$tableIdOption, maxDestSplits=$maxDestSplits, maxSourceSplitsPerMerge=$maxSourceSplitsPerMerge"
      )
      val result = MergeSplitsCommand.apply(
        pathOption,
        tableIdOption,
        wherePredicates,
        targetSize,
        maxDestSplits,
        maxSourceSplitsPerMerge,
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

  override def visitPurgeIndexTable(ctx: PurgeIndexTableContext): LogicalPlan = {
    logger.debug(s"visitPurgeIndexTable called with context: $ctx")
    logger.debug(s"ctx.path = ${ctx.path}, ctx.table = ${ctx.table}")

    try {
      // Extract table path or identifier
      val tablePath = if (ctx.path != null) {
        logger.debug(s"Processing path: ${ctx.path.getText}")
        val pathStr = ParserUtils.string(ctx.path)
        logger.debug(s"Parsed path: $pathStr")
        pathStr
      } else if (ctx.table != null) {
        logger.debug(s"Processing table: ${ctx.table.getText}")
        val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
        logger.debug(s"Parsed table ID: $tableId")
        tableId.mkString(".")
      } else {
        throw new IllegalArgumentException("PURGE INDEXTABLE requires either a path or table identifier")
      }

      // Extract retention period (convert to hours)
      val retentionHours: Option[Long] = if (ctx.retentionNumber != null && ctx.retentionUnit != null) {
        val number = ctx.retentionNumber.getText.toLong
        val unit   = ctx.retentionUnit.getText.toUpperCase

        val hours = unit match {
          case "DAYS"  => number * 24
          case "HOURS" => number
          case other   => throw new IllegalArgumentException(s"Invalid retention unit: $other")
        }

        logger.debug(s"Found retention: $number $unit = $hours hours")
        Some(hours)
      } else {
        logger.debug("No retention period specified, will use default")
        None
      }

      // Extract transaction log retention period (convert to milliseconds)
      val txLogRetentionDuration: Option[Long] =
        if (ctx.txLogRetentionNumber != null && ctx.txLogRetentionUnit != null) {
          val number = ctx.txLogRetentionNumber.getText.toLong
          val unit   = ctx.txLogRetentionUnit.getText.toUpperCase

          val milliseconds = unit match {
            case "DAYS"  => number * 24 * 60 * 60 * 1000
            case "HOURS" => number * 60 * 60 * 1000
            case other   => throw new IllegalArgumentException(s"Invalid transaction log retention unit: $other")
          }

          logger.debug(s"Found transaction log retention: $number $unit = $milliseconds ms")
          Some(milliseconds)
        } else {
          logger.debug("No transaction log retention period specified, will use default")
          None
        }

      // Extract DRY RUN flag
      val dryRun = ctx.DRY() != null && ctx.RUN() != null
      logger.debug(s"DRY RUN flag: $dryRun")

      // Create command (use OneRowRelation as child - standard pattern for commands that don't have logical plan children)
      logger.debug(s"Creating PurgeOrphanedSplitsCommand with tablePath=$tablePath, retentionHours=$retentionHours, txLogRetentionDuration=$txLogRetentionDuration, dryRun=$dryRun")
      val result = PurgeOrphanedSplitsCommand(
        child = org.apache.spark.sql.catalyst.plans.logical.OneRowRelation(),
        tablePath = tablePath,
        retentionHours = retentionHours,
        txLogRetentionDuration = txLogRetentionDuration,
        dryRun = dryRun
      )
      logger.debug(s"Created PurgeOrphanedSplitsCommand: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Exception in visitPurgeIndexTable: ${e.getMessage}", e)
        throw e
    }
  }

  override def visitDropPartitions(ctx: DropPartitionsContext): LogicalPlan = {
    logger.debug(s"visitDropPartitions called with context: $ctx")
    logger.debug(s"ctx.path = ${ctx.path}, ctx.table = ${ctx.table}")

    try {
      // Extract table path or identifier
      val (pathOption, tableIdOption) = if (ctx.path != null) {
        logger.debug(s"Processing path: ${ctx.path.getText}")
        val pathStr = ParserUtils.string(ctx.path)
        logger.debug(s"Parsed path: $pathStr")
        (Some(pathStr), None)
      } else if (ctx.table != null) {
        logger.debug(s"Processing table: ${ctx.table.getText}")
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
        throw new IllegalArgumentException("DROP INDEXTABLES PARTITIONS requires either a path or table identifier")
      }

      // Extract WHERE clause - REQUIRED for this command
      val wherePredicates = if (ctx.whereClause != null) {
        val originalText = extractRawText(ctx.whereClause)
        logger.debug(s"Found WHERE clause: $originalText")
        Seq(originalText)
      } else {
        throw new IllegalArgumentException(
          "DROP INDEXTABLES PARTITIONS requires a WHERE clause specifying partition predicates"
        )
      }

      // Create command
      logger.debug(s"Creating DropPartitionsCommand with pathOption=$pathOption, tableIdOption=$tableIdOption")
      val result = DropPartitionsCommand.apply(pathOption, tableIdOption, wherePredicates)
      logger.debug(s"Created DropPartitionsCommand: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Exception in visitDropPartitions: ${e.getMessage}", e)
        throw e
    }
  }

  override def visitFlushIndexTablesCache(ctx: FlushIndexTablesCacheContext): LogicalPlan =
    FlushIndexTablesCacheCommand()

  override def visitFlushDiskCache(ctx: FlushDiskCacheContext): LogicalPlan =
    FlushDiskCacheCommand()

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

  override def visitDescribeTransactionLog(ctx: DescribeTransactionLogContext): LogicalPlan = {
    logger.debug(s"visitDescribeTransactionLog called with context: $ctx")
    logger.debug(s"ctx.path = ${ctx.path}, ctx.table = ${ctx.table}")

    try {
      // Extract table path or identifier
      val tablePath = if (ctx.path != null) {
        logger.debug(s"Processing path: ${ctx.path.getText}")
        val pathStr = ParserUtils.string(ctx.path)
        logger.debug(s"Parsed path: $pathStr")
        pathStr
      } else if (ctx.table != null) {
        logger.debug(s"Processing table: ${ctx.table.getText}")
        val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
        logger.debug(s"Parsed table ID: $tableId")
        tableId.mkString(".")
      } else {
        throw new IllegalArgumentException(
          "DESCRIBE INDEXTABLES TRANSACTION LOG requires either a path or table identifier"
        )
      }

      // Extract INCLUDE ALL flag
      val includeAll = ctx.INCLUDE() != null && ctx.ALL() != null
      logger.debug(s"INCLUDE ALL flag: $includeAll")

      // Create command (use OneRowRelation as child - standard pattern for commands that don't have logical plan children)
      logger.debug(s"Creating DescribeTransactionLogCommand with tablePath=$tablePath, includeAll=$includeAll")
      val result = DescribeTransactionLogCommand(
        child = org.apache.spark.sql.catalyst.plans.logical.OneRowRelation(),
        tablePath = tablePath,
        includeAll = includeAll
      )
      logger.debug(s"Created DescribeTransactionLogCommand: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Exception in visitDescribeTransactionLog: ${e.getMessage}", e)
        throw e
    }
  }

  override def visitDescribeDiskCache(ctx: DescribeDiskCacheContext): LogicalPlan = {
    logger.debug("visitDescribeDiskCache called")
    val result = DescribeDiskCacheCommand()
    logger.debug(s"Created DescribeDiskCacheCommand: $result")
    result
  }

  override def visitDescribeStorageStats(ctx: DescribeStorageStatsContext): LogicalPlan = {
    logger.debug("visitDescribeStorageStats called")
    val result = DescribeStorageStatsCommand()
    logger.debug(s"Created DescribeStorageStatsCommand: $result")
    result
  }

  override def visitDescribeEnvironment(ctx: DescribeEnvironmentContext): LogicalPlan = {
    logger.debug("visitDescribeEnvironment called")
    val result = DescribeEnvironmentCommand()
    logger.debug(s"Created DescribeEnvironmentCommand: $result")
    result
  }

  override def visitPrewarmCache(ctx: PrewarmCacheContext): LogicalPlan = {
    logger.debug(s"visitPrewarmCache called with context: $ctx")
    logger.debug(s"ctx.path = ${ctx.path}, ctx.table = ${ctx.table}")

    try {
      // Extract table path or identifier
      val tablePath = if (ctx.path != null) {
        logger.debug(s"Processing path: ${ctx.path.getText}")
        val pathStr = ParserUtils.string(ctx.path)
        logger.debug(s"Parsed path: $pathStr")
        pathStr
      } else if (ctx.table != null) {
        logger.debug(s"Processing table: ${ctx.table.getText}")
        val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
        logger.debug(s"Parsed table ID: $tableId")
        tableId.mkString(".")
      } else {
        throw new IllegalArgumentException("PREWARM INDEXTABLES CACHE requires either a path or table identifier")
      }

      // Extract segment list (FOR SEGMENTS clause)
      val segments: Seq[String] = if (ctx.segmentList != null) {
        val segList = ctx.segmentList.identifier().asScala.map(_.getText.toUpperCase).toSeq
        logger.debug(s"Found segments: ${segList.mkString(", ")}")
        segList
      } else {
        logger.debug("No segments specified, will use defaults")
        Seq.empty
      }

      // Extract field list (ON FIELDS clause)
      val fields: Option[Seq[String]] = if (ctx.fieldList != null) {
        val fieldSeq = ctx.fieldList.identifier().asScala.map(_.getText).toSeq
        logger.debug(s"Found fields: ${fieldSeq.mkString(", ")}")
        Some(fieldSeq)
      } else {
        logger.debug("No fields specified, will prewarm all fields")
        None
      }

      // Extract parallelism (WITH PERWORKER PARALLELISM OF N)
      val splitsPerTask: Int = if (ctx.parallelism != null) {
        val parallelismValue = ctx.parallelism.getText.toInt
        logger.debug(s"Found parallelism: $parallelismValue")
        if (parallelismValue <= 0) {
          throw new IllegalArgumentException(s"PERWORKER PARALLELISM must be positive, got: $parallelismValue")
        }
        parallelismValue
      } else {
        logger.debug("No parallelism specified, will use default (2)")
        2 // Default: 2 splits per task
      }

      // Extract WHERE clause for partition filtering
      val wherePredicates: Seq[String] = if (ctx.whereClause != null) {
        val originalText = extractRawText(ctx.whereClause)
        logger.debug(s"Found WHERE clause: $originalText")
        Seq(originalText)
      } else {
        logger.debug("No WHERE clause")
        Seq.empty
      }

      // Create command
      logger.debug(
        s"Creating PrewarmCacheCommand with tablePath=$tablePath, segments=$segments, " +
          s"fields=$fields, splitsPerTask=$splitsPerTask, wherePredicates=$wherePredicates"
      )
      val result = PrewarmCacheCommand(
        tablePath = tablePath,
        segments = segments,
        fields = fields,
        splitsPerTask = splitsPerTask,
        wherePredicates = wherePredicates
      )
      logger.debug(s"Created PrewarmCacheCommand: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Exception in visitPrewarmCache: ${e.getMessage}", e)
        throw e
    }
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
