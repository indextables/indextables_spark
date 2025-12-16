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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.xref.{XRefBuildManager, XRefBuildResult, XRefConfig}
import org.slf4j.LoggerFactory

/**
 * SQL command to build cross-reference (XRef) indexes for IndexTables4Spark tables.
 *
 * XRef indexes consolidate term dictionaries from multiple source splits into lightweight indexes that enable 10-100x
 * faster query routing for selective queries.
 *
 * Syntax:
 * {{{
 *   INDEX CROSSREFERENCES FOR '/path/to/table'
 *   INDEX CROSSREFERENCES FOR '/path/to/table' WHERE date = '2024-01-01'
 *   INDEX CROSSREFERENCES FOR '/path/to/table' FORCE REBUILD
 *   INDEX CROSSREFERENCES FOR '/path/to/table' DRY RUN
 *   INDEX CROSSREFERENCES FOR my_table FORCE REBUILD DRY RUN
 * }}}
 *
 * This command:
 *   1. Evaluates current XRef state from transaction log
 *   2. Determines which XRefs need to be built/rebuilt
 *   3. Builds new XRef splits using tantivy4java
 *   4. Atomically commits XRef changes to transaction log
 *   5. Returns build metrics
 *
 * Options:
 *   - WHERE clause: Build XRefs only for matching partitions
 *   - FORCE REBUILD: Rebuild all XRefs even if valid
 *   - MAX XREF BUILDS n: Limit number of XRefs built per run
 *   - DRY RUN: Preview what would be built without executing
 */
case class IndexCrossReferencesCommand(
  override val child: LogicalPlan,
  pathOption: Option[String],
  tableIdOption: Option[TableIdentifier],
  wherePredicates: Seq[String],
  forceRebuild: Boolean,
  dryRun: Boolean,
  maxXRefBuilds: Option[Int] = None)
    extends RunnableCommand
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[IndexCrossReferencesCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): IndexCrossReferencesCommand =
    copy(child = newChild)

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference(
      "result",
      StructType(
        Seq(
          StructField("action", StringType, nullable = false),
          StructField("xref_path", StringType, nullable = true),
          StructField("source_splits_count", IntegerType, nullable = false),
          StructField("total_terms", LongType, nullable = false),
          StructField("xref_size_bytes", LongType, nullable = false),
          StructField("build_duration_ms", LongType, nullable = false),
          StructField("force_rebuild", BooleanType, nullable = false),
          StructField("dry_run", BooleanType, nullable = false),
          StructField("error_message", StringType, nullable = true)
        )
      )
    )()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTime = System.currentTimeMillis()

    // Resolve table path
    val tablePath = resolveTablePath(sparkSession)
    val normalizedPath = new Path(tablePath).toString

    logger.info(s"Starting INDEX CROSSREFERENCES for table: $normalizedPath")
    logger.info(s"Parameters: forceRebuild=$forceRebuild, dryRun=$dryRun, maxXRefBuilds=$maxXRefBuilds, wherePredicates=$wherePredicates")

    try {
      // Create transaction log
      val transactionLog = TransactionLogFactory.create(new Path(normalizedPath), sparkSession)

      try {
        // Create XRef build manager with optional SQL override for maxXRefsPerRun
        val baseConfig = XRefConfig.fromSparkSession(sparkSession)
        val xrefConfig = maxXRefBuilds match {
          case Some(limit) =>
            // Override config with SQL-specified limit
            baseConfig.copy(build = baseConfig.build.copy(maxXRefsPerRun = Some(limit)))
          case None => baseConfig
        }
        val buildManager = new XRefBuildManager(
          tablePath = normalizedPath,
          transactionLog = transactionLog,
          config = xrefConfig,
          sparkSession = sparkSession
        )

        // Parse WHERE clause predicates into Expression
        val whereClause = if (wherePredicates.nonEmpty) {
          val partitionColumns = transactionLog.getPartitionColumns()
          if (partitionColumns.isEmpty) {
            throw new IllegalArgumentException(
              "WHERE clause not supported for non-partitioned tables"
            )
          }
          val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns)
          val parsedPredicates = PartitionPredicateUtils.parseAndValidatePredicates(
            wherePredicates, partitionSchema, sparkSession
          )
          // Combine multiple predicates with AND
          if (parsedPredicates.nonEmpty) {
            Some(parsedPredicates.reduce((a, b) =>
              org.apache.spark.sql.catalyst.expressions.And(a, b)
            ))
          } else {
            None
          }
        } else {
          None
        }

        // Execute build
        val results = buildManager.buildCrossReferences(
          whereClause = whereClause,
          forceRebuild = forceRebuild,
          dryRun = dryRun
        )

        val duration = System.currentTimeMillis() - startTime
        logger.info(s"INDEX CROSSREFERENCES completed in ${duration}ms with ${results.size} result(s)")

        // Convert results to rows
        results.map { result =>
          Row(
            normalizedPath,
            Row(
              result.action,
              result.xrefPath.orNull,
              result.sourceSplitsCount,
              result.totalTerms,
              result.xrefSizeBytes,
              result.buildDurationMs,
              forceRebuild,
              dryRun,
              result.errorMessage.orNull
            )
          )
        }
      } finally {
        transactionLog.close()
      }
    } catch {
      case e: Exception =>
        logger.error(s"INDEX CROSSREFERENCES failed: ${e.getMessage}", e)
        val duration = System.currentTimeMillis() - startTime

        // Return error row
        Seq(
          Row(
            normalizedPath,
            Row(
              "error",
              null,
              0,
              0L,
              0L,
              duration,
              forceRebuild,
              dryRun,
              e.getMessage
            )
          )
        )
    }
  }

  /**
   * Resolve table path from path option or table identifier.
   */
  private def resolveTablePath(sparkSession: SparkSession): String =
    pathOption match {
      case Some(path) => path
      case None =>
        tableIdOption match {
          case Some(tableId) =>
            // Resolve table identifier to path
            val catalog = sparkSession.catalog
            val tableName = tableId.database match {
              case Some(db) => s"$db.${tableId.table}"
              case None     => tableId.table
            }
            try {
              val table = catalog.getTable(tableName)
              // Try to get the location from table properties
              Option(table.tableType).filter(_ == "EXTERNAL") match {
                case Some(_) =>
                  // For external tables, get the location
                  sparkSession.sql(s"DESCRIBE EXTENDED $tableName")
                    .collect()
                    .find(row => row.getString(0) == "Location")
                    .map(_.getString(1))
                    .getOrElse(throw new IllegalArgumentException(
                      s"Cannot determine path for table $tableName. Please specify path directly."
                    ))
                case None =>
                  throw new IllegalArgumentException(
                    s"Table $tableName is not an external table. Please specify path directly."
                  )
              }
            } catch {
              case _: Exception =>
                throw new IllegalArgumentException(
                  s"Cannot resolve table $tableName. Please specify path directly."
                )
            }
          case None =>
            throw new IllegalArgumentException(
              "INDEX CROSSREFERENCES requires either a path or table identifier"
            )
        }
    }
}

object IndexCrossReferencesCommand {

  /**
   * Factory method to create command from parsed parameters.
   */
  def apply(
    pathOption: Option[String],
    tableIdOption: Option[TableIdentifier],
    wherePredicates: Seq[String],
    forceRebuild: Boolean,
    dryRun: Boolean,
    maxXRefBuilds: Option[Int]
  ): IndexCrossReferencesCommand =
    IndexCrossReferencesCommand(
      child = org.apache.spark.sql.catalyst.plans.logical.OneRowRelation(),
      pathOption = pathOption,
      tableIdOption = tableIdOption,
      wherePredicates = wherePredicates,
      forceRebuild = forceRebuild,
      dryRun = dryRun,
      maxXRefBuilds = maxXRefBuilds
    )
}
