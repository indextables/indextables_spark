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
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.slf4j.LoggerFactory

/**
 * SQL command to purge orphaned split files from IndexTables4Spark tables.
 *
 * Syntax: PURGE ORPHANED SPLITS '/path/to/table' PURGE ORPHANED SPLITS '/path/to/table' OLDER THAN 7 DAYS PURGE
 * ORPHANED SPLITS '/path/to/table' OLDER THAN 168 HOURS PURGE ORPHANED SPLITS '/path/to/table' DRY RUN PURGE ORPHANED
 * SPLITS '/path/to/table' OLDER THAN 14 DAYS DRY RUN
 *
 * This command:
 *   1. Lists all .split and .crc files in the table directory (distributed across executors) 2. Loads all referenced
 *      split files from transaction log (includes checkpoints) 3. Computes orphaned files (files in filesystem but not
 *      in transaction log) 4. Applies retention filter (only delete files older than retention period) 5. Deletes
 *      orphaned files (or returns preview if DRY RUN) 6. Returns metrics (deleted count, size, duration)
 *
 * Safety features:
 *   - Minimum retention period enforced (default 24 hours)
 *   - DRY RUN mode shows preview before deletion
 *   - LEFT ANTI JOIN ensures only truly orphaned files are deleted
 *   - Retry logic handles transient cloud storage errors
 *   - Graceful partial failure (continues deleting even if some files fail)
 */
case class PurgeOrphanedSplitsCommand(
  override val child: LogicalPlan,
  tablePath: String,
  retentionHours: Option[Long],
  txLogRetentionDuration: Option[Long],
  dryRun: Boolean)
    extends RunnableCommand
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[PurgeOrphanedSplitsCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): PurgeOrphanedSplitsCommand =
    copy(child = newChild)

  /** Default retention period: 7 days (168 hours) */
  val DEFAULT_RETENTION_HOURS: Long = 168L

  /** Minimum retention period: 24 hours (safety check) */
  val MIN_RETENTION_HOURS: Long = 24L

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference(
      "metrics",
      StructType(
        Seq(
          StructField("status", StringType, nullable = false),
          StructField("orphaned_files_found", LongType, nullable = true),
          StructField("orphaned_files_deleted", LongType, nullable = true),
          StructField("size_mb_deleted", DoubleType, nullable = true),
          StructField("retention_hours", LongType, nullable = true),
          StructField("transaction_logs_deleted", LongType, nullable = true),
          StructField("dry_run", BooleanType, nullable = false),
          StructField("duration_ms", LongType, nullable = true),
          StructField("message", StringType, nullable = true)
        )
      )
    )()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTime = System.currentTimeMillis()

    // Validate and normalize table path
    val normalizedPath = new Path(tablePath).toString
    logger.info(s"Starting PURGE ORPHANED SPLITS for table: $normalizedPath")
    logger.info(s"Parameters: retentionHours=$retentionHours, dryRun=$dryRun")

    // Validate retention period
    val effectiveRetention = retentionHours.getOrElse(DEFAULT_RETENTION_HOURS)
    validateRetentionPeriod(sparkSession, effectiveRetention)

    // Delegate to executor
    val executor = new PurgeOrphanedSplitsExecutor(
      sparkSession,
      normalizedPath,
      effectiveRetention,
      txLogRetentionDuration,
      dryRun
    )

    val result =
      try
        executor.purge()
      catch {
        case e: Exception =>
          logger.error(s"PURGE ORPHANED SPLITS failed: ${e.getMessage}", e)
          throw e
      }

    val duration = System.currentTimeMillis() - startTime
    logger.info(s"PURGE ORPHANED SPLITS completed in ${duration}ms")
    logger.info(s"Result: status=${result.status}, found=${result.orphanedFilesFound}, deleted=${result.orphanedFilesDeleted}, sizeMB=${result.sizeMBDeleted}")

    // Return result row
    Seq(
      Row(
        normalizedPath,
        Row(
          result.status,
          result.orphanedFilesFound,
          result.orphanedFilesDeleted,
          result.sizeMBDeleted,
          effectiveRetention,
          result.transactionLogsDeleted,
          dryRun,
          duration,
          result.message.orNull
        )
      )
    )
  }

  private def validateRetentionPeriod(
    spark: SparkSession,
    retentionHours: Long
  ): Unit = {
    require(retentionHours >= 0, s"Retention period cannot be negative: $retentionHours")

    val checkEnabled = spark.conf
      .getOption("spark.indextables.purge.retentionCheckEnabled")
      .forall(_ != "false") // Default true

    if (checkEnabled && retentionHours < MIN_RETENTION_HOURS) {
      throw new IllegalArgumentException(
        s"""Retention period $retentionHours hours is less than minimum $MIN_RETENTION_HOURS hours.
           |This operation may delete files from active transactions and corrupt the table.
           |To disable this check, set spark.indextables.purge.retentionCheckEnabled=false
           |""".stripMargin
      )
    }
  }
}

/**
 * Result of purge operation.
 *
 * @param status
 *   Status string: "SUCCESS", "DRY_RUN", "PARTIAL_SUCCESS", "FAILED"
 * @param orphanedFilesFound
 *   Total number of orphaned split files found
 * @param orphanedFilesDeleted
 *   Number of split files successfully deleted
 * @param sizeMBDeleted
 *   Total size in MB of deleted split files
 * @param transactionLogsDeleted
 *   Number of old transaction log files deleted
 * @param message
 *   Optional message with details
 */
case class PurgeResult(
  status: String,
  orphanedFilesFound: Long,
  orphanedFilesDeleted: Long,
  sizeMBDeleted: Double,
  transactionLogsDeleted: Long,
  message: Option[String])
