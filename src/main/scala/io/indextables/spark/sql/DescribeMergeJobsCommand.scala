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
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._

import io.indextables.spark.merge.{AsyncMergeOnWriteManager, MergeJobStatus}
import org.slf4j.LoggerFactory

/**
 * SQL command to describe async merge-on-write job status on the driver.
 *
 * Syntax:
 * {{{
 * DESCRIBE INDEXTABLES MERGE JOBS
 * DESCRIBE TANTIVY4SPARK MERGE JOBS
 * }}}
 *
 * Returns a DataFrame with async merge job status:
 *   - job_id: Unique job identifier
 *   - table_path: Path of the table being merged
 *   - status: Job status (RUNNING, COMPLETED, FAILED, CANCELLED)
 *   - total_groups: Total number of merge groups to process
 *   - completed_groups: Number of groups merged so far
 *   - total_batches: Total number of batches
 *   - completed_batches: Number of batches completed
 *   - progress_pct: Percentage progress (completed_groups/total_groups * 100)
 *   - duration_ms: Duration in milliseconds
 *   - error_message: Error message if failed (null otherwise)
 *
 * Note: Merge jobs run on the driver, so this command returns status from the driver-side AsyncMergeOnWriteManager.
 */
case class DescribeMergeJobsCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeMergeJobsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("job_id", StringType, nullable = false)(),
    AttributeReference("table_path", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("total_groups", IntegerType, nullable = false)(),
    AttributeReference("completed_groups", IntegerType, nullable = false)(),
    AttributeReference("total_batches", IntegerType, nullable = false)(),
    AttributeReference("completed_batches", IntegerType, nullable = false)(),
    AttributeReference("progress_pct", DoubleType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("error_message", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing async merge-on-write job status")

    try {
      // Get all job status from the manager
      val jobs = AsyncMergeOnWriteManager.getAllJobStatus

      if (jobs.isEmpty) {
        logger.info("No async merge jobs found")
        return Seq.empty
      }

      val rows = jobs.map { job =>
        val progressPct = if (job.totalGroups > 0) {
          (job.completedGroups.toDouble / job.totalGroups) * 100.0
        } else {
          0.0
        }

        Row(
          job.jobId,
          job.tablePath,
          job.status,
          job.totalGroups,
          job.completedGroups,
          job.totalBatches,
          job.completedBatches,
          progressPct,
          job.durationMs,
          job.errorMessage.orNull
        )
      }

      logger.info(s"Found ${rows.size} async merge jobs")
      rows

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to retrieve merge job status: ${e.getMessage}", e)
        Seq.empty
    }
  }
}
