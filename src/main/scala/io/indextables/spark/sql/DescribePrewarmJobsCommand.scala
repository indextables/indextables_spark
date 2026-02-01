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

import io.indextables.spark.prewarm.{AsyncPrewarmJobManager, PrewarmJobStatus}
import org.slf4j.LoggerFactory

/**
 * SQL command to describe async prewarm job status across all executors.
 *
 * Syntax:
 * {{{
 * DESCRIBE INDEXTABLES PREWARM JOBS
 * DESCRIBE TANTIVY4SPARK PREWARM JOBS
 * }}}
 *
 * Returns a DataFrame with async prewarm job status from all executors:
 *   - executor_id: Identifier of the executor
 *   - host: Host address (ip:port) of the executor
 *   - job_id: Unique job identifier
 *   - table_path: Path of the table being prewarmed
 *   - status: Job status (RUNNING, COMPLETED, FAILED, CANCELLED)
 *   - total_splits: Total number of splits to prewarm
 *   - completed_splits: Number of splits prewarmed so far
 *   - progress_pct: Percentage progress (completed/total * 100)
 *   - duration_ms: Duration in milliseconds
 *   - error_message: Error message if failed (null otherwise)
 *
 * Note: Each executor maintains its own async prewarm job state. This command aggregates job status from all active
 * executors in the cluster.
 */
case class DescribePrewarmJobsCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribePrewarmJobsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("executor_id", StringType, nullable = false)(),
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("job_id", StringType, nullable = false)(),
    AttributeReference("table_path", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("total_splits", IntegerType, nullable = false)(),
    AttributeReference("completed_splits", IntegerType, nullable = false)(),
    AttributeReference("progress_pct", DoubleType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("error_message", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing async prewarm job status across all executors")

    val sc = sparkSession.sparkContext

    // Get the driver's block manager to exclude it from executor count
    val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
    val driverHostPort       = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

    // Count only actual executors (exclude driver), at least 1 for local mode
    val numExecutors = math.max(1, sc.getExecutorMemoryStatus.keys.count(_ != driverHostPort))

    try {
      // Collect job status from all executors
      val executorJobs = sc
        .parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val executorId     = s"executor-$index"
          val blockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
          val host           = s"${blockManagerId.host}:${blockManagerId.port}"

          // Get all job status from this executor
          val jobs = AsyncPrewarmJobManager.getAllJobStatus

          if (jobs.isEmpty) {
            Iterator.empty
          } else {
            jobs.map { job =>
              val progressPct = if (job.totalSplits > 0) {
                (job.completedSplits.toDouble / job.totalSplits) * 100.0
              } else {
                0.0
              }

              Row(
                executorId,
                host,
                job.jobId,
                job.tablePath,
                job.status,
                job.totalSplits,
                job.completedSplits,
                progressPct,
                job.durationMs,
                job.errorMessage.orNull
              )
            }.iterator
          }
        }
        .collect()
        .toSeq

      // Also get driver jobs
      val driverJobs = AsyncPrewarmJobManager.getAllJobStatus.map { job =>
        val progressPct = if (job.totalSplits > 0) {
          (job.completedSplits.toDouble / job.totalSplits) * 100.0
        } else {
          0.0
        }

        Row(
          "driver",
          driverHostPort,
          job.jobId,
          job.tablePath,
          job.status,
          job.totalSplits,
          job.completedSplits,
          progressPct,
          job.durationMs,
          job.errorMessage.orNull
        )
      }

      // Return driver + executor jobs
      val allJobs = driverJobs ++ executorJobs
      logger.info(s"Found ${allJobs.size} async prewarm jobs across cluster")
      allJobs

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor job status, returning driver-only status: ${e.getMessage}")
        // Fallback to driver-only jobs
        val driverJobs = AsyncPrewarmJobManager.getAllJobStatus.map { job =>
          val progressPct = if (job.totalSplits > 0) {
            (job.completedSplits.toDouble / job.totalSplits) * 100.0
          } else {
            0.0
          }

          Row(
            "driver",
            driverHostPort,
            job.jobId,
            job.tablePath,
            job.status,
            job.totalSplits,
            job.completedSplits,
            progressPct,
            job.durationMs,
            job.errorMessage.orNull
          )
        }
        driverJobs
    }
  }
}
