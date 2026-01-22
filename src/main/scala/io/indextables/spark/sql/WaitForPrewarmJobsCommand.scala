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
import org.apache.spark.SparkContext

import io.indextables.spark.prewarm.{AsyncPrewarmJobManager, PrewarmJobStatus}
import org.slf4j.LoggerFactory

/**
 * SQL command to wait for async prewarm jobs to complete.
 *
 * Syntax:
 * {{{
 * WAIT FOR INDEXTABLES PREWARM JOBS
 * WAIT FOR INDEXTABLES PREWARM JOBS 's3://bucket/path'
 * WAIT FOR INDEXTABLES PREWARM JOBS JOB 'job-uuid'
 * WAIT FOR INDEXTABLES PREWARM JOBS 's3://bucket/path' TIMEOUT 300
 * WAIT FOR INDEXTABLES PREWARM JOBS JOB 'job-uuid' TIMEOUT 60
 * }}}
 *
 * Blocks until all matching async prewarm jobs complete (or timeout is reached).
 *
 * Returns a DataFrame with final job status:
 *   - executor_id: Identifier of the executor
 *   - host: Host address (ip:port) of the executor
 *   - job_id: Unique job identifier
 *   - table_path: Path of the table that was prewarmed
 *   - status: Final job status (COMPLETED, FAILED, CANCELLED, TIMEOUT)
 *   - total_splits: Total number of splits
 *   - splits_prewarmed: Number of splits successfully prewarmed
 *   - duration_ms: Total duration in milliseconds
 *   - error_message: Error message if failed (null otherwise)
 *
 * @param tablePath Optional table path to filter jobs
 * @param jobId Optional job ID to filter jobs
 * @param timeoutSeconds Maximum time to wait (default: 3600 seconds = 1 hour)
 */
case class WaitForPrewarmJobsCommand(
  tablePath: Option[String] = None,
  jobId: Option[String] = None,
  timeoutSeconds: Int = 3600)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[WaitForPrewarmJobsCommand])
  private val pollIntervalMs = 2000L // Poll every 2 seconds

  override val output: Seq[Attribute] = Seq(
    AttributeReference("executor_id", StringType, nullable = false)(),
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("job_id", StringType, nullable = false)(),
    AttributeReference("table_path", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("total_splits", IntegerType, nullable = false)(),
    AttributeReference("splits_prewarmed", IntegerType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("error_message", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Waiting for async prewarm jobs (tablePath=$tablePath, jobId=$jobId, timeout=${timeoutSeconds}s)")

    val sc = sparkSession.sparkContext
    val startTime = System.currentTimeMillis()
    val timeoutMs = timeoutSeconds * 1000L

    // Get driver info
    val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
    val driverHostPort = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

    while (System.currentTimeMillis() - startTime < timeoutMs) {
      // Poll all executors for job status
      val allJobs = collectJobStatusFromCluster(sc, driverHostPort)

      // Filter to relevant jobs based on tablePath/jobId
      val relevantJobs = allJobs.filter { case (_, _, job) =>
        tablePath.forall(_ == job.tablePath) && jobId.forall(_ == job.jobId)
      }

      if (relevantJobs.isEmpty) {
        // No matching jobs found
        logger.info("No matching async prewarm jobs found")
        return Seq(
          Row(
            "none",
            "none",
            jobId.getOrElse("none"),
            tablePath.getOrElse("none"),
            "not_found",
            0,
            0,
            0L,
            null
          )
        )
      }

      // Check if all relevant jobs are complete (COMPLETED, FAILED, or CANCELLED)
      val runningJobs = relevantJobs.filter { case (_, _, job) => job.status == "RUNNING" }

      if (runningJobs.isEmpty) {
        // All jobs complete - return final status
        logger.info(s"All ${relevantJobs.size} async prewarm jobs completed")
        return relevantJobs.map { case (executorId, host, job) =>
          Row(
            executorId,
            host,
            job.jobId,
            job.tablePath,
            job.status,
            job.totalSplits,
            job.completedSplits,
            job.durationMs,
            job.errorMessage.orNull
          )
        }
      }

      // Still running - log progress and sleep
      val totalSplits = relevantJobs.map(_._3.totalSplits).sum
      val completedSplits = relevantJobs.map(_._3.completedSplits).sum
      val progressPct = if (totalSplits > 0) (completedSplits.toDouble / totalSplits) * 100 else 0
      logger.debug(f"Waiting for ${runningJobs.size} jobs, progress: $progressPct%.1f%% ($completedSplits/$totalSplits splits)")

      Thread.sleep(pollIntervalMs)
    }

    // Timeout reached - return current status with timeout indication
    logger.warn(s"Timeout waiting for async prewarm jobs after ${timeoutSeconds}s")
    val finalJobs = collectJobStatusFromCluster(sc, driverHostPort)
    val relevantJobs = finalJobs.filter { case (_, _, job) =>
      tablePath.forall(_ == job.tablePath) && jobId.forall(_ == job.jobId)
    }

    relevantJobs.map { case (executorId, host, job) =>
      val finalStatus = if (job.status == "RUNNING") s"TIMEOUT (was: ${job.status})" else job.status
      Row(
        executorId,
        host,
        job.jobId,
        job.tablePath,
        finalStatus,
        job.totalSplits,
        job.completedSplits,
        job.durationMs,
        job.errorMessage.orNull
      )
    }
  }

  /**
   * Collect job status from all executors in the cluster.
   * Returns tuples of (executorId, host, PrewarmJobStatus).
   */
  private def collectJobStatusFromCluster(
    sc: SparkContext,
    driverHostPort: String
  ): Seq[(String, String, PrewarmJobStatus)] = {
    // Count only actual executors (exclude driver), at least 1 for local mode
    val numExecutors = math.max(1, sc.getExecutorMemoryStatus.keys.count(_ != driverHostPort))

    try {
      // Collect from executors
      val executorJobs = sc
        .parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val executorId = s"executor-$index"
          val blockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
          val host = s"${blockManagerId.host}:${blockManagerId.port}"

          AsyncPrewarmJobManager.getAllJobStatus.map { job =>
            (executorId, host, job)
          }.iterator
        }
        .collect()
        .toSeq

      // Also get driver jobs
      val driverJobs = AsyncPrewarmJobManager.getAllJobStatus.map { job =>
        ("driver", driverHostPort, job)
      }

      driverJobs ++ executorJobs

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor job status: ${e.getMessage}")
        // Fallback to driver-only
        AsyncPrewarmJobManager.getAllJobStatus.map { job =>
          ("driver", driverHostPort, job)
        }
    }
  }
}
