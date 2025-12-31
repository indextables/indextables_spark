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

import io.indextables.tantivy4java.split.SplitCacheManager
import org.slf4j.LoggerFactory

/**
 * SQL command to describe object storage statistics across all executors.
 *
 * Syntax: DESCRIBE INDEXTABLES STORAGE STATS DESCRIBE TANTIVY4SPARK STORAGE STATS
 *
 * Returns a DataFrame with object storage access statistics aggregated from all executors:
 *   - executor_id: Identifier of the executor
 *   - host: Host address of the executor
 *   - bytes_fetched: Total bytes fetched from object storage (S3/Azure/etc)
 *   - requests: Total number of object storage requests
 *
 * Note: These are cumulative counters since JVM startup. Each executor maintains its own counters.
 */
case class DescribeStorageStatsCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeStorageStatsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("executor_id", StringType, nullable = false)(),
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("bytes_fetched", LongType, nullable = false)(),
    AttributeReference("requests", LongType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing object storage statistics across all executors")

    val sc = sparkSession.sparkContext

    // Get the driver's block manager to exclude it from executor count
    val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
    val driverHostPort       = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

    // Count only actual executors (exclude driver), at least 1 for local mode
    val numExecutors = math.max(1, sc.getExecutorMemoryStatus.keys.count(_ != driverHostPort))

    try {
      // Collect stats from all executors
      val executorStats = sc
        .parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val executorId = s"executor-$index"
          // Get the host from this executor's block manager
          val blockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
          val host           = s"${blockManagerId.host}:${blockManagerId.port}"

          val bytesFetched = SplitCacheManager.getObjectStorageBytesFetched()
          val requests     = SplitCacheManager.getObjectStorageRequestCount()

          val row = Row(executorId, host, bytesFetched, requests)
          Iterator(row)
        }
        .collect()
        .toSeq

      // Also get driver stats
      val driverBytesFetched = SplitCacheManager.getObjectStorageBytesFetched()
      val driverRequests     = SplitCacheManager.getObjectStorageRequestCount()

      val driverRow = Row("driver", driverHostPort, driverBytesFetched, driverRequests)

      // Return driver + executor stats
      driverRow +: executorStats

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor stats, returning driver-only stats: ${e.getMessage}")
        // Fallback to driver-only stats
        val driverBytesFetched = SplitCacheManager.getObjectStorageBytesFetched()
        val driverRequests     = SplitCacheManager.getObjectStorageRequestCount()

        val driverRow = Row("driver", driverHostPort, driverBytesFetched, driverRequests)
        Seq(driverRow)
    }
  }
}
