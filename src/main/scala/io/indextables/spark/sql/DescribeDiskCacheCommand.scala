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

import io.indextables.spark.storage.GlobalSplitCacheManager
import org.slf4j.LoggerFactory

/**
 * SQL command to describe disk cache statistics across all executors.
 *
 * Syntax:
 *   DESCRIBE INDEXTABLES DISK CACHE
 *   DESCRIBE TANTIVY4SPARK DISK CACHE
 *
 * Returns a DataFrame with disk cache statistics aggregated from all executors:
 *   - executor_id: Identifier of the executor
 *   - enabled: Whether disk cache is enabled
 *   - total_bytes: Total bytes currently cached
 *   - max_bytes: Maximum cache size
 *   - usage_percent: Percentage of cache used
 *   - splits_cached: Number of splits in cache
 *   - components_cached: Number of components in cache
 *
 * Note: Each executor maintains its own independent disk cache. This command
 * aggregates statistics from all active executors in the cluster.
 */
case class DescribeDiskCacheCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeDiskCacheCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("executor_id", StringType, nullable = false)(),
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("enabled", BooleanType, nullable = false)(),
    AttributeReference("total_bytes", LongType, nullable = true)(),
    AttributeReference("max_bytes", LongType, nullable = true)(),
    AttributeReference("usage_percent", DoubleType, nullable = true)(),
    AttributeReference("splits_cached", LongType, nullable = true)(),
    AttributeReference("components_cached", LongType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing disk cache statistics across all executors")

    val sc = sparkSession.sparkContext

    // Get the driver's block manager to exclude it from executor count
    val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
    val driverHostPort = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

    // Count only actual executors (exclude driver), at least 1 for local mode
    val numExecutors = math.max(1, sc.getExecutorMemoryStatus.keys.count(_ != driverHostPort))

    try {
      // Collect stats from all executors
      val executorStats = sc.parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val executorId = s"executor-$index"
          // Get the host from this executor's block manager
          val blockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
          val host = s"${blockManagerId.host}:${blockManagerId.port}"
          val enabled = GlobalSplitCacheManager.isDiskCacheEnabled()
          val stats = GlobalSplitCacheManager.getDiskCacheStats()

          val row = stats match {
            case Some(s) =>
              Row(
                executorId,
                host,
                enabled,
                s.totalBytes,
                s.maxBytes,
                s.usagePercent,
                s.splitCount,
                s.componentCount
              )
            case None =>
              Row(
                executorId,
                host,
                enabled,
                null,
                null,
                null,
                null,
                null
              )
          }
          Iterator(row)
        }
        .collect()
        .toSeq

      // Also get driver stats
      val driverEnabled = GlobalSplitCacheManager.isDiskCacheEnabled()
      val driverStats = GlobalSplitCacheManager.getDiskCacheStats()

      val driverRow = driverStats match {
        case Some(s) =>
          Row(
            "driver",
            driverHostPort,
            driverEnabled,
            s.totalBytes,
            s.maxBytes,
            s.usagePercent,
            s.splitCount,
            s.componentCount
          )
        case None =>
          Row(
            "driver",
            driverHostPort,
            driverEnabled,
            null,
            null,
            null,
            null,
            null
          )
      }

      // Return driver + executor stats
      driverRow +: executorStats

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor stats, returning driver-only stats: ${e.getMessage}")
        // Fallback to driver-only stats
        val driverEnabled = GlobalSplitCacheManager.isDiskCacheEnabled()
        val driverStats = GlobalSplitCacheManager.getDiskCacheStats()

        val driverRow = driverStats match {
          case Some(s) =>
            Row(
              "driver",
              driverHostPort,
              driverEnabled,
              s.totalBytes,
              s.maxBytes,
              s.usagePercent,
              s.splitCount,
              s.componentCount
            )
          case None =>
            Row(
              "driver",
              driverHostPort,
              driverEnabled,
              null,
              null,
              null,
              null,
              null
            )
        }
        Seq(driverRow)
    }
  }
}
