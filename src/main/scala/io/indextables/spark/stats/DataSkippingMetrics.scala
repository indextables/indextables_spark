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

package io.indextables.spark.stats

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.slf4j.LoggerFactory

/**
 * Metrics for data skipping effectiveness tracking.
 * Tracks files scanned vs skipped at each level of filtering.
 */
case class DataSkippingStats(
  totalFiles: Long,
  partitionPrunedFiles: Long,
  dataSkippedFiles: Long,
  finalFiles: Long,
  partitionSkipRate: Double,
  dataSkipRate: Double,
  totalSkipRate: Double,
  filterTypes: Map[String, Long] // Count of each filter type that contributed to skipping
)

/**
 * Thread-safe metrics collector for data skipping operations.
 * Supports per-table and global metrics aggregation.
 */
object DataSkippingMetrics {

  private val logger = LoggerFactory.getLogger(DataSkippingMetrics.getClass)

  // Per-table metrics
  private val tableMetrics = new ConcurrentHashMap[String, TableMetrics]()

  // Global metrics (aggregated across all tables)
  private val globalTotalFiles = new AtomicLong(0)
  private val globalPartitionSkipped = new AtomicLong(0)
  private val globalDataSkipped = new AtomicLong(0)
  private val globalFinalFiles = new AtomicLong(0)
  private val globalFilterTypeSkips = new ConcurrentHashMap[String, AtomicLong]()

  /**
   * Record data skipping results for a scan operation.
   *
   * @param tablePath Path to the table
   * @param totalFiles Total files before any filtering
   * @param afterPartitionPruning Files remaining after partition pruning
   * @param afterDataSkipping Files remaining after min/max data skipping
   * @param filterTypesUsed Map of filter type names to count of files skipped by that filter type
   */
  def recordScan(
    tablePath: String,
    totalFiles: Long,
    afterPartitionPruning: Long,
    afterDataSkipping: Long,
    filterTypesUsed: Map[String, Long] = Map.empty
  ): Unit = {
    val partitionSkipped = totalFiles - afterPartitionPruning
    val dataSkipped = afterPartitionPruning - afterDataSkipping

    // Update per-table metrics
    val metrics = tableMetrics.computeIfAbsent(tablePath, _ => new TableMetrics())
    metrics.totalFiles.addAndGet(totalFiles)
    metrics.partitionSkipped.addAndGet(partitionSkipped)
    metrics.dataSkipped.addAndGet(dataSkipped)
    metrics.finalFiles.addAndGet(afterDataSkipping)
    filterTypesUsed.foreach { case (filterType, count) =>
      metrics.filterTypeSkips.computeIfAbsent(filterType, _ => new AtomicLong(0)).addAndGet(count)
    }

    // Update global metrics
    globalTotalFiles.addAndGet(totalFiles)
    globalPartitionSkipped.addAndGet(partitionSkipped)
    globalDataSkipped.addAndGet(dataSkipped)
    globalFinalFiles.addAndGet(afterDataSkipping)
    filterTypesUsed.foreach { case (filterType, count) =>
      globalFilterTypeSkips.computeIfAbsent(filterType, _ => new AtomicLong(0)).addAndGet(count)
    }

    logger.debug(
      s"Data skipping for $tablePath: $totalFiles -> $afterPartitionPruning (partition) -> $afterDataSkipping (data)"
    )
  }

  /**
   * Get metrics for a specific table.
   */
  def getTableStats(tablePath: String): Option[DataSkippingStats] = {
    Option(tableMetrics.get(tablePath)).map { metrics =>
      val total = metrics.totalFiles.get()
      val partitionSkipped = metrics.partitionSkipped.get()
      val dataSkipped = metrics.dataSkipped.get()
      val finalFiles = metrics.finalFiles.get()

      val afterPartition = total - partitionSkipped
      val partitionSkipRate = if (total > 0) partitionSkipped.toDouble / total else 0.0
      val dataSkipRate = if (afterPartition > 0) dataSkipped.toDouble / afterPartition else 0.0
      val totalSkipRate = if (total > 0) (total - finalFiles).toDouble / total else 0.0

      val filterTypes = scala.collection.mutable.Map[String, Long]()
      metrics.filterTypeSkips.forEach { (k, v) =>
        filterTypes.put(k, v.get())
      }

      DataSkippingStats(
        totalFiles = total,
        partitionPrunedFiles = partitionSkipped,
        dataSkippedFiles = dataSkipped,
        finalFiles = finalFiles,
        partitionSkipRate = partitionSkipRate,
        dataSkipRate = dataSkipRate,
        totalSkipRate = totalSkipRate,
        filterTypes = filterTypes.toMap
      )
    }
  }

  /**
   * Get global aggregated metrics across all tables.
   */
  def getGlobalStats(): DataSkippingStats = {
    val total = globalTotalFiles.get()
    val partitionSkipped = globalPartitionSkipped.get()
    val dataSkipped = globalDataSkipped.get()
    val finalFiles = globalFinalFiles.get()

    val afterPartition = total - partitionSkipped
    val partitionSkipRate = if (total > 0) partitionSkipped.toDouble / total else 0.0
    val dataSkipRate = if (afterPartition > 0) dataSkipped.toDouble / afterPartition else 0.0
    val totalSkipRate = if (total > 0) (total - finalFiles).toDouble / total else 0.0

    val filterTypes = scala.collection.mutable.Map[String, Long]()
    globalFilterTypeSkips.forEach { (k, v) =>
      filterTypes.put(k, v.get())
    }

    DataSkippingStats(
      totalFiles = total,
      partitionPrunedFiles = partitionSkipped,
      dataSkippedFiles = dataSkipped,
      finalFiles = finalFiles,
      partitionSkipRate = partitionSkipRate,
      dataSkipRate = dataSkipRate,
      totalSkipRate = totalSkipRate,
      filterTypes = filterTypes.toMap
    )
  }

  /**
   * Reset metrics for a specific table.
   */
  def resetTableStats(tablePath: String): Unit = {
    tableMetrics.remove(tablePath)
    logger.debug(s"Reset data skipping metrics for $tablePath")
  }

  /**
   * Reset all metrics (both per-table and global).
   */
  def resetAll(): Unit = {
    tableMetrics.clear()
    globalTotalFiles.set(0)
    globalPartitionSkipped.set(0)
    globalDataSkipped.set(0)
    globalFinalFiles.set(0)
    globalFilterTypeSkips.clear()
    logger.debug("Reset all data skipping metrics")
  }

  /**
   * Get a formatted summary string for logging/display.
   */
  def formatStats(stats: DataSkippingStats): String = {
    val sb = new StringBuilder()
    sb.append(s"Data Skipping Statistics:\n")
    sb.append(s"  Total files considered: ${stats.totalFiles}\n")
    sb.append(s"  Partition pruned: ${stats.partitionPrunedFiles} (${f"${stats.partitionSkipRate * 100}%.1f"}%%)\n")
    sb.append(s"  Data skipped (min/max): ${stats.dataSkippedFiles} (${f"${stats.dataSkipRate * 100}%.1f"}%%)\n")
    sb.append(s"  Final files to scan: ${stats.finalFiles}\n")
    sb.append(s"  Total skip rate: ${f"${stats.totalSkipRate * 100}%.1f"}%%\n")
    if (stats.filterTypes.nonEmpty) {
      sb.append(s"  Filter type contributions:\n")
      stats.filterTypes.toSeq.sortBy(-_._2).foreach { case (filterType, count) =>
        sb.append(s"    - $filterType: $count files skipped\n")
      }
    }
    sb.toString()
  }

  // Internal class for per-table metrics tracking
  private class TableMetrics {
    val totalFiles = new AtomicLong(0)
    val partitionSkipped = new AtomicLong(0)
    val dataSkipped = new AtomicLong(0)
    val finalFiles = new AtomicLong(0)
    val filterTypeSkips = new ConcurrentHashMap[String, AtomicLong]()
  }
}
