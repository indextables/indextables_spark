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

import io.indextables.spark.stats.{DataSkippingMetrics, FilterExpressionCache}
import io.indextables.spark.transaction.PartitionFilterCache
import org.slf4j.LoggerFactory

/**
 * SQL command to describe data skipping statistics.
 *
 * Syntax: DESCRIBE INDEXTABLES DATA SKIPPING STATS DESCRIBE TANTIVY4SPARK DATA SKIPPING STATS
 *
 * Returns statistics about:
 *   - Data skipping effectiveness (files scanned vs skipped)
 *   - Filter expression cache hit rates
 *   - Partition filter cache hit rates
 *   - Per-filter-type skip counts
 *
 * Note: All stats are collected on the driver since scan planning (where data skipping occurs) happens on the driver.
 */
case class DescribeDataSkippingStatsCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeDataSkippingStatsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("metric_type", StringType, nullable = false)(),
    AttributeReference("metric_name", StringType, nullable = false)(),
    AttributeReference("metric_value", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing data skipping statistics")

    val rows = scala.collection.mutable.ArrayBuffer[Row]()

    // ========================================================================
    // Global data skipping stats (aggregated across all scans)
    // ========================================================================
    val globalStats = DataSkippingMetrics.getGlobalStats()

    rows += Row("data_skipping", "total_files_considered", globalStats.totalFiles.toString)
    rows += Row("data_skipping", "partition_pruned_files", globalStats.partitionPrunedFiles.toString)
    rows += Row("data_skipping", "data_skipped_files", globalStats.dataSkippedFiles.toString)
    rows += Row("data_skipping", "final_files_scanned", globalStats.finalFiles.toString)
    rows += Row("data_skipping", "partition_skip_rate", f"${globalStats.partitionSkipRate * 100}%.2f%%")
    rows += Row("data_skipping", "data_skip_rate", f"${globalStats.dataSkipRate * 100}%.2f%%")
    rows += Row("data_skipping", "total_skip_rate", f"${globalStats.totalSkipRate * 100}%.2f%%")

    // Per-filter-type stats (sorted by count descending)
    if (globalStats.filterTypes.nonEmpty) {
      globalStats.filterTypes.toSeq.sortBy(-_._2).foreach {
        case (filterType, count) =>
          rows += Row("filter_type_skips", filterType, count.toString)
      }
    }

    // ========================================================================
    // Filter expression cache stats
    // ========================================================================
    val (simplifiedHits, simplifiedMisses, inRangeHits, inRangeMisses) = FilterExpressionCache.getStats()
    val (simplifiedHitRate, inRangeHitRate)                            = FilterExpressionCache.getHitRates()
    val (simplifiedCacheSize, inRangeCacheSize)                        = FilterExpressionCache.getCacheSizes()

    rows += Row("filter_expr_cache", "simplified_hits", simplifiedHits.toString)
    rows += Row("filter_expr_cache", "simplified_misses", simplifiedMisses.toString)
    rows += Row("filter_expr_cache", "simplified_hit_rate", f"${simplifiedHitRate * 100}%.2f%%")
    rows += Row("filter_expr_cache", "simplified_cache_size", simplifiedCacheSize.toString)
    rows += Row("filter_expr_cache", "in_range_hits", inRangeHits.toString)
    rows += Row("filter_expr_cache", "in_range_misses", inRangeMisses.toString)
    rows += Row("filter_expr_cache", "in_range_hit_rate", f"${inRangeHitRate * 100}%.2f%%")
    rows += Row("filter_expr_cache", "in_range_cache_size", inRangeCacheSize.toString)

    // ========================================================================
    // Partition filter cache stats
    // ========================================================================
    val (partitionHits, partitionMisses, partitionHitRate) = PartitionFilterCache.getStats()

    rows += Row("partition_filter_cache", "hits", partitionHits.toString)
    rows += Row("partition_filter_cache", "misses", partitionMisses.toString)
    rows += Row("partition_filter_cache", "hit_rate", f"${partitionHitRate * 100}%.2f%%")
    rows += Row("partition_filter_cache", "size", PartitionFilterCache.size().toString)

    rows.toSeq
  }
}

/**
 * SQL command to reset/flush data skipping statistics.
 *
 * Syntax: FLUSH INDEXTABLES DATA SKIPPING STATS FLUSH TANTIVY4SPARK DATA SKIPPING STATS RESET INDEXTABLES DATA SKIPPING
 * STATS RESET TANTIVY4SPARK DATA SKIPPING STATS
 */
case class FlushDataSkippingStatsCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[FlushDataSkippingStatsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("action", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Flushing data skipping statistics")

    val rows = scala.collection.mutable.ArrayBuffer[Row]()

    // Reset data skipping metrics
    DataSkippingMetrics.resetAll()
    rows += Row("data_skipping_metrics", "reset")

    // Reset filter expression cache stats (but keep cached entries)
    FilterExpressionCache.resetStats()
    rows += Row("filter_expr_cache_stats", "reset")

    // Reset partition filter cache stats (but keep cached entries)
    PartitionFilterCache.resetStats()
    rows += Row("partition_filter_cache_stats", "reset")

    logger.info("Data skipping statistics flushed")
    rows.toSeq
  }
}

/**
 * SQL command to invalidate data skipping caches (clears both stats AND cached entries).
 *
 * Syntax: INVALIDATE INDEXTABLES DATA SKIPPING CACHE INVALIDATE TANTIVY4SPARK DATA SKIPPING CACHE
 */
case class InvalidateDataSkippingCacheCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[InvalidateDataSkippingCacheCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("action", StringType, nullable = false)(),
    AttributeReference("entries_cleared", LongType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Invalidating data skipping caches")

    val rows = scala.collection.mutable.ArrayBuffer[Row]()

    // Get sizes before clearing
    val (simplifiedSize, inRangeSize) = FilterExpressionCache.getCacheSizes()
    val partitionSize                 = PartitionFilterCache.size()

    // Reset data skipping metrics
    DataSkippingMetrics.resetAll()
    rows += Row("data_skipping_metrics", 0L)

    // Invalidate filter expression cache (clears entries and stats)
    FilterExpressionCache.invalidateAll()
    rows += Row("filter_expr_cache", simplifiedSize + inRangeSize)

    // Invalidate partition filter cache (clears entries and stats)
    PartitionFilterCache.invalidate()
    rows += Row("partition_filter_cache", partitionSize.toLong)

    logger.info(
      s"Data skipping caches invalidated: " +
        s"filter_expr=${simplifiedSize + inRangeSize} entries, " +
        s"partition_filter=$partitionSize entries"
    )

    rows.toSeq
  }
}
