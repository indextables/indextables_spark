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

package io.indextables.spark.util

/** Utility functions for partition planning and distribution. */
object PartitionUtils {

  /**
   * Interleave partitions by host for better cluster utilization.
   *
   * Instead of scheduling all partitions for host1, then host2, etc.: [h1,h1,h1,h2,h2,h2,h3,h3,h3]
   *
   * This produces a round-robin ordering: [h1,h2,h3,h1,h2,h3,h1,h2,h3]
   *
   * This ensures Spark distributes work across all hosts from the start, rather than saturating one host at a time.
   *
   * @param batchesByHost
   *   Map of host -> sequence of partitions for that host
   * @tparam T
   *   The partition type
   * @return
   *   Interleaved sequence of partitions
   */
  def interleaveByHost[T](batchesByHost: Map[String, Seq[T]]): Seq[T] = {
    if (batchesByHost.isEmpty) return Seq.empty

    // Sort by host name for deterministic ordering
    val hostBatches: Seq[(String, Seq[T])] = batchesByHost.toSeq.sortBy(_._1)
    val maxBatches                         = hostBatches.map(_._2.length).max

    // Round-robin: take batch i from each host, then batch i+1, etc.
    val result = scala.collection.mutable.ArrayBuffer[T]()
    for (batchIndex <- 0 until maxBatches)
      for ((_, batches) <- hostBatches)
        if (batchIndex < batches.length) {
          result += batches(batchIndex)
        }
    result.toSeq
  }
}

/**
 * Calculates optimal splitsPerTask based on cluster size and split count.
 *
 * Auto-selection algorithm:
 *   - If totalSplits <= defaultParallelism × 2: use 1 split per task (maximize parallelism)
 *   - Otherwise: ensure at least 4 × defaultParallelism groups, capped at maxSplitsPerTask
 */
object SplitsPerTaskCalculator {

  /** Default maximum splits per task when using auto-selection */
  val DefaultMaxSplitsPerTask: Int = 8

  /**
   * Calculate optimal splitsPerTask based on cluster size and split count.
   *
   * @param totalSplits
   *   Total number of splits to process
   * @param defaultParallelism
   *   spark.sparkContext.defaultParallelism
   * @param configuredValue
   *   User-configured value ("auto", numeric string, or None)
   * @param maxSplitsPerTask
   *   Maximum splits per task (cap for auto-selection)
   * @return
   *   Optimal splitsPerTask value (always >= 1)
   */
  def calculate(
    totalSplits: Int,
    defaultParallelism: Int,
    configuredValue: Option[String],
    maxSplitsPerTask: Int = DefaultMaxSplitsPerTask
  ): Int =
    // Check if user explicitly set a numeric value
    configuredValue.flatMap(v => scala.util.Try(v.toInt).toOption) match {
      case Some(explicit) =>
        // User explicitly configured a numeric value - use it
        explicit.max(1)
      case None =>
        // Auto-selection: "auto", empty, or invalid value
        autoSelect(totalSplits, defaultParallelism, maxSplitsPerTask)
    }

  /**
   * Auto-select optimal splitsPerTask based on cluster size and split count.
   *
   * Algorithm:
   *   - Small tables (splits <= parallelism × 2): 1 split per task for max parallelism
   *   - Large tables: batch to ensure >= 4 × parallelism groups, capped at maxSplitsPerTask
   *
   * @param totalSplits
   *   Total number of splits to process
   * @param defaultParallelism
   *   spark.sparkContext.defaultParallelism
   * @param maxSplitsPerTask
   *   Maximum splits per task (cap)
   * @return
   *   Optimal splitsPerTask value (always >= 1)
   */
  private def autoSelect(
    totalSplits: Int,
    defaultParallelism: Int,
    maxSplitsPerTask: Int
  ): Int = {
    val parallelism = math.max(1, defaultParallelism) // Guard against 0
    val threshold   = parallelism * 2

    if (totalSplits <= threshold) {
      // Small table: maximize parallelism with 1 split per task
      1
    } else {
      // Large table: ensure at least 4 × defaultParallelism groups
      val minTargetGroups = parallelism * 4
      val calculated      = math.ceil(totalSplits.toDouble / minTargetGroups).toInt
      math.max(1, math.min(maxSplitsPerTask, calculated))
    }
  }
}
