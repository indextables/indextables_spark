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

/**
 * Utility functions for partition planning and distribution.
 */
object PartitionUtils {

  /**
   * Interleave partitions by host for better cluster utilization.
   *
   * Instead of scheduling all partitions for host1, then host2, etc.:
   *   [h1,h1,h1,h2,h2,h2,h3,h3,h3]
   *
   * This produces a round-robin ordering:
   *   [h1,h2,h3,h1,h2,h3,h1,h2,h3]
   *
   * This ensures Spark distributes work across all hosts from the start,
   * rather than saturating one host at a time.
   *
   * @param batchesByHost Map of host -> sequence of partitions for that host
   * @tparam T The partition type
   * @return Interleaved sequence of partitions
   */
  def interleaveByHost[T](batchesByHost: Map[String, Seq[T]]): Seq[T] = {
    if (batchesByHost.isEmpty) return Seq.empty

    // Sort by host name for deterministic ordering
    val hostBatches: Seq[(String, Seq[T])] = batchesByHost.toSeq.sortBy(_._1)
    val maxBatches = hostBatches.map(_._2.length).max

    // Round-robin: take batch i from each host, then batch i+1, etc.
    val result = scala.collection.mutable.ArrayBuffer[T]()
    for (batchIndex <- 0 until maxBatches) {
      for ((_, batches) <- hostBatches) {
        if (batchIndex < batches.length) {
          result += batches(batchIndex)
        }
      }
    }
    result.toSeq
  }
}
