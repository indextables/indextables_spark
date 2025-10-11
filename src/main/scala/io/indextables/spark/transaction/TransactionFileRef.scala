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

package io.indextables.spark.transaction

/**
 * Serializable reference to a transaction log file.
 * Lightweight object that can be distributed to executors for parallel processing.
 *
 * @param version Transaction version number
 * @param path Relative path within _transaction_log directory (e.g., "000000000000000123.json")
 * @param size File size in bytes
 * @param modificationTime File modification timestamp in milliseconds
 */
case class TransactionFileRef(
  version: Long,
  path: String,
  size: Long,
  modificationTime: Long
) extends Serializable

/**
 * Checksum for transaction log state validation.
 * Used to verify consistency of distributed transaction log reads.
 *
 * @param checkpointVersion Optional checkpoint version if checkpoint exists
 * @param incrementalVersions Sequence of incremental transaction versions
 * @param totalFiles Total number of files (checkpoint + incremental)
 * @param checksumValue Checksum computed from file metadata
 */
case class TransactionLogChecksum(
  checkpointVersion: Option[Long],
  incrementalVersions: Seq[Long],
  totalFiles: Int,
  checksumValue: String
) extends Serializable

/**
 * Metrics for distributed transaction log operations.
 * Tracks performance characteristics of parallel transaction log reads.
 *
 * @param totalFiles Total number of files processed
 * @param checkpointFiles Number of files from checkpoint
 * @param incrementalFiles Number of incremental transaction files
 * @param parallelism Number of parallel tasks used
 * @param readTimeMs Time spent reading files from storage
 * @param parseTimeMs Time spent parsing JSON content
 * @param reduceTimeMs Time spent reducing actions to final state
 * @param cacheHitRate Cache hit rate for executor-local cache (0.0 to 1.0)
 * @param executorCount Number of executors involved
 */
case class DistributedTransactionLogMetrics(
  totalFiles: Int,
  checkpointFiles: Int,
  incrementalFiles: Int,
  parallelism: Int,
  readTimeMs: Long,
  parseTimeMs: Long,
  reduceTimeMs: Long,
  cacheHitRate: Double,
  executorCount: Int
) extends Serializable {

  /** Total time for all operations */
  def totalTimeMs: Long = readTimeMs + parseTimeMs + reduceTimeMs

  /** Convert to Spark SQL Row for display */
  def toRow: org.apache.spark.sql.Row = org.apache.spark.sql.Row(
    totalFiles,
    checkpointFiles,
    incrementalFiles,
    parallelism,
    totalTimeMs,
    cacheHitRate,
    executorCount
  )

  override def toString: String = {
    s"DistributedTransactionLogMetrics(" +
      s"total=$totalFiles, " +
      s"checkpoint=$checkpointFiles, " +
      s"incremental=$incrementalFiles, " +
      s"parallelism=$parallelism, " +
      s"totalTime=${totalTimeMs}ms, " +
      s"read=${readTimeMs}ms, " +
      s"parse=${parseTimeMs}ms, " +
      s"reduce=${reduceTimeMs}ms, " +
      s"cacheHitRate=${(cacheHitRate * 100).formatted("%.1f")}%, " +
      s"executors=$executorCount)"
  }
}
