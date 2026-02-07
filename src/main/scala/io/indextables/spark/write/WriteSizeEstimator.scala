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

package io.indextables.spark.write

import io.indextables.spark.transaction.TransactionLogInterface
import io.indextables.spark.util.SizeParser
import org.slf4j.LoggerFactory

/**
 * Calculates the advisory partition size for Spark's AQE to produce well-sized splits.
 *
 * Two estimation modes:
 *
 *   - '''Sampling mode''' (no qualifying history): Uses a configurable ratio to estimate the
 *     relationship between shuffle data and on-disk split size. Advisory = targetSplitSize / samplingRatio.
 *
 *   - '''History mode''' (qualifying splits in transaction log): Uses real bytes-per-row from
 *     existing splits. Advisory = targetSplitSize (we know the actual compression ratio).
 *
 * @param transactionLog
 *   Transaction log to read existing split metadata from
 * @param config
 *   Optimized write configuration
 */
class WriteSizeEstimator(
  transactionLog: TransactionLogInterface,
  config: OptimizedWriteConfig) {

  private val logger = LoggerFactory.getLogger(classOf[WriteSizeEstimator])

  /**
   * Calculates bytes-per-row from qualifying splits in the transaction log.
   *
   * @return Some(bytesPerRow) if qualifying splits exist, None otherwise
   */
  def calculateBytesPerRow(): Option[Double] =
    try {
      val addActions = transactionLog.listFiles()
      val qualifying = addActions.filter(_.numRecords.exists(_ >= config.minRowsForEstimation))

      if (qualifying.nonEmpty) {
        val totalBytes = qualifying.map(_.size).sum
        val totalRows = qualifying.flatMap(_.numRecords).sum
        val bytesPerRow = totalBytes.toDouble / totalRows
        logger.info(
          s"History mode: ${qualifying.length} qualifying splits, " +
            s"bytesPerRow=${f"$bytesPerRow%.1f"}"
        )
        Some(bytesPerRow)
      } else {
        logger.info("No qualifying splits found for bytes-per-row estimation")
        None
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Bytes-per-row estimation failed: ${e.getMessage}")
        None
    }

  def calculateAdvisoryPartitionSize(): Long =
    calculateBytesPerRow() match {
      case Some(_) =>
        val advisory = config.targetSplitSizeBytes
        logger.info(s"History mode: advisory=${SizeParser.formatBytes(advisory)}")
        advisory
      case None =>
        val advisory = (config.targetSplitSizeBytes / config.samplingRatio).toLong
        logger.info(
          s"Sampling mode: advisory=${SizeParser.formatBytes(advisory)} " +
            s"(target=${SizeParser.formatBytes(config.targetSplitSizeBytes)} / ratio=${config.samplingRatio})"
        )
        advisory
    }

  /**
   * Calculates the maximum number of rows per split for balanced mode rolling.
   *
   * Uses bytes-per-row from transaction log history to estimate how many rows
   * fit within maxSplitSizeBytes. Returns None if no history is available
   * (first write), in which case no rolling occurs.
   *
   * @return Some(maxRows) if history exists, None otherwise
   */
  def calculateMaxRowsPerSplit(): Option[Long] =
    calculateBytesPerRow().map { bpr =>
      val maxRows = math.max(1L, (config.maxSplitSizeBytes / bpr).toLong)
      logger.info(
        s"Max rows per split: $maxRows " +
          s"(maxSplitSize=${SizeParser.formatBytes(config.maxSplitSizeBytes)}, bytesPerRow=${f"$bpr%.1f"})"
      )
      maxRows
    }
}
