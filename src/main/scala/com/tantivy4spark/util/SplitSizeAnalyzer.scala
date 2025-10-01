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

package com.tantivy4spark.util

import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory, AddAction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/** Analyzer for historical split sizes to calculate optimal target rows for auto-sizing. */
class SplitSizeAnalyzer(
  tablePath: Path,
  spark: SparkSession,
  options: CaseInsensitiveStringMap) {

  private val logger = LoggerFactory.getLogger(classOf[SplitSizeAnalyzer])

  /** Data class for historical split information. */
  case class SplitInfo(
    size: Long,
    numRecords: Long,
    bytesPerRecord: Double)

  /**
   * Calculate target number of rows per split based on historical data.
   *
   * @param targetSizeBytes
   *   Target size in bytes for each split
   * @param maxHistoricalSplits
   *   Maximum number of recent splits to analyze (default: 10)
   * @return
   *   Target number of rows per split, or None if insufficient historical data
   */
  def calculateTargetRows(targetSizeBytes: Long, maxHistoricalSplits: Int = 10): Option[Long] =
    try {
      // Create transaction log reader
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Get recent split data from the transaction log
        val recentSplits = getRecentSplitInfo(transactionLog, maxHistoricalSplits)

        if (recentSplits.isEmpty) {
          logger.info("No historical split data found for auto-sizing analysis")
          return None
        }

        // Calculate average bytes per record from historical data
        val averageBytesPerRecord = calculateAverageBytesPerRecord(recentSplits)

        if (averageBytesPerRecord <= 0) {
          logger.warn("Invalid average bytes per record calculated from historical data")
          return None
        }

        // Calculate target rows based on target size and average bytes per record
        val targetRows = math.ceil(targetSizeBytes.toDouble / averageBytesPerRecord).toLong

        logger.info(
          f"Auto-sizing analysis: target size=${SizeParser.formatBytes(targetSizeBytes)}, " +
            f"avg bytes/record=$averageBytesPerRecord%.2f, " +
            f"calculated target rows=$targetRows, " +
            f"based on ${recentSplits.length} historical splits"
        )

        Some(math.max(1L, targetRows)) // Ensure at least 1 row per split

      } finally
        transactionLog.close()
    } catch {
      case ex: Exception =>
        logger.error("Failed to analyze historical split data for auto-sizing", ex)
        None
    }

  /** Extract recent split information from the transaction log. */
  private def getRecentSplitInfo(transactionLog: TransactionLog, maxSplits: Int): Seq[SplitInfo] = {
    val addActions = transactionLog.listFiles()

    logger.debug(s"Found ${addActions.length} total splits in transaction log")

    // Filter to splits with both size and record count information
    val validSplits =
      addActions.filter(action => action.size > 0 && action.numRecords.exists(_ > 0)).takeRight(maxSplits) // Take the most recent valid splits

    logger.debug(s"Found ${validSplits.length} valid splits with size and record count data")

    // Convert to SplitInfo objects
    val splitInfos = validSplits.flatMap { action =>
      action.numRecords.map { records =>
        val bytesPerRecord = action.size.toDouble / records.toDouble
        SplitInfo(action.size, records, bytesPerRecord)
      }
    }

    // Log analysis details
    if (splitInfos.nonEmpty) {
      val avgSize           = splitInfos.map(_.size).sum.toDouble / splitInfos.length
      val avgRecords        = splitInfos.map(_.numRecords).sum.toDouble / splitInfos.length
      val avgBytesPerRecord = splitInfos.map(_.bytesPerRecord).sum / splitInfos.length

      logger.debug(
        f"Historical split analysis: " +
          f"avg size=${SizeParser.formatBytes(avgSize.toLong)}, " +
          f"avg records=$avgRecords%.0f, " +
          f"avg bytes/record=$avgBytesPerRecord%.2f"
      )
    }

    splitInfos
  }

  /** Calculate average bytes per record from historical split data. */
  private def calculateAverageBytesPerRecord(splits: Seq[SplitInfo]): Double = {
    if (splits.isEmpty) return 0.0

    // Calculate weighted average based on record count
    val totalBytes   = splits.map(_.size).sum.toDouble
    val totalRecords = splits.map(_.numRecords).sum.toDouble

    if (totalRecords <= 0) {
      return 0.0
    }

    totalBytes / totalRecords
  }
}

object SplitSizeAnalyzer {

  /** Factory method to create analyzer instance. */
  def apply(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap
  ): SplitSizeAnalyzer =
    new SplitSizeAnalyzer(tablePath, spark, options)

  /** Quick method to calculate target rows without creating analyzer instance. */
  def calculateTargetRows(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap,
    targetSizeBytes: Long,
    maxHistoricalSplits: Int = 10
  ): Option[Long] = {
    val analyzer = SplitSizeAnalyzer(tablePath, spark, options)
    analyzer.calculateTargetRows(targetSizeBytes, maxHistoricalSplits)
  }
}
