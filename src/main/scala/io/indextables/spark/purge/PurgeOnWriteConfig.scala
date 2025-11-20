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

package io.indextables.spark.purge

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.slf4j.LoggerFactory

/**
 * Configuration for purge-on-write feature.
 *
 * Purge-on-write automatically cleans up orphaned split files and old transaction logs after write operations, similar
 * to merge-on-write.
 *
 * Configuration options:
 *   - spark.indextables.purgeOnWrite.enabled: Enable/disable purge-on-write (default: false)
 *   - spark.indextables.purgeOnWrite.triggerAfterMerge: Run purge after merge-on-write completes (default: true)
 *   - spark.indextables.purgeOnWrite.triggerAfterWrites: Run purge after N writes (default: 0 = disabled)
 *   - spark.indextables.purgeOnWrite.splitRetentionHours: Split file retention period in hours (default: 168 = 7 days)
 *   - spark.indextables.purgeOnWrite.txLogRetentionHours: Transaction log retention period in hours (default: 720 = 30
 *     days)
 *
 * Trigger modes:
 *   1. After merge-on-write: Purge runs when merge-on-write completes (if triggerAfterMerge=true) 2. After N writes:
 *      Purge runs every N write operations on the table (if triggerAfterWrites > 0)
 *
 * Safety features:
 *   - Minimum retention period enforced (24 hours for splits, unless disabled)
 *   - Per-session transaction counter to trigger purge
 *   - Graceful failure handling (purge failures don't fail writes)
 */
case class PurgeOnWriteConfig(
  enabled: Boolean,
  triggerAfterMerge: Boolean,
  triggerAfterWrites: Int,
  splitRetentionHours: Long,
  txLogRetentionHours: Long)

object PurgeOnWriteConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Default split retention: 7 days (168 hours) */
  val DEFAULT_SPLIT_RETENTION_HOURS: Long = 168L

  /** Default transaction log retention: 30 days (720 hours) */
  val DEFAULT_TX_LOG_RETENTION_HOURS: Long = 720L

  /** Minimum split retention: 24 hours (safety check) */
  val MIN_SPLIT_RETENTION_HOURS: Long = 24L

  /** Load purge-on-write configuration from write options. */
  def fromOptions(options: CaseInsensitiveStringMap): PurgeOnWriteConfig = {
    val enabled = options.getOrDefault("spark.indextables.purgeOnWrite.enabled", "false").toBoolean

    val triggerAfterMerge = options.getOrDefault("spark.indextables.purgeOnWrite.triggerAfterMerge", "true").toBoolean

    val triggerAfterWrites =
      try
        options.getOrDefault("spark.indextables.purgeOnWrite.triggerAfterWrites", "0").toInt
      catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid triggerAfterWrites value, using 0: ${e.getMessage}")
          0
      }

    val splitRetentionHours =
      try
        options
          .getOrDefault("spark.indextables.purgeOnWrite.splitRetentionHours", DEFAULT_SPLIT_RETENTION_HOURS.toString)
          .toLong
      catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid splitRetentionHours value, using default: ${e.getMessage}")
          DEFAULT_SPLIT_RETENTION_HOURS
      }

    val txLogRetentionHours =
      try
        options
          .getOrDefault("spark.indextables.purgeOnWrite.txLogRetentionHours", DEFAULT_TX_LOG_RETENTION_HOURS.toString)
          .toLong
      catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid txLogRetentionHours value, using default: ${e.getMessage}")
          DEFAULT_TX_LOG_RETENTION_HOURS
      }

    logger.debug(s"PurgeOnWriteConfig: enabled=$enabled, triggerAfterMerge=$triggerAfterMerge, triggerAfterWrites=$triggerAfterWrites, splitRetentionHours=$splitRetentionHours, txLogRetentionHours=$txLogRetentionHours")

    PurgeOnWriteConfig(
      enabled = enabled,
      triggerAfterMerge = triggerAfterMerge,
      triggerAfterWrites = triggerAfterWrites,
      splitRetentionHours = splitRetentionHours,
      txLogRetentionHours = txLogRetentionHours
    )
  }

  /** Validate configuration values. */
  def validate(config: PurgeOnWriteConfig, retentionCheckEnabled: Boolean = true): Unit = {
    require(config.splitRetentionHours >= 0, s"Split retention cannot be negative: ${config.splitRetentionHours}")
    require(
      config.txLogRetentionHours >= 0,
      s"Transaction log retention cannot be negative: ${config.txLogRetentionHours}"
    )
    require(config.triggerAfterWrites >= 0, s"Trigger after writes cannot be negative: ${config.triggerAfterWrites}")

    if (retentionCheckEnabled && config.splitRetentionHours < MIN_SPLIT_RETENTION_HOURS) {
      throw new IllegalArgumentException(
        s"""Split retention period ${config.splitRetentionHours} hours is less than minimum $MIN_SPLIT_RETENTION_HOURS hours.
           |This operation may delete files from active transactions and corrupt the table.
           |To disable this check, set spark.indextables.purge.retentionCheckEnabled=false
           |""".stripMargin
      )
    }
  }
}
