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

package io.indextables.spark.merge

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.slf4j.LoggerFactory

/**
 * Configuration for async merge-on-write feature.
 *
 * Async merge-on-write allows indexing to continue in parallel with merge operations by:
 *   - Breaking merge batches into groups no bigger than 1/6th of total cluster CPUs
 *   - Executing a maximum of 3 batches simultaneously
 *   - Running merge process in a separate driver thread
 *
 * Configuration options:
 *   - spark.indextables.mergeOnWrite.enabled: Enable/disable merge-on-write (default: false)
 *   - spark.indextables.mergeOnWrite.async.enabled: Run merges in background thread (default: true)
 *   - spark.indextables.mergeOnWrite.batchCpuFraction: Fraction of cluster CPUs per batch (default: 0.167)
 *   - spark.indextables.mergeOnWrite.maxConcurrentBatches: Max batches running simultaneously (default: 3)
 *   - spark.indextables.mergeOnWrite.minBatchesToTrigger: Min batches worth of groups to trigger (default: 1)
 *   - spark.indextables.mergeOnWrite.targetSize: Target size for merged splits (default: 4G)
 *   - spark.indextables.mergeOnWrite.shutdownTimeoutMs: Graceful shutdown wait (default: 300000 = 5 min)
 *
 * Threshold formula: threshold = batchSize * minBatchesToTrigger Batch size formula: batchSize = max(1,
 * totalClusterCpus * batchCpuFraction)
 *
 * @param enabled
 *   Whether merge-on-write is enabled
 * @param asyncEnabled
 *   Whether to run merges asynchronously in background thread
 * @param batchCpuFraction
 *   Fraction of cluster CPUs to use per merge batch (0.0 to 1.0)
 * @param maxConcurrentBatches
 *   Maximum number of merge batches running simultaneously
 * @param minBatchesToTrigger
 *   Minimum batches worth of merge groups required to trigger merge
 * @param targetSizeBytes
 *   Target size for merged splits in bytes
 * @param shutdownTimeoutMs
 *   Graceful shutdown timeout in milliseconds
 */
case class AsyncMergeOnWriteConfig(
  enabled: Boolean = AsyncMergeOnWriteConfig.DEFAULT_ENABLED,
  asyncEnabled: Boolean = AsyncMergeOnWriteConfig.DEFAULT_ASYNC_ENABLED,
  batchCpuFraction: Double = AsyncMergeOnWriteConfig.DEFAULT_BATCH_CPU_FRACTION,
  maxConcurrentBatches: Int = AsyncMergeOnWriteConfig.DEFAULT_MAX_CONCURRENT_BATCHES,
  minBatchesToTrigger: Int = AsyncMergeOnWriteConfig.DEFAULT_MIN_BATCHES_TO_TRIGGER,
  targetSizeBytes: Long = AsyncMergeOnWriteConfig.DEFAULT_TARGET_SIZE_BYTES,
  shutdownTimeoutMs: Long = AsyncMergeOnWriteConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS) {

  /**
   * Calculate the batch size (number of merge groups per batch) based on total cluster CPUs.
   *
   * Formula: batchSize = max(1, totalClusterCpus * batchCpuFraction)
   *
   * @param totalClusterCpus
   *   Total number of CPUs in the cluster (typically spark.sparkContext.defaultParallelism)
   * @return
   *   The number of merge groups per batch
   */
  def calculateBatchSize(totalClusterCpus: Int): Int =
    math.max(1, (totalClusterCpus * batchCpuFraction).toInt)

  /**
   * Calculate the merge threshold (minimum merge groups required to trigger merge).
   *
   * Formula: threshold = batchSize * minBatchesToTrigger
   *
   * @param totalClusterCpus
   *   Total number of CPUs in the cluster
   * @return
   *   The minimum number of merge groups required to trigger merge
   */
  def calculateMergeThreshold(totalClusterCpus: Int): Int =
    calculateBatchSize(totalClusterCpus) * minBatchesToTrigger

  /**
   * Validate configuration values and return this config if valid.
   *
   * @throws IllegalArgumentException
   *   if any configuration value is invalid
   * @return
   *   This config instance if valid
   */
  def validate(): AsyncMergeOnWriteConfig = {
    require(
      batchCpuFraction > 0.0 && batchCpuFraction <= 1.0,
      s"batchCpuFraction must be > 0.0 and <= 1.0, got: $batchCpuFraction"
    )
    require(
      maxConcurrentBatches > 0,
      s"maxConcurrentBatches must be > 0, got: $maxConcurrentBatches"
    )
    require(
      minBatchesToTrigger > 0,
      s"minBatchesToTrigger must be > 0, got: $minBatchesToTrigger"
    )
    require(
      targetSizeBytes > 0,
      s"targetSizeBytes must be > 0, got: $targetSizeBytes"
    )
    require(
      shutdownTimeoutMs > 0,
      s"shutdownTimeoutMs must be > 0, got: $shutdownTimeoutMs"
    )
    this
  }

  /** Get target size as a human-readable string (e.g., "4G"). */
  def targetSizeString: String = AsyncMergeOnWriteConfig.formatBytes(targetSizeBytes)
}

object AsyncMergeOnWriteConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration key constants
  val KEY_ENABLED                = "spark.indextables.mergeOnWrite.enabled"
  val KEY_ASYNC_ENABLED          = "spark.indextables.mergeOnWrite.async.enabled"
  val KEY_BATCH_CPU_FRACTION     = "spark.indextables.mergeOnWrite.batchCpuFraction"
  val KEY_MAX_CONCURRENT_BATCHES = "spark.indextables.mergeOnWrite.maxConcurrentBatches"
  val KEY_MIN_BATCHES_TO_TRIGGER = "spark.indextables.mergeOnWrite.minBatchesToTrigger"
  val KEY_TARGET_SIZE            = "spark.indextables.mergeOnWrite.targetSize"
  val KEY_SHUTDOWN_TIMEOUT_MS    = "spark.indextables.mergeOnWrite.shutdownTimeoutMs"

  // Legacy key for backwards compatibility
  val KEY_MERGE_GROUP_MULTIPLIER = "spark.indextables.mergeOnWrite.mergeGroupMultiplier"

  // Default values
  val DEFAULT_ENABLED: Boolean            = false
  val DEFAULT_ASYNC_ENABLED: Boolean      = true
  val DEFAULT_BATCH_CPU_FRACTION: Double  = 0.167                   // 1/6
  val DEFAULT_MAX_CONCURRENT_BATCHES: Int = 3
  val DEFAULT_MIN_BATCHES_TO_TRIGGER: Int = 1
  val DEFAULT_TARGET_SIZE_BYTES: Long     = 4L * 1024 * 1024 * 1024 // 4GB
  val DEFAULT_SHUTDOWN_TIMEOUT_MS: Long   = 300000L                 // 5 minutes

  /** Create default configuration. */
  def default: AsyncMergeOnWriteConfig = AsyncMergeOnWriteConfig()

  /**
   * Load async merge-on-write configuration from write options.
   *
   * @param options
   *   Spark write options
   * @return
   *   Parsed configuration
   */
  def fromOptions(options: CaseInsensitiveStringMap): AsyncMergeOnWriteConfig = {
    val enabled      = getBooleanOption(options, KEY_ENABLED, DEFAULT_ENABLED)
    val asyncEnabled = getBooleanOption(options, KEY_ASYNC_ENABLED, DEFAULT_ASYNC_ENABLED)
    val batchCpuFraction = getDoubleOption(
      options,
      KEY_BATCH_CPU_FRACTION,
      DEFAULT_BATCH_CPU_FRACTION,
      minExclusive = Some(0.0),
      maxInclusive = Some(1.0)
    )
    val maxConcurrentBatches = getIntOption(
      options,
      KEY_MAX_CONCURRENT_BATCHES,
      DEFAULT_MAX_CONCURRENT_BATCHES,
      mustBePositive = true
    )
    val minBatchesToTrigger = getIntOption(
      options,
      KEY_MIN_BATCHES_TO_TRIGGER,
      DEFAULT_MIN_BATCHES_TO_TRIGGER,
      mustBePositive = true
    )
    val targetSizeBytes = parseBytes(options.getOrDefault(KEY_TARGET_SIZE, formatBytes(DEFAULT_TARGET_SIZE_BYTES)))
    val shutdownTimeoutMs = getLongOption(
      options,
      KEY_SHUTDOWN_TIMEOUT_MS,
      DEFAULT_SHUTDOWN_TIMEOUT_MS,
      mustBePositive = true
    )

    val config = AsyncMergeOnWriteConfig(
      enabled = enabled,
      asyncEnabled = asyncEnabled,
      batchCpuFraction = batchCpuFraction,
      maxConcurrentBatches = maxConcurrentBatches,
      minBatchesToTrigger = minBatchesToTrigger,
      targetSizeBytes = targetSizeBytes,
      shutdownTimeoutMs = shutdownTimeoutMs
    )

    logger.debug(
      s"AsyncMergeOnWriteConfig: enabled=$enabled, asyncEnabled=$asyncEnabled, " +
        s"batchCpuFraction=$batchCpuFraction, maxConcurrentBatches=$maxConcurrentBatches, " +
        s"minBatchesToTrigger=$minBatchesToTrigger, targetSize=${config.targetSizeString}, " +
        s"shutdownTimeoutMs=$shutdownTimeoutMs"
    )

    // Note: Validation is performed inline during parsing - invalid values use defaults
    config
  }

  /**
   * Load configuration from a Map[String, String] (e.g., serializable config).
   *
   * Key lookup is case-insensitive.
   *
   * @param configs
   *   Configuration map
   * @return
   *   Parsed configuration
   */
  def fromMap(configs: Map[String, String]): AsyncMergeOnWriteConfig = {
    // Build a case-insensitive lookup map
    val lowerCaseConfigs                 = configs.map { case (k, v) => k.toLowerCase -> v }
    def get(key: String): Option[String] = lowerCaseConfigs.get(key.toLowerCase)

    AsyncMergeOnWriteConfig(
      enabled = get(KEY_ENABLED).map(_.toBoolean).getOrElse(DEFAULT_ENABLED),
      asyncEnabled = get(KEY_ASYNC_ENABLED).map(_.toBoolean).getOrElse(DEFAULT_ASYNC_ENABLED),
      batchCpuFraction = get(KEY_BATCH_CPU_FRACTION).map(_.toDouble).getOrElse(DEFAULT_BATCH_CPU_FRACTION),
      maxConcurrentBatches = get(KEY_MAX_CONCURRENT_BATCHES).map(_.toInt).getOrElse(DEFAULT_MAX_CONCURRENT_BATCHES),
      minBatchesToTrigger = get(KEY_MIN_BATCHES_TO_TRIGGER).map(_.toInt).getOrElse(DEFAULT_MIN_BATCHES_TO_TRIGGER),
      targetSizeBytes = get(KEY_TARGET_SIZE).map(parseBytes).getOrElse(DEFAULT_TARGET_SIZE_BYTES),
      shutdownTimeoutMs = get(KEY_SHUTDOWN_TIMEOUT_MS).map(_.toLong).getOrElse(DEFAULT_SHUTDOWN_TIMEOUT_MS)
    ).validate()
  }

  private def getBooleanOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Boolean
  ): Boolean = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try
        value.toBoolean
      catch {
        case e: IllegalArgumentException =>
          logger.warn(s"Invalid boolean value for $key: '$value', using default: $default")
          default
      }
    }
  }

  private def getIntOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Int,
    mustBePositive: Boolean = false
  ): Int = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed = value.toInt
        if (mustBePositive && parsed <= 0) {
          logger.warn(s"Invalid value for $key: '$value' (must be > 0), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid integer value for $key: '$value', using default: $default")
          default
      }
    }
  }

  private def getLongOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Long,
    mustBePositive: Boolean = false
  ): Long = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed = value.toLong
        if (mustBePositive && parsed <= 0) {
          logger.warn(s"Invalid value for $key: '$value' (must be > 0), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid long value for $key: '$value', using default: $default")
          default
      }
    }
  }

  private def getDoubleOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Double,
    minExclusive: Option[Double] = None,
    maxInclusive: Option[Double] = None
  ): Double = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed  = value.toDouble
        val tooLow  = minExclusive.exists(min => parsed <= min)
        val tooHigh = maxInclusive.exists(max => parsed > max)
        if (tooLow || tooHigh) {
          val range = (minExclusive, maxInclusive) match {
            case (Some(min), Some(max)) => s"must be > $min and <= $max"
            case (Some(min), None)      => s"must be > $min"
            case (None, Some(max))      => s"must be <= $max"
            case _                      => "out of range"
          }
          logger.warn(s"Invalid value for $key: '$value' ($range), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case e: NumberFormatException =>
          logger.warn(s"Invalid double value for $key: '$value', using default: $default")
          default
      }
    }
  }

  /** Parse a byte size string (e.g., "2G", "512M", "1.5G", "1024K") into bytes. */
  def parseBytes(size: String): Long = {
    val trimmed = size.trim.toUpperCase
    val multiplier = trimmed.last match {
      case 'K' => 1024L
      case 'M' => 1024L * 1024
      case 'G' => 1024L * 1024 * 1024
      case 'T' => 1024L * 1024 * 1024 * 1024
      case _   => 1L
    }

    val numericPart = if (multiplier > 1) {
      trimmed.dropRight(1).trim
    } else {
      trimmed
    }

    (BigDecimal(numericPart) * multiplier).toLong
  }

  /** Format bytes as a human-readable string. */
  def formatBytes(bytes: Long): String =
    if (bytes >= 1024L * 1024 * 1024 * 1024) s"${bytes / (1024L * 1024 * 1024 * 1024)}T"
    else if (bytes >= 1024L * 1024 * 1024) s"${bytes / (1024L * 1024 * 1024)}G"
    else if (bytes >= 1024L * 1024) s"${bytes / (1024L * 1024)}M"
    else if (bytes >= 1024L) s"${bytes / 1024L}K"
    else s"${bytes}B"
}
