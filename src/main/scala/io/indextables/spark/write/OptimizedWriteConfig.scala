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

import io.indextables.spark.util.SizeParser
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * Configuration for optimized writes using Spark's RequiresDistributionAndOrdering interface.
 *
 * When enabled, this feature requests a shuffle before writing to produce well-sized splits
 * (~1GB by default). For partitioned tables, data is clustered by partition columns so each
 * writer task handles a single partition, producing fewer, larger splits.
 *
 * @param enabled
 *   Whether optimized write is enabled (default: false)
 * @param targetSplitSizeBytes
 *   Target on-disk split size in bytes (default: 1GB)
 * @param samplingRatio
 *   Ratio of split-size to shuffle-data, used when no history is available (default: 1.1)
 * @param minRowsForEstimation
 *   Minimum rows per split to qualify for history-based size estimation (default: 10000)
 * @param distributionMode
 *   Distribution mode: "hash" clusters by partition columns, "none" uses unspecified (default: "hash")
 */
case class OptimizedWriteConfig(
  enabled: Boolean = OptimizedWriteConfig.DEFAULT_ENABLED,
  targetSplitSizeBytes: Long = OptimizedWriteConfig.DEFAULT_TARGET_SPLIT_SIZE_BYTES,
  samplingRatio: Double = OptimizedWriteConfig.DEFAULT_SAMPLING_RATIO,
  minRowsForEstimation: Long = OptimizedWriteConfig.DEFAULT_MIN_ROWS_FOR_ESTIMATION,
  distributionMode: String = OptimizedWriteConfig.DEFAULT_DISTRIBUTION_MODE) {

  def validate(): OptimizedWriteConfig = {
    require(
      targetSplitSizeBytes > 0,
      s"targetSplitSizeBytes must be > 0, got: $targetSplitSizeBytes"
    )
    require(
      samplingRatio > 0.0,
      s"samplingRatio must be > 0.0, got: $samplingRatio"
    )
    require(
      minRowsForEstimation > 0,
      s"minRowsForEstimation must be > 0, got: $minRowsForEstimation"
    )
    require(
      OptimizedWriteConfig.VALID_DISTRIBUTION_MODES.contains(distributionMode),
      s"distributionMode must be one of ${OptimizedWriteConfig.VALID_DISTRIBUTION_MODES.mkString(", ")}, got: $distributionMode"
    )
    this
  }

  def targetSplitSizeString: String = SizeParser.formatBytes(targetSplitSizeBytes)
}

object OptimizedWriteConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration key constants
  val KEY_ENABLED              = "spark.indextables.write.optimizeWrite.enabled"
  val KEY_TARGET_SPLIT_SIZE    = "spark.indextables.write.optimizeWrite.targetSplitSize"
  val KEY_SAMPLING_RATIO       = "spark.indextables.write.optimizeWrite.samplingRatio"
  val KEY_MIN_ROWS_FOR_EST     = "spark.indextables.write.optimizeWrite.minRowsForEstimation"
  val KEY_DISTRIBUTION_MODE    = "spark.indextables.write.optimizeWrite.distributionMode"

  // Default values
  val DEFAULT_ENABLED: Boolean                = false
  val DEFAULT_TARGET_SPLIT_SIZE_BYTES: Long   = 1L * 1024 * 1024 * 1024 // 1GB
  val DEFAULT_SAMPLING_RATIO: Double          = 1.1
  val DEFAULT_MIN_ROWS_FOR_ESTIMATION: Long   = 10000L
  val DEFAULT_DISTRIBUTION_MODE: String       = "hash"

  val VALID_DISTRIBUTION_MODES: Set[String] = Set("hash", "none")

  def default: OptimizedWriteConfig = OptimizedWriteConfig()

  def fromOptions(options: CaseInsensitiveStringMap): OptimizedWriteConfig = {
    val enabled = getBooleanOption(options, KEY_ENABLED, DEFAULT_ENABLED)
    val targetSplitSizeBytes = parseSizeOption(
      options, KEY_TARGET_SPLIT_SIZE, DEFAULT_TARGET_SPLIT_SIZE_BYTES
    )
    val samplingRatio = getDoubleOption(
      options, KEY_SAMPLING_RATIO, DEFAULT_SAMPLING_RATIO, minExclusive = Some(0.0)
    )
    val minRowsForEstimation = getLongOption(
      options, KEY_MIN_ROWS_FOR_EST, DEFAULT_MIN_ROWS_FOR_ESTIMATION, mustBePositive = true
    )
    val distributionMode = getStringOption(
      options, KEY_DISTRIBUTION_MODE, DEFAULT_DISTRIBUTION_MODE, VALID_DISTRIBUTION_MODES
    )

    val config = OptimizedWriteConfig(
      enabled = enabled,
      targetSplitSizeBytes = targetSplitSizeBytes,
      samplingRatio = samplingRatio,
      minRowsForEstimation = minRowsForEstimation,
      distributionMode = distributionMode
    )

    logger.debug(
      s"OptimizedWriteConfig: enabled=$enabled, targetSplitSize=${config.targetSplitSizeString}, " +
        s"samplingRatio=$samplingRatio, minRowsForEstimation=$minRowsForEstimation, " +
        s"distributionMode=$distributionMode"
    )

    config
  }

  def fromMap(configs: Map[String, String]): OptimizedWriteConfig = {
    val lowerCaseConfigs                 = configs.map { case (k, v) => k.toLowerCase -> v }
    def get(key: String): Option[String] = lowerCaseConfigs.get(key.toLowerCase)

    OptimizedWriteConfig(
      enabled = get(KEY_ENABLED).map(_.toBoolean).getOrElse(DEFAULT_ENABLED),
      targetSplitSizeBytes = get(KEY_TARGET_SPLIT_SIZE)
        .map(SizeParser.parseSize).getOrElse(DEFAULT_TARGET_SPLIT_SIZE_BYTES),
      samplingRatio = get(KEY_SAMPLING_RATIO).map(_.toDouble).getOrElse(DEFAULT_SAMPLING_RATIO),
      minRowsForEstimation = get(KEY_MIN_ROWS_FOR_EST).map(_.toLong).getOrElse(DEFAULT_MIN_ROWS_FOR_ESTIMATION),
      distributionMode = get(KEY_DISTRIBUTION_MODE).getOrElse(DEFAULT_DISTRIBUTION_MODE)
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
        case _: IllegalArgumentException =>
          logger.warn(s"Invalid boolean value for $key: '$value', using default: $default")
          default
      }
    }
  }

  private def getDoubleOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Double,
    minExclusive: Option[Double] = None
  ): Double = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed = value.toDouble
        if (minExclusive.exists(min => parsed <= min)) {
          logger.warn(s"Invalid value for $key: '$value' (must be > ${minExclusive.get}), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case _: NumberFormatException =>
          logger.warn(s"Invalid double value for $key: '$value', using default: $default")
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
        case _: NumberFormatException =>
          logger.warn(s"Invalid long value for $key: '$value', using default: $default")
          default
      }
    }
  }

  private def parseSizeOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Long
  ): Long = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try
        SizeParser.parseSize(value)
      catch {
        case _: IllegalArgumentException =>
          logger.warn(s"Invalid size value for $key: '$value', using default: ${SizeParser.formatBytes(default)}")
          default
      }
    }
  }

  private def getStringOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: String,
    validValues: Set[String]
  ): String = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      val lower = value.toLowerCase
      if (validValues.contains(lower)) lower
      else {
        logger.warn(
          s"Invalid value for $key: '$value' (must be one of ${validValues.mkString(", ")}), using default: $default"
        )
        default
      }
    }
  }
}
