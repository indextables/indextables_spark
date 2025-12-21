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

package io.indextables.spark.prescan

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.util.{ConfigurationResolver, OptionsConfigSource, HadoopConfigSource}

import org.slf4j.LoggerFactory

/**
 * Configuration for prescan filtering with priority resolution:
 * 1. DataFrame read options
 * 2. Spark session properties (spark.indextables.read.prescan.*)
 * 3. Session state (from ENABLE/DISABLE commands)
 *
 * @param enabled Whether prescan filtering is enabled
 * @param minSplitThreshold Minimum number of splits to trigger prescan (default: 2 * defaultParallelism)
 * @param maxConcurrency Maximum concurrent prescan threads (default: 4 * physical CPUs)
 * @param timeoutMs Timeout per split in milliseconds (default: 30000)
 */
case class PrescanConfig(
  enabled: Boolean,
  minSplitThreshold: Int,
  maxConcurrency: Int,
  timeoutMs: Long
)

object PrescanConfig {

  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration keys
  val PRESCAN_ENABLED = "spark.indextables.read.prescan.enabled"
  val PRESCAN_MIN_SPLIT_THRESHOLD = "spark.indextables.read.prescan.minSplitThreshold"
  val PRESCAN_MAX_CONCURRENCY = "spark.indextables.read.prescan.maxConcurrency"
  val PRESCAN_TIMEOUT_MS = "spark.indextables.read.prescan.timeoutMs"

  // Default values
  val DEFAULT_TIMEOUT_MS = 30000L

  /**
   * Resolve prescan configuration from multiple sources with priority ordering.
   *
   * Resolution priority:
   * 1. Session state override (from ENABLE/DISABLE SQL commands)
   * 2. DataFrame read options
   * 3. Spark/Hadoop configuration properties
   * 4. Computed defaults based on cluster resources
   *
   * @param sparkSession Active Spark session
   * @param options DataFrame read options
   * @param tablePath Table path for session state lookup
   * @return Resolved PrescanConfig
   */
  def resolve(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    tablePath: String
  ): PrescanConfig = {
    val sc = sparkSession.sparkContext
    val hadoopConf = sc.hadoopConfiguration

    // Build config sources in priority order
    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    // Calculate defaults based on cluster resources
    val defaultParallelism = sc.defaultParallelism
    val physicalCores = Runtime.getRuntime.availableProcessors()
    val defaultMinThreshold = 2 * defaultParallelism
    val defaultMaxConcurrency = 4 * physicalCores

    // Check session state override first (from ENABLE/DISABLE commands)
    implicit val spark: SparkSession = sparkSession
    val sessionEnabled = PrescanSessionState.isEnabled(tablePath)

    // Resolve enabled state: session override > config sources > default (false)
    val enabled = sessionEnabled.getOrElse(
      ConfigurationResolver.resolveBoolean(PRESCAN_ENABLED, sources, default = false)
    )

    // Resolve other config values
    val minThreshold = ConfigurationResolver.resolveInt(
      PRESCAN_MIN_SPLIT_THRESHOLD,
      sources,
      default = defaultMinThreshold
    )

    val maxConcurrency = ConfigurationResolver.resolveInt(
      PRESCAN_MAX_CONCURRENCY,
      sources,
      default = defaultMaxConcurrency
    )

    val timeoutMs = ConfigurationResolver.resolveLong(
      PRESCAN_TIMEOUT_MS,
      sources,
      default = DEFAULT_TIMEOUT_MS
    )

    val config = PrescanConfig(enabled, minThreshold, maxConcurrency, timeoutMs)

    if (enabled) {
      logger.debug(s"Prescan config resolved for $tablePath: " +
        s"enabled=$enabled, minThreshold=$minThreshold, maxConcurrency=$maxConcurrency, timeoutMs=$timeoutMs")
    }

    config
  }

  /**
   * Resolve prescan configuration from a simple options map.
   * Useful for testing or when SparkSession is not available.
   */
  def resolve(options: Map[String, String], defaultParallelism: Int = 8): PrescanConfig = {
    val physicalCores = Runtime.getRuntime.availableProcessors()

    PrescanConfig(
      enabled = options.get(PRESCAN_ENABLED).exists(_.toBoolean),
      minSplitThreshold = options.get(PRESCAN_MIN_SPLIT_THRESHOLD)
        .map(_.toInt)
        .getOrElse(2 * defaultParallelism),
      maxConcurrency = options.get(PRESCAN_MAX_CONCURRENCY)
        .map(_.toInt)
        .getOrElse(4 * physicalCores),
      timeoutMs = options.get(PRESCAN_TIMEOUT_MS)
        .map(_.toLong)
        .getOrElse(DEFAULT_TIMEOUT_MS)
    )
  }
}
