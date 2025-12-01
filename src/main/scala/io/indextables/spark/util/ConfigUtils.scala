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

import org.apache.spark.broadcast.Broadcast

import io.indextables.spark.storage.SplitCacheConfig
import org.slf4j.LoggerFactory

/** Utility functions for configuration management. */
object ConfigUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Statistics truncation configuration constants
  val STATS_TRUNCATION_ENABLED    = "spark.indextables.stats.truncation.enabled"
  val STATS_TRUNCATION_MAX_LENGTH = "spark.indextables.stats.truncation.maxLength"

  // Data skipping statistics column configuration (Delta Lake compatible)
  // spark.indextables.dataSkippingStatsColumns - explicit list of columns to collect stats for (comma-separated)
  // spark.indextables.dataSkippingNumIndexedCols - number of columns to index (default 32, -1 for all)
  val DATA_SKIPPING_STATS_COLUMNS     = "spark.indextables.dataSkippingStatsColumns"
  val DATA_SKIPPING_NUM_INDEXED_COLS  = "spark.indextables.dataSkippingNumIndexedCols"

  // Default values
  val DEFAULT_STATS_TRUNCATION_ENABLED     = true
  val DEFAULT_STATS_TRUNCATION_MAX_LENGTH  = 32
  val DEFAULT_DATA_SKIPPING_NUM_INDEXED_COLS = 32

  /**
   * Create a SplitCacheConfig from configuration map.
   *
   * This utility consolidates the duplicated cache configuration logic across partition readers, aggregate readers, and
   * other components.
   *
   * @param config
   *   Configuration map
   * @param tablePathOpt
   *   Optional table path for generating unique cache names
   * @return
   *   Configured SplitCacheConfig instance
   */
  def createSplitCacheConfig(
    config: Map[String, String],
    tablePathOpt: Option[String] = None
  ): SplitCacheConfig = {

    val configMap = config

    // Helper function to get config with defaults
    // Tries both original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
    def getConfig(configKey: String, default: String = ""): String = {
      configMap.get(configKey)
        .orElse(configMap.get(configKey.toLowerCase))
        .getOrElse(default)
    }

    def getConfigOption(configKey: String): Option[String] =
      // Try both the original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
      configMap.get(configKey).orElse(configMap.get(configKey.toLowerCase))

    SplitCacheConfig(
      cacheName = {
        val configName = getConfig("spark.indextables.cache.name", "")
        if (configName.trim().nonEmpty) {
          configName.trim()
        } else {
          // Use table path as cache name for table-specific caching
          tablePathOpt match {
            case Some(tablePath) =>
              s"tantivy4spark-${tablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
            case None =>
              s"tantivy4spark-default-${System.currentTimeMillis()}"
          }
        }
      },
      maxCacheSize = {
        val value = getConfig("spark.indextables.cache.maxSize", "200000000")
        try
          value.toLong
        catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.indextables.cache.maxSize: '$value'")
            throw e
        }
      },
      maxConcurrentLoads = {
        val value = getConfig("spark.indextables.cache.maxConcurrentLoads", "8")
        try
          value.toInt
        catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.indextables.cache.maxConcurrentLoads: '$value'")
            throw e
        }
      },
      enableQueryCache = getConfig("spark.indextables.cache.queryCache", "true").toBoolean,
      enableDocBatch = getConfig("spark.indextables.docBatch.enabled", "true").toBoolean,
      docBatchMaxSize = getConfig("spark.indextables.docBatch.maxSize", "1000").toInt,
      splitCachePath = getConfigOption("spark.indextables.cache.directoryPath")
        .orElse(SplitCacheConfig.getDefaultCachePath()),
      // AWS configuration from broadcast
      awsAccessKey = getConfigOption("spark.indextables.aws.accessKey"),
      awsSecretKey = getConfigOption("spark.indextables.aws.secretKey"),
      awsSessionToken = getConfigOption("spark.indextables.aws.sessionToken"),
      awsRegion = getConfigOption("spark.indextables.aws.region"),
      awsEndpoint = getConfigOption("spark.indextables.s3.endpoint"),
      awsPathStyleAccess = getConfigOption("spark.indextables.s3.pathStyleAccess").map(_.toBoolean),
      // Azure configuration from broadcast
      azureAccountName = getConfigOption("spark.indextables.azure.accountName"),
      azureAccountKey = getConfigOption("spark.indextables.azure.accountKey"),
      azureConnectionString = getConfigOption("spark.indextables.azure.connectionString"),
      azureBearerToken = getConfigOption("spark.indextables.azure.bearerToken"),
      azureTenantId = getConfigOption("spark.indextables.azure.tenantId"),
      azureClientId = getConfigOption("spark.indextables.azure.clientId"),
      azureClientSecret = getConfigOption("spark.indextables.azure.clientSecret"),
      azureEndpoint = getConfigOption("spark.indextables.azure.endpoint"),
      // GCP configuration from broadcast
      gcpProjectId = getConfigOption("spark.indextables.gcp.projectId"),
      gcpServiceAccountKey = getConfigOption("spark.indextables.gcp.serviceAccountKey"),
      gcpCredentialsFile = getConfigOption("spark.indextables.gcp.credentialsFile"),
      gcpEndpoint = getConfigOption("spark.indextables.gcp.endpoint"),
      // Batch optimization configuration
      batchOptimizationEnabled = getConfigOption("spark.indextables.read.batchOptimization.enabled").map(_.toBoolean),
      batchOptimizationProfile = getConfigOption("spark.indextables.read.batchOptimization.profile"),
      batchOptMaxRangeSize = getConfigOption("spark.indextables.read.batchOptimization.maxRangeSize")
        .map(SplitCacheConfig.parseSizeString),
      batchOptGapTolerance = getConfigOption("spark.indextables.read.batchOptimization.gapTolerance")
        .map(SplitCacheConfig.parseSizeString),
      batchOptMinDocs = getConfigOption("spark.indextables.read.batchOptimization.minDocsForOptimization").map(_.toInt),
      batchOptMaxConcurrentPrefetch =
        getConfigOption("spark.indextables.read.batchOptimization.maxConcurrentPrefetch").map(_.toInt),
      // Adaptive tuning configuration
      adaptiveTuningEnabled = getConfigOption("spark.indextables.read.adaptiveTuning.enabled").map(_.toBoolean),
      adaptiveTuningMinBatches =
        getConfigOption("spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment").map(_.toInt)
    )
  }

  /**
   * Create a SplitCacheConfig from broadcast configuration.
   *
   * Wrapper method for backward compatibility - delegates to createSplitCacheConfig.
   *
   * @param broadcastConfig
   *   Broadcast variable containing configuration map
   * @param tablePathOpt
   *   Optional table path for generating unique cache names
   * @return
   *   Configured SplitCacheConfig instance
   */
  def createSplitCacheConfigFromBroadcast(
    broadcastConfig: Broadcast[Map[String, String]],
    tablePathOpt: Option[String] = None
  ): SplitCacheConfig =
    createSplitCacheConfig(broadcastConfig.value, tablePathOpt)

  /**
   * Get a boolean configuration value from the config map with a default value.
   *
   * @param config
   *   Configuration map
   * @param key
   *   Configuration key
   * @param defaultValue
   *   Default value if key not found
   * @return
   *   Boolean value
   */
  def getBoolean(
    config: Map[String, String],
    key: String,
    defaultValue: Boolean
  ): Boolean =
    config.get(key).map(_.toBoolean).getOrElse(defaultValue)

  /**
   * Get an integer configuration value from the config map with a default value.
   *
   * @param config
   *   Configuration map
   * @param key
   *   Configuration key
   * @param defaultValue
   *   Default value if key not found
   * @return
   *   Integer value
   */
  def getInt(
    config: Map[String, String],
    key: String,
    defaultValue: Int
  ): Int =
    config.get(key).map(_.toInt).getOrElse(defaultValue)

  /**
   * Get a string configuration value from the config map with a default value.
   *
   * @param config
   *   Configuration map
   * @param key
   *   Configuration key
   * @param defaultValue
   *   Default value if key not found
   * @return
   *   String value
   */
  def getString(
    config: Map[String, String],
    key: String,
    defaultValue: String
  ): String =
    config.getOrElse(key, defaultValue)

  /**
   * Get the explicit list of columns to collect statistics for.
   * Returns None if not configured (will fall back to numIndexedCols).
   *
   * @param config
   *   Configuration map
   * @return
   *   Optional set of column names
   */
  def getDataSkippingStatsColumns(config: Map[String, String]): Option[Set[String]] =
    config.get(DATA_SKIPPING_STATS_COLUMNS)
      .orElse(config.get(DATA_SKIPPING_STATS_COLUMNS.toLowerCase))
      .filter(_.trim.nonEmpty)
      .map { value =>
        value.split(",").map(_.trim).filter(_.nonEmpty).toSet
      }

  /**
   * Get the number of columns to collect statistics for.
   * Returns -1 if all columns should be indexed.
   *
   * @param config
   *   Configuration map
   * @return
   *   Number of columns to index (-1 for all)
   */
  def getDataSkippingNumIndexedCols(config: Map[String, String]): Int =
    config.get(DATA_SKIPPING_NUM_INDEXED_COLS)
      .orElse(config.get(DATA_SKIPPING_NUM_INDEXED_COLS.toLowerCase))
      .map(_.toInt)
      .getOrElse(DEFAULT_DATA_SKIPPING_NUM_INDEXED_COLS)
}
