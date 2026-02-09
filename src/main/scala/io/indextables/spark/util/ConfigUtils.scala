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

import com.google.common.cache.{Cache, CacheBuilder}
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
  val DATA_SKIPPING_STATS_COLUMNS    = "spark.indextables.dataSkippingStatsColumns"
  val DATA_SKIPPING_NUM_INDEXED_COLS = "spark.indextables.dataSkippingNumIndexedCols"

  // Default values
  val DEFAULT_STATS_TRUNCATION_ENABLED       = true
  val DEFAULT_STATS_TRUNCATION_MAX_LENGTH    = 32
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
    def getConfig(configKey: String, default: String): String =
      configMap
        .get(configKey)
        .orElse(configMap.get(configKey.toLowerCase))
        .getOrElse(default)

    def getConfigOption(configKey: String): Option[String] =
      // Try both the original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
      configMap.get(configKey).orElse(configMap.get(configKey.toLowerCase))

    // Resolve AWS credentials through credential provider if configured.
    // This is essential for Unity Catalog and other custom credential providers
    // that don't store credentials directly in config but resolve them dynamically.
    val resolvedAwsCreds = tablePathOpt.flatMap { tablePath =>
      try
        io.indextables.spark.utils.CredentialProviderFactory.resolveAWSCredentialsFromConfig(configMap, tablePath)
      catch {
        case ex: Exception =>
          logger.warn(s"Failed to resolve AWS credentials from provider: ${ex.getMessage}")
          None
      }
    }

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
      // AWS credentials - resolved through credential provider if configured (e.g., UnityCatalog)
      // Falls back to direct config lookup if no provider or explicit credentials are present
      awsAccessKey = resolvedAwsCreds.map(_.accessKey).orElse(getConfigOption("spark.indextables.aws.accessKey")),
      awsSecretKey = resolvedAwsCreds.map(_.secretKey).orElse(getConfigOption("spark.indextables.aws.secretKey")),
      awsSessionToken =
        resolvedAwsCreds.flatMap(_.sessionToken).orElse(getConfigOption("spark.indextables.aws.sessionToken")),
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
        getConfigOption("spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment").map(_.toInt),
      // L2 Disk Cache configuration (persistent NVMe caching)
      diskCacheEnabled = getConfigOption("spark.indextables.cache.disk.enabled").map(_.toBoolean),
      diskCachePath = getConfigOption("spark.indextables.cache.disk.path"),
      diskCacheMaxSize = getConfigOption("spark.indextables.cache.disk.maxSize")
        .map(SplitCacheConfig.parseSizeString),
      diskCacheCompression = getConfigOption("spark.indextables.cache.disk.compression"),
      diskCacheMinCompressSize = getConfigOption("spark.indextables.cache.disk.minCompressSize")
        .map(SplitCacheConfig.parseSizeString),
      diskCacheManifestSyncInterval = getConfigOption("spark.indextables.cache.disk.manifestSyncInterval").map(_.toInt),
      // Companion mode (parquet companion splits)
      companionSourceTableRoot = getConfigOption("spark.indextables.companion.parquetTableRoot")
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
   * Get the explicit list of columns to collect statistics for. Returns None if not configured (will fall back to
   * numIndexedCols).
   *
   * @param config
   *   Configuration map
   * @return
   *   Optional set of column names
   */
  def getDataSkippingStatsColumns(config: Map[String, String]): Option[Set[String]] =
    config
      .get(DATA_SKIPPING_STATS_COLUMNS)
      .orElse(config.get(DATA_SKIPPING_STATS_COLUMNS.toLowerCase))
      .filter(_.trim.nonEmpty)
      .map(value => value.split(",").map(_.trim).filter(_.nonEmpty).toSet)

  /**
   * Get the number of columns to collect statistics for. Returns -1 if all columns should be indexed.
   *
   * @param config
   *   Configuration map
   * @return
   *   Number of columns to index (-1 for all)
   */
  def getDataSkippingNumIndexedCols(config: Map[String, String]): Int =
    config
      .get(DATA_SKIPPING_NUM_INDEXED_COLS)
      .orElse(config.get(DATA_SKIPPING_NUM_INDEXED_COLS.toLowerCase))
      .map(_.toInt)
      .getOrElse(DEFAULT_DATA_SKIPPING_NUM_INDEXED_COLS)

  /**
   * Creates a Hadoop Configuration populated with spark.indextables.* keys from the config map.
   *
   * This is essential for executor-side credential resolution where credential providers (e.g.,
   * UnityCatalogAWSCredentialProvider) need access to configuration properties like workspace URLs and API tokens.
   *
   * IMPORTANT: This was introduced to fix a regression from PR #100 which removed driver-side credential resolution.
   * Without populating the Hadoop config, credential providers would fail with "not configured" errors on executors.
   *
   * @param config
   *   Configuration map containing spark.indextables.* keys
   * @return
   *   Hadoop Configuration populated with indextables configuration
   */
  def createHadoopConfiguration(config: Map[String, String]): org.apache.hadoop.conf.Configuration = {
    val conf = new org.apache.hadoop.conf.Configuration()
    config.foreach {
      case (key, value) if key.startsWith("spark.indextables.") =>
        conf.set(key, value)
      case _ => // Ignore non-indextables keys
    }
    conf
  }

  /**
   * JVM-wide cache for Hadoop Configuration objects to avoid repeated creation.
   *
   * PERFORMANCE OPTIMIZATION: Creating Hadoop Configuration is expensive due to XML parsing and class loading. This
   * cache reduces overhead for non-UC providers that still need Hadoop Configuration by reusing instances with the same
   * config map content.
   *
   * Cache characteristics:
   *   - Maximum 50 entries (typical cluster has few distinct config combinations)
   *   - 1 hour expiration (configurations rarely change during a session)
   *   - Keyed by hash of relevant spark.indextables.* config values
   */
  private val hadoopConfigCache: Cache[Integer, org.apache.hadoop.conf.Configuration] = CacheBuilder
    .newBuilder()
    .maximumSize(50)
    .expireAfterAccess(60, java.util.concurrent.TimeUnit.MINUTES)
    .build[Integer, org.apache.hadoop.conf.Configuration]()

  /**
   * Get or create a Hadoop Configuration from a config map with caching.
   *
   * This method is the preferred way to obtain Hadoop Configuration objects when working with config maps, as it reuses
   * cached configurations to avoid the expensive creation of new Configuration objects.
   *
   * IMPORTANT: For UnityCatalogAWSCredentialProvider, prefer using the Map-based factory method directly to avoid
   * Hadoop Configuration entirely. This cached method is for other credential providers that require Hadoop
   * Configuration.
   *
   * @param config
   *   Configuration map containing spark.indextables.* keys
   * @return
   *   Cached or newly created Hadoop Configuration
   */
  def getOrCreateHadoopConfiguration(config: Map[String, String]): org.apache.hadoop.conf.Configuration = {
    // Extract only relevant keys for the cache key computation
    // This ensures that unrelated config changes don't invalidate the cache
    val relevantConfig = config.filter(_._1.startsWith("spark.indextables."))
    val cacheKey       = relevantConfig.hashCode()

    // Try to get from cache first
    var cached = hadoopConfigCache.getIfPresent(cacheKey)
    if (cached != null) {
      logger.debug(s"Reusing cached Hadoop Configuration (cache key: $cacheKey)")
      return cached
    }

    // Create new configuration
    val newConf = createHadoopConfiguration(config)

    // Try to put in cache (another thread might have created one already)
    synchronized {
      cached = hadoopConfigCache.getIfPresent(cacheKey)
      if (cached != null) {
        // Another thread beat us to it
        return cached
      }
      hadoopConfigCache.put(cacheKey, newConf)
    }

    logger.debug(s"Created and cached new Hadoop Configuration (cache key: $cacheKey)")
    newConf
  }

  /** Clear the Hadoop Configuration cache. Useful for testing. */
  def clearHadoopConfigCache(): Unit = {
    hadoopConfigCache.invalidateAll()
    logger.debug("Cleared Hadoop Configuration cache")
  }
}
