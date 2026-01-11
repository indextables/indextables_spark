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
        getConfigOption("spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment").map(_.toInt),
      // L2 Disk Cache configuration (persistent NVMe caching)
      diskCacheEnabled = getConfigOption("spark.indextables.cache.disk.enabled").map(_.toBoolean),
      diskCachePath = getConfigOption("spark.indextables.cache.disk.path"),
      diskCacheMaxSize = getConfigOption("spark.indextables.cache.disk.maxSize")
        .map(SplitCacheConfig.parseSizeString),
      diskCacheCompression = getConfigOption("spark.indextables.cache.disk.compression"),
      diskCacheMinCompressSize = getConfigOption("spark.indextables.cache.disk.minCompressSize")
        .map(SplitCacheConfig.parseSizeString),
      diskCacheManifestSyncInterval = getConfigOption("spark.indextables.cache.disk.manifestSyncInterval").map(_.toInt)
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
   * Resolve AWS credentials from a custom credential provider on the driver.
   *
   * This method checks if `spark.indextables.aws.credentialsProviderClass` is configured.
   * If so, it instantiates the provider on the driver and calls getCredentials() to obtain
   * actual AWS credentials. The returned config map contains the resolved credentials
   * in the standard `aws.accessKey`, `aws.secretKey`, and `aws.sessionToken` keys.
   *
   * This enables workers to receive actual credentials without needing to run the
   * credential provider themselves (which may require driver-only resources like
   * Databricks API tokens).
   *
   * @param config
   *   Configuration map containing spark.indextables.* settings
   * @param tablePath
   *   Table path to use for credential resolution (e.g., s3://bucket/path)
   * @return
   *   Config map with resolved credentials, or original config if no provider configured
   */
  def resolveCredentialsFromProviderOnDriver(
    config: Map[String, String],
    tablePath: String
  ): Map[String, String] = {
    // Only resolve AWS credentials for S3 paths
    val isS3Path = tablePath != null && (tablePath.startsWith("s3://") || tablePath.startsWith("s3a://"))
    if (!isS3Path) {
      logger.debug(s"Skipping AWS credential resolution for non-S3 path: $tablePath")
      return config
    }

    val providerClassKey = "spark.indextables.aws.credentialsProviderClass"
    val providerClass = config.get(providerClassKey)
      .orElse(config.get(providerClassKey.toLowerCase))

    providerClass match {
      case Some(className) if className.trim.nonEmpty =>
        logger.info(s"Resolving AWS credentials using provider: $className for path: $tablePath")
        // If a credential provider is explicitly configured and fails, let the error propagate
        // Don't silently fall back - the user should know their provider configuration is broken
        resolveCredentialsUsingProvider(config, className.trim, tablePath)
      case _ =>
        // No custom provider configured, return config as-is
        config
    }
  }

  /**
   * Internal method to instantiate a credential provider and resolve credentials.
   */
  private def resolveCredentialsUsingProvider(
    config: Map[String, String],
    providerClassName: String,
    tablePath: String
  ): Map[String, String] = {
    import org.apache.hadoop.conf.Configuration
    import java.net.URI
    import com.amazonaws.auth.{AWSCredentialsProvider, AWSSessionCredentials}

    // Create Hadoop Configuration with all indextables configs
    val hadoopConf = new Configuration()
    config.foreach { case (key, value) =>
      hadoopConf.set(key, value)
    }

    // Parse the table path as URI
    val uri = new URI(tablePath)

    // Load the provider class
    val providerClass = Class.forName(providerClassName)

    // Find constructor that takes (URI, Configuration)
    val constructor = providerClass.getConstructor(classOf[URI], classOf[Configuration])

    // Instantiate the provider
    val provider = constructor.newInstance(uri, hadoopConf).asInstanceOf[AWSCredentialsProvider]

    // Get credentials
    logger.info(s"Calling getCredentials() on provider $providerClassName")
    val credentials = provider.getCredentials()

    if (credentials == null) {
      throw new RuntimeException(s"Provider $providerClassName returned null credentials")
    }

    // Extract credential values
    val accessKey = credentials.getAWSAccessKeyId
    val secretKey = credentials.getAWSSecretKey
    val sessionToken = credentials match {
      case session: AWSSessionCredentials => Some(session.getSessionToken)
      case _ => None
    }

    logger.info(s"Successfully resolved credentials from $providerClassName (accessKeyId: ${accessKey.take(8)}...)")

    // Update config with resolved credentials
    val updatedConfig = config +
      ("spark.indextables.aws.accessKey" -> accessKey) +
      ("spark.indextables.aws.secretKey" -> secretKey)

    // Add session token if present
    sessionToken match {
      case Some(token) =>
        updatedConfig + ("spark.indextables.aws.sessionToken" -> token)
      case None =>
        updatedConfig
    }
  }
}
