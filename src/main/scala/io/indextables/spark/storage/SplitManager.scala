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

package io.indextables.spark.storage

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.UUID

import scala.util.Try

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import io.indextables.spark.util.SizeParser
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitCacheManager
import org.slf4j.LoggerFactory

/**
 * Throttle for controlling parallelism of tantivy index to quickwit split conversions.
 *
 * The conversion process (QuickwitSplit.convertIndexFromPath) is CPU and I/O intensive, so we limit how many can run
 * concurrently across the executor to prevent resource exhaustion.
 *
 * Configuration:
 *   - spark.indextables.splitConversion.maxParallelism: Maximum concurrent conversions
 *   - Default: max(1, defaultParallelism / 2)
 */
object SplitConversionThrottle {
  private val logger = LoggerFactory.getLogger(getClass)

  @volatile private var semaphore: Option[Semaphore] = None
  @volatile private var currentMaxParallelism: Int   = -1
  private val lock                                   = new Object

  /**
   * Initialize or update the throttle with the given max parallelism.
   *
   * @param maxParallelism
   *   Maximum number of concurrent split conversions
   */
  def initialize(maxParallelism: Int): Unit = lock.synchronized {
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException(s"maxParallelism must be positive, got: $maxParallelism")
    }

    if (currentMaxParallelism != maxParallelism) {
      logger.info(s"Initializing split conversion throttle with maxParallelism=$maxParallelism")
      semaphore = Some(new Semaphore(maxParallelism, true)) // fair=true for FIFO ordering
      currentMaxParallelism = maxParallelism
    }
  }

  /**
   * Execute a block of code with throttling applied. Acquires a permit before execution and releases it after
   * completion.
   */
  def withThrottle[T](block: => T): T = {
    val sem = semaphore.getOrElse {
      throw new IllegalStateException("SplitConversionThrottle not initialized. Call initialize() first.")
    }

    logger.debug(s"Acquiring split conversion permit (available: ${sem.availablePermits()}/$currentMaxParallelism)")
    sem.acquire()
    try {
      logger.debug(s"Acquired split conversion permit (available: ${sem.availablePermits()}/$currentMaxParallelism)")
      block
    } finally {
      sem.release()
      logger.debug(s"Released split conversion permit (available: ${sem.availablePermits()}/$currentMaxParallelism)")
    }
  }

  /** Get the current max parallelism setting. */
  def getMaxParallelism: Int = currentMaxParallelism

  /** Get the number of available permits. */
  def getAvailablePermits: Int = semaphore.map(_.availablePermits()).getOrElse(0)
}

/**
 * Manager for IndexTables4Spark split operations using tantivy4java's QuickwitSplit functionality.
 *
 * Replaces the previous zip-based archive system with split files (.split) that use tantivy4java's optimized storage
 * format and caching system.
 */
object SplitManager {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a split file from a Tantivy index.
   *
   * @param indexPath
   *   Path to the Tantivy index directory
   * @param outputPath
   *   Path where the split file should be written (must end with .split)
   * @param partitionId
   *   Partition identifier for this split
   * @param nodeId
   *   Node identifier (typically hostname or Spark executor ID)
   * @param options
   *   Optional Spark options for cloud configuration
   * @param hadoopConf
   *   Optional Hadoop configuration for cloud storage
   * @return
   *   Metadata about the created split
   */
  def createSplit(
    indexPath: String,
    outputPath: String,
    partitionId: Long,
    nodeId: String,
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
    hadoopConf: Configuration = new Configuration()
  ): QuickwitSplit.SplitMetadata = {
    logger.info(s"Creating split from index: $indexPath -> $outputPath")

    // Check if the index directory exists and validate it
    val indexDir = new java.io.File(indexPath)
    if (!indexDir.exists()) {
      logger.error(s"Index directory does not exist: $indexPath")
      logger.error(s"Parent directory exists: ${indexDir.getParent} -> ${new java.io.File(indexDir.getParent).exists()}")
      throw new RuntimeException(s"Index directory does not exist: $indexPath")
    }
    if (!indexDir.isDirectory()) {
      logger.error(s"Index path is not a directory: $indexPath")
      throw new RuntimeException(s"Index path is not a directory: $indexPath")
    }

    // List files in the index directory for debugging
    val files = indexDir.listFiles()
    if (files == null || files.isEmpty) {
      logger.error(s"Index directory is empty: $indexPath")
      throw new RuntimeException(s"Index directory is empty: $indexPath")
    }

    logger.info(s"Index directory $indexPath contains ${files.length} files: ${files.map(_.getName).mkString(", ")}")

    // Also check that the output directory exists
    val outputDir = new java.io.File(outputPath).getParentFile
    if (outputDir != null && !outputDir.exists()) {
      logger.info(s"Creating output directory: ${outputDir.getAbsolutePath}")
      outputDir.mkdirs()
    }

    // Generate unique identifiers for the split
    val indexUid      = s"tantivy4spark-${UUID.randomUUID().toString}"
    val sourceId      = "tantivy4spark"
    val docMappingUid = "default"

    // Create split configuration
    val config = new QuickwitSplit.SplitConfig(
      indexUid,
      sourceId,
      nodeId,
      docMappingUid,
      partitionId,
      Instant.now(),
      Instant.now(),
      null,
      null
    )

    // Determine if we need to use cloud storage
    val protocol = ProtocolBasedIOFactory.determineProtocol(outputPath)

    if (protocol == ProtocolBasedIOFactory.S3Protocol || protocol == ProtocolBasedIOFactory.AzureProtocol) {
      // For S3 and Azure, create the split locally first, then upload
      val tempSplitPath = s"/tmp/tantivy4spark-split-${UUID.randomUUID()}.split"

      try {
        // Create split file locally with throttling to limit concurrent conversions
        val metadata = SplitConversionThrottle.withThrottle {
          QuickwitSplit.convertIndexFromPath(indexPath, tempSplitPath, config)
        }
        logger.info(s"Split created locally: ${metadata.getSplitId()}, ${metadata.getNumDocs()} documents")

        // LOG DOCMAPPINGJSON TO INVESTIGATE FAST FIELDS
        val docMappingJson = metadata.getDocMappingJson()
        logger.debug(s"SPLIT CREATED: docMappingJson from tantivy4java:")
        logger.debug(s"SPLIT CREATED: $docMappingJson")
        if (docMappingJson != null && docMappingJson.contains("\"fast\":false")) {
          logger.debug(s"SPLIT CREATED WITH fast=false in schema")
        } else if (docMappingJson != null && docMappingJson.contains("\"fast\":true")) {
          logger.debug(s"SPLIT CREATED WITH fast=true in schema")
        }

        // Upload to S3 using cloud storage provider with streaming for memory efficiency
        val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, options, hadoopConf)
        try {
          val splitFile = Paths.get(tempSplitPath)
          val fileSize  = Files.size(splitFile)

          // Configurable threshold for streaming uploads (default 100MB)
          val streamingThreshold = options.getLong("spark.indextables.s3.streamingThreshold", 100L * 1024 * 1024)

          if (fileSize > streamingThreshold) {
            logger.info(s"🚀 Using streaming upload for large split: $outputPath (${fileSize / (1024 * 1024)} MB)")
            // Use streaming upload for large files to avoid OOM
            val inputStream = Files.newInputStream(splitFile)
            try {
              cloudProvider.writeFileFromStream(outputPath, inputStream, Some(fileSize))
              logger.info(s"✅ Streaming upload completed: $outputPath (${fileSize / (1024 * 1024)} MB)")
            } finally
              inputStream.close()
          } else {
            logger.info(s"📄 Using traditional upload for small split: $outputPath (${fileSize / (1024 * 1024)} MB)")
            // Use traditional method for smaller files
            val splitContent = Files.readAllBytes(splitFile)
            cloudProvider.writeFile(outputPath, splitContent)
            logger.info(s"✅ Split uploaded to cloud storage: $outputPath (${splitContent.length} bytes)")
          }
        } finally
          cloudProvider.close()

        // Clean up temporary file
        Files.deleteIfExists(Paths.get(tempSplitPath))

        metadata
      } catch {
        case e: Exception =>
          // Clean up temporary file on error
          Files.deleteIfExists(Paths.get(tempSplitPath))
          logger.error(s"Failed to create and upload split from $indexPath to $outputPath", e)
          throw e
      }
    } else {
      // For non-S3 paths, create directly with throttling to limit concurrent conversions
      try {
        val metadata = SplitConversionThrottle.withThrottle {
          QuickwitSplit.convertIndexFromPath(indexPath, outputPath, config)
        }
        logger.info(s"Split created successfully: ${metadata.getSplitId()}, ${metadata.getNumDocs()} documents")

        // LOG DOCMAPPINGJSON TO INVESTIGATE FAST FIELDS
        val docMappingJson = metadata.getDocMappingJson()
        logger.debug(s"SPLIT CREATED (non-S3): docMappingJson from tantivy4java:")
        logger.debug(s"SPLIT CREATED (non-S3): $docMappingJson")
        if (docMappingJson != null && docMappingJson.contains("\"fast\":false")) {
          logger.debug(s"SPLIT CREATED WITH fast=false in schema")
        } else if (docMappingJson != null && docMappingJson.contains("\"fast\":true")) {
          logger.debug(s"SPLIT CREATED WITH fast=true in schema")
        }

        metadata
      } catch {
        case e: Exception =>
          logger.error(s"Failed to create split from $indexPath to $outputPath", e)
          throw e
      }
    }
  }

  /** Validate that a split file is well-formed and readable. */
  def validateSplit(splitPath: String): Boolean =
    try {
      val isValid = QuickwitSplit.validateSplit(splitPath)
      logger.debug(s"Split validation for $splitPath: $isValid")
      isValid
    } catch {
      case e: Exception =>
        logger.warn(s"Split validation failed for $splitPath", e)
        false
    }

  /** Read split metadata without fully loading the split. */
  def readSplitMetadata(splitPath: String): Option[QuickwitSplit.SplitMetadata] =
    Try {
      QuickwitSplit.readSplitMetadata(splitPath)
    }.toOption

  /** List all files contained within a split. */
  def listSplitFiles(splitPath: String): List[String] =
    try {
      import scala.jdk.CollectionConverters._
      QuickwitSplit.listSplitFiles(splitPath).asScala.toList
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to list files in split $splitPath", e)
        List.empty
    }
}

/** Configuration for the global split cache system. */
case class SplitCacheConfig(
  cacheName: String = "tantivy4spark-default-cache",
  maxCacheSize: Long = 200000000L, // 200MB default
  maxConcurrentLoads: Int = 8,
  enableQueryCache: Boolean = true,
  enableDocBatch: Boolean = true,        // Default to true for better performance
  docBatchMaxSize: Int = 1000,           // Maximum documents per batch when enabled
  splitCachePath: Option[String] = None, // Custom cache directory path
  awsAccessKey: Option[String] = None,
  awsSecretKey: Option[String] = None,
  awsSessionToken: Option[String] = None,
  awsRegion: Option[String] = None,
  awsEndpoint: Option[String] = None,
  awsPathStyleAccess: Option[Boolean] = None,
  azureAccountName: Option[String] = None,
  azureAccountKey: Option[String] = None,
  azureConnectionString: Option[String] = None,
  azureBearerToken: Option[String] = None,
  azureTenantId: Option[String] = None,
  azureClientId: Option[String] = None,
  azureClientSecret: Option[String] = None,
  azureEndpoint: Option[String] = None,
  gcpProjectId: Option[String] = None,
  gcpServiceAccountKey: Option[String] = None,
  gcpCredentialsFile: Option[String] = None,
  gcpEndpoint: Option[String] = None,
  // Batch optimization configuration
  batchOptimizationEnabled: Option[Boolean] = None,
  batchOptimizationProfile: Option[String] = None, // "conservative", "balanced", "aggressive", "disabled"
  batchOptMaxRangeSize: Option[Long] = None,       // bytes (parsed from string like "16M")
  batchOptGapTolerance: Option[Long] = None,       // bytes (parsed from string like "512K")
  batchOptMinDocs: Option[Int] = None,
  batchOptMaxConcurrentPrefetch: Option[Int] = None,
  // Adaptive tuning configuration
  adaptiveTuningEnabled: Option[Boolean] = None,
  adaptiveTuningMinBatches: Option[Int] = None) {

  private val logger = LoggerFactory.getLogger(classOf[SplitCacheConfig])

  /** Convert to tantivy4java CacheConfig. */
  def toJavaCacheConfig(): SplitCacheManager.CacheConfig = {
    var config = new SplitCacheManager.CacheConfig(cacheName)
      .withMaxCacheSize(maxCacheSize)
      .withMaxConcurrentLoads(maxConcurrentLoads)
      .withQueryCache(enableQueryCache)

    // Configure split cache directory path with auto-detection
    val effectiveCachePath = splitCachePath.orElse(SplitCacheConfig.getDefaultCachePath())
    effectiveCachePath.foreach { cachePath =>
      logger.info(
        s"🔧 Split cache directory configured: $cachePath (Note: GlobalCacheConfig initialization required separately)"
      )
      // Note: splitCachePath configuration is handled through GlobalCacheConfig.initialize()
      // before creating any SplitCacheManager instances - this is documented but not implemented
      // in this version of tantivy4java
    }

    // AWS configuration with detailed verification
    logger.info(s"SplitCacheConfig AWS Verification:")
    logger.info(s"  - awsAccessKey: ${awsAccessKey.map(k => s"${k.take(4)}...").getOrElse("None")}")
    logger.info(s"  - awsSecretKey: ${awsSecretKey.map(_ => "***").getOrElse("None")}")
    logger.info(s"  - awsRegion: ${awsRegion.getOrElse("None")}")
    logger.info(s"  - awsSessionToken: ${awsSessionToken.map(_ => "***").getOrElse("None")}")
    logger.info(s"  - awsEndpoint: ${awsEndpoint.getOrElse("None")}")

    // Configure AWS credentials (access key and secret key with optional session token)
    (awsAccessKey, awsSecretKey) match {
      case (Some(key), Some(secret)) =>
        logger.info(s"✅ AWS credentials present - configuring tantivy4java")
        config = awsSessionToken match {
          case Some(token) =>
            logger.info(
              s"🔧 Calling config.withAwsCredentials(accessKey=${key.take(4)}..., secretKey=***, sessionToken=***)"
            )
            val result = config.withAwsCredentials(key, secret, token)
            logger.info(s"🔧 withAwsCredentials returned: $result")
            result
          case None =>
            logger.info(s"🔧 Calling config.withAwsCredentials(accessKey=${key.take(4)}..., secretKey=***)")
            val result = config.withAwsCredentials(key, secret)
            logger.info(s"🔧 withAwsCredentials returned: $result")
            result
        }
      case (Some(key), None) =>
        logger.warn(
          s"⚠️  AWS access key provided but SECRET KEY is missing! accessKey=${key.take(4)}..., secretKey=None"
        )
      case (None, Some(_)) =>
        logger.warn(s"⚠️  AWS secret key provided but ACCESS KEY is missing! accessKey=None, secretKey=***")
      case _ => // No AWS credentials provided
        logger.debug("🔧 SplitCacheConfig: No AWS credentials provided - using default credentials chain")
    }

    // Configure AWS region separately
    awsRegion match {
      case Some(region) =>
        logger.info(s"🔧 Calling config.withAwsRegion(region=$region)")
        config = config.withAwsRegion(region)
        logger.info(s"🔧 withAwsRegion returned: $config")
      case None =>
        // Only warn about missing AWS region if AWS credentials are configured (or if Azure/GCP are not configured)
        val hasAwsCredentials   = awsAccessKey.isDefined || awsSecretKey.isDefined
        val hasAzureCredentials = azureAccountName.isDefined || azureConnectionString.isDefined
        val hasGcpCredentials   = gcpProjectId.isDefined || gcpServiceAccountKey.isDefined

        if (hasAwsCredentials && !hasAzureCredentials && !hasGcpCredentials) {
          logger.warn(s"⚠️  AWS region not provided - this may cause 'A region must be set when sending requests to S3' error in tantivy4java")
        } else {
          logger.debug(s"🔧 AWS region not provided, but using Azure/GCP or no cloud credentials configured")
        }
    }

    awsEndpoint.foreach { endpoint =>
      logger.info(s"🔧 Configuring AWS endpoint: $endpoint")
      config = config.withAwsEndpoint(endpoint)
    }
    awsPathStyleAccess.foreach { pathStyle =>
      logger.info(s"🔧 Configuring AWS path-style access: $pathStyle")
      config = config.withAwsPathStyleAccess(pathStyle)
    }

    // Azure configuration with detailed verification
    // Priority: 1) Bearer Token, 2) Account Key, 3) Connection String
    logger.debug(s"SplitCacheConfig Azure Verification:")
    logger.debug(s"  - azureAccountName: ${azureAccountName.getOrElse("None")}")
    logger.debug(s"  - azureAccountKey: ${azureAccountKey.map(_ => "***").getOrElse("None")}")
    logger.debug(s"  - azureConnectionString: ${azureConnectionString.map(_ => "***").getOrElse("None")}")
    logger.debug(s"  - azureBearerToken: ${azureBearerToken.map(t => s"***${t.takeRight(10)}").getOrElse("None")}")
    logger.debug(s"  - azureTenantId: ${azureTenantId.getOrElse("None")}")
    logger.debug(s"  - azureClientId: ${azureClientId.getOrElse("None")}")
    logger.debug(s"  - azureClientSecret: ${azureClientSecret.map(_ => "***").getOrElse("None")}")
    logger.debug(s"  - azureEndpoint: ${azureEndpoint.getOrElse("None")}")

    // Priority 1: Bearer Token (OAuth)
    (azureAccountName, azureBearerToken) match {
      case (Some(name), Some(token)) =>
        logger.debug(s"✅ Azure OAuth bearer token present - configuring tantivy4java")
        logger.debug(s"🔧 Calling config.withAzureBearerToken(accountName=$name, bearerToken=***)")
        config = config.withAzureBearerToken(name, token)
        logger.debug(s"🔧 withAzureBearerToken returned: $config")
      case (Some(name), None) =>
        // Priority 2: Account Key
        azureAccountKey match {
          case Some(key) =>
            logger.debug(s"✅ Azure account key present - configuring tantivy4java")
            logger.debug(s"🔧 Calling config.withAzureCredentials(accountName=$name, accountKey=***)")
            config = config.withAzureCredentials(name, key)
            logger.debug(s"🔧 withAzureCredentials returned: $config")
          case None =>
            logger.warn(s"⚠️  Azure account name provided but neither bearer token nor account key is present")
        }
      case (None, Some(_)) =>
        logger.warn(s"⚠️  Azure bearer token provided but ACCOUNT NAME is missing!")
      case (None, None) =>
        // Priority 3: Connection String
        logger.debug(s"⚠️  No Azure account name - checking for connection string")
        azureConnectionString.foreach { connStr =>
          logger.debug(s"🔧 Calling config.withAzureConnectionString(connectionString=***)")
          config = config.withAzureConnectionString(connStr)
          logger.debug(s"🔧 withAzureConnectionString returned: $config")
        }
    }

    // Note: Azure endpoint configuration not supported in tantivy4java CacheConfig API

    // GCP configuration
    (gcpProjectId, gcpServiceAccountKey) match {
      case (Some(projectId), Some(serviceKey)) =>
        config = config.withGcpCredentials(projectId, serviceKey)
      case _ => // Try credentials file
        gcpCredentialsFile.foreach(credFile => config = config.withGcpCredentialsFile(credFile))
    }

    // Note: GCP endpoint configuration not supported in tantivy4java CacheConfig API

    // Configure batch optimization
    createBatchOptimizationConfig().foreach { batchOptConfig =>
      logger.debug(s"Batch optimization configured: profile=${batchOptimizationProfile.getOrElse("balanced")}")
      config = config.withBatchOptimization(batchOptConfig)
    }

    logger.debug(s"Final tantivy4java CacheConfig: $config")
    config
  }

  /**
   * Create BatchOptimizationConfig from configuration parameters. Returns None if optimization is explicitly disabled.
   */
  private def createBatchOptimizationConfig(): Option[io.indextables.tantivy4java.split.BatchOptimizationConfig] = {
    import io.indextables.tantivy4java.split.BatchOptimizationConfig

    // If explicitly disabled, return disabled config
    if (batchOptimizationEnabled.contains(false)) {
      logger.debug("Batch optimization explicitly disabled")
      return Some(BatchOptimizationConfig.disabled())
    }

    // Use preset profile if specified
    batchOptimizationProfile match {
      case Some("disabled") =>
        logger.debug("Using disabled batch optimization profile")
        return Some(BatchOptimizationConfig.disabled())

      case Some("conservative") =>
        logger.debug("Using conservative batch optimization profile")
        var config = BatchOptimizationConfig.conservative()
        config = applyCustomBatchOptParams(config)
        return Some(config)

      case Some("balanced") =>
        logger.debug("Using balanced batch optimization profile")
        var config = BatchOptimizationConfig.balanced()
        config = applyCustomBatchOptParams(config)
        return Some(config)

      case Some("aggressive") =>
        logger.debug("Using aggressive batch optimization profile")
        var config = BatchOptimizationConfig.aggressive()
        config = applyCustomBatchOptParams(config)
        return Some(config)

      case Some(other) =>
        logger.warn(
          s"Unknown batch optimization profile: '$other'. " +
            "Valid profiles: conservative, balanced, aggressive, disabled. Using 'balanced' as default."
        )
        var config = BatchOptimizationConfig.balanced()
        config = applyCustomBatchOptParams(config)
        return Some(config)

      case None =>
        // If enabled without profile, use balanced as default
        if (batchOptimizationEnabled.contains(true)) {
          logger.debug("Batch optimization enabled with default balanced profile")
          var config = BatchOptimizationConfig.balanced()
          config = applyCustomBatchOptParams(config)
          return Some(config)
        }

        // If any custom parameters are specified, use balanced as base
        if (hasAnyBatchOptParams) {
          logger.debug("Custom batch optimization parameters specified, using balanced as base")
          var config = BatchOptimizationConfig.balanced()
          config = applyCustomBatchOptParams(config)
          return Some(config)
        }

        // Default: enabled with balanced profile (don't log - this is the normal case)
        var config = BatchOptimizationConfig.balanced()
        config = applyCustomBatchOptParams(config)
        Some(config)
    }
  }

  /** Apply custom batch optimization parameters to an existing config. */
  private def applyCustomBatchOptParams(
    config: io.indextables.tantivy4java.split.BatchOptimizationConfig
  ): io.indextables.tantivy4java.split.BatchOptimizationConfig = {
    var result = config

    // Apply custom parameters if specified
    batchOptMaxRangeSize.foreach { size =>
      logger.debug(s"Custom maxRangeSize: $size bytes")
      result = result.setMaxRangeSize(size)
    }

    batchOptGapTolerance.foreach { gap =>
      logger.debug(s"Custom gapTolerance: $gap bytes")
      result = result.setGapTolerance(gap)
    }

    batchOptMinDocs.foreach { minDocs =>
      logger.debug(s"Custom minDocsForOptimization: $minDocs")
      result = result.setMinDocsForOptimization(minDocs)
    }

    batchOptMaxConcurrentPrefetch.foreach { maxConcurrent =>
      logger.debug(s"Custom maxConcurrentPrefetch: $maxConcurrent")
      result = result.setMaxConcurrentPrefetch(maxConcurrent)
    }

    result
  }

  /** Check if any custom batch optimization parameters are specified. */
  private def hasAnyBatchOptParams: Boolean =
    batchOptMaxRangeSize.isDefined ||
      batchOptGapTolerance.isDefined ||
      batchOptMinDocs.isDefined ||
      batchOptMaxConcurrentPrefetch.isDefined
}

/** Companion object for SplitCacheConfig with auto-detection utilities. */
object SplitCacheConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Auto-detect the optimal cache directory path. Defaults to /local_disk0 if it exists and is writable, otherwise
   * returns None for system default.
   */
  def getDefaultCachePath(): Option[String] = {
    val localDisk0 = new java.io.File("/local_disk0")
    if (localDisk0.exists() && localDisk0.isDirectory && localDisk0.canWrite()) {
      logger.info("Auto-detected /local_disk0 for cache directory - using high-performance local storage")
      Some("/local_disk0/tantivy4spark-cache")
    } else {
      logger.debug("/local_disk0 not available - using system default cache directory")
      None
    }
  }

  /**
   * Get the optimal temp directory path for operations. Defaults to /local_disk0 if it exists and is writable,
   * otherwise returns None for system default.
   */
  def getDefaultTempPath(): Option[String] = {
    val localDisk0 = new java.io.File("/local_disk0")
    if (localDisk0.exists() && localDisk0.isDirectory && localDisk0.canWrite()) {
      logger.info("Auto-detected /local_disk0 for temp directory - using high-performance local storage")
      Some("/local_disk0/tantivy4spark-temp")
    } else {
      logger.debug("/local_disk0 not available - using system default temp directory")
      None
    }
  }

  /**
   * Parse size string with support for units: "123456" (bytes), "1K", "1M", "1G". Examples: "512K" -> 524288, "16M" ->
   * 16777216, "1G" -> 1073741824
   */
  def parseSizeString(sizeStr: String): Long = SizeParser.parseSize(sizeStr)
}

/**
 * Registry for tracking which hosts have cached which splits. This enables Spark to use preferredLocations for better
 * data locality.
 */
object SplitLocationRegistry {
  private val logger = LoggerFactory.getLogger(getClass)

  // Map from splitPath to Set of hostnames that have cached it
  @volatile private var splitLocations: Map[String, Set[String]] = Map.empty
  private val lock                                               = new Object

  /** Record that a split has been accessed/cached on a particular host. */
  def recordSplitAccess(splitPath: String, hostname: String): Unit =
    lock.synchronized {
      val currentHosts = splitLocations.getOrElse(splitPath, Set.empty)
      val updatedHosts = currentHosts + hostname
      splitLocations = splitLocations + (splitPath -> updatedHosts)
      logger.debug(s"Recorded split access: $splitPath on host $hostname (total hosts: ${updatedHosts.size})")
    }

  /** Get the list of hosts that have likely cached this split. */
  def getPreferredHosts(splitPath: String): Array[String] =
    splitLocations.getOrElse(splitPath, Set.empty).toArray

  /** Clear location tracking for a specific split (e.g., when it's no longer relevant). */
  def clearSplitLocations(splitPath: String): Unit =
    lock.synchronized {
      splitLocations = splitLocations - splitPath
      logger.debug(s"Cleared location tracking for split: $splitPath")
    }

  /** Get current hostname for this JVM. */
  def getCurrentHostname: String =
    try
      java.net.InetAddress.getLocalHost.getHostName
    catch {
      case ex: Exception =>
        logger.warn(s"Could not determine hostname, using 'unknown': ${ex.getMessage}")
        "unknown"
    }

  /** Clear all split location tracking information. Returns the number of entries that were cleared. */
  def clearAllLocations(): SplitLocationFlushResult =
    lock.synchronized {
      val clearedCount = splitLocations.size
      splitLocations = Map.empty
      logger.info(s"Cleared all split location tracking ($clearedCount entries)")
      SplitLocationFlushResult(clearedCount)
    }
}

/**
 * Global manager for split cache instances - thin wrapper around tantivy4java's SplitCacheManager.
 *
 * This object provides a Spark-friendly interface to tantivy4java's native caching system. It translates Spark
 * configuration to Java configuration and delegates to tantivy4java's singleton cache manager, which already handles
 * credential-aware caching internally.
 *
 * Key design decisions:
 *   - No separate cache map maintained at Scala level (delegates to tantivy4java)
 *   - Credential rotation is handled automatically by tantivy4java's comprehensive cache key
 *   - Session token changes result in different cache keys, creating new cache instances automatically
 *   - Provides Scala-friendly APIs that return case classes for Spark integration
 *
 * How credential rotation works:
 *   1. User refreshes AWS credentials (new session token) 2. New credentials passed to SplitCacheConfig 3.
 *      tantivy4java's CacheConfig.getCacheKey() generates key including session token 4. Different session token →
 *      Different cache key → New cache instance created 5. Old cache instance remains until explicitly closed or GC'd
 *      via shutdown hook
 */
object GlobalSplitCacheManager {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Get or create a global split cache manager.
   *
   * This method delegates to tantivy4java's SplitCacheManager.getInstance(), which maintains its own singleton cache
   * with credential-aware cache keys. When credentials change (including session token rotation), tantivy4java
   * automatically creates a new cache instance with the new credentials.
   *
   * The session token value itself is included in tantivy4java's cache key generation, ensuring that credential changes
   * result in new cache instances without any explicit timestamp tracking.
   */
  def getInstance(config: SplitCacheConfig): SplitCacheManager = {
    logger.debug(s"GlobalSplitCacheManager.getInstance called with cacheName: ${config.cacheName}")
    logger.debug(s"Current cache config - awsRegion: ${config.awsRegion.getOrElse("None")}, awsEndpoint: ${config.awsEndpoint.getOrElse("None")}")
    logger.debug(s"Session token present: ${config.awsSessionToken.isDefined}")

    // Convert Scala config to Java config and delegate to tantivy4java's singleton
    val javaConfig = config.toJavaCacheConfig()
    val manager    = SplitCacheManager.getInstance(javaConfig)

    logger.debug(s"Retrieved cache manager from tantivy4java: ${config.cacheName}")
    manager
  }

  /**
   * Close all cache managers (typically called during JVM shutdown).
   *
   * Delegates to tantivy4java's getAllInstances() to find and close all cache managers.
   */
  def closeAll(): Unit = {
    logger.info(s"Closing all tantivy4java split cache managers")
    import scala.jdk.CollectionConverters._
    val instances = SplitCacheManager.getAllInstances().asScala
    instances.values.foreach { manager =>
      try
        manager.close()
      catch {
        case e: Exception =>
          logger.warn("Error closing cache manager", e)
      }
    }
  }

  /**
   * Flush all global split cache managers. This will close all cache managers managed by tantivy4java. Returns the
   * number of cache managers that were flushed.
   *
   * Note: After flushing, tantivy4java's shutdown hook will clean up the instances map.
   */
  def flushAllCaches(): SplitCacheFlushResult = {
    import scala.jdk.CollectionConverters._
    val instances    = SplitCacheManager.getAllInstances().asScala
    val flushedCount = instances.size

    logger.info(s"Flushing all split cache managers ($flushedCount managers)")

    instances.values.foreach { manager =>
      try {
        manager.close()
        logger.debug(s"Flushed cache manager")
      } catch {
        case ex: Exception =>
          logger.warn(s"Error flushing cache manager: ${ex.getMessage}")
      }
    }

    logger.info(s"Successfully flushed all split cache managers")
    SplitCacheFlushResult(flushedCount)
  }

  /**
   * Get statistics for all active cache managers.
   *
   * Queries tantivy4java for all active cache instances and returns their stats.
   */
  def getGlobalStats(): Map[String, SplitCacheManager.GlobalCacheStats] = {
    import scala.jdk.CollectionConverters._
    SplitCacheManager
      .getAllInstances()
      .asScala
      .map {
        case (cacheKey, manager) =>
          cacheKey -> manager.getGlobalCacheStats()
      }
      .toMap
  }

  /**
   * Get the number of active cache managers.
   *
   * Delegates to tantivy4java to count active cache instances.
   */
  def getCacheManagerCount(): Int = {
    import scala.jdk.CollectionConverters._
    SplitCacheManager.getAllInstances().size
  }

  /**
   * Invalidate cache managers with stale credentials.
   *
   * This method closes all tantivy4java cache instances, forcing a fresh cache creation on next access with current
   * credentials. This is useful when you know credentials have been rotated and want to immediately invalidate all old
   * cache instances rather than waiting for them to be garbage collected.
   *
   * Note: In normal operation, you don't need to call this - tantivy4java automatically creates new cache instances
   * when credentials change (different session token = different cache key). This method is primarily for explicit
   * cleanup scenarios.
   *
   * @return
   *   Number of cache managers that were invalidated
   */
  def invalidateAllCredentialCaches(): Int = {
    logger.info("Invalidating all credential-based cache managers")
    val result = flushAllCaches()
    result.flushedManagers
  }

  /**
   * Clear all cache managers (for testing purposes).
   *
   * Delegates to tantivy4java to close all active cache instances.
   */
  def clearAll(): Unit = {
    import scala.jdk.CollectionConverters._
    val instances = SplitCacheManager.getAllInstances().asScala
    instances.values.foreach { manager =>
      try
        manager.close()
      catch {
        case e: Exception =>
          logger.warn("Error closing cache manager during clear", e)
      }
    }
  }
}

// Result classes for cache flush operations
case class SplitCacheFlushResult(flushedManagers: Int)
case class SplitLocationFlushResult(clearedEntries: Int)

/**
 * Batch optimization metrics collected from tantivy4java native layer.
 *
 * These metrics track the effectiveness of batch retrieval optimization, showing how many S3 requests were consolidated
 * and the resulting cost savings and efficiency gains.
 *
 * Metrics are collected from SplitCacheManager.getBatchMetrics() on executors and aggregated back to the driver via
 * Spark accumulators.
 *
 * @param totalOperations
 *   Number of batch retrieval operations performed
 * @param totalDocuments
 *   Total documents requested across all batch operations
 * @param totalRequests
 *   S3 requests that would have been made without optimization (baseline)
 * @param consolidatedRequests
 *   Actual S3 requests made after consolidation (5-10% of baseline)
 * @param bytesTransferred
 *   Total bytes transferred from S3 (includes useful data and gap bytes)
 * @param bytesWasted
 *   Estimated bytes wasted (gap data fetched to consolidate ranges)
 */
case class BatchOptMetrics(
  totalOperations: Long = 0,
  totalDocuments: Long = 0,
  totalRequests: Long = 0,
  consolidatedRequests: Long = 0,
  bytesTransferred: Long = 0,
  bytesWasted: Long = 0,
  totalPrefetchDurationMs: Long = 0,
  segmentsProcessed: Long = 0) {

  /** Check if metrics are empty (no operations recorded). */
  def isEmpty: Boolean = totalOperations == 0

  /**
   * Merge two metrics objects (for accumulator aggregation).
   *
   * @param other
   *   Metrics to merge
   * @return
   *   Combined metrics
   */
  def merge(other: BatchOptMetrics): BatchOptMetrics = BatchOptMetrics(
    totalOperations = this.totalOperations + other.totalOperations,
    totalDocuments = this.totalDocuments + other.totalDocuments,
    totalRequests = this.totalRequests + other.totalRequests,
    consolidatedRequests = this.consolidatedRequests + other.consolidatedRequests,
    bytesTransferred = this.bytesTransferred + other.bytesTransferred,
    bytesWasted = this.bytesWasted + other.bytesWasted,
    totalPrefetchDurationMs = this.totalPrefetchDurationMs + other.totalPrefetchDurationMs,
    segmentsProcessed = this.segmentsProcessed + other.segmentsProcessed
  )

  /**
   * Calculate consolidation ratio (how many requests were avoided per actual request).
   *
   * Higher is better. Typical values: 10-20x for well-optimized batches.
   *
   * @return
   *   Consolidation ratio (e.g., 15.0 means 15 requests consolidated into 1)
   */
  def consolidationRatio: Double =
    if (consolidatedRequests == 0) 0.0
    else totalRequests.toDouble / consolidatedRequests.toDouble

  /**
   * Calculate cost savings percentage (reduction in S3 requests).
   *
   * Typical values: 90-95% for production workloads.
   *
   * @return
   *   Cost savings as percentage (0-100)
   */
  def costSavingsPercent: Double =
    if (totalRequests == 0) 0.0
    else ((totalRequests - consolidatedRequests).toDouble / totalRequests.toDouble) * 100.0

  /**
   * Calculate bandwidth efficiency percentage (useful bytes vs. wasted bytes).
   *
   * Higher is better. Typical values: 85-95%.
   *
   * @return
   *   Efficiency percentage (0-100)
   */
  def efficiencyPercent: Double =
    if (bytesTransferred == 0) 0.0
    else ((bytesTransferred - bytesWasted).toDouble / bytesTransferred.toDouble) * 100.0

  /**
   * Calculate average documents per batch operation.
   *
   * @return
   *   Average batch size
   */
  def averageBatchSize: Double =
    if (totalOperations == 0) 0.0
    else totalDocuments.toDouble / totalOperations.toDouble

  /**
   * Format metrics as human-readable string for logging.
   *
   * @return
   *   Multi-line summary string
   */
  def summary: String =
    s"""Batch Optimization Metrics:
       |  Total Operations: $totalOperations
       |  Documents Retrieved: $totalDocuments (avg $averageBatchSize per batch)
       |  S3 Requests: $totalRequests → $consolidatedRequests (${consolidationRatio}x consolidation)
       |  Cost Savings: $costSavingsPercent%%
       |  Bandwidth Efficiency: $efficiencyPercent%%
       |  Data Transferred: $bytesTransferred bytes ($bytesWasted wasted)
       |""".stripMargin
}

object BatchOptMetrics {

  /** Empty metrics instance (no operations recorded). */
  val empty: BatchOptMetrics = BatchOptMetrics()

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Collect metrics from tantivy4java native layer.
   *
   * This should be called AFTER batch operations complete to capture accumulated statistics from
   * SplitCacheManager.getBatchMetrics().
   *
   * Note: Local file:// splits don't trigger batch optimization, so metrics will be zero. Only S3/Azure splits record
   * metrics.
   *
   * @return
   *   Current batch optimization metrics
   */
  def fromJavaMetrics(): BatchOptMetrics =
    try {
      val javaMetrics = SplitCacheManager.getBatchMetrics()

      BatchOptMetrics(
        totalOperations = javaMetrics.getTotalBatchOperations(),
        totalDocuments = javaMetrics.getTotalDocumentsRequested(),
        totalRequests = javaMetrics.getTotalRequests(),
        consolidatedRequests = javaMetrics.getConsolidatedRequests(),
        bytesTransferred = javaMetrics.getBytesTransferred(),
        bytesWasted = javaMetrics.getBytesWasted(),
        totalPrefetchDurationMs = javaMetrics.getTotalPrefetchDurationMs(),
        segmentsProcessed = javaMetrics.getSegmentsProcessed()
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect batch optimization metrics: ${e.getMessage}")
        BatchOptMetrics.empty
    }
}

/**
 * Spark accumulator for collecting batch optimization metrics from executors.
 *
 * This accumulator aggregates metrics across all executors and makes them accessible to the driver for validation in
 * tests and monitoring in production.
 *
 * Usage:
 * {{{
 * // In driver (test code):
 * val metricsAcc = new BatchOptimizationMetricsAccumulator()
 * spark.sparkContext.register(metricsAcc, "batch-optimization-metrics")
 *
 * // Pass to executors via broadcast or scan configuration
 * // ...
 *
 * // In executor (SplitPartitionReader):
 * val metrics = BatchOptMetrics.fromJavaMetrics()
 * metricsAcc.add(metrics)
 *
 * // Back in driver after query completes:
 * val finalMetrics = metricsAcc.value
 * println(s"Consolidation ratio: \${finalMetrics.consolidationRatio}x")
 * println(s"Cost savings: \${finalMetrics.costSavingsPercent}%")
 * }}}
 */
class BatchOptimizationMetricsAccumulator
    extends org.apache.spark.util.AccumulatorV2[BatchOptMetrics, BatchOptMetrics] {

  private var _metrics: BatchOptMetrics = BatchOptMetrics.empty

  override def isZero: Boolean = _metrics.isEmpty

  override def copy(): BatchOptimizationMetricsAccumulator = {
    val newAcc = new BatchOptimizationMetricsAccumulator
    newAcc._metrics = _metrics.copy()
    newAcc
  }

  override def reset(): Unit =
    _metrics = BatchOptMetrics.empty

  override def add(v: BatchOptMetrics): Unit =
    _metrics = _metrics.merge(v)

  override def merge(other: org.apache.spark.util.AccumulatorV2[BatchOptMetrics, BatchOptMetrics]): Unit =
    other match {
      case o: BatchOptimizationMetricsAccumulator =>
        _metrics = _metrics.merge(o._metrics)
      case _ =>
      // Ignore incompatible accumulator types
    }

  override def value: BatchOptMetrics = _metrics
}

/**
 * Global registry for batch optimization metrics accumulators.
 *
 * This registry allows tests and production monitoring to access batch optimization statistics after queries complete.
 * Accumulators are automatically registered when metrics collection is enabled via configuration.
 *
 * Thread-safe for concurrent access across multiple queries.
 *
 * Usage in tests:
 * {{{
 * // Enable metrics via configuration
 * spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")
 *
 * // Execute query
 * val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path").collect()
 *
 * // Access metrics using getMetricsDelta (computes delta from baseline captured at scan start)
 * val metrics = BatchOptMetricsRegistry.getMetricsDelta("s3://bucket/path")
 * assert(metrics.consolidationRatio >= 10.0)
 * assert(metrics.costSavingsPercent >= 90.0)
 *
 * // Cleanup baselines
 * BatchOptMetricsRegistry.clearAllBaselines()
 * }}}
 *
 * Usage in production:
 * {{{
 * // Enable metrics for monitoring
 * spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")
 *
 * // Later, check all active metrics
 * BatchOptMetricsRegistry.getAllMetrics().foreach { case (path, metrics) =>
 *   logger.info(s"Table $path: ${metrics.consolidationRatio}x consolidation, ${metrics.costSavingsPercent}% savings")
 * }
 * }}}
 */
object BatchOptMetricsRegistry {

  private val logger = LoggerFactory.getLogger(getClass)

  // Thread-safe map of table path -> metrics accumulator
  private val accumulators = new java.util.concurrent.ConcurrentHashMap[String, BatchOptimizationMetricsAccumulator]()

  // Thread-safe map of table path -> baseline metrics (captured at scan start)
  private val baselines = new java.util.concurrent.ConcurrentHashMap[String, BatchOptMetrics]()

  /**
   * Register a metrics accumulator for a table path.
   *
   * Called automatically by IndexTables4SparkScan when metrics collection is enabled. Tests and production code should
   * not call this directly.
   *
   * @param tablePath
   *   Table path (e.g., "s3://bucket/path")
   * @param accumulator
   *   Metrics accumulator to register
   */
  def register(tablePath: String, accumulator: BatchOptimizationMetricsAccumulator): Unit = {
    accumulators.put(tablePath, accumulator)
    logger.debug(s"📊 Registered batch optimization metrics for table: $tablePath")
  }

  /**
   * Get metrics for a specific table path.
   *
   * Call this after a query completes to access the accumulated batch optimization statistics.
   *
   * @param tablePath
   *   Table path (e.g., "s3://bucket/path")
   * @return
   *   Metrics if registered, None otherwise
   */
  def getMetrics(tablePath: String): Option[BatchOptMetrics] =
    Option(accumulators.get(tablePath)).map(_.value)

  /**
   * Get all registered metrics across all tables.
   *
   * Useful for production monitoring to see batch optimization performance across multiple queries.
   *
   * @return
   *   Map of table path -> metrics
   */
  def getAllMetrics(): Map[String, BatchOptMetrics] = {
    import scala.jdk.CollectionConverters._
    accumulators.asScala.map { case (path, acc) => (path, acc.value) }.toMap
  }

  /**
   * Get metrics summary for logging/monitoring.
   *
   * Returns a formatted string with key metrics for all registered tables.
   *
   * @return
   *   Multi-line summary string
   */
  def getSummary(): String = {
    val allMetrics = getAllMetrics()
    if (allMetrics.isEmpty) {
      "No batch optimization metrics registered"
    } else {
      val lines = allMetrics.map {
        case (path, metrics) =>
          s"  $path: ${metrics.consolidationRatio}x consolidation, ${metrics.costSavingsPercent}% savings, ${metrics.totalOperations} ops"
      }
      s"Batch Optimization Metrics (${allMetrics.size} tables):\n${lines.mkString("\n")}"
    }
  }

  /**
   * Check if metrics are registered for a table.
   *
   * @param tablePath
   *   Table path to check
   * @return
   *   True if metrics are registered
   */
  def hasMetrics(tablePath: String): Boolean =
    accumulators.containsKey(tablePath)

  /**
   * Clear metrics for a specific table path.
   *
   * Call this in test cleanup to prevent metrics from leaking between tests.
   *
   * @param tablePath
   *   Table path to clear
   */
  def clear(tablePath: String): Unit = {
    accumulators.remove(tablePath)
    logger.debug(s"🧹 Cleared batch optimization metrics for table: $tablePath")
  }

  /**
   * Clear all registered metrics and baselines.
   *
   * Useful for test suite cleanup or production maintenance.
   */
  def clearAll(): Unit = {
    val accCount      = accumulators.size()
    val baselineCount = baselines.size()
    accumulators.clear()
    baselines.clear()
    if (accCount > 0 || baselineCount > 0) {
      logger.debug(s"🧹 Cleared all batch optimization metrics ($accCount accumulators, $baselineCount baselines)")
    }
  }

  /**
   * Get the number of registered tables.
   *
   * @return
   *   Number of tables with registered metrics
   */
  def size(): Int = accumulators.size()

  /**
   * Capture baseline metrics at scan start for delta computation.
   *
   * Called automatically by IndexTables4SparkScan at the start of planInputPartitions(). This captures the current
   * global metrics values so that getMetricsDelta() can compute per-query metrics.
   *
   * @param tablePath
   *   Table path (e.g., "s3://bucket/path")
   */
  def captureBaseline(tablePath: String): Unit = {
    val baseline = BatchOptMetrics.fromJavaMetrics()
    baselines.put(tablePath, baseline)
    logger.debug(
      s"📊 Captured baseline metrics for $tablePath: ops=${baseline.totalOperations}, docs=${baseline.totalDocuments}"
    )
  }

  /**
   * Get per-query metrics delta since baseline was captured.
   *
   * Call this after a query completes to get metrics for just that query (not cumulative). The delta is computed as
   * (current global metrics - baseline captured at scan start).
   *
   * Note: This method removes the baseline after reading to prevent memory leaks in long-running applications. If you
   * need to read the delta multiple times, call captureBaseline() again before each query.
   *
   * @param tablePath
   *   Table path (e.g., "s3://bucket/path")
   * @return
   *   Metrics delta since baseline, or current metrics if no baseline exists (with warning)
   */
  def getMetricsDelta(tablePath: String): BatchOptMetrics = {
    val current = BatchOptMetrics.fromJavaMetrics()

    // Remove baseline after reading to prevent memory leak
    val baseline = Option(baselines.remove(tablePath)).getOrElse {
      logger.warn(
        s"📊 No baseline captured for $tablePath - returning cumulative metrics. " +
          "Call captureBaseline() before query execution for accurate per-query metrics."
      )
      BatchOptMetrics.empty
    }

    val delta = BatchOptMetrics(
      totalOperations = current.totalOperations - baseline.totalOperations,
      totalDocuments = current.totalDocuments - baseline.totalDocuments,
      totalRequests = current.totalRequests - baseline.totalRequests,
      consolidatedRequests = current.consolidatedRequests - baseline.consolidatedRequests,
      bytesTransferred = current.bytesTransferred - baseline.bytesTransferred,
      bytesWasted = current.bytesWasted - baseline.bytesWasted
    )

    logger.debug(
      s"📊 Metrics delta for $tablePath: ops=${delta.totalOperations}, docs=${delta.totalDocuments}, " +
        s"requests=${delta.totalRequests}, consolidated=${delta.consolidatedRequests}"
    )

    delta
  }

  /**
   * Get current global metrics (not delta).
   *
   * For per-query metrics, use getMetricsDelta() instead.
   *
   * @return
   *   Current global batch optimization metrics
   */
  def getGlobalMetrics(): BatchOptMetrics = BatchOptMetrics.fromJavaMetrics()

  /**
   * Clear baseline for a specific table.
   *
   * @param tablePath
   *   Table path to clear baseline for
   */
  def clearBaseline(tablePath: String): Unit =
    baselines.remove(tablePath)

  /** Clear all baselines. */
  def clearAllBaselines(): Unit =
    baselines.clear()
}
