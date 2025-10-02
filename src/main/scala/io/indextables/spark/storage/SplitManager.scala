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

import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitCacheManager
import io.indextables.spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.util.Try
import java.util.UUID
import java.time.Instant
import java.io.File
import java.nio.file.{Files, Paths}

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

    if (protocol == ProtocolBasedIOFactory.S3Protocol) {
      // For S3, create the split locally first, then upload
      val tempSplitPath = s"/tmp/tantivy4spark-split-${UUID.randomUUID()}.split"

      try {
        // Create split file locally
        val metadata = QuickwitSplit.convertIndexFromPath(indexPath, tempSplitPath, config)
        logger.info(s"Split created locally: ${metadata.getSplitId()}, ${metadata.getNumDocs()} documents")

        // LOG DOCMAPPINGJSON TO INVESTIGATE FAST FIELDS
        val docMappingJson = metadata.getDocMappingJson()
        logger.warn(s"🔍 SPLIT CREATED: docMappingJson from tantivy4java:")
        logger.warn(s"🔍 SPLIT CREATED: $docMappingJson")
        if (docMappingJson != null && docMappingJson.contains("\"fast\":false")) {
          logger.error(s"❌ SPLIT CREATED WITH fast=false! This is the bug!")
        } else if (docMappingJson != null && docMappingJson.contains("\"fast\":true")) {
          logger.warn(s"✅ SPLIT CREATED WITH fast=true - correct!")
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
            logger.info(s"Split uploaded: $outputPath (${splitContent.length} bytes)")
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
      // For non-S3 paths, create directly
      try {
        val metadata = QuickwitSplit.convertIndexFromPath(indexPath, outputPath, config)
        logger.info(s"Split created successfully: ${metadata.getSplitId()}, ${metadata.getNumDocs()} documents")

        // LOG DOCMAPPINGJSON TO INVESTIGATE FAST FIELDS
        val docMappingJson = metadata.getDocMappingJson()
        println(s"🔍 SPLIT CREATED (non-S3): docMappingJson from tantivy4java:")
        println(s"🔍 SPLIT CREATED (non-S3): $docMappingJson")
        if (docMappingJson != null && docMappingJson.contains("\"fast\":false")) {
          println(s"❌ SPLIT CREATED WITH fast=false! This is the bug!")
        } else if (docMappingJson != null && docMappingJson.contains("\"fast\":true")) {
          println(s"✅ SPLIT CREATED WITH fast=true - correct!")
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
  azureEndpoint: Option[String] = None,
  gcpProjectId: Option[String] = None,
  gcpServiceAccountKey: Option[String] = None,
  gcpCredentialsFile: Option[String] = None,
  gcpEndpoint: Option[String] = None) {

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
    logger.info(s"🔍 SplitCacheConfig AWS Verification:")
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
        logger.warn(s"⚠️  AWS region not provided - this may cause 'A region must be set when sending requests to S3' error in tantivy4java")
    }

    awsEndpoint.foreach { endpoint =>
      logger.info(s"🔧 Configuring AWS endpoint: $endpoint")
      config = config.withAwsEndpoint(endpoint)
    }
    awsPathStyleAccess.foreach { pathStyle =>
      logger.info(s"🔧 Configuring AWS path-style access: $pathStyle")
      config = config.withAwsPathStyleAccess(pathStyle)
    }

    // Azure configuration
    (azureAccountName, azureAccountKey) match {
      case (Some(name), Some(key)) =>
        config = config.withAzureCredentials(name, key)
      case _ => // Try connection string
        azureConnectionString.foreach(connStr => config = config.withAzureConnectionString(connStr))
    }

    azureEndpoint.foreach(endpoint => config = config.withAzureEndpoint(endpoint))

    // GCP configuration
    (gcpProjectId, gcpServiceAccountKey) match {
      case (Some(projectId), Some(serviceKey)) =>
        config = config.withGcpCredentials(projectId, serviceKey)
      case _ => // Try credentials file
        gcpCredentialsFile.foreach(credFile => config = config.withGcpCredentialsFile(credFile))
    }

    gcpEndpoint.foreach(endpoint => config = config.withGcpEndpoint(endpoint))

    logger.info(s"🔧 Final tantivy4java CacheConfig before returning: $config")
    config
  }
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
 * Global manager for split cache instances.
 *
 * Maintains JVM-wide split cache managers that are shared across all IndexTables4Spark operations within a single JVM
 * (e.g., all tasks in a Spark executor).
 */
object GlobalSplitCacheManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // JVM-wide cache managers indexed by cache key (includes all config elements including session tokens)
  @volatile private var cacheManagers: Map[String, SplitCacheManager] = Map.empty
  private val lock                                                    = new Object

  /**
   * Generate a cache key that includes all configuration elements including session tokens. This ensures cache managers
   * are not shared between different credential configurations.
   */
  private def generateCacheKey(config: SplitCacheConfig): String = {
    val keyElements = Seq(
      s"name=${config.cacheName}",
      s"maxSize=${config.maxCacheSize}",
      s"maxConcurrent=${config.maxConcurrentLoads}",
      s"queryCache=${config.enableQueryCache}",
      s"splitCachePath=${config.splitCachePath.orElse(SplitCacheConfig.getDefaultCachePath()).getOrElse("default")}",
      s"awsKey=${config.awsAccessKey.getOrElse("none")}",
      s"awsSecret=${config.awsSecretKey.map(_ => "***").getOrElse("none")}",
      s"awsToken=${config.awsSessionToken.map(_ => "***").getOrElse("none")}",
      s"awsRegion=${config.awsRegion.getOrElse("none")}",
      s"awsEndpoint=${config.awsEndpoint.getOrElse("none")}",
      s"azureName=${config.azureAccountName.getOrElse("none")}",
      s"azureKey=${config.azureAccountKey.map(_ => "***").getOrElse("none")}",
      s"azureConn=${config.azureConnectionString.map(_ => "***").getOrElse("none")}",
      s"azureEndpoint=${config.azureEndpoint.getOrElse("none")}",
      s"gcpProject=${config.gcpProjectId.getOrElse("none")}",
      s"gcpKey=${config.gcpServiceAccountKey.map(_ => "***").getOrElse("none")}",
      s"gcpFile=${config.gcpCredentialsFile.getOrElse("none")}",
      s"gcpEndpoint=${config.gcpEndpoint.getOrElse("none")}"
    )
    keyElements.mkString("|")
  }

  /**
   * Get or create a global split cache manager. Cache managers are now keyed by all config elements (including session
   * tokens) to prevent configuration mismatches when reusing cache instances.
   */
  def getInstance(config: SplitCacheConfig): SplitCacheManager = {
    val cacheKey = generateCacheKey(config)

    logger.debug(s"GlobalSplitCacheManager.getInstance called with cacheName: ${config.cacheName}")
    logger.debug(s"Current cache config - awsRegion: ${config.awsRegion.getOrElse("None")}, awsEndpoint: ${config.awsEndpoint.getOrElse("None")}")
    logger.debug(s"Generated cache key: $cacheKey")
    logger.debug(s"Existing cache managers: ${cacheManagers.keySet.size} entries")

    cacheManagers.get(cacheKey) match {
      case Some(manager) =>
        logger.debug(s"Reusing existing cache manager with matching configuration: ${config.cacheName}")
        manager
      case None =>
        lock.synchronized {
          // Double-check pattern
          cacheManagers.get(cacheKey) match {
            case Some(manager) => manager
            case None =>
              logger.info(
                s"Creating new global split cache manager: ${config.cacheName}, max size: ${config.maxCacheSize}"
              )
              logger.info(s"Cache key: $cacheKey")
              val javaConfig = config.toJavaCacheConfig()
              val manager    = SplitCacheManager.getInstance(javaConfig)
              cacheManagers = cacheManagers + (cacheKey -> manager)
              manager
          }
        }
    }
  }

  /** Close all cache managers (typically called during JVM shutdown). */
  def closeAll(): Unit =
    lock.synchronized {
      logger.info(s"Closing ${cacheManagers.size} split cache managers")
      cacheManagers.values.foreach { manager =>
        try
          manager.close()
        catch {
          case e: Exception =>
            logger.warn("Error closing cache manager", e)
        }
      }
      cacheManagers = Map.empty
    }

  /**
   * Flush all global split cache managers. This will close all cache managers and clear the global registry. Returns
   * the number of cache managers that were flushed.
   */
  def flushAllCaches(): SplitCacheFlushResult =
    lock.synchronized {
      val flushedCount = cacheManagers.size
      logger.info(s"Flushing all split cache managers ($flushedCount managers)")

      cacheManagers.values.foreach { manager =>
        try {
          manager.close()
          logger.debug(s"Flushed cache manager")
        } catch {
          case ex: Exception =>
            logger.warn(s"Error flushing cache manager: ${ex.getMessage}")
        }
      }

      cacheManagers = Map.empty
      logger.info(s"Successfully flushed all split cache managers")
      SplitCacheFlushResult(flushedCount)
    }

  /** Get statistics for all active cache managers. */
  def getGlobalStats(): Map[String, SplitCacheManager.GlobalCacheStats] =
    cacheManagers.map {
      case (cacheKey, manager) =>
        cacheKey -> manager.getGlobalCacheStats()
    }

  /** Get the number of active cache managers. */
  def getCacheManagerCount(): Int = cacheManagers.size

  /** Clear all cache managers (for testing purposes). */
  def clearAll(): Unit =
    lock.synchronized {
      cacheManagers.values.foreach { manager =>
        try
          manager.close()
        catch {
          case e: Exception =>
            logger.warn("Error closing cache manager during clear", e)
        }
      }
      cacheManagers = Map.empty
    }
}

// Result classes for cache flush operations
case class SplitCacheFlushResult(flushedManagers: Int)
case class SplitLocationFlushResult(clearedEntries: Int)
