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

package com.tantivy4spark.io

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory
import java.io.{InputStream, OutputStream, Closeable}
import scala.util.{Try}
import scala.jdk.CollectionConverters._

/**
 * High-performance cloud storage abstraction that bypasses Hadoop filesystem
 * for direct cloud API access. Supports asynchronous, parallel operations
 * optimized for each cloud provider.
 */
trait CloudStorageProvider extends Closeable {
  
  /**
   * List files in a directory with optional prefix filtering
   */
  def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo]
  
  /**
   * Check if a file exists
   */
  def exists(path: String): Boolean
  
  /**
   * Get file metadata
   */
  def getFileInfo(path: String): Option[CloudFileInfo]
  
  /**
   * Read entire file content
   */
  def readFile(path: String): Array[Byte]
  
  /**
   * Read a range of bytes from a file
   */
  def readRange(path: String, offset: Long, length: Long): Array[Byte]
  
  /**
   * Open an input stream for reading
   */
  def openInputStream(path: String): InputStream
  
  /**
   * Create an output stream for writing
   */
  def createOutputStream(path: String): OutputStream
  
  /**
   * Write content to a file
   */
  def writeFile(path: String, content: Array[Byte]): Unit
  
  /**
   * Delete a file
   */
  def deleteFile(path: String): Boolean
  
  /**
   * Create directory (if supported)
   */
  def createDirectory(path: String): Boolean
  
  /**
   * Read multiple files in parallel for better performance
   */
  def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]]
  
  /**
   * Check existence of multiple files in parallel
   */
  def existsParallel(paths: Seq[String]): Map[String, Boolean]
  
  /**
   * Get provider type for logging/metrics
   */
  def getProviderType: String
  
  /**
   * Normalize path for tantivy4java compatibility.
   * For S3 providers, converts s3a:// and s3n:// protocols to s3://.
   * Other providers can return the path unchanged.
   */
  def normalizePathForTantivy(path: String): String = path // Default implementation returns unchanged
}

/**
 * File metadata information
 */
case class CloudFileInfo(
  path: String,
  size: Long,
  modificationTime: Long,
  isDirectory: Boolean
)

/**
 * Cloud storage configuration extracted from Spark options and Hadoop config
 */
case class CloudStorageConfig(
  // AWS configuration
  awsAccessKey: Option[String] = None,
  awsSecretKey: Option[String] = None,
  awsSessionToken: Option[String] = None,
  awsRegion: Option[String] = None,
  awsEndpoint: Option[String] = None,
  awsPathStyleAccess: Boolean = false,
  
  // Azure configuration
  azureAccountName: Option[String] = None,
  azureAccountKey: Option[String] = None,
  azureConnectionString: Option[String] = None,
  azureEndpoint: Option[String] = None,
  azureContainerName: Option[String] = None,
  
  // GCP configuration
  gcpProjectId: Option[String] = None,
  gcpServiceAccountKey: Option[String] = None,
  gcpCredentialsFile: Option[String] = None,
  gcpEndpoint: Option[String] = None,
  gcpBucketName: Option[String] = None,
  
  // Performance tuning
  maxConnections: Int = 50,
  connectionTimeout: Int = 10000,
  readTimeout: Int = 30000,
  maxRetries: Option[Int] = None,
  bufferSize: Int = 16 * 1024 * 1024, // 16MB default

  // Multipart upload configuration
  multipartUploadThreshold: Option[Long] = None, // Defaults to 100MB if not specified
  maxConcurrency: Option[Int] = None             // Defaults to 4 if not specified
)

/**
 * Factory for creating cloud storage providers
 */
object CloudStorageProviderFactory {
  
  private val logger = LoggerFactory.getLogger(CloudStorageProviderFactory.getClass)
  
  /**
   * Create appropriate cloud storage provider based on path protocol
   */
  def createProvider(path: String, options: CaseInsensitiveStringMap, hadoopConf: Configuration): CloudStorageProvider = {
    val protocol = ProtocolBasedIOFactory.determineProtocol(path)
    
    logger.debug(s"CloudStorageProviderFactory.createProvider called for path: $path")
    logger.debug(s"Options passed (${options.size()} total tantivy4spark options)")
    import scala.jdk.CollectionConverters._
    if (logger.isDebugEnabled) {
      options.entrySet().asScala.foreach { entry =>
        if (entry.getKey.startsWith("spark.tantivy4spark.")) {
          val displayValue = if (entry.getKey.contains("secret") || entry.getKey.contains("session")) "***" else entry.getValue
          logger.debug(s"   ${entry.getKey} = $displayValue")
        }
      }
    }
    
    // For cloud storage, extract configuration from both options and Hadoop/Spark config
    // Also try to get configuration from Spark session if available
    val enrichedHadoopConf = enrichHadoopConfWithSparkConf(hadoopConf)
    val config = extractCloudConfig(options, enrichedHadoopConf)
    
    logger.info(s"Creating ${ProtocolBasedIOFactory.protocolName(protocol)} storage provider for path: $path")
    
    protocol match {
      case ProtocolBasedIOFactory.S3Protocol => 
        logger.info(s"S3 config - endpoint: ${config.awsEndpoint}, region: ${config.awsRegion}, pathStyle: ${config.awsPathStyleAccess}")
        logger.info(s"S3 credentials - accessKey: ${config.awsAccessKey.map(_.take(4) + "...").getOrElse("None")}, secretKey: ${config.awsSecretKey.map(_ => "***").getOrElse("None")}")
        new S3CloudStorageProvider(config)
      case ProtocolBasedIOFactory.HDFSProtocol | ProtocolBasedIOFactory.FileProtocol | ProtocolBasedIOFactory.LocalProtocol =>
        new HadoopCloudStorageProvider(hadoopConf)
    }
  }
  
  /**
   * Enrich Hadoop configuration with Spark configuration values
   */
  private def enrichHadoopConfWithSparkConf(hadoopConf: Configuration): Configuration = {
    try {
      // Try to get the active Spark session and copy relevant configurations
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.getActiveSession
      
      spark match {
        case Some(session) =>
          val enriched = new Configuration(hadoopConf)
          val sparkConf = session.conf
          
          // Copy Tantivy4Spark specific configurations
          // String configurations
          val stringConfigs = Seq(
            "spark.tantivy4spark.aws.accessKey",
            "spark.tantivy4spark.aws.secretKey",
            "spark.tantivy4spark.aws.sessionToken",
            "spark.tantivy4spark.aws.region",
            "spark.tantivy4spark.s3.endpoint"
          )
          
          // Boolean configurations
          val booleanConfigs = Seq(
            "spark.tantivy4spark.s3.pathStyleAccess"
          )
          
          // Copy string configurations - only if they exist
          stringConfigs.foreach { key =>
            try {
              // Try to get the config with a unique default value to detect if it exists
              val defaultValue = s"__NOT_SET__${key}__"
              val value = sparkConf.get(key, defaultValue)
              if (value != defaultValue) {
                enriched.set(key, value)
                logger.debug(s"Copied string Spark config to Hadoop conf: $key = $value")
                logger.info(s"✅ Copied string Spark config to Hadoop conf: $key = $value")
              } else {
                // Configuration doesn't exist - this is normal for optional configs like sessionToken
                logger.debug(s"🔧 Spark config key $key not set (optional)")
              }
            } catch {
              case ex: Exception => 
                logger.warn(s"Failed to copy string Spark config key $key: ${ex.getMessage}")
                logger.info(s"❌ Failed to copy string Spark config key $key: ${ex.getMessage}")
            }
          }
          
          // Copy boolean configurations - only if they exist
          booleanConfigs.foreach { key =>
            try {
              // Try to get the config with a unique default value to detect if it exists
              val defaultValue = s"__NOT_SET__${key}__"
              val value = sparkConf.get(key, defaultValue)
              if (value != defaultValue) {
                enriched.setBoolean(key, value.toBoolean)
                logger.debug(s"Copied boolean Spark config to Hadoop conf: $key = $value")
                logger.info(s"✅ Copied boolean Spark config to Hadoop conf: $key = $value")
              } else {
                // Configuration doesn't exist - this is normal for optional configs
                logger.debug(s"🔧 Spark config key $key not set (optional)")
              }
            } catch {
              case ex: Exception => 
                logger.warn(s"Failed to copy boolean Spark config key $key: ${ex.getMessage}")
                logger.info(s"❌ Failed to copy boolean Spark config key $key: ${ex.getMessage}")
            }
          }
          
          enriched
        case None =>
          logger.debug("No active Spark session found, using original Hadoop conf")
          hadoopConf
      }
    } catch {
      case ex: Exception =>
        logger.warn("Failed to enrich Hadoop conf with Spark conf", ex)
        hadoopConf
    }
  }
  
  /**
   * Extract cloud storage configuration from Spark options and Hadoop config
   */
  private def extractCloudConfig(options: CaseInsensitiveStringMap, hadoopConf: Configuration): CloudStorageConfig = {
    // Debug logging for configuration extraction
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Extracting cloud config from options:")
    options.entrySet().asScala.foreach { entry =>
      val displayValue = if (entry.getKey.contains("secret") || entry.getKey.contains("Secret")) "***" else entry.getValue
      logger.info(s"  ${entry.getKey} = $displayValue")
    }
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.tantivy4spark.aws.accessKey: ${hadoopConf.get("spark.tantivy4spark.aws.accessKey")}")
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.tantivy4spark.aws.region: ${hadoopConf.get("spark.tantivy4spark.aws.region")}")
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.hadoop.fs.s3a.access.key: ${hadoopConf.get("spark.hadoop.fs.s3a.access.key")}")
    
    // Trace credential extraction step by step
    val accessKeyFromOptions = Option(options.get("spark.tantivy4spark.aws.accessKey"))
    val accessKeyFromHadoopTantivy = Option(hadoopConf.get("spark.tantivy4spark.aws.accessKey"))
    val accessKeyFromHadoopS3a = Option(hadoopConf.get("spark.hadoop.fs.s3a.access.key"))
    val accessKeyFromS3a = Option(hadoopConf.get("fs.s3a.access.key"))
    
    logger.info(s"Access key extraction:")
    logger.info(s"  - From options: $accessKeyFromOptions")
    logger.info(s"  - From hadoop tantivy config: $accessKeyFromHadoopTantivy")
    logger.info(s"  - From hadoop s3a config: $accessKeyFromHadoopS3a")
    logger.info(s"  - From s3a config: $accessKeyFromS3a")
    
    val secretKeyFromOptions = Option(options.get("spark.tantivy4spark.aws.secretKey"))
    val secretKeyFromHadoopTantivy = Option(hadoopConf.get("spark.tantivy4spark.aws.secretKey"))
    val secretKeyFromHadoopS3a = Option(hadoopConf.get("spark.hadoop.fs.s3a.secret.key"))
    val secretKeyFromS3a = Option(hadoopConf.get("fs.s3a.secret.key"))
    
    logger.info(s"Secret key extraction:")
    logger.info(s"  - From options: ${secretKeyFromOptions.map(_ => "***")}")
    logger.info(s"  - From hadoop tantivy config: ${secretKeyFromHadoopTantivy.map(_ => "***")}")
    logger.info(s"  - From hadoop s3a config: ${secretKeyFromHadoopS3a.map(_ => "***")}")
    logger.info(s"  - From s3a config: ${secretKeyFromS3a.map(_ => "***")}")
    
    val finalAccessKey = accessKeyFromOptions
      .orElse(accessKeyFromHadoopTantivy)
      .orElse(accessKeyFromHadoopS3a)
      .orElse(accessKeyFromS3a)
    
    val finalSecretKey = secretKeyFromOptions
      .orElse(secretKeyFromHadoopTantivy)
      .orElse(secretKeyFromHadoopS3a)
      .orElse(secretKeyFromS3a)
    
    // Extract session token from all possible sources
    val sessionTokenFromOptions = Option(options.get("spark.tantivy4spark.aws.sessionToken"))
    val sessionTokenFromHadoopTantivy = Option(hadoopConf.get("spark.tantivy4spark.aws.sessionToken"))
    val sessionTokenFromS3a = Option(hadoopConf.get("fs.s3a.session.token"))
    
    val finalSessionToken = sessionTokenFromOptions
      .orElse(sessionTokenFromHadoopTantivy)
      .orElse(sessionTokenFromS3a)
    
    logger.info(s"Final credentials: accessKey=${finalAccessKey.map(_.take(4) + "...")}, secretKey=${finalSecretKey.map(_ => "***")}, sessionToken=${finalSessionToken.map(_ => "***")}")
    if (logger.isDebugEnabled) {
      logger.debug(s"CREDENTIAL EXTRACTION DEBUG:")
      logger.debug(s"  Final accessKey: ${finalAccessKey.map(_.take(4) + "...")}, secretKey present: ${finalSecretKey.isDefined}, sessionToken present: ${finalSessionToken.isDefined}")
    }
    
    // If credentials are still missing and we're in an executor context, log a warning
    if (finalAccessKey.isEmpty || finalSecretKey.isEmpty) {
      logger.warn("AWS credentials not found in configuration. S3CloudStorageProvider will fall back to DefaultCredentialsProvider.")
      logger.warn("This usually happens in Spark executor context where SparkSession is not available.")
      // Keep the important warnings visible
      // AWS credentials are missing which could cause issues
      if (logger.isWarnEnabled) {
        logger.warn("AWS credentials missing - falling back to DefaultCredentialsProvider")
      }
    }
    
    CloudStorageConfig(
      // AWS configuration - prioritize DataFrame options, then Spark conf, then Hadoop config
      awsAccessKey = finalAccessKey,
      awsSecretKey = finalSecretKey,
      awsSessionToken = finalSessionToken,
        
      awsRegion = {
        val regionFromOptions = Option(options.get("spark.tantivy4spark.aws.region"))
        val regionFromHadoopTantivy = Option(hadoopConf.get("spark.tantivy4spark.aws.region"))
        val regionFromHadoopS3a = Option(hadoopConf.get("fs.s3a.endpoint.region"))
        val regionFromSystemProp = Option(System.getProperty("aws.region"))
        val regionFromAwsEnv = Option(System.getenv("AWS_DEFAULT_REGION")).orElse(Option(System.getenv("AWS_REGION")))
        
        val finalRegion = regionFromOptions
          .orElse(regionFromHadoopTantivy)
          .orElse(regionFromHadoopS3a)
          .orElse(regionFromSystemProp)
          .orElse(regionFromAwsEnv)
        
        logger.info(s"🔍 REGION RESOLUTION PRIORITY:")
        logger.info(s"  - From options: $regionFromOptions")
        logger.info(s"  - From Hadoop Tantivy config: $regionFromHadoopTantivy")
        logger.info(s"  - From Hadoop S3a config: $regionFromHadoopS3a")
        logger.info(s"  - From system properties: $regionFromSystemProp")
        logger.info(s"  - From environment variables: $regionFromAwsEnv")
        logger.info(s"  - Final region: $finalRegion")
        
        
        if (finalRegion.isEmpty) {
          logger.warn("No AWS region configured! S3CloudStorageProvider will use AWS SDK default region resolution")
          logger.warn("This may cause S3 307 redirect errors if the bucket is in a different region")
        }
        
        finalRegion
      },
        
      // Support multiple ways to specify S3 service endpoint override (for S3Mock, MinIO, etc.)
      awsEndpoint = Option(options.get("spark.tantivy4spark.s3.endpoint"))
        .orElse(Option(options.get("spark.tantivy4spark.s3.serviceUrl")))
        .orElse(Option(hadoopConf.get("spark.tantivy4spark.s3.endpoint")))
        .orElse(Option(hadoopConf.get("spark.hadoop.fs.s3a.endpoint")))
        .orElse(Option(hadoopConf.get("fs.s3a.endpoint"))),
        
      awsPathStyleAccess = {
        val pathStyleFromOptions1 = Try(options.getBoolean("spark.tantivy4spark.aws.pathStyleAccess", false)).getOrElse(false)
        val pathStyleFromOptions2 = Try(options.getBoolean("spark.tantivy4spark.s3.pathStyleAccess", false)).getOrElse(false)
        // Also try to get directly from active Spark session
        val pathStyleFromSparkSession = try {
          import org.apache.spark.sql.SparkSession
          SparkSession.getActiveSession match {
            case Some(session) => 
              try {
                val value = session.conf.get("spark.tantivy4spark.s3.pathStyleAccess", "false")
                logger.debug(s"Found pathStyleAccess directly from SparkSession: $value")
                value.toBoolean
              } catch {
                case ex: Exception =>
                  logger.debug(s"Failed to get pathStyleAccess from SparkSession: ${ex.getMessage}")
                  false
              }
            case None => 
              logger.debug("No active SparkSession found")
              false
          }
        } catch {
          case ex: Exception =>
            logger.debug(s"Exception getting SparkSession: ${ex.getMessage}")
            false
        }
        
        val pathStyleFromHadoop1 = hadoopConf.getBoolean("spark.tantivy4spark.s3.pathStyleAccess", false)
        val pathStyleFromHadoop2 = hadoopConf.getBoolean("spark.hadoop.fs.s3a.path.style.access", false)
        val pathStyleFromHadoop3 = hadoopConf.getBoolean("fs.s3a.path.style.access", false)
        
        if (logger.isDebugEnabled) {
          logger.debug("PATH STYLE ACCESS EXTRACTION:")
          logger.debug(s"  Options: aws=$pathStyleFromOptions1, s3=$pathStyleFromOptions2")
          logger.debug(s"  Spark: $pathStyleFromSparkSession, Hadoop: $pathStyleFromHadoop1/$pathStyleFromHadoop2/$pathStyleFromHadoop3")
        }
        
        // Check if we have a localhost endpoint - if so, force path-style access for S3Mock
        val endpointValue = Option(options.get("spark.tantivy4spark.s3.endpoint"))
          .orElse(Option(hadoopConf.get("spark.tantivy4spark.s3.endpoint")))
        
        val isLocalHostEndpoint = endpointValue.exists(_.contains("localhost"))
        
        val finalPathStyle = pathStyleFromOptions1 || pathStyleFromOptions2 || pathStyleFromSparkSession || pathStyleFromHadoop1 || pathStyleFromHadoop2 || pathStyleFromHadoop3 || isLocalHostEndpoint
        
        if (logger.isDebugEnabled) {
          logger.debug(s"  Localhost endpoint detected: $isLocalHostEndpoint, Final pathStyleAccess: $finalPathStyle")
        }
        
        finalPathStyle
      },
      
      // Azure configuration
      azureAccountName = Option(options.get("spark.tantivy4spark.azure.accountName")),
      azureAccountKey = Option(options.get("spark.tantivy4spark.azure.accountKey")),
      azureConnectionString = Option(options.get("spark.tantivy4spark.azure.connectionString")),
      azureEndpoint = Option(options.get("spark.tantivy4spark.azure.endpoint")),
      azureContainerName = Option(options.get("spark.tantivy4spark.azure.containerName")),
      
      // GCP configuration
      gcpProjectId = Option(options.get("spark.tantivy4spark.gcp.projectId")),
      gcpServiceAccountKey = Option(options.get("spark.tantivy4spark.gcp.serviceAccountKey")),
      gcpCredentialsFile = Option(options.get("spark.tantivy4spark.gcp.credentialsFile")),
      gcpEndpoint = Option(options.get("spark.tantivy4spark.gcp.endpoint")),
      gcpBucketName = Option(options.get("spark.tantivy4spark.gcp.bucketName")),
      
      // Performance configuration
      maxConnections = options.getInt("spark.tantivy4spark.cloud.maxConnections", 50),
      connectionTimeout = options.getInt("spark.tantivy4spark.cloud.connectionTimeout", 10000),
      readTimeout = options.getInt("spark.tantivy4spark.cloud.readTimeout", 30000),
      maxRetries = if (options.containsKey("spark.tantivy4spark.cloud.maxRetries"))
        Some(options.getInt("spark.tantivy4spark.cloud.maxRetries", 3)) else None,
      bufferSize = options.getInt("spark.tantivy4spark.cloud.bufferSize", 16 * 1024 * 1024),

      // Multipart upload configuration
      multipartUploadThreshold = if (options.containsKey("spark.tantivy4spark.s3.multipartThreshold"))
        Some(options.getLong("spark.tantivy4spark.s3.multipartThreshold", 100L * 1024 * 1024)) else None,
      maxConcurrency = if (options.containsKey("spark.tantivy4spark.s3.maxConcurrency"))
        Some(options.getInt("spark.tantivy4spark.s3.maxConcurrency", 4)) else None
    )
  }
  
  /**
   * Static method to normalize a path for tantivy4java compatibility without creating a provider instance.
   * This applies protocol normalization (s3a:// -> s3://) and S3Mock path flattening if needed.
   */
  def normalizePathForTantivy(path: String, options: CaseInsensitiveStringMap, hadoopConf: Configuration): String = {
    // First normalize protocol (s3a:// -> s3://)
    val protocolNormalized = if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
      path.replaceFirst("^s3[an]://", "s3://")
    } else {
      path
    }
    
    // Check if we're in S3Mock mode by looking at the endpoint
    val endpointValue = Option(options.get("spark.tantivy4spark.s3.endpoint"))
      .orElse(Option(options.get("spark.tantivy4spark.aws.endpoint")))
      .orElse(Option(hadoopConf.get("spark.tantivy4spark.s3.endpoint")))
      .orElse(Option(hadoopConf.get("spark.tantivy4spark.aws.endpoint")))
      .orElse(Option(hadoopConf.get("fs.s3a.endpoint")))
    
    val isS3Mock = endpointValue.exists(endpoint => endpoint.contains("localhost") || endpoint.contains("127.0.0.1"))
    
    // Apply S3Mock path flattening if needed
    if (isS3Mock && protocolNormalized.startsWith("s3://")) {
      val uri = java.net.URI.create(protocolNormalized)
      val bucket = uri.getHost
      val key = uri.getPath.stripPrefix("/")
      
      // Convert nested paths to flat structure: path/to/file.txt -> path___to___file.txt
      val flattenedKey = if (key.contains("/")) {
        key.replace("/", "___")
      } else {
        key
      }
      
      s"s3://$bucket/$flattenedKey"
    } else {
      protocolNormalized
    }
  }
}