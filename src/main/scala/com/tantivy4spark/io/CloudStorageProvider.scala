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
import java.net.URI
import scala.util.{Try, Success, Failure}
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
  maxRetries: Int = 3,
  bufferSize: Int = 16 * 1024 * 1024 // 16MB default
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
                println(s"✅ Copied string Spark config to Hadoop conf: $key = $value")
                logger.info(s"✅ Copied string Spark config to Hadoop conf: $key = $value")
              } else {
                // Configuration doesn't exist - this is normal for optional configs like sessionToken
                logger.debug(s"🔧 Spark config key $key not set (optional)")
              }
            } catch {
              case ex: Exception => 
                println(s"❌ Failed to copy string Spark config key $key: ${ex.getMessage}")
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
                println(s"✅ Copied boolean Spark config to Hadoop conf: $key = $value")
                logger.info(s"✅ Copied boolean Spark config to Hadoop conf: $key = $value")
              } else {
                // Configuration doesn't exist - this is normal for optional configs
                logger.debug(s"🔧 Spark config key $key not set (optional)")
              }
            } catch {
              case ex: Exception => 
                println(s"❌ Failed to copy boolean Spark config key $key: ${ex.getMessage}")
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
    logger.info(s"Extracting cloud config from options: ${options.entrySet().asScala.map(e => s"${e.getKey}=${e.getValue}").mkString(", ")}")
    logger.info(s"Hadoop conf spark.tantivy4spark.aws.accessKey: ${hadoopConf.get("spark.tantivy4spark.aws.accessKey")}")
    logger.info(s"Hadoop conf spark.hadoop.fs.s3a.access.key: ${hadoopConf.get("spark.hadoop.fs.s3a.access.key")}")
    
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
    println(s"🔍 CREDENTIAL EXTRACTION DEBUG:")
    println(s"  Final accessKey: ${finalAccessKey.map(_.take(4) + "...")}")
    println(s"  Final secretKey: ${finalSecretKey.map(_ => "***")}")
    println(s"  Final sessionToken: ${finalSessionToken.map(_ => "***")}")
    
    // If credentials are still missing and we're in an executor context, log a warning
    if (finalAccessKey.isEmpty || finalSecretKey.isEmpty) {
      logger.warn("AWS credentials not found in configuration. S3CloudStorageProvider will fall back to DefaultCredentialsProvider.")
      logger.warn("This usually happens in Spark executor context where SparkSession is not available.")
      println(s"⚠️  AWS credentials missing - falling back to DefaultCredentialsProvider")
    }
    
    CloudStorageConfig(
      // AWS configuration - prioritize DataFrame options, then Spark conf, then Hadoop config
      awsAccessKey = finalAccessKey,
      awsSecretKey = finalSecretKey,
      awsSessionToken = finalSessionToken,
        
      awsRegion = Option(options.get("spark.tantivy4spark.aws.region"))
        .orElse(Option(hadoopConf.get("spark.tantivy4spark.aws.region")))
        .orElse(Option(hadoopConf.get("fs.s3a.endpoint.region")))
        .orElse(Option(System.getProperty("aws.region")))
        .orElse(Some("us-east-1")),
        
      // Support multiple ways to specify S3 service endpoint override (for S3Mock, MinIO, etc.)
      awsEndpoint = Option(options.get("spark.tantivy4spark.aws.endpoint"))
        .orElse(Option(options.get("spark.tantivy4spark.s3.endpoint")))
        .orElse(Option(options.get("spark.tantivy4spark.s3.serviceUrl")))
        .orElse(Option(hadoopConf.get("spark.tantivy4spark.aws.endpoint")))
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
                println(s"🔧 Found pathStyleAccess directly from SparkSession: $value")
                value.toBoolean
              } catch {
                case ex: Exception =>
                  println(s"🔧 Failed to get pathStyleAccess from SparkSession: ${ex.getMessage}")
                  false
              }
            case None => 
              println(s"🔧 No active SparkSession found")
              false
          }
        } catch {
          case ex: Exception =>
            println(s"🔧 Exception getting SparkSession: ${ex.getMessage}")
            false
        }
        
        val pathStyleFromHadoop1 = hadoopConf.getBoolean("spark.tantivy4spark.s3.pathStyleAccess", false)
        val pathStyleFromHadoop2 = hadoopConf.getBoolean("spark.hadoop.fs.s3a.path.style.access", false)
        val pathStyleFromHadoop3 = hadoopConf.getBoolean("fs.s3a.path.style.access", false)
        
        println(s"🔄 PATH STYLE ACCESS EXTRACTION:")
        println(s"  - From options aws.pathStyleAccess: $pathStyleFromOptions1")
        println(s"  - From options s3.pathStyleAccess: $pathStyleFromOptions2")
        println(s"  - From spark session directly: $pathStyleFromSparkSession")
        println(s"  - From hadoop tantivy s3.pathStyleAccess: $pathStyleFromHadoop1")
        println(s"  - From hadoop s3a path.style.access: $pathStyleFromHadoop2")
        println(s"  - From s3a path.style.access: $pathStyleFromHadoop3")
        
        // Check if we have a localhost endpoint - if so, force path-style access for S3Mock
        val endpointValue = Option(options.get("spark.tantivy4spark.aws.endpoint"))
          .orElse(Option(options.get("spark.tantivy4spark.s3.endpoint")))
          .orElse(Option(hadoopConf.get("spark.tantivy4spark.s3.endpoint")))
        
        val isLocalHostEndpoint = endpointValue.exists(_.contains("localhost"))
        
        val finalPathStyle = pathStyleFromOptions1 || pathStyleFromOptions2 || pathStyleFromSparkSession || pathStyleFromHadoop1 || pathStyleFromHadoop2 || pathStyleFromHadoop3 || isLocalHostEndpoint
        
        println(s"  - Localhost endpoint detected: $isLocalHostEndpoint")  
        println(s"  - Final pathStyleAccess: $finalPathStyle")
        
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
      maxRetries = options.getInt("spark.tantivy4spark.cloud.maxRetries", 3),
      bufferSize = options.getInt("spark.tantivy4spark.cloud.bufferSize", 16 * 1024 * 1024)
    )
  }
}