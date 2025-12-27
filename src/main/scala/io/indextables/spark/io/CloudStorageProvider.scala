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

package io.indextables.spark.io

import java.io.{Closeable, InputStream, OutputStream}

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration

import org.slf4j.LoggerFactory

/**
 * High-performance cloud storage abstraction that bypasses Hadoop filesystem for direct cloud API access. Supports
 * asynchronous, parallel operations optimized for each cloud provider.
 */
trait CloudStorageProvider extends Closeable {

  /** List files in a directory with optional prefix filtering */
  def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo]

  /** Check if a file exists */
  def exists(path: String): Boolean

  /** Get file metadata */
  def getFileInfo(path: String): Option[CloudFileInfo]

  /** Read entire file content */
  def readFile(path: String): Array[Byte]

  /** Read a range of bytes from a file */
  def readRange(
    path: String,
    offset: Long,
    length: Long
  ): Array[Byte]

  /** Open an input stream for reading */
  def openInputStream(path: String): InputStream

  /** Create an output stream for writing */
  def createOutputStream(path: String): OutputStream

  /** Write content to a file */
  def writeFile(path: String, content: Array[Byte]): Unit

  /**
   * Write content to a file ONLY if it does not already exist (conditional write). This is critical for transaction log
   * integrity - prevents accidental overwrites.
   *
   * @param path
   *   The file path
   * @param content
   *   The content to write
   * @return
   *   true if file was written, false if file already exists
   * @throws RuntimeException
   *   if write fails for reasons other than file already existing
   */
  def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean

  /** Write content to a file from an input stream (memory-efficient for large files) */
  def writeFileFromStream(
    path: String,
    inputStream: InputStream,
    contentLength: Option[Long] = None
  ): Unit

  /** Delete a file */
  def deleteFile(path: String): Boolean

  /** Create directory (if supported) */
  def createDirectory(path: String): Boolean

  /** Read multiple files in parallel for better performance */
  def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]]

  /** Check existence of multiple files in parallel */
  def existsParallel(paths: Seq[String]): Map[String, Boolean]

  /** Get provider type for logging/metrics */
  def getProviderType: String

  /**
   * Normalize path for tantivy4java compatibility. For S3 providers, converts s3a:// and s3n:// protocols to s3://.
   * Other providers can return the path unchanged.
   */
  def normalizePathForTantivy(path: String): String = path // Default implementation returns unchanged
}

/** File metadata information */
case class CloudFileInfo(
  path: String,
  size: Long,
  modificationTime: Long,
  isDirectory: Boolean)

/** Cloud storage configuration extracted from Spark options and Hadoop config */
case class CloudStorageConfig(
  // AWS configuration
  awsAccessKey: Option[String] = None,
  awsSecretKey: Option[String] = None,
  awsSessionToken: Option[String] = None,
  awsRegion: Option[String] = None,
  awsEndpoint: Option[String] = None,
  awsPathStyleAccess: Boolean = false,
  awsCredentialsProviderClass: Option[String] = None,

  // Azure configuration
  azureAccountName: Option[String] = None,
  azureAccountKey: Option[String] = None,
  azureConnectionString: Option[String] = None,
  azureBearerToken: Option[String] = None,
  azureTenantId: Option[String] = None,
  azureClientId: Option[String] = None,
  azureClientSecret: Option[String] = None,
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
  multipartUploadThreshold: Option[Long] = None, // Defaults to 200MB if not specified
  partSize: Option[Long] = None,                 // Defaults to 128MB if not specified
  maxConcurrency: Option[Int] = None,            // Defaults to 4 if not specified
  maxQueueSize: Option[Int] = None               // Defaults to 3 if not specified (3 * 128MB = 384MB max buffered)
)

/** Factory for creating cloud storage providers */
object CloudStorageProviderFactory {

  private val logger = LoggerFactory.getLogger(CloudStorageProviderFactory.getClass)

  /** Create appropriate cloud storage provider based on path protocol */
  def createProvider(
    path: String,
    options: CaseInsensitiveStringMap,
    hadoopConf: Configuration
  ): CloudStorageProvider = {
    val protocol = ProtocolBasedIOFactory.determineProtocol(path)

    logger.info(s"CloudStorageProviderFactory.createProvider called for path: $path")
    logger.info(s"Options passed (${options.size()} total options)")
    import scala.jdk.CollectionConverters._
    options.entrySet().asScala.foreach { entry =>
      if (entry.getKey.startsWith("spark.indextables.")) {
        val displayValue =
          if (entry.getKey.contains("secret") || entry.getKey.contains("session")) "***" else entry.getValue
        logger.info(s"   Option: ${entry.getKey} = $displayValue")
      }
    }

    // For cloud storage, extract configuration from both options and Hadoop/Spark config
    // Also try to get configuration from Spark session if available
    val enrichedHadoopConf = enrichHadoopConfWithSparkConf(hadoopConf)
    val config             = extractCloudConfig(options, enrichedHadoopConf, protocol)

    logger.info(s"Creating ${ProtocolBasedIOFactory.protocolName(protocol)} storage provider for path: $path")

    protocol match {
      case ProtocolBasedIOFactory.S3Protocol =>
        logger.info(s"S3 config - endpoint: ${config.awsEndpoint}, region: ${config.awsRegion}, pathStyle: ${config.awsPathStyleAccess}")
        logger.info(s"S3 credentials - accessKey: ${config.awsAccessKey
            .map(_.take(4) + "...")
            .getOrElse("None")}, secretKey: ${config.awsSecretKey.map(_ => "***").getOrElse("None")}")
        logger.info(s"S3 custom provider - class: ${config.awsCredentialsProviderClass.getOrElse("None")}")
        new S3CloudStorageProvider(config, enrichedHadoopConf, path)
      case ProtocolBasedIOFactory.AzureProtocol =>
        logger.info(
          s"Azure config - endpoint: ${config.azureEndpoint}, accountName: ${config.azureAccountName.getOrElse("None")}"
        )
        logger.info(s"Azure credentials - accountKey: ${config.azureAccountKey
            .map(_ => "***")
            .getOrElse("None")}, connectionString: ${config.azureConnectionString.map(_ => "***").getOrElse("None")}")
        new AzureCloudStorageProvider(config, enrichedHadoopConf, path)
      case ProtocolBasedIOFactory.HDFSProtocol | ProtocolBasedIOFactory.FileProtocol |
          ProtocolBasedIOFactory.LocalProtocol =>
        new HadoopCloudStorageProvider(hadoopConf)
    }
  }

  /** Enrich Hadoop configuration with Spark configuration values */
  private def enrichHadoopConfWithSparkConf(hadoopConf: Configuration): Configuration =
    try {
      // Try to get the active Spark session and copy relevant configurations
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.getActiveSession

      spark match {
        case Some(session) =>
          val enriched  = new Configuration(hadoopConf)
          val sparkConf = session.conf

          // Copy IndexTables4Spark specific configurations
          // String configurations - check both prefixes
          val stringConfigs = Seq(
            "spark.indextables.aws.accessKey",
            "spark.indextables.aws.secretKey",
            "spark.indextables.aws.sessionToken",
            "spark.indextables.aws.region",
            "spark.indextables.s3.endpoint",
            "spark.indextables.aws.credentialsProviderClass",
            "spark.indextables.aws.accessKey",
            "spark.indextables.aws.secretKey",
            "spark.indextables.aws.sessionToken",
            "spark.indextables.aws.region",
            "spark.indextables.s3.endpoint",
            "spark.indextables.aws.credentialsProviderClass"
          )

          // Boolean configurations
          val booleanConfigs = Seq(
            "spark.indextables.s3.pathStyleAccess",
            "spark.indextables.s3.pathStyleAccess"
          )

          // Copy string configurations - only if they exist
          stringConfigs.foreach { key =>
            try {
              // Try to get the config with a unique default value to detect if it exists
              val defaultValue = s"__NOT_SET__${key}__"
              val value        = sparkConf.get(key, defaultValue)
              if (value != defaultValue) {
                enriched.set(key, value)
                val maskedValue = io.indextables.spark.util.CredentialRedaction.redactValue(key, value)
                logger.debug(s"Copied string Spark config to Hadoop conf: $key = $maskedValue")
                logger.info(s"✅ Copied string Spark config to Hadoop conf: $key = $maskedValue")
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
              val value        = sparkConf.get(key, defaultValue)
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

  /** Extract cloud storage configuration from Spark options and Hadoop config */
  def extractCloudConfig(
    options: CaseInsensitiveStringMap,
    hadoopConf: Configuration,
    protocol: ProtocolBasedIOFactory.StorageProtocol
  ): CloudStorageConfig = {
    // Debug logging for configuration extraction
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Extracting cloud config from options:")
    options.entrySet().asScala.foreach { entry =>
      val key = entry.getKey.toLowerCase
      val isSensitive = key.contains("secret") || key.contains("sessiontoken") || key.contains("password") || key
        .contains("key") && key.contains("account")
      val displayValue = if (isSensitive) "***[REDACTED]***" else entry.getValue
      logger.info(s"  ${entry.getKey} = $displayValue")
    }
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.indextables.aws.accessKey: ${Option(
        hadoopConf.get("spark.indextables.aws.accessKey")
      ).map(_.take(4) + "...").getOrElse("None")}")
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.indextables.aws.region: ${hadoopConf.get("spark.indextables.aws.region")}")
    logger.info(s"⚙️ EXTRACT CLOUD CONFIG DEBUG - Hadoop conf spark.hadoop.fs.s3a.access.key: ${Option(
        hadoopConf.get("spark.hadoop.fs.s3a.access.key")
      ).map(_.take(4) + "...").getOrElse("None")}")

    // Use ConfigurationResolver for AWS credential extraction
    val awsSources = Seq(
      io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.aws"),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.hadoop.fs.s3a"),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "fs.s3a")
    )

    val finalAccessKey = Option(options.get("spark.indextables.aws.accessKey"))
      .orElse(
        io.indextables.spark.util.ConfigurationResolver.resolveString(
          "accessKey",
          awsSources,
          logMask = true
        )
      )

    val finalSecretKey = Option(options.get("spark.indextables.aws.secretKey"))
      .orElse(
        io.indextables.spark.util.ConfigurationResolver.resolveString(
          "secretKey",
          awsSources,
          logMask = true
        )
      )

    // Session token uses slightly different key names
    val sessionTokenSources = Seq(
      io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.aws"),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "fs.s3a")
    )

    val finalSessionToken = Option(options.get("spark.indextables.aws.sessionToken"))
      .orElse(
        io.indextables.spark.util.ConfigurationResolver
          .resolveString(
            "sessionToken",
            sessionTokenSources,
            logMask = true
          )
      )
      .orElse(
        // Also check fs.s3a.session.token (different key name)
        Option(hadoopConf.get("fs.s3a.session.token"))
      )

    logger.info(s"Final credentials: accessKey=${finalAccessKey.map(_.take(4) + "...")}, secretKey=${finalSecretKey
        .map(_ => "***")}, sessionToken=${finalSessionToken.map(_ => "***")}")
    if (logger.isDebugEnabled) {
      logger.debug(s"CREDENTIAL EXTRACTION DEBUG:")
      logger.debug(
        s"  Final accessKey: ${finalAccessKey.map(_.take(4) + "...")}, secretKey present: ${finalSecretKey.isDefined}, sessionToken present: ${finalSessionToken.isDefined}"
      )
    }

    // If credentials are still missing and we're using S3 protocol, log a warning
    // Only warn about AWS credentials when actually using S3 - not for Azure or other providers
    if ((finalAccessKey.isEmpty || finalSecretKey.isEmpty) && protocol == ProtocolBasedIOFactory.S3Protocol) {
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
      awsCredentialsProviderClass = Option(options.get("spark.indextables.aws.credentialsProviderClass"))
        .orElse(Option(hadoopConf.get("spark.indextables.aws.credentialsProviderClass")))
        .orElse(Option(hadoopConf.get("spark.indextables.aws.credentialsProviderClass"))),
      awsRegion = {
        // Region resolution with extended sources (system properties and environment)
        val finalRegion = Option(options.get("spark.indextables.aws.region"))
          .orElse(
            io.indextables.spark.util.ConfigurationResolver
              .resolveString(
                "region",
                awsSources,
                logMask = false
              )
          )
          .orElse(
            Option(hadoopConf.get("fs.s3a.endpoint.region"))
          )
          .orElse(
            Option(System.getProperty("aws.region"))
          )
          .orElse(
            Option(System.getenv("AWS_DEFAULT_REGION"))
          )
          .orElse(
            Option(System.getenv("AWS_REGION"))
          )

        if (logger.isInfoEnabled) {
          logger.info(s"AWS Region resolved: $finalRegion")
        }

        // Only warn about AWS region when actually using S3 - not for Azure or other providers
        if (finalRegion.isEmpty && protocol == ProtocolBasedIOFactory.S3Protocol) {
          logger.warn("No AWS region configured! S3CloudStorageProvider will use AWS SDK default region resolution")
          logger.warn("This may cause S3 307 redirect errors if the bucket is in a different region")
        }

        finalRegion
      },

      // Support multiple ways to specify S3 service endpoint override (for S3Mock, MinIO, etc.)
      awsEndpoint = Option(options.get("spark.indextables.s3.endpoint"))
        .orElse(Option(options.get("spark.indextables.s3.serviceUrl")))
        .orElse(Option(hadoopConf.get("spark.indextables.s3.endpoint")))
        .orElse(Option(hadoopConf.get("spark.indextables.s3.endpoint")))
        .orElse(Option(hadoopConf.get("spark.hadoop.fs.s3a.endpoint")))
        .orElse(Option(hadoopConf.get("fs.s3a.endpoint"))),
      awsPathStyleAccess = {
        val pathStyleFromOptions1 =
          Try(options.getBoolean("spark.indextables.aws.pathStyleAccess", false)).getOrElse(false)
        val pathStyleFromOptions2 =
          Try(options.getBoolean("spark.indextables.s3.pathStyleAccess", false)).getOrElse(false)
        // Also try to get directly from active Spark session
        val pathStyleFromSparkSession =
          try {
            import org.apache.spark.sql.SparkSession
            SparkSession.getActiveSession match {
              case Some(session) =>
                try {
                  val value = session.conf.get("spark.indextables.s3.pathStyleAccess", "false")
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

        val pathStyleFromHadoop1           = hadoopConf.getBoolean("spark.indextables.s3.pathStyleAccess", false)
        val pathStyleFromHadoopIndexTables = hadoopConf.getBoolean("spark.indextables.s3.pathStyleAccess", false)
        val pathStyleFromHadoop2           = hadoopConf.getBoolean("spark.hadoop.fs.s3a.path.style.access", false)
        val pathStyleFromHadoop3           = hadoopConf.getBoolean("fs.s3a.path.style.access", false)

        if (logger.isDebugEnabled) {
          logger.debug("PATH STYLE ACCESS EXTRACTION:")
          logger.debug(s"  Options: aws=$pathStyleFromOptions1, s3=$pathStyleFromOptions2")
          logger.debug(s"  Spark: $pathStyleFromSparkSession, Hadoop: $pathStyleFromHadoop1/$pathStyleFromHadoopIndexTables/$pathStyleFromHadoop2/$pathStyleFromHadoop3")
        }

        // Check if we have a localhost endpoint - if so, force path-style access for S3Mock
        val endpointValue = Option(options.get("spark.indextables.s3.endpoint"))
          .orElse(Option(hadoopConf.get("spark.indextables.s3.endpoint")))

        val isLocalHostEndpoint = endpointValue.exists(_.contains("localhost"))

        val finalPathStyle =
          pathStyleFromOptions1 || pathStyleFromOptions2 || pathStyleFromSparkSession || pathStyleFromHadoop1 || pathStyleFromHadoopIndexTables || pathStyleFromHadoop2 || pathStyleFromHadoop3 || isLocalHostEndpoint

        if (logger.isDebugEnabled) {
          logger.debug(s"  Localhost endpoint detected: $isLocalHostEndpoint, Final pathStyleAccess: $finalPathStyle")
        }

        finalPathStyle
      },

      // Azure configuration - use ConfigurationResolver with environment variable fallbacks
      azureAccountName = {
        val azureSources = Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
          io.indextables.spark.util.EnvironmentConfigSource()
        )
        io.indextables.spark.util.ConfigurationResolver
          .resolveString(
            "accountName",
            azureSources,
            logMask = false
          )
          .orElse(Option(System.getenv("AZURE_STORAGE_ACCOUNT")))
      },
      azureAccountKey = {
        val azureSources = Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
          io.indextables.spark.util.EnvironmentConfigSource()
        )
        io.indextables.spark.util.ConfigurationResolver
          .resolveString(
            "accountKey",
            azureSources,
            logMask = true
          )
          .orElse(Option(System.getenv("AZURE_STORAGE_KEY")))
      },
      azureConnectionString = io.indextables.spark.util.ConfigurationResolver.resolveString(
        "connectionString",
        Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure")
        ),
        logMask = true
      ),
      azureBearerToken = io.indextables.spark.util.ConfigurationResolver.resolveString(
        "bearerToken",
        Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure")
        ),
        logMask = true
      ),
      azureTenantId = io.indextables.spark.util.ConfigurationResolver
        .resolveString(
          "tenantId",
          Seq(
            io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
            io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
            io.indextables.spark.util.EnvironmentConfigSource()
          ),
          logMask = false
        )
        .orElse(Option(System.getenv("AZURE_TENANT_ID"))),
      azureClientId = io.indextables.spark.util.ConfigurationResolver
        .resolveString(
          "clientId",
          Seq(
            io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
            io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
            io.indextables.spark.util.EnvironmentConfigSource()
          ),
          logMask = false
        )
        .orElse(Option(System.getenv("AZURE_CLIENT_ID"))),
      azureClientSecret = io.indextables.spark.util.ConfigurationResolver
        .resolveString(
          "clientSecret",
          Seq(
            io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
            io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
            io.indextables.spark.util.EnvironmentConfigSource()
          ),
          logMask = true
        )
        .orElse(Option(System.getenv("AZURE_CLIENT_SECRET"))),
      azureEndpoint = io.indextables.spark.util.ConfigurationResolver.resolveString(
        "endpoint",
        Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure")
        ),
        logMask = false
      ),
      azureContainerName = io.indextables.spark.util.ConfigurationResolver.resolveString(
        "containerName",
        Seq(
          io.indextables.spark.util.OptionsConfigSource(options.asScala.asJava),
          io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.azure")
        ),
        logMask = false
      ),

      // GCP configuration
      gcpProjectId = Option(options.get("spark.indextables.gcp.projectId")),
      gcpServiceAccountKey = Option(options.get("spark.indextables.gcp.serviceAccountKey")),
      gcpCredentialsFile = Option(options.get("spark.indextables.gcp.credentialsFile")),
      gcpEndpoint = Option(options.get("spark.indextables.gcp.endpoint")),
      gcpBucketName = Option(options.get("spark.indextables.gcp.bucketName")),

      // Performance configuration
      maxConnections = options.getInt("spark.indextables.cloud.maxConnections", 50),
      connectionTimeout = options.getInt("spark.indextables.cloud.connectionTimeout", 10000),
      readTimeout = options.getInt("spark.indextables.cloud.readTimeout", 30000),
      maxRetries =
        if (options.containsKey("spark.indextables.cloud.maxRetries"))
          Some(options.getInt("spark.indextables.cloud.maxRetries", 3))
        else None,
      bufferSize = options.getInt("spark.indextables.cloud.bufferSize", 16 * 1024 * 1024),

      // Multipart upload configuration
      multipartUploadThreshold =
        if (options.containsKey("spark.indextables.s3.multipartThreshold"))
          Some(options.getLong("spark.indextables.s3.multipartThreshold", 200L * 1024 * 1024))
        else None,
      partSize =
        if (options.containsKey("spark.indextables.s3.partSize"))
          Some(options.getLong("spark.indextables.s3.partSize", 128L * 1024 * 1024))
        else None,
      maxConcurrency =
        if (options.containsKey("spark.indextables.s3.maxConcurrency"))
          Some(options.getInt("spark.indextables.s3.maxConcurrency", 4))
        else None,
      maxQueueSize =
        if (options.containsKey("spark.indextables.s3.maxQueueSize"))
          Some(options.getInt("spark.indextables.s3.maxQueueSize", 3))
        else None
    )
  }

  /**
   * Static method to normalize a path for tantivy4java compatibility without creating a provider instance. This applies
   * protocol normalization:
   *   - S3: s3a:// and s3n:// -> s3://
   *   - Azure: wasb://, wasbs://, abfs://, abfss:// -> azure://
   *   - S3Mock: applies path flattening if needed
   */
  def normalizePathForTantivy(
    path: String,
    options: CaseInsensitiveStringMap,
    hadoopConf: Configuration
  ): String = {
    // First normalize protocol using centralized utility
    val protocolNormalized = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(path)

    // Check if we're in S3Mock mode by looking at the endpoint (only applies to S3 paths)
    if (protocolNormalized.startsWith("s3://")) {
      val endpointValue = Option(options.get("spark.indextables.s3.endpoint"))
        .orElse(Option(options.get("spark.indextables.aws.endpoint")))
        .orElse(Option(hadoopConf.get("spark.indextables.s3.endpoint")))
        .orElse(Option(hadoopConf.get("spark.indextables.aws.endpoint")))
        .orElse(Option(hadoopConf.get("fs.s3a.endpoint")))

      val isS3Mock = endpointValue.exists(endpoint => endpoint.contains("localhost") || endpoint.contains("127.0.0.1"))

      // Apply S3Mock path flattening if needed
      if (isS3Mock) {
        val uri    = java.net.URI.create(protocolNormalized)
        val bucket = uri.getHost
        val key    = uri.getPath.stripPrefix("/")

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
    } else {
      protocolNormalized
    }
  }
}
