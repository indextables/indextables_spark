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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CompletableFuture, Executors}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.core.http.rest.PagedIterable
import com.azure.identity.{ClientSecretCredentialBuilder, DefaultAzureCredentialBuilder}
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.{BlobItem, BlobItemProperties, BlobRequestConditions, BlobStorageException}
import com.azure.storage.blob.specialized.BlockBlobClient
import com.azure.storage.common.StorageSharedKeyCredential
import org.slf4j.LoggerFactory

/**
 * High-performance Azure Blob Storage provider using Azure SDK directly. Bypasses Hadoop filesystem for better
 * performance and reliability.
 *
 * Supports multiple authentication methods:
 *   - Account key authentication (accountName + accountKey)
 *   - Connection string authentication
 *   - OAuth bearer token (Azure AD / Managed Identity) - future support
 *   - DefaultAzureCredential chain - future support
 */
class AzureCloudStorageProvider(
  config: CloudStorageConfig,
  hadoopConf: Configuration = null,
  tablePath: String = "azure://dummy")
    extends CloudStorageProvider {

  private val logger   = LoggerFactory.getLogger(classOf[AzureCloudStorageProvider])
  private val executor = Executors.newCachedThreadPool()

  logger.debug(s"AzureCloudStorageProvider CONFIG:")
  logger.debug(s"  - accountName: ${config.azureAccountName}")
  logger.debug(s"  - accountKey: ${config.azureAccountKey.map(_ => "***")}")
  logger.debug(s"  - connectionString: ${config.azureConnectionString.map(_ => "***")}")
  logger.debug(s"  - endpoint: ${config.azureEndpoint}")

  // Detect if we're running against Azurite for compatibility adjustments
  private val isAzurite = config.azureEndpoint.exists(e => e.contains("localhost") || e.contains("127.0.0.1"))

  /**
   * Parse Azure URI to extract container and blob path Supports multiple URL schemes:
   *   - azure://container/path (tantivy4java native)
   *   - wasb://container@account.blob.core.windows.net/path (Spark legacy)
   *   - wasbs://container@account.blob.core.windows.net/path (Spark legacy secure)
   *   - abfs://container@account.dfs.core.windows.net/path (Spark modern)
   *   - abfss://container@account.dfs.core.windows.net/path (Spark modern secure)
   *
   * Returns (containerName, blobPath)
   */
  private def parseAzureUri(path: String): (String, String) = {
    val uri    = new URI(path)
    val scheme = uri.getScheme

    // Validate supported Azure schemes
    require(
      scheme == "azure" || scheme == "wasb" || scheme == "wasbs" || scheme == "abfs" || scheme == "abfss",
      s"Invalid Azure URI scheme: $scheme (expected: azure, wasb, wasbs, abfs, or abfss)"
    )

    val (container, blobPath) = scheme match {
      case "azure" =>
        // azure://container/path
        val container = uri.getHost
        require(container != null && container.nonEmpty, s"Invalid Azure URI - missing container: $path")
        val blobPath = uri.getPath.stripPrefix("/")
        (container, blobPath)

      case "wasb" | "wasbs" | "abfs" | "abfss" =>
        // wasb://container@account.blob.core.windows.net/path
        // abfs://container@account.dfs.core.windows.net/path
        val authority = uri.getAuthority
        require(
          authority != null && authority.contains("@"),
          s"Invalid $scheme URI - expected format: $scheme://container@account.blob.core.windows.net/path"
        )

        val container = authority.split("@")(0)
        require(container.nonEmpty, s"Invalid $scheme URI - missing container: $path")

        val blobPath = uri.getPath.stripPrefix("/")
        (container, blobPath)
    }

    (container, blobPath)
  }

  /**
   * Credentials loaded from ~/.azure/credentials file Supports both account key and Service Principal (OAuth)
   * credentials
   */
  case class AzureCredentialsFromFile(
    accountName: Option[String],
    accountKey: Option[String],
    tenantId: Option[String],
    clientId: Option[String],
    clientSecret: Option[String])

  /**
   * Load Azure credentials from ~/.azure/credentials file (matches tantivy4java pattern) Supports both account key
   * authentication and Service Principal (OAuth) authentication
   *
   * File format: [default] storage_account = mystorageaccount account_key = your-account-key-here # For account key
   * auth tenant_id = your-tenant-id # For Service Principal auth client_id = your-client-id # For Service Principal
   * auth client_secret = your-client-secret # For Service Principal auth
   */
  private def loadAzureCredentialsFromFile(): AzureCredentialsFromFile =
    try {
      val credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials")
      if (!Files.exists(credentialsPath)) {
        logger.debug("Azure credentials file not found at: " + credentialsPath)
        return AzureCredentialsFromFile(None, None, None, None, None)
      }

      val lines        = Files.readAllLines(credentialsPath).asScala
      var inDefault    = false
      var accountName  = Option.empty[String]
      var accountKey   = Option.empty[String]
      var tenantId     = Option.empty[String]
      var clientId     = Option.empty[String]
      var clientSecret = Option.empty[String]

      for (line <- lines) {
        val trimmed = line.trim
        if (trimmed == "[default]") {
          inDefault = true
        } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          inDefault = false
        } else if (inDefault && trimmed.contains("=")) {
          val parts = trimmed.split("=", 2)
          if (parts.length == 2) {
            val key   = parts(0).trim
            val value = parts(1).trim
            key match {
              case "storage_account" => accountName = Some(value)
              case "account_key"     => accountKey = Some(value)
              case "tenant_id"       => tenantId = Some(value)
              case "client_id"       => clientId = Some(value)
              case "client_secret"   => clientSecret = Some(value)
              case _                 => // Ignore unknown keys
            }
          }
        }
      }

      val creds = AzureCredentialsFromFile(accountName, accountKey, tenantId, clientId, clientSecret)

      // Log what we found (without exposing secrets)
      if (accountName.isDefined) {
        if (accountKey.isDefined) {
          logger.info("✅ Loaded Azure account key credentials from ~/.azure/credentials")
        } else if (tenantId.isDefined && clientId.isDefined && clientSecret.isDefined) {
          logger.info("✅ Loaded Azure Service Principal credentials from ~/.azure/credentials")
        } else {
          logger.debug("Azure credentials file found but incomplete")
        }
      }

      creds
    } catch {
      case ex: Exception =>
        logger.debug(s"Failed to read Azure credentials from file: ${ex.getMessage}")
        AzureCredentialsFromFile(None, None, None, None, None)
    }

  /** Acquire OAuth bearer token using Service Principal credentials */
  private def acquireBearerToken(
    tenantId: String,
    clientId: String,
    clientSecret: String
  ): Option[String] =
    try {
      logger.info("Acquiring OAuth bearer token using Service Principal credentials...")
      val credential = new ClientSecretCredentialBuilder()
        .clientId(clientId)
        .clientSecret(clientSecret)
        .tenantId(tenantId)
        .build()

      val context = new TokenRequestContext()
      context.addScopes("https://storage.azure.com/.default")

      val token = credential.getToken(context).block()
      if (token != null) {
        logger.info(s"✅ OAuth bearer token acquired successfully (expires: ${token.getExpiresAt})")
        Some(token.getToken)
      } else {
        logger.error("Failed to acquire OAuth bearer token - token is null")
        None
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to acquire OAuth bearer token: ${ex.getMessage}", ex)
        None
    }

  // Create Azure blob service client with credential resolution
  private val blobServiceClient: BlobServiceClient = {
    val builder = new BlobServiceClientBuilder()

    // Load credentials from file
    val fileCredsOpt = loadAzureCredentialsFromFile()

    // Configure endpoint (for Azurite emulator or custom endpoints)
    val effectiveAccountName = config.azureAccountName
      .orElse(fileCredsOpt.accountName)
      .getOrElse {
        throw new IllegalArgumentException("Azure storage account name is required for azure:// URLs")
      }

    val endpoint = config.azureEndpoint.getOrElse {
      s"https://$effectiveAccountName.blob.core.windows.net"
    }

    logger.info(s"Configuring Azure client with endpoint: $endpoint")
    builder.endpoint(endpoint)

    // Configure credentials with priority:
    // 1. Connection string (if provided)
    // 2. Bearer token (OAuth - explicit or acquired from Service Principal)
    // 3. Account key authentication
    // 4. ~/.azure/credentials file
    config.azureConnectionString match {
      case Some(connectionString) =>
        logger.info("Using Azure connection string authentication")
        builder.connectionString(connectionString)

      case None =>
        // Priority 2: Bearer token (OAuth)
        val bearerTokenOpt = config.azureBearerToken.orElse {
          // Try to acquire token using Service Principal credentials
          (
            config.azureTenantId.orElse(fileCredsOpt.tenantId),
            config.azureClientId.orElse(fileCredsOpt.clientId),
            config.azureClientSecret.orElse(fileCredsOpt.clientSecret)
          ) match {
            case (Some(tenantId), Some(clientId), Some(clientSecret)) =>
              acquireBearerToken(tenantId, clientId, clientSecret)
            case _ => None
          }
        }

        bearerTokenOpt match {
          case Some(bearerToken) =>
            logger.info("Using Azure OAuth bearer token authentication")
            // Create a TokenCredential that returns the bearer token
            val tokenCredential = new TokenCredential {
              override def getToken(request: TokenRequestContext): reactor.core.publisher.Mono[AccessToken] = {
                // Return the bearer token with a far-future expiration (tokens should be refreshed externally)
                val expiresAt = java.time.OffsetDateTime.now().plusHours(1)
                reactor.core.publisher.Mono.just(new AccessToken(bearerToken, expiresAt))
              }
            }
            builder.credential(tokenCredential)

          case None =>
            // Priority 3: Account key
            val accountKeyOpt = config.azureAccountKey.orElse(fileCredsOpt.accountKey)

            accountKeyOpt match {
              case Some(accountKey) =>
                logger.info(s"Using Azure account key authentication with account: $effectiveAccountName")
                val credential = new StorageSharedKeyCredential(effectiveAccountName, accountKey)
                builder.credential(credential)

              case None =>
                logger.warn("No Azure credentials configured. Operations will likely fail.")
                logger.warn("Configure one of: accountKey, bearerToken, Service Principal credentials, connectionString, or ~/.azure/credentials file")
            }
        }
    }

    builder.buildClient()
  }

  /** Get or create a container client for the given container name */
  private def getContainerClient(containerName: String): BlobContainerClient = {
    val containerClient = blobServiceClient.getBlobContainerClient(containerName)

    // Create container if it doesn't exist (for Azurite and development)
    if (!containerClient.exists()) {
      logger.info(s"Creating Azure container: $containerName")
      try
        containerClient.create()
      catch {
        case ex: BlobStorageException if ex.getStatusCode == 409 =>
          // Container already exists (race condition)
          logger.debug(s"Container $containerName already exists")
        case ex: Exception =>
          logger.error(s"Failed to create container $containerName", ex)
          throw ex
      }
    }

    containerClient
  }

  /** Get blob client for a given path */
  private def getBlobClient(path: String): BlobClient = {
    val (container, blobPath) = parseAzureUri(path)
    val containerClient       = getContainerClient(container)
    containerClient.getBlobClient(blobPath)
  }

  override def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo] = {
    val (container, prefix) = parseAzureUri(path)
    val containerClient     = getContainerClient(container)

    val results = ArrayBuffer[CloudFileInfo]()

    try {
      // Always use prefix filter to only list blobs under the specified path
      import com.azure.storage.blob.models.ListBlobsOptions
      val options = new ListBlobsOptions().setPrefix(prefix)
      val blobs: PagedIterable[BlobItem] = containerClient.listBlobs(options, null)

      blobs.forEach { blobItem =>
        val properties = blobItem.getProperties
        val fullPath   = s"azure://$container/${blobItem.getName}"

        results += CloudFileInfo(
          path = fullPath,
          size = properties.getContentLength,
          modificationTime = properties.getLastModified.toInstant.toEpochMilli,
          isDirectory = false // Azure uses virtual directories
        )
      }

      results.toSeq
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to list files in $path", ex)
        Seq.empty
    }
  }

  override def exists(path: String): Boolean =
    try {
      val blobClient = getBlobClient(path)
      blobClient.exists()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to check existence of $path", ex)
        false
    }

  override def getFileInfo(path: String): Option[CloudFileInfo] =
    try {
      val blobClient = getBlobClient(path)
      if (blobClient.exists()) {
        val properties = blobClient.getProperties
        Some(
          CloudFileInfo(
            path = path,
            size = properties.getBlobSize,
            modificationTime = properties.getLastModified.toInstant.toEpochMilli,
            isDirectory = false
          )
        )
      } else {
        None
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to get file info for $path", ex)
        None
    }

  override def readFile(path: String): Array[Byte] = {
    val blobClient   = getBlobClient(path)
    val outputStream = new ByteArrayOutputStream()
    blobClient.download(outputStream)
    outputStream.toByteArray
  }

  override def readRange(
    path: String,
    offset: Long,
    length: Long
  ): Array[Byte] = {
    val blobClient   = getBlobClient(path)
    val outputStream = new ByteArrayOutputStream()

    // Azure SDK uses BlobRange for range reads
    import com.azure.storage.blob.models.BlobRange
    val range = new BlobRange(offset, length)
    blobClient.downloadWithResponse(outputStream, range, null, null, false, null, null)

    outputStream.toByteArray
  }

  override def openInputStream(path: String): InputStream = {
    val blobClient = getBlobClient(path)
    blobClient.openInputStream()
  }

  override def createOutputStream(path: String): OutputStream =
    // Azure doesn't support direct output stream - use a buffered approach
    new ByteArrayOutputStream() {
      override def close(): Unit = {
        super.close()
        val content = this.toByteArray
        writeFile(path, content)
      }
    }

  override def writeFile(path: String, content: Array[Byte]): Unit = {
    val blobClient  = getBlobClient(path)
    val inputStream = new ByteArrayInputStream(content)
    blobClient.upload(inputStream, content.length, true) // overwrite = true
  }

  override def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean =
    try {
      val blobClient = getBlobClient(path)

      // Check if blob already exists
      if (blobClient.exists()) {
        logger.debug(s"Blob already exists, skipping write: $path")
        false
      } else {
        // Upload with "If-None-Match: *" condition for atomic create-if-not-exists
        val inputStream = new ByteArrayInputStream(content)
        val conditions  = new BlobRequestConditions().setIfNoneMatch("*")

        try {
          blobClient.uploadWithResponse(inputStream, content.length, null, null, null, null, conditions, null, null)
          logger.debug(s"Successfully wrote blob (conditional): $path")
          true
        } catch {
          case ex: BlobStorageException if ex.getStatusCode == 409 =>
            // Blob was created by another process (race condition)
            logger.debug(s"Blob created by another process: $path")
            false
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to write file conditionally: $path", ex)
        throw new RuntimeException(s"Failed to write file conditionally: $path", ex)
    }

  override def writeFileFromStream(
    path: String,
    inputStream: InputStream,
    contentLength: Option[Long] = None
  ): Unit = {
    val blobClient = getBlobClient(path)

    contentLength match {
      case Some(length) =>
        // Known content length - direct upload
        blobClient.upload(inputStream, length, true) // overwrite = true

      case None =>
        // Unknown content length - buffer in memory (could be improved with block blob staged upload)
        val buffer = new ByteArrayOutputStream()
        val bytes  = new Array[Byte](8192)
        var count  = 0
        while ({ count = inputStream.read(bytes); count != -1 })
          buffer.write(bytes, 0, count)

        val content    = buffer.toByteArray
        val byteStream = new ByteArrayInputStream(content)
        blobClient.upload(byteStream, content.length, true)
    }
  }

  override def deleteFile(path: String): Boolean =
    try {
      val blobClient = getBlobClient(path)
      blobClient.delete()
      true
    } catch {
      case ex: BlobStorageException if ex.getStatusCode == 404 =>
        logger.debug(s"Blob not found for deletion: $path")
        false
      case ex: Exception =>
        logger.error(s"Failed to delete blob: $path", ex)
        false
    }

  override def createDirectory(path: String): Boolean =
    // Azure Blob Storage doesn't have directories - they are virtual
    // Return true as a no-op
    true

  override def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]] = {
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.concurrent.duration._
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val futures = paths.map { path =>
      Future {
        path -> readFile(path)
      }
    }

    val results = Await.result(Future.sequence(futures), 5.minutes)
    results.toMap
  }

  override def existsParallel(paths: Seq[String]): Map[String, Boolean] = {
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.concurrent.duration._
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val futures = paths.map { path =>
      Future {
        path -> exists(path)
      }
    }

    val results = Await.result(Future.sequence(futures), 5.minutes)
    results.toMap
  }

  override def getProviderType: String = "Azure"

  override def normalizePathForTantivy(path: String): String =
    // Normalize all Spark Azure schemes (wasb, wasbs, abfs, abfss) to tantivy4java's azure:// scheme
    io.indextables.spark.util.ProtocolNormalizer.normalizeAzureProtocol(path)

  override def close(): Unit =
    executor.shutdown()
  // Azure BlobServiceClient doesn't need explicit close
}
