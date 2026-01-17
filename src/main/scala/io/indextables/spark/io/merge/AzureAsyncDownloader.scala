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

package io.indextables.spark.io.merge

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.CompletableFuture

import scala.util.Try

import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.storage.blob.{BlobAsyncClient, BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import org.slf4j.LoggerFactory

/**
 * Azure Blob Storage async downloader.
 *
 * Uses Azure SDK's async client to download files directly to disk without buffering in JVM heap.
 *
 * @param serviceClient
 *   The Azure Blob Service async client
 * @param accountName
 *   The Azure storage account name
 */
class AzureAsyncDownloader(
  serviceClient: BlobServiceAsyncClient,
  accountName: String)
    extends AsyncDownloader {

  private val logger = LoggerFactory.getLogger(classOf[AzureAsyncDownloader])

  override def protocol: String = "azure"

  override def canHandle(path: String): Boolean =
    path.startsWith("azure://") ||
      path.startsWith("wasb://") ||
      path.startsWith("wasbs://") ||
      path.startsWith("abfs://") ||
      path.startsWith("abfss://")

  override def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult] = {
    val startTime = System.currentTimeMillis()
    val destFile  = new File(request.destinationPath)

    // Ensure parent directory exists
    val parentDir = destFile.getParentFile
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs()
    }

    // Parse Azure path to get container and blob
    val (container, blobPath) = parseAzurePath(request.sourcePath)

    logger.debug(s"Starting async download: $container/$blobPath -> ${request.destinationPath}")

    try {
      // Get blob client for this specific blob
      val blobClient = serviceClient
        .getBlobContainerAsyncClient(container)
        .getBlobAsyncClient(blobPath)

      // Download to file asynchronously
      // Azure SDK's downloadToFile returns Mono<BlobProperties>, convert to CompletableFuture
      val monoDownload = blobClient.downloadToFile(destFile.getAbsolutePath, true)

      // Convert Reactor Mono to CompletableFuture
      val future = new CompletableFuture[DownloadResult]()

      monoDownload.subscribe(
        // onNext - download complete
        { props =>
          val durationMs = System.currentTimeMillis() - startTime
          val fileSize   = destFile.length()

          logger.debug(s"Downloaded $container/$blobPath ($fileSize bytes in ${durationMs}ms)")

          future.complete(
            DownloadResult.success(
              request = request,
              localPath = destFile.getAbsolutePath,
              bytesDownloaded = fileSize,
              durationMs = durationMs
            )
          )
        },
        // onError
        { error =>
          val durationMs = System.currentTimeMillis() - startTime

          logger.warn(s"Failed to download $container/$blobPath: ${error.getMessage}")

          // Clean up partial file
          Try(Files.deleteIfExists(destFile.toPath))

          future.complete(
            DownloadResult.failure(
              request = request,
              error = error,
              durationMs = durationMs
            )
          )
        }
      )

      future
    } catch {
      case ex: Exception =>
        val durationMs = System.currentTimeMillis() - startTime
        logger.error(s"Error initiating download for $container/$blobPath", ex)
        CompletableFuture.completedFuture(
          DownloadResult.failure(request, ex, durationMs)
        )
    }
  }

  /**
   * Parse an Azure path into container and blob path.
   *
   * Handles various Azure URL schemes: azure://, wasb://, wasbs://, abfs://, abfss://
   */
  private def parseAzurePath(path: String): (String, String) = {
    // Normalize to azure:// scheme for parsing
    val normalizedPath = path
      .replaceFirst("^wasbs?://", "azure://")
      .replaceFirst("^abfss?://", "azure://")

    val uri = new URI(normalizedPath)

    // Container is the host or first path component depending on URL format
    // azure://container/path or azure://container@account/path
    val hostPart = uri.getHost
    val pathPart = uri.getPath.stripPrefix("/")

    // If host contains @, it's in format container@account
    if (hostPart.contains("@")) {
      val container = hostPart.split("@")(0)
      (container, pathPart)
    } else {
      // Host is the container
      (hostPart, pathPart)
    }
  }

  override def close(): Unit = {
    // BlobServiceAsyncClient is typically shared and should not be closed here
  }
}

object AzureAsyncDownloader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create an AzureAsyncDownloader with account key authentication.
   *
   * @param accountName
   *   Azure storage account name
   * @param accountKey
   *   Azure storage account key
   * @return
   *   A new AzureAsyncDownloader instance
   */
  def createWithAccountKey(accountName: String, accountKey: String): AzureAsyncDownloader = {
    logger.debug(s"Creating Azure async downloader for account: $accountName")

    val credential = new StorageSharedKeyCredential(accountName, accountKey)

    val serviceClient = new BlobServiceClientBuilder()
      .endpoint(s"https://$accountName.blob.core.windows.net")
      .credential(credential)
      .buildAsyncClient()

    new AzureAsyncDownloader(serviceClient, accountName)
  }

  /**
   * Create an AzureAsyncDownloader with OAuth (Service Principal) authentication.
   *
   * @param accountName
   *   Azure storage account name
   * @param tenantId
   *   Azure AD tenant ID
   * @param clientId
   *   Azure AD client ID
   * @param clientSecret
   *   Azure AD client secret
   * @return
   *   A new AzureAsyncDownloader instance
   */
  def createWithOAuth(
    accountName: String,
    tenantId: String,
    clientId: String,
    clientSecret: String
  ): AzureAsyncDownloader = {
    logger.debug(s"Creating Azure async downloader with OAuth for account: $accountName")

    val credential: ClientSecretCredential = new ClientSecretCredentialBuilder()
      .tenantId(tenantId)
      .clientId(clientId)
      .clientSecret(clientSecret)
      .build()

    val serviceClient = new BlobServiceClientBuilder()
      .endpoint(s"https://$accountName.blob.core.windows.net")
      .credential(credential)
      .buildAsyncClient()

    new AzureAsyncDownloader(serviceClient, accountName)
  }

  /**
   * Create an AzureAsyncDownloader from a serializable config map.
   *
   * This is useful when creating downloaders on executors from broadcast configuration.
   */
  def fromConfig(configs: Map[String, String]): Option[AzureAsyncDownloader] = {
    def get(key: String): Option[String] =
      configs.get(key).orElse(configs.get(key.toLowerCase)).filter(_.nonEmpty)

    val accountName  = get("spark.indextables.azure.accountName")
    val accountKey   = get("spark.indextables.azure.accountKey")
    val tenantId     = get("spark.indextables.azure.tenantId")
    val clientId     = get("spark.indextables.azure.clientId")
    val clientSecret = get("spark.indextables.azure.clientSecret")

    accountName match {
      case Some(account) =>
        // Try account key first, then OAuth
        accountKey match {
          case Some(key) =>
            Some(createWithAccountKey(account, key))
          case None =>
            (tenantId, clientId, clientSecret) match {
              case (Some(tenant), Some(client), Some(secret)) =>
                Some(createWithOAuth(account, tenant, client, secret))
              case _ =>
                logger.warn("Azure account name provided but no valid credentials found")
                None
            }
        }
      case None =>
        logger.debug("No Azure account name configured")
        None
    }
  }
}
