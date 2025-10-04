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

package io.indextables.spark.auth.unity

import java.net.URI
import java.util.concurrent.{Callable, TimeUnit}

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.conf.Configuration

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicSessionCredentials}
import com.databricks.sdk.service.catalog.{
  AwsCredentials,
  GenerateTemporaryPathCredentialRequest,
  GenerateTemporaryPathCredentialResponse,
  PathOperation
}
import com.databricks.sdk.WorkspaceClient
import com.google.common.cache.{Cache, CacheBuilder, CacheStats}
import org.slf4j.LoggerFactory

/**
 * AWS Credential Provider that integrates with Databricks Unity Catalog.
 *
 * This provider fetches temporary AWS credentials from Unity Catalog for accessing cloud storage paths. It implements
 * intelligent caching with a 20-minute TTL and automatic fallback from READ_WRITE to READ permissions.
 *
 * Usage: spark.conf.set("spark.indextables.aws.credentialsProviderClass",
 * "io.indextables.spark.auth.unity.UnityCatalogCredentialProvider")
 */
class UnityCatalogCredentialProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider {
  import UnityCatalogCredentialProvider._

  private val fallbackEnabled = conf.getBoolean(FallbackEnabledKey, DefaultFallbackEnabled)
  private val retryAttempts   = conf.getInt(RetryAttemptsKey, DefaultRetryAttempts)

  logger.info(s"Initializing UnityCatalogCredentialProvider for URI: $uri")

  // Initialize process-global resources only once
  initializeGlobalResources(conf)

  logger.info("UnityCatalogCredentialProvider initialized successfully")

  /**
   * Get AWS credentials for the configured URI.
   *
   * This method: 1. Checks the cache for valid credentials 2. If not cached, requests READ_WRITE credentials 3. Falls
   * back to READ credentials if READ_WRITE fails 4. Caches the credentials with a 20-minute TTL
   */
  override def getCredentials(): AWSCredentials = {
    val path     = extractPath(uri)
    val cacheKey = buildCacheKey(path)

    logger.debug(s"Getting credentials for path: $path")

    try {
      val cached = globalCredentialsCache.get(
        cacheKey,
        new Callable[CachedCredentials] {
          override def call(): CachedCredentials =
            fetchCredentialsWithFallback(path)
        }
      )

      logCacheStats()

      cached.sessionToken match {
        case Some(token) =>
          new BasicSessionCredentials(cached.accessKeyId, cached.secretAccessKey, token)
        case None =>
          new BasicAWSCredentials(cached.accessKeyId, cached.secretAccessKey)
      }

    } catch {
      case e: Exception =>
        logger.error(s"Failed to obtain Unity Catalog credentials for path: $path", e)
        throw new RuntimeException("Failed to obtain Unity Catalog credentials", e.getCause)
    }
  }

  /** Refresh credentials (forced refresh, bypassing cache). */
  override def refresh(): Unit = {
    val path     = extractPath(uri)
    val cacheKey = buildCacheKey(path)

    logger.info(s"Refreshing credentials for path: $path")

    // Invalidate cached entry
    globalCredentialsCache.invalidate(cacheKey)

    // Fetch new credentials and update cache
    Try {
      val freshCredentials = fetchCredentialsWithFallback(path)
      globalCredentialsCache.put(cacheKey, freshCredentials)
      logger.info(s"Successfully refreshed and cached credentials for path: $path")
      freshCredentials
    } match {
      case Failure(e) => logger.warn(s"Failed to refresh credentials for path: $path", e)
      case Success(_) => // Success
    }
  }

  /** Fetch credentials with automatic fallback from READ_WRITE to READ. */
  private def fetchCredentialsWithFallback(path: String): CachedCredentials =
    // Try READ_WRITE first
    Try {
      logger.debug(s"Attempting to fetch READ_WRITE credentials for path: $path")
      val creds = fetchCredentials(path, "READ_WRITE")
      logger.info(s"Successfully obtained READ_WRITE credentials for path: $path")
      creds
    } match {
      case Success(creds) => creds
      case Failure(e) =>
        if (!fallbackEnabled) {
          throw e
        }

        logger.info(s"READ_WRITE credentials unavailable for path: $path, falling back to READ")

        // Fallback to READ
        Try {
          val creds = fetchCredentials(path, "READ")
          logger.info(s"Successfully obtained READ credentials for path: $path")
          creds
        } match {
          case Success(creds) => creds
          case Failure(readException) =>
            logger.error(s"Failed to obtain both READ_WRITE and READ credentials for path: $path")
            throw new RuntimeException(
              s"Unable to obtain any credentials from Unity Catalog for path: $path",
              readException
            )
        }
    }

  /** Fetch credentials from Unity Catalog API. */
  private def fetchCredentials(path: String, operation: String): CachedCredentials = {
    var lastException: Option[Exception] = None

    for (attempt <- 1 to retryAttempts)
      Try {
        logger.debug(s"Fetching $operation credentials for path: $path (attempt $attempt/$retryAttempts)")

        val pathOperation = operation match {
          case "READ_WRITE" => PathOperation.PATH_READ_WRITE
          case "READ"       => PathOperation.PATH_READ
          case _            => PathOperation.PATH_READ_WRITE
        }

        val request = new GenerateTemporaryPathCredentialRequest()
          .setUrl(path)
          .setOperation(pathOperation)

        val response = globalWorkspaceClient
          .temporaryPathCredentials()
          .generateTemporaryPathCredentials(request)

        val awsCreds = response.getAwsTempCredentials

        CachedCredentials(
          awsCreds.getAccessKeyId,
          awsCreds.getSecretAccessKey,
          Option(awsCreds.getSessionToken),
          System.currentTimeMillis()
        )
      } match {
        case Success(creds) => return creds
        case Failure(e: Exception) =>
          lastException = Some(e)
          if (attempt < retryAttempts) {
            val backoffMillis = Math.pow(2, attempt - 1).toLong * 1000
            logger.warn(s"Attempt $attempt failed, retrying in $backoffMillis ms", e)
            Thread.sleep(backoffMillis)
          }
        case Failure(t) => throw t // Re-throw non-exceptions (errors, etc.)
      }

    throw new Exception(
      s"Failed to fetch $operation credentials after $retryAttempts attempts",
      lastException.orNull
    )
  }

  /** Extract the path from the URI. */
  private def extractPath(uri: URI): String = {
    // Convert URI to Unity Catalog compatible path
    val scheme = uri.getScheme
    val host   = uri.getHost
    val path   = uri.getPath

    if (scheme == null || host == null) {
      throw new IllegalArgumentException(s"Invalid URI for Unity Catalog: $uri")
    }

    // Construct full path (e.g., s3://bucket/path)
    val fullPath = s"$scheme://$host$path"

    // Remove trailing slashes
    if (fullPath.endsWith("/")) fullPath.dropRight(1) else fullPath
  }

  /** Build a cache key that includes user context. */
  private def buildCacheKey(path: String): String = {
    val userId = getCurrentUserId()
    // Cache key format: userId:path
    // We don't include operation because we handle fallback internally
    s"$userId:$path"
  }

  /** Get the current user ID for cache isolation. */
  private def getCurrentUserId(): String = {
    // Try to get user from CommandContext first (using reflection for optional dependency)
    val commandContextUser = Try {
      val commandContextClass    = Class.forName("com.databricks.spark.util.CommandContext")
      val getContextObjectMethod = commandContextClass.getMethod("getContextObject")
      val contextObject          = getContextObjectMethod.invoke(null)

      if (contextObject != null) {
        val attributionContextMethod = contextObject.getClass.getMethod("attributionContext")
        val attributionContext =
          attributionContextMethod.invoke(contextObject).asInstanceOf[scala.collection.Map[String, String]]
        attributionContext.get("user")
      } else {
        None
      }
    }.toOption.flatten

    commandContextUser.getOrElse {
      // Try to get user from SparkSession local property
      val sparkSessionUser = Try {
        Option(SparkSession.active.sparkContext.getLocalProperty("user"))
      }.toOption.flatten

      sparkSessionUser.getOrElse {
        // No user found, log warning and return default
        logger.warn("Unable to determine user from CommandContext or SparkSession")
        "NoActiveUser"
      }
    }
  }
}

object UnityCatalogCredentialProvider {
  private val logger = LoggerFactory.getLogger(classOf[UnityCatalogCredentialProvider])

  // Configuration keys
  private val WorkspaceHostKey   = "spark.databricks.workspace.host"
  private val TokenKey           = "spark.databricks.token"
  private val UserEmailKey       = "spark.databricks.user.email"
  private val CacheTtlKey        = "spark.indextables.unity.cache.ttl.minutes"
  private val CacheMaxSizeKey    = "spark.indextables.unity.cache.maxSize"
  private val FallbackEnabledKey = "spark.indextables.unity.fallback.enabled"
  private val RetryAttemptsKey   = "spark.indextables.unity.retry.attempts"

  // Default values
  private val DefaultCacheTtlMinutes = 20
  private val DefaultCacheMaxSize    = 100
  private val DefaultFallbackEnabled = true
  private val DefaultRetryAttempts   = 3
  private val CredentialExpiryMillis = 3600000L // 1 hour

  // Process-global static variables for sharing across all instances
  @volatile private var globalWorkspaceClient: WorkspaceClient                   = _
  @volatile private var globalCredentialsCache: Cache[String, CachedCredentials] = _
  private val initLock                                                           = new Object

  /**
   * Initialize process-global resources (workspace client and cache) only once. This ensures all instances share the
   * same client and cache for efficiency.
   */
  private def initializeGlobalResources(conf: Configuration): Unit =
    if (globalWorkspaceClient == null || globalCredentialsCache == null) {
      initLock.synchronized {
        // Double-check locking pattern
        if (globalWorkspaceClient == null) {
          logger.info("Initializing process-global WorkspaceClient")
          globalWorkspaceClient = initializeWorkspaceClient()
        }
        if (globalCredentialsCache == null) {
          logger.info("Initializing process-global credentials cache")
          globalCredentialsCache = initializeCache(conf)
        }
      }
    }

  /**
   * Initialize the Databricks Workspace Client.
   *
   * The WorkspaceClient will look for credentials in the following order: 1. DATABRICKS_HOST and DATABRICKS_TOKEN
   * environment variables 2. DEFAULT profile in ~/.databrickscfg 3. Databricks CLI configuration 4. Databricks runtime
   * environment (when running on Databricks)
   */
  private def initializeWorkspaceClient(): WorkspaceClient = {
    logger.info("Initializing WorkspaceClient with default configuration (inherits from environment)")

    try {
      // Use default constructor - automatically discovers credentials from environment
      val client = new WorkspaceClient()

      logger.info("WorkspaceClient initialized successfully")
      client
    } catch {
      case e: Exception =>
        logger.error(
          "Failed to initialize WorkspaceClient. Ensure Databricks credentials are configured in environment",
          e
        )
        throw new IllegalStateException(
          "Failed to initialize WorkspaceClient. Credentials should be available via: " +
            "1) DATABRICKS_HOST/DATABRICKS_TOKEN environment variables, " +
            "2) ~/.databrickscfg file, " +
            "3) Databricks CLI configuration, or " +
            "4) Databricks runtime environment",
          e
        )
    }
  }

  /** Initialize the credentials cache with configured TTL and size limits. */
  private def initializeCache(conf: Configuration): Cache[String, CachedCredentials] = {
    val ttlMinutes = conf.getInt(CacheTtlKey, DefaultCacheTtlMinutes)
    val maxSize    = conf.getInt(CacheMaxSizeKey, DefaultCacheMaxSize)

    logger.info(s"Initializing credentials cache: TTL=$ttlMinutes minutes, maxSize=$maxSize")

    CacheBuilder
      .newBuilder()
      .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
      .maximumSize(maxSize)
      .recordStats()
      .build[String, CachedCredentials]()
  }

  /** Log cache statistics for monitoring. */
  private def logCacheStats(): Unit = {
    val stats = globalCredentialsCache.stats()
    if (stats.requestCount() % 100 == 0) { // Log every 100 requests
      logger.info(
        s"Credentials cache stats: hits=${stats.hitCount()}, misses=${stats.missCount()}, " +
          s"hitRate=${f"${stats.hitRate() * 100}%.2f"}%%, size=${globalCredentialsCache.size()}"
      )
    }
  }

  /** Clear all cached credentials. This is a process-global operation that affects all instances. */
  def clearCache(): Unit =
    if (globalCredentialsCache != null) {
      logger.info("Clearing process-global credentials cache")
      globalCredentialsCache.invalidateAll()
    }

  /** Get current cache statistics. Useful for monitoring and debugging. */
  def getCacheStatistics(): Option[CacheStats] =
    Option(globalCredentialsCache).map(_.stats())

  /** Shutdown and cleanup all process-global resources. Should be called when the application is shutting down. */
  def shutdown(): Unit =
    initLock.synchronized {
      logger.info("Shutting down UnityCatalogCredentialProvider global resources")

      if (globalCredentialsCache != null) {
        globalCredentialsCache.invalidateAll()
        globalCredentialsCache = null
      }

      if (globalWorkspaceClient != null) {
        try
          // Close workspace client if it has a close method
          globalWorkspaceClient = null
        catch {
          case e: Exception => logger.warn("Error closing workspace client", e)
        }
      }
    }

  /** Internal case class for cached credentials. */
  private case class CachedCredentials(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: Option[String],
    timestamp: Long)

  /** Basic AWS Credentials implementation for non-session credentials. */
  private class BasicAWSCredentials(val accessKeyId: String, val secretAccessKey: String) extends AWSCredentials {

    override def getAWSAccessKeyId(): String = accessKeyId

    override def getAWSSecretKey(): String = secretAccessKey
  }
}
