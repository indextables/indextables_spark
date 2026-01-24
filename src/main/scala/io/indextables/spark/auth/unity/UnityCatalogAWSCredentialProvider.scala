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

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import java.time.Instant

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicSessionCredentials}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.cache.{Cache, CacheBuilder, CacheStats}
import io.indextables.spark.transaction.EnhancedTransactionLogCache
import io.indextables.spark.util.{ConfigSource, ConfigurationResolver, HadoopConfigSource}
import org.slf4j.LoggerFactory

/**
 * AWS Credential Provider that integrates with Databricks Unity Catalog via HTTP API.
 *
 * This provider fetches temporary AWS credentials from Unity Catalog's temporary path credentials API. It implements
 * intelligent caching with expiration tracking and automatic fallback from READ_WRITE to READ permissions.
 *
 * Configuration:
 *   - spark.indextables.databricks.workspaceUrl: Databricks workspace URL (required)
 *   - spark.indextables.databricks.apiToken: Databricks API token (required)
 *   - spark.indextables.databricks.credential.refreshBuffer.minutes: Minutes before expiration to refresh (default: 40)
 *   - spark.indextables.databricks.cache.maxSize: Maximum cached entries (default: 100)
 *   - spark.indextables.databricks.fallback.enabled: Enable READ fallback (default: true)
 *   - spark.indextables.databricks.retry.attempts: Retry attempts on failure (default: 3)
 *
 * Usage:
 * {{{
 * spark.conf.set("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
 * spark.conf.set("spark.indextables.databricks.apiToken", "<your-token>")
 * spark.conf.set("spark.indextables.aws.credentialsProviderClass",
 *   "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")
 * }}}
 */
class UnityCatalogAWSCredentialProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider {
  import UnityCatalogAWSCredentialProvider._

  // Resolve configuration
  private val (workspaceUrl, token) = resolveConfig(conf)
  private val refreshBufferMinutes  = conf.getInt(RefreshBufferKey, DefaultRefreshBufferMinutes)
  private val fallbackEnabled       = conf.getBoolean(FallbackEnabledKey, DefaultFallbackEnabled)
  private val retryAttempts         = conf.getInt(RetryAttemptsKey, DefaultRetryAttempts)

  logger.info(s"Initializing UnityCatalogAWSCredentialProvider for URI: $uri")
  logger.info(s"  Workspace URL: $workspaceUrl")
  logger.info(s"  Refresh buffer: $refreshBufferMinutes minutes before expiration")

  // Initialize process-global cache only once
  initializeGlobalCache(conf)

  logger.info("UnityCatalogAWSCredentialProvider initialized successfully")

  /**
   * Get AWS credentials for the configured URI.
   *
   * This method:
   *   1. Checks the cache for valid credentials (not near expiration) 2. If cached and valid, returns them 3.
   *      Otherwise, fetches fresh credentials with READ_WRITE/READ fallback 4. Caches the new credentials
   */
  override def getCredentials(): AWSCredentials = {
    val path     = extractPath(uri)
    val cacheKey = buildCacheKey(token, path)

    logger.debug(s"Getting credentials for path: $path")

    // Check cache first
    val cached = globalCredentialsCache.getIfPresent(cacheKey)
    if (cached != null && !isNearExpiration(cached)) {
      logger.debug(
        s"Using cached credentials for path: $path (expires at ${Instant.ofEpochMilli(cached.expirationTime)})"
      )
      logCacheStats()
      return cached.toAWSCredentials
    }

    // Fetch fresh credentials
    try {
      val freshCredentials = fetchCredentialsWithFallback(path)
      globalCredentialsCache.put(cacheKey, freshCredentials)
      logCacheStats()
      freshCredentials.toAWSCredentials
    } catch {
      case e: Exception =>
        logger.error(s"Failed to obtain Unity Catalog credentials for path: $path", e)
        throw new RuntimeException("Failed to obtain Unity Catalog credentials", e)
    }
  }

  /** Refresh credentials (forced refresh, bypassing cache). */
  override def refresh(): Unit = {
    val path     = extractPath(uri)
    val cacheKey = buildCacheKey(token, path)

    logger.info(s"Refreshing credentials for path: $path")

    // Invalidate cached entry
    globalCredentialsCache.invalidate(cacheKey)

    // Fetch new credentials and update cache
    Try {
      val freshCredentials = fetchCredentialsWithFallback(path)
      globalCredentialsCache.put(cacheKey, freshCredentials)
      logger.info(s"Successfully refreshed and cached credentials for path: $path")
    } match {
      case Failure(e) => logger.warn(s"Failed to refresh credentials for path: $path", e)
      case Success(_) => // Success
    }
  }

  /** Check if credentials are near expiration and should be refreshed. */
  private def isNearExpiration(creds: CachedCredentials): Boolean = {
    val refreshBufferMs = refreshBufferMinutes.toLong * 60 * 1000
    val now             = System.currentTimeMillis()
    val refreshAt       = creds.expirationTime - refreshBufferMs

    if (now >= refreshAt) {
      logger.debug(
        s"Credentials near expiration: now=$now, expires=${creds.expirationTime}, " +
          s"refreshBuffer=${refreshBufferMinutes}min, shouldRefresh=true"
      )
      true
    } else {
      false
    }
  }

  /** Fetch credentials with automatic fallback from READ_WRITE to READ. */
  private def fetchCredentialsWithFallback(path: String): CachedCredentials =
    // Try READ_WRITE first
    Try {
      logger.debug(s"Attempting to fetch PATH_READ_WRITE credentials for path: $path")
      val creds = fetchCredentials(path, "PATH_READ_WRITE")
      logger.info(s"Successfully obtained PATH_READ_WRITE credentials for path: $path")
      creds
    } match {
      case Success(creds) => creds
      case Failure(e) =>
        if (!fallbackEnabled) {
          throw e
        }

        logger.info(s"PATH_READ_WRITE credentials unavailable for path: $path, falling back to PATH_READ")
        logger.debug(s"PATH_READ_WRITE failure reason: ${e.getMessage}")

        // Fallback to READ
        Try {
          val creds = fetchCredentials(path, "PATH_READ")
          logger.info(s"Successfully obtained PATH_READ credentials for path: $path")
          creds
        } match {
          case Success(creds) => creds
          case Failure(readException) =>
            logger.error(s"Failed to obtain both PATH_READ_WRITE and PATH_READ credentials for path: $path")
            throw new RuntimeException(
              s"Unable to obtain any credentials from Unity Catalog for path: $path. " +
                s"PATH_READ_WRITE error: ${e.getMessage}, PATH_READ error: ${readException.getMessage}",
              readException
            )
        }
    }

  /** Fetch credentials from Unity Catalog API via HTTP. */
  private def fetchCredentials(path: String, operation: String): CachedCredentials = {
    var lastException: Option[Exception] = None

    for (attempt <- 1 to retryAttempts)
      Try {
        logger.debug(s"Fetching $operation credentials for path: $path (attempt $attempt/$retryAttempts)")

        val requestBody = s"""{"operation": "$operation", "url": "$path"}"""

        val request = HttpRequest
          .newBuilder()
          .uri(URI.create(s"$workspaceUrl/api/2.1/unity-catalog/temporary-path-credentials"))
          .header("Authorization", s"Bearer $token")
          .header("Content-Type", "application/json")
          .timeout(Duration.ofSeconds(30))
          .POST(HttpRequest.BodyPublishers.ofString(requestBody))
          .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() != 200) {
          throw new RuntimeException(
            s"Unity Catalog API returned ${response.statusCode()}: ${response.body()}"
          )
        }

        parseCredentialsResponse(response.body())
      } match {
        case Success(creds) => return creds
        case Failure(e: Exception) =>
          lastException = Some(e)
          if (attempt < retryAttempts) {
            val backoffMillis = Math.pow(2, attempt - 1).toLong * 1000
            logger.warn(s"Attempt $attempt failed, retrying in $backoffMillis ms: ${e.getMessage}")
            Thread.sleep(backoffMillis)
          }
        case Failure(t) => throw t // Re-throw non-exceptions (errors, etc.)
      }

    throw new Exception(
      s"Failed to fetch $operation credentials after $retryAttempts attempts",
      lastException.orNull
    )
  }

  /** Parse the API response JSON into CachedCredentials. */
  private def parseCredentialsResponse(json: String): CachedCredentials = {
    EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
    val root     = objectMapper.readTree(json)
    val awsCreds = root.get("aws_temp_credentials")

    if (awsCreds == null) {
      throw new RuntimeException(s"Response missing 'aws_temp_credentials' field: $json")
    }

    val accessKeyId     = getRequiredField(awsCreds, "access_key_id")
    val secretAccessKey = getRequiredField(awsCreds, "secret_access_key")
    val sessionToken    = Option(awsCreds.get("session_token")).map(_.asText())
    val expirationTime  = parseExpirationTime(root)

    CachedCredentials(
      accessKeyId = accessKeyId,
      secretAccessKey = secretAccessKey,
      sessionToken = sessionToken,
      expirationTime = expirationTime,
      fetchedAt = System.currentTimeMillis()
    )
  }

  /** Get a required field from JSON, throwing if missing. */
  private def getRequiredField(node: JsonNode, fieldName: String): String = {
    val field = node.get(fieldName)
    if (field == null || field.isNull) {
      throw new RuntimeException(s"Missing required field: $fieldName")
    }
    field.asText()
  }

  /** Parse expiration time from API response (epoch milliseconds). */
  private def parseExpirationTime(root: JsonNode): Long = {
    val expirationNode = root.get("expiration_time")
    if (expirationNode != null && !expirationNode.isNull) {
      expirationNode.asLong()
    } else {
      // Default: 1 hour from now if not provided
      logger.warn("No expiration_time in response, using default 1 hour")
      System.currentTimeMillis() + 3600000L
    }
  }

  /** Extract the path from the URI. */
  private def extractPath(uri: URI): String = {
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
}

object UnityCatalogAWSCredentialProvider {
  private val logger = LoggerFactory.getLogger(classOf[UnityCatalogAWSCredentialProvider])

  // Configuration keys - Databricks connection
  private val WorkspaceUrlKey = "workspaceUrl"
  private val TokenKey        = "apiToken"

  // Configuration keys - Databricks behavior
  private val RefreshBufferKey   = "spark.indextables.databricks.credential.refreshBuffer.minutes"
  private val CacheMaxSizeKey    = "spark.indextables.databricks.cache.maxSize"
  private val FallbackEnabledKey = "spark.indextables.databricks.fallback.enabled"
  private val RetryAttemptsKey   = "spark.indextables.databricks.retry.attempts"

  // Default values
  private val DefaultRefreshBufferMinutes = 40
  private val DefaultCacheMaxSize         = 100
  private val DefaultFallbackEnabled      = true
  private val DefaultRetryAttempts        = 3

  // Process-global HTTP client (thread-safe, reusable)
  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(30))
    .build()

  // Process-global JSON parser
  private val objectMapper: ObjectMapper = new ObjectMapper()

  // Process-global credentials cache
  @volatile private var globalCredentialsCache: Cache[String, CachedCredentials] = _
  private val initLock                                                           = new Object

  /** Resolve Databricks configuration from Hadoop/Spark configuration. */
  private def resolveConfig(conf: Configuration): (String, String) = {
    val sources: Seq[ConfigSource] = Seq(
      HadoopConfigSource(conf, "spark.indextables.databricks"),
      HadoopConfigSource(conf)
    )

    val workspaceUrl = ConfigurationResolver
      .resolveString(WorkspaceUrlKey, sources)
      .map(url => if (url.endsWith("/")) url.dropRight(1) else url) // Remove trailing slash
      .getOrElse(
        throw new IllegalStateException(
          "Databricks workspace URL not configured. Set spark.indextables.databricks.workspaceUrl"
        )
      )

    val token = ConfigurationResolver
      .resolveString(TokenKey, sources, logMask = true)
      .getOrElse(
        throw new IllegalStateException(
          "Databricks API token not configured. Set spark.indextables.databricks.apiToken"
        )
      )

    (workspaceUrl, token)
  }

  /** Initialize the process-global credentials cache. Uses double-checked locking for thread safety. */
  private def initializeGlobalCache(conf: Configuration): Unit =
    if (globalCredentialsCache == null) {
      initLock.synchronized {
        if (globalCredentialsCache == null) {
          val maxSize = conf.getInt(CacheMaxSizeKey, DefaultCacheMaxSize)
          logger.info(s"Initializing process-global credentials cache: maxSize=$maxSize")

          globalCredentialsCache = CacheBuilder
            .newBuilder()
            .maximumSize(maxSize)
            .recordStats()
            .build[String, CachedCredentials]()
        }
      }
    }

  /**
   * Build a cache key that includes token identity for multi-user support.
   *
   * Format: tokenHash:path This ensures different API tokens get separate cached credentials.
   */
  private def buildCacheKey(token: String, path: String): String = {
    // Use hash of token to avoid storing the full token in memory
    val tokenHash = Integer.toHexString(token.hashCode)
    s"$tokenHash:$path"
  }

  /** Log cache statistics for monitoring. */
  private def logCacheStats(): Unit =
    if (globalCredentialsCache != null) {
      val stats = globalCredentialsCache.stats()
      if (stats.requestCount() % 100 == 0 && stats.requestCount() > 0) {
        logger.info(
          s"Credentials cache stats: hits=${stats.hitCount()}, misses=${stats.missCount()}, " +
            s"hitRate=${f"${stats.hitRate() * 100}%.2f"}%%, size=${globalCredentialsCache.size()}"
        )
      }
    }

  /** Clear all cached credentials. This is a process-global operation. */
  def clearCache(): Unit =
    if (globalCredentialsCache != null) {
      logger.info("Clearing process-global credentials cache")
      globalCredentialsCache.invalidateAll()
    }

  /** Get current cache statistics. Useful for monitoring and debugging. */
  def getCacheStatistics(): Option[CacheStats] =
    Option(globalCredentialsCache).map(_.stats())

  /** Shutdown and cleanup all process-global resources. */
  def shutdown(): Unit =
    initLock.synchronized {
      logger.info("Shutting down UnityCatalogAWSCredentialProvider global resources")

      if (globalCredentialsCache != null) {
        globalCredentialsCache.invalidateAll()
        globalCredentialsCache = null
      }
    }

  /** Internal case class for cached credentials with expiration tracking. */
  private[unity] case class CachedCredentials(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: Option[String],
    expirationTime: Long,
    fetchedAt: Long) {

    /** Convert to AWS SDK credentials. */
    def toAWSCredentials: AWSCredentials =
      sessionToken match {
        case Some(token) =>
          new BasicSessionCredentials(accessKeyId, secretAccessKey, token)
        case None =>
          new BasicAWSCredentialsImpl(accessKeyId, secretAccessKey)
      }
  }

  /** Basic AWS Credentials implementation for non-session credentials. */
  private class BasicAWSCredentialsImpl(val accessKeyId: String, val secretAccessKey: String) extends AWSCredentials {
    override def getAWSAccessKeyId(): String = accessKeyId
    override def getAWSSecretKey(): String   = secretAccessKey
  }
}
