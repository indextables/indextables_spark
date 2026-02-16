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

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicSessionCredentials}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.cache.{Cache, CacheBuilder, CacheStats}
import io.indextables.spark.transaction.EnhancedTransactionLogCache
import io.indextables.spark.util.{ConfigSource, ConfigurationResolver, MapConfigSource}
import org.slf4j.LoggerFactory

/**
 * AWS Credential Provider that integrates with Databricks Unity Catalog via HTTP API.
 *
 * This provider fetches temporary AWS credentials from Unity Catalog's temporary path credentials API. It implements
 * intelligent caching with expiration tracking and automatic fallback from READ_WRITE to READ permissions.
 *
 * IMPORTANT: This provider MUST be created using the fromConfig() factory method or by passing a Map[String, String]
 * config. The Hadoop Configuration constructor has been removed to enforce the fast path that avoids expensive Hadoop
 * Configuration creation.
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
 * // Use the factory method (preferred)
 * val provider = UnityCatalogAWSCredentialProvider.fromConfig(uri, configMap)
 *
 * // Or use driver-side credential resolution (recommended for scans)
 * // Resolve credentials on driver, pass actual keys to executors
 * }}}
 */
class UnityCatalogAWSCredentialProvider private[unity] (uri: URI, config: Map[String, String])
    extends AWSCredentialsProvider {
  import UnityCatalogAWSCredentialProvider._

  // Resolve all configuration from the Map
  private val (_workspaceUrl, _token, _refreshBufferMinutes, _fallbackEnabled, _retryAttempts) =
    resolveConfigFromMap(config)

  // Initialize the global cache
  initializeGlobalCacheFromMap(config)

  // Log initialization
  logger.info(s"Initializing UnityCatalogAWSCredentialProvider for URI: $uri")
  logger.info(s"  Workspace URL: ${_workspaceUrl}")
  logger.info(s"  Refresh buffer: ${_refreshBufferMinutes} minutes before expiration")
  logger.info("UnityCatalogAWSCredentialProvider initialized successfully")

  // Accessors for resolved config (for cleaner code below)
  private def workspaceUrl: String      = _workspaceUrl
  private def token: String             = _token
  private def refreshBufferMinutes: Int = _refreshBufferMinutes
  private def fallbackEnabled: Boolean  = _fallbackEnabled
  private def retryAttempts: Int        = _retryAttempts

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

        UnityCatalogAWSCredentialProvider.parseCredentialsResponse(response.body())
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

object UnityCatalogAWSCredentialProvider extends io.indextables.spark.utils.TableCredentialProvider {
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

  /**
   * Fast factory method that creates a provider from a config Map without creating Hadoop Configuration.
   *
   * This is the preferred method for creating providers when you already have config as a Map, as it bypasses the
   * expensive Hadoop Configuration creation that happens tens of thousands of times during scan operations.
   *
   * IMPORTANT: The URI should be normalized to the table root path before calling this method to ensure consistent
   * cache keys. Use TablePathNormalizer.normalizeToTablePath().
   *
   * @param uri
   *   The URI for the table (should be normalized to table root)
   * @param config
   *   Map containing spark.indextables.databricks.* configuration
   * @return
   *   A new UnityCatalogAWSCredentialProvider instance
   */
  def fromConfig(uri: URI, config: Map[String, String]): UnityCatalogAWSCredentialProvider = {
    logger.debug(s"Creating UnityCatalogAWSCredentialProvider from config Map for URI: $uri")
    new UnityCatalogAWSCredentialProvider(uri, config)
  }

  /**
   * Resolve Databricks configuration from a Map[String, String]. This is the fast path that avoids Hadoop Configuration
   * creation.
   */
  private def resolveConfigFromMap(config: Map[String, String]): (String, String, Int, Boolean, Int) = {
    val sources: Seq[ConfigSource] = Seq(
      MapConfigSource(config, "spark.indextables.databricks"),
      MapConfigSource(config)
    )

    val workspaceUrl = ConfigurationResolver
      .resolveString(WorkspaceUrlKey, sources)
      .map(url => if (url.endsWith("/")) url.dropRight(1) else url)
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

    val refreshBuffer = ConfigurationResolver
      .resolveInt(RefreshBufferKey, sources, DefaultRefreshBufferMinutes)

    val fallbackEnabled = ConfigurationResolver
      .resolveBoolean(FallbackEnabledKey, sources, DefaultFallbackEnabled)

    val retryAttempts = ConfigurationResolver
      .resolveInt(RetryAttemptsKey, sources, DefaultRetryAttempts)

    (workspaceUrl, token, refreshBuffer, fallbackEnabled, retryAttempts)
  }

  // Process-global HTTP client (thread-safe, reusable)
  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(30))
    .build()

  // Process-global JSON parser
  private val objectMapper: ObjectMapper = new ObjectMapper()

  // Process-global credentials cache
  @volatile private var globalCredentialsCache: Cache[String, CachedCredentials] = _

  // Process-global table info cache (table name → TableInfo with table_id + storage_location)
  // Table info rarely changes, so we use a 1-hour TTL to avoid unnecessary UC API calls.
  @volatile private var globalTableInfoCache: Cache[String, io.indextables.spark.utils.TableInfo] = _

  private val initLock = new Object

  /** Initialize the process-global credentials and table info caches from Map config. */
  private def initializeGlobalCacheFromMap(config: Map[String, String]): Unit =
    if (globalCredentialsCache == null || globalTableInfoCache == null) {
      initLock.synchronized {
        if (globalCredentialsCache == null) {
          val sources = Seq(MapConfigSource(config, "spark.indextables.databricks"), MapConfigSource(config))
          val maxSize = ConfigurationResolver.resolveInt(CacheMaxSizeKey, sources, DefaultCacheMaxSize)
          logger.info(s"Initializing process-global credentials cache from Map: maxSize=$maxSize")

          globalCredentialsCache = CacheBuilder
            .newBuilder()
            .maximumSize(maxSize)
            .recordStats()
            .build[String, CachedCredentials]()
        }
        if (globalTableInfoCache == null) {
          logger.info("Initializing process-global table info cache: maxSize=100, TTL=1h")
          globalTableInfoCache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterWrite(1, java.util.concurrent.TimeUnit.HOURS)
            .recordStats()
            .build[String, io.indextables.spark.utils.TableInfo]()
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

  /** Clear all cached credentials and table info. This is a process-global operation. */
  def clearCache(): Unit = {
    if (globalCredentialsCache != null) {
      logger.info("Clearing process-global credentials cache")
      globalCredentialsCache.invalidateAll()
    }
    if (globalTableInfoCache != null) {
      logger.info("Clearing process-global table info cache")
      globalTableInfoCache.invalidateAll()
    }
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
      if (globalTableInfoCache != null) {
        globalTableInfoCache.invalidateAll()
        globalTableInfoCache = null
      }
    }

  // ===== Shared response parsing (used by both path and table credential flows) =====

  /** Parse the API response JSON into CachedCredentials. */
  private[unity] def parseCredentialsResponse(json: String): CachedCredentials = {
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

  // ===== Table-based credential API (for Iceberg and Delta via Unity Catalog) =====

  /**
   * Shared implementation: call GET /tables/{name} and return both table_id and storage_location. Used by both
   * resolveTableId (returns ID only) and resolveTableInfo (returns both).
   *
   * Results are cached in the process-global table info cache (1-hour TTL) to avoid redundant UC API calls, since
   * table_id and storage_location rarely change.
   */
  private def fetchTableInfoInternal(
    fullTableName: String,
    workspaceUrl: String,
    token: String,
    retryAttempts: Int
  ): io.indextables.spark.utils.TableInfo = {
    val tokenHash = Integer.toHexString(token.hashCode)
    val cacheKey  = s"$tokenHash:tableinfo:$fullTableName"

    // Check cache first
    if (globalTableInfoCache != null) {
      val cached = globalTableInfoCache.getIfPresent(cacheKey)
      if (cached != null) {
        logger.debug(s"Using cached table info for '$fullTableName' -> table_id=${cached.tableId}")
        return cached
      }
    }

    val encodedName                      = java.net.URLEncoder.encode(fullTableName, "UTF-8")
    var lastException: Option[Exception] = None

    for (attempt <- 1 to retryAttempts)
      Try {
        logger.info(s"Resolving table info for '$fullTableName' (attempt $attempt/$retryAttempts)")

        val request = HttpRequest
          .newBuilder()
          .uri(URI.create(s"$workspaceUrl/api/2.1/unity-catalog/tables/$encodedName"))
          .header("Authorization", s"Bearer $token")
          .header("Content-Type", "application/json")
          .timeout(Duration.ofSeconds(30))
          .GET()
          .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() != 200) {
          throw new RuntimeException(
            s"Unity Catalog GET /tables/$encodedName returned ${response.statusCode()}: ${response.body()}"
          )
        }

        val root        = objectMapper.readTree(response.body())
        val tableIdNode = root.get("table_id")
        if (tableIdNode == null || tableIdNode.isNull) {
          throw new RuntimeException(
            s"Response missing 'table_id' for table '$fullTableName': ${response.body()}"
          )
        }
        val tableId = tableIdNode.asText()

        val storageLocation = Option(root.get("storage_location"))
          .filterNot(_.isNull)
          .map(_.asText())
          .getOrElse("")

        logger.info(s"Resolved table '$fullTableName' -> table_id=$tableId, storage_location=$storageLocation")
        io.indextables.spark.utils.TableInfo(tableId, storageLocation)
      } match {
        case Success(info) =>
          // Cache the result
          if (globalTableInfoCache != null) {
            globalTableInfoCache.put(cacheKey, info)
          }
          return info
        case Failure(e: Exception) =>
          lastException = Some(e)
          if (attempt < retryAttempts) {
            val backoffMillis = Math.pow(2, attempt - 1).toLong * 1000
            logger.warn(s"Attempt $attempt failed, retrying in $backoffMillis ms: ${e.getMessage}")
            Thread.sleep(backoffMillis)
          }
        case Failure(t) => throw t
      }

    throw new RuntimeException(
      s"Failed to resolve table info for '$fullTableName' after $retryAttempts attempts",
      lastException.orNull
    )
  }

  /**
   * Resolve a Unity Catalog table name to its table_id UUID. Called once on the driver side.
   *
   * @param fullTableName
   *   Three-part name: catalog.namespace.table
   * @param config
   *   Config map with workspace URL and API token
   * @return
   *   The table_id UUID string
   */
  def resolveTableId(fullTableName: String, config: Map[String, String]): String = {
    val (workspaceUrl, token, _, _, retryAttempts) = resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)
    fetchTableInfoInternal(fullTableName, workspaceUrl, token, retryAttempts).tableId
  }

  /**
   * Resolve a Unity Catalog table name to both its table_id UUID and storage_location. Called once on the driver side.
   * Uses the same GET /tables/{name} API as resolveTableId but also extracts storage_location from the response.
   *
   * @param fullTableName
   *   Three-part name: catalog.namespace.table
   * @param config
   *   Config map with workspace URL and API token
   * @return
   *   TableInfo containing table_id and storage_location
   */
  override def resolveTableInfo(
    fullTableName: String,
    config: Map[String, String]
  ): io.indextables.spark.utils.TableInfo = {
    val (workspaceUrl, token, _, _, retryAttempts) = resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)
    fetchTableInfoInternal(fullTableName, workspaceUrl, token, retryAttempts)
  }

  /**
   * Get AWS credentials for a Unity Catalog table using the table-based credential API. Used by executors when
   * `spark.indextables.iceberg.uc.tableId` is present in config.
   *
   * @param tableId
   *   The table UUID (resolved on the driver via resolveTableId)
   * @param config
   *   Config map with workspace URL and API token
   * @return
   *   BasicAWSCredentials suitable for S3 access
   */
  def getTableCredentials(
    tableId: String,
    config: Map[String, String]
  ): io.indextables.spark.utils.CredentialProviderFactory.BasicAWSCredentials = {
    val (workspaceUrl, token, refreshBufferMinutes, fallbackEnabled, retryAttempts) =
      resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)

    val tokenHash = Integer.toHexString(token.hashCode)
    val cacheKey  = s"$tokenHash:table:$tableId"

    // Check cache first
    val cached = globalCredentialsCache.getIfPresent(cacheKey)
    if (cached != null) {
      val refreshBufferMs = refreshBufferMinutes.toLong * 60 * 1000
      val now             = System.currentTimeMillis()
      if (now < cached.expirationTime - refreshBufferMs) {
        logger.debug(s"Using cached table credentials for tableId=$tableId")
        return toBasicCredentials(cached)
      }
    }

    // Fetch with READ_WRITE -> READ fallback
    val creds = Try {
      logger.debug(s"Fetching READ_WRITE table credentials for tableId=$tableId")
      fetchTableCredentials(tableId, "READ_WRITE", workspaceUrl, token, retryAttempts)
    } match {
      case Success(c) => c
      case Failure(e) =>
        if (!fallbackEnabled) throw e
        logger.info(s"READ_WRITE table credentials failed for tableId=$tableId, falling back to READ")
        Try {
          fetchTableCredentials(tableId, "READ", workspaceUrl, token, retryAttempts)
        } match {
          case Success(c) => c
          case Failure(readEx) =>
            throw new RuntimeException(
              s"Failed to obtain table credentials for tableId=$tableId. " +
                s"READ_WRITE: ${e.getMessage}, READ: ${readEx.getMessage}",
              readEx
            )
        }
    }

    globalCredentialsCache.put(cacheKey, creds)
    toBasicCredentials(creds)
  }

  /** Fetch table credentials from Unity Catalog temporary-table-credentials API. */
  private def fetchTableCredentials(
    tableId: String,
    operation: String,
    workspaceUrl: String,
    token: String,
    retryAttempts: Int
  ): CachedCredentials = {
    var lastException: Option[Exception] = None

    for (attempt <- 1 to retryAttempts)
      Try {
        logger.debug(s"Fetching $operation table credentials for tableId=$tableId (attempt $attempt/$retryAttempts)")

        val requestBody = s"""{"table_id": "$tableId", "operation": "$operation"}"""

        val request = HttpRequest
          .newBuilder()
          .uri(URI.create(s"$workspaceUrl/api/2.1/unity-catalog/temporary-table-credentials"))
          .header("Authorization", s"Bearer $token")
          .header("Content-Type", "application/json")
          .timeout(Duration.ofSeconds(30))
          .POST(HttpRequest.BodyPublishers.ofString(requestBody))
          .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() != 200) {
          throw new RuntimeException(
            s"Unity Catalog table credentials API returned ${response.statusCode()}: ${response.body()}"
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
        case Failure(t) => throw t
      }

    throw new RuntimeException(
      s"Failed to fetch $operation table credentials after $retryAttempts attempts",
      lastException.orNull
    )
  }

  /** Convert CachedCredentials to CredentialProviderFactory.BasicAWSCredentials. */
  private def toBasicCredentials(
    cached: CachedCredentials
  ): io.indextables.spark.utils.CredentialProviderFactory.BasicAWSCredentials =
    io.indextables.spark.utils.CredentialProviderFactory.BasicAWSCredentials(
      cached.accessKeyId,
      cached.secretAccessKey,
      cached.sessionToken
    )

  /**
   * Auto-derive Iceberg catalog configuration from Databricks Unity Catalog settings.
   *
   * When Unity Catalog is the credential provider, we can derive:
   *   - `spark.indextables.iceberg.uri` from the workspace URL (Iceberg REST endpoint)
   *   - `spark.indextables.iceberg.token` from the Databricks API token
   *   - `spark.indextables.iceberg.catalogType` defaults to "rest"
   *
   * These are returned as defaults — user-explicit values always take precedence.
   */
  override def icebergCatalogDefaults(config: Map[String, String]): Map[String, String] = {
    val (workspaceUrl, token, _, _, _) = resolveConfigFromMap(config)

    val defaults = Map(
      "spark.indextables.iceberg.uri"         -> s"$workspaceUrl/api/2.1/unity-catalog/iceberg-rest",
      "spark.indextables.iceberg.token"       -> token,
      "spark.indextables.iceberg.catalogType" -> "rest"
    )

    logger.info(
      s"Auto-derived Iceberg catalog defaults from Unity Catalog: " +
        s"uri=${defaults("spark.indextables.iceberg.uri")}, catalogType=rest"
    )

    defaults
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
