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
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.cache.{Cache, CacheBuilder, CacheStats}
import io.indextables.spark.transaction.EnhancedTransactionLogCache
import io.indextables.spark.util.{ConfigSource, ConfigurationResolver, MapConfigSource}
import org.slf4j.LoggerFactory

/**
 * AWS Credential Provider that integrates with Databricks Unity Catalog via HTTP API.
 *
 * This provider fetches temporary AWS credentials from Unity Catalog's temporary path credentials API. It implements
 * intelligent caching with expiration tracking and automatic fallback from PATH_READ_WRITE to PATH_READ.
 *
 * By default, credentials are requested as PATH_READ_WRITE with fallback to PATH_READ on 403. Read paths can set
 * credential.operation=PATH_READ to skip the fallback and request read credentials directly.
 *
 * IMPORTANT: This provider MUST be created using the fromConfig() factory method or by passing a Map[String, String]
 * config. The Hadoop Configuration constructor has been removed to enforce the fast path that avoids expensive Hadoop
 * Configuration creation.
 *
 * === Auth Modes ===
 *
 * Two mutually exclusive auth strategies are supported:
 *
 * '''Static API token''' (original mode):
 *   - spark.indextables.databricks.apiToken: Databricks personal access token
 *
 * '''OAuth2 Client Credentials''' (machine-to-machine, no personal token needed):
 *   - spark.indextables.databricks.clientId: OAuth2 client ID
 *   - spark.indextables.databricks.clientSecret: OAuth2 client secret
 *   - spark.indextables.databricks.accountId: Databricks account ID (for the OIDC token endpoint)
 *
 * When all three OAuth keys are present they take precedence. Partial OAuth config (only one or two
 * of the three keys) is an error. OAuth tokens are cached process-globally keyed by clientId and
 * refreshed automatically when within `spark.indextables.databricks.oauth.refreshBuffer.seconds`
 * (default: 60) of expiry. The AWS credential cache key is stable across token rotations (keyed by
 * clientId, not the transient access token).
 *
 * Configuration:
 *   - spark.indextables.databricks.workspaceUrl: Databricks workspace URL (required)
 *   - spark.indextables.databricks.apiToken: Databricks API token (static auth mode)
 *   - spark.indextables.databricks.clientId: OAuth2 client ID (OAuth auth mode)
 *   - spark.indextables.databricks.clientSecret: OAuth2 client secret (OAuth auth mode)
 *   - spark.indextables.databricks.accountId: Databricks account ID (OAuth auth mode)
 *   - spark.indextables.databricks.oauth.refreshBuffer.seconds: Seconds before OAuth token expiry to re-exchange (default: 600 = 10 min). Effective threshold is min(this, expires_in/2).
 *   - spark.indextables.databricks.oauth.scope: OAuth2 scope for client-credentials grant (default: "all-apis")
 *   - spark.indextables.databricks.credential.refreshBuffer.minutes: Minutes before AWS credential expiry to refresh (default: 40)
 *   - spark.indextables.databricks.cache.maxSize: Maximum cached entries (default: 100)
 *   - spark.indextables.databricks.credential.operation: PATH_READ or PATH_READ_WRITE (default: PATH_READ_WRITE)
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
  private val (_workspaceUrl, _authMode, _refreshBufferMinutes, _retryAttempts) =
    resolveConfigFromMap(config)

  private val credentialOperation = resolveCredentialOperation(config)

  // Initialize the global cache
  initializeGlobalCacheFromMap(config)

  // Log initialization
  logger.info(s"Initializing UnityCatalogAWSCredentialProvider for URI: $uri")
  logger.info(s"  Workspace URL: ${_workspaceUrl}")
  logger.info(s"  Auth mode: ${_authMode.getClass.getSimpleName}")
  logger.info(s"  Refresh buffer: ${_refreshBufferMinutes} minutes before expiration")
  logger.info(s"  Credential operation: $credentialOperation")
  logger.info("UnityCatalogAWSCredentialProvider initialized successfully")

  // Accessors for resolved config (for cleaner code below)
  private def workspaceUrl: String      = _workspaceUrl
  // Resolves a fresh-or-cached bearer token on each call. For StaticToken this is a no-op field
  // read; for ClientCredentials it checks the OAuth token cache and re-exchanges if near expiry.
  private def token: String             = resolveToken(_authMode)
  private def refreshBufferMinutes: Int = _refreshBufferMinutes
  private def retryAttempts: Int        = _retryAttempts

  /**
   * Get AWS credentials for the configured URI.
   *
   * This method:
   *   1. Checks the cache for valid credentials (not near expiration) 2. If cached and valid, returns them 3.
   *      Otherwise, fetches fresh credentials. If credential.operation is PATH_READ, fetches directly; otherwise uses
   *      PATH_READ_WRITE with automatic fallback to PATH_READ on 403. 4. Caches the new credentials
   */
  override def getCredentials(): AWSCredentials = {
    val path     = extractPath(uri)
    val cacheKey = buildCacheKey(authIdentity(_authMode), credentialOperation, path)

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

    // Fetch fresh credentials:
    // - If explicitly PATH_READ, fetch directly (no fallback needed)
    // - Otherwise, try PATH_READ_WRITE with automatic fallback to PATH_READ
    try {
      val freshCredentials =
        if (credentialOperation == "PATH_READ") fetchCredentials(path, "PATH_READ")
        else fetchCredentialsWithFallback(path)
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
    val cacheKey = buildCacheKey(authIdentity(_authMode), credentialOperation, path)

    logger.info(s"Refreshing credentials for path: $path")

    // Invalidate cached entry
    globalCredentialsCache.invalidate(cacheKey)

    // Fetch new credentials and update cache
    Try {
      val freshCredentials =
        if (credentialOperation == "PATH_READ") fetchCredentials(path, "PATH_READ")
        else fetchCredentialsWithFallback(path)
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

  /** Fetch credentials with automatic fallback from PATH_READ_WRITE to PATH_READ. */
  private def fetchCredentialsWithFallback(path: String): CachedCredentials =
    Try {
      logger.debug(s"Attempting to fetch PATH_READ_WRITE credentials for path: $path")
      val creds = fetchCredentials(path, "PATH_READ_WRITE")
      logger.info(s"Successfully obtained PATH_READ_WRITE credentials for path: $path")
      creds
    } match {
      case Success(creds) => creds
      case Failure(e) =>
        logger.info(s"PATH_READ_WRITE credentials unavailable for path: $path, falling back to PATH_READ")
        logger.debug(s"PATH_READ_WRITE failure reason: ${e.getMessage}")

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
  private val RefreshBufferKey       = "spark.indextables.databricks.credential.refreshBuffer.minutes"
  private val CacheMaxSizeKey        = "spark.indextables.databricks.cache.maxSize"
  private val CredentialOperationKey = "spark.indextables.databricks.credential.operation"
  private val RetryAttemptsKey       = "spark.indextables.databricks.retry.attempts"

  // Configuration keys - OAuth client credentials (bare-leaf form, resolved through ConfigurationResolver
  // with the same two-source prefix/bare-leaf/case-insensitive semantics as apiToken)
  private val ClientIdKey     = "clientId"
  private val ClientSecretKey = "clientSecret"
  private val AccountIdKey    = "accountId"
  // Override for testing — defaults to the Databricks accounts OIDC base URL
  private[unity] val OidcBaseUrlKey = "oidc.baseUrl"
  private val DefaultOidcBaseUrl    = "https://accounts.cloud.databricks.com"
  // Configurable refresh buffer for OAuth tokens (seconds before expiry to re-exchange)
  // Default: 600s (10 minutes). For short-lived tokens the effective threshold is min(this, expires_in/2).
  private val OAuthRefreshBufferKey        = "spark.indextables.databricks.oauth.refreshBuffer.seconds"
  private val DefaultOAuthRefreshBufferSec = 600
  // Configurable OAuth scope — Databricks currently only supports "all-apis" but may add finer-grained
  // scopes in the future. Bare-leaf key; resolved through ConfigurationResolver with the same
  // prefix-aware, case-insensitive semantics as the other OAuth keys.
  private[unity] val OAuthScopeKey = "oauth.scope"
  private val DefaultOAuthScope    = "all-apis"

  // Default values
  private val DefaultRefreshBufferMinutes = 40
  private val DefaultCacheMaxSize         = 100
  private val DefaultCredentialOperation  = "PATH_READ_WRITE"
  private val DefaultRetryAttempts        = 3
  // Maximum sleep duration when honouring a Retry-After header on 429 responses (60 seconds).
  // Caps runaway values from misconfigured proxies or extremely long server-side back-off windows.
  private val RetryAfterMaxSleepMs        = 60000L

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
   *
   * Auth mode priority:
   *   1. ClientCredentials — when clientId + clientSecret + accountId are all present
   *   2. StaticToken       — when apiToken is present
   *   3. Error             — neither is configured
   */
  private def resolveConfigFromMap(config: Map[String, String]): (String, AuthMode, Int, Int) = {
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

    // Resolve OAuth keys through ConfigurationResolver for the same prefix-aware, case-insensitive,
    // and log-masked semantics as apiToken (bare-leaf keys + spark.indextables.databricks.* prefix).
    val clientId     = ConfigurationResolver.resolveString(ClientIdKey, sources)
    val clientSecret = ConfigurationResolver.resolveString(ClientSecretKey, sources, logMask = true)
    val accountId    = ConfigurationResolver.resolveString(AccountIdKey, sources)
    val oidcBaseUrl  = ConfigurationResolver.resolveString(OidcBaseUrlKey, sources)
                         .getOrElse(DefaultOidcBaseUrl)

    val authMode: AuthMode = (clientId, clientSecret, accountId) match {
      case (Some(id), Some(secret), Some(acct)) if id.nonEmpty && secret.nonEmpty && acct.nonEmpty =>
        val oauthRefreshSec = ConfigurationResolver
          .resolveInt(OAuthRefreshBufferKey, sources, DefaultOAuthRefreshBufferSec).toLong
        val retryAttempts = ConfigurationResolver
          .resolveInt(RetryAttemptsKey, sources, DefaultRetryAttempts)
        val oauthScope = ConfigurationResolver
          .resolveString(OAuthScopeKey, sources)
          .getOrElse(DefaultOAuthScope)
        logger.info(s"Using OAuth client credentials auth (clientId=$id, oidcBaseUrl=$oidcBaseUrl, scope=$oauthScope)")
        ClientCredentials(id, secret, acct, oidcBaseUrl, retryAttempts, oauthRefreshSec, oauthScope)
      case (Some(_), _, _) | (_, Some(_), _) | (_, _, Some(_)) =>
        // Partial OAuth config — give a clear error rather than falling back silently.
        // If you intended to use a static API token, remove all clientId/clientSecret/accountId
        // entries. If you intended OAuth, set all three together.
        throw new IllegalStateException(
          "Incomplete OAuth configuration: spark.indextables.databricks.clientId, " +
            "spark.indextables.databricks.clientSecret, and " +
            "spark.indextables.databricks.accountId must all be set together. " +
            "To use a static API token instead, remove all three OAuth keys and set apiToken."
        )
      case _ =>
        val token = ConfigurationResolver
          .resolveString(TokenKey, sources, logMask = true)
          .getOrElse(
            throw new IllegalStateException(
              "Databricks auth not configured. Set spark.indextables.databricks.apiToken " +
                "or spark.indextables.databricks.clientId/clientSecret/accountId."
            )
          )
        StaticToken(token)
    }

    val refreshBuffer = ConfigurationResolver
      .resolveInt(RefreshBufferKey, sources, DefaultRefreshBufferMinutes)

    val retryAttempts = ConfigurationResolver
      .resolveInt(RetryAttemptsKey, sources, DefaultRetryAttempts)

    (workspaceUrl, authMode, refreshBuffer, retryAttempts)
  }

  /**
   * Resolve the path credential operation from config. Read paths set "PATH_READ"; write paths default to
   * "PATH_READ_WRITE".
   */
  private def resolveCredentialOperation(config: Map[String, String]): String =
    config.getOrElse(CredentialOperationKey, DefaultCredentialOperation)

  // Process-global HTTP client (thread-safe, reusable)
  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(30))
    .build()

  // Process-global JSON parser
  private val objectMapper = io.indextables.spark.util.JsonUtil.mapper

  // Process-global credentials cache
  @volatile private var globalCredentialsCache: Cache[String, CachedCredentials] = _

  // Process-global table info cache (table name → TableInfo with table_id + storage_location)
  // Table info rarely changes, so we use a 1-hour TTL to avoid unnecessary UC API calls.
  @volatile private var globalTableInfoCache: Cache[String, io.indextables.spark.utils.TableInfo] = _

  // Process-global OAuth token cache (clientId → CachedOAuthToken).
  // Keyed by clientId so a single cached token is reused across all URIs for the same identity.
  // We manage expiration ourselves via CachedOAuthToken.expirationTime; the 2-hour safety TTL
  // here prevents stale entries from surviving indefinitely if the process runs for a long time.
  @volatile private[unity] var globalOAuthTokenCache: Cache[String, CachedOAuthToken] = _

  // Per-clientId lock objects for singleflight coordination on OIDC cache misses.
  // On executor fan-out (hundreds of tasks starting simultaneously after token expiry) this ensures
  // only one thread per clientId performs the OIDC exchange; all others block on the lock and then
  // hit the fast path on wake-up, avoiding Databricks OIDC rate-limiting (429) under load.
  private val oauthExchangeLocks = new java.util.concurrent.ConcurrentHashMap[String, AnyRef]()

  private val initLock = new Object

  /** Initialize the process-global credentials and table info caches from Map config. */
  private def initializeGlobalCacheFromMap(config: Map[String, String]): Unit =
    if (globalCredentialsCache == null || globalTableInfoCache == null || globalOAuthTokenCache == null) {
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
        if (globalOAuthTokenCache == null) {
          logger.info("Initializing process-global OAuth token cache: maxSize=50, TTL=2h")
          globalOAuthTokenCache = CacheBuilder
            .newBuilder()
            .maximumSize(50)
            .expireAfterWrite(2, java.util.concurrent.TimeUnit.HOURS)
            .recordStats()
            .build[String, CachedOAuthToken]()
        }
      }
    }

  /**
   * Resolve a bearer token from the configured auth mode.
   *
   *   - StaticToken: returns the configured API token as-is.
   *   - ClientCredentials: checks the process-global OAuth token cache; if missing or near expiry,
   *     POSTs to the Databricks OIDC token endpoint and caches the result.
   *
   * Refresh threshold = min(cc.oauthRefreshBufferSec * 1000, expiresInSeconds * 1000 / 2).
   * The `expires_in / 2` floor ensures we never use more than half the advertised token lifetime,
   * which protects against unusually short-lived tokens (e.g. an OIDC proxy returning 5-minute
   * tokens where a 10-minute fixed buffer would be larger than the token's entire lifetime).
   *
   * Singleflight: on cache miss a per-clientId lock prevents concurrent OIDC exchanges from the
   * same process. Only one thread executes the POST; all others block, then hit the cache on wake-up.
   */
  private[unity] def resolveToken(authMode: AuthMode): String = authMode match {
    case StaticToken(token) => token
    case cc: ClientCredentials =>
      // Fast path (unsynchronized) — avoids lock contention on the common case.
      val cached = if (globalOAuthTokenCache != null) globalOAuthTokenCache.getIfPresent(cc.clientId) else null
      if (cached != null && isOAuthTokenFresh(cached, cc)) {
        logger.debug(s"Using cached OAuth token for clientId=${cc.clientId}")
        return cached.accessToken
      }

      // Cache miss or near-expiry — acquire per-clientId lock to prevent thundering herd.
      val lock = oauthExchangeLocks.computeIfAbsent(cc.clientId, _ => new AnyRef)
      lock.synchronized {
        // Double-check: a concurrent thread may have already refreshed the token while we waited.
        val rechecked = if (globalOAuthTokenCache != null) globalOAuthTokenCache.getIfPresent(cc.clientId) else null
        if (rechecked != null && isOAuthTokenFresh(rechecked, cc)) {
          logger.debug(s"Using cached OAuth token for clientId=${cc.clientId} (double-check hit)")
          return rechecked.accessToken
        }

        logger.info(
          s"Exchanging client credentials for OAuth token (clientId=${cc.clientId}, accountId=${cc.accountId})"
        )
        val tokenUrl = s"${cc.oidcBaseUrl}/oidc/accounts/${cc.accountId}/v1/token"
        val formBody =
          s"grant_type=client_credentials" +
            s"&client_id=${java.net.URLEncoder.encode(cc.clientId, "UTF-8")}" +
            s"&client_secret=${java.net.URLEncoder.encode(cc.clientSecret, "UTF-8")}" +
            s"&scope=${java.net.URLEncoder.encode(cc.oauthScope, "UTF-8")}"

        // Retry with exponential backoff — consistent with fetchCredentials and fetchTableInfoInternal.
        // On HTTP 429 the server-supplied Retry-After header is honoured when present (capped at
        // RetryAfterMaxSleepMs) so we back off at the rate Databricks OIDC requests rather than
        // hammering at our own exponential schedule.
        var lastException: Option[Exception] = None
        for (attempt <- 1 to cc.retryAttempts) {
          scala.util.Try {
            logger.debug(s"OIDC token exchange attempt $attempt/${cc.retryAttempts} for clientId=${cc.clientId}")
            val t0 = System.currentTimeMillis()
            val request = HttpRequest
              .newBuilder()
              .uri(URI.create(tokenUrl))
              .header("Content-Type", "application/x-www-form-urlencoded")
              .timeout(Duration.ofSeconds(30))
              .POST(HttpRequest.BodyPublishers.ofString(formBody))
              .build()

            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            val elapsedMs = System.currentTimeMillis() - t0

            if (response.statusCode() == 429) {
              // Rate-limited: respect Retry-After if the server provides it.
              val retryAfterHeader = Option(response.headers().firstValue("Retry-After").orElse(null))
              val retryAfterMs = retryAfterHeader.flatMap(v => scala.util.Try(v.toLong * 1000L).toOption).getOrElse(0L)
              val sleepMs = if (retryAfterMs > 0) math.min(retryAfterMs, RetryAfterMaxSleepMs)
                            else Math.pow(2, attempt - 1).toLong * 1000
              throw new RateLimitedException(
                s"OIDC rate-limited (429) for clientId=${cc.clientId} — sleeping ${sleepMs}ms", sleepMs
              )
            }

            if (response.statusCode() != 200) {
              throw new RuntimeException(
                s"OAuth token exchange failed for clientId=${cc.clientId} " +
                  s"(${response.statusCode()}): ${response.body()}"
              )
            }

            val root = objectMapper.readTree(response.body())
            val accessToken = Option(root.get("access_token"))
              .filterNot(_.isNull)
              .map(_.asText())
              .getOrElse(
                throw new RuntimeException(
                  s"OAuth token response missing 'access_token' field for clientId=${cc.clientId}: ${response.body()}"
                )
              )
            val expiresIn = Option(root.get("expires_in")).map(_.asLong()).getOrElse(3600L)
            val expiresAt = System.currentTimeMillis() + expiresIn * 1000L

            val cachedToken = CachedOAuthToken(accessToken, expiresIn, expiresAt)
            if (globalOAuthTokenCache != null) globalOAuthTokenCache.put(cc.clientId, cachedToken)

            logger.info(
              s"OAuth token obtained for clientId=${cc.clientId}, expires in ${expiresIn}s, " +
                s"exchange took ${elapsedMs}ms"
            )
            accessToken
          } match {
            case scala.util.Success(token) => return token
            case scala.util.Failure(e: RateLimitedException) =>
              lastException = Some(e)
              if (attempt < cc.retryAttempts) {
                logger.warn(s"OIDC token exchange attempt $attempt rate-limited, sleeping ${e.sleepMs}ms")
                Thread.sleep(e.sleepMs)
              }
            case scala.util.Failure(e: Exception) =>
              lastException = Some(e)
              if (attempt < cc.retryAttempts) {
                val backoffMs = Math.pow(2, attempt - 1).toLong * 1000
                logger.warn(
                  s"OIDC token exchange attempt $attempt failed, retrying in ${backoffMs}ms: ${e.getMessage}"
                )
                Thread.sleep(backoffMs)
              }
            case scala.util.Failure(t) => throw t
          }
        }

        throw new RuntimeException(
          s"Failed to obtain OAuth token for clientId=${cc.clientId} after ${cc.retryAttempts} attempts",
          lastException.orNull
        )
      }
  }

  /**
   * Returns true if the cached OAuth token has sufficient remaining lifetime to be reused.
   *
   * Threshold = min(cc.oauthRefreshBufferSec * 1000, cached.expiresInSeconds * 1000 / 2). The
   * expires_in / 2 floor prevents the configured buffer from exceeding half the token lifetime for
   * unusually short-lived tokens (e.g. a proxy that issues 5-minute tokens where the default 10-minute
   * buffer would be larger than the entire lifetime, making every call trigger a re-exchange).
   */
  private def isOAuthTokenFresh(cached: CachedOAuthToken, cc: ClientCredentials): Boolean = {
    val configuredMs = cc.oauthRefreshBufferSec * 1000L
    val halfLifeMs   = cached.expiresInSeconds * 1000L / 2
    val thresholdMs  = math.min(configuredMs, halfLifeMs)
    System.currentTimeMillis() < cached.expirationTime - thresholdMs
  }

  /**
   * Return a stable identity string for cache-keying purposes.
   *
   * For StaticToken this is the token itself (same behaviour as before).
   * For ClientCredentials this is the clientId, so the AWS credential cache entry survives OAuth
   * token refreshes — the clientId is stable across token exchanges, the access token is not.
   */
  private def authIdentity(authMode: AuthMode): String = authMode match {
    case StaticToken(token)                => token
    case ClientCredentials(clientId, _, _, _, _, _, _) => clientId
  }

  /**
   * Build a cache key that includes token identity and credential operation for multi-user support.
   *
   * Format: tokenHash:credentialOperation:path This ensures different API tokens AND different credential operations
   * (PATH_READ vs PATH_READ_WRITE) get separate cached credentials, preventing read queries from poisoning the write
   * credential cache.
   */
  private def buildCacheKey(
    authIdentity: String,
    credentialOperation: String,
    path: String
  ): String = {
    // Hash the identity to avoid storing the full token/clientId in memory.
    // For StaticToken this is the token; for ClientCredentials this is the clientId,
    // which is stable across OAuth token refreshes.
    val identityHash = Integer.toHexString(authIdentity.hashCode)
    s"$identityHash:$credentialOperation:$path"
  }

  /** Log cache statistics for monitoring. */
  private def logCacheStats(): Unit = {
    if (globalCredentialsCache != null) {
      val stats = globalCredentialsCache.stats()
      if (stats.requestCount() % 100 == 0 && stats.requestCount() > 0) {
        logger.info(
          s"Credentials cache stats: hits=${stats.hitCount()}, misses=${stats.missCount()}, " +
            s"hitRate=${f"${stats.hitRate() * 100}%.2f"}%%, size=${globalCredentialsCache.size()}"
        )
      }
    }
    if (globalOAuthTokenCache != null) {
      val stats = globalOAuthTokenCache.stats()
      if (stats.requestCount() % 100 == 0 && stats.requestCount() > 0) {
        logger.info(
          s"OAuth token cache stats: hits=${stats.hitCount()}, misses=${stats.missCount()}, " +
            s"hitRate=${f"${stats.hitRate() * 100}%.2f"}%%, size=${globalOAuthTokenCache.size()}"
        )
      }
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
    if (globalOAuthTokenCache != null) {
      logger.info("Clearing process-global OAuth token cache")
      globalOAuthTokenCache.invalidateAll()
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
      if (globalOAuthTokenCache != null) {
        globalOAuthTokenCache.invalidateAll()
        globalOAuthTokenCache = null
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
    retryAttempts: Int,
    authMode: AuthMode
  ): io.indextables.spark.utils.TableInfo = {
    // Key by authIdentity (clientId for OAuth, token for static) so the tableInfo cache entry
    // survives OAuth token rotation — consistent with the AWS credential cache key.
    val cacheKey = s"${Integer.toHexString(authIdentity(authMode).hashCode)}:tableinfo:$fullTableName"

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
    val (workspaceUrl, authMode, _, retryAttempts) = resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)
    fetchTableInfoInternal(fullTableName, workspaceUrl, resolveToken(authMode), retryAttempts, authMode).tableId
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
    val (workspaceUrl, authMode, _, retryAttempts) = resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)
    fetchTableInfoInternal(fullTableName, workspaceUrl, resolveToken(authMode), retryAttempts, authMode)
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
    val (workspaceUrl, authMode, refreshBufferMinutes, retryAttempts) =
      resolveConfigFromMap(config)
    initializeGlobalCacheFromMap(config)

    val token     = resolveToken(authMode)
    val cacheKey  = s"${Integer.toHexString(authIdentity(authMode).hashCode)}:table:$tableId"

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

    // Fetch READ credentials directly (we never write via table credentials)
    val creds = fetchTableCredentials(tableId, "READ", workspaceUrl, token, retryAttempts)

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
    val (workspaceUrl, authMode, _, _) = resolveConfigFromMap(config)
    val token                          = resolveToken(authMode)

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

  /** Authentication mode — either a static API token or OAuth client credentials. */
  private[unity] sealed trait AuthMode
  private[unity] case class StaticToken(token: String) extends AuthMode
  private[unity] case class ClientCredentials(
    clientId: String,
    clientSecret: String,
    accountId: String,
    oidcBaseUrl: String         = "https://accounts.cloud.databricks.com",
    retryAttempts: Int          = 3,
    oauthRefreshBufferSec: Long = 600L,
    oauthScope: String          = "all-apis"
  ) extends AuthMode

  /** Signals an HTTP 429 response with a pre-computed sleep duration (Retry-After or exponential). */
  private class RateLimitedException(message: String, val sleepMs: Long) extends Exception(message)

  /**
   * Cached OAuth access token with expiration tracking.
   *
   * @param expiresInSeconds
   *   The `expires_in` value from the OIDC response (seconds). Stored so the refresh threshold can
   *   be computed as min(configuredBufferMs, expiresInSeconds * 1000 / 2) on each freshness check,
   *   preventing the configured buffer from exceeding half the token's advertised lifetime.
   */
  private[unity] case class CachedOAuthToken(accessToken: String, expiresInSeconds: Long, expirationTime: Long)

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
