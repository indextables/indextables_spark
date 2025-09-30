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

package com.tantivy4spark.auth.unity;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.GenerateTemporaryPathCredentialsRequest;
import com.databricks.sdk.service.catalog.GenerateTemporaryPathCredentialsResponse;
import com.databricks.sdk.service.catalog.AwsCredentials;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * AWS Credential Provider that integrates with Databricks Unity Catalog.
 *
 * This provider fetches temporary AWS credentials from Unity Catalog for accessing
 * cloud storage paths. It implements intelligent caching with a 20-minute TTL and
 * automatic fallback from READ_WRITE to READ permissions.
 *
 * Usage:
 * spark.conf.set("spark.indextables.aws.credentialsProviderClass",
 *                "com.tantivy4spark.auth.unity.UnityCredentialProvider")
 */
public class UnityCredentialProvider implements AWSCredentialsProvider {
    private static final Logger logger = LoggerFactory.getLogger(UnityCredentialProvider.class);

    // Configuration keys
    private static final String WORKSPACE_HOST_KEY = "spark.databricks.workspace.host";
    private static final String TOKEN_KEY = "spark.databricks.token";
    private static final String USER_EMAIL_KEY = "spark.databricks.user.email";
    private static final String CACHE_TTL_KEY = "spark.indextables.unity.cache.ttl.minutes";
    private static final String CACHE_MAX_SIZE_KEY = "spark.indextables.unity.cache.maxSize";
    private static final String FALLBACK_ENABLED_KEY = "spark.indextables.unity.fallback.enabled";
    private static final String RETRY_ATTEMPTS_KEY = "spark.indextables.unity.retry.attempts";

    // Default values
    private static final int DEFAULT_CACHE_TTL_MINUTES = 20;
    private static final int DEFAULT_CACHE_MAX_SIZE = 100;
    private static final boolean DEFAULT_FALLBACK_ENABLED = true;
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private static final long CREDENTIAL_EXPIRY_MILLIS = 3600000; // 1 hour

    // Process-global static variables for sharing across all instances
    private static volatile WorkspaceClient globalWorkspaceClient;
    private static volatile Cache<String, CachedCredentials> globalCredentialsCache;
    private static final Object INIT_LOCK = new Object();

    // Instance variables
    private final URI uri;
    private final Configuration conf;
    private final boolean fallbackEnabled;
    private final int retryAttempts;

    /**
     * Constructor required by the credential provider interface.
     *
     * @param uri The URI of the resource being accessed
     * @param conf Hadoop configuration containing Databricks settings
     */
    public UnityCredentialProvider(URI uri, Configuration conf) {
        this.uri = uri;
        this.conf = conf;
        this.fallbackEnabled = conf.getBoolean(FALLBACK_ENABLED_KEY, DEFAULT_FALLBACK_ENABLED);
        this.retryAttempts = conf.getInt(RETRY_ATTEMPTS_KEY, DEFAULT_RETRY_ATTEMPTS);

        logger.info("Initializing UnityCredentialProvider for URI: {}", uri);

        // Initialize process-global resources only once
        initializeGlobalResources(conf);

        logger.info("UnityCredentialProvider initialized successfully");
    }

    /**
     * Initialize process-global resources (workspace client and cache) only once.
     * This ensures all instances share the same client and cache for efficiency.
     */
    private static void initializeGlobalResources(Configuration conf) {
        if (globalWorkspaceClient == null || globalCredentialsCache == null) {
            synchronized (INIT_LOCK) {
                // Double-check locking pattern
                if (globalWorkspaceClient == null) {
                    logger.info("Initializing process-global WorkspaceClient");
                    globalWorkspaceClient = initializeWorkspaceClient();
                }
                if (globalCredentialsCache == null) {
                    logger.info("Initializing process-global credentials cache");
                    globalCredentialsCache = initializeCache(conf);
                }
            }
        }
    }

    /**
     * Initialize the Databricks Workspace Client.
     * Uses the default constructor which automatically inherits credentials from the environment.
     *
     * The WorkspaceClient will look for credentials in the following order:
     * 1. DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
     * 2. DEFAULT profile in ~/.databrickscfg
     * 3. Databricks CLI configuration
     * 4. Databricks runtime environment (when running on Databricks)
     */
    private static WorkspaceClient initializeWorkspaceClient() {
        logger.info("Initializing WorkspaceClient with default configuration (inherits from environment)");

        try {
            // Use default constructor - automatically discovers credentials from environment
            WorkspaceClient client = new WorkspaceClient();

            logger.info("WorkspaceClient initialized successfully");
            return client;
        } catch (Exception e) {
            logger.error("Failed to initialize WorkspaceClient. Ensure Databricks credentials are configured in environment", e);
            throw new IllegalStateException(
                "Failed to initialize WorkspaceClient. Credentials should be available via: " +
                "1) DATABRICKS_HOST/DATABRICKS_TOKEN environment variables, " +
                "2) ~/.databrickscfg file, " +
                "3) Databricks CLI configuration, or " +
                "4) Databricks runtime environment",
                e
            );
        }
    }

    /**
     * Initialize the credentials cache with configured TTL and size limits.
     */
    private static Cache<String, CachedCredentials> initializeCache(Configuration conf) {
        int ttlMinutes = conf.getInt(CACHE_TTL_KEY, DEFAULT_CACHE_TTL_MINUTES);
        int maxSize = conf.getInt(CACHE_MAX_SIZE_KEY, DEFAULT_CACHE_MAX_SIZE);

        logger.info("Initializing credentials cache: TTL={} minutes, maxSize={}", ttlMinutes, maxSize);

        return CacheBuilder.newBuilder()
            .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
            .maximumSize(maxSize)
            .recordStats()
            .build();
    }

    /**
     * Get AWS credentials for the configured URI.
     *
     * This method:
     * 1. Checks the cache for valid credentials
     * 2. If not cached, requests READ_WRITE credentials
     * 3. Falls back to READ credentials if READ_WRITE fails
     * 4. Caches the credentials with a 20-minute TTL
     */
    @Override
    public AWSCredentials getCredentials() {
        String path = extractPath(uri);
        String cacheKey = buildCacheKey(path);

        logger.debug("Getting credentials for path: {}", path);

        try {
            CachedCredentials cached = globalCredentialsCache.get(cacheKey, new Callable<CachedCredentials>() {
                @Override
                public CachedCredentials call() throws Exception {
                    return fetchCredentialsWithFallback(path);
                }
            });

            logCacheStats();

            if (cached.sessionToken != null) {
                return new BasicSessionCredentials(
                    cached.accessKeyId,
                    cached.secretAccessKey,
                    cached.sessionToken
                );
            } else {
                return new BasicAWSCredentials(
                    cached.accessKeyId,
                    cached.secretAccessKey
                );
            }

        } catch (ExecutionException e) {
            logger.error("Failed to obtain Unity Catalog credentials for path: {}", path, e);
            throw new RuntimeException("Failed to obtain Unity Catalog credentials", e.getCause());
        }
    }

    /**
     * Refresh credentials (forced refresh, bypassing cache).
     */
    @Override
    public void refresh() {
        String path = extractPath(uri);
        String cacheKey = buildCacheKey(path);

        logger.info("Refreshing credentials for path: {}", path);

        // Invalidate cached entry
        globalCredentialsCache.invalidate(cacheKey);

        // Pre-fetch new credentials
        try {
            getCredentials();
        } catch (Exception e) {
            logger.warn("Failed to pre-fetch credentials during refresh", e);
        }
    }

    /**
     * Fetch credentials with automatic fallback from READ_WRITE to READ.
     */
    private CachedCredentials fetchCredentialsWithFallback(String path) throws Exception {
        // Try READ_WRITE first
        try {
            logger.debug("Attempting to fetch READ_WRITE credentials for path: {}", path);
            CachedCredentials creds = fetchCredentials(path, "READ_WRITE");
            logger.info("Successfully obtained READ_WRITE credentials for path: {}", path);
            return creds;
        } catch (Exception e) {
            if (!fallbackEnabled) {
                throw e;
            }

            logger.info("READ_WRITE credentials unavailable for path: {}, falling back to READ", path);

            // Fallback to READ
            try {
                CachedCredentials creds = fetchCredentials(path, "READ");
                logger.info("Successfully obtained READ credentials for path: {}", path);
                return creds;
            } catch (Exception readException) {
                logger.error("Failed to obtain both READ_WRITE and READ credentials for path: {}", path);
                throw new RuntimeException(
                    "Unable to obtain any credentials from Unity Catalog for path: " + path,
                    readException
                );
            }
        }
    }

    /**
     * Fetch credentials from Unity Catalog API.
     */
    private CachedCredentials fetchCredentials(String path, String operation) throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= retryAttempts; attempt++) {
            try {
                logger.debug("Fetching {} credentials for path: {} (attempt {}/{})",
                           operation, path, attempt, retryAttempts);

                GenerateTemporaryPathCredentialsRequest request =
                    new GenerateTemporaryPathCredentialsRequest()
                        .setPath(path)
                        .setOperation(operation)
                        .setExpirationTime(System.currentTimeMillis() + CREDENTIAL_EXPIRY_MILLIS);

                GenerateTemporaryPathCredentialsResponse response =
                    globalWorkspaceClient.temporaryPathCredentials()
                        .generateTemporaryPathCredentials(request);

                AwsCredentials awsCreds = response.getAwsCredentials();

                return new CachedCredentials(
                    awsCreds.getAccessKeyId(),
                    awsCreds.getSecretAccessKey(),
                    awsCreds.getSessionToken(),
                    System.currentTimeMillis()
                );

            } catch (Exception e) {
                lastException = e;
                if (attempt < retryAttempts) {
                    long backoffMillis = (long) Math.pow(2, attempt - 1) * 1000;
                    logger.warn("Attempt {} failed, retrying in {} ms", attempt, backoffMillis, e);
                    Thread.sleep(backoffMillis);
                }
            }
        }

        throw new Exception("Failed to fetch " + operation + " credentials after " + retryAttempts + " attempts", lastException);
    }

    /**
     * Extract the path from the URI.
     */
    private String extractPath(URI uri) {
        // Convert URI to Unity Catalog compatible path
        String scheme = uri.getScheme();
        String host = uri.getHost();
        String path = uri.getPath();

        if (scheme == null || host == null) {
            throw new IllegalArgumentException("Invalid URI for Unity Catalog: " + uri);
        }

        // Construct full path (e.g., s3://bucket/path)
        String fullPath = scheme + "://" + host + path;

        // Remove trailing slashes
        if (fullPath.endsWith("/")) {
            fullPath = fullPath.substring(0, fullPath.length() - 1);
        }

        return fullPath;
    }

    /**
     * Build a cache key that includes user context.
     */
    private String buildCacheKey(String path) {
        String userId = getCurrentUserId();
        // Cache key format: userId:path
        // We don't include operation because we handle fallback internally
        return userId + ":" + path;
    }

    /**
     * Get the current user ID for cache isolation.
     */
    private String getCurrentUserId() {
        String userId = conf.get(USER_EMAIL_KEY);
        if (userId == null) {
            userId = System.getenv("DATABRICKS_USER_EMAIL");
        }
        if (userId == null) {
            userId = System.getProperty("user.name", "unknown");
        }
        return userId;
    }

    /**
     * Log cache statistics for monitoring.
     */
    private static void logCacheStats() {
        CacheStats stats = globalCredentialsCache.stats();
        if (stats.requestCount() % 100 == 0) { // Log every 100 requests
            logger.info("Credentials cache stats: hits={}, misses={}, hitRate={}, size={}",
                       stats.hitCount(), stats.missCount(),
                       String.format("%.2f%%", stats.hitRate() * 100),
                       globalCredentialsCache.size());
        }
    }

    /**
     * Clear all cached credentials.
     * This is a process-global operation that affects all instances.
     */
    public static void clearCache() {
        if (globalCredentialsCache != null) {
            logger.info("Clearing process-global credentials cache");
            globalCredentialsCache.invalidateAll();
        }
    }

    /**
     * Get current cache statistics.
     * Useful for monitoring and debugging.
     */
    public static CacheStats getCacheStatistics() {
        if (globalCredentialsCache != null) {
            return globalCredentialsCache.stats();
        }
        return null;
    }

    /**
     * Shutdown and cleanup all process-global resources.
     * Should be called when the application is shutting down.
     */
    public static void shutdown() {
        synchronized (INIT_LOCK) {
            logger.info("Shutting down UnityCredentialProvider global resources");

            if (globalCredentialsCache != null) {
                globalCredentialsCache.invalidateAll();
                globalCredentialsCache = null;
            }

            if (globalWorkspaceClient != null) {
                try {
                    // Close workspace client if it has a close method
                    // Note: WorkspaceClient may not have a close method, check SDK documentation
                    globalWorkspaceClient = null;
                } catch (Exception e) {
                    logger.warn("Error closing workspace client", e);
                }
            }
        }
    }

    /**
     * Internal class for cached credentials.
     */
    private static class CachedCredentials {
        final String accessKeyId;
        final String secretAccessKey;
        final String sessionToken;
        final long timestamp;

        CachedCredentials(String accessKeyId, String secretAccessKey,
                         String sessionToken, long timestamp) {
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
            this.sessionToken = sessionToken;
            this.timestamp = timestamp;
        }
    }

    /**
     * Basic AWS Credentials implementation for non-session credentials.
     */
    private static class BasicAWSCredentials implements AWSCredentials {
        private final String accessKeyId;
        private final String secretAccessKey;

        BasicAWSCredentials(String accessKeyId, String secretAccessKey) {
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
        }

        @Override
        public String getAWSAccessKeyId() {
            return accessKeyId;
        }

        @Override
        public String getAWSSecretKey() {
            return secretAccessKey;
        }
    }
}