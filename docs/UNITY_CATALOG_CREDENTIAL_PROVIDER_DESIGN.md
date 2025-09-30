# Unity Catalog Credential Provider Design

## Overview
A custom AWS credential provider that integrates with Databricks Unity Catalog to obtain temporary AWS credentials for accessing cloud storage paths. The provider uses the Databricks Workspace Client to request credentials via the TemporaryPathCredentials API with intelligent caching and fallback mechanisms.

## Architecture

### Process-Global Design
The provider uses **process-global static variables** for the WorkspaceClient and credentials cache, ensuring:
- **Single connection** to Databricks workspace per JVM process
- **Shared cache** across all provider instances for maximum efficiency
- **Reduced memory footprint** - only one cache and client per process
- **Consistent credentials** - all threads see the same cached credentials
- **Automatic environment inheritance** - credentials discovered from Databricks runtime

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    UnityCredentialProvider                   │
│                                                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────┐ │
│  │ Constructor     │  │ Workspace Client │  │   Guava    │ │
│  │ (URI, Config)   │──│    Factory       │  │   Cache    │ │
│  └─────────────────┘  └──────────────────┘  └────────────┘ │
│           │                    │                     │       │
│           ▼                    ▼                     ▼       │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────┐ │
│  │ Path Extraction │  │ Credentials API  │  │  20 min    │ │
│  │   & Parsing     │  │    Client        │  │    TTL     │ │
│  └─────────────────┘  └──────────────────┘  └────────────┘ │
│           │                    │                     │       │
│           ▼                    ▼                     ▼       │
│  ┌─────────────────────────────────────────────────────┐    │
│  │           Credential Resolution Strategy             │    │
│  │  1. Try READ_WRITE                                  │    │
│  │  2. Fallback to READ                                │    │
│  │  3. Return credentials or throw exception           │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Design

### 1. Class Structure

```java
package com.tantivy4spark.auth.unity;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class UnityCredentialProvider implements AWSCredentialsProvider {
    // Process-global static variables for sharing across all instances
    private static volatile WorkspaceClient globalWorkspaceClient;
    private static volatile Cache<String, CachedCredentials> globalCredentialsCache;
    private static final Object INIT_LOCK = new Object();

    // Instance variables
    private final URI uri;
    private final Configuration conf;

    // Constructor matching requirement
    public UnityCredentialProvider(URI uri, Configuration conf) {
        this.uri = uri;
        this.conf = conf;
        // Initialize process-global resources only once
        initializeGlobalResources(conf);
    }
}
```

### 2. Key Methods

#### 2.1 Workspace Client Initialization
```java
private static WorkspaceClient initializeWorkspaceClient() {
    // Use default constructor - automatically discovers credentials from environment
    // The WorkspaceClient will look for credentials in the following order:
    // 1. DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
    // 2. DEFAULT profile in ~/.databrickscfg
    // 3. Databricks CLI configuration
    // 4. Databricks runtime environment (when running on Databricks)

    WorkspaceClient client = new WorkspaceClient();
    return client;
}
```

#### 2.2 Cache Initialization
```java
private static Cache<String, CachedCredentials> initializeCache(Configuration conf) {
    int ttlMinutes = conf.getInt(CACHE_TTL_KEY, DEFAULT_CACHE_TTL_MINUTES);
    int maxSize = conf.getInt(CACHE_MAX_SIZE_KEY, DEFAULT_CACHE_MAX_SIZE);

    return CacheBuilder.newBuilder()
        .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)  // Configurable TTL
        .maximumSize(maxSize)  // Configurable cache size
        .recordStats()        // Enable statistics
        .build();
}

// Process-global initialization (called once)
private static void initializeGlobalResources(Configuration conf) {
    if (globalWorkspaceClient == null || globalCredentialsCache == null) {
        synchronized (INIT_LOCK) {
            if (globalWorkspaceClient == null) {
                globalWorkspaceClient = initializeWorkspaceClient();
            }
            if (globalCredentialsCache == null) {
                globalCredentialsCache = initializeCache(conf);
            }
        }
    }
}
```

#### 2.3 Credential Resolution Logic
```java
@Override
public AWSCredentials getCredentials() {
    String path = extractPath(uri);
    String cacheKey = buildCacheKey(path);

    try {
        return globalCredentialsCache.get(cacheKey, () -> {
            // Try READ_WRITE first
            try {
                return fetchCredentials(path, "READ_WRITE");
            } catch (Exception e) {
                // Fallback to READ
                logger.info("READ_WRITE credentials unavailable, falling back to READ");
                return fetchCredentials(path, "READ");
            }
        });
    } catch (Exception e) {
        throw new RuntimeException("Failed to obtain Unity Catalog credentials", e);
    }
}
```

#### 2.4 Fetch Credentials from Unity Catalog
```java
private CachedCredentials fetchCredentials(String path, String operation) {
    GenerateTemporaryPathCredentialsRequest request =
        new GenerateTemporaryPathCredentialsRequest()
            .setPath(path)
            .setOperation(operation)
            .setExpirationTime(System.currentTimeMillis() + 3600000); // 1 hour

    GenerateTemporaryPathCredentialsResponse response =
        workspaceClient.temporaryPathCredentials()
            .generateTemporaryPathCredentials(request);

    AwsCredentials awsCreds = response.getAwsCredentials();

    return new CachedCredentials(
        awsCreds.getAccessKeyId(),
        awsCreds.getSecretAccessKey(),
        awsCreds.getSessionToken(),
        System.currentTimeMillis()
    );
}
```

### 3. Cache Key Strategy

```java
private String buildCacheKey(String path) {
    // Include user context for multi-tenancy
    String userId = getCurrentUserId();
    String operation = determineOperation();
    return String.format("%s:%s:%s", userId, path, operation);
}

private String getCurrentUserId() {
    // Get from Spark context or environment
    return conf.get("spark.databricks.user.email",
                   System.getenv("DATABRICKS_USER_EMAIL"));
}
```

### 4. Process-Global Utility Methods

```java
// Clear all cached credentials (affects all instances)
public static void clearCache() {
    if (globalCredentialsCache != null) {
        globalCredentialsCache.invalidateAll();
    }
}

// Get cache statistics for monitoring
public static CacheStats getCacheStatistics() {
    if (globalCredentialsCache != null) {
        return globalCredentialsCache.stats();
    }
    return null;
}

// Shutdown and cleanup all resources (call on application shutdown)
public static void shutdown() {
    synchronized (INIT_LOCK) {
        if (globalCredentialsCache != null) {
            globalCredentialsCache.invalidateAll();
            globalCredentialsCache = null;
        }
        globalWorkspaceClient = null;
    }
}
```

### 5. Error Handling & Logging

```java
private static final Logger logger = LoggerFactory.getLogger(UnityCredentialProvider.class);

private void logCredentialRequest(String path, String operation, boolean success) {
    if (success) {
        logger.info("Successfully obtained {} credentials for path: {}", operation, path);
    } else {
        logger.warn("Failed to obtain {} credentials for path: {}", operation, path);
    }
}
```

## Maven Dependencies

```xml
<!-- Databricks SDK - marked as provided -->
<dependency>
    <groupId>com.databricks</groupId>
    <artifactId>databricks-sdk-java</artifactId>
    <version>0.20.0</version>
    <scope>provided</scope>
</dependency>

<!-- AWS SDK v1 for compatibility - marked as provided -->
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>aws-java-sdk-core</artifactId>
    <version>1.12.500</version>
    <scope>provided</scope>
</dependency>

<!-- Guava for caching - included in runtime -->
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>32.1.2-jre</version>
</dependency>

<!-- Logging -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.36</version>
    <scope>provided</scope>
</dependency>
```

## Configuration Properties

### Required Configuration
- `spark.indextables.aws.credentialsProviderClass` - Set to `com.tantivy4spark.auth.unity.UnityCredentialProvider`

### Databricks Authentication
The provider automatically inherits Databricks credentials from the environment in the following order:
1. **Environment variables**: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
2. **Configuration file**: `~/.databrickscfg` (DEFAULT profile)
3. **Databricks CLI**: Uses CLI configuration if available
4. **Databricks runtime**: Automatically uses runtime credentials when running on Databricks

### Optional Configuration
- `spark.indextables.unity.cache.ttl.minutes` - Cache TTL (default: 20)
- `spark.indextables.unity.cache.maxSize` - Max cache entries (default: 100)
- `spark.indextables.unity.fallback.enabled` - Enable READ fallback (default: true)
- `spark.indextables.unity.retry.attempts` - API retry attempts (default: 3)

## Usage Example

```scala
// Spark configuration (Databricks credentials inherited from environment)
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
               "com.tantivy4spark.auth.unity.UnityCredentialProvider")

// Optional: Configure cache settings
spark.conf.set("spark.indextables.unity.cache.ttl.minutes", "30")
spark.conf.set("spark.indextables.unity.cache.maxSize", "200")

// Use with Tantivy4Spark
df.write.format("tantivy4spark")
  .save("s3://my-unity-catalog-bucket/path")

// Monitor cache performance (in driver code)
import com.tantivy4spark.auth.unity.UnityCredentialProvider
val stats = UnityCredentialProvider.getCacheStatistics()
println(s"Cache hit rate: ${stats.hitRate()}")
```

## Security Considerations

### 1. Token Management
- Never hardcode tokens
- Use Databricks secrets or environment variables
- Implement token rotation policies

### 2. Permission Boundaries
- Provider respects Unity Catalog ACLs
- Automatic fallback from READ_WRITE to READ ensures least privilege
- Credentials are scoped to specific paths

### 3. Audit Trail
- All credential requests are logged
- Cache hit/miss statistics available
- Integration with Databricks audit logs

## Performance Optimizations

### 1. Caching Strategy
- 20-minute TTL balances security and performance
- Per-user, per-path caching prevents cross-contamination
- Cache warm-up on provider initialization

### 2. Connection Pooling
- Reuse WorkspaceClient connections
- Implement retry with exponential backoff
- Circuit breaker for API failures

### 3. Async Credential Refresh
- Background refresh before expiry
- Non-blocking credential resolution
- Graceful degradation on refresh failure

## Testing Strategy

### 1. Unit Tests
```java
@Test
public void testReadWriteFallbackToRead() {
    // Mock workspace client to fail on READ_WRITE
    // Verify fallback to READ succeeds
}

@Test
public void testCacheExpiry() {
    // Verify credentials refresh after 20 minutes
}

@Test
public void testMultiUserCaching() {
    // Verify cache isolation between users
}
```

### 2. Integration Tests
```scala
test("Unity Catalog credential provider integration") {
  // Test with actual Databricks workspace
  // Verify S3 access with obtained credentials
  // Test permission boundaries
}
```

## Deployment Considerations

### 1. JAR Packaging
- Shade Guava dependencies to avoid conflicts
- Exclude provided dependencies from fat JAR
- Include META-INF services for provider discovery

### 2. Cluster Configuration
```bash
# Init script to set environment variables
echo "export DATABRICKS_HOST='$DB_HOST'" >> /databricks/spark/conf/spark-env.sh
echo "export DATABRICKS_TOKEN='$DB_TOKEN'" >> /databricks/spark/conf/spark-env.sh
```

### 3. Monitoring
- Track cache hit rates
- Monitor API latencies
- Alert on authentication failures

## Error Scenarios

### 1. No Available Credentials
```
ERROR: Unable to obtain credentials for path s3://bucket/path
- READ_WRITE: Permission denied
- READ: Permission denied
Action: Check Unity Catalog permissions for current user
```

### 2. API Timeout
```
WARN: Databricks API timeout after 3 attempts
Action: Using cached credentials if available, otherwise fail
```

### 3. Invalid Path
```
ERROR: Path not managed by Unity Catalog: s3://external-bucket/
Action: Fall back to default credential chain
```

## Future Enhancements

1. **Multi-cloud Support**: Extend to Azure and GCP
2. **Credential Pre-warming**: Proactive credential refresh
3. **Metrics Integration**: Export to Prometheus/Datadog
4. **Role Assumption**: Support for cross-account access
5. **Fine-grained Caching**: Path prefix-based cache strategies