# Cloud Configuration

S3, Azure, and Unity Catalog configuration for IndexTables4Spark.

---

## S3 Configuration

```scala
// Basic auth
spark.indextables.aws.accessKey: <key>
spark.indextables.aws.secretKey: <secret>

// Custom credential provider
spark.indextables.aws.credentialsProviderClass: "com.example.MyProvider"

// Upload performance
spark.indextables.s3.maxConcurrency: 4 (parallel uploads)
spark.indextables.s3.partSize: "64M"
```

---

## Azure Configuration

```scala
// Account key auth
spark.indextables.azure.accountName: <account>
spark.indextables.azure.accountKey: <key>

// OAuth Service Principal
spark.indextables.azure.tenantId: <tenant>
spark.indextables.azure.clientId: <client>
spark.indextables.azure.clientSecret: <secret>

// Supports: abfss://, wasbs://, abfs:// URLs
// Uses ~/.azure/credentials file or environment variables
```

### Basic Azure Write/Read
```scala
// Session-level config
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.accountKey", "your-account-key")

// Write (supports abfss://, wasbs://, abfs://)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("abfss://container/path")

// Read
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("abfss://container/path")
```

### Azure OAuth (Service Principal)
```scala
spark.conf.set("spark.indextables.azure.accountName", "account")
spark.conf.set("spark.indextables.azure.tenantId", "tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "client-secret")

df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("abfss://container@account.dfs.core.windows.net/path")
```

---

## Unity Catalog Integration (Databricks)

Access Unity Catalog-managed S3 paths using temporary credentials from the Databricks API.

**Provider class:** `io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider`

### Configuration Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `spark.indextables.databricks.workspaceUrl` | Yes | - | Databricks workspace URL |
| `spark.indextables.databricks.apiToken` | Yes | - | Databricks API token (PAT or OAuth) |
| `spark.indextables.databricks.credential.refreshBuffer.minutes` | No | 40 | Minutes before expiration to refresh |
| `spark.indextables.databricks.cache.maxSize` | No | 100 | Maximum cached credential entries |
| `spark.indextables.databricks.fallback.enabled` | No | true | Fallback to READ if READ_WRITE fails |
| `spark.indextables.databricks.retry.attempts` | No | 3 | Retry attempts on API failure |

### Session-Level Configuration
```scala
spark.conf.set("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
spark.conf.set("spark.indextables.databricks.apiToken", sys.env("DATABRICKS_TOKEN"))
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
  "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")

// Read from Unity Catalog-managed path
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://unity-catalog-bucket/path")

// Write to Unity Catalog-managed path
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://unity-catalog-bucket/new-path")
```

### Per-Operation Configuration
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
  .option("spark.indextables.databricks.apiToken", token)
  .option("spark.indextables.aws.credentialsProviderClass",
    "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")
  .save("s3://bucket/path")
```

### Key Features
- **Driver-side resolution:** Credentials resolved on driver, executors use pre-resolved credentials directly (no network calls to Databricks from workers)
- **Multi-user caching:** Credentials cached per API token + path
- **Automatic fallback:** If READ_WRITE fails (403), automatically falls back to READ credentials
- **Expiration-aware:** Refreshes credentials 40 minutes before expiration (configurable)
- **No SDK dependency:** Uses HTTP API directly, no Databricks SDK required
- **Credential priority:** Explicit credentials always take precedence over provider class
