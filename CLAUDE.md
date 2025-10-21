# CLAUDE.md

**IndexTables4Spark** is a high-performance Spark DataSource implementing fast full-text search using Tantivy via tantivy4java. It runs embedded in Spark executors without server-side components.

## Note
- This is the only way to run a single test for this project:  mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.DateStringFilterValidationTest'

## Key Features
- **Split-based architecture**: Write-only indexes with QuickwitSplit format
- **Transaction log**: Delta Lake-style with atomic operations, high-performance compaction, and GZIP compression
- **Transaction log compaction**: Automatic checkpoint creation with parallel S3 retrieval for scalable performance
- **Transaction log compression**: GZIP compression for 60-70% storage reduction and faster S3 downloads
- **Aggregate pushdown**: Complete support for COUNT(), SUM(), AVG(), MIN(), MAX() aggregations with transaction log optimization and auto-fast-field configuration
- **Partitioned datasets**: Full support for partitioned tables with partition pruning and WHERE clauses
- **Direct merge operations**: In-process merge architecture for efficient split consolidation
- **Merge splits optimization**: SQL-based split consolidation with intelligent bin packing, configurable limits, partition-aware operations, and robust skipped files handling
- **Broadcast locality management**: Cluster-wide cache locality tracking for optimal task scheduling
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'` and `_indexall indexquery 'query'`)
- **Optimized writes**: Automatic split sizing with adaptive shuffle
- **Auto-sizing**: Intelligent DataFrame partitioning based on historical split analysis with 28/28 tests passing
- **V1/V2 DataSource compatibility**: Both legacy and modern Spark DataSource APIs fully supported (V2 recommended for partition column indexing)
- **Multi-cloud storage**: Full support for S3 and Azure Blob Storage with native authentication and high-performance operations
- **Working directory configuration**: Custom root working areas for index creation and split operations
- **Parallel upload performance**: Multi-threaded S3 uploads with configurable concurrency and memory-efficient streaming
- **Schema-aware filtering**: Field validation prevents native crashes and ensures compatibility with unified data skipping across all scan types
- **Statistics truncation**: Automatic transaction log optimization by dropping min/max stats for long values (>256 chars), preventing bloat while maintaining query correctness
- **High-performance I/O**: Parallel transaction log reading with configurable concurrency and retry policies
- **Enterprise-grade configurability**: Comprehensive configuration hierarchy with validation and fallback mechanisms
- **100% test coverage**: 228+ tests passing, 0 failing, comprehensive partitioned dataset test suite, aggregate pushdown validation, schema-based IndexQuery cache, custom credential provider integration tests, statistics truncation validation, and transaction log compression tests

## Build & Test
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # Set Java 11
mvn clean compile  # Build
mvn test          # Run tests
```

## Configuration

### Core Settings
Key settings with defaults:
- `spark.indextables.indexWriter.heapSize`: `100000000` (100MB, supports human-readable formats like "2G", "500M", "1024K")
- `spark.indextables.indexWriter.batchSize`: `10000` documents
- `spark.indextables.indexWriter.threads`: `2`
- `spark.indextables.cache.maxSize`: `200000000` (200MB)
- `spark.indextables.cache.prewarm.enabled`: `false` (Enable proactive cache warming)
- `spark.indextables.docBatch.enabled`: `true` (Enable batch document retrieval for better performance)
- `spark.indextables.docBatch.maxSize`: `1000` (Maximum documents per batch)
- `spark.indextables.optimizeWrite.targetRecordsPerSplit`: `1000000`

### Data Skipping Statistics Configuration

**New in v1.14**: Automatic truncation of min/max statistics for columns with excessively long values to prevent transaction log bloat.

#### Statistics Truncation Settings
- `spark.indextables.stats.truncation.enabled`: `true` (Enable automatic stats truncation - enabled by default)
- `spark.indextables.stats.truncation.maxLength`: `256` (Maximum character length for min/max values)

#### Benefits
- **98% transaction log size reduction** for tables with long text fields
- **10-100x faster transaction log reads** with smaller files
- **Reduced memory footprint** for transaction log caching
- **Faster checkpoint creation** with compact statistics
- **No data loss**: Only statistics affected, all data remains readable and queryable

#### Behavior
- **Enabled by default** for optimal performance
- **Columns with short values**: Statistics preserved, data skipping works normally
- **Columns with long values**: Statistics dropped, falls back to Spark filtering
- **Conservative approach**: Statistics completely dropped (not truncated) to avoid misleading partial data
- **Threshold**: Values exceeding 256 characters have their min/max statistics dropped
- **Fallback logic**: Data skipping gracefully handles missing statistics by conservatively including all files

#### Configuration Examples

```scala
// Default behavior (recommended) - automatic truncation enabled
df.write.format("indextables").save("s3://bucket/data")

// Custom threshold (drop statistics for values > 512 characters)
df.write.format("indextables")
  .option("spark.indextables.stats.truncation.maxLength", "512")
  .save("s3://bucket/data")

// Disable truncation (not recommended - may cause transaction log bloat)
df.write.format("indextables")
  .option("spark.indextables.stats.truncation.enabled", "false")
  .save("s3://bucket/data")

// Session-level configuration
spark.conf.set("spark.indextables.stats.truncation.enabled", "true")
spark.conf.set("spark.indextables.stats.truncation.maxLength", "256")

// All subsequent writes use these settings
df1.write.format("indextables").save("s3://bucket/data1")
df2.write.format("indextables").save("s3://bucket/data2")
```

#### Performance Impact

**Transaction Log Size Reduction:**
- **Before** (no truncation): Transaction files can reach 10+ MB with long text fields
- **After** (with truncation): Transaction files reduced to 100-500 KB (98% reduction)

**Expected Benefits:**
- **Transaction log reads**: 10-100x faster for tables with long text fields
- **Checkpoint creation**: Significantly faster with smaller stats
- **Cache memory usage**: Reduced memory footprint for transaction log cache
- **Write performance**: Minimal overhead (<1% for truncation logic)

**Data Skipping Trade-offs:**
- **Columns with stats**: Data skipping works as before (no performance regression)
- **Truncated columns**: Falls back to Spark filtering (minimal impact as long text fields rarely benefit from data skipping)
- **Net impact**: Positive due to dramatically faster transaction log operations

### Custom AWS Credential Providers

**New in v1.9**: Support for custom AWS credential providers via reflection, allowing integration with enterprise credential management systems without compile-time dependencies.

#### Configuration
- `spark.indextables.aws.credentialsProviderClass`: Fully qualified class name of custom AWS credential provider

#### Requirements
Custom credential providers must:
1. **Implement standard AWS SDK interfaces**: Either v1 `AWSCredentialsProvider` or v2 `AwsCredentialsProvider`
2. **Have required constructor**: `public MyProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration conf)`
3. **Return valid credentials**: Access key, secret key, and optional session token

#### Credential Resolution Priority
1. **Custom Provider** (if configured via `spark.indextables.aws.credentialsProviderClass`)
2. **Explicit Credentials** (access key/secret key in configuration)
3. **Default Provider Chain** (IAM roles, environment variables, etc.)

#### Configuration Examples

```scala
// Basic custom provider configuration
spark.conf.set("spark.indextables.aws.credentialsProviderClass", "com.example.MyCredentialProvider")

// Per-operation configuration
df.write.format("indextables")
  .option("spark.indextables.aws.credentialsProviderClass", "com.example.MyCredentialProvider")
  .save("s3://bucket/path")

// Hadoop configuration (also supported)
hadoopConf.set("spark.indextables.aws.credentialsProviderClass", "com.example.MyCredentialProvider")
```

#### Example Custom Provider (AWS SDK v2)

```java
public class MyCredentialProvider implements AwsCredentialsProvider {
    public MyCredentialProvider(URI uri, Configuration conf) {
        // Initialize with custom logic
    }

    @Override
    public AwsCredentials resolveCredentials() {
        // Return custom credentials
        return AwsBasicCredentials.create("access-key", "secret-key");
    }
}
```

#### Example Custom Provider (AWS SDK v1)

```java
public class MyLegacyCredentialProvider implements AWSCredentialsProvider {
    public MyLegacyCredentialProvider(URI uri, Configuration conf) {
        // Initialize with custom logic
    }

    @Override
    public AWSCredentials getCredentials() {
        // Return custom credentials
        return new BasicAWSCredentials("access-key", "secret-key");
    }

    @Override
    public void refresh() {
        // Refresh credentials if needed
    }
}
```

#### Key Benefits
- **No SDK Dependencies**: Uses reflection to avoid compile-time AWS SDK dependencies
- **Version Agnostic**: Supports both AWS SDK v1 and v2 providers automatically
- **Enterprise Integration**: Easy integration with custom credential management systems
- **Fallback Safety**: Graceful fallback to explicit credentials or default provider chain
- **Configuration Hierarchy**: Full support for DataFrame options, Spark config, and Hadoop config

#### URI Path Handling & Testing

**Table-Level URI Consistency**: Custom credential providers receive **table-level URIs** (not individual file paths) for consistent caching and configuration purposes.

**URI Scheme Normalization**: During read operations, URI schemes are normalized from `s3a://` to `s3://` for tantivy4java compatibility while preserving table-level path structure.

**Comprehensive Integration Testing**: Real S3 integration tests validate:
- ✅ **Table path validation**: URIs passed to credential providers are table paths (e.g., `s3://bucket/table-name`)
- ✅ **No file paths**: URIs never contain file extensions (`.split`, `.json`, `.parquet`) or file patterns (`part-`, `000000`)
- ✅ **Scheme normalization**: Proper `s3a://` → `s3://` conversion during read operations
- ✅ **Cross-scheme compatibility**: Write with `s3a://` and read with `s3://` work correctly
- ✅ **Configuration propagation**: Custom provider settings flow through driver and executor contexts
- ✅ **Production scenarios**: Tests include caching behavior, configuration precedence, and error handling

**Validation Implementation**:
```scala
// Example validation logic (automatically applied in tests)
private def validateTablePath(uri: URI, testDescription: String): Unit = {
  val uriPath = uri.getPath

  // Negative validations: should NOT contain file patterns
  uriPath should not endWith ".split"
  uriPath should not endWith ".json"
  uriPath should not endWith ".parquet"
  uriPath should not include "part-"

  // Positive validation: ensure it's a valid table path
  uriPath should not be empty
  println(s"✅ VALIDATED ($testDescription): URI '$uri' is a table path, not a file path")
}
```

**Test Coverage**:
- **4/4 integration tests passing** with real S3 validation
- **Table path consistency** verified across write/read operations
- **URI normalization** validated for both `s3a://` and `s3://` schemes
- **Configuration precedence** tested with multiple credential sources
- **Distributed context behavior** validated in executor environments

#### Production Recommendations

**Write Operations**: Custom credential providers work reliably for write operations in driver context:
```scala
df.write.format("indextables")
  .option("spark.indextables.aws.credentialsProviderClass", "com.example.MyProvider")
  .save("s3a://bucket/table")
```

**Read Operations**: For maximum reliability in distributed executor contexts, use explicit credentials:
```scala
val df = spark.read.format("indextables")
  .option("spark.indextables.aws.accessKey", accessKey)
  .option("spark.indextables.aws.secretKey", secretKey)
  .load("s3://bucket/table")
```

**Mixed Approach**: Combine custom providers for writes with explicit credentials for reads for optimal reliability and security.

### Azure Blob Storage Configuration

**New in v2.0**: Full Azure Blob Storage support for multi-cloud deployments with native authentication and high-performance operations.

#### Azure Authentication

IndexTables4Spark supports multiple Azure authentication methods with flexible credential resolution:

**Authentication Methods:**
1. **OAuth Service Principal (Client Credentials)**: Azure Active Directory authentication with bearer tokens
2. **Account Key Authentication**: Storage account name + account key
3. **Connection String Authentication**: Complete Azure connection string
4. **~/.azure/credentials File**: Shared credentials file supporting both account keys and Service Principal credentials

**Authentication Priority (Highest to Lowest):**
1. **OAuth Bearer Token**: Explicit bearer token or automatic token acquisition from Service Principal credentials
2. **Account Key**: Storage account key authentication
3. **Connection String**: Azure connection string

#### Credential Resolution Priority
1. **DataFrame Write Options**: `.option("spark.indextables.azure.accountName", ...)`
2. **Spark Session Configuration**: `spark.conf.set("spark.indextables.azure.accountName", ...)`
3. **Hadoop Configuration**: `hadoopConf.set("spark.indextables.azure.accountName", ...)`
4. **Environment Variables**: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`
5. **~/.azure/credentials File**: Automatic credential file loading (supports both account key and Service Principal)

#### Azure Configuration Keys

**Account Key Authentication:**
- `spark.indextables.azure.accountName`: Azure storage account name
- `spark.indextables.azure.accountKey`: Azure storage account key
- `spark.indextables.azure.connectionString`: Azure connection string (alternative to account name/key)
- `spark.indextables.azure.endpoint`: Custom Azure endpoint (optional, for Azurite or custom endpoints)

**OAuth Service Principal Authentication:**
- `spark.indextables.azure.accountName`: Azure storage account name (required)
- `spark.indextables.azure.tenantId`: Azure AD tenant ID
- `spark.indextables.azure.clientId`: Service Principal application (client) ID
- `spark.indextables.azure.clientSecret`: Service Principal client secret
- `spark.indextables.azure.bearerToken`: Explicit OAuth bearer token (optional - auto-acquired if not provided)

#### Configuration Examples

**Basic Write to Azure**
```scala
// Using azure:// scheme (tantivy4java native - recommended for simplicity)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("azure://mycontainer/data")

// Using abfss:// scheme (Spark modern - recommended for ADLS Gen2)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/data")

// Using wasbs:// scheme (Spark legacy - for compatibility)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("wasbs://mycontainer@mystorageaccount.blob.core.windows.net/data")

// Using connection string (works with any URL scheme)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.connectionString", "DefaultEndpointsProtocol=https;AccountName=...")
  .save("azure://mycontainer/data")
```

**Session-Level Configuration**
```scala
// Configure once for all operations
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.accountKey", "your-account-key")

// All subsequent operations use these credentials
df1.write.format("indextables").save("azure://container1/data1")
df2.write.format("indextables").save("azure://container2/data2")

val df3 = spark.read.format("indextables").load("azure://container1/data1")
```

**~/.azure/credentials File (Account Key)**
```ini
# File: ~/.azure/credentials
[default]
storage_account = mystorageaccount
account_key = your-account-key-here
```

```scala
// Credentials automatically loaded from file - no configuration needed!
df.write.format("indextables").save("azure://mycontainer/data")
val df = spark.read.format("indextables").load("azure://mycontainer/data")
```

**~/.azure/credentials File (Service Principal - OAuth)**
```ini
# File: ~/.azure/credentials
[default]
storage_account = mystorageaccount
tenant_id = your-tenant-id
client_id = your-client-id
client_secret = your-client-secret
```

```scala
// OAuth credentials automatically loaded from file - no configuration needed!
df.write.format("indextables").save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/data")
val df = spark.read.format("indextables").load("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/data")
```

**Environment Variables**
```bash
# Account Key authentication
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=your-account-key

# Or OAuth Service Principal authentication
export AZURE_TENANT_ID=your-tenant-id
export AZURE_CLIENT_ID=your-client-id
export AZURE_CLIENT_SECRET=your-client-secret

# Or use connection string
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
```

```scala
// Credentials automatically loaded from environment - no configuration needed!
df.write.format("indextables").save("azure://mycontainer/data")
```

#### OAuth Service Principal (Azure AD) Authentication

**New in v2.0**: Full OAuth bearer token support with automatic token acquisition from Azure Active Directory Service Principal credentials.

**Creating a Service Principal:**
```bash
# Create Service Principal with Azure CLI
az ad sp create-for-rbac --name "indextables-spark-sp" \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>

# Output will contain:
# - appId (client_id)
# - password (client_secret)
# - tenant (tenant_id)
```

**Explicit OAuth Configuration with abfss:// (Recommended for ADLS Gen2)**
```scala
// Using abfss:// protocol for ADLS Gen2 with OAuth
val storageAccount = "mystorageaccount"
val container = "mycontainer"
val tenantId = "your-tenant-id"
val clientId = "your-client-id"
val clientSecret = "your-client-secret"

// Write with OAuth Service Principal
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", storageAccount)
  .option("spark.indextables.azure.tenantId", tenantId)
  .option("spark.indextables.azure.clientId", clientId)
  .option("spark.indextables.azure.clientSecret", clientSecret)
  .save(s"abfss://$container@$storageAccount.dfs.core.windows.net/data")

println("✅ Wrote DataFrame using OAuth Service Principal authentication")

// Read with OAuth Service Principal
val readDf = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", storageAccount)
  .option("spark.indextables.azure.tenantId", tenantId)
  .option("spark.indextables.azure.clientId", clientId)
  .option("spark.indextables.azure.clientSecret", clientSecret)
  .load(s"abfss://$container@$storageAccount.dfs.core.windows.net/data")

println("✅ Successfully read DataFrame using OAuth authentication")
```

**Session-Level OAuth Configuration**
```scala
// Configure OAuth at session level for all operations
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.tenantId", "your-tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "your-client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "your-client-secret")

// All subsequent operations use OAuth authentication
df.write.format("indextables")
  .save("abfss://container1@mystorageaccount.dfs.core.windows.net/data1")

df.write.format("indextables")
  .partitionBy("date", "hour")
  .save("abfss://container2@mystorageaccount.dfs.core.windows.net/data2")

val df1 = spark.read.format("indextables")
  .load("abfss://container1@mystorageaccount.dfs.core.windows.net/data1")
```

**OAuth with MERGE SPLITS Operations**
```scala
// Configure OAuth credentials
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.tenantId", "your-tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "your-client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "your-client-secret")

// Write data using abfss:// protocol
val abfssPath = "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/data"
df.write.format("indextables").save(abfssPath)

// Execute MERGE SPLITS with OAuth authentication
spark.sql(s"MERGE SPLITS '$abfssPath' TARGET SIZE 100M")

println("✅ MERGE SPLITS executed successfully with OAuth authentication")
```

**Explicit Bearer Token (Advanced)**
```scala
// If you already have an OAuth bearer token, you can provide it directly
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.bearerToken", "your-bearer-token")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/data")
```

**OAuth Benefits:**
- **Enhanced Security**: No storage account keys in code or configuration
- **Azure AD Integration**: Centralized identity and access management
- **Role-Based Access Control (RBAC)**: Fine-grained permissions via Azure AD roles
- **Audit Trail**: All operations logged in Azure AD for compliance
- **Token Rotation**: Automatic token refresh and rotation support
- **Enterprise Ready**: Integrates with existing Azure AD infrastructure

#### Azure-Specific Features

**Automatic Container Creation**
- Containers are automatically created in development/testing environments if they don't exist
- Production environments require pre-existing containers

**Atomic Transaction Log Writes**
- Uses conditional PUT with `If-None-Match: *` for transaction log integrity
- Prevents race conditions during concurrent writes
- Ensures exactly-once semantics for transaction log entries

**Native Azure Protocol Support**
- Supports multiple Azure URL schemes for Spark compatibility:
  - `azure://container/path` (tantivy4java native - recommended)
  - `wasb://container@account.blob.core.windows.net/path` (Spark legacy)
  - `wasbs://container@account.blob.core.windows.net/path` (Spark legacy secure)
  - `abfs://container@account.dfs.core.windows.net/path` (Spark modern - Azure Data Lake Gen2)
  - `abfss://container@account.dfs.core.windows.net/path` (Spark modern secure - recommended for ADLS Gen2)
- Automatic URL normalization to `azure://` format for tantivy4java compatibility
- No Hadoop filesystem dependencies for Azure operations
- High-performance Azure SDK for Java integration

#### Multi-Cloud Operations

**Write to Both S3 and Azure**
```scala
val df = spark.createDataFrame(data).toDF("id", "content", "score")

// Write to S3
df.write.format("indextables")
  .option("spark.indextables.aws.accessKey", s3AccessKey)
  .option("spark.indextables.aws.secretKey", s3SecretKey)
  .save("s3://s3-bucket/data")

// Write same data to Azure
df.write.format("indextables")
  .option("spark.indextables.azure.accountName", azureAccount)
  .option("spark.indextables.azure.accountKey", azureKey)
  .save("azure://azure-container/data")
```

**Read from Multiple Cloud Sources**
```scala
// Read from S3
val s3Df = spark.read.format("indextables")
  .option("spark.indextables.aws.accessKey", s3AccessKey)
  .option("spark.indextables.aws.secretKey", s3SecretKey)
  .load("s3://s3-bucket/data")

// Read from Azure
val azureDf = spark.read.format("indextables")
  .option("spark.indextables.azure.accountName", azureAccount)
  .option("spark.indextables.azure.accountKey", azureKey)
  .load("azure://azure-container/data")

// Union DataFrames from different clouds
val combinedDf = s3Df.union(azureDf)
combinedDf.count()
```

**Cross-Cloud Merge Operations**
```scala
// Azure credentials configured in session
spark.conf.set("spark.indextables.azure.accountName", azureAccount)
spark.conf.set("spark.indextables.azure.accountKey", azureKey)

// Merge splits in Azure container
spark.sql("MERGE SPLITS 'azure://mycontainer/data' TARGET SIZE 100M")

// Merge operations work identically across S3 and Azure
spark.sql("MERGE SPLITS 's3://s3-bucket/data' TARGET SIZE 100M")
```

#### Azure with Advanced Features

**Partitioned Datasets on Azure (abfss:// with OAuth)**
```scala
// Configure OAuth Service Principal credentials
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.tenantId", "your-tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "your-client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "your-client-secret")

// Write partitioned data using abfss:// protocol (ADLS Gen2)
df.write.format("indextables")
  .partitionBy("date", "hour")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/partitioned-data")

// Read with partition pruning
val df = spark.read.format("indextables")
  .load("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/partitioned-data")
df.filter($"date" === "2024-01-01" && $"hour" === 10).show()
```

**Partitioned Datasets on Azure (abfss:// with Account Key)**
```scala
// Write partitioned data with account key authentication
df.write.format("indextables")
  .partitionBy("date", "hour")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/partitioned-data")

// Read with partition pruning
val df = spark.read.format("indextables")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .load("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/partitioned-data")
df.filter($"date" === "2024-01-01" && $"hour" === 10).show()
```

**IndexQuery Operations on Azure (abfss:// with OAuth)**
```scala
// Write with text field configuration using OAuth
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.tenantId", "your-tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "your-client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "your-client-secret")

df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.content", "text")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/searchable-data")

// Query with IndexQuery
import org.apache.spark.sql.indextables.IndexQueryExpression._
val df = spark.read.format("indextables")
  .load("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/searchable-data")
df.filter($"content" indexquery "machine learning").show()
```

**Aggregate Pushdown on Azure (abfss://)**
```scala
// Write with fast field configuration
df.write.format("indextables")
  .option("spark.indextables.indexing.fastfields", "score,value")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/aggregatable-data")

// Aggregations pushed to tantivy
val df = spark.read.format("indextables")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .load("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/aggregatable-data")
df.agg(count("*"), sum("score"), avg("value")).show()
```

**Auto-Sizing with Azure (abfss://)**
```scala
// Intelligent partitioning based on historical data
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/auto-sized-data")
```

#### Azure Performance Optimization

**Working Directory Configuration (abfss:// with OAuth)**
```scala
// Use high-performance local storage for index creation with OAuth
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.tenantId", "your-tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "your-client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "your-client-secret")

df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/tantivy-temp")
  .option("spark.indextables.cache.directoryPath", "/local_disk0/tantivy-cache")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/optimized-data")
```

**Transaction Log Compression (abfss://)**
```scala
// Enable compression for reduced Azure storage costs
df.write.format("indextables")
  .option("spark.indextables.transaction.compression.enabled", "true")
  .option("spark.indextables.transaction.compression.gzip.level", "6")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/compressed-logs")
```

#### Azure Testing Support

**Test Credentials**
```bash
# System properties
-Dtest.azure.container=tantivy4spark-test
-Dtest.azure.storageAccount=mystorageaccount
-Dtest.azure.accountKey=your-account-key

# Or use ~/.azure/credentials file (recommended)
# Or use environment variables
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=your-account-key
```

**Azurite Local Testing** (Future)
```scala
// Configure for Azurite emulator
spark.conf.set("spark.indextables.azure.endpoint", "http://127.0.0.1:10000/devstoreaccount1")
spark.conf.set("spark.indextables.azure.accountName", "devstoreaccount1")
spark.conf.set("spark.indextables.azure.accountKey", "Eby8vdM02xNOcqF...")
```

#### Azure URL Scheme Compatibility

IndexTables4Spark supports all standard Spark Azure URL schemes with automatic normalization:

**Supported URL Schemes:**
- ✅ `azure://container/path` - tantivy4java native (simple, recommended for new projects)
- ✅ `wasb://container@account.blob.core.windows.net/path` - Spark legacy (deprecated, use abfss instead)
- ✅ `wasbs://container@account.blob.core.windows.net/path` - Spark legacy secure (deprecated, use abfss instead)
- ✅ `abfs://container@account.dfs.core.windows.net/path` - Spark modern for ADLS Gen2
- ✅ `abfss://container@account.dfs.core.windows.net/path` - Spark modern secure for ADLS Gen2 (recommended for enterprise)

**URL Normalization:**
All Spark Azure schemes (`wasb://`, `wasbs://`, `abfs://`, `abfss://`) are automatically normalized to `azure://` format when passing URLs to tantivy4java, ensuring compatibility while preserving Spark conventions.

**Best Practices:**
1. **New Projects**: Use `azure://` for simplicity (no storage account in URL)
2. **ADLS Gen2**: Use `abfss://` for hierarchical namespace features and security
3. **Legacy Compatibility**: `wasbs://` supported but migrate to `abfss://` for better performance
4. **Multi-cloud**: Mix `s3://`, `azure://`, and `abfss://` URLs freely in the same application

**Example - Mixed URL Schemes:**
```scala
// Session-level Azure config applies to all Azure URL schemes
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.accountKey", "your-account-key")

// All these work identically (normalized to azure:// internally)
df.write.format("indextables").save("azure://container1/data")
df.write.format("indextables").save("abfss://container2@mystorageaccount.dfs.core.windows.net/data")
df.write.format("indextables").save("wasbs://container3@mystorageaccount.blob.core.windows.net/data")

// Read from any scheme
val df1 = spark.read.format("indextables").load("azure://container1/data")
val df2 = spark.read.format("indextables").load("abfss://container2@mystorageaccount.dfs.core.windows.net/data")
```

#### Azure Known Limitations

**Current Implementation:**
- ✅ Account key authentication fully supported
- ✅ Connection string authentication fully supported
- ✅ OAuth Service Principal (Client Credentials) authentication fully supported
- ✅ Bearer token authentication fully supported
- ✅ ~/.azure/credentials file loading supported (account key and Service Principal)
- ✅ Environment variable support (account key and OAuth)
- ✅ Automatic OAuth bearer token acquisition from Azure AD
- ⏳ DefaultAzureCredential - requires tantivy4java 0.26.0+ (future)
- ⏳ Managed Identity authentication - requires tantivy4java 0.26.0+ (future)

**Merge Operations:**
- ✅ Azure config passed to merge operations
- ✅ OAuth credentials properly serialized for executor distribution
- ✅ Azure URL normalization (abfss:// to azure://) in merge operations
- ✅ Cross-cloud merge support validated in tests
- ✅ Partition-aware merge operations working

#### Production Recommendations

**Write Operations:**
```scala
// Recommended: Session-level configuration for simplicity
spark.conf.set("spark.indextables.azure.accountName", azureAccount)
spark.conf.set("spark.indextables.azure.accountKey", azureKey)

df.write.format("indextables").save("azure://container/data")
```

**Read Operations:**
```scala
// Recommended: Explicit credentials for distributed executor reliability
val df = spark.read.format("indextables")
  .option("spark.indextables.azure.accountName", azureAccount)
  .option("spark.indextables.azure.accountKey", azureKey)
  .load("azure://container/data")
```

**Multi-Cloud Deployments:**
```scala
// Configure both clouds in session
spark.conf.set("spark.indextables.aws.accessKey", s3AccessKey)
spark.conf.set("spark.indextables.aws.secretKey", s3SecretKey)
spark.conf.set("spark.indextables.azure.accountName", azureAccount)
spark.conf.set("spark.indextables.azure.accountKey", azureKey)

// Use either cloud transparently
df.write.format("indextables").save("s3://s3-bucket/data")      // S3
df.write.format("indextables").save("azure://container/data")   // Azure

val s3Data = spark.read.format("indextables").load("s3://s3-bucket/data")
val azureData = spark.read.format("indextables").load("azure://container/data")
```

### Working Directory & Cache Configuration

**New in v1.5**: Custom working directory support for index creation and split operations provides control over where temporary files are stored during processing.

**New in v1.6**: Automatic `/local_disk0` detection and cache directory override for optimal performance on Databricks and high-performance storage environments.

#### Directory Settings
- `spark.indextables.indexWriter.tempDirectoryPath`: Custom working directory for index creation during writes (default: auto-detect `/local_disk0` or system temp)
- `spark.indextables.merge.tempDirectoryPath`: Custom temporary directory for split merge operations (default: auto-detect `/local_disk0` or system temp)
- `spark.indextables.cache.directoryPath`: Custom split cache directory for downloaded files (default: auto-detect `/local_disk0` or system temp)

#### Merge Operation Settings
- `spark.indextables.merge.heapSize`: Heap size for merge operations in bytes (default: `1073741824` - 1GB, supports human-readable formats like "2G", "500M")
- `spark.indextables.merge.debug`: Enable debug logging in merge operations (default: `false`)
- `spark.indextables.merge.batchSize`: Number of merge groups per batch (default: `defaultParallelism` - Spark's default parallelism)
- `spark.indextables.merge.maxConcurrentBatches`: Maximum number of batches to process concurrently (default: `2`)

#### Automatic `/local_disk0` Detection
All directory configurations now automatically detect and use `/local_disk0` when available and writable:
- **Databricks Clusters**: Automatically uses high-performance local SSDs
- **EMR/EC2 Instance Storage**: Leverages ephemeral storage when available
- **Custom Environments**: Detects any mounted `/local_disk0` directory
- **Graceful Fallback**: Uses system defaults when `/local_disk0` unavailable

**Use Cases & Examples:**
```scala
// Automatic detection (recommended - uses /local_disk0 when available)
// No configuration needed - automatically optimized!

// Manual Databricks optimization
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
spark.conf.set("spark.indextables.merge.tempDirectoryPath", "/local_disk0/merge-temp")
spark.conf.set("spark.indextables.cache.directoryPath", "/local_disk0/tantivy-cache")

// High-performance storage: Use NVMe SSD
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/fast-nvme/tantivy-temp")
spark.conf.set("spark.indextables.cache.directoryPath", "/fast-nvme/tantivy-cache")

// Memory filesystem: For maximum speed (sufficient RAM required)
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/dev/shm/tantivy-index")
spark.conf.set("spark.indextables.cache.directoryPath", "/dev/shm/tantivy-cache")

// Per-write operation configuration
df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/fast-storage/index-temp")
  .option("spark.indextables.cache.directoryPath", "/fast-storage/cache")
  .save("s3://bucket/path")

// Read with custom cache directory
val df = spark.read.format("indextables")
  .option("spark.indextables.cache.directoryPath", "/nvme/tantivy-cache")
  .load("s3://bucket/path")

// Merge splits with custom temporary directory and heap size
spark.conf.set("spark.indextables.merge.tempDirectoryPath", "/fast-nvme/merge-temp")
spark.conf.set("spark.indextables.merge.heapSize", "2147483648")  // 2GB heap for large merges
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 500M")
```

**Validation & Safety Features:**
- **Path Validation**: Ensures specified directory exists and is writable
- **Automatic Fallback**: Uses system temp directory if custom path is invalid
- **Process Isolation**: Creates unique subdirectories to prevent conflicts between concurrent operations
- **Automatic Cleanup**: Removes temporary files after processing completion regardless of success/failure

### Auto-Sizing Configuration

**New in v1.6**: Intelligent auto-sizing that dynamically repartitions DataFrames based on historical split data to achieve target split sizes with comprehensive test coverage.

#### Auto-Sizing Settings
- `spark.indextables.autoSize.enabled`: `false` (Enable auto-sizing based on historical data)
- `spark.indextables.autoSize.targetSplitSize`: Target size per split (supports: `"100M"`, `"1G"`, `"512K"`, `"123456"` bytes)
- `spark.indextables.autoSize.inputRowCount`: Explicit row count for accurate partitioning (required for V2 API, optional for V1)

#### How Auto-Sizing Works
1. **Historical Analysis**: Examines recent splits in the transaction log to extract size and row count data
2. **Bytes-per-Record Calculation**: Calculates average bytes per record from historical data (weighted by record count)
3. **Target Rows Calculation**: Determines optimal rows per split: `ceil(targetSizeBytes / avgBytesPerRecord)`
4. **DataFrame Counting**: V1 API automatically counts DataFrames when auto-sizing enabled; V2 API uses explicit count
5. **Dynamic Repartitioning**: Partitions DataFrame using: `max(1, ceil(rowCount / targetRows))`

#### API-Specific Behavior
- **V1 DataSource API**: Automatically counts DataFrame when auto-sizing is enabled (performance optimized)
- **V2 DataSource API**: Requires explicit row count option for accurate results; estimates if not provided

#### Configuration Formats
**Boolean Values**: `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off` (case insensitive)
**Size Formats**:
- **Bytes**: `"123456"` → 123,456 bytes
- **Kilobytes**: `"512K"` → 524,288 bytes
- **Megabytes**: `"100M"` → 104,857,600 bytes
- **Gigabytes**: `"2G"` → 2,147,483,648 bytes
- **Case insensitive**: `"100m"`, `"2g"` work correctly

#### Usage Examples

**V1 API (Recommended for Auto-Sizing)**
```scala
// V1 with automatic DataFrame counting
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/path")

// V1 with different size formats
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "1")        // Extended boolean support
  .option("spark.indextables.autoSize.targetSplitSize", "512K") // Kilobyte format
  .save("s3://bucket/path")
```

**V2 API (Explicit Row Count Required)**
```scala
// V2 with explicit row count for accurate auto-sizing
val rowCount = df.count()
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "50M")
  .option("spark.indextables.autoSize.inputRowCount", rowCount.toString)
  .save("s3://bucket/path")

// V2 without explicit count (uses estimation with warning)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "2G")
  .save("s3://bucket/path")
```

**Global Configuration**
```scala
// Session-level configuration
spark.conf.set("spark.indextables.autoSize.enabled", "yes")    // Extended boolean
spark.conf.set("spark.indextables.autoSize.targetSplitSize", "200M")
df.write.format("indextables").save("s3://bucket/path")

// Write options override session config
df.write.format("indextables")
  .option("spark.indextables.autoSize.targetSplitSize", "1G")  // Overrides session config
  .save("s3://bucket/path")
```

#### Performance Optimizations
- **Conditional DataFrame Counting**: `df.count()` only called when auto-sizing is enabled in V1 API
- **Smart Fallbacks**: Gracefully falls back to manual configuration when historical analysis fails
- **Historical Data Limiting**: Analyzes up to 10 recent splits by default to balance accuracy and performance
- **Error Resilience**: Continues with manual configuration if auto-sizing encounters errors

#### Requirements & Limitations
- **Historical Data**: Requires existing splits with size and record count metadata in transaction log
- **Optimized Writes**: Auto-sizing only works when `optimizeWrite` is enabled (default: true)
- **V2 API Limitation**: Requires explicit row count for optimal partitioning accuracy
- **Fallback Behavior**: Falls back to `targetRecordsPerSplit` configuration if historical analysis fails

#### Error Handling
- **Invalid size formats**: Clear error messages with supported format examples
- **Zero/negative sizes**: Properly rejected with validation errors
- **Empty configuration values**: Treated as unspecified (None) rather than causing crashes
- **Historical analysis failures**: Logged as warnings, execution continues with fallback

#### Test Coverage
- **✅ 28/28 unit tests passing**: Complete coverage of all auto-sizing components
- **SizeParser**: 17 tests covering format parsing, validation, and edge cases
- **Configuration Options**: 11 tests covering boolean parsing, numeric validation, and error handling
- **Integration Ready**: Comprehensive test suite validates all usage scenarios

### Large File Upload Configuration

**New in v1.3**: Memory-efficient streaming uploads for large splits (4GB+) to prevent OOM errors.

#### Upload Performance Settings
- `spark.indextables.s3.streamingThreshold`: `104857600` (100MB - files larger than this use streaming upload)
- `spark.indextables.s3.multipartThreshold`: `104857600` (100MB - threshold for S3 multipart upload)
- `spark.indextables.s3.maxConcurrency`: `4` (Number of parallel upload threads for both byte array and streaming uploads)
- `spark.indextables.s3.partSize`: `67108864` (64MB - size of each multipart upload part)

#### Advanced Upload Features
**Parallel Streaming Uploads (New in v1.3):**
- **Multi-threaded streaming**: Uses buffered chunking strategy that reads stream chunks into memory buffers and uploads them concurrently
- **Configurable parallelism**: Set global concurrency or override per write operation
- **Memory-efficient processing**: Controlled buffer usage with backpressure to prevent OOM errors
- **Intelligent upload strategy**: Automatic selection between single-part and multipart uploads based on file size
- **Per-operation tuning**: Override global settings for specific write operations with high-performance requirements

**Performance Characteristics:**
- **Dramatic throughput improvement** for large file uploads (4GB+)
- **Scalable concurrency**: Performance scales linearly with thread count up to network/storage limits
- **Memory safety**: Buffer queue management prevents excessive memory usage during large uploads
- **Error resilience**: Individual part failures are retried without affecting other concurrent uploads

### Transaction Log Performance & Compaction

**New in v1.2**: High-performance transaction log with Delta Lake-style checkpoint compaction and parallel S3 retrieval.

#### Checkpoint Configuration
- `spark.indextables.checkpoint.enabled`: `true` (Enable automatic checkpoint creation)
- `spark.indextables.checkpoint.interval`: `10` (Create checkpoint every N transactions)
- `spark.indextables.checkpoint.parallelism`: `4` (Thread pool size for parallel I/O)
- `spark.indextables.checkpoint.read.timeoutSeconds`: `30` (Timeout for parallel read operations)

#### Data Retention Policies
- `spark.indextables.logRetention.duration`: `2592000000` (30 days in milliseconds)
- `spark.indextables.checkpointRetention.duration`: `7200000` (2 hours in milliseconds)

#### File Cleanup & Safety
- `spark.indextables.cleanup.enabled`: `true` (Enable automatic cleanup of old transaction files)
- `spark.indextables.cleanup.failurePolicy`: `continue` (Continue operations if cleanup fails)
- `spark.indextables.cleanup.dryRun`: `false` (Set to true to log cleanup actions without deleting files)

#### Advanced Performance Features
- `spark.indextables.checkpoint.checksumValidation.enabled`: `true` (Enable data integrity validation)
- `spark.indextables.checkpoint.multipart.enabled`: `false` (Enable multi-part checkpoints for large tables)
- `spark.indextables.checkpoint.multipart.maxActionsPerPart`: `50000` (Actions per checkpoint part)
- `spark.indextables.checkpoint.auto.enabled`: `true` (Enable automatic checkpoint optimization)
- `spark.indextables.checkpoint.auto.minFileAge`: `600000` (10 minutes in milliseconds)

#### Transaction Log Cache
- `spark.indextables.transaction.cache.enabled`: `true` (Enable transaction log caching)
- `spark.indextables.transaction.cache.expirationSeconds`: `300` (5 minutes cache TTL)

#### Transaction Log Compression

**New in v1.15**: GZIP compression for transaction log files and checkpoints to reduce S3 storage costs and improve download performance.

##### Compression Configuration
- `spark.indextables.transaction.compression.enabled`: `true` (Enable transaction log compression - enabled by default)
- `spark.indextables.transaction.compression.codec`: `"gzip"` (Compression codec to use)
- `spark.indextables.transaction.compression.gzip.level`: `6` (GZIP compression level, 1-9)

##### Benefits
- **60-70% storage reduction** for transaction log files
- **2-3x compression ratio** for typical JSON transaction logs
- **Faster S3 downloads** with smaller file sizes
- **Backward compatible** - reads both compressed and uncompressed files automatically
- **Transparent operation** - no code changes required

##### Usage Examples

```scala
// Default behavior (recommended) - compression enabled automatically
df.write.format("indextables").save("s3://bucket/data")

// Explicit compression configuration
df.write.format("indextables")
  .option("spark.indextables.transaction.compression.enabled", "true")
  .option("spark.indextables.transaction.compression.codec", "gzip")
  .option("spark.indextables.transaction.compression.gzip.level", "6")
  .save("s3://bucket/data")

// Disable compression (not recommended)
df.write.format("indextables")
  .option("spark.indextables.transaction.compression.enabled", "false")
  .save("s3://bucket/data")

// Higher compression for maximum space savings (slower writes)
df.write.format("indextables")
  .option("spark.indextables.transaction.compression.gzip.level", "9")
  .save("s3://bucket/data")

// Faster compression for high-throughput writes
df.write.format("indextables")
  .option("spark.indextables.transaction.compression.gzip.level", "1")
  .save("s3://bucket/data")
```

##### Implementation Details
- **Magic byte format**: Compressed files start with `0x01 0x01` followed by codec identifier
- **GZIP compression**: Using standard Java GZIP streams (no external dependencies)
- **Automatic detection**: `CompressionUtils.isCompressed()` checks for magic bytes
- **Transparent reading**: `CompressionUtils.readTransactionFile()` handles both formats
- **Checkpoint support**: Checkpoints are also compressed when enabled
- **Mixed-mode support**: Can read tables with mix of compressed and uncompressed files

##### Performance Impact
- **Write overhead**: ~5-10% for GZIP level 6 (default)
- **Read speedup**: 2-3x faster downloads from S3 due to smaller file sizes
- **Storage savings**: 60-70% reduction in transaction log storage costs
- **Net benefit**: Positive for most workloads due to reduced S3 transfer times

## Field Indexing Configuration

**New in v1.1**: Advanced field indexing configuration with support for string, text, and JSON field types.

### Field Type Configuration
- `spark.indextables.indexing.typemap.<field_name>`: Set field indexing type
  - **`string`** (default): Exact string matching with raw tokenizer, supports precise filter pushdown
  - **`text`**: Full-text search with default tokenizer, best-effort filtering with Spark post-processing
  - **`json`**: JSON field indexing with tokenization

### Field Behavior Configuration
- `spark.indextables.indexing.fastfields`: Comma-separated list of fields for fast access (e.g., `"id,score,timestamp"`)
- `spark.indextables.indexing.storeonlyfields`: Fields stored but not indexed (e.g., `"metadata,description"`)
- `spark.indextables.indexing.indexonlyfields`: Fields indexed but not stored (e.g., `"searchterms,keywords"`)

### Tokenizer Configuration
- `spark.indextables.indexing.tokenizer.<field_name>`: Custom tokenizer for text fields
  - **`default`**: Standard tokenizer
  - **`whitespace`**: Whitespace-only tokenization
  - **`raw`**: No tokenization

### Configuration Examples

#### Field Configuration
```scala
// Configure field types and behavior
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.title", "string")        // Exact matching
  .option("spark.indextables.indexing.typemap.content", "text")        // Full-text search
  .option("spark.indextables.indexing.typemap.metadata", "json")       // JSON indexing
  .option("spark.indextables.indexing.fastfields", "score,timestamp")  // Fast fields
  .option("spark.indextables.indexing.storeonlyfields", "raw_data")     // Store only
  .option("spark.indextables.indexing.tokenizer.content", "default")   // Custom tokenizer
  .save("s3://bucket/path")
```

#### High-Performance Upload Configuration
```scala
// Maximum performance with parallel streaming uploads
df.write.format("indextables")
  .option("spark.indextables.s3.maxConcurrency", "12")                  // 12 parallel upload threads
  .option("spark.indextables.s3.partSize", "268435456")                 // 256MB part size
  .option("spark.indextables.s3.multipartThreshold", "104857600")       // 100MB threshold
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/fast-nvme/tantivy-temp")
  .save("s3://bucket/high-performance")

// Memory filesystem for extreme performance (requires sufficient RAM)
df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/dev/shm/tantivy-index")
  .option("spark.indextables.s3.maxConcurrency", "16")                  // Maximum concurrency
  .option("spark.indextables.indexWriter.batchSize", "50000")           // Large batches
  .save("s3://bucket/memory-optimized")

// Databricks optimized configuration
df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
  .option("spark.indextables.s3.maxConcurrency", "8")                   // Balanced concurrency
  .option("spark.indextables.s3.partSize", "134217728")                 // 128MB parts
  .save("s3://bucket/databricks-optimized")
```

#### High-Performance Transaction Log Configuration
```scala
// Optimize for high-transaction workloads
df.write.format("indextables")
  .option("spark.indextables.checkpoint.enabled", "true")
  .option("spark.indextables.checkpoint.interval", "5")                 // Checkpoint every 5 transactions
  .option("spark.indextables.checkpoint.parallelism", "8")              // Use 8 threads for parallel I/O
  .option("spark.indextables.logRetention.duration", "86400000")        // 1 day retention
  .save("s3://bucket/high-volume-data")

// For very large tables with many transactions
df.write.format("indextables")
  .option("spark.indextables.checkpoint.enabled", "true")
  .option("spark.indextables.checkpoint.interval", "20")                // Less frequent checkpoints
  .option("spark.indextables.checkpoint.multipart.enabled", "true")     // Multi-part checkpoints
  .option("spark.indextables.checkpoint.parallelism", "12")             // Higher parallelism
  .option("spark.indextables.checkpoint.read.timeoutSeconds", "60")     // Longer timeout
  .save("s3://bucket/enterprise-data")

// Conservative settings for stability
df.write.format("indextables")
  .option("spark.indextables.checkpoint.enabled", "true")
  .option("spark.indextables.checkpoint.interval", "50")                // Infrequent checkpoints
  .option("spark.indextables.checkpoint.parallelism", "2")              // Conservative parallelism
  .option("spark.indextables.checkpoint.checksumValidation.enabled", "true")
  .save("s3://bucket/critical-data")
```

### Field Type Behavior

#### String Fields (`string` type)
- **Tokenizer**: Raw tokenizer (no tokenization)
- **Exact matching**: Full support for precise filter pushdown (`===`, `contains`, etc.)
- **Performance**: All equality and substring filters execute at data source level
- **Use cases**: IDs, exact titles, status codes, categories

#### Text Fields (`text` type)
- **Tokenizer**: Default tokenizer (tokenized)
- **Search capability**: Full-text search with IndexQuery operators
- **Exact matching**: Best-effort at data source level + Spark post-processing for precision
- **Performance**: IndexQuery filters pushed down, equality filters handled by Spark
- **Use cases**: Article content, descriptions, searchable text

#### JSON Fields (`json` type)
- **Tokenizer**: Default tokenizer applied to JSON content
- **Search capability**: Tokenized JSON content search
- **Performance**: Similar to text fields with tokenized search

#### Filter Pushdown Behavior
- **String fields**: All standard filters (`EqualTo`, `StringContains`, etc.) pushed to data source
- **Text fields**: Only `IndexQuery` filters pushed to data source, exact match filters post-processed by Spark
- **Configuration persistence**: Settings are automatically stored and validated on subsequent writes

## Skipped Files Configuration

**New in v1.7**: Robust handling of corrupted or problematic files during merge operations with intelligent cooldown and retry mechanisms.

### Core Settings
- `spark.indextables.skippedFiles.trackingEnabled`: `true` (Enable skipped files tracking and cooldown)
- `spark.indextables.skippedFiles.cooldownDuration`: `24` (Hours to wait before retrying failed files)

### Skipped Files Behavior

**When merge operations encounter problematic files:**
- ✅ **Skipped files are logged** with timestamps, reasons, and metadata
- ✅ **Original files remain accessible** (not marked as "removed" in transaction log)
- ✅ **Cooldown periods prevent repeated failures** on the same files
- ✅ **Automatic retry after cooldown expires** for eventual recovery
- ⚠️ **Warning logs generated** for all skipped files and failed merge attempts
- ❌ **No task failures** - operations continue gracefully despite file issues

**Null/Empty indexUid Handling:**
When tantivy4java returns null or empty indexUid (indicating no merge was performed):
- Handles null, empty string, and whitespace-only indexUids identically
- Files are not marked as "removed" from transaction log
- Skipped files are still tracked with proper cooldown
- Warning logs indicate no merge occurred
- Operation continues without failing

### Configuration Examples

```scala
// Production settings with 48-hour cooldown
df.write.format("indextables")
  .option("spark.indextables.skippedFiles.trackingEnabled", "true")
  .option("spark.indextables.skippedFiles.cooldownDuration", "48")
  .save("s3://bucket/production-data")

// Development with shorter cooldown for faster testing
df.write.format("indextables")
  .option("spark.indextables.skippedFiles.cooldownDuration", "1")
  .save("s3://bucket/dev-data")

// Disable skipped files tracking (not recommended)
df.write.format("indextables")
  .option("spark.indextables.skippedFiles.trackingEnabled", "false")
  .save("s3://bucket/path")
```

### Transaction Log Integration

Skipped files are recorded in the transaction log using `SkipAction` with:
- **File path and metadata** (size, partition values)
- **Skip timestamp and reason** for debugging
- **Operation context** (e.g., "merge")
- **Retry timestamp** for cooldown management
- **Skip count** for tracking repeated failures

## Usage Examples

### Write

**DataSource API Recommendation**: Use V2 API (`"io.indextables.spark.core.IndexTables4SparkTableProvider"`) for new projects to ensure partition columns are properly indexed. V1 API (`"indextables"`) is maintained for compatibility but excludes partition columns from indexing.

```scala
// Basic write (string fields by default) - V1 API
df.write.format("indextables").save("s3://bucket/path")

// Recommended: V2 API for new projects (proper partition column indexing)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")

// With field type configuration
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.title", "string")     // Exact matching
  .option("spark.indextables.indexing.typemap.content", "text")     // Full-text search
  .option("spark.indextables.indexing.fastfields", "score")         // Fast field access
  .save("s3://bucket/path")

// With auto-sizing (V1 API - recommended for auto-sizing)
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/path")

// V2 API with auto-sizing and explicit row count
val rowCount = df.count()
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "50M")
  .option("spark.indextables.autoSize.inputRowCount", rowCount.toString)
  .save("s3://bucket/path")

// With custom configuration
df.write.format("indextables")
  .option("spark.indextables.indexWriter.batchSize", "20000")
  .option("targetRecordsPerSplit", "500000")
  .save("s3://bucket/path")

// With custom working directory for high-performance storage
df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/fast-nvme/tantivy-temp")
  .option("spark.indextables.indexWriter.batchSize", "20000")
  .save("s3://bucket/path")
```

### Read & Search
```scala
val df = spark.read.format("indextables").load("s3://bucket/path")

// String field exact matching (default behavior - pushed to data source)
df.filter($"title" === "exact title").show()

// Text field exact matching (handled by Spark after data source filtering)
df.filter($"content" === "machine learning").show()  // Exact string match, not tokenized

// Standard DataFrame operations
df.filter($"title".contains("Spark")).show()

// Native Tantivy queries
df.filter($"content" indexquery "machine learning AND spark").show()

// Cross-field search
df.filter($"_indexall" indexquery "apache OR python").show()
```

### Partitioned Datasets
```scala
// Write partitioned data
df.write.format("indextables")
  .partitionBy("load_date", "load_hour")
  .option("spark.indextables.indexing.typemap.message", "text")
  .save("s3://bucket/partitioned-data")

// V2 DataSource API (modern) with custom working directory
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("year", "month", "day")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/fast-storage/tantivy-temp")
  .save("s3://bucket/v2-partitioned")

// Read with partition pruning
val df = spark.read.format("indextables").load("s3://bucket/partitioned-data")
df.filter($"load_date" === "2024-01-01" && $"load_hour" === 10).show()

// Complex queries with partition and content filters
df.filter($"load_date" === "2024-01-01" && $"message" indexquery "error OR warning").show()
```

### SQL
```sql
-- Register extensions (if using SQL)
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Native queries
SELECT * FROM documents WHERE content indexquery 'AI AND (neural OR deep)';
SELECT * FROM documents WHERE _indexall indexquery 'spark AND sql';

-- Partitioned queries with IndexQuery
SELECT * FROM partitioned_data
WHERE load_date = '2024-01-01' AND message indexquery 'error OR warning';

-- Split optimization with automatic skipped files handling
MERGE SPLITS 's3://bucket/path' TARGET SIZE 104857600;  -- 100MB
MERGE SPLITS 's3://bucket/path' MAX GROUPS 10;          -- Limit to 10 merge groups
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M MAX GROUPS 5;  -- Both constraints

-- Partition-aware split optimization
MERGE SPLITS 's3://bucket/partitioned-data'
WHERE load_date = '2024-01-01' AND load_hour = 10
TARGET SIZE 100M;

-- Note: Corrupted or problematic files are automatically skipped with cooldown tracking
```

### Split Optimization
```scala
// Merge splits to reduce small file overhead
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 104857600")

// Target sizes support unit suffixes (M for megabytes, G for gigabytes)
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M")
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G")

// Limit the number of split groups created by a single command
spark.sql("MERGE SPLITS 's3://bucket/path' MAX GROUPS 10")

// Partition-aware optimization - merge only specific partitions
spark.sql("""
  MERGE SPLITS 's3://bucket/partitioned-data'
  WHERE load_date = '2024-01-01' AND load_hour = 10
  TARGET SIZE 100M
""")

// Global optimization across all partitions
spark.sql("MERGE SPLITS 's3://bucket/partitioned-data' TARGET SIZE 100M")

// Combine TARGET SIZE and MAX GROUPS for fine-grained control
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M MAX GROUPS 5")
```

## Aggregate Pushdown

**New in v1.10**: Complete aggregate pushdown support for COUNT(), SUM(), AVG(), MIN(), MAX() operations with transaction log optimization and auto-fast-field configuration.

### Supported Aggregations
- **COUNT()**: Document counting with transaction log optimization
- **SUM()**: Numeric field summation
- **AVG()**: Average calculation
- **MIN()/MAX()**: Minimum and maximum values
- **Multiple aggregations**: Multiple operations in single query

### Fast Field Requirements
Aggregations require fields to be configured as "fast fields" for optimal performance:

```scala
// Configure fast fields for aggregation support
df.write.format("indextables")
  .option("spark.indextables.indexing.fastfields", "score,value,timestamp")
  .save("s3://bucket/path")
```

### Auto-Fast-Field Configuration
**New feature**: When no fast fields are explicitly configured, the first numeric/date field is automatically marked as fast:

```scala
// Auto-fast-field: score field automatically becomes fast
val data = Seq(
  ("doc1", "content1", 100),  // score field auto-configured as fast
  ("doc2", "content2", 200)
).toDF("id", "content", "score")

df.write.format("indextables")
  .save("s3://bucket/path")  // No explicit fast field config needed
```

### Transaction Log Optimization
COUNT queries without filters are optimized using transaction log metadata:

```scala
// Optimized COUNT - uses transaction log, no split access
df.count()  // Fast metadata-based count

// COUNT with filters - uses aggregation pushdown
df.filter($"score" > 50).count()  // Pushdown to splits with filters
```

### Usage Examples

#### Basic Aggregations
```scala
val df = spark.read.format("indextables").load("s3://bucket/path")

// Individual aggregations - all pushed down to tantivy
df.agg(count("*")).show()
df.agg(sum("score")).show()
df.agg(avg("score")).show()
df.agg(min("score"), max("score")).show()
```

#### Multiple Aggregations
```scala
// Multiple aggregations in single query - all pushed down
df.agg(
  count("*").as("total_docs"),
  sum("score").as("total_score"),
  avg("score").as("avg_score"),
  min("score").as("min_score"),
  max("score").as("max_score")
).show()
```

#### With Filters and GroupBy
```scala
// Aggregations with filters - pushed down with filter predicates
df.filter($"category" === "premium")
  .agg(count("*"), avg("score"))
  .show()

// GroupBy aggregations - partial pushdown optimization
df.groupBy("category")
  .agg(count("*"), sum("score"))
  .show()
```

### Configuration Options
```scala
// Explicit fast field configuration for aggregation performance
df.write.format("indextables")
  .option("spark.indextables.indexing.fastfields", "score,value,response_time")
  .option("spark.indextables.indexing.typemap.category", "string")
  .option("spark.indextables.indexing.typemap.description", "text")
  .save("s3://bucket/data")
```

### Performance Benefits
- **10-100x speedup**: Aggregations execute in tantivy instead of pulling all data through Spark
- **Memory efficiency**: No data transfer for COUNT operations using transaction log
- **Native performance**: Leverages tantivy's high-performance aggregation engine
- **Automatic optimization**: Smart field validation and rejection of unsupported operations

### Validation and Error Handling
- **Field validation**: Ensures aggregation fields are properly configured as fast fields
- **Operation validation**: Rejects unsupported aggregation types (e.g., custom functions)
- **Filter validation**: Validates filter predicates are compatible with pushdown
- **Graceful fallback**: Falls back to Spark processing when pushdown not possible

## Schema Support
**Supported**: String (text), Integer/Long (i64), Float/Double (f64), Boolean (i64), Date (date), Timestamp (i64), Binary (bytes)
**Unsupported**: Arrays, Maps, Structs (throws UnsupportedOperationException)

## DataSource API Compatibility

IndexTables4Spark supports both legacy V1 and modern V2 Spark DataSource APIs with full feature parity:

### V1 DataSource API (Legacy)
```scala
// V1 format - compatible with older Spark applications
df.write.format("indextables")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

val df = spark.read.format("indextables").load("s3://bucket/path")
```

### V2 DataSource API (Modern)
```scala
// V2 format - modern Spark 3.x+ with enhanced capabilities
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/path")
```

**Key Benefits of V2 API:**
- Enhanced partition pruning and metadata optimization
- Better integration with Spark's Catalyst optimizer
- Improved schema inference and validation
- Native support for partition-aware operations

Both APIs support identical functionality including partitioned datasets, IndexQuery operations, MERGE SPLITS commands, and all field indexing configurations.

## Transaction Log Operational Best Practices

### **Production Deployment**
- **Use default retention (30 days)** for production systems to ensure recovery capabilities
- **Monitor checkpoint creation frequency** - should occur every 10-50 transactions based on workload
- **Set up alerting** on cleanup failures (logged as warnings but don't break operations)
- **Consider shorter retention** (1-7 days) for high-volume systems with frequent checkpoints

### **Storage Planning**
```scala
// High-volume production (1000+ transactions/day)
df.write.format("indextables")
  .option("spark.indextables.checkpoint.interval", "20")           // More frequent checkpoints
  .option("spark.indextables.logRetention.duration", "604800000") // 7 days retention
  .save("s3://bucket/high-volume-data")

// Conservative production (< 100 transactions/day)
df.write.format("indextables")
  .option("spark.indextables.checkpoint.interval", "50")             // Less frequent checkpoints
  .option("spark.indextables.logRetention.duration", "2592000000")   // 30 days retention
  .save("s3://bucket/conservative-data")
```

### **Development & Testing**
```scala
// Development with faster cleanup for testing
df.write.format("indextables")
  .option("spark.indextables.checkpoint.interval", "5")
  .option("spark.indextables.logRetention.duration", "3600000")     // 1 hour retention
  .option("spark.indextables.cleanup.dryRun", "true")               // Log only, don't delete
  .save("s3://bucket/dev-data")
```

### **Monitoring & Troubleshooting**
- **Checkpoint files**: Look for `*.checkpoint.json` files in `_transaction_log/` directory
- **Last checkpoint**: Check `_last_checkpoint` file for current checkpoint version
- **Cleanup logs**: Monitor for "Cleaned up N old transaction log files" messages
- **Performance**: Measure read times - should improve significantly with active checkpoints

## Architecture
- **File format**: `*.split` files with UUID naming
- **Transaction log**: `_transaction_log/` directory (Delta Lake compatible) with checkpoint compaction
- **Checkpoint system**: Automatic compaction of transaction logs into `*.checkpoint.json` files for performance
- **Parallel I/O**: Configurable thread pools for concurrent transaction log reading from S3
- **Partitioned datasets**: Full partition pruning with metadata optimization
- **Split merging**: Distributed merge operations with REMOVE+ADD transaction patterns
- **Locality tracking**: BroadcastSplitLocalityManager for cluster-wide cache awareness
- **Batch processing**: Uses tantivy4java's BatchDocumentBuilder
- **Caching**: JVM-wide SplitCacheManager with host-based locality and checkpoint-aware invalidation
- **Storage**: S3OptimizedReader for S3, StandardFileReader for local/HDFS with intelligent retry policies
- **IndexQuery cache**: Schema-based LRU cache (1000 entries) with stable relation keys derived from field names and types

## Implementation Status
- ✅ **Core features**: Transaction log, optimized writes, IndexQuery operators, merge splits
- ✅ **Production ready**: IndexQuery (49/49 tests), IndexQueryAll (44/44 tests), MergeSplits (9/9 tests)
- ✅ **Aggregate pushdown**: Complete COUNT/SUM/AVG/MIN/MAX support with transaction log optimization and auto-fast-field configuration (14/14 tests passing)
- ✅ **Partitioned datasets**: Full partitioned table support with comprehensive test suite (7/7 tests)
- ✅ **V1/V2 DataSource compatibility**: Both APIs fully functional with partitioning support
- ✅ **Direct merge operations**: In-process merge architecture with efficient split consolidation
- ✅ **Split optimization**: SQL-based merge commands with MAX GROUPS limits and partition-aware operations
- ✅ **Skipped files handling**: Robust merge operation resilience with cooldown tracking (5/5 tests passing)
- ✅ **Broadcast locality**: Cluster-wide cache locality management
- ✅ **Schema validation**: Field type compatibility checks prevent configuration conflicts
- ✅ **Field type filtering**: Intelligent filter pushdown based on field type capabilities (478/478 tests passing)
- ✅ **Transaction log compaction**: Complete Delta Lake-style checkpoint system with parallel S3 retrieval (6/6 tests passing)
- ✅ **Incremental transaction reading**: Full checkpoint + incremental workflow working perfectly
- ✅ **Performance optimization**: 60% transaction log read improvement validated in production tests
- ✅ **Working directory configuration**: Custom root working area support for index creation and split operations with validation and fallback
- ✅ **Parallel streaming uploads**: Multi-threaded S3 uploads with configurable concurrency and memory-efficient buffering
- ✅ **Enterprise configuration hierarchy**: Complete write option/spark property/table property configuration chain with validation
- ✅ **Custom credential providers**: Full AWS credential provider integration with table-level URI validation and comprehensive real S3 testing (4/4 tests passing)
- ✅ **Data skipping optimization**: Unified data skipping logic across all scan types with proper schema awareness and field type detection
- ✅ **Aggregate cache locality**: Full cache locality support for both simple and GROUP BY aggregate operations
- ✅ **Schema-based IndexQuery cache**: Simplified cache implementation using schema hashing for stable relation keys (35/35 tests passing)
- **Next**: Enhanced GroupBy aggregation optimization, additional performance improvements

## Latest Updates

### **v1.15 - Transaction Log Compression**
- **GZIP compression**: Transaction log files and checkpoints now use GZIP compression by default
- **60-70% storage reduction**: Dramatic reduction in S3 storage costs for transaction logs
- **2-3x compression ratio**: Typical JSON transaction logs compress efficiently
- **Faster S3 downloads**: Smaller file sizes result in 2-3x faster downloads
- **Backward compatible**: Automatically reads both compressed and uncompressed files
- **Transparent operation**: No code changes required - compression enabled by default
- **Magic byte detection**: Files prefixed with `0x01 0x01` for automatic format detection
- **Configurable compression level**: GZIP levels 1-9 supported (default: 6)
- **Mixed-mode support**: Tables can contain mix of compressed and uncompressed files
- **Checkpoint compression**: Checkpoints also benefit from compression
- **No external dependencies**: Uses standard Java GZIP streams
- **Minimal write overhead**: ~5-10% overhead offset by faster S3 transfers
- **Production ready**: All 228+ tests passing with compression enabled

### **v1.14 - Data Skipping Statistics Truncation**
- **Automatic statistics truncation**: Prevents transaction log bloat by dropping min/max statistics for columns with values exceeding 256 characters
- **Enabled by default**: Automatic optimization for all writes without configuration required
- **98% transaction log size reduction**: Dramatic reduction in transaction file sizes for tables with long text fields
- **10-100x faster transaction log reads**: Smaller files load significantly faster from S3
- **Conservative approach**: Statistics completely dropped (not truncated) to avoid misleading partial data that could cause incorrect query results
- **Graceful fallback**: Data skipping automatically handles missing statistics by conservatively including files
- **Configurable threshold**: `spark.indextables.stats.truncation.maxLength` allows customization (default: 256 characters)
- **No data loss**: Only statistics metadata affected, all data remains fully readable and queryable
- **Minimal write overhead**: <1% performance impact during write operations
- **Comprehensive testing**: 13 unit tests covering all edge cases and configuration scenarios

### **v1.13 - Schema-Based IndexQuery Cache Implementation**
- **Schema-based cache keys**: Revolutionary simplified approach using schema hash instead of execution ID or table path
- **Stable relation identification**: Keys generated from `hash(field1:type1,field2:type2,...)` ensuring consistency across planning phases
- **No execution ID required**: Eliminates timing dependencies between Catalyst optimization and physical execution phases
- **No table path extraction**: Removes complex reflection-based path extraction and URI protocol stripping
- **Universal availability**: Schema accessible in both V2IndexQueryExpressionRule and ScanBuilder contexts
- **Natural query isolation**: Different schemas automatically generate different cache keys for concurrent queries
- **Multi-table support**: Each relation in JOIN queries has unique schema, preventing filter cross-contamination
- **Production validation**: 35/35 IndexQuery tests passing including aggregates, integration tests, and cache tests
- **Simplified implementation**: Reduced from 50+ lines of path/execution extraction to 3 lines of schema hashing
- **Key format**: `relation_${hashCode}_${fieldCount}` where hashCode is deterministic hash of schema string

### **v1.12 - Data Skipping & Aggregate Optimization**
- **Unified data skipping architecture**: All scan types (regular, simple aggregate, GROUP BY aggregate) now use shared data skipping logic with proper schema awareness
- **Fixed date field filtering**: Resolved critical schema passing issue where aggregate scans used empty schemas, causing date field type detection to fail
- **Corrected filter logic**: Fixed AND vs OR logic error in `canFileMatchFilters` that was incorrectly using `filters.exists` instead of `filters.forall`
- **V2 DataSource API consistency**: Updated all tests to use V2 API ("io.indextables.spark.core.IndexTables4SparkTableProvider") for proper partition column indexing
- **Cache locality for aggregates**: Implemented `preferredLocations()` for both `IndexTables4SparkSimpleAggregatePartition` and `IndexTables4SparkGroupByAggregatePartition`
- **Schema-aware field detection**: Proper `DateType` detection enables correct date-to-days-since-epoch conversion for accurate comparison
- **Aggregate pushdown reliability**: COUNT operations now return correct results instead of 0, with proper data skipping applied
- **Production validation**: All date filtering tests now pass with correct aggregate behavior and optimized performance

### **v1.11 - Optimized Transaction Log Implementation**
- **Complete optimization framework**: OptimizedTransactionLog with advanced caching, parallel operations, and memory optimizations
- **FileSystem caching issue resolved**: Fixed consistency problems with version listing during overwrite operations through consistent version passing
- **Enhanced caching system**: Multi-level Guava-based caches with TTL for logs, snapshots, file lists, metadata, versions, and checkpoints
- **Parallel operations**: Configurable parallel file listing, version reading, and batch operations with thread pool management
- **Memory-optimized operations**: Streaming checkpoint creation and efficient state reconstruction
- **Advanced optimizations**: Backward listing optimization, incremental checksums, async updates with staleness tolerance
- **Factory pattern integration**: Automatic selection of optimized implementation via TransactionLogFactory with seamless backward compatibility
- **Adapter pattern**: TransactionLogAdapter enables drop-in replacement without changing existing code
- **Configuration**: All optimizations configurable via spark.indextables.* properties (remapped from spark.indextables.* for consistency)
- **Production ready**: 5/5 tests passing including complex overwrite scenarios and factory integration
- **Thread pool infrastructure**: Centralized thread pool management with dedicated pools for different operation types

### **v1.10 - Complete Aggregate Pushdown Implementation**
- **Full aggregation support**: COUNT(), SUM(), AVG(), MIN(), MAX() with tantivy4java native execution
- **Transaction log optimization**: COUNT queries without filters use metadata for 100x speedup
- **Auto-fast-field configuration**: Automatic configuration of first numeric/date field as fast when no explicit configuration provided
- **Multiple aggregations**: Support for multiple aggregation operations in single query with proper cache isolation
- **Smart validation**: Field validation ensures aggregation fields are properly configured as fast fields with graceful fallback
- **Complete interface compliance**: Full implementation of Spark's SupportsPushDownAggregates with proper rejection logic
- **Production-ready performance**: 10-100x speedup for aggregation operations with comprehensive test coverage (14/14 tests passing)

### **v1.9 - Custom AWS Credential Provider Integration**
- **Custom credential provider support**: Full integration with enterprise credential management systems via reflection
- **Table-level URI consistency**: Credential providers receive table paths (not file paths) for consistent caching behavior
- **AWS SDK version agnostic**: Supports both v1 (`AWSCredentialsProvider`) and v2 (`AwsCredentialsProvider`) interfaces automatically
- **Comprehensive validation**: Real S3 integration tests with table path validation ensuring URIs never contain file extensions or file patterns
- **URI scheme normalization**: Proper `s3a://` to `s3://` conversion while preserving table-level path structure
- **Configuration hierarchy**: Full support for DataFrame options, Spark config, and Hadoop configuration sources
- **Production recommendations**: Write operations use custom providers reliably; read operations recommended to use explicit credentials for distributed reliability


### **v1.5 - Performance & Configuration Enhancements**
- **Parallel streaming uploads**: Revolutionary buffered chunking strategy for S3 multipart uploads
- **Working directory configuration**: Enterprise-grade control over temporary file locations
- **Enhanced S3 performance**: Configurable upload concurrency with intelligent fallback
- **Comprehensive configuration hierarchy**: Full support for write options, Spark properties, and Hadoop configuration
- **Production-ready validation**: Path validation, automatic fallback, and process isolation
- **Performance tuning guide**: Complete documentation with environment-specific recommendations

### **v1.4 - Temporary Directory Control**
- **Custom temporary directories**: Support for high-performance storage (NVMe, memory filesystems)
- **Split merge optimization**: Custom working areas for merge operations
- **Validation and safety**: Directory existence, writability checks with graceful fallback

### **v1.3 - Large File Upload Optimization**
- **Memory-efficient streaming**: Support for 4GB+ files without OOM errors
- **Intelligent upload strategy**: Automatic single-part vs multipart selection
- **S3 performance optimization**: Parallel part uploads with retry logic

## Transaction Log Performance & Behavior

### High-Performance Compaction System
**New in v1.2**: The transaction log now uses Delta Lake-inspired checkpoint compaction for dramatic performance improvements:

- **Checkpoint Creation**: Every N transactions (configurable), the system creates a consolidated `*.checkpoint.json` file
- **Parallel Retrieval**: Transaction log files are read concurrently using configurable thread pools
- **Intelligent Caching**: Checkpoint-aware cache invalidation ensures data consistency while maximizing performance
- **Automatic Cleanup**: Old transaction log files are cleaned up based on retention policies after checkpoint creation

### Performance Characteristics
- **Sequential Reads (Pre-v1.2)**: O(n) where n = number of transactions (~1,300ms for 50 transactions)
- **Checkpoint Reads (v1.2+)**: O(1) for checkpoint + O(k) for incremental changes, where k << n (~500ms for 50 transactions)
- **Performance Improvement**: **60% faster** (2.5x speedup) validated in comprehensive tests
- **Parallel I/O**: Configurable concurrency (default: 4 threads) for remaining transaction files
- **S3 Optimization**: Reduced API calls through intelligent batching and retry policies
- **Scalability**: Performance improvements increase with transaction count due to checkpoint efficiency

### Transaction Log Behavior
**Overwrite Operations**: Reset visible data completely, removing all previous files from transaction log
**Merge Operations**: Consolidate only files visible at merge time (respects overwrite boundaries)
**Read Behavior**: Leverages checkpoints for base state, then applies incremental changes
**Checkpoint Strategy**: Automatically created based on transaction count with configurable intervals

### Transaction Log File Management & Retention

#### **Automatic File Cleanup (Ultra-Conservative by Design)**
Transaction files are automatically cleaned up following a safety-first approach that prioritizes data consistency:

**Deletion Criteria**: Files are deleted ONLY when ALL conditions are met:
- ✅ **Age Requirement**: `fileAge > logRetentionDuration` (default: 30 days)
- ✅ **Checkpoint Inclusion**: `version < checkpointVersion` (file contents preserved in checkpoint)
- ✅ **Version Safety**: `version < currentVersion` (not actively being written)

**Multiple Safety Gates Prevent Data Loss**:
```scala
// Transaction file deleted ONLY if:
if (fileAge > logRetentionDuration &&
    version < checkpointVersion &&
    version < currentVersion) {
  deleteFile() // Safe to delete
}
```

#### **Retention Configuration**
```scala
// Conservative (Production Default)
"spark.indextables.logRetention.duration" -> "2592000000"  // 30 days

// Moderate (High-Volume Production)
"spark.indextables.logRetention.duration" -> "86400000"   // 1 day

// Aggressive (Development/Testing)
"spark.indextables.logRetention.duration" -> "3600000"    // 1 hour

// Checkpoint Files
"spark.indextables.checkpointRetention.duration" -> "7200000" // 2 hours
```

#### **Environment-Specific Behavior**

**🏢 Production Environment:**
- **Month 1-30**: All transaction files preserved (safety period)
- **Month 2+**: Pre-checkpoint files gradually cleaned up based on age
- **Long-term**: Only recent incremental files + checkpoints maintained
- **Storage Pattern**: Gradual, predictable cleanup prevents storage runaway

**🧪 Development/Testing:**
- **Files rarely deleted**: Too new for retention period in typical dev cycles
- **Safe rapid iteration**: No data loss during active development
- **Configurable cleanup**: Shorter retention for testing scenarios

#### **Data Consistency Guarantees**
**Pre-Checkpoint Reading Optimization**: When checkpoints exist, transaction log reading:
- ✅ **Loads base state from checkpoint** (O(1) operation)
- ✅ **Skips reading pre-checkpoint transaction files** (major performance gain)
- ✅ **Only reads incremental transactions after checkpoint** (minimal I/O)
- ✅ **Maintains complete data consistency** even when old files are cleaned up
- ✅ **Graceful failure handling**: Cleanup failures never break transaction log operations

**Storage Safety**: All data remains accessible regardless of cleanup timing:
- **Checkpoint redundancy**: Multiple checkpoint versions can coexist
- **Incremental preservation**: Post-checkpoint transactions always preserved
- **Failure isolation**: Individual file cleanup failures don't affect others

**Example sequence with checkpoints:**
1. `add1(append)` + `add2(append)` → visible: add1+add2
2. `add3(overwrite)` → visible: add3 only (add1+add2 invisible)
3. `add4(append)` → visible: add3+add4
4. **`checkpoint()`** → creates consolidated checkpoint file with add3+add4 state
5. `add5(append)` + `add6(append)` → checkpoint loads add3+add4, then applies add5+add6
6. Old transaction files cleaned up based on retention policy

## Breaking Changes & Migration

### v1.1 Field Type Changes
- **Default string field type changed from `text` to `string`** for exact matching behavior
- **Existing tables**: Continue to work with their original field type configuration
- **New tables**: Use `string` fields by default unless explicitly configured

### Migration Guide
```scala
// Pre-v1.1 behavior (text fields by default)
// No configuration needed - was automatic

// v1.1+ equivalent behavior
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.content", "text")  // Explicit text type
  .save("path")

// v1.1+ recommended (new default)
df.write.format("indextables")
  // No configuration - defaults to string fields for exact matching
  .save("path")
```

## Performance Benchmarks

### Transaction Log Performance (v1.2)
Based on comprehensive performance tests with full checkpoint + incremental workflow:

- **Sequential I/O (Pre-v1.2)**: ~1,300ms average read time (50 transactions)
- **Checkpoint + Parallel I/O (v1.2+)**: ~520ms average read time (50 transactions)
- **Performance Improvement**: **60% faster** (2.5x speedup)
- **Test Coverage**: 6/6 performance tests passing, including cleanup validation

**Key Performance Factors:**
- **Thread Pool Utilization**: Configurable parallelism (default: 4 threads)
- **S3 API Optimization**: Reduced round-trips through concurrent reads
- **Checkpoint Efficiency**: O(1) base state loading + O(k) incremental changes
- **Pre-checkpoint Avoidance**: Zero unnecessary file reads (validated in tests)
- **Intelligent Caching**: Checkpoint-aware cache invalidation reduces redundant I/O

**Scaling Characteristics:**
- **Performance improvements increase with transaction count** due to checkpoint efficiency
- **Memory usage remains constant** through checkpoint compaction
- **S3 performance scales linearly** with parallelism configuration
- **Storage growth controlled** through automatic retention-based cleanup

**Production Validation:**
- ✅ **Complete checkpoint + incremental workflow working**
- ✅ **Data consistency maintained during cleanup operations**
- ✅ **Ultra-conservative deletion policies prevent data loss**
- ✅ **Graceful failure handling for all cleanup scenarios**

## Performance Tuning Guide

### Configuration Hierarchy and Best Practices

**Configuration Priority (Highest to Lowest):**
1. **Write Options**: `.option("spark.indextables.setting", "value")`
2. **Spark Session**: `spark.conf.set("spark.indextables.setting", "value")`
3. **Hadoop Configuration**: Set via `hadoopConf.set(...)`
4. **System Defaults**: Built-in fallback values

### Performance Optimization Strategies

#### **1. Storage Directory Optimization**
```scala
// Automatic optimization (recommended) - uses /local_disk0 when available
// No configuration needed on Databricks, EMR, or systems with /local_disk0

// NVMe SSD for maximum I/O performance
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/fast-nvme/tantivy")
spark.conf.set("spark.indextables.merge.tempDirectoryPath", "/fast-nvme/tantivy-merge")
spark.conf.set("spark.indextables.cache.directoryPath", "/fast-nvme/tantivy-cache")

// Memory filesystem for extreme performance (RAM permitting)
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/dev/shm/tantivy")
spark.conf.set("spark.indextables.cache.directoryPath", "/dev/shm/tantivy-cache")
```

#### **2. Upload Performance Tuning**
```scala
// High-throughput uploads for large datasets
spark.conf.set("spark.indextables.s3.maxConcurrency", "16")          // Parallel uploads
spark.conf.set("spark.indextables.s3.partSize", "268435456")         // 256MB parts
spark.conf.set("spark.indextables.s3.multipartThreshold", "52428800") // 50MB threshold
```

#### **3. Index Writer Optimization**
```scala
// Large batch processing for high-volume writes
spark.conf.set("spark.indextables.indexWriter.batchSize", "50000")   // Large batches
spark.conf.set("spark.indextables.indexWriter.heapSize", "500000000") // 500MB heap
spark.conf.set("spark.indextables.indexWriter.threads", "4")         // Parallel indexing
```

#### **4. Transaction Log Performance**
```scala
// Optimized for high-frequency writes
spark.conf.set("spark.indextables.checkpoint.enabled", "true")
spark.conf.set("spark.indextables.checkpoint.interval", "10")        // Frequent checkpoints
spark.conf.set("spark.indextables.checkpoint.parallelism", "8")      // Parallel I/O
```

### Environment-Specific Recommendations

#### **Databricks**
```scala
// Automatic optimization (recommended) - no configuration needed!
// Uses /local_disk0 automatically when available

// Manual optimization (optional)
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
spark.conf.set("spark.indextables.cache.directoryPath", "/local_disk0/tantivy-cache")
spark.conf.set("spark.indextables.s3.maxConcurrency", "8")
spark.conf.set("spark.indextables.indexWriter.batchSize", "25000")
// Enable optimized transaction log
spark.conf.set("spark.indextables.parallel.read.enabled", "true")
spark.conf.set("spark.indextables.async.updates.enabled", "true")
```

#### **EMR/EC2 with Instance Storage**
```scala
// Automatic /local_disk0 detection where available, otherwise manual configuration:
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/mnt/tmp/tantivy")
spark.conf.set("spark.indextables.cache.directoryPath", "/mnt/tmp/tantivy-cache")
spark.conf.set("spark.indextables.s3.maxConcurrency", "12")
spark.conf.set("spark.indextables.s3.partSize", "134217728")          // 128MB
```

#### **On-Premises with High-Performance Storage**
```scala
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/fast-storage/tantivy")
spark.conf.set("spark.indextables.cache.directoryPath", "/fast-storage/tantivy-cache")
spark.conf.set("spark.indextables.s3.maxConcurrency", "16")
spark.conf.set("spark.indextables.indexWriter.heapSize", "1000000000") // 1GB heap
```

### Monitoring and Troubleshooting

#### **Key Performance Indicators**
- **Index creation time**: Monitor temporary directory I/O performance
- **Upload throughput**: Track MB/s rates in logs for S3 uploads
- **Transaction log read times**: Should improve significantly with checkpoints
- **Split merge performance**: Watch for optimal bin packing in merge operations

#### **Common Performance Issues**
- **Slow index creation**: Check temporary directory is on fast storage
- **Upload bottlenecks**: Increase `maxConcurrency` and `partSize` for large files
- **Memory issues**: Reduce `batchSize` or increase executor memory
- **Transaction log slowness**: Enable checkpoints and increase parallelism

## Important Notes
- **tantivy4java integration**: Pure Java bindings, no Rust compilation needed
- **AWS support**: Full session token support for temporary credentials
- **Merge compression**: Tantivy achieves 30-70% size reduction through deduplication
- **Distributed operations**: Serializable AWS configs for executor-based merge operations
- **Error handling**: Comprehensive validation with descriptive error messages
- **Merge resilience**: Robust handling of corrupted files with automatic skipping, cooldown tracking, and eventual retry
- **Direct merge operations**: Efficient in-process merge architecture for split consolidation
- **Performance**: Batch processing, predictive I/O, smart caching, broadcast locality, parallel transaction log processing, configurable working directories, unified data skipping across all scan types
- **Transaction log performance**: Delta Lake-level performance with parallel operations, advanced caching, and streaming optimizations
- **Data safety**: Multiple safety gates prevent data loss, graceful failure handling for all operations, skipped files never marked as removed
- **Production readiness**: Complete test coverage for all major features including parallel uploads, working directory configuration, skipped files handling, and aggregate cache locality optimization

---

**Instructions**: Do exactly what's asked, nothing more. Prefer editing existing files over creating new ones. Never create documentation files unless explicitly requested.
