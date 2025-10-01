# CLAUDE.md

**IndexTables4Spark** is a high-performance Spark DataSource implementing fast full-text search using Tantivy via tantivy4java. It runs embedded in Spark executors without server-side components.

## Note
- This is the only way to run a single test for this project:  mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.DateStringFilterValidationTest'

## Key Features
- **Split-based architecture**: Write-only indexes with QuickwitSplit format
- **Transaction log**: Delta Lake-style with atomic operations and high-performance compaction
- **Transaction log compaction**: Automatic checkpoint creation with parallel S3 retrieval for scalable performance
- **Aggregate pushdown**: Complete support for COUNT(), SUM(), AVG(), MIN(), MAX() aggregations with transaction log optimization and auto-fast-field configuration
- **Partitioned datasets**: Full support for partitioned tables with partition pruning and WHERE clauses
- **Direct merge operations**: In-process merge architecture for efficient split consolidation
- **Merge splits optimization**: SQL-based split consolidation with intelligent bin packing, configurable limits, partition-aware operations, and robust skipped files handling
- **Broadcast locality management**: Cluster-wide cache locality tracking for optimal task scheduling
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'` and `_indexall indexquery 'query'`)
- **Optimized writes**: Automatic split sizing with adaptive shuffle
- **Auto-sizing**: Intelligent DataFrame partitioning based on historical split analysis with 28/28 tests passing
- **V1/V2 DataSource compatibility**: Both legacy and modern Spark DataSource APIs fully supported (V2 recommended for partition column indexing)
- **S3-optimized storage**: Intelligent caching and session token support with parallel streaming uploads
- **Working directory configuration**: Custom root working areas for index creation and split operations
- **Parallel upload performance**: Multi-threaded S3 uploads with configurable concurrency and memory-efficient streaming
- **Schema-aware filtering**: Field validation prevents native crashes and ensures compatibility with unified data skipping across all scan types
- **High-performance I/O**: Parallel transaction log reading with configurable concurrency and retry policies
- **Enterprise-grade configurability**: Comprehensive configuration hierarchy with validation and fallback mechanisms
- **100% test coverage**: 205 tests passing, 0 failing, comprehensive partitioned dataset test suite, aggregate pushdown validation, and custom credential provider integration tests

## Build & Test
```bash
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
- `spark.indextables.cache.prewarm.enabled`: `true` (Enable proactive cache warming)
- `spark.indextables.docBatch.enabled`: `true` (Enable batch document retrieval for better performance)
- `spark.indextables.docBatch.maxSize`: `1000` (Maximum documents per batch)
- `spark.indextables.optimizeWrite.targetRecordsPerSplit`: `1000000`

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

### Working Directory & Cache Configuration

**New in v1.5**: Custom working directory support for index creation and split operations provides control over where temporary files are stored during processing.

**New in v1.6**: Automatic `/local_disk0` detection and cache directory override for optimal performance on Databricks and high-performance storage environments.

#### Directory Settings
- `spark.indextables.indexWriter.tempDirectoryPath`: Custom working directory for index creation during writes (default: auto-detect `/local_disk0` or system temp)
- `spark.indextables.merge.tempDirectoryPath`: Custom temporary directory for split merge operations (default: auto-detect `/local_disk0` or system temp)
- `spark.indextables.cache.directoryPath`: Custom split cache directory for downloaded files (default: auto-detect `/local_disk0` or system temp)

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

// Merge splits with custom temporary directory
spark.conf.set("spark.indextables.merge.tempDirectoryPath", "/fast-nvme/merge-temp")
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
- **Next**: Enhanced GroupBy aggregation optimization, additional performance improvements

## Latest Updates

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
