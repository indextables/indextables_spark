# CLAUDE.md

**IndexTables4Spark** is a high-performance Spark DataSource implementing fast full-text search using Tantivy via tantivy4java. It runs embedded in Spark executors without server-side components.

## Essential Commands

### Build & Test
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # Set Java 11
mvn clean compile  # Build
mvn test          # Run tests
# Run single test:
mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.DateStringFilterValidationTest'
```

## Core Features

- **Split-based architecture**: Write-only indexes with QuickwitSplit format
- **Transaction log**: Delta Lake-style with atomic operations, checkpoints, and GZIP compression
- **Aggregate pushdown**: COUNT(), SUM(), AVG(), MIN(), MAX() with transaction log optimization
- **Bucket aggregations**: DateHistogram, Histogram, and Range bucketing via SQL functions for time-series and distribution analysis
- **Partitioned datasets**: Full support with partition pruning
- **Merge operations**: SQL-based split consolidation (`MERGE SPLITS`) with Scala-side I/O (priority queuing, async downloads)
- **Drop partitions**: SQL-based partition removal with WHERE clause validation (`DROP INDEXTABLES PARTITIONS`)
- **Purge operations**: SQL-based cleanup of orphaned splits and old transaction logs (`PURGE INDEXTABLE`)
- **Purge-on-write**: Automatic table hygiene during write operations
- **Cache prewarming**: SQL-based (`PREWARM INDEXTABLES CACHE`) and read-time prewarming with segment/field selection
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'`)
- **V2 DataSource API**: Recommended for partition column indexing
- **Multi-cloud**: S3 and Azure Blob Storage with native authentication
- **JSON field support**: Native Struct/Array/Map fields with filter pushdown and configurable indexing modes (114/114 tests passing)
- **Statistics truncation**: Automatic optimization for long text fields (enabled by default)
- **Batch retrieval optimization**: 90-95% reduction in S3 GET requests for read operations, 2-3x faster (enabled by default)
- **L2 Disk Cache**: Persistent NVMe caching across JVM restarts, 10-50x faster repeated queries (auto-enabled on Databricks/EMR when `/local_disk0` detected, use `DESCRIBE INDEXTABLES DISK CACHE` to monitor)

## Key Configuration Settings

### Essential Settings
```scala
// Index Writer
spark.indextables.indexWriter.heapSize: "100M" (supports "2G", "500M", "1024K")
spark.indextables.indexWriter.batchSize: 10000
spark.indextables.indexWriter.maxBatchBufferSize: "90M" (default: 90MB, prevents native 100MB limit errors)
spark.indextables.indexWriter.threads: 2

// Split Conversion (controls parallelism of tantivy index -> quickwit split conversion)
spark.indextables.splitConversion.maxParallelism: <auto> (default: max(1, availableProcessors))

// Transaction Log
spark.indextables.checkpoint.enabled: true
spark.indextables.checkpoint.interval: 10
spark.indextables.transaction.compression.enabled: true (default)

// Transaction Log Concurrent Write Retry (automatic retry on version conflicts)
spark.indextables.transaction.retry.maxAttempts: 10 (default: 10, max retry attempts on conflict)
spark.indextables.transaction.retry.baseDelayMs: 100 (default: 100ms, initial backoff delay)
spark.indextables.transaction.retry.maxDelayMs: 5000 (default: 5000ms, maximum backoff cap)

// Statistics Truncation (enabled by default)
spark.indextables.stats.truncation.enabled: true
spark.indextables.stats.truncation.maxLength: 32

// Data Skipping Statistics (Delta Lake compatible)
spark.indextables.dataSkippingStatsColumns: <column_list> (comma-separated, takes precedence over numIndexedCols)
spark.indextables.dataSkippingNumIndexedCols: 32 (default: 32, -1 for all eligible columns, 0 to disable)

// Merge-On-Write (automatic split consolidation during writes via Spark shuffle)
spark.indextables.mergeOnWrite.enabled: false (default: false)
spark.indextables.mergeOnWrite.targetSize: "4G" (default: 4G, target merged split size)
spark.indextables.mergeOnWrite.mergeGroupMultiplier: 2.0 (default: 2.0, threshold = parallelism × multiplier)
spark.indextables.mergeOnWrite.minDiskSpaceGB: 20 (default: 20GB, use 1GB for tests)
spark.indextables.mergeOnWrite.maxConcurrentMergesPerWorker: <auto> (default: auto-calculated based on heap size)
spark.indextables.mergeOnWrite.memoryOverheadFactor: 3.0 (default: 3.0, memory overhead multiplier for merge size)

// Merge I/O (Scala-side download/upload for merge operations)
// Downloads source splits to local disk before merge, uploads merged split after
spark.indextables.merge.download.maxConcurrencyPerCore: 8 (default: 8 concurrent downloads per CPU core)
spark.indextables.merge.download.memoryBudget: "2G" (default: 2GB memory budget per executor)
spark.indextables.merge.download.retries: 3 (default: 3 retry attempts with exponential backoff)
spark.indextables.merge.upload.maxConcurrency: 6 (default: 6 concurrent upload threads)

// Purge-On-Write (automatic cleanup of orphaned files and old transaction logs)
spark.indextables.purgeOnWrite.enabled: false (default: false)
spark.indextables.purgeOnWrite.triggerAfterMerge: true (default: true, run purge after merge-on-write)
spark.indextables.purgeOnWrite.triggerAfterWrites: 0 (default: 0 = disabled, run purge after N writes)
spark.indextables.purgeOnWrite.splitRetentionHours: 168 (default: 168 = 7 days)
spark.indextables.purgeOnWrite.txLogRetentionHours: 720 (default: 720 = 30 days)

// Batch Retrieval Optimization (reduces S3 requests by 90-95% for read operations)
spark.indextables.read.batchOptimization.enabled: true (default: true, transparent optimization)
spark.indextables.read.batchOptimization.profile: "balanced" (options: conservative, balanced, aggressive, disabled)
spark.indextables.read.batchOptimization.maxRangeSize: "16M" (default: 16MB, range: 2MB-32MB)
spark.indextables.read.batchOptimization.gapTolerance: "512K" (default: 512KB, range: 64KB-2MB)
spark.indextables.read.batchOptimization.minDocsForOptimization: 50 (default: 50, range: 10-200)
spark.indextables.read.batchOptimization.maxConcurrentPrefetch: 8 (default: 8, per-batch-operation, range: 2-32)

// Adaptive Tuning (automatic parameter optimization based on performance metrics)
spark.indextables.read.adaptiveTuning.enabled: true (default: true, auto-adjust parameters)
spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment: 5 (default: 5, minimum batches to track)

// L2 Disk Cache (persistent NVMe caching across JVM restarts)
// Auto-enabled when /local_disk0 is detected (Databricks/EMR NVMe storage)
spark.indextables.cache.disk.enabled: <auto> (default: auto-enabled when /local_disk0 detected, set to "false" to disable)
spark.indextables.cache.disk.path: <auto> (default: "/local_disk0/tantivy4spark_slicecache" when auto-detected)
spark.indextables.cache.disk.maxSize: "100G" (default: 0 = auto, 2/3 available disk)
spark.indextables.cache.disk.compression: "lz4" (options: lz4, zstd, none)
spark.indextables.cache.disk.minCompressSize: "4K" (default: 4096 bytes)
spark.indextables.cache.disk.manifestSyncInterval: 30 (default: 30 seconds)

// Read Limits (controls default result set size when no explicit LIMIT is specified)
spark.indextables.read.defaultLimit: 250 (default: 250, maximum documents per partition when no LIMIT pushed down)

// Partition Pruning Optimization (reduces complexity from O(n*f) to O(p*f) where n=files, p=partitions, f=filters)
spark.indextables.partitionPruning.filterCacheEnabled: true (default: true, LRU cache for filter evaluations)
spark.indextables.partitionPruning.indexEnabled: true (default: true, O(1) index lookup for equality/IN filters)
spark.indextables.partitionPruning.parallelThreshold: 100 (default: 100, parallelize when unique partitions > threshold)
spark.indextables.partitionPruning.selectivityOrdering: true (default: true, evaluate most selective filters first)

// Data Skipping Optimization (reduces file scans using min/max statistics)
// Features: IN filter optimization, expression simplification, filter caching, metrics tracking
// Use DESCRIBE INDEXTABLES DATA SKIPPING STATS to view effectiveness metrics
// Use FLUSH INDEXTABLES DATA SKIPPING STATS to reset metrics
// Use INVALIDATE INDEXTABLES DATA SKIPPING CACHE to clear caches

// String Pattern Filter Pushdown (all disabled by default)
// Enable these to allow aggregate pushdown with pattern filters
// Note: These match individual tokens, not full strings for text fields
spark.indextables.filter.stringPattern.pushdown: false (master switch - enables all three below)
spark.indextables.filter.stringStartsWith.pushdown: false (efficient - uses sorted index terms)
spark.indextables.filter.stringEndsWith.pushdown: false (less efficient - requires term scanning)
spark.indextables.filter.stringContains.pushdown: false (least efficient - cannot leverage index structure)
```

### Working Directories (auto-detects `/local_disk0` when available)
```scala
spark.indextables.indexWriter.tempDirectoryPath: "/local_disk0/temp" (or auto-detect)
spark.indextables.cache.directoryPath: "/local_disk0/cache" (or auto-detect)
spark.indextables.merge.tempDirectoryPath: "/local_disk0/merge-temp" (or auto-detect)
```

### S3 Configuration
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

### Azure Configuration
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

### Unity Catalog Integration (Databricks)
Access Unity Catalog-managed S3 paths using temporary credentials from the Databricks API.

**Provider class:** `io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider`

#### Configuration Properties
| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `spark.indextables.databricks.workspaceUrl` | Yes | - | Databricks workspace URL |
| `spark.indextables.databricks.apiToken` | Yes | - | Databricks API token (PAT or OAuth) |
| `spark.indextables.databricks.credential.refreshBuffer.minutes` | No | 40 | Minutes before expiration to refresh |
| `spark.indextables.databricks.cache.maxSize` | No | 100 | Maximum cached credential entries |
| `spark.indextables.databricks.fallback.enabled` | No | true | Fallback to READ if READ_WRITE fails |
| `spark.indextables.databricks.retry.attempts` | No | 3 | Retry attempts on API failure |

#### Usage Examples

**Session-level configuration:**
```scala
// Configure Databricks credentials
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

**Per-operation configuration:**
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
  .option("spark.indextables.databricks.apiToken", token)
  .option("spark.indextables.aws.credentialsProviderClass",
    "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")
  .save("s3://bucket/path")
```

**Key features:**
- **Driver-side resolution:** Credentials resolved on driver, executors use pre-resolved credentials directly (no network calls to Databricks from workers)
- **Multi-user caching:** Credentials cached per API token + path, so different users get separate credentials
- **Automatic fallback:** If READ_WRITE fails (403), automatically falls back to READ credentials
- **Expiration-aware:** Refreshes credentials 40 minutes before expiration (configurable)
- **No SDK dependency:** Uses HTTP API directly, no Databricks SDK required
- **Credential priority:** Explicit credentials always take precedence over provider class (enables driver-side resolution)

## Field Indexing

### Field Types
```scala
// String fields (default) - exact matching, full filter pushdown
spark.indextables.indexing.typemap.<field>: "string"

// Text fields - full-text search, IndexQuery only
spark.indextables.indexing.typemap.<field>: "text"

// JSON fields - automatic for Struct/Array/Map types
// No configuration needed - auto-detected
// Optional: Control JSON indexing mode
spark.indextables.indexing.json.mode: "full" (default) or "minimal"
```

#### List-Based Typemap Syntax (Recommended)
Configure multiple fields with the same type in one line:
```scala
// New syntax: typemap.<type> = "field1,field2,..."
spark.indextables.indexing.typemap.text: "title,content,body,description"
spark.indextables.indexing.typemap.string: "status,category,tags"
spark.indextables.indexing.typemap.json: "metadata,attributes"

// Old per-field syntax still works for typemap
spark.indextables.indexing.typemap.title: "text"
```

### Index Record Options (for text fields)
Controls what information is stored in the inverted index for text fields:
- `basic` - Document IDs only (smallest index)
- `freq` - Document IDs + term frequency (enables TF-IDF scoring)
- `position` - Document IDs + frequency + positions (enables phrase queries, default)

```scala
// Default for all text fields (default: "position")
spark.indextables.indexing.text.indexRecordOption: "position"

// List-based syntax (recommended): indexrecordoption.<option> = "field1,field2,..."
spark.indextables.indexing.indexrecordoption.position: "title,content,body"
spark.indextables.indexing.indexrecordoption.basic: "logs,metrics"

// Old per-field syntax still works
spark.indextables.indexing.indexrecordoption.logs: "basic"
```

### Tokenizers (for text fields)
Configure which tokenizer to use for text fields:
```scala
// List-based syntax: tokenizer.<tokenizer_name> = "field1,field2,..."
spark.indextables.indexing.tokenizer.en_stem: "title,content,body"  // English stemming
spark.indextables.indexing.tokenizer.default: "exact_match_field"   // No stemming

// Old per-field syntax still works
spark.indextables.indexing.tokenizer.content: "en_stem"

// Available tokenizers: default, raw, en_stem, whitespace
```

### Fast Fields (for aggregations)
```scala
spark.indextables.indexing.fastfields: "score,value,timestamp"
// Auto-fast-field: first numeric field becomes fast if not configured
```

## Common Usage

### Basic Write
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")
```

### Write with Field Configuration
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexing.typemap.title", "string")
  .option("spark.indextables.indexing.typemap.content", "text")
  .option("spark.indextables.indexing.fastfields", "score")
  .save("s3://bucket/path")
```

### Read & Query
```scala
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")

// Standard filters (pushed down for string fields)
df.filter($"title" === "exact title").show()

// IndexQuery (for text fields)
import org.apache.spark.sql.indextables.IndexQueryExpression._
df.filter($"content" indexquery "machine learning").show()

// Aggregations (pushed down to tantivy)
df.agg(count("*"), sum("score"), avg("score")).show()
```

### Bucket Aggregations

Bucket aggregation functions enable time-series analysis and numeric distribution analysis using SQL GROUP BY clauses. These execute directly in Tantivy.

#### SQL Functions
- `indextables_histogram(column, interval)` - Fixed-interval numeric bucketing
- `indextables_date_histogram(column, interval)` - Time-based bucketing (supports: `ms`, `s`, `m`, `h`, `d`)
- `indextables_range(column, name1, from1, to1, ...)` - Custom named range buckets

#### Examples
```sql
-- Histogram: Bucket by price in $50 intervals with sub-aggregations
SELECT indextables_histogram(price, 50.0) as price_bucket,
       COUNT(*) as cnt, SUM(quantity) as total_qty
FROM products
GROUP BY indextables_histogram(price, 50.0)

-- DateHistogram: Bucket events by day
SELECT indextables_date_histogram(event_time, '1d') as day_bucket, COUNT(*) as cnt
FROM events
GROUP BY indextables_date_histogram(event_time, '1d')

-- Range: Create custom price tiers (NULL = unbounded)
SELECT indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as tier,
       COUNT(*) as cnt
FROM products
GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
```

**Requirements:**
- Fields must be configured as fast fields: `spark.indextables.indexing.fastfields: "price,event_time"`
- DateHistogram works with Spark `Timestamp` columns (indexed as tantivy date fields)

### Batch Retrieval Optimization

**Overview**: Batch optimization dramatically reduces S3 GET requests (90-95%) and improves read latency (2-3x) for queries returning 50+ documents. Enabled by default with balanced profile.

**Performance Impact:**
- Without optimization: 1,000 docs = 1,000 S3 requests, ~3.4 seconds
- With optimization: 1,000 docs = 50-100 S3 requests, ~1.5-2.0 seconds
- **Per-batch-operation concurrency**: Each batch retrieval uses up to `maxConcurrentPrefetch` concurrent requests (scales with Spark executors)

#### Example 1: Default Behavior (Automatic)
```scala
// Batch optimization enabled by default - no configuration needed
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")
df.filter($"score" > 0.5).collect()  // Automatically optimized for batches ≥50 docs
```

#### Example 2: Aggressive Profile for Cost Optimization
```scala
// Maximize S3 request consolidation
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "aggressive")
  .load("s3://bucket/path")

// Large aggregation - maximum consolidation
df.groupBy("category").count().show()
```

#### Example 3: Custom Parameters
```scala
// Fine-tune optimization parameters
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "balanced")
  .option("spark.indextables.read.batchOptimization.maxRangeSize", "32M")
  .option("spark.indextables.read.batchOptimization.gapTolerance", "2M")
  .option("spark.indextables.read.batchOptimization.maxConcurrentPrefetch", "16")
  .load("s3://bucket/path")

df.count()  // Optimized with custom parameters
```

#### Example 4: Conservative Profile for Memory-Constrained Environments
```scala
// Smaller memory footprint, fewer concurrent requests
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "conservative")
  .load("s3://bucket/path")

df.filter($"status" === "active").show()
```

#### Example 5: Disable Optimization for Debugging
```scala
// Turn off optimization completely
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.enabled", "false")
  .load("s3://bucket/path")

df.show()  // Standard retrieval without optimization
```

#### Example 6: Session-Level Configuration
```scala
// Configure globally for all read operations
spark.conf.set("spark.indextables.read.batchOptimization.profile", "aggressive")
spark.conf.set("spark.indextables.read.adaptiveTuning.enabled", "true")

val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")
// All queries automatically use aggressive optimization
df.filter($"date" >= "2024-01-01").count()
```

**When to Use:**
- ✅ Queries returning 50+ documents
- ✅ High-frequency read workloads
- ✅ Cost-sensitive S3 operations
- ✅ Aggregate queries with large result sets

**When NOT to Use:**
- ❌ Single document lookups (automatically bypassed)
- ❌ Very small result sets (<10 documents)
- ❌ Already disabled via configuration

### JSON Fields (Struct/Array/Map)
```scala
// Struct - automatic JSON field detection
case class User(name: String, age: Int, city: String)
val df1 = Seq((1, User("Alice", 30, "NYC"))).toDF("id", "user")

// Default: full mode (all features including fast fields for range queries/aggregations)
df1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path1")

// Optional: Minimal mode (smaller index, no range queries/aggregations)
df1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexing.json.mode", "minimal")
  .save("s3://bucket/path1-minimal")

// Array - automatic detection
val df2 = Seq((1, Seq("tag1", "tag2", "tag3"))).toDF("id", "tags")
df2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path2")

// Map - automatic detection (keys converted to strings in JSON)
val df3 = Seq(
  (1, Map("color" -> "red", "size" -> "large")),
  (2, Map(1 -> "first", 2 -> "second"))  // Integer keys supported
).toDF("id", "attributes")
df3.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path3")

// Read with filter pushdown (Struct fields)
val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/path1")
  .filter($"user.name" === "Alice")  // Pushed down to tantivy
  .filter($"user.age" > 28)          // Pushed down to tantivy

// Supported: ===, >, >=, <, <=, isNull, isNotNull, &&, ||
```

### Partitioned Datasets
```scala
// Write
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

// Read with partition pruning
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")
df.filter($"date" === "2024-01-01" && $"hour" === 10).show()
```

### Prewarm Cache

Pre-warm index caches across all executors for optimal query performance.

```sql
-- Register extensions
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Basic prewarm (preloads default segments: TERM, POSTINGS)
PREWARM INDEXTABLES CACHE 's3://bucket/path';

-- Prewarm specific segments
PREWARM INDEXTABLES CACHE 's3://bucket/path' FOR SEGMENTS (TERM_DICT, FAST_FIELD);

-- Prewarm with field selection
PREWARM INDEXTABLES CACHE 's3://bucket/path' ON FIELDS (title, content, timestamp);

-- Prewarm with partition filter
PREWARM INDEXTABLES CACHE 's3://bucket/path' WHERE date >= '2024-01-01';

-- Prewarm with custom parallelism (splits per task)
PREWARM INDEXTABLES CACHE 's3://bucket/path' WITH PERWORKER PARALLELISM OF 5;

-- Full syntax with all options
PREWARM INDEXTABLES CACHE 's3://bucket/path'
  FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM)
  ON FIELDS (title, content)
  WITH PERWORKER PARALLELISM OF 4
  WHERE region = 'us-east';
```

**Segment Aliases:**
| SQL Name | Description |
|----------|-------------|
| TERM_DICT, TERM | Term dictionary (FST) |
| FAST_FIELD, FASTFIELD | Fast fields for aggregations |
| POSTINGS, POSTING_LISTS | Inverted index postings |
| POSITIONS, POSITION_LISTS | Term positions within documents |
| FIELD_NORM, FIELDNORM | Field norms for scoring |
| DOC_STORE, STORE | Document storage |

**Default segments**: TERM_DICT, POSTINGS (minimal set for query operations)

**Read-time prewarm configuration:**
```scala
// Enable prewarm on read
spark.indextables.prewarm.enabled: false (default)

// Segment selection (comma-separated)
spark.indextables.prewarm.segments: "TERM_DICT,POSTINGS"

// Field selection (comma-separated, empty = all fields)
spark.indextables.prewarm.fields: ""

// Splits per Spark task (controls batch size)
spark.indextables.prewarm.splitsPerTask: 2 (default)

// Partition filter WHERE clause (prewarm only matching partitions)
spark.indextables.prewarm.partitionFilter: "" (default: empty = all partitions)

// Missing field handling (true = fail, false = warn and skip)
spark.indextables.prewarm.failOnMissingField: true (default)

// Catch-up behavior for new hosts
spark.indextables.prewarm.catchUpNewHosts: false (default)
```

**Output schema:**
- `host`: Executor hostname
- `splits_prewarmed`: Count of splits prewarmed
- `segments`: Comma-separated segment names
- `fields`: Comma-separated field names (or "all")
- `duration_ms`: Prewarm duration in milliseconds
- `status`: "success", "partial", or "no_splits"
- `skipped_fields`: Missing fields (if any)

### Merge Splits
```sql
-- Register extensions
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Merge operations
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M;
MERGE SPLITS 's3://bucket/path' MAX DEST SPLITS 10;  -- Limit to 10 destination (merged) splits
MERGE SPLITS 's3://bucket/path' MAX SOURCE SPLITS PER MERGE 500;  -- Limit source splits per merge
MERGE SPLITS 's3://bucket/path' WHERE date = '2024-01-01' TARGET SIZE 100M;
MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G MAX DEST SPLITS 5 MAX SOURCE SPLITS PER MERGE 100;
```

**Merge Splits Configuration**:
- `TARGET SIZE`: Maximum size of merged splits (default: 5GB)
- `MAX DEST SPLITS`: Limits the number of destination (merged) splits to process (oldest first)
- `MAX SOURCE SPLITS PER MERGE`: Maximum number of source splits that can be merged into a single destination split (default: 1000)
  - Configuration: `spark.indextables.merge.maxSourceSplitsPerMerge: 1000`
- **Skip Split Threshold**: Splits already at or above this size are excluded from merge consideration
  - Configuration: `spark.indextables.merge.skipSplitThreshold: 0.45` (default: 45% of target size)
  - Range: 0.0 to 1.0 (percentage of target size)
  - Example: With 5GB target and 0.45 threshold, splits >= 2.25GB are skipped

### Purge IndexTable
```sql
-- Register extensions (same as above)
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Purge operations (removes orphaned split files and old transaction logs)
PURGE INDEXTABLE 's3://bucket/path' DRY RUN;  -- Preview what would be deleted
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 7 DAYS;  -- Delete split files older than 7 days
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 168 HOURS;  -- Same as above (7*24=168)
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 14 DAYS DRY RUN;  -- Preview with custom retention

-- Purge with transaction log cleanup (keeps logs for longer than splits)
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 7 DAYS TRANSACTION LOG RETENTION 30 DAYS;

-- Configuration
spark.indextables.purge.defaultRetentionHours: 168 (7 days for splits)
spark.indextables.purge.minRetentionHours: 24 (safety check)
spark.indextables.purge.retentionCheckEnabled: true
spark.indextables.purge.parallelism: <auto>
spark.indextables.purge.maxFilesToDelete: 1000000
spark.indextables.purge.deleteRetries: 3
```

**Common scenarios:**
- After failed writes that leave orphaned split files
- After MERGE SPLITS operations
- Regular maintenance (weekly/monthly cleanup)
- Before archiving or migrating tables
- Transaction log cleanup to reclaim storage

**Safety features:**
- Minimum retention period enforced (default 24 hours)
- DRY RUN mode shows preview before deletion
- Distributed deletion across executors for scalability
- Retry logic handles transient cloud storage errors
- LEFT ANTI JOIN ensures only truly orphaned files deleted
- Transaction log files referenced by checkpoints are never deleted

### Drop Partitions
```sql
-- Register extensions (same as above)
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Drop partitions from a table (logically removes data)
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year = '2023';
DROP INDEXTABLES PARTITIONS FROM my_table WHERE date = '2024-01-01';

-- Range predicates
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year > '2020';
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE month < 6;
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE month BETWEEN 1 AND 6;

-- Compound predicates (AND, OR)
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE region = 'us-east' AND year > '2020';
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year = '2022' OR year = '2023';
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE status = 'inactive' OR created < '2023-01-01';

-- Also works with TANTIVY4SPARK keyword
DROP TANTIVY4SPARK PARTITIONS FROM 's3://bucket/path' WHERE partition_key = 'value';
```

**Key features:**
- **WHERE clause required** - Prevents accidental full table drops
- **Partition columns only** - Validates that WHERE clause references only partition columns
- **Logical deletion** - Adds RemoveAction entries to transaction log without physical deletion
- **Physical cleanup via PURGE** - Use PURGE INDEXTABLE to delete files after retention period
- **Returns status** - Reports partitions dropped, splits removed, and total size

**Example workflow:**
```scala
// 1. Drop old partitions
spark.sql("DROP INDEXTABLES PARTITIONS FROM 's3://bucket/table' WHERE year < '2022'").show()

// 2. Verify with DESCRIBE (optional)
spark.sql("DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/table' INCLUDE ALL").show()

// 3. After retention period, clean up physical files
spark.sql("PURGE INDEXTABLE 's3://bucket/table' OLDER THAN 7 DAYS").show()
```

**Error handling:**
- Fails if WHERE clause references non-partition columns
- Fails if table has no partition columns defined
- Returns no_action if no partitions match the predicates

### Checkpoint IndexTable
```sql
-- Register extensions (same as above)
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Force a checkpoint on a table (creates V3 checkpoint, upgrades protocol)
CHECKPOINT INDEXTABLES 's3://bucket/path';
CHECKPOINT INDEXTABLES my_table;
CHECKPOINT TANTIVY4SPARK 's3://bucket/path';  -- alternate syntax

-- Example output:
-- +--------------------+-------+------------------+-----------+---------+----------------+-------------+
-- |table_path          |status |checkpoint_version|num_actions|num_files|protocol_version|is_multi_part|
-- +--------------------+-------+------------------+-----------+---------+----------------+-------------+
-- |s3://bucket/path    |SUCCESS|               42 |      1502 |     1500|               3|        false|
-- +--------------------+-------+------------------+-----------+---------+----------------+-------------+
```

**Key features:**
- **Protocol upgrade**: Always creates a V3 checkpoint with latest protocol features
- **Schema deduplication**: V3 checkpoints use hash-based schema references to reduce checkpoint size
- **Multi-part support**: Large checkpoints (>10K actions) are automatically split into parts
- **Streaming write**: Uses streaming to avoid OOM for large tables

**When to use:**
- **Force V3 upgrade**: Upgrade existing V2 tables to V3 protocol
- **After bulk operations**: Create checkpoint after large batch inserts or merges
- **Performance optimization**: Checkpoints speed up table reads by consolidating transaction log state
- **Before maintenance**: Create checkpoint before PURGE operations to ensure consistent state

**Output schema:**
| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Resolved table path |
| `status` | String | "SUCCESS" or "ERROR: <message>" |
| `checkpoint_version` | Long | Transaction version at checkpoint |
| `num_actions` | Long | Total actions in checkpoint |
| `num_files` | Long | Number of split files |
| `protocol_version` | Long | Protocol version (3 for V3) |
| `is_multi_part` | Boolean | True if checkpoint split into parts |

### Truncate Time Travel
```sql
-- Remove all historical transaction log versions, keeping only current state
TRUNCATE INDEXTABLES TIME TRAVEL 's3://bucket/path';
TRUNCATE INDEXTABLES TIME TRAVEL 's3://bucket/path' DRY RUN;  -- Preview only
TRUNCATE TANTIVY4SPARK TIME TRAVEL my_table;  -- Alternate syntax

-- Example output:
-- +-------------------+-------+------------------+----------------+-------------------+---------------+------------------------+
-- |table_path         |status |checkpoint_version|versions_deleted|checkpoints_deleted|files_preserved|message                 |
-- +-------------------+-------+------------------+----------------+-------------------+---------------+------------------------+
-- |s3://bucket/path   |SUCCESS|               20 |             19 |                  2|             20|Successfully trunca...|
-- +-------------------+-------+------------------+----------------+-------------------+---------------+------------------------+
```

**Key features:**
- **Creates checkpoint first**: If no checkpoint exists at current version, creates one automatically
- **Deletes historical versions**: Removes all transaction log version files older than the checkpoint
- **Deletes old checkpoints**: Removes all checkpoint files except the latest
- **Preserves data files**: Only transaction log metadata is affected; split files are preserved
- **DRY RUN mode**: Preview what would be deleted without making changes

**When to use:**
- **Reduce storage overhead**: After many small write operations accumulate transaction log files
- **Clean up history**: When time travel capability is no longer needed
- **Prepare for archival**: Remove history before archiving a table
- **After MERGE operations**: Clean up transaction log after split consolidation

**Output schema:**
| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Resolved table path |
| `status` | String | "SUCCESS", "DRY_RUN", or "ERROR" |
| `checkpoint_version` | Long | Version at which checkpoint exists |
| `versions_deleted` | Long | Number of version files deleted |
| `checkpoints_deleted` | Long | Number of checkpoint files deleted |
| `files_preserved` | Long | Number of data files (splits) in table |
| `message` | String | Descriptive status message |

**Safety notes:**
- After truncation, time travel to earlier versions is no longer possible
- Always use DRY RUN first to preview what will be deleted
- This operation is irreversible - deleted transaction log files cannot be recovered

### Describe Disk Cache
```sql
-- View disk cache statistics across all executors
DESCRIBE INDEXTABLES DISK CACHE;
DESCRIBE TANTIVY4SPARK DISK CACHE;  -- alternate syntax

-- Example output (disk cache auto-enabled on Databricks/EMR):
-- +-----------+--------------------+-------+-----------+------------+-------------+-------------+-----------------+
-- |executor_id|host                |enabled|total_bytes|   max_bytes|usage_percent|splits_cached|components_cached|
-- +-----------+--------------------+-------+-----------+------------+-------------+-------------+-----------------+
-- |driver     |10.0.0.1:44444      |   true| 5242880000|107374182400|          4.9|          125|              875|
-- |executor-0 |10.0.0.2:33333      |   true| 5242880000|107374182400|          4.9|          125|              875|
-- |executor-1 |10.0.0.3:33333      |   true| 4831838208|107374182400|          4.5|          118|              826|
-- +-----------+--------------------+-------+-----------+------------+-------------+-------------+-----------------+
```

**Key points:**
- **Auto-enabled on Databricks/EMR**: Disk cache is automatically enabled when `/local_disk0` is detected
- Each executor maintains its own independent disk cache
- Aggregates statistics from driver and all executors
- `host` column shows IP:port to identify which executor reported each row
- Returns NULL values when cache is disabled (no `/local_disk0` and not explicitly configured)
- Use to monitor cache utilization and tune `maxSize` setting
- To disable auto-detection, set `spark.indextables.cache.disk.enabled=false`

### Describe Storage Stats
```sql
-- View object storage (S3/Azure) access statistics across all executors
DESCRIBE INDEXTABLES STORAGE STATS;
DESCRIBE TANTIVY4SPARK STORAGE STATS;  -- alternate syntax

-- Example output:
-- +-----------+-------------------+-------------+--------+
-- |executor_id|host               |bytes_fetched|requests|
-- +-----------+-------------------+-------------+--------+
-- |driver     |10.0.0.1:44444     |     64838000|    1250|
-- |executor-0 |10.0.0.2:33333     |     52480000|    1100|
-- |executor-1 |10.0.0.3:33333     |     48320000|    1050|
-- +-----------+-------------------+-------------+--------+
```

**Key points:**
- Shows cumulative bytes fetched and request counts from object storage (S3/Azure/etc)
- Counters are cumulative since JVM startup
- Each executor maintains its own independent counters
- Useful for monitoring S3 costs and validating cache effectiveness
- After a full prewarm, `bytes_fetched` should not increase during subsequent queries

### Describe Data Skipping Stats
```sql
-- View data skipping effectiveness statistics
DESCRIBE INDEXTABLES DATA SKIPPING STATS;
DESCRIBE TANTIVY4SPARK DATA SKIPPING STATS;  -- alternate syntax

-- Example output:
-- +----------------------+----------------------+---------------+
-- |metric_type           |metric_name           |metric_value   |
-- +----------------------+----------------------+---------------+
-- |data_skipping         |total_files_considered|1000           |
-- |data_skipping         |partition_pruned_files|400            |
-- |data_skipping         |data_skipped_files    |300            |
-- |data_skipping         |final_files_scanned   |300            |
-- |data_skipping         |partition_skip_rate   |40.00%         |
-- |data_skipping         |data_skip_rate        |50.00%         |
-- |data_skipping         |total_skip_rate       |70.00%         |
-- |filter_type_skips     |EqualTo               |200            |
-- |filter_type_skips     |GreaterThan           |100            |
-- |filter_expr_cache     |simplified_hits       |500            |
-- |filter_expr_cache     |simplified_hit_rate   |83.33%         |
-- |partition_filter_cache|hits                  |800            |
-- |partition_filter_cache|hit_rate              |88.89%         |
-- +----------------------+----------------------+---------------+

-- Reset statistics (keeps cache entries)
FLUSH INDEXTABLES DATA SKIPPING STATS;
FLUSH TANTIVY4SPARK DATA SKIPPING STATS;

-- Clear caches and statistics
INVALIDATE INDEXTABLES DATA SKIPPING CACHE;
INVALIDATE TANTIVY4SPARK DATA SKIPPING CACHE;
```

**Key points:**
- `data_skipping` metrics track overall file pruning effectiveness
- `filter_type_skips` shows which filter types contributed most to skipping
- `filter_expr_cache` tracks expression simplification cache performance
- `partition_filter_cache` tracks partition filter evaluation cache performance
- Statistics are cumulative since session start or last reset
- Use FLUSH to reset stats while keeping cached entries
- Use INVALIDATE to clear both caches and stats

**Spark UI Integration:**
Data skipping metrics are also reported to the Spark UI SQL tab. During query execution, the following metrics appear under the scan operator:
- `total files considered` - All files in the table before filtering
- `files pruned by partitions` - Files eliminated by partition predicates
- `files skipped by stats` - Files eliminated by min/max column statistics
- `files to scan` - Final files that will be read
- `total skip rate` - Percentage of files skipped (e.g., "70.0%")

These metrics are visible in the Spark UI under the SQL tab for each query, similar to how Iceberg and Delta Lake report data skipping effectiveness.

### Describe Environment
```sql
-- View Spark and Hadoop configuration properties across all executors
DESCRIBE INDEXTABLES ENVIRONMENT;
DESCRIBE TANTIVY4SPARK ENVIRONMENT;  -- alternate syntax

-- Example output:
-- +----------------+------+-------------+---------------------------------+----------------+
-- |host            |role  |property_type|property_name                    |property_value  |
-- +----------------+------+-------------+---------------------------------+----------------+
-- |10.0.0.1:44444  |driver|spark        |spark.app.name                   |MyApp           |
-- |10.0.0.1:44444  |driver|spark        |spark.master                     |local[4]        |
-- |10.0.0.1:44444  |driver|spark        |spark.indextables.aws.secretKey  |***REDACTED***  |
-- |10.0.0.1:44444  |driver|hadoop       |fs.defaultFS                     |file:///        |
-- |10.0.0.2:33333  |worker|spark        |spark.app.name                   |MyApp           |
-- |10.0.0.2:33333  |worker|hadoop       |fs.defaultFS                     |file:///        |
-- +----------------+------+-------------+---------------------------------+----------------+
```

**Output schema:**
| Column | Type | Description |
|--------|------|-------------|
| `host` | String | Host address (ip:port) of the node |
| `role` | String | "driver" or "worker" |
| `property_type` | String | "spark" or "hadoop" |
| `property_name` | String | Configuration property name |
| `property_value` | String | Property value (sensitive values redacted) |

**Key points:**
- Collects Spark and Hadoop properties from driver and all worker nodes
- Sensitive values containing "secret", "key", "password", "token", "credential", or "session" are automatically redacted as `***REDACTED***`
- Useful for debugging configuration issues across the cluster
- Results can be filtered using SQL after registering as a temp view

**Example usage:**
```scala
// View all environment properties
spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT").show(false)

// Filter to specific properties
spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT").createOrReplaceTempView("env_props")
spark.sql("SELECT * FROM env_props WHERE property_name LIKE 'spark.indextables%'").show()

// Compare driver vs worker configuration
spark.sql("SELECT property_name, role, property_value FROM env_props WHERE property_type = 'spark' ORDER BY property_name, role").show()
```

### Flush Disk Cache
```sql
-- Flush disk cache across all executors (clears cached data and locality state)
FLUSH INDEXTABLES DISK CACHE;
FLUSH TANTIVY4SPARK DISK CACHE;  -- alternate syntax

-- Example output:
-- +-----------+----------------+-------+------------+-------------+------------------------------------------+
-- |executor_id|cache_type      |status |bytes_freed |files_deleted|message                                   |
-- +-----------+----------------+-------+------------+-------------+------------------------------------------+
-- |driver     |split_cache     |success|           0|            2|Flushed 2 split cache managers            |
-- |driver     |locality_manager|success|           0|            0|Cleared split locality assignments...     |
-- |driver     |disk_cache_files|success|   524288000|          125|Deleted 125 files (524288000 bytes)...    |
-- |executor-0 |disk_cache      |success|   524288000|          125|Deleted 125 files (524288000 bytes)...    |
-- +-----------+----------------+-------+------------+-------------+------------------------------------------+
```

**Key points:**
- Flushes split cache managers (closes disk cache handles)
- Clears split locality assignments and prewarm state tracking
- Deletes all disk cache files at the configured path
- Uses explicitly configured path (`spark.indextables.cache.disk.path`) or auto-detected default
- Useful for testing, maintenance, and troubleshooting cache issues

### Purge-On-Write (Automatic Table Hygiene)
```scala
// Enable automatic purge after every 10 writes
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.purgeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "10")
  .option("spark.indextables.purgeOnWrite.splitRetentionHours", "168")  // 7 days
  .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "720")  // 30 days
  .save("s3://bucket/path")

// Enable purge after merge-on-write completes
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.mergeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
  .save("s3://bucket/path")
```

**Purge-On-Write Features:**
- **Disabled by default** - Must be explicitly enabled
- **Two trigger modes:**
  1. After merge-on-write completes (default: enabled)
  2. After N write operations (default: disabled, set triggerAfterWrites > 0)
- **Automatic credential propagation** - Write options passed to purge executor
- **Per-session counters** - Transaction counts tracked per table path
- **Separate retention periods** - Different retention for splits vs transaction logs
- **Graceful failure handling** - Purge failures don't fail writes
- **Session-scoped** - Counters reset between Spark sessions

**When to use:**
- High-frequency write workloads that generate many small splits
- Tables with frequent merge operations
- Long-running Spark applications with periodic writes
- Development/testing environments with rapid iteration

**Example: Automatic Hygiene Configuration**
```scala
// Complete automatic table hygiene (merge + purge)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  // Enable merge-on-write
  .option("spark.indextables.mergeOnWrite.enabled", "true")
  .option("spark.indextables.mergeOnWrite.targetSize", "4G")
  .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "2.0")
  // Enable purge-on-write
  .option("spark.indextables.purgeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")  // Run after each merge
  .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "20")   // Also run every 20 writes
  .option("spark.indextables.purgeOnWrite.splitRetentionHours", "168") // Keep splits 7 days
  .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "720") // Keep logs 30 days
  .save("s3://bucket/path")
```

## Azure Multi-Cloud Examples

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

## Performance Tuning

### High-Performance Configuration
```scala
// Databricks (automatic /local_disk0 detection)
// No configuration needed!

// Manual optimization
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
spark.conf.set("spark.indextables.s3.maxConcurrency", "8")
spark.conf.set("spark.indextables.indexWriter.batchSize", "25000")
```

### Transaction Log Performance
```scala
spark.conf.set("spark.indextables.checkpoint.enabled", "true")
spark.conf.set("spark.indextables.checkpoint.interval", "10")
spark.conf.set("spark.indextables.checkpoint.parallelism", "8")
```

## Supported Schema Types
- ✅ String, Integer/Long, Float/Double, Boolean, Date, Timestamp, Binary
- ✅ Struct (via JSON fields with filter pushdown)
- ✅ Array (via JSON fields with filter pushdown)
- ✅ Map (via JSON fields, keys converted to strings)

## Architecture Notes
- **File format**: `*.split` files with UUID naming
- **Transaction log**: `_transaction_log/` directory (Delta Lake compatible)
- **Checkpoints**: `*.checkpoint.json` files for performance
- **Compression**: GZIP compression enabled by default (60-70% size reduction)
- **Caching**: JVM-wide SplitCacheManager with locality tracking
- **Storage**: S3OptimizedReader for S3, StandardFileReader for local/HDFS

## Test Status
- ✅ **350+ tests passing**: Complete coverage for all major features
- ⚠️  **Merge-on-write**: Tests need updating for new post-commit design
  - Previous shuffle-based implementation removed
  - New design: post-commit evaluation with MERGE SPLITS invocation
  - Tests to be updated for new architecture
- ✅ **JSON fields**: 114/114 tests passing (99 Struct/Array/Map tests + 15 aggregate/configuration tests)
- ✅ **JSON configuration**: 6/6 tests passing (json.mode validation)
- ✅ **JSON aggregates**: 9/9 tests passing (aggregates on nested fields with filter pushdown)
- ✅ **Bucket aggregations**: 4/4 tests passing (Histogram, DateHistogram, Range, sub-aggregations)
- ✅ **Partitioned datasets**: 7/7 tests
- ✅ **Transaction log**: All checkpoint and compression tests passing
- ✅ **PURGE INDEXTABLE**: 40/40 tests passing (integration, parsing, error handling, transaction log cleanup)
- ✅ **Purge-on-write**: 8/8 tests passing (local/Hadoop integration)
  - Real S3/Azure purge-on-write tests available but require credentials
- ✅ **DESCRIBE ENVIRONMENT**: 10/10 tests passing (parsing, schema, redaction, queryability)
- ✅ **Merge I/O**: 34/34 tests passing (config, priority queue, download manager, local copy downloader)

## Important Notes
- **DataSource API**: Use `io.indextables.spark.core.IndexTables4SparkTableProvider` for all read/write operations
- **Field types**: String fields (default) get full filter pushdown; text fields require IndexQuery
- **Statistics truncation**: Enabled by default to prevent transaction log bloat
- **Working directories**: Automatic /local_disk0 detection on Databricks/EMR
- **JSON fields**: Automatic detection for Struct/Array/Map types with complete filter pushdown
- **JSON configuration**: Use `spark.indextables.indexing.json.mode` to control indexing behavior ("full" or "minimal")
- **Merge-on-write**: Post-commit evaluation architecture - writes complete normally, then evaluates if merge is worthwhile based on merge group count vs parallelism threshold
- **Merge-on-write threshold**: Merge runs if merge groups ≥ (defaultParallelism × mergeGroupMultiplier), default multiplier is 2.0
- **Merge-on-write delegation**: Invokes existing MERGE SPLITS command programmatically after transaction commit
- **Purge-on-write**: Automatic cleanup of orphaned splits and old transaction logs during write operations
- **Purge-on-write triggers**: Can run after merge-on-write or after N writes per table (per-session counters)
- **Purge-on-write safety**: Disabled by default, respects minimum retention periods, propagates credentials, graceful failure handling
- **L2 Disk Cache**: Auto-enabled on Databricks/EMR when `/local_disk0` detected; disabled on spinning disk systems (no benefit). Use `spark.indextables.cache.disk.enabled=false` to explicitly disable.
- **Bucket aggregations**: Use `indextables_histogram`, `indextables_date_histogram`, `indextables_range` functions in GROUP BY for time-series and distribution analysis. Requires fast fields.
- **Merge I/O**: Downloads/uploads handled in Scala using AWS SDK v2 async client; tantivy4java receives only local paths. Priority queue ensures first-submitted batches complete first while maximizing throughput.

---

**Full documentation backup**: See `CLAUDE.md.full.backup` for complete reference.
