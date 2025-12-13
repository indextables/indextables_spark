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
- **Partitioned datasets**: Full support with partition pruning
- **Merge operations**: SQL-based split consolidation (`MERGE SPLITS`)
- **Drop partitions**: SQL-based partition removal with WHERE clause validation (`DROP INDEXTABLES PARTITIONS`)
- **Purge operations**: SQL-based cleanup of orphaned splits and old transaction logs (`PURGE INDEXTABLE`)
- **Purge-on-write**: Automatic table hygiene during write operations
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'`)
- **V2 DataSource API**: Recommended for partition column indexing
- **Multi-cloud**: S3 and Azure Blob Storage with native authentication
- **JSON field support**: Native Struct/Array/Map fields with filter pushdown and configurable indexing modes (114/114 tests passing)
- **Statistics truncation**: Automatic optimization for long text fields (enabled by default)
- **Batch retrieval optimization**: 90-95% reduction in S3 GET requests for read operations, 2-3x faster (enabled by default)

## Key Configuration Settings

### Essential Settings
```scala
// Index Writer
spark.indextables.indexWriter.heapSize: "100M" (supports "2G", "500M", "1024K")
spark.indextables.indexWriter.batchSize: 10000
spark.indextables.indexWriter.maxBatchBufferSize: "90M" (default: 90MB, prevents native 100MB limit errors)
spark.indextables.indexWriter.threads: 2

// Split Conversion (controls parallelism of tantivy index -> quickwit split conversion)
spark.indextables.splitConversion.maxParallelism: <auto> (default: max(1, availableProcessors / 4))

// Transaction Log
spark.indextables.checkpoint.enabled: true
spark.indextables.checkpoint.interval: 10
spark.indextables.transaction.compression.enabled: true (default)

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

// Read Limits (controls default result set size when no explicit LIMIT is specified)
spark.indextables.read.defaultLimit: 250 (default: 250, maximum documents per partition when no LIMIT pushed down)

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

### Merge Splits
```sql
-- Register extensions
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Merge operations
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M;
MERGE SPLITS 's3://bucket/path' MAX GROUPS 10;
MERGE SPLITS 's3://bucket/path' WHERE date = '2024-01-01' TARGET SIZE 100M;
```

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
- ✅ **Partitioned datasets**: 7/7 tests
- ✅ **Transaction log**: All checkpoint and compression tests passing
- ✅ **PURGE INDEXTABLE**: 40/40 tests passing (integration, parsing, error handling, transaction log cleanup)
- ✅ **Purge-on-write**: 8/8 tests passing (local/Hadoop integration)
  - Real S3/Azure purge-on-write tests available but require credentials

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

---

**Full documentation backup**: See `CLAUDE.md.full.backup` for complete reference.
