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
- **Purge operations**: SQL-based cleanup of orphaned splits and old transaction logs (`PURGE INDEXTABLE`)
- **Purge-on-write**: Automatic table hygiene during write operations
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'`)
- **Auto-sizing**: Intelligent DataFrame partitioning based on historical split analysis
- **V2 DataSource API**: Recommended for partition column indexing
- **Multi-cloud**: S3 and Azure Blob Storage with native authentication
- **JSON field support**: Native Struct/Array/Map fields with filter pushdown and configurable indexing modes (114/114 tests passing)
- **Statistics truncation**: Automatic optimization for long text fields (enabled by default)

## Key Configuration Settings

### Essential Settings
```scala
// Index Writer
spark.indextables.indexWriter.heapSize: "100M" (supports "2G", "500M", "1024K")
spark.indextables.indexWriter.batchSize: 10000
spark.indextables.indexWriter.threads: 2

// Split Conversion (controls parallelism of tantivy index -> quickwit split conversion)
spark.indextables.splitConversion.maxParallelism: <auto> (default: max(1, availableProcessors / 4))

// Auto-sizing
spark.indextables.autoSize.enabled: false
spark.indextables.autoSize.targetSplitSize: "100M"
spark.indextables.autoSize.inputRowCount: <row_count> (required for V2 API)

// Transaction Log
spark.indextables.checkpoint.enabled: true
spark.indextables.checkpoint.interval: 10
spark.indextables.transaction.compression.enabled: true (default)

// Statistics Truncation (enabled by default)
spark.indextables.stats.truncation.enabled: true
spark.indextables.stats.truncation.maxLength: 256

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
// V2 API (recommended for partition columns)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")

// V1 API (legacy compatibility)
df.write.format("indextables").save("s3://bucket/path")
```

### Write with Auto-Sizing (V1 recommended)
```scala
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/path")
```

### Write with Field Configuration
```scala
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.title", "string")
  .option("spark.indextables.indexing.typemap.content", "text")
  .option("spark.indextables.indexing.fastfields", "score")
  .save("s3://bucket/path")
```

### Read & Query
```scala
val df = spark.read.format("indextables").load("s3://bucket/path")

// Standard filters (pushed down for string fields)
df.filter($"title" === "exact title").show()

// IndexQuery (for text fields)
import org.apache.spark.sql.indextables.IndexQueryExpression._
df.filter($"content" indexquery "machine learning").show()

// Aggregations (pushed down to tantivy)
df.agg(count("*"), sum("score"), avg("score")).show()
```

### JSON Fields (Struct/Array/Map)
```scala
// Struct - automatic JSON field detection
case class User(name: String, age: Int, city: String)
val df1 = Seq((1, User("Alice", 30, "NYC"))).toDF("id", "user")

// Default: full mode (all features including fast fields for range queries/aggregations)
df1.write.format("indextables").save("s3://bucket/path1")

// Optional: Minimal mode (smaller index, no range queries/aggregations)
df1.write.format("indextables")
  .option("spark.indextables.indexing.json.mode", "minimal")
  .save("s3://bucket/path1-minimal")

// Array - automatic detection
val df2 = Seq((1, Seq("tag1", "tag2", "tag3"))).toDF("id", "tags")
df2.write.format("indextables").save("s3://bucket/path2")

// Map - automatic detection (keys converted to strings in JSON)
val df3 = Seq(
  (1, Map("color" -> "red", "size" -> "large")),
  (2, Map(1 -> "first", 2 -> "second"))  // Integer keys supported
).toDF("id", "attributes")
df3.write.format("indextables").save("s3://bucket/path3")

// Read with filter pushdown (Struct fields)
val result = spark.read.format("indextables")
  .load("s3://bucket/path1")
  .filter($"user.name" === "Alice")  // Pushed down to tantivy
  .filter($"user.age" > 28)          // Pushed down to tantivy

// Supported: ===, >, >=, <, <=, isNull, isNotNull, &&, ||
```

### Partitioned Datasets
```scala
// Write
df.write.format("indextables")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

// Read with partition pruning
val df = spark.read.format("indextables").load("s3://bucket/path")
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

### Purge-On-Write (Automatic Table Hygiene)
```scala
// Enable automatic purge after every 10 writes
df.write.format("indextables")
  .option("spark.indextables.purgeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "10")
  .option("spark.indextables.purgeOnWrite.splitRetentionHours", "168")  // 7 days
  .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "720")  // 30 days
  .save("s3://bucket/path")

// Enable purge after merge-on-write completes
df.write.format("indextables")
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
df.write.format("indextables")
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
df.write.format("indextables").save("abfss://container/path")

// Read
val df = spark.read.format("indextables").load("abfss://container/path")
```

### Azure OAuth (Service Principal)
```scala
spark.conf.set("spark.indextables.azure.accountName", "account")
spark.conf.set("spark.indextables.azure.tenantId", "tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "client-secret")

df.write.format("indextables")
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
- **V2 DataSource API recommended**: Use `io.indextables.spark.core.IndexTables4SparkTableProvider` for partition column indexing
- **Auto-sizing**: V1 API auto-counts DataFrame; V2 requires explicit row count
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
