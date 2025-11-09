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

// Merge-On-Write (automatic split consolidation after writes via post-commit evaluation)
spark.indextables.mergeOnWrite.enabled: false (default: false)
spark.indextables.mergeOnWrite.targetSize: "4G" (default: 4G, target merged split size)
spark.indextables.mergeOnWrite.mergeGroupMultiplier: 2.0 (default: 2.0, threshold = parallelism × multiplier)
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

// Supports: azure://, wasbs://, abfss:// URLs
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

## Azure Multi-Cloud Examples

### Basic Azure Write/Read
```scala
// Session-level config
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.accountKey", "your-account-key")

// Write (supports azure://, wasbs://, abfss://)
df.write.format("indextables").save("azure://container/path")

// Read
val df = spark.read.format("indextables").load("azure://container/path")
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
- ✅ **300+ tests passing**: Complete coverage for all major features
- ⚠️  **Merge-on-write**: Tests need updating for new post-commit design
  - Previous shuffle-based implementation removed
  - New design: post-commit evaluation with MERGE SPLITS invocation
  - Tests to be updated for new architecture
- ✅ **JSON fields**: 114/114 tests passing (99 Struct/Array/Map tests + 15 aggregate/configuration tests)
- ✅ **JSON configuration**: 6/6 tests passing (json.mode validation)
- ✅ **JSON aggregates**: 9/9 tests passing (aggregates on nested fields with filter pushdown)
- ✅ **Partitioned datasets**: 7/7 tests
- ✅ **Transaction log**: All checkpoint and compression tests passing

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

---

**Full documentation backup**: See `CLAUDE.md.full.backup` for complete reference.
