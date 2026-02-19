# CLAUDE.md

**IndexTables4Spark** is a high-performance Spark DataSource implementing fast full-text search using Tantivy via tantivy4java. It runs embedded in Spark executors without server-side components.

## Essential Commands

### Build & Test
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # Set Java 11
mvn clean compile                              # Build

# Run all tests (parallel, avoids OOM from `mvn test`):
./run_tests_individually.sh          # Default: 4 parallel jobs
./run_tests_individually.sh -j 8     # 8 parallel jobs
./run_tests_individually.sh -j 1     # Sequential
./run_tests_individually.sh --dry-run # List test classes without running

# Run single test:
mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.DateStringFilterValidationTest'
```

> **Note:** `mvn test` may OOM on laptops due to 360+ test classes. Use `./run_tests_individually.sh` which compiles once and runs each test class in a separate JVM. Per-test logs are saved to a temp directory; failed test log paths are printed in the summary.

## Core Features

- **Split-based architecture**: Write-only indexes with QuickwitSplit format
- **Transaction log**: Delta Lake-style with atomic operations, checkpoints, GZIP compression
- **Avro state format**: 10x faster checkpoint reads (default since v4)
- **Aggregate pushdown**: COUNT(), SUM(), AVG(), MIN(), MAX() with transaction log optimization
- **Bucket aggregations**: DateHistogram, Histogram, Range via SQL functions
- **Partitioned datasets**: Full support with partition pruning
- **Merge operations**: SQL-based split consolidation (`MERGE SPLITS`)
- **Drop partitions**: SQL-based partition removal (`DROP INDEXTABLES PARTITIONS`)
- **Purge operations**: SQL-based cleanup (`PURGE INDEXTABLE`)
- **Purge-on-write**: Automatic table hygiene during write operations
- **Cache prewarming**: SQL-based (`PREWARM INDEXTABLES CACHE`) and read-time
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'`)
- **V2 DataSource API**: Recommended for partition column indexing
- **Multi-cloud**: S3 and Azure Blob Storage with native authentication
- **JSON field support**: Native Struct/Array/Map fields with filter pushdown
- **Statistics truncation**: Automatic optimization for long text fields (enabled by default)
- **Batch retrieval optimization**: 90-95% reduction in S3 GET requests (enabled by default)
- **L2 Disk Cache**: Persistent NVMe caching, auto-enabled on Databricks/EMR when `/local_disk0` detected
- **Optimized Writes**: Iceberg-style shuffle before writing for well-sized splits

## Field Types

- **String** (default): Exact matching, full filter pushdown (`===`, `>`, `<`, `IN`, etc.)
- **Text**: Full-text search via IndexQuery only. Configure: `spark.indextables.indexing.typemap.text: "field1,field2"`
- **JSON**: Automatic for Struct/Array/Map types. Mode: `spark.indextables.indexing.json.mode: "full"` (default) or `"minimal"`
- **Fast fields** (for aggregations): `spark.indextables.indexing.fastfields: "score,timestamp"`

See `docs/reference/field-indexing.md` for tokenizers, index record options, token length limits, and list-based typemap syntax.

## Common Usage

### DataSource API
Use `io.indextables.spark.core.IndexTables4SparkTableProvider` for all read/write operations.

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

### Optimized Write (shuffle before writing)
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.write.optimizeWrite.enabled", "true")
  .option("spark.indextables.write.optimizeWrite.targetSplitSize", "1G")
  .partitionBy("date")
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

## SQL Extensions

Register extensions for SQL commands:
```scala
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")
```

Available commands: `MERGE SPLITS`, `PURGE INDEXTABLE`, `DROP INDEXTABLES PARTITIONS`, `CHECKPOINT/COMPACT INDEXTABLES`, `TRUNCATE INDEXTABLES TIME TRAVEL`, `PREWARM INDEXTABLES CACHE`, `DESCRIBE INDEXTABLES STATE/DISK CACHE/STORAGE STATS/DATA SKIPPING STATS/ENVIRONMENT/MERGE JOBS`, `FLUSH INDEXTABLES DISK CACHE/DATA SKIPPING STATS`, `INVALIDATE INDEXTABLES DATA SKIPPING CACHE`.

All commands support both `INDEXTABLES` and `TANTIVY4SPARK` keywords. See `docs/reference/sql-commands.md` for full syntax, examples, and output schemas.

## Supported Schema Types

- String, Integer/Long, Float/Double, Boolean, Date, Timestamp, Binary
- Struct (via JSON fields with filter pushdown)
- Array (via JSON fields with filter pushdown)
- Map (via JSON fields, keys converted to strings)

## Architecture Notes

- **File format**: `*.split` files with UUID naming
- **Transaction log**: `_transaction_log/` directory (Delta Lake compatible)
- **Checkpoints**: `*.checkpoint.json` files for performance
- **Compression**: GZIP compression enabled by default (60-70% size reduction)
- **Caching**: JVM-wide SplitCacheManager with locality tracking
- **Storage**: S3OptimizedReader for S3, StandardFileReader for local/HDFS

## Key Behaviors

- **Merge-on-write**: Post-commit evaluation architecture. Async by default (`mergeOnWrite.async.enabled=true`), runs in background thread. Threshold: `batchSize x minBatchesToTrigger` where `batchSize = max(1, totalClusterCpus x batchCpuFraction)`.
- **L2 Disk Cache**: Auto-enabled on Databricks/EMR when `/local_disk0` detected. Use `spark.indextables.cache.disk.enabled=false` to disable. Use `DESCRIBE INDEXTABLES DISK CACHE` to monitor.
- **Merge I/O**: Downloads/uploads handled in Scala (AWS SDK v2 async); tantivy4java receives only local paths.
- **Working directories**: Automatic `/local_disk0` detection on Databricks/EMR.

## Test Status

- 350+ tests passing across all major features
- Merge-on-write tests need updating for new post-commit design
- JSON fields: 114/114 tests, Bucket aggregations: 4/4, Partitioned datasets: 7/7
- PURGE INDEXTABLE: 40/40, Purge-on-write: 8/8, DESCRIBE ENVIRONMENT: 10/10
- Merge I/O: 34/34, Optimized Writes: 44/44

## Development Principles

- **Branch Workflow**: All work must be done on a feature branch and submitted as a pull request. Never commit directly to `main`.
- **Design First**: All major features should have design documents before implementation
- **Test Coverage**: Maintain 100% test pass rate with comprehensive integration tests
- **Performance**: Benchmark all major features against baseline
- **Documentation**: Keep CLAUDE.md and reference docs updated with implementation details

## Reference Documentation

- `docs/reference/configuration.md` - All configuration settings (writer, state, merge, cache, read optimization, etc.)
- `docs/reference/sql-commands.md` - Full SQL command syntax, examples, and output schemas
- `docs/reference/features.md` - Detailed feature docs (batch retrieval, JSON fields, bucket aggregations, purge-on-write, optimized writes)
- `docs/reference/cloud-configuration.md` - S3, Azure, Unity Catalog integration
- `docs/reference/field-indexing.md` - Field types, typemap syntax, tokenizers, token length limits
- `docs/reference/protocol.md` - Protocol specification (transaction log format, versions V1-V4)
- `docs/reference/table-protocol.md` - Table protocol specification (ACID, schema evolution, time travel)
- `llms.txt` - Complete document index
