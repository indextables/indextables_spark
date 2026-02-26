# Features Reference

Detailed documentation for IndexTables4Spark features.

---

## Batch Retrieval Optimization

Dramatically reduces S3 GET requests (90-95%) and improves read latency (2-3x) for queries returning 50+ documents. Enabled by default with balanced profile.

**Performance Impact:**
- Without optimization: 1,000 docs = 1,000 S3 requests, ~3.4 seconds
- With optimization: 1,000 docs = 50-100 S3 requests, ~1.5-2.0 seconds
- Per-batch-operation concurrency: Each batch retrieval uses up to `maxConcurrentPrefetch` concurrent requests (scales with Spark executors)

### Example 1: Default Behavior (Automatic)
```scala
// Batch optimization enabled by default - no configuration needed
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")
df.filter($"score" > 0.5).collect()  // Automatically optimized for batches >= 50 docs
```

### Example 2: Aggressive Profile for Cost Optimization
```scala
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "aggressive")
  .load("s3://bucket/path")
df.groupBy("category").count().show()
```

### Example 3: Custom Parameters
```scala
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "balanced")
  .option("spark.indextables.read.batchOptimization.maxRangeSize", "32M")
  .option("spark.indextables.read.batchOptimization.gapTolerance", "2M")
  .option("spark.indextables.read.batchOptimization.maxConcurrentPrefetch", "16")
  .load("s3://bucket/path")
df.count()
```

### Example 4: Conservative Profile for Memory-Constrained Environments
```scala
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.profile", "conservative")
  .load("s3://bucket/path")
df.filter($"status" === "active").show()
```

### Example 5: Disable Optimization for Debugging
```scala
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.read.batchOptimization.enabled", "false")
  .load("s3://bucket/path")
df.show()
```

### Example 6: Session-Level Configuration
```scala
spark.conf.set("spark.indextables.read.batchOptimization.profile", "aggressive")
spark.conf.set("spark.indextables.read.adaptiveTuning.enabled", "true")
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")
df.filter($"date" >= "2024-01-01").count()
```

**When to Use:**
- Queries returning 50+ documents
- High-frequency read workloads
- Cost-sensitive S3 operations
- Aggregate queries with large result sets

**When NOT to Use:**
- Single document lookups (automatically bypassed)
- Very small result sets (<10 documents)

---

## JSON Fields (Struct/Array/Map)

Automatic detection and indexing for complex Spark types with full filter pushdown.

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

// Supported operators: ===, >, >=, <, <=, isNull, isNotNull, &&, ||
```

---

## Bucket Aggregations

SQL functions for time-series analysis and numeric distribution analysis, executed directly in Tantivy.

**SQL Functions:**
- `indextables_histogram(column, interval)` - Fixed-interval numeric bucketing
- `indextables_date_histogram(column, interval)` - Time-based bucketing (supports: `ms`, `s`, `m`, `h`, `d`)
- `indextables_range(column, name1, from1, to1, ...)` - Custom named range buckets

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

---

## Purge-On-Write (Automatic Table Hygiene)

Automatic cleanup of orphaned splits and old transaction logs during write operations.

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

**Features:**
- Disabled by default - must be explicitly enabled
- Two trigger modes: after merge-on-write, or after N write operations
- Automatic credential propagation from write options
- Per-session counters tracked per table path
- Separate retention periods for splits vs transaction logs
- Graceful failure handling - purge failures don't fail writes

**Complete automatic table hygiene (merge + purge):**
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.mergeOnWrite.enabled", "true")
  .option("spark.indextables.mergeOnWrite.targetSize", "4G")
  .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "2.0")
  .option("spark.indextables.purgeOnWrite.enabled", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
  .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "20")
  .option("spark.indextables.purgeOnWrite.splitRetentionHours", "168")
  .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "720")
  .save("s3://bucket/path")
```

---

## Companion/Sync Indexes

Build companion search indexes for existing Delta Lake, Parquet, and Apache Iceberg tables without duplicating data. Companion splits store only the Tantivy index and reference the original parquet files, achieving 45-70% split size reduction compared to standard IndexTables indexes. This lets you add full-text search and filter pushdown to tables managed by other systems.

### When to Use

- You have existing Delta, Parquet, or Iceberg tables and want fast full-text search or filter pushdown without migrating data
- You want to keep your source-of-truth table format (e.g., Delta Lake) while adding search capabilities
- You need incremental sync so the companion index stays up to date as the source table changes
- You want to reduce storage costs compared to a full data copy (companion splits are 45-70% smaller)

### Supported Formats

| Format | Reader | Versioned | Notes |
|--------|--------|-----------|-------|
| **Delta Lake** | delta-kernel-rs (JNI) | Yes (Delta version) | Supports `FROM VERSION` for starting point, Unity Catalog via `CATALOG`/`TYPE` |
| **Parquet** | Directory listing | No | Requires `SCHEMA SOURCE` pointing to a sample parquet file for schema inference |
| **Apache Iceberg** | iceberg-rust (JNI) | Yes (snapshot ID) | Supports `FROM SNAPSHOT`, requires `CATALOG`/`TYPE`/`WAREHOUSE`. Only Parquet data files are supported. |

### How It Works

1. **Initial build**: The command reads all data files from the source table, groups them by partition and target input size, and dispatches indexing tasks across Spark executors using batched concurrent execution via the FAIR scheduler.

2. **Incremental sync**: On subsequent runs, the command performs an anti-join reconciliation between the source table's current file list and the files already tracked by existing companion splits. Only new or changed files are indexed. Splits referencing files that no longer exist in the source are invalidated (removed), and any remaining valid files from those invalidated splits are re-indexed alongside the new files.

3. **Commit**: Each batch of indexing results is committed atomically to the companion table's transaction log, including both new `AddAction` entries and `RemoveAction` entries for invalidated splits.

The anti-join uses relative file paths, making it bucket-independent and compatible with cross-region failover scenarios.

### Examples

**Build a companion index for a Delta table:**
```sql
BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/events'
  AT LOCATION 's3://bucket/events_index';
```

**Incremental sync (run the same command again):**
```sql
-- Only new/changed files are indexed; removed files are invalidated
BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/events'
  AT LOCATION 's3://bucket/events_index';
```

**Delta with field configuration and partition filter:**
```sql
BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/logs'
  INDEXING MODES ('message':'text', 'trace_id':'exact_only')
  FASTFIELDS MODE PARQUET_ONLY
  TARGET INPUT SIZE 1G
  WHERE year = '2024'
  AT LOCATION 's3://bucket/logs_index';
```

**Parquet directory with schema source:**
```sql
BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/data'
  SCHEMA SOURCE 's3://bucket/data/part-00000.parquet'
  AT LOCATION 's3://bucket/data_index';
```

**Iceberg with catalog and warehouse:**
```sql
BUILD INDEXTABLES COMPANION FOR ICEBERG 'analytics.web_events'
  CATALOG 'uc_catalog' TYPE 'rest'
  WAREHOUSE 's3://unity-warehouse/iceberg'
  AT LOCATION 's3://bucket/web_events_index';
```

**Read the companion index:**
```scala
val df = spark.read.format("io.indextables.provider.IndexTablesProvider")
  .load("s3://bucket/events_index")

// Standard filter pushdown (for string fields)
df.filter($"status" === "error").show()

// Full-text search (for text fields)
df.createOrReplaceTempView("events")
spark.sql("SELECT * FROM events WHERE message indexquery 'connection timeout'").show()
```

### Read Path

Companion tables are read via the standard IndexTables DataSource API. At read time, the scan builder detects companion metadata in the transaction log and automatically resolves the source table path for parquet file retrieval.

**Read-only enforcement**: Companion tables reject direct writes with a clear error. To update a companion index, run `BUILD INDEXTABLES COMPANION` again for incremental sync.

**Aggregate pushdown guard**: Aggregate queries on companion tables (e.g., `COUNT(*)`, `SUM()`) are subject to a safety guard. If Spark requests aggregate pushdown but the companion table's field configuration does not support it, the query fails fast with a descriptive error rather than returning silently incorrect results. To disable this guard (not recommended), set `spark.indextables.read.requireAggregatePushdown` to `false`.

### Key Configuration

Configuration is set via Spark session properties or as DataSource options. For the full list of `BUILD INDEXTABLES COMPANION` options (including `INDEXING MODES`, `FASTFIELDS MODE`, `DRY RUN`, and more), see the [Build Companion](sql-commands.md#build-companion) SQL command reference.

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.indextables.companion.sync.batchSize` | `<defaultParallelism>` | Number of indexing tasks per batch |
| `spark.indextables.companion.sync.maxConcurrentBatches` | `6` | Maximum batches running concurrently |
| `spark.indextables.companion.writerHeapSize` | `1G` | Writer heap memory per executor |
| `spark.indextables.companion.readerBatchSize` | `8192` | Rows per read batch from source parquet files |
| `spark.indextables.companion.schedulerPool` | `indextables-companion` | FAIR scheduler pool for indexing tasks |

### Limitations

- **Iceberg**: Only Parquet data files are supported (ORC and Avro are not).
- **Read-only**: Companion tables cannot be written to via the DataSource API. Use `BUILD INDEXTABLES COMPANION` for all updates.
- **Parquet directories**: Unversioned. There is no version tracking, so the anti-join reconciliation operates purely on file presence.
- **Compact string modes**: Available only for companion builds. See [Compact String Indexing](#compact-string-indexing-companion-splits) below for details on `exact_only`, `text_uuid_exactonly`, and other size-reduction modes.

---

## Compact String Indexing (Companion Splits)

Reduces index size for high-cardinality string fields and UUID-heavy text in companion splits. Configured via `INDEXING MODES` in the `BUILD INDEXTABLES COMPANION` command.

### Modes

| Mode | Effect | Size Reduction |
|------|--------|----------------|
| `exact_only` | Replace string with U64 xxHash64 hash. Supports `EqualTo` filter pushdown (hash-based). | ~80% for ID fields |
| `text_uuid_exactonly` | Strip UUIDs from text, index with "default" tokenizer. UUIDs go to companion U64 hash field for exact lookup. | Significant for UUID-heavy logs |
| `text_uuid_strip` | Strip UUIDs from text, index with "default" tokenizer. UUIDs discarded entirely. | Maximum for UUID-heavy logs |
| `text_custom_exactonly:<regex>` | Same as `text_uuid_exactonly` but with custom regex for pattern extraction. | Depends on pattern frequency |
| `text_custom_strip:<regex>` | Same as `text_uuid_strip` but with custom regex for pattern stripping. | Depends on pattern frequency |

### When to Use Each Mode

- **`exact_only`**: ID columns (trace IDs, request IDs, UUIDs) where you only need exact equality lookups. Wildcard/regex queries are not supported.
- **`text_uuid_exactonly`**: Log messages containing UUIDs where you need both text search on the message and exact UUID lookup.
- **`text_uuid_strip`**: Log messages containing UUIDs where you only need text search and UUIDs are noise.
- **`text_custom_exactonly:<regex>`**: Like `text_uuid_exactonly` but for custom patterns (e.g., order numbers, SSN-like patterns).
- **`text_custom_strip:<regex>`**: Like `text_uuid_strip` but for custom patterns.

### Query Behavior

Query rewriting is handled transparently by tantivy4java:

- **`exact_only`**: `EqualTo`, `parseQuery()`, and phrase queries are auto-converted to hashed U64 term queries. Wildcard/regex/phrase_prefix queries are blocked with a clear error.
- **`text_*_exactonly`**: Regex-matching values route to companion hash field; non-matching values search stripped text.
- **`text_*_strip`**: All queries operate on stripped text.

### Filter Pushdown

- **`exact_only`**: `EqualTo` is pushed down to tantivy (hash-based).
- **`text_*` modes**: `EqualTo` is NOT pushed down (deferred to Spark). Use IndexQuery for text search.

### Example

```sql
BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/logs'
  INDEXING MODES (
    'trace_id':'exact_only',
    'request_id':'exact_only',
    'message':'text_uuid_exactonly',
    'raw_log':'text_uuid_strip'
  )
  AT LOCATION 's3://bucket/log_index';
```

```scala
// After building companion:
val df = spark.read.format("io.indextables.provider.IndexTablesProvider")
  .load("s3://bucket/log_index")

// exact_only: EqualTo pushed down (hash-based)
df.filter($"trace_id" === "abc-123-def-456").show()

// text_uuid_exactonly: Use IndexQuery for text search
df.createOrReplaceTempView("logs")
spark.sql("SELECT * FROM logs WHERE message indexquery 'error timeout'").show()
```

---

## Optimized Writes

Iceberg-style shuffle before writing. Disabled by default. When enabled, uses Spark's `RequiresDistributionAndOrdering` to shuffle data by partition columns, producing well-sized (~1GB) splits. Uses AQE advisory partition size with history-based or sampling-based estimation.

```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.write.optimizeWrite.enabled", "true")
  .option("spark.indextables.write.optimizeWrite.targetSplitSize", "1G")
  .partitionBy("date")
  .save("s3://bucket/path")
```
