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

## Optimized Writes

Iceberg-style shuffle before writing. Disabled by default. When enabled, uses Spark's `RequiresDistributionAndOrdering` to shuffle data by partition columns, producing well-sized (~1GB) splits. Uses AQE advisory partition size with history-based or sampling-based estimation.

```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.write.optimizeWrite.enabled", "true")
  .option("spark.indextables.write.optimizeWrite.targetSplitSize", "1G")
  .partitionBy("date")
  .save("s3://bucket/path")
```
