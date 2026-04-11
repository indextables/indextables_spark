# 02 — Read Path

This chapter covers everything that happens between `spark.read.format(...).load(path)` and a `ColumnarBatch` flowing into Spark. It lives primarily in `spark/core/` and `spark/catalyst/`.

## The scan pipeline

### `core/IndexTables4SparkScanBuilder.scala`
The first step for a read. Implements `ScanBuilder` plus every `SupportsPushDown*` interface Spark exposes (`SupportsPushDownFilters`, `SupportsPushDownAggregates`, `SupportsPushDownLimit`, and related). Its responsibilities:

- Convert Spark filters to Tantivy queries via `core/FiltersToQueryConverter`.
- Recognize `IndexQueryFilter` / `IndexQueryAllFilter` wrappers injected by the catalyst rules.
- Detect aggregation plans (simple aggregate vs GROUP BY) and bucket expressions.
- Store per-relation pushdown state in a `ThreadLocal` that the catalyst rules also use to coordinate with the optimizer.
- Decide which `Scan` implementation to build (`IndexTables4SparkScan`, `SimpleAggregateScan`, `GroupByAggregateScan`, or `TransactionLogCountScan`).

### `core/IndexTables4SparkScan.scala`
The main scan for row-producing queries. Implements `Scan` and `SupportsReportStatistics`. It:

- Uses the transaction log to list `AddAction`s.
- Applies partition pruning via `transaction/PartitionPredicateUtils` and `transaction/SparkFilterToNativeFilter`.
- Packs splits into Spark tasks (configurable splits-per-task), reporting locality from `storage/DriverSplitLocalityManager`.
- Builds a `PartitionReaderFactory` that dispatches to the right columnar reader.

### `core/IndexTables4SparkPartitions.scala`
Defines the `InputPartition` classes that the scan emits:

- `IndexTables4SparkStandardPartition` — rows or projections.
- `IndexTables4SparkSimpleAggregatePartition` — COUNT/SUM/AVG/MIN/MAX without grouping.
- `IndexTables4SparkGroupByAggregatePartition` — GROUP BY including bucket aggregations.

Each partition carries a serialized list of `AddAction`s and enough config to rebuild a `SplitSearchEngine` on the executor.

### `core/IndexTables4SparkStatistics.scala`
Implements Spark's `Statistics` so the optimizer can cost-estimate an IndexTables scan. Numbers come from the `numRecords` and `sizeInBytes` fields of `AddAction` plus per-column min/max stats when present.

### `core/IndexTablesFileIndex.scala`
A `FileIndex` backed by the transaction log instead of `FileSystem.listStatus`. Used by callers that need a `FileIndex` (some of the extension paths) without the cost of a directory scan. It also defines `SerializableFileInfo`, a wrapper that avoids serializing Hadoop `FileStatus` objects (which carry a `Configuration`).

## Partition readers

All readers share a setup block factored into `SplitReaderContext`.

### `core/SplitReaderContext.scala`
Encapsulates path resolution, cache configuration, footer validation, `SplitSearchEngine` construction, partition-filter separation, range optimization, and `IndexQuery` post-processing. Every reader below starts by constructing one.

### `core/CompanionColumnarPartitionReader.scala`
The standard single-split columnar reader. Implements `PartitionReader[ColumnarBatch]`. For each batch it runs a search through `search/SplitSearchEngine`, imports the result via `arrow/ArrowFfiBridge`, and yields a `ColumnarBatch`. Used for both regular IndexTables tables and companion indexes.

### `core/CompanionColumnarMultiSplitPartitionReader.scala`
Wraps multiple single-split readers so one Spark task can process several splits sequentially. Supports early termination when a `LIMIT` is pushed down — once the limit is hit the remaining splits are skipped without being opened.

### `core/SimpleAggregateColumnarReader.scala`
Specialized reader for COUNT/SUM/AVG/MIN/MAX without GROUP BY. Calls `SplitSearcher.aggregateArrowFfi()` once per split, assembles a single-row `ColumnarBatch`, and reports it. Collaborates with `arrow/AggregationArrowFfiHelper`.

### `core/GroupByAggregateColumnarReader.scala`
Reader for GROUP BY aggregations including `DateHistogram`, `Histogram`, and `Range` bucket aggregations. Builds a `SplitAggregation` tree from the Spark aggregate expressions plus the bucket configs left by `V2BucketExpressionRule`, executes it via Arrow FFI, and assembles result rows.

### `core/GroupByColumnarReaderUtils.scala`
Object of shared helpers for the GROUP BY readers: empty batch construction, type coercion (terms → `UTF8String`, buckets → appropriate types), partition-column injection. Keeps single-split and multi-split readers in sync.

## Specialized scans

### `core/IndexTables4SparkSimpleAggregateScan.scala`
Scan variant that produces `SimpleAggregatePartition`s. Builds Tantivy aggregation requests from the Spark aggregate expressions and leaves the heavy lifting to `SimpleAggregateColumnarReader`.

### `core/IndexTables4SparkGroupByAggregateScan.scala`
Scan variant for GROUP BY. Manages cross-split aggregation result combination and builds the output schema (including bucket buckets and nested aggregations).

### `core/TransactionLogCountScan.scala`
Ultra-fast `COUNT(*)` and `GROUP BY <partition-col>, COUNT(*)` path. It never opens a split — it sums `numRecords` from `AddAction`s in the transaction log. Used when the scan builder determines that no data-level predicates remain.

## Filter and query translation

### `core/FiltersToQueryConverter.scala`
Converts a tree of Spark `Filter`s to a Tantivy `SplitQuery`. Handles `EqualTo`, range predicates, `StringContains`/`StartsWith`/`EndsWith`, `IsNull`, `In`, boolean combinators, and the custom `IndexQueryFilter`/`IndexQueryAllFilter`. Also performs range merging (multiple `>`/`<` filters on the same field collapse into a single range query).

### `schema/SchemaMapping.scala`
Used by the reader to translate between a Spark `StructType` and the Tantivy schema recovered from split metadata. See chapter 05.

## Catalyst rules

These rules run inside the Spark optimizer after the DataSource V2 logical plan is built and must coordinate with `IndexTables4SparkScanBuilder` via `ThreadLocal`s.

### `catalyst/V2IndexQueryExpressionRule.scala`
Finds `IndexQueryExpression` and `IndexQueryAllExpression` nodes in the logical plan and rewrites them as `IndexQueryV2Filter` so Spark pushes them through its filter interfaces. It also clears stale ThreadLocal state between analysis and optimization phases (which is important for multi-statement sessions).

### `catalyst/V2BucketExpressionRule.scala`
Detects `DateHistogramExpression` / `HistogramExpression` / `RangeExpression` in `Aggregate` GROUP BY keys and rewrites them as ordinary attribute references, stashing the bucket configs where `IndexTables4SparkScanBuilder` can find them. Without this rule Spark would refuse to push the aggregation down because the group key would be an unknown expression.

## Split locality

Locality matters for read performance: if the same split is consistently read by the same executor host, the Tantivy `SplitCacheManager` and the L2 disk cache on that host stay warm. These modules exist to make that happen.

### `storage/DriverSplitLocalityManager.scala`
Driver-side sticky split-to-host assignment. Given a list of splits and the set of available executors, it returns stable assignments so the same split is reliably picked up by the same host across queries. Replaces an older broadcast-based approach: persistent assignments live on the driver and feed `IndexTables4SparkScan`'s `InputPartition.preferredLocations()`. Also tracks per-split prewarm state so the prewarm subsystem below knows which splits are already warm on which hosts.

## Cache prewarming

Prewarm is a read-performance subsystem: it warms the Tantivy index components a query will need *before* the query runs, so the first batch of every split is hot. Triggered by the `PREWARM INDEXTABLES CACHE` SQL command (chapter 08) and optionally implicitly before large scans.

### `prewarm/PreWarmManager.scala`
Two-phase orchestration:

1. **Pre-scan** — using `DriverSplitLocalityManager`, distribute warmup tasks to the executors that already hold the relevant splits in cache.
2. **Post-warm** — when the main query starts, join on the warmup futures so no split is read cold.

Returns `PreWarmStats` with cache-hit/miss counts for observability.

### `prewarm/AsyncPrewarmJobManager.scala`
JVM-wide singleton tracking async prewarm jobs on executors. Semaphore-bounded concurrency, a job registry for `DESCRIBE INDEXTABLES PREWARM JOBS`, and auto-cleanup of old completed jobs. Structurally mirrors `AsyncMergeOnWriteManager` (chapter 07) — same pattern, different purpose.

### `prewarm/IndexComponentMapping.scala`
Maps SQL-level component names (`TERM`, `FASTFIELD`, `POSTINGS`, `POSITIONS`, `FIELDNORM`, `STORE`) to the `tantivy4java` `IndexComponent` enum, and defines the default component set warmed when the user does not specify one.

## How the pieces connect

```
IndexTables4SparkTable.newScanBuilder
 → IndexTables4SparkScanBuilder
     ├─ FiltersToQueryConverter        (Spark Filter → Tantivy query)
     ├─ (reads ThreadLocal from V2IndexQueryExpressionRule / V2BucketExpressionRule)
     └─ build()
         ├─ IndexTables4SparkScan            (standard rows)
         ├─ IndexTables4SparkSimpleAggregateScan
         ├─ IndexTables4SparkGroupByAggregateScan
         └─ TransactionLogCountScan           (metadata-only COUNT/GROUP BY partition)

Scan.planInputPartitions()
 → transaction log list + partition pruning
 → IndexTables4SparkPartitions (Standard / SimpleAgg / GroupByAgg)
     └─ on executor: PartitionReader
         ├─ SplitReaderContext            (shared setup)
         ├─ SplitSearchEngine             (search/)
         ├─ ArrowFfiBridge                (arrow/)
         └─ ColumnarBatch → Spark

Locality / prewarm (optional, before the scan):
  DriverSplitLocalityManager    (sticky split→host assignments)
        ↓
  PreWarmManager
    └─ AsyncPrewarmJobManager (on executors)
         └─ warms IndexComponentMapping components
```

Write-path chapter next.
