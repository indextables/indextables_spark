# Native Porting Analysis: JVM → Rust

**Date:** 2026-03-28

## Current State

**Code split:** ~85K lines Rust (tantivy4java native) / ~63K lines Scala (JVM)

### What's Already Native (Rust)
- Tantivy index read/write/search/merge
- Arrow FFI columnar data exchange
- Split cache management (L1/L2)
- Transaction log I/O (Avro state, checkpoints, versioning, optimistic concurrency)
- Aggregations (COUNT, SUM, AVG, MIN, MAX, bucket aggs)
- Parquet companion reader (Delta/Iceberg)
- Query parsing and execution
- Document retrieval and serialization

### What Remains in the JVM (69 core files, ~63K lines)

Categorized into tiers based on how much sense it makes to port to Rust.

---

## Tier 1: Strong Candidates for Rust (engine-agnostic today)

These have **zero or trivial** Spark dependencies and represent core business logic that every query engine would need.

### 1. Cloud Storage I/O (~15 files, largest code mass)
**Files:** `io/CloudStorageProvider`, `S3CloudStorageProvider` (100+ KB), `AzureCloudStorageProvider`, `HadoopCloudStorageProvider`, `S3MultipartUploader`, `ProtocolBasedIOFactory`
**Plus merge I/O:** `AsyncDownloader`, `S3AsyncDownloader`, `AzureAsyncDownloader`, `CloudDownloadManager`, `MergeUploader`, `PriorityDownloadQueue`, `MergeIOThreadPools`

- 13 of 15 files have **zero** Spark imports
- S3 provider alone is 100+ KB of AWS SDK v2 async client code
- Every engine (Trino, Presto, DuckDB) would need identical S3/Azure I/O
- Rust has excellent async I/O (tokio, object_store crate) — this would likely be *faster* and *smaller* in Rust
- **Impact: HIGH** — eliminates the need to reimplement cloud I/O per engine

### 2. Transaction Log Data Models & Serialization (~5 files)
**Files:** `Actions.scala`, `ActionJsonSerializer.scala`, `AddActionConverter.scala`, `ProtocolVersion.scala`, `EnhancedTransactionLogCache.scala`

- All **zero** Spark imports
- Pure data classes (AddAction, RemoveAction, MetadataAction, ProtocolAction) + JSON serialization
- Protocol version management (V1-V4 feature flags)
- Already partially duplicated — native Rust txlog has its own models that `AddActionConverter` bridges to
- **Impact: MEDIUM** — eliminates the JSON serialization bridge between JVM and native

### 3. Partition Pruning Logic (~4 files)
**Files:** `PartitionPruning.scala`, `PartitionIndex.scala`, `PartitionFilterCache.scala`, `FilterSelectivityEstimator.scala`

- `PartitionIndex` and cache: zero Spark imports
- `PartitionPruning`: only uses `Filter` interface (trivially abstractable)
- ~600 lines of optimization logic (selectivity ordering, short-circuit evaluation, parallel pruning for >100 files)
- **Impact: MEDIUM** — every engine needs partition pruning; doing it natively avoids re-implementation

### 4. Configuration (~4 files)
**Files:** `IndexTables4SparkConfig.scala`, `IndexTables4SparkSQLConf.scala`, `ConfigParsingUtils.scala`, `SizeParser.scala`

- Zero Spark imports
- Pure config key definitions and parsing
- **Impact: LOW** — small code, but having a canonical config parser in native avoids drift between engine integrations

### 5. Statistics & Data Skipping (~4 files)
**Files:** `DataSkippingMetrics.scala`, `StatisticsTruncation.scala`, `ExpressionSimplifier.scala`, `FilterExpressionCache.scala`

- Mostly zero or light Spark dependencies
- Min/max statistics, filter simplification, truncation for long text fields
- **Impact: MEDIUM** — data skipping is critical for performance; shared implementation ensures consistency

### 6. Merge Planning & Orchestration (~3 files)
**Files:** `WorkerLocalSplitMerger.scala`, `AsyncMergeOnWriteConfig.scala`, `MergeIOConfig.scala`

- Already calls native tantivy4java for the actual merge
- The JVM layer adds: disk space checks, concurrency semaphores, config parsing
- **Impact: MEDIUM** — merge orchestration (which splits to merge, when) should be engine-agnostic

### 7. Purge Logic (~2 files)
**Files:** `PurgeOnWriteConfig.scala`, `PurgeOnWriteTransactionCounter.scala`

- Zero or light dependencies
- Transaction counting to decide when to auto-purge
- **Impact: LOW** — small, but logically belongs with native transaction log

### 8. Prewarm / Cache Management (~3 files)
**Files:** `AsyncPrewarmJobManager.scala`, `IndexComponentMapping.scala`, `PreWarmManager.scala`

- Prewarm job tracking and component mapping are engine-agnostic
- **Impact: LOW** — split cache is already native; prewarm orchestration could follow

---

## Tier 2: Moderate Candidates (need interface abstraction)

These use Spark types like `StructType` and `Filter` but the logic itself is engine-agnostic. Porting requires defining native equivalents of these types.

### 9. Filter-to-Query Conversion (~2 files)
**Files:** `FiltersToQueryConverter.scala`, `JsonPredicateTranslator.scala`

- Converts Spark filter predicates to Tantivy query AST
- The *logic* is universal (equality → TermQuery, range → RangeQuery, etc.)
- Requires a native `Filter` enum/trait in Rust that each engine adapter maps into
- **Impact: HIGH** — this is the core pushdown logic; duplicating per engine would be error-prone

### 10. Schema Mapping (~3 files)
**Files:** `SchemaMapping.scala`, `SchemaConverter.scala`, `SparkSchemaToTantivyMapper.scala`

- Converts between engine schema and Tantivy schema
- Each engine has its own schema representation, but the Tantivy mapping is universal
- **Impact: MEDIUM** — define a native schema type; each engine adapter converts to it

### 11. JSON Field Support (~2 files)
**Files:** `SparkToTantivyConverter.scala`, `TantivyToSparkConverter.scala`

- Struct/Array/Map to Tantivy JSON field conversion
- Logic is reusable; just needs engine-neutral row/column types
- **Impact: MEDIUM**

### 12. Row/Data Conversion (~2 files)
**Files:** `RowConverter.scala`, `TypeConversionUtil.scala`

- Converts between Spark InternalRow and search results
- Each engine will need its own version, but common patterns could be shared
- **Impact: LOW** — inherently engine-specific at the edges

---

## Tier 3: Must Remain Engine-Specific

These are deeply coupled to Spark internals and are the proper "adapter layer" for each engine.

| Category | Files | Why it stays |
|----------|-------|-------------|
| **DataSource V2 API** | ~22 files in `core/` | Spark-specific scan/write/partition contracts |
| **SQL Parser & Commands** | ~33 files in `sql/` | Spark Catalyst AST, RunnableCommand |
| **Catalyst Rules** | 2 files in `catalyst/` | Spark optimizer integration |
| **Custom Expressions** | 5 files in `expressions/` | Spark expression framework |
| **Spark Metrics** | 2 files in `metrics/`, `org.apache.spark.sql.indextables/` | Spark UI integration |
| **Auth/Unity Catalog** | 3 files in `auth/` | Spark credential provider API |
| **Companion Sync** | 10 files in `sync/` | Uses Spark to read Delta/Iceberg/Parquet |
| **Arrow FFI Bridge** | 4 files in `arrow/` | Spark ColumnarBatch integration |

These ~80 files / ~35K lines are the Spark adapter. Each new engine (Trino, DuckDB) needs its own equivalent adapter, but it should be *much thinner* if the core logic moves to Rust.

---

## Recommended Porting Roadmap

### Phase 1: Cloud Storage (highest ROI)
Port `CloudStorageProvider` + S3/Azure implementations to Rust using the `object_store` crate. This is the **largest single block** of engine-agnostic code (~15 files, ~200+ KB). Benefits:
- Eliminates AWS/Azure SDK dependency from every engine's JVM classpath
- Rust `object_store` already supports S3, Azure, GCS, local FS
- Merge I/O (download/upload) comes along for free
- Single binary handles all cloud I/O across all engines

### Phase 2: Filter to Query Conversion
Define a native `Filter` enum in Rust and port `FiltersToQueryConverter` + `JsonPredicateTranslator`. Each engine adapter then just maps its filter representation to the native enum. This ensures pushdown logic is identical across Spark/Trino/DuckDB.

### Phase 3: Partition Pruning + Statistics
Port `PartitionPruning`, `PartitionIndex`, `FilterSelectivityEstimator`, `StatisticsTruncation`, `DataSkippingMetrics`. These are already nearly dependency-free and represent critical query optimization logic.

### Phase 4: Merge Orchestration
Move merge planning (which splits to merge, target sizes, batch grouping) into native code. The JVM already delegates the actual merge to native — this just moves the *decision-making* too.

### Phase 5: Schema & Config Canonicalization
Define native schema and config types. Each engine adapter converts to/from them. Eliminates drift in how field types, tokenizers, and options are interpreted.

---

## Summary

| Layer | Lines (est.) | Files | Port to Rust? |
|-------|-------------|-------|---------------|
| Already native (Rust) | ~85K | 203 | Already done |
| **Tier 1: Port** (engine-agnostic) | ~18K | ~35 | Yes — phases 1-5 |
| **Tier 2: Port with abstraction** | ~5K | ~9 | Yes — with native Filter/Schema types |
| **Tier 3: Keep per-engine** | ~35K | ~80 | No — Spark adapter layer |
| Utilities/glue | ~5K | ~20 | Partial — case by case |

Moving Tier 1 + Tier 2 (~23K lines, ~44 files) to Rust would mean each new engine integration only needs to implement the ~35K-line adapter layer (DataSource API, SQL parser, expression framework), while all core business logic — storage, transaction log, pruning, merging, filtering, statistics — lives in a single native library shared across Spark, Trino, Presto, and DuckDB.
