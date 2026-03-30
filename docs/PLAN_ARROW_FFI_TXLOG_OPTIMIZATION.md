# Plan: Arrow FFI Transaction Log Optimization

**Goal:** Eliminate JSON serialization overhead and redundant JVM-side data structures by using Arrow FFI as the primary interchange format between the native transaction log and JVM code.

**Depends on:** 4 tantivy4java feature requests (see `TANTIVY4JAVA_ARROW_FFI_TXLOG_FEATURE_REQUESTS.md`)

---

## Overview

Three changes, in priority order:

| # | Change | JVM Code Removable | Estimated Impact |
|---|--------|-------------------|-----------------|
| 1 | Arrow FFI for `listFiles()` with native partition pruning, data skipping, and cooldown filtering | 6 files + ~300 lines in scan (~1,800+ LOC) | Eliminates per-file object allocation, all JVM-side filtering, and JSON-in-Arrow |
| 2 | Arrow FFI for `addFiles()` / `commitMergeSplits()` write path (unified schema) | 1 file (~290 LOC) | Eliminates JSON round-trip on every write |
| 3 | Arrow FFI for retained files cursor (purge path) | 0 files (simplifies existing code) | Replaces TANT binary with standard Arrow |

---

## Change 1: Arrow FFI Read Path with All Native Filtering

### Current flow (hot path, every scan)

```
Native: reads manifests → has Vec<FileEntry> in Rust memory
  → serializes each FileEntry to TANT binary (custom format)
    → JNI returns byte[] to JVM
      → JVM deserializes TANT → TxLogFileEntry (Java object per file)
        → AddActionConverter copies fields → AddAction (Scala case class per file)
          → PartitionPruning.scala filters by partition values
            → shouldSkipFile() filters by min/max statistics
              → filterFilesInCooldown() excludes skipped files (separate JNI round-trip)
                → Scan planning uses surviving AddActions
```

**Problems:**
- Every file entry crosses JNI twice: once as TANT bytes, once as field-by-field Java object
- `AddActionConverter.toAddAction()` allocates a new Scala case class per file
- All filtering happens *after* all files are materialized on JVM heap
- Tables with 100K+ splits materialize 100K+ objects just to discard most of them
- Cooldown/skip filtering requires a *second* JNI round-trip (`nativeListSkipActions`) followed by JVM-side set subtraction
- Scan builder calls `getPartitionColumns()`, `getSchema()`, AND `listFiles()` separately — 3 snapshot reads for the same version

### Proposed flow

```
JVM: converts Spark Filter[] → partition filter JSON + data filter JSON
  → single JNI call with filter JSON + Arrow struct addresses
    → Native: reads snapshot (cached) — returns schema, partition cols, protocol alongside files
      → prunes manifests (existing may_match_bounds)
        → prunes individual files by partition values (existing evaluate())
          → skips files by min/max statistics (new can_skip_by_stats())
            → excludes cooldown files (existing list_skip_actions)
              → exports ONLY surviving files as Arrow RecordBatch (no JSON-in-Arrow)
                → JVM: ArrowFfiBridge imports batch
                  → extracts AddAction objects ONLY for surviving files (post-filtering)
                    → schema/partitionCols/protocol available from same call (no extra JNI)
```

**Key insight on AddAction construction:** `IndexTables4SparkInputPartition` holds a full `AddAction` that gets Java-serialized to each executor. We cannot avoid constructing AddAction objects entirely — but the win is that we only construct them for **surviving files** after all native filtering, not for the full file list. For a table with 100K splits where filtering reduces to 500 relevant splits, we go from 100K object allocations to 500.

### tantivy4java changes required

**Feature Request 1:** `nativeListFilesArrowFfi()` — new JNI entry point that:
- Accepts partition filter JSON (nullable) — evaluated against `partition_values`
- Accepts data filter JSON (nullable) — evaluated against `min_values`/`max_values` for data skipping
- Accepts `excludeCooldownFiles` flag — filters out skipped/cooldown files in the same pass
- Accepts `partitionColumns` array — native exports partition values as separate typed Arrow columns (no JSON maps)
- Reads snapshot, applies manifest-level pruning via `prune_manifests()`
- Reads matching manifests, applies file-level partition pruning via `evaluate()`
- Applies min/max data skipping via new `can_skip_by_stats()` method
- Excludes cooldown files via existing `list_skip_actions()` (no second JNI round-trip)
- Replays post-checkpoint changes (adds/removes)
- Exports surviving files via Arrow FFI v2 schema — **zero JSON-in-Arrow columns**
  - `partition_values` map → dynamic `partition:{name}` columns
  - `min_values`/`max_values`/`stats` → not exported (consumed during native filtering)
  - `split_tags`/`companion_source_files` → native Arrow `List<Utf8>`
- Returns row count (jlong)

See feature request for full specification.

### JVM-side changes

#### New code (~100 lines total)

**`SparkFilterToNativeFilter.scala`** (~80 lines) — thin converter used for both partition and data filters:
```scala
object SparkFilterToNativeFilter {
  /** Convert Spark filters to PartitionFilter JSON for native evaluation. */
  def convert(filters: Array[Filter]): String = {
    val pf = filters.map {
      case EqualTo(col, value) => PartitionFilter.eq(col, value.toString)
      case GreaterThan(col, value) => PartitionFilter.gt(col, value.toString)
      case GreaterThanOrEqual(col, value) => PartitionFilter.gte(col, value.toString)
      case LessThan(col, value) => PartitionFilter.lt(col, value.toString)
      case LessThanOrEqual(col, value) => PartitionFilter.lte(col, value.toString)
      case In(col, values) => PartitionFilter.in(col, values.map(_.toString): _*)
      case IsNull(col) => PartitionFilter.isNull(col)
      case IsNotNull(col) => PartitionFilter.isNotNull(col)
      case StringStartsWith(col, v) => PartitionFilter.startsWith(col, v)
      case StringEndsWith(col, v) => PartitionFilter.endsWith(col, v)
      case StringContains(col, v) => PartitionFilter.contains(col, v)
      case And(l, r) => PartitionFilter.and(convert(Array(l)), convert(Array(r)))
      case Or(l, r) => PartitionFilter.or(convert(Array(l)), convert(Array(r)))
      case Not(child) => PartitionFilter.not(convert(Array(child)))
    }
    if (pf.length == 1) pf.head.toJson()
    else PartitionFilter.and(pf: _*).toJson()
  }
}
```

**Modify `NativeTransactionLog`** — all filtering + metadata in one native call:
```scala
/** Result type wrapping Arrow batch + table metadata + filtering metrics */
case class NativeListFilesResult(
  files: Seq[AddAction],
  schema: StructType,
  partitionColumns: Seq[String],
  protocol: ProtocolAction,
  metadataConfig: Map[String, String],
  metrics: DataSkippingMetrics.ScanMetrics
)

override def listFiles(): Seq[AddAction] =
  listFilesArrow(partitionFilters = null, dataFilters = null, excludeCooldown = false).files

override def listFilesWithPartitionFilters(filters: Seq[Filter]): Seq[AddAction] = {
  val (partFilters, dataFilters) = separateFilters(filters, cachedPartitionColumns)
  listFilesArrow(
    partitionFilters = SparkFilterToNativeFilter.convertOrNull(partFilters),
    dataFilters = SparkFilterToNativeFilter.convertOrNull(dataFilters),
    excludeCooldown = false).files
}

def listFilesExcludingCooldown(filters: Seq[Filter] = Seq.empty): Seq[AddAction] = {
  val (partFilters, dataFilters) = separateFilters(filters, cachedPartitionColumns)
  listFilesArrow(
    partitionFilters = SparkFilterToNativeFilter.convertOrNull(partFilters),
    dataFilters = SparkFilterToNativeFilter.convertOrNull(dataFilters),
    excludeCooldown = true).files
}

/** Full result including metadata — used by scan builder to avoid separate JNI calls */
def listFilesWithMetadata(
    partitionFilters: Seq[Filter], dataFilters: Seq[Filter], excludeCooldown: Boolean
): NativeListFilesResult =
  listFilesArrow(
    SparkFilterToNativeFilter.convertOrNull(partitionFilters),
    SparkFilterToNativeFilter.convertOrNull(dataFilters),
    excludeCooldown)

private def listFilesArrow(
    partitionFilters: String, dataFilters: String, excludeCooldown: Boolean
): NativeListFilesResult = {
  val numCols = 25 // safe upper bound; native determines actual column count
  val bridge = new ArrowFfiBridge(numCols)
  val result = TransactionLogReader.nativeListFilesArrowFfi(
    nativeTablePath, nativeConfig, partitionFilters, dataFilters,
    excludeCooldown, bridge.arrayAddrs, bridge.schemaAddrs)

  val batch = bridge.importAsColumnarBatch()
  val partCols = result.partitionColumns.toSeq
  val files = ArrowAddActionExtractor.extract(batch, partCols)

  NativeListFilesResult(
    files = files,
    schema = DataType.fromJson(result.schemaJson).asInstanceOf[StructType],
    partitionColumns = partCols,
    protocol = mapper.readValue(result.protocolJson, classOf[ProtocolAction]),
    metadataConfig = parseConfigJson(result.metadataConfigJson),
    metrics = DataSkippingMetrics.ScanMetrics(
      result.totalFilesBeforeFiltering,
      result.filesAfterPartitionPruning,
      result.filesAfterDataSkipping,
      result.filesAfterCooldownFiltering)
  )
}
```

**Scan builder** switches from 3+ JNI calls to 1:
```scala
// Before: 3 separate JNI round-trips, each reading the same snapshot
val schema = transactionLog.getSchema()
val partitionCols = transactionLog.getPartitionColumns()
val allFiles = transactionLog.listFilesWithPartitionFilters(partitionFilters)
val afterSkipping = allFiles.filter(a => canFileMatchFilters(a, nonPartitionFilters))

// After: single call returns everything
val result = transactionLog.listFilesWithMetadata(partitionFilters, dataFilters, excludeCooldown = false)
val schema = result.schema
val partitionCols = result.partitionColumns
val files = result.files  // already fully filtered
DataSkippingMetrics.recordScan(tablePath, result.metrics)
```

**MergeSplitsCommand** switches from:
```scala
val allFiles = transactionLog.listFiles()
val filtered = transactionLog.filterFilesInCooldown(allFiles)
```
to:
```scala
val filtered = transactionLog.listFilesExcludingCooldown()
```

#### Files removable after this change

| File | Lines | Reason |
|------|-------|--------|
| `PartitionPruning.scala` | ~600 | Partition pruning now in native |
| `PartitionIndex.scala` | ~250 | O(1) lookup index no longer needed |
| `PartitionFilterCache.scala` | ~100 | Filter eval caching no longer needed |
| `FilterSelectivityEstimator.scala` | ~150 | Selectivity ordering now in native |
| `ExpressionSimplifier.scala` | ~200 | Filter simplification before pruning |
| `FilterExpressionCache.scala` | ~200 | Simplified filter caching |
| ~300 lines in `IndexTables4SparkScan.scala` | ~300 | `shouldSkipFile()`, `canFileMatchFilters()`, `canFilterMatchFile()`, `convertValuesForComparison()`, `convertNumericValues()` — all data skipping logic |
| ~60 lines in `SplitReaderContext.scala` | ~60 | `isRangeFilterRedundantByStats()` and min/max access — moved to native split search (Feature Request 4) |

**Also eliminable from `NativeTransactionLog.scala`:**
- `filterFilesInCooldown()` — cooldown handled natively
- `getFilesInCooldown()` — no longer called from JVM
- `getSkippedFiles()` — no longer called from JVM for filtering (keep for `DESCRIBE` commands)
- `getSchema()`, `getPartitionColumns()`, `getProtocol()` — still available but bypassed by `listFilesWithMetadata()` which returns everything in one call

**Total: ~1,860+ lines removed, ~200 lines added.**

#### Migration strategy

1. Add `listFilesArrow()` as a private method alongside existing `listFiles()`
2. Add a feature flag: `spark.indextables.transaction.arrowFfi.enabled` (default false)
3. Toggle between old path and new path based on flag
4. Validate correctness with existing test suite (362 test classes)
5. Performance benchmark on large tables (10K+ splits)
6. Remove old path and flag once validated

---

## Change 2: Arrow FFI Write Path (Unified Schema)

### Current flow (every write commit)

```
JVM: has Seq[AddAction] (Scala case classes from writer)
  → ActionJsonSerializer.addActionsToJson() builds JSON manually via Jackson ObjectNode
    → Produces JSON string with all 25+ fields per AddAction
      → JNI passes JSON string to native
        → Native: serde_json::from_str() parses entire JSON back into Rust AddAction
          → Writes to cloud storage
```

**Problems:**
- Jackson ObjectNode construction is allocation-heavy (one ObjectNode per field)
- JSON string encoding/decoding is CPU-intensive for large batches
- `addActionToObjectNode()` manually maps all 25+ fields (260 lines of boilerplate)
- Native parses the same JSON it just received — pure waste

### Proposed flow

```
JVM: has Seq[Action] (AddActions, RemoveActions, etc.)
  → builds Arrow RecordBatch with unified action schema (action_type discriminator column)
    → exports via ArrowFfiBridge to pre-allocated FFI structs
      → JNI passes struct addresses to native
        → Native: import_arrow_batch() → arrow_batch_to_actions()
          → writes to cloud storage
```

**Key design decision:** One unified Arrow schema for ALL action types, not Arrow-for-adds + JSON-for-removes. Two serialization formats means two code paths to maintain on both sides, and you can never delete `ActionJsonSerializer`. Arrow handles sparse nulls via bitmaps at ~1 bit per null column — a remove row with 36 null add-specific columns costs 4.5 bytes, not worth a separate code path.

### tantivy4java changes required

**Feature Request 2:** `nativeWriteVersionArrowFfi()` — single JNI entry point that:
- Accepts a unified Arrow RecordBatch with `action_type` discriminator column
- Dispatches each row to the correct `Action` variant based on `action_type` value
- Calls existing `distributed::write_version()` with configurable retry
- Returns `WriteResult` (version, retries, conflicts)

Replaces `nativeWriteVersion`, `nativeAddFiles`, and `nativeWriteVersionOnce` — one function instead of three.

See feature request for full specification including the unified schema (42 columns covering all action types).

### JVM-side changes

#### New code (~120 lines)

**`ActionsToArrowConverter.scala`** (~120 lines):
```scala
object ActionsToArrowConverter {
  def toRecordBatch(actions: Seq[Action], allocator: BufferAllocator): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(UNIFIED_ACTION_SCHEMA, allocator)
    root.setRowCount(actions.size)
    actions.zipWithIndex.foreach { case (action, i) =>
      action match {
        case a: AddAction =>
          actionTypeVector.setSafe(i, "add".getBytes)
          pathVector.setSafe(i, a.path.getBytes)
          sizeVector.setSafe(i, a.size)
          // ... remaining add fields
        case r: RemoveAction =>
          actionTypeVector.setSafe(i, "remove".getBytes)
          pathVector.setSafe(i, r.path.getBytes)
          r.deletionTimestamp.foreach(deletionTimestampVector.setSafe(i, _))
          // ... remaining remove fields (most columns left null)
        case s: SkipAction => // ...
        case p: ProtocolAction => // ...
        case m: MetadataAction => // ...
      }
    }
    root
  }
}
```

#### Files removable after this change

| File | Lines | Reason |
|------|-------|--------|
| `ActionJsonSerializer.scala` | ~290 | Fully replaced — all serialization now via Arrow |

**Note:** `ActionJsonSerializer.parseActionFromJsonNode()` is used by `readVersion()` for reading raw version files. This single method (~15 lines) can be inlined into `NativeTransactionLog.parseActionsFromContent()`.

#### Migration strategy

1. Add Arrow-based write methods alongside existing JSON methods
2. Feature flag: `spark.indextables.transaction.arrowFfi.write.enabled` (default false)
3. Validate with write-heavy tests (optimized write, merge-on-write, companion sync)
4. Benchmark: measure time spent in serialization for 10K-split commits
5. Remove JSON path once validated

---

## Change 3: Arrow FFI for Retained Files Cursor (Purge)

### Current flow

```
Native: opens cursor over retained files
  → serializes to TANT binary (3 columns: path, size, version)
    → JVM: deserializes TANT → Map<String, String> per row
      → PurgeOrphanedSplitsExecutor processes rows
```

### Proposed flow

```
Native: opens cursor over retained files
  → exports batch as Arrow RecordBatch via FFI (full 25-column schema)
    → JVM: ArrowFfiBridge.importAsColumnarBatch()
      → PurgeOrphanedSplitsExecutor reads Arrow vectors directly
```

### tantivy4java changes required

**Feature Request 3:** `nativeReadNextRetainedFilesBatchArrowFfi()` — modify existing cursor to export via Arrow FFI instead of TANT binary.

See feature request for full specification.

### JVM-side changes

Modify `PurgeOrphanedSplitsExecutor` to consume `ColumnarBatch` instead of `Map<String, String>`. Minimal code change — the cursor open/close pattern stays the same.

---

## Dependency Graph

```
tantivy4java Feature Request 1 (listFiles Arrow FFI + all filtering + metadata)
  └── JVM Change 1 (read path) ← highest priority, biggest win
        └── Remove 8 JVM files + ~360 lines in scan/reader

tantivy4java Feature Request 2 (unified write Arrow FFI)
  └── JVM Change 2 (write path)
        └── Remove ActionJsonSerializer (fully — no JSON write path remains)

tantivy4java Feature Request 3 (retained files Arrow FFI)
  └── JVM Change 3 (purge path)
        └── Simplify PurgeOrphanedSplitsExecutor

tantivy4java Feature Request 4 (native redundant range filter elimination)
  └── JVM Change 1 (enables removing min/max from Arrow output)
        └── Remove isRangeFilterRedundantByStats from SplitReaderContext
```

FR 1-3 are independent — can be implemented in parallel.
FR 4 depends on FR 1 (min/max removed from Arrow output means JVM can't do the optimization).

---

## Expected Outcomes

| Metric | Before | After |
|--------|--------|-------|
| JVM objects per `listFiles()` call | 2 per file (TxLogFileEntry + AddAction) | 1 AddAction per *surviving* file only (post-filtering) |
| Serialization format (read) | TANT binary → Java fields → Scala fields | Arrow FFI (no JSON-in-Arrow) |
| Serialization format (write) | Scala fields → Jackson JSON → serde JSON | Arrow FFI → Rust struct (one unified schema, all action types) |
| JVM-side filtering code | ~1,800 lines (partition + data skipping + cooldown) | ~80-line filter converter |
| Write-path serialization code | ~290 lines (ActionJsonSerializer) | ~120 lines (unified Arrow builder) |
| Filtering location | JVM (after full materialization) | Native (before export — only surviving files cross JNI) |
| Executor-side filter optimization | JVM (`isRangeFilterRedundantByStats`, needs min/max from txlog) | Native split search engine (uses split footer stats) |
| JSON round-trips in Arrow columns | 5 per row (partition_values, min/max, stats, split_tags) | 0 (dynamic partition columns, native lists, stats not exported) |
| JNI round-trips per scan | 4+ (snapshot + N manifests + post-checkpoint + skip actions + getSchema + getPartitionColumns) | 1 (`nativeListFilesArrowFfi` returns files + metadata + metrics) |
| Data skipping metrics | JVM-side counters (DataSkippingMetrics.recordScan) | Native returns counts in ListFilesResult; JVM just records them |

---

## Files Impacted (JVM side)

### Modified
- `NativeTransactionLog.scala` — new Arrow-based list/write methods
- `PurgeOrphanedSplitsExecutor.scala` — consume Arrow batches
- `IndexTables4SparkScan.scala` — consume Arrow-based file listings (or extract to AddAction as bridge)

### New
- `SparkFilterToNativeFilter.scala` (~80 lines)
- `ActionsToArrowConverter.scala` (~120 lines)

### Removable (after validation)
- `PartitionPruning.scala`
- `PartitionIndex.scala`
- `PartitionFilterCache.scala`
- `FilterSelectivityEstimator.scala`
- `ExpressionSimplifier.scala`
- `FilterExpressionCache.scala`
- `ActionJsonSerializer.scala` (keep ~15 lines inlined for `CheckpointCommand`/`TruncateTimeTravelCommand` admin paths)
- `AddActionConverter.scala` (no longer needed — Arrow replaces TANT)
- ~300 lines data skipping in `IndexTables4SparkScan.scala`
- ~60 lines redundant filter logic in `SplitReaderContext.scala`

### Known limitations (acceptable for v1)
- **`StatisticsTruncation`** stays on JVM — called during writes before Arrow conversion. Small, stable code.
- **`ActionJsonSerializer`** partially retained — `CheckpointCommand` and `TruncateTimeTravelCommand` (low-frequency admin ops) still use JSON serialization. Can be migrated to Arrow write path later.
- **`initialize()` / `upgradeProtocol()`** still use JSON — rare operations (once per table create / version upgrade). Not worth optimizing.
- **`DocMappingMetadata` parsing** stays on JVM — per-schema (not per-file) with Guava caching. Future optimization target.

**Net: ~2,500+ lines removed, ~200 lines added.**
