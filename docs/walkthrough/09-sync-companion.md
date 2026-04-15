# 09 — Companion Sync

A *companion index* is an IndexTables4Spark table that mirrors an external Delta, Parquet, or Iceberg table purely for full-text search. The original data is not moved or copied; only the indexed columns are extracted, tokenized, and written as `.split` files keyed to the same file identity as the source. The `BUILD INDEXTABLES COMPANION` command (chapter 08) drives this subsystem. Everything described here lives in `spark/sync/`.

## Reader abstraction

### `sync/CompanionSourceReader.scala`
The trait every companion source implements. Exposes the source version (used to detect changes), the list of source files, the partition schema, and the data schema. Also defines `CompanionSourceFile`, the per-file metadata value used downstream.

### `sync/DeltaSourceReader.scala`
`CompanionSourceReader` implementation for Delta Lake. Delegates the heavy lifting to `DeltaLogReader`.

### `sync/DeltaLogReader.scala`
Wraps `tantivy4java`'s `delta-kernel-rs` binding — a Rust-based Delta reader embedded in the same native library. Translates `spark.indextables.*` credential keys into the flat key/value pairs `delta-kernel-rs` expects. Reading Delta through this path is much faster than going through the JVM Delta client.

### `sync/IcebergSourceReader.scala`
Iceberg implementation. Uses the Iceberg Java API to enumerate the latest snapshot's files, handles both Spark-style and Iceberg-style schema JSON formats, and translates credentials. Also supports incremental snapshots so repeated syncs only index what changed.

### `sync/ParquetDirectoryReader.scala`
Reader for bare Parquet directories (no metadata layer). Lists files, reads the Parquet footer of a representative file for schema inference, and translates credentials the same way the Delta/Iceberg readers do.

## Distributed file handling

Source tables can be huge. All sync operations that scale with file count run as distributed Spark jobs to avoid driver OOM.

### `sync/DistributedSourceScanner.scala`
Runs file listing as a Spark RDD transformation. Returns a `DistributedScanResult` containing an `RDD[CompanionSourceFile]` plus scan metadata (source version, schema, partition columns, whether the result is incremental, list of removed files). Works identically for Delta, Parquet, and Iceberg.

### `sync/DistributedAntiJoin.scala`
The change-detection heart of the sync. Given the set of source files and the set of already-indexed files from the companion's own transaction log, it computes:

- **new** — files in source, not in companion → need to be indexed.
- **gone** — files in companion, not in source → need `RemoveAction`s.
- **unchanged** — files in both → no work.

The anti-join runs as a Spark shuffle so it scales to tens of millions of files.

### `sync/SparkPredicateToPartitionFilter.scala`
Translates Spark SQL `WHERE` predicates into the `tantivy4java` `PartitionFilter` JSON. Lets the sync command filter source files natively (inside Rust) rather than shipping the full file listing to the JVM.

## Execution

### `sync/SyncTaskExecutor.scala`
Executor-side task: takes a `SyncIndexingGroup` (a batch of Parquet files to turn into one `.split`), reads the Parquet data, indexes it, and returns a `SyncResult` containing the resulting `AddAction` and metrics. `SyncConfig` carries all driver-decided settings.

### `sync/SyncMetrics.scala`
`SyncMetricsAccumulator` and related metric objects. Tracks bytes read from Parquet, splits written, file counts, and timings. Aggregates via Spark accumulators so the Spark UI shows progress during a long sync.

## How the pieces connect

```
BUILD INDEXTABLES COMPANION FOR '<delta | iceberg | parquet source>'
  └─ sql/SyncToExternalCommand                       (chapter 08)
       ├─ select CompanionSourceReader:
       │    DeltaSourceReader   → DeltaLogReader   → delta-kernel-rs (Rust)
       │    IcebergSourceReader → Iceberg Java API
       │    ParquetDirectoryReader → Parquet footer read
       │
       ├─ DistributedSourceScanner
       │     └─ DistributedScanResult(RDD[CompanionSourceFile], metadata)
       │
       ├─ DistributedAntiJoin(source files, companion txlog)
       │     └─ (new, gone, unchanged)
       │
       ├─ SparkPredicateToPartitionFilter
       │     └─ (native-side pruning)
       │
       └─ SyncTaskExecutor (per SyncIndexingGroup, on executors)
             ├─ reads Parquet
             ├─ indexes via search/IndexTablesDirectInterface
             │     (chapter 05)
             ├─ uploads via io/CloudStorageProvider      (chapter 06)
             └─ returns SyncResult(AddAction, SyncMetrics)
        ↓
   NativeTransactionLog.commit(Adds + Removes)         (chapter 04)
```

For streaming companions, `sql/StreamingCompanionManager` (chapter 08) wraps this whole pipeline in a polling loop that skips work when the source version has not changed.

The final chapter covers the cross-cutting infrastructure — config, auth, metrics, and the general-purpose utilities under `spark/util/`.
