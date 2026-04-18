# 03 — Write Path

This chapter covers the modules that turn a `df.write.save(path)` call into committed `.split` files. Everything here lives in `spark/core/` (write classes), `spark/arrow/` (Arrow FFI), and `spark/write/` (configuration).

## The write pipeline

### `core/IndexTables4SparkWriteBuilder.scala`
Entry point for writes. Implements `WriteBuilder`, `SupportsOverwrite`, and `SupportsTruncate`. Decides whether to use an optimized write or a standard write based on `spark.indextables.write.optimizeWrite.enabled`, then builds one of the two `Write` implementations below.

### `core/IndexTables4SparkStandardWrite.scala`
The baseline `Write`/`BatchWrite` implementation. Creates `IndexTables4SparkBatchWrite` to own the executor-side writing and handles driver-side commit. The `commit()` path:

1. Collects `CommitMessage`s from executors.
2. Builds `AddAction` entries.
3. Appends a new version to the transaction log via `transaction/NativeTransactionLog`.
4. Triggers post-commit hooks: `merge/AsyncMergeOnWriteManager` (merge-on-write) and `purge/PurgeOnWriteTransactionCounter` (purge-on-write).

It deliberately avoids broadcasting a `Configuration` to executors — all settings flow as a serializable map.

### `core/IndexTables4SparkOptimizedWrite.scala`
Extends `StandardWrite` and additionally implements `RequiresDistributionAndOrdering`. This is the Iceberg-style "shuffle before writing" path: it requests a `ClusteredDistribution` on the partition columns plus an advisory partition size (computed by `write/WriteSizeEstimator`) so Spark AQE coalesces input into well-sized splits. All commit logic is inherited from `StandardWrite`.

### `core/IndexTables4SparkBatchWrite.scala`
Implements `BatchWrite`. On the driver it builds the `DataWriterFactory`; on executors it produces one `IndexTables4SparkArrowDataWriter` per task. It is also where the parallelism and memory settings for Arrow FFI are wired up.

### `core/IndexTables4SparkArrowDataWriter.scala`
The executor-side `DataWriter[InternalRow]`. The hot path is:

```
InternalRow
   └─ ArrowFfiWriteBridge.append()          (spark/arrow)
        └─ fills VectorSchemaRoot           (Arrow Java)
             └─ exports via FFI structs     (C Data Interface)
                  └─ QuickwitSplit.addArrowBatch()   (Rust side, zero-copy)
```

It also computes per-field min/max/null statistics using `util/StatisticsCalculator`, applies `util/StatisticsTruncation` for long text fields, normalizes the protocol level with `util/ProtocolNormalizer`, and uploads the resulting `.split` file through the appropriate `io/CloudStorageProvider`. The `CommitMessage` it returns carries the uploaded file's metadata so the driver can emit an `AddAction`.

## Arrow FFI bridge (write direction)

### `arrow/ArrowFfiWriteBridge.scala`
Owns the Arrow `BufferAllocator` and `VectorSchemaRoot` used to marshal rows to Rust. It knows how to write every Spark type into the corresponding Arrow vector, including nested `Struct`/`List`/`Map` for JSON fields. When a batch is full it exports the Arrow buffers via C Data Interface structs and hands them to `QuickwitSplit.addArrowBatch()` without copying.

### `arrow/ArrowFfiBridge.scala`
The read-direction analogue (imports FFI data back into a `ColumnarBatch`). Written here because the same allocator class is shared between the read and write paths.

### `write/ArrowFfiWriteConfig.scala`
Batch size (rows) and native heap size (MB) for Arrow FFI writes. Default: 8192 rows per batch, 256 MB per writer task.

### `write/OptimizedWriteConfig.scala`
Config knobs for the optimized-write path: target split size (default 1 GB), sampling ratio, min rows for history-based estimation, and distribution mode.

### `write/WriteSizeEstimator.scala`
Computes the advisory partition byte size for Spark AQE. Two strategies:

- **Sampling mode** — estimate bytes-per-row from a sample of the input DataFrame.
- **History mode** — read prior `AddAction`s from the transaction log and take an average `size / numRecords`.

## Post-commit hooks (referenced here, detailed in chapter 07)

After `StandardWrite.commit()` succeeds, two async subsystems may fire:

- `merge/AsyncMergeOnWriteManager` — launches background merge jobs when the batch-size threshold is reached.
- `purge/PurgeOnWriteTransactionCounter` — tracks writes per table and triggers a purge of orphaned splits / old txlog versions according to `PurgeOnWriteConfig`.

Both are intentionally out-of-band: the write commit is never blocked on them.

## How the pieces connect

```
df.write.format("io.indextables.provider.IndexTablesProvider").save(path)
 → IndexTables4SparkTable.newWriteBuilder
    → IndexTables4SparkWriteBuilder
       ├─ IndexTables4SparkStandardWrite              (default)
       └─ IndexTables4SparkOptimizedWrite             (shuffles for split sizing)
           └─ WriteSizeEstimator + OptimizedWriteConfig
                ↓
        IndexTables4SparkBatchWrite
          └─ on executor:
              IndexTables4SparkArrowDataWriter
               └─ ArrowFfiWriteBridge
                    └─ QuickwitSplit.addArrowBatch()
               └─ CloudStorageProvider.upload(.split)
               └─ returns CommitMessage(AddAction)

  driver commit():
   └─ NativeTransactionLog.commit(AddActions)
   └─ AsyncMergeOnWriteManager.maybeTrigger()
   └─ PurgeOnWriteTransactionCounter.increment()
```

The next chapter describes the transaction log that both the read and write paths depend on.
