# 07 — Merge and Purge

Two post-commit maintenance subsystems share a common shape: a driver-side singleton plus a configuration object plus executor-side workers. They are intentionally out-of-band — the write commit path in chapter 03 is never blocked on them. Plus one small but important throttle that keeps merges from trampling each other.

This chapter covers `spark/merge/`, `spark/purge/`, and `spark/storage/SplitManager.scala`. (Cache prewarming and `DriverSplitLocalityManager` moved to chapter 02, since they are read-path concerns.)

## Merge-on-write

### `merge/AsyncMergeOnWriteConfig.scala`
All knobs for async merge-on-write: batch CPU fraction (fraction of total cluster CPUs that count as one batch), max concurrent merge batches, merge group sizing (how many source splits per merge), target merged split size, graceful shutdown timeout, and a handful of safety thresholds. The batch-size threshold that triggers a merge is `batchSize × minBatchesToTrigger`, where `batchSize = max(1, totalClusterCpus × batchCpuFraction)`.

### `merge/AsyncMergeOnWriteManager.scala`
The JVM-wide singleton that owns async merge jobs on the driver. Responsibilities:

- A `Semaphore` limits concurrent merges.
- A job registry tracks active, completed, and failed merges per table (so `DESCRIBE INDEXTABLES MERGE JOBS` has something to report).
- Deduplicates merges — if one is already running for a table, a second `maybeTrigger()` call is a no-op.
- Supports graceful shutdown so a Spark session teardown waits for in-flight merges to finish (up to the configured timeout).

Post-commit, `IndexTables4SparkStandardWrite` calls `maybeTrigger()`; the manager checks the threshold and either launches a job or does nothing. This is the entire API surface the rest of the connector sees.

### `merge/WorkerLocalSplitMerger.scala`
The executor-side worker that actually performs a merge. Runs inside the shuffle-based merge task that the `MERGE SPLITS` SQL command (or the async manager) schedules. Responsibilities:

- Downloads source splits via `io/merge/CloudDownloadManager`.
- Calls `QuickwitSplit.convert*` to merge them into a new local split.
- Uploads the merged split via `io/merge/MergeUploader`.
- Applies gap mitigations: per-worker memory cap, merge timeout, disk-space precheck.
- Returns a `MergedSplitMetadata` value the driver uses to build the commit message.

## Purge-on-write

### `purge/PurgeOnWriteConfig.scala`
Configuration: enable/disable, trigger mode (after every merge, or after N writes), retention hours for split files, retention hours for transaction-log versions, and a hard minimum retention of 24 hours to protect against accidental data loss.

### `purge/PurgeOnWriteTransactionCounter.scala`
Per-session counter. A `ConcurrentHashMap<tablePath, AtomicInteger>` tracks writes per table; when the count crosses the configured threshold the counter fires a purge and resets. Thread-safe and lock-free on the hot path. The purge itself is handled by `sql/PurgeOrphanedSplitsCommand` and `sql/PurgeOrphanedSplitsExecutor` (chapter 08).

## Split conversion throttle

### `storage/SplitManager.scala`
Despite the filename, this file exports `SplitConversionThrottle`: a JVM-wide `Semaphore` limiting concurrent `QuickwitSplit.convertIndexFromPath` operations. Split conversion is CPU-heavy and disk-heavy, and running too many in parallel causes thrashing. Both `WorkerLocalSplitMerger` and the write path acquire from this throttle before kicking off a conversion, so it is the one choke point that keeps the box healthy when many tasks are writing or merging at once.

## How the pieces connect

```
Write commit (chapter 03)
      │
      ├─ AsyncMergeOnWriteManager.maybeTrigger()
      │     └─ (if threshold crossed) launch merge job
      │         └─ WorkerLocalSplitMerger on executors
      │             ├─ io/merge/CloudDownloadManager  (chapter 06)
      │             ├─ storage/SplitConversionThrottle
      │             └─ io/merge/MergeUploader
      │         └─ NativeTransactionLog.commit(Add+Remove)
      │
      └─ PurgeOnWriteTransactionCounter.increment()
            └─ (if threshold crossed) PurgeOrphanedSplitsCommand (chapter 08)
```

Both subsystems are designed so that failure is tolerable: a missed merge or a deferred purge just degrades performance, never correctness.

The next chapter covers the SQL commands that drive these subsystems (and the read-path prewarm subsystem in chapter 02) from user code.
