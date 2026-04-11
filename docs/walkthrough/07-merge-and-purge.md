# 07 — Merge, Purge, Prewarm

Three post-commit maintenance subsystems share a common shape: a driver-side singleton plus a configuration object plus some executor-side workers. They are all opt-in or conservative by default so they never block the write commit path.

This chapter covers `spark/merge/`, `spark/purge/`, `spark/prewarm/`, and `spark/storage/`.

## Merge-on-write

### `merge/AsyncMergeOnWriteConfig.scala`
All knobs for async merge-on-write: batch CPU fraction (fraction of total cluster CPUs that count as one batch), max concurrent merge batches, merge group sizing (how many source splits per merge), target merged split size, graceful shutdown timeout, and a handful of safety thresholds. The batch-size threshold that triggers a merge is `batchSize × minBatchesToTrigger`, where `batchSize = max(1, totalClusterCpus × batchCpuFraction)`.

### `merge/AsyncMergeOnWriteManager.scala`
The JVM-wide singleton that owns async merge jobs on the driver. Responsibilities:

- A `Semaphore` limits concurrent merges.
- A job registry tracks active, completed, and failed merges per table (so `DESCRIBE INDEXTABLES MERGE JOBS` has something to report).
- Deduplicates merges — if one is already running for a table, a second `maybeTrigger()` call is a no-op.
- Supports graceful shutdown so a Spark session teardown waits for in-flight merges to finish (up to the configured timeout).

Post-commit, `IndexTables4SparkStandardWrite` calls `maybeTrigger()`; the manager checks the threshold and either launches a job or does nothing. This is the entire API surface the rest of the connector sees. Internally the manager follows the same singleton pattern as `AsyncPrewarmJobManager`.

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

## Prewarm

### `prewarm/PreWarmManager.scala`
Two-phase cache warmup orchestration:

1. **Pre-scan** — distribute warmup tasks to executors that already hold the relevant splits in cache, using `storage/DriverSplitLocalityManager` to target the right host.
2. **Post-warm** — when the main query runs, join on the warmup futures so no split is accessed cold.

Returns `PreWarmStats` with cache-hit/miss counts for observability. Triggered by `PREWARM INDEXTABLES CACHE` and also implicitly before large read queries.

### `prewarm/AsyncPrewarmJobManager.scala`
Executor-side singleton analogous to `AsyncMergeOnWriteManager`: a semaphore-bounded registry of async prewarm jobs with auto-cleanup of completed jobs. This is what `DESCRIBE INDEXTABLES PREWARM JOBS` queries.

### `prewarm/IndexComponentMapping.scala`
Maps the user-friendly names used in SQL (`TERM`, `FASTFIELD`, `POSTINGS`, `POSITIONS`, `FIELDNORM`, `STORE`) to the `tantivy4java` `IndexComponent` enum. Also defines the default set of components that are warmed when the user does not specify one explicitly.

## Split locality and throttling

### `storage/DriverSplitLocalityManager.scala`
Driver-side sticky split-to-host assignment. Replaces an older broadcast-based approach. Given a list of splits and a set of executors, it returns stable assignments so the same split is reliably read by the same host across queries (improving cache hit rates). It also tracks prewarm state so `PreWarmManager` knows which splits are already warm on which hosts.

### `storage/SplitManager.scala`
Despite the filename, this file exports `SplitConversionThrottle`: a JVM-wide `Semaphore` limiting concurrent `QuickwitSplit.convertIndexFromPath` operations. Split conversion is CPU and disk heavy, and running too many in parallel causes thrashing — this throttle is the safety valve.

## How the pieces connect

```
Write commit (chapter 03)
      │
      ├─ AsyncMergeOnWriteManager.maybeTrigger()
      │     └─ (if threshold) launch merge job
      │         └─ WorkerLocalSplitMerger on executors
      │             ├─ io/merge/CloudDownloadManager  (chapter 06)
      │             ├─ storage/SplitConversionThrottle (protect CPU/disk)
      │             └─ io/merge/MergeUploader
      │         └─ NativeTransactionLog.commit(Add+Remove)
      │
      └─ PurgeOnWriteTransactionCounter.increment()
            └─ (if threshold) PurgeOrphanedSplitsCommand (chapter 08)

SQL PREWARM INDEXTABLES CACHE:
  PreWarmManager
    ├─ DriverSplitLocalityManager.assign(splits)
    └─ AsyncPrewarmJobManager on executors
         └─ (warms components from IndexComponentMapping)
```

All three subsystems are designed so that failure is tolerable: a missed merge, a deferred purge, or a cold prewarm just degrades performance, never correctness.

The next chapter covers the SQL commands that drive these subsystems from user code.
