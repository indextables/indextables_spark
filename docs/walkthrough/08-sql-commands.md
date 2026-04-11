# 08 — SQL Commands

Registering `io.indextables.extensions.IndexTablesSparkExtensions` in `spark.sql.extensions` unlocks a family of custom SQL commands. Every command is in `spark/sql/` and extends one of Spark's `RunnableCommand` / `LeafRunnableCommand` classes. Both `INDEXTABLES` and `TANTIVY4SPARK` keywords are accepted; every command listed here supports both.

## Parser and extension plumbing

### `sql/IndexTables4SparkSessionExtension.scala`
Thin `SparkSessionExtensions` hook that installs only the custom SQL parser. Used by embedding contexts that want the SQL grammar without the Catalyst rules.

### `sql/IndexTables4SparkSqlParser.scala`
Subclass of Spark's parser that recognizes the custom grammar. Falls back to the default parser for standard SQL. Uses an ANTLR lexer/parser for the IndexTables grammar and delegates tree building to the AST builder.

### `sql/parser/IndexTables4SparkSqlAstBuilder.scala`
ANTLR parse-tree visitor. Converts each grammar rule into the corresponding command case class from `sql/`. This is where new grammar productions get wired up.

### `sql/TableRootUtils.scala`
Shared utility for the commands that need to resolve, validate, or mutate named table roots (used by multi-region companion indexing). Provides builders, validators, and credential scope helpers.

## Split and table maintenance

### `sql/MergeSplitsCommand.scala`
`MERGE SPLITS` — consolidates small splits into larger ones within each partition. Follows the Delta `OPTIMIZE` pattern: reads the current transaction log, groups eligible splits, schedules merge tasks (which run through `merge/WorkerLocalSplitMerger`), and commits a single new transaction containing both `Add` and `Remove` actions.

### `sql/PurgeOrphanedSplitsCommand.scala`
`PURGE INDEXTABLE` — removes split files that exist in storage but are no longer referenced by any version of the transaction log. Anti-joins the storage listing against the txlog. Applies a retention threshold so recently orphaned files (potentially still in-flight) are never touched.

### `sql/PurgeOrphanedSplitsExecutor.scala`
Executor-side logic for the purge command. Distributed file listing (to avoid driver OOM on huge tables), distributed anti-join between listing and referenced files, batched deletes with retry on transient cloud errors. Also defines `FileInfo`.

### `sql/DropPartitionsCommand.scala`
`DROP INDEXTABLES PARTITIONS` — adds `RemoveAction` entries for every file matching a partition predicate. Validates that the `WHERE` clause touches only partition columns (no data predicates allowed) so the drop is safe without opening splits.

### `sql/TruncateTimeTravelCommand.scala`
`TRUNCATE INDEXTABLES TIME TRAVEL` — deletes all historical transaction-log versions except the current one. Data files are left alone. Used to reclaim metadata space on tables with long commit histories.

### `sql/CheckpointCommand.scala`
`CHECKPOINT INDEXTABLES` / `COMPACT INDEXTABLES` — forces checkpoint creation on the transaction log. Delegates to the native checkpoint implementation.

### `sql/RepairIndexFilesTransactionLogCommand.scala`
`REPAIR INDEXFILES TRANSACTION LOG` — rebuilds a corrupted transaction log from the set of valid split files still in storage. Read-only with respect to data; applies `util/StatisticsTruncation` so the rebuilt log stays small.

## Cache and prewarm commands

### `sql/PrewarmCacheCommand.scala`
`PREWARM INDEXTABLES CACHE` — warms selected index components on selected splits across executors. Translates user-friendly component names through `prewarm/IndexComponentMapping` and hands the job to `prewarm/PreWarmManager` / `prewarm/AsyncPrewarmJobManager`.

### `sql/FlushIndexTablesCacheCommand.scala`
`FLUSH INDEXTABLES SEARCHER CACHE` — invalidates the native-side `SplitCacheManager`.

### `sql/FlushDiskCacheCommand.scala`
`FLUSH INDEXTABLES DISK CACHE` — deletes the on-disk L2 cache across executors and clears any split-to-host locality assignments.

### `sql/InvalidateTransactionLogCacheCommand.scala`
`INVALIDATE INDEXTABLES TRANSACTION LOG CACHE` — invalidates the process-level transaction log cache either globally or per table. Reports cache hit/miss rates before and after so users can see the effect.

## Describe / introspection commands

These commands never mutate state. Every one of them returns a DataFrame with a well-defined schema; see `docs/reference/sql-commands.md` for the column definitions.

### `sql/DescribeStateCommand.scala`
`DESCRIBE INDEXTABLES STATE` — transaction log format version, compression, checkpoint age, whether compaction is recommended.

### `sql/DescribeStorageStatsCommand.scala`
`DESCRIBE INDEXTABLES STORAGE STATS` — per-executor bytes/requests metrics collected from `SplitCacheManager` counters.

### `sql/DescribeDiskCacheCommand.scala`
`DESCRIBE INDEXTABLES DISK CACHE` — per-executor L2 cache statistics.

### `sql/DescribeEnvironmentCommand.scala`
`DESCRIBE INDEXTABLES ENVIRONMENT` — Spark and Hadoop config values from the driver and every worker. Runs credential redaction so values matching `key`, `secret`, `token`, `credential`, or `password` are masked.

### `sql/DescribeComponentSizesCommand.scala`
`DESCRIBE INDEXTABLES COMPONENT SIZES` — per-component (fastfield/term/postings/…) sizes for each split, computed by reading split footers.

### `sql/DescribeMergeJobsCommand.scala`
`DESCRIBE INDEXTABLES MERGE JOBS` — dumps the `AsyncMergeOnWriteManager` registry (active, completed, failed).

### `sql/DescribePrewarmJobsCommand.scala`
`DESCRIBE INDEXTABLES PREWARM JOBS` — same, for `AsyncPrewarmJobManager`.

### `sql/DescribeTransactionLogCommand.scala`
`DESCRIBE INDEXTABLES TRANSACTION LOG` — dumps the contents of the transaction log, optionally including historical versions.

### `sql/DescribeTransactionLogExecutor.scala`
Shared helper used by the describe-transaction-log command to iterate versions from the latest checkpoint forward.

### `sql/DescribeTableRootsCommand.scala`
`DESCRIBE INDEXTABLES TABLE ROOTS` — lists the registered named table roots (used in multi-region companion setups).

### `sql/SetTableRootCommand.scala`
`SET TABLE ROOT …` — adds or updates a named root. Uses the transaction log's metadata update API for concurrent safety.

### `sql/FfiProfilerCommands.scala`
`ENABLE / DISABLE / RESET / DESCRIBE INDEXTABLES PROFILER` — drives the FFI read-path profiler that lives inside `tantivy4java`. Useful for diagnosing which component of a read is hot.

## Wait commands

### `sql/WaitForPrewarmJobsCommand.scala`
(File lives alongside the prewarm command.) Blocks until every pending prewarm job either completes or times out. Lets scripts issue `PREWARM …` followed by `WAIT FOR INDEXTABLES PREWARM JOBS` to guarantee warm caches before the workload starts.

## Companion sync command

### `sql/SyncToExternalCommand.scala`
`BUILD INDEXTABLES COMPANION …` — builds a companion search index on top of an external Delta, Parquet, or Iceberg table. Supports full and incremental modes. Uses everything in `spark/sync/` (next chapter) plus the credential resolution machinery in chapter 10.

### `sql/StreamingCompanionManager.scala`
Manages a long-running streaming companion. Polls the source version and only fires a sync if something changed. Applies exponential backoff when idle.

### `sql/StreamingCompanionMetrics.scala`
Metric objects reported by the streaming companion to Spark UI and logs.

## How the pieces connect

```
SQL text
  └─ IndexTables4SparkSqlParser                  (parser/)
        └─ IndexTables4SparkSqlAstBuilder         (parser/)
             └─ RunnableCommand case class        (one per file above)
                 ├─ TransactionLog (chapter 04)
                 ├─ AsyncMergeOnWriteManager      (chapter 07)
                 ├─ PreWarmManager                (chapter 07)
                 ├─ PurgeOrphanedSplitsExecutor
                 └─ sync/*                        (chapter 09)
```

The next chapter covers the sync package, which powers `BUILD INDEXTABLES COMPANION`.
