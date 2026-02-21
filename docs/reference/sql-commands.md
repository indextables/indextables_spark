# SQL Commands Reference

All SQL commands require the extensions to be registered:

```scala
// In SparkSession builder:
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "io.indextables.extensions.IndexTablesSparkExtensions")
  .getOrCreate()
```

All commands support both `INDEXTABLES` and `TANTIVY4SPARK` keywords interchangeably.

---

## Merge Splits

Consolidate small splits into larger ones for better read performance.

```sql
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M;
MERGE SPLITS 's3://bucket/path' MAX DEST SPLITS 10;
MERGE SPLITS 's3://bucket/path' MAX SOURCE SPLITS PER MERGE 500;
MERGE SPLITS 's3://bucket/path' WHERE date = '2024-01-01' TARGET SIZE 100M;
MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G MAX DEST SPLITS 5 MAX SOURCE SPLITS PER MERGE 100;
```

**Configuration**:
- `TARGET SIZE`: Maximum size of merged splits (default: 5GB)
- `MAX DEST SPLITS`: Limits the number of destination (merged) splits to process (oldest first)
- `MAX SOURCE SPLITS PER MERGE`: Maximum source splits per merge (default: 1000)
  - Config: `spark.indextables.merge.maxSourceSplitsPerMerge: 1000`
- **Skip Split Threshold**: Splits at or above this size are excluded from merge
  - Config: `spark.indextables.merge.skipSplitThreshold: 0.45` (default: 45% of target size)
  - Range: 0.0 to 1.0 (percentage of target size)
  - Example: With 5GB target and 0.45 threshold, splits >= 2.25GB are skipped

---

## Purge IndexTable

Remove orphaned split files and old transaction logs.

```sql
PURGE INDEXTABLE 's3://bucket/path' DRY RUN;
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 7 DAYS;
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 168 HOURS;
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 14 DAYS DRY RUN;
PURGE INDEXTABLE 's3://bucket/path' OLDER THAN 7 DAYS TRANSACTION LOG RETENTION 30 DAYS;
```

**Configuration**:
```scala
spark.indextables.purge.defaultRetentionHours: 168 (7 days for splits)
spark.indextables.purge.minRetentionHours: 24 (safety check)
spark.indextables.purge.retentionCheckEnabled: true
spark.indextables.purge.parallelism: <auto>
spark.indextables.purge.maxFilesToDelete: 1000000
spark.indextables.purge.deleteRetries: 3
```

**Common scenarios:**
- After failed writes that leave orphaned split files
- After MERGE SPLITS operations
- Regular maintenance (weekly/monthly cleanup)
- Before archiving or migrating tables
- Transaction log cleanup to reclaim storage

**Safety features:**
- Minimum retention period enforced (default 24 hours)
- DRY RUN mode shows preview before deletion
- Distributed deletion across executors for scalability
- Retry logic handles transient cloud storage errors
- LEFT ANTI JOIN ensures only truly orphaned files deleted
- Transaction log files referenced by checkpoints are never deleted

---

## Drop Partitions

Logically remove partitions from a table.

```sql
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year = '2023';
DROP INDEXTABLES PARTITIONS FROM my_table WHERE date = '2024-01-01';

-- Range predicates
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year > '2020';
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE month < 6;
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE month BETWEEN 1 AND 6;

-- Compound predicates (AND, OR)
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE region = 'us-east' AND year > '2020';
DROP INDEXTABLES PARTITIONS FROM 's3://bucket/path' WHERE year = '2022' OR year = '2023';
```

**Key features:**
- **WHERE clause required** - Prevents accidental full table drops
- **Partition columns only** - Validates that WHERE clause references only partition columns
- **Logical deletion** - Adds RemoveAction entries to transaction log without physical deletion
- **Physical cleanup via PURGE** - Use PURGE INDEXTABLE to delete files after retention period
- **Returns status** - Reports partitions dropped, splits removed, and total size

**Example workflow:**
```scala
// 1. Drop old partitions
spark.sql("DROP INDEXTABLES PARTITIONS FROM 's3://bucket/table' WHERE year < '2022'").show()

// 2. Verify with DESCRIBE (optional)
spark.sql("DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/table' INCLUDE ALL").show()

// 3. After retention period, clean up physical files
spark.sql("PURGE INDEXTABLE 's3://bucket/table' OLDER THAN 7 DAYS").show()
```

**Error handling:**
- Fails if WHERE clause references non-partition columns
- Fails if table has no partition columns defined
- Returns no_action if no partitions match the predicates

---

## Checkpoint / Compact IndexTable

Force a checkpoint (creates V4 checkpoint with Avro state format).

```sql
CHECKPOINT INDEXTABLES 's3://bucket/path';
CHECKPOINT INDEXTABLES my_table;

-- COMPACT is an alias for CHECKPOINT
COMPACT INDEXTABLES 's3://bucket/path';
```

**Key features:**
- **Full compaction**: Reads all current files and writes a fresh Avro state with no tombstones
- **Protocol upgrade**: Creates V4 checkpoint with Avro state format (10x faster reads)
- **Schema deduplication**: Uses hash-based schema references to reduce state size
- **Streaming write**: Uses streaming to avoid OOM for large tables
- **Removes tombstones**: Clears accumulated tombstones by writing only live file entries

**When to use:**
- **Force compaction**: Use `COMPACT` when tombstone ratio is high (check with `DESCRIBE INDEXTABLES STATE`)
- **Force V4 upgrade**: Upgrade existing V2/V3 tables to V4 protocol with Avro state
- **After bulk deletes**: Compact after DROP PARTITIONS or large MERGE operations
- **Performance optimization**: Compaction speeds up table reads by consolidating state
- **Before maintenance**: Create checkpoint before PURGE operations

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Resolved table path |
| `status` | String | "SUCCESS" or "ERROR: <message>" |
| `checkpoint_version` | Long | Transaction version at checkpoint |
| `num_actions` | Long | Total actions in checkpoint |
| `num_files` | Long | Number of split files |
| `protocol_version` | Long | Protocol version (4 for V4 with Avro state) |
| `is_multi_part` | Boolean | True if checkpoint split into parts |

---

## Truncate Time Travel

Remove all historical transaction log versions, keeping only current state.

```sql
TRUNCATE INDEXTABLES TIME TRAVEL 's3://bucket/path';
TRUNCATE INDEXTABLES TIME TRAVEL 's3://bucket/path' DRY RUN;
```

**Key features:**
- Creates checkpoint first if none exists at current version
- Deletes all transaction log version files older than the checkpoint
- Deletes all checkpoint files except the latest
- Preserves data files (only transaction log metadata affected)
- DRY RUN mode for preview

**When to use:**
- Reduce storage overhead after many small write operations
- Clean up history when time travel is no longer needed
- Prepare for archival
- After MERGE operations

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Resolved table path |
| `status` | String | "SUCCESS", "DRY_RUN", or "ERROR" |
| `checkpoint_version` | Long | Version at which checkpoint exists |
| `versions_deleted` | Long | Number of version files deleted |
| `checkpoints_deleted` | Long | Number of checkpoint files deleted |
| `files_preserved` | Long | Number of data files (splits) in table |
| `message` | String | Descriptive status message |

**Safety notes:**
- After truncation, time travel to earlier versions is no longer possible
- Always use DRY RUN first to preview what will be deleted
- This operation is irreversible

---

## Prewarm Cache

Pre-warm index caches across all executors for optimal query performance.

```sql
-- Basic prewarm (preloads default segments: TERM, POSTINGS)
PREWARM INDEXTABLES CACHE 's3://bucket/path';

-- Prewarm specific segments
PREWARM INDEXTABLES CACHE 's3://bucket/path' FOR SEGMENTS (TERM_DICT, FAST_FIELD);

-- Prewarm with field selection
PREWARM INDEXTABLES CACHE 's3://bucket/path' ON FIELDS (title, content, timestamp);

-- Prewarm with partition filter
PREWARM INDEXTABLES CACHE 's3://bucket/path' WHERE date >= '2024-01-01';

-- Prewarm with custom parallelism
PREWARM INDEXTABLES CACHE 's3://bucket/path' WITH PERWORKER PARALLELISM OF 5;

-- Full syntax with all options
PREWARM INDEXTABLES CACHE 's3://bucket/path'
  FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM)
  ON FIELDS (title, content)
  WITH PERWORKER PARALLELISM OF 4
  WHERE region = 'us-east';
```

**Segment Aliases:**

| SQL Name | Description |
|----------|-------------|
| TERM_DICT, TERM | Term dictionary (FST) |
| FAST_FIELD, FASTFIELD | Fast fields for aggregations |
| POSTINGS, POSTING_LISTS | Inverted index postings |
| POSITIONS, POSITION_LISTS | Term positions within documents |
| FIELD_NORM, FIELDNORM | Field norms for scoring |
| DOC_STORE, STORE | Document storage |

**Default segments**: TERM_DICT, POSTINGS (minimal set for query operations)

**Read-time prewarm configuration:**
```scala
spark.indextables.prewarm.enabled: false (default)
spark.indextables.prewarm.segments: "TERM_DICT,POSTINGS"
spark.indextables.prewarm.fields: "" (empty = all fields)
spark.indextables.prewarm.splitsPerTask: 2 (default)
spark.indextables.prewarm.partitionFilter: "" (default: empty = all partitions)
spark.indextables.prewarm.failOnMissingField: true (default)
spark.indextables.prewarm.catchUpNewHosts: false (default)
```

**Output schema:**
- `host`: Executor hostname
- `splits_prewarmed`: Count of splits prewarmed
- `segments`: Comma-separated segment names
- `fields`: Comma-separated field names (or "all")
- `duration_ms`: Prewarm duration in milliseconds
- `status`: "success", "partial", or "no_splits"
- `skipped_fields`: Missing fields (if any)

---

## Describe State

View checkpoint/state format information for a table.

```sql
DESCRIBE INDEXTABLES STATE 's3://bucket/path';
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `format` | String | "avro-state", "json", "json-multipart", or "none" |
| `version` | Long | Transaction version at checkpoint |
| `num_files` | Long | Number of split files |
| `num_manifests` | Long | Number of Avro manifest files (0 for JSON) |
| `num_tombstones` | Long | Number of removed file entries |
| `tombstone_ratio` | Double | Ratio of tombstones to total entries |
| `needs_compaction` | Boolean | True if tombstone ratio exceeds threshold |

**Migration from JSON to Avro:**
```sql
DESCRIBE INDEXTABLES STATE 's3://bucket/path';    -- Check current format
CHECKPOINT INDEXTABLES 's3://bucket/path';         -- Upgrade to Avro
DESCRIBE INDEXTABLES STATE 's3://bucket/path';    -- Verify (should show "avro-state")
```

---

## Describe Disk Cache

View disk cache statistics across all executors.

```sql
DESCRIBE INDEXTABLES DISK CACHE;
```

Output includes: `executor_id`, `host`, `enabled`, `total_bytes`, `max_bytes`, `usage_percent`, `splits_cached`, `components_cached`.

- Auto-enabled on Databricks/EMR when `/local_disk0` detected
- Each executor maintains independent cache
- Returns NULL when cache is disabled

---

## Describe Storage Stats

View object storage (S3/Azure) access statistics across all executors.

```sql
DESCRIBE INDEXTABLES STORAGE STATS;
```

Output includes: `executor_id`, `host`, `bytes_fetched`, `requests`. Counters are cumulative since JVM startup.

---

## Describe Data Skipping Stats

> **Note**: These commands are not yet wired into the SQL grammar. The command implementations exist (`DescribeDataSkippingStatsCommand`, `FlushDataSkippingStatsCommand`, `InvalidateDataSkippingCacheCommand`) but require ANTLR grammar rules to be added before they can be invoked via SQL.

```sql
DESCRIBE INDEXTABLES DATA SKIPPING STATS;
FLUSH INDEXTABLES DATA SKIPPING STATS;        -- Reset stats, keep cache
INVALIDATE INDEXTABLES DATA SKIPPING CACHE;   -- Clear caches and stats
```

Metrics include: file pruning rates, filter type effectiveness, expression cache hit rates, partition filter cache performance.

**Spark UI Integration:** Data skipping metrics are also reported to the Spark UI SQL tab under the scan operator, showing `total files considered`, `files pruned by partitions`, `files skipped by stats`, `files to scan`, and `total skip rate`.

---

## Describe Environment

View Spark and Hadoop configuration properties across all executors.

```sql
DESCRIBE INDEXTABLES ENVIRONMENT;
```

Output includes: `host`, `role` (driver/worker), `property_type` (spark/hadoop), `property_name`, `property_value`.

- Sensitive values are automatically redacted (`***REDACTED***`)
- Results can be filtered via temp view

```scala
spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT").createOrReplaceTempView("env_props")
spark.sql("SELECT * FROM env_props WHERE property_name LIKE 'spark.indextables%'").show()
```

---

## Describe Merge Jobs

View async merge-on-write job status on the driver.

```sql
DESCRIBE INDEXTABLES MERGE JOBS;
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | String | Unique job identifier |
| `table_path` | String | Path of the table being merged |
| `status` | String | RUNNING, COMPLETED, FAILED, CANCELLED |
| `total_groups` | Integer | Total merge groups to process |
| `completed_groups` | Integer | Groups merged so far |
| `total_batches` | Integer | Total number of batches |
| `completed_batches` | Integer | Batches completed |
| `progress_pct` | Double | Percentage progress |
| `duration_ms` | Long | Duration in milliseconds |
| `error_message` | String | Error message if failed |

```scala
// Wait for merges to complete
spark.sql("DESCRIBE INDEXTABLES MERGE JOBS").createOrReplaceTempView("merge_jobs")
while (spark.sql("SELECT COUNT(*) FROM merge_jobs WHERE status = 'RUNNING'").head().getLong(0) > 0) {
  Thread.sleep(5000)
}
```

---

## Flush Disk Cache

Flush disk cache across all executors (clears cached data and locality state).

```sql
FLUSH INDEXTABLES DISK CACHE;
```

- Flushes split cache managers (closes disk cache handles)
- Clears split locality assignments and prewarm state tracking
- Deletes all disk cache files at the configured path
- Useful for testing, maintenance, and troubleshooting

---

## Flush Searcher Cache

Flush in-memory searcher caches on the driver (split cache, locality manager, tantivy4java native caches).

```sql
FLUSH INDEXTABLES SEARCHER CACHE;
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `cache_type` | String | Cache component: "split_cache", "locality_manager", "tantivy_java_cache", or "error" |
| `status` | String | "success" or "failed" |
| `cleared_entries` | Long | Number of entries/managers flushed |
| `message` | String | Descriptive message |

- Flushes GlobalSplitCacheManager instances (closes split cache handles)
- Clears driver-side split locality assignments
- Flushes tantivy4java native caches
- Returns one row per operation with success/failure status

---

## Invalidate Transaction Log Cache

Invalidate transaction log caches to force fresh reads from storage.

```sql
-- Global invalidation (all caches)
INVALIDATE INDEXTABLES TRANSACTION LOG CACHE;

-- For a specific table
INVALIDATE INDEXTABLES TRANSACTION LOG CACHE FOR 's3://bucket/path';
INVALIDATE INDEXTABLES TRANSACTION LOG CACHE FOR my_table;
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Table path or "GLOBAL" for global invalidation |
| `result` | String | Descriptive result message |
| `cache_hits_before` | Long | Total cache hits before invalidation |
| `cache_misses_before` | Long | Total cache misses before invalidation |
| `hit_rate_before` | String | Cache hit rate percentage (e.g., "85.3%") or "N/A" |

- **Global** (no FOR clause): Clears all caches in EnhancedTransactionLogCache (actions, checkpoint, Avro manifest, filtered schema caches)
- **Table-specific** (with FOR): Reads cache stats, then invalidates cache for that table
- Returns hit/miss statistics collected before invalidation

---

## Describe Transaction Log

View all transaction log actions for a table.

```sql
-- Current state (from latest checkpoint forward)
DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/path';
DESCRIBE INDEXTABLES TRANSACTION LOG my_catalog.my_database.my_table;

-- Complete history from version 0
DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/path' INCLUDE ALL;
```

**Output schema (key columns -- 47 columns total):**

| Column | Type | Description |
|--------|------|-------------|
| `version` | Long | Transaction version number |
| `log_file_path` | String | Path to the transaction log file |
| `action_type` | String | "add", "remove", "skip", "protocol", "metadata", "unknown" |
| `path` | String | File path (for add/remove/skip actions) |
| `partition_values` | String | JSON map of partition values |
| `size` | Long | File size in bytes |
| `data_change` | Boolean | Whether action represents a data change |
| `num_records` | Long | Number of records (add actions) |
| `deletion_timestamp` | Timestamp | When file was deleted (remove actions) |
| `is_checkpoint` | Boolean | True if action came from a checkpoint file |

Additional columns include: `tags`, `modification_time`, `stats`, `min_values`, `max_values`, `footer_start_offset`, `footer_end_offset`, `hotcache_start_offset`, `hotcache_length`, `has_footer_offsets`, `time_range_start`, `time_range_end`, `split_tags`, `delete_opstamp`, `num_merge_ops`, `doc_mapping_json`, `doc_mapping_ref`, `uncompressed_size_bytes`, `extended_file_metadata`, `skip_timestamp`, `skip_reason`, `skip_operation`, `skip_retry_after`, `skip_count`, `protocol_min_reader_version`, `protocol_min_writer_version`, `protocol_reader_features`, `protocol_writer_features`, `metadata_id`, `metadata_name`, `metadata_description`, `metadata_format_provider`, `metadata_format_options`, `metadata_schema_string`, `metadata_partition_columns`, `metadata_configuration`, `metadata_created_time`.

- Without `INCLUDE ALL`: Reads from latest checkpoint forward (more efficient for large tables)
- With `INCLUDE ALL`: Reads complete history from version 0
- Shows all action types: AddAction (files added), RemoveAction (files removed), SkipAction (files skipped), ProtocolAction (protocol versions), MetadataAction (schema/config)
- Queryable via temp views for analysis

```scala
spark.sql("DESCRIBE INDEXTABLES TRANSACTION LOG '/path'").createOrReplaceTempView("txlog")
spark.sql("SELECT version, action_type, COUNT(*) FROM txlog GROUP BY version, action_type").show()
```

---

## Repair IndexFiles Transaction Log

Repair corrupted or problematic transaction logs by validating split files exist and writing a clean log.

```sql
REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log'
  AT LOCATION 's3://bucket/table/_transaction_log_repaired';
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `source_path` | String | Source transaction log path |
| `target_path` | String | Target (repaired) transaction log path |
| `source_version` | Long | Version of the source transaction log (-1 on error) |
| `total_splits` | Integer | Total splits in source |
| `valid_splits` | Integer | Splits validated as existing in storage |
| `missing_splits` | Integer | Splits not found in storage (excluded from repair) |
| `status` | String | "SUCCESS" or "ERROR: message" |

- **Read-only repair**: Never modifies or deletes source files
- Validates all referenced split files actually exist in storage
- Writes clean transaction log to a NEW location (target must not exist or be empty)
- Applies statistics truncation automatically
- Uses streaming write to avoid OOM for large tables
- Source path must end with `_transaction_log`

**Common scenarios:**
- Corrupted checkpoint recovery
- Orphaned file cleanup (create log excluding missing splits)
- Transaction log optimization (consolidate fragmented history)
- Migration preparation

**Post-repair workflow:**
```scala
// 1. Run repair
spark.sql("""REPAIR INDEXFILES TRANSACTION LOG '/path/table/_transaction_log'
  AT LOCATION '/path/repaired/_transaction_log'""").show()

// 2. Backup original and swap
val fs = new Path(tablePath).getFileSystem(hadoopConf)
fs.rename(originalLogPath, backupPath)
fs.rename(repairedLogPath, originalLogPath)

// 3. Table is now readable with the repaired log
spark.read.format("io.indextables.provider.IndexTablesProvider").load(tablePath)
```

---

## Describe Component Sizes

Inspect per-field sub-component sizes across all splits for storage analysis.

```sql
DESCRIBE INDEXTABLES COMPONENT SIZES 's3://bucket/path';
DESCRIBE INDEXTABLES COMPONENT SIZES my_table;

-- With partition filter
DESCRIBE INDEXTABLES COMPONENT SIZES '/path' WHERE year = '2024';
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `split_path` | String | Path to the split file |
| `partition_values` | String | JSON map of partition key/values (null if unpartitioned) |
| `component_key` | String | Component identifier (e.g., `score.fastfield`, `_term_total`) |
| `size_bytes` | Long | Size in bytes |
| `component_type` | String | Category: `fastfield`, `fieldnorm`, `term`, `postings`, `positions`, `store` |
| `field_name` | String | Field name (null for segment-level components like `_term_total`) |

- Component key formats: per-field (`{field}.fastfield`, `{field}.fieldnorm`) and segment-level (`_term_total`, `_postings_total`, `_positions_total`, `_store`)
- Supports WHERE clause for partition filtering
- Executes in parallel across executors
- Queryable via temp views:

```scala
spark.sql("DESCRIBE INDEXTABLES COMPONENT SIZES '/path'").createOrReplaceTempView("components")
spark.sql("SELECT component_type, SUM(size_bytes) FROM components GROUP BY component_type").show()
```

---

## Describe Prewarm Jobs

View async prewarm job status across all executors.

```sql
DESCRIBE INDEXTABLES PREWARM JOBS;
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `executor_id` | String | Executor identifier (e.g., "executor-0", "driver") |
| `host` | String | Host address of the executor |
| `job_id` | String | Unique job identifier |
| `table_path` | String | Path of the table being prewarmed |
| `status` | String | RUNNING, COMPLETED, FAILED, CANCELLED |
| `total_splits` | Integer | Total splits to prewarm |
| `completed_splits` | Integer | Splits prewarmed so far |
| `progress_pct` | Double | Percentage progress |
| `duration_ms` | Long | Duration in milliseconds |
| `error_message` | String | Error message if failed (null otherwise) |

- Collects job status from every executor and the driver
- Each executor maintains its own async prewarm job state

---

## Wait For Prewarm Jobs

Block until async prewarm jobs complete or timeout.

```sql
-- Wait for all jobs (1 hour default timeout)
WAIT FOR INDEXTABLES PREWARM JOBS;

-- Wait for jobs on a specific table
WAIT FOR INDEXTABLES PREWARM JOBS 's3://bucket/table';
WAIT FOR INDEXTABLES PREWARM JOBS my_table;

-- Wait for a specific job
WAIT FOR INDEXTABLES PREWARM JOBS JOB 'abc-123-def-456';

-- Wait with custom timeout (seconds)
WAIT FOR INDEXTABLES PREWARM JOBS TIMEOUT 300;

-- Full syntax
WAIT FOR INDEXTABLES PREWARM JOBS 's3://bucket/path' JOB 'job-123' TIMEOUT 1800;
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `executor_id` | String | Executor identifier |
| `host` | String | Host address of the executor |
| `job_id` | String | Unique job identifier |
| `table_path` | String | Path of the table prewarmed |
| `status` | String | COMPLETED, FAILED, CANCELLED, TIMEOUT, not_found |
| `total_splits` | Integer | Total number of splits |
| `splits_prewarmed` | Integer | Splits successfully prewarmed |
| `duration_ms` | Long | Total duration in milliseconds |
| `error_message` | String | Error message if failed (null otherwise) |

- Polls all executors every 5 seconds
- Default timeout: 3600 seconds (1 hour)
- Can filter by table path, specific job ID, or both
- Returns immediately with `not_found` if no matching jobs
- On timeout, returns current status as `TIMEOUT (was: RUNNING)`

---

## Build Companion

Build a companion search index for an external table (Delta, Parquet, or Iceberg).

```sql
-- Basic Delta companion
BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta_table'
  AT LOCATION 's3://bucket/index';

-- Delta with options
BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta'
  INDEXING MODES ('content':'text', 'status':'string')
  FASTFIELDS MODE PARQUET_ONLY
  TARGET INPUT SIZE 1G
  WRITER HEAP SIZE 512M
  FROM VERSION 42
  WHERE year = '2024'
  AT LOCATION 's3://bucket/index'
  DRY RUN;

-- Delta with Unity Catalog
BUILD INDEXTABLES COMPANION FOR DELTA 'my_schema.events'
  CATALOG 'unity' TYPE 'rest'
  AT LOCATION 's3://bucket/companion';

-- Parquet with schema source
BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/data'
  SCHEMA SOURCE 's3://bucket/data/part-00000.parquet'
  AT LOCATION 's3://bucket/index';

-- Iceberg with catalog and warehouse
BUILD INDEXTABLES COMPANION FOR ICEBERG 'analytics.web_events'
  CATALOG 'uc_catalog' TYPE 'rest'
  WAREHOUSE 's3://unity-warehouse/iceberg'
  AT LOCATION 's3://bucket/index';
```

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| `table_path` | String | Destination IndexTables path |
| `source_path` | String | Source table path |
| `status` | String | "success", "no_action", "dry_run", or "error" |
| `source_version` | Long | Delta version, Iceberg snapshot ID, or null |
| `splits_created` | Integer | Companion splits created |
| `splits_invalidated` | Integer | Companion splits invalidated |
| `parquet_files_indexed` | Integer | Parquet files processed |
| `parquet_bytes_downloaded` | Long | Bytes downloaded from source |
| `split_bytes_uploaded` | Long | Bytes uploaded for splits |
| `duration_ms` | Long | Duration in milliseconds |
| `message` | String | Descriptive status message |

**Options:**

| Option | Applies To | Description |
|--------|-----------|-------------|
| `SCHEMA SOURCE path` | Parquet only | Path to a parquet file for schema inference |
| `CATALOG name` | Delta, Iceberg | Catalog name for table resolution |
| `TYPE type` | Delta, Iceberg | Catalog type (e.g., "rest" for Unity Catalog) |
| `WAREHOUSE path` | Iceberg only | Warehouse path |
| `INDEXING MODES (...)` | All | Per-field indexing modes, e.g., `('content':'text', 'ip_addr':'ipaddress')` |
| `FASTFIELDS MODE` | All | HYBRID (default), DISABLED, or PARQUET_ONLY |
| `TARGET INPUT SIZE size` | All | Max input size per indexing group (default: 2GB) |
| `WRITER HEAP SIZE size` | All | Writer heap memory per executor |
| `FROM VERSION n` | Delta only | Start from a specific Delta version |
| `FROM SNAPSHOT n` | Iceberg only | Start from a specific snapshot ID |
| `WHERE predicate` | All | Partition filter predicate |
| `DRY RUN` | All | Preview without creating splits |

**Configuration:**
```scala
spark.indextables.companion.sync.batchSize: <defaultParallelism> (indexing tasks per batch)
spark.indextables.companion.sync.maxConcurrentBatches: 6
spark.indextables.companion.writerHeapSize: 1G
spark.indextables.companion.readerBatchSize: 8192
spark.indextables.companion.schedulerPool: "indextables-companion" (FAIR scheduler pool)
```

- Creates minimal Quickwit splits that reference external parquet files (45-70% split size reduction)
- Supports incremental sync via anti-join reconciliation (detects new/removed parquet files)
- Batched concurrent dispatch using Spark's FAIR scheduler
- Companion index is read via standard IndexTables DataSource API
