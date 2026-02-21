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
