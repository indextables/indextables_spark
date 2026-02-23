# IndexTables Table Protocol

**Version:** 2.1
**Status:** Production
**Last Updated:** February 2026
**Protocol Version:** V4 (Avro State)

## Overview

IndexTables implements a Delta Lake-inspired transaction log protocol that provides ACID guarantees, time travel, and schema evolution safety for tables of Tantivy search indexes. Every table operation -- write, merge, overwrite, purge -- is recorded as an atomic, versioned transaction. The current protocol version (V4) uses an Avro-based state format with Iceberg-style manifest reuse for efficient incremental state writes.

This document covers the conceptual architecture, ACID guarantees, operational guidance, and performance characteristics of the table protocol. For the detailed wire-level specification -- action schemas, Avro field IDs, file format details, and configuration reference -- see [protocol.md](protocol.md). For the original design rationale and implementation details of the Avro state format, see [Avro State Design Document](../design/avro-state-file.md).

## Design Principles

1. **Immutability** -- Transaction log files are write-once, never modified after creation.
2. **Atomicity** -- All operations within a version are visible together or not at all.
3. **Isolation** -- Concurrent writers are detected and serialized via conditional writes.
4. **Durability** -- Cloud storage conditional writes (S3 `If-None-Match`, Azure leases) prevent accidental overwrites.
5. **Consistency** -- Monotonically increasing version numbers provide strict ordering.

---

## ACID Guarantees

### Atomicity

Each write operation produces a single version file containing all actions (adds, removes) for that transaction. Readers either see the complete version or none of it. The version file is written atomically using cloud storage conditional writes.

### Consistency

The transaction log enforces a consistent view of the table at every version:

- **Version 0 establishes the table.** It contains the ProtocolAction and MetadataAction that define the table's identity, schema, and partition columns. Version 0 is protected by conditional writes during initial table creation to prevent duplicate initialization. Once its contents are captured in a checkpoint or Avro state snapshot, version 0 may be cleaned up by PURGE or TRUNCATE TIME TRAVEL operations -- the table's identity is preserved in the checkpoint.
- **Active file computation** is deterministic: replay all adds and removes from the latest checkpoint forward to derive the set of live files.
- **Protocol version checks** run before every read and write, preventing incompatible clients from corrupting the table.

### Isolation

Writers use optimistic concurrency control:

1. Read the current latest version.
2. Prepare actions for the next version.
3. Attempt to write the version file with conditional `ifNotExists` semantics.
4. On conflict (HTTP 412), retry with exponential backoff and a re-read of current state.

This serializes concurrent writers without requiring locks. Retry behavior is configurable (see [protocol.md -- Concurrency Control](protocol.md#concurrency-control)).

### Durability

Once a version file is successfully written to cloud storage, its contents are durable. S3 Conditional Writes (`If-None-Match: *`) and equivalent Azure mechanisms guarantee that no subsequent write can overwrite an existing version file.

---

## Directory Structure

```
<table_path>/
├── splits/                              # Data files (tantivy4java split files)
│   ├── split-<uuid>.split
│   └── ...
├── <partition_col>=<value>/             # Partitioned data (if applicable)
│   └── splits/
│       └── split-<uuid>.split
└── _transaction_log/
    ├── 00000000000000000000.json        # Version 0 (protocol + metadata)
    ├── 00000000000000000001.json        # Version 1 (incremental changes)
    ├── ...
    ├── _last_checkpoint                 # Pointer to latest checkpoint/state
    ├── manifests/                       # Shared Avro manifest directory (V4+)
    │   ├── manifest-<hash>.avro         # Reusable manifest files
    │   └── ...
    └── state-v<version>/                # Avro state directory (V4+)
        └── _manifest.avro              # State manifest referencing shared manifests
```

**Key directories:**

- **`_transaction_log/`** -- All metadata lives here. Version files record incremental changes; the state directory captures consolidated snapshots.
- **`manifests/`** -- Shared Avro manifest files written once and referenced by multiple state versions (Iceberg-style reuse).
- **`state-v<version>/`** -- Each state snapshot is a directory containing a `_manifest.avro` that references entries in `manifests/`.

For detailed file naming conventions, version file format, and compression options, see [protocol.md -- Transaction Log Format](protocol.md#transaction-log-format).

---

## Protocol Versions

| Version | Status | Key Features |
|---------|--------|--------------|
| V1 | Historical | Basic transaction log: AddAction, RemoveAction, MetadataAction |
| V2 | Historical | SkipAction, extended AddAction metadata, footer offsets |
| V3 | Historical | Schema deduplication (`docMappingRef`), feature flags |
| **V4** | **Current** | **Avro state format, manifest-level partition pruning, incremental state writes** |

All new tables are created at V4. Tables at older protocol versions are automatically upgraded to V4 when a checkpoint is created.

For the complete feature flag list and version compatibility matrix, see [protocol.md -- Protocol Versions](protocol.md#protocol-versions).

---

## Read Path

### How a Read Works

1. **Check `_last_checkpoint`** -- Locate the latest Avro state directory.
2. **Load base state** -- Read `_manifest.avro` from the state directory. Apply partition pruning at the manifest level to skip irrelevant manifests entirely. Read matching Avro manifests in parallel (default: 8 threads). Filter out tombstoned entries.
3. **Apply incremental versions** -- Read any version files written after the state snapshot and apply their adds/removes.
4. **Return active files** -- The final set of AddActions represents all live splits in the table.

### Partition Pruning

Each manifest entry in `_manifest.avro` carries `partitionBounds` -- the min/max values for each partition column across all files in that manifest. When a query includes partition filters, entire manifests are skipped if their bounds don't overlap the filter predicates.

For a table with 1M files across 1,000 partitions, a single-partition query can skip 99.9% of manifests when compaction has sorted files by partition (see [Compaction](#compaction) below).

### Performance Characteristics

| Scenario | Performance |
|----------|-------------|
| Read 70K files | <500ms |
| Read 100K files | <700ms |
| Incremental write (add 100 files to 70K file table) | Write only 100 new entries (O(delta), not O(N)) |
| Query 1 partition (1M file table) | Load only matching manifests |
| Cold read (no state snapshot) | Parse all version files from version 0 |

---

## Write Path

### How a Write Works

1. **Initialize** (first write only) -- Write version 0 with ProtocolAction and MetadataAction. Protected by conditional write to prevent multiple initializations.
2. **Determine next version** -- Read the latest version and increment atomically.
3. **Write version file** -- Serialize all actions (adds, removes) as newline-delimited JSON, optionally GZIP-compressed. Write with conditional `ifNotExists`.
4. **Update state** (if checkpoint interval reached):
   - Write a new Avro manifest to `manifests/` containing only the new file entries (incremental -- existing manifests are not rewritten).
   - Write a new `_manifest.avro` in `state-v<version>/` that references all existing manifests plus the new one, with any new tombstones appended.
   - Update `_last_checkpoint` to point to the new state directory.
5. **Handle conflicts** -- If the version file write fails (412), re-read state and retry with exponential backoff.

### Incremental State Writes

A key design feature borrowed from Apache Iceberg: state writes are incremental. When a transaction adds 100 files to a table with 70,000 existing files, only the 100 new entries are written as a new manifest. The state manifest simply references the existing manifests plus the new one. This reduces write amplification from O(N) to O(delta).

```
Transaction 100: 70,000 files
  state-v100/_manifest.avro --> [manifests/manifest-aaa.avro (50K entries),
                                  manifests/manifest-bbb.avro (20K entries)]

Transaction 101: +100 files
  state-v101/_manifest.avro --> [manifests/manifest-aaa.avro (50K entries),   # reused
                                  manifests/manifest-bbb.avro (20K entries),  # reused
                                  manifests/manifest-ccc.avro (100 entries)]  # new
```

### Write Modes

| Mode | Behavior |
|------|----------|
| **Append** | Adds new split files. Version file contains only AddActions. |
| **Overwrite** | Removes all existing files and adds new ones. Version file contains RemoveActions for every active file, followed by AddActions. |
| **Merge** | Consolidates small splits. Removes source files, adds merged output. A single atomic version. |

### Merge Resilience (SkipAction)

During a merge operation, individual split files may fail to process -- for example, due to transient I/O errors or data corruption. Rather than failing the entire merge, the system records a SkipAction for the problematic file. The merge proceeds with the remaining files, and the skipped file is left in place as an active split.

Skipped files have a cooldown period (`retryAfter` timestamp) before the system attempts to include them in a subsequent merge. Each skip increments a counter (`skipCount`) that tracks how many times the file has been skipped. The current skip state for a table is visible via `DESCRIBE INDEXTABLES STATE`. For the SkipAction field-level schema, see [protocol.md](protocol.md).

### Automatic Maintenance

**Purge-on-write.** When enabled (`spark.indextables.purgeOnWrite.enabled`), write operations automatically trigger cleanup of orphaned split files and old transaction log versions as part of the write commit. This provides transparent table hygiene without requiring scheduled maintenance. See [configuration.md](configuration.md) for purge-on-write settings and thresholds.

**Merge-on-write.** When enabled (`spark.indextables.mergeOnWrite.enabled`), the system evaluates whether a merge is warranted after each write commits. If the number of small splits exceeds a configurable threshold, a merge is initiated -- asynchronously by default (`spark.indextables.mergeOnWrite.async.enabled`). See [configuration.md](configuration.md) for merge-on-write settings.

---

## Checkpoint and State Management

### State Format

The Avro state format uses a `state-v<version>/` directory containing a `_manifest.avro` file that references shared Avro manifest files in the `manifests/` directory. Each manifest file contains binary Avro-encoded `FileEntry` records representing live splits.

Key properties of the Avro state format:

- **Binary encoding** -- Avro binary format for fast serialization/deserialization.
- **Manifest reuse** -- New transactions write only a manifest for new files, referencing existing manifests by path (Iceberg-style incremental writes).
- **Partition bounds** -- Each manifest entry in the state manifest carries min/max partition values, enabling manifest-level pruning before any file entries are read.
- **Zstandard compression** -- Manifests use zstd compression by default (configurable).

For the full Avro schema (FileEntry, ManifestInfo, PartitionBounds) and field ID ranges, see [protocol.md -- Avro State Format](protocol.md#avro-state-format-v4). For design rationale, see [Avro State Design Document](../design/avro-state-file.md).

### When Checkpoints Are Created

- **Automatically** every N transactions (default: 10, configured via `spark.indextables.checkpoint.interval`).
- **Manually** via `CHECKPOINT INDEXTABLES '<path>'` or `COMPACT INDEXTABLES '<path>'` SQL commands.
- **After MERGE SPLITS** operations (when `spark.indextables.state.compaction.afterMerge` is enabled, which is the default).

### Compaction

Over time, incremental state writes accumulate tombstones (removed file paths) and manifest fragmentation. Compaction rewrites the entire state cleanly:

**Compaction triggers:**

| Condition | Default Threshold | Config Key |
|-----------|------------------|------------|
| Tombstone ratio exceeds threshold | >10% | `spark.indextables.state.compaction.tombstoneThreshold` |
| Manifest count exceeds limit | >20 | `spark.indextables.state.compaction.maxManifests` |
| After MERGE SPLITS | Always | `spark.indextables.state.compaction.afterMerge` |
| Force via SQL | N/A | `COMPACT INDEXTABLES '<path>'` |

**Compaction process:**

1. Read all live files (tombstones applied).
2. Sort files by partition values for locality -- this ensures files from the same partition end up in the same manifest, maximizing the effectiveness of partition pruning.
3. Partition into chunks of up to 50,000 entries (configurable via `spark.indextables.state.entriesPerManifest`).
4. Write new manifests with computed partition bounds.
5. Write a clean state manifest with zero tombstones.

### Observability

Use `DESCRIBE INDEXTABLES STATE` to inspect the current state:

```sql
DESCRIBE INDEXTABLES STATE 's3://bucket/path';
```

Returns the current state format, version, file count, manifest count, tombstone count and ratio, and whether compaction is recommended.

---

## Retention and Cleanup

### Time Travel

The transaction log enables time travel -- the ability to query the table's state at any historical version. Because each version file records a delta of changes (files added and removed), replaying versions from a checkpoint (or from version 0 if no checkpoint exists) up to version N reconstructs the exact table state at version N.

Time travel is bounded by retention settings. As old version files and state directories are cleaned up, the range of reachable historical versions narrows. Once a version file is deleted, the table state at that version can no longer be reconstructed. The `TRUNCATE INDEXTABLES TIME TRAVEL` SQL command explicitly removes all historical versions, collapsing the table to only its current state. Always run with `DRY RUN` first to preview what will be deleted.

### Transaction Log Retention

Old version files are cleaned up when all conditions are met:

1. File age exceeds retention period (default: configurable via `spark.indextables.purge.txLogRetentionHours`).
2. File version is covered by an existing checkpoint.
3. File is not the current version.
4. Version 0 follows the same rules -- it may be deleted once covered by a checkpoint. The checkpoint preserves the protocol and metadata information originally recorded in version 0.

### State Retention

Multiple state directories may exist temporarily. Retention controls:

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.retention.versions` | 2 | Number of old state versions to keep |
| `spark.indextables.state.retention.hours` | 168 (7 days) | Time-based retention for old states |

### Manifest Garbage Collection

With shared manifests, orphaned manifest files can accumulate when state directories are deleted but their manifests are no longer referenced. The garbage collector:

1. Scans all retained state versions to build a set of reachable manifests.
2. Lists all files in `manifests/`.
3. Deletes unreferenced manifests older than the safety window (default: 1 hour, configured via `spark.indextables.state.gc.minManifestAgeHours`).

The age-based safety window prevents race conditions with in-flight writes that reference newly-created manifests.

### SQL Commands for Maintenance

| Command | Purpose |
|---------|---------|
| `CHECKPOINT INDEXTABLES '<path>'` | Force state snapshot creation |
| `COMPACT INDEXTABLES '<path>'` | Alias for CHECKPOINT -- forces full compaction |
| `TRUNCATE INDEXTABLES TIME TRAVEL '<path>'` | Remove all historical versions, keep only current state |
| `PURGE INDEXTABLE '<path>' OLDER THAN 7 DAYS` | Remove orphaned splits, old versions, old state dirs |
| `DESCRIBE INDEXTABLES STATE '<path>'` | Inspect state format, version, tombstone ratio |

See [sql-commands.md](sql-commands.md) for full syntax, options, and output schemas.

---

## Concurrency Control

### Optimistic Concurrency with Conditional Writes

All transaction log writes use cloud storage conditional writes:

- **S3**: `PutObject` with `If-None-Match: *` header. Returns HTTP 412 on conflict.
- **Azure**: Equivalent lease-based conditional writes.

This provides serializable write isolation without distributed locks. When two writers race for the same version:

```
Writer A: writes version 5 --> SUCCESS (file created)
Writer B: writes version 5 --> FAILURE (412 Precondition Failed)
Writer B: re-reads state, writes version 6 --> SUCCESS
```

### State Write Concurrency

Avro state writes also use conditional semantics:

1. Check if the target state directory exists.
2. Write `_manifest.avro` with `ifNotExists`.
3. On conflict, **re-read the base state** (to pick up the concurrent writer's changes), increment version, and retry.

Re-reading the base state on every retry is critical to avoid stale manifest lists.

### Retry Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.transaction.retry.maxAttempts` | 10 | Maximum version-file retry attempts |
| `spark.indextables.transaction.retry.baseDelayMs` | 100 | Initial backoff delay |
| `spark.indextables.transaction.retry.maxDelayMs` | 5000 | Maximum backoff cap |
| `spark.indextables.state.retry.maxAttempts` | 10 | Maximum state-write retry attempts |
| `spark.indextables.state.retry.baseDelayMs` | 100 | Initial backoff delay |
| `spark.indextables.state.retry.maxDelayMs` | 5000 | Maximum backoff cap |

---

## Schema Deduplication

Large schemas (400+ columns) can bloat the transaction log when each AddAction carries a full copy. Schema deduplication (V3+) solves this:

1. Compute a canonical SHA-256 hash of the schema JSON (sorted keys, sorted named arrays, Base64-encoded, truncated to 16 characters).
2. Store the schema once in `MetadataAction.configuration` under the key `docMappingSchema.<hash>`.
3. Each AddAction carries only the hash in `docMappingRef` instead of the full schema in `docMappingJson`.

For details on hash computation and renormalization, see [protocol.md -- Schema Deduplication](protocol.md#schema-deduplication-v3).

---

## Data Skipping

AddActions carry per-column `minValues` and `maxValues` maps that enable data skipping at the split level. Long text values are truncated (default: 32 characters) to prevent state bloat. Partition columns are never truncated.

With Avro state (V4), partition bounds are also stored at the manifest level in `ManifestInfo.partitionBounds`, enabling an additional layer of pruning before individual file entries are even read.

Configuration:

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.stats.truncation.enabled` | true | Enable statistics truncation |
| `spark.indextables.stats.truncation.maxLength` | 32 | Maximum characters for string statistics |
| `spark.indextables.dataSkippingNumIndexedCols` | 32 | Number of columns to index for skipping |

---

## Comparison to Delta Lake and Iceberg

| Aspect | IndexTables (V4) | Delta Lake | Apache Iceberg |
|--------|-----------------|------------|----------------|
| **Data file format** | Tantivy search index (`.split`) | Parquet | Parquet, ORC, Avro |
| **Primary use case** | Full-text search | OLAP/Analytics | OLAP/Analytics |
| **State format** | Avro manifests (binary) | JSON checkpoint + Parquet | Avro manifest files |
| **Manifest reuse** | Iceberg-style shared manifests | No (full rewrite) | Yes (manifest lists) |
| **Partition pruning** | Manifest-level bounds | File-level statistics | Manifest-level bounds |
| **Conditional writes** | S3 `If-None-Match` (native) | DynamoDB / S3 multi-part | Catalog-based locking |
| **Schema evolution** | Immutable (future planned) | Full support | Full support |
| **Time travel** | Via transaction log | Via transaction log | Via snapshots |
| **Merge resilience** | SkipAction with cooldown | Not present | Not present |

IndexTables' state architecture is most similar to Iceberg's manifest list + manifest file approach. The key difference is that IndexTables stores search indexes rather than columnar data files.

---

## Migration Guide

### Upgrading Existing Tables

Tables created at older protocol versions (V1, V2, V3) are automatically upgraded to V4 Avro state when a checkpoint is created. You can also force the upgrade explicitly:

```sql
-- Check current state format
DESCRIBE INDEXTABLES STATE 's3://bucket/path';

-- Upgrade to V4 Avro state
CHECKPOINT INDEXTABLES 's3://bucket/path';

-- Verify upgrade
DESCRIBE INDEXTABLES STATE 's3://bucket/path';
-- format should now show "avro-state"
```

The upgrade reads the current table state (from version files or any existing checkpoint), writes a new Avro state directory with all live files, and updates `_last_checkpoint`. No data files are modified -- the upgrade only affects transaction log metadata.

---

## Troubleshooting

### Table Unreadable ("No transaction log found")

**Cause:** No checkpoint and no version 0 exist. On a healthy table, version 0 may have been deleted after a checkpoint captured its contents -- this is normal. The error indicates that neither a checkpoint nor version 0 can be found.

```bash
# Check for a checkpoint or state directory
aws s3 ls s3://bucket/table/_transaction_log/_last_checkpoint
aws s3 ls s3://bucket/table/_transaction_log/state-v

# If no checkpoint exists, check for version 0
aws s3 ls s3://bucket/table/_transaction_log/00000000000000000000.json
```

**Recovery:** If a checkpoint exists but `_last_checkpoint` is missing or corrupted, recreate it. If no checkpoint or version files exist, use `REPAIR INDEXFILES TRANSACTION LOG '<path>'` to reconstruct from existing split files.

### Slow Reads

**Cause:** No state snapshot exists, forcing replay of all version files from version 0.

```sql
-- Check current state
DESCRIBE INDEXTABLES STATE 's3://bucket/path';

-- Create a state snapshot
CHECKPOINT INDEXTABLES 's3://bucket/path';
```

### High Tombstone Ratio

**Cause:** Many merge or delete operations without compaction.

```sql
-- Check tombstone ratio
DESCRIBE INDEXTABLES STATE 's3://bucket/path';

-- If needs_compaction is true:
COMPACT INDEXTABLES 's3://bucket/path';
```

### Concurrent Write Conflicts

**Symptom:** `IllegalStateException: Failed to write transaction log version N - file already exists`

**Cause:** Multiple writers attempted the same version. The system retries automatically (up to 10 attempts by default). If retries are exhausted, increase `spark.indextables.transaction.retry.maxAttempts` or reduce write concurrency.

---

## Best Practices

### For Writers

- **Prefer append mode** -- Appends are the cheapest operation (no file listing required).
- **Batch small writes** -- Many tiny transactions create checkpoint/compaction overhead. Prefer fewer, larger transactions.
- **Let checkpoints happen automatically** -- The default interval of 10 transactions is appropriate for most workloads.

### For Readers

- **Upgrade older tables** -- Run `CHECKPOINT INDEXTABLES` on any pre-V4 tables to upgrade them to the Avro state format.
- **Leverage partition pruning** -- Partition your data and filter by partition columns to skip irrelevant manifests.
- **Configure cache TTL appropriately** -- Default cache expiration is 5 minutes (`spark.indextables.transaction.cache.expirationSeconds`). Increase for read-heavy workloads; decrease for write-heavy workloads where freshness matters.

### For Operations

- **Run periodic PURGE** -- Schedule `PURGE INDEXTABLE` to clean up orphaned files and old state directories.
- **Monitor tombstone ratio** -- Use `DESCRIBE INDEXTABLES STATE` to check health. Compact when tombstone ratio exceeds 10%.
- **Monitor checkpoint existence** -- Ensure at least one valid checkpoint or Avro state directory exists. Version 0 is routinely deleted after checkpoints, so monitor for checkpoint health rather than version 0 file presence.
- **Use TRUNCATE TIME TRAVEL with caution** -- This is irreversible. Always run with `DRY RUN` first.

---

## References

- [protocol.md](protocol.md) -- Detailed wire-level protocol specification (action schemas, Avro field IDs, configuration tables)
- [sql-commands.md](sql-commands.md) -- Full SQL command syntax and output schemas
- [configuration.md](configuration.md) -- Complete configuration reference
- [Avro State Design Document](../design/avro-state-file.md) -- Original design document for the V4 Avro state format
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) -- Inspiration for the transaction log design
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/) -- Inspiration for the manifest reuse architecture

---

## Changelog

### v2.1 (February 2026)

- Removed all references to the deprecated JSON checkpoint/state format
- Document now exclusively covers the V4 Avro state protocol
- Simplified read path, migration guide, and performance table
- Updated cross-references to avro-state-file.md design document

### v2.0 (February 2026)

- Rewrote document to reflect V4 Avro state as the current protocol version
- Removed spec-level detail that duplicates protocol.md
- Added ACID guarantees section with conceptual explanations
- Added migration guide for V1/V2/V3 to V4 upgrades
- Updated directory structure to include `manifests/` and `state-v*/`
- Updated comparison table to include Apache Iceberg
- Added compaction triggers and operational guidance
- Added SQL command summary for maintenance operations
- Added troubleshooting section

### v1.0 (October 2025)

- Initial protocol specification
- S3 Conditional Writes implementation
- Version 0 conditional write protection
- SkipAction for merge resilience
