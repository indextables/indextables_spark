# IndexTables Table Protocol

**Version:** 2.0
**Status:** Production
**Last Updated:** February 2026
**Protocol Version:** V4 (Avro State)

## Overview

IndexTables implements a Delta Lake-inspired transaction log protocol that provides ACID guarantees, time travel, and schema evolution safety for tables of Tantivy search indexes. Every table operation -- write, merge, overwrite, purge -- is recorded as an atomic, versioned transaction. The protocol's current version (V4) uses an Avro-based state format that delivers 10-28x faster state reads compared to the original JSON format.

This document covers the conceptual architecture, ACID guarantees, operational guidance, and performance characteristics of the table protocol. For the detailed wire-level specification -- action schemas, Avro field IDs, file format details, and configuration reference -- see [protocol.md](protocol.md).

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

- **Version 0 is sacrosanct.** It contains the ProtocolAction and MetadataAction that define the table's identity, schema, and partition columns. Version 0 is protected by conditional writes and is never deleted by any cleanup operation.
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
    ├── 00000000000000000000.json        # Version 0 (protocol + metadata, never deleted)
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

| Version | Introduced | Key Features |
|---------|-----------|--------------|
| V1 | Initial | Basic transaction log: AddAction, RemoveAction, MetadataAction |
| V2 | | SkipAction, extended AddAction metadata, footer offsets |
| V3 | | Multi-part JSON checkpoints, schema deduplication (`docMappingRef`), feature flags |
| V4 | Current | Avro state format, manifest-level partition pruning, 10-28x faster reads |

New tables are created at V4 by default. Tables at older protocol versions are automatically upgraded when a checkpoint is created. The current implementation can read all versions (V1-V4) and writes at V4.

For the complete feature flag list and version compatibility matrix, see [protocol.md -- Protocol Versions](protocol.md#protocol-versions).

---

## Read Path

### How a Read Works

1. **Check `_last_checkpoint`** -- Determine whether the latest checkpoint is Avro state, JSON multi-part, or single-file JSON.
2. **Load base state:**
   - **Avro state (V4):** Read `_manifest.avro` from the state directory. Apply partition pruning at the manifest level to skip irrelevant manifests entirely. Read matching Avro manifests in parallel (default: 8 threads). Filter out tombstoned entries.
   - **JSON checkpoint (legacy):** Parse the JSON checkpoint file(s), converting to the same in-memory representation.
3. **Apply incremental versions** -- Read any version files written after the checkpoint and apply their adds/removes.
4. **Return active files** -- The final set of AddActions represents all live splits in the table.

### Partition Pruning

With V4 Avro state, each manifest entry in `_manifest.avro` carries `partitionBounds` -- the min/max values for each partition column across all files in that manifest. When a query includes partition filters, entire manifests are skipped if their bounds don't overlap the filter predicates.

For a table with 1M files across 1,000 partitions, a single-partition query can skip 99.9% of manifests when compaction has sorted files by partition (see [Compaction](#compaction) below).

### Performance Characteristics

| Scenario | JSON Checkpoint | Avro State (V4) |
|----------|----------------|-----------------|
| Read 70K files | ~14 seconds | <500ms |
| Read 100K files | ~20 seconds | <700ms |
| Query 1 partition (1M file table) | Load all 1M entries | Load only matching manifests |
| Cold read (no checkpoint) | Parse all version files | Direct state read |

---

## Write Path

### How a Write Works

1. **Initialize** (first write only) -- Write version 0 with ProtocolAction and MetadataAction. Protected by conditional write to prevent multiple initializations.
2. **Determine next version** -- Read the latest version and increment atomically.
3. **Write version file** -- Serialize all actions (adds, removes) as newline-delimited JSON, optionally GZIP-compressed. Write with conditional `ifNotExists`.
4. **Update state** (if checkpoint interval reached):
   - Write a new Avro manifest to `manifests/` containing only the new file entries.
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

---

## Checkpoint and State Management

### Checkpoint Formats

| Format | Protocol | Description |
|--------|----------|-------------|
| Single-file JSON | V1-V2 | `<version>.checkpoint.json` -- all actions in one GZIP-compressed file |
| Multi-part JSON | V3 | Manifest + parts: `<version>.checkpoint.<uuid>.<part>.json` |
| **Avro state** | **V4 (default)** | `state-v<version>/_manifest.avro` referencing shared Avro manifests |

Avro state is the default for all new tables and checkpoint operations. JSON formats are supported for reading (backward compatibility) but deprecated for writing.

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

Returns the current format (`avro-state`, `json`, etc.), version, file count, manifest count, tombstone count and ratio, and whether compaction is recommended.

---

## Retention and Cleanup

### Transaction Log Retention

Old version files are cleaned up when all conditions are met:

1. File age exceeds retention period (default: configurable via `spark.indextables.purge.txLogRetentionHours`).
2. File version is covered by an existing checkpoint.
3. File is not the current version.
4. **Version 0 is never deleted.**

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
| `CHECKPOINT INDEXTABLES '<path>'` | Force checkpoint creation (upgrades to V4 Avro) |
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

### Upgrading to V4 (Avro State)

Tables at V1, V2, or V3 are automatically upgraded to V4 when any checkpoint is created. You can also force the upgrade:

```sql
-- Check current state format
DESCRIBE INDEXTABLES STATE 's3://bucket/path';

-- Upgrade to V4 Avro state
CHECKPOINT INDEXTABLES 's3://bucket/path';

-- Verify upgrade
DESCRIBE INDEXTABLES STATE 's3://bucket/path';
-- format should now show "avro-state"
```

The upgrade process:

1. Reads current state from whatever format exists (JSON, JSON multi-part, or version file replay).
2. Writes a new Avro state directory with all live files.
3. Updates `_last_checkpoint` with `format: "avro-state"`.
4. Subsequent reads use the new Avro format automatically.

No data files are modified during upgrade. The upgrade only affects transaction log metadata.

### Emergency Downgrade

If issues are discovered with the Avro format (not expected in production):

```scala
spark.conf.set("spark.indextables.state.format", "json")
spark.sql("CHECKPOINT INDEXTABLES '<table_path>'")
```

This forces the next checkpoint to use JSON format. JSON format is deprecated and will be removed in a future release. Keep read support indefinitely.

### Mixed-Version Clusters

During rolling upgrades where some nodes run older code:

- Older code (pre-V4) can only read JSON checkpoints.
- Newer code reads both JSON and Avro.
- **Recommendation:** Upgrade all nodes before enabling Avro writes, or ensure all checkpoint-creating operations run on upgraded nodes.

---

## Troubleshooting

### Table Unreadable ("No transaction log found")

**Cause:** Version 0 is missing or corrupted.

```bash
# Verify version 0 exists
aws s3 ls s3://bucket/table/_transaction_log/00000000000000000000.json
```

**Recovery:** Restore version 0 from backup, or use `REPAIR INDEXFILES TRANSACTION LOG '<path>'` to reconstruct from existing split files.

### Slow Reads

**Cause:** No checkpoint exists, forcing replay of all version files. Or JSON checkpoint on a large table.

```sql
-- Check current state
DESCRIBE INDEXTABLES STATE 's3://bucket/path';

-- Create/upgrade checkpoint
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

- **Use Avro state (V4)** -- Ensure tables are upgraded for optimal read performance.
- **Leverage partition pruning** -- Partition your data and filter by partition columns to skip irrelevant manifests.
- **Configure cache TTL appropriately** -- Default cache expiration is 5 minutes (`spark.indextables.transaction.cache.expirationSeconds`). Increase for read-heavy workloads; decrease for write-heavy workloads where freshness matters.

### For Operations

- **Run periodic PURGE** -- Schedule `PURGE INDEXTABLE` to clean up orphaned files and old state directories.
- **Monitor tombstone ratio** -- Use `DESCRIBE INDEXTABLES STATE` to check health. Compact when tombstone ratio exceeds 10%.
- **Monitor version 0** -- Alert if version 0 is ever missing; it is the table's identity and cannot be recreated.
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

### v2.0 (February 2026)

- Rewrote document to reflect V4 Avro state as the current protocol version
- Removed spec-level detail that duplicates protocol.md
- Added ACID guarantees section with conceptual explanations
- Added migration guide for V1/V2/V3 to V4 upgrades
- Updated performance numbers for Avro state (10-28x faster reads, <500ms for 70K files)
- Updated directory structure to include `manifests/` and `state-v*/`
- Updated comparison table to include Apache Iceberg
- Added compaction triggers and operational guidance
- Added SQL command summary for maintenance operations
- Added troubleshooting section

### v1.0 (October 2025)

- Initial protocol specification
- S3 Conditional Writes implementation
- Version 0 immutability protection
- JSON checkpoint system
- SkipAction for merge resilience
