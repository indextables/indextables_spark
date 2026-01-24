# IndexTables4Spark Protocol

This document describes the protocol specification for IndexTables4Spark, a high-performance Spark DataSource implementing full-text search using Tantivy via tantivy4java.

## Overview

IndexTables4Spark stores table metadata in a transaction log directory (`_transaction_log/`) alongside the data files. The transaction log provides:

- **Atomicity**: All changes are either fully committed or not visible
- **Consistency**: Readers always see a consistent snapshot
- **Durability**: Once committed, changes are persisted
- **Time Travel**: Historical versions can be accessed (when retained)

## Table Directory Structure

```
<table_path>/
├── splits/                           # Data directory (tantivy4java split files)
│   ├── split-<uuid>.split
│   └── ...
├── <partition_column>=<value>/       # Partitioned data (if applicable)
│   └── splits/
│       └── split-<uuid>.split
└── _transaction_log/
    ├── 00000000000000000000.json     # Version 0 (initial protocol + metadata)
    ├── 00000000000000000001.json     # Version 1 (incremental changes)
    ├── ...
    ├── _last_checkpoint              # Pointer to latest checkpoint
    ├── manifests/                    # Shared manifest directory (Protocol V4+)
    │   ├── manifest-<hash>.avro      # Reusable Avro manifest files
    │   └── ...
    └── state-v00000000000000000042/  # Avro state directory (Protocol V4+)
        └── _manifest.json            # State manifest (references shared manifests)
```

---

## Protocol Versions

| Version | Features |
|---------|----------|
| 1 | Basic transaction log, AddAction, RemoveAction, MetadataAction |
| 2 | Extended metadata, footer offsets, SkipAction, split metadata |
| 3 | Multi-part checkpoint, schema deduplication, feature flags |
| 4 | Avro state format, partition pruning, 10x faster reads |

### Version Compatibility

- **minReaderVersion**: Minimum protocol version required to read the table
- **minWriterVersion**: Minimum protocol version required to write to the table
- Readers/writers must support all features required by the protocol version

---

## Transaction Log Format

### Version Files

Each transaction is recorded in a version file with a 20-digit zero-padded filename:

```
00000000000000000000.json  # Version 0
00000000000000000001.json  # Version 1
...
```

Version files are **immutable** once written. Concurrent writers use atomic `ifNotExists` semantics.

### Version File Contents

Each version file contains one or more newline-delimited JSON objects. Each object represents an **Action** with a single top-level key indicating the action type:

```json
{"protocol": {...}}
{"metaData": {...}}
{"add": {...}}
{"add": {...}}
{"remove": {...}}
```

### Compression

Version files may be GZIP compressed (default: enabled). Readers must detect compression by examining file headers.

---

## Actions

### Protocol Action

Specifies the minimum reader and writer versions required for the table.

| Field | Type | Description |
|-------|------|-------------|
| `minReaderVersion` | Int | Minimum version for readers |
| `minWriterVersion` | Int | Minimum version for writers |
| `readerFeatures` | Set[String] | Optional feature flags for readers (V3+) |
| `writerFeatures` | Set[String] | Optional feature flags for writers (V3+) |

**Example:**
```json
{
  "protocol": {
    "minReaderVersion": 4,
    "minWriterVersion": 4,
    "readerFeatures": ["avroState", "schemaDeduplication"],
    "writerFeatures": ["avroState", "schemaDeduplication"]
  }
}
```

**Feature Flags (V3+):**
- `avroState`: Avro state checkpoint format
- `multiPartCheckpoint`: Multi-part JSON checkpoint support
- `schemaDeduplication`: Schema registry deduplication

### Metadata Action

Stores table metadata including schema, partitioning, and configuration.

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Unique table identifier (UUID) |
| `name` | String? | Optional table name |
| `description` | String? | Optional description |
| `format` | Object | Format specification (`{"provider": "indextables"}`) |
| `schemaString` | String | JSON-encoded Apache Spark schema |
| `partitionColumns` | Array[String] | Column names used for partitioning |
| `configuration` | Map[String, String] | Key-value configuration |
| `createdTime` | Long? | Creation timestamp (epoch milliseconds) |

**Example:**
```json
{
  "metaData": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "format": {"provider": "indextables", "options": {}},
    "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
    "partitionColumns": ["date"],
    "configuration": {
      "docMappingSchema.ab12cd34ef56gh78": "[{\"name\":\"title\",...}]"
    },
    "createdTime": 1704067200000
  }
}
```

**Configuration Keys:**
- `docMappingSchema.<hash>`: Schema registry entries (V3+ schema deduplication)
- Custom indexing configuration

### Add Action

Represents a file (split) added to the table.

| Field | Type | Description |
|-------|------|-------------|
| `path` | String | Relative path to split file |
| `partitionValues` | Map[String, String] | Partition column values |
| `size` | Long | File size in bytes |
| `modificationTime` | Long | Modification timestamp (epoch ms) |
| `dataChange` | Boolean | Whether this is a data change |
| `stats` | String? | JSON-encoded statistics |
| `minValues` | Map[String, String]? | Minimum values per column |
| `maxValues` | Map[String, String]? | Maximum values per column |
| `numRecords` | Long? | Number of records in file |
| `hasFooterOffsets` | Boolean | Whether footer offsets are populated |
| `footerStartOffset` | Long? | Byte offset where footer begins |
| `footerEndOffset` | Long? | Byte offset where footer ends |
| `splitTags` | Set[String]? | Tags associated with this split |
| `numMergeOps` | Int? | Number of merge operations applied |
| `docMappingRef` | String? | Schema hash reference (V3+) |
| `docMappingJson` | String? | Inline schema JSON (legacy, pre-V3) |
| `uncompressedSizeBytes` | Long? | Uncompressed data size |

**Example:**
```json
{
  "add": {
    "path": "date=2024-01-01/splits/split-abc123.split",
    "partitionValues": {"date": "2024-01-01"},
    "size": 1048576,
    "modificationTime": 1704067200000,
    "dataChange": true,
    "minValues": {"score": "0.1"},
    "maxValues": {"score": "0.9"},
    "numRecords": 1000,
    "hasFooterOffsets": true,
    "footerStartOffset": 900000,
    "footerEndOffset": 1048500,
    "docMappingRef": "ab12cd34ef56gh78"
  }
}
```

### Remove Action

Marks a file as logically deleted.

| Field | Type | Description |
|-------|------|-------------|
| `path` | String | Path to file being removed |
| `deletionTimestamp` | Long? | When file was deleted (epoch ms) |
| `dataChange` | Boolean | Whether this affects data |
| `partitionValues` | Map[String, String]? | Partition values |
| `size` | Long? | File size |

**Example:**
```json
{
  "remove": {
    "path": "splits/old-split.split",
    "deletionTimestamp": 1704153600000,
    "dataChange": true
  }
}
```

### Skip Action (V2+)

Records files that should be temporarily skipped during operations.

| Field | Type | Description |
|-------|------|-------------|
| `path` | String | Path to file |
| `skipTimestamp` | Long | When skip was recorded |
| `reason` | String | Why file was skipped |
| `operation` | String | Operation that failed |
| `retryAfter` | Long? | Timestamp after which retry is allowed |
| `skipCount` | Int | Number of consecutive skips |

**Example:**
```json
{
  "mergeskip": {
    "path": "splits/corrupted.split",
    "skipTimestamp": 1704067200000,
    "reason": "Corrupted index footer",
    "operation": "merge",
    "retryAfter": 1704153600000,
    "skipCount": 1
  }
}
```

---

## Checkpoints

Checkpoints consolidate the cumulative state of the table at a specific version, enabling faster reads by avoiding scanning all version files.

### `_last_checkpoint` File

Points to the current checkpoint. JSON format:

```json
{
  "version": 42,
  "size": 15000,
  "sizeInBytes": 104857600,
  "numFiles": 15000,
  "createdTime": 1704067200000,
  "format": "avro-state",
  "stateDir": "state-v00000000000000000042"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `version` | Long | Transaction version |
| `size` | Long | Number of actions |
| `sizeInBytes` | Long | Total bytes |
| `numFiles` | Long | Number of AddActions |
| `createdTime` | Long | Creation timestamp |
| `parts` | Int? | Number of parts (multi-part JSON) |
| `checkpointId` | String? | UUID for multi-part (V3) |
| `format` | String? | `"json"`, `"json-multipart"`, or `"avro-state"` |
| `stateDir` | String? | State directory name (V4 Avro) |

### JSON Checkpoint (Legacy)

**Single-File Format:**
- Filename: `<version>.checkpoint.json`
- Contains all actions from version 0 to checkpoint version
- GZIP compressed

**Multi-Part Format (V3):**
- Manifest: `<version>.checkpoint.json` (contains `MultiPartCheckpointManifest`)
- Parts: `<version>.checkpoint.<uuid>.<part>.json`

### Avro State Format (V4+)

The Avro state format provides 10x faster reads compared to JSON checkpoints.

#### State Directory Structure

```
_transaction_log/
├── manifests/                        # Shared manifest directory
│   ├── manifest-<hash>.avro          # Reusable across state versions
│   └── manifest-<hash2>.avro
└── state-v<version>/
    └── _manifest.json                # References manifests/ directory
```

**Incremental Writes (Iceberg-style):**

New transactions only write a new manifest for new files. Existing manifests are
referenced by path, avoiding O(n) rewrites:

```
BEFORE (full rewrite):
  state-v100/manifest-001.avro  (100K entries)
  state-v101/manifest-002.avro  (100K entries - ALL rewritten!)

AFTER (incremental):
  manifests/manifest-001.avro   (100K entries - written once, shared)
  manifests/manifest-002.avro   (10 entries - only NEW files)
  state-v100/_manifest.json → [manifests/manifest-001.avro]
  state-v101/_manifest.json → [manifests/manifest-001.avro, manifests/manifest-002.avro]
```

#### State Manifest (`_manifest.json`)

```json
{
  "formatVersion": 1,
  "stateVersion": 42,
  "createdAt": 1704067200000,
  "numFiles": 15000,
  "totalBytes": 1099511627776,
  "protocolVersion": 4,
  "manifests": [
    {
      "path": "manifests/manifest-a1b2c3d4.avro",
      "numEntries": 50000,
      "minAddedAtVersion": 0,
      "maxAddedAtVersion": 42,
      "partitionBounds": {
        "date": {"min": "2024-01-01", "max": "2024-01-31"}
      }
    }
  ],
  "tombstones": [],
  "schemaRegistry": {
    "ab12cd34ef56gh78": "[{\"name\":\"title\",...}]"
  },
  "metadata": "{\"metaData\": {...}}"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `formatVersion` | Int | State format version (currently 1) |
| `stateVersion` | Long | Transaction version this state represents |
| `createdAt` | Long | Creation timestamp (epoch ms) |
| `numFiles` | Long | Total live files (after tombstones) |
| `totalBytes` | Long | Total size of live files |
| `protocolVersion` | Int | Protocol version (4 for Avro state) |
| `manifests` | Array[ManifestInfo] | List of Avro manifest files |
| `tombstones` | Array[String] | Paths of removed files |
| `schemaRegistry` | Map[String, String] | Schema hash -> schema JSON |
| `metadata` | String? | JSON-encoded MetadataAction |

#### ManifestInfo

| Field | Type | Description |
|-------|------|-------------|
| `path` | String | Path to manifest file (see path formats below) |
| `numEntries` | Long | Number of FileEntry records |
| `minAddedAtVersion` | Long | Earliest version in manifest |
| `maxAddedAtVersion` | Long | Latest version in manifest |
| `partitionBounds` | Map[String, PartitionBounds]? | Bounds for partition pruning |

**Manifest Path Formats:**

Manifest paths are relative to the transaction log root and support three formats:

| Format | Example | Description |
|--------|---------|-------------|
| Shared | `manifests/manifest-abc123.avro` | New format: shared across state versions |
| Normalized legacy | `state-v00000000000000000042/manifest-abc123.avro` | Legacy path normalized with state dir prefix |
| Legacy | `manifest-abc123.avro` | Legacy format: relative to current state dir |

**Path Resolution:**

```scala
def resolveManifestPath(path: String, txLogRoot: String, stateDir: String): String = {
  if (path.startsWith("manifests/"))      // Shared format
    s"$txLogRoot/$path"
  else if (path.startsWith("state-v"))    // Normalized legacy
    s"$txLogRoot/$path"
  else                                     // Legacy format
    s"$stateDir/$path"
}
```

#### PartitionBounds

| Field | Type | Description |
|-------|------|-------------|
| `min` | String? | Minimum partition value |
| `max` | String? | Maximum partition value |

#### FileEntry (Avro Record)

Binary Avro records with field IDs for schema evolution:

| Field ID Range | Category |
|----------------|----------|
| 100-109 | Basic file info |
| 110-119 | Statistics |
| 120-129 | Footer offsets |
| 130-139 | Split metadata |
| 140-149 | Streaming fields |

Fields mirror AddAction with additions:
- `addedAtVersion`: Transaction version when file was added
- `addedAtTimestamp`: Timestamp when file was added

#### Compression

Avro manifests support configurable compression:
- `zstd` (default, level 3)
- `snappy`
- `none`

---

## Schema Deduplication (V3+)

Large schemas (400+ columns) can bloat the transaction log significantly. Schema deduplication reduces this by storing each unique schema once.

### Mechanism

1. **Hash Computation:**
   - Normalize JSON to canonical form (sort object keys alphabetically)
   - Sort arrays of named objects by their `"name"` field
   - Compute SHA-256 hash of canonical JSON
   - Base64 encode and truncate to 16 characters (96 bits entropy)

2. **Write Path:**
   - Compute hash of `docMappingJson`
   - Store hash in `docMappingRef` field of AddAction
   - Store schema in `MetadataAction.configuration` with key `docMappingSchema.<hash>`
   - Remove inline `docMappingJson` from AddAction

3. **Read Path:**
   - Load schema registry from MetadataAction configuration
   - Restore `docMappingJson` from registry using `docMappingRef`

### Schema Registry Key Format

```
docMappingSchema.<16-char-base64-hash>
```

Example:
```
docMappingSchema.ab12cd34ef56gh78
```

### Renormalization

When unique `docMappingRef` count exceeds a threshold (default: 5), checkpoint operations recalculate all hashes using canonical normalization. This consolidates schemas that may have been hashed with different JSON orderings.

---

## Data Skipping

### Column Statistics

AddActions may include `minValues` and `maxValues` maps for data skipping:

```json
{
  "minValues": {"score": "0.1", "date": "2024-01-01"},
  "maxValues": {"score": "0.9", "date": "2024-01-31"}
}
```

Statistics are stored as strings and compared lexicographically for string columns or parsed for numeric columns.

### Statistics Truncation

Long text values are truncated to reduce checkpoint size:
- Default: 32 characters
- Partition columns: Never truncated
- Configurable via `spark.indextables.stats.truncation.maxLength`

### Partition Pruning

With Avro state format, partition bounds in ManifestInfo enable manifest-level pruning:

1. Reader evaluates partition predicates against ManifestInfo.partitionBounds
2. Entire manifests with non-matching bounds are skipped
3. Reduces I/O for large partitioned tables

---

## Concurrency Control

### Optimistic Concurrency

Writers use optimistic concurrency with atomic `ifNotExists` writes:

1. Read current version from transaction log
2. Prepare actions for new version
3. Attempt to write version file with `ifNotExists`
4. If file exists (conflict), retry with higher version

### Retry Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.transaction.retry.maxAttempts` | 10 | Maximum retry attempts |
| `spark.indextables.transaction.retry.baseDelayMs` | 100 | Base delay for exponential backoff |
| `spark.indextables.transaction.retry.maxDelayMs` | 5000 | Maximum backoff delay |

### State Write Concurrency

Avro state writes also use conditional writes:
- State directory existence check before writing
- `_manifest.json` written with `ifNotExists`
- On conflict, increment version and retry
- **Re-read base state on every retry**: Prevents stale manifest lists from concurrent writers

**Incremental Write with Retry:**

```scala
def writeIncrementalWithRetry(newFiles, removedPaths, ...): StateWriteResult = {
  while (attempt <= maxAttempts) {
    // CRITICAL: Re-read base state on EVERY retry to pick up concurrent changes
    val baseState = findLatestState()

    if (needsCompaction(baseState, removedPaths.size, config)) {
      tryWriteCompactedState(...)
    } else {
      // Reference existing manifests, only write new manifest for new files
      tryWriteIncrementalState(baseState, newFiles, removedPaths, ...)
    }

    // On conflict, retry with higher version
  }
}
```

---

## Retention and Cleanup

### Transaction Log Retention

Old version files are cleaned up after checkpoint creation:
- Default retention: 30 days
- Configurable via `spark.indextables.purge.txLogRetentionHours`

### Checkpoint Retention

Multiple checkpoints may exist temporarily:
- Only the latest checkpoint is used for reads
- Old checkpoints cleaned up by PURGE operations

### Avro State Compaction

State directories accumulate tombstones over time. Compaction triggers when:
- Tombstone ratio > 10% (`spark.indextables.state.compaction.tombstoneThreshold`)
- Manifest count > 20 (`spark.indextables.state.compaction.maxManifests`)
- Force compaction requested (`CompactionConfig.forceCompaction = true`)

Compaction rewrites state with tombstones applied and optimized manifest layout.

### Manifest Garbage Collection

With shared manifests, orphaned manifest files can accumulate when state directories
are deleted but their manifests are no longer referenced. The `ManifestGarbageCollector`
tracks reachable manifests and cleans up orphans:

**Collection Process:**
1. Scan retained state versions to build reachable manifest set
2. List all manifests in `manifests/` directory
3. Delete unreferenced manifests older than `minManifestAgeHours`

**Age-Based Protection:**
- Manifests younger than `minManifestAgeHours` (default: 1 hour) are never deleted
- Prevents race conditions with in-flight writes that reference new manifests

**Integration:**
- `PURGE INDEXTABLE` command invokes manifest GC after state directory cleanup
- Automatic GC during `PurgeOrphanedSplitsExecutor` execution

---

## Configuration Reference

### Transaction Log

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.checkpoint.enabled` | true | Enable checkpointing |
| `spark.indextables.checkpoint.interval` | 10 | Versions between checkpoints |
| `spark.indextables.transaction.compression.enabled` | true | GZIP compress version files |

### Avro State Format

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.format` | avro | `"avro"` or `"json"` |
| `spark.indextables.state.compression` | zstd | `"zstd"`, `"snappy"`, `"none"` |
| `spark.indextables.state.compressionLevel` | 3 | Compression level (1-22 for zstd) |
| `spark.indextables.state.entriesPerManifest` | 50000 | Max entries per Avro manifest |
| `spark.indextables.state.read.parallelism` | 8 | Parallel manifest reads |

### Schema Deduplication

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.schema.renormalizeThreshold` | 5 | Trigger renormalization above this unique ref count |

### Compaction

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.compaction.tombstoneThreshold` | 0.10 | Tombstone ratio to trigger compaction |
| `spark.indextables.state.compaction.maxManifests` | 20 | Manifest count to trigger compaction |
| `spark.indextables.state.compaction.largeRemoveThreshold` | MAX_INT | Large remove count (disabled by default) |
| `spark.indextables.state.compaction.afterMerge` | true | Compact after MERGE SPLITS |

### Retention

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.retention.versions` | 2 | State versions to retain |
| `spark.indextables.state.retention.hours` | 168 | Hours to retain old states |

### Manifest Garbage Collection

| Config | Default | Description |
|--------|---------|-------------|
| `spark.indextables.state.gc.minManifestAgeHours` | 1 | Don't delete manifests younger than this |

---

## Appendix A: JSON Schemas

### ProtocolAction

```json
{
  "type": "object",
  "properties": {
    "minReaderVersion": {"type": "integer"},
    "minWriterVersion": {"type": "integer"},
    "readerFeatures": {"type": "array", "items": {"type": "string"}},
    "writerFeatures": {"type": "array", "items": {"type": "string"}}
  },
  "required": ["minReaderVersion", "minWriterVersion"]
}
```

### MetadataAction

```json
{
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "description": {"type": "string"},
    "format": {
      "type": "object",
      "properties": {
        "provider": {"type": "string"},
        "options": {"type": "object"}
      }
    },
    "schemaString": {"type": "string"},
    "partitionColumns": {"type": "array", "items": {"type": "string"}},
    "configuration": {"type": "object"},
    "createdTime": {"type": "integer"}
  },
  "required": ["id", "format", "schemaString", "partitionColumns", "configuration"]
}
```

### AddAction

```json
{
  "type": "object",
  "properties": {
    "path": {"type": "string"},
    "partitionValues": {"type": "object"},
    "size": {"type": "integer"},
    "modificationTime": {"type": "integer"},
    "dataChange": {"type": "boolean"},
    "stats": {"type": "string"},
    "minValues": {"type": "object"},
    "maxValues": {"type": "object"},
    "numRecords": {"type": "integer"},
    "hasFooterOffsets": {"type": "boolean"},
    "footerStartOffset": {"type": "integer"},
    "footerEndOffset": {"type": "integer"},
    "splitTags": {"type": "array", "items": {"type": "string"}},
    "numMergeOps": {"type": "integer"},
    "docMappingRef": {"type": "string"},
    "docMappingJson": {"type": "string"},
    "uncompressedSizeBytes": {"type": "integer"}
  },
  "required": ["path", "partitionValues", "size", "modificationTime", "dataChange"]
}
```

---

## Appendix B: Avro Schema

### FileEntry

```avro
{
  "type": "record",
  "name": "FileEntry",
  "namespace": "io.indextables.spark.transaction.avro",
  "fields": [
    {"name": "path", "type": "string", "field-id": 100},
    {"name": "partitionValues", "type": {"type": "map", "values": "string"}, "field-id": 101},
    {"name": "size", "type": "long", "field-id": 102},
    {"name": "modificationTime", "type": "long", "field-id": 103},
    {"name": "dataChange", "type": "boolean", "field-id": 104},
    {"name": "stats", "type": ["null", "string"], "default": null, "field-id": 110},
    {"name": "minValues", "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 111},
    {"name": "maxValues", "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 112},
    {"name": "numRecords", "type": ["null", "long"], "default": null, "field-id": 113},
    {"name": "footerStartOffset", "type": ["null", "long"], "default": null, "field-id": 120},
    {"name": "footerEndOffset", "type": ["null", "long"], "default": null, "field-id": 121},
    {"name": "hasFooterOffsets", "type": "boolean", "default": false, "field-id": 122},
    {"name": "splitTags", "type": ["null", {"type": "array", "items": "string"}], "default": null, "field-id": 130},
    {"name": "numMergeOps", "type": ["null", "int"], "default": null, "field-id": 131},
    {"name": "docMappingRef", "type": ["null", "string"], "default": null, "field-id": 132},
    {"name": "uncompressedSizeBytes", "type": ["null", "long"], "default": null, "field-id": 133},
    {"name": "addedAtVersion", "type": "long", "field-id": 140},
    {"name": "addedAtTimestamp", "type": "long", "field-id": 141}
  ]
}
```

---

## Appendix C: Migration Guide

### Upgrading from V3 (JSON) to V4 (Avro)

1. Run `CHECKPOINT INDEXTABLES '<table_path>'`
2. The checkpoint command automatically:
   - Creates Avro state directory
   - Migrates schema registry
   - Updates `_last_checkpoint` with `format: "avro-state"`
3. Verify with `DESCRIBE INDEXTABLES STATE '<table_path>'`

### Downgrading (Emergency Only)

To force JSON checkpoint (not recommended):
```scala
spark.conf.set("spark.indextables.state.format", "json")
spark.sql("CHECKPOINT INDEXTABLES '<table_path>'")
```

Note: JSON format is deprecated and will be removed in a future release.
