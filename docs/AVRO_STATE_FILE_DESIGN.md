# Avro State File Design

## Overview

This document describes the design for a new Avro-based state file format that replaces JSON checkpoints. The new format provides:

1. **Fast reads** - Avro binary format instead of JSON (~10x faster)
2. **Streaming support** - Timestamps on all file entries
3. **Clean state** - Only live files, no remove history
4. **Manifest reuse** - Incremental writes without rewriting all entries
5. **Parallel reads** - Multiple manifest parts read concurrently
6. **Partition pruning** - Skip manifests based on partition bounds for gigantic tables

## Goals

| Goal | Current (JSON) | New (Avro) |
|------|----------------|------------|
| Read 70K files | ~14 seconds | <500ms |
| Write after adding 100 files | Rewrite all 70K | Write 100 entries |
| Write after merge (1000 removes) | Rewrite all 69K | Add 1000 tombstones |
| Streaming queries | Not supported | Full support |
| Cold read (no history) | Must parse all actions | Direct state read |
| Query 1 partition (1M file table) | Load all 1M entries | Load only matching manifests |

## File Structure

```
_transaction_log/
  # Version files (small JSON, unchanged - for streaming/incremental)
  00000000000000000001.json
  00000000000000000002.json
  ...

  # State directory (new Avro format)
  state-v00000000000000000100/
    _manifest.json              # State manifest (small JSON)
    manifest-a1b2c3d4.avro      # File entries (Avro)
    manifest-e5f6g7h8.avro      # File entries (Avro)
    ...

  # Legacy checkpoint (for backward compatibility)
  00000000000000000050.checkpoint.json

  # Pointer to latest state
  _last_checkpoint
```

## State Manifest Format

The `_manifest.json` file is small JSON that describes the state:

```json
{
  "formatVersion": 1,
  "stateVersion": 100,
  "createdAt": 1705123456789,
  "numFiles": 70500,
  "totalBytes": 1234567890,
  "manifests": [
    {
      "path": "manifest-a1b2c3d4.avro",
      "numEntries": 50000,
      "minAddedAtVersion": 1,
      "maxAddedAtVersion": 50,
      "partitionBounds": {
        "date": {"min": "2024-01-01", "max": "2024-01-15"},
        "region": {"min": "us-east", "max": "us-east"}
      }
    },
    {
      "path": "manifest-e5f6g7h8.avro",
      "numEntries": 20000,
      "minAddedAtVersion": 51,
      "maxAddedAtVersion": 99,
      "partitionBounds": {
        "date": {"min": "2024-01-16", "max": "2024-01-31"},
        "region": {"min": "us-east", "max": "us-west"}
      }
    },
    {
      "path": "manifest-i9j0k1l2.avro",
      "numEntries": 500,
      "minAddedAtVersion": 100,
      "maxAddedAtVersion": 100,
      "partitionBounds": {
        "date": {"min": "2024-02-01", "max": "2024-02-01"},
        "region": {"min": "eu-west", "max": "eu-west"}
      }
    }
  ],
  "tombstones": [
    "splits/part=2024-01-01/split-abc123.split",
    "splits/part=2024-01-01/split-def456.split"
  ],
  "schemaRegistry": {
    "_schema_abc123": "{\"fields\":[...]}",
    "_schema_def456": "{\"fields\":[...]}"
  },
  "protocolVersion": 4
}
```

### Manifest Entry Schema

Each `manifest-*.avro` file contains `FileEntry` records:

```json
{
  "type": "record",
  "name": "FileEntry",
  "namespace": "io.indextables.state",
  "doc": "A file entry in the state manifest",
  "fields": [
    {"name": "path", "type": "string", "field-id": 100, "doc": "Split file path"},
    {"name": "partitionValues", "type": {"type": "map", "values": "string"}, "field-id": 101},
    {"name": "size", "type": "long", "field-id": 102, "doc": "File size in bytes"},
    {"name": "modificationTime", "type": "long", "field-id": 103, "doc": "File modification timestamp"},
    {"name": "dataChange", "type": "boolean", "field-id": 104},

    {"name": "stats", "type": ["null", "string"], "default": null, "field-id": 110, "doc": "JSON statistics"},
    {"name": "minValues", "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 111},
    {"name": "maxValues", "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 112},
    {"name": "numRecords", "type": ["null", "long"], "default": null, "field-id": 113},

    {"name": "footerStartOffset", "type": ["null", "long"], "default": null, "field-id": 120},
    {"name": "footerEndOffset", "type": ["null", "long"], "default": null, "field-id": 121},
    {"name": "hasFooterOffsets", "type": "boolean", "default": false, "field-id": 124},

    {"name": "splitTags", "type": ["null", {"type": "array", "items": "string"}], "default": null, "field-id": 132},
    {"name": "numMergeOps", "type": ["null", "int"], "default": null, "field-id": 134},
    {"name": "docMappingRef", "type": ["null", "string"], "default": null, "field-id": 135},
    {"name": "uncompressedSizeBytes", "type": ["null", "long"], "default": null, "field-id": 136},

    {"name": "addedAtVersion", "type": "long", "field-id": 140, "doc": "Transaction version when file was added"},
    {"name": "addedAtTimestamp", "type": "long", "field-id": 141, "doc": "Timestamp when file was added (epoch ms)"}
  ]
}
```

### Field ID Ranges

| Range | Category | Fields |
|-------|----------|--------|
| 100-109 | Basic file info | path, partitionValues, size, modificationTime, dataChange |
| 110-119 | Statistics | stats, minValues, maxValues, numRecords |
| 120-129 | Footer offsets | footerStartOffset, footerEndOffset, hasFooterOffsets |
| 130-139 | Split metadata | splitTags, numMergeOps, docMappingRef, uncompressedSizeBytes |
| 140-149 | Streaming | addedAtVersion, addedAtTimestamp |
| 150-159 | Reserved | Future use |

## Read Path

### Reading State

```scala
def readState(tablePath: String, partitionFilter: Option[Expression] = None): StateSnapshot = {
  val lastCheckpoint = readLastCheckpoint()

  lastCheckpoint.format match {
    case "avro-state" =>
      readAvroState(lastCheckpoint.stateDir, partitionFilter)
    case "json" | "json-multipart" =>
      // Legacy: read JSON checkpoint and convert
      readLegacyCheckpoint(lastCheckpoint.version)
  }
}

def readAvroState(stateDir: String, partitionFilter: Option[Expression] = None): StateSnapshot = {
  // 1. Read manifest (small JSON)
  val manifest = readStateManifest(stateDir)
  val tombstoneSet = manifest.tombstones.toSet

  // 2. Prune manifests based on partition bounds (key optimization for gigantic tables)
  val relevantManifests = partitionFilter match {
    case Some(filter) =>
      manifest.manifests.filter { manifestInfo =>
        manifestInfo.partitionBounds match {
          case Some(bounds) => boundsOverlapFilter(bounds, filter)
          case None => true  // No bounds = must read (backward compatibility)
        }
      }
    case None =>
      manifest.manifests
  }

  log.debug(s"Partition pruning: ${manifest.manifests.size} manifests -> ${relevantManifests.size} after filter")

  // 3. Read only relevant Avro manifests in parallel
  val fileEntries = relevantManifests.par.flatMap { manifestInfo =>
    readAvroManifest(s"$stateDir/${manifestInfo.path}")
  }.filterNot(entry => tombstoneSet.contains(entry.path))

  StateSnapshot(
    version = manifest.stateVersion,
    files = fileEntries.toSeq,
    schemaRegistry = manifest.schemaRegistry
  )
}

/** Check if partition bounds overlap with a filter expression */
def boundsOverlapFilter(bounds: Map[String, PartitionBounds], filter: Expression): Boolean = {
  filter match {
    case EqualTo(attr, value) =>
      bounds.get(attr.name) match {
        case Some(b) => value >= b.min && value <= b.max
        case None => true  // Unknown column, can't prune
      }
    case GreaterThan(attr, value) =>
      bounds.get(attr.name).forall(b => b.max > value)
    case LessThan(attr, value) =>
      bounds.get(attr.name).forall(b => b.min < value)
    case In(attr, values) =>
      bounds.get(attr.name) match {
        case Some(b) => values.exists(v => v >= b.min && v <= b.max)
        case None => true
      }
    case And(left, right) =>
      boundsOverlapFilter(bounds, left) && boundsOverlapFilter(bounds, right)
    case Or(left, right) =>
      boundsOverlapFilter(bounds, left) || boundsOverlapFilter(bounds, right)
    case _ => true  // Unknown expression, can't prune
  }
}
```

### Streaming Reads (Changes Since Version)

```scala
def getChangesSince(tablePath: String, sinceVersion: Long): ChangeSet = {
  val state = readState(tablePath)

  // Filter to files added after sinceVersion
  val newFiles = state.files.filter(_.addedAtVersion > sinceVersion)

  // Read version files to get removes
  val versionFiles = listVersionsAfter(sinceVersion)
  val removes = versionFiles.flatMap(readVersionFile).collect {
    case r: RemoveAction => r
  }

  ChangeSet(
    adds = newFiles,
    removes = removes,
    newVersion = state.version
  )
}
```

## Write Path

### Writing New State (Incremental)

```scala
def writeState(
  tablePath: String,
  newFiles: Seq[FileEntry],
  removedPaths: Set[String],
  currentVersion: Long
): Unit = {
  val oldManifest = readStateManifest(currentStateDir)
  val newStateDir = s"state-v${formatVersion(currentVersion)}"

  // 1. Decide whether to compact or append
  val shouldCompact = needsCompaction(oldManifest, removedPaths.size)

  if (shouldCompact) {
    writeCompactedState(tablePath, newStateDir, currentVersion)
  } else {
    writeIncrementalState(tablePath, newStateDir, oldManifest, newFiles, removedPaths, currentVersion)
  }
}

def writeIncrementalState(
  tablePath: String,
  newStateDir: String,
  oldManifest: StateManifest,
  newFiles: Seq[FileEntry],
  removedPaths: Set[String],
  currentVersion: Long
): Unit = {
  // 1. Copy references to existing manifests
  val existingManifests = oldManifest.manifests

  // 2. Write new manifest with only new files (if any)
  val newManifests = if (newFiles.nonEmpty) {
    val manifestId = generateManifestId()
    val manifestPath = s"manifest-$manifestId.avro"
    writeAvroManifest(s"$newStateDir/$manifestPath", newFiles)

    existingManifests :+ ManifestInfo(
      path = manifestPath,
      numEntries = newFiles.size,
      minAddedAtVersion = newFiles.map(_.addedAtVersion).min,
      maxAddedAtVersion = newFiles.map(_.addedAtVersion).max
    )
  } else {
    existingManifests
  }

  // 3. Merge tombstones
  val newTombstones = oldManifest.tombstones ++ removedPaths

  // 4. Write new manifest
  val newManifest = StateManifest(
    formatVersion = 1,
    stateVersion = currentVersion,
    createdAt = System.currentTimeMillis(),
    numFiles = calculateLiveFileCount(newManifests, newTombstones),
    totalBytes = calculateTotalBytes(newManifests, newTombstones),
    manifests = newManifests,
    tombstones = newTombstones.toSeq,
    schemaRegistry = mergeSchemaRegistries(oldManifest.schemaRegistry, newSchemas),
    protocolVersion = 4
  )

  writeStateManifest(s"$newStateDir/_manifest.json", newManifest)

  // 5. Update _last_checkpoint
  updateLastCheckpoint(currentVersion, newStateDir, format = "avro-state")
}
```

### Writing Avro Manifest

```scala
def writeAvroManifest(path: String, entries: Seq[FileEntry]): Unit = {
  val schema = new Schema.Parser().parse(FILE_ENTRY_SCHEMA)
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)

  val outputStream = cloudProvider.createOutputStream(path)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

  // Use zstd compression for better ratio
  dataFileWriter.setCodec(CodecFactory.zstandardCodec(3))
  dataFileWriter.create(schema, outputStream)

  entries.foreach { entry =>
    dataFileWriter.append(toGenericRecord(entry, schema))
  }

  dataFileWriter.close()
}
```

## Compaction

### When to Compact

```scala
def needsCompaction(manifest: StateManifest, newRemoves: Int): Boolean = {
  val tombstoneRatio = manifest.tombstones.size.toDouble / manifest.numFiles
  val manifestCount = manifest.manifests.size

  // Compact when:
  // 1. Tombstones exceed 10% of live files
  tombstoneRatio > 0.10 ||
  // 2. Too many small manifests (fragmentation)
  manifestCount > 20 ||
  // 3. After a merge operation (lots of removes)
  newRemoves > 1000 ||
  // 4. Explicit compaction requested
  forceCompaction
}
```

### Compaction Process

```scala
def writeCompactedState(
  tablePath: String,
  newStateDir: String,
  currentVersion: Long
): Unit = {
  // 1. Read all live files (applies tombstones)
  val liveFiles = readState(tablePath).files

  // 2. Sort files by partition values for locality (key for partition pruning!)
  //    This ensures files from the same partition end up in the same manifest,
  //    maximizing the effectiveness of partition bounds filtering.
  val sortedFiles = liveFiles.sortBy { entry =>
    entry.partitionValues.toSeq.sorted.map(_._2).mkString("|")
  }

  // 3. Partition into manifest chunks (50K files each), preserving partition locality
  val manifestChunks = sortedFiles.grouped(50000).toSeq

  // 4. Write new manifests with partition bounds
  val newManifests = manifestChunks.zipWithIndex.map { case (chunk, idx) =>
    val manifestId = generateManifestId()
    val manifestPath = s"manifest-$manifestId.avro"
    writeAvroManifest(s"$newStateDir/$manifestPath", chunk)

    // Compute partition bounds for this manifest
    val partitionBounds = computePartitionBounds(chunk)

    ManifestInfo(
      path = manifestPath,
      numEntries = chunk.size,
      minAddedAtVersion = chunk.map(_.addedAtVersion).min,
      maxAddedAtVersion = chunk.map(_.addedAtVersion).max,
      partitionBounds = Some(partitionBounds)
    )
  }

  // 5. Write clean manifest (no tombstones)
  val newManifest = StateManifest(
    formatVersion = 1,
    stateVersion = currentVersion,
    createdAt = System.currentTimeMillis(),
    numFiles = liveFiles.size,
    totalBytes = liveFiles.map(_.size).sum,
    manifests = newManifests,
    tombstones = Seq.empty,  // Clean!
    schemaRegistry = buildSchemaRegistry(liveFiles),
    protocolVersion = 4
  )

  writeStateManifest(s"$newStateDir/_manifest.json", newManifest)
  updateLastCheckpoint(currentVersion, newStateDir, format = "avro-state")

  // 6. Schedule cleanup of old state directories (after retention)
  scheduleStateCleanup(tablePath, keepVersions = 2)
}

/** Compute min/max bounds for each partition column across a set of file entries */
def computePartitionBounds(entries: Seq[FileEntry]): Map[String, PartitionBounds] = {
  if (entries.isEmpty) return Map.empty

  // Get all partition columns from any entry
  val partitionColumns = entries.head.partitionValues.keys.toSet

  partitionColumns.map { col =>
    val values = entries.flatMap(_.partitionValues.get(col))
    if (values.isEmpty) {
      col -> PartitionBounds(min = None, max = None)  // All nulls
    } else {
      col -> PartitionBounds(min = Some(values.min), max = Some(values.max))
    }
  }.toMap
}

case class PartitionBounds(min: Option[String], max: Option[String])
```

### Partition Locality Strategy

For gigantic tables (millions of files), partition-aware compaction is critical:

| Strategy | Manifests to Read (1M files, 1K partitions, query 1 partition) |
|----------|---------------------------------------------------------------|
| Random ordering | ~100% of manifests (files scattered across all manifests) |
| Partition-sorted | ~0.1% of manifests (files clustered by partition) |

**Implementation notes:**

1. **Sort by partition values** before chunking to ensure locality
2. **Tight bounds** result from partition clustering - each manifest covers few partitions
3. **Incremental writes** may reduce locality over time, solved by periodic compaction
4. **Null partition values** use special bounds (min=null, max=null) and are never pruned

## Auto-Upgrade

### Detecting Format

```scala
case class LastCheckpointInfo(
  version: Long,
  size: Long,
  sizeInBytes: Long,
  numFiles: Long,
  createdTime: Long,
  // Existing fields for multi-part JSON
  parts: Option[Int],
  checkpointId: Option[String],
  // New fields for Avro state
  format: Option[String],        // "json", "json-multipart", "avro-state"
  stateDir: Option[String]       // e.g., "state-v00000000000000000100"
)
```

### Upgrade on Read (Lazy)

When reading a table with legacy JSON checkpoint:

```scala
def readState(tablePath: String): StateSnapshot = {
  val lastCheckpoint = readLastCheckpoint()

  lastCheckpoint.format.getOrElse("json") match {
    case "avro-state" =>
      readAvroState(lastCheckpoint.stateDir.get)

    case "json" | "json-multipart" =>
      // Read legacy format
      val actions = readLegacyCheckpoint(lastCheckpoint.version)

      // Convert to StateSnapshot (in-memory, no persistence)
      val liveFiles = actions.collect { case a: AddAction => a }
        .map(addActionToFileEntry)

      StateSnapshot(
        version = lastCheckpoint.version,
        files = liveFiles,
        schemaRegistry = extractSchemaRegistry(actions)
      )
  }
}
```

### Upgrade via CHECKPOINT SQL

The `CHECKPOINT INDEXTABLES` command upgrades to the new format:

```sql
-- Upgrade to Avro state format
CHECKPOINT INDEXTABLES 's3://bucket/table';
```

```scala
object CheckpointCommand {
  def run(tablePath: String): DataFrame = {
    val currentVersion = getLatestVersion(tablePath)
    val lastCheckpoint = readLastCheckpoint()

    // Read current state (works with any format)
    val currentState = readState(tablePath)

    // Always write in new Avro format
    val newStateDir = s"state-v${formatVersion(currentVersion)}"
    writeCompactedState(
      tablePath = tablePath,
      newStateDir = newStateDir,
      currentVersion = currentVersion,
      liveFiles = currentState.files
    )

    // Return status
    Seq(CheckpointResult(
      tablePath = tablePath,
      status = "SUCCESS",
      previousFormat = lastCheckpoint.format.getOrElse("json"),
      newFormat = "avro-state",
      version = currentVersion,
      numFiles = currentState.files.size
    )).toDF()
  }
}
```

### Upgrade on Write (Automatic)

New writes automatically use Avro format when checkpoint is created:

```scala
def maybeCreateCheckpoint(tablePath: String, currentVersion: Long): Unit = {
  if (shouldCreateCheckpoint(currentVersion)) {
    val currentState = readState(tablePath)

    // New files from this transaction
    val newFiles = getNewFilesFromVersion(currentVersion)
    val removedPaths = getRemovedPathsFromVersion(currentVersion)

    // Write in new Avro format
    writeState(tablePath, newFiles, removedPaths, currentVersion)
  }
}
```

## _last_checkpoint Format

Updated format to support both legacy and new formats:

```json
{
  "version": 100,
  "size": 70500,
  "sizeInBytes": 1234567890,
  "numFiles": 70500,
  "createdTime": 1705123456789,
  "format": "avro-state",
  "stateDir": "state-v00000000000000000100",
  "protocolVersion": 4
}
```

Legacy format (still supported for reading):

```json
{
  "version": 50,
  "size": 50000,
  "sizeInBytes": 987654321,
  "numFiles": 50000,
  "createdTime": 1704123456789,
  "parts": 3,
  "checkpointId": "abc123def456"
}
```

## Configuration

```scala
// State file configuration
spark.indextables.state.format: "avro"                    // "avro" or "json" (default: "avro")
spark.indextables.state.compression: "zstd"               // "zstd", "snappy", "none" (default: "zstd")
spark.indextables.state.compressionLevel: 3               // 1-22 for zstd (default: 3)
spark.indextables.state.entriesPerManifest: 50000         // Max entries per manifest file (default: 50000)

// Compaction configuration
spark.indextables.state.compaction.tombstoneThreshold: 0.10   // Compact when tombstones > 10% (default: 0.10)
spark.indextables.state.compaction.maxManifests: 20           // Compact when manifests > 20 (default: 20)
spark.indextables.state.compaction.afterMerge: true           // Auto-compact after merge (default: true)

// Parallel read configuration
spark.indextables.state.read.parallelism: 8               // Parallel manifest reads (default: 8)

// Retention configuration
spark.indextables.state.retention.versions: 2             // Keep N old state versions (default: 2)
spark.indextables.state.retention.hours: 168              // Keep states for N hours (default: 168 = 7 days)
```

## Migration Strategy

### Phase 1: Read Support (Backward Compatible)

1. Add Avro reader that can read new format
2. Update `readState()` to detect format and use appropriate reader
3. No changes to write path yet
4. All existing tables continue to work

### Phase 2: Write Support (Opt-in)

1. Add Avro writer for new state format
2. `CHECKPOINT INDEXTABLES` command writes in new format
3. Configuration option to enable Avro writes: `spark.indextables.state.format: "avro"`
4. Default remains JSON for safety

### Phase 3: Default Avro (After Validation)

1. Change default to Avro: `spark.indextables.state.format: "avro"`
2. New tables automatically use Avro
3. Existing tables upgrade on next checkpoint
4. JSON write support remains for rollback

### Phase 4: Deprecate JSON (Future)

1. Log warning when reading JSON checkpoints
2. Recommend running `CHECKPOINT INDEXTABLES` to upgrade
3. Eventually remove JSON write support (keep read support indefinitely)

## Backward Compatibility

### Reading Old Tables

Tables with JSON checkpoints continue to work:

1. `_last_checkpoint` without `format` field → assume "json"
2. `_last_checkpoint` with `checkpointId` → "json-multipart"
3. `_last_checkpoint` with `format: "avro-state"` → new format

### Downgrade Path

If issues are discovered with Avro format:

1. Set `spark.indextables.state.format: "json"`
2. Run `CHECKPOINT INDEXTABLES` to rewrite in JSON format
3. Or manually delete `state-v*` directories and rely on version file replay

### Mixed Clusters

During rolling upgrades:

1. Old code can only read JSON checkpoints
2. New code can read both JSON and Avro
3. Recommendation: Upgrade all nodes before enabling Avro writes

## Garbage Collection

### State Directory Cleanup

```scala
def cleanupOldStates(tablePath: String): Unit = {
  val retentionVersions = getConfig("state.retention.versions", 2)
  val retentionHours = getConfig("state.retention.hours", 168)
  val cutoffTime = System.currentTimeMillis() - (retentionHours * 3600 * 1000)

  val allStateDirs = listStateDirs(tablePath)
  val currentStateDir = readLastCheckpoint().stateDir

  allStateDirs
    .filterNot(_ == currentStateDir)
    .filter { dir =>
      val manifest = readStateManifest(dir)
      manifest.createdAt < cutoffTime
    }
    .drop(retentionVersions)  // Keep at least N versions
    .foreach(deleteStateDir)
}
```

### Integration with PURGE

The `PURGE INDEXTABLE` command cleans up:

1. Orphaned split files (existing behavior)
2. Old transaction log versions (existing behavior)
3. Old state directories (new)

```sql
PURGE INDEXTABLE 's3://bucket/table' OLDER THAN 7 DAYS;
-- Also cleans up old state-v* directories
```

## Observability

### DESCRIBE INDEXTABLES STATE

New SQL command to inspect state:

```sql
DESCRIBE INDEXTABLES STATE 's3://bucket/table';
```

Output:
```
+------------------+-------------------+
| property         | value             |
+------------------+-------------------+
| format           | avro-state        |
| version          | 100               |
| numFiles         | 70500             |
| totalBytes       | 1234567890        |
| numManifests     | 3                 |
| numTombstones    | 150               |
| tombstoneRatio   | 0.21%             |
| createdAt        | 2024-01-13 10:30  |
| protocolVersion  | 4                 |
+------------------+-------------------+
```

### Metrics

New metrics for monitoring:

- `indextables.state.read.duration_ms` - Time to read state
- `indextables.state.read.manifest_count` - Number of manifests read
- `indextables.state.read.file_count` - Number of file entries
- `indextables.state.write.duration_ms` - Time to write state
- `indextables.state.write.compacted` - Whether write triggered compaction
- `indextables.state.tombstone_count` - Current tombstone count
- `indextables.state.tombstone_ratio` - Tombstones / live files

## Appendix: Full Avro Schema

```json
{
  "type": "record",
  "name": "FileEntry",
  "namespace": "io.indextables.state",
  "doc": "A file entry representing a split in the table state",
  "fields": [
    {
      "name": "path",
      "type": "string",
      "field-id": 100,
      "doc": "Relative path to the split file"
    },
    {
      "name": "partitionValues",
      "type": {"type": "map", "values": "string"},
      "field-id": 101,
      "doc": "Partition column values as string map"
    },
    {
      "name": "size",
      "type": "long",
      "field-id": 102,
      "doc": "File size in bytes"
    },
    {
      "name": "modificationTime",
      "type": "long",
      "field-id": 103,
      "doc": "File modification time (epoch milliseconds)"
    },
    {
      "name": "dataChange",
      "type": "boolean",
      "field-id": 104,
      "doc": "Whether this file represents a data change"
    },
    {
      "name": "stats",
      "type": ["null", "string"],
      "default": null,
      "field-id": 110,
      "doc": "JSON-encoded statistics"
    },
    {
      "name": "minValues",
      "type": ["null", {"type": "map", "values": "string"}],
      "default": null,
      "field-id": 111,
      "doc": "Minimum values per column for data skipping"
    },
    {
      "name": "maxValues",
      "type": ["null", {"type": "map", "values": "string"}],
      "default": null,
      "field-id": 112,
      "doc": "Maximum values per column for data skipping"
    },
    {
      "name": "numRecords",
      "type": ["null", "long"],
      "default": null,
      "field-id": 113,
      "doc": "Number of records in the file"
    },
    {
      "name": "footerStartOffset",
      "type": ["null", "long"],
      "default": null,
      "field-id": 120,
      "doc": "Byte offset where footer/metadata begins"
    },
    {
      "name": "footerEndOffset",
      "type": ["null", "long"],
      "default": null,
      "field-id": 121,
      "doc": "Byte offset where footer/metadata ends"
    },
    {
      "name": "hasFooterOffsets",
      "type": "boolean",
      "default": false,
      "field-id": 124,
      "doc": "Whether footer offsets are populated"
    },
    {
      "name": "splitTags",
      "type": ["null", {"type": "array", "items": "string"}],
      "default": null,
      "field-id": 132,
      "doc": "Tags associated with this split"
    },
    {
      "name": "numMergeOps",
      "type": ["null", "int"],
      "default": null,
      "field-id": 134,
      "doc": "Number of merge operations this split has been through"
    },
    {
      "name": "docMappingRef",
      "type": ["null", "string"],
      "default": null,
      "field-id": 135,
      "doc": "Reference to doc mapping in schema registry"
    },
    {
      "name": "uncompressedSizeBytes",
      "type": ["null", "long"],
      "default": null,
      "field-id": 136,
      "doc": "Uncompressed size of the split data"
    },
    {
      "name": "addedAtVersion",
      "type": "long",
      "field-id": 140,
      "doc": "Transaction version when this file was added"
    },
    {
      "name": "addedAtTimestamp",
      "type": "long",
      "field-id": 141,
      "doc": "Timestamp when this file was added (epoch milliseconds)"
    }
  ]
}
```

## Test Plan

This section outlines comprehensive test coverage for the Avro state file implementation, organized by functional area with parity to Iceberg's manifest testing patterns.

### 1. Avro Manifest Reader Tests

**Reference:** Iceberg `TestManifestReader.java`

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testReadEmptyManifest` | Read manifest with zero entries | High |
| `testReadSingleEntry` | Read manifest with one file entry | High |
| `testReadMultipleEntries` | Read manifest with many (1000+) entries | High |
| `testReadAllFields` | Verify all 18 fields are correctly deserialized | High |
| `testReadNullableFields` | Handle null values for optional fields (stats, minValues, etc.) | High |
| `testReadWithPartitionValues` | Correctly parse partition value maps | High |
| `testReadWithEmptyPartitionValues` | Handle empty partition map (non-partitioned table) | Medium |
| `testReadLargeManifest` | Read manifest with 100K+ entries (performance) | High |
| `testReadCorruptedManifest` | Graceful error handling for corrupted Avro files | Medium |
| `testReadTruncatedManifest` | Handle incomplete/truncated manifest files | Medium |
| `testReadManifestFromS3` | Integration test reading from S3 | High |
| `testReadManifestFromAzure` | Integration test reading from Azure Blob | Medium |
| `testParallelManifestRead` | Read multiple manifests in parallel | High |
| `testFilterByAddedAtVersion` | Filter entries by `addedAtVersion > N` | High |
| `testFilterByAddedAtTimestamp` | Filter entries by `addedAtTimestamp > T` | High |
| `testFilterByPartition` | Skip manifests based on partition bounds | Medium |

### 2. Avro Manifest Writer Tests

**Reference:** Iceberg `TestManifestWriter.java`

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testWriteEmptyManifest` | Write manifest with zero entries | High |
| `testWriteSingleEntry` | Write and read back single entry | High |
| `testWriteMultipleEntries` | Write 1000+ entries | High |
| `testWriteAllFields` | Verify all fields survive round-trip | High |
| `testWriteNullableFields` | Correctly serialize null values | High |
| `testWriteWithZstdCompression` | Compression works correctly | High |
| `testWriteWithSnappyCompression` | Alternative compression codec | Low |
| `testWriteWithNoCompression` | Uncompressed manifest for debugging | Low |
| `testWriteToS3` | Integration test writing to S3 | High |
| `testWriteToAzure` | Integration test writing to Azure | Medium |
| `testWriteLargeManifest` | Write 100K+ entries (memory efficiency) | High |
| `testWriteStreamingAppend` | Streaming writes without buffering all entries | High |
| `testManifestFileSizeEstimation` | Accurate size estimation for splitting decisions | Medium |
| `testConcurrentWrites` | Multiple writers creating manifests simultaneously | Medium |

### 3. State Manifest Tests

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testReadStateManifest` | Parse `_manifest.json` correctly | High |
| `testWriteStateManifest` | Write valid `_manifest.json` | High |
| `testStateManifestWithTombstones` | Handle tombstone list | High |
| `testStateManifestWithSchemaRegistry` | Parse embedded schema registry | High |
| `testStateManifestVersionBounds` | Verify min/max version tracking per manifest | High |
| `testStateManifestNumFiles` | Accurate file count calculation | High |
| `testStateManifestTotalBytes` | Accurate byte count calculation | High |
| `testApplyTombstones` | Filter out tombstoned entries during read | High |
| `testEmptyTombstones` | Handle empty tombstone list | Medium |
| `testLargeTombstoneList` | Performance with 10K+ tombstones | Medium |

### 4. Incremental State Write Tests

**Reference:** Iceberg manifest reuse patterns

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testIncrementalAddFiles` | Add new files without rewriting existing manifests | High |
| `testIncrementalRemoveFiles` | Add tombstones without rewriting | High |
| `testIncrementalMixedAddRemove` | Combined add and remove in single transaction | High |
| `testManifestReuse` | New state references existing manifest files | High |
| `testManifestReuseWithPartialOverlap` | Some manifests reused, some new | Medium |
| `testStateVersionIncrement` | State version increments correctly | High |
| `testMultipleIncrementalWrites` | Chain of 10+ incremental writes | High |
| `testIncrementalWriteToS3` | Incremental writes to cloud storage | High |

### 5. Compaction Tests

**Reference:** Iceberg `TestRewriteManifests.java`, `TestRewriteManifestsAction.java`

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testCompactionTriggerByTombstoneRatio` | Compact when tombstones > 10% | High |
| `testCompactionTriggerByManifestCount` | Compact when manifests > 20 | High |
| `testCompactionTriggerAfterMerge` | Auto-compact after many removes | High |
| `testCompactionProducesCleanState` | No tombstones after compaction | High |
| `testCompactionPreservesAllFiles` | All live files present after compaction | High |
| `testCompactionSplitsLargeManifests` | Output respects `entriesPerManifest` limit | High |
| `testCompactionMergesSmallManifests` | Consolidate fragmented manifests | High |
| `testCompactionWithPartitionedData` | Maintain partition ordering | Medium |
| `testCompactionSkipsCleanState` | No-op when state is already clean | Medium |
| `testConcurrentCompaction` | Handle concurrent compaction attempts | Medium |
| `testCompactionPerformance70K` | Compact 70K files in < 2 seconds | High |
| `testCompactionMemoryEfficiency` | Memory usage during large compaction | Medium |
| `testCompactionPreservesPartitionLocality` | Files sorted by partition in output | High |
| `testCompactionComputesPartitionBounds` | Partition bounds present in new manifests | High |

### 6. Partition Pruning Tests

**Reference:** Iceberg manifest partition summary patterns

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testPartitionBoundsComputation` | Correct min/max for each partition column | High |
| `testPartitionBoundsWithNulls` | Handle null partition values correctly | High |
| `testPartitionBoundsMultipleColumns` | Bounds computed for all partition columns | High |
| `testPruneByEqualityFilter` | Skip manifests where value outside bounds | High |
| `testPruneByRangeFilter` | Skip manifests with non-overlapping ranges | High |
| `testPruneByInFilter` | Skip manifests when no IN values in bounds | High |
| `testPruneByAndFilter` | Combine multiple filter conditions | High |
| `testPruneByOrFilter` | Union of filter conditions | High |
| `testNoPruningWithoutBounds` | Read all manifests when bounds are null | High |
| `testNoPruningForUnknownColumn` | Read all manifests for non-partition columns | Medium |
| `testPartitionPruningEffectiveness` | Verify actual manifest skip rate | High |
| `testPartitionPruningPerformance1M` | Query 1 partition from 1M files | High |
| `testPartitionLocalitySorting` | Verify compaction sorts by partition | High |
| `testIncrementalWritePartitionBounds` | New manifests have correct bounds | High |
| `testMixedBoundsAndNoBounds` | Handle manifests with and without bounds | Medium |
| `testPartitionPruningWithTombstones` | Pruning works with active tombstones | Medium |

### 7. Migration and Upgrade Tests

**Reference:** Iceberg version upgrade patterns

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testReadLegacyJsonCheckpoint` | Read existing JSON checkpoint | High |
| `testAutoUpgradeOnRead` | Legacy checkpoint read produces StateSnapshot | High |
| `testCheckpointSqlUpgrade` | `CHECKPOINT INDEXTABLES` converts to Avro | High |
| `testUpgradePreservesAllFiles` | No data loss during upgrade | High |
| `testUpgradePreservesPartitionValues` | Partition values maintained | High |
| `testUpgradePreservesStatistics` | Min/max/stats preserved | High |
| `testUpgradePreservesSchemaRegistry` | Doc mapping refs maintained | High |
| `testWriteAfterUpgrade` | New writes use Avro format | High |
| `testMixedFormatRollback` | Downgrade path to JSON | Medium |
| `testUpgradeFromV2ToV4` | Protocol version upgrade | High |
| `testUpgradeLargeTable` | Upgrade 70K files efficiently | High |
| `testUpgradeWithActiveTombstones` | Handle pending removes during upgrade | Medium |

### 8. Streaming Support Tests

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testGetChangesSinceVersion` | Retrieve files added after version N | High |
| `testGetChangesSinceTimestamp` | Retrieve files added after timestamp T | High |
| `testChangeSetIncludesRemoves` | Remove actions from version files | High |
| `testStreamingReadEmptyDelta` | No changes since last version | High |
| `testStreamingReadAllChanges` | Get all changes from version 0 | High |
| `testManifestVersionFiltering` | Skip manifests based on version bounds | High |
| `testStreamingWithCompaction` | Streaming works after compaction | Medium |
| `testStreamingPerformance` | Fast delta computation for large tables | High |

### 9. Schema Evolution Tests

**Reference:** Iceberg field ID patterns

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testAddNewField` | Add field with new field-id | High |
| `testFieldIdPreservation` | Existing field-ids unchanged | High |
| `testReadOldSchemaWithNewReader` | Forward compatibility | High |
| `testReadNewSchemaWithOldReader` | Backward compatibility (ignore unknown fields) | High |
| `testRenameField` | Field renamed but field-id preserved | Medium |
| `testPromoteFieldType` | Type widening (int → long) | Low |

### 10. Spark Integration Tests

**Reference:** Iceberg `TestManifestFileSerialization.java`, `TestRewriteManifestsAction.java`

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testFileEntryKryoSerialization` | FileEntry survives Kryo round-trip | High |
| `testFileEntryJavaSerialization` | FileEntry survives Java serialization | High |
| `testStateSnapshotSerialization` | Full StateSnapshot serializable | High |
| `testDistributedStateRead` | Read state across executors | High |
| `testDistributedCompaction` | Compaction with Spark parallelism | Medium |
| `testCheckpointSqlParsing` | SQL grammar for CHECKPOINT command | High |
| `testCheckpointSqlExecution` | End-to-end CHECKPOINT execution | High |
| `testDescribeStateCommand` | DESCRIBE INDEXTABLES STATE | High |
| `testReadWithSparkDataSource` | DataSource V2 reads Avro state | High |
| `testWriteWithSparkDataSource` | DataSource V2 writes Avro state | High |

### 11. Error Handling Tests

**Reference:** Iceberg `CommitStateUnknownException` patterns

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testReadMissingManifest` | Graceful error for missing manifest file | High |
| `testReadMissingStateDir` | Handle missing state directory | High |
| `testPartialManifestWrite` | Recovery from interrupted write | Medium |
| `testConcurrentStateUpdate` | Conflict detection and retry | High |
| `testCommitStateUnknown` | Handle unknown commit state | Medium |
| `testCorruptedAvroHeader` | Detect and report Avro corruption | Medium |
| `testInvalidSchemaVersion` | Handle unknown schema versions | Medium |
| `testStorageException` | Retry on transient storage errors | High |

### 12. Performance Tests

| Test Name | Description | Target |
|-----------|-------------|--------|
| `testRead70KFilesLatency` | Read 70K files from Avro state | < 500ms |
| `testRead100KFilesLatency` | Read 100K files | < 700ms |
| `testIncrementalWrite100FilesLatency` | Add 100 files incrementally | < 100ms |
| `testCompaction70KFilesLatency` | Full compaction of 70K files | < 2s |
| `testParallelManifestReadThroughput` | Read 10 manifests in parallel | < 200ms |
| `testMemoryUsageDuringRead` | Peak memory for 100K file state | < 500MB |
| `testMemoryUsageDuringCompaction` | Peak memory during compaction | < 1GB |
| `testS3ReadLatency` | State read from S3 | < 2s |
| `testPartitionPruning1MFiles` | Query 1 partition from 1M file table | < 100ms |
| `testPartitionPruningMemory1M` | Memory for 1 partition query (1M files) | < 50MB |

### 13. Garbage Collection Tests

**Reference:** Iceberg `TestManifestCleanup.java`

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testCleanupOldStateDirs` | Remove state dirs past retention | High |
| `testRetentionByVersionCount` | Keep N most recent state versions | High |
| `testRetentionByAge` | Keep states within retention window | High |
| `testCleanupPreservesCurrentState` | Never delete active state dir | High |
| `testCleanupWithPurgeIntegration` | PURGE INDEXTABLE cleans state dirs | High |
| `testCleanupOrphanedManifests` | Remove unreferenced manifest files | Medium |
| `testConcurrentCleanup` | Handle concurrent cleanup operations | Medium |

### 14. Cloud Storage Integration Tests

| Test Name | Description | Priority |
|-----------|-------------|----------|
| `testS3FullLifecycle` | Write, read, compact, cleanup on S3 | High |
| `testAzureFullLifecycle` | Write, read, compact, cleanup on Azure | Medium |
| `testS3EventualConsistency` | Handle S3 consistency model | Medium |
| `testS3MultipartUpload` | Large manifest uses multipart upload | Low |
| `testCrossRegionRead` | Read state from different region | Low |

### Test Implementation Notes

1. **Test Fixtures**: Create reusable test fixtures for common scenarios (empty table, partitioned table, large table with 70K files, table with statistics).

2. **Property-Based Testing**: Use ScalaCheck for property-based tests on serialization round-trips and field preservation.

3. **Performance Baselines**: Establish performance baselines and fail tests if latency exceeds targets.

4. **Parameterized Tests**: Use parameterized tests for:
   - Different compression codecs
   - Different storage backends (local, S3, Azure)
   - Different table sizes (100, 1K, 10K, 70K, 100K files)

5. **Integration Test Tags**: Tag integration tests that require cloud credentials for conditional execution:
   ```scala
   @Tag("s3-integration")
   @Tag("azure-integration")
   ```

6. **Test Data Generators**: Create generators for:
   - Random FileEntry with all fields populated
   - Realistic partition value distributions
   - Statistics with min/max bounds

### Test Coverage Matrix

| Category | Unit Tests | Integration Tests | Performance Tests |
|----------|------------|-------------------|-------------------|
| Reader | 16 | 2 | 2 |
| Writer | 14 | 2 | 2 |
| State Manifest | 10 | 0 | 1 |
| Incremental Write | 8 | 1 | 1 |
| Compaction | 14 | 1 | 2 |
| Partition Pruning | 14 | 0 | 2 |
| Migration | 12 | 1 | 1 |
| Streaming | 8 | 0 | 1 |
| Schema Evolution | 6 | 0 | 0 |
| Spark Integration | 10 | 2 | 0 |
| Error Handling | 8 | 2 | 0 |
| GC/Cleanup | 7 | 1 | 0 |
| Cloud Storage | 0 | 5 | 0 |
| **Total** | **127** | **17** | **12** |

### Iceberg Parity Checklist

| Iceberg Test File | Corresponding Tests | Status |
|-------------------|---------------------|--------|
| `TestManifestReader.java` | §1 Avro Manifest Reader | ✓ |
| `TestManifestWriter.java` | §2 Avro Manifest Writer | ✓ |
| `TestManifestWriterVersions.java` | §9 Schema Evolution | ✓ |
| `TestManifestCaching.java` | N/A (caching at higher layer) | - |
| `TestManifestCleanup.java` | §13 Garbage Collection | ✓ |
| `TestRewriteManifests.java` | §5 Compaction, §6 Partition Pruning | ✓ |
| `TestRewriteManifestsAction.java` | §5 Compaction, §6 Partition Pruning, §10 Spark Integration | ✓ |
| `TestManifestFileSerialization.java` | §10 Spark Integration (serialization) | ✓ |
| `TestRewriteManifestsProcedure.java` | §10 Spark Integration (SQL commands) | ✓ |
