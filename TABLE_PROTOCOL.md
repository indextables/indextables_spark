# IndexTables Table Protocol Specification

**Version:** 1.0
**Status:** Production
**Last Updated:** October 2025

## Overview

IndexTables implements a Delta Lake-inspired transaction log protocol for ACID guarantees, schema evolution, and time-travel capabilities. The protocol ensures atomic operations, data consistency, and protection against concurrent write conflicts.

## Design Principles

1. **Immutability**: Transaction log files are write-once, never modified
2. **Atomicity**: All operations are atomic at the file level
3. **Isolation**: Concurrent operations are detected and prevented
4. **Durability**: S3 Conditional Writes ensure no accidental overwrites
5. **Consistency**: Version numbering provides strict ordering

## Directory Structure

```
table_root/
├── _transaction_log/
│   ├── 00000000000000000000.json          # Version 0 (metadata + protocol)
│   ├── 00000000000000000001.json          # Version 1 (add files)
│   ├── 00000000000000000002.json          # Version 2 (more files)
│   ├── 00000000000000000010.checkpoint.json  # Checkpoint at version 10
│   ├── _last_checkpoint                    # Points to latest checkpoint
│   └── ...
├── part-00000-uuid.split                  # Data files
├── part-00001-uuid.split
└── ...
```

## File Naming Convention

### Version Files
- **Format**: `{version:020d}.json`
- **Example**: `00000000000000000042.json` (version 42)
- **Encoding**: 20-digit zero-padded decimal number
- **Content**: JSON Lines format (newline-delimited JSON objects)

### Checkpoint Files
- **Format**: `{version:020d}.checkpoint.json`
- **Example**: `00000000000000000050.checkpoint.json`
- **Content**: Consolidated state up to specified version

### Last Checkpoint Marker
- **File**: `_last_checkpoint`
- **Format**: JSON with checkpoint version and timestamp

## Transaction Log Format

### JSON Lines Structure

Each transaction file contains one JSON object per line:

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"uuid","format":{"provider":"indextables","options":{}},"schemaString":"...","partitionColumns":[],"configuration":{},"createdTime":1696000000000}}
{"add":{"path":"s3://bucket/table/part-00000.split","partitionValues":{},"size":1048576,"modificationTime":1696000000000,"dataChange":true,"stats":"{...}","tags":{}}}
{"remove":{"path":"s3://bucket/table/old-file.split","deletionTimestamp":1696000000001,"dataChange":true}}
{"mergeskip":{"path":"s3://bucket/corrupted.split","skipTimestamp":1696000000002,"reason":"Corrupted file","operation":"merge","retryAfter":1696086400000}}
```

## Action Types

### 1. ProtocolAction (Version 0 Only)

Defines protocol version compatibility. Follows Delta Lake's protocol versioning approach.

```scala
case class ProtocolAction(
  minReaderVersion: Int,                      // Minimum reader version required
  minWriterVersion: Int,                      // Minimum writer version required
  readerFeatures: Option[Set[String]] = None, // Optional reader feature names (version 3+)
  writerFeatures: Option[Set[String]] = None  // Optional writer feature names (version 3+)
)
```

**Current Protocol:**
- `minReaderVersion: 1` - Basic read operations
- `minWriterVersion: 2` - Supports batch writes and merge operations

**Readers and writers MUST:**
- Check protocol version before performing any operations
- Silently ignore unknown fields and actions
- Reject operations if protocol version is incompatible

**JSON Example (Basic):**
```json
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}
```

**JSON Example (With Features):**
```json
{
  "protocol": {
    "minReaderVersion": 3,
    "minWriterVersion": 3,
    "readerFeatures": ["footerOffsets", "tantivy4java"],
    "writerFeatures": ["conditionalWrites", "checkpoints"]
  }
}
```

### 2. MetadataAction (Version 0 Only)

Contains table schema, partition information, and configuration.

```scala
case class MetadataAction(
  id: String,                          // Unique table identifier (UUID)
  name: Option[String],                // Optional table name
  description: Option[String],         // Optional description
  format: FileFormat,                  // File format specification
  schemaString: String,                // JSON-encoded Spark schema
  partitionColumns: Seq[String],       // Partition column names
  configuration: Map[String, String],  // Table configuration
  createdTime: Option[Long]            // Creation timestamp (milliseconds)
)

case class FileFormat(
  provider: String,                    // "indextables"
  options: Map[String, String]         // Format-specific options
)
```

**JSON Example:**
```json
{
  "metaData": {
    "id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
    "name": null,
    "description": null,
    "format": {
      "provider": "indextables",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"content\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": ["year", "month"],
    "configuration": {},
    "createdTime": 1696000000000
  }
}
```

### 3. AddAction

Records addition of a data file to the table.

```scala
case class AddAction(
  // Core file metadata
  path: String,                                // File path (absolute)
  partitionValues: Map[String, String],        // Partition column values
  size: Long,                                  // File size in bytes
  modificationTime: Long,                      // File modification timestamp
  dataChange: Boolean,                         // Whether this changes user-visible data

  // Statistics and metadata
  stats: Option[String] = None,                // Optional JSON-encoded statistics
  tags: Option[Map[String, String]] = None,    // Optional metadata tags
  minValues: Option[Map[String, String]] = None,  // Minimum values per column
  maxValues: Option[Map[String, String]] = None,  // Maximum values per column
  numRecords: Option[Long] = None,             // Number of records in file

  // Footer offset optimization metadata for tantivy4java splits
  footerStartOffset: Option[Long] = None,      // Start offset of footer in split file
  footerEndOffset: Option[Long] = None,        // End offset of footer in split file
  hotcacheStartOffset: Option[Long] = None,    // Start offset of hot cache data
  hotcacheLength: Option[Long] = None,         // Length of hot cache data
  hasFooterOffsets: Boolean = false,           // Flag indicating if footer offsets are available

  // Complete tantivy4java SplitMetadata fields
  timeRangeStart: Option[String] = None,       // Time range start (ISO timestamp)
  timeRangeEnd: Option[String] = None,         // Time range end (ISO timestamp)
  splitTags: Option[Set[String]] = None,       // Tantivy4java split tags
  deleteOpstamp: Option[Long] = None,          // Delete operation stamp
  numMergeOps: Option[Int] = None,             // Number of merge operations
  docMappingJson: Option[String] = None,       // Document mapping as JSON string
  uncompressedSizeBytes: Option[Long] = None   // Uncompressed size of the split
)
```

**JSON Example (Basic):**
```json
{
  "add": {
    "path": "s3://bucket/table/part-00000-a1b2c3d4.split",
    "partitionValues": {"year": "2024", "month": "10"},
    "size": 1048576,
    "modificationTime": 1696000000000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":1000}}",
    "tags": {"source": "batch-job-123"},
    "numRecords": 1000
  }
}
```

**JSON Example (With Footer Offsets and Tantivy Metadata):**
```json
{
  "add": {
    "path": "s3://bucket/table/part-00000-a1b2c3d4.split",
    "partitionValues": {"year": "2024", "month": "10"},
    "size": 1048576,
    "modificationTime": 1696000000000,
    "dataChange": true,
    "numRecords": 1000,
    "minValues": {"id": "1", "timestamp": "2024-10-01T00:00:00Z"},
    "maxValues": {"id": "1000", "timestamp": "2024-10-01T23:59:59Z"},
    "footerStartOffset": 1040000,
    "footerEndOffset": 1048576,
    "hotcacheStartOffset": 0,
    "hotcacheLength": 8192,
    "hasFooterOffsets": true,
    "timeRangeStart": "2024-10-01T00:00:00Z",
    "timeRangeEnd": "2024-10-01T23:59:59Z",
    "splitTags": ["production", "batch-123"],
    "deleteOpstamp": 0,
    "numMergeOps": 0,
    "docMappingJson": "{\"fields\":[{\"name\":\"id\",\"type\":\"i64\"},{\"name\":\"content\",\"type\":\"text\"}]}",
    "uncompressedSizeBytes": 2097152
  }
}
```

### 4. RemoveAction

Records removal of a data file from the table.

```scala
case class RemoveAction(
  path: String,                                  // File path to remove
  deletionTimestamp: Option[Long],               // When file was logically deleted
  dataChange: Boolean,                           // Whether this changes user-visible data
  extendedFileMetadata: Option[Boolean],         // Reserved for future use
  partitionValues: Option[Map[String, String]],  // Partition values
  size: Option[Long],                            // File size in bytes
  tags: Option[Map[String, String]] = None       // Optional metadata tags
)
```

**JSON Example:**
```json
{
  "remove": {
    "path": "s3://bucket/table/old-file.split",
    "deletionTimestamp": 1696000001000,
    "dataChange": true,
    "extendedFileMetadata": null,
    "partitionValues": {"year": "2024", "month": "09"},
    "size": 524288,
    "tags": {"reason": "overwrite"}
  }
}
```

### 5. SkipAction

Records files skipped during merge operations with cooldown tracking.

```scala
case class SkipAction(
  path: String,                          // File path that was skipped
  skipTimestamp: Long,                   // When file was skipped
  reason: String,                        // Why it was skipped
  operation: String,                     // Operation that skipped it (e.g., "merge")
  partitionValues: Option[Map[String, String]], // Partition values
  size: Option[Long],                    // File size
  retryAfter: Option[Long],              // Timestamp when retry is allowed
  skipCount: Int                         // Number of times skipped
)
```

**JSON Example:**
```json
{
  "mergeskip": {
    "path": "s3://bucket/table/corrupted-file.split",
    "skipTimestamp": 1696000000000,
    "reason": "Failed to merge: corrupted split metadata",
    "operation": "merge",
    "partitionValues": {"year": "2024", "month": "10"},
    "size": 102400,
    "retryAfter": 1696086400000,
    "skipCount": 1
  }
}
```

## Version 0 Requirements

**Version 0 (`00000000000000000000.json`) MUST contain exactly:**
1. **One ProtocolAction** - Defines protocol compatibility
2. **One MetadataAction** - Defines table schema and configuration

**Critical Protection:**
- Version 0 is **immutable** and **protected by S3 Conditional Writes**
- Once written, version 0 **cannot be overwritten** - attempts will fail with `IllegalStateException`
- Loss of version 0 means **loss of table metadata** - the table becomes unreadable

## Write Operations

### Initialization (Version 0)

```scala
// Check if already initialized
if (cloudProvider.exists("_transaction_log/00000000000000000000.json")) {
  return // Already initialized
}

// Write protocol and metadata atomically
val actions = Seq(
  ProtocolAction(minReaderVersion = 1, minWriterVersion = 2),
  MetadataAction(/* ... */)
)

// Uses S3 Conditional Write (If-None-Match: *)
writeActions(version = 0, actions)
```

### Append Mode

```scala
// Get next version number atomically
val version = getNextVersion()  // e.g., 5

// Write new files
val addActions = Seq(
  AddAction(path = "s3://bucket/table/new-file-1.split", /* ... */),
  AddAction(path = "s3://bucket/table/new-file-2.split", /* ... */)
)

// Uses S3 Conditional Write to prevent conflicts
writeActions(version, addActions)
```

### Overwrite Mode

```scala
// Get all existing files
val existingFiles = listFiles()

// Create remove actions for all existing files
val removeActions = existingFiles.map { file =>
  RemoveAction(path = file.path, deletionTimestamp = now, /* ... */)
}

// Create add actions for new files
val addActions = /* new files */

// Get next version and write both removes and adds atomically
val version = getNextVersion()
writeActions(version, removeActions ++ addActions)
```

### Merge Operations

```scala
// Merge multiple small files into larger ones
val filesToMerge = selectFilesForMerge()

// Create merged file
val mergedFile = performMerge(filesToMerge)

// Atomic transaction: remove old files, add merged file
val version = getNextVersion()
val actions = filesToMerge.map(f => RemoveAction(f.path, /* ... */)) :+
              AddAction(mergedFile.path, /* ... */)

writeActions(version, actions)
```

## S3 Conditional Writes

**Critical Feature:** All transaction log writes use S3 Conditional Writes to prevent overwrites.

### Implementation

```scala
// S3 Provider uses If-None-Match: * header
val request = PutObjectRequest
  .builder()
  .bucket(bucket)
  .key(key)
  .ifNoneMatch("*")  // Only write if object doesn't exist
  .build()

try {
  s3Client.putObject(request, requestBody)
  return true  // Write succeeded
} catch {
  case ex: S3Exception if ex.statusCode() == 412 =>
    return false  // File already exists (412 Precondition Failed)
}
```

### Protection Guarantees

1. **No Overwrites**: Transaction files cannot be accidentally overwritten
2. **Concurrent Write Detection**: Multiple writers attempting same version will conflict
3. **Metadata Protection**: Version 0 is permanently protected
4. **Atomic Creates**: S3 handles atomicity server-side (no race conditions)

### Error Handling

```scala
val writeSucceeded = cloudProvider.writeFileIfNotExists(versionFilePath, content)

if (!writeSucceeded) {
  throw new IllegalStateException(
    s"Failed to write transaction log version $version - file already exists. " +
    "This indicates a concurrent write conflict or version counter synchronization issue."
  )
}
```

## Checkpoint System

### Purpose

Checkpoints consolidate transaction history to improve read performance:
- **Without checkpoints**: Must read all N transaction files (O(N))
- **With checkpoints**: Read 1 checkpoint + K incremental files (O(1) + O(K), where K << N)

### Checkpoint Creation

**Trigger Conditions:**
- Every N transactions (configurable, default: 10)
- Manual trigger via API

**Content:**
```json
{
  "protocol": { /* latest protocol */ },
  "metaData": { /* latest metadata */ },
  "add": [ /* all active files */ ]
}
```

**Process:**
1. Read all transactions from 0 to current version
2. Compute current state (apply all adds/removes)
3. Write consolidated state to `{version}.checkpoint.json`
4. Update `_last_checkpoint` marker
5. **Optionally** clean up old transaction files based on retention policy

### Checkpoint Reading

```scala
// 1. Check for checkpoint
val lastCheckpoint = readLastCheckpoint()

// 2. Load base state from checkpoint
val baseState = if (lastCheckpoint.isDefined) {
  readCheckpoint(lastCheckpoint.get.version)
} else {
  Seq.empty
}

// 3. Apply incremental changes since checkpoint
val incrementalVersions = getVersions().filter(_ > lastCheckpoint.get.version)
val incrementalActions = incrementalVersions.flatMap(readVersion)

// 4. Compute final state
val finalState = applyActions(baseState, incrementalActions)
```

### Performance Impact

**Measured Improvement:**
- **Before checkpoints**: ~1,300ms for 50 transactions (O(N))
- **After checkpoints**: ~500ms for 50 transactions (60% faster, 2.5x speedup)
- **Scalability**: Performance improvements increase with transaction count

## File Retention

### Configuration

```scala
// Transaction log retention (default: 30 days)
"spark.indextables.logRetention.duration" -> "2592000000"

// Checkpoint retention (default: 2 hours)
"spark.indextables.checkpointRetention.duration" -> "7200000"

// Cleanup policy (default: continue)
"spark.indextables.cleanup.failurePolicy" -> "continue"

// Dry run mode for testing
"spark.indextables.cleanup.dryRun" -> "false"
```

### Cleanup Logic

**Ultra-Conservative Deletion** - Files deleted ONLY when ALL conditions met:

```scala
if (fileAge > logRetentionDuration &&
    version < checkpointVersion &&
    version < currentVersion) {
  // Safe to delete - contents preserved in checkpoint
  deleteFile(versionFile)
}
```

**Safety Gates:**
1. ✅ File is older than retention period
2. ✅ File version is included in a checkpoint
3. ✅ File is not the current version being written
4. ✅ **Version 0 is NEVER deleted** (metadata protection)

### Cleanup Guarantees

- **Data Consistency**: All data remains accessible during and after cleanup
- **Checkpoint Redundancy**: Multiple checkpoint versions can coexist
- **Graceful Failures**: Cleanup failures don't affect operations (logged as warnings)
- **Version 0 Protection**: Metadata file never cleaned up

## Read Path

### Schema Resolution

```scala
// 1. Get latest version
val versions = getVersions()
val latestVersion = versions.max

// 2. Read version 0 for metadata
val metadataActions = readVersion(0)
val metadata = metadataActions.collectFirst {
  case m: MetadataAction => m
}.getOrElse(throw new IllegalStateException("No metadata found"))

// 3. Extract and deserialize schema
val schema = DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
```

### File Listing

```scala
// 1. Load checkpoint if available
val checkpoint = readLastCheckpoint()
val baseActions = checkpoint.map(c => readCheckpoint(c.version)).getOrElse(Seq.empty)

// 2. Load incremental changes
val startVersion = checkpoint.map(_.version + 1).getOrElse(0L)
val incrementalActions = getVersions()
  .filter(_ >= startVersion)
  .flatMap(readVersion)

// 3. Combine and compute active files
val allActions = baseActions ++ incrementalActions
val activeFiles = computeActiveFiles(allActions)
```

### Active File Computation

```scala
def computeActiveFiles(actions: Seq[Action]): Seq[AddAction] = {
  val addedFiles = mutable.Map[String, AddAction]()

  actions.foreach {
    case add: AddAction =>
      addedFiles(add.path) = add
    case remove: RemoveAction =>
      addedFiles.remove(remove.path)
    case _ => // Ignore protocol, metadata, skip actions
  }

  addedFiles.values.toSeq
}
```

## Concurrency Control

### Version Counter

```scala
// Atomic version counter for thread-safe version assignment
private val versionCounter = new AtomicLong(-1L)

def getNextVersion(): Long = {
  // Initialize from latest version if needed
  if (versionCounter.get() == -1L) {
    val latest = getLatestVersion()
    versionCounter.set(latest)
  }

  // Atomically increment and return
  versionCounter.incrementAndGet()
}
```

### Write Conflicts

**Scenario**: Two writers simultaneously try to write version 5

```
Writer A: getNextVersion() -> 5
Writer B: getNextVersion() -> 5
Writer A: writeActions(5, actions) -> SUCCESS (file created)
Writer B: writeActions(5, actions) -> FAILURE (412 Precondition Failed)
```

**Result**: Writer B fails with `IllegalStateException`, must retry with new version

### Isolation Levels

- **Read Committed**: Readers see only committed transactions
- **Write Serialization**: Writes are serialized via version counter + conditional writes
- **No Dirty Reads**: Uncommitted data is never visible
- **No Lost Updates**: Concurrent writes are detected and prevented

## Cache Invalidation

### Invalidation Triggers

```scala
// After every write operation
cache.foreach(_.invalidateVersionDependentCaches())

// After checkpoint creation
cache.foreach(_.invalidateAll())

// Manual invalidation API
transactionLog.invalidateCache()
```

### Cache Types

1. **Version Cache**: Stores parsed actions for each version
2. **File List Cache**: Stores computed active file list
3. **Metadata Cache**: Stores table schema and configuration
4. **Checkpoint Cache**: Stores checkpoint locations

### TTL Configuration

```scala
// Cache expiration (default: 5 minutes)
"spark.indextables.transaction.cache.expirationSeconds" -> "300"

// Disable caching (for debugging)
"spark.indextables.transaction.cache.enabled" -> "false"
```

## Partition Support

### Partition Metadata

Partition columns are stored in `MetadataAction`:

```json
{
  "metaData": {
    "partitionColumns": ["year", "month", "day"],
    ...
  }
}
```

### Partition Values in AddAction

```json
{
  "add": {
    "path": "s3://bucket/table/year=2024/month=10/day=15/part-00000.split",
    "partitionValues": {
      "year": "2024",
      "month": "10",
      "day": "15"
    },
    ...
  }
}
```

### Partition Pruning

```scala
// Filter files by partition values
val prunedFiles = activeFiles.filter { file =>
  file.partitionValues.get("year") == "2024" &&
  file.partitionValues.get("month") == "10"
}
```

## Example Workflows

### Create New Table

```
1. Write 00000000000000000000.json:
   - ProtocolAction (minReaderVersion=1, minWriterVersion=2)
   - MetadataAction (schema, partitions, config)

State: Empty table with schema defined
Files: []
```

### First Data Write (Append)

```
2. Write 00000000000000000001.json:
   - AddAction(path="file-1.split", size=1048576, numRecords=1000)
   - AddAction(path="file-2.split", size=1048576, numRecords=1000)

State: Table with 2 files
Files: [file-1.split, file-2.split]
```

### Second Write (Append)

```
3. Write 00000000000000000002.json:
   - AddAction(path="file-3.split", size=1048576, numRecords=1000)

State: Table with 3 files
Files: [file-1.split, file-2.split, file-3.split]
```

### Overwrite

```
4. Write 00000000000000000003.json:
   - RemoveAction(path="file-1.split")
   - RemoveAction(path="file-2.split")
   - RemoveAction(path="file-3.split")
   - AddAction(path="file-4.split", size=3145728, numRecords=3000)

State: Table with 1 file (overwrote previous data)
Files: [file-4.split]
```

### Merge Operation

```
5. Write 00000000000000000004.json:
   - AddAction(path="file-5.split", size=524288, numRecords=500)

6. Write 00000000000000000005.json:
   - AddAction(path="file-6.split", size=524288, numRecords=500)

State: Table with 3 small files
Files: [file-4.split, file-5.split, file-6.split]

7. MERGE SPLITS operation - Write 00000000000000000006.json:
   - RemoveAction(path="file-5.split")
   - RemoveAction(path="file-6.split")
   - AddAction(path="file-7-merged.split", size=1048576, numRecords=1000)

State: Table with 2 files (merged small files)
Files: [file-4.split, file-7-merged.split]
```

### Checkpoint Creation

```
8. After 10 transactions, create checkpoint:
   - Write 00000000000000000010.checkpoint.json
   - Update _last_checkpoint
   - Optionally clean up old transaction files

State: Same data, but faster reads via checkpoint
Files: [file-4.split, file-7-merged.split]
```

## Error Scenarios

### Corrupted Version 0

```
Error: "No transaction log found"
Cause: Version 0 file missing or corrupted
Impact: Table completely unreadable
Recovery: Must reconstruct metadata from split files or backups
Prevention: S3 Conditional Writes protect version 0 from overwrites
```

### Version Gap

```
Files: 00000.json, 00001.json, 00003.json (missing 00002.json)
Behavior: Stop reading at version 1, ignore version 3
Impact: Data from version 2+ is invisible
Recovery: Restore missing version file or accept data loss
```

### Concurrent Write Conflict

```
Error: "Failed to write transaction log version 5 - file already exists"
Cause: Two writers tried to write same version simultaneously
Impact: One writer succeeds, other fails
Recovery: Failed writer retries with new version number
```

### Checkpoint Read Failure

```
Error: Failed to read checkpoint at version 50
Behavior: Fall back to reading all individual transaction files
Impact: Slower read performance, but data remains accessible
Recovery: Recreate checkpoint or continue with degraded performance
```

## Migration and Upgrades

### Protocol Version Upgrades

```scala
// Automatic upgrade when new features are used
transactionLog.upgradeProtocol(
  newMinReaderVersion = 2,  // Requires readers to understand new features
  newMinWriterVersion = 3   // Requires writers to support new operations
)

// Writes new protocol version in next transaction
```

### Schema Evolution

**Not Currently Supported** - Schema is immutable after table creation.

**Future Enhancement:**
- Add `SchemaEvolutionAction` to track schema changes
- Support column additions, deletions, type changes
- Maintain backward compatibility with old data files

### Backward Compatibility

- **Version 0 Format**: Must remain stable forever (breaking changes require new table)
- **Action Types**: New action types can be added (old readers ignore unknown types)
- **Optional Fields**: New optional fields can be added to existing actions

## Best Practices

### For Table Writers

1. **Always check initialization**: Call `initialize()` is idempotent
2. **Use atomic operations**: Prefer batch writes over individual file commits
3. **Handle conflicts gracefully**: Retry with new version on conflict
4. **Set proper retention**: Balance storage costs vs recovery capabilities
5. **Enable checkpoints**: Dramatically improves read performance

### For Table Readers

1. **Cache aggressively**: Transaction log reads can be expensive
2. **Use checkpoints**: Always prefer checkpoint + incremental over full read
3. **Handle missing files**: Gracefully handle temporary S3 inconsistencies
4. **Validate schema**: Always check protocol compatibility before reading

### For Operations Teams

1. **Monitor version 0**: Alert if version 0 is ever missing or corrupted
2. **Set up retention policies**: Configure appropriate retention for your use case
3. **Enable cleanup**: Automatic cleanup prevents unbounded storage growth
4. **Test recovery**: Regularly verify you can recover from checkpoint
5. **Monitor checkpoint creation**: Ensure checkpoints are being created on schedule

## Performance Tuning

### Parallel I/O

```scala
// Configure parallel transaction log reading
"spark.indextables.checkpoint.parallelism" -> "8"  // Use 8 threads for I/O

// Configure read timeout
"spark.indextables.checkpoint.read.timeoutSeconds" -> "60"
```

### Checkpoint Frequency

```scala
// More frequent checkpoints = faster reads but more I/O
"spark.indextables.checkpoint.interval" -> "10"  // Checkpoint every 10 transactions

// Less frequent checkpoints = slower reads but less I/O
"spark.indextables.checkpoint.interval" -> "50"  // Checkpoint every 50 transactions
```

### Cache Tuning

```scala
// Longer cache TTL = fewer reads but stale data risk
"spark.indextables.transaction.cache.expirationSeconds" -> "600"  // 10 minutes

// Shorter cache TTL = fresher data but more I/O
"spark.indextables.transaction.cache.expirationSeconds" -> "60"  // 1 minute
```

## Comparison to Delta Lake

### Similarities

- JSON-based transaction log format
- Version numbering with zero-padding
- Checkpoint compaction system
- Add/Remove action semantics
- Protocol versioning
- Metadata in version 0

### Differences

| Feature | IndexTables | Delta Lake |
|---------|-------------|------------|
| **File Format** | Tantivy search index (.split) | Parquet (.parquet) |
| **Primary Use Case** | Full-text search | OLAP/Analytics |
| **Conditional Writes** | S3 If-None-Match (native) | S3 PutObject with conflict detection |
| **Version Protection** | All versions immutable via S3 | Optimistic concurrency control |
| **Schema Evolution** | Not yet supported | Full support |
| **Time Travel** | Via transaction log | Via transaction log + @ syntax |
| **SkipAction** | Built-in for merge resilience | Not present |
| **Merge Operations** | In-process via tantivy4java | Spark-based merge |

## Future Enhancements

### Planned

1. **Schema Evolution**: Support for schema changes over time
2. **Column Mapping**: Decouple physical column names from logical names
3. **Deletion Vectors**: Efficient row-level deletes without rewriting files
4. **Multi-Part Checkpoints**: Support for very large tables (millions of files)
5. **Incremental Checksums**: Faster checkpoint validation

### Under Consideration

1. **Time Travel Queries**: `SELECT * FROM table@v100` syntax
2. **VACUUM Command**: Physically delete removed files
3. **OPTIMIZE Command**: Automatic file compaction
4. **Change Data Feed**: Track row-level changes
5. **Liquid Clustering**: Automatic data organization

## Troubleshooting

### Table Unreadable

**Symptom**: "No transaction log found" error
**Diagnosis**:
```bash
# Check if version 0 exists
aws s3 ls s3://bucket/table/_transaction_log/00000000000000000000.json

# Check if _transaction_log directory exists
aws s3 ls s3://bucket/table/_transaction_log/
```

**Solutions**:
1. Restore version 0 from backup
2. Reconstruct metadata from existing split files
3. If unrecoverable, recreate table with same schema

### Slow Reads

**Symptom**: Reads taking multiple seconds
**Diagnosis**:
```scala
// Check number of transaction files
val versions = transactionLog.getVersions()
println(s"Transaction files: ${versions.length}")

// Check if checkpoints exist
val checkpoint = transactionLog.readLastCheckpoint()
println(s"Last checkpoint: ${checkpoint.map(_.version)}")
```

**Solutions**:
1. Create checkpoint manually
2. Increase checkpoint frequency
3. Enable parallel I/O
4. Increase cache TTL

### High S3 Costs

**Symptom**: Excessive S3 GET requests
**Diagnosis**: Monitor S3 access logs for transaction log reads
**Solutions**:
1. Increase cache TTL
2. Create checkpoints more frequently
3. Enable aggressive cleanup of old versions

## References

- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [AWS S3 Conditional Writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)
- [JSON Lines Format](https://jsonlines.org/)

## Changelog

### v1.0 (October 2025)
- Initial protocol specification
- S3 Conditional Writes implementation
- Version 0 immutability protection
- Checkpoint system with parallel I/O
- SkipAction for merge resilience
- Enhanced cache management
