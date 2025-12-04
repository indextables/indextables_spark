# Section 3: Transaction Log System

**Document Version:** 1.0
**Last Updated:** 2025-10-06
**Target Audience:** Developers familiar with Apache Spark but not necessarily with Spark internals

---

## Table of Contents

1. [Transaction Log Architecture](#31-transaction-log-architecture)
2. [Checkpoint System](#32-checkpoint-system)
3. [Checkpoint Configuration & Behavior](#33-checkpoint-configuration--behavior)
4. [Transaction Log Cache](#34-transaction-log-cache)
5. [Optimized Transaction Log](#35-optimized-transaction-log)
6. [Skipped Files Management](#36-skipped-files-management)
7. [Transaction Log Compression](#37-transaction-log-compression)

---

## 3.1 Transaction Log Architecture

### 3.1.1 Overview

The **Transaction Log** is IndexTables4Spark's implementation of ACID transactional semantics, inspired by Delta Lake. It provides a durable, consistent record of all table operations stored in the `_transaction_log/` directory.

**Key Responsibilities:**
- Track all split files (additions and removals)
- Store table schema and metadata
- Maintain protocol version for compatibility
- Provide ACID transaction guarantees
- Enable time-travel and audit capabilities

**Directory Structure:**

```
s3://bucket/my-table/
├── _transaction_log/
│   ├── 00000000000000000000.json       # Version 0: Protocol + Metadata
│   ├── 00000000000000000001.json       # Version 1: ADD actions (first write)
│   ├── 00000000000000000002.json       # Version 2: ADD actions (append)
│   ├── 00000000000000000010.checkpoint.json  # Checkpoint at version 10
│   ├── 00000000000000000011.json       # Version 11: Incremental changes
│   └── _last_checkpoint                # Pointer to latest checkpoint
└── partition=2024-01-01/
    └── *.split                          # Actual split files
```

### 3.1.2 Delta Lake Compatibility

IndexTables4Spark's transaction log is **Delta Lake-compatible** where appropriate:

| Feature | Delta Lake | IndexTables4Spark | Compatibility |
|---------|-----------|-------------------|---------------|
| **File format** | JSON lines | JSON lines | ✅ 100% |
| **Version numbering** | Zero-padded | Zero-padded (20 digits) | ✅ 100% |
| **Action types** | ADD/REMOVE/METADATA | ADD/REMOVE/METADATA/PROTOCOL/SKIP | ⚠️ Partial (extra SKIP action) |
| **Checkpoint format** | Parquet/JSON | JSON only | ⚠️ Partial |
| **ACID semantics** | Full ACID | Full ACID | ✅ 100% |
| **Protocol versioning** | Yes | Yes | ✅ 100% |

**Why Delta Lake Compatibility?**
1. **Familiarity**: Users already know Delta Lake patterns
2. **Tooling**: Potential future interoperability with Delta tools
3. **Best practices**: Proven transactional semantics
4. **Documentation**: Well-documented patterns

### 3.1.3 ACID Guarantees

IndexTables4Spark provides full ACID guarantees:

#### Atomicity

All changes in a transaction commit together or not at all:

```scala
// Example: Overwrite operation
def overwriteFiles(addActions: Seq[AddAction]): Long = {
  val existingFiles = listFiles()
  val removeActions = existingFiles.map(toRemoveAction)

  val version = getNextVersion()

  // ATOMIC: Both REMOVE and ADD in single transaction
  writeActions(version, removeActions ++ addActions)

  version
}
```

**Implementation:**
- Single transaction file per version
- Conditional PUT prevents overwrites
- Version counter ensures sequential ordering

#### Consistency

Schema validation and protocol versioning ensure valid table states:

```scala
def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
  // Validate partition columns exist in schema
  val schemaFields = schema.fieldNames.toSet
  val invalidCols = partitionColumns.filterNot(schemaFields.contains)

  if (invalidCols.nonEmpty) {
    throw new IllegalArgumentException(
      s"Partition columns ${invalidCols.mkString(", ")} not found in schema"
    )
  }

  // Write protocol first, then metadata
  writeActions(0, Seq(
    ProtocolVersion.defaultProtocol(),
    MetadataAction(...)
  ))
}
```

**Validation Points:**
- Schema validation during table initialization
- Protocol version checking on read/write
- Partition column existence verification
- Configuration value validation

#### Isolation

Optimistic concurrency with version counters:

```scala
private val versionCounter = new AtomicLong(-1L)

private def getNextVersion(): Long = {
  // Atomically increment version
  versionCounter.incrementAndGet()
}

private def writeActions(version: Long, actions: Seq[Action]): Unit = {
  val versionFile = new Path(transactionLogPath, f"$version%020d.json")

  // CRITICAL: Conditional write prevents overwrites
  val writeSucceeded = cloudProvider.writeFileIfNotExists(
    versionFile.toString,
    content.getBytes("UTF-8")
  )

  if (!writeSucceeded) {
    throw new IllegalStateException(
      s"Failed to write version $version - file already exists. " +
      "Concurrent write conflict detected."
    )
  }
}
```

**Concurrency Control:**
- Atomic version counter
- Conditional PUT (write-if-not-exists)
- Conflict detection via version collision
- No locks required (optimistic concurrency)

#### Durability

Writes to durable cloud storage with immutability:

```scala
// Transaction log files are NEVER overwritten
// writeFileIfNotExists ensures immutability
cloudProvider.writeFileIfNotExists(path, content)

// Files remain available even after cleanup
// Cleanup only removes files safely included in checkpoints
```

**Durability Features:**
- Cloud storage replication (S3, Azure Blob, GCS)
- Immutable files (never modified after creation)
- Checkpoint redundancy
- Retention policies prevent premature deletion

### 3.1.4 Version Management

**Atomic Version Counter:**

```scala
private val versionCounter = new AtomicLong(-1L)

// Initialize from existing transactions
private def getLatestVersion(): Long = {
  val versions = getVersions()
  val latest = if (versions.nonEmpty) versions.max else -1L

  // Update counter to max of (current, latest from disk)
  versionCounter.updateAndGet(current => math.max(current, latest))
  latest
}

// Thread-safe version assignment
private def getNextVersion(): Long = {
  if (versionCounter.get() == -1L) {
    getLatestVersion()  // Initialize on first use
  }
  versionCounter.incrementAndGet()
}
```

**Version Sequence:**

```
Version 0: ProtocolAction + MetadataAction (table initialization)
Version 1: AddAction (first write)
Version 2: AddAction (append)
Version 3: RemoveAction + AddAction (merge splits)
Version 4: AddAction (append)
...
Version 10: Checkpoint created
Version 11: AddAction (incremental change after checkpoint)
```

### 3.1.5 Action Types

IndexTables4Spark supports five action types:

#### ProtocolAction

Defines minimum reader/writer versions for compatibility:

```scala
case class ProtocolAction(
  minReaderVersion: Int,
  minWriterVersion: Int,
  readerFeatures: Option[Set[String]] = None,  // For version 3+
  writerFeatures: Option[Set[String]] = None   // For version 3+
) extends Action
```

**Example:**

```json
{
  "protocol": {
    "minReaderVersion": 2,
    "minWriterVersion": 2
  }
}
```

**Usage:**
- Written in version 0 during table initialization
- Updated when table is upgraded to new protocol
- Checked before every read/write operation
- Prevents incompatible clients from corrupting data

**Version History:**

| Version | Features | Status |
|---------|----------|--------|
| **v1** | Legacy (no protocol action) | Deprecated |
| **v2** | Protocol versioning, checkpoints | Current default |
| **v3** | Feature flags (future) | Planned |

#### MetadataAction

Stores table schema, partition columns, and configuration:

```scala
case class MetadataAction(
  id: String,                           // Unique table ID
  name: Option[String],                 // Optional table name
  description: Option[String],          // Optional description
  format: FileFormat,                   // Format provider info
  schemaString: String,                 // JSON schema
  partitionColumns: Seq[String],        // Partition column names
  configuration: Map[String, String],   // Table properties
  createdTime: Option[Long]             // Creation timestamp
) extends Action
```

**Example:**

```json
{
  "metaData": {
    "id": "a8b3c4d5-e6f7-g8h9-i0j1-k2l3m4n5o6p7",
    "name": null,
    "description": null,
    "format": {
      "provider": "indextables",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"message\",\"type\":\"string\",...}]}",
    "partitionColumns": ["date", "hour"],
    "configuration": {},
    "createdTime": 1704067200000
  }
}
```

**Usage:**
- Written in version 0 during table initialization
- Can be updated (new MetadataAction) for schema evolution
- Read by `getSchema()` and `getPartitionColumns()`
- Cached for performance

#### AddAction

Records split file additions with comprehensive metadata:

```scala
case class AddAction(
  path: String,                         // Relative path to split file
  partitionValues: Map[String, String], // Partition column values
  size: Long,                           // File size in bytes
  modificationTime: Long,               // Last modified timestamp
  dataChange: Boolean,                  // Whether this changes data
  stats: Option[String] = None,         // Optional statistics JSON
  tags: Option[Map[String, String]] = None,

  // Min/max values for data skipping
  minValues: Option[Map[String, String]] = None,
  maxValues: Option[Map[String, String]] = None,
  numRecords: Option[Long] = None,

  // Footer offset optimization (tantivy4java)
  footerStartOffset: Option[Long] = None,
  footerEndOffset: Option[Long] = None,
  hotcacheStartOffset: Option[Long] = None,
  hotcacheLength: Option[Long] = None,
  hasFooterOffsets: Boolean = false,

  // Complete tantivy4java SplitMetadata fields
  timeRangeStart: Option[String] = None,
  timeRangeEnd: Option[String] = None,
  splitTags: Option[Set[String]] = None,
  deleteOpstamp: Option[Long] = None,
  numMergeOps: Option[Int] = None,
  docMappingJson: Option[String] = None,
  uncompressedSizeBytes: Option[Long] = None
) extends Action
```

**Example:**

```json
{
  "add": {
    "path": "date=2024-01-01/hour=10/3f2504e0-4f89-11d3-9a0c-0305e82c3301.split",
    "partitionValues": {"date": "2024-01-01", "hour": "10"},
    "size": 104857600,
    "modificationTime": 1704067200000,
    "dataChange": true,
    "minValues": {"timestamp": "1704067200", "level": "DEBUG"},
    "maxValues": {"timestamp": "1704070800", "level": "ERROR"},
    "numRecords": 100000,
    "footerStartOffset": 104857000,
    "footerEndOffset": 104857600,
    "hasFooterOffsets": true
  }
}
```

**Key Features:**

| Field | Purpose | Usage |
|-------|---------|-------|
| `path` | Split file location | File retrieval |
| `partitionValues` | Partition column values | Partition pruning |
| `minValues`/`maxValues` | Value ranges | Data skipping |
| `numRecords` | Row count | Statistics, auto-sizing |
| `footerStartOffset`/`footerEndOffset` | Fast metadata access | Optimized split reading |
| `size` | File size | Statistics, planning |

#### RemoveAction

Marks split files as removed (tombstone pattern):

```scala
case class RemoveAction(
  path: String,                              // Path to removed file
  deletionTimestamp: Option[Long],           // When removed
  dataChange: Boolean,                       // Whether this changes data
  extendedFileMetadata: Option[Boolean],
  partitionValues: Option[Map[String, String]],
  size: Option[Long],
  tags: Option[Map[String, String]] = None
) extends Action
```

**Example:**

```json
{
  "remove": {
    "path": "date=2024-01-01/hour=10/old-split.split",
    "deletionTimestamp": 1704070800000,
    "dataChange": true,
    "partitionValues": {"date": "2024-01-01", "hour": "10"},
    "size": 52428800
  }
}
```

**Usage:**
- **Overwrite operations**: Remove all existing files, add new files
- **Merge splits**: Remove small splits, add merged split
- **Time travel** (future): Reconstruct table state at specific version
- **Vacuum** (future): Physically delete files after retention period

**Important:** RemoveAction is a **logical deletion** (tombstone). Physical files remain until explicitly vacuumed.

#### SkipAction

Tracks corrupted or problematic files with cooldown mechanism:

```scala
case class SkipAction(
  path: String,                              // Path to skipped file
  skipTimestamp: Long,                       // When skipped
  reason: String,                            // Why skipped (exception message)
  operation: String,                         // "merge", "read", etc.
  partitionValues: Option[Map[String, String]] = None,
  size: Option[Long] = None,
  retryAfter: Option[Long] = None,          // Cooldown expiration timestamp
  skipCount: Int = 1                         // Number of times skipped
) extends Action
```

**Example:**

```json
{
  "mergeskip": {
    "path": "date=2024-01-01/hour=10/corrupted.split",
    "skipTimestamp": 1704067200000,
    "reason": "Failed to merge split: Invalid footer offset",
    "operation": "merge",
    "partitionValues": {"date": "2024-01-01", "hour": "10"},
    "size": 104857600,
    "retryAfter": 1704153600000,
    "skipCount": 2
  }
}
```

**Usage:**
- **Merge operations**: Skip corrupted files without failing entire job
- **Cooldown tracking**: Prevent repeated failures on same files
- **Automatic retry**: Retry after cooldown period expires
- **Operational visibility**: Track problematic files for investigation

See Section 3.6 for detailed skipped files management.

### 3.1.6 Transaction Log Operations

#### Writing Actions

```scala
private def writeActions(version: Long, actions: Seq[Action]): Unit = {
  val versionFile = new Path(transactionLogPath, f"$version%020d.json")

  // Build JSON content
  val content = new StringBuilder()
  actions.foreach { action =>
    val wrappedAction = action match {
      case protocol: ProtocolAction => Map("protocol" -> protocol)
      case metadata: MetadataAction => Map("metaData" -> metadata)
      case add: AddAction           => Map("add" -> add)
      case remove: RemoveAction     => Map("remove" -> remove)
      case skip: SkipAction         => Map("mergeskip" -> skip)
    }

    content.append(JsonUtil.mapper.writeValueAsString(wrappedAction))
    content.append("\n")
  }

  // CRITICAL: Conditional write ensures immutability
  val writeSucceeded = cloudProvider.writeFileIfNotExists(
    versionFile.toString,
    content.toString.getBytes("UTF-8")
  )

  if (!writeSucceeded) {
    throw new IllegalStateException(
      s"Transaction log version $version already exists - concurrent write conflict"
    )
  }

  // Invalidate caches
  cache.foreach(_.invalidateVersionDependentCaches())

  // Create checkpoint if needed
  checkpoint.foreach { cp =>
    if (cp.shouldCreateCheckpoint(version)) {
      cp.createCheckpoint(version, getAllCurrentActions(version))
      cp.cleanupOldVersions(version)
    }
  }
}
```

#### Reading Versions

```scala
def readVersion(version: Long): Seq[Action] = {
  // Check cache first
  cache.flatMap(_.getCachedVersion(version)) match {
    case Some(cachedActions) => cachedActions
    case None =>
      val versionFile = new Path(transactionLogPath, f"$version%020d.json")

      val content = new String(cloudProvider.readFile(versionFile.toString), "UTF-8")
      val actions = content.split("\n").filter(_.nonEmpty).map { line =>
        val jsonNode = JsonUtil.mapper.readTree(line)

        if (jsonNode.has("protocol")) {
          JsonUtil.mapper.readValue(jsonNode.get("protocol").toString, classOf[ProtocolAction])
        } else if (jsonNode.has("metaData")) {
          JsonUtil.mapper.readValue(jsonNode.get("metaData").toString, classOf[MetadataAction])
        } else if (jsonNode.has("add")) {
          JsonUtil.mapper.readValue(jsonNode.get("add").toString, classOf[AddAction])
        } else if (jsonNode.has("remove")) {
          JsonUtil.mapper.readValue(jsonNode.get("remove").toString, classOf[RemoveAction])
        } else if (jsonNode.has("mergeskip")) {
          JsonUtil.mapper.readValue(jsonNode.get("mergeskip").toString, classOf[SkipAction])
        } else {
          throw new IllegalArgumentException(s"Unknown action type in: $line")
        }
      }.toSeq

      // Cache result
      cache.foreach(_.cacheVersion(version, actions))
      actions
  }
}
```

#### Listing Files

```scala
def listFiles(): Seq[AddAction] = {
  // Check cache first
  cache.flatMap(_.getCachedFiles()) match {
    case Some(cachedFiles) => cachedFiles
    case None =>
      val files = ListBuffer[AddAction]()

      // Try checkpoint first for performance
      checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
        case Some(checkpointActions) =>
          // Apply checkpoint base state
          checkpointActions.foreach {
            case add: AddAction       => files += add
            case remove: RemoveAction => files --= files.filter(_.path == remove.path)
            case _ => // Ignore other actions
          }

          // Apply incremental changes since checkpoint
          val checkpointVersion = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)
          val versionsAfterCheckpoint = getVersions().filter(_ > checkpointVersion)

          versionsAfterCheckpoint.sorted.foreach { version =>
            readVersion(version).foreach {
              case add: AddAction       => files += add
              case remove: RemoveAction => files --= files.filter(_.path == remove.path)
              case _ => // Ignore other actions
            }
          }

        case None =>
          // No checkpoint - read all versions sequentially
          getVersions().sorted.foreach { version =>
            readVersion(version).foreach {
              case add: AddAction       => files += add
              case remove: RemoveAction => files --= files.filter(_.path == remove.path)
              case _ => // Ignore other actions
            }
          }
      }

      val result = files.toSeq
      cache.foreach(_.cacheFiles(result))
      result
  }
}
```

---

## 3.2 Checkpoint System

### 3.2.1 Overview

**Checkpoint compaction** is a critical performance optimization that prevents unbounded transaction log growth. Without checkpoints, reading a table would require replaying thousands of individual transaction files.

**Problem:**

```
# Without checkpoints (1000 transactions)
Read time = readFile(000...000.json) + readFile(000...001.json) + ... + readFile(000...999.json)
          = 1000 * ~5ms
          = 5000ms (5 seconds)
```

**Solution:**

```
# With checkpoints (checkpoint at version 900)
Read time = readFile(000...900.checkpoint.json) + readFile(901.json) + ... + readFile(999.json)
          = 1 * ~50ms + 100 * ~5ms
          = 550ms (60% faster)
```

### 3.2.2 Checkpoint Format

Checkpoint files consolidate table state into a single JSON file:

**File Naming:**
```
00000000000000000010.checkpoint.json  # Checkpoint at version 10
```

**Content Structure:**

```json
{
  "protocol": {
    "minReaderVersion": 2,
    "minWriterVersion": 2
  },
  "metaData": {
    "id": "...",
    "schemaString": "...",
    "partitionColumns": ["date", "hour"]
  },
  "add": [
    {
      "path": "date=2024-01-01/hour=10/split1.split",
      "partitionValues": {"date": "2024-01-01", "hour": "10"},
      ...
    },
    {
      "path": "date=2024-01-01/hour=11/split2.split",
      "partitionValues": {"date": "2024-01-01", "hour": "11"},
      ...
    }
  ]
}
```

**Checkpoint Contains:**
- **Protocol**: Latest ProtocolAction
- **Metadata**: Latest MetadataAction
- **Active Files**: All AddActions not removed by RemoveActions
- **Excludes**: RemoveActions, SkipActions (transient)

### 3.2.3 Checkpoint Creation

Checkpoints are created automatically when configured interval is reached:

```scala
class TransactionLogCheckpoint(
  transactionLogPath: Path,
  cloudProvider: CloudStorageProvider,
  options: CaseInsensitiveStringMap
) {

  private val checkpointInterval = options.getInt("spark.indextables.checkpoint.interval", 10)

  def shouldCreateCheckpoint(version: Long): Boolean = {
    version > 0 && version % checkpointInterval == 0
  }

  def createCheckpoint(version: Long, actions: Seq[Action]): Unit = {
    val checkpointFile = new Path(transactionLogPath, f"$version%020d.checkpoint.json")

    // Separate actions by type
    var latestProtocol: Option[ProtocolAction] = None
    var latestMetadata: Option[MetadataAction] = None
    val activeFiles = ListBuffer[AddAction]()

    actions.foreach {
      case p: ProtocolAction => latestProtocol = Some(p)
      case m: MetadataAction => latestMetadata = Some(m)
      case a: AddAction      => activeFiles += a
      case _: RemoveAction   => // Already processed (files not in activeFiles)
      case _: SkipAction     => // Skip transient actions
    }

    // Build checkpoint JSON
    val checkpointContent = Map(
      "protocol" -> latestProtocol,
      "metaData" -> latestMetadata,
      "add" -> activeFiles.toSeq
    )

    val json = JsonUtil.mapper.writeValueAsString(checkpointContent)
    cloudProvider.writeFile(checkpointFile.toString, json.getBytes("UTF-8"))

    // Update _last_checkpoint pointer
    updateLastCheckpointPointer(version)

    logger.info(s"Created checkpoint at version $version with ${activeFiles.length} active files")
  }

  private def updateLastCheckpointPointer(version: Long): Unit = {
    val lastCheckpointFile = new Path(transactionLogPath, "_last_checkpoint")
    val content = Map("version" -> version)
    val json = JsonUtil.mapper.writeValueAsString(content)
    cloudProvider.writeFile(lastCheckpointFile.toString, json.getBytes("UTF-8"))
  }
}
```

### 3.2.4 Parallel Retrieval

**New in v1.2**: Checkpoint system includes parallel transaction file reading for remaining incremental changes:

```scala
def readVersionsInParallel(versions: Seq[Long]): Map[Long, Seq[Action]] = {
  import scala.concurrent.{Await, Future}
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val parallelism = options.getInt("spark.indextables.checkpoint.parallelism", 4)
  val timeout = options.getInt("spark.indextables.checkpoint.read.timeoutSeconds", 30)

  // Create futures for each version
  val futures: Seq[Future[(Long, Seq[Action])]] = versions.map { version =>
    Future {
      version -> readVersion(version)
    }
  }

  // Wait for all futures with timeout
  val results = Await.result(Future.sequence(futures), timeout.seconds)
  results.toMap
}
```

**Performance Impact:**

| Scenario | Sequential | Parallel (4 threads) | Improvement |
|----------|-----------|---------------------|-------------|
| 100 versions after checkpoint | ~500ms | ~150ms | 70% faster |
| 50 versions after checkpoint | ~250ms | ~80ms | 68% faster |
| 10 versions after checkpoint | ~50ms | ~20ms | 60% faster |

### 3.2.5 Incremental Reading Workflow

The optimized read workflow combines checkpoint + parallel incremental reading:

```
1. Check for checkpoint
   ├─ No checkpoint → Read all versions sequentially (fallback)
   └─ Checkpoint exists → Continue to step 2

2. Load base state from checkpoint (single file read)
   └─ Get Protocol, Metadata, and active AddActions

3. Determine incremental versions
   └─ versions.filter(_ > checkpointVersion)

4. Read incremental versions in parallel
   └─ Use thread pool with configurable parallelism

5. Apply incremental changes in version order
   └─ Maintain consistency by sorting before applying

6. Cache final result
   └─ Cache both checkpoint actions and final file list
```

**Code Example:**

```scala
def listFiles(): Seq[AddAction] = {
  val files = ListBuffer[AddAction]()

  checkpoint.flatMap(_.getActionsFromCheckpoint()) match {
    case Some(checkpointActions) =>
      // Step 2: Apply checkpoint base state
      checkpointActions.foreach {
        case add: AddAction       => files += add
        case remove: RemoveAction => files --= files.filter(_.path == remove.path)
        case _ => // Protocol/Metadata already processed
      }

      // Step 3: Get incremental versions
      val checkpointVersion = checkpoint.flatMap(_.getLastCheckpointVersion()).getOrElse(-1L)
      val incrementalVersions = getVersions().filter(_ > checkpointVersion)

      if (incrementalVersions.nonEmpty) {
        // Step 4: Parallel read
        val parallelResults = checkpoint.get.readVersionsInParallel(incrementalVersions)

        // Step 5: Apply in order
        incrementalVersions.sorted.foreach { version =>
          parallelResults.get(version).foreach { actions =>
            actions.foreach {
              case add: AddAction       => files += add
              case remove: RemoveAction => files --= files.filter(_.path == remove.path)
              case _ => // Ignore
            }
          }
        }
      }

    case None =>
      // Fallback: No checkpoint available
      getVersions().sorted.foreach { version =>
        readVersion(version).foreach {
          case add: AddAction       => files += add
          case remove: RemoveAction => files --= files.filter(_.path == remove.path)
          case _ => // Ignore
        }
      }
  }

  files.toSeq
}
```

---

## 3.3 Checkpoint Configuration & Behavior

### 3.3.1 Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `spark.indextables.checkpoint.enabled` | `true` | Enable automatic checkpoints |
| `spark.indextables.checkpoint.interval` | `10` | Create checkpoint every N transactions |
| `spark.indextables.checkpoint.parallelism` | `4` | Thread pool size for parallel I/O |
| `spark.indextables.checkpoint.read.timeoutSeconds` | `30` | Timeout for parallel reads |
| `spark.indextables.logRetention.duration` | `2592000000` | 30 days (milliseconds) |
| `spark.indextables.checkpointRetention.duration` | `7200000` | 2 hours (milliseconds) |
| `spark.indextables.cleanup.enabled` | `true` | Enable automatic cleanup |
| `spark.indextables.cleanup.failurePolicy` | `continue` | Continue on cleanup failure |

### 3.3.2 Retention Policies

IndexTables4Spark uses **ultra-conservative retention policies** with multiple safety gates:

**Transaction File Deletion Criteria:**

A transaction file is deleted ONLY when ALL conditions are met:

```scala
def shouldDeleteFile(fileAge: Long, version: Long, checkpointVersion: Long, currentVersion: Long): Boolean = {
  fileAge > logRetentionDuration &&              // Old enough
  version < checkpointVersion &&                 // Included in checkpoint
  version < currentVersion                       // Not actively being written
}
```

**Safety Gates:**

| Gate | Purpose | Example |
|------|---------|---------|
| **Age requirement** | Prevent premature deletion | File must be > 30 days old |
| **Checkpoint inclusion** | Ensure data preserved | Version < checkpoint version |
| **Version safety** | Avoid active writes | Version < current version |

**Example Scenario:**

```
Current time: 2024-02-01
Current version: 100
Latest checkpoint: Version 90
Log retention: 30 days

Transaction files:
├─ Version 50 (created 2023-12-15, 47 days old)
│  └─ Age: ✅ > 30 days
│  └─ Version: ✅ < 90 (in checkpoint)
│  └─ Current: ✅ < 100
│  └─ RESULT: ✅ CAN DELETE
│
├─ Version 85 (created 2024-01-25, 7 days old)
│  └─ Age: ❌ < 30 days
│  └─ RESULT: ❌ KEEP (too recent)
│
└─ Version 95 (created 2024-01-28, 4 days old)
   └─ Age: ❌ < 30 days
   └─ Version: ❌ > 90 (not in checkpoint)
   └─ RESULT: ❌ KEEP (not in checkpoint + too recent)
```

### 3.3.3 Automatic Cleanup

```scala
def cleanupOldVersions(currentVersion: Long): Unit = {
  if (!cleanupEnabled) return

  val checkpointVersion = getLastCheckpointVersion().getOrElse(-1L)
  val now = System.currentTimeMillis()

  val versions = getVersions()
  var deletedCount = 0

  versions.foreach { version =>
    if (version < checkpointVersion && version < currentVersion) {
      val versionFile = new Path(transactionLogPath, f"$version%020d.json")

      try {
        val fileAge = now - cloudProvider.getModificationTime(versionFile.toString)

        if (fileAge > logRetentionDuration) {
          cloudProvider.deleteFile(versionFile.toString)
          deletedCount += 1
        }
      } catch {
        case e: Exception =>
          if (failurePolicy == "fail") {
            throw e
          } else {
            logger.warn(s"Failed to delete version $version", e)
          }
      }
    }
  }

  logger.info(s"Cleaned up $deletedCount old transaction log files")
}
```

### 3.3.4 Read Optimization

**Pre-Checkpoint File Skipping:**

When a checkpoint exists, reading the transaction log **skips all files before the checkpoint**:

```scala
// WITHOUT checkpoint optimization:
def listFiles(): Seq[AddAction] = {
  getVersions().foreach { version =>      // Reads ALL versions
    readVersion(version)                   // 1000+ file reads
  }
}

// WITH checkpoint optimization:
def listFiles(): Seq[AddAction] = {
  checkpoint.getActionsFromCheckpoint()    // 1 checkpoint file read
  +
  getVersions()
    .filter(_ > checkpointVersion)         // ONLY incremental versions
    .foreach(readVersion)                  // 10-100 file reads instead of 1000+
}
```

**Performance Impact:**

| Table Age | Transactions | Without Checkpoint | With Checkpoint | Improvement |
|-----------|-------------|-------------------|-----------------|-------------|
| 1 month | 100 | ~500ms | ~150ms | 70% |
| 6 months | 500 | ~2500ms | ~200ms | 92% |
| 1 year | 1000 | ~5000ms | ~250ms | 95% |

---

## 3.4 Transaction Log Cache

### 3.4.1 Multi-Level Caching

The `TransactionLogCache` provides multi-level caching with proper TTL and invalidation:

```scala
class TransactionLogCache(expirationSeconds: Long) {
  import com.google.common.cache.{Cache, CacheBuilder}
  import java.util.concurrent.TimeUnit

  // Version-independent caches (survive writes)
  private val protocolCache: Cache[String, ProtocolAction] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
      .build()

  private val metadataCache: Cache[String, MetadataAction] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
      .build()

  // Version-dependent caches (invalidated on writes)
  private val versionCache: Cache[Long, Seq[Action]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
      .maximumSize(1000)
      .build()

  private val filesCache: Cache[String, Seq[AddAction]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
      .build()

  private val versionsListCache: Cache[String, Seq[Long]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
      .build()
}
```

**Cache Categories:**

| Cache | TTL | Invalidation Strategy | Purpose |
|-------|-----|----------------------|---------|
| **Protocol** | 5 min | Explicit (on upgrade) | Protocol version |
| **Metadata** | 5 min | Explicit (on schema change) | Table schema |
| **Versions** | 5 min | On every write | Individual transaction content |
| **Files** | 5 min | On every write | Active file list |
| **Versions List** | 5 min | On every write | Available version numbers |

### 3.4.2 Invalidation Strategy

```scala
def invalidateVersionDependentCaches(): Unit = {
  // Invalidate caches that depend on current version
  versionCache.invalidateAll()
  filesCache.invalidateAll()
  versionsListCache.invalidateAll()

  // Keep protocol and metadata (changed less frequently)
}

def invalidateProtocol(): Unit = {
  protocolCache.invalidateAll()
}

def invalidateMetadata(): Unit = {
  metadataCache.invalidateAll()
}

def invalidateAll(): Unit = {
  protocolCache.invalidateAll()
  metadataCache.invalidateAll()
  versionCache.invalidateAll()
  filesCache.invalidateAll()
  versionsListCache.invalidateAll()
}
```

### 3.4.3 Cache Statistics

```scala
case class CacheStats(
  protocolHitRate: Double,
  metadataHitRate: Double,
  versionHitRate: Double,
  filesHitRate: Double,
  versionsListHitRate: Double
)

def getStats(): CacheStats = {
  CacheStats(
    protocolCache.stats().hitRate(),
    metadataCache.stats().hitRate(),
    versionCache.stats().hitRate(),
    filesCache.stats().hitRate(),
    versionsListCache.stats().hitRate()
  )
}
```

**Usage:**

```scala
val stats = transactionLog.getCacheStats()
stats.foreach { s =>
  println(s"Protocol cache hit rate: ${s.protocolHitRate * 100}%")
  println(s"Files cache hit rate: ${s.filesHitRate * 100}%")
}
```

---

## 3.5 Optimized Transaction Log

### 3.5.1 Factory Pattern

**New in v1.11**: `TransactionLogFactory` automatically selects optimized implementation:

```scala
object TransactionLogFactory {
  def create(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap
  ): TransactionLog = {

    val useOptimized = options.getBoolean("spark.indextables.transaction.useOptimized", true)

    if (useOptimized) {
      new OptimizedTransactionLog(tablePath, spark, options)
    } else {
      new TransactionLog(tablePath, spark, options)
    }
  }
}
```

**Usage:**

```scala
// Automatic selection (recommended)
val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

// Force standard implementation (for testing/debugging)
val transactionLog = TransactionLogFactory.create(
  tablePath,
  spark,
  options.set("spark.indextables.transaction.useOptimized", "false")
)
```

### 3.5.2 Advanced Optimizations

The `OptimizedTransactionLog` includes several advanced optimizations:

**Backward Listing Optimization:**
```scala
// List versions in reverse order for metadata lookups
def getMetadata(): MetadataAction = {
  val latestVersion = getLatestVersion()

  // Search backward from latest version
  for (version <- latestVersion to 0L by -1) {
    readVersion(version).collectFirst {
      case metadata: MetadataAction => return metadata
    }
  }
}
```

**Incremental Checksums:**
```scala
// Validate checkpoint integrity
def validateCheckpoint(checkpointFile: Path): Boolean = {
  val content = cloudProvider.readFile(checkpointFile.toString)
  val expectedChecksum = computeChecksum(content)
  val actualChecksum = readStoredChecksum(checkpointFile)
  expectedChecksum == actualChecksum
}
```

**Async Updates with Staleness Tolerance:**
```scala
// Allow slightly stale reads for better performance
def listFiles(maxStalenessSeconds: Int = 5): Seq[AddAction] = {
  cache.getCachedFiles() match {
    case Some(files) if isFresh(files, maxStalenessSeconds) => files
    case _ => refreshFilesCache()
  }
}
```

**Streaming Checkpoint Creation:**
```scala
// Create checkpoints without loading entire state in memory
def createCheckpointStreaming(version: Long): Unit = {
  val outputStream = cloudProvider.createOutputStream(checkpointFile)

  try {
    writeProtocol(outputStream)
    writeMetadata(outputStream)

    // Stream active files in batches
    streamActiveFiles(outputStream, batchSize = 1000)
  } finally {
    outputStream.close()
  }
}
```

### 3.5.3 Thread Pool Management

Dedicated thread pools for different operation types:

```scala
object TransactionLogThreadPools {
  private val fileListingPool = Executors.newFixedThreadPool(
    Runtime.getRuntime.availableProcessors()
  )

  private val versionReadingPool = Executors.newFixedThreadPool(
    4  // Configurable via spark.indextables.checkpoint.parallelism
  )

  private val checkpointCreationPool = Executors.newFixedThreadPool(2)

  def shutdown(): Unit = {
    fileListingPool.shutdown()
    versionReadingPool.shutdown()
    checkpointCreationPool.shutdown()
  }
}
```

---

## 3.6 Skipped Files Management

### 3.6.1 Overview

Skipped files tracking prevents repeated failures on corrupted or problematic files during merge operations:

**Problem:**
```
Merge operation encounters corrupted split
  → Merge fails
  → Next merge attempt processes same file
  → Merge fails again
  → Infinite loop of failures
```

**Solution:**
```
Merge operation encounters corrupted split
  → Record SkipAction with cooldown timestamp
  → Filter out file during cooldown period
  → Automatic retry after cooldown expires
  → Eventually fixed or investigated
```

### 3.6.2 Recording Skipped Files

```scala
def recordSkippedFile(
  filePath: String,
  reason: String,
  operation: String,
  partitionValues: Option[Map[String, String]] = None,
  size: Option[Long] = None,
  cooldownHours: Int = 24
): Long = {

  val timestamp = System.currentTimeMillis()
  val retryAfter = timestamp + (cooldownHours * 60 * 60 * 1000L)

  // Check if file was already skipped and increment count
  val existingSkips = getSkippedFiles().filter(_.path == filePath)
  val skipCount = if (existingSkips.nonEmpty) {
    existingSkips.map(_.skipCount).max + 1
  } else {
    1
  }

  val skipAction = SkipAction(
    path = filePath,
    skipTimestamp = timestamp,
    reason = reason,
    operation = operation,
    partitionValues = partitionValues,
    size = size,
    retryAfter = Some(retryAfter),
    skipCount = skipCount
  )

  val version = getNextVersion()
  writeActions(version, Seq(skipAction))

  logger.info(s"Recorded skipped file: $filePath (skip count: $skipCount, retry after: ${Instant.ofEpochMilli(retryAfter)})")
  version
}
```

### 3.6.3 Cooldown Mechanism

```scala
def isFileInCooldown(filePath: String): Boolean = {
  val now = System.currentTimeMillis()
  val recentSkips = getSkippedFiles()
    .filter(_.path == filePath)
    .filter(skip => skip.retryAfter.exists(_ > now))

  recentSkips.nonEmpty
}

def getFilesInCooldown(): Map[String, Long] = {
  val now = System.currentTimeMillis()
  getSkippedFiles()
    .filter(skip => skip.retryAfter.exists(_ > now))
    .groupBy(_.path)
    .map { case (path, skips) =>
      val latestRetryAfter = skips.flatMap(_.retryAfter).max
      path -> latestRetryAfter
    }
}
```

### 3.6.4 Filtering During Operations

```scala
def filterFilesInCooldown(candidateFiles: Seq[AddAction]): Seq[AddAction] = {
  val filesInCooldown = getFilesInCooldown().keySet
  val filtered = candidateFiles.filterNot(file => filesInCooldown.contains(file.path))

  val filteredCount = candidateFiles.length - filtered.length
  if (filteredCount > 0) {
    logger.info(s"Filtered out $filteredCount files currently in cooldown period")
    filesInCooldown.foreach { path =>
      val retryTime = getFilesInCooldown().get(path)
      logger.debug(s"File in cooldown: $path (retry after: ${retryTime.map(Instant.ofEpochMilli)})")
    }
  }

  filtered
}
```

### 3.6.5 Operational Safety

**Important Guarantees:**

1. **Files remain accessible**: Skipped files are NOT marked as "removed" in transaction log
2. **No data loss**: Original files remain readable during cooldown
3. **Automatic retry**: Files become eligible after cooldown expires
4. **Visibility**: All skipped files tracked in transaction log for investigation

**Example Workflow:**

```
Day 1: Merge encounters corrupted file "split-123.split"
  → Record SkipAction (cooldown: 24 hours)
  → Continue merge without this file
  → Warning logged for investigation

Day 2: Another merge operation
  → Check cooldown status
  → File still in cooldown (< 24 hours)
  → Skip file again

Day 3: Another merge operation
  → Check cooldown status
  → Cooldown expired (> 24 hours)
  → Attempt merge again
  → Either succeeds (file fixed) or creates new SkipAction
```

---

## Summary

## 3.7 Transaction Log Compression

### 3.7.1 Overview

**Version:** 1.15
**Status:** Production-ready (enabled by default)

Transaction log compression reduces S3 storage costs and improves download performance by compressing transaction log files and checkpoints using GZIP.

### 3.7.2 Motivation

**Problem:**
- Transaction log files are JSON text, which compresses well
- Uncompressed JSON wastes S3 storage and bandwidth
- Larger files result in slower S3 downloads
- High-volume tables can accumulate significant storage costs

**Solution:**
- Transparent GZIP compression for all transaction log files
- Magic byte prefix for automatic format detection
- Backward compatible - reads both compressed and uncompressed files
- Enabled by default for all new writes

### 3.7.3 Implementation

#### Compression Format

Compressed files use a magic byte prefix:

```
Byte 0: 0x01 (Format version)
Byte 1: 0x01 (GZIP codec identifier)
Bytes 2+: GZIP compressed data
```

Uncompressed files start directly with JSON (`{` or `[`).

#### Core Components

**`CompressionCodec` trait:**
```scala
trait CompressionCodec {
  def name: String
  def compress(data: Array[Byte]): Array[Byte]
  def decompress(data: Array[Byte]): Array[Byte]
}
```

**`GzipCompressionCodec` implementation:**
- Uses standard Java `GZIPOutputStream` and `GZIPInputStream`
- Configurable compression level (1-9, default: 6)
- No external dependencies required

**`CompressionUtils` helper:**
- `isCompressed(bytes: Array[Byte]): Boolean` - checks magic bytes
- `readTransactionFile(bytes: Array[Byte]): Array[Byte]` - automatically decompresses
- `writeTransactionFile(bytes: Array[Byte], codec: Option[CompressionCodec]): Array[Byte]` - optionally compresses
- `getCodec(enabled: Boolean, codecName: String, level: Int): Option[CompressionCodec]` - factory method

#### Integration Points

**Write Path:**
1. `TransactionLog.writeActions()` calls `getCompressionCodec()`
2. JSON is serialized to UTF-8 bytes
3. `CompressionUtils.writeTransactionFile()` optionally compresses
4. Compressed bytes written to S3 via `CloudProvider`

**Read Path:**
1. Raw bytes read from S3 via `CloudProvider`
2. `CompressionUtils.readTransactionFile()` checks magic bytes
3. If compressed, decompresses automatically
4. JSON parsed from decompressed UTF-8 bytes

**Both `TransactionLog` and `OptimizedTransactionLog` support compression:**
- Same `getCompressionCodec()` logic using ThreadLocal for write-time options
- Same `CompressionUtils` for transparent read/write

**Checkpoint support:**
- `TransactionLogCheckpoint.createCheckpoint()` uses compression when enabled
- `TransactionLogCheckpoint.getActionsFromCheckpoint()` decompresses automatically
- Parallel reads via `readSingleVersion()` handle compressed files

### 3.7.4 Configuration

```scala
// Default behavior (compression enabled)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/data")

// Explicit configuration
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.transaction.compression.enabled", "true")
  .option("spark.indextables.transaction.compression.codec", "gzip")
  .option("spark.indextables.transaction.compression.gzip.level", "6")
  .save("s3://bucket/data")

// Disable compression (not recommended)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.transaction.compression.enabled", "false")
  .save("s3://bucket/data")
```

**Configuration Options:**
- `spark.indextables.transaction.compression.enabled`: Enable/disable compression (default: true)
- `spark.indextables.transaction.compression.codec`: Codec name (default: "gzip")
- `spark.indextables.transaction.compression.gzip.level`: GZIP level 1-9 (default: 6)

### 3.7.5 Performance Characteristics

**Compression Ratios:**
- Typical JSON transaction logs: **2-3x compression**
- Tables with long text fields: **Up to 5x compression**
- Protocol/metadata files: **60-70% size reduction**

**Write Performance:**
- GZIP level 6 (default): ~5-10% overhead
- GZIP level 1 (fast): ~2-3% overhead
- GZIP level 9 (maximum): ~15-20% overhead

**Read Performance:**
- **2-3x faster S3 downloads** due to smaller files
- Decompression overhead: ~1-2ms per file
- Net benefit: Positive for all but the smallest files

**Storage Savings:**
- High-volume table (1000 transactions): ~600MB → ~200MB (67% reduction)
- Checkpoint files: Similar compression ratios

### 3.7.6 Backward Compatibility

**Mixed-Mode Support:**
- Tables can contain both compressed and uncompressed files
- Automatic detection via magic bytes
- No migration required - gradual adoption

**Upgrade Path:**
1. Deploy v1.15+ code (compression enabled by default)
2. New writes are compressed automatically
3. Old uncompressed files remain readable
4. No downtime or data migration required

**Downgrade Path:**
- Older code cannot read compressed files
- Would fail with JSON parse errors
- Recommendation: Disable compression before downgrading

### 3.7.7 Testing

**Test Coverage:**
- 5 integration tests for compression feature
- Mixed compressed/uncompressed file scenarios
- Checkpoint compression validation
- All existing tests pass with compression enabled (228+ tests)

**Key Test Scenarios:**
- Write compressed, read compressed
- Write uncompressed, read uncompressed
- Write compressed, read with compression disabled (still works)
- Checkpoint creation with compression
- Mixed compressed/uncompressed in same table

---

## Summary

This section covered the Transaction Log System:

- **Architecture**: Delta Lake-compatible ACID transactions with immutable files
- **Action Types**: ProtocolAction, MetadataAction, AddAction, RemoveAction, SkipAction
- **Checkpoint System**: Automatic compaction with 60% performance improvement
- **Cache System**: Multi-level caching with TTL and proper invalidation
- **Optimized Implementation**: Factory pattern with advanced optimizations
- **Skipped Files**: Robust handling of corrupted files with cooldown tracking
- **Compression**: GZIP compression for 60-70% storage reduction and faster S3 downloads

The transaction log provides the foundation for ACID guarantees and efficient table management in IndexTables4Spark.

---

**Previous Section:** [Section 2: DataSource V2 API Implementation](02_datasource_v2_api.md)
**Next Section:** [Section 4: Scan Planning & Execution](04_scan_planning.md)
