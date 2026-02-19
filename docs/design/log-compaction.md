# Transaction Log Compaction Design Document

## Overview

Transaction log compaction is a Delta Lake optimization that periodically consolidates multiple JSON transaction log files into a single Parquet checkpoint file. This prevents the need to read hundreds or thousands of individual JSON files when opening a table, significantly improving metadata read performance for tables with long transaction histories.

## Current State

IndexTables4Spark's transaction log currently:
- Stores each transaction as a separate JSON file (`000000000000000001.json`, `000000000000000002.json`, etc.)
- Requires reading all JSON files from version 0 to get the complete table state
- Grows linearly with the number of write operations
- Can become slow for tables with hundreds of transactions

## Problem Statement

As tables accumulate write operations, metadata operations become increasingly expensive:
- **Table reads**: Must read all JSON files to get current file list and schema
- **Query planning**: Requires parsing all transaction history for partition pruning
- **Cold starts**: New Spark sessions must read entire transaction history
- **Storage overhead**: Many small JSON files create overhead compared to single Parquet file

## Proposed Architecture

### Core Components

#### 1. Checkpoint File Format
```scala
case class CheckpointMetadata(
  version: Long,              // Last transaction version included in checkpoint
  size: Long,                 // Number of files in the table
  parts: Option[Int] = None,  // Number of checkpoint parts (for large tables)
  createdTime: Long = System.currentTimeMillis()
)

case class CheckpointEntry(
  // Flattened representation combining all action types
  path: Option[String] = None,                    // For ADD actions
  partitionValues: Option[Map[String, String]] = None,  // For ADD actions
  size: Option[Long] = None,                      // For ADD actions
  numRecords: Option[Long] = None,                // For ADD actions
  minValues: Option[Map[String, String]] = None,  // For ADD actions
  maxValues: Option[Map[String, String]] = None,  // For ADD actions
  
  // Metadata fields (only one entry per checkpoint)
  metadataId: Option[String] = None,              // For METADATA actions
  schemaString: Option[String] = None,            // For METADATA actions
  partitionColumns: Option[Seq[String]] = None,   // For METADATA actions
  format: Option[Map[String, String]] = None,     // For METADATA actions
  
  // Action type discriminator
  actionType: String  // "add", "remove", "metadata"
)
```

#### 2. Checkpoint Writer
```scala
class CheckpointWriter(transactionLog: TransactionLog) {
  
  def shouldCreateCheckpoint(currentVersion: Long): Boolean = {
    val lastCheckpointVersion = getLastCheckpointVersion()
    val transactionsSinceCheckpoint = currentVersion - lastCheckpointVersion
    transactionsSinceCheckpoint >= checkpointInterval
  }
  
  def writeCheckpoint(version: Long): Unit = {
    val tableState = computeTableState(version)
    val checkpointPath = getCheckpointPath(version)
    
    // Write checkpoint as Parquet file
    import spark.implicits._
    val checkpointDF = spark.createDataset(tableState).toDF()
    
    checkpointDF.write
      .mode("overwrite")
      .parquet(checkpointPath)
      
    // Write checkpoint metadata
    writeCheckpointMetadata(version, tableState.size)
    
    logger.info(s"Created checkpoint at version $version with ${tableState.size} entries")
  }
  
  private def computeTableState(version: Long): Seq[CheckpointEntry] = {
    // Replay all transactions from 0 to version and compute final state
    val finalFiles = mutable.Map[String, AddAction]()
    var latestMetadata: Option[MetadataAction] = None
    
    for (v <- 0L to version) {
      val actions = transactionLog.readVersion(v)
      actions.foreach {
        case add: AddAction => finalFiles(add.path) = add
        case remove: RemoveAction => finalFiles.remove(remove.path)
        case metadata: MetadataAction => latestMetadata = Some(metadata)
      }
    }
    
    val entries = finalFiles.values.map(convertToCheckpointEntry) ++
                  latestMetadata.map(convertToCheckpointEntry)
                  
    entries.toSeq
  }
}
```

#### 3. Checkpoint Reader
```scala
class CheckpointReader(transactionLog: TransactionLog) {
  
  def getTableStateFromCheckpoint(): Option[TableState] = {
    val latestCheckpoint = findLatestCheckpoint()
    
    latestCheckpoint.map { checkpoint =>
      val checkpointPath = getCheckpointPath(checkpoint.version)
      val checkpointDF = spark.read.parquet(checkpointPath)
      
      import spark.implicits._
      val entries = checkpointDF.as[CheckpointEntry].collect()
      
      val files = entries.filter(_.actionType == "add").map(convertToAddAction)
      val metadata = entries.find(_.actionType == "metadata").map(convertToMetadataAction)
      
      // Read any transactions after the checkpoint
      val additionalActions = readTransactionsSince(checkpoint.version + 1)
      val finalFiles = applyActions(files, additionalActions)
      
      TableState(finalFiles, metadata, checkpoint.version)
    }
  }
  
  def findLatestCheckpoint(): Option[CheckpointMetadata] = {
    val checkpointDir = new Path(transactionLog.getTablePath(), "_transaction_log/_checkpoints")
    
    Try {
      val files = cloudProvider.listFiles(checkpointDir.toString, recursive = false)
      val checkpointFiles = files.filter(_.path.contains("checkpoint"))
      
      if (checkpointFiles.nonEmpty) {
        // Find the highest version checkpoint
        val versions = checkpointFiles.map(extractVersionFromPath).sorted
        val latestVersion = versions.max
        Some(readCheckpointMetadata(latestVersion))
      } else {
        None
      }
    }.getOrElse(None)
  }
}
```

### Integration Points

#### 1. TransactionLog Enhancement
```scala
class TransactionLog(/* existing parameters */) {
  private val checkpointWriter = new CheckpointWriter(this)
  private val checkpointReader = new CheckpointReader(this)
  
  // Configuration
  private val checkpointInterval = options.getInt("spark.indextables.checkpoint.interval", 10)
  private val enableCheckpoints = options.getBoolean("spark.indextables.checkpoint.enabled", true)
  
  override def addFiles(addActions: Seq[AddAction]): Long = {
    val version = super.addFiles(addActions)
    
    // Check if we should create a checkpoint
    if (enableCheckpoints && checkpointWriter.shouldCreateCheckpoint(version)) {
      checkpointWriter.writeCheckpoint(version)
      cleanupOldTransactionFiles(version)
    }
    
    version
  }
  
  override def listFiles(): Seq[AddAction] = {
    // Try to use checkpoint for faster loading
    checkpointReader.getTableStateFromCheckpoint() match {
      case Some(tableState) => tableState.files
      case None => super.listFiles() // Fallback to JSON replay
    }
  }
  
  private def cleanupOldTransactionFiles(checkpointVersion: Long): Unit = {
    val retentionVersions = options.getInt("spark.indextables.checkpoint.retention", 30)
    val deleteBeforeVersion = checkpointVersion - retentionVersions
    
    if (deleteBeforeVersion > 0) {
      for (version <- 0L until deleteBeforeVersion) {
        val versionFile = new Path(transactionLogPath, f"$version%020d.json")
        Try {
          cloudProvider.deleteFile(versionFile.toString)
          logger.debug(s"Deleted old transaction file: $version")
        }
      }
    }
  }
}
```

#### 2. File Structure
```
table_path/
├── _transaction_log/
│   ├── 00000000000000000000.json    # Transaction files
│   ├── 00000000000000000001.json
│   ├── ...
│   ├── 00000000000000000250.json
│   └── _checkpoints/
│       ├── 00000000000000000250.checkpoint.parquet
│       └── 00000000000000000250.checkpoint.json  # Metadata
```

### Configuration Options

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.checkpoint.enabled` | `true` | Enable/disable checkpoint creation |
| `spark.indextables.checkpoint.interval` | `10` | Create checkpoint every N transactions |
| `spark.indextables.checkpoint.retention` | `30` | Keep N transactions before cleanup |
| `spark.indextables.checkpoint.compression` | `snappy` | Parquet compression codec |
| `spark.indextables.checkpoint.maxFileSize` | `134217728` | Max checkpoint file size (128MB) |

## Implementation Plan

### Phase 1: Basic Checkpointing
1. Implement `CheckpointEntry` data structure and Parquet serialization
2. Create `CheckpointWriter` with basic table state computation
3. Add checkpoint creation logic to `TransactionLog.addFiles()`
4. Unit tests for checkpoint creation and table state computation

### Phase 2: Checkpoint Reading
1. Implement `CheckpointReader` with Parquet deserialization
2. Modify `TransactionLog.listFiles()` to use checkpoints
3. Add logic to read incremental transactions after checkpoint
4. Integration tests comparing checkpoint vs JSON performance

### Phase 3: Cleanup and Optimization
1. Implement old transaction file cleanup
2. Add configuration options for checkpoint behavior
3. Optimize checkpoint file format and compression
4. Add monitoring metrics for checkpoint operations

### Phase 4: Advanced Features
1. Multi-part checkpoints for very large tables
2. Incremental checkpointing for faster creation
3. Background checkpoint creation
4. Checkpoint validation and recovery

## Testing Strategy

### Unit Tests
```scala
test("should create checkpoint after configured interval") {
  val tablePath = createTestTable()
  val transactionLog = new TransactionLog(new Path(tablePath), spark)
  
  // Write multiple transactions
  for (i <- 1 to 15) {
    val data = spark.range(100).select($"id", lit(s"batch_$i").as("name"))
    writeDataFrame(data, tablePath, SaveMode.Append)
  }
  
  // Verify checkpoint was created at transaction 10
  val checkpointPath = new Path(tablePath, "_transaction_log/_checkpoints/00000000000000000010.checkpoint.parquet")
  Files.exists(checkpointPath.toString) shouldBe true
  
  // Verify table state is correct when reading from checkpoint
  val files = transactionLog.listFiles()
  files.length shouldBe 15  // All 15 batches present
}

test("should cleanup old transaction files after checkpoint") {
  // Similar test verifying cleanup behavior
}
```

### Performance Tests
```scala
test("checkpoint reading should be faster than JSON replay") {
  val tablePath = createTableWithManyTransactions(1000)
  
  val jsonReplayTime = time {
    new TransactionLog(new Path(tablePath), spark, 
      new CaseInsensitiveStringMap(Map("spark.indextables.checkpoint.enabled" -> "false").asJava))
      .listFiles()
  }
  
  val checkpointTime = time {
    new TransactionLog(new Path(tablePath), spark).listFiles()
  }
  
  checkpointTime should be < (jsonReplayTime / 2)  // At least 2x faster
}
```

## Performance Benefits

### Expected Improvements
- **Cold start time**: 10-100x faster for tables with many transactions
- **Memory usage**: Reduced from O(transactions) to O(active_files)
- **I/O operations**: Single Parquet read vs hundreds of JSON reads
- **Query planning**: Faster partition pruning with consolidated metadata

### Benchmarks (Projected)
| Transactions | JSON Replay | Checkpoint | Speedup |
|--------------|------------|------------|---------|
| 10 | 50ms | 45ms | 1.1x |
| 100 | 500ms | 50ms | 10x |
| 1000 | 5000ms | 55ms | 90x |
| 10000 | 50000ms | 60ms | 833x |

## Compatibility

### Delta Lake Compatibility
- Follow Delta Lake checkpoint format conventions where applicable
- Use similar Parquet schema for checkpoint entries
- Support Delta Lake checkpoint reading tools (where feasible)

### Backward Compatibility
- Tables without checkpoints continue working normally
- Gradual migration: checkpoints supplement, don't replace JSON files
- Configuration allows disabling checkpoints entirely

### Storage Compatibility
- Works with all supported storage backends (local, HDFS, S3)
- Uses same cloud storage providers as transaction log
- Respects existing authentication and configuration

## Error Handling

### Checkpoint Creation Failures
```scala
sealed trait CheckpointError extends Exception
case class CheckpointWriteError(version: Long, cause: Throwable) extends CheckpointError
case class CheckpointValidationError(version: Long, reason: String) extends CheckpointError
case class CheckpointCorruptionError(path: String) extends CheckpointError
```

### Recovery Strategies
1. **Graceful Degradation**: Fall back to JSON replay if checkpoint reading fails
2. **Automatic Repair**: Recreate corrupted checkpoints from JSON files
3. **Validation**: Verify checkpoint consistency before cleanup
4. **Rollback**: Keep previous checkpoint until new one is validated

## Monitoring and Observability

### Metrics
- Checkpoint creation time and frequency
- Checkpoint file size and compression ratio
- Table state loading time (JSON vs checkpoint)
- Number of files cleaned up

### Logging
```scala
logger.info(s"Created checkpoint at version $version: ${entries.size} entries, ${fileSize}MB")
logger.info(s"Cleaned up ${deletedCount} transaction files before version $retentionVersion") 
logger.debug(s"Table state loaded from checkpoint in ${loadTime}ms")
```

## Future Enhancements

1. **Incremental Checkpoints**: Only store changes since last checkpoint
2. **Compressed JSON**: Alternative to Parquet for smaller tables
3. **Background Processing**: Asynchronous checkpoint creation
4. **Multi-Version Checkpoints**: Support for time travel queries
5. **Cross-Table Checkpoints**: Shared checkpoints for related tables
6. **Checkpoint Replication**: Backup checkpoints across storage locations