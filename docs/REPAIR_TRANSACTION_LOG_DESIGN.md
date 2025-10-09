REPAIR INDEXFILES TRANSACTION LOG Design

  Overview

  A new SQL command to rebuild corrupted or problematic transaction logs by reading the existing log, validating split files, and writing a clean transaction log with metadata and checkpoint to a new
  location. This is a read-only repair operation that never modifies or deletes source files.

  SQL Syntax

  REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log'
  AT LOCATION 's3://bucket/table/_transaction_log_repaired'

  Use Cases

  1. Corrupted checkpoint recovery: Rebuild checkpoint from valid transaction files
  2. Orphaned file cleanup: Create clean log excluding splits that no longer exist
  3. Transaction log optimization: Consolidate fragmented transaction history
  4. Migration scenarios: Prepare clean transaction log for table relocation
  5. Audit and validation: Generate validated transaction log with integrity checks

  Implementation Architecture

  1. SQL Parser Extension (DeltaSqlAstBuilder.scala)

  // Add new grammar rule
  def visitRepairIndexFilesTransactionLog(
    ctx: RepairIndexFilesTransactionLogContext
  ): LogicalPlan = {
    val sourcePath = visitStringLit(ctx.sourcePath)
    val targetPath = visitStringLit(ctx.targetPath)

    RepairIndexFilesTransactionLogCommand(
      sourcePath = sourcePath,
      targetPath = targetPath
    )
  }

  2. Grammar Definition (DeltaSqlBase.g4)

  repairIndexFilesTransactionLog
      : REPAIR INDEXFILES TRANSACTION LOG sourcePath=STRING
        AT LOCATION targetPath=STRING
      ;

  3. Logical Plan (RepairIndexFilesTransactionLogCommand.scala)

  New file: spark/src/main/scala/org/apache/spark/sql/delta/commands/RepairIndexFilesTransactionLogCommand.scala

  case class RepairIndexFilesTransactionLogCommand(
    sourcePath: String,
    targetPath: String
  ) extends LeafRunnableCommand {

    override def run(sparkSession: SparkSession): Seq[Row] = {
      // Implementation details below
    }
  }

  Core Implementation Flow

  Phase 1: Read & Validate Source Transaction Log

  // 1. Read existing transaction log (with checkpoint if available)
  val transactionLog = TransactionLog.forPath(sourcePath, hadoopConf)
  val currentVersion = transactionLog.getCurrentVersion()
  val snapshot = transactionLog.getSnapshot()

  // 2. Collect all visible splits from snapshot
  val visibleSplits = snapshot.getAllVisibleSplits()

  // 3. Validate split files exist in storage
  val validatedSplits = visibleSplits.filter { split =>
    val path = new Path(split.path)
    val fs = path.getFileSystem(hadoopConf)
    val exists = fs.exists(path)

    if (!exists) {
      logWarning(s"Split file not found: ${split.path}")
    }
    exists
  }

  // 4. Track validation statistics
  val totalSplits = visibleSplits.size
  val validSplits = validatedSplits.size
  val missingSplits = totalSplits - validSplits

  Phase 2: Build Clean Transaction Log with Statistics Truncation

  // 5. Create target transaction log directory
  val targetLogPath = new Path(targetPath)
  val fs = targetLogPath.getFileSystem(hadoopConf)
  fs.mkdirs(targetLogPath)

  // 6. Write metadata transaction (version 000000000000000000.json)
  val metadataAction = snapshot.getMetadataAction()
  val version0 = new Path(targetLogPath, "000000000000000000.json")
  writeTransactionFile(version0, Seq(metadataAction))

  // 7. Write consolidated add actions with truncated statistics (version 000000000000000001.json)
  // NOTE: Apply statistics truncation to optimize transaction log size
  // See DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md for full details
  val addActions = validatedSplits.map { split =>
    // Apply statistics truncation based on configuration
    val truncatedStats = split.stats.map { stats =>
      truncateStatistics(
        stats,
        snapshot.getSchema(),
        sparkSession.conf.getAll
      )
    }

    AddAction(
      path = split.path,
      partitionValues = split.partitionValues,
      size = split.size,
      modificationTime = split.modificationTime,
      dataChange = true,
      stats = truncatedStats,  // Use truncated statistics
      tags = split.tags
    )
  }
  val version1 = new Path(targetLogPath, "000000000000000001.json")
  writeTransactionFile(version1, addActions)

  Phase 3: Create Checkpoint

  // 8. Generate checkpoint file (000000000000000001.checkpoint.json)
  val checkpointData = CheckpointData(
    version = 1,
    size = validSplits,
    actions = Seq(metadataAction) ++ addActions,
    timestamp = System.currentTimeMillis()
  )

  val checkpointPath = new Path(targetLogPath, "000000000000000001.checkpoint.json")
  writeCheckpointFile(checkpointPath, checkpointData)

  // 9. Write _last_checkpoint metadata
  val lastCheckpointPath = new Path(targetLogPath, "_last_checkpoint")
  val lastCheckpointContent = CheckpointMetadata(
    version = 1,
    size = validSplits,
    parts = None,
    sizeInBytes = None,
    numOfAddFiles = Some(validSplits.toLong),
    checkpointSchema = None
  )
  writeLastCheckpoint(lastCheckpointPath, lastCheckpointContent)

  Phase 4: Return Results

  // 10. Return repair statistics
  Seq(Row(
    sourcePath,
    targetPath,
    currentVersion,
    totalSplits,
    validSplits,
    missingSplits,
    "SUCCESS"
  ))

  Output Schema

  override val output: Seq[Attribute] = Seq(
    AttributeReference("source_path", StringType)(),
    AttributeReference("target_path", StringType)(),
    AttributeReference("source_version", LongType)(),
    AttributeReference("total_splits", IntegerType)(),
    AttributeReference("valid_splits", IntegerType)(),
    AttributeReference("missing_splits", IntegerType)(),
    AttributeReference("status", StringType)()
  )

  Example Output

  +----------------------------------------+------------------------------------------------+--------------+------------+-------------+---------------+---------+
  | source_path                            | target_path                                    | source_version| total_splits| valid_splits| missing_splits| status  |
  +----------------------------------------+------------------------------------------------+--------------+------------+-------------+---------------+---------+
  | s3://bucket/table/_transaction_log     | s3://bucket/table/_transaction_log_repaired    | 47           | 150        | 148         | 2             | SUCCESS |
  +----------------------------------------+------------------------------------------------+--------------+------------+-------------+---------------+---------+

  Safety Features

  1. Read-Only Source Operations

  - No source file deletion: Original transaction log remains untouched
  - No source file modification: Only reads from source location
  - Validation only: Missing files logged but not removed from source

  2. Target Path Validation

  // Prevent accidental overwrites
  if (fs.exists(targetLogPath) && fs.listStatus(targetLogPath).nonEmpty) {
    throw new IllegalArgumentException(
      s"Target location already exists and is not empty: $targetPath"
    )
  }

  3. Atomic Write Operations

  // Use temporary files + rename pattern
  val tempFile = new Path(targetLogPath, s".${fileName}.tmp")
  writeToFile(tempFile, content)
  fs.rename(tempFile, finalPath)

  4. Transaction Integrity

  - Metadata preservation: Original metadata action carried forward
  - Partition structure: Partition values preserved in add actions
  - Statistics retention: Split stats maintained for query optimization
  - Timestamp accuracy: Modification times preserved for each split

  Error Handling

  try {
    // Repair operation
  } catch {
    case e: FileNotFoundException =>
      logError(s"Source transaction log not found: $sourcePath", e)
      Seq(Row(sourcePath, targetPath, -1, 0, 0, 0, s"ERROR: ${e.getMessage}"))

    case e: IOException =>
      logError(s"I/O error during repair: ${e.getMessage}", e)
      Seq(Row(sourcePath, targetPath, -1, 0, 0, 0, s"ERROR: ${e.getMessage}"))

    case e: Exception =>
      logError(s"Unexpected error during repair", e)
      Seq(Row(sourcePath, targetPath, -1, 0, 0, 0, s"ERROR: ${e.getMessage}"))
  }

  Configuration Options

  // Optional: Control repair behavior
  spark.conf.set("spark.indextables.repair.validateSplits", "true")      // Validate split files exist
  spark.conf.set("spark.indextables.repair.preserveStats", "true")       // Keep split statistics
  spark.conf.set("spark.indextables.repair.parallelism", "4")            // Parallel validation
  spark.conf.set("spark.indextables.repair.failOnMissing", "false")      // Continue if splits missing

  // Statistics truncation options (see DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md)
  spark.conf.set("spark.indextables.stats.truncation.enabled", "true")   // Truncate long statistics (default: true)
  spark.conf.set("spark.indextables.stats.truncation.maxLength", "1024") // Max char length (default: 1024)
  spark.conf.set("spark.indextables.stats.truncation.strategy", "drop")  // Strategy: "drop" or "truncate"

  Manual Recovery Workflow

  # 1. Run repair command to generate clean transaction log
  spark-sql> REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log'
             AT LOCATION 's3://bucket/table/_transaction_log_repaired';

  # 2. Backup original transaction log
  aws s3 sync s3://bucket/table/_transaction_log s3://bucket/table/_transaction_log_backup

  # 3. Replace transaction log (manual step - user controlled)
  aws s3 rm --recursive s3://bucket/table/_transaction_log
  aws s3 sync s3://bucket/table/_transaction_log_repaired s3://bucket/table/_transaction_log

  # 4. Verify repair was successful
  spark-sql> SELECT COUNT(*) FROM indextables.`s3://bucket/table`;

  Testing Strategy

  Unit Tests

  - Grammar parsing validation
  - Command execution with mock FileSystem
  - Error handling scenarios
  - Output schema validation

  Integration Tests

  1. Happy path: Repair healthy transaction log
  2. Missing splits: Handle splits that no longer exist
  3. Corrupted checkpoint: Rebuild from transaction files
  4. Empty source: Handle empty or missing transaction log
  5. Target exists: Reject overwrites of existing target

  Files to Modify/Create

  New Files

  1. spark/src/main/scala/org/apache/spark/sql/delta/commands/RepairIndexFilesTransactionLogCommand.scala
  2. spark/src/test/scala/io/delta/sql/parser/RepairIndexFilesTransactionLogSuite.scala

  Modified Files

  1. spark/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4 - Add grammar rule
  2. spark/src/main/scala/io/delta/sql/parser/DeltaSqlAstBuilder.scala - Add visitor method
  3. spark/src/main/scala/io/indextables/spark/extensions/IndexTables4SparkExtensions.scala - Register command

  Benefits

  ✅ **Safe repair mechanism**: No source file modifications or deletions
  ✅ **Manual control**: User explicitly controls when to replace transaction log
  ✅ **Validation reporting**: Clear statistics on missing/invalid splits
  ✅ **Checkpoint optimization**: Clean checkpoint created at target location
  ✅ **Audit trail**: Original transaction log preserved for forensic analysis
  ✅ **Production recovery**: Enables recovery from corrupted transaction logs without data loss
  ✅ **Automatic statistics truncation**: Repaired transaction logs automatically apply statistics truncation (see DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md), reducing transaction log size by up to 98% for tables with long text fields
  ✅ **Optimized transaction log size**: Repaired logs are significantly smaller than originals when long statistics are present

  Limitations

  ⚠️ **Manual replacement required**: User must manually copy repaired log to original location
  ⚠️ **No automatic cleanup**: Original transaction log files remain (user must clean up)
  ⚠️ **Single-partition checkpoint**: Does not support multi-part checkpoints (can be added later)
  ⚠️ **Metadata preservation only**: Does not rewrite split files, only transaction log metadata

## Statistics Truncation Integration

### Overview
The REPAIR command automatically applies statistics truncation during the repair process to optimize transaction log size. This is particularly beneficial when repairing transaction logs that contain excessively long min/max statistics for text columns.

### Cross-Reference
**See**: `DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md` for complete statistics truncation design including:
- Configuration options (`spark.indextables.stats.truncation.*`)
- Truncation strategies (DROP vs TRUNCATE)
- Performance benefits (98% size reduction)
- Data skipping fallback behavior

### Implementation Details

The repair process applies statistics truncation in Phase 2 when building add actions:

```scala
// In RepairIndexFilesTransactionLogCommand.run()
val addActions = validatedSplits.map { split =>
  // Apply statistics truncation using shared truncateStatistics() method
  val truncatedStats = split.stats.map { stats =>
    truncateStatistics(
      stats = stats,
      schema = snapshot.getSchema(),
      config = sparkSession.conf.getAll
    )
  }

  AddAction(
    path = split.path,
    partitionValues = split.partitionValues,
    size = split.size,
    modificationTime = split.modificationTime,
    dataChange = true,
    stats = truncatedStats,  // Truncated statistics
    tags = split.tags
  )
}
```

### Configuration Control

Users can control statistics truncation during repair operations:

```scala
// Enable truncation (default)
spark.conf.set("spark.indextables.stats.truncation.enabled", "true")
spark.conf.set("spark.indextables.stats.truncation.maxLength", "1024")
spark.conf.set("spark.indextables.stats.truncation.strategy", "drop")

spark.sql("""
  REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log'
  AT LOCATION 's3://bucket/table/_transaction_log_repaired'
""")

// Disable truncation to preserve original statistics
spark.conf.set("spark.indextables.stats.truncation.enabled", "false")
spark.sql("REPAIR INDEXFILES TRANSACTION LOG ...")
```

### Benefits During Repair

1. **Massive size reduction**: Repaired transaction logs can be 90-98% smaller when original logs contain long text statistics
2. **Faster checkpoint creation**: Smaller statistics mean faster checkpoint file writes
3. **Improved readability**: Repaired logs are easier to inspect and validate
4. **Consistent behavior**: Repaired logs behave identically to newly written tables with truncation enabled

### Use Case Example

**Scenario**: Repair a transaction log for a table with article content fields:

```scala
// Before repair: Transaction log with 50MB of statistics data
// Original min/max values contain full article text (thousands of characters)

spark.sql("""
  REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/articles/_transaction_log'
  AT LOCATION 's3://bucket/articles/_transaction_log_repaired'
""")

// After repair: Transaction log with ~1MB of statistics data
// Long article_content statistics dropped, preserving stats for id, score, etc.
// Result: 98% size reduction, identical query behavior
```

### Shared Implementation

The repair command uses the same `truncateStatistics()` method as write operations, ensuring:
- ✅ Consistent behavior across write and repair operations
- ✅ Single source of truth for truncation logic
- ✅ Unified configuration handling
- ✅ Identical logging and observability

### Testing Additions

Add tests to `RepairIndexFilesTransactionLogReplacementSuite.scala`:

```scala
test("repaired transaction log should truncate long statistics") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // Create table with long text field
    val data = Seq(
      ("doc1", "x" * 2000, 100),
      ("doc2", "y" * 2000, 200)
    ).toDF("id", "long_text", "score")

    // Write WITHOUT truncation to simulate old table
    data.write.format("indextables")
      .option("spark.indextables.stats.truncation.enabled", "false")
      .save(tablePath)

    // Measure original transaction log size
    val originalLogSize = measureTransactionLogSize(s"$tablePath/_transaction_log")
    assert(originalLogSize > 100000, "Original log should be large")

    // Run repair WITH truncation enabled (default)
    spark.sql(s"""
      REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
      AT LOCATION '$repairedPath/_transaction_log'
    """)

    // Measure repaired transaction log size
    val repairedLogSize = measureTransactionLogSize(s"$repairedPath/_transaction_log")

    // Verify significant size reduction
    val reductionPercent = ((originalLogSize - repairedLogSize).toDouble / originalLogSize) * 100
    assert(reductionPercent > 80, s"Expected >80% reduction, got ${reductionPercent}%")

    // Verify data remains readable after replacement
    replaceTransactionLog(tablePath, repairedPath)
    val df = spark.read.format("indextables").load(tablePath)
    assert(df.count() === 2)
  }
}

test("repair with stats truncation disabled preserves original statistics") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // Create table with long text
    val data = Seq(("doc1", "x" * 2000)).toDF("id", "text")
    data.write.format("indextables").save(tablePath)

    // Repair with truncation disabled
    spark.conf.set("spark.indextables.stats.truncation.enabled", "false")
    spark.sql(s"""
      REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
      AT LOCATION '$repairedPath/_transaction_log'
    """)

    // Verify statistics are NOT truncated in repaired log
    val repairedStats = readStatisticsFromTransactionLog(s"$repairedPath/_transaction_log")
    assert(repairedStats("text").minValue.length > 1024, "Stats should not be truncated")
  }
}
```

### Documentation Updates

The repair command documentation should include:
- Statistics truncation is applied by default during repair
- Configuration options to control or disable truncation
- Expected transaction log size benefits
- Cross-reference to `DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md`
