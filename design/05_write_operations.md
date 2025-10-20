# Section 5: Write Operations

**Document Version:** 2.0
**Last Updated:** 2025-10-06

---

## Table of Contents

- [5.1 Write Builder Overview](#51-write-builder-overview)
- [5.2 Write Mode Selection](#52-write-mode-selection)
- [5.3 Standard Write Implementation](#53-standard-write-implementation)
- [5.4 Optimized Write Implementation](#54-optimized-write-implementation)
- [5.5 Auto-Sizing System](#55-auto-sizing-system)
- [5.6 Batch Write Coordination](#56-batch-write-coordination)
- [5.7 Data Writer Factory](#57-data-writer-factory)
- [5.8 Split Creation & Document Indexing](#58-split-creation--document-indexing)
- [5.9 Partitioned Writes](#59-partitioned-writes)
- [5.10 S3 Upload Optimization](#510-s3-upload-optimization)
- [5.11 Transaction Commit Protocol](#511-transaction-commit-protocol)
- [5.12 Configuration Validation](#512-configuration-validation)

---

## 5.1 Write Builder Overview

### Architecture

**Class:** `IndexTables4SparkWriteBuilder`

The `WriteBuilder` serves as the **entry point** for all write operations in the V2 DataSource API. It implements Spark's `WriteBuilder` interface along with write mode capabilities.

**Implemented Interfaces:**
```scala
class IndexTables4SparkWriteBuilder
  extends WriteBuilder
  with SupportsTruncate    // Full table overwrite
  with SupportsOverwrite   // Filter-based overwrite (future)
```

**Key Responsibilities:**
1. **Write mode detection** (append vs overwrite)
2. **Optimized write selection** (standard vs optimized)
3. **Configuration hierarchy resolution**
4. **Write instance creation** with serialized config

### Constructor Parameters

| Parameter | Type | Purpose |
|-----------|------|---------|
| `transactionLog` | `TransactionLog` | Transaction log instance for atomic commits |
| `tablePath` | `Path` | Base table path for split file storage |
| `info` | `LogicalWriteInfo` | Schema and write metadata from Spark |
| `options` | `CaseInsensitiveStringMap` | DataFrame write options |
| `hadoopConf` | `Configuration` | Hadoop configuration for cloud credentials |

### Write Mode Support

#### Truncate Mode
```scala
override def truncate(): WriteBuilder = {
  logger.info("Truncate mode enabled for write operation")
  isOverwrite = true
  this
}
```

**Behavior:** Full table replacement with new data, removing all existing files.

#### Overwrite with Filters
```scala
override def overwrite(filters: Array[Filter]): WriteBuilder = {
  logger.info(s"Overwrite mode enabled with ${filters.length} filters")
  isOverwrite = true
  // TODO: Implement filter-based overwrite (replaceWhere functionality)
  this
}
```

**Current Status:** Filter-based overwrite not yet implemented. All overwrite operations perform full table replacement.

**Future Enhancement:** Implement `replaceWhere` functionality for partition-level or filter-based overwrites (similar to Delta Lake's `replaceWhere`).

---

## 5.2 Write Mode Selection

### Decision Logic

The `build()` method selects between **Standard Write** and **Optimized Write** based on configuration:

```
Write Mode Decision Tree:
├─ optimizeWrite.enabled = true
│  └─> IndexTables4SparkOptimizedWrite
│     • RequiresDistributionAndOrdering
│     • Auto-sizing with historical analysis
│     • Intelligent repartitioning
│
└─ optimizeWrite.enabled = false
   └─> IndexTables4SparkStandardWrite
      • Direct execution (no repartitioning)
      • Manual partition control
      • Compatibility mode
```

### Configuration Hierarchy

The `optimizeWrite.enabled` setting is resolved through a **4-level hierarchy**:

| Priority | Source | Example |
|----------|--------|---------|
| **1 (Highest)** | DataFrame write options | `.option("spark.indextables.optimizeWrite.enabled", "true")` |
| **2** | Spark session configuration | `spark.conf.set("spark.indextables.optimizeWrite.enabled", "true")` |
| **3** | Table properties | Stored in transaction log `MetadataAction` |
| **4 (Default)** | System default | `true` (optimized write is default) |

### Implementation Code

```scala
val optimizeWriteEnabled = tantivyOptions.optimizeWrite.getOrElse {
  // Check Spark session configuration
  spark.conf
    .getOption("spark.indextables.optimizeWrite.enabled")
    .map(_.toBoolean)
    .getOrElse {
      // Check table properties or use default
      try {
        val metadata = transactionLog.getMetadata()
        IndexTables4SparkConfig.OPTIMIZE_WRITE
          .fromMetadata(metadata)
          .getOrElse(
            IndexTables4SparkConfig.OPTIMIZE_WRITE.defaultValue
          )
      } catch {
        case _: Exception =>
          IndexTables4SparkConfig.OPTIMIZE_WRITE.defaultValue
      }
    }
}
```

### Write Instance Creation

**Optimized Write:**
```scala
new IndexTables4SparkOptimizedWrite(
  transactionLog,
  tablePath,
  info,
  serializedOptions,
  hadoopConf,
  isOverwrite,
  estimatedRowCount
)
```

**Standard Write:**
```scala
new IndexTables4SparkStandardWrite(
  transactionLog,
  tablePath,
  info,
  serializedOptions,
  hadoopConf,
  isOverwrite
)
```

**Critical Design Note:** Both write implementations use **serialized options** (`Map[String, String]`) instead of `CaseInsensitiveStringMap` to avoid serialization issues when distributing tasks to executors.

---

## 5.3 Standard Write Implementation

### Overview

**Class:** `IndexTables4SparkStandardWrite`

Standard write provides **simple, direct write execution** without automatic repartitioning or distribution requirements. It uses Spark's default partitioning behavior.

**Interfaces Implemented:**
```scala
class IndexTables4SparkStandardWrite
  extends Write
  with BatchWrite
  with Serializable
```

**Key Characteristic:** Does NOT implement `RequiresDistributionAndOrdering`, allowing Spark to use its default partitioning strategy.

### Use Cases

**When to Use Standard Write:**
1. **Small datasets** (< 1GB) where repartitioning overhead outweighs benefits
2. **Manual partitioning control** when you've already optimized DataFrame partitioning
3. **Testing/debugging** to avoid auto-optimization complexity
4. **Compatibility mode** for legacy workflows or specific edge cases

**Example Configuration:**
```scala
df.write
  .option("spark.indextables.optimizeWrite.enabled", "false")
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")
```

### Serialization Strategy

To avoid Hadoop `Configuration` serialization issues, the implementation **serializes only relevant config properties**:

```scala
private val serializedHadoopConf = {
  val props = scala.collection.mutable.Map[String, String]()
  val iter  = hadoopConf.iterator()
  while (iter.hasNext) {
    val entry = iter.next()
    if (entry.getKey.startsWith("spark.indextables.")) {
      props.put(entry.getKey, entry.getValue)
    }
  }
  props.toMap
}
```

**Benefits:**
- Avoids Hadoop `Configuration` serialization errors
- Reduces serialized payload size
- Only transmits IndexTables4Spark-specific settings

### Partition Column Extraction

Standard write extracts partition column information from **DataFrame write options**:

```scala
serializedOptions.get("__partition_columns") match {
  case Some(partitionColumnsJson) =>
    // Parse JSON array: ["col1","col2"]
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val partitionCols = mapper.readValue(
      partitionColumnsJson,
      classOf[Array[String]]
    ).toSeq

  case None =>
    // Fallback: read from transaction log
    transactionLog.getPartitionColumns()
}
```

**Spark Internal Detail:** When using `.partitionBy()`, Spark sets a special `__partition_columns` option containing a JSON array of column names.

### Batch Writer Factory Creation

```scala
override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
  logger.info(s"Creating standard batch writer factory for ${info.numPartitions} partitions")
  logger.info(s"Using Spark's default partitioning (no optimization)")

  // Combine serialized configs
  val combinedHadoopConfig = serializedHadoopConf ++ normalizedTantivyOptions

  new IndexTables4SparkWriterFactory(
    tablePath,
    writeSchema,
    serializedOptions,
    combinedHadoopConfig,
    partitionColumns
  )
}
```

### Commit Logic

**Standard write commit flow:**

1. **Extract `AddAction` messages** from all task executors
2. **Filter empty partitions** (0 records)
3. **Determine write mode** (append vs overwrite)
4. **Initialize transaction log** with schema and partition columns
5. **Atomic commit** to transaction log

**Overwrite Detection:**
```scala
val shouldOverwrite = if (isOverwrite) {
  true  // Explicit from truncate()/overwrite()
} else {
  serializedOptions.get("saveMode") match {
    case Some("Overwrite") => true
    case Some("Append") | Some("ErrorIfExists") | Some("Ignore") => false
    case None =>
      // Initial write detection
      val existingFiles = transactionLog.listFiles()
      existingFiles.isEmpty
  }
}
```

**Transaction Log Operations:**
```scala
if (shouldOverwrite) {
  transactionLog.overwriteFiles(addActions)  // Removes all existing files
} else {
  transactionLog.addFiles(addActions)        // Append new files
}
```

### Configuration Validation

**Append operations validate field type configuration** to prevent schema conflicts:

```scala
private def validateIndexingConfigurationForAppend(): Unit = {
  val existingDocMapping = transactionLog.listFiles()
    .flatMap(_.docMappingJson)
    .headOption

  if (existingDocMapping.isDefined) {
    // Parse existing field types
    val existingMapping = JsonUtil.mapper.readTree(existingDocMapping.get)

    writeSchema.fields.foreach { field =>
      val currentConfig = tantivyOptions.getFieldIndexingConfig(field.name)
      val existingFieldConfig = existingMapping.find(_.get("name").asText() == field.name)

      // Validate field type compatibility
      if (currentConfig.fieldType.isDefined && existingFieldConfig.isDefined) {
        val currentType = currentConfig.fieldType.get
        val existingType = existingFieldConfig.get.get("type").asText()

        if (existingType != currentType) {
          throw new IllegalArgumentException(
            s"Field '${field.name}' type mismatch: " +
            s"existing table has $existingType field, " +
            s"cannot append with $currentType configuration"
          )
        }
      }
    }
  }
}
```

**Validation Rules:**
- **Text → String**: ❌ Not allowed (tokenization conflict)
- **String → Text**: ❌ Not allowed (exact match conflict)
- **String → String**: ✅ Allowed (compatible)
- **Text → Text**: ✅ Allowed (compatible)

---

## 5.4 Optimized Write Implementation

> **⚠️ PRODUCTION NOTICE:** Optimized write capabilities (`IndexTables4SparkOptimizedWrite`, `RequiresDistributionAndOrdering`, and auto-sizing features) have not been extensively integration tested in real-world production environments. While comprehensive unit tests validate the implementation (28/28 auto-sizing tests passing), users should exercise caution and perform thorough testing in staging environments before deploying to production workloads. Consider starting with standard write mode for critical production systems until additional production validation is available.

### Overview

**Class:** `IndexTables4SparkOptimizedWrite`

Optimized write implements **intelligent DataFrame repartitioning** to achieve target split sizes, similar to Delta Lake's optimized write functionality.

**Key Interface:**
```scala
class IndexTables4SparkOptimizedWrite
  extends Write
  with BatchWrite
  with RequiresDistributionAndOrdering  // ← Controls Spark's shuffle
  with Serializable
```

**Critical Feature:** The `RequiresDistributionAndOrdering` interface allows controlling Spark's physical plan execution, enabling automatic repartitioning based on target split sizes.

### RequiresDistributionAndOrdering Implementation

This interface provides **three key methods** that control Spark's shuffle behavior:

#### 1. Required Distribution

```scala
override def requiredDistribution(): Distribution = {
  if (partitionColumns.nonEmpty) {
    // Cluster by partition columns for partitioned tables
    val clusteredColumns = partitionColumns.toArray
    Distributions.clustered(clusteredColumns.map(Expressions.identity))

  } else if (writeSchema.fields.nonEmpty) {
    // Cluster by first schema field for non-partitioned tables
    Distributions.clustered(
      Array(Expressions.identity(writeSchema.fields(0).name))
    )

  } else {
    // Fallback: no specific distribution requirement
    Distributions.unspecified()
  }
}
```

**Distribution Strategies:**
- **Partitioned tables:** Cluster by partition columns to co-locate partition values
- **Non-partitioned tables:** Cluster by first field for deterministic distribution
- **Empty schema:** Use Spark's default distribution

#### 2. Required Number of Partitions

This is the **most critical method** for auto-sizing:

```scala
override def requiredNumPartitions(): Int = {
  val targetRecords = getTargetRecordsPerSplit()
  val actualRowCount = /* row count from auto-sizing or estimate */

  val numPartitions = math.ceil(actualRowCount.toDouble / targetRecords).toInt
  val finalPartitions = math.max(1, numPartitions)

  logger.info(
    s"Auto-sizing: Requesting $finalPartitions partitions " +
    s"for $actualRowCount records with target $targetRecords per split"
  )

  finalPartitions
}
```

**Calculation:**
```
numPartitions = ⌈actualRowCount / targetRecordsPerSplit⌉
```

**Example:**
- **Input:** 5,000,000 records
- **Target:** 1,000,000 records per split
- **Result:** 5 partitions (each creating ~1M record split)

#### 3. Required Ordering

```scala
override def requiredOrdering(): Array[SortOrder] = Array.empty
```

**Note:** IndexTables4Spark does not require any specific ordering for writes.

### Target Records Per Split Configuration

The `getTargetRecordsPerSplit()` method implements a **5-level configuration hierarchy**:

| Priority | Source | When Used |
|----------|--------|-----------|
| **1** | Auto-sizing from historical data | If `autoSizeEnabled = true` and historical splits exist |
| **2** | DataFrame write options | `.option("targetRecordsPerSplit", "500000")` |
| **3** | Spark session configuration | `spark.conf.set("spark.indextables.optimizeWrite.targetRecordsPerSplit", "500000")` |
| **4** | Table properties | Stored in transaction log metadata |
| **5 (Default)** | System default | `1,000,000` records |

**Implementation:**
```scala
private def getTargetRecordsPerSplit(): Long = {
  val spark = SparkSession.active

  // Priority 1: Auto-sizing
  val autoSizeEnabled = tantivyOptions.autoSizeEnabled.getOrElse {
    spark.conf
      .getOption("spark.indextables.autoSize.enabled")
      .map(_.toBoolean)
      .getOrElse(false)
  }

  if (autoSizeEnabled) {
    val targetSizeBytes = /* parse from config */
    val analyzer = SplitSizeAnalyzer(tablePath, spark, options)

    analyzer.calculateTargetRows(targetSizeBytes) match {
      case Some(calculatedRows) =>
        logger.info(s"Auto-sizing calculated: $calculatedRows rows per split")
        return calculatedRows
      case None =>
        logger.warn("Auto-sizing failed, falling back to manual config")
    }
  }

  // Priority 2-5: Manual configuration
  tantivyOptions.targetRecordsPerSplit
    .orElse(sessionConfig)
    .orElse(tablePropertyConfig)
    .getOrElse(1000000L)
}
```

### Batch Writer Factory Creation

Optimized write creates the same `DataWriterFactory` as standard write:

```scala
override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
  logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")
  logger.info(s"V2 optimized write enabled, target records per split: $targetRecords")
  logger.info(s"Actual partitions: ${info.numPartitions} (controlled by requiredNumPartitions)")

  new IndexTables4SparkWriterFactory(
    tablePath,
    writeSchema,
    serializedOptions,
    combinedHadoopConfig,
    partitionColumns
  )
}
```

**Key Difference:** The number of partitions (`info.numPartitions`) is controlled by `requiredNumPartitions()`, not Spark's default logic.

### Commit Protocol

Optimized write commit is similar to standard write, with additional partition column extraction:

```scala
override def commit(messages: Array[WriterCommitMessage]): Unit = {
  // Extract partition columns from serialized options
  val partitionColumns = serializedOptions.get("__partition_columns") match {
    case Some(json) => parsePartitionColumns(json)
    case None => Seq.empty
  }

  // Initialize transaction log
  transactionLog.initialize(writeInfo.schema(), partitionColumns)

  // Atomic commit
  if (shouldOverwrite) {
    transactionLog.overwriteFiles(addActions)
  } else {
    transactionLog.addFiles(addActions)
  }
}
```

---

## 5.5 Auto-Sizing System

> **⚠️ PRODUCTION NOTICE:** Auto-sizing features have not been extensively integration tested in real-world production environments. While unit tests validate correctness (28/28 tests passing), production users should validate behavior in staging environments first. Monitor initial production deployments carefully to ensure split sizes meet expectations.

### Overview

Auto-sizing provides **intelligent DataFrame repartitioning** based on historical split size analysis, eliminating manual tuning of `targetRecordsPerSplit`.

**Key Components:**
1. **Historical split analysis** from transaction log
2. **Bytes-per-record calculation** weighted by record count
3. **Target rows calculation** based on desired split size
4. **Dynamic repartitioning** using `requiredNumPartitions()`

### Configuration

**Enable Auto-Sizing:**
```scala
df.write
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/path")
```

**Size Format Support:**
| Format | Example | Bytes |
|--------|---------|-------|
| Bytes | `"104857600"` | 104,857,600 |
| Kilobytes | `"512K"` | 524,288 |
| Megabytes | `"100M"` | 104,857,600 |
| Gigabytes | `"2G"` | 2,147,483,648 |

**Case Insensitive:** `"100m"`, `"2g"`, `"512k"` are all valid.

### Auto-Sizing Workflow

```
Auto-Sizing Pipeline:
1. Read recent splits from transaction log
   └─> SELECT path, size, numRecords FROM transaction_log
       WHERE numRecords IS NOT NULL
       ORDER BY modificationTime DESC
       LIMIT 10

2. Calculate weighted bytes-per-record
   └─> avgBytesPerRecord = SUM(size * numRecords) / SUM(numRecords)
       (weighted by record count for accuracy)

3. Calculate target rows per split
   └─> targetRows = ⌈targetSizeBytes / avgBytesPerRecord⌉

4. Get or count DataFrame rows
   └─> V2 API: explicit count required
       V1 API: automatic df.count()

5. Calculate required partitions
   └─> numPartitions = ⌈rowCount / targetRows⌉

6. Spark repartitions DataFrame
   └─> df.repartition(numPartitions)
```

### SplitSizeAnalyzer

**Class:** `SplitSizeAnalyzer`

**Responsibility:** Analyze historical splits and calculate optimal target rows.

```scala
class SplitSizeAnalyzer(
  tablePath: Path,
  spark: SparkSession,
  options: CaseInsensitiveStringMap
) {
  def calculateTargetRows(targetSizeBytes: Long): Option[Long] = {
    val addActions = transactionLog.listFiles().take(10)

    // Filter splits with valid size and record count
    val validSplits = addActions.filter { action =>
      action.size > 0 &&
      action.numRecords.exists(_ > 0)
    }

    if (validSplits.isEmpty) {
      logger.warn("No valid historical splits for auto-sizing")
      return None
    }

    // Calculate weighted average bytes per record
    val totalBytes = validSplits.map(_.size).sum
    val totalRecords = validSplits.flatMap(_.numRecords).sum
    val avgBytesPerRecord = totalBytes.toDouble / totalRecords

    // Calculate target rows
    val targetRows = math.ceil(targetSizeBytes / avgBytesPerRecord).toLong

    logger.info(
      s"Auto-sizing analysis: " +
      s"avgBytesPerRecord=$avgBytesPerRecord, " +
      s"targetRows=$targetRows"
    )

    Some(targetRows)
  }
}
```

### V1 vs V2 API Auto-Sizing

**V1 API (Automatic Count):**
```scala
// V1 automatically counts DataFrame when auto-sizing enabled
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/path")
```

**V2 API (Explicit Count Required):**
```scala
// V2 requires explicit row count for optimal auto-sizing
val rowCount = df.count()
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .option("spark.indextables.autoSize.inputRowCount", rowCount.toString)
  .save("s3://bucket/path")
```

**V2 API Behavior without Explicit Count:**
```scala
explicitRowCount match {
  case Some(rowCount) =>
    logger.info(s"V2 Auto-sizing: using explicit row count = $rowCount")
    rowCount

  case None =>
    logger.warn(
      "V2 Auto-sizing: enabled but no explicit row count provided. " +
      "Using estimated row count for partitioning. " +
      "For accurate auto-sizing with V2 API, call df.count() first"
    )
    estimatedRowCount  // Falls back to estimate
}
```

### Fallback Behavior

**Auto-sizing gracefully falls back to manual configuration** when:
1. **No historical data:** Transaction log has no existing splits with size/count metadata
2. **Invalid historical data:** All splits have missing or zero size/record count
3. **Analysis failure:** Exception during historical split analysis
4. **Missing configuration:** `targetSplitSize` not specified

**Fallback Log Example:**
```
WARN Auto-sizing failed to calculate target rows from historical data,
     falling back to manual configuration
```

---

## 5.6 Batch Write Coordination

### Overview

**Class:** `IndexTables4SparkBatchWrite` (legacy V1 implementation)

The batch write coordinator is responsible for:
1. **Creating writer factories** for distributed execution
2. **Collecting commit messages** from executors
3. **Atomic transaction log commits**
4. **Abort handling** for failed writes

**Note:** Both `StandardWrite` and `OptimizedWrite` implement `BatchWrite` directly, while `IndexTables4SparkBatchWrite` is the legacy V1 implementation.

### Writer Factory Creation

```scala
override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
  logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")

  // Enrich Hadoop configuration with DataFrame options
  val enrichedHadoopConf = new Configuration(hadoopConf)
  options.entrySet().asScala.foreach { entry =>
    if (entry.getKey.startsWith("spark.indextables.")) {
      enrichedHadoopConf.set(entry.getKey, entry.getValue)
    }
  }

  // Serialize configuration for executor distribution
  val serializedHadoopConfig = extractSerializedConfig(enrichedHadoopConf)

  new IndexTables4SparkWriterFactory(
    tablePath,
    writeInfo.schema(),
    serializedOptions,
    serializedHadoopConfig
  )
}
```

**Configuration Propagation Strategy:**
1. **Start with Hadoop configuration** (contains AWS credentials, etc.)
2. **Overlay DataFrame options** (user-specified write options)
3. **Serialize to Map[String, String]** (avoid Configuration serialization issues)
4. **Transmit to executors** via DataWriterFactory

### Commit Message Collection

**Distributed Execution Flow:**
```
Driver                          Executor 1              Executor 2              Executor N
  │                                 │                       │                       │
  │  createBatchWriterFactory()    │                       │                       │
  ├──────────────────────────────>│                       │                       │
  │                                │ write(row1)           │                       │
  │                                │ write(row2)           │                       │
  │                                │ ...                   │                       │
  │                                │ commit()              │                       │
  │                                │   └─> AddAction       │                       │
  │<───────────────────────────────┤                       │                       │
  │                                                        │ write(rowN)           │
  │                                                        │ commit()              │
  │                                                        │   └─> AddAction       │
  │<────────────────────────────────────────────────────────┤                       │
  │                                                                                │ commit()
  │                                                                                │   └─> AddAction
  │<────────────────────────────────────────────────────────────────────────────────┤
  │
  │  collect all AddActions
  │  transactionLog.addFiles(allAddActions)
  │
```

### Commit Implementation

```scala
override def commit(messages: Array[WriterCommitMessage]): Unit = {
  // Extract partition columns from options
  val partitionColumns = parsePartitionColumns(options)

  // Initialize transaction log
  transactionLog.initialize(writeInfo.schema(), partitionColumns)

  // Collect AddActions from all executors
  val addActions: Seq[AddAction] = messages.flatMap {
    case msg: IndexTables4SparkCommitMessage => msg.addActions
    case _ => Seq.empty[AddAction]
  }

  // Filter empty partitions
  val emptyPartitionsCount = messages.length - addActions.length
  if (emptyPartitionsCount > 0) {
    logger.info(
      s"Filtered out $emptyPartitionsCount empty partitions " +
      s"(0 records) from transaction log"
    )
  }

  // Atomic commit
  val version = transactionLog.addFiles(addActions)
  logger.info(s"Added ${addActions.length} files in transaction version $version")
}
```

**Empty Partition Handling:** Executors that process zero records return `IndexTables4SparkCommitMessage(Seq.empty)`. The driver filters these out before transaction log commit.

### Abort Implementation

```scala
override def abort(messages: Array[WriterCommitMessage]): Unit = {
  logger.warn(s"Aborting write with ${messages.length} messages")

  val addActions: Seq[AddAction] = messages.flatMap {
    case msg: IndexTables4SparkCommitMessage => msg.addActions
    case _ => Seq.empty[AddAction]
  }

  // TODO: Delete physical split files from S3
  logger.warn(s"Would clean up ${addActions.length} uncommitted files")
}
```

**Current Limitation:** Physical file cleanup is not yet implemented. In case of write failure, split files may remain in S3 but won't be visible in the transaction log.

**Future Enhancement:** Implement file deletion using `CloudStorageProvider` to remove uncommitted splits.

---

## 5.7 Data Writer Factory

### Overview

**Class:** `IndexTables4SparkWriterFactory`

The writer factory creates **one `DataWriter` instance per Spark partition**, enabling parallel split creation across the cluster.

**Key Responsibilities:**
1. **Reconstruct Hadoop Configuration** from serialized properties
2. **Create DataWriter instances** for each task partition
3. **Preserve partition column information** for partitioned writes

### Factory Implementation

```scala
class IndexTables4SparkWriterFactory(
  tablePath: Path,
  writeSchema: StructType,
  serializedOptions: Map[String, String],
  serializedHadoopConfig: Map[String, String],
  partitionColumns: Seq[String] = Seq.empty
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating writer for partition $partitionId, task $taskId")

    // Reconstruct Hadoop Configuration
    val reconstructedHadoopConf = new Configuration()
    serializedHadoopConfig.foreach { case (key, value) =>
      reconstructedHadoopConf.set(key, value)
    }

    new IndexTables4SparkDataWriter(
      tablePath,
      writeSchema,
      partitionId,
      taskId,
      serializedOptions,
      reconstructedHadoopConf,
      partitionColumns
    )
  }
}
```

**Serialization Design:**
- **Input:** Serialized `Map[String, String]` (safe for Spark serialization)
- **Output:** Reconstructed `Configuration` object (required by tantivy4java)

---

## 5.8 Split Creation & Document Indexing

### Overview

**Class:** `IndexTables4SparkDataWriter`

Each `DataWriter` instance creates **one or more Tantivy index splits** from the rows in its Spark partition.

**Key Components:**
1. **TantivySearchEngine:** Manages Tantivy index creation and document addition
2. **StatisticsCalculator:** Computes min/max values for data skipping
3. **Split file creation:** Exports Tantivy index as QuickwitSplit format
4. **S3 upload:** Transfers split files to cloud storage

### DataWriter Lifecycle

```
DataWriter Lifecycle:
1. Construction
   └─> Create TantivySearchEngine instance
   └─> Initialize StatisticsCalculator

2. write(row) × N
   └─> engine.addDocument(row)
   └─> statistics.updateRow(row)

3. commit()
   └─> engine.commitAndCreateSplit(outputPath)
   └─> Upload split to S3
   └─> Calculate statistics (min/max values)
   └─> Create AddAction metadata
   └─> Return IndexTables4SparkCommitMessage
```

### Write Method

**Non-Partitioned Writes:**
```scala
override def write(record: InternalRow): Unit = {
  val (engine, stats, count) = singleWriter.get
  engine.addDocument(record)
  stats.updateRow(record)
  singleWriter = Some((engine, stats, count + 1))
}
```

**Partitioned Writes:**
```scala
override def write(record: InternalRow): Unit = {
  // Extract partition values
  val partitionValues = PartitionUtils.extractPartitionValues(
    record,
    writeSchema,
    partitionColumns
  )
  val partitionKey = PartitionUtils.createPartitionPath(
    partitionValues,
    partitionColumns
  )

  // Get or create writer for this partition value combination
  val (engine, stats, count) = partitionWriters.getOrElseUpdate(
    partitionKey,
    (
      new TantivySearchEngine(writeSchema, options, hadoopConf),
      new StatisticsCalculator.DatasetStatistics(writeSchema),
      0L
    )
  )

  // Store complete record in split (including partition columns)
  engine.addDocument(record)
  stats.updateRow(record)
  partitionWriters(partitionKey) = (engine, stats, count + 1)
}
```

**Partition Key Format:**
```
partitionKey = "date=2024-01-15/hour=10"
```

**Multi-Partition Handling:** A single DataWriter can create **multiple splits** if it receives rows with different partition values. Each unique partition value combination gets its own `TantivySearchEngine` instance.

### Commit Method

**Non-Partitioned Commit:**
```scala
override def commit(): WriterCommitMessage = {
  if (singleWriter.isDefined) {
    val (searchEngine, statistics, recordCount) = singleWriter.get

    if (recordCount == 0) {
      return IndexTables4SparkCommitMessage(Seq.empty)
    }

    val addAction = commitWriter(
      searchEngine,
      statistics,
      recordCount,
      Map.empty,
      ""
    )
    IndexTables4SparkCommitMessage(Seq(addAction))
  }
}
```

**Partitioned Commit:**
```scala
override def commit(): WriterCommitMessage = {
  val allAddActions = partitionWriters.flatMap {
    case (partitionKey, (engine, stats, count)) =>
      if (count > 0) {
        val partitionValues = parsePartitionKey(partitionKey)
        Some(commitWriter(engine, stats, count, partitionValues, partitionKey))
      } else {
        None  // Skip empty partitions
      }
  }.toSeq

  IndexTables4SparkCommitMessage(allAddActions)
}
```

### Split File Creation

The `commitWriter()` method handles **split file creation and metadata generation**:

```scala
private def commitWriter(
  searchEngine: TantivySearchEngine,
  statistics: StatisticsCalculator.DatasetStatistics,
  recordCount: Long,
  partitionValues: Map[String, String],
  partitionKey: String
): AddAction = {

  // Generate split file name
  val splitId = UUID.randomUUID().toString
  val fileName = f"part-$partitionId%05d-$taskId-$splitId.split"

  // Determine file path (with partition directory if applicable)
  val filePath = if (partitionValues.nonEmpty) {
    val partitionDir = new Path(tablePath, partitionKey)
    new Path(partitionDir, fileName)
  } else {
    new Path(tablePath, fileName)
  }

  // Normalize path for tantivy4java
  val outputPath = normalizePathForTantivy(filePath)

  // Generate node ID
  val nodeId = java.net.InetAddress.getLocalHost.getHostName + "-" +
    Option(System.getProperty("spark.executor.id")).getOrElse("driver")

  // Create split from Tantivy index
  val (splitPath, splitMetadata) = searchEngine.commitAndCreateSplit(
    outputPath,
    partitionId.toLong,
    nodeId
  )

  // Get split file size
  val splitSize = getFileSizeFromS3(outputPath)

  // Extract metadata from tantivy4java
  val (
    footerStartOffset,
    footerEndOffset,
    hasFooterOffsets,
    timeRangeStart,
    timeRangeEnd,
    splitTags,
    deleteOpstamp,
    numMergeOps,
    docMappingJson,
    uncompressedSizeBytes
  ) = extractSplitMetadata(splitMetadata)

  // Create AddAction
  AddAction(
    path = calculateRelativePath(outputPath),
    partitionValues = partitionValues,
    size = splitSize,
    modificationTime = System.currentTimeMillis(),
    dataChange = true,
    numRecords = Some(recordCount),
    minValues = statistics.getMinValues,
    maxValues = statistics.getMaxValues,
    footerStartOffset = footerStartOffset,
    footerEndOffset = footerEndOffset,
    hasFooterOffsets = hasFooterOffsets,
    timeRangeStart = timeRangeStart,
    timeRangeEnd = timeRangeEnd,
    splitTags = splitTags,
    deleteOpstamp = deleteOpstamp,
    numMergeOps = numMergeOps,
    docMappingJson = docMappingJson,
    uncompressedSizeBytes = uncompressedSizeBytes
  )
}
```

**Split File Path Examples:**

**Non-Partitioned (S3):**
```
s3://bucket/table/part-00000-1234567890-abc123.split
```

**Non-Partitioned (Azure ADLS Gen2):**
```
abfss://container@account.dfs.core.windows.net/table/part-00000-1234567890-abc123.split
```

**Partitioned (S3):**
```
s3://bucket/table/date=2024-01-15/hour=10/part-00000-1234567890-abc123.split
```

**Partitioned (Azure):**
```
abfss://container@account.dfs.core.windows.net/table/date=2024-01-15/hour=10/part-00000-1234567890-abc123.split
```

### AddAction Metadata

The `AddAction` contains **comprehensive split metadata** for optimal read performance:

| Field | Type | Purpose |
|-------|------|---------|
| `path` | `String` | Relative path to split file |
| `partitionValues` | `Map[String, String]` | Partition column values |
| `size` | `Long` | Split file size in bytes |
| `numRecords` | `Option[Long]` | Record count for auto-sizing |
| `minValues` | `Option[Map[String, Any]]` | Min values for data skipping |
| `maxValues` | `Option[Map[String, Any]]` | Max values for data skipping |
| `footerStartOffset` | `Option[Long]` | Footer start byte offset |
| `footerEndOffset` | `Option[Long]` | Footer end byte offset |
| `docMappingJson` | `Option[String]` | Tantivy field schema JSON |
| `uncompressedSizeBytes` | `Option[Long]` | Uncompressed data size |
| `timeRangeStart` | `Option[String]` | Earliest timestamp in split |
| `timeRangeEnd` | `Option[String]` | Latest timestamp in split |
| `splitTags` | `Option[Set[String]]` | Split tags for filtering |
| `deleteOpstamp` | `Option[Long]` | Delete operation stamp |
| `numMergeOps` | `Option[Int]` | Number of merge operations |

---

## 5.9 Partitioned Writes

### Partition Value Extraction

**Utility:** `PartitionUtils.extractPartitionValues()`

```scala
object PartitionUtils {
  def extractPartitionValues(
    row: InternalRow,
    schema: StructType,
    partitionColumns: Seq[String]
  ): Map[String, String] = {

    partitionColumns.map { colName =>
      val fieldIndex = schema.fieldIndex(colName)
      val field = schema.fields(fieldIndex)
      val value = row.get(fieldIndex, field.dataType)

      val stringValue = convertToString(value, field.dataType)
      colName -> stringValue
    }.toMap
  }

  private def convertToString(value: Any, dataType: DataType): String = {
    dataType match {
      case DateType =>
        val days = value.asInstanceOf[Int]
        java.time.LocalDate.ofEpochDay(days.toLong).toString

      case TimestampType =>
        val micros = value.asInstanceOf[Long]
        java.time.Instant.ofEpochMilli(micros / 1000).toString

      case _ =>
        value.toString
    }
  }
}
```

### Partition Directory Structure

**Directory Creation:**
```scala
def createPartitionPath(
  partitionValues: Map[String, String],
  partitionColumns: Seq[String]
): String = {
  partitionColumns
    .map { col => s"$col=${partitionValues(col)}" }
    .mkString("/")
}
```

**Example:**
```scala
partitionColumns = Seq("year", "month", "day")
partitionValues = Map("year" -> "2024", "month" -> "01", "day" -> "15")

partitionPath = "year=2024/month=01/day=15"
```

### Physical Layout

**Partitioned Table Structure (S3 example):**
```
s3://bucket/table/
├── _transaction_log/
│   ├── 000000000000000000.json
│   ├── 000000000000000001.json
│   └── _last_checkpoint
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   ├── part-00000-123-abc.split
│   │   │   └── part-00001-456-def.split
│   │   └── day=02/
│   │       └── part-00000-789-ghi.split
│   └── month=02/
│       └── day=01/
│           └── part-00000-321-jkl.split
```

**Partitioned Table Structure (Azure ADLS Gen2 example):**
```
abfss://container@account.dfs.core.windows.net/table/
├── _transaction_log/
│   ├── 000000000000000000.json
│   ├── 000000000000000001.json
│   └── _last_checkpoint
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   ├── part-00000-123-abc.split
│   │   │   └── part-00001-456-def.split
│   │   └── day=02/
│   │       └── part-00000-789-ghi.split
│   └── month=02/
│       └── day=01/
│           └── part-00000-321-jkl.split
```

**AddAction Path Storage:**
```json
{
  "add": {
    "path": "year=2024/month=01/day=01/part-00000-123-abc.split",
    "partitionValues": {
      "year": "2024",
      "month": "01",
      "day": "01"
    },
    ...
  }
}
```

### Data Storage Strategy

**Critical Design Decision:** Partition column values are **stored directly in split data**, not just in directory structure.

**Benefits:**
1. **Full filter pushdown** on partition columns (can query partition values in Tantivy)
2. **Consistent with Quickwit** split-based architecture
3. **Simplifies read logic** (no partition value reconstruction needed)
4. **Enables flexible partition evolution** (can change partition scheme without rewriting data)

**Implementation:**
```scala
// Partition columns are included in writeSchema sent to TantivySearchEngine
engine.addDocument(record)  // Complete record with partition values
```

---

## 5.10 Cloud Storage Upload Optimization

### Upload Strategy Selection

IndexTables4Spark uses **intelligent upload strategy selection** based on file size:

```
Upload Strategy Decision:
├─ splitSize < 100MB (default threshold)
│  └─> Direct upload
│     • Single PUT operation (S3/Azure)
│     • Simple and fast for small files
│
└─ splitSize >= 100MB
   └─> Parallel streaming multipart upload
      • Memory-efficient streaming
      • Configurable concurrency (S3)
      • Supports files up to 5TB
```

### Configuration

**S3 Upload Settings (AWS):**

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.indextables.s3.streamingThreshold` | `104857600` (100MB) | Files larger than this use streaming upload |
| `spark.indextables.s3.multipartThreshold` | `104857600` (100MB) | Threshold for multipart upload |
| `spark.indextables.s3.maxConcurrency` | `4` | Parallel upload threads |
| `spark.indextables.s3.partSize` | `67108864` (64MB) | Multipart upload part size |

**Azure Upload Settings:**
- Azure uploads use native Azure Blob Storage SDK with automatic optimization
- All uploads are handled through `AzureCloudStorageProvider`
- OAuth bearer tokens are automatically refreshed when using Service Principal authentication

**Example Configuration (AWS S3):**
```scala
df.write
  .option("spark.indextables.s3.maxConcurrency", "12")
  .option("spark.indextables.s3.partSize", "134217728")  // 128MB
  .save("s3://bucket/path")
```

**Example Configuration (Azure):**
```scala
df.write
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.accountKey", "your-account-key")
  .save("abfss://container@mystorageaccount.dfs.core.windows.net/path")
```

### Small File Upload

**Direct PUT Operation (S3 PutObject / Azure Put Blob):**

**AWS S3:**
```java
// Read entire file into memory
byte[] fileContent = Files.readAllBytes(splitFilePath);

// Upload in single operation
s3Client.putObject(
  PutObjectRequest.builder()
    .bucket(bucket)
    .key(key)
    .build(),
  RequestBody.fromBytes(fileContent)
);
```

**Azure Blob Storage:**
```java
// Upload directly via CloudStorageProvider
cloudStorageProvider.putObject(
  normalizedPath,
  fileContent
);
// Automatically uses Azure SDK BlobClient.upload()
```

**Characteristics:**
- **Fast:** Single network round-trip
- **Simple:** No part management overhead
- **Memory:** Entire file loaded into memory
- **Limit:** Practical for files < 100MB

### Large File Upload (Parallel Streaming)

**Note:** Parallel streaming multipart upload is currently implemented for AWS S3 only. Azure uploads use the Azure Blob Storage SDK's native upload method.

**Buffered Chunking Strategy (S3):**

```java
// Create multipart upload
CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(
  CreateMultipartUploadRequest.builder()
    .bucket(bucket)
    .key(key)
    .build()
);

// Stream file in chunks with parallel uploads
val executorService = Executors.newFixedThreadPool(maxConcurrency)
val completedParts = Collections.synchronizedList(new ArrayList[CompletedPart]())

// Read stream in chunks
var partNumber = 1
val buffer = new byte[partSize]
var bytesRead = 0

while ((bytesRead = inputStream.read(buffer)) > 0) {
  val partData = Arrays.copyOf(buffer, bytesRead)
  val currentPartNumber = partNumber

  // Submit part upload to thread pool
  executorService.submit(new Runnable {
    def run(): Unit = {
      val uploadPartResponse = s3Client.uploadPart(
        UploadPartRequest.builder()
          .bucket(bucket)
          .key(key)
          .uploadId(multipartUpload.uploadId())
          .partNumber(currentPartNumber)
          .build(),
        RequestBody.fromBytes(partData)
      )

      completedParts.add(
        CompletedPart.builder()
          .partNumber(currentPartNumber)
          .eTag(uploadPartResponse.eTag())
          .build()
      )
    }
  })

  partNumber += 1
}

// Wait for all parts to complete
executorService.shutdown()
executorService.awaitTermination(1, TimeUnit.HOURS)

// Complete multipart upload
s3Client.completeMultipartUpload(
  CompleteMultipartUploadRequest.builder()
    .bucket(bucket)
    .key(key)
    .uploadId(multipartUpload.uploadId())
    .multipartUpload(
      CompletedMultipartUpload.builder()
        .parts(completedParts)
        .build()
    )
    .build()
)
```

**Performance Characteristics:**
- **Memory:** Only `partSize * maxConcurrency` bytes in memory
- **Throughput:** Scales linearly with `maxConcurrency` up to network limits
- **Resilience:** Individual part failures can be retried
- **Supports:** Files up to 5TB (10,000 parts × 500MB max part size)

### Backpressure Management

**Buffer Queue Control:**
```java
// Limit concurrent in-flight parts to prevent OOM
val semaphore = new Semaphore(maxConcurrency)

while (reading parts) {
  semaphore.acquire()  // Block if too many parts in flight

  executorService.submit(new Runnable {
    def run(): Unit = {
      try {
        uploadPart(...)
      } finally {
        semaphore.release()  // Allow next part to proceed
      }
    }
  })
}
```

**Benefits:**
- **Prevents OOM:** Limits memory usage to `partSize * maxConcurrency`
- **Maintains throughput:** Keeps network saturated without overwhelming memory
- **Adaptive:** Automatically adjusts to upload speed

---

## 5.11 Transaction Commit Protocol

### Atomic Commit Guarantee

IndexTables4Spark provides **ACID transaction guarantees** through atomic transaction log commits:

```
Transaction Commit Flow:
1. All executors create splits and upload to S3
   └─> Physical files exist but not visible

2. All executors return AddAction metadata
   └─> No transaction log entry yet

3. Driver collects all AddActions
   └─> Single atomic array of metadata

4. Driver writes transaction log entry
   └─> Atomic file creation (JSON)

5. Transaction log entry visible
   └─> ALL splits now visible to readers
```

**Atomicity:** Either **all** splits from a write become visible, or **none** do. There's no partial visibility.

### Transaction Log Entry Format

**Append Operation:**
```json
{
  "add": {
    "path": "part-00000-123-abc.split",
    "partitionValues": {},
    "size": 52428800,
    "modificationTime": 1704326400000,
    "dataChange": true,
    "numRecords": 1000000,
    "minValues": {"timestamp": "2024-01-01T00:00:00Z"},
    "maxValues": {"timestamp": "2024-01-01T23:59:59Z"},
    "footerStartOffset": 52428000,
    "footerEndOffset": 52428800,
    "hasFooterOffsets": true,
    "docMappingJson": "{\"fields\": {...}}",
    "uncompressedSizeBytes": 104857600
  }
}
```

**Overwrite Operation:**

Creates a transaction with:
1. **Remove actions** for all existing files
2. **Add actions** for all new files

```json
{
  "remove": {
    "path": "old-file.split",
    "deletionTimestamp": 1704326400000,
    "dataChange": true
  }
}
{
  "add": {
    "path": "new-file.split",
    ...
  }
}
```

### Concurrent Write Handling

**Current Status:** Last-write-wins semantics.

**Behavior:**
1. **Writer A** and **Writer B** start simultaneously
2. Both create splits and upload to S3
3. **Writer A** commits to transaction log (version N)
4. **Writer B** commits to transaction log (version N+1)
5. **Result:** Both writes are visible (additive)

**For Overwrite Operations:**
1. **Writer A** performs overwrite (removes all files, adds new ones)
2. **Writer B** performs overwrite concurrently
3. **Writer B's** overwrite wins (removes A's files)

**Future Enhancement:** Implement optimistic concurrency control with version checks (similar to Delta Lake).

---

## 5.12 Configuration Validation

### Write-Time Validation

IndexTables4Spark validates configuration at write time to **prevent runtime errors** and **ensure data consistency**.

**Validation Categories:**
1. **Schema validation**
2. **Field type compatibility**
3. **Auto-sizing configuration**
4. **Partition column validation**

### Schema Validation

**Unsupported Types:**
```scala
def validateSchema(schema: StructType): Unit = {
  schema.fields.foreach { field =>
    field.dataType match {
      case _: ArrayType | _: MapType | _: StructType =>
        throw new UnsupportedOperationException(
          s"Field '${field.name}' has unsupported type ${field.dataType}. " +
          s"Supported types: String, Integer, Long, Float, Double, " +
          s"Boolean, Date, Timestamp, Binary"
        )
      case _ => // Supported type
    }
  }
}
```

**Supported Types:**
- String (text/string fields in Tantivy)
- Integer, Long (i64 in Tantivy)
- Float, Double (f64 in Tantivy)
- Boolean (i64 in Tantivy)
- Date (date in Tantivy)
- Timestamp (i64 in Tantivy, stored as microseconds)
- Binary (bytes in Tantivy)

### Field Type Compatibility Validation

**Append Operation Validation:**

```scala
private def validateFieldTypeCompatibility(
  existingDocMapping: String,
  newFieldConfigs: Map[String, FieldIndexingConfig]
): Unit = {

  val existingMapping = JsonUtil.mapper.readTree(existingDocMapping)

  newFieldConfigs.foreach { case (fieldName, config) =>
    val existingFieldConfig = findFieldInMapping(existingMapping, fieldName)

    if (existingFieldConfig.isDefined && config.fieldType.isDefined) {
      val existingType = existingFieldConfig.get.get("type").asText()
      val newType = config.fieldType.get

      if (existingType != newType) {
        throw new IllegalArgumentException(
          s"Field '$fieldName' type mismatch: " +
          s"existing table has $existingType field, " +
          s"cannot append with $newType configuration"
        )
      }
    }
  }
}
```

**Validation Rules:**

| Existing Type | New Type | Result |
|---------------|----------|--------|
| `text` | `text` | ✅ Allowed |
| `text` | `string` | ❌ Rejected (tokenization conflict) |
| `string` | `string` | ✅ Allowed |
| `string` | `text` | ❌ Rejected (exact match conflict) |
| `i64` | `i64` | ✅ Allowed |
| `f64` | `f64` | ✅ Allowed |
| `date` | `date` | ✅ Allowed |

### Auto-Sizing Configuration Validation

**Target Split Size Validation:**
```scala
val targetSizeBytes = targetSplitSizeStr match {
  case Some(sizeStr) =>
    val bytes = SizeParser.parseSize(sizeStr)
    if (bytes <= 0) {
      throw new IllegalArgumentException(
        s"Auto-sizing target split size must be positive: $sizeStr"
      )
    }
    bytes

  case None =>
    throw new IllegalArgumentException(
      "Auto-sizing enabled but no target split size specified"
    )
}
```

**Size Format Validation:**
```scala
object SizeParser {
  def parseSize(sizeStr: String): Long = {
    val pattern = """(\d+)([KMGT]?)""".r

    sizeStr.toUpperCase match {
      case pattern(number, unit) =>
        val base = number.toLong
        unit match {
          case "" => base
          case "K" => base * 1024
          case "M" => base * 1024 * 1024
          case "G" => base * 1024 * 1024 * 1024
          case "T" => base * 1024 * 1024 * 1024 * 1024
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Invalid size format: $sizeStr. " +
          s"Supported formats: '100M', '2G', '512K', '104857600'"
        )
    }
  }
}
```

### Partition Column Validation

**Validation at Commit Time:**
```scala
def validatePartitionColumns(
  partitionColumns: Seq[String],
  schema: StructType
): Unit = {

  partitionColumns.foreach { colName =>
    if (!schema.fieldNames.contains(colName)) {
      throw new IllegalArgumentException(
        s"Partition column '$colName' does not exist in schema. " +
        s"Available columns: ${schema.fieldNames.mkString(", ")}"
      )
    }
  }
}
```

---

## Summary

### Key Components

| Component | Responsibility | Key Features |
|-----------|---------------|--------------|
| **WriteBuilder** | Write mode selection | Truncate/overwrite support, configuration hierarchy |
| **StandardWrite** | Direct write execution | Spark default partitioning, simple execution |
| **OptimizedWrite** | Intelligent write optimization | Auto-sizing, intelligent repartitioning, RequiresDistributionAndOrdering |
| **Auto-Sizing** | Historical analysis | Bytes-per-record calculation, target rows determination |
| **BatchWrite** | Coordinator | Collects AddActions, atomic commits |
| **WriterFactory** | Executor initialization | Creates DataWriter instances per partition |
| **DataWriter** | Split creation | Tantivy indexing, split upload, metadata generation |
| **S3 Upload** | File transfer | Parallel streaming, backpressure management |

### Write Operation Flow

```
Complete Write Pipeline:
1. WriteBuilder.build()
   └─> Select StandardWrite or OptimizedWrite

2. OptimizedWrite (if enabled)
   └─> Auto-sizing analysis
   └─> Calculate requiredNumPartitions()
   └─> Spark repartitions DataFrame

3. createBatchWriterFactory()
   └─> Create WriterFactory with serialized config

4. Executors: createWriter(partitionId)
   └─> Create DataWriter per partition

5. Executors: write(row) × N
   └─> Add documents to Tantivy index
   └─> Update statistics for data skipping

6. Executors: commit()
   └─> Create split file
   └─> Upload to S3 (parallel streaming if large)
   └─> Return AddAction metadata

7. Driver: collect CommitMessages
   └─> Filter empty partitions
   └─> Validate configuration (append mode)

8. Driver: atomic transaction log commit
   └─> transactionLog.addFiles(addActions) or overwriteFiles(addActions)

9. Splits now visible to readers
```

### Configuration Best Practices

**Small Tables (< 1GB):**
```scala
df.write
  .option("spark.indextables.optimizeWrite.enabled", "false")
  .save("s3://bucket/small-table")
```

**Medium Tables (1GB - 100GB):**
```scala
df.write
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .option("targetRecordsPerSplit", "1000000")
  .save("s3://bucket/medium-table")
```

**Large Tables (> 100GB) with Auto-Sizing:**
```scala
// V1 API (automatic count)
df.write
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://bucket/large-table")

// V2 API (explicit count)
val rowCount = df.count()
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .option("spark.indextables.autoSize.inputRowCount", rowCount.toString)
  .save("s3://bucket/large-table")
```

**High-Performance Configuration (S3):**
```scala
df.write
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
  .option("spark.indextables.s3.maxConcurrency", "12")
  .option("spark.indextables.s3.partSize", "134217728")  // 128MB
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "200M")
  .save("s3://bucket/optimized-table")
```

**High-Performance Configuration (Azure):**
```scala
df.write
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
  .option("spark.indextables.azure.accountName", "mystorageaccount")
  .option("spark.indextables.azure.tenantId", "your-tenant-id")
  .option("spark.indextables.azure.clientId", "your-client-id")
  .option("spark.indextables.azure.clientSecret", "your-client-secret")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "200M")
  .save("abfss://container@mystorageaccount.dfs.core.windows.net/optimized-table")
```

---

**Previous Section:** [Section 4: Scan Planning & Execution](04_scan_planning.md)
**Next Section:** [Section 6: Partition Support](06_partition_support.md)
