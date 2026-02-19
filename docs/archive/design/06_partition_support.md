# Section 6: Partition Support

**Document Version:** 2.0
**Last Updated:** 2025-10-06

---

## Table of Contents

- [6.1 Partitioning Architecture](#61-partitioning-architecture)
- [6.2 V1 vs V2 Partition Behavior](#62-v1-vs-v2-partition-behavior)
- [6.3 Partition Value Extraction](#63-partition-value-extraction)
- [6.4 Partition Directory Structure](#64-partition-directory-structure)
- [6.5 Partition Pruning](#65-partition-pruning)
- [6.6 Partition-Aware Write Operations](#66-partition-aware-write-operations)
- [6.7 Partition-Aware Read Operations](#67-partition-aware-read-operations)
- [6.8 Transaction Log Optimizations](#68-transaction-log-optimizations)
- [6.9 Partition Evolution](#69-partition-evolution)

---

## 6.1 Partitioning Architecture

### Overview

IndexTables4Spark implements **Hive-style partitioning** compatible with Apache Spark's DataSource V2 API, enabling efficient data organization and query performance optimization through partition pruning.

**Key Design Principles:**

1. **Partition columns are indexed in splits** (V2 API only - critical difference from V1)
2. **Hive-style directory structure** (`col=value/col2=value2/`)
3. **Partition metadata in transaction log** for fast pruning
4. **Full filter pushdown** on partition columns (V2 API)
5. **Transaction log optimization** for partition-only queries

### Partitioned Table Example

**Write Operation:**
```scala
// Create partitioned table
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("year", "month", "day")
  .save("s3://bucket/logs")
```

**Physical Layout:**
```
s3://bucket/logs/
├── _transaction_log/
│   ├── 000000000000000000.json
│   ├── 000000000000000001.json
│   └── _last_checkpoint
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   ├── part-00000-123-abc.split
│   │   │   ├── part-00001-456-def.split
│   │   │   └── part-00002-789-ghi.split
│   │   ├── day=02/
│   │   │   └── part-00000-321-jkl.split
│   │   └── day=03/
│   │       └── part-00000-654-mno.split
│   └── month=02/
│       └── day=01/
│           └── part-00000-987-pqr.split
└── year=2025/
    └── month=01/
        └── day=01/
            └── part-00000-147-stu.split
```

**Transaction Log Entry:**
```json
{
  "add": {
    "path": "year=2024/month=01/day=01/part-00000-123-abc.split",
    "partitionValues": {
      "year": "2024",
      "month": "01",
      "day": "01"
    },
    "size": 104857600,
    "numRecords": 1000000,
    "modificationTime": 1704067200000,
    "dataChange": true,
    "minValues": {"timestamp": 1704067200, "score": 0},
    "maxValues": {"timestamp": 1704153599, "score": 100}
  }
}
```

### Supported Partition Types

| Spark Type | Storage Format | Example Value | Notes |
|------------|---------------|---------------|-------|
| `StringType` | Direct string | `"2024-01-01"` | Most common for dates |
| `IntegerType` | Integer string | `"42"` | Efficient for enumerated values |
| `LongType` | Long string | `"1704067200"` | Unix timestamps |
| `DateType` | ISO date string | `"2024-01-01"` | Converted from days-since-epoch |
| `TimestampType` | ISO timestamp | `"2024-01-01T10:00:00Z"` | Converted from microseconds |
| `BooleanType` | Boolean string | `"true"` | For binary partitioning |
| `FloatType` | Float string | `"3.14"` | Use with caution (precision) |
| `DoubleType` | Double string | `"2.718"` | Use with caution (precision) |
| `DecimalType` | Decimal string | `"123.45"` | Exact decimal representation |

**Unsupported Types:**
- `ArrayType`, `MapType`, `StructType` (complex types)
- `BinaryType` (binary data)

---

## 6.2 V1 vs V2 Partition Behavior

### Critical Difference

**V1 DataSource API:**
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")
```

**Behavior:**
- ❌ **Partition columns NOT indexed in splits**
- ⚠️ **Filter pushdown limited** (partition filters deferred to Spark)
- ⚠️ **Reduced query performance** on partition columns
- ✅ **Partition pruning works** (directory-level optimization)

**V2 DataSource API (Recommended):**
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")
```

**Behavior:**
- ✅ **Partition columns ARE indexed in splits**
- ✅ **Full filter pushdown** on partition columns
- ✅ **High query performance** on partition columns
- ✅ **Partition pruning works** (directory-level optimization)

### Comparison Table

| Feature | V1 API | V2 API | Impact |
|---------|--------|--------|--------|
| Partition directory structure | ✅ Yes | ✅ Yes | File organization |
| Partition metadata in transaction log | ✅ Yes | ✅ Yes | Partition pruning |
| Partition columns indexed in splits | ❌ No | ✅ Yes | Query performance |
| Filter pushdown on partition columns | ❌ Limited | ✅ Full | 10-100x faster queries |
| IndexQuery on partition columns | ❌ No | ✅ Yes | Advanced querying |
| Recommended for production | ❌ No | ✅ Yes | Future compatibility |

**Migration Recommendation:**

> **Use V2 API for all new tables.** V1 API is maintained for backward compatibility but lacks critical partition column indexing. For existing V1 tables with partitions, consider migrating to V2 API for improved query performance.

---

## 6.3 Partition Value Extraction

### PartitionUtils Object

**Location:** `io.indextables.spark.transaction.PartitionUtils`

The `PartitionUtils` object provides utilities for extracting and managing partition values during write operations.

### Extract Partition Values

**Method Signature:**
```scala
def extractPartitionValues(
  row: InternalRow,
  schema: StructType,
  partitionColumns: Seq[String]
): Map[String, String]
```

**Implementation:**
```scala
def extractPartitionValues(
  row: InternalRow,
  schema: StructType,
  partitionColumns: Seq[String]
): Map[String, String] = {

  if (partitionColumns.isEmpty) {
    return Map.empty
  }

  val partitionValues = mutable.Map[String, String]()
  val fieldMap = schema.fields.zipWithIndex
    .map { case (field, idx) => field.name -> ((field, idx)) }
    .toMap

  for (partitionCol <- partitionColumns) {
    fieldMap.get(partitionCol) match {
      case Some((field, index)) =>
        val value = convertPartitionValue(row, index, field.dataType)
        partitionValues(partitionCol) = value
      case None =>
        logger.warn(s"Partition column '$partitionCol' not found in schema")
        partitionValues(partitionCol) = null
    }
  }

  partitionValues.toMap
}
```

**Process:**
1. Build field index map from schema
2. For each partition column, locate field index
3. Convert value to string representation
4. Return map of column name → string value

### Value Conversion

**Type-Specific Conversion:**
```scala
private def convertPartitionValue(
  row: InternalRow,
  index: Int,
  dataType: DataType
): String = {

  if (row.isNullAt(index)) {
    return null
  }

  dataType match {
    case StringType => row.getUTF8String(index).toString
    case IntegerType => row.getInt(index).toString
    case LongType => row.getLong(index).toString
    case FloatType => row.getFloat(index).toString
    case DoubleType => row.getDouble(index).toString
    case BooleanType => row.getBoolean(index).toString

    case DateType =>
      // Convert days-since-epoch to ISO date string
      val days = row.getInt(index)
      java.time.LocalDate.ofEpochDay(days.toLong).toString
      // Result: "2024-01-01"

    case TimestampType =>
      // Convert microseconds to ISO timestamp string
      val micros = row.getLong(index)
      val instant = java.time.Instant.ofEpochSecond(
        micros / 1000000,
        (micros % 1000000) * 1000
      )
      instant.toString
      // Result: "2024-01-01T10:30:00Z"

    case dt: DecimalType =>
      val decimal = row.getDecimal(index, dt.precision, dt.scale)
      decimal.toJavaBigDecimal.toString

    case _ =>
      logger.warn(s"Unsupported partition column type: $dataType")
      row.get(index, dataType).toString
  }
}
```

**Conversion Examples:**

| Type | Internal Value | String Representation |
|------|---------------|----------------------|
| `DateType` | `19723` (days) | `"2024-01-01"` |
| `TimestampType` | `1704067200000000` (micros) | `"2024-01-01T00:00:00Z"` |
| `IntegerType` | `42` | `"42"` |
| `StringType` | `UTF8String("tech")` | `"tech"` |
| `BooleanType` | `true` | `"true"` |

### Null Handling

**Null Partition Values:**
```scala
if (row.isNullAt(index)) {
  return null
}
```

**Directory Encoding:**
```scala
val encodedValue = if (value == null) {
  "__HIVE_DEFAULT_PARTITION__"
} else {
  escapePathName(value)
}
```

**Example:**
```
// NULL date value creates:
year=2024/month=01/day=__HIVE_DEFAULT_PARTITION__/part-00000.split
```

### Validation

**Partition Column Validation:**
```scala
def validatePartitionColumns(
  schema: StructType,
  partitionColumns: Seq[String]
): Unit = {

  val schemaFields = schema.fieldNames.toSet

  // Check existence
  val missingColumns = partitionColumns.filterNot(schemaFields.contains)
  if (missingColumns.nonEmpty) {
    throw new IllegalArgumentException(
      s"Partition columns not found in schema: ${missingColumns.mkString(", ")}"
    )
  }

  // Check type support
  val fieldMap = schema.fields.map(f => f.name -> f.dataType).toMap
  val unsupportedColumns = partitionColumns.filter { colName =>
    fieldMap.get(colName) match {
      case Some(dataType) => !isSupportedPartitionType(dataType)
      case None => false
    }
  }

  if (unsupportedColumns.nonEmpty) {
    throw new IllegalArgumentException(
      s"Unsupported partition column types: ${unsupportedColumns.mkString(", ")}"
    )
  }
}
```

**Validation Timing:**
- **Write time:** Validates before creating splits
- **Read time:** No validation needed (uses existing partitions)

---

## 6.4 Partition Directory Structure

### Path Creation

**Create Partition Path:**
```scala
def createPartitionPath(
  partitionValues: Map[String, String],
  partitionColumns: Seq[String]
): String = {

  partitionColumns
    .map { col =>
      val value = partitionValues.getOrElse(col, null)
      val encodedValue = if (value == null) {
        "__HIVE_DEFAULT_PARTITION__"
      } else {
        escapePathName(value)
      }
      s"$col=$encodedValue"
    }
    .mkString("/")
}
```

**Example:**
```scala
partitionColumns = Seq("year", "month", "day")
partitionValues = Map(
  "year" -> "2024",
  "month" -> "01",
  "day" -> "15"
)

result = "year=2024/month=01/day=15"
```

### Path Escaping

**Special Character Encoding:**
```scala
private def escapePathName(path: String): String = {
  if (path == null || path.isEmpty) {
    "__HIVE_DEFAULT_PARTITION__"
  } else {
    path
      .replace("/", "%2F")
      .replace("\\", "%5C")
      .replace(":", "%3A")
      .replace("*", "%2A")
      .replace("?", "%3F")
      .replace("\"", "%22")
      .replace("<", "%3C")
      .replace(">", "%3E")
      .replace("|", "%7C")
      .replace(" ", "%20")
  }
}
```

**Encoding Table:**

| Character | Encoded | Reason |
|-----------|---------|--------|
| `/` | `%2F` | Directory separator conflict |
| `\` | `%5C` | Windows path separator |
| `:` | `%3A` | Drive letter separator (Windows) |
| `*` | `%2A` | Wildcard character |
| `?` | `%3F` | Wildcard character |
| `"` | `%22` | Quote character |
| `<` | `%3C` | Redirect operator |
| `>` | `%3E` | Redirect operator |
| `|` | `%7C` | Pipe operator |
| ` ` | `%20` | Space character |

**Example:**
```scala
// Input: "2024-01-01 10:30:00"
// Output: "2024-01-01%2010%3A30%3A00"
```

### Multi-Level Partitioning

**Hierarchical Partition Structure:**
```scala
// 3-level partitioning
df.write
  .partitionBy("year", "month", "day")
  .save("s3://bucket/data")

// Creates hierarchy:
year=2024/
  month=01/
    day=01/  ← Files for Jan 1, 2024
    day=02/  ← Files for Jan 2, 2024
  month=02/
    day=01/  ← Files for Feb 1, 2024
```

**Benefits:**
- **Efficient pruning** at each level
- **Natural time-series organization**
- **Scalable to billions of partitions**

**Limitations:**
- **Cardinality explosion** with too many partition columns
- **Small file problem** with high granularity
- **Metadata overhead** in transaction log

**Recommended Partition Granularity:**

| Use Case | Recommended Partitioning | Example |
|----------|-------------------------|---------|
| Daily logs | `year`, `month`, `day` | `year=2024/month=01/day=15/` |
| Hourly metrics | `date`, `hour` | `date=2024-01-15/hour=10/` |
| Geographic data | `region`, `country` | `region=EMEA/country=DE/` |
| Multi-tenant | `tenant_id` | `tenant_id=acme-corp/` |

---

## 6.5 Partition Pruning

### Overview

Partition pruning is the process of **eliminating entire partitions from consideration** based on filter predicates, dramatically reducing the amount of data that needs to be scanned.

**Performance Impact:**
- **50-99% file reduction** for partition-filtered queries
- **No data read** from pruned partitions
- **Metadata-only operation** (fast)

### Pruning Logic

**Implementation:**
```scala
object PartitionPruning {
  def prunePartitions(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter]
  ): Seq[AddAction] = {

    if (partitionColumns.isEmpty || filters.isEmpty) {
      return addActions  // No pruning possible
    }

    // Extract filters that reference partition columns
    val partitionFilters = filters.filter { filter =>
      val referencedColumns = getFilterReferencedColumns(filter)
      referencedColumns.exists(partitionColumns.contains)
    }

    if (partitionFilters.isEmpty) {
      return addActions  // No partition filters
    }

    // Apply partition value matching
    addActions.filter { addAction =>
      partitionFilters.forall(matchesPartitionFilter(addAction, _))
    }
  }

  private def matchesPartitionFilter(
    addAction: AddAction,
    filter: Filter
  ): Boolean = {
    filter match {
      case EqualTo(attribute, value) =>
        addAction.partitionValues.get(attribute) match {
          case Some(partValue) => partValue == value.toString
          case None => true  // Not a partition column
        }

      case In(attribute, values) =>
        addAction.partitionValues.get(attribute) match {
          case Some(partValue) => values.map(_.toString).contains(partValue)
          case None => true
        }

      case And(left, right) =>
        matchesPartitionFilter(addAction, left) &&
        matchesPartitionFilter(addAction, right)

      case Or(left, right) =>
        matchesPartitionFilter(addAction, left) ||
        matchesPartitionFilter(addAction, right)

      case _ => true  // Unknown filter - keep partition
    }
  }
}
```

### Pruning Examples

**Example 1: Single Partition Filter**
```scala
// Query
df.filter($"date" === "2024-01-15").count()

// Partition pruning:
// - 1000 total partitions (365 days × 3 years)
// - 1 partition matches: date=2024-01-15
// - 999 partitions pruned (99.9% reduction)
```

**Example 2: Range Filter on Partition Column**
```scala
// Query
df.filter($"year" === 2024 && $"month".between(1, 3)).count()

// Partition pruning:
// - Year pruning: eliminates 2022, 2023, 2025
// - Month pruning: keeps months 01, 02, 03
// - Result: 3 months × ~30 days = ~90 partitions
```

**Example 3: IN Clause**
```scala
// Query
df.filter($"region".isin("US", "CA")).count()

// Partition pruning:
// - Total: 50 regions
// - Matches: 2 regions (US, CA)
// - Pruned: 48 regions (96% reduction)
```

### Multi-Level Pruning

**Hierarchical Filtering:**
```scala
// Query with multiple partition levels
df.filter(
  $"year" === 2024 &&
  $"month" === 1 &&
  $"day".between(10, 15)
).count()

// Pruning steps:
// 1. Year level: year=2024 → 365 partitions remain
// 2. Month level: month=01 → 31 partitions remain
// 3. Day level: day between 10-15 → 6 partitions remain
//
// Final: 6 partitions scanned (from potentially thousands)
```

### Partition Filter Detection

**Extract Referenced Columns:**
```scala
private def getFilterReferencedColumns(filter: Filter): Set[String] = {
  filter match {
    case EqualTo(attribute, _) => Set(attribute)
    case In(attribute, _) => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case And(left, right) =>
      getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
    case Or(left, right) =>
      getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
    case Not(child) =>
      getFilterReferencedColumns(child)
    case _ => Set.empty
  }
}
```

### Partition Pruning Metrics

**Logging Output:**
```
INFO Partition pruning summary:
  - Total partitions: 1000
  - Partition filters: [EqualTo(date, 2024-01-15)]
  - Partitions after pruning: 1
  - Partitions pruned: 999 (99.9%)
  - Files to scan: 5 (from original 50000)
```

---

## 6.6 Partition-Aware Write Operations

### DataWriter Partition Handling

**Multiple Partition Writers:**

Each `DataWriter` instance can create **multiple splits** if it receives rows with different partition values:

```scala
class IndexTables4SparkDataWriter(
  tablePath: Path,
  writeSchema: StructType,
  partitionId: Int,
  taskId: Long,
  serializedOptions: Map[String, String],
  hadoopConf: Configuration,
  partitionColumns: Seq[String] = Seq.empty
) extends DataWriter[InternalRow] {

  // Map of partition key → (TantivySearchEngine, Statistics, RecordCount)
  private val partitionWriters =
    mutable.Map[String, (TantivySearchEngine, StatisticsCalculator, Long)]()

  override def write(record: InternalRow): Unit = {
    // Extract partition values from row
    val partitionValues = PartitionUtils.extractPartitionValues(
      record,
      writeSchema,
      partitionColumns
    )

    // Create partition directory path
    val partitionKey = PartitionUtils.createPartitionPath(
      partitionValues,
      partitionColumns
    )

    // Get or create writer for this partition combination
    val (engine, stats, count) = partitionWriters.getOrElseUpdate(
      partitionKey,
      (
        new TantivySearchEngine(writeSchema, options, hadoopConf),
        new StatisticsCalculator.DatasetStatistics(writeSchema),
        0L
      )
    )

    // Add complete row (including partition columns) to split
    engine.addDocument(record)
    stats.updateRow(record)
    partitionWriters(partitionKey) = (engine, stats, count + 1)
  }
}
```

**Key Points:**
1. **Partition values extracted** from each row during write
2. **Separate writer** created for each unique partition combination
3. **Complete row stored** in split (including partition columns for V2 API)
4. **Multiple splits per task** possible if data crosses partition boundaries

### Commit Process

**Multi-Partition Commit:**
```scala
override def commit(): WriterCommitMessage = {
  val allAddActions = partitionWriters.flatMap {
    case (partitionKey, (engine, stats, count)) =>
      if (count > 0) {
        // Create split in partition directory
        val partitionValues = parsePartitionKey(partitionKey)
        val partitionDir = new Path(tablePath, partitionKey)
        val fileName = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.split"
        val filePath = new Path(partitionDir, fileName)

        // Commit Tantivy index and create split
        val (splitPath, splitMetadata) = engine.commitAndCreateSplit(
          filePath.toString,
          partitionId.toLong,
          nodeId
        )

        // Create AddAction with partition metadata
        Some(AddAction(
          path = s"$partitionKey/$fileName",
          partitionValues = partitionValues,
          size = getSplitSize(splitPath),
          numRecords = Some(count),
          ...
        ))
      } else {
        None  // Skip empty partitions
      }
  }.toSeq

  IndexTables4SparkCommitMessage(allAddActions)
}
```

### Write Example

**Input DataFrame:**
```scala
val data = Seq(
  ("2024-01-01", 10, "event1"),
  ("2024-01-01", 11, "event2"),
  ("2024-01-02", 10, "event3"),
  ("2024-01-02", 10, "event4")
).toDF("date", "hour", "event_type")
```

**Partitioned Write:**
```scala
data.write
  .partitionBy("date", "hour")
  .save("s3://bucket/events")
```

**Result - Single Task Processing:**
```
partitionWriters = Map(
  "date=2024-01-01/hour=10" → (engine1, stats1, 1),
  "date=2024-01-01/hour=11" → (engine2, stats2, 1),
  "date=2024-01-02/hour=10" → (engine3, stats3, 2)
)

Files created:
- s3://bucket/events/date=2024-01-01/hour=10/part-00000-123.split
- s3://bucket/events/date=2024-01-01/hour=11/part-00000-456.split
- s3://bucket/events/date=2024-01-02/hour=10/part-00000-789.split
```

---

## 6.7 Partition-Aware Read Operations

### Partition Value Storage

**V2 API Behavior:**

Partition column values are **stored directly in split data**, not reconstructed from directory paths:

```scala
// During write:
engine.addDocument(record)  // Complete row including partition columns

// During read:
splitSearchEngine.search(query)  // Returns complete rows
```

**Benefits:**
1. **Full filter pushdown** on partition columns (can query in Tantivy)
2. **No partition value reconstruction** needed
3. **Consistent with Quickwit** split-based architecture
4. **Partition evolution friendly** (can change partition scheme)

### Read Example

**Query:**
```scala
val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/events")

df.filter($"date" === "2024-01-01" && $"event_type" === "login")
  .select("hour", "user_id")
  .show()
```

**Execution Flow:**
```
1. Partition Pruning (Transaction Log):
   - Filter: date = "2024-01-01"
   - Prune to partitions: date=2024-01-01/*
   - Files: 24 files (one per hour)

2. Filter Pushdown (Tantivy):
   - Filter: event_type = "login"
   - Pushed to Tantivy query
   - Executed in split search

3. Column Pruning:
   - Only read: hour, user_id, date, event_type
   - Skip other columns

4. Result:
   - Fast partition pruning (metadata)
   - Fast filter execution (Tantivy)
   - Minimal data transfer
```

---

## 6.8 Transaction Log Optimizations

### Partition-Only COUNT

**Optimization:** COUNT queries with **only partition filters** use transaction log metadata:

```scala
// Optimized COUNT (no split access)
df.filter($"date" === "2024-01-01").count()

// Process:
// 1. Prune partitions: date=2024-01-01
// 2. Sum numRecords from AddActions
// 3. Return count (no Tantivy access)
```

**Requirements:**
- ✅ Only partition column filters
- ✅ No data column filters
- ✅ No IndexQuery filters
- ✅ `numRecords` metadata available

**Implementation:**
```scala
class TransactionLogCountScan(
  transactionLog: TransactionLog,
  partitionFilters: Array[Filter],
  partitionColumns: Seq[String]
) extends Scan {

  override def readSchema(): StructType = {
    new StructType().add("count", LongType)
  }

  override def toBatch: Batch = new Batch {
    override def planInputPartitions(): Array[InputPartition] = {
      // 1. Read transaction log
      val addActions = transactionLog.listFiles()

      // 2. Apply partition pruning
      val prunedActions = PartitionPruning.prunePartitions(
        addActions,
        partitionColumns,
        partitionFilters
      )

      // 3. Sum record counts
      val totalCount = prunedActions.flatMap(_.numRecords).sum

      // 4. Return single partition with count
      Array(new CountResultPartition(totalCount))
    }
  }
}
```

**Performance:**
```
Traditional COUNT:
- Read all matching splits: 10 seconds
- Count documents: 5 seconds
- Total: 15 seconds

Transaction Log COUNT:
- Read transaction log: 0.1 seconds
- Sum metadata: 0.01 seconds
- Total: 0.11 seconds

Speedup: 136x faster
```

### Partition-Only GROUP BY

**Optimization:** GROUP BY on **only partition columns** with COUNT aggregation:

```scala
// Optimized GROUP BY (no split access)
df.groupBy("date").agg(count("*")).show()

// Process:
// 1. Group AddActions by date partition value
// 2. Sum numRecords for each group
// 3. Return results (no Tantivy access)
```

**Requirements:**
- ✅ GROUP BY uses only partition columns
- ✅ Only COUNT(*) aggregation
- ✅ No data column filters
- ✅ No IndexQuery filters

**Example:**
```scala
// Input: 365 partitions (one per day)
df.groupBy("date").count().show()

// Result:
+----------+-------+
|      date|  count|
+----------+-------+
|2024-01-01| 100000|
|2024-01-02| 150000|
|2024-01-03|  95000|
...
```

**Performance:**
```
Traditional GROUP BY:
- Scan all splits: 30 seconds
- Group and count: 10 seconds
- Total: 40 seconds

Transaction Log GROUP BY:
- Read transaction log: 0.1 seconds
- Group metadata: 0.05 seconds
- Total: 0.15 seconds

Speedup: 266x faster
```

---

## 6.9 Partition Evolution

### Adding Partition Columns

**Not Supported:** IndexTables4Spark does not currently support adding partition columns to existing tables.

**Workaround:**
```scala
// 1. Read existing table
val oldData = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/old-table")

// 2. Add partition column
val newData = oldData.withColumn("region", lit("US"))

// 3. Write to new partitioned table
newData.write
  .partitionBy("region", "date")
  .save("s3://bucket/new-table")
```

### Changing Partition Granularity

**Example: Daily → Hourly Partitioning**

```scala
// Old: Daily partitions
df.write.partitionBy("date")
  .save("s3://bucket/daily-data")

// New: Hourly partitions
df.write.partitionBy("date", "hour")
  .save("s3://bucket/hourly-data")
```

**Migration Process:**
1. **Read old table** with daily partitions
2. **Extract hour** from timestamp column
3. **Write to new table** with hourly partitions
4. **Update queries** to use new table path
5. **Deprecate old table** after validation

### Partition Column Renaming

**Not Supported:** Partition column names cannot be renamed in place.

**Workaround:**
```scala
// 1. Read with column renaming
val data = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/old")
  .withColumnRenamed("old_date", "new_date")

// 2. Write to new location
data.write
  .partitionBy("new_date")
  .save("s3://bucket/new")
```

---

## Summary

### Key Features

| Feature | Capability | Performance Impact |
|---------|-----------|-------------------|
| **Partition Pruning** | Metadata-based file elimination | 50-99% file reduction |
| **V2 Partition Indexing** | Partition columns indexed in splits | 10-100x faster queries |
| **Transaction Log COUNT** | Partition-only COUNT optimization | 100-1000x faster |
| **Transaction Log GROUP BY** | Partition-only GROUP BY optimization | 100-1000x faster |
| **Multi-level Partitioning** | Hierarchical partition structure | Scalable to billions of partitions |
| **Hive Compatibility** | Standard Hive-style directories | Compatible with other engines |

### Best Practices

**Partition Column Selection:**
1. **Choose low-cardinality columns** for top-level partitions (e.g., year, region)
2. **Increase granularity gradually** (year → month → day, not date → hour → minute)
3. **Avoid high-cardinality partitions** (user_id, session_id)
4. **Use time-based partitioning** for time-series data
5. **Limit partition depth** to 2-4 levels maximum

**Partition Granularity:**
```scala
// ✅ Good: 365 partitions per year
df.write.partitionBy("date").save(...)

// ⚠️ Caution: 8760 partitions per year
df.write.partitionBy("date", "hour").save(...)

// ❌ Avoid: 525,600 partitions per year
df.write.partitionBy("date", "hour", "minute").save(...)
```

**Query Optimization:**
```scala
// ✅ Optimal: Partition filter first
df.filter($"date" === "2024-01-01")
  .filter($"event_type" === "click")

// ⚠️ Less optimal: Data filter first
df.filter($"event_type" === "click")
  .filter($"date" === "2024-01-01")
```

### Configuration Summary

**Partitioned Write:**
```scala
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("year", "month", "day")
  .save("s3://bucket/partitioned-data")
```

**Partitioned Read:**
```scala
val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/partitioned-data")

// Partition pruning automatically applied
df.filter($"year" === 2024).count()
```

**MERGE SPLITS with Partitions:**
```sql
MERGE SPLITS 's3://bucket/partitioned-data'
WHERE year = 2024 AND month = 1
TARGET SIZE 100M
```

---

**Previous Section:** [Section 5: Write Operations](05_write_operations.md)
**Next Section:** [Section 7: IndexQuery System](07_indexquery_system.md)
