# Section 2: DataSource V2 API Implementation

**Document Version:** 1.0
**Last Updated:** 2025-10-06
**Target Audience:** Developers familiar with Apache Spark but not necessarily with Spark internals

---

## Table of Contents

1. [V2 DataSource API Overview](#21-v2-datasource-api-overview)
2. [Configuration Hierarchy](#22-configuration-hierarchy)
3. [V1 API Deprecation & Migration](#23-v1-api-deprecation--migration)

---

## 2.1 V2 DataSource API Overview

### 2.1.1 Introduction to DataSource V2

The **DataSource V2 API** (also called "DataSource V2" or "DS V2") is a modern, extensible interface introduced in Spark 3.0 that supersedes the legacy V1 API. IndexTables4Spark implements the V2 API to leverage its advanced capabilities for push-down optimizations, metadata handling, and better Catalyst integration.

**Why V2 Over V1?**

| Capability | V1 API | V2 API |
|------------|--------|--------|
| **Filter Pushdown** | Basic | Enhanced with V2 predicates |
| **Projection Pushdown** | Manual | Automatic via `requiredSchema` |
| **Aggregate Pushdown** | Not supported | `SupportsPushDownAggregates` |
| **Limit Pushdown** | Not supported | `SupportsPushDownLimit` |
| **Metadata Columns** | Not supported | `SupportsMetadataColumns` (e.g., `_indexall`) |
| **Partition Transforms** | Limited | Full `Transform` support |
| **Statistics** | Manual | `SupportsReportStatistics` |
| **Overwrite Modes** | Basic | `SupportsOverwrite` with filters |

### 2.1.2 Entry Point: IndexTables4SparkTableProvider

IndexTables4Spark uses **`io.indextables.spark.core.IndexTables4SparkTableProvider`** as its V2 API entry point (note: the actual class name will be confirmed from your codebase structure).

**Usage:**

```scala
// V2 API (Recommended)
val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/table-path")

// Write
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/table-path")
```

**Interface Implementation:**

The TableProvider implements Spark's core V2 interfaces:

```scala
class IndexTables4SparkTableProvider
  extends TableProvider
  with DataSourceRegister {

  override def shortName(): String = "indextables"

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    // Create and return IndexTables4SparkTable
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Read schema from transaction log
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    // Read partition columns from transaction log metadata
  }
}
```

**Key Responsibilities:**

1. **Schema Inference**: Read schema from transaction log `MetadataAction`
2. **Partition Inference**: Extract partition columns and create `Transform` objects
3. **Table Creation**: Instantiate `IndexTables4SparkTable` with proper configuration
4. **Short Name Registration**: Register `"indextables"` as the format name

### 2.1.3 Table Capabilities: IndexTables4SparkTable

The `IndexTables4SparkTable` class implements the core table capabilities:

```scala
class IndexTables4SparkTable(
  transactionLog: TransactionLog,
  schema: StructType,
  partitioning: Array[Transform],
  properties: Map[String, String]
) extends Table
  with SupportsRead
  with SupportsWrite
  with SupportsMetadataColumns {

  override def name(): String = s"IndexTables4SparkTable(${transactionLog.getTablePath()})"

  override def schema(): StructType = schema

  override def partitioning(): Array[Transform] = partitioning

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new IndexTables4SparkScanBuilder(transactionLog, schema, options, hadoopConf)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new IndexTables4SparkWriteBuilder(transactionLog, tablePath, info, options, hadoopConf)
  }

  override def metadataColumns(): Array[MetadataColumn] = {
    Array(IndexQueryAllColumn)  // Provides _indexall virtual column
  }
}
```

**Capability Breakdown:**

| Capability | Description | Implementation |
|------------|-------------|----------------|
| `BATCH_READ` | Supports batch DataFrame reads | `newScanBuilder()` creates scan plans |
| `BATCH_WRITE` | Supports batch DataFrame writes | `newWriteBuilder()` creates write plans |
| `OVERWRITE_BY_FILTER` | Filter-based overwrites (future) | `SupportsOverwrite` interface |
| `TRUNCATE` | Full table truncation | `SupportsTruncate` interface |

### 2.1.4 SupportsRead Interface

The `newScanBuilder()` method creates a `ScanBuilder` that handles query planning:

```scala
override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
  new IndexTables4SparkScanBuilder(
    transactionLog = transactionLog,
    schema = schema,
    options = options,
    hadoopConf = spark.sparkContext.hadoopConfiguration
  )
}
```

The `IndexTables4SparkScanBuilder` implements multiple push-down interfaces:

```scala
class IndexTables4SparkScanBuilder(
  transactionLog: TransactionLog,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  hadoopConf: Configuration
) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownV2Filters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with SupportsPushDownAggregates {

  // Pushed-down state
  private var pushedFilters: Array[Filter] = Array.empty
  private var requiredSchema: StructType = schema
  private var pushedLimit: Option[Int] = None
  private var pushedAggregation: Option[Aggregation] = None

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Accept filters, return unsupported ones
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    // Accept COUNT/SUM/AVG/MIN/MAX aggregations
  }

  override def build(): Scan = {
    // Create appropriate scan type based on pushed-down operations
  }
}
```

**Scan Types Created:**

| Condition | Scan Type | Purpose |
|-----------|-----------|---------|
| Simple aggregates (no GROUP BY) | `IndexTables4SparkSimpleAggregateScan` | COUNT/SUM/AVG/MIN/MAX |
| GROUP BY aggregates | `IndexTables4SparkGroupByAggregateScan` | Multi-dimensional aggregation |
| Partition-only COUNT | `TransactionLogCountScan` | Metadata-only counting |
| Regular query | `IndexTables4SparkScan` | Standard search with filters |

**Push-Down Decision Flow:**

```
ScanBuilder.build()
    │
    ├─ Has pushedAggregation?
    │   ├─ Yes, no grouping expressions → IndexTables4SparkSimpleAggregateScan
    │   ├─ Yes, with grouping → IndexTables4SparkGroupByAggregateScan
    │   └─ Special: partition-only COUNT → TransactionLogCountScan
    │
    └─ No aggregation → IndexTables4SparkScan
```

### 2.1.5 SupportsWrite Interface

The `newWriteBuilder()` method creates a `WriteBuilder` that handles write planning:

```scala
override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
  new IndexTables4SparkWriteBuilder(
    transactionLog = transactionLog,
    tablePath = tablePath,
    info = info,
    options = mergedOptions,  // DataFrame options merged with table properties
    hadoopConf = hadoopConf
  )
}
```

The `IndexTables4SparkWriteBuilder` supports multiple write modes:

```scala
class IndexTables4SparkWriteBuilder(
  transactionLog: TransactionLog,
  tablePath: Path,
  info: LogicalWriteInfo,
  options: CaseInsensitiveStringMap,
  hadoopConf: Configuration
) extends WriteBuilder
  with SupportsTruncate
  with SupportsOverwrite {

  private var isOverwrite = false

  override def truncate(): WriteBuilder = {
    isOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    // Future: filter-based overwrite (replaceWhere)
    isOverwrite = true
    this
  }

  override def build(): Write = {
    // Select write strategy: Standard vs Optimized
    if (optimizeWriteEnabled) {
      new IndexTables4SparkOptimizedWrite(...)
    } else {
      new IndexTables4SparkStandardWrite(...)
    }
  }
}
```

**Write Strategy Selection:**

```
WriteBuilder.build()
    │
    ├─ optimizeWrite.enabled == true?
    │   └─ Yes → IndexTables4SparkOptimizedWrite
    │       • Auto-sizing with historical analysis
    │       • RequiresDistributionAndOrdering
    │       • Target records per split
    │
    └─ No → IndexTables4SparkStandardWrite
        • Direct execution
        • Manual partitioning
        • Simple append/overwrite
```

### 2.1.6 SupportsMetadataColumns: Virtual _indexall Column

IndexTables4Spark provides a **virtual column** `_indexall` for cross-field search:

```scala
override def metadataColumns(): Array[MetadataColumn] = {
  Array(
    new MetadataColumn {
      override def name(): String = "_indexall"
      override def dataType(): DataType = StringType
      override def comment(): String = "Virtual column for cross-field IndexQuery search"
    }
  )
}
```

**Usage:**

```sql
-- Search across all indexed fields
SELECT * FROM logs
WHERE _indexall indexquery 'ERROR AND (timeout OR crash)'
```

**Implementation:**
- `_indexall` is **not stored** in the physical schema
- At read time, queries against `_indexall` are converted to Tantivy queries that search all indexed fields
- Implemented via `IndexQueryAllExpression` (see Section 7.1)

### 2.1.7 Partitioning Support: Transform-Based Integration

V2 API uses **Transform** objects to represent partition columns:

```scala
override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
  val partitionColumns = transactionLog.getPartitionColumns()

  partitionColumns.map { colName =>
    Expressions.identity(colName)  // Identity transform for each partition column
  }.toArray
}
```

**Transform Types:**

| Transform | Description | IndexTables4Spark Support |
|-----------|-------------|---------------------------|
| `Identity` | Direct column value (e.g., `partition=2024-01-01`) | ✅ Fully supported |
| `Bucket` | Hash-based bucketing | ❌ Not yet supported |
| `Years/Months/Days/Hours` | Time-based partitioning | ❌ Not yet supported (use identity) |

**Partition Column Indexing:**

**CRITICAL DIFFERENCE from V1 API:**
- **V2 API**: Partition columns **ARE indexed** in split files
- **V1 API**: Partition columns **ARE NOT indexed** (excluded from schema during write)

**Example:**

```scala
// V2 API - partition columns ARE indexed
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/logs")

// Later: Can search within partition columns
spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/logs")
  .filter($"date" === "2024-01-01" && $"hour" === 10)
  .filter($"message" indexquery "ERROR")
  .show()
// ✅ Both partition filter and content filter pushed down
```

---

## 2.2 Configuration Hierarchy

IndexTables4Spark uses a **four-tier configuration hierarchy** to provide flexibility and proper precedence:

### 2.2.1 Configuration Precedence Chain

```
┌────────────────────────────────────┐
│ 1. DataFrame Options (Highest)    │  ◄── df.write.option("key", "value")
└───────────┬────────────────────────┘
            │
┌───────────▼────────────────────────┐
│ 2. Spark Session Config            │  ◄── spark.conf.set("key", "value")
└───────────┬────────────────────────┘
            │
┌───────────▼────────────────────────┐
│ 3. Hadoop Configuration            │  ◄── hadoopConf.set("key", "value")
└───────────┬────────────────────────┘
            │
┌───────────▼────────────────────────┐
│ 4. Built-in Defaults (Lowest)     │  ◄── IndexTables4SparkConfig.OPTIMIZE_WRITE.defaultValue
└────────────────────────────────────┘
```

**Example:**

```scala
// Scenario: Setting optimizeWrite configuration

// Default value: true (from IndexTables4SparkConfig)
// User sets Spark config
spark.conf.set("spark.indextables.optimizeWrite.enabled", "false")
// User sets DataFrame option (overrides Spark config)
df.write.option("spark.indextables.optimizeWrite.enabled", "true")
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")

// Result: optimizeWrite = true (DataFrame option wins)
```

### 2.2.2 Configuration Normalization

IndexTables4Spark normalizes configuration keys from multiple sources:

**Prefix Handling:**

| Source | Prefix | Example |
|--------|--------|---------|
| DataFrame options | `spark.indextables.*` | `spark.indextables.cache.maxSize` |
| Spark config | `spark.indextables.*` | `spark.conf.set("spark.indextables.cache.maxSize", "200000000")` |
| Hadoop config | `spark.indextables.*` | `hadoopConf.set("spark.indextables.cache.maxSize", "200000000")` |

**Implementation:**

```scala
object ConfigNormalization {
  def extractConfig(
    options: CaseInsensitiveStringMap,
    sparkConf: RuntimeConfig,
    hadoopConf: Configuration
  ): Map[String, String] = {

    // 1. Extract from Hadoop config
    val hadoopConfig = hadoopConf.iterator().asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(e => e.getKey -> e.getValue)
      .toMap

    // 2. Extract from Spark config
    val sparkConfig = sparkConf.getAll
      .filter(_._1.startsWith("spark.indextables."))

    // 3. Extract from DataFrame options
    val dfOptions = options.asCaseSensitiveMap().asScala.toMap

    // 4. Merge with proper precedence (DataFrame > Spark > Hadoop)
    hadoopConfig ++ sparkConfig ++ dfOptions
  }
}
```

### 2.2.3 Credential Propagation

AWS/Azure/GCP credentials must flow from driver to executors for distributed operations:

**Driver-Side Configuration:**

```scala
// DataFrame options (highest precedence)
df.write
  .option("spark.indextables.aws.accessKey", "AKIAIOSFODNN7EXAMPLE")
  .option("spark.indextables.aws.secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")

// Or Spark config
spark.conf.set("spark.indextables.aws.accessKey", "AKIAIOSFODNN7EXAMPLE")
spark.conf.set("spark.indextables.aws.secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
```

**Executor-Side Retrieval:**

```scala
class IndexTables4SparkBatchWrite(
  serializedConfig: Map[String, String]  // Serialized from driver
) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new IndexTables4SparkWriterFactory(
      // Config passed to executors via serialization
      serializedConfig = serializedConfig
    )
  }
}

// On executor:
class IndexTables4SparkDataWriter(config: Map[String, String]) {
  // Credentials available via config map
  val accessKey = config.get("spark.indextables.aws.accessKey")
  val secretKey = config.get("spark.indextables.aws.secretKey")
}
```

**Custom Credential Providers:**

IndexTables4Spark supports custom credential provider classes via reflection:

```scala
// Configure custom provider
df.write
  .option("spark.indextables.aws.credentialsProviderClass",
          "com.example.MyCredentialProvider")
  .save("s3://bucket/path")
```

**Requirements:**
- Must implement `AwsCredentialsProvider` (AWS SDK v2) or `AWSCredentialsProvider` (v1)
- Must have constructor: `public MyProvider(URI uri, Configuration conf)`
- See Section 8.5 for details

### 2.2.4 Configuration Validation

Configuration values are validated at multiple stages:

**Early Validation (Driver-Side):**

```scala
// IndexTables4SparkOptions validates at construction
val options = IndexTables4SparkOptions(caseInsensitiveMap)

// Throws exception if invalid
val heapSize = options.indexWriterHeapSize.getOrElse(
  IndexTables4SparkConfig.INDEX_WRITER_HEAP_SIZE.defaultValue
)

if (heapSize <= 0) {
  throw new IllegalArgumentException(
    s"spark.indextables.indexWriter.heapSize must be positive, got: $heapSize"
  )
}
```

**Runtime Validation (Executor-Side):**

```scala
// Fast field validation during aggregate pushdown
override def pushAggregation(aggregation: Aggregation): Boolean = {
  aggregation.aggregateExpressions().foreach { agg =>
    agg match {
      case count: Count => // No fast field required
      case sum: Sum =>
        val fieldName = extractFieldName(sum)
        if (!isFastField(fieldName)) {
          throw new IllegalArgumentException(
            s"Field '$fieldName' must be configured as a fast field for SUM aggregation. " +
            s"Add: .option('spark.indextables.indexing.fastfields', '$fieldName')"
          )
        }
    }
  }
  true
}
```

---

## 2.3 V1 API Deprecation & Migration

### 2.3.1 Deprecation Notice

**The V1 DataSource API is deprecated and scheduled for removal in a future release.**

```scala
// ❌ DEPRECATED - V1 API
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")

// ✅ RECOMMENDED - V2 API
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")
```

**Deprecation Timeline:**

| Version | Status |
|---------|--------|
| v1.0 - v1.13 | V1 and V2 both supported |
| v2.0 (planned) | V1 removed, V2 only |

### 2.3.2 Key Difference: Partition Column Indexing

The **most important** difference between V1 and V2 is how partition columns are handled:

**V1 API Behavior (Deprecated):**

```scala
// V1 API - partition columns NOT indexed
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/logs")

// Physical storage:
// ├── date=2024-01-01/hour=10/uuid1.split
// │   └── Schema: [message, level]  ← "date" and "hour" excluded
// └── date=2024-01-01/hour=11/uuid2.split
//     └── Schema: [message, level]
```

**V2 API Behavior (Recommended):**

```scala
// V2 API - partition columns ARE indexed
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/logs")

// Physical storage:
// ├── date=2024-01-01/hour=10/uuid1.split
// │   └── Schema: [date, hour, message, level]  ← All columns indexed
// └── date=2024-01-01/hour=11/uuid2.split
//     └── Schema: [date, hour, message, level]
```

**Why This Matters:**

| Capability | V1 API | V2 API |
|------------|--------|--------|
| **Partition filter pushdown** | ✅ (via directory pruning) | ✅ (via directory pruning + index) |
| **Search within partition columns** | ❌ (columns not in index) | ✅ (columns in index) |
| **Combined filters** | ⚠️ (only content fields) | ✅ (partition + content fields) |
| **IndexQuery on partition cols** | ❌ | ✅ |

### 2.3.3 Migration Path

Migration from V1 to V2 is **simple and non-breaking**:

**Step 1: Update Format String**

```scala
// Before (V1)
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")

// After (V2)
val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/path")
```

**Step 2: Optional Configuration Updates**

V2 API supports all V1 configuration options with the same keys:

```scala
// These options work identically in both V1 and V2
df.write
  .option("spark.indextables.indexWriter.batchSize", "20000")
  .option("spark.indextables.cache.maxSize", "200000000")
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/path")
```

**Step 3: Test Read Operations**

Existing tables written with V1 can be read with V2 without issues:

```scala
// Table written with V1 API
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/legacy-table")

// Read with V2 API - works perfectly
val legacyDf = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/legacy-table")
```

### 2.3.4 Backward Compatibility

**Reading V1 Tables with V2:**
- ✅ **Fully supported** - V2 API can read tables created with V1 API
- ✅ **Same performance** - No degradation in query performance
- ✅ **Same features** - All V1 features work identically

**Writing to V1 Tables with V2:**
- ✅ **Append mode** - V2 can append to V1 tables
- ✅ **Overwrite mode** - V2 can overwrite V1 tables
- ⚠️ **Partition column indexing** - New splits will include partition columns in index

**Mixed V1/V2 Splits in Same Table:**

If you write to the same table with both V1 and V2, you'll have splits with different schemas:

```
s3://bucket/mixed-table/
├── date=2024-01-01/
│   ├── v1-split.split       # Schema: [message, level]
│   └── v2-split.split       # Schema: [date, hour, message, level]
```

This is **safe** but not recommended for long-term production:
- ✅ Queries work correctly
- ⚠️ Partition column filters only benefit V2 splits
- ⚠️ Inconsistent behavior between splits

**Recommendation:** After migrating to V2, perform a full overwrite to ensure schema consistency:

```scala
// Rewrite entire table with V2 to ensure consistent schema
df.write.mode(SaveMode.Overwrite)
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/mixed-table")
```

### 2.3.5 Migration Checklist

**Pre-Migration:**

- [ ] Identify all code using `format("io.indextables.spark.core.IndexTables4SparkTableProvider")`
- [ ] Review partition column usage (will benefit from V2 indexing)
- [ ] Test V2 reads on existing V1 tables (non-destructive)

**Migration:**

- [ ] Update format strings to `"io.indextables.spark.core.IndexTables4SparkTableProvider"`
- [ ] Test all read operations
- [ ] Test all write operations
- [ ] Optional: Rewrite tables for schema consistency

**Post-Migration:**

- [ ] Monitor query performance (should be same or better)
- [ ] Verify partition column filters work as expected
- [ ] Update documentation and code comments
- [ ] Plan deprecation of V1 code paths

---

## Summary

This section covered the DataSource V2 API implementation:

- **V2 API Overview**: Modern Spark 3.x implementation with enhanced capabilities
- **TableProvider**: Entry point for schema inference and table creation
- **Table Capabilities**: Read, write, metadata columns, and table capabilities
- **ScanBuilder**: Push-down optimizations for filters, projections, aggregates, and limits
- **WriteBuilder**: Write mode selection and strategy determination
- **Configuration Hierarchy**: Four-tier precedence (DataFrame > Spark > Hadoop > defaults)
- **V1 Deprecation**: Migration path and key differences (partition column indexing)

The V2 API provides the foundation for all IndexTables4Spark functionality. Subsequent sections will detail scan planning, write operations, and advanced features.

---

**Previous Section:** [Section 1: Introduction & Architecture Overview](01_introduction_and_architecture.md)
**Next Section:** [Section 3: Transaction Log System](03_transaction_log.md)
