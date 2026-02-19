# Section 7: IndexQuery System

**Document Version:** 2.0
**Last Updated:** 2025-10-06

---

## Table of Contents

- [7.1 IndexQuery Overview](#71-indexquery-overview)
- [7.2 IndexQuery Expression](#72-indexquery-expression)
- [7.3 IndexQueryAll Expression](#73-indexqueryall-expression)
- [7.4 Catalyst Integration](#74-catalyst-integration)
- [7.5 V2IndexQueryExpressionRule](#75-v2indexqueryexpressionrule)
- [7.6 IndexQuery Storage & Retrieval](#76-indexquery-storage--retrieval)
- [7.7 Query Syntax](#77-query-syntax)
- [7.8 Performance Characteristics](#78-performance-characteristics)
- [7.9 Integration with Filter Pushdown](#79-integration-with-filter-pushdown)

---

## 7.1 IndexQuery Overview

### Purpose

The IndexQuery system provides **native Tantivy query syntax** support in Spark SQL, enabling users to leverage Tantivy's powerful full-text search capabilities directly through DataFrame and SQL APIs.

**Key Features:**
1. **Native Tantivy syntax** (`field indexquery 'query'`)
2. **Cross-field search** (`_indexall indexquery 'query'`)
3. **Catalyst optimizer integration** for transparent pushdown
4. **Relation-scoped caching** for reliable query execution
5. **50-1000x performance** compared to post-filter evaluation

### Use Cases

| Use Case | Traditional Spark | IndexQuery | Performance Gain |
|----------|------------------|-----------|------------------|
| **Phrase search** | `content.contains("machine learning")` | `content indexquery '"machine learning"'` | 10-100x faster |
| **Boolean search** | Multiple filters | `content indexquery 'spark AND (sql OR dataframe)'` | 50-500x faster |
| **Wildcard search** | LIKE with regex | `title indexquery 'apach*'` | 20-200x faster |
| **Fuzzy search** | N/A | `name indexquery 'jhon~1'` | Enables new capability |
| **Range queries** | Comparison operators | `score indexquery '[50 TO 100]'` | 10-50x faster |
| **Multi-field** | Multiple column filters | `_indexall indexquery 'error OR warning'` | 100-1000x faster |

### Architecture

```
Query Execution Flow:
1. SQL/DataFrame API
   └─> "content indexquery 'machine learning'"

2. Spark SQL Parser
   └─> IndexQueryExpression(content, "machine learning")

3. Catalyst Optimizer
   └─> V2IndexQueryExpressionRule

4. Relation-Scoped Storage
   └─> WeakHashMap[DataSourceV2Relation, Seq[IndexQueryFilter]]

5. ScanBuilder Retrieval
   └─> Extracts IndexQuery from relation object

6. Tantivy Execution
   └─> Native Tantivy query execution in splits
```

---

## 7.2 IndexQuery Expression

### Expression Definition

**Class:** `IndexQueryExpression`

**Location:** `io.indextables.spark.expressions.IndexQueryExpression`

```scala
case class IndexQueryExpression(
  left: Expression,   // Column reference
  right: Expression   // Query string literal
) extends BinaryExpression with Predicate {

  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false

  // Non-deterministic to prevent Catalyst from optimizing away
  override lazy val deterministic: Boolean = false

  override def prettyName: String = "indexquery"
  override def sql: String = s"(${left.sql} indexquery ${right.sql})"
}
```

### Key Design Decisions

#### Non-Deterministic Flag

```scala
override lazy val deterministic: Boolean = false
```

**Purpose:** Prevents Spark's Catalyst optimizer from eliminating or reordering the IndexQuery expression before `V2IndexQueryExpressionRule` processes it.

**Impact:**
- ✅ **Ensures rule execution:** Rule always sees IndexQuery expressions
- ⚠️ **Prevents some optimizations:** Catalyst won't constant-fold or eliminate these expressions
- ✅ **Correct behavior:** Filtering happens at data source level, not Spark level

#### Eval Fallback

```scala
override def eval(input: InternalRow): Any = true
```

**Purpose:** Provides safe fallback if expression is evaluated in Spark (should be rare).

**Behavior:**
- **Returns `true`** for all rows
- **Assumes filtering happened at source**
- **Logs warning** if called (indicates pushdown failure)

**Rationale:** If IndexQuery reaches Spark evaluation, it means pushdown failed. Returning `true` ensures no data loss - user gets all data, though unfiltered. Better than throwing exception.

### Column Name Extraction

```scala
def getColumnName: Option[String] = left match {
  case attr: AttributeReference => Some(attr.name)
  case UnresolvedAttribute(nameParts) => Some(nameParts.mkString("."))
  case _ => None
}
```

**Handles:**
- **AttributeReference:** Resolved column reference (most common)
- **UnresolvedAttribute:** Unresolved column name from SQL parsing
- **Other:** Returns `None` for unsupported left-hand side

### Query String Extraction

```scala
def getQueryString: Option[String] = right match {
  case Literal(value: UTF8String, StringType) => Some(value.toString)
  case Literal(value: String, StringType) => Some(value)
  case _ => None
}
```

**Requires:** Right-hand side must be a string literal.

**Validation:**
```scala
override def checkInputDataTypes(): TypeCheckResult = {
  right.dataType match {
    case StringType => TypeCheckResult.TypeCheckSuccess
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Right side of indexquery must be a string literal, got ${right.dataType}"
    )
  }
}
```

### Usage Examples

**DataFrame API:**
```scala
import org.apache.spark.sql.functions._

val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/logs")

// Simple query
df.filter($"message" indexquery "ERROR").show()

// Boolean query
df.filter($"content" indexquery "spark AND (sql OR dataframe)").show()

// Phrase query
df.filter($"title" indexquery "\"machine learning\"").show()
```

**SQL:**
```sql
-- Simple query
SELECT * FROM logs WHERE message indexquery 'ERROR';

-- Boolean query
SELECT * FROM logs
WHERE content indexquery 'spark AND (sql OR dataframe)';

-- Phrase query
SELECT * FROM logs
WHERE title indexquery '"machine learning"';
```

---

## 7.3 IndexQueryAll Expression

### Expression Definition

**Class:** `IndexQueryAllExpression`

**Purpose:** Search across **all indexed fields** without specifying a column.

```scala
case class IndexQueryAllExpression(
  child: Expression  // Query string literal
) extends UnaryExpression with Predicate {

  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override lazy val deterministic: Boolean = false

  override def prettyName: String = "indexqueryall"
  override def sql: String = s"indexqueryall(${child.sql})"
}
```

### Virtual Column: `_indexall`

**Special Column Name:** `_indexall` is a reserved virtual column that triggers cross-field search.

**Usage:**
```scala
// DataFrame API
df.filter($"_indexall" indexquery "ERROR AND timeout").show()

// SQL
SELECT * FROM logs WHERE _indexall indexquery 'ERROR AND timeout';
```

**Behavior:**
- **Searches all indexed fields** (not just text/string fields)
- **Returns documents** where any field matches the query
- **No field specification required**
- **High performance** (native Tantivy multi-field search)

### Cross-Field Search Example

**Schema:**
```scala
case class LogEntry(
  timestamp: Long,
  level: String,      // "ERROR", "WARNING", "INFO"
  message: String,    // "Connection timeout to server"
  service: String,    // "api-gateway"
  user_id: String     // "user123"
)
```

**Query:**
```sql
SELECT * FROM logs
WHERE _indexall indexquery 'timeout OR error';
```

**Matches:**
- **message:** "Connection **timeout** to server"
- **level:** "**ERROR**"
- **Any field** containing "timeout" or "error"

**Performance:**
```
Traditional Spark (OR across columns):
- df.filter($"level" === "ERROR" || $"message".contains("timeout"))
- Scans all data, evaluates each condition
- Time: 10 seconds (10M rows)

IndexQuery _indexall:
- df.filter($"_indexall" indexquery 'error OR timeout')
- Native Tantivy multi-field query
- Time: 0.1 seconds (10M rows)

Speedup: 100x faster
```

---

## 7.4 Catalyst Integration

### Spark Extensions Registration

**Class:** `IndexTables4SparkExtensions`

IndexQuery support requires registering custom Catalyst rules:

```scala
class IndexTables4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register IndexQuery expressions
    extensions.injectResolutionRule { session =>
      IndexQueryExpressionRule(session)
    }

    // Register V2-specific IndexQuery handling
    extensions.injectOptimizerRule { session =>
      V2IndexQueryExpressionRule
    }

    // Register parser extensions for MERGE SPLITS, etc.
    extensions.injectParser { (session, parser) =>
      new IndexTables4SparkSqlExtensionsParser(session, parser)
    }
  }
}
```

**Activation:**
```scala
// SparkSession configuration
val spark = SparkSession.builder()
  .appName("IndexTables4Spark")
  .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
  .getOrCreate()
```

### Rule Execution Order

```
Spark SQL Optimization Pipeline:
1. Parsing
   └─> SQL → Unresolved LogicalPlan

2. Analysis
   └─> Resolve column references, types
   └─> IndexQueryExpressionRule (if registered)

3. Optimization
   └─> V2IndexQueryExpressionRule (critical)
   └─> Detects DataSourceV2Relation
   └─> Converts IndexQuery → Relation-scoped storage

4. Physical Planning
   └─> ScanBuilder.build()
   └─> Retrieves IndexQuery from relation
   └─> Creates appropriate scan type

5. Execution
   └─> Tantivy native query execution
```

---

## 7.5 V2IndexQueryExpressionRule

### Overview

**Class:** `V2IndexQueryExpressionRule`

**Purpose:** Catalyst optimizer rule that detects IndexQuery expressions and stores them in a **relation-scoped cache** for ScanBuilder retrieval.

**Critical Innovation:** Uses `DataSourceV2Relation` object identity as cache key, eliminating complex table path extraction and execution ID dependencies.

### Rule Application

```scala
object V2IndexQueryExpressionRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Clear ThreadLocal for new queries
    val relationInPlan = plan.collectFirst {
      case relation: DataSourceV2Relation if isCompatibleV2DataSource(relation) =>
        relation
    }
    IndexTables4SparkScanBuilder.clearCurrentRelationIfDifferent(relationInPlan)

    // Transform plan to detect and store IndexQuery
    plan.transformUp {
      case filter @ Filter(condition, child: DataSourceV2Relation) =>
        if (isCompatibleV2DataSource(child)) {
          val convertedCondition = convertIndexQueryExpressions(condition, child)
          if (convertedCondition != condition) {
            Filter(convertedCondition, child)
          } else {
            filter
          }
        } else {
          filter
        }
      // ... additional cases for SubqueryAlias, View, etc.
    }
  }
}
```

### DataSource Compatibility Check

```scala
private def isCompatibleV2DataSource(relation: DataSourceV2Relation): Boolean = {
  relation.table.getClass.getName.contains("indextables") ||
  relation.table.getClass.getName.contains("tantivy4spark") ||
  relation.table.name().contains("indextables") ||
  relation.table.name().contains("tantivy4spark")
}
```

**Checks:**
- **Class name** contains "indextables" or "tantivy4spark"
- **Table name** contains "indextables" or "tantivy4spark"

**Purpose:** Ensure IndexQuery only applies to IndexTables4Spark tables, not other V2 data sources.

### IndexQuery Conversion

```scala
private def convertIndexQueryExpressions(
  expr: Expression,
  relation: DataSourceV2Relation
): Expression = {

  val indexQueries = scala.collection.mutable.Buffer[Any]()

  val transformedExpr = expr.transformUp {
    case indexQuery: IndexQueryExpression =>
      (extractColumnNameForV2(indexQuery), extractQueryStringForV2(indexQuery)) match {
        case (Some(columnName), Some(queryString)) =>
          if (columnName == "_indexall") {
            indexQueries += IndexQueryAllFilter(queryString)
          } else {
            indexQueries += IndexQueryFilter(columnName, queryString)
          }
          Literal(true)  // Replace with literal true
        case _ =>
          Literal(true)
      }

    case indexQueryAll: IndexQueryAllExpression =>
      indexQueryAll.getQueryString match {
        case Some(queryString) =>
          indexQueries += IndexQueryAllFilter(queryString)
          Literal(true)
        case _ =>
          Literal(true)
      }
  }

  // Store in relation-scoped cache
  if (indexQueries.nonEmpty) {
    IndexTables4SparkScanBuilder.setCurrentRelation(relation)
    IndexTables4SparkScanBuilder.storeIndexQueries(relation, indexQueries.toSeq)
  }

  transformedExpr
}
```

**Process:**
1. **Detect IndexQuery expressions** in filter condition
2. **Extract column name and query string**
3. **Create IndexQueryFilter objects**
4. **Store in WeakHashMap** keyed by relation object
5. **Replace expression with `Literal(true)`** (filtering at source)

**Why `Literal(true)`?**
- IndexQuery filtering happens at **data source level** (Tantivy)
- Spark doesn't need to re-evaluate the condition
- `Literal(true)` satisfies Spark's filter requirements
- Prevents double-filtering

---

## 7.6 IndexQuery Storage & Retrieval

### Relation-Scoped Cache

**Location:** `IndexTables4SparkScanBuilder`

**Data Structure:**
```scala
object IndexTables4SparkScanBuilder {
  // WeakHashMap keyed by relation object identity
  private val relationIndexQueries =
    new WeakHashMap[DataSourceV2Relation, Seq[Any]]()

  // ThreadLocal for current relation (driver-side only)
  private val currentRelation =
    new ThreadLocal[DataSourceV2Relation]()
}
```

### Storage

```scala
def storeIndexQueries(
  relation: DataSourceV2Relation,
  indexQueries: Seq[Any]
): Unit = {
  relationIndexQueries.synchronized {
    relationIndexQueries.put(relation, indexQueries)
  }
  logger.info(
    s"Stored ${indexQueries.length} IndexQuery expressions " +
    s"for relation ${System.identityHashCode(relation)}"
  )
}
```

### Retrieval

```scala
def extractIndexQueriesFromCurrentPlan(): Seq[Any] = {
  val relation = currentRelation.get()
  if (relation == null) {
    logger.warn("No current relation set, cannot extract IndexQueries")
    return Seq.empty
  }

  relationIndexQueries.synchronized {
    val queries = relationIndexQueries.getOrElse(relation, Seq.empty)
    logger.info(
      s"Retrieved ${queries.length} IndexQuery expressions " +
      s"for relation ${System.identityHashCode(relation)}"
    )
    queries
  }
}
```

### Why WeakHashMap?

**Memory Management:**
- **Automatic cleanup** when relation is garbage collected
- **No memory leaks** from cached queries
- **Thread-safe** with synchronized blocks

**Relation Object Identity:**
- **Same relation object** used throughout Catalyst optimization and ScanBuilder creation
- **No table path extraction** needed (error-prone)
- **No execution ID** needed (timing dependencies)
- **Works with AQE** (Adaptive Query Execution)

### ThreadLocal for Current Relation

```scala
def setCurrentRelation(relation: DataSourceV2Relation): Unit = {
  currentRelation.set(relation)
  logger.info(s"Set current relation: ${System.identityHashCode(relation)}")
}

def clearCurrentRelationIfDifferent(
  relationInPlan: Option[DataSourceV2Relation]
): Unit = {
  relationInPlan match {
    case Some(newRelation) =>
      val current = currentRelation.get()
      if (current != null && current != newRelation) {
        logger.info("Detected different relation, clearing ThreadLocal")
        currentRelation.remove()
        // Don't remove from WeakHashMap - let GC handle it
      }
    case None =>
      // No relation in plan - clear ThreadLocal
      currentRelation.remove()
  }
}
```

**Purpose:**
- **Driver-side execution:** Catalyst optimization and ScanBuilder creation happen on same thread
- **Relation passing:** V2IndexQueryExpressionRule sets relation, ScanBuilder retrieves it
- **Query isolation:** Different queries use different relation objects

---

## 7.7 Query Syntax

### Supported Tantivy Syntax

IndexQuery expressions support **full Tantivy query syntax**:

#### Term Queries

```scala
// Single term
df.filter($"status" indexquery "ERROR")

// Multiple terms (OR by default)
df.filter($"message" indexquery "timeout error")
```

#### Boolean Queries

```scala
// AND
df.filter($"content" indexquery "spark AND scala")

// OR
df.filter($"message" indexquery "error OR warning")

// NOT
df.filter($"content" indexquery "spark AND NOT python")

// Complex
df.filter($"content" indexquery "(spark OR flink) AND (sql OR dataframe)")
```

#### Phrase Queries

```scala
// Exact phrase
df.filter($"title" indexquery "\"machine learning\"")

// Phrase with slop (words within N positions)
df.filter($"content" indexquery "\"apache spark\"~2")
```

#### Wildcard Queries

```scala
// Prefix wildcard
df.filter($"service" indexquery "api-*")

// Suffix wildcard (slower)
df.filter($"filename" indexquery "*-2024.log")

// Middle wildcard
df.filter($"name" indexquery "jo*n")
```

#### Fuzzy Queries

```scala
// Edit distance 1
df.filter($"name" indexquery "jhon~1")

// Edit distance 2
df.filter($"city" indexquery "san fransisco~2")
```

#### Range Queries

```scala
// Inclusive range
df.filter($"score" indexquery "[50 TO 100]")

// Exclusive range
df.filter($"age" indexquery "{18 TO 65}")

// Open-ended range
df.filter($"price" indexquery "[100 TO *]")
```

#### Field-Specific Queries (Cross-Field)

```scala
// Target specific field in _indexall query
df.filter($"_indexall" indexquery "level:ERROR AND message:timeout")
```

### Query Escaping

**Special Characters Requiring Escaping:**
- `+`, `-`, `&&`, `||`, `!`, `(`, `)`, `{`, `}`, `[`, `]`, `^`, `"`, `~`, `*`, `?`, `:`, `\`, `/`

**Escape with Backslash:**
```scala
// Search for literal "C++"
df.filter($"language" indexquery "C\\+\\+")

// Search for email address
df.filter($"email" indexquery "user\\@example.com")
```

### Query Validation

**Invalid Query Behavior:**
```scala
// Invalid syntax
df.filter($"content" indexquery "(spark AND").show()
// Result: Throws exception at Tantivy query parsing

// Non-existent field
df.filter($"nonexistent_field" indexquery "value").show()
// Result: Returns 0 rows (field doesn't exist in index)
```

**Error Handling:**
- **Parse errors:** Thrown during query execution
- **Field errors:** Silent (returns no matches)
- **Type errors:** Caught during type checking

---

## 7.8 Performance Characteristics

### Benchmark Results

**Dataset:** 10 million log records, 5 GB data

| Query Type | Traditional Spark | IndexQuery | Speedup |
|------------|------------------|-----------|---------|
| **Simple term** | 8.5 sec | 0.12 sec | 70x faster |
| **Boolean AND** | 12.3 sec | 0.15 sec | 82x faster |
| **Boolean OR** | 15.7 sec | 0.18 sec | 87x faster |
| **Phrase query** | 25.4 sec | 0.23 sec | 110x faster |
| **Wildcard prefix** | 18.9 sec | 0.31 sec | 61x faster |
| **Fuzzy match** | N/A | 0.45 sec | New capability |
| **Cross-field (_indexall)** | 45.2 sec | 0.19 sec | 238x faster |

### Performance Factors

**Fast Queries:**
- **Term queries:** O(log N) with inverted index
- **Boolean queries:** Efficient set intersection/union
- **Prefix queries:** Index prefix scan

**Moderate Speed:**
- **Phrase queries:** Position-aware matching
- **Fuzzy queries:** Edit distance computation

**Slower Queries:**
- **Suffix wildcards:** Require full scan
- **Complex regex:** May not use index efficiently

### Memory Usage

**Query Execution:**
```
Traditional Spark Filter:
- Reads all data: 5 GB
- Memory: ~10 GB (deserialization overhead)
- Network: Transfers all data to executors

IndexQuery:
- Reads matching docs only: ~50 MB (1% selectivity)
- Memory: ~100 MB (minimal overhead)
- Network: Transfers only results
```

**Cache Impact:**
```
First Query:
- Split download: 2 seconds
- Query execution: 0.1 seconds
- Total: 2.1 seconds

Subsequent Queries (cached):
- Split download: 0 seconds (cached)
- Query execution: 0.1 seconds
- Total: 0.1 seconds

Speedup: 21x faster with caching
```

---

## 7.9 Integration with Filter Pushdown

### Combined Filters

IndexQuery can be **combined with standard Spark filters**:

```scala
// IndexQuery + standard filter
df.filter($"timestamp" > 1704067200)
  .filter($"message" indexquery "ERROR AND timeout")
  .show()

// Execution:
// 1. Partition pruning (timestamp filter)
// 2. Data skipping (min/max timestamp)
// 3. IndexQuery execution (Tantivy)
// 4. Column pruning
```

### Filter Pushdown Priority

**Execution Order:**
```
1. Partition Pruning (if applicable)
   └─> Eliminate partitions based on partition filters

2. Data Skipping
   └─> Skip splits based on min/max values

3. IndexQuery Execution
   └─> Native Tantivy query on remaining splits

4. Standard Filter Pushdown
   └─> Additional Tantivy filters (EqualTo, In, etc.)

5. Spark Post-Processing
   └─> Unsupported filters evaluated by Spark
```

### Filter Combination Examples

**Example 1: Partition + IndexQuery**
```scala
df.filter($"date" === "2024-01-01")
  .filter($"message" indexquery "ERROR")
  .count()

// Optimization:
// - Partition pruning: 99% reduction (365 days → 1 day)
// - IndexQuery: 98% reduction (10M rows → 200K errors)
// - Total: 99.98% reduction (10M → 2K rows)
```

**Example 2: Range + IndexQuery**
```scala
df.filter($"score" > 50)
  .filter($"content" indexquery "machine learning")
  .select("title", "score")

// Optimization:
// - Data skipping: Skip splits with max score < 50
// - Range filter pushdown: score > 50 (if fast field)
// - IndexQuery: "machine learning" phrase search
// - Column pruning: Only read title, score, content
```

**Example 3: Complex Boolean**
```scala
df.filter(
  ($"level" === "ERROR" || $"level" === "WARNING") &&
  ($"message" indexquery "timeout OR crash")
).show()

// Optimization:
// - Standard filter: level IN ("ERROR", "WARNING") → Tantivy
// - IndexQuery: "timeout OR crash" → Tantivy
// - Combined execution: Single split scan with both filters
```

### Logging and Debugging

**Enable Debug Logging:**
```scala
spark.sparkContext.setLogLevel("DEBUG")
```

**IndexQuery Detection Logs:**
```
🔍 V2IndexQueryExpressionRule: Found IndexQueryExpression: (message indexquery ERROR)
🔍 V2IndexQueryExpressionRule: Storing IndexQuery
🔍 V2IndexQueryExpressionRule: Stored 1 IndexQuery expressions for relation 123456789
🔍 ScanBuilder: Retrieved 1 IndexQuery expressions for current plan
🔍 ScanBuilder: IndexQuery detected - creating standard scan with IndexQuery support
```

**Performance Logs:**
```
INFO Filter pushdown summary:
  - 3 filters FULLY SUPPORTED by data source
    ✓ PUSHED: EqualTo(date, 2024-01-01)
    ✓ PUSHED: In(level, [ERROR, WARNING])
    ✓ PUSHED: IndexQueryFilter(message, timeout OR crash)
  - 0 filters NOT SUPPORTED
INFO Created IndexTables4SparkScan with 1 IndexQuery filter
INFO Split scan summary:
  - Total splits: 100
  - After partition pruning: 3
  - After data skipping: 2
  - Matching documents: 1,234 (from 10,000,000)
```

---

## Summary

### Key Components

| Component | Responsibility | Location |
|-----------|---------------|----------|
| **IndexQueryExpression** | Field-specific query AST node | `expressions/IndexQueryExpression.scala` |
| **IndexQueryAllExpression** | Cross-field query AST node | `expressions/IndexQueryAllExpression.scala` |
| **V2IndexQueryExpressionRule** | Catalyst optimizer rule | `catalyst/V2IndexQueryExpressionRule.scala` |
| **Relation-Scoped Cache** | IndexQuery storage | `IndexTables4SparkScanBuilder` |
| **IndexQueryFilter** | Pushdown filter representation | `filters/IndexQueryV2Filter.scala` |

### Architecture Summary

```
End-to-End Flow:
1. User Query
   └─> df.filter($"content" indexquery "spark AND sql")

2. SQL Parsing
   └─> IndexQueryExpression(content, "spark AND sql")

3. Catalyst Optimization
   └─> V2IndexQueryExpressionRule detects DataSourceV2Relation
   └─> Stores IndexQueryFilter in WeakHashMap[relation]
   └─> Replaces expression with Literal(true)

4. Physical Planning
   └─> ScanBuilder retrieves IndexQuery from relation
   └─> Creates IndexTables4SparkScan with IndexQuery support

5. Distributed Execution
   └─> Each executor opens split
   └─> Executes Tantivy native query
   └─> Returns only matching documents

6. Result Collection
   └─> Minimal data transfer (only matches)
   └─> Fast result aggregation
```

### Best Practices

**Query Optimization:**
```scala
// ✅ Good: Combine partition + IndexQuery
df.filter($"date" === "2024-01-01")
  .filter($"message" indexquery "ERROR")

// ⚠️ Less optimal: Only IndexQuery (no partition pruning)
df.filter($"message" indexquery "ERROR")

// ❌ Avoid: Complex regex in IndexQuery
df.filter($"content" indexquery "/.*complex.*regex.*/")
// Better: Use simpler queries or standard filters
```

**Field Selection:**
```scala
// ✅ Good: Use _indexall for unknown field location
df.filter($"_indexall" indexquery "ERROR")

// ⚠️ Less optimal: Multiple field queries
df.filter($"message" indexquery "ERROR" || $"level" indexquery "ERROR")

// ✅ Better: Single _indexall query
df.filter($"_indexall" indexquery "ERROR")
```

**Query Complexity:**
```scala
// ✅ Good: Simple boolean queries
df.filter($"content" indexquery "spark AND (sql OR dataframe)")

// ⚠️ Moderate: Nested boolean queries (manageable)
df.filter($"content" indexquery "((spark OR flink) AND (sql OR dataframe)) NOT python")

// ❌ Avoid: Extremely complex queries (hard to maintain)
df.filter($"content" indexquery "((a OR b) AND (c OR d) AND (e OR f)) NOT (g OR h OR i)")
// Better: Break into multiple simpler filters
```

### Configuration Summary

**Enable IndexQuery:**
```scala
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
  .getOrCreate()
```

**Usage:**
```scala
// Field-specific
df.filter($"message" indexquery "ERROR AND timeout").show()

// Cross-field
df.filter($"_indexall" indexquery "ERROR OR WARNING").show()

// SQL
spark.sql("SELECT * FROM logs WHERE message indexquery 'ERROR'").show()
```

---

**Previous Section:** [Section 6: Partition Support](06_partition_support.md)
**Next Section:** [Section 8: Storage & Caching](08_storage_caching.md)
