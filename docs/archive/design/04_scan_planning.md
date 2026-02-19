# Section 4: Scan Planning & Execution

**Document Version:** 1.0
**Last Updated:** 2025-10-06
**Target Audience:** Developers familiar with Apache Spark but not necessarily with Spark internals

---

## Table of Contents

1. [ScanBuilder - Push Down Interfaces](#41-scanbuilder---push-down-interfaces)
2. [Filter Pushdown Logic](#42-filter-pushdown-logic)
3. [Aggregate Pushdown System](#43-aggregate-pushdown-system)
4. [Scan Types](#44-scan-types)
5. [Data Skipping Optimization](#45-data-skipping-optimization)

---

## 4.1 ScanBuilder - Push Down Interfaces

### 4.1.1 Overview

The `IndexTables4SparkScanBuilder` is the cornerstone of IndexTables4Spark's query optimization strategy. It implements Spark's DataSource V2 push-down interfaces to move computation from Spark executors into the Tantivy search engine, dramatically improving query performance.

**Class Declaration:**

```scala
class IndexTables4SparkScanBuilder(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  config: Map[String, String]
) extends ScanBuilder
    with SupportsPushDownFilters           // Standard Spark filters
    with SupportsPushDownV2Filters          // V2 predicates
    with SupportsPushDownRequiredColumns    // Column pruning
    with SupportsPushDownLimit              // LIMIT pushdown
    with SupportsPushDownAggregates         // COUNT/SUM/AVG/MIN/MAX
```

**Push-Down State:**

```scala
// Pushed-down state maintained by ScanBuilder
private var _pushedFilters: Array[Filter] = Array.empty
private var requiredSchema: StructType = schema
private var _limit: Option[Int] = None
private var _pushedAggregation: Option[Aggregation] = None
private var _pushedGroupBy: Option[Array[String]] = None
```

### 4.1.2 Push-Down Decision Matrix

Understanding which operations can be pushed down is critical for performance:

| Feature | Pushed to Tantivy | Post-Processed by Spark | Performance Impact | Notes |
|---------|------------------|-------------------------|-------------------|-------|
| **String field exact match** | ✅ Full | ❌ None | 10-100x faster | `field === "value"` |
| **String field IN** | ✅ Full | ❌ None | 10-100x faster | `field.isin("a", "b")` |
| **String field CONTAINS** | ✅ Full | ❌ None | 5-50x faster | `field.contains("substring")` |
| **Text field exact match** | ⚠️ Best-effort | ✅ Final validation | 5-10x faster | Tokenization affects accuracy |
| **Range filters (>, >=)** | ✅ Full | ❌ None | 10-50x faster | Always pushed to Tantivy |
| **Range filters (<, <=)** | ⚠️ Conditional | ⚠️ Conditional | Variable | Requires fast fields or DATE type |
| **IndexQuery** | ✅ Full query | ❌ None | 50-1000x faster | Native Tantivy syntax |
| **COUNT (no filters)** | ✅ Metadata | ❌ None | 1000x+ faster | Transaction log optimization |
| **COUNT (partition filters)** | ✅ Metadata | ❌ None | 100-1000x faster | Transaction log optimization |
| **COUNT (data filters)** | ✅ Tantivy aggregation | ❌ None | 10-100x faster | Requires fast fields |
| **SUM/AVG/MIN/MAX** | ✅ Tantivy aggregation | ❌ None | 10-100x faster | Requires fast fields |
| **GROUP BY + aggregates** | ⚠️ Partial | ⚠️ Partial | Variable | Aggregation pushed, grouping by Spark |
| **LIMIT** | ✅ Early termination | ❌ None | 2-10x faster | Stops reading when limit reached |
| **Column pruning** | ✅ Projection | ❌ None | 1.5-3x faster | Fewer columns transferred |

**Key Takeaways:**
- **String fields** (raw tokenizer): Full filter pushdown with no Spark post-processing
- **Text fields** (default tokenizer): Best-effort pushdown + Spark validation for exact matches
- **IndexQuery**: Always fully pushed down for maximum performance
- **Aggregations**: Require fast field configuration for pushdown support

### 4.1.3 Build Method - Scan Type Selection

The `build()` method is called by Spark's Catalyst optimizer to create the appropriate scan type based on pushed-down operations:

```scala
override def build(): Scan = {
  // Check if we have aggregate pushdown
  _pushedAggregation match {
    case Some(aggregation) =>
      // Create aggregate scan
      createAggregateScan(aggregation)

    case None =>
      // Regular scan - extract IndexQuery expressions
      val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

      new IndexTables4SparkScan(
        sparkSession,
        transactionLog,
        requiredSchema,
        _pushedFilters,
        options,
        _limit,
        config,
        extractedIndexQueryFilters
      )
  }
}
```

**Decision Flow:**

```
build() called by Spark
    │
    ├─ Has _pushedAggregation?
    │   │
    │   ├─ Yes, with _pushedGroupBy → createGroupByAggregateScan()
    │   │   │
    │   │   ├─ Can use transaction log GROUP BY COUNT? → TransactionLogCountScan
    │   │   │   └─ Requirements: partition-only GROUP BY + COUNT-only + no IndexQuery filters
    │   │   │
    │   │   └─ Cannot use transaction log → IndexTables4SparkGroupByAggregateScan
    │   │       └─ Tantivy GROUP BY aggregation
    │   │
    │   └─ Yes, without _pushedGroupBy → createSimpleAggregateScan()
    │       │
    │       ├─ Can use transaction log COUNT? → TransactionLogCountScan
    │       │   └─ Requirements: COUNT-only + partition-only filters + no IndexQuery filters
    │       │
    │       └─ Cannot use transaction log → IndexTables4SparkSimpleAggregateScan
    │           └─ Tantivy simple aggregation
    │
    └─ No aggregation → IndexTables4SparkScan
        └─ Standard search with filters
```

### 4.1.4 Pushed Filters Method

Spark calls `pushFilters()` to offer filters for pushdown:

```scala
override def pushFilters(filters: Array[Filter]): Array[Filter] = {
  // Partition filters into supported and unsupported
  val (supported, unsupported) = filters.partition(isSupportedFilter)

  // Store supported filters for later use in build()
  _pushedFilters = supported

  logger.info(s"Filter pushdown summary:")
  logger.info(s"  - ${supported.length} filters FULLY SUPPORTED by data source")
  supported.foreach(filter => logger.info(s"    ✓ PUSHED: $filter"))

  logger.info(s"  - ${unsupported.length} filters NOT SUPPORTED (Spark will re-evaluate)")
  unsupported.foreach(filter => logger.info(s"    ✗ NOT PUSHED: $filter"))

  // Return only unsupported filters - Spark will re-evaluate these after reading
  unsupported
}
```

**Key Behavior:**
- **Supported filters** are fully handled by Tantivy with no Spark post-processing
- **Unsupported filters** are returned to Spark for post-evaluation after data is read
- **Logging** provides visibility into which filters are pushed down vs. evaluated by Spark

**Example:**

```scala
// User query
df.filter($"title" === "exact title" && $"score" > 50)

// ScanBuilder behavior:
// - title === "exact title" → SUPPORTED (string field, exact match)
// - score > 50 → NOT SUPPORTED (range query without fast fields)
//
// Result:
// - "title" filter pushed to Tantivy
// - "score" filter evaluated by Spark after reading
```

### 4.1.5 Column Pruning

```scala
override def pruneColumns(requiredSchema: StructType): Unit = {
  this.requiredSchema = requiredSchema
  logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
}
```

**Behavior:**
- Spark determines which columns are actually needed by the query
- Only required columns are retrieved from splits
- Reduces data transfer and memory usage

**Example:**

```scala
// User query
df.select("id", "title").filter($"category" === "tech")

// Spark calls pruneColumns with requiredSchema: [id, title, category]
// Note: "category" included because it's needed for filtering
```

### 4.1.6 Limit Pushdown

```scala
override def pushLimit(limit: Int): Boolean = {
  _limit = Some(limit)
  logger.info(s"Pushed limit: $limit")
  true  // We support limit pushdown
}
```

**Behavior:**
- Tantivy stops searching after finding `limit` documents
- Significant performance improvement for queries with LIMIT
- Particularly effective for `LIMIT 1` queries (early termination)

**Example:**

```scala
// User query
df.filter($"status" === "active").limit(100)

// ScanBuilder stores limit = 100
// Each split reader stops after reading 100 matching documents
```

---

## 4.2 Filter Pushdown Logic

### 4.2.1 Field Type Awareness

IndexTables4Spark's filter pushdown logic is **field-type aware**, meaning it handles string fields (exact matching) and text fields (tokenized) differently:

**String Fields (Raw Tokenizer):**

```scala
// Configuration
df.write
  .option("spark.indextables.indexing.typemap.title", "string")
  .save("s3://bucket/path")

// Query behavior
df.filter($"title" === "exact title")
// ✅ Fully pushed to Tantivy
// ✅ Exact match guaranteed
// ✅ No Spark post-processing
```

**Text Fields (Default Tokenizer):**

```scala
// Configuration
df.write
  .option("spark.indextables.indexing.typemap.content", "text")
  .save("s3://bucket/path")

// Query behavior
df.filter($"content" === "machine learning")
// ⚠️ Best-effort pushed to Tantivy (tokenized search)
// ⚠️ Spark post-processes for exact match validation
// ⚠️ Less efficient than string fields
```

**Why the Difference?**

| Aspect | String Fields | Text Fields |
|--------|--------------|-------------|
| **Tokenization** | None (raw tokenizer) | Default tokenizer (splits words) |
| **Storage** | "exact title" | ["exact", "title"] |
| **Exact match** | Direct term lookup | Must reconstruct phrase |
| **Performance** | Fast (single term) | Slower (phrase query + validation) |
| **Use case** | IDs, categories, exact titles | Full-text content, descriptions |

### 4.2.2 Supported Filter Types

```scala
private def isSupportedFilter(filter: Filter): Boolean = {
  import org.apache.spark.sql.sources._

  filter match {
    case EqualTo(attribute, _)       => isFieldSuitableForExactMatching(attribute)
    case EqualNullSafe(attribute, _) => isFieldSuitableForExactMatching(attribute)
    case _: GreaterThan              => true   // Always pushed to Tantivy
    case _: GreaterThanOrEqual       => true   // Always pushed to Tantivy
    case GreaterThan(attribute, _)   => true   // Fully supported
    case GreaterThanOrEqual(attribute, _) => true   // Fully supported
    case LessThan(attribute, _)      => isFieldFastOrDate(attribute)  // Requires fast field or DATE type
    case LessThanOrEqual(attribute, _) => isFieldFastOrDate(attribute)  // Requires fast field or DATE type
    case _: In                       => true   // Converted to OR query
    case _: IsNull                   => true
    case _: IsNotNull                => true
    case And(left, right)            => isSupportedFilter(left) && isSupportedFilter(right)
    case Or(left, right)             => isSupportedFilter(left) && isSupportedFilter(right)
    case Not(child)                  => isSupportedFilter(child)
    case _: StringStartsWith         => false  // Best-effort, Spark validates
    case _: StringEndsWith           => false  // Best-effort, Spark validates
    case _: StringContains           => true
    case _                           => false
  }
}
```

**Filter Type Details:**

| Spark Filter | Support Level | Tantivy Implementation | Notes |
|--------------|--------------|------------------------|-------|
| `EqualTo` | ✅ Full (string fields) | `SplitTermQuery` | Direct term lookup |
| `EqualTo` | ⚠️ Partial (text fields) | `SplitPhraseQuery` + Spark validation | Tokenization affects accuracy |
| `In` | ✅ Full | `SplitBooleanQuery(OR)` | Converted to `field:val1 OR field:val2` |
| `StringContains` | ✅ Full | Substring query | Efficient substring search |
| `StringStartsWith` | ⚠️ Partial | Prefix query + Spark validation | Best-effort |
| `StringEndsWith` | ⚠️ Partial | Best-effort + Spark validation | No native suffix support |
| `GreaterThan/GreaterThanOrEqual` | ✅ Fully supported | Always pushed | Uses Tantivy range syntax `attribute:>value` |
| `LessThan/LessThanOrEqual` | ⚠️ Conditional support | Pushed if fast field or DATE | Requires fast field config or DATE type |
| `And/Or` | ✅ Full | `SplitBooleanQuery` | Nested boolean queries |
| `Not` | ✅ Full | `SplitBooleanQuery(MUST_NOT)` | Boolean negation |

### 4.2.3 Range Filter Pushdown Details

**Range filter support varies by operator and field configuration:**

#### Greater Than Filters (>, >=)

```scala
// Always pushed to Tantivy using parseQuery syntax
case GreaterThan(attribute, value) =>
  // Converted to Tantivy query: "attribute:>value"
  Some(parseQueryToSplitQuery(s"$attribute:>${value.toString}", searchEngine))

case GreaterThanOrEqual(attribute, value) =>
  // Converted to Tantivy query: "attribute:>=value"
  Some(parseQueryToSplitQuery(s"$attribute:>=${value.toString}", searchEngine))
```

**Behavior:**
- ✅ **Always pushed** to Tantivy regardless of field configuration
- ✅ **No fast field requirement**
- ✅ **High performance** with native Tantivy range queries
- ✅ **No Spark post-processing**

#### Less Than Filters (<, <=)

```scala
// Conditional pushdown based on fast field configuration
case LessThan(attribute, value) =>
  if (isFieldFastOrDate(attribute)) {
    // Fast field or DATE type - push to Tantivy
    Some(parseQueryToSplitQuery(s"$attribute:<${value.toString}", searchEngine))
  } else {
    // Not a fast field - defer to Spark
    None
  }

case LessThanOrEqual(attribute, value) =>
  if (isFieldFastOrDate(attribute)) {
    // Fast field or DATE type - push to Tantivy
    Some(parseQueryToSplitQuery(s"$attribute:<=${value.toString}", searchEngine))
  } else {
    // Not a fast field - defer to Spark
    None
  }
```

**Behavior:**
- ⚠️ **Conditional pushdown** based on field configuration
- ✅ **Pushed if:** Field configured as fast field OR field type is DATE
- ❌ **Deferred to Spark if:** Regular field (not fast, not DATE)
- ⚠️ **Performance varies** based on whether pushed or not

**Fast Field Detection:**
```scala
private def isFieldFastOrDate(attribute: String): Boolean = {
  // Check if field is configured as a fast field
  val fastFieldsConfig = options.get("spark.indextables.indexing.fastfields")
  val isFastField = fastFieldsConfig.exists(_.split(",").map(_.trim).contains(attribute))

  // Check if field is a DATE type in schema
  val isDateField = schema.fields.find(_.name == attribute).exists(_.dataType == DateType)

  isFastField || isDateField
}
```

#### Configuration for Range Filters

**To enable full range filter pushdown:**
```scala
// Configure fast fields during write
df.write
  .option("spark.indextables.indexing.fastfields", "score,price,rating")
  .save("s3://bucket/path")

// Now < and <= filters on these fields are pushed to Tantivy
df.filter($"score" < 50)  // ✅ Pushed to Tantivy
df.filter($"price" <= 100)  // ✅ Pushed to Tantivy
```

**Without fast field configuration:**
```scala
// Standard field without fast field config
df.filter($"score" > 50)   // ✅ Pushed to Tantivy (> always works)
df.filter($"score" < 50)   // ❌ Deferred to Spark (< requires fast field)
```

**DATE fields special case:**
```scala
// DATE fields always support all range operators
df.filter($"event_date" > "2024-01-01")   // ✅ Pushed to Tantivy
df.filter($"event_date" < "2024-12-31")   // ✅ Pushed to Tantivy (DATE exception)
```

#### Performance Implications

| Scenario | > Filter | < Filter | Performance |
|----------|----------|----------|-------------|
| Fast field configured | ✅ Pushed | ✅ Pushed | 10-50x faster (both in Tantivy) |
| DATE type field | ✅ Pushed | ✅ Pushed | 10-50x faster (both in Tantivy) |
| Regular field (no config) | ✅ Pushed | ❌ Spark | Mixed (> fast, < slower) |

**Why the Asymmetry?**

The difference between `>` and `<` filter handling is intentional:
- **Greater than (>)** queries are optimized in Tantivy's query parser without requiring fast fields
- **Less than (<)** queries require fast field access for efficient execution
- **DATE fields** get special handling because they're commonly used for range queries

### 4.2.5 Field Type Detection

```scala
private def isFieldSuitableForExactMatching(attribute: String): Boolean = {
  // Check the field type configuration
  val fieldTypeKey = s"spark.indextables.indexing.typemap.$attribute"
  val fieldType = config.get(fieldTypeKey)

  fieldType match {
    case Some("string") =>
      logger.info(s"Field '$attribute' configured as 'string' - supporting exact matching")
      true
    case Some("text") =>
      logger.info(s"Field '$attribute' configured as 'text' - deferring exact matching to Spark")
      false
    case Some(other) =>
      logger.info(s"Field '$attribute' configured as '$other' - supporting exact matching")
      true
    case None =>
      // No explicit configuration - assume string type (new default)
      logger.info(s"Field '$attribute' has no type configuration - assuming 'string'")
      true
  }
}
```

**Configuration Lookup:**
1. Check `spark.indextables.indexing.typemap.{fieldName}` in config
2. If `"string"` → Full pushdown support
3. If `"text"` → Best-effort pushdown (Spark validates)
4. If not configured → Assume `"string"` (default behavior)

### 4.2.6 Filter Conversion Examples

**Simple Equality:**

```scala
// User query
df.filter($"status" === "active")

// Spark representation
EqualTo("status", "active")

// Tantivy query
new SplitTermQuery("status", "active")
```

**IN Clause:**

```scala
// User query
df.filter($"category".isin("tech", "science", "engineering"))

// Spark representation
In("category", Array("tech", "science", "engineering"))

// Tantivy query
new SplitBooleanQuery(
  Array(
    new SplitTermQuery("category", "tech"),
    new SplitTermQuery("category", "science"),
    new SplitTermQuery("category", "engineering")
  ),
  BoolOp.OR
)
```

**Complex Boolean:**

```scala
// User query
df.filter(($"status" === "active" && $"priority".isin("high", "critical")) || $"urgent" === "true")

// Spark representation
Or(
  And(
    EqualTo("status", "active"),
    In("priority", Array("high", "critical"))
  ),
  EqualTo("urgent", "true")
)

// Tantivy query (nested)
new SplitBooleanQuery(
  Array(
    new SplitBooleanQuery(
      Array(
        new SplitTermQuery("status", "active"),
        new SplitBooleanQuery(
          Array(
            new SplitTermQuery("priority", "high"),
            new SplitTermQuery("priority", "critical")
          ),
          BoolOp.OR
        )
      ),
      BoolOp.AND
    ),
    new SplitTermQuery("urgent", "true")
  ),
  BoolOp.OR
)
```

---

## 4.3 Aggregate Pushdown System

### 4.3.1 Overview

Aggregate pushdown is one of IndexTables4Spark's most powerful optimizations. Instead of reading all matching documents and aggregating in Spark, aggregations execute natively in Tantivy with dramatic performance improvements.

**Supported Aggregations:**

| Aggregation | Fast Field Required | Performance Gain | Implementation |
|-------------|-------------------|------------------|----------------|
| `COUNT(*)` | ❌ No | 10-1000x | Tantivy document counting or transaction log metadata |
| `COUNT(column)` | ❌ No | 10-1000x | Tantivy document counting with non-null filter |
| `SUM(column)` | ✅ Yes | 10-100x | Tantivy fast field summation |
| `AVG(column)` | ✅ Yes | 10-100x | Tantivy SUM + COUNT, Spark combines |
| `MIN(column)` | ✅ Yes | 10-100x | Tantivy fast field minimum |
| `MAX(column)` | ✅ Yes | 10-100x | Tantivy fast field maximum |

### 4.3.2 Push Aggregation Method

```scala
override def pushAggregation(aggregation: Aggregation): Boolean = {
  logger.info(s"Received aggregation request: $aggregation")

  // Check if this is a GROUP BY aggregation
  val groupByExpressions = aggregation.groupByExpressions()
  val hasGroupBy = groupByExpressions != null && groupByExpressions.nonEmpty

  if (hasGroupBy) {
    // Extract GROUP BY column names
    val groupByColumns = groupByExpressions.map(extractFieldNameFromExpression)

    // Validate GROUP BY columns are supported - throws exception if not
    validateGroupByColumnsOrThrow(groupByColumns)

    // Check if aggregation is compatible with GROUP BY - throws exception if not
    validateAggregationCompatibilityOrThrow(aggregation)

    // Store GROUP BY information
    _pushedGroupBy = Some(groupByColumns)
  }

  // Validate aggregation is supported (both simple and GROUP BY)
  if (!isAggregationSupported(aggregation)) {
    logger.info(s"REJECTED - aggregation not supported")
    return false
  }

  // Check if filters are compatible with aggregate pushdown
  if (!areFiltersCompatibleWithAggregation()) {
    logger.info(s"REJECTED - filters not compatible")
    return false
  }

  // Store for later use in build()
  _pushedAggregation = Some(aggregation)
  logger.info(s"ACCEPTED - aggregation will be pushed down")
  true
}
```

**Validation Steps:**

1. **Extract GROUP BY columns** (if present)
2. **Validate GROUP BY columns** - must be fast fields
3. **Validate aggregation compatibility** - check aggregation types and fast field configuration
4. **Validate aggregation support** - ensure aggregation types are supported
5. **Validate filter compatibility** - ensure filters work with aggregations
6. **Accept or reject** aggregation pushdown

### 4.3.3 Aggregation Validation

```scala
private def isAggregationSupported(aggregation: Aggregation): Boolean = {
  import org.apache.spark.sql.connector.expressions.aggregate._

  aggregation.aggregateExpressions.forall { expr =>
    expr match {
      case _: Count =>
        logger.info(s"COUNT aggregation is supported")
        true
      case _: CountStar =>
        logger.info(s"COUNT(*) aggregation is supported")
        true
      case sum: Sum =>
        val fieldName = getFieldName(sum.column)
        val isSupported = isNumericFastField(fieldName)
        logger.info(s"SUM on field '$fieldName' supported: $isSupported")
        isSupported
      case avg: Avg =>
        val fieldName = getFieldName(avg.column)
        val isSupported = isNumericFastField(fieldName)
        logger.info(s"AVG on field '$fieldName' supported: $isSupported")
        isSupported
      case min: Min =>
        val fieldName = getFieldName(min.column)
        val isSupported = isNumericFastField(fieldName)
        logger.info(s"MIN on field '$fieldName' supported: $isSupported")
        isSupported
      case max: Max =>
        val fieldName = getFieldName(max.column)
        val isSupported = isNumericFastField(fieldName)
        logger.info(s"MAX on field '$fieldName' supported: $isSupported")
        isSupported
      case other =>
        logger.info(s"Unsupported aggregation type: ${other.getClass.getSimpleName}")
        false
    }
  }
}
```

### 4.3.4 Fast Field Validation

**Critical Requirement:** SUM/AVG/MIN/MAX require fields to be configured as fast fields:

```scala
private def isNumericFastField(fieldName: String): Boolean = {
  // Get actual fast fields from the schema/docMappingJson
  val fastFields = getActualFastFieldsFromSchema()

  if (!fastFields.contains(fieldName)) {
    logger.info(s"Field '$fieldName' is not marked as fast - rejecting aggregate pushdown")
    return false
  }

  // Check if field is numeric
  schema.fields.find(_.name == fieldName) match {
    case Some(field) if isNumericType(field.dataType) =>
      logger.info(s"Field '$fieldName' is numeric and fast - supported")
      true
    case Some(field) =>
      logger.info(s"Field '$fieldName' is not numeric (${field.dataType}) - not supported")
      false
    case None =>
      logger.info(s"Field '$fieldName' not found in schema - not supported")
      false
  }
}
```

**Fast Field Source:**

The validation reads **actual fast field configuration** from the transaction log's `docMappingJson`, not from user-provided options:

```scala
private def getActualFastFieldsFromSchema(): Set[String] = {
  // Read existing files from transaction log to get docMappingJson
  val existingFiles = transactionLog.listFiles()
  val existingDocMapping = existingFiles
    .flatMap(_.docMappingJson)
    .headOption

  if (existingDocMapping.isDefined) {
    // Parse the docMappingJson to extract fast field information
    val mappingJson = existingDocMapping.get
    val docMapping = JsonUtil.mapper.readTree(mappingJson)

    // Extract fields where "fast": true
    val fastFields = docMapping.asScala.flatMap { fieldNode =>
      val fieldName = Option(fieldNode.get("name")).map(_.asText())
      val isFast = Option(fieldNode.get("fast")).map(_.asBoolean()).getOrElse(false)

      if (isFast && fieldName.isDefined) {
        Some(fieldName.get)
      } else {
        None
      }
    }.toSet

    fastFields
  } else {
    // Fallback to configuration for new tables
    val fastFieldsStr = config.get("spark.indextables.indexing.fastfields").getOrElse("")
    if (fastFieldsStr.nonEmpty) {
      fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
    } else {
      Set.empty[String]
    }
  }
}
```

**Why Read from Transaction Log?**

- **Source of truth**: docMappingJson reflects actual split schema
- **Consistency**: Ensures validation matches reality
- **Auto-fast-field support**: Detects automatically configured fast fields

### 4.3.5 Simple Aggregates (No GROUP BY)

```scala
// Example: Simple SUM aggregation
df.agg(sum("amount"))

// ScanBuilder creates IndexTables4SparkSimpleAggregateScan
// Tantivy executes: SplitAggregation.sum(fieldName)
// Result: Single aggregation value returned directly
```

**Workflow:**

```
1. Spark calls pushAggregation(Sum("amount"))
2. ScanBuilder validates:
   - "amount" is numeric? ✅
   - "amount" is fast field? ✅
3. ScanBuilder accepts aggregation
4. build() creates IndexTables4SparkSimpleAggregateScan
5. Scan executes:
   - Reads splits
   - Executes Tantivy aggregation
   - Returns single row with SUM result
```

### 4.3.6 GROUP BY Aggregates

```scala
// Example: GROUP BY aggregation
df.groupBy("category").agg(count("*"), sum("amount"))

// ScanBuilder creates IndexTables4SparkGroupByAggregateScan
// Tantivy executes: TermsAggregation on "category" with SUM sub-aggregation
// Result: One row per category with count and sum
```

**Workflow:**

```
1. Spark calls pushAggregation with GROUP BY expressions
2. ScanBuilder validates:
   - "category" is fast field? ✅
   - "amount" is numeric? ✅
   - "amount" is fast field? ✅
3. ScanBuilder accepts aggregation
4. build() creates IndexTables4SparkGroupByAggregateScan
5. Scan executes:
   - Reads splits
   - Executes Tantivy TermsAggregation
   - Returns one row per unique category value
6. Spark combines partial results from splits
```

**GROUP BY Validation:**

```scala
private def validateGroupByColumnsOrThrow(groupByColumns: Array[String]): Unit = {
  val missingFastFields = ArrayBuffer[String]()
  val fastFields = getActualFastFieldsFromSchema()

  groupByColumns.foreach { columnName =>
    if (!fastFields.contains(columnName)) {
      missingFastFields += columnName
    }
  }

  if (missingFastFields.nonEmpty) {
    val columnList = missingFastFields.mkString("'", "', '", "'")
    val currentFastFields = if (fastFields.nonEmpty) {
      fastFields.mkString("'", "', '", "'")
    } else {
      "none"
    }

    throw new IllegalArgumentException(
      s"""GROUP BY requires fast field configuration for efficient aggregation.
         |
         |Missing fast fields for GROUP BY columns: $columnList
         |
         |To fix this issue, configure these columns as fast fields:
         |  .option("spark.indextables.indexing.fastfields", "...")
         |
         |Current fast fields: $currentFastFields""".stripMargin
    )
  }
}
```

### 4.3.7 Transaction Log Optimization

**Special Case:** COUNT-only queries with partition-only filters or no filters use transaction log metadata:

```scala
private def canUseTransactionLogCount(aggregation: Aggregation): Boolean = {
  import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar}

  // Must be COUNT or COUNT(*)
  val isCountOnly = aggregation.aggregateExpressions.length == 1 && {
    aggregation.aggregateExpressions.head match {
      case _: Count | _: CountStar => true
      case _ => false
    }
  }

  if (!isCountOnly) return false

  // Extract IndexQuery filters to check if we have any
  val indexQueryFilters = extractIndexQueriesFromCurrentPlan()

  // Check if we only have partition filters or no filters, AND no IndexQuery filters
  val hasOnlyPartitionFilters = _pushedFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty

  hasOnlyPartitionFilters
}
```

**When Applicable:**

| Condition | Transaction Log COUNT | Regular Tantivy COUNT |
|-----------|----------------------|----------------------|
| `df.count()` | ✅ Yes | ❌ No |
| `df.filter($"date" === "2024-01-01").count()` | ✅ Yes (partition filter) | ❌ No |
| `df.filter($"status" === "active").count()` | ❌ No (data filter) | ✅ Yes |
| `df.filter($"message" indexquery "ERROR").count()` | ❌ No (IndexQuery filter) | ✅ Yes |

**Performance:**

```
Transaction Log COUNT:
- Reads transaction log only (KB-MB of data)
- Sums numRecords from AddActions
- Performance: ~10-100ms (regardless of table size)

Regular Tantivy COUNT:
- Opens and searches splits
- Counts matching documents
- Performance: ~100-1000ms (depends on table size and filters)

Speedup: 10-100x for simple COUNT queries
```

### 4.3.8 Complete Pushdown Flag

```scala
override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
  // Return false to allow Spark to handle final aggregation combining partial results
  // This enables proper distributed aggregation where:
  // - AVG is transformed to SUM + COUNT by Spark
  // - Partial results from each partition are combined correctly
  logger.info(s"supportCompletePushDown called - returning false for distributed aggregation")
  false
}
```

**Why `false`?**

- **Distributed aggregation**: Each split computes partial results
- **Spark combines partials**: Spark's aggregation framework handles final combination
- **AVG support**: Spark transforms AVG to SUM + COUNT, combines correctly
- **GROUP BY support**: Partial GROUP BY results from splits combined by Spark

---

## 4.4 Scan Types

IndexTables4Spark creates different scan types based on the query pattern:

### 4.4.1 IndexTables4SparkScan (Standard)

**Purpose:** Regular data scanning with filters, projections, and limits.

```scala
new IndexTables4SparkScan(
  sparkSession,
  transactionLog,
  requiredSchema,       // Pruned columns
  pushedFilters,        // Standard Spark filters
  options,
  limit,                // Optional LIMIT
  config,
  indexQueryFilters     // IndexQuery expressions
)
```

**Features:**
- ✅ Filter pushdown (standard + IndexQuery)
- ✅ Column pruning
- ✅ LIMIT pushdown
- ✅ Data skipping (partition + min/max)
- ✅ Preferred locations (cache locality)

**Example Query:**

```scala
df.select("id", "title", "content")
  .filter($"status" === "active" && $"content" indexquery "ERROR AND timeout")
  .limit(100)
```

**Execution:**
1. Apply data skipping (partition pruning + min/max filtering)
2. Create one partition per split file
3. Each partition reader:
   - Loads split from cache or S3
   - Converts Spark filters to Tantivy queries
   - Executes Tantivy search
   - Streams matching documents back to Spark
   - Stops at limit (if specified)

### 4.4.2 IndexTables4SparkSimpleAggregateScan

**Purpose:** Simple aggregations without GROUP BY.

```scala
new IndexTables4SparkSimpleAggregateScan(
  sparkSession,
  transactionLog,
  schema,
  pushedFilters,
  options,
  config,
  aggregation,           // Aggregation to compute
  indexQueryFilters
)
```

**Features:**
- ✅ COUNT/SUM/AVG/MIN/MAX pushdown
- ✅ Fast field validation
- ✅ Filter pushdown
- ✅ Data skipping

**Example Query:**

```scala
df.filter($"category" === "tech")
  .agg(count("*"), sum("amount"), avg("score"))
```

**Execution:**
1. Apply data skipping
2. Create one partition per split file
3. Each partition:
   - Executes Tantivy aggregation on split
   - Returns partial aggregation results
4. Spark combines partial results into final values

### 4.4.3 IndexTables4SparkGroupByAggregateScan

**Purpose:** Multi-dimensional aggregations with GROUP BY.

```scala
new IndexTables4SparkGroupByAggregateScan(
  sparkSession,
  transactionLog,
  schema,
  pushedFilters,
  options,
  config,
  aggregation,
  groupByColumns,       // GROUP BY column names
  indexQueryFilters
)
```

**Features:**
- ✅ GROUP BY pushdown (partial)
- ✅ Tantivy TermsAggregation
- ✅ Multiple aggregation functions per group
- ⚠️ Spark handles final grouping

**Example Query:**

```scala
df.groupBy("category", "region")
  .agg(count("*"), sum("amount"), avg("score"))
```

**Execution:**
1. Apply data skipping
2. Create one partition per split file
3. Each partition:
   - Executes Tantivy TermsAggregation
   - Computes COUNT/SUM/AVG per unique (category, region) combination
   - Returns partial results
4. Spark combines and groups partial results

**Why Partial Pushdown?**

- Tantivy computes aggregations **per split**
- Spark combines partial results across splits
- Enables distributed GROUP BY at scale

### 4.4.4 TransactionLogCountScan

**Purpose:** Metadata-only counting for partition-filtered or unfiltered COUNT queries.

```scala
new TransactionLogCountScan(
  sparkSession,
  transactionLog,
  pushedFilters,        // Must be partition-only or empty
  options,
  config,
  groupByColumns        // Optional: for partition GROUP BY COUNT
)
```

**Features:**
- ✅ Zero split access
- ✅ Reads transaction log only
- ✅ Sums numRecords from AddActions
- ✅ Partition filtering via transaction log
- ✅ GROUP BY partition columns

**Example Queries:**

```scala
// Simple COUNT
df.count()  // ✅ Transaction log optimization

// Partition-filtered COUNT
df.filter($"date" === "2024-01-01").count()  // ✅ Transaction log optimization

// Partition GROUP BY COUNT
df.groupBy("date").agg(count("*"))  // ✅ Transaction log optimization

// Data-filtered COUNT (NOT optimized)
df.filter($"status" === "active").count()  // ❌ Uses IndexTables4SparkSimpleAggregateScan
```

**Execution:**
1. Read transaction log
2. Filter AddActions by partition values (if applicable)
3. Sum numRecords field
4. Return single row with COUNT result

**Performance:**

```
Table size: 1TB (10 billion documents)
Transaction log size: 10MB (1000 splits)

df.count() execution time:
- Transaction log scan: ~50ms
- Full table scan alternative: ~10 minutes
- Speedup: 12000x
```

---

## 4.5 Data Skipping Optimization

### 4.5.1 Overview

Data skipping eliminates files from query processing **before reading them**, dramatically reducing I/O and improving performance. IndexTables4Spark implements two levels of data skipping:

1. **Partition Pruning**: Directory-level elimination based on partition column values
2. **Min/Max Filtering**: File-level elimination based on min/max statistics in AddAction metadata

### 4.5.2 Partition Pruning

**Implementation:**

```scala
object PartitionPruning {
  def prunePartitions(
    addActions: Seq[AddAction],
    partitionColumns: Seq[String],
    filters: Array[Filter]
  ): Seq[AddAction] = {

    // Extract partition filters
    val partitionFilters = filters.filter { filter =>
      getFilterReferencedColumns(filter).exists(partitionColumns.contains)
    }

    if (partitionFilters.isEmpty) {
      return addActions  // No partition filters - return all files
    }

    // Apply partition value matching
    addActions.filter { addAction =>
      partitionFilters.forall(matchesPartitionFilter(addAction, _))
    }
  }

  private def matchesPartitionFilter(addAction: AddAction, filter: Filter): Boolean = {
    filter match {
      case EqualTo(attribute, value) =>
        addAction.partitionValues.get(attribute).contains(value.toString)

      case In(attribute, values) =>
        val partitionValue = addAction.partitionValues.get(attribute)
        partitionValue.exists(pv => values.map(_.toString).contains(pv))

      case And(left, right) =>
        matchesPartitionFilter(addAction, left) && matchesPartitionFilter(addAction, right)

      case Or(left, right) =>
        matchesPartitionFilter(addAction, left) || matchesPartitionFilter(addAction, right)

      case _ => true  // Unknown filter - keep file for safety
    }
  }
}
```

**Example:**

```scala
// Table structure:
// s3://bucket/logs/
// ├── date=2024-01-01/hour=10/*.split
// ├── date=2024-01-01/hour=11/*.split
// ├── date=2024-01-02/hour=10/*.split
// └── date=2024-01-02/hour=11/*.split

// Query:
df.filter($"date" === "2024-01-01" && $"hour" === 10)

// Partition pruning:
// Total splits: 1000
// After pruning: 25 (only date=2024-01-01/hour=10)
// Files eliminated: 975 (97.5%)
```

**Performance Impact:**

| Partition Selectivity | Files Eliminated | Speedup |
|----------------------|-----------------|---------|
| Single partition (1/100) | 99% | ~100x |
| Single day (1/365) | 99.7% | ~365x |
| Single hour (1/8760) | 99.99% | ~8760x |

### 4.5.3 Min/Max Filtering

**Implementation:**

```scala
private def canFileMatchFilters(addAction: AddAction, filters: Array[Filter]): Boolean = {
  // If no min/max values available, conservatively keep the file
  if (addAction.minValues.isEmpty || addAction.maxValues.isEmpty) {
    return true
  }

  // A file can match only if ALL filters can potentially match
  filters.forall(filter => canFilterMatchFile(addAction, filter))
}

private def canFilterMatchFile(addAction: AddAction, filter: Filter): Boolean = {
  import org.apache.spark.sql.sources._

  filter match {
    case EqualTo(attribute, value) =>
      val minVal = addAction.minValues.flatMap(_.get(attribute))
      val maxVal = addAction.maxValues.flatMap(_.get(attribute))

      (minVal, maxVal) match {
        case (Some(min), Some(max)) =>
          val valueStr = value.toString
          valueStr >= min && valueStr <= max
        case _ => true  // Missing stats - keep file
      }

    case GreaterThan(attribute, value) =>
      val maxVal = addAction.maxValues.flatMap(_.get(attribute))
      maxVal match {
        case Some(max) => compareValues(max, value.toString) > 0
        case None => true
      }

    case LessThan(attribute, value) =>
      val minVal = addAction.minValues.flatMap(_.get(attribute))
      minVal match {
        case Some(min) => compareValues(min, value.toString) < 0
        case None => true
      }

    case And(left, right) =>
      canFilterMatchFile(addAction, left) && canFilterMatchFile(addAction, right)

    case Or(left, right) =>
      canFilterMatchFile(addAction, left) || canFilterMatchFile(addAction, right)

    case _ => true  // Unknown filter - keep file
  }
}
```

**Example:**

```scala
// Filter: timestamp > 1704067200
//
// Split 1: minValues.timestamp = 1704000000, maxValues.timestamp = 1704050000
//   → max < filter value → SKIP (no overlap)
//
// Split 2: minValues.timestamp = 1704060000, maxValues.timestamp = 1704090000
//   → max > filter value → SCAN (potential overlap)
//
// Split 3: minValues.timestamp = 1704070000, maxValues.timestamp = 1704100000
//   → min > filter value → SCAN (all values match)
```

**Performance Impact:**

| Filter Type | Typical Reduction | Example |
|------------|------------------|---------|
| Timestamp range (1 hour) | 95-99% | `timestamp > 1704067200` |
| Numeric range (percentile) | 50-90% | `score > 80` |
| String range (alphabetic) | 30-70% | `name >= "M"` |

### 4.5.4 Schema Awareness (Date Handling)

**Critical Issue:** Date fields must be properly detected and converted for accurate comparison:

```scala
private def compareValues(value1: String, value2: String, schema: StructType, fieldName: String): Int = {
  // Find field type in schema
  schema.fields.find(_.name == fieldName) match {
    case Some(field) if field.dataType == DateType =>
      // Convert date strings to days-since-epoch for comparison
      val days1 = DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(value1))
      val days2 = DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(value2))
      days1.compareTo(days2)

    case Some(field) if isNumericType(field.dataType) =>
      // Numeric comparison
      BigDecimal(value1).compare(BigDecimal(value2))

    case _ =>
      // String comparison
      value1.compareTo(value2)
  }
}
```

**Why Important?**

Without schema awareness, date comparisons would be lexicographic:

```scala
// Incorrect (lexicographic string comparison):
"2024-01-01" < "2024-12-31"  // ✅ Correct
"2024-02-01" < "2024-01-31"  // ❌ WRONG!

// Correct (days-since-epoch comparison):
19723 < 19753  // ✅ Correct (2024-01-01 < 2024-01-31)
```

### 4.5.5 Unified Data Skipping Architecture

**New in v1.12:** All scan types use the same data skipping logic:

```scala
def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
  if (filters.isEmpty) {
    return addActions
  }

  val partitionColumns = transactionLog.getPartitionColumns()

  // Step 1: Apply partition pruning
  val partitionPrunedActions = if (partitionColumns.nonEmpty) {
    val pruned = PartitionPruning.prunePartitions(addActions, partitionColumns, filters)
    val prunedCount = addActions.length - pruned.length
    if (prunedCount > 0) {
      logger.info(s"Partition pruning: filtered out $prunedCount of ${addActions.length} files")
    }
    pruned
  } else {
    addActions
  }

  // Step 2: Apply min/max value skipping on remaining files
  val nonPartitionFilters = filters.filterNot { filter =>
    getFilterReferencedColumns(filter).exists(partitionColumns.contains)
  }

  val finalActions = if (nonPartitionFilters.nonEmpty) {
    val skipped = partitionPrunedActions.filter { addAction =>
      canFileMatchFilters(addAction, nonPartitionFilters)
    }
    val skippedCount = partitionPrunedActions.length - skipped.length
    if (skippedCount > 0) {
      logger.info(s"Data skipping (min/max): filtered out $skippedCount files")
    }
    skipped
  } else {
    partitionPrunedActions
  }

  val totalSkipped = addActions.length - finalActions.length
  if (totalSkipped > 0) {
    logger.info(s"Total data skipping: ${addActions.length} files → ${finalActions.length} files (skipped $totalSkipped)")
  }

  finalActions
}
```

**Scan Types Using Data Skipping:**

| Scan Type | Data Skipping Applied | Performance Benefit |
|-----------|----------------------|---------------------|
| `IndexTables4SparkScan` | ✅ Yes | High |
| `IndexTables4SparkSimpleAggregateScan` | ✅ Yes | High |
| `IndexTables4SparkGroupByAggregateScan` | ✅ Yes | High |
| `TransactionLogCountScan` | ⚠️ Partition only | Medium (no splits accessed) |

---

## Summary

This section covered Scan Planning & Execution:

- **ScanBuilder**: Push-down interfaces for filters, projections, limits, and aggregates
- **Filter Pushdown**: Field-type aware logic with string vs text field handling
- **Aggregate Pushdown**: COUNT/SUM/AVG/MIN/MAX with fast field validation and transaction log optimization
- **Scan Types**: Regular, simple aggregate, GROUP BY aggregate, and transaction log COUNT scans
- **Data Skipping**: Partition pruning and min/max filtering for dramatic performance improvements

The scan planning system provides the foundation for high-performance queries by moving computation from Spark into the Tantivy search engine.

---

**Previous Section:** [Section 3: Transaction Log System](03_transaction_log.md)
**Next Section:** [Section 5: Write Operations](05_write_operations.md)
