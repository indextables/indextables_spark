# 05 — Search Engine, Filters, Expressions, FFI, JSON

This chapter covers everything under `spark/search/`, `spark/filters/`, `spark/expressions/`, `spark/arrow/`, `spark/json/`, `spark/schema/`, and `spark/exceptions/`. Together these modules turn Spark expressions and `Filter`s into Tantivy queries, execute them against `.split` files, and translate the results back into Spark rows.

## Search façade

### `search/SplitSearchEngine.scala`
The modern search engine used by every read path. Wraps `tantivy4java`'s `SplitSearcher` and its global `SplitCacheManager`, handling split open/close, footer retrieval, and `SplitQuery` execution. All partition readers instantiate one of these per split (or reuse one via the cache). Replaces an older zip-file-based engine that is no longer used.

### `search/IndexTablesSearchEngine.scala`
Defines `class TantivySearchEngine` (plus companion `object`). Historical façade around the Tantivy index writer used by the build-side of a few tests. Wraps `TantivyDirectInterface` with `CaseInsensitiveStringMap` options handling and working-directory setup. Kept because the indexing/test harness still references it — the production read path uses `SplitSearchEngine`.

### `search/IndexTablesDirectInterface.scala`
Defines `class TantivyDirectInterface` (plus companion `object`). The core FFI wrapper for a Tantivy `Index` + `IndexWriter` on a single executor. Handles document appending, commit, and the global schema-creation lock that prevents two threads from racing when building a new Tantivy schema. Per-executor lifecycle: one instance per table path.

### `search/IndexTablesJavaInterface.scala`
Contains `object TantivyJavaAdapter` (the actual `Long`-handle → `TantivyDirectInterface` map used when legacy callers pass opaque handles) and `object TantivyNative` (compatibility shim for older call sites). Exists purely for backward compatibility with handle-based interop.

### `search/QueryParser.scala`
Parses a human query string (supporting wildcards, phrases, field-specific queries, prefix queries) into a Tantivy `Query` object. Used when an `IndexQueryFilter` is pushed down.

### `search/RowConverter.scala`
Bidirectional JSON ↔ `InternalRow` converter for a given `StructType`. Used on the indexing side (row → JSON document to add to Tantivy) and in a few diagnostic read paths.

### `search/SchemaConverter.scala`
Converts Spark `StructType` to the Tantivy schema JSON that the native index expects, and back again. Used during write-side schema construction and when reflecting on a split's schema.

## Filters (IndexQuery custom filter types)

These are the "virtual filters" that represent `col indexquery 'q'` syntax. They round-trip through Spark's `SupportsPushDownFilters` machinery.

### `filters/IndexQueryFilter.scala`
Case class wrapping a column name + Tantivy query string. `references()` returns the column so Spark can reason about it like any other filter. The scan builder recognizes instances of this class.

### `filters/IndexQueryAllFilter.scala`
Sibling of `IndexQueryFilter` for the `indexqueryall('q')` variant, which searches across every indexed field. `references()` is empty so Spark does not tie it to a specific column.

### `filters/IndexQueryV2Filter.scala`
A Catalyst `Expression` + `Predicate` bridge. Since Spark V2 pushdown wants expressions, not filters, `V2IndexQueryExpressionRule` rewrites `IndexQueryExpression` into this class, and `IndexTables4SparkScanBuilder` later recovers the underlying `IndexQueryFilter` via `toFilter()`.

### `filters/IndexQueryRegistry.scala`
Query-scoped registry of IndexQuery metadata keyed by UUID instead of `ThreadLocal`. This is important because Spark's scheduler can run catalyst rules and scan builders on different threads for the same query; the UUID keys let them find each other reliably.

## Expressions (Catalyst expression types)

### `expressions/IndexQueryExpression.scala`
The `BinaryExpression` implementing the `col indexquery 'query_string'` operator. Non-deterministic so the optimizer never folds it away. The V2 rule rewrites it into an `IndexQueryV2Filter`.

### `expressions/IndexQueryAllExpression.scala`
The `UnaryExpression` for `indexqueryall('query_string')`. Same machinery, no left-hand column.

### `expressions/BucketExpressions.scala`
Sealed trait `BucketExpression` plus `DateHistogramExpression`, `HistogramExpression`, `RangeExpression`. These are the expression trees produced by the bucket aggregation SQL functions. They appear inside `Aggregate` GROUP BY keys until `V2BucketExpressionRule` strips them out.

### `expressions/BucketAggregationConfig.scala`
Serializable config objects (`DateHistogramConfig`, `HistogramConfig`, `RangeConfig`) extracted from the expressions above. These are what the scan builder captures in its `ThreadLocal` and hands to the reader so the aggregation can be executed natively.

### `expressions/BucketFunctionBuilder.scala`
Builder registered with the SparkSession function registry. Parses the literal arguments of `indextables_date_histogram(...)`, `indextables_histogram(...)`, and `indextables_range(...)` calls into the appropriate `BucketExpression`.

## Arrow FFI bridges

Arrow's C Data Interface is how the connector avoids copying rows between Rust and the JVM.

### `arrow/ArrowFfiBridge.scala`
Read direction: owns a `RootAllocator`, `CDataDictionaryProvider`, and the `ArrowArray`/`ArrowSchema` C structs used to import data from `tantivy4java`. Reused across batches inside a partition reader to minimize allocation churn.

### `arrow/ArrowFfiWriteBridge.scala`
Write direction: owns a `BufferAllocator` and `VectorSchemaRoot`, knows how to write every Spark type (including nested `Struct`/`List`/`Map`) into Arrow vectors, and exports them via FFI structs. See chapter 03.

### `arrow/AggregationArrowFfiHelper.scala`
Higher-level helper for aggregations. Encapsulates the FFI protocol for schema introspection, single-split and multi-split aggregation calls, and Arrow import into `ColumnarBatch`. Used by `SimpleAggregateColumnarReader` and `GroupByAggregateColumnarReader`.

### `arrow/AggregationArrowFfiConfig.scala`
Config flag controlling whether aggregation uses Arrow FFI or the legacy JSON-based path (default: enabled).

## JSON / struct converters

Used when the table has `StructType`, `ArrayType`, or `MapType` columns that are stored as Tantivy JSON fields.

### `json/SparkSchemaToTantivyMapper.scala`
Decides whether a given Spark field should map to a Tantivy JSON field. Considers the field's Spark type and any explicit `typemap` configuration.

### `json/SparkToTantivyConverter.scala`
Converts a Spark `Row` value (including nested structs/arrays/maps) into the Java collection shape that `tantivy4java` expects when indexing.

### `json/TantivyToSparkConverter.scala`
The inverse: walks a Java `Map`/`List` retrieved from Tantivy and rebuilds a Spark `Row`/`Array`/`Map` with the correct types.

### `json/JsonPredicateTranslator.scala`
Translates Spark `Filter`s that target nested JSON/array fields into the Tantivy query syntax for JSON fields. This is what makes `filter($"address.city" === "Seattle")` pushdown-eligible.

## Schema mapping

### `schema/SchemaMapping.scala`
The big orchestrator. On the write side it decides, for each Spark field, what Tantivy field type to create (string, text, JSON, fast, indexed, etc.). On the read side it recovers the original Spark types from the Tantivy schema stored in split metadata. Delegates to the `json/` converters for nested types and to `util/TimestampUtils` for temporal types. Also throws `SchemaConflictException` when mixing incompatible schemas across appends.

## Exceptions

### `exceptions/IndexQueryParseException.scala`
Raised when a Tantivy query string fails to parse (unbalanced parentheses, unclosed quotes, invalid syntax). Carries the offending query string and field name for good error messages.

## How the pieces connect

```
Pushdown (logical plan side):
  user: WHERE content indexquery 'machine learning'
   └─ IndexQueryExpression        (expressions/)
       └─ V2IndexQueryExpressionRule (chapter 02)
            └─ IndexQueryV2Filter   (filters/)
                 └─ IndexQueryFilter (filters/)
                      └─ IndexTables4SparkScanBuilder

Aggregation pushdown:
  user: GROUP BY indextables_date_histogram(ts,'1d')
   └─ BucketFunctionBuilder → DateHistogramExpression
        └─ V2BucketExpressionRule → DateHistogramConfig in ScanBuilder ThreadLocal

Execution (on the executor):
  PartitionReader
    └─ SplitSearchEngine.openSplit()
         └─ FiltersToQueryConverter builds SplitQuery
              ├─ JsonPredicateTranslator  (nested fields)
              ├─ SchemaMapping            (Spark type ↔ Tantivy type)
              └─ QueryParser              (IndexQuery)
         └─ SplitSearcher.searchArrowFfi()
              └─ ArrowFfiBridge imports ColumnarBatch
                   └─ TantivyToSparkConverter for JSON columns
```

The next chapter covers how those `.split` files get in and out of S3/Azure/HDFS in the first place.
