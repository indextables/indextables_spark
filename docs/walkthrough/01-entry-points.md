# 01 — Entry Points

These are the classes Spark discovers first: the `TableProvider` that backs `spark.read.format(...)` and `df.write.format(...)`, and the `SparkSessionExtensions` that registers the custom SQL parser and optimizer rules.

## Public aliases

These classes exist so that user code never has to import the `spark.*` internal packages. They are thin subclasses that add no behavior — everything lives in the internal implementation classes.

### `provider/IndexTablesProvider.scala`
Public alias for the DataSource V2 `TableProvider`. Users write `df.write.format("io.indextables.provider.IndexTablesProvider")`. It extends `IndexTables4SparkTableProvider` and is otherwise empty. The internal class name (`io.indextables.spark.core.IndexTables4SparkTableProvider`) is still valid and is used extensively in the test suite — the two are interchangeable.

### `extensions/IndexTablesSparkExtensions.scala`
Public alias for `IndexTables4SparkExtensions`. Used in `spark.sql.extensions` configuration to register the SQL parser, catalyst rules, and bucket functions.

## Internal entry points

### `spark/core/IndexTables4SparkDataSource.scala`
This is the primary DataSource V2 entry point. It defines:

- **`IndexTables4SparkTableProvider`** — implements `TableProvider` and `DataSourceRegister` (short name `"indextables"`). Responsible for schema inference from the transaction log and delegating table creation.
- **`IndexTables4SparkTable`** — implements `SupportsRead` and `SupportsWrite`. Holds the table path, schema, and options, and hands off to a `ScanBuilder` or `WriteBuilder`. It also owns the lifecycle of the `TransactionLog` cache for a given logical table.

Configuration merging (Spark session conf → Hadoop conf → write/read options) happens here with well-defined precedence, so downstream code can read from a single `CaseInsensitiveStringMap`.

### `spark/extensions/IndexTables4SparkExtensions.scala`
The real implementation behind the public alias. As a `SparkSessionExtensions => Unit` function it:

- Injects `sql/IndexTables4SparkSqlParser` so the custom `MERGE SPLITS`, `PURGE INDEXTABLE`, `DESCRIBE INDEXTABLES …` grammar is recognized.
- Registers the SQL functions `tantivy4spark_indexquery`, `indextables_date_histogram`, `indextables_histogram`, `indextables_range`, and friends via `expressions/BucketFunctionBuilder`.
- Injects two optimizer rules from `catalyst/`:
  - `V2IndexQueryExpressionRule` — rewrites IndexQuery expressions into pushdown-friendly V2 filters.
  - `V2BucketExpressionRule` — rewrites bucket aggregation expressions in `GROUP BY` clauses.

### `spark/sql/IndexTables4SparkSessionExtension.scala`
A second, narrower `SparkSessionExtensions` hook. It exists so the custom SQL parser can be installed without also registering the Catalyst rules (used in a handful of embedded contexts). The parent extensions class delegates to this for parser installation.

## Relationships

```
user: spark.read.format("io.indextables.provider.IndexTablesProvider")
  └─ provider/IndexTablesProvider
       └─ core/IndexTables4SparkTableProvider     (schema inference)
            └─ core/IndexTables4SparkTable         (SupportsRead/SupportsWrite)
                 ├─ core/IndexTables4SparkScanBuilder   (read — chapter 02)
                 └─ core/IndexTables4SparkWriteBuilder  (write — chapter 03)

user: spark.sql.extensions = "io.indextables.extensions.IndexTablesSparkExtensions"
  └─ extensions/IndexTablesSparkExtensions
       └─ spark/extensions/IndexTables4SparkExtensions
            ├─ sql/IndexTables4SparkSqlParser         (chapter 08)
            ├─ catalyst/V2IndexQueryExpressionRule    (chapter 02)
            ├─ catalyst/V2BucketExpressionRule        (chapter 02)
            └─ expressions/BucketFunctionBuilder       (chapter 05)
```

The next chapter follows the read path from `IndexTables4SparkTable.newScanBuilder()` all the way down to the Tantivy split.
