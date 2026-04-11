# IndexTables4Spark Code Walkthrough

This walkthrough describes the structure of the IndexTables4Spark source tree and the purpose of each module. It is written to orient a new reader: it explains what each package is for, what the individual source files inside it do, and how the modules connect to each other.

IndexTables4Spark is an Apache Spark DataSource V2 connector that turns a Spark table into a high-performance, full-text-searchable index backed by [Tantivy](https://github.com/quickwit-oss/tantivy) (via the `tantivy4java` JNI binding). A table is stored as a collection of immutable Quickwit `.split` files plus a Delta-Lake-style `_transaction_log/`. Read and write paths run entirely inside Spark executors — there is no server process.

## Top-level package layout

```
src/main/scala/io/indextables/
├── provider/          — Public TableProvider alias
├── extensions/        — Public SparkSessionExtensions alias
└── spark/
    ├── core/          — DataSource V2 entry points, scans, writes, partition readers
    ├── catalyst/      — Logical-plan optimizer rules for pushdown
    ├── extensions/    — Internal SparkSessionExtensions implementation
    ├── sql/           — Custom SQL commands (MERGE, PURGE, PREWARM, DESCRIBE …)
    │   └── parser/    — ANTLR AST builder for the SQL extensions
    ├── transaction/   — Delta-Lake-style transaction log (native + Scala layers)
    ├── io/            — Cloud storage providers
    │   └── merge/     — Async download/upload pipeline for merge jobs
    ├── merge/         — Async merge-on-write system
    ├── purge/         — Purge-on-write config and counters
    ├── prewarm/       — Cache prewarming
    ├── storage/       — Split locality / conversion throttling
    ├── search/        — Tantivy search facade and query building
    ├── filters/       — Custom IndexQuery filter types
    ├── expressions/   — IndexQuery and bucket aggregation Catalyst expressions
    ├── arrow/         — Arrow FFI bridges (read + write)
    ├── json/          — Spark ↔ Tantivy JSON/struct converters
    ├── schema/        — SchemaMapping (write/read schema orchestration)
    ├── exceptions/    — Public exception types
    ├── sync/          — Companion sync readers (Delta/Iceberg/Parquet)
    ├── auth/          — Credential providers (Unity Catalog, SDK adapters)
    ├── utils/         — Credential provider factory
    ├── config/        — Config entries and SQLConf keys
    ├── write/         — Write-path config (optimized write, arrow sizing)
    ├── metrics/       — Spark UI custom metrics (read pipeline, data skipping)
    ├── stats/         — Data skipping effectiveness counters
    ├── memory/        — Native memory pool initialization
    └── util/          — Shared helpers (paths, filters, configs, sizes, …)
```

## Reading order

Each chapter is self-contained but they build on each other:

1. [01 — Entry Points](01-entry-points.md) — the public provider and extension classes Spark loads first.
2. [02 — Read Path](02-read-path.md) — how a `spark.read` call becomes a scan, gets pushdown, and returns `ColumnarBatch`es; also covers split locality and cache prewarming.
3. [03 — Write Path](03-write-path.md) — how a `df.write.save()` call becomes split files and a committed transaction.
4. [04 — Transaction Log](04-transaction-log.md) — the Delta-style log that is the source of truth for table state.
5. [05 — Search Engine](05-search-engine.md) — the Tantivy facade, query parsing, filters, and FFI bridges.
6. [06 — Storage and Merge I/O](06-storage-io.md) — cloud storage abstraction and the async merge I/O pipeline.
7. [07 — Merge and Purge](07-merge-and-purge.md) — post-commit maintenance subsystems (merge-on-write and purge-on-write).
8. [08 — SQL Commands](08-sql-commands.md) — the `INDEXTABLES`/`TANTIVY4SPARK` SQL extension.
9. [09 — Companion Sync](09-sync-companion.md) — building search companions for external Delta/Parquet/Iceberg tables.
10. [10 — Config, Auth, Utilities](10-config-auth-util.md) — cross-cutting infrastructure.

## How the pieces fit together

A one-paragraph mental model:

> When Spark loads the data source, `provider/IndexTablesProvider` is instantiated. Reads flow through `core/IndexTables4SparkScanBuilder` → `core/IndexTables4SparkScan` → one of the columnar `*PartitionReader`s, which use `search/SplitSearchEngine` to query `.split` files and return data via `arrow/ArrowFfiBridge`. The read path is assisted by `storage/DriverSplitLocalityManager` (sticky split→host assignments) and `prewarm/` (cache warming). Writes flow through `core/IndexTables4SparkWriteBuilder` → `IndexTables4SparkOptimizedWrite`/`StandardWrite` → `IndexTables4SparkArrowDataWriter`, which exports rows via `arrow/ArrowFfiWriteBridge` into Tantivy. Every read and write consults the `transaction/` log for file listings and commits. Filter and aggregation pushdown is coordinated by `catalyst/` rules plus `filters/`, `expressions/`, and `json/JsonPredicateTranslator`. Storage is abstracted behind `io/CloudStorageProvider`. Post-commit maintenance (merge-on-write, purge-on-write) runs through singletons in `merge/` and `purge/`. SQL extensions in `sql/` add operational commands, and `sync/` builds companion indexes over foreign Delta/Parquet/Iceberg tables.

Subsequent chapters zoom in on each of those responsibilities.
