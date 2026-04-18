# 10 — Config, Auth, Metrics, Memory, Utilities

The remaining packages are cross-cutting infrastructure: they don't define their own execution path but are used from every other chapter. This chapter covers `spark/config/`, `spark/auth/`, `spark/utils/`, `spark/metrics/`, `spark/stats/`, `spark/memory/`, `spark/write/`, and `spark/util/`.

## Configuration

### `config/IndexTables4SparkConfig.scala`
Table-level config entries stored *inside* the transaction log metadata (so they travel with the table). Defines a type-safe `ConfigEntry` trait plus `BooleanConfigEntry`, `LongConfigEntry`, `StringConfigEntry`. Each entry knows how to serialize and deserialize itself from the flat string map the transaction log uses.

### `config/IndexTables4SparkSQLConf.scala`
Every `spark.indextables.*` and legacy `spark.tantivy4spark.*` config key the connector understands — AWS credentials, Azure credentials, merge settings, cache settings, writer settings, companion settings, and so on. The constants defined here are referenced from across the codebase so there is a single source of truth for key names.

## Authentication and credentials

### `auth/unity/UnityCatalogAWSCredentialProvider.scala`
AWS credential provider that calls the Databricks Unity Catalog HTTP API to mint scoped, short-lived credentials. Features:

- Intelligent caching with expiration tracking and a pre-expiry refresh buffer.
- Fallback from `PATH_READ_WRITE` to `PATH_READ` scope when write access is not needed.
- Used automatically when a table resides in a Unity-Catalog-managed location.

### `auth/V1ToV2CredentialsProviderAdapter.scala`
Adapter that wraps an AWS SDK v1 `AWSCredentialsProvider` for use with the AWS SDK v2 `S3Client`. Crucially, `resolveCredentials()` is called on every request, so long-running Spark jobs pick up rotated credentials without reopening the S3 client.

### `utils/CredentialProviderFactory.scala`
Reflection-based factory for instantiating AWS credential providers without compile-time dependencies on the AWS SDK version. Caches instantiated providers and detects whether a class implements `TableCredentialProvider`. Also defines the simple `BasicAWSCredentials` value class used when the user passes keys explicitly.

### `utils/TableCredentialProvider.scala`
Trait for *table-scoped* credential resolution — catalog systems (Unity Catalog is the prototype) that mint different credentials per table. Also defines `TableInfo`. When a provider implements this trait, the factory passes it the table name so it can fetch per-table temporary credentials.

## Metrics and stats

### `metrics/ReadPipelineMetrics.scala`
Per-split granular timing collected during reads: split engine creation, query build, session start, batch retrieval, batch assembly. Nanosecond precision. `ReadPipelineMetrics` instances are mergeable so the driver can aggregate per-split numbers into per-task and per-query totals.

### `metrics/ReadPipelineSparkUIMetrics.scala`
The Spark UI custom metrics (`CustomSumMetric` / `CustomTaskMetric`) that expose the timing fields above in the SQL tab. One metric per pipeline stage.

### `metrics/DataSkippingSparkUIMetrics.scala`
Spark UI metrics for data skipping: total files considered, partition-pruned files, data-skipped files, per-filter-type breakdowns. Mixes `SumMetric` (aggregated across tasks) and `MaxMetric` (reported once from the driver) depending on the metric's semantics.

### `stats/DataSkippingMetrics.scala`
Process-level counters for data skipping effectiveness. Per-table stats kept in a `ConcurrentHashMap`, plus global counters via `AtomicLong`. Used as the data source for the Spark UI metrics above and for logging.

## Memory

### `memory/NativeMemoryInitializer.scala`
Initializes `tantivy4java`'s native memory pool and links it to Spark's unified memory manager via `TaskContext`. Idempotent: safe to call on every task. Without this, native allocations would be invisible to Spark's memory accounting and could OOM a container.

## Write configuration

### `write/OptimizedWriteConfig.scala`
Target split size, sampling ratio, min rows for history-based estimation, and distribution mode for the optimized-write path. See chapter 03.

### `write/WriteSizeEstimator.scala`
Computes the advisory partition size for Spark AQE during optimized writes. Sampling mode or history mode.

### `write/ArrowFfiWriteConfig.scala`
Arrow FFI write tunables: batch size (rows) and native heap size (MB).

## General utilities (`spark/util/`)

This is the grab-bag. Each file is small and does one thing — listed here so a reader can find the right helper quickly.

- **`CloudPathUtils.scala`** — parses S3 (`s3://`, `s3a://`) and Azure (`wasb`, `abfs`, `azure://`) URIs into bucket/container + key pairs.
- **`ConfigNormalization.scala`** — canonicalizes `spark.indextables.*` config keys (including legacy `tantivy4spark.*` aliases). Used by every reader and writer to look up settings consistently.
- **`ConfigParsingUtils.scala`** — typed parsing (`Int`, `Long`, `Boolean`, size string, …) out of `CaseInsensitiveStringMap`.
- **`ConfigurationResolver.scala`** — multi-source config resolution (Spark session → Hadoop → options → overrides) with well-defined precedence. Defines the `ConfigSource` trait used for custom sources.
- **`ConfigUtils.scala`** — convenience accessors that read `spark.indextables.*` values directly from a `SparkSession`.
- **`CredentialRedaction.scala`** — masks config values whose keys match `secret`, `key`, `token`, `credential`, or `password`. Used by `DESCRIBE INDEXTABLES ENVIRONMENT` and log statements.
- **`ErrorUtil.scala`** — error message construction and exception chaining.
- **`ExpressionUtils.scala`** — helpers for parsing and manipulating Spark Catalyst expressions (used by the `WHERE` clause support in SQL commands).
- **`FilterUtils.scala`** — walks a Spark V1 `Filter` tree to extract referenced field names. Used by filter analysis and pushdown decisions.
- **`IndexingModes.scala`** — enum handling for indexing modes (`FULL`, `NONE`, `NGRAM`, `PHRASE`, …). Mode parsing and validation for `BUILD COMPANION`.
- **`JsonUtil.scala`** — shared JSON serialization/deserialization helpers (backed by Jackson).
- **`PartitionUtils.scala`** — partition column and value handling on the write path.
- **`ProtocolNormalizer.scala`** — normalizes cloud protocol schemes (`s3a` → `s3`, etc.) so downstream code only has to handle one form.
- **`SchemaValidator.scala`** — validates Spark schemas against IndexTables requirements (no unsupported types, no duplicate field names, etc.).
- **`SizeParser.scala`** — parses human-readable sizes like `1G`, `512M`, `100KB`.
- **`SplitMetadataFactory.scala`** — constructs split metadata objects used in transaction log actions.
- **`StatisticsCalculator.scala`** — computes min/max/null/histogram statistics for data skipping during writes.
- **`StatisticsTruncation.scala`** — truncates statistics to keep the transaction log small (up to 98% savings on wide tables with long text fields).
- **`TablePathNormalizer.scala`** — normalizes table paths, resolving catalog names and URIs.
- **`TimestampUtils.scala`** — timestamp parsing and unit conversion (second / millisecond / microsecond / nanosecond precision).
- **`TimingUtils.scala`** — lightweight timing helpers (stopwatch-style).
- **`TypeConversionUtil.scala`** — converts between Java/Scala primitive types and Spark Catalyst types.

## How the pieces connect

Cross-cutting modules don't form a pipeline, but they do follow a few consistent patterns:

```
Every read/write/command:
  ├─ Credentials
  │     ConfigNormalization → CredentialProviderFactory
  │        ├─ UnityCatalogAWSCredentialProvider  (if Unity Catalog)
  │        └─ V1ToV2CredentialsProviderAdapter   (if legacy v1 provider)
  ├─ Configuration
  │     ConfigurationResolver + IndexTables4SparkSQLConf keys
  ├─ Paths and protocols
  │     CloudPathUtils + ProtocolNormalizer
  ├─ Observability
  │     ReadPipelineMetrics → ReadPipelineSparkUIMetrics
  │     DataSkippingMetrics → DataSkippingSparkUIMetrics
  └─ Native memory
        NativeMemoryInitializer (once per task)
```

That completes the walkthrough. The [README](README.md) has the chapter index and a one-paragraph mental model; each chapter above zooms into one slice of the tree. When adding a new module, pick the chapter it belongs in and extend the relevant section — and update the index if you introduce a new top-level package.
