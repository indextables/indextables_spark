# 04 — Transaction Log

The transaction log is the source of truth for every table's state: what split files exist, what schema they carry, what partitioning is in effect, which versions have been checkpointed, and what protocol features are required to read them. It is a Delta-Lake-compatible format stored under `_transaction_log/` with optional GZIP compression, Avro state files, and checkpoints. All of this lives in `spark/transaction/`.

## Core interfaces and factory

### `transaction/TransactionLogInterface.scala`
The trait every transaction-log implementation conforms to. Defines the API for initialization, reads (list files, get metadata, get protocol, get schema, get table config), writes (append `Action`s, commit versions, write checkpoints), and lifecycle (close, invalidate). Having a single trait lets the rest of the codebase depend on an interface rather than a concrete implementation.

### `transaction/NativeTransactionLog.scala`
The production implementation. It is backed by the Rust transaction-log reader/writer in `tantivy4java` via JNI. Features:

- Optimistic concurrency with retries on commit conflict.
- Avro state format by default (V4 protocol) — ~10x faster than JSON-lines checkpoint reads.
- Automatic checkpoint creation on configurable thresholds.
- Internal LRU cache with TTL so repeated reads of the same version are free.
- Incremental state writes to avoid rewriting the full state on every commit.

### `transaction/TransactionLogFactory.scala`
The single entry point for constructing a `TransactionLogInterface`. Consumers never instantiate a log directly — they call the factory. It is responsible for:

- Merging credentials across Hadoop conf, Spark session conf, and user options via `transaction/ConfigMapper` and `util/ConfigNormalization`.
- Enforcing the `allowDirectUsage` safety check that keeps stray code from instantiating a transaction log outside the managed DataSource lifecycle.
- Returning shared, cached log instances when appropriate.

## Actions and serialization

### `transaction/Actions.scala`
The Delta-style action hierarchy: `AddAction`, `RemoveAction`, `SkipAction`, `ProtocolAction`, `MetadataAction`, plus supporting case classes. Also defines `DocMappingMetadata`, a cached representation of the Tantivy schema inside the log so it doesn't have to be re-parsed on every read.

### `transaction/ActionJsonSerializer.scala`
Serializes `Action` objects to the JSON formats consumed by the native `tantivy4java` writer — both JSON-lines and JSON-array shapes, used in different call sites.

### `transaction/ActionsToArrowConverter.scala`
Alternative serialization: packs actions into an Arrow `RecordBatch` with an `action_type` discriminator column. This is used by the Arrow-FFI commit path, which can append a new version by passing a single batch across FFI instead of a JSON blob per action. Uses `arrow/ArrowFfiBridge`.

### `transaction/ArrowFileEntryExtractor.scala`
The inverse of the converter above. When the native `listFiles` call returns an Arrow `ColumnarBatch` of file entries, this extractor walks the Arrow vectors and rebuilds `AddAction` objects without going through JSON. It is the reason list operations on large tables are cheap.

### `transaction/NativeListFilesResult.scala`
Value object returned from the native list call. Holds the file list plus associated metadata (schema, partition schema, protocol, stats) and `NativeFilteringMetrics` (how many files were pruned at which layer). The metrics feed into `stats/DataSkippingMetrics` and the Spark UI.

## Caches

### `transaction/EnhancedTransactionLogCache.scala`
Global caches for *immutable* metadata that can be safely shared across `TransactionLog` instances — primarily `DocMappingMetadata` and filtered schemas. Also exposes a JSON parse counter used by tests to assert that repeated opens do not re-parse the log. This is separate from the per-instance LRU inside `NativeTransactionLog`.

## Partition and filter support

### `transaction/PartitionPredicateUtils.scala`
Parses, validates, and evaluates partition predicates. Used by both `IndexTables4SparkScan` (for partition pruning at scan time) and the `MERGE SPLITS` / `DROP PARTITIONS` commands (which need to identify which `AddAction`s a `WHERE` clause targets).

### `transaction/PartitionUtils.scala`
Runtime helpers for partition extraction during writes: precomputes a `PartitionColumnInfo` once per schema so every row costs O(1) to extract partition values.

### `transaction/SparkFilterToNativeFilter.scala`
Translates Spark V2 `Filter`s into the `PartitionFilter` JSON format the native reader understands. Used to push partition pruning all the way into the native list call, so the JVM never sees the pruned files.

## Protocol versioning and configuration

### `transaction/ProtocolVersion.scala`
Constants for reader/writer protocol versions V1 through V4 and the feature flags each version introduced (`skippedFiles`, `extendedMetadata`, `footerOffsets`, `multiPartCheckpoint`, `schemaDeduplication`, `avroState`). Used when writing a new `ProtocolAction` and when a reader must decide whether it can open a given table.

### `transaction/ConfigMapper.scala`
Centralizes the translation between the `spark.indextables.*` configuration keys understood by the connector and the flat key/value pairs the native layer expects. Every reader that needs credentials goes through this so the mapping is not duplicated.

## How the pieces connect

```
Read path (chapter 02)                 Write path (chapter 03)
        │                                      │
        ▼                                      ▼
  TransactionLogFactory.getOrCreate(path, options, hadoopConf)
        │
        ▼
  TransactionLogInterface
        │
        └── NativeTransactionLog  ──── JNI ────▶  tantivy4java (Rust)
                 │                                       │
                 ├─ list()   ── ArrowFileEntryExtractor ──┘
                 ├─ commit() ── ActionJsonSerializer
                 │           or ActionsToArrowConverter
                 ├─ uses ConfigMapper for credentials
                 └─ shares DocMappingMetadata via
                    EnhancedTransactionLogCache
```

With the transaction log in place, the next chapter covers the search engine that actually queries the split files the log points to.
