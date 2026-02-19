# Configuration Reference

Complete configuration settings for IndexTables4Spark.

## Index Writer

```scala
spark.indextables.indexWriter.heapSize: "100M" (supports "2G", "500M", "1024K")
spark.indextables.indexWriter.batchSize: 10000
spark.indextables.indexWriter.maxBatchBufferSize: "90M" (default: 90MB, prevents native 100MB limit errors)
spark.indextables.indexWriter.threads: 2
```

## Split Conversion

```scala
// Controls parallelism of tantivy index -> quickwit split conversion
spark.indextables.splitConversion.maxParallelism: <auto> (default: max(1, availableProcessors))
```

## Transaction Log

```scala
spark.indextables.checkpoint.enabled: true
spark.indextables.checkpoint.interval: 10
spark.indextables.transaction.compression.enabled: true (default)
```

### Concurrent Write Retry

Automatic retry on version conflicts:

```scala
spark.indextables.transaction.retry.maxAttempts: 10 (default: 10)
spark.indextables.transaction.retry.baseDelayMs: 100 (default: 100ms, initial backoff delay)
spark.indextables.transaction.retry.maxDelayMs: 5000 (default: 5000ms, maximum backoff cap)
```

## Avro State Format

10x faster checkpoint reads, default since Protocol V4. Replaces JSON checkpoints with binary Avro manifests.

```scala
spark.indextables.state.format: "avro" (default: "avro", options: "avro", "json")
spark.indextables.state.compression: "zstd" (default: "zstd", options: "zstd", "snappy", "none")
spark.indextables.state.compressionLevel: 3 (default: 3, range 1-22 for zstd)
spark.indextables.state.entriesPerManifest: 50000 (default: 50000)
spark.indextables.state.read.parallelism: 8 (default: 8)
```

### State Compaction

Automatic rewrite to remove tombstones and optimize layout:

```scala
spark.indextables.state.compaction.tombstoneThreshold: 0.10 (default: 10%)
spark.indextables.state.compaction.maxManifests: 20 (default: 20)
spark.indextables.state.compaction.afterMerge: true (default: true)
```

### State Retention

```scala
spark.indextables.state.retention.versions: 2 (default: 2, keep N old state versions)
spark.indextables.state.retention.hours: 168 (default: 168 = 7 days)
```

## Statistics Truncation

Enabled by default to prevent transaction log bloat:

```scala
spark.indextables.stats.truncation.enabled: true
spark.indextables.stats.truncation.maxLength: 32
```

## Token Length Limits

Tokens longer than the limit are filtered out (not truncated):

```scala
spark.indextables.indexing.text.maxTokenLength: 255 (default: 255, Quickwit-compatible)
// Named constants: "tantivy_max" (65530), "default" (255), "legacy" (40), "min" (1)
// Per-field overrides: spark.indextables.indexing.tokenLength.<field>: <value>
// List-based syntax: spark.indextables.indexing.tokenLength.<value>: "field1,field2,..."
```

## Data Skipping Statistics

Delta Lake compatible:

```scala
spark.indextables.dataSkippingStatsColumns: <column_list> (comma-separated, takes precedence over numIndexedCols)
spark.indextables.dataSkippingNumIndexedCols: 32 (default: 32, -1 for all eligible columns, 0 to disable)
```

## Optimized Writes

Iceberg-style shuffle before writing. Produces well-sized splits (~1GB) via AQE advisory partition sizes.

```scala
spark.indextables.write.optimizeWrite.enabled: false (default: false)
spark.indextables.write.optimizeWrite.targetSplitSize: "1G" (default: 1GB)
spark.indextables.write.optimizeWrite.samplingRatio: 1.1 (default: 1.1)
spark.indextables.write.optimizeWrite.minRowsForEstimation: 10000 (default: 10000)
spark.indextables.write.optimizeWrite.distributionMode: "hash" (default: "hash", options: "hash", "none")
```

## Merge-On-Write

Automatic split consolidation during writes:

```scala
spark.indextables.mergeOnWrite.enabled: false (default: false)
spark.indextables.mergeOnWrite.targetSize: "4G" (default: 4G)
```

### Async Merge-On-Write

Runs merges in background thread, allows indexing to continue:

```scala
spark.indextables.mergeOnWrite.async.enabled: true (default: true)
spark.indextables.mergeOnWrite.batchCpuFraction: 0.167 (default: 1/6, fraction of cluster CPUs per batch)
spark.indextables.mergeOnWrite.maxConcurrentBatches: 3 (default: 3)
spark.indextables.mergeOnWrite.minBatchesToTrigger: 1 (default: 1)
spark.indextables.mergeOnWrite.shutdownTimeoutMs: 300000 (default: 5 minutes)

// Threshold formula: threshold = batchSize x minBatchesToTrigger
// Batch size formula: batchSize = max(1, totalClusterCpus x batchCpuFraction)
// Example: 24 CPUs with defaults = batchSize 4, threshold 4 groups to trigger merge
```

### Legacy Merge-On-Write Settings (deprecated)

```scala
spark.indextables.mergeOnWrite.mergeGroupMultiplier: 2.0 (deprecated, use batchCpuFraction + minBatchesToTrigger)
spark.indextables.mergeOnWrite.minDiskSpaceGB: 20 (default: 20GB, use 1GB for tests)
spark.indextables.mergeOnWrite.maxConcurrentMergesPerWorker: <auto> (default: auto-calculated based on heap size)
spark.indextables.mergeOnWrite.memoryOverheadFactor: 3.0 (default: 3.0)
```

## Merge I/O

Downloads source splits to local disk before merge, uploads merged split after:

```scala
spark.indextables.merge.download.maxConcurrencyPerCore: 8 (default: 8)
spark.indextables.merge.download.memoryBudget: "2G" (default: 2GB per executor)
spark.indextables.merge.download.retries: 3 (default: 3 with exponential backoff)
spark.indextables.merge.upload.maxConcurrency: 6 (default: 6)
```

## Purge-On-Write

Automatic cleanup of orphaned files and old transaction logs:

```scala
spark.indextables.purgeOnWrite.enabled: false (default: false)
spark.indextables.purgeOnWrite.triggerAfterMerge: true (default: true)
spark.indextables.purgeOnWrite.triggerAfterWrites: 0 (default: 0 = disabled)
spark.indextables.purgeOnWrite.splitRetentionHours: 168 (default: 168 = 7 days)
spark.indextables.purgeOnWrite.txLogRetentionHours: 720 (default: 720 = 30 days)
```

## Batch Retrieval Optimization

Reduces S3 requests by 90-95% for read operations:

```scala
spark.indextables.read.batchOptimization.enabled: true (default: true)
spark.indextables.read.batchOptimization.profile: "balanced" (options: conservative, balanced, aggressive, disabled)
spark.indextables.read.batchOptimization.maxRangeSize: "16M" (default: 16MB, range: 2MB-32MB)
spark.indextables.read.batchOptimization.gapTolerance: "512K" (default: 512KB, range: 64KB-2MB)
spark.indextables.read.batchOptimization.minDocsForOptimization: 50 (default: 50, range: 10-200)
spark.indextables.read.batchOptimization.maxConcurrentPrefetch: 8 (default: 8, range: 2-32)
```

### Adaptive Tuning

```scala
spark.indextables.read.adaptiveTuning.enabled: true (default: true)
spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment: 5 (default: 5)
```

## L2 Disk Cache

Persistent NVMe caching across JVM restarts. Auto-enabled when `/local_disk0` detected (Databricks/EMR):

```scala
spark.indextables.cache.disk.enabled: <auto> (default: auto-enabled when /local_disk0 detected)
spark.indextables.cache.disk.path: <auto> (default: "/local_disk0/tantivy4spark_slicecache")
spark.indextables.cache.disk.maxSize: "100G" (default: 0 = auto, 2/3 available disk)
spark.indextables.cache.disk.compression: "lz4" (options: lz4, zstd, none)
spark.indextables.cache.disk.minCompressSize: "4K" (default: 4096 bytes)
spark.indextables.cache.disk.manifestSyncInterval: 30 (default: 30 seconds)
```

## Read Limits

```scala
spark.indextables.read.defaultLimit: 250 (default: 250, max docs per partition when no LIMIT pushed down)
```

## Splits Per Task

Auto-selection: 1 split/task for small tables, batched for larger tables:

```scala
spark.indextables.read.splitsPerTask: "auto" (default: auto, or numeric value)
spark.indextables.read.maxSplitsPerTask: 8 (default: 8)
spark.indextables.read.aggregate.splitsPerTask: (falls back to read.splitsPerTask)
spark.indextables.read.aggregate.maxSplitsPerTask: (falls back to read.maxSplitsPerTask)
```

## Partition Pruning Optimization

Reduces complexity from O(n*f) to O(p*f):

```scala
spark.indextables.partitionPruning.filterCacheEnabled: true (default: true)
spark.indextables.partitionPruning.indexEnabled: true (default: true)
spark.indextables.partitionPruning.parallelThreshold: 100 (default: 100)
spark.indextables.partitionPruning.selectivityOrdering: true (default: true)
```

## String Pattern Filter Pushdown

All disabled by default. Enable to allow aggregate pushdown with pattern filters. Note: These match individual tokens, not full strings for text fields.

```scala
spark.indextables.filter.stringPattern.pushdown: false (master switch)
spark.indextables.filter.stringStartsWith.pushdown: false (efficient - uses sorted index terms)
spark.indextables.filter.stringEndsWith.pushdown: false (less efficient - requires term scanning)
spark.indextables.filter.stringContains.pushdown: false (least efficient)
```

## Working Directories

Auto-detects `/local_disk0` when available:

```scala
spark.indextables.indexWriter.tempDirectoryPath: "/local_disk0/temp" (or auto-detect)
spark.indextables.cache.directoryPath: "/local_disk0/cache" (or auto-detect)
spark.indextables.merge.tempDirectoryPath: "/local_disk0/merge-temp" (or auto-detect)
```

## Purge Configuration

```scala
spark.indextables.purge.defaultRetentionHours: 168 (7 days for splits)
spark.indextables.purge.minRetentionHours: 24 (safety check)
spark.indextables.purge.retentionCheckEnabled: true
spark.indextables.purge.parallelism: <auto>
spark.indextables.purge.maxFilesToDelete: 1000000
spark.indextables.purge.deleteRetries: 3
```
