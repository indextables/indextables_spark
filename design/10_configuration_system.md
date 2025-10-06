# Section 10: Configuration System

## 10.1 Overview

IndexTables4Spark implements a comprehensive configuration system with multiple precedence levels, validation mechanisms, and backward compatibility support. The system is designed to support:

- **Hierarchical configuration**: DataFrame options → Spark conf → Hadoop conf → Table properties → Defaults
- **Dual prefix support**: Both `spark.indextables.*` and legacy `spark.indextables.*` prefixes
- **Type-safe access**: Strongly-typed configuration entries with validation
- **Per-operation tuning**: Write/read-specific configuration overrides
- **Dynamic reconfiguration**: Runtime configuration changes without restarts

### 10.1.1 Configuration Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  Configuration Sources                        │
│  (Ordered by precedence, highest to lowest)                  │
├──────────────────────────────────────────────────────────────┤
│  1. DataFrame Options (.option("key", "value"))              │
│     ├─→ Write options: df.write.option(...)                  │
│     └─→ Read options: spark.read.option(...)                 │
├──────────────────────────────────────────────────────────────┤
│  2. Spark Configuration (spark.conf.set(...))                │
│     ├─→ Session-level: SparkSession.conf                     │
│     └─→ spark-defaults.conf file                             │
├──────────────────────────────────────────────────────────────┤
│  3. Hadoop Configuration (hadoopConf.set(...))               │
│     ├─→ core-site.xml                                        │
│     └─→ hdfs-site.xml                                        │
├──────────────────────────────────────────────────────────────┤
│  4. Table Properties (metadata configuration)                │
│     └─→ Stored in transaction log MetadataAction             │
├──────────────────────────────────────────────────────────────┤
│  5. Default Values (hardcoded in ConfigEntry)                │
│     └─→ Fallback when no explicit configuration provided     │
└──────────────────────────────────────────────────────────────┘
```

### 10.1.2 Key Components

| Component | Responsibility | File |
|-----------|---------------|------|
| **ConfigNormalization** | Dual prefix support and normalization | `util/ConfigNormalization.scala` |
| **IndexTables4SparkConfig** | Type-safe configuration entries | `config/IndexTables4SparkConfig.scala` |
| **Configuration merging** | Precedence handling and override logic | Various components |
| **Validation** | Type checking and value range validation | ConfigEntry implementations |

## 10.2 Configuration Hierarchy

### 10.2.1 Precedence Rules

Configuration values are resolved using a strict precedence hierarchy:

```scala
def resolveConfigValue(key: String): Option[String] = {
  // 1. DataFrame options (highest precedence)
  dataframeOptions.get(key)
    .orElse {
      // 2. Spark session configuration
      sparkSession.conf.getOption(key)
    }
    .orElse {
      // 3. Hadoop configuration
      Option(hadoopConf.get(key))
    }
    .orElse {
      // 4. Table properties (from metadata)
      tableMetadata.configuration.get(key)
    }
    .orElse {
      // 5. Default value (lowest precedence)
      configEntry.defaultValue
    }
}
```

**Example: Configuration Override Flow**

```scala
// Scenario: Multiple configuration sources with same key

// 1. Default value (in code)
// Default: spark.indextables.indexWriter.batchSize = 10000

// 2. Table property (persisted in metadata)
CREATE TABLE events ... WITH (
  'spark.indextables.indexWriter.batchSize' = '20000'
)
// Value now: 20000

// 3. Hadoop configuration (in core-site.xml)
<property>
  <name>spark.indextables.indexWriter.batchSize</name>
  <value>30000</value>
</property>
// Value now: 30000

// 4. Spark session configuration
spark.conf.set("spark.indextables.indexWriter.batchSize", "40000")
// Value now: 40000

// 5. DataFrame options (highest precedence)
df.write.format("indextables")
  .option("spark.indextables.indexWriter.batchSize", "50000")
  .save("s3://bucket/path")
// Final value: 50000
```

### 10.2.2 Per-Operation Configuration

DataFrame options provide per-operation configuration overrides:

```scala
// Write-specific configuration
df.write.format("indextables")
  .option("spark.indextables.indexWriter.heapSize", "500000000")  // 500MB
  .option("spark.indextables.indexWriter.batchSize", "20000")
  .option("spark.indextables.s3.maxConcurrency", "8")
  .save("s3://bucket/high-priority-writes")

// Read-specific configuration
val df = spark.read.format("indextables")
  .option("spark.indextables.cache.maxSize", "500000000")  // 500MB cache
  .option("spark.indextables.cache.directoryPath", "/fast-nvme/cache")
  .load("s3://bucket/high-priority-reads")

// Global configuration (applies to all operations)
spark.conf.set("spark.indextables.indexWriter.threads", "4")
```

**Benefits of Per-Operation Configuration**:
- **Workload-specific tuning**: Different batch sizes for bulk vs incremental writes
- **Resource isolation**: High-priority operations get more resources
- **Testing flexibility**: Test different configurations without global changes
- **Multi-tenant support**: Different teams can tune their operations independently

## 10.3 Configuration Key Normalization

### 10.3.1 Dual Prefix Support

IndexTables4Spark supports both legacy and new configuration prefixes for backward compatibility:

| Legacy Prefix | New Prefix | Status |
|--------------|------------|--------|
| `spark.indextables.*` | `spark.indextables.*` | **Recommended** (normalized internally) |
| `spark.indextables.*` | `spark.indextables.*` | Deprecated (supported for compatibility) |

**Normalization Example**:

```scala
// Both of these configurations are equivalent
spark.conf.set("spark.indextables.indexWriter.batchSize", "20000")  // Legacy
spark.conf.set("spark.indextables.indexWriter.batchSize", "20000")  // New

// Internally normalized to:
// "spark.indextables.indexWriter.batchSize" = "20000"
```

### 10.3.2 ConfigNormalization Utility

**File**: `src/main/scala/io/indextables/spark/util/ConfigNormalization.scala`

```scala
object ConfigNormalization {

  /** Normalize legacy prefix to new prefix */
  def normalizeKey(key: String): String = {
    if (key.startsWith("spark.indextables.")) {
      key.replace("spark.indextables.", "spark.indextables.")
    } else {
      key
    }
  }

  /** Check if key is IndexTables4Spark-related (either prefix) */
  def isTantivyKey(key: String): Boolean = {
    key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
  }

  /** Extract and normalize configs from Spark session */
  def extractTantivyConfigsFromSpark(spark: SparkSession): Map[String, String] = {
    spark.conf.getAll
      .filter { case (key, value) => isTantivyKey(key) && value != null }
      .map { case (key, value) => normalizeKey(key) -> value }
      .toMap
  }

  /** Extract and normalize configs from Hadoop configuration */
  def extractTantivyConfigsFromHadoop(hadoopConf: Configuration): Map[String, String] = {
    val configs = mutable.Map[String, String]()
    hadoopConf.iterator().asScala.foreach { entry =>
      if (isTantivyKey(entry.getKey) && entry.getValue != null) {
        configs.put(normalizeKey(entry.getKey), entry.getValue)
      }
    }
    configs.toMap
  }

  /** Merge configs with precedence (later maps override earlier) */
  def mergeWithPrecedence(configMaps: Map[String, String]*): Map[String, String] = {
    val result = mutable.Map[String, String]()
    configMaps.foreach { configMap =>
      val normalized = configMap.map { case (k, v) => normalizeKey(k) -> v }
      result ++= normalized
    }
    result.toMap
  }
}
```

### 10.3.3 Configuration Merging Example

```scala
// Collect configuration from all sources
val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
val optionConfigs = ConfigNormalization.extractTantivyConfigsFromOptions(options)

// Merge with precedence (options > spark > hadoop)
val mergedConfig = ConfigNormalization.mergeWithPrecedence(
  hadoopConfigs,   // Lowest precedence
  sparkConfigs,    // Medium precedence
  optionConfigs    // Highest precedence
)

// Access merged configuration
val batchSize = mergedConfig.getOrElse("spark.indextables.indexWriter.batchSize", "10000").toLong
```

## 10.4 Type-Safe Configuration

### 10.4.1 ConfigEntry System

**File**: `src/main/scala/io/indextables/spark/config/IndexTables4SparkConfig.scala`

IndexTables4Spark defines type-safe configuration entries with validation:

```scala
sealed trait ConfigEntry[T] {
  def key: String
  def defaultValue: T
  def fromString(value: String): T
  def toString(value: T): String
  def fromMetadata(metadata: MetadataAction): Option[T]
}

case class BooleanConfigEntry(
  key: String,
  defaultValue: Boolean
) extends ConfigEntry[Boolean] {
  override def fromString(value: String): Boolean = value.toBoolean
  override def toString(value: Boolean): String = value.toString
}

case class LongConfigEntry(
  key: String,
  defaultValue: Long,
  validator: Long => Boolean = _ => true
) extends ConfigEntry[Long] {
  override def fromString(value: String): Long = {
    val parsed = value.toLong
    require(validator(parsed), s"Invalid value for $key: $value")
    parsed
  }
  override def toString(value: Long): String = value.toString
}

case class StringConfigEntry(
  key: String,
  defaultValue: String,
  validator: String => Boolean = _ => true
) extends ConfigEntry[String] {
  override def fromString(value: String): String = {
    require(validator(value), s"Invalid value for $key: $value")
    value
  }
  override def toString(value: String): String = value
}
```

### 10.4.2 Configuration Definitions

```scala
object IndexTables4SparkConfig {

  ////// Optimized Write Configuration //////

  val OPTIMIZE_WRITE: BooleanConfigEntry = BooleanConfigEntry(
    "tantivy4spark.autoOptimize.optimizeWrite",
    defaultValue = false
  )

  val OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT: LongConfigEntry = LongConfigEntry(
    "tantivy4spark.optimizeWrite.targetRecordsPerSplit",
    defaultValue = 1000000L,
    validator = _ > 0
  )

  ////// General Table Configuration //////

  val BLOOM_FILTERS_ENABLED: BooleanConfigEntry = BooleanConfigEntry(
    "tantivy4spark.bloom.filters.enabled",
    defaultValue = true
  )

  val STORAGE_FORCE_STANDARD: BooleanConfigEntry = BooleanConfigEntry(
    "tantivy4spark.storage.force.standard",
    defaultValue = false
  )

  ////// Access Method with Fallback //////

  def getConfigValue[T](config: ConfigEntry[T], metadata: MetadataAction): T = {
    config.fromMetadata(metadata).getOrElse(config.defaultValue)
  }
}
```

### 10.4.3 Validation

Configuration entries include validators for type and range checking:

```scala
// Positive integer validator
val BATCH_SIZE: LongConfigEntry = LongConfigEntry(
  "spark.indextables.indexWriter.batchSize",
  defaultValue = 10000,
  validator = value => value > 0 && value <= 1000000
)

// Memory size validator
val HEAP_SIZE: LongConfigEntry = LongConfigEntry(
  "spark.indextables.indexWriter.heapSize",
  defaultValue = 100000000,  // 100MB
  validator = value => value >= 1024 * 1024  // Minimum 1MB
)

// Enum-like string validator
val STORAGE_TYPE: StringConfigEntry = StringConfigEntry(
  "spark.indextables.storage.type",
  defaultValue = "auto",
  validator = value => Set("auto", "s3", "local", "hdfs").contains(value)
)

// Usage with validation
try {
  val batchSize = BATCH_SIZE.fromString("20000")  // OK
  val invalid = BATCH_SIZE.fromString("-500")     // Throws IllegalArgumentException
} catch {
  case e: IllegalArgumentException =>
    // Invalid value for spark.indextables.indexWriter.batchSize: -500
    logError(s"Configuration error: ${e.getMessage}")
}
```

## 10.5 Complete Configuration Reference

### 10.5.1 Index Writer Configuration

Controls index creation behavior during write operations:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.indexWriter.heapSize` | Long | 100000000 (100MB) | Heap size for index writer in bytes (supports "100M", "2G") |
| `spark.indextables.indexWriter.batchSize` | Long | 10000 | Number of documents per batch during indexing |
| `spark.indextables.indexWriter.threads` | Int | 2 | Number of indexing threads per executor |
| `spark.indextables.indexWriter.tempDirectoryPath` | String | auto-detect | Custom working directory for index creation (auto-detects `/local_disk0`) |

**Examples**:

```scala
// Large batch processing
df.write.format("indextables")
  .option("spark.indextables.indexWriter.heapSize", "500M")
  .option("spark.indextables.indexWriter.batchSize", "50000")
  .option("spark.indextables.indexWriter.threads", "4")
  .save("s3://bucket/large-dataset")

// Memory filesystem for extreme performance
df.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/dev/shm/tantivy")
  .option("spark.indextables.indexWriter.heapSize", "1G")
  .save("s3://bucket/high-performance")
```

### 10.5.2 Optimized Write Configuration

Controls automatic write optimization and split sizing:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.optimizeWrite.enabled` | Boolean | true | Enable optimized write with automatic partitioning |
| `spark.indextables.optimizeWrite.targetRecordsPerSplit` | Long | 1000000 | Target number of records per split file |
| `spark.indextables.autoSize.enabled` | Boolean | false | Enable auto-sizing based on historical data |
| `spark.indextables.autoSize.targetSplitSize` | String | "100M" | Target size per split (supports "100M", "1G", bytes) |
| `spark.indextables.autoSize.inputRowCount` | Long | (estimated) | Explicit row count for accurate partitioning (V2 API) |

**Examples**:

```scala
// Auto-sizing based on historical analysis
df.write.format("indextables")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "200M")
  .save("s3://bucket/auto-sized-table")

// Manual optimization with large splits
df.write.format("indextables")
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .option("spark.indextables.optimizeWrite.targetRecordsPerSplit", "5000000")
  .save("s3://bucket/large-splits")
```

### 10.5.3 Field Indexing Configuration

Controls how fields are indexed and tokenized:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.indexing.typemap.<field>` | String | "string" | Field type: "string", "text", "json" |
| `spark.indextables.indexing.fastfields` | String | (auto) | Comma-separated list of fast fields |
| `spark.indextables.indexing.storeonlyfields` | String | empty | Fields stored but not indexed |
| `spark.indextables.indexing.indexonlyfields` | String | empty | Fields indexed but not stored |
| `spark.indextables.indexing.tokenizer.<field>` | String | "default" | Tokenizer: "default", "whitespace", "raw" |

**Examples**:

```scala
// Mixed field types with fast fields
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.title", "string")      // Exact matching
  .option("spark.indextables.indexing.typemap.content", "text")      // Full-text search
  .option("spark.indextables.indexing.typemap.metadata", "json")     // JSON indexing
  .option("spark.indextables.indexing.fastfields", "score,timestamp") // Aggregation support
  .option("spark.indextables.indexing.storeonlyfields", "raw_data")   // Store only
  .save("s3://bucket/mixed-types")

// Custom tokenizers
df.write.format("indextables")
  .option("spark.indextables.indexing.tokenizer.title", "raw")        // No tokenization
  .option("spark.indextables.indexing.tokenizer.content", "default")  // Standard tokenizer
  .save("s3://bucket/custom-tokenization")
```

### 10.5.4 Cache Configuration

Controls split caching behavior:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.cache.maxSize` | Long | 200000000 (200MB) | Maximum cache size in bytes |
| `spark.indextables.cache.directoryPath` | String | auto-detect | Custom cache directory (auto-detects `/local_disk0`) |
| `spark.indextables.cache.prewarm.enabled` | Boolean | false | Enable proactive cache warming |
| `spark.indextables.docBatch.enabled` | Boolean | true | Enable batch document retrieval |
| `spark.indextables.docBatch.maxSize` | Int | 1000 | Maximum documents per batch |

**Examples**:

```scala
// Large cache for query-heavy workloads
spark.conf.set("spark.indextables.cache.maxSize", "1000000000")  // 1GB

// High-performance cache directory
spark.conf.set("spark.indextables.cache.directoryPath", "/fast-nvme/cache")

// Prewarm cache for repeated queries
val df = spark.read.format("indextables")
  .option("spark.indextables.cache.prewarm.enabled", "true")
  .load("s3://bucket/frequently-accessed")
```

### 10.5.5 S3 Upload Configuration

Controls S3 upload performance:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.s3.streamingThreshold` | Long | 104857600 (100MB) | Files larger than this use streaming upload |
| `spark.indextables.s3.multipartThreshold` | Long | 104857600 (100MB) | Threshold for multipart upload |
| `spark.indextables.s3.maxConcurrency` | Int | 4 | Number of parallel upload threads |
| `spark.indextables.s3.partSize` | Long | 67108864 (64MB) | Size of each multipart upload part |

**Examples**:

```scala
// High-throughput uploads for large files
df.write.format("indextables")
  .option("spark.indextables.s3.maxConcurrency", "12")
  .option("spark.indextables.s3.partSize", "268435456")  // 256MB parts
  .option("spark.indextables.s3.multipartThreshold", "52428800")  // 50MB
  .save("s3://bucket/high-throughput")

// Conservative settings for network-constrained environments
df.write.format("indextables")
  .option("spark.indextables.s3.maxConcurrency", "2")
  .option("spark.indextables.s3.partSize", "33554432")  // 32MB parts
  .save("s3://bucket/slow-network")
```

### 10.5.6 Transaction Log Configuration

Controls transaction log performance and compaction:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.checkpoint.enabled` | Boolean | true | Enable automatic checkpoint creation |
| `spark.indextables.checkpoint.interval` | Int | 10 | Create checkpoint every N transactions |
| `spark.indextables.checkpoint.parallelism` | Int | 4 | Thread pool size for parallel I/O |
| `spark.indextables.checkpoint.read.timeoutSeconds` | Int | 30 | Timeout for parallel read operations |
| `spark.indextables.logRetention.duration` | Long | 2592000000 (30 days) | Log retention in milliseconds |
| `spark.indextables.checkpointRetention.duration` | Long | 7200000 (2 hours) | Checkpoint retention in milliseconds |
| `spark.indextables.cleanup.enabled` | Boolean | true | Enable automatic cleanup of old files |
| `spark.indextables.transaction.cache.enabled` | Boolean | true | Enable transaction log caching |
| `spark.indextables.transaction.cache.expirationSeconds` | Int | 300 (5 min) | Cache TTL in seconds |

**Examples**:

```scala
// High-frequency writes with aggressive checkpointing
df.write.format("indextables")
  .option("spark.indextables.checkpoint.enabled", "true")
  .option("spark.indextables.checkpoint.interval", "5")       // Every 5 transactions
  .option("spark.indextables.checkpoint.parallelism", "8")    // 8 parallel threads
  .save("s3://bucket/high-frequency-writes")

// Long retention for audit requirements
spark.conf.set("spark.indextables.logRetention.duration", "7776000000")  // 90 days
```

### 10.5.7 Merge Splits Configuration

Controls split consolidation behavior:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.merge.heapSize` | Long | 1073741824 (1GB) | Heap size for merge operations (supports "2G", "500M") |
| `spark.indextables.merge.debug` | Boolean | false | Enable debug logging for merges |
| `spark.indextables.merge.batchSize` | Int | (defaultParallelism) | Number of merge groups per batch |
| `spark.indextables.merge.maxConcurrentBatches` | Int | 2 | Maximum concurrent batch processing |
| `spark.indextables.merge.tempDirectoryPath` | String | auto-detect | Custom temp directory for merges |

**Examples**:

```scala
// Large merge operations
spark.sql("""
  MERGE SPLITS 's3://bucket/large-table'
  TARGET SIZE 5G
""")

// Set large heap for merge via configuration
spark.conf.set("spark.indextables.merge.heapSize", "2147483648")  // 2GB
```

### 10.5.8 Skipped Files Configuration

Controls handling of problematic files during merge:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.skippedFiles.trackingEnabled` | Boolean | true | Enable skipped files tracking |
| `spark.indextables.skippedFiles.cooldownDuration` | Int | 24 (hours) | Cooldown before retrying failed files |

**Examples**:

```scala
// Production with long cooldown
spark.conf.set("spark.indextables.skippedFiles.trackingEnabled", "true")
spark.conf.set("spark.indextables.skippedFiles.cooldownDuration", "48")  // 48 hours

// Development with short cooldown for testing
spark.conf.set("spark.indextables.skippedFiles.cooldownDuration", "1")  // 1 hour
```

### 10.5.9 AWS Credential Configuration

Controls AWS credential resolution:

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `spark.indextables.aws.accessKey` | String | (required for S3) | AWS access key ID |
| `spark.indextables.aws.secretKey` | String | (required for S3) | AWS secret access key |
| `spark.indextables.aws.sessionToken` | String | (optional) | AWS session token for temporary credentials |
| `spark.indextables.aws.credentialsProviderClass` | String | (optional) | Custom AWS credential provider class (v1.9+) |

**Examples**:

```scala
// Explicit credentials
df.write.format("indextables")
  .option("spark.indextables.aws.accessKey", "AKIA...")
  .option("spark.indextables.aws.secretKey", "secret...")
  .save("s3://bucket/path")

// Session token support
df.write.format("indextables")
  .option("spark.indextables.aws.accessKey", "ASIA...")
  .option("spark.indextables.aws.secretKey", "secret...")
  .option("spark.indextables.aws.sessionToken", "token...")
  .save("s3://bucket/path")

// Custom credential provider (v1.9+)
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
  "com.example.CustomCredentialProvider")
```

## 10.6 Configuration Patterns

### 10.6.1 Environment-Specific Configuration

**Development Environment**:
```scala
// spark-defaults.conf for development
spark.indextables.indexWriter.heapSize=50000000           // 50MB (small)
spark.indextables.indexWriter.batchSize=5000               // Small batches
spark.indextables.cache.maxSize=50000000                  // 50MB cache
spark.indextables.checkpoint.interval=5                    // Frequent checkpoints
spark.indextables.logRetention.duration=3600000           // 1 hour retention
```

**Production Environment**:
```scala
// spark-defaults.conf for production
spark.indextables.indexWriter.heapSize=500000000          // 500MB
spark.indextables.indexWriter.batchSize=50000              // Large batches
spark.indextables.indexWriter.threads=4                    // More threads
spark.indextables.cache.maxSize=1000000000                // 1GB cache
spark.indextables.checkpoint.enabled=true
spark.indextables.checkpoint.interval=20                   // Less frequent
spark.indextables.checkpoint.parallelism=8                 // More parallelism
spark.indextables.logRetention.duration=2592000000        // 30 days
```

**Databricks Environment**:
```scala
// Automatic /local_disk0 detection - minimal configuration needed
spark.indextables.cache.maxSize=2000000000                // 2GB cache
spark.indextables.s3.maxConcurrency=8                      // High concurrency
```

### 10.6.2 Workload-Specific Configuration

**Bulk Data Loading**:
```scala
df.write.format("indextables")
  .option("spark.indextables.indexWriter.heapSize", "1G")
  .option("spark.indextables.indexWriter.batchSize", "100000")
  .option("spark.indextables.indexWriter.threads", "8")
  .option("spark.indextables.s3.maxConcurrency", "16")
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .option("spark.indextables.optimizeWrite.targetRecordsPerSplit", "5000000")
  .save("s3://bucket/bulk-load")
```

**Incremental Updates**:
```scala
df.write.format("indextables")
  .mode("append")
  .option("spark.indextables.indexWriter.batchSize", "10000")
  .option("spark.indextables.checkpoint.interval", "5")
  .save("s3://bucket/incremental")
```

**Query-Heavy Workloads**:
```scala
val df = spark.read.format("indextables")
  .option("spark.indextables.cache.maxSize", "5000000000")  // 5GB cache
  .option("spark.indextables.cache.prewarm.enabled", "true")
  .option("spark.indextables.docBatch.enabled", "true")
  .option("spark.indextables.docBatch.maxSize", "5000")
  .load("s3://bucket/query-heavy")
```

### 10.6.3 Testing and Debugging Configuration

```scala
// Enable debug logging and validation
spark.conf.set("spark.indextables.merge.debug", "true")
spark.conf.set("spark.indextables.checkpoint.checksumValidation.enabled", "true")
spark.conf.set("spark.indextables.cleanup.dryRun", "true")  // Log without deleting

// Small cache for testing eviction behavior
spark.conf.set("spark.indextables.cache.maxSize", "10000000")  // 10MB

// Frequent checkpoints for testing transaction log
spark.conf.set("spark.indextables.checkpoint.interval", "2")
```

## 10.7 Configuration Validation

### 10.7.1 Validation Mechanisms

IndexTables4Spark validates configuration values at multiple levels:

**1. Type Validation**:
```scala
// Automatic type checking
val heapSize = config.get("spark.indextables.indexWriter.heapSize").toLong
// Throws NumberFormatException if not a valid long

val enabled = config.get("spark.indextables.cache.prewarm.enabled").toBoolean
// Throws IllegalArgumentException if not "true"/"false"
```

**2. Range Validation**:
```scala
val BATCH_SIZE: LongConfigEntry = LongConfigEntry(
  "spark.indextables.indexWriter.batchSize",
  defaultValue = 10000,
  validator = value => {
    value > 0 && value <= 1000000
  }
)

// Usage
try {
  BATCH_SIZE.fromString("2000000")  // Exceeds maximum
} catch {
  case e: IllegalArgumentException =>
    // Invalid value for spark.indextables.indexWriter.batchSize: 2000000
}
```

**3. Semantic Validation**:
```scala
// Check directory exists and is writable
val tempDir = config.get("spark.indextables.indexWriter.tempDirectoryPath")
val dir = new File(tempDir)
require(dir.exists(), s"Directory does not exist: $tempDir")
require(dir.canWrite(), s"Directory is not writable: $tempDir")

// Validate size format
val sizeStr = config.get("spark.indextables.autoSize.targetSplitSize")
require(
  sizeStr.matches("\\d+[MmGgKk]?"),
  s"Invalid size format: $sizeStr"
)
```

### 10.7.2 Configuration Errors

Common configuration errors and their resolutions:

| Error | Cause | Resolution |
|-------|-------|-----------|
| `NumberFormatException: For input string "abc"` | Non-numeric value for numeric config | Provide valid numeric value |
| `Invalid value for spark.indextables.indexWriter.batchSize: -100` | Negative value | Use positive value |
| `Target size must be at least 1MB` | Size too small | Increase to minimum 1MB |
| `Directory does not exist: /invalid/path` | Invalid temp directory | Provide valid existing directory |
| `AWS credentials required for S3 storage` | Missing S3 credentials | Provide access key/secret key |

**Example Error Handling**:

```scala
try {
  df.write.format("indextables")
    .option("spark.indextables.indexWriter.batchSize", "-500")  // Invalid
    .save("s3://bucket/path")
} catch {
  case e: IllegalArgumentException =>
    println(s"Configuration error: ${e.getMessage}")
    // Configuration error: Invalid value for spark.indextables.indexWriter.batchSize: -500
}
```

## 10.8 Dynamic Configuration

### 10.8.1 Runtime Configuration Changes

Spark session configuration can be changed at runtime without restart:

```scala
// Initial configuration
spark.conf.set("spark.indextables.cache.maxSize", "200000000")  // 200MB

// ... run some queries ...

// Increase cache size at runtime
spark.conf.set("spark.indextables.cache.maxSize", "1000000000")  // 1GB

// Next read operation will use new cache size
val df = spark.read.format("indextables").load("s3://bucket/path")
```

**Note**: Some configurations are read during specific lifecycle events:

| Configuration | Read At | Change Takes Effect |
|--------------|---------|---------------------|
| Cache size | Scan planning | Next read operation |
| Batch size | Write initialization | Next write operation |
| Heap size | Index writer creation | Next write operation |
| Checkpoint interval | Transaction commit | Next commit |
| S3 concurrency | Upload initialization | Next upload |

### 10.8.2 Table-Level Configuration Persistence

Table properties are persisted in the transaction log and cannot be changed at runtime:

```scala
// Table properties set during CREATE TABLE
CREATE TABLE events (
  id STRING,
  timestamp LONG,
  message STRING
) USING indextables
OPTIONS (
  'path' 's3://bucket/events',
  'spark.indextables.optimizeWrite.enabled' 'true',
  'spark.indextables.optimizeWrite.targetRecordsPerSplit' '2000000'
)

// These properties are stored in MetadataAction and apply to all operations
```

To change table properties, you must update the table metadata:

```sql
-- Not supported directly - would require ALTER TABLE implementation
-- Workaround: Use DataFrame options to override
df.write.format("indextables")
  .mode("append")
  .option("spark.indextables.optimizeWrite.targetRecordsPerSplit", "3000000")
  .save("s3://bucket/events")
```

## 10.9 Configuration Best Practices

### 10.9.1 Recommendations by Use Case

**Small to Medium Tables (< 1TB)**:
```scala
spark.conf.set("spark.indextables.indexWriter.heapSize", "200M")
spark.conf.set("spark.indextables.indexWriter.batchSize", "20000")
spark.conf.set("spark.indextables.cache.maxSize", "500M")
spark.conf.set("spark.indextables.optimizeWrite.targetRecordsPerSplit", "1000000")
```

**Large Tables (1TB - 10TB)**:
```scala
spark.conf.set("spark.indextables.indexWriter.heapSize", "500M")
spark.conf.set("spark.indextables.indexWriter.batchSize", "50000")
spark.conf.set("spark.indextables.indexWriter.threads", "4")
spark.conf.set("spark.indextables.cache.maxSize", "2G")
spark.conf.set("spark.indextables.s3.maxConcurrency", "8")
spark.conf.set("spark.indextables.optimizeWrite.targetRecordsPerSplit", "5000000")
```

**Very Large Tables (> 10TB)**:
```scala
spark.conf.set("spark.indextables.indexWriter.heapSize", "1G")
spark.conf.set("spark.indextables.indexWriter.batchSize", "100000")
spark.conf.set("spark.indextables.indexWriter.threads", "8")
spark.conf.set("spark.indextables.cache.maxSize", "5G")
spark.conf.set("spark.indextables.s3.maxConcurrency", "16")
spark.conf.set("spark.indextables.optimizeWrite.targetRecordsPerSplit", "10000000")
spark.conf.set("spark.indextables.checkpoint.parallelism", "12")
```

### 10.9.2 Performance Tuning Guidelines

**Memory Constraints**:
- Start with default cache size (200MB)
- Monitor eviction rate
- Increase cache size if eviction rate > 50%
- Maximum: 20% of executor memory

**CPU Constraints**:
- Start with 2 indexing threads
- Increase to number of available cores
- Monitor CPU utilization
- Diminishing returns beyond 8 threads

**Network Constraints**:
- Start with maxConcurrency=4
- Increase if network bandwidth underutilized
- Monitor upload/download throughput
- Decrease if seeing network timeouts

**Storage I/O Constraints**:
- Use /local_disk0 on Databricks/EMR
- Use NVMe SSD for temp directories
- Consider tmpfs for extreme performance
- Monitor I/O wait times

### 10.9.3 Common Pitfalls

**Anti-Pattern 1: Overly Large Batch Sizes**
```scala
// BAD: May cause OOM errors
.option("spark.indextables.indexWriter.batchSize", "1000000")

// GOOD: Reasonable batch size
.option("spark.indextables.indexWriter.batchSize", "50000")
```

**Anti-Pattern 2: Insufficient Cache**
```scala
// BAD: Cache thrashing with 10MB cache for 10GB working set
.option("spark.indextables.cache.maxSize", "10000000")

// GOOD: 10% of working set
.option("spark.indextables.cache.maxSize", "1000000000")  // 1GB
```

**Anti-Pattern 3: Network Disk for Temp Directory**
```scala
// BAD: Slow network-mounted storage
.option("spark.indextables.indexWriter.tempDirectoryPath", "/mnt/nfs/temp")

// GOOD: Local high-performance storage
.option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
```

## 10.10 Summary

IndexTables4Spark's configuration system provides:

✅ **Comprehensive hierarchy**: DataFrame options > Spark conf > Hadoop conf > Table properties > Defaults
✅ **Dual prefix support**: Both `spark.indextables.*` and legacy prefixes with automatic normalization
✅ **Type-safe access**: Strongly-typed ConfigEntry system with validation
✅ **Per-operation tuning**: Write/read-specific configuration overrides
✅ **Automatic optimization**: /local_disk0 detection, auto-fast-fields, intelligent defaults
✅ **Validation mechanisms**: Type checking, range validation, semantic validation
✅ **Dynamic reconfiguration**: Runtime changes without restarts
✅ **Production-ready**: 50+ configuration parameters covering all major features

**Key Capabilities**:
- **Hierarchical precedence** ensures predictable configuration resolution
- **ConfigNormalization** utility handles dual prefix support transparently
- **Type-safe ConfigEntry** system prevents invalid configurations
- **Per-operation overrides** enable fine-grained tuning without global changes
- **Comprehensive reference** with 50+ parameters for all components
- **Environment-specific patterns** for development, production, and cloud deployments

**Next Section**: Sections 11-16 will provide high-level outlines covering Performance Optimizations, Error Handling, Integration Points, Testing, Usage Examples, and Appendices.
