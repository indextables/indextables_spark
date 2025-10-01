# IndexTables for Spark

IndexTables is an experimental open-table format that provides a transactional layer on top of indexed storage. It enables fast search, retrieval, and aggregation across terabytes of data, often with sub-second performance. Originally designed for interactive log observability and cybersecurity investigations, IndexTables is versatile enough to support many other use cases requiring extremely fast retrieval.

On Spark—the only supported platform today (with potential future support for Presto, Trino, and others)—IndexTables requires no additional components beyond a Spark cluster and object storage (currently tested on AWS S3). It has been verified on OSS Spark 3.5.2 and Databricks 15.4 LTS. We welcome community feedback on other working distributions and are happy to collaborate on resolving any issues that prevent broader adoption.

IndexTables leverages [Tantivy](https://github.com/quickwit-oss/tantivy) and [Quickwit splits](https://github.com/quickwit-oss/quickwit) instead of Parquet as its underlying storage format. This hybrid of row and columnar storage, combined with powerful indexing technology, enables extremely fast keyword searches across massive datasets.

To contact the original author and maintainer of this repository, [Scott Schenkein](https://www.linkedin.com/in/schenksj/), please open a GitHub issue or connect on LinkedIn.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [OSS Spark](#oss-spark)
  - [Databricks](#databricks)
  - [Usage](#usage)
    - [Field Indexing Configuration](#field-indexing-configuration-write-options)
  - [Configuration Options](#configuration-options-read-options-andor-spark-properties)
    - [Auto-Sizing Configuration](#auto-sizing-configuration)
    - [S3 Upload Configuration](#s3-upload-configuration)
    - [Transaction Log Configuration](#transaction-log-configuration)
    - [IndexWriter Performance Configuration](#indexwriter-performance-configuration)
    - [AWS Configuration](#aws-configuration)
    - [Split Cache Configuration](#split-cache-configuration)
    - [IndexQuery and IndexQueryAll Operators](#indexquery-and-indexqueryall-operators)
      - [SQL Usage](#sql-usage)
      - [Programmatic Usage](#programmatic-usage)
    - [IndexQueryAll Operator](#indexqueryall-operator)
      - [SQL Usage](#sql-usage-1)
      - [Programmatic Usage](#programmatic-usage-1)
    - [Split Optimization with MERGE SPLITS](#split-optimization-with-merge-splits)
      - [SQL Syntax](#sql-syntax)
      - [Scala/DataFrame API](#scaladataframe-api)
- [File Format](#file-format)
  - [Split Files](#split-files)
  - [Transaction Log](#transaction-log)
- [Development](#development)
  - [Project Structure](#project-structure)
  - [Contributing](#contributing)
  - [Optimization Tips](#optimization-tips)
  - [Optimization Features](#optimization-features)
- [Roadmap](#roadmap)
  - [Planned Features](#planned-features)
- [Known Issues and Solutions](#known-issues-and-solutions)
- [License](#license)
- [Support](#support)

## Features

- 🚀 **Embedded Search**: Runs directly within Spark executors without additional infrastructure
- 💾 **AWS S3 Backend**: Persists and consumes search indexes from inexpensive object storage
- ⚡ **Smart File Skipping**: Delta/Iceberg-like transaction log metadata to support efficient query pruning based on "where" predicates
- 🔍 **Custom Query Operators**: "indexquery" SQL operand to provide access to the full Quickwit search syntax
- 📊 **Extensive Predicate Acceleration**: Most where-clause components are automatically translated to search operations for fast retrieval
- 🎯 **Fast Aggregates**: Accelerated count, sum, min, max, and average operators with and without aggregation
- 🔐 **AWS Session Support**: Support for AWS credentials through instance profile, programmatic access, and custom credential providers

---

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│             Spark Application                   │
├─────────────────────────────────────────────────┤
│        IndexTables DataSource V2 API            │
├─────────────┬───────────────────────────────────┤
│   Tantivy   │      Transaction Log              │
│   Engine    │      (Delta-style)                │
│  (Native)   │   ┌──────────────────┐            │
│             │   │ Metadata Tracking│            │
│             │   │ Min/Max Stats    │            │
│             │   │ File Management  │            │
│             │   └──────────────────┘            │
├─────────────┴───────────────────────────────────┤
│            S3 Storage Layer                     │
│   ┌──────────┐ ┌──────────┐ ┌──────────────┐   │
│   │  Splits  │ │  Splits  │ │ Transaction  │   │
│   │  (.split)│ │  (.split)│ │     Log      │   │
│   └──────────┘ └──────────┘ └──────────────┘   │
└─────────────────────────────────────────────────┘
```

### Key Components:

- **Spark Integration**: Native DataSource V2 implementation for seamless Spark SQL integration
- **Tantivy Engine**: High-performance Rust-based search engine (via tantivy4java JNI bindings)
- **Transaction Log**: Delta Lake-style ACID transaction support with checkpoint optimization
- **Split Storage**: Compressed, indexed data segments optimized for search and analytics
- **Cache Layer**: Intelligent split caching with locality awareness for performance

---

## Installation
### OSS Spark
 1. Install the proper [platform JAR](https://provide.mavencentral.link) file for IndexTables in your boot classpath for your Spark executors and driver
 2. Enable custom SQL extensions by setting the appropriate Spark property (`spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions`)
 3. Configure your driver and worker memory to use only 1/2 of the regular memory amount for Spark (the search system runs mostly in native heap)
 4. Assure that your Spark instance is using at least Java 11.

### Databricks
 On Databricks, you will want to:
  1) Install the appropriate platform-specific JAR files to your workspace root (ex: /Workspace/Users/me/indextables_x86_64_0.0.1.jar)
  2) Create a startup script in your workspace called "add_indextables_to_classpath.sh" with the following text (to add to classpath)

```
#!/bin/bash
cp /Workspace/Users/me/indextables_x86_64_0.0.1.jar /databricks/jars
```

  3) Add the startup script to your server startup configuration
  4) Add the following Spark properties to your server config: 

```
spark.executor.memory=27016m  # This is the setting for an r6id.2xlarge server, set to 1/2 of the default
spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions
```

  5) Add the following environment variable if on DBX 15.4 to upgrade Java to 17 (JNAME=zulu17-ca-amd64)


### Usage

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("IndexTables Example")
  .getOrCreate()

// Write data - see sections below about heap sizing, temporary space, etc...
df.write
  .format("io.indextables.provider.IndexTablesProvider")
  .mode("append")
  // Index the field "message" for full-text search instead
  // of the default exact matching
  .option("spark.indextables.indexing.typemap.message", "text")
  .save("s3://bucket/path/table")

// Merge index segments to a 4GB target size
// for maximum performance and scaling -- THIS IS CRITICAL, bigger is better...
spark.sql("MERGE SPLITS 's3://bucket/path/table' TARGET SIZE 4G")

// Read data with optimized caching
val df = spark.read
  .format("io.indextables.provider.IndexTablesProvider")
  .load("s3://bucket/path/table")

// Query with filters (automatically converted to Tantivy queries)
df.filter($"name".contains("John") && $"age" > 25).show()

// SQL queries including full Spark SQL syntax plus Quickwit filters via "indexquery" operand
df.createOrReplaceTempView("my_table")
spark.sql("SELECT * FROM my_table WHERE category = 'technology' and message indexquery 'critical AND infrastructure' LIMIT 10")
spark.sql("SELECT * FROM my_table WHERE status IN ('active', 'pending') AND score > 85")

// Cross-field search - any record containing the text "mytable"
spark.sql("SELECT * FROM my_table WHERE _indexall indexquery 'mytable'")
```


#### Field Indexing Configuration (Write options)

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.indexing.typemap.<field_name>` | `string` | Field indexing type: `string`, `text`, or `json` |
| `spark.indextables.indexing.fastfields` | - | Comma-separated list of fields for fast access |
| `spark.indextables.indexing.storeonlyfields` | - | Fields stored but not indexed |
| `spark.indextables.indexing.indexonlyfields` | - | Fields indexed but not stored |
| `spark.indextables.indexing.tokenizer.<field_name>` | - | Tokenizer type: `default`, `whitespace`, or `raw` |

---

## Common Use Cases

### 📊 Log Analysis and Observability
```scala
// Search application logs for errors and exceptions
val errors = logs
  .filter($"level" === "ERROR" &&
          $"timestamp" > current_timestamp() - expr("INTERVAL 1 HOUR") &&
          $"message" indexquery "OutOfMemory OR StackOverflow OR NullPointerException")
  .select("timestamp", "service", "message", "stack_trace")
  .show()

// Aggregate errors by service
logs.filter($"level" === "ERROR" &&
            $"message" indexquery "exception OR failed OR timeout")
    .groupBy(window($"timestamp", "5 minutes"), $"service")
    .count()
    .orderBy($"window".desc)
    .show()
```

### 🔐 Security Investigation and SIEM
```scala
// Find all login attempts from suspicious IPs in the last 24 hours
val suspicious_activity = security_logs
  .filter($"event_type".isin("login", "auth_attempt", "access") &&
          $"timestamp" > current_timestamp() - expr("INTERVAL 24 HOURS") &&
          ($"ip_address".isin(suspicious_ips: _*) ||
           $"_indexall" indexquery "failed OR unauthorized OR denied"))
  .select("timestamp", "user", "ip_address", "event_type", "outcome")

// Identify potential brute force attempts
security_logs
  .filter($"event_type" === "login" && $"outcome" === "failed")
  .groupBy($"ip_address", window($"timestamp", "1 minute"))
  .agg(
    count("*").as("attempts"),
    collect_set("user").as("targeted_users")
  )
  .filter($"attempts" > 5)  // More than 5 attempts per minute
  .orderBy($"window".desc)
  .show()
```

### 📈 Application Performance Monitoring (APM)
```scala
// Find slow API calls with specific error patterns
val slow_apis = api_logs
  .filter($"response_time" > 1000 &&  // Response time > 1 second
          $"endpoint" indexquery "GET OR POST" &&
          ($"response_body" indexquery "timeout" || $"status_code" >= 500))
  .select("timestamp", "endpoint", "response_time", "status_code", "trace_id")
  .orderBy($"response_time".desc)
  .show(50)

// Analyze error patterns in microservices
api_logs
  .filter($"trace_id".isNotNull &&
          $"_indexall" indexquery "error OR exception OR failed")
  .groupBy("service_name", "endpoint")
  .agg(
    count("*").as("error_count"),
    avg("response_time").as("avg_response_time"),
    percentile_approx($"response_time", 0.95).as("p95_response_time")
  )
  .orderBy($"error_count".desc)
  .show()
```

### 🔍 Full-Text Search in Documents
```scala
// Search knowledge base for relevant documents
val relevant_docs = documents
  .filter($"content" indexquery "machine learning AND (tensorflow OR pytorch)")
  .filter($"published_date" >= "2023-01-01")
  .select("title", "author", "published_date", "abstract")
  .show()

// Find documents with complex boolean queries
documents
  .filter($"content" indexquery """
    (artificial intelligence OR machine learning) AND
    (python OR scala) AND
    NOT deprecated AND
    "neural network"
  """)
  .select("title", "tags", "last_updated")
  .show()
```

### 📊 Business Intelligence and Analytics
```scala
// Search customer feedback for specific issues
val customer_issues = feedback
  .filter($"_indexall" indexquery "refund OR complaint OR dissatisfied")
  .filter($"rating" <= 3)
  .groupBy("product_category", "issue_type")
  .agg(
    count("*").as("issue_count"),
    avg("rating").as("avg_rating")
  )
  .orderBy($"issue_count".desc)

// Analyze support tickets with natural language queries
support_tickets
  .filter($"status" === "open" &&
          $"description" indexquery "billing problem OR payment failed" &&
          $"priority" === "high")
  .select("ticket_id", "customer_id", "created_at", "description")
  .show()
```

---

## Migration Guide

### 📦 From Parquet to IndexTables
```scala
// Step 1: Read existing Parquet data
val parquetDF = spark.read.parquet("s3://bucket/parquet-data")

// Step 2: Analyze your schema and identify search fields
parquetDF.printSchema()

// Step 3: Convert to IndexTables with appropriate field configurations
parquetDF.write
  .format("io.indextables.provider.IndexTablesProvider")
  .mode("overwrite")
  // Configure text fields for full-text search
  .option("spark.indextables.indexing.typemap.message", "text")
  .option("spark.indextables.indexing.typemap.description", "text")
  .option("spark.indextables.indexing.typemap.content", "text")
  // Configure exact match fields (default)
  .option("spark.indextables.indexing.typemap.id", "string")
  .option("spark.indextables.indexing.typemap.status", "string")
  // Enable fast fields for aggregations
  .option("spark.indextables.indexing.fastfields", "timestamp,count,score")
  .save("s3://bucket/indextable-data")

// Step 4: Optimize the splits for better performance
spark.sql("MERGE SPLITS 's3://bucket/indextable-data' TARGET SIZE 1G")
```

### 🔺 From Delta Lake to IndexTables
```scala
// Read from Delta table
val deltaDF = spark.read.format("delta").load("s3://bucket/delta-table")

// Preserve partitioning if needed
val partitionColumns = Seq("year", "month", "day")

// Convert with partitioning preserved
deltaDF.write
  .format("io.indextables.provider.IndexTablesProvider")
  .partitionBy(partitionColumns: _*)
  .option("spark.indextables.indexing.typemap.event_data", "json")
  .option("spark.indextables.indexing.typemap.log_message", "text")
  .mode("overwrite")
  .save("s3://bucket/indextable-data")
```

### 📋 From CSV/JSON to IndexTables
```scala
// Read CSV with schema inference
val csvDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("s3://bucket/csv-files/*.csv")

// Read JSON
val jsonDF = spark.read.json("s3://bucket/json-files/*.json")

// Transform and write to IndexTables
csvDF.union(jsonDF)
  .write
  .format("io.indextables.provider.IndexTablesProvider")
  .option("spark.indextables.indexing.typemap.comment", "text")
  .option("spark.indextables.indexing.fastfields", "rating,timestamp")
  .save("s3://bucket/unified-indextable")
```

### 🔄 Incremental Migration Strategy
```scala
// For large datasets, migrate incrementally by time ranges
def migrateTimeRange(startDate: String, endDate: String) = {
  spark.read
    .parquet("s3://bucket/source-data")
    .filter($"date" >= startDate && $"date" < endDate)
    .write
    .format("io.indextables.provider.IndexTablesProvider")
    .mode("append")
    .option("spark.indextables.indexing.typemap.message", "text")
    .save("s3://bucket/indextable-data")
}

// Migrate in batches
val dateRanges = Seq(
  ("2023-01-01", "2023-02-01"),
  ("2023-02-01", "2023-03-01"),
  ("2023-03-01", "2023-04-01")
)

dateRanges.foreach { case (start, end) =>
  migrateTimeRange(start, end)
  // Optimize after each batch
  spark.sql(s"MERGE SPLITS 's3://bucket/indextable-data' TARGET SIZE 2G")
}
```

---

## Best Practices

### ✅ DO's

#### 🎯 **DO: Optimize Split Sizes**
```scala
// Target 1-4GB splits for optimal performance
spark.sql("MERGE SPLITS 's3://bucket/table' TARGET SIZE 2G")

// For time-series data, merge by partition
spark.sql("""
  MERGE SPLITS 's3://bucket/table'
  WHERE date = '2024-01-01'
  TARGET SIZE 1G
""")
```

#### 🔍 **DO: Choose Field Types Wisely**
```scala
df.write.format("io.indextables.provider.IndexTablesProvider")
  // Use 'text' for fields requiring full-text search
  .option("spark.indextables.indexing.typemap.message", "text")
  .option("spark.indextables.indexing.typemap.description", "text")
  // Use 'string' (default) for exact matching
  .option("spark.indextables.indexing.typemap.status", "string")
  .option("spark.indextables.indexing.typemap.user_id", "string")
  // Use 'json' for JSON content
  .option("spark.indextables.indexing.typemap.metadata", "json")
  .save(path)
```

#### 💾 **DO: Configure Memory Correctly**
```scala
// Allocate 50% to Spark heap, 50% to native memory
spark.conf.set("spark.executor.memory", "8g")  // If total memory is 16GB
spark.conf.set("spark.executor.memoryOverhead", "8g")
```

#### ⚡ **DO: Use Fast Fields for Aggregations**
```scala
// Configure frequently aggregated fields as fast fields
df.write.format("io.indextables.provider.IndexTablesProvider")
  .option("spark.indextables.indexing.fastfields", "timestamp,count,total,score")
  .save(path)
```

#### 📅 **DO: Partition Time-Series Data**
```scala
// Partition by time for efficient pruning
df.write.format("io.indextables.provider.IndexTablesProvider")
  .partitionBy("year", "month", "day")
  .save(path)
```

### ❌ DON'T's

#### 🚫 **DON'T: Create Small Splits**
```scala
// BAD: Too many small splits hurt performance
df.repartition(1000).write...  // Avoid over-partitioning

// GOOD: Let IndexTables optimize or use auto-sizing
df.write
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "500M")
```

#### 🚫 **DON'T: Use Default Memory Settings**
```scala
// BAD: Using default Spark memory configuration
spark.conf.set("spark.executor.memory", "16g")  // Using all memory for heap

// GOOD: Split memory between heap and native
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "8g")
```

#### 🚫 **DON'T: Index Everything**
```scala
// BAD: Indexing fields that don't need search
.option("spark.indextables.indexing.typemap.internal_id", "text")  // Wasteful

// GOOD: Only index searchable fields, store others
.option("spark.indextables.indexing.storeonlyfields", "internal_id,system_field")
```

#### 🚫 **DON'T: Forget to Merge Splits**
```scala
// BAD: Writing data without optimization
df.write.format("io.indextables.provider.IndexTablesProvider").save(path)
// Forgetting to merge...

// GOOD: Always merge after large ingestions
df.write.format("io.indextables.provider.IndexTablesProvider").save(path)
spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 2G")
```

#### 🚫 **DON'T: Mix Field Types Incorrectly**
```scala
// BAD: Using 'string' type for content that needs full-text search
.option("spark.indextables.indexing.typemap.log_message", "string")
// This will only allow exact matches, not text search!

// GOOD: Use appropriate field types
.option("spark.indextables.indexing.typemap.log_message", "text")
```

### 📊 Performance Tips

> **⚡ Tip:** Pre-warm caches for better query performance
> ```scala
> spark.conf.set("spark.indextables.cache.prewarm.enabled", "true")
> ```

> **🎯 Tip:** Monitor split statistics regularly
> ```scala
> spark.sql("SELECT COUNT(*) as split_count, AVG(size) as avg_size FROM transaction_log")
> ```

> **💡 Tip:** Use IndexQuery for complex searches instead of multiple filters
> ```scala
> // Instead of: df.filter($"msg".contains("error") || $"msg".contains("fail"))
> df.filter($"msg" indexquery "error OR fail")
> ```

---

### Configuration Options (Read options and/or Spark properties)

The system supports several configuration options for performance tuning:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.storage.force.standard` | `false` | Force standard Hadoop operations for all protocols |
| `spark.indextables.cache.name` | `"tantivy4spark-cache"` | Name of the JVM-wide split cache |
| `spark.indextables.cache.maxSize` | `200000000` | Maximum cache size in bytes (200MB default) |
| `spark.indextables.cache.maxConcurrentLoads` | `8` | Maximum concurrent component loads |
| `spark.indextables.cache.queryCache` | `true` | Enable query result caching |
| `spark.indextables.cache.directoryPath` | auto-detect `/local_disk0` | Custom cache directory path (auto-detects optimal location) |
| `spark.indextables.cache.prewarm.enabled` | `true` | Enable proactive cache warming |
| `spark.indextables.docBatch.enabled` | `true` | Enable batch document retrieval for better performance |
| `spark.indextables.docBatch.maxSize` | `1000` | Maximum documents per batch |
| `spark.indextables.indexWriter.heapSize` | `100000000` | Index writer heap size in bytes (100MB default, supports "2G", "500M", "1024K") |
| `spark.indextables.indexWriter.threads` | `2` | Number of indexing threads (2 threads default) |
| `spark.indextables.indexWriter.batchSize` | `10000` | Batch size for bulk document indexing (10,000 documents default) |
| `spark.indextables.indexWriter.useBatch` | `true` | Enable batch writing for better performance (enabled by default) |
| `spark.indextables.indexWriter.tempDirectoryPath` | auto-detect `/local_disk0` | Custom temp directory for index creation (auto-detects optimal location) |
| `spark.indextables.merge.tempDirectoryPath` | auto-detect `/local_disk0` | Custom temp directory for split merging (auto-detects optimal location) |
| `spark.indextables.aws.accessKey` | - | AWS access key for S3 split access |
| `spark.indextables.aws.secretKey` | - | AWS secret key for S3 split access |
| `spark.indextables.aws.sessionToken` | - | AWS session token for temporary credentials (STS) |
| `spark.indextables.aws.region` | - | AWS region for S3 split access |
| `spark.indextables.aws.endpoint` | - | Custom AWS S3 endpoint |
| `spark.indextables.aws.credentialsProviderClass` | - | Fully qualified class name of custom AWS credential provider |
| `spark.indextables.s3.endpoint` | - | S3 endpoint URL (alternative to aws.endpoint) |
| `spark.indextables.s3.pathStyleAccess` | `false` | Use path-style access for S3 (required for some S3-compatible services) |


#### Auto-Sizing Configuration

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.autoSize.enabled` | `false` | Enable auto-sizing based on historical data |
| `spark.indextables.autoSize.targetSplitSize` | - | Target size per split (supports: "100M", "1G", "512K", bytes) |
| `spark.indextables.autoSize.inputRowCount` | - | Explicit row count for accurate partitioning (required for V2 API) |

#### S3 Upload Configuration

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.s3.streamingThreshold` | `104857600` | Files larger than this use streaming upload (100MB default) |
| `spark.indextables.s3.multipartThreshold` | `104857600` | Threshold for S3 multipart upload (100MB default) |
| `spark.indextables.s3.maxConcurrency` | `4` | Number of parallel upload threads |


#### Transaction Log Configuration

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.parallel.read.enabled` | `true` | Enable parallel transaction log operations |
| `spark.indextables.async.updates.enabled` | `true` | Enable asynchronous snapshot updates |
| `spark.indextables.snapshot.maxStaleness` | `5000` | Maximum staleness tolerance in milliseconds |
| `spark.indextables.cache.log.size` | `1000` | Log cache maximum entries |
| `spark.indextables.cache.log.ttl` | `5` | Log cache TTL in minutes |
| `spark.indextables.cache.snapshot.size` | `100` | Snapshot cache maximum entries |
| `spark.indextables.cache.snapshot.ttl` | `10` | Snapshot cache TTL in minutes |
| `spark.indextables.cache.filelist.size` | `50` | File list cache maximum entries |
| `spark.indextables.cache.filelist.ttl` | `2` | File list cache TTL in minutes |
| `spark.indextables.cache.metadata.size` | `100` | Metadata cache maximum entries |
| `spark.indextables.cache.metadata.ttl` | `30` | Metadata cache TTL in minutes |
| `spark.indextables.checkpoint.enabled` | `true` | Enable automatic checkpoint creation |
| `spark.indextables.checkpoint.interval` | `10` | Create checkpoint every N transactions |
| `spark.indextables.checkpoint.parallelism` | `4` | Thread pool size for parallel I/O |
| `spark.indextables.checkpoint.read.timeoutSeconds` | `30` | Timeout for parallel read operations |
| `spark.indextables.logRetention.duration` | `2592000000` | Log retention duration (30 days in milliseconds) |
| `spark.indextables.checkpointRetention.duration` | `7200000` | Checkpoint retention duration (2 hours in milliseconds) |
| `spark.indextables.checkpoint.checksumValidation.enabled` | `true` | Enable data integrity validation |
| `spark.indextables.checkpoint.multipart.enabled` | `false` | Enable multi-part checkpoints for large tables |
| `spark.indextables.checkpoint.multipart.maxActionsPerPart` | `50000` | Actions per checkpoint part |
| `spark.indextables.checkpoint.auto.enabled` | `true` | Enable automatic checkpoint optimization |
| `spark.indextables.checkpoint.auto.minFileAge` | `600000` | Minimum file age for auto checkpoint (10 minutes in milliseconds) |
| `spark.indextables.transaction.cache.enabled` | `true` | Enable transaction log caching |
| `spark.indextables.transaction.cache.expirationSeconds` | `300` | Transaction cache TTL (5 minutes) |

#### IndexWriter Performance Configuration

Configure indexWriter for optimal batch processing performance:

```scala
// Configure index writer performance settings via Spark session
spark.conf.set("spark.indextables.indexWriter.heapSize", "200000000") // 200MB heap
spark.conf.set("spark.indextables.indexWriter.threads", "4") // 4 indexing threads
spark.conf.set("spark.indextables.indexWriter.batchSize", "20000") // 20,000 documents per batch
spark.conf.set("spark.indextables.indexWriter.useBatch", "true") // Enable batch writing

// Configure per DataFrame write operation (overrides session config)
df.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
  .option("spark.indextables.indexWriter.heapSize", "150000000") // 150MB heap
  .option("spark.indextables.indexWriter.threads", "3") // 3 indexing threads
  .option("spark.indextables.indexWriter.batchSize", "15000") // 15,000 documents per batch
  .save("s3://bucket/path")

// Disable batch writing for debugging (use individual document indexing)
df.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
  .option("spark.indextables.indexWriter.useBatch", "false")
  .save("s3://bucket/path")

// High-throughput configuration for large datasets
spark.conf.set("spark.indextables.indexWriter.heapSize", "500000000") // 500MB heap
spark.conf.set("spark.indextables.indexWriter.threads", "8") // 8 indexing threads
spark.conf.set("spark.indextables.indexWriter.batchSize", "50000") // 50,000 documents per batch
df.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").save("s3://bucket/large-dataset")
```

#### AWS Configuration

Configure AWS credentials for S3 operations:

```scala
// Standard AWS credentials
spark.conf.set("spark.indextables.aws.accessKey", "your-access-key")
spark.conf.set("spark.indextables.aws.secretKey", "your-secret-key")
spark.conf.set("spark.indextables.aws.region", "us-west-2")

// AWS credentials with session token (temporary credentials from STS)
spark.conf.set("spark.indextables.aws.accessKey", "your-temporary-access-key")
spark.conf.set("spark.indextables.aws.secretKey", "your-temporary-secret-key")
spark.conf.set("spark.indextables.aws.sessionToken", "your-session-token")
spark.conf.set("spark.indextables.aws.region", "us-west-2")

// Custom S3 endpoint (for S3-compatible services like MinIO, LocalStack)
spark.conf.set("spark.indextables.aws.endpoint", "https://s3.custom-provider.com")

df.write.format("io.indextables.provider.IndexTablesProvider").save("s3://bucket/path")

// Alternative: Pass credentials via write options (automatically propagated to executors)
df.write.format("io.indextables.provider.IndexTablesProvider")
  .option("spark.indextables.aws.accessKey", "your-access-key")
  .option("spark.indextables.aws.secretKey", "your-secret-key")
  .option("spark.indextables.aws.sessionToken", "your-session-token")
  .option("spark.indextables.aws.region", "us-west-2")
  .save("s3://bucket/path")
```

#### Split Cache Configuration

Configure the JVM-wide split cache for optimal performance:

```scala
// Automatic optimization (recommended) - uses /local_disk0 when available
// No configuration needed on Databricks, EMR, or systems with /local_disk0

// Configure split cache settings
spark.conf.set("spark.indextables.cache.maxSize", "500000000") // 500MB cache
spark.conf.set("spark.indextables.cache.maxConcurrentLoads", "16") // More concurrent loads
spark.conf.set("spark.indextables.cache.queryCache", "true") // Enable query caching
spark.conf.set("spark.indextables.cache.directoryPath", "/fast-ssd/tantivy-cache") // Custom cache location

// Configure temp directories for high-performance storage
spark.conf.set("spark.indextables.indexWriter.tempDirectoryPath", "/fast-ssd/tantivy-temp")
spark.conf.set("spark.indextables.merge.tempDirectoryPath", "/fast-ssd/merge-temp")

// Configure per DataFrame write (overrides session config)
df.write.format("io.indextables.provider.IndexTablesProvider")
  .option("spark.indextables.cache.maxSize", "1000000000") // 1GB cache for this operation
  .option("spark.indextables.cache.directoryPath", "/nvme/cache") // High-performance cache
  .save("s3://bucket/path")
```

#### IndexQuery and IndexQueryAll Operators

Tantivy4Spark supports powerful query operators for native Tantivy query syntax with full filter pushdown:

- **IndexQuery**: Field-specific search with column specification
- **IndexQueryAll**: All-fields search using virtual `_indexall` column

##### SQL Usage

```sql
-- Register Tantivy4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("com.tantivy4spark.extensions.Tantivy4SparkExtensions")

-- Create table/view from Tantivy4Spark data
CREATE TEMPORARY VIEW my_documents
USING io.indextables.provider.IndexTablesProvider
OPTIONS (path 's3://bucket/my-data');

-- Basic IndexQuery usage in SQL (field-specific)
SELECT * FROM my_documents WHERE title indexquery 'apache AND spark';

-- Basic IndexQueryAll usage in SQL (all-fields search with virtual _indexall column)
SELECT * FROM my_documents WHERE _indexall indexquery 'VERIZON OR T-MOBILE';

-- Complex boolean queries
SELECT * FROM my_documents WHERE content indexquery '(machine AND learning) OR (data AND science)';
SELECT * FROM my_documents WHERE _indexall indexquery '(apache AND spark) OR (machine AND learning)';

-- Field-specific queries vs all-fields
SELECT * FROM my_documents WHERE description indexquery 'title:(fast OR quick) AND content:"deep learning"';
SELECT * FROM my_documents WHERE _indexall indexquery '"artificial intelligence" AND NOT deprecated';

-- Phrase searches
SELECT * FROM my_documents WHERE content indexquery '"artificial intelligence"';
SELECT * FROM my_documents WHERE _indexall indexquery '"natural language processing"';

-- Negation queries
SELECT * FROM my_documents WHERE tags indexquery 'python AND NOT deprecated';
SELECT * FROM my_documents WHERE _indexall indexquery 'apache AND NOT legacy';

-- Combined with standard SQL predicates
SELECT title, content, score 
FROM my_documents 
WHERE content indexquery 'spark AND sql' 
  AND category = 'technology' 
  AND published_date >= '2023-01-01'
ORDER BY score DESC 
LIMIT 10;
```

##### Programmatic Usage

```scala
import com.tantivy4spark.expressions.IndexQueryExpression
import com.tantivy4spark.util.ExpressionUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column

// Create IndexQuery expressions programmatically for field-specific queries
val titleColumn = col("title").expr
val complexQuery = Literal(UTF8String.fromString("(apache AND spark) OR (hadoop AND mapreduce)"), StringType)
val indexQuery = IndexQueryExpression(titleColumn, complexQuery)

// Create all-fields search using virtual _indexall column
val allFieldsQuery = Literal(UTF8String.fromString("VERIZON OR T-MOBILE"), StringType)
val indexQueryAll = IndexQueryExpression(col("_indexall").expr, allFieldsQuery)

// Use in DataFrame operations
df.filter(indexQuery).show()                    // Field-specific search
df.filter(new Column(indexQueryAll)).show()    // All-fields search using _indexall

// Advanced query patterns
val patterns = Seq(
  "title:(spark AND sql)",           // Field-specific boolean query
  "content:\"machine learning\"",    // Phrase search
  "description:(fast OR quick)",     // OR queries
  "tags:(python AND NOT deprecated)" // Negation queries
)

// Apply multiple IndexQuery filters
patterns.foreach { pattern =>
  val query = IndexQueryExpression(col("content").expr, 
    Literal(UTF8String.fromString(pattern), StringType))
  df.filter(query).show()
}
```

#### IndexQueryAll Operator

Tantivy4Spark supports searching across all fields using the virtual `_indexall` column with the `indexquery` operator:

##### SQL Usage

```sql
-- Register Tantivy4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("com.tantivy4spark.extensions.Tantivy4SparkExtensions")

-- Create table/view from Tantivy4Spark data
CREATE TEMPORARY VIEW my_documents
USING io.indextables.provider.IndexTablesProvider
OPTIONS (path 's3://bucket/my-data');

-- Basic IndexQueryAll usage - searches across ALL fields using virtual _indexall column
SELECT * FROM my_documents WHERE _indexall indexquery 'VERIZON OR T-MOBILE';

-- Complex boolean queries across all fields
SELECT * FROM my_documents WHERE _indexall indexquery '(apache AND spark) OR (machine AND learning)';

-- Phrase searches across all fields
SELECT * FROM my_documents WHERE _indexall indexquery '"artificial intelligence"';

-- Combined with standard SQL predicates
SELECT title, content, category 
FROM my_documents 
WHERE _indexall indexquery 'spark AND sql' 
  AND category = 'technology' 
  AND status = 'published'
ORDER BY score DESC 
LIMIT 10;

-- Multiple search patterns
SELECT * FROM my_documents 
WHERE _indexall indexquery 'apache OR python' 
   OR _indexall indexquery 'machine learning';
```

##### Programmatic Usage

```scala
import com.tantivy4spark.expressions.IndexQueryExpression
import com.tantivy4spark.util.ExpressionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.unsafe.types.UTF8String

// Create all-fields search using virtual _indexall column
val allFieldsQuery = IndexQueryExpression(
  col("_indexall").expr,
  Literal(UTF8String.fromString("VERIZON OR T-MOBILE"), StringType)
)

// Use in DataFrame operations
df.filter(new Column(allFieldsQuery)).show()

// Complex patterns across all fields using _indexall virtual column
val patterns = Seq(
  "apache AND spark",           // Boolean query across all fields
  "\"machine learning\"",       // Phrase search across all fields
  "(python OR scala)",         // OR queries across all fields
  "data AND NOT deprecated"     // Negation queries across all fields
)

patterns.foreach { pattern =>
  val query = IndexQueryExpression(
    col("_indexall").expr,
    Literal(UTF8String.fromString(pattern), StringType)
  )
  df.filter(new Column(query)).show()
}

// Alternative: Use Spark SQL with temp view for cleaner syntax
df.createOrReplaceTempView("my_docs")
spark.sql("SELECT * FROM my_docs WHERE _indexall indexquery 'apache AND spark'").show()
```

#### Split Optimization with MERGE SPLITS

Tantivy4Spark provides SQL-based split consolidation to reduce small file overhead and optimize query performance:

##### SQL Syntax

```sql
-- Register Tantivy4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("com.tantivy4spark.extensions.Tantivy4SparkExtensions")

-- Basic merge splits command
MERGE SPLITS 's3://bucket/path';

-- With target size constraint (consolidate to specific size)
MERGE SPLITS 's3://bucket/path' TARGET SIZE 104857600;  -- 100MB
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M;       -- 100MB with suffix
MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G;         -- 1GB with suffix

-- With group limit constraint (limit number of merge operations)
MERGE SPLITS 's3://bucket/path' MAX GROUPS 10;          -- Limit to 10 merge groups

-- Combined constraints for fine-grained control
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M MAX GROUPS 5;

-- With WHERE clause for partition filtering
MERGE SPLITS 's3://bucket/path' WHERE year = 2023 TARGET SIZE 100M;
```

##### Scala/DataFrame API

```scala
// Basic merge splits operation
spark.sql("MERGE SPLITS 's3://bucket/path'")

// With target size constraints
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M")
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G")

// Limit the number of merge groups created
spark.sql("MERGE SPLITS 's3://bucket/path' MAX GROUPS 10")

// Combined size and job constraints
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 4G MAX GROUPS 5")

// With partition filtering
spark.sql("MERGE SPLITS 's3://bucket/path' WHERE year = 2023 TARGET SIZE 100M")
```

## File Format

### Split Files

Tantivy indexes are stored as `.split` files (QuickwitSplit format):
- **Split-based storage**: Optimized binary format for fast loading and caching
- **UUID-based naming**: Split files use UUID-based naming (`part-{partitionId}-{taskId}-{uuid}.split`) for guaranteed uniqueness across concurrent writes
- **JVM-wide caching**: Shared `SplitCacheManager` reduces memory usage across executors
- **Native compatibility**: Direct integration with tantivy4java library
- **S3-optimized**: Efficient partial loading and caching for object storage
- **Cache locality tracking**: Automatic tracking of which hosts have cached which splits for optimal scheduling

### Transaction Log

Located in `_transaction_log/` directory (Delta Lake compatible):
- **Batched operations**: Single JSON file per transaction with multiple ADD entries
- **Atomic operations**: REMOVE + ADD actions in single transaction for overwrite mode
- **Partition tracking**: Comprehensive partition support with pruning integration
- **Row count tracking**: Per-file record counts for statistics and optimization
- `00000000000000000000.json` - Initial metadata and schema
- `00000000000000000001.json` - First transaction (multiple ADD operations)
- `00000000000000000002.json` - Second transaction (additional files)
- Stores min/max values for data skipping and query optimization

## Development

### Project Structure

```
src/main/scala/
├── com/tantivy4spark/
│   ├── catalyst/       # Spark Catalyst optimizer integration
│   ├── config/         # Configuration management
│   ├── conversion/     # Type conversion utilities
│   ├── core/           # Spark DataSource V2 integration
│   ├── expressions/    # IndexQuery expression support
│   ├── extensions/     # Spark SQL extensions (MERGE SPLITS, etc.)
│   ├── filters/        # Query filter handling
│   ├── io/             # I/O utilities and cloud storage support
│   ├── optimize/       # Write optimization and auto-sizing
│   ├── prewarm/        # Cache pre-warming management
│   ├── schema/         # Schema mapping and conversion
│   ├── search/         # Tantivy search engine wrapper via tantivy4java
│   ├── sql/            # SQL command implementations
│   ├── storage/        # S3-optimized storage layer
│   ├── transaction/    # Transaction log system
│   ├── util/           # General utilities
│   └── utils/          # Additional utility classes
└── io/indextables/     # Alias namespace for vendor-neutral interface
    ├── extensions/     # IndexTablesSparkExtensions alias
    └── provider/       # IndexTablesProvider alias

src/main/java/com/tantivy4spark/
└── auth/
    └── unity/          # Unity Catalog credential provider

src/main/antlr4/        # ANTLR grammar definitions
└── com/tantivy4spark/sql/parser/
    └── Tantivy4SparkSqlBase.g4  # SQL parser grammar for custom commands

src/test/scala/         # Comprehensive test suite (205+ tests passing)
├── com/tantivy4spark/
│   ├── autosize/       # Auto-sizing feature tests
│   ├── comprehensive/  # Comprehensive integration tests
│   ├── config/         # Configuration tests
│   ├── core/           # Core functionality tests including SQL pushdown
│   ├── debug/          # Debug and diagnostic tests
│   ├── demo/           # Demo and example tests
│   ├── expressions/    # IndexQuery expression tests
│   ├── filters/        # Filter pushdown tests
│   ├── indexing/       # Indexing behavior tests
│   ├── indexquery/     # IndexQuery operator tests
│   ├── integration/    # End-to-end integration tests
│   ├── io/             # I/O and cloud storage tests
│   ├── locality/       # Cache locality tests
│   ├── optimize/       # Optimized writes tests
│   ├── performance/    # Performance benchmark tests
│   ├── prewarm/        # Cache pre-warming tests
│   ├── schema/         # Schema mapping tests
│   ├── search/         # Search engine tests
│   ├── sql/            # SQL command tests
│   ├── storage/        # Storage protocol tests
│   ├── transaction/    # Transaction log tests
│   └── util/           # Utility tests
└── io/indextables/
    └── extensions/     # Alias extension tests
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass and coverage requirements are met
5. Submit a pull request

### Optimization Tips

1. Use appropriate data types in your schema
2. Enable S3 optimization for cloud workloads  
3. Leverage data skipping with min/max statistics
4. Partition large datasets by common query dimensions

### Optimization Features

1. **Split Cache Locality**: The system automatically tracks which hosts have cached which splits and uses Spark's `preferredLocations` API to schedule tasks on those hosts, reducing network I/O and improving query performance.

2. **Intelligent Task Scheduling**: When reading data, Spark preferentially schedules tasks on hosts that have already cached the required split files, leading to:
   - Faster query execution due to local cache hits
   - Reduced network bandwidth usage
   - Better cluster resource utilization

## Roadmap

See [BACKLOG.md](BACKLOG.md) for detailed development roadmap including:

### Planned Features
- **Table hygiene**: Capability similar to Delta "VACUUM" command
- **Multi-cloud Enhancements**: Expanded Azure and GCP support
- **Catalog support**: Support for Hive catalogs
- **Schema migration**: Support for updating schemas and indexing schemes
- **Re-indexing support**: Support for changing indexing types of fields from plain strings to full-text search
- **Prewarming enhancements**: Better support for pre-warming caches on new clusters
- **Memory auto-tuning**: Better support for automatically tuning native heaps for indexing, merging, and queries
- **VARIANT Data types**: Support for JSON fields
- **Arrays and embedded structures**: Support for complex column types


## Known Issues and Solutions
- TBD

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

- GitHub Issues: Report bugs and request features
- Documentation: Comprehensive test suite with 179 tests demonstrating usage patterns
- Community: Check the test files in `src/test/scala/` for detailed usage examples
- SQL Pushdown: See `SqlPushdownTest.scala` for detailed examples of predicate and limit pushdown verification
