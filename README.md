# IndexTables for Spark

IndexTables is an experimental open-table format for Apache Spark that enables fast retrieval and full-text search across large-scale data. It integrates seamlessly with Spark SQL, allowing you to combine powerful search capabilities with joins, aggregations, and standard SQL operations. Originally built for log observability and cybersecurity investigations, IndexTables works well for any use case requiring fast data retrieval.

IndexTables runs entirely within your existing Spark cluster with no additional infrastructure. It stores data in object storage (tested on AWS S3) and has been verified on OSS Spark 3.5.2 and Databricks 15.4 LTS. While Spark is the only supported platform today, we're exploring future support for Presto and Trino. We welcome community feedback on our plans, our implementation, and anything else.

Under the hood, IndexTables uses [Tantivy](https://github.com/quickwit-oss/tantivy) and [Quickwit splits](https://github.com/quickwit-oss/quickwit) instead of Parquet. This hybrid row and columnar storage format, combined with advanced indexing, delivers extremely fast keyword searches across massive datasets.

> **⚠️ Development Status**: IndexTables is under active development with frequent updates and improvements. APIs and features may change as the project evolves. We recommend thorough testing in non-production environments before deploying to production workloads.  *DO NOT USE THIS TABLE FORMAT TO STORE THE ONLY COPY OF YOUR BUSINESS DATA*

To contact the original author and maintainer of this repository, [Scott Schenkein](https://www.linkedin.com/in/schenksj/), please open a GitHub issue or connect on LinkedIn.

### Usage Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("IndexTables Example") \
    .getOrCreate()

# Write data (Make message eligible for full-text search
#  by declaring it as a "text" type)
(df.write
    .format("io.indextables.provider.IndexTablesProvider")
    .mode("append")
    .option("spark.indextables.indexing.typemap.message", "text")
    .save("s3://bucket/path/table")

# Merge index segments for optimal performance
# Larger splits (1+GB) improve query performance and reduce overhead
spark.sql("MERGE SPLITS 's3://bucket/path/table' TARGET SIZE 4G")

# Read data
df = spark.read \
    .format("io.indextables.provider.IndexTablesProvider") \
    .load("s3://bucket/path/table")

# Optionally explicitly set your aws credentials
#
# read.option("spark.indextables.indexing.aws.accessKey", accessKey) \
#     .option("spark.indextables.indexing.aws.secretKey", secretKey) \
#     .option("spark.indextables.indexing.aws.sessionToken", sessionToken) \
#            -- or --  (see docs for more info)
#     .option("spark.indextables.aws.credentialsProviderClass", "com.MyCredentialProvider")

# SQL queries including full Spark SQL syntax
# plus Quickwit filters via "indexquery" operand
#
# Note: LIMITS are important for interactive use cases,
#  as scanning tables is very fast but retrieving lots of 
#  rows can take time, AND interactive users typically
#  only look at a few rows before pivoting
#
df.createOrReplaceTempView("my_table")
spark.sql("""
    SELECT * FROM my_table
    WHERE category = 'technology'
      AND message indexquery 'critical AND infrastructure'
    LIMIT 100
""").show()

# Cross-field search - any record containing the term "mytable"
# in ANY field.  Using default limit (5000)
spark.sql("""
    SELECT * FROM my_table
    WHERE _indexall indexquery 'mytable'
""").show()

# Query with programmatic filters
df.filter((col("name").contains("John")) & (col("age") > 25)).show()
```

> **⚠️ NOTE ON "LIMITS"**: Currently, our integration with the underlying Quickwit search libraries pulls documents in small chunks from S3, which impacts document retrieval performance and increases S3 API usage. We plan to work with the Quickwit team, or fork that part of the implementation, to more efficiently pull and cache large numbers of documents.  For most search applications, a low limit is acceptable since interactive searchers only look at the first few documents. However, we plan to address this for future non-human uses.

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Installation](#installation)
  - [OSS Spark](#oss-spark)
  - [Databricks](#databricks)
- [Common Use Cases](#common-use-cases)
  - [Log Analysis and Observability](#-log-analysis-and-observability)
  - [Security Investigation and SIEM](#-security-investigation-and-siem)
  - [Application Performance Monitoring](#-application-performance-monitoring-apm)
  - [Full-Text Search in Documents](#-full-text-search-in-documents)
  - [Business Intelligence and Analytics](#-business-intelligence-and-analytics)
- [Migration Guide](#migration-guide)
- [Best Practices](#best-practices)
- [Configuration Options](#configuration-options-read-options-andor-spark-properties)
  - [Field Indexing Configuration](#field-indexing-configuration)
  - [Auto-Sizing Configuration](#auto-sizing-configuration)
  - [S3 Upload Configuration](#s3-upload-configuration)
  - [Transaction Log Configuration](#transaction-log-configuration)
  - [IndexWriter Performance Configuration](#indexwriter-performance-configuration)
  - [AWS Configuration](#aws-configuration)
  - [Split Cache Configuration](#split-cache-configuration)
  - [IndexQuery and IndexQueryAll Operators](#indexquery-and-indexqueryall-operators)
  - [Split Optimization with MERGE SPLITS](#split-optimization-with-merge-splits)
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
- [FAQ](#-frequently-asked-questions-faq)
- [License](#license)
- [Support](#support)
- [Acknowledgments](#-acknowledgments)

## Features

- 🚀 **Embedded Search**: Runs directly within Spark executors with no additional infrastructure required
- 💾 **S3 Storage**: Stores indexed data in cost-effective object storage (tested on AWS S3)
- ⚡ **Smart File Skipping**: Delta/Iceberg-style transaction log with min/max statistics for efficient query pruning
- 🔍 **Full-Text Search**: Native `indexquery` operator provides access to complete Tantivy search syntax
- 📊 **Predicate Pushdown**: WHERE clause filters automatically convert to native search operations for faster execution
- 🎯 **Aggregate Pushdown**: COUNT, SUM, AVG, MIN, MAX execute directly in the search engine (10-100x faster)
- 🔐 **Flexible AWS Authentication**: Supports instance profiles, programmatic credentials, and custom credential providers

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
│   ┌──────────┐ ┌──────────┐ ┌──────────────┐    │
│   │  Splits  │ │  Splits  │ │ Transaction  │    │
│   │  (.split)│ │  (.split)│ │     Log      │    │
│   └──────────┘ └──────────┘ └──────────────┘    │
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

1. **Install the JAR**: Add the platform-specific [IndexTables JAR](https://provide.mavencentral.link) to the boot classpath for both executors and driver
2. **Enable SQL extensions**: Set `spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions`
3. **Configure memory**: Allocate 50% for Spark heap and 50% for native memory overhead (IndexTables runs primarily in native heap)
4. **Java version**: Requires Java 11 or higher

### Databricks

Follow these steps to install IndexTables on Databricks:

1. **Upload JAR**: Install the platform-specific JAR to your workspace (e.g., `/Workspace/Users/me/indextables_x86_64_0.0.1.jar`)
2. **Create startup script**: Add a script named `add_indextables_to_classpath.sh` to copy the JAR to the Databricks jars directory:

```
#!/bin/bash
cp /Workspace/Users/me/indextables_x86_64_0.0.1.jar /databricks/jars
```

3. **Configure startup script**: Add the script to your cluster's startup configuration
4. **Set Spark properties**: Add these configurations to your cluster settings:

```
spark.executor.memory=27016m  # Example for r6id.2xlarge: 50% of default memory
spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions
```

5. **Upgrade Java (Databricks 15.4)**: Set environment variable `JNAME=zulu17-ca-amd64` to use Java 17

6. **Configure Unity Catalog credentials (optional)**: If using Unity Catalog External Locations to access S3 data, configure the Unity Catalog credential provider: *NOTE THAT THIS HAS NOT BEEN VALIDATED YET, PLEASE LET ME KNOW IF IT WORKS FOR YOU*

```scala
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
  "io.indextables.spark.auth.unity.UnityCredentialProvider")
```

*Note*: Photon does not directly accelerate indextables, so it is not necessary to enable it.

---

## Common Use Cases

### 📊 Log Analysis and Observability
```sql
-- Search application logs for errors and exceptions
SELECT timestamp, service, message, stack_trace
FROM logs
WHERE level = 'ERROR'
  AND timestamp > current_timestamp() - INTERVAL 1 HOUR
  AND message indexquery 'OutOfMemory OR StackOverflow OR NullPointerException';

-- Aggregate errors by service and time
SELECT date, hour, minute,
       service,
       COUNT(*) as error_count
FROM logs
WHERE level = 'ERROR'
  AND message indexquery 'exception OR failed OR timeout'
GROUP BY date, hour, minute, service
ORDER BY date DESC, hour DESC, minute DESC;
```

### 🔐 Security Investigation and SIEM
```sql
-- Find all login attempts from suspicious IPs in the last 24 hours
SELECT timestamp, user, ip_address, event_type, outcome
FROM security_logs
WHERE event_type IN ('login', 'auth_attempt', 'access')
  AND timestamp > current_timestamp() - INTERVAL 24 HOURS
  AND (ip_address IN ('192.168.1.100', '10.0.0.50')  -- suspicious IPs
       OR _indexall indexquery 'failed OR unauthorized OR denied');

-- Identify potential brute force attempts
SELECT ip_address,
       date, hour, minute,
       COUNT(*) as attempts
FROM security_logs
WHERE event_type = 'login'
  AND outcome = 'failed'
  AND event_date BETWEEN '2025-01-01' and '2025-02-01'
GROUP BY ip_address, date, hour, minute
```

### 📈 Application Performance Monitoring (APM)
```sql
-- Find slow API calls with specific error patterns
SELECT timestamp, endpoint, response_time, status_code, trace_id
FROM api_logs
WHERE response_time > 1000  -- Response time > 1 second
  AND endpoint indexquery 'GET OR POST'
  AND (response_body indexquery 'timeout' OR status_code >= 500)
  AND event_date BETWEEN '2025-01-01' and '2025-02-01'
ORDER BY response_time DESC
LIMIT 50;

-- Analyze error patterns in microservices
SELECT service_name,
       endpoint,
       COUNT(*) as error_count,
       AVG(response_time) as avg_response_time
FROM api_logs
WHERE trace_id IS NOT NULL
  AND _indexall indexquery 'error OR exception OR failed'
GROUP BY service_name, endpoint
ORDER BY error_count DESC;
```

### 🔍 Full-Text Search in Documents
```sql
-- Search knowledge base for relevant documents
SELECT title, author, published_date, abstract
FROM documents
WHERE content indexquery 'machine learning AND (tensorflow OR pytorch)'
  AND published_date >= '2023-01-01';

-- Find documents with complex boolean queries
SELECT title, tags, last_updated
FROM documents
WHERE content indexquery '
  (artificial intelligence OR machine learning) AND
  (python OR scala) AND
  NOT deprecated AND
  "neural network"
';
```

### 📊 Business Intelligence and Analytics
```sql
-- Search customer feedback for specific issues
SELECT product_category,
       issue_type,
       COUNT(*) as issue_count,
       AVG(rating) as avg_rating
FROM feedback
WHERE _indexall indexquery 'refund OR complaint OR dissatisfied'
  AND rating <= 3
GROUP BY product_category, issue_type
ORDER BY issue_count DESC;

-- Analyze support tickets with natural language queries
SELECT ticket_id, customer_id, created_at, description
FROM support_tickets
WHERE status = 'open'
  AND description indexquery 'billing problem OR payment failed'
  AND priority = 'high';
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
  .save("s3://bucket/indextable-data")

// Step 4: Optimize the splits for better performance
spark.sql("MERGE SPLITS 's3://bucket/indextable-data' TARGET SIZE 4G")
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
  spark.sql(s"MERGE SPLITS 's3://bucket/indextable-data' TARGET SIZE 4G")
}
```

---

## Best Practices

### ✅ DO's

#### 🎯 **DO: Optimize Split Sizes**
```scala
// Target 1-4GB splits for optimal performance
spark.sql("MERGE SPLITS 's3://bucket/table' TARGET SIZE 4G")

// For time-series data, merge by partition
spark.sql("""
  MERGE SPLITS 's3://bucket/table'
  WHERE date = '2024-01-01'
  TARGET SIZE 4G
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

#### 🚫 **DON'T: Forget to Merge Splits**
```scala
// BAD: Writing data without optimization
df.write.format("io.indextables.provider.IndexTablesProvider").save(path)
// Forgetting to merge...

// GOOD: Always merge after large ingestions
df.write.format("io.indextables.provider.IndexTablesProvider").save(path)
spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 4G")
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
| `spark.indextables.cache.name` | `"indextables-cache"` | Name of the JVM-wide split cache |
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
| `spark.indextables.merge.batchSize` | `defaultParallelism` | Number of merge groups per batch (defaults to Spark's defaultParallelism) |
| `spark.indextables.merge.maxConcurrentBatches` | `2` | Maximum number of batches to process concurrently |
| `spark.indextables.aws.accessKey` | - | AWS access key for S3 split access |
| `spark.indextables.aws.secretKey` | - | AWS secret key for S3 split access |
| `spark.indextables.aws.sessionToken` | - | AWS session token for temporary credentials (STS) |
| `spark.indextables.aws.region` | - | AWS region for S3 split access |
| `spark.indextables.aws.endpoint` | - | Custom AWS S3 endpoint |
| `spark.indextables.aws.credentialsProviderClass` | - | Fully qualified class name of custom AWS credential provider |
| `spark.indextables.s3.endpoint` | - | S3 endpoint URL (alternative to aws.endpoint) |
| `spark.indextables.s3.pathStyleAccess` | `false` | Use path-style access for S3 (required for some S3-compatible services) |

#### Field Indexing Configuration

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.indextables.indexing.typemap.<field_name>` | `string` | Field indexing type: `string`, `text`, or `json` |
| `spark.indextables.indexing.fastfields` | - | Comma-separated list of fields for fast access |
| `spark.indextables.indexing.storeonlyfields` | - | Fields stored but not indexed |
| `spark.indextables.indexing.indexonlyfields` | - | Fields indexed but not stored |
| `spark.indextables.indexing.tokenizer.<field_name>` | - | Tokenizer type: `default`, `whitespace`, or `raw` |

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
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexWriter.heapSize", "150000000") // 150MB heap
  .option("spark.indextables.indexWriter.threads", "3") // 3 indexing threads
  .option("spark.indextables.indexWriter.batchSize", "15000") // 15,000 documents per batch
  .save("s3://bucket/path")

// Disable batch writing for debugging (use individual document indexing)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexWriter.useBatch", "false")
  .save("s3://bucket/path")

// High-throughput configuration for large datasets
spark.conf.set("spark.indextables.indexWriter.heapSize", "500000000") // 500MB heap
spark.conf.set("spark.indextables.indexWriter.threads", "8") // 8 indexing threads
spark.conf.set("spark.indextables.indexWriter.batchSize", "50000") // 50,000 documents per batch
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/large-dataset")
```

#### AWS Configuration

Configure AWS credentials for S3 operations:

```scala
// Recommended: Use custom credential provider (e.g., Unity Catalog)
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
  "io.indextables.spark.auth.unity.UnityCredentialProvider")

df.write.format("io.indextables.provider.IndexTablesProvider").save("s3://bucket/path")

// Alternative: Explicit AWS credentials
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

// Pass credentials via write options (automatically propagated to executors)
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

IndexTables4Spark supports powerful query operators for native Tantivy query syntax with full filter pushdown:

- **IndexQuery**: Field-specific search with column specification
- **IndexQueryAll**: All-fields search using virtual `_indexall` column

##### SQL Usage

```sql
-- Register IndexTables4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Create table/view from IndexTables4Spark data
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
import io.indextables.spark.expressions.IndexQueryExpression
import io.indextables.spark.util.ExpressionUtils
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

IndexTables4Spark supports searching across all fields using the virtual `_indexall` column with the `indexquery` operator:

##### SQL Usage

```sql
-- Register IndexTables4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

-- Create table/view from IndexTables4Spark data
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
import io.indextables.spark.expressions.IndexQueryExpression
import io.indextables.spark.util.ExpressionUtils
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

IndexTables4Spark provides SQL-based split consolidation to reduce small file overhead and optimize query performance:

##### SQL Syntax

```sql
-- Register IndexTables4Spark extensions for SQL parsing
spark.sparkSession.extensions.add("io.indextables.spark.extensions.IndexTables4SparkExtensions")

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
├── io/indextables/spark/
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

src/main/java/io/indextables/spark/
└── auth/
    └── unity/          # Unity Catalog credential provider

src/main/antlr4/        # ANTLR grammar definitions
└── io/indextables/spark/sql/parser/
    └── IndexTables4SparkSqlBase.g4  # SQL parser grammar for custom commands

src/test/scala/         # Comprehensive test suite (205+ tests passing)
├── io/indextables/spark/
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

**Cache Locality Tracking**: IndexTables automatically tracks which executors have cached splits and uses Spark's `preferredLocations` API to schedule tasks on those hosts. This optimization provides:
- Faster query execution through local cache hits
- Reduced network bandwidth usage
- Better cluster resource utilization

## Roadmap

See [BACKLOG.md](BACKLOG.md) for detailed development roadmap including:

### Planned Features
- **Table hygiene**: Capability similar to Delta "VACUUM" command
- **Transaction log hygiene**: Better testing for purging of old log segments
- **Transaction log storage efficiencye**: Consider use of parquet (like delta) of avro (like iceberg) with checkpoints
- **Transaction log multi-process concurrency**: Tolearate multiple writer processes, especially when checkpointing
- **Multi-cloud Enhancements**: Expanded Azure and GCP support
- **Catalog support**: Support for Hive catalogs
- **Schema migration**: Support for updating schemas and indexing schemes
- **Re-indexing support**: Support for changing indexing types of fields from plain strings to full-text search
- **Index creation syntax**: SQL DDL commands for creating and managing indexes (e.g., `CREATE INDEX`, `DROP INDEX`, `ALTER INDEX`) with declarative field type and tokenizer configuration
- **Advanced optimized writes**: Enhanced write-time optimizations including intelligent bucketing, adaptive compression, and dynamic split sizing based on workload patterns
- **Auto-merge capabilities**: Automatic background merge operations triggered by configurable policies (split count, size thresholds, time-based schedules)
- **Auto-purging**: Automatic cleanup of old splits, transaction logs, and checkpoints based on retention policies and time-to-live (TTL) settings
- **Prewarming enhancements**: Better support for pre-warming caches on new clusters
- **Memory auto-tuning**: Better support for automatically tuning native heaps for indexing, merging, and queries
- **Enhanced windowing functions**: Improved support for time-based windowing and tumbling window aggregations
- **VARIANT Data types**: Support for JSON fields
- **Arrays and embedded structures**: Support for complex column types
- **S3 Mock Test Improvementss**: Remove requirement for "real" S3 access for many test cases
- **Test independence**:  "mvn test" can't run without large available memory (using run_tests_individually.sh method)
- **Legacy cleanup**: Removal of unused legacy V1 datasource code
- **Redundant code refactor**: Clean up duplicative code (from AI code generation)
- **Legacy naming cleanup**: Removing old references to "tantivy4spark" (old name of indextables for spark)


## Known Issues and Solutions
- TBD

## ❓ Frequently Asked Questions (FAQ)

### General Questions

**Q: What's the difference between IndexTables4Spark and traditional Spark DataSources like Parquet?**
A: IndexTables4Spark is optimized for full-text search and analytical queries with features like IndexQuery operators, aggregate pushdown, and native Tantivy search. Parquet excels at columnar analytics but lacks built-in search capabilities.

**Q: Can I use IndexTables4Spark alongside Delta Lake or Parquet?**
A: Yes! IndexTables4Spark can read from and write to any Spark-compatible data source. You can easily migrate data or use it in hybrid architectures.

**Q: What's the relationship between IndexTables4Spark and IndexTables?**
A: IndexTables is a vendor-neutral alias for IndexTables4Spark. Use `io.indextables.extensions.IndexTablesSparkExtensions` and `io.indextables.provider.IndexTablesProvider` for the same functionality with a generic namespace.

### Performance Questions

**Q: How does aggregate pushdown improve performance?**
A: Aggregate pushdown executes COUNT(), SUM(), AVG(), MIN(), MAX() directly in Tantivy instead of pulling all data through Spark. This provides 10-100x speedup for aggregation queries.

**Q: Why are my COUNT queries so fast?**
A: COUNT queries without filters use transaction log metadata optimization, which reads only metadata instead of accessing splits. This provides near-instant results.

**Q: How do I optimize upload performance for large datasets?**
A: Use parallel streaming uploads with `spark.indextables.s3.maxConcurrency=16` and configure fast storage like `/local_disk0` or NVMe SSDs for temporary directories.

**Q: What's the benefit of checkpoint compaction?**
A: Checkpoint compaction reduces transaction log read times by 60% (2.5x speedup) by consolidating transaction history into snapshot files, especially beneficial for tables with 50+ transactions.

### Configuration Questions

**Q: What's the difference between V1 and V2 DataSource APIs?**
A: V2 API (`io.indextables.spark.core.IndexTables4SparkTableProvider`) is recommended for new projects as it properly indexes partition columns. V1 API (`indextables`) is maintained for backward compatibility.

**Q: How do I configure auto-sizing?**
A: Enable auto-sizing with `spark.indextables.autoSize.enabled=true` and set `spark.indextables.autoSize.targetSplitSize=100M`. V1 API automatically counts DataFrames; V2 API requires explicit row count.

**Q: What's the difference between string and text field types?**
A: String fields use raw tokenization for exact matching (pushed to data source). Text fields use tokenization for full-text search with IndexQuery operators (best-effort filtering).

**Q: How do I use custom AWS credential providers?**
A: Set `spark.indextables.aws.credentialsProviderClass` to your provider class name. The provider must implement AWS SDK v1 or v2 credential interfaces with a specific constructor signature.

### Operational Questions

**Q: How does MERGE SPLITS work?**
A: MERGE SPLITS consolidates small split files into larger ones to reduce overhead. Use `MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M` to merge splits up to 100MB each.

**Q: What happens when a merge operation encounters corrupted files?**
A: Corrupted files are automatically skipped with cooldown tracking (default: 24 hours). Original files remain accessible and the operation continues gracefully without failing.

**Q: How do I invalidate cached splits across the cluster?**
A: Use `INVALIDATE TRANSACTION LOG CACHE 's3://bucket/path'` for table-level invalidation or `INVALIDATE TRANSACTION LOG CACHE` for global cache invalidation.

**Q: How long are transaction log files retained?**
A: Default retention is 30 days. Files are only deleted when they're older than retention period AND included in a checkpoint AND not actively being written.

### Migration Questions

**Q: How do I migrate from Parquet to IndexTables4Spark?**
A: Simply read from Parquet and write to IndexTables4Spark:
```scala
val df = spark.read.parquet("s3://bucket/parquet-data")
df.write.format("indextables")
  .option("spark.indextables.indexing.typemap.content", "text")
  .save("s3://bucket/tantivy-data")
```

**Q: Can I do incremental migration?**
A: Yes! Use a hybrid query approach where you union results from both old (Parquet) and new (IndexTables4Spark) data sources during migration.

**Q: How do I handle schema evolution?**
A: Currently schema migration is planned but not implemented. For now, create a new table with the updated schema and migrate data.

### Troubleshooting Questions

**Q: Why am I getting UnsupportedOperationException for Arrays/Maps/Structs?**
A: Complex types (Arrays, Maps, Structs) are not yet supported. Flatten these into primitive fields or use JSON serialization for the json field type.

**Q: Why are my exact match filters on text fields not working?**
A: Text fields are tokenized, so exact matching requires Spark post-processing. Use string field type for exact matching with full filter pushdown support.

**Q: Why is my upload failing with OutOfMemoryError?**
A: Large files (4GB+) require streaming upload. Ensure `spark.indextables.s3.streamingThreshold` is set appropriately (default: 100MB) and reduce batch sizes if needed.

**Q: How do I debug cache locality issues?**
A: Enable debug logging and look for `[DRIVER]` and `[EXECUTOR]` prefixed messages showing broadcast locality updates and preferred location assignments.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

- GitHub Issues: Report bugs and request features
- Documentation: Comprehensive test suite with 179 tests demonstrating usage patterns
- Community: Check the test files in `src/test/scala/` for detailed usage examples
- SQL Pushdown: See `SqlPushdownTest.scala` for detailed examples of predicate and limit pushdown verification

## 🙏 Acknowledgments

### Built on Open Source

IndexTables stands on the shoulders of these exceptional open source projects:

- **[Apache Spark](https://github.com/apache/spark)** - The foundation that powers distributed data processing and analytics
- **[Delta Lake](https://github.com/delta-io/delta)** - Inspiration for the transaction log architecture and ACID semantics
- **[Tantivy](https://github.com/quickwit-oss/tantivy)** - The high-performance full-text search engine at the core of IndexTables
- **[Quickwit](https://github.com/quickwit-oss/quickwit)** - Source of the split file format and remote search patterns
- **[Tantivy4Java](https://github.com/indextables/tantivy4java)** - Java wrapper around Tantivy and Quickwit

### Development Assistance

This project was developed with coding assistance from **Anthropic Claude**, an AI assistant that helped with implementation, testing, documentation, and architectural design decisions throughout the development process.
