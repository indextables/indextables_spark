# Section 15: Usage Examples (Outline)

## 15.1 Overview

This section provides comprehensive usage examples for IndexTables4Spark covering common scenarios, best practices, and real-world use cases. Examples are organized by workflow pattern and include both DataFrame API and SQL syntax.

## 15.2 Getting Started

### 15.2.1 Installation and Setup
```scala
// Add dependency to pom.xml or build.sbt
libraryDependencies += "io.indextables" %% "indextables-spark" % "1.13.0"

// Configure Spark session
val spark = SparkSession.builder()
  .appName("IndexTables4Spark Example")
  .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
  .config("spark.indextables.aws.accessKey", "AKIA...")
  .config("spark.indextables.aws.secretKey", "...")
  .getOrCreate()
```

### 15.2.2 Basic Write Example
```scala
// Create DataFrame
val data = Seq(
  ("doc1", "Apache Spark is a unified analytics engine", 100),
  ("doc2", "Tantivy is a full-text search engine", 95),
  ("doc3", "Delta Lake provides ACID transactions", 88)
).toDF("id", "content", "score")

// Write to IndexTables4Spark
data.write.format("indextables")
  .option("spark.indextables.indexing.typemap.content", "text")
  .option("spark.indextables.indexing.fastfields", "score")
  .save("s3://my-bucket/documents")
```

### 15.2.3 Basic Read Example
```scala
// Read data
val df = spark.read.format("indextables")
  .load("s3://my-bucket/documents")

// Basic query
df.show()

// Full-text search
df.filter($"content" indexquery "spark AND analytics").show()

// Aggregate query
df.agg(count("*"), avg("score")).show()
```

## 15.3 Log Analysis Use Case

### 15.3.1 Scenario Description
- **Data**: Application logs with timestamp, level, message, user_id
- **Volume**: 100M events/day, 3TB total
- **Partitioning**: By date and hour
- **Query pattern**: Search for errors, filter by time range, aggregate by user

### 15.3.2 Schema and Write
```scala
case class LogEntry(
  timestamp: java.sql.Timestamp,
  level: String,
  message: String,
  user_id: String,
  request_id: String,
  response_time_ms: Long
)

val logs = spark.read.json("s3://logs-raw/2024-01-01/*.json")
  .as[LogEntry]

logs.write.format("indextables")
  .partitionBy("date", "hour")
  .option("spark.indextables.indexing.typemap.message", "text")
  .option("spark.indextables.indexing.fastfields", "response_time_ms")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "200M")
  .save("s3://logs-indexed/app-logs")
```

### 15.3.3 Query Examples
```scala
val logs = spark.read.format("indextables")
  .load("s3://logs-indexed/app-logs")

// Find all errors in specific time range
logs.filter($"date" === "2024-01-01" && $"hour" >= 10 && $"hour" <= 12)
  .filter($"message" indexquery "error OR exception")
  .select("timestamp", "level", "message", "user_id")
  .show()

// Aggregate errors by user
logs.filter($"date" === "2024-01-01")
  .filter($"level" === "ERROR")
  .groupBy("user_id")
  .agg(count("*").as("error_count"))
  .orderBy($"error_count".desc)
  .show()

// Average response time for specific operation
logs.filter($"date" >= "2024-01-01" && $"date" <= "2024-01-31")
  .filter($"message" indexquery "checkout AND completed")
  .agg(avg("response_time_ms"))
  .show()
```

### 15.3.4 Maintenance
```sql
-- Optimize splits monthly
MERGE SPLITS 's3://logs-indexed/app-logs'
WHERE date >= '2024-01-01' AND date < '2024-02-01'
TARGET SIZE 500M
MAX GROUPS 20;
```

## 15.4 E-Commerce Search Use Case

### 15.4.1 Scenario Description
- **Data**: Product catalog with title, description, category, price
- **Volume**: 10M products, 50GB total
- **Query pattern**: Full-text search, faceted search, price range filters
- **Update pattern**: Incremental updates daily

### 15.4.2 Schema and Initial Load
```scala
case class Product(
  product_id: String,
  title: String,
  description: String,
  category: String,
  brand: String,
  price: Double,
  rating: Double,
  review_count: Long
)

val products = spark.read.parquet("s3://raw-data/products/*.parquet")
  .as[Product]

products.write.format("indextables")
  .partitionBy("category")
  .option("spark.indextables.indexing.typemap.title", "string")
  .option("spark.indextables.indexing.typemap.description", "text")
  .option("spark.indextables.indexing.fastfields", "price,rating,review_count")
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .save("s3://catalog/products")
```

### 15.3.3 Search Queries
```scala
val catalog = spark.read.format("indextables")
  .load("s3://catalog/products")

// Product search with filters
catalog.filter($"category" === "Electronics")
  .filter($"description" indexquery "laptop AND (gaming OR professional)")
  .filter($"price" >= 800 && $"price" <= 2000)
  .select("product_id", "title", "price", "rating")
  .orderBy($"rating".desc)
  .show()

// Faceted aggregations
catalog.filter($"_indexall" indexquery "wireless headphones")
  .groupBy("brand")
  .agg(
    count("*").as("product_count"),
    avg("price").as("avg_price"),
    avg("rating").as("avg_rating")
  )
  .orderBy($"product_count".desc)
  .show()

// Top products by review count
catalog.filter($"category" === "Home & Kitchen")
  .filter($"rating" >= 4.0)
  .orderBy($"review_count".desc)
  .limit(50)
  .show()
```

### 15.4.4 Incremental Updates
```scala
// Daily product updates
val updates = spark.read.parquet("s3://raw-data/products/2024-01-02/*.parquet")
  .as[Product]

updates.write.format("indextables")
  .mode("append")
  .partitionBy("category")
  .option("spark.indextables.indexing.typemap.title", "string")
  .option("spark.indextables.indexing.typemap.description", "text")
  .save("s3://catalog/products")
```

## 15.5 Document Management Use Case

### 15.5.1 Scenario Description
- **Data**: PDFs, Word docs converted to text
- **Volume**: 1M documents, 500GB total
- **Query pattern**: Keyword search, author filter, date range
- **Compliance**: 7-year retention requirement

### 15.5.2 Document Ingestion
```scala
case class Document(
  doc_id: String,
  title: String,
  author: String,
  created_date: java.sql.Date,
  content: String,
  doc_type: String,
  file_size_bytes: Long
)

val documents = spark.read.parquet("s3://raw-docs/extracted/*.parquet")
  .as[Document]

documents.write.format("indextables")
  .partitionBy("year", "month")
  .option("spark.indextables.indexing.typemap.title", "string")
  .option("spark.indextables.indexing.typemap.content", "text")
  .option("spark.indextables.indexing.typemap.author", "string")
  .option("spark.indextables.indexing.fastfields", "file_size_bytes")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "100M")
  .save("s3://document-store/archive")
```

### 15.5.3 Search and Retrieval
```scala
val docs = spark.read.format("indextables")
  .load("s3://document-store/archive")

// Full-text search with metadata filters
docs.filter($"year" === "2024" && $"month" === "01")
  .filter($"content" indexquery "contract AND (amendment OR addendum)")
  .filter($"author" === "Legal Department")
  .select("doc_id", "title", "created_date")
  .show()

// Large document identification
docs.filter($"file_size_bytes" > 100 * 1024 * 1024)  // > 100MB
  .select("doc_id", "title", "file_size_bytes")
  .orderBy($"file_size_bytes".desc)
  .show()

// Document count by type and year
docs.groupBy("year", "doc_type")
  .agg(count("*").as("document_count"))
  .orderBy("year", "doc_type")
  .show()
```

## 15.6 Time Series Monitoring Use Case

### 15.6.1 Scenario Description
- **Data**: Metrics from IoT devices (temperature, pressure, status)
- **Volume**: 1B events/day, 10TB total
- **Partitioning**: By device_type and hour
- **Query pattern**: Time range queries, anomaly detection

### 15.6.2 Metrics Ingestion
```scala
case class DeviceMetric(
  device_id: String,
  device_type: String,
  timestamp: java.sql.Timestamp,
  temperature: Double,
  pressure: Double,
  status: String,
  alert_message: Option[String]
)

val metrics = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "device-metrics")
  .load()
  .select(from_json($"value", schema).as("data"))
  .select("data.*")
  .as[DeviceMetric]

metrics.writeStream
  .format("indextables")
  .partitionBy("device_type", "hour")
  .option("spark.indextables.indexing.typemap.alert_message", "text")
  .option("spark.indextables.indexing.fastfields", "temperature,pressure")
  .option("checkpointLocation", "s3://checkpoints/metrics")
  .start("s3://metrics-store/devices")
```

### 15.6.3 Monitoring Queries
```scala
val metrics = spark.read.format("indextables")
  .load("s3://metrics-store/devices")

// Temperature anomalies
metrics.filter($"device_type" === "HVAC" && $"hour" >= "2024-01-01-10")
  .filter($"temperature" > 80.0)
  .groupBy("device_id")
  .agg(
    count("*").as("anomaly_count"),
    avg("temperature").as("avg_temp"),
    max("temperature").as("max_temp")
  )
  .orderBy($"anomaly_count".desc)
  .show()

// Alert analysis
metrics.filter($"hour" >= "2024-01-01-00" && $"hour" <= "2024-01-01-23")
  .filter($"alert_message" indexquery "critical OR failure")
  .select("device_id", "timestamp", "alert_message")
  .show()
```

## 15.7 Advanced Patterns

### 15.7.1 Multi-Table Joins with IndexQuery
```scala
val products = spark.read.format("indextables").load("s3://catalog/products")
val reviews = spark.read.format("indextables").load("s3://catalog/reviews")

// Join with full-text filter on both sides
products.filter($"description" indexquery "gaming laptop")
  .join(
    reviews.filter($"review_text" indexquery "excellent AND performance"),
    "product_id"
  )
  .select("product_id", "title", "review_text", "rating")
  .show()
```

### 15.7.2 Window Functions with IndexQuery
```scala
val logs = spark.read.format("indextables").load("s3://logs/app-logs")

// Rank errors by user
logs.filter($"message" indexquery "error OR exception")
  .withColumn("error_rank",
    row_number().over(Window.partitionBy("user_id").orderBy($"timestamp".desc))
  )
  .filter($"error_rank" <= 10)
  .show()
```

### 15.7.3 Complex Aggregations
```scala
val events = spark.read.format("indextables").load("s3://events/user-activity")

// Multi-dimensional aggregation with IndexQuery filter
events.filter($"activity_description" indexquery "checkout AND completed")
  .groupBy("date", "user_segment", "product_category")
  .agg(
    count("*").as("event_count"),
    sum("revenue").as("total_revenue"),
    countDistinct("user_id").as("unique_users"),
    avg("session_duration_sec").as("avg_session_duration")
  )
  .orderBy("date", "total_revenue")
  .show()
```

### 15.7.4 Conditional Writes
```scala
val data = spark.read.parquet("s3://raw-data/staging/*.parquet")

// Write different partitions to different tables based on quality
data.filter($"quality_score" >= 95)
  .write.format("indextables")
  .mode("append")
  .save("s3://gold-tier/high-quality")

data.filter($"quality_score" >= 70 && $"quality_score" < 95)
  .write.format("indextables")
  .mode("append")
  .save("s3://silver-tier/medium-quality")
```

## 15.8 Performance Tuning Examples

### 15.8.1 Optimizing for Bulk Load
```scala
spark.conf.set("spark.indextables.indexWriter.heapSize", "1G")
spark.conf.set("spark.indextables.indexWriter.batchSize", "100000")
spark.conf.set("spark.indextables.indexWriter.threads", "8")
spark.conf.set("spark.indextables.s3.maxConcurrency", "16")

largeDataset.write.format("indextables")
  .option("spark.indextables.indexWriter.tempDirectoryPath", "/local_disk0/temp")
  .option("spark.indextables.optimizeWrite.enabled", "true")
  .save("s3://bulk-load/large-dataset")
```

### 15.8.2 Optimizing for Query Performance
```scala
// Large cache for repeated queries
spark.conf.set("spark.indextables.cache.maxSize", "5000000000")  // 5GB
spark.conf.set("spark.indextables.cache.prewarm.enabled", "true")
spark.conf.set("spark.indextables.docBatch.maxSize", "5000")

val df = spark.read.format("indextables")
  .option("spark.indextables.cache.directoryPath", "/fast-nvme/cache")
  .load("s3://query-heavy/dataset")
```

### 15.8.3 Transaction Log Optimization
```scala
// High-frequency writes
spark.conf.set("spark.indextables.checkpoint.enabled", "true")
spark.conf.set("spark.indextables.checkpoint.interval", "5")
spark.conf.set("spark.indextables.checkpoint.parallelism", "12")
spark.conf.set("spark.indextables.logRetention.duration", "604800000")  // 7 days
```

## 15.9 Migration Examples

### 15.9.1 Parquet to IndexTables4Spark
```scala
// Read existing Parquet data
val parquetData = spark.read.parquet("s3://old-data/parquet/*.parquet")

// Write to IndexTables4Spark with partitioning
parquetData.write.format("indextables")
  .partitionBy("date")
  .option("spark.indextables.indexing.typemap.description", "text")
  .option("spark.indextables.autoSize.enabled", "true")
  .option("spark.indextables.autoSize.targetSplitSize", "200M")
  .save("s3://new-data/indextables")

// Optimize after migration
spark.sql("MERGE SPLITS 's3://new-data/indextables' TARGET SIZE 200M")
```

### 15.9.2 Elasticsearch to IndexTables4Spark
```scala
// Read from Elasticsearch
val esData = spark.read.format("org.elasticsearch.spark.sql")
  .option("es.nodes", "es-cluster:9200")
  .option("es.resource", "my-index")
  .load()

// Write to IndexTables4Spark
esData.write.format("indextables")
  .option("spark.indextables.indexing.typemap.content", "text")
  .option("spark.indextables.indexing.fastfields", "score,timestamp")
  .save("s3://migrated-data/from-es")
```

## 15.10 Error Handling Examples

### 15.10.1 Handling Missing Credentials
```scala
try {
  val df = spark.read.format("indextables").load("s3://bucket/path")
  df.show()
} catch {
  case e: IllegalStateException if e.getMessage.contains("AWS credentials") =>
    println("AWS credentials not configured. Set access key and secret key.")
    // Configure and retry
    spark.conf.set("spark.indextables.aws.accessKey", "AKIA...")
    spark.conf.set("spark.indextables.aws.secretKey", "...")
}
```

### 15.10.2 Handling Schema Mismatches
```scala
try {
  df.filter($"nonexistent_field" indexquery "query").show()
} catch {
  case e: AnalysisException =>
    println(s"Field not found in schema: ${e.getMessage}")
    // Adjust query to use existing fields
}
```

### 15.10.3 Retry Failed Operations
```scala
def writeWithRetry(df: DataFrame, path: String, maxRetries: Int = 3): Unit = {
  var attempt = 0
  var success = false

  while (attempt < maxRetries && !success) {
    try {
      df.write.format("indextables").save(path)
      success = true
    } catch {
      case e: Exception =>
        attempt += 1
        if (attempt >= maxRetries) throw e
        Thread.sleep(1000 * attempt)  // Exponential backoff
    }
  }
}
```

## 15.11 Best Practices Summary

### 15.11.1 Schema Design
- **Use string fields** for exact matching (IDs, categories)
- **Use text fields** for full-text search (descriptions, content)
- **Configure fast fields** for aggregation columns
- **Partition by date/time** for time-series data
- **Limit partition count** (< 1000 partitions recommended)

### 15.11.2 Write Optimization
- **Enable auto-sizing** for consistent split sizes
- **Use /local_disk0** on Databricks/EMR
- **Configure appropriate batch sizes** (10K-100K)
- **Increase concurrency** for large files (8-16 threads)
- **Run MERGE SPLITS** periodically after bulk loads

### 15.11.3 Query Optimization
- **Use IndexQuery** for full-text search (50-1000x faster)
- **Leverage partition pruning** (date filters first)
- **Configure large cache** for repeated queries (1-5GB)
- **Use aggregate pushdown** for COUNT/SUM/AVG
- **Avoid SELECT *** unless necessary

### 15.11.4 Maintenance
- **Regular MERGE SPLITS**: Weekly or monthly
- **Monitor cache hit rates**: Target 60-90%
- **Track transaction log size**: Enable checkpoints
- **Validate split sizes**: Target 100MB-1GB per split
- **Test configuration changes**: In development first

## 15.12 Summary

IndexTables4Spark usage examples demonstrate:

✅ **Getting started**: Installation, basic write/read operations
✅ **Real-world use cases**: Logs, e-commerce, documents, time-series
✅ **Advanced patterns**: Joins, windows, complex aggregations, conditional writes
✅ **Performance tuning**: Bulk load, query performance, transaction log optimization
✅ **Migration scenarios**: Parquet, Elasticsearch to IndexTables4Spark
✅ **Error handling**: Credentials, schema mismatches, retry logic
✅ **Best practices**: Schema design, write optimization, query optimization, maintenance

**Key Takeaways**:
- **Simple API**: DataFrame and SQL syntax familiar to Spark users
- **Powerful search**: IndexQuery provides 50-1000x speedup for full-text queries
- **Production-ready**: Handles large-scale data with partitioning and optimization
- **Flexible configuration**: Per-operation tuning for diverse workloads
- **Easy maintenance**: SQL commands for split optimization and cache management
