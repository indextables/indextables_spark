# IndexTables for Spark

IndexTables is an open-table format for Apache Spark that enables fast retrieval and full-text search across large-scale data. It integrates seamlessly with Spark SQL, combining powerful search capabilities with joins, aggregations, and standard SQL operations. Originally built for log observability and cybersecurity investigations, IndexTables works well for any use case requiring fast data retrieval.

IndexTables runs entirely within your existing Spark cluster with no additional infrastructure. It stores data in object storage (AWS S3 and Azure Blob Storage) and has been verified on OSS Spark 3.5.2 and Databricks 15.4 LTS.

> **Documentation**: [https://www.indextables.io](https://www.indextables.io)

> **Development Status**: IndexTables is under active development. APIs and features may change. We recommend testing in non-production environments before deploying to production workloads.

## Key Features

- **Embedded Search** - Runs directly within Spark executors, no additional infrastructure
- **Multi-Cloud Storage** - AWS S3 and Azure Blob Storage fully supported
- **Full-Text Search** - Native `indexquery` operator with complete Tantivy search syntax
- **Predicate Pushdown** - WHERE clause filters convert to native search operations
- **Aggregate Pushdown** - COUNT, SUM, AVG, MIN, MAX execute directly in the search engine
- **Bucket Aggregations** - DateHistogram, Histogram, and Range for time-series analysis
- **JSON Field Support** - Native Struct, Array, and Map fields with filter pushdown
- **Smart File Skipping** - Delta/Iceberg-style transaction log with min/max statistics
- **Batch Optimization** - 90-95% reduction in S3 GET requests (enabled by default)
- **L2 Disk Cache** - Persistent NVMe caching with LZ4/ZSTD compression (auto-enabled on Databricks/EMR)
- **Cache Prewarming** - SQL command to eliminate cold-start latency

## Quick Start

### Installation

1. Add the [IndexTables JAR](https://repo1.maven.org/maven2/io/indextables/indextables_spark/0.4.5_spark_3.5.3/) to your Spark classpath
2. Set `spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions`
3. Requires Java 11+

See the [Installation Guide](https://www.indextables.io/docs/getting-started/installation) for detailed instructions including Databricks setup.

### Basic Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IndexTables Example").getOrCreate()

# Write data (declare "message" as full-text searchable)
df.write \
    .format("io.indextables.provider.IndexTablesProvider") \
    .mode("append") \
    .option("spark.indextables.indexing.typemap.message", "text") \
    .save("s3://bucket/path/table")

# Merge index segments for optimal performance
spark.sql("MERGE SPLITS 's3://bucket/path/table' TARGET SIZE 4G")

# Read and query
df = spark.read \
    .format("io.indextables.provider.IndexTablesProvider") \
    .load("s3://bucket/path/table")

df.createOrReplaceTempView("my_table")

# Full-text search with SQL
spark.sql("""
    SELECT * FROM my_table
    WHERE category = 'technology'
      AND message indexquery 'critical AND infrastructure'
    LIMIT 100
""").show()

# Cross-field search
spark.sql("SELECT * FROM my_table WHERE _indexall indexquery 'error'").show()
```

## Documentation

| Topic | Description |
|-------|-------------|
| [Getting Started](https://www.indextables.io/docs/getting-started/installation) | Installation, quickstart, first index |
| [Core Concepts](https://www.indextables.io/docs/core-concepts/split-architecture) | Split architecture, transaction log, field types |
| [Configuration](https://www.indextables.io/docs/configuration/writer-settings) | Writer, reader, cache, and cloud settings |
| [Query Guide](https://www.indextables.io/docs/query-guide/filter-pushdown) | Filter pushdown, aggregations, full-text search |
| [SQL Commands](https://www.indextables.io/docs/sql-commands/merge-splits) | MERGE SPLITS, PURGE, PREWARM CACHE, and more |
| [Cloud Deployment](https://www.indextables.io/docs/cloud-deployment/databricks) | Databricks, AWS EMR deployment guides |
| [Configuration Reference](https://www.indextables.io/docs/reference/configuration-reference) | Complete configuration options |

## Development

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # Java 11 required
mvn clean compile                              # Build
mvn test                                       # Run tests

# Run single test
mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.YourTest'
```

Under the hood, IndexTables uses [Tantivy](https://github.com/quickwit-oss/tantivy) and [Quickwit splits](https://github.com/quickwit-oss/quickwit) instead of Parquet.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues & Features**: [GitHub Issues](https://github.com/indextables/indextables/issues)
- **Contact**: [Scott Schenkein](https://www.linkedin.com/in/schenksj/) (maintainer)
- **Documentation**: [https://www.indextables.io](https://www.indextables.io)
