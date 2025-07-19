# Spark Tantivy Handler

A high-performance Apache Spark file handler that integrates embedded Tantivy search capabilities with S3-optimized storage and Delta-style transaction logging.

## Features

- **Embedded Tantivy Search**: Native Rust-based search engine via JNI bindings
- **S3-Optimized Storage**: Aggressive predictive I/O with intelligent caching
- **Transaction Logging**: Append-only Delta-style transaction management
- **Schema Evolution**: Automatic mapping between Spark and Tantivy schemas
- **Cross-Platform**: Native library support for Linux, macOS, and Windows

## Architecture

This project implements a custom Spark DataSource that uses Tantivy as an embedded search engine. Unlike traditional setups that require separate Tantivy servers, this approach embeds Tantivy directly into Spark executors via JNI bindings.

### Key Components

- **Core Integration**: `TantivyFileFormat` provides Spark DataSource V1 implementation
- **Native Bridge**: Rust JNI library wraps Tantivy functionality for Java/Scala
- **Storage Layer**: S3-optimized reader with predictive I/O and LRU caching  
- **Transaction Management**: Append-only logging modeled after Delta Lake
- **Configuration**: Automatic schema mapping and compatibility checking

## Quick Start

### Prerequisites

- Java 8+
- Scala 2.12
- Apache Spark 3.4+
- Rust toolchain (for building native components)
- Maven 3.6+

### Building

```bash
# Build the entire project including native components
mvn clean package

# Build only Rust JNI library
cd src/main/rust && cargo build --release
```

### Usage

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Tantivy Search Example")
  .getOrCreate()

// Write data with Tantivy indexing
df.write
  .format("tantivy")
  .option("index.id", "my_index")
  .option("tantivy.base.path", "s3://my-bucket/tantivy-data")
  .save("s3://my-bucket/data/my_index")

// Read with search capabilities
val searchResults = spark.read
  .format("tantivy")
  .option("query", "error AND status:500")
  .option("max.results", "1000")
  .load("s3://my-bucket/data/my_index")
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `index.id` | `spark_index` | Unique identifier for the Tantivy index |
| `tantivy.base.path` | `./tantivy-data` | Base path for Tantivy data storage |
| `max.results` | `1000` | Maximum search results to return |
| `batch.size` | `100` | Number of documents to buffer before flushing |
| `segment.size` | `67108864` | Target segment size in bytes (64MB) |
| `predictive.read.size` | `1048576` | Predictive read ahead size (1MB) |

## Schema Management

The system automatically maps Spark schemas to Tantivy field mappings:

- `StringType` → `text`
- `LongType` → `i64` 
- `DoubleType` → `f64`
- `BooleanType` → `bool`
- `TimestampType` → `datetime`

Custom field configurations can be specified via options:

```scala
.option("field.title.indexed", "true")
.option("field.content.stored", "true") 
.option("field.score.fast", "true")
```

## Transaction Logging

The system maintains Delta-style transaction logs for ACID guarantees:

- Append-only operation log
- Atomic commits with rollback support
- Conflict detection and resolution
- Metadata tracking for all operations

## S3 Optimization

Advanced S3 storage optimizations include:

- **Predictive I/O**: Automatic read-ahead based on access patterns
- **LRU Caching**: In-memory caching of frequently accessed segments
- **Concurrent Reads**: Parallel fetching with configurable concurrency
- **Metadata Embedding**: S3 location and offset information in documents

## Development

See [CLAUDE.md](CLAUDE.md) for detailed development setup and architecture information.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.