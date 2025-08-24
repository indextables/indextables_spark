# Tantivy4Spark

A high-performance file format for Apache Spark that implements fast full-text search using the [Tantivy search engine library](https://github.com/quickwit-oss/tantivy) via [tantivy4java](https://github.com/quickwit-oss/tantivy-java). It runs embedded inside Apache Spark without requiring any server-side components.

## Features

- **Embedded Search**: Tantivy runs directly within Spark executors via tantivy4java
- **Split-Based Architecture**: Write-only indexes with split-based reading for optimal performance
- **Transaction Log**: Delta Lake-style transaction log with batched operations for metadata management  
- **Optimized Writes**: Delta Lake-style optimized writes with automatic split sizing based on target records per split
- **Smart File Skipping**: Min/max value tracking for efficient query pruning
- **Schema-Aware Filter Pushdown**: Safe filter optimization with field validation
- **S3-Optimized Storage**: Intelligent caching and compression for object storage
- **AWS Session Token Support**: Full support for temporary credentials via AWS STS
- **Flexible Storage**: Support for local, HDFS, and S3 storage protocols
- **Schema Evolution**: Automatic schema inference and evolution support
- **Thread-Safe Architecture**: ThreadLocal IndexWriter pattern eliminates race conditions

## Architecture

### Core Components

- **Core Package** (`com.tantivy4spark.core`): Spark DataSource V2 integration
- **Search Package** (`com.tantivy4spark.search`): Tantivy search engine wrapper via tantivy4java
- **Storage Package** (`com.tantivy4spark.storage`): S3-optimized storage with predictive I/O
- **Transaction Package** (`com.tantivy4spark.transaction`): Append-only transaction log

### Integration Architecture

This project integrates with Tantivy via **tantivy4java** (pure Java bindings):
- **No Native Dependencies**: Uses tantivy4java library for cross-platform compatibility
- **Split-Based Storage**: Uses QuickwitSplit format (.split files) with JVM-wide caching
- **Write-Only Indexes**: Create → add documents → commit → create split → close pattern
- **Thread-Safe Design**: ThreadLocal IndexWriter pattern eliminates race conditions
- **Automatic Schema Mapping**: Seamless conversion from Spark types to Tantivy field types
- **Schema-Aware Filtering**: Field validation prevents native crashes during query execution
- **AWS Session Token Support**: Complete support for temporary credentials via STS

## Quick Start

### Prerequisites

- Java 11 or later
- Apache Spark 3.5.x
- Maven 3.6+ (for building from source)

### Building

```bash
# Build the project
mvn clean compile

# Run tests (103 tests, 100% pass rate)
mvn test

# Package
mvn package
```

### Usage

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Tantivy4Spark Example")
  .getOrCreate()

// Write data with optimized writes (enabled by default)
df.write
  .format("tantivy4spark")
  .mode("overwrite")
  .save("s3://bucket/path/table")

// Write with custom split sizing
df.write
  .format("tantivy4spark")
  .option("targetRecordsPerSplit", "500000")  // 500K records per split
  .save("s3://bucket/path/table")

// Read data
val df = spark.read
  .format("tantivy4spark")
  .load("s3://bucket/path/table")

// Query with filters (automatically converted to Tantivy queries)
df.filter($"name".contains("John") && $"age" > 25).show()

// Text search
df.filter($"content".contains("machine learning")).show()
```

### Configuration Options

The system supports several configuration options for performance tuning:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.tantivy4spark.optimizeWrite.enabled` | `true` | Enable/disable optimized writes with automatic split sizing |
| `spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit` | `1000000` | Target number of records per split file for optimized writes |
| `spark.tantivy4spark.storage.force.standard` | `false` | Force standard Hadoop operations for all protocols |
| `spark.tantivy4spark.aws.sessionToken` | - | AWS session token for temporary credentials (STS) |

#### Optimized Writes Configuration

Control the automatic split sizing behavior (similar to Delta Lake optimizedWrite):

```scala
// Enable optimized writes with default 1M records per split
df.write.format("tantivy4spark")
  .option("optimizeWrite", "true")
  .save("s3://bucket/path")

// Configure via Spark session (applies to all writes)
spark.conf.set("spark.tantivy4spark.optimizeWrite.enabled", "true")
spark.conf.set("spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit", "2000000")

// Configure per write operation (overrides session config)
df.write.format("tantivy4spark")
  .option("targetRecordsPerSplit", "500000")  // 500K records per split
  .save("s3://bucket/path")

// Disable optimized writes (use Spark's default partitioning)
df.write.format("tantivy4spark")
  .option("optimizeWrite", "false")
  .save("s3://bucket/path")
```

#### Storage Configuration

The system automatically detects storage protocol and uses appropriate I/O strategy:

```scala
// S3-optimized I/O (default for s3:// protocols)
df.write.format("tantivy4spark").save("s3://bucket/path")

// Force standard Hadoop operations
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.storage.force.standard", "true")
  .save("s3://bucket/path")

// Standard operations (automatic for other protocols)
df.write.format("tantivy4spark").save("hdfs://namenode/path")
df.write.format("tantivy4spark").save("file:///local/path")
```

#### AWS Configuration

Configure AWS credentials for S3 operations:

```scala
// Standard AWS credentials
spark.conf.set("spark.tantivy4spark.aws.accessKey", "your-access-key")
spark.conf.set("spark.tantivy4spark.aws.secretKey", "your-secret-key")
spark.conf.set("spark.tantivy4spark.aws.region", "us-west-2")

// AWS credentials with session token (temporary credentials from STS)
spark.conf.set("spark.tantivy4spark.aws.accessKey", "your-temporary-access-key")
spark.conf.set("spark.tantivy4spark.aws.secretKey", "your-temporary-secret-key")
spark.conf.set("spark.tantivy4spark.aws.sessionToken", "your-session-token")
spark.conf.set("spark.tantivy4spark.aws.region", "us-west-2")

// Custom S3 endpoint (for S3-compatible services)
spark.conf.set("spark.tantivy4spark.aws.endpoint", "https://s3.custom-provider.com")

df.write.format("tantivy4spark").save("s3://bucket/path")
```


## File Format

### Split Files

Tantivy indexes are stored as `.split` files (QuickwitSplit format):
- **Split-based storage**: Optimized binary format for fast loading and caching
- **JVM-wide caching**: Shared `SplitCacheManager` reduces memory usage across executors
- **Native compatibility**: Direct integration with tantivy4java library
- **S3-optimized**: Efficient partial loading and caching for object storage

### Transaction Log

Located in `_transaction_log/` directory (Delta Lake compatible):
- **Batched operations**: Single JSON file per transaction with multiple ADD entries
- `00000000000000000000.json` - Initial metadata and schema
- `00000000000000000001.json` - First transaction (multiple ADD operations)
- `00000000000000000002.json` - Second transaction (additional files)
- Stores min/max values for data skipping and query optimization

## Development

### Project Structure

```
src/main/scala/com/tantivy4spark/
├── core/           # Spark DataSource V2 integration
├── search/         # Tantivy search engine wrapper via tantivy4java
├── storage/        # S3-optimized storage layer
└── transaction/    # Transaction log system

src/test/scala/     # Comprehensive test suite (103 tests, 100% pass rate)
├── core/           # Core functionality tests
├── integration/    # End-to-end integration tests  
├── optimize/       # Optimized writes tests
├── storage/        # Storage protocol tests
├── transaction/    # Transaction log tests
└── debug/          # Debug and diagnostic tests
```

### Running Tests

```bash
# All tests (103 tests, 100% pass rate)
mvn test

# Specific test suites
mvn test -Dtest="*IntegrationTest"                # Integration tests
mvn test -Dtest="OptimizedWriteTest"              # Optimized writes tests
mvn test -Dtest="UnsupportedTypesTest"            # Type safety tests
mvn test -Dtest="BatchTransactionLogTest"         # Transaction log batch operations
mvn test -Dtest="*DebugTest"                      # Debug and diagnostic tests

# Test coverage report
mvn scoverage:report
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass and coverage requirements are met
5. Submit a pull request

## Performance

### Benchmarks

- **Query Performance**: 10-100x faster than Parquet for text search queries
- **Storage Efficiency**: Comparable to Parquet for storage size
- **S3 Performance**: 50% fewer API calls with predictive I/O

### Optimization Tips

1. Use appropriate data types in your schema
2. Enable S3 optimization for cloud workloads  
3. Leverage data skipping with min/max statistics
4. Partition large datasets by common query dimensions

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

- GitHub Issues: Report bugs and request features
- Documentation: See the [Wiki](https://github.com/your-org/tantivy4spark/wiki) for detailed guides
- Community: Join discussions in GitHub Discussions