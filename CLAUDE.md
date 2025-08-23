# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tantivy4Spark** is a high-performance file format for Apache Spark that implements fast full-text search using the [Tantivy search engine library](https://github.com/quickwit-oss/tantivy) directly via JNI. It runs embedded inside Apache Spark without requiring any server-side components.

### Key Features
- **Embedded search**: Tantivy runs directly within Spark executors via JNI
- **Transaction log**: Delta-like transaction log for metadata management  
- **Smart file skipping**: Min/max value tracking for efficient query pruning
- **S3-optimized storage**: Intelligent caching and compression for object storage
- **Flexible storage**: Support for local, HDFS, and S3 storage protocols
- **Schema evolution**: Automatic schema inference and evolution support

## Architecture

### File Format
- **Split files**: `*.split` files contain Tantivy indexes using tantivy4java's optimized Quickwit split format
- **Split structure**: Self-contained immutable index files with native tantivy4java caching support
- **Transaction log**: `_transaction_log/` directory (similar to Delta's `_delta_log/`) contains:
  - Schema metadata
  - ADD entries for each split file created
  - Min/max values for query pruning
  - Partition key information and row counts

### Integration Design
The system implements a custom Spark DataSource V2 provider modeled after [Delta Lake's DeltaDataSource](https://github.com/delta-io/delta/blob/d16d591ae90d5e2024e3d2306b1018361bda8e80/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaDataSource.scala):

- **Custom relation provider**: Reads ADD entries instead of using default Spark file listing
- **Query pruning**: Skips indexes based on min/max values stored in transaction log  
- **File format integration**: Similar to [Delta's ParquetFileFormat](https://github.com/delta-io/delta/blob/51150999ee12f120d370b26b8273b6c293f39a43/spark/src/main/scala/org/apache/spark/sql/delta/DeltaParquetFileFormat.scala)

### Java Integration (via tantivy4java)
- **Pure Java bindings**: Uses tantivy4java library for Tantivy integration with Quickwit split support
- **No native compilation**: Eliminates Rust/Cargo build dependencies
- **Schema mapping**: Automatic conversion between Spark and Tantivy data types
- **Cross-platform**: Supports Linux, macOS, and Windows via tantivy4java native libraries
- **Split format**: Uses tantivy4java's Quickwit split format for immutable, optimized index storage
- **JVM-wide caching**: Shared SplitCacheManager instances for optimal memory utilization across executors


## Project Structure

```
src/main/scala/com/tantivy4spark/
├── core/           # Core Spark DataSource V2 integration
│   ├── Tantivy4SparkDataSource.scala      # Main data source implementation
│   ├── Tantivy4SparkScanBuilder.scala     # Query planning and filter pushdown
│   ├── FiltersToQueryConverter.scala      # Convert Spark filters to Tantivy queries
│   └── ...
├── search/         # Tantivy search engine integration
│   ├── TantivySearchEngine.scala          # Main search interface (for write operations)
│   ├── SplitSearchEngine.scala            # Split-based search engine with caching
│   ├── TantivyJavaInterface.scala         # tantivy4java integration adapter
│   ├── SchemaConverter.scala              # Spark ↔ Tantivy schema mapping
│   └── RowConverter.scala                 # Row data conversion
├── storage/        # Storage abstraction and split management
│   ├── StorageStrategy.scala              # Protocol detection and routing
│   ├── S3OptimizedReader.scala            # S3-optimized I/O with caching
│   ├── StandardFileReader.scala           # Standard Hadoop operations
│   ├── SplitManager.scala                 # Split creation and validation
│   └── GlobalSplitCacheManager.scala      # JVM-wide split cache management
└── transaction/    # Transaction log implementation
    ├── TransactionLog.scala               # Main transaction log interface
    └── Actions.scala                      # Action types (ADD, REMOVE, METADATA)

```

## Build System

### Requirements
- **Java**: Version 11+
- **Scala**: 2.12.17  
- **Apache Spark**: 3.5.3
- **Hadoop**: 3.3.4
- **Maven**: 3.6+ (primary build system)

### Dependencies
- **AWS SDK**: 2.20.26+ for S3 optimized storage
- **Jackson**: 2.15.2+ for JSON processing
- **tantivy4java**: 0.24.0+ (Java bindings for Tantivy search engine)

### Build Commands
```bash
# Full build (using tantivy4java dependency)
mvn clean compile

# Run tests
mvn test

# Package JAR
mvn package
```

## Storage Configuration

The system automatically detects storage protocols and applies appropriate I/O optimizations:

### Protocol Detection
| Protocol | Reader | Optimizations |
|----------|--------|---------------|
| `s3://`, `s3a://`, `s3n://` | S3OptimizedReader | Predictive I/O, caching, range requests |
| `hdfs://`, `file://`, local paths | StandardFileReader | Standard Hadoop file operations |

### Configuration Options

The system supports several configuration options for performance tuning:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.tantivy4spark.storage.force.standard` | `false` | Force standard Hadoop operations for all protocols |
| `spark.tantivy4spark.cache.name` | `"tantivy4spark-cache"` | Name of the JVM-wide split cache |
| `spark.tantivy4spark.cache.maxSize` | `200000000` | Maximum cache size in bytes (200MB default) |
| `spark.tantivy4spark.cache.maxConcurrentLoads` | `8` | Maximum concurrent component loads |
| `spark.tantivy4spark.cache.queryCache` | `true` | Enable query result caching |
| `spark.tantivy4spark.aws.accessKey` | - | AWS access key for S3 split access |
| `spark.tantivy4spark.aws.secretKey` | - | AWS secret key for S3 split access |
| `spark.tantivy4spark.aws.region` | - | AWS region for S3 split access |
| `spark.tantivy4spark.aws.endpoint` | - | Custom AWS S3 endpoint |
| `spark.tantivy4spark.azure.accountName` | - | Azure storage account name |
| `spark.tantivy4spark.azure.accountKey` | - | Azure storage account key |
| `spark.tantivy4spark.gcp.projectId` | - | GCP project ID for Cloud Storage |
| `spark.tantivy4spark.gcp.credentialsFile` | - | Path to GCP service account credentials file |


### Usage Examples
```scala
// Write data as split files with automatic cloud storage optimization
df.write.format("tantivy4spark").save("s3://bucket/path")

// Configure split cache settings
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.cache.maxSize", "500000000") // 500MB cache
  .save("s3://bucket/path")

// AWS-specific configuration for split access
spark.conf.set("spark.tantivy4spark.aws.accessKey", "your-access-key")
spark.conf.set("spark.tantivy4spark.aws.secretKey", "your-secret-key")
spark.conf.set("spark.tantivy4spark.aws.region", "us-west-2")
df.write.format("tantivy4spark").save("s3://bucket/path")

// Standard operations for local/HDFS
df.write.format("tantivy4spark").save("hdfs://namenode/path")
df.write.format("tantivy4spark").save("file:///local/path")

// Reading with automatic schema inference
val df = spark.read.format("tantivy4spark").load("s3://bucket/path")

// Complex queries with filter pushdown
df.filter($"title".contains("Spark") && $"category" === "technology")
  .groupBy("department")
  .agg(count("*"), avg("salary"))
  .show()

// Text search queries
df.filter($"content".contains("machine learning"))
  .filter($"author".startsWith("John"))
  .select("title", "content", "timestamp")
  .orderBy($"timestamp".desc)
  .show()

// Multiple text search conditions
df.filter($"description".contains("Apache") && $"tags".contains("spark"))
  .show()
```


## Schema Handling

### Supported Data Types
**Supported types** (automatically converted to Tantivy equivalents):
- **String → text**: Full-text search enabled
- **Integer/Long → i64**: Numeric range queries
- **Float/Double → f64**: Floating-point range queries  
- **Boolean → i64**: Stored as 0/1
- **Binary → bytes**: Raw binary data
- **Timestamp → i64**: Stored as epoch milliseconds
- **Date → i64**: Stored as days since epoch

**Unsupported types** (explicitly rejected with clear error messages):
- **Array types**: `ArrayType(StringType)` → UnsupportedOperationException
- **Map types**: `MapType(StringType, StringType)` → UnsupportedOperationException
- **Struct types**: `StructType(...)` → UnsupportedOperationException
- **Complex nested types**: Any combination of above → UnsupportedOperationException

### Write Behavior
- **New datasets**: Schema inferred from DataFrame at write time
- **Existing datasets**: Schema read from transaction log
- **Type validation**: Unsupported types rejected early with descriptive error messages
- **Schema enforcement**: Prevents runtime errors by validating schema during write

### Read Behavior  
- Schema always read from transaction log for consistency
- Supports schema evolution through transaction log versioning
- **Search Integration**: Uses Tantivy's AllQuery for comprehensive document retrieval
- **Type Conversion**: Automatic handling of type mismatches (Integer ↔ Long, Integer ↔ Boolean)
- **Error Recovery**: Fallback to manual row conversion when Catalyst conversion fails

## Test Coverage

The project maintains comprehensive test coverage with **105 tests** achieving **100% pass rate**:
- **Unit tests**: 90%+ coverage for all core classes
- **Integration tests**: End-to-end workflow validation with comprehensive test data
- **Type safety tests**: Comprehensive validation of supported/unsupported data type handling
- **Mock framework**: Testing with graceful degradation when native JNI library unavailable
- **Performance tests**: Large-scale operation validation with 1000+ document datasets
- **Query Testing**: Full coverage of text search, numeric ranges, boolean logic, null handling
- **Schema Evolution**: Testing of schema changes and backward compatibility

### Test Structure
```
src/test/scala/com/tantivy4spark/
├── TestBase.scala                         # Common test utilities and Spark session
├── core/                                  # Core functionality tests
│   ├── FiltersToQueryConverterTest.scala  # Filter conversion testing
│   ├── PathParameterTest.scala            # Path handling tests
│   ├── UnsupportedTypesTest.scala         # Data type validation testing
│   └── Tantivy4SparkIntegrationTest.scala # DataSource integration tests
├── search/
│   └── SchemaConverterTest.scala          # Schema mapping tests
├── storage/
│   ├── StorageStrategyTest.scala          # Storage protocol tests
│   └── TantivyArchiveFormatTest.scala     # Archive format tests
├── transaction/
│   ├── TransactionLogTest.scala           # Transaction log tests
│   └── TransactionLogStatisticsTest.scala # Statistics collection validation
├── integration/
│   ├── Tantivy4SparkFullIntegrationTest.scala # End-to-end integration tests
│   └── DataSkippingVerificationTest.scala # File skipping validation
└── debug/                                 # Debug and diagnostic tests
    ├── DocumentExtractionTest.scala       # Document handling tests
    └── NativeLibraryTest.scala            # JNI library loading tests
```

## Development Notes

### Data Source Registration
The project is registered as a Spark data source via:
- `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
- `META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider`

### tantivy4java Integration
- **Library Loading**: Automatic native library loading via tantivy4java
- **Platform Detection**: Handled by tantivy4java for OS and architecture
- **Resource Management**: tantivy4java manages native library lifecycle
- **Exception Handling**: Integration errors gracefully handled

### Performance Optimizations
- **Predictive I/O**: S3OptimizedReader prefetches sequential chunks
- **Caching Strategy**: Chunk-based caching with ConcurrentHashMap
- **Parallel Processing**: Multi-threaded document processing in executors
- **Memory Efficiency**: Streaming document conversion without buffering entire datasets

### Key Implementation Details
- **DataSource V2 API**: Full implementation of modern Spark connector interface
- **Filter pushdown**: Converts Spark filters to Tantivy query syntax
- **Partition pruning**: Leverages min/max statistics for efficient query execution
- **Transaction isolation**: ACID properties through transaction log
- **Type safety**: Explicit rejection of unsupported data types with clear error messages
- **Error handling**: Graceful degradation when native library unavailable
- **tantivy4java Integration**: Pure Java bindings via tantivy4java library
- **Search Engine**: Uses Tantivy's AllQuery for comprehensive document retrieval
- **Type Conversion**: Robust handling of Spark ↔ Tantivy type mappings (Integer/Long/Boolean)
- **Archive Format**: Custom `.tnt4s` format with footer-based component indexing

### Build Integration
- **Maven Dependencies**: Uses tantivy4java as a standard Maven dependency
- **Native Library**: Managed by tantivy4java, no custom native compilation needed
- **Service Registration**: Auto-discovery via META-INF service provider files
- **Cross-Platform**: tantivy4java handles platform-specific native libraries

### Runtime Configuration
- **Library Integration**: tantivy4java handles native library loading automatically
- **Memory Management**: tantivy4java manages native handles and resources
- **Logging Integration**: SLF4J logging for Scala components and tantivy4java integration
- **Error Propagation**: Structured error handling from tantivy4java to Spark

---

## Important Instructions

**Do what has been asked; nothing more, nothing less.**

- **NEVER create files** unless they're absolutely necessary for achieving your goal
- **ALWAYS prefer editing** an existing file to creating a new one  
- **NEVER proactively create documentation files** (*.md) or README files
- Only create documentation files if **explicitly requested** by the user