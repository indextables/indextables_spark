# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tantivy4Spark** is a high-performance file format for Apache Spark that implements fast full-text search using the [Tantivy search engine library](https://github.com/quickwit-oss/tantivy) directly via JNI. It runs embedded inside Apache Spark without requiring any server-side components.

### Key Features
- **Embedded search**: Tantivy runs directly within Spark executors via JNI
- **Transaction log**: Delta-like transaction log for metadata management  
- **Smart file skipping**: Min/max value tracking for efficient query pruning
- **Bloom filter acceleration**: Splunk-style bloom filters for text search optimization
- **S3-optimized storage**: Intelligent caching and compression for object storage
- **Flexible storage**: Support for local, HDFS, and S3 storage protocols
- **Schema evolution**: Automatic schema inference and evolution support

## Architecture

### File Format
- **Index files**: `*.tnt4s` files contain Tantivy indexes stored as single archive files
- **Archive structure**: Footer describes offset and size of each component for efficient partial downloads
- **Transaction log**: `_transaction_log/` directory (similar to Delta's `_delta_log/`) contains:
  - Schema metadata
  - ADD entries for each index file created
  - Min/max values for query pruning
  - Bloom filters for text search acceleration
  - Partition key information and row counts

### Integration Design
The system implements a custom Spark DataSource V2 provider modeled after [Delta Lake's DeltaDataSource](https://github.com/delta-io/delta/blob/d16d591ae90d5e2024e3d2306b1018361bda8e80/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaDataSource.scala):

- **Custom relation provider**: Reads ADD entries instead of using default Spark file listing
- **Query pruning**: Skips indexes based on min/max values stored in transaction log  
- **Bloom filter skipping**: Accelerates text searches using probabilistic data structures
- **File format integration**: Similar to [Delta's ParquetFileFormat](https://github.com/delta-io/delta/blob/51150999ee12f120d370b26b8273b6c293f39a43/spark/src/main/scala/org/apache/spark/sql/delta/DeltaParquetFileFormat.scala)

### Java Integration (via tantivy4java)
- **Pure Java bindings**: Uses tantivy4java library for Tantivy integration
- **No native compilation**: Eliminates Rust/Cargo build dependencies
- **Schema mapping**: Automatic conversion between Spark and Tantivy data types
- **Cross-platform**: Supports Linux, macOS, and Windows via tantivy4java native libraries

### Bloom Filter Architecture (Splunk-style Text Search Acceleration)

**Design Philosophy**: Minimize I/O to object storage by creating probabilistic data structures that can quickly eliminate files that definitely don't contain search terms.

**Key Components**:
- **BloomFilter**: Space-efficient probabilistic data structure with configurable false positive rate (~1%)
- **TextTokenizer**: Splunk-style tokenization (words, n-grams, delimited terms) for comprehensive text coverage
- **BloomFilterManager**: Orchestrates bloom filter creation and querying across multiple text columns
- **BloomFilterStorage**: S3-optimized compression, encoding, and caching layer

**S3 Optimizations**:
- **Batch Retrieval**: Preload bloom filters for multiple files to minimize S3 requests
- **Compression**: GZIP compression reduces bloom filter storage size by ~70%
- **Base64 Encoding**: JSON-compatible storage in transaction log
- **LRU Caching**: In-memory cache (default 1000 entries) to avoid repeated downloads
- **Parallel Processing**: Concurrent bloom filter evaluation for large file sets

**File Skipping Process**:
1. **Query Analysis**: Extract text search terms from Spark filters (Contains, StartsWith, etc.)
2. **Batch Preload**: Load bloom filters for candidate files in parallel  
3. **Probabilistic Filtering**: Test each bloom filter against search terms
4. **Conservative Inclusion**: Include file if bloom filter unavailable or decode fails
5. **Metrics**: Track skipping effectiveness and cache performance

**Performance Characteristics**:
- **Space Efficiency**: ~10-50 bytes per file for typical text columns
- **Query Speed**: Sub-millisecond bloom filter evaluation
- **False Positive Rate**: Configurable (default 1%), no false negatives
- **S3 Request Reduction**: 60-90% fewer files processed for text searches
- **Compression Ratio**: 70-80% size reduction with GZIP

## Project Structure

```
src/main/scala/com/tantivy4spark/
├── core/           # Core Spark DataSource V2 integration
│   ├── Tantivy4SparkDataSource.scala      # Main data source implementation
│   ├── Tantivy4SparkScanBuilder.scala     # Query planning and filter pushdown
│   ├── FiltersToQueryConverter.scala      # Convert Spark filters to Tantivy queries
│   └── ...
├── search/         # Tantivy search engine integration
│   ├── TantivySearchEngine.scala          # Main search interface
│   ├── TantivyJavaInterface.scala         # tantivy4java integration adapter
│   ├── SchemaConverter.scala              # Spark ↔ Tantivy schema mapping
│   └── RowConverter.scala                 # Row data conversion
├── storage/        # Storage abstraction layer
│   ├── StorageStrategy.scala              # Protocol detection and routing
│   ├── S3OptimizedReader.scala            # S3-optimized I/O with caching
│   └── StandardFileReader.scala           # Standard Hadoop operations
├── bloom/          # Bloom filter text search acceleration
│   ├── BloomFilterManager.scala           # Bloom filter orchestration and querying
│   └── BloomFilterStorage.scala           # S3-optimized storage and caching
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
| `spark.tantivy4spark.bloom.filters.enabled` | `true` | Enable/disable bloom filter creation for text search acceleration |

#### Bloom Filter Configuration

Bloom filters can be disabled to reduce write time and storage overhead when text search acceleration is not needed:

```scala
// Disable bloom filters via DataFrame option
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.bloom.filters.enabled", "false")
  .save("s3://bucket/path")

// Disable bloom filters via Spark configuration (affects all writes)
spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", "false")
df.write.format("tantivy4spark").save("s3://bucket/path")

// DataFrame option takes precedence over Spark configuration
spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", "false")
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.bloom.filters.enabled", "true")  // This overrides Spark config
  .save("s3://bucket/path")
```

### Usage Examples
```scala
// S3-optimized I/O (automatic)
df.write.format("tantivy4spark").save("s3://bucket/path")

// Force standard operations  
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.storage.force.standard", "true")
  .save("s3://bucket/path")

// Disable bloom filters for faster writes when text search not needed
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.bloom.filters.enabled", "false")
  .save("s3://bucket/path")

// Standard operations (automatic)
df.write.format("tantivy4spark").save("hdfs://namenode/path")
df.write.format("tantivy4spark").save("file:///local/path")

// Reading with automatic schema inference
val df = spark.read.format("tantivy4spark").load("s3://bucket/path")

// Complex queries with filter pushdown and bloom filter acceleration
df.filter($"title".contains("Spark") && $"category" === "technology")
  .groupBy("department")
  .agg(count("*"), avg("salary"))
  .show()

// Text search queries optimized by bloom filters
df.filter($"content".contains("machine learning"))  // Bloom filter skips irrelevant files
  .filter($"author".startsWith("John"))              // Further filtering
  .select("title", "content", "timestamp")
  .orderBy($"timestamp".desc)
  .show()

// Multiple text search conditions (bloom filter intersection)
df.filter($"description".contains("Apache") && $"tags".contains("spark"))
  .show()
```

### Bloom Filter Usage Examples

```scala
// Bloom filters are automatically created during write operations
// for all string columns, providing transparent acceleration

// Writing data - bloom filters created automatically
val documents = Seq(
  (1, "Apache Spark Performance Guide", "This guide covers Spark optimization..."),
  (2, "Machine Learning with MLlib", "MLlib provides scalable ML algorithms..."),
  (3, "Stream Processing Tutorial", "Real-time data processing with Spark...")
).toDF("id", "title", "content")

documents.write.format("tantivy4spark").save("s3://bucket/documents")

// Reading with bloom filter acceleration
val df = spark.read.format("tantivy4spark").load("s3://bucket/documents")

// These queries benefit from bloom filter file skipping:
df.filter($"title".contains("Spark"))           // Fast file elimination
df.filter($"content".contains("optimization"))  // Skips non-matching files  
df.filter($"title".startsWith("Machine"))       // Prefix matching acceleration
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
- **Bloom filter tests**: Complete coverage of text search acceleration and S3 optimization
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
├── bloom/                                 # Bloom filter acceleration tests
│   ├── BloomFilterTest.scala              # Core bloom filter functionality
│   └── BloomFilterIntegrationTest.scala   # S3-optimized bloom filter testing
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
- **Bloom filter acceleration**: Splunk-style probabilistic file skipping for text searches
- **Transaction isolation**: ACID properties through transaction log
- **Type safety**: Explicit rejection of unsupported data types with clear error messages
- **Error handling**: Graceful degradation when native library unavailable
- **tantivy4java Integration**: Pure Java bindings via tantivy4java library
- **Search Engine**: Uses Tantivy's AllQuery for comprehensive document retrieval
- **Type Conversion**: Robust handling of Spark ↔ Tantivy type mappings (Integer/Long/Boolean)
- **Archive Format**: Custom `.tnt4s` format with footer-based component indexing
- **Shaded Dependencies**: Google Guava repackaged to avoid conflicts (`tantivy4spark.com.google.common`)

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