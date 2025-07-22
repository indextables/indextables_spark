# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tantivy4Spark** is a high-performance file format for Apache Spark that implements fast full-text search using the [Tantivy search engine library](https://github.com/quickwit-oss/tantivy) directly via JNI. It runs embedded inside Apache Spark without requiring any server-side components.

### Key Features
- **Embedded search**: Tantivy runs directly within Spark executors via JNI
- **Transaction log**: Delta-like transaction log for metadata management  
- **Smart file skipping**: Min/max value tracking for efficient query pruning
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
  - Partition key information and row counts

### Integration Design
The system implements a custom Spark DataSource V2 provider modeled after [Delta Lake's DeltaDataSource](https://github.com/delta-io/delta/blob/d16d591ae90d5e2024e3d2306b1018361bda8e80/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaDataSource.scala):

- **Custom relation provider**: Reads ADD entries instead of using default Spark file listing
- **Query pruning**: Skips indexes based on min/max values stored in transaction log  
- **File format integration**: Similar to [Delta's ParquetFileFormat](https://github.com/delta-io/delta/blob/51150999ee12f120d370b26b8273b6c293f39a43/spark/src/main/scala/org/apache/spark/sql/delta/DeltaParquetFileFormat.scala)

### Native Integration (JNI)
- **Rust implementation**: `src/main/rust/` contains JNI bindings to Tantivy
- **Build integration**: Native library built with Cargo and embedded in JAR
- **Schema mapping**: Automatic conversion between Spark and Tantivy data types
- **Cross-platform**: Supports Linux, macOS, and Windows

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
│   ├── SchemaConverter.scala              # Spark ↔ Tantivy schema mapping
│   └── RowConverter.scala                 # Row data conversion
├── storage/        # Storage abstraction layer
│   ├── StorageStrategy.scala              # Protocol detection and routing
│   ├── S3OptimizedReader.scala            # S3-optimized I/O with caching
│   └── StandardFileReader.scala           # Standard Hadoop operations
└── transaction/    # Transaction log implementation
    ├── TransactionLog.scala               # Main transaction log interface
    └── Actions.scala                      # Action types (ADD, REMOVE, METADATA)

src/main/rust/      # Native JNI implementation  
├── Cargo.toml      # Rust dependencies (Tantivy crates)
└── src/
    ├── lib.rs                             # JNI exports and method bindings
    ├── index_manager.rs                   # Tantivy index management
    ├── schema_mapper.rs                   # Schema conversion utilities
    └── error.rs                           # Error handling and conversions
```

## Build System

### Requirements
- **Java**: Version 11+
- **Scala**: 2.12.17  
- **Apache Spark**: 3.5.3
- **Hadoop**: 3.3.4
- **Rust toolchain**: For building JNI native library (Rust 2021 edition)
- **Maven**: 3.6+ (primary build system)

### Dependencies
- **AWS SDK**: 2.20.26+ for S3 optimized storage
- **Jackson**: 2.15.2+ for JSON processing
- **Tantivy**: 0.22+ (Rust crate for search engine)
- **JNI**: 0.21+ (Rust crate for Java integration)

### Build Commands
```bash
# Full build (includes Rust native library)
mvn clean compile

# Run tests
mvn test

# Package with native library
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
- `spark.tantivy4spark.storage.force.standard = true`: Force standard Hadoop operations for all protocols
- Useful for S3-compatible storage that works better with standard operations

### Usage Examples
```scala
// S3-optimized I/O (automatic)
df.write.format("tantivy4spark").save("s3://bucket/path")

// Force standard operations  
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.storage.force.standard", "true")
  .save("s3://bucket/path")

// Standard operations (automatic)
df.write.format("tantivy4spark").save("hdfs://namenode/path")
df.write.format("tantivy4spark").save("file:///local/path")

// Reading with automatic schema inference
val df = spark.read.format("tantivy4spark").load("s3://bucket/path")

// Complex queries with filter pushdown
df.filter($"title".contains("Spark") && $"category" === "technology")
  .groupBy("department")
  .agg(count("*"), avg("salary"))
  .show()
```

## Schema Handling

### Write Behavior
- **New datasets**: Schema inferred from DataFrame at write time
- **Existing datasets**: Schema read from transaction log

### Read Behavior  
- Schema always read from transaction log for consistency
- Supports schema evolution through transaction log versioning
- **Search Integration**: Uses Tantivy's AllQuery for comprehensive document retrieval
- **Type Conversion**: Automatic handling of type mismatches (Integer ↔ Long, Integer ↔ Boolean)
- **Error Recovery**: Fallback to manual row conversion when Catalyst conversion fails

## Test Coverage

The project maintains comprehensive test coverage with **71 tests** achieving **100% pass rate**:
- **Unit tests**: 90%+ coverage for all core classes
- **Integration tests**: End-to-end workflow validation with comprehensive test data
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
│   └── Tantivy4SparkIntegrationTest.scala # DataSource integration tests
├── search/
│   └── SchemaConverterTest.scala          # Schema mapping tests
├── storage/
│   ├── StorageStrategyTest.scala          # Storage protocol tests
│   └── TantivyArchiveFormatTest.scala     # Archive format tests
├── transaction/
│   └── TransactionLogTest.scala           # Transaction log tests
├── integration/
│   └── Tantivy4SparkFullIntegrationTest.scala # End-to-end integration tests
└── debug/                                 # Debug and diagnostic tests
    ├── DocumentExtractionTest.scala       # Document handling tests
    └── NativeLibraryTest.scala            # JNI library loading tests
```

## Development Notes

### Data Source Registration
The project is registered as a Spark data source via:
- `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
- `META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider`

### Native Library Management
- **Loading Strategy**: TantivyNative.ensureLibraryLoaded() with resource extraction
- **Platform Detection**: Automatic detection of OS and architecture
- **Resource Bundling**: Native libraries packaged in JAR under `/` root path
- **Exception Handling**: UnsatisfiedLinkError gracefully handled in tests

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
- **Error handling**: Graceful degradation when native library unavailable
- **JNI Integration**: Native method bindings with proper Scala object name encoding (`00024`)
- **Search Engine**: Uses Tantivy's AllQuery for comprehensive document retrieval
- **Type Conversion**: Robust handling of Spark ↔ Tantivy type mappings (Integer/Long/Boolean)
- **Archive Format**: Custom `.tnt4s` format with footer-based component indexing

### Build Integration
- **Cargo Integration**: Maven executes Cargo for Rust compilation via exec plugin
- **Native Library**: `libtantivy4spark.dylib` (macOS) embedded in JAR resources
- **Service Registration**: Auto-discovery via META-INF service provider files
- **Cross-Platform**: Native library built per platform during CI/CD

### Runtime Configuration
- **Native Library Loading**: Automatic extraction and loading from JAR resources
- **Memory Management**: Proper cleanup of native handles and resources
- **Logging Integration**: SLF4J logging for both Scala and native components
- **Error Propagation**: Structured error handling from Rust through JNI to Spark

---

## Important Instructions

**Do what has been asked; nothing more, nothing less.**

- **NEVER create files** unless they're absolutely necessary for achieving your goal
- **ALWAYS prefer editing** an existing file to creating a new one  
- **NEVER proactively create documentation files** (*.md) or README files
- Only create documentation files if **explicitly requested** by the user