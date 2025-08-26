# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tantivy4Spark** is a high-performance file format for Apache Spark that implements fast full-text search using the [Tantivy search engine library](https://github.com/quickwit-oss/tantivy) via [tantivy4java](https://github.com/quickwit-oss/tantivy-java). It runs embedded inside Apache Spark without requiring any server-side components.

### Key Features
- **Embedded search**: Tantivy runs directly within Spark executors via tantivy4java
- **Split-based architecture**: Write-only indexes with split-based reading using QuickwitSplit format
- **Optimized writes**: Delta Lake-style automatic split sizing with adaptive shuffle for optimal performance
- **Transaction log**: Delta Lake-style transaction log with batched operations for metadata management  
- **Smart file skipping**: Min/max value tracking for efficient query pruning
- **Schema-aware filter pushdown**: Field validation prevents native crashes during query execution
- **S3-optimized storage**: Intelligent caching and compression for object storage
- **Flexible storage**: Support for local, HDFS, and S3 storage protocols
- **Schema evolution**: Automatic schema inference and evolution support
- **JVM-wide caching**: Shared SplitCacheManager reduces memory usage across executors

## Architecture

### File Format
- **Split files**: `*.split` files contain Tantivy indexes using tantivy4java's optimized QuickwitSplit format
- **Split structure**: Self-contained immutable index files with native tantivy4java caching support
- **Write-only pattern**: Create index → add documents → commit → create split → close index
- **Transaction log**: `_transaction_log/` directory (Delta Lake compatible) contains:
  - Schema metadata
  - **Batched ADD operations**: Multiple ADD entries per transaction file (like Delta Lake)
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
- **AWS session token support**: Full support for temporary credentials via AWS STS session tokens
- **Smart cache locality**: Host-based split location tracking with Spark's preferredLocations API for optimal task scheduling


## Project Structure

```
src/main/scala/com/tantivy4spark/
├── core/           # Core Spark DataSource V2 integration
│   ├── Tantivy4SparkDataSource.scala      # Main data source implementation
│   ├── Tantivy4SparkScanBuilder.scala     # Query planning and filter pushdown
│   ├── Tantivy4SparkOptions.scala         # Configuration options utility
│   ├── Tantivy4SparkOptimizedWrite.scala  # Optimized write implementation
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
│   ├── SplitManager.scala                 # Split creation, validation, and location tracking
│   └── GlobalSplitCacheManager.scala      # JVM-wide split cache management
├── config/         # Configuration system
│   ├── Tantivy4SparkSQLConf.scala         # Spark SQL configuration constants
│   └── Tantivy4SparkConfig.scala          # Table property configuration
├── optimize/       # Optimized write functionality
│   └── Tantivy4SparkOptimizedWriterExec.scala # Delta Lake-style adaptive shuffle execution
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
| `spark.tantivy4spark.optimizeWrite.enabled` | `true` | Enable/disable optimized writes with automatic split sizing |
| `spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit` | `1000000` | Target number of records per split file for optimized writes |
| `spark.tantivy4spark.storage.force.standard` | `false` | Force standard Hadoop operations for all protocols |
| `spark.tantivy4spark.cache.name` | `"tantivy4spark-cache"` | Name of the JVM-wide split cache |
| `spark.tantivy4spark.cache.maxSize` | `200000000` | Maximum cache size in bytes (200MB default) |
| `spark.tantivy4spark.cache.maxConcurrentLoads` | `8` | Maximum concurrent component loads |
| `spark.tantivy4spark.cache.queryCache` | `true` | Enable query result caching |
| `spark.tantivy4spark.aws.accessKey` | - | AWS access key for S3 split access |
| `spark.tantivy4spark.aws.secretKey` | - | AWS secret key for S3 split access |
| `spark.tantivy4spark.aws.sessionToken` | - | AWS session token for temporary credentials (STS) |
| `spark.tantivy4spark.aws.region` | - | AWS region for S3 split access |
| `spark.tantivy4spark.aws.endpoint` | - | Custom AWS S3 endpoint |
| `spark.tantivy4spark.azure.accountName` | - | Azure storage account name |
| `spark.tantivy4spark.azure.accountKey` | - | Azure storage account key |
| `spark.tantivy4spark.gcp.projectId` | - | GCP project ID for Cloud Storage |
| `spark.tantivy4spark.gcp.credentialsFile` | - | Path to GCP service account credentials file |


### Usage Examples
```scala
// Write data with optimized writes (enabled by default)
df.write.format("tantivy4spark").save("s3://bucket/path")

// Write with custom split sizing
df.write.format("tantivy4spark")
  .option("targetRecordsPerSplit", "500000")  // 500K records per split
  .save("s3://bucket/path")

// Configure optimized writes via Spark session (applies to all writes)
spark.conf.set("spark.tantivy4spark.optimizeWrite.enabled", "true")
spark.conf.set("spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit", "2000000")

// Disable optimized writes (use Spark's default partitioning)
df.write.format("tantivy4spark")
  .option("optimizeWrite", "false")
  .save("s3://bucket/path")

// Configure split cache settings
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.cache.maxSize", "500000000") // 500MB cache
  .save("s3://bucket/path")

// AWS-specific configuration for split access
spark.conf.set("spark.tantivy4spark.aws.accessKey", "your-access-key")
spark.conf.set("spark.tantivy4spark.aws.secretKey", "your-secret-key")
spark.conf.set("spark.tantivy4spark.aws.region", "us-west-2")
df.write.format("tantivy4spark").save("s3://bucket/path")

// AWS configuration with temporary credentials (STS session token)
spark.conf.set("spark.tantivy4spark.aws.accessKey", "your-temporary-access-key")
spark.conf.set("spark.tantivy4spark.aws.secretKey", "your-temporary-secret-key")
spark.conf.set("spark.tantivy4spark.aws.sessionToken", "your-session-token")
spark.conf.set("spark.tantivy4spark.aws.region", "us-west-2")
df.write.format("tantivy4spark").save("s3://bucket/path")

// Alternative: Pass credentials via DataFrame write options (automatically propagated)
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.aws.accessKey", "your-access-key")
  .option("spark.tantivy4spark.aws.secretKey", "your-secret-key") 
  .option("spark.tantivy4spark.aws.sessionToken", "your-session-token")
  .option("spark.tantivy4spark.aws.region", "us-west-2")
  .option("spark.tantivy4spark.aws.endpoint", "https://s3.custom-provider.com")
  .save("s3://bucket/path")

// Split cache configuration with AWS session tokens
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.cache.maxSize", "500000000") // 500MB cache
  .option("spark.tantivy4spark.aws.sessionToken", "your-session-token") // Session token for cache
  .save("s3://bucket/path")

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
- **Split-based reading**: Uses SplitSearchEngine with document field extraction via document.getFirst()
- **JVM-wide caching**: Shared SplitCacheManager optimizes memory usage across executors
- **Type Conversion**: Automatic handling of type mismatches (Integer ↔ Long, Integer ↔ Boolean)
- **Error Recovery**: Fallback to manual row conversion when Catalyst conversion fails
- **Schema-aware filtering**: Field validation prevents native crashes during query execution

## Test Coverage

The project maintains comprehensive test coverage with **127 tests** achieving **126 pass, 1 known failure** (99.2% pass rate):
- **Unit tests**: 90%+ coverage for all core classes
- **Integration tests**: End-to-end workflow validation with comprehensive test data
- **Optimized write tests**: Comprehensive validation of automatic split sizing and configuration hierarchy
- **Split-based architecture tests**: Comprehensive validation of write-only indexes and split reading
- **Transaction log batch tests**: Testing of Delta Lake-compatible batched operations
- **Schema-aware filter tests**: Validation of field existence checking to prevent native crashes
- **Type safety tests**: Comprehensive validation of supported/unsupported data type handling
- **Performance tests**: Large-scale operation validation with 1000+ document datasets
- **Query Testing**: Full coverage of text search, numeric ranges, boolean logic, null handling
- **Schema Evolution**: Testing of schema changes and backward compatibility

### Known Issues
- **Boolean Filtering on S3**: One test failure related to boolean filtering with S3 storage (under investigation)
- **Root Cause**: Split-based filtering bypasses type conversion logic in `FiltersToQueryConverter`
- **Workaround**: Boolean filtering works correctly with local/HDFS storage

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
├── optimize/                              # Optimized write tests
│   └── OptimizedWriteTest.scala           # Delta Lake-style optimized write functionality
├── transaction/
│   ├── TransactionLogTest.scala           # Transaction log tests
│   ├── BatchTransactionLogTest.scala     # Delta Lake-style batched operations
│   └── TransactionLogStatisticsTest.scala # Statistics collection validation
├── integration/
│   ├── Tantivy4SparkFullIntegrationTest.scala # End-to-end integration tests
│   └── DataSkippingVerificationTest.scala # File skipping validation
└── debug/                                 # Debug and diagnostic tests
    ├── DocumentExtractionTest.scala       # Document handling tests
    ├── FieldExtractionDebugTest.scala     # Split-based field extraction debugging
    ├── MinimalDirectTest.scala            # Split-based architecture tests
    └── NativeLibraryTest.scala            # Split-based edge case tests
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
- **Smart Cache Locality**: Split location registry tracks which hosts have cached which splits, enabling Spark's scheduler to prefer those hosts for better data locality

### Key Implementation Details
- **DataSource V2 API**: Full implementation of modern Spark connector interface
- **Unified I/O Architecture**: Replaced direct Hadoop filesystem usage with CloudStorageProvider abstraction
  - **CloudStorageProviderFactory**: Centralized provider creation with protocol detection
  - **S3CloudStorageProvider**: AWS SDK v2 with session token support, path-style access, custom endpoints
  - **HadoopCloudStorageProvider**: Wrapper for Hadoop-based operations (local, HDFS)
  - **Configuration Propagation**: DataFrame write options → Spark config → Hadoop config → executors
- **Optimized Write Architecture**: Delta Lake-style automatic split sizing with adaptive shuffle
  - **Configuration Hierarchy**: Write options → Spark session config → table properties → defaults
  - **Adaptive Partitioning**: Calculates optimal partition count using ceiling function: `ceil(totalRecords / targetRecords)`
  - **Shuffle Strategies**: HashPartitioning for partitioned tables, RoundRobinPartitioning for non-partitioned
  - **Metrics Integration**: SQL metrics for monitoring split count and record distribution
- **Enhanced AWS Integration**: Complete session token support throughout the system
  - **Session Token Flow**: DataFrame options → TransactionLog → S3CloudStorageProvider → tantivy4java CacheConfig
  - **Multi-context Support**: Works in driver, executor, and serialized contexts
  - **Configuration Extraction**: From Spark session, Hadoop config, and write options
- **Schema-aware filter pushdown**: Field validation prevents FieldNotFound panics at native level
- **Split-based architecture**: Write-only indexes with immutable split files for reading
- **Partition pruning**: Leverages min/max statistics for efficient query execution
- **Transaction isolation**: ACID properties through Delta Lake-compatible transaction log
- **Batched transactions**: Multiple ADD entries per transaction file (Delta Lake pattern)
- **Type safety**: Explicit rejection of unsupported data types with clear error messages
- **Error handling**: Graceful degradation when native library unavailable
- **tantivy4java Integration**: Pure Java bindings via tantivy4java library
- **Search Engine**: Uses SplitSearchEngine with document field extraction via document.getFirst()
- **Type Conversion**: Robust handling of Spark ↔ Tantivy type mappings (Integer/Long/Boolean)
- **Split format**: QuickwitSplit (.split) format with JVM-wide caching support
- **Shaded Dependencies**: Google Guava repackaged to avoid conflicts (`tantivy4spark.com.google.common`)
- **Split Location Tracking**: SplitLocationRegistry tracks host-to-split cache mappings for optimal task scheduling
  - **Automatic Host Recording**: Records which hosts access which splits during partition reader initialization
  - **PreferredLocations Integration**: Implements Spark's InputPartition.preferredLocations() API
  - **Locality-Aware Scheduling**: Spark scheduler uses preferred locations to assign tasks to hosts with cached splits
  - **Performance Benefits**: Reduces network I/O and improves query performance through better data locality

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

### AWS Configuration Distribution (Fixed Issue)

**Problem Solved**: AWS credentials and region configuration was not properly propagating from Spark driver to executors, causing "A region must be set when sending requests to S3" errors during split reading operations.

**Root Cause**: Multiple code paths with different configuration propagation mechanisms:
1. **V2 DataSource API Path**: Uses scan builder → scan → reader factory → partition reader
2. **V1 DataSource API Path**: Uses buildScan → processFile standalone method

**Solutions Implemented**:

#### V2 DataSource API Fix (Broadcast Variables)
- **Driver Side** (`Tantivy4SparkTable.newScanBuilder()`): Creates broadcast variable containing all `spark.tantivy4spark.*` configurations
- **Executor Side** (`Tantivy4SparkPartitionReader.createCacheConfig()`): Accesses broadcast configuration with fallback pattern: `options → broadcast → default`
- **Flow**: `newScanBuilder()` → `Tantivy4SparkScanBuilder` → `Tantivy4SparkScan` → `Tantivy4SparkReaderFactory` → `Tantivy4SparkPartitionReader`

#### V1 DataSource API Fix (Enhanced buildScan)
- **Driver Side** (`Tantivy4SparkRelation.buildScan()`): Extracts all `spark.tantivy4spark.*` configurations from Hadoop configuration and includes them in `hadoopConfProps`
- **Executor Side** (`processFile()` method): Uses `hadoopConfProps` to create `SplitCacheConfig` with proper AWS credentials
- **Flow**: `buildScan()` → `processFile()` → `SplitSearchEngine.fromSplitFile()`

#### Key Implementation Details
```scala
// V2 API: Broadcast variables approach
val tantivyConfigs = hadoopConf.iterator().asScala
  .filter(_.getKey.startsWith("spark.tantivy4spark."))
  .map(entry => entry.getKey -> entry.getValue)
  .toMap
val broadcastConfig = spark.sparkContext.broadcast(tantivyConfigs)

// V1 API: Enhanced hadoopConfProps
val tantivyProps = hadoopConf.iterator().asScala
  .filter(_.getKey.startsWith("spark.tantivy4spark."))
  .map(entry => entry.getKey -> entry.getValue)
  .toMap
val hadoopConfProps = baseHadoopProps ++ tantivyProps
```

**Verification**: Both paths now successfully create `SplitCacheConfig` with proper AWS values:
- ✅ `awsRegion=us-east-1` (instead of `awsRegion=None`)
- ✅ `awsEndpoint=http://localhost:port` (instead of `awsEndpoint=None`)
- ✅ AWS credentials properly propagate to tantivy4java's `SplitCacheManager`

**Remaining Issue**: Custom S3 endpoints with tantivy4java may still have compatibility issues at the native library level, but configuration propagation is now working correctly.

## Wildcard Query Support

### Overview
Tantivy4Spark now fully supports wildcard queries through integration with tantivy4java's comprehensive wildcard implementation. This feature enables powerful pattern-matching searches across text fields.

### Supported Wildcard Patterns
- `*` - Matches any sequence of characters (including empty)
- `?` - Matches exactly one character
- `\*` - Literal asterisk (escaped)
- `\?` - Literal question mark (escaped)

### Query Examples
```scala
// Basic wildcard patterns
df.filter($"title".contains("Spark*"))       // Prefix search
df.filter($"name".contains("*smith"))        // Suffix search
df.filter($"content".contains("*data*"))     // Contains search
df.filter($"code".contains("test?"))         // Single character wildcard

// Complex patterns
df.filter($"description".contains("pro*ing")) // Middle wildcard
df.filter($"path".contains("/usr/*/bin"))    // Path pattern matching
```

### Implementation Details
- **Native Support**: Uses tantivy4java's `Query.wildcardQuery()` method for optimal performance
- **Automatic Conversion**: Spark filter predicates with wildcards are automatically converted to Tantivy wildcard queries
- **Lenient Mode**: Missing fields are handled gracefully without errors
- **Fallback Strategy**: If wildcard query fails, automatically falls back to regex query
- **Multi-token Support**: Advanced patterns with spaces are tokenized and combined with boolean AND logic
- **Case Sensitivity**: Respects the tokenizer's case handling (default is case-insensitive)

### Performance Considerations
- **Efficient**: Patterns starting with literals (`prefix*`) are faster than suffix patterns (`*suffix`)
- **Optimized**: Uses native Tantivy index structures for pattern matching
- **Cached**: Results are cached in the JVM-wide SplitCacheManager

### Technical Notes
- Wildcard queries operate at the **term level** after tokenization
- Pattern `"Hello*World"` won't match "Hello World" (two separate terms)
- Use `"Hello* *World"` for multi-term patterns (requires both terms present)
- See `/Users/schenksj/tmp/x/tantivy4java_pyport/WILDCARD_IMPLEMENTATION_GUIDE.md` for complete implementation details

---

## Important Instructions

**Do what has been asked; nothing more, nothing less.**

- **NEVER create files** unless they're absolutely necessary for achieving your goal
- **ALWAYS prefer editing** an existing file to creating a new one  
- **NEVER proactively create documentation files** (*.md) or README files
- Only create documentation files if **explicitly requested** by the user