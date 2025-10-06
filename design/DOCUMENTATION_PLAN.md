# Comprehensive Design Documentation Plan for IndexTables4Spark DataSourceV2

**Generated:** 2025-10-06
**Target Audience:** Developers familiar with Apache Spark but not necessarily with Spark internals
**Estimated Length:** 220-250 pages

---

## IMPORTANT: V1 DataSource API Deprecation Notice

**The V1 DataSource API (`IndexTables4SparkDataSource`) is deprecated and scheduled for removal in a future release.**

This documentation focuses exclusively on the **V2 DataSource API** (`IndexTables4SparkTableProvider`), which is the recommended and actively maintained implementation. The V1 API will only be mentioned where necessary to:

1. Inform users of its deprecation status
2. Provide migration guidance from V1 to V2
3. Explain key differences in partition column indexing behavior

All new development, examples, and usage patterns will use the V2 API.

---

## Document Structure Overview

The documentation will be organized into the following major sections, suitable for developers familiar with Spark but not necessarily with its internals:

---

## 1. Introduction & Architecture Overview (15-20 pages)

### 1.1 System Overview
- **Purpose**: High-performance full-text search DataSource for Apache Spark using Tantivy
- **Key Value Proposition**: Embedded execution without server-side components
- **Architecture Philosophy**: Delta Lake-inspired transaction log with split-based storage

### 1.2 Core Architectural Patterns
- **DataSource V2 API**: Modern Spark 3.x DataSource implementation (V1 API deprecated)
- **Split-based Architecture**: QuickwitSplit format with immutable `.split` files
- **Transaction Log Pattern**: Delta Lake-compatible ACID transactions with checkpoint compaction
- **Embedded Search Engine**: Tantivy execution within Spark executors

### 1.3 High-Level Data Flow
- Write path: DataFrame → Optimized Write → Split Creation → Transaction Log
- Read path: Transaction Log → Partition Planning → Split Reading → Search Execution
- Query path: Filter Pushdown → Partition Pruning → Data Skipping → Result Aggregation

### 1.4 Component Relationship Diagram
- Visual mapping of how all components interact
- Layer separation: API Layer, Planning Layer, Execution Layer, Storage Layer

---

## 2. DataSource V2 API Implementation (15-20 pages)

### 2.1 V2 DataSource API Overview (`IndexTables4SparkTableProvider`)
- **TableProvider Interface**: Modern Spark 3.x entry point
- **IndexTables4SparkTable Capabilities**:
  - `SupportsRead`: Read operations
  - `SupportsWrite`: Write operations with overwrite support
  - `SupportsMetadataColumns`: Virtual `_indexall` column
- **Partitioning Support**: Transform-based partition handling

### 2.2 Configuration Hierarchy
- **Precedence Chain**: DataFrame options → Spark config → Hadoop config → defaults
- **Configuration Normalization**: `spark.indextables.*` prefix handling
- **Credential Propagation**: AWS/Azure/GCP credential flow to executors

### 2.3 V1 API Deprecation & Migration
- **Deprecation Notice**: V1 API (`IndexTables4SparkDataSource`) scheduled for removal
- **Key Difference**: V1 excludes partition columns from indexing; V2 includes them
- **Migration Path**: Simple format string change from `"indextables"` to `"io.indextables.spark.core.IndexTables4SparkTableProvider"`
- **Backward Compatibility**: Existing tables created with V1 remain readable with V2

---

## 3. Transaction Log System (30-35 pages)

### 3.1 Transaction Log Architecture
- **Delta Lake Compatibility**: File format and semantics
- **ACID Guarantees**: Atomicity, consistency, isolation, durability
- **Version Management**: Atomic counter and version sequencing
- **Action Types**:
  - `ProtocolAction`: Version compatibility
  - `MetadataAction`: Schema and table properties
  - `AddAction`: File additions with metadata
  - `RemoveAction`: File removals
  - `SkipAction`: Corrupted file tracking with cooldown

### 3.2 Checkpoint System (`TransactionLogCheckpoint`)
- **Compaction Strategy**: Periodic consolidation of transaction log
- **Checkpoint Format**: JSON snapshots of table state
- **Performance Optimization**: 60% read improvement (validated in tests)
- **Parallel Retrieval**: Concurrent transaction file reading with thread pools
- **Incremental Reading**: Checkpoint + delta workflow

### 3.3 Checkpoint Configuration & Behavior
- **Interval Control**: Configurable checkpoint frequency
- **Retention Policies**: Automatic cleanup with ultra-conservative safety
- **File Management**: Multi-level safety gates prevent data loss
- **Read Optimization**: Pre-checkpoint file skipping

### 3.4 Transaction Log Cache (`TransactionLogCache`)
- **Multi-level Caching**: Versions, files, metadata, protocol, snapshots
- **TTL Management**: Configurable cache expiration (default: 5 minutes)
- **Invalidation Strategy**: Version-dependent vs version-independent caches
- **Performance Impact**: Dramatic reduction in S3 API calls

### 3.5 Optimized Transaction Log (`OptimizedTransactionLog`)
- **Factory Pattern**: Automatic selection via `TransactionLogFactory`
- **Advanced Optimizations**:
  - Backward listing optimization
  - Incremental checksums
  - Async updates with staleness tolerance
  - Streaming checkpoint creation
- **Thread Pool Management**: Dedicated pools for different operation types

### 3.6 Skipped Files Management
- **Cooldown Tracking**: Prevents repeated failures on corrupted files
- **Retry Logic**: Automatic retry after cooldown expiration
- **Transaction Log Integration**: SkipAction persistence
- **Operational Safety**: Skipped files never marked as "removed"

---

## 4. Scan Planning & Execution (25-30 pages)

### 4.1 ScanBuilder (`IndexTables4SparkScanBuilder`)
- **Push Down Interfaces**:
  - `SupportsPushDownFilters`: Standard Spark filter pushdown
  - `SupportsPushDownV2Filters`: V2 predicate pushdown
  - `SupportsPushDownRequiredColumns`: Column pruning
  - `SupportsPushDownLimit`: Limit pushdown
  - `SupportsPushDownAggregates`: Aggregate pushdown (COUNT/SUM/AVG/MIN/MAX)

### 4.2 Filter Pushdown Logic
- **Field Type Awareness**: String vs Text field handling
- **Supported Filter Types**: EqualTo, In, StringContains, range filters
- **Fast Field Validation**: Ensures aggregate compatibility
- **IndexQuery Expression Handling**: Native Tantivy query integration

### 4.3 Aggregate Pushdown System
- **Simple Aggregates**: COUNT, SUM, AVG, MIN, MAX without GROUP BY
- **GROUP BY Aggregates**: Multi-dimensional aggregation with Tantivy
- **Transaction Log Optimization**: Metadata-based COUNT for partition-only filters
- **Fast Field Requirements**: Validation and error messaging
- **Auto-Fast-Field Configuration**: Automatic numeric/date field configuration

### 4.4 Scan Types
- **IndexTables4SparkScan**: Standard data scanning with filters
- **IndexTables4SparkSimpleAggregateScan**: Simple aggregations
- **IndexTables4SparkGroupByAggregateScan**: Grouped aggregations
- **TransactionLogCountScan**: Metadata-optimized counting

### 4.5 Data Skipping Optimization
- **Partition Pruning**: `PartitionPruning` utility for partition-aware filtering
- **Min/Max Filtering**: Range-based file skipping using footer metadata
- **Unified Architecture**: Same data skipping across all scan types
- **Schema Awareness**: Field type detection for proper date handling

---

## 5. Write Operations (20-25 pages)

### 5.1 Write Builder (`IndexTables4SparkWriteBuilder`)
- **Write Mode Support**:
  - `SupportsTruncate`: Overwrite entire table
  - `SupportsOverwrite`: Filter-based overwrite (future: replaceWhere)
- **Write Strategy Selection**: Standard vs Optimized write paths

### 5.2 Standard Write (`IndexTables4SparkStandardWrite`)
- **Direct Execution**: No RequiresDistributionAndOrdering
- **Partition Handling**: Manual partition awareness
- **Use Cases**: Simple writes, compatibility mode

### 5.3 Optimized Write (`IndexTables4SparkOptimizedWrite`)
- **Auto-Sizing**: Intelligent DataFrame repartitioning based on historical data
- **Target Records Per Split**: Configurable split sizing
- **DataFrame Counting**: Conditional counting only when auto-sizing enabled
- **Historical Analysis**: `SplitSizeAnalyzer` for bytes-per-record calculation
- **Size Format Support**: Human-readable sizes (100M, 2G, 512K)

### 5.4 Batch Write Coordination (`IndexTables4SparkBatchWrite`)
- **Distributed Write Execution**: Executor-based split creation
- **Writer Factory**: Per-partition writer instantiation
- **Commit Protocol**: Atomic transaction log updates
- **Hadoop Configuration Propagation**: Config distribution to executors

### 5.5 Split Creation (`IndexTables4SparkWriterFactory` / Writer)
- **Tantivy Index Creation**: Schema mapping and field configuration
- **Batch Document Processing**: Efficient document batching
- **Footer Offset Metadata**: SplitMetadata generation for fast access
- **S3 Upload Optimization**: Parallel streaming uploads for large files

---

## 6. Partition Support (15-20 pages)

### 6.1 Partitioning Architecture
- **V2 API Partitioning**: Transform-based partition column injection
- **Partition Column Storage**: Transaction log metadata persistence
- **Physical Layout**: Directory-based partition organization
- **Partition Column Indexing**: V2 API indexes partition columns (V1 did not)

### 6.2 Partition Pruning (`PartitionPruning` utility)
- **Filter Analysis**: Partition vs data column separation
- **Pruning Logic**: Early file elimination based on partition values
- **Performance Impact**: Dramatic reduction in files scanned
- **Integration**: Used in all scan types (regular, aggregate, count)

### 6.3 Partition-Aware Operations
- **MERGE SPLITS with WHERE**: Partition-scoped split consolidation
- **Transaction Log COUNT**: Partition-aware counting optimization
- **GROUP BY Partitions**: Metadata-based grouped counting

---

## 7. IndexQuery System (20-25 pages)

### 7.1 IndexQuery Expression Model
- **IndexQueryExpression**: Field-specific Tantivy queries
- **IndexQueryAllExpression**: Cross-field virtual column search
- **SQL Parser Integration**: `indexquery` operator support
- **Function Registration**: `tantivy4spark_indexquery()` UDFs

### 7.2 V2 IndexQuery Expression Rule (`V2IndexQueryExpressionRule`)
- **Catalyst Integration**: Resolution rule injection
- **Expression Detection**: IndexQuery extraction from logical plan
- **Relation-based Storage**: WeakHashMap for IndexQuery tracking
- **ThreadLocal Management**: Relation object propagation to ScanBuilder

### 7.3 IndexQuery Cache Architecture
- **Schema-Based Keys**: Stable cache keys using schema hash
- **Relation Isolation**: Different schemas generate different cache keys
- **Multi-Table Support**: Natural isolation for JOIN queries
- **Simplified Implementation**: Eliminated execution ID and path extraction complexity

### 7.4 Filter Conversion (`FiltersToQueryConverter`)
- **Spark Filter to Tantivy Query**: Comprehensive filter mapping
- **Query Type Generation**:
  - `SplitMatchAllQuery`: No filters
  - `SplitTermQuery`: Exact matching
  - `SplitPhraseQuery`: Phrase matching
  - `SplitBooleanQuery`: Complex boolean logic
- **Field Validation**: Schema-based field existence checking

---

## 8. Storage & Caching (20-25 pages)

### 8.1 Split Cache Manager
- **JVM-Wide Singleton**: Shared cache across all Spark executors
- **Host-Based Locality**: Executor hostname tracking for task scheduling
- **Cache Configuration**: Size limits, concurrent loads, query caching
- **Batch Document Retrieval**: Performance optimization for multi-document reads

### 8.2 Broadcast Split Locality Manager
- **Cluster-Wide Tracking**: Broadcast variables for cache locality
- **Executor Registration**: Automatic host detection and registration
- **Task Scheduling Integration**: Preferred locations for cache-aware scheduling
- **Lifecycle Management**: Registration during split creation, updates during reads

### 8.3 Storage Strategies
- **S3 Optimized Reader**: Direct S3 access with retry logic and session tokens
- **Standard File Reader**: Local/HDFS access with standard Hadoop FileSystem
- **Protocol-Based Selection**: Automatic strategy choice based on URI scheme

### 8.4 S3 Upload Optimization
- **Parallel Streaming Uploads**: Multi-threaded uploads with buffered chunking
- **Multipart Upload Strategy**: Automatic single-part vs multipart selection
- **Configurable Concurrency**: Thread pool management
- **Memory Safety**: Buffer queue management prevents OOM

### 8.5 Cloud Storage Providers
- **Factory Pattern**: `CloudStorageProviderFactory` for provider selection
- **S3 Provider**: AWS SDK integration with session token support
- **Hadoop Provider**: Fallback for all Hadoop-compatible filesystems
- **Custom Credential Providers**: Enterprise integration via reflection

---

## 9. SQL Extensions & Commands (15-20 pages)

### 9.1 Spark Session Extensions (`IndexTables4SparkExtensions`)
- **Parser Injection**: Custom SQL syntax handling
- **Function Registration**: UDF integration
- **Resolution Rules**: Catalyst rule injection
- **Configuration**: Session extension setup

### 9.2 MERGE SPLITS Command (`MergeSplitsCommand`)
- **Purpose**: Split file consolidation to reduce small file overhead
- **Syntax**: `MERGE SPLITS '<path>' TARGET SIZE <size> [MAX GROUPS <n>] [WHERE <filters>]`
- **Operation**: Distributed merge using tantivy4java SplitMerger
- **Bin Packing**: Intelligent grouping to achieve target sizes
- **Atomic Commits**: REMOVE+ADD transaction patterns
- **Skipped Files Handling**: Robust error handling with cooldown tracking

### 9.3 Cache Management Commands
- **FLUSH INDEXTABLES CACHE**: Clear executor-side split caches
- **INVALIDATE TRANSACTION LOG CACHE**: Clear driver-side transaction log caches
- **Use Cases**: Debugging, testing, manual cache control

### 9.4 Custom SQL Parser (`IndexTables4SparkSqlParser`)
- **Operator Overloading**: `indexquery` operator implementation
- **ANTLR Grammar**: Custom grammar for IndexTables4Spark extensions
- **AST Builder**: Parse tree to command/expression conversion

---

## 10. Configuration System (15-20 pages)

### 10.1 Configuration Architecture
- **IndexTables4SparkOptions**: Type-safe configuration wrapper
- **IndexTables4SparkConfig**: Metadata-based config with defaults
- **IndexTables4SparkSQLConf**: SQL-level configurations
- **ConfigUtils**: Helper utilities for config extraction

### 10.2 Configuration Categories
- **Index Writer**: Heap size, batch size, threads, temporary directories
- **Cache**: Size limits, prewarm, doc batching, directory paths
- **S3 Upload**: Concurrency, part size, streaming threshold
- **Transaction Log**: Checkpoint interval, parallelism, retention policies
- **Auto-Sizing**: Target split size, row count hints
- **Aggregate**: Fast field requirements
- **Skipped Files**: Tracking, cooldown duration

### 10.3 Configuration Normalization (`ConfigNormalization`)
- **Prefix Mapping**: `spark.indextables.*` and legacy prefix handling
- **Source Extraction**: Hadoop config, Spark config, options extraction
- **Merged Configuration**: Proper precedence handling

### 10.4 Working Directory Configuration
- **Automatic /local_disk0 Detection**: Databricks and EMR optimization
- **Custom Directories**: Index creation, split merge, cache storage
- **Path Validation**: Existence and writability checks
- **Automatic Fallback**: System temp directory when custom paths fail

---

## 11. Performance Optimizations (20-25 pages)

### 11.1 Auto-Sizing System
- **Historical Analysis**: `SplitSizeAnalyzer` reads past split data
- **Bytes-per-Record Calculation**: Weighted average across splits
- **Dynamic Repartitioning**: DataFrame coalesce/repartition based on target
- **Row Count Configuration**: Explicit row count recommended for optimal partitioning
- **Test Coverage**: 28/28 tests passing

### 11.2 Transaction Log Performance
- **Checkpoint Compaction**: 60% read time improvement
- **Parallel I/O**: Configurable thread pools for concurrent file reading
- **Caching Strategy**: Multi-level caches with proper invalidation
- **Pre-warm Optimization**: Aggressive cache population on table initialization

### 11.3 Data Skipping
- **Partition Pruning**: Early elimination of irrelevant partitions
- **Min/Max Filtering**: Range-based file skipping
- **Schema-Aware Skipping**: Proper field type detection (especially dates)
- **Unified Architecture**: Same logic across all scan types

### 11.4 Cache Locality Optimization
- **Preferred Locations**: Task scheduling hints based on cache presence
- **Broadcast Variables**: Cluster-wide cache tracking
- **Pre-Warm Manager**: Proactive cache warming before query execution
- **Executor Affinity**: Host-based task assignment

### 11.5 Aggregate Pushdown Performance
- **10-100x Speedup**: Native Tantivy aggregation vs Spark processing
- **Transaction Log COUNT**: Zero data access for partition-only queries
- **Memory Efficiency**: Aggregation results only, no document transfer

---

## 12. Error Handling & Resilience (10-15 pages)

### 12.1 Protocol Versioning
- **ProtocolVersion System**: Reader/writer compatibility checking
- **Auto-Upgrade**: Configurable protocol version upgrades
- **Feature Flags**: Extensible feature support mechanism
- **Legacy Table Support**: Backward compatibility with version 1 tables

### 12.2 Skipped Files Management
- **Cooldown Mechanism**: Prevents repeated processing of corrupted files
- **Retry Logic**: Automatic retry after cooldown expiration
- **Transaction Log Tracking**: Persistent skip history with timestamps
- **Operational Safety**: Files remain accessible during cooldown

### 12.3 Validation & Error Messages
- **Fast Field Validation**: Descriptive errors for missing fast field configuration
- **GROUP BY Validation**: Clear error messages for unsupported aggregations
- **Schema Validation**: Field existence and type checking
- **Configuration Validation**: Early detection of invalid settings

### 12.4 Graceful Degradation
- **Checkpoint Failure Handling**: Write operations continue on checkpoint failures
- **Cleanup Failure Handling**: Operations continue if cleanup fails
- **Cache Failure Handling**: Queries work without cache optimization
- **Pre-Warm Failure Handling**: Query execution continues if pre-warm fails

---

## 13. Integration Points (10-15 pages)

### 13.1 Tantivy4Java Integration
- **SplitSearchEngine**: Primary interface to tantivy4java
- **Schema Conversion**: Spark StructType to Tantivy schema mapping
- **Document Conversion**: InternalRow to Tantivy document mapping
- **Query Conversion**: Spark filters to Tantivy queries
- **Aggregation Integration**: Tantivy aggregation API usage

### 13.2 Spark Catalyst Integration
- **Custom Expressions**: IndexQueryExpression, IndexQueryAllExpression
- **Resolution Rules**: V2IndexQueryExpressionRule for expression rewriting
- **Filter Conversion**: CatalystToSparkFilterConverter for predicate handling
- **Optimizer Integration**: Data skipping and aggregate pushdown hooks

### 13.3 Spark Execution Integration
- **Partition Planning**: InputPartition creation with locality hints
- **Reader Factory**: PartitionReaderFactory for executor-side reading
- **Broadcast Variables**: Split locality and configuration distribution
- **Task Scheduling**: Preferred locations for cache-aware execution

---

## 14. Testing & Validation (5-10 pages)

### 14.1 Test Coverage Summary
- **Total Tests**: 210+ passing, 0 failing
- **Core Features**:
  - IndexQuery: 49/49 tests
  - IndexQueryAll: 44/44 tests
  - MergeSplits: 9/9 tests
  - Aggregate Pushdown: 14/14 tests
  - Partitioned Datasets: 7/7 tests
- **Performance Tests**: 6/6 checkpoint performance tests
- **Integration Tests**: Real S3 credential provider tests (4/4 passing)

### 14.2 Test Organization
- **Unit Tests**: Component-level validation
- **Integration Tests**: End-to-end workflows
- **Performance Tests**: Benchmark validation
- **Regression Tests**: Known issue prevention

---

## 15. Usage Examples (15-20 pages)

### 15.1 Basic Write Operations
- V2 API write examples (recommended)
- Partitioned writes
- Auto-sizing configuration
- V1 API deprecation notice

### 15.2 Read & Search Operations
- Standard DataFrame operations
- IndexQuery syntax
- Filter pushdown examples
- Aggregate pushdown examples

### 15.3 Advanced Operations
- MERGE SPLITS execution
- Custom credential providers
- Performance tuning
- Cache management

### 15.4 Configuration Examples
- Databricks optimization
- EMR optimization
- On-premises high-performance storage
- Memory filesystem configuration

---

## 16. Appendices (10-15 pages)

### 16.1 Configuration Reference
- Complete list of all configuration properties
- Default values
- Valid ranges and formats
- Deprecation notices

### 16.2 SQL Command Reference
- Complete syntax for all custom SQL commands
- Parameter descriptions
- Usage examples

### 16.3 API Reference
- Key classes and interfaces
- Method signatures
- Return types

### 16.4 Performance Tuning Guide
- Environment-specific recommendations
- Configuration hierarchy best practices
- Monitoring and troubleshooting

---

## Estimated Total Documentation: 250-300 pages

---

## Documentation Delivery Format

1. **Master Document**: Single comprehensive PDF/Markdown document
2. **Section Modules**: Individual markdown files per major section
3. **Code Examples**: Executable Scala code snippets with expected output
4. **Architecture Diagrams**: High-level visual representations (Mermaid or PlantUML)
5. **API Reference**: Generated Scaladoc integration

---

## Implementation Notes

### Key Reference Files for Documentation

Based on code analysis, the following source files will be primary references:

#### Core DataSource Implementation
- `IndexTables4SparkTableProvider.scala` - V2 API entry point (primary focus)
- `IndexTables4SparkTable.scala` - V2 table capabilities
- `IndexTables4SparkDataSource.scala` - V1 API implementation (deprecated, mentioned for migration only)
- `IndexTables4SparkRelation.scala` - V1 relation implementation (deprecated, mentioned for migration only)

#### Scan & Read Path
- `IndexTables4SparkScanBuilder.scala` - Scan planning and pushdown
- `IndexTables4SparkScan.scala` - Standard scan execution
- `IndexTables4SparkSimpleAggregateScan.scala` - Simple aggregate execution
- `IndexTables4SparkGroupByAggregateScan.scala` - GROUP BY aggregate execution
- `TransactionLogCountScan.scala` - Optimized COUNT execution

#### Write Path
- `IndexTables4SparkWriteBuilder.scala` - Write mode selection
- `IndexTables4SparkStandardWrite.scala` - Standard write execution
- `IndexTables4SparkOptimizedWrite.scala` - Optimized write with auto-sizing
- `IndexTables4SparkBatchWrite.scala` - Batch coordination

#### Transaction Log
- `TransactionLog.scala` - Core transaction log implementation
- `TransactionLogFactory.scala` - Factory for log selection
- `OptimizedTransactionLog.scala` - Performance-optimized implementation
- `TransactionLogCheckpoint.scala` - Checkpoint management
- `TransactionLogCache.scala` - Multi-level caching
- `Actions.scala` - Transaction log action types

#### Storage & Caching
- `SplitManager.scala` - Split cache management
- `BroadcastSplitLocalityManager.scala` - Cluster-wide locality tracking
- `S3OptimizedReader.scala` - S3-specific optimizations
- `CloudStorageProviderFactory.scala` - Storage provider selection

#### Extensions & Commands
- `IndexTables4SparkExtensions.scala` - Spark session extensions
- `MergeSplitsCommand.scala` - MERGE SPLITS implementation
- `IndexTables4SparkSqlParser.scala` - Custom SQL parser
- `V2IndexQueryExpressionRule.scala` - Catalyst integration

#### Configuration
- `IndexTables4SparkOptions.scala` - Type-safe options
- `IndexTables4SparkConfig.scala` - Configuration framework
- `ConfigNormalization.scala` - Config extraction utilities

---

## Next Steps

1. **Section Generation**: Begin with Architecture Overview and work through sections sequentially
2. **Diagram Creation**: Generate Mermaid/PlantUML diagrams for key architectural concepts
3. **Code Example Extraction**: Pull real examples from test suite for usage documentation
4. **API Documentation**: Generate Scaladoc for public interfaces and key classes
5. **Review & Refinement**: Technical review with subject matter experts

---

**Document Status**: Planning Complete
**Ready for Implementation**: Yes
**Estimated Timeline**: 3-4 weeks for complete documentation suite
