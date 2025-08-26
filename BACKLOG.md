# Tantivy4Spark Development Backlog

## High Priority Features

### ReplaceWhere with Partition Predicates
**Status**: Design Complete  
**Design Document**: [docs/REPLACE_WHERE_DESIGN.md](docs/REPLACE_WHERE_DESIGN.md)  
**Description**: Implement Delta Lake-style replaceWhere functionality for selective partition replacement based on predicate conditions.

**Key Features**:
- Selective partition replacement using SQL predicates
- Support for `=`, `IN`, and `AND` operators on partition columns
- Transaction log integration with ReplaceWhereAction
- Comprehensive validation and error handling
- Full test coverage with integration tests

**Implementation Phases**:
1. Core infrastructure (predicate parser, transaction log methods)
2. DataSource integration (writer, validation)
3. Advanced features (IN predicates, comprehensive error handling)
4. Testing and documentation

**Priority**: High - Enables advanced data management patterns

---

### Transaction Log Compaction
**Status**: Design Complete  
**Design Document**: [docs/LOG_COMPACTION_DESIGN.md](docs/LOG_COMPACTION_DESIGN.md)  
**Description**: Implement Delta Lake-style checkpoint system to consolidate transaction log JSON files into Parquet checkpoints for improved metadata performance.

**Key Features**:
- Periodic checkpoint creation from transaction JSON files
- Parquet-based checkpoint format with configurable compression
- Automatic cleanup of old transaction files
- Graceful fallback to JSON replay if checkpoints fail
- Significant performance improvements for tables with many transactions

**Performance Benefits**:
- 10-100x faster cold start times for tables with many transactions
- Reduced memory usage and I/O operations
- Faster query planning through consolidated metadata

**Implementation Phases**:
1. Basic checkpointing (creation and Parquet serialization)
2. Checkpoint reading (deserialization and incremental transaction replay)
3. Cleanup and optimization (old file removal, configuration options)
4. Advanced features (multi-part checkpoints, background creation)

**Priority**: High - Critical for production scale with many writes

---

## Medium Priority Features

### Enhanced Query Optimization
- **Bloom Filters**: Implement bloom filters for better file skipping
- **Column Statistics**: Enhanced min/max tracking for better pruning
- **Join Optimization**: Better integration with Spark's join planning

### Storage Optimizations
- **Compression Improvements**: Evaluate additional compression codecs
- **Caching Enhancements**: Smarter cache eviction policies
- **Async I/O**: Background prefetching for better performance

### Monitoring and Observability
- **Metrics Dashboard**: Comprehensive performance metrics
- **Query Profiling**: Detailed query execution statistics
- **Health Checks**: Automated table integrity validation

## Low Priority Features

### Advanced Text Search
- **Fuzzy Search**: Approximate string matching
- **Synonym Support**: Query expansion with synonyms
- **Multi-language**: Enhanced tokenization for different languages

### Schema Evolution
- **Column Addition**: Support for adding new columns
- **Type Changes**: Safe data type evolution
- **Column Removal**: Deprecation and removal workflows

### Ecosystem Integration
- **Databricks Integration**: Certified compatibility
- **Apache Iceberg**: Interoperability layer
- **Apache Hudi**: Cross-format compatibility

---

## Completed Features ✅

### Core Transaction Log
- ✅ Delta Lake-style transaction log with ADD/REMOVE actions
- ✅ Overwrite and append modes with atomic operations
- ✅ Partition tracking and pruning integration
- ✅ JSON serialization with proper type handling
- ✅ Comprehensive test coverage

### Storage and Performance
- ✅ S3-optimized storage with path flattening
- ✅ Split-based architecture with immutable index files
- ✅ JVM-wide caching with SplitCacheManager
- ✅ AWS session token support for temporary credentials

### Error Handling and Validation
- ✅ Proper exception throwing for missing tables
- ✅ Schema validation and type safety
- ✅ Comprehensive error messages and debugging
- ✅ Production-ready logging (removed debug noise)

---

## Notes
- **Design First**: All major features should have design documents before implementation
- **Test Coverage**: Maintain 100% test pass rate with comprehensive integration tests
- **Performance**: Benchmark all major features against baseline
- **Documentation**: Keep CLAUDE.md updated with implementation details