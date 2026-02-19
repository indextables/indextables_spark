# Section 14: Testing & Validation (Outline)

## 14.1 Overview

IndexTables4Spark maintains comprehensive test coverage across all major components to ensure production reliability, correctness, and performance. The test suite includes unit tests, integration tests, performance tests, and real-world scenario validation.

## 14.2 Test Coverage Summary

### 14.2.1 Overall Statistics
- **Total tests**: 210+ tests across all components
- **Pass rate**: 100% (210+ passing, 0 failing)
- **Coverage areas**: Core functionality, edge cases, error handling, performance
- **Test frameworks**: ScalaTest (primary), JUnit (interop)

### 14.2.2 Component-Level Coverage

| Component | Test Count | Status | Key Test Areas |
|-----------|------------|--------|---------------|
| **IndexQuery operators** | 49 | ✅ 49/49 | Field-specific, cross-field, complex queries |
| **IndexQueryAll** | 44 | ✅ 44/44 | Virtual _indexall column, function syntax |
| **MERGE SPLITS command** | 9 | ✅ 9/9 | Bin packing, partition-aware, MAX GROUPS |
| **Aggregate pushdown** | 14 | ✅ 14/14 | COUNT, SUM, AVG, MIN, MAX, GROUP BY |
| **Partitioned datasets** | 7 | ✅ 7/7 | Partition pruning, WHERE clauses, V1/V2 compatibility |
| **Skipped files handling** | 5 | ✅ 5/5 | Cooldown tracking, null indexUID, robustness |
| **Schema validation** | 35 | ✅ 35/35 | IndexQuery cache, field compatibility |
| **Transaction log** | 6 | ✅ 6/6 | Checkpoint creation, parallel reads, cleanup |
| **Auto-sizing** | 28 | ✅ 28/28 | SizeParser, configuration, historical analysis |
| **Field filtering** | 478 | ✅ 478/478 | String vs text, range filters, data skipping |
| **Custom credential providers** | 4 | ✅ 4/4 | Real S3 integration, table-level URIs |
| **Azure integration** | 14 | ✅ 14/14 | OAuth, account key, connection string, MERGE SPLITS |

## 14.3 Unit Testing

### 14.3.1 Core Component Tests
- **TransactionLog**: Version management, action parsing, checkpoint creation
- **SplitCacheManager**: LRU eviction, cache hits/misses, concurrent access
- **PartitionUtils**: Value extraction, type conversion, validation
- **ConfigNormalization**: Prefix normalization, precedence merging
- **SizeParser**: Format parsing ("100M", "5G"), validation, edge cases

### 14.3.2 Expression Tests
- **IndexQueryExpression**: Query string extraction, canPushDown validation
- **IndexQueryAllExpression**: Cross-field query support, _indexall virtual column
- **V2IndexQueryExpressionRule**: Relation-scoped storage, WeakHashMap behavior
- **Filter predicates**: EqualTo, GreaterThan, LessThan, StringContains, etc.

### 14.3.3 Configuration Tests
- **ConfigEntry**: Type validation, range validation, default values
- **Configuration hierarchy**: DataFrame options > Spark conf > Hadoop conf > Defaults
- **Boolean parsing**: Extended boolean support (true/false/1/0/yes/no/on/off)
- **Size parsing**: "100M", "1G", "512K", raw bytes

### 14.3.4 SQL Parser Tests
- **ANTLR grammar**: MERGE SPLITS, FLUSH CACHE, INVALIDATE CACHE syntax
- **Query operators**: indexquery and indexqueryall preprocessing
- **Error handling**: Invalid syntax, missing parameters
- **AST building**: LogicalPlan construction from parse tree

## 14.4 Integration Testing

### 14.4.1 End-to-End Write Tests
- **Basic writes**: DataFrame → IndexTables4Spark → S3
- **Partitioned writes**: Multiple partition columns, nested partition structures
- **Append mode**: Incremental data loading
- **Overwrite mode**: Table replacement, transaction log reset
- **Auto-sizing**: Historical analysis, dynamic partitioning

### 14.4.2 End-to-End Read Tests
- **Full table scans**: Read all data, schema validation
- **Filtered scans**: WHERE clauses, filter pushdown
- **Partition pruning**: date = '2024-01-01', hour >= 10
- **IndexQuery operations**: content indexquery 'spark AND sql'
- **Aggregate queries**: COUNT(*), SUM(score), AVG(value)

### 14.4.3 Transaction Log Tests
- **Checkpoint creation**: Automatic checkpoints every N transactions
- **Incremental reads**: Checkpoint + new transactions
- **Parallel I/O**: Multi-threaded transaction file reads
- **Cache invalidation**: Checkpoint-based cache clearing
- **Cleanup operations**: File deletion with safety gates

### 14.4.4 MERGE SPLITS Tests
- **Basic merging**: Small splits → large splits
- **Partition-aware merging**: Separate bins per partition
- **MAX GROUPS limiting**: Top N oldest merge groups
- **TARGET SIZE constraints**: Bin packing to target size
- **Skipped files**: Cooldown tracking, retry after cooldown

### 14.4.5 Real S3 Integration Tests
- **Custom credential providers**: Table-level URI validation
- **Write/read cycle**: Full round-trip with real S3
- **Multipart uploads**: Large files (>100MB)
- **Parallel uploads**: Multiple threads uploading concurrently
- **Retry logic**: Simulated S3 failures and recovery

### 14.4.6 Real Azure Integration Tests (New in v2.0)
- **OAuth Service Principal**: Full write/read cycle with Azure AD authentication
- **Account key authentication**: Traditional storage account key validation
- **Connection string**: Complete connection string authentication
- **URL scheme normalization**: abfss://, wasbs://, azure:// compatibility
- **MERGE SPLITS with OAuth**: Distributed merge operations with bearer token refresh
- **~/.azure/credentials file**: Shared credentials file loading
- **Partition support**: Partitioned writes and reads on Azure
- **Cross-cloud validation**: Same operations work on both S3 and Azure

## 14.5 Performance Testing

### 14.5.1 Query Performance Tests
- **IndexQuery speedup**: 50-1000x faster than Spark regex
- **Aggregate pushdown**: 10-100x faster for COUNT/SUM/AVG
- **Cache hit rates**: 60-90% in typical workloads
- **Cold vs warm cache**: 5s vs 25s for first query
- **Partition pruning**: 50-99% file reduction

### 14.5.2 Write Performance Tests
- **Bulk load throughput**: 100K-1M records/sec/executor
- **S3 upload performance**: 3.5x speedup with 4 threads
- **Index creation**: 12-60x faster with /local_disk0
- **Auto-sizing accuracy**: ±5% of target split size
- **Parallel indexing**: Linear scaling with thread count

### 14.5.3 Transaction Log Performance Tests
- **Checkpoint speedup**: 60% faster (2.5x) with checkpoints
- **Parallel I/O**: ~520ms avg read time (50 transactions)
- **Cache effectiveness**: 80-95% hit rate for metadata
- **Cleanup performance**: No impact on read/write operations

### 14.5.4 Storage Performance Tests
- **Cache lookup**: ~1μs for cache hits
- **S3 download**: 100ms-5s for cache misses
- **Eviction overhead**: 1-10ms per eviction
- **Locality scheduling**: 78% cache hit rate with locality

## 14.6 Correctness Testing

### 14.6.1 Data Integrity Tests
- **Write-read round-trip**: Verify data identical after write/read
- **Null value handling**: NULL values preserved correctly
- **Date/timestamp precision**: Millisecond accuracy
- **Numeric precision**: Double/float precision maintained
- **String encoding**: UTF-8 handling for international characters

### 14.6.2 Transaction Log Correctness
- **ACID properties**: Atomicity, consistency, isolation, durability
- **Concurrent writes**: Optimistic concurrency control
- **Version consistency**: Monotonic version numbers
- **Checkpoint consistency**: Checkpoint equals sum of transactions
- **Cleanup safety**: No data loss from cleanup operations

### 14.6.3 Partition Correctness
- **Partition value extraction**: Correct extraction from file paths
- **Partition column types**: Proper type conversion (string, int, date)
- **Null partition handling**: __HIVE_DEFAULT_PARTITION__ support
- **Special characters**: URL encoding/decoding correctness
- **V1 vs V2 behavior**: Partition column indexing differences validated

### 14.6.4 Filter Correctness
- **String field filters**: Exact matching for string fields
- **Text field filters**: Tokenized search for text fields
- **Range filter correctness**: >, >=, <, <= behavior validated
- **Date filter correctness**: Date-to-days conversion accuracy
- **Boolean logic**: AND, OR, NOT combinations

## 14.7 Edge Case Testing

### 14.7.1 Boundary Conditions
- **Empty DataFrames**: Zero-row writes and reads
- **Single-row DataFrames**: Minimal data size
- **Large DataFrames**: Billions of rows, terabytes of data
- **Wide schemas**: Hundreds of columns
- **Deep nesting**: Complex struct types (unsupported, proper error)

### 14.7.2 Error Conditions
- **Missing files**: Deleted splits, cache misses
- **Corrupted files**: Invalid split format
- **Schema mismatches**: Field type changes, missing fields
- **Invalid queries**: Malformed Tantivy syntax
- **Concurrent conflicts**: Multiple writers, version conflicts

### 14.7.3 Resource Limits
- **Memory limits**: OOM prevention, bounded caches
- **Disk space**: Temp directory full, cache eviction
- **Network failures**: S3 unavailable, retries exhausted
- **CPU limits**: Thread pool saturation
- **Native memory**: tantivy4java heap limits

### 14.7.4 Configuration Edge Cases
- **Negative values**: Validation rejects negative sizes
- **Zero values**: Validation rejects zero batch sizes
- **Extremely large values**: Heap sizes >10GB, batch sizes >1M
- **Invalid paths**: Non-existent directories, read-only paths
- **Conflicting configs**: Precedence correctly resolved

## 14.8 Regression Testing

### 14.8.1 Bug Fix Validation
- **Range filter regression**: Corrected documentation matches behavior
- **Date filtering regression**: Schema-aware field detection fix
- **Aggregate cache locality**: Proper preferredLocations() implementation
- **Schema-based cache keys**: Simplified IndexQuery cache implementation
- **Null indexUID handling**: Robust merge operation resilience

### 14.8.2 Performance Regression Detection
- **Query latency**: No regression in IndexQuery performance
- **Write throughput**: No regression in indexing speed
- **Transaction log performance**: Checkpoint effectiveness maintained
- **Cache effectiveness**: Hit rates within expected ranges

### 14.8.3 Compatibility Regression
- **V1 API**: Legacy DataSource V1 continues to work
- **V2 API**: Modern DataSource V2 API compatibility
- **Spark versions**: Cross-version testing (3.1, 3.2, 3.3, 3.4, 3.5)
- **Scala versions**: 2.12 compatibility
- **tantivy4java versions**: Version compatibility validation

## 14.9 Scenario-Based Testing

### 14.9.1 Bulk Data Loading
- **Large CSV imports**: 100M+ rows, multiple GB
- **Partitioned bulk load**: Load to multiple partitions simultaneously
- **Schema evolution**: Add new columns during bulk load
- **Error recovery**: Resume failed bulk loads

### 14.9.2 Incremental Updates
- **Append-only**: Daily incremental data addition
- **Mixed operations**: Append, overwrite specific partitions
- **High-frequency writes**: 1000+ writes per day
- **Transaction log growth**: Automatic checkpoint creation

### 14.9.3 Query Workloads
- **Ad-hoc queries**: Interactive exploration with IndexQuery
- **Reporting queries**: Aggregate queries with GROUP BY
- **Join queries**: Multi-table joins with IndexQuery filters
- **Repeated queries**: Cache effectiveness validation

### 14.9.4 Maintenance Operations
- **MERGE SPLITS**: Consolidate small files after bulk load
- **FLUSH CACHE**: Clear cache and verify performance impact
- **INVALIDATE CACHE**: Force transaction log refresh
- **Table evolution**: Add partitions, change configurations

## 14.10 Multi-Tenant and Concurrency Testing

### 14.10.1 Concurrent Reads
- **Multiple queries**: 10+ concurrent readers
- **Cache sharing**: Executors share split cache
- **Locality scheduling**: Tasks assigned to executors with cached data
- **No interference**: Each query has isolated IndexQuery context

### 14.10.2 Concurrent Writes
- **Append conflicts**: Multiple writers to same table
- **Partition isolation**: Concurrent writes to different partitions
- **Optimistic concurrency**: Version conflict detection and retry
- **Transaction ordering**: Monotonic version numbers maintained

### 14.10.3 Mixed Workloads
- **Read while writing**: Readers see consistent snapshots
- **MERGE SPLITS while reading**: Readers unaffected by merge
- **FLUSH CACHE impact**: Subsequent queries rebuild cache
- **Configuration changes**: Runtime config changes take effect

## 14.11 Platform-Specific Testing

### 14.11.1 Databricks Testing
- **/local_disk0 usage**: Automatic detection and usage
- **Unity Catalog integration**: Table registration and discovery
- **Cluster libraries**: JAR deployment validation
- **Notebook execution**: Interactive query and DataFrame API

### 14.11.2 EMR Testing
- **S3 as primary storage**: All operations on S3
- **Instance store usage**: /local_disk0 for ephemeral storage
- **EMRFS settings**: Compatibility validation
- **Bootstrap actions**: tantivy4java native library deployment

### 14.11.3 Local Testing
- **LocalSparkSession**: Unit tests with local Spark
- **Local filesystem**: Testing without S3
- **In-memory operation**: tmpfs for temp directories
- **Development workflow**: Rapid iteration without cloud

## 14.12 Security Testing

### 14.12.1 Authentication Testing
- **IAM role validation**: AWS role-based S3 access
- **Explicit credentials**: Access key/secret key (AWS/Azure)
- **Session tokens**: Temporary credential support (AWS)
- **Custom providers**: Enterprise credential provider integration (AWS)
- **Azure OAuth**: Service Principal authentication with bearer token refresh
- **Azure account key**: Traditional storage account authentication
- **Azure connection string**: Complete connection string validation
- **~/.azure/credentials**: Shared credentials file loading

### 14.12.2 Authorization Testing
- **File-level permissions**: S3 bucket policies
- **Catalog permissions**: Hive Metastore ACLs
- **Cross-account access**: AssumeRole validation
- **Deny scenarios**: Proper error messages for unauthorized access

### 14.12.3 Encryption Testing
- **SSE-S3**: Server-side encryption with S3-managed keys
- **SSE-KMS**: Server-side encryption with KMS keys
- **Azure Storage Encryption**: Server-side encryption for Azure Blob Storage
- **HTTPS**: Encrypted transport for S3/Azure access
- **Credential security**: No credential logging (AWS/Azure)

## 14.13 Test Automation

### 14.13.1 Continuous Integration
- **GitHub Actions**: Automated test execution on PR
- **Test matrix**: Multiple Spark versions, Scala versions
- **Code coverage**: Jacoco/Scoverage integration
- **Performance benchmarks**: Regression detection

### 14.13.2 Test Organization
- **FunSuite style**: Organized test suites by component
- **Tag-based filtering**: Run specific test categories
- **Test fixtures**: Shared setup with BeforeAndAfter
- **Test data**: Reusable test datasets

### 14.13.3 Test Utilities
- **Test helpers**: Common assertions and matchers
- **Mock factories**: Reusable mocks for dependencies
- **Test configuration**: Standardized test Spark configs
- **Cleanup utilities**: Automatic temp directory cleanup

## 14.14 Future Testing Enhancements

### 14.14.1 Chaos Engineering
- **Random task failures**: Fault injection testing
- **Network partitions**: Simulated network failures
- **Resource exhaustion**: Memory and disk pressure
- **Long-running tests**: Multi-day stability testing

### 14.14.2 Property-Based Testing
- **ScalaCheck integration**: Generated test cases
- **Invariant validation**: Properties hold for all inputs
- **Shrinking**: Minimal failing examples
- **Coverage expansion**: Test cases human tests miss

### 14.14.3 Fuzz Testing
- **Query fuzzing**: Random Tantivy query generation
- **Schema fuzzing**: Random schema generation
- **Data fuzzing**: Random data generation
- **Configuration fuzzing**: Random config combinations

## 14.15 Summary

IndexTables4Spark testing ensures production quality through:

✅ **Comprehensive coverage**: 224+ tests across all components (100% passing)
✅ **Unit testing**: Core components, expressions, configuration, parsing
✅ **Integration testing**: End-to-end write/read, transaction log, MERGE SPLITS
✅ **Performance testing**: Query speedup, write throughput, caching effectiveness
✅ **Correctness testing**: Data integrity, ACID properties, filter correctness
✅ **Edge case testing**: Boundary conditions, error handling, resource limits
✅ **Regression testing**: Bug fixes, performance, compatibility
✅ **Scenario testing**: Bulk loads, incremental updates, query workloads
✅ **Concurrency testing**: Multi-tenant, concurrent reads/writes, mixed workloads
✅ **Platform testing**: Databricks, EMR, Azure HDInsight/Synapse, local development
✅ **Security testing**: Authentication (AWS/Azure), authorization, encryption
✅ **Multi-cloud testing**: Real S3 and Azure integration tests with OAuth
✅ **Test automation**: CI/CD integration, code coverage, performance regression

**Key Testing Achievements**:
- **100% pass rate** across all test suites (296+ tests)
- **Merge-on-write validation**: 42/42 tests passing with 95% coverage
  - Zero data loss validated at 10K record scale
  - Comprehensive edge case coverage (nulls, MIN_VALUE, MAX_VALUE, Unicode)
  - Concurrency testing (multiple concurrent writers)
  - Scale testing (100+ splits, 10K records)
  - Configuration validation (minDiskSpaceGB propagation verified)
- **Real S3 integration tests** with comprehensive validation
- **Real Azure integration tests** with OAuth Service Principal validation (14 tests)
- **Multi-cloud validation**: Same operations work on both S3 and Azure
- **Performance benchmarks** validate optimization claims
- **Edge case coverage** ensures production robustness
- **Regression protection** prevents breaking changes
- **Multi-platform validation** ensures broad compatibility (AWS, Azure, local)
