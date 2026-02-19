# IndexTables4Spark Development Backlog

## Blocking: Known Test Infrastructure Issues

### OOM running full test suite via `mvn test`
**Status**: Open
**Severity**: Blocking
**Reporter**: Scott
**Description**: `mvn test` runs out of memory (at least on laptops). Scott has a script (`run_tests_individually.sh`) on the `fix/string-field-query-and-companion-aggregate-guard` branch that runs each test class separately via `mvn scalatest:test -DwildcardSuites=...`. It currently runs sequentially but Scott reports running 4-wide in parallel works fine. The script should be checked into main and documented in CLAUDE.md as the recommended way to run the full suite.
**Reference**: https://github.com/indextables/indextables_spark/blob/fix/string-field-query-and-companion-aggregate-guard/run_tests_individually.sh

### JVM shutdown crashes in tantivy4java tests
**Status**: Open
**Severity**: Blocking
**Reporter**: Scott
**Upstream**: tantivy4java
**Description**: Several tantivy4java test cases crash on JVM shutdown after the tests pass. Root cause unknown. This is an upstream issue in the tantivy4java dependency, not in indextables_spark itself.

---

## Critical: LLM Documentation Fixes

> These issues cause LLM agents to generate broken code. Fix before other work.

### Fix IndexQuery usage in CLAUDE.md
**Status**: Open
**Severity**: Critical
**Description**: CLAUDE.md line 84 shows a wrong import path (`org.apache.spark.sql.indextables.IndexQueryExpression._`) and a DataFrame API syntax (`$"content" indexquery "query"`) that doesn't exist. All tests use SQL syntax. Fix to show correct SQL-based usage:
```scala
df.createOrReplaceTempView("table")
spark.sql("SELECT * FROM table WHERE content indexquery 'machine learning'")
```

### Fix extensions registration syntax in CLAUDE.md and sql-commands-reference.md
**Status**: Open
**Severity**: Critical
**Description**: Both docs show `spark.sparkSession.extensions.add(...)` which is not a valid Spark API. Correct approach (per all test files):
```scala
SparkSession.builder()
  .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
  .getOrCreate()
```
**Files**: CLAUDE.md line 107, docs/reference/sql-commands.md line 6

### Remove or annotate unwired data skipping SQL commands
**Status**: Open
**Severity**: Critical
**Description**: CLAUDE.md line 110 and docs/reference/sql-commands.md lines 325-333 document `DESCRIBE/FLUSH/INVALIDATE INDEXTABLES DATA SKIPPING` commands. The Command classes exist but are NOT wired into the ANTLR grammar — they cannot be invoked via `spark.sql(...)`. Either wire them into the parser or remove from docs.

---

## High Priority: Documentation Accuracy

### Document both class name aliases in CLAUDE.md
**Status**: Open
**Description**: README.md uses public aliases (`io.indextables.provider.IndexTablesProvider`, `io.indextables.extensions.IndexTablesSparkExtensions`). CLAUDE.md uses internal names (`io.indextables.spark.core.IndexTables4SparkTableProvider`). Both are valid. CLAUDE.md should acknowledge both and recommend the public alias as primary.

### Fix storage class names in CLAUDE.md
**Status**: Open
**Description**: CLAUDE.md line 128 references `S3OptimizedReader` and `StandardFileReader` which don't exist. Actual classes: `S3CloudStorageProvider` and `HadoopCloudStorageProvider`.

### Document missing SQL commands
**Status**: Open
**Description**: These commands exist in the ANTLR grammar but are absent from CLAUDE.md and sql-commands-reference.md:
- `BUILD INDEXTABLES COMPANION FOR DELTA|PARQUET|ICEBERG <source> AT LOCATION <dest>`
- `DESCRIBE INDEXTABLES COMPONENT SIZES <path>`
- `DESCRIBE INDEXTABLES PREWARM JOBS`
- `WAIT FOR INDEXTABLES PREWARM JOBS`
- `DESCRIBE INDEXTABLES TRANSACTION LOG <path>`
- `REPAIR INDEXFILES TRANSACTION LOG <source> AT LOCATION <target>`
- `FLUSH INDEXTABLES SEARCHER CACHE`
- `INVALIDATE INDEXTABLES TRANSACTION LOG CACHE`

### Archive stale design documents
**Status**: Complete
**Description**: Completed design documents archived to `docs/archive/`. Active design docs moved to `docs/design/` with normalized naming. Reference docs moved to `docs/reference/`. llms.txt updated to reflect new structure. Remaining items in `docs/archive/` from prior work: `dup_code_reduction/`, JSON field completion summaries, checkpoint truncation implementation, repair transaction log summaries, and `GITHUB_ISSUE_16_RESOLUTION.md`.

---

## Medium Priority: Documentation Completeness

### Document companion/sync feature
**Status**: Open
**Description**: 7 source files in `src/main/scala/io/indextables/spark/sync/`, 14+ test files, and a SQL command (`BUILD INDEXTABLES COMPANION`) — entirely absent from CLAUDE.md and all reference docs.

### Fix deprecated config in features-reference.md
**Status**: Open
**Description**: Line 185 uses `mergeOnWrite.mergeGroupMultiplier` which docs/reference/features.md marks as deprecated.

### Expand or remove performance-tuning.md
**Status**: Complete
**Description**: Removed. Content was a 27-line stub duplicating configuration-reference.md. Redirected to `docs/reference/configuration.md`.

### Improve llms.txt
**Status**: Complete
**Description**: Moved llms.txt to repo root. Restructured with project description, protocol section, proper hierarchy (Primary/Reference/Design/Archived sections), and updated all paths to match new docs/ directory structure.

### Document IP address fields and structured streaming
**Status**: Open
**Description**: Test files exist for IP address field types (`IpAddressFieldTest.scala`, `IpAddressIndexQueryTest.scala`) and structured streaming (`StructuredStreamingTest.scala`) but neither feature is documented anywhere.

---

## High Priority Features

### ReplaceWhere with Partition Predicates
**Status**: Design Complete
**Design Document**: [docs/design/replace-where.md](../docs/design/replace-where.md)
**Description**: Implement Delta Lake-style replaceWhere functionality for selective partition replacement based on predicate conditions.

**Key Features**:
- Selective partition replacement using SQL predicates
- Support for `=`, `IN`, and `AND` operators on partition columns
- Transaction log integration with ReplaceWhereAction
- Comprehensive validation and error handling
- Full test coverage with integration tests

**Priority**: High - Enables advanced data management patterns

---

### Transaction Log Compaction (Parquet-based)
**Status**: Needs Review
**Design Document**: [docs/design/log-compaction.md](../docs/design/log-compaction.md)
**Description**: Parquet-based checkpoint system for transaction log consolidation. Note: Avro-based checkpoints are already implemented (Protocol V4, default). This item may be partially superseded — review whether Parquet-based approach is still needed on top of existing Avro state format.

**Priority**: Needs re-triage

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

## Completed Features

### Core Transaction Log
- Delta Lake-style transaction log with ADD/REMOVE actions
- Overwrite and append modes with atomic operations
- Partition tracking and pruning integration
- JSON serialization with proper type handling
- Avro state format (10x faster checkpoint reads, default since Protocol V4)
- Transaction log compression (GZIP, 60-70% reduction)
- Concurrent write retry with exponential backoff

### Search and Query
- Full-text search via IndexQuery operator (Tantivy syntax)
- Aggregate pushdown (COUNT, SUM, AVG, MIN, MAX)
- Bucket aggregations (DateHistogram, Histogram, Range)
- JSON field support (Struct/Array/Map with filter pushdown)
- Statistics truncation for long text fields
- Data skipping via min/max statistics

### Storage and Performance
- S3-optimized storage with path flattening
- Split-based architecture with immutable index files
- JVM-wide caching with GlobalSplitCacheManager
- L2 Disk Cache (auto-enabled on Databricks/EMR)
- Batch retrieval optimization (90-95% S3 GET reduction)
- Optimized writes (Iceberg-style shuffle)
- AWS session token support for temporary credentials

### Operations
- MERGE SPLITS (split consolidation)
- PURGE INDEXTABLE (orphan cleanup)
- DROP INDEXTABLES PARTITIONS
- CHECKPOINT/COMPACT INDEXTABLES
- PREWARM INDEXTABLES CACHE
- Purge-on-write (automatic table hygiene)
- Merge-on-write (async post-commit)
- Multi-cloud (S3, Azure Blob Storage, Unity Catalog)

### Error Handling and Validation
- Proper exception throwing for missing tables
- Schema validation and type safety
- Comprehensive error messages and debugging
- Production-ready logging

---

## Notes
- **Design First**: All major features should have design documents before implementation
- **Test Coverage**: Maintain 100% test pass rate (381 test files, 350+ tests)
- **Performance**: Benchmark all major features against baseline
- **Documentation**: Keep CLAUDE.md and reference docs updated with implementation details