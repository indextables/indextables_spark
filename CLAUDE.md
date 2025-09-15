# CLAUDE.md

**Tantivy4Spark** is a high-performance Spark DataSource implementing fast full-text search using Tantivy via tantivy4java. It runs embedded in Spark executors without server-side components.

## Key Features
- **Split-based architecture**: Write-only indexes with QuickwitSplit format
- **Transaction log**: Delta Lake-style with atomic operations
- **Partitioned datasets**: Full support for partitioned tables with partition pruning and WHERE clauses
- **Merge splits optimization**: SQL-based split consolidation with intelligent bin packing, configurable limits, and partition-aware operations
- **Broadcast locality management**: Cluster-wide cache locality tracking for optimal task scheduling
- **IndexQuery operators**: Native Tantivy syntax (`content indexquery 'query'` and `_indexall indexquery 'query'`)
- **Optimized writes**: Automatic split sizing with adaptive shuffle
- **V1/V2 DataSource compatibility**: Both legacy and modern Spark DataSource APIs fully supported
- **S3-optimized storage**: Intelligent caching and session token support
- **Schema-aware filtering**: Field validation prevents native crashes and ensures compatibility
- **100% test coverage**: 194 tests passing, 0 failing, comprehensive partitioned dataset test suite

## Build & Test
```bash
mvn clean compile  # Build
mvn test          # Run tests  
```

## Configuration
Key settings with defaults:
- `spark.tantivy4spark.indexWriter.heapSize`: `100000000` (100MB)
- `spark.tantivy4spark.indexWriter.batchSize`: `10000` documents
- `spark.tantivy4spark.indexWriter.threads`: `2`
- `spark.tantivy4spark.cache.maxSize`: `200000000` (200MB)
- `spark.tantivy4spark.cache.prewarm.enabled`: `true` (Enable proactive cache warming)
- `spark.tantivy4spark.docBatch.enabled`: `true` (Enable batch document retrieval for better performance)
- `spark.tantivy4spark.docBatch.maxSize`: `1000` (Maximum documents per batch)
- `spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit`: `1000000`

## Field Indexing Configuration

**New in v1.1**: Advanced field indexing configuration with support for string, text, and JSON field types.

### Field Type Configuration
- `spark.tantivy4spark.indexing.typemap.<field_name>`: Set field indexing type
  - **`string`** (default): Exact string matching, not tokenized
  - **`text`**: Full-text search with tokenization
  - **`json`**: JSON field indexing with tokenization

### Field Behavior Configuration
- `spark.tantivy4spark.indexing.fastfields`: Comma-separated list of fields for fast access (e.g., `"id,score,timestamp"`)
- `spark.tantivy4spark.indexing.storeonlyfields`: Fields stored but not indexed (e.g., `"metadata,description"`)
- `spark.tantivy4spark.indexing.indexonlyfields`: Fields indexed but not stored (e.g., `"searchterms,keywords"`)

### Tokenizer Configuration
- `spark.tantivy4spark.indexing.tokenizer.<field_name>`: Custom tokenizer for text fields
  - **`default`**: Standard tokenizer
  - **`whitespace`**: Whitespace-only tokenization
  - **`raw`**: No tokenization

### Configuration Examples
```scala
// Configure field types and behavior
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.indexing.typemap.title", "string")        // Exact matching
  .option("spark.tantivy4spark.indexing.typemap.content", "text")        // Full-text search
  .option("spark.tantivy4spark.indexing.typemap.metadata", "json")       // JSON indexing
  .option("spark.tantivy4spark.indexing.fastfields", "score,timestamp")  // Fast fields
  .option("spark.tantivy4spark.indexing.storeonlyfields", "raw_data")     // Store only
  .option("spark.tantivy4spark.indexing.tokenizer.content", "default")   // Custom tokenizer
  .save("s3://bucket/path")
```

### Field Type Behavior
- **String fields**: Exact matching, support all filter pushdown operations
- **Text fields**: Tokenized search with AND logic for multiple terms
- **JSON fields**: Tokenized JSON content search
- **Configuration persistence**: Settings are automatically stored and validated on subsequent writes

## Usage Examples

### Write
```scala
// Basic write (string fields by default)
df.write.format("tantivy4spark").save("s3://bucket/path")

// With field type configuration
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.indexing.typemap.title", "string")     // Exact matching
  .option("spark.tantivy4spark.indexing.typemap.content", "text")     // Full-text search
  .option("spark.tantivy4spark.indexing.fastfields", "score")         // Fast field access
  .save("s3://bucket/path")

// With custom configuration
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.indexWriter.batchSize", "20000")
  .option("targetRecordsPerSplit", "500000")
  .save("s3://bucket/path")
```

### Read & Search
```scala
val df = spark.read.format("tantivy4spark").load("s3://bucket/path")

// String field exact matching (default behavior)
df.filter($"title" === "exact title").show()

// Text field tokenized search (if configured as text type)
df.filter($"content" === "machine learning").show()  // Matches docs with both "machine" AND "learning"

// Standard DataFrame operations
df.filter($"title".contains("Spark")).show()

// Native Tantivy queries
df.filter($"content" indexquery "machine learning AND spark").show()

// Cross-field search
df.filter($"_indexall" indexquery "apache OR python").show()
```

### Partitioned Datasets
```scala
// Write partitioned data
df.write.format("tantivy4spark")
  .partitionBy("load_date", "load_hour")
  .option("spark.tantivy4spark.indexing.typemap.message", "text")
  .save("s3://bucket/partitioned-data")

// V2 DataSource API (modern)
df.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
  .partitionBy("year", "month", "day")
  .save("s3://bucket/v2-partitioned")

// Read with partition pruning
val df = spark.read.format("tantivy4spark").load("s3://bucket/partitioned-data")
df.filter($"load_date" === "2024-01-01" && $"load_hour" === 10).show()

// Complex queries with partition and content filters
df.filter($"load_date" === "2024-01-01" && $"message" indexquery "error OR warning").show()
```

### SQL
```sql
-- Register extensions (if using SQL)
spark.sparkSession.extensions.add("com.tantivy4spark.extensions.Tantivy4SparkExtensions")

-- Native queries
SELECT * FROM documents WHERE content indexquery 'AI AND (neural OR deep)';
SELECT * FROM documents WHERE _indexall indexquery 'spark AND sql';

-- Partitioned queries with IndexQuery
SELECT * FROM partitioned_data
WHERE load_date = '2024-01-01' AND message indexquery 'error OR warning';

-- Split optimization
MERGE SPLITS 's3://bucket/path' TARGET SIZE 104857600;  -- 100MB
MERGE SPLITS 's3://bucket/path' MAX GROUPS 10;          -- Limit to 10 merge groups
MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M MAX GROUPS 5;  -- Both constraints

-- Partition-aware split optimization
MERGE SPLITS 's3://bucket/partitioned-data'
WHERE load_date = '2024-01-01' AND load_hour = 10
TARGET SIZE 100M;
```

### Split Optimization
```scala
// Merge splits to reduce small file overhead
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 104857600")

// Target sizes support unit suffixes (M for megabytes, G for gigabytes)
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M")
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 1G")

// Limit the number of split groups created by a single command
spark.sql("MERGE SPLITS 's3://bucket/path' MAX GROUPS 10")

// Partition-aware optimization - merge only specific partitions
spark.sql("""
  MERGE SPLITS 's3://bucket/partitioned-data'
  WHERE load_date = '2024-01-01' AND load_hour = 10
  TARGET SIZE 100M
""")

// Global optimization across all partitions
spark.sql("MERGE SPLITS 's3://bucket/partitioned-data' TARGET SIZE 100M")

// Combine TARGET SIZE and MAX GROUPS for fine-grained control
spark.sql("MERGE SPLITS 's3://bucket/path' TARGET SIZE 100M MAX GROUPS 5")
```

## Schema Support
**Supported**: String (text), Integer/Long (i64), Float/Double (f64), Boolean (i64), Date (date), Timestamp (i64), Binary (bytes)
**Unsupported**: Arrays, Maps, Structs (throws UnsupportedOperationException)

## DataSource API Compatibility

Tantivy4Spark supports both legacy V1 and modern V2 Spark DataSource APIs with full feature parity:

### V1 DataSource API (Legacy)
```scala
// V1 format - compatible with older Spark applications
df.write.format("tantivy4spark")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

val df = spark.read.format("tantivy4spark").load("s3://bucket/path")
```

### V2 DataSource API (Modern)
```scala
// V2 format - modern Spark 3.x+ with enhanced capabilities
df.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
  .partitionBy("date", "hour")
  .save("s3://bucket/path")

val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
  .load("s3://bucket/path")
```

**Key Benefits of V2 API:**
- Enhanced partition pruning and metadata optimization
- Better integration with Spark's Catalyst optimizer
- Improved schema inference and validation
- Native support for partition-aware operations

Both APIs support identical functionality including partitioned datasets, IndexQuery operations, MERGE SPLITS commands, and all field indexing configurations.

## Architecture
- **File format**: `*.split` files with UUID naming
- **Transaction log**: `_transaction_log/` directory (Delta Lake compatible)
- **Partitioned datasets**: Full partition pruning with metadata optimization
- **Split merging**: Distributed merge operations with REMOVE+ADD transaction patterns
- **Locality tracking**: BroadcastSplitLocalityManager for cluster-wide cache awareness
- **Batch processing**: Uses tantivy4java's BatchDocumentBuilder
- **Caching**: JVM-wide SplitCacheManager with host-based locality
- **Storage**: S3OptimizedReader for S3, StandardFileReader for local/HDFS

## Implementation Status
- ✅ **Core features**: Transaction log, optimized writes, IndexQuery operators, merge splits
- ✅ **Production ready**: IndexQuery (49/49 tests), IndexQueryAll (44/44 tests), MergeSplits (9/9 tests)
- ✅ **Partitioned datasets**: Full partitioned table support with comprehensive test suite (7/7 tests)
- ✅ **V1/V2 DataSource compatibility**: Both APIs fully functional with partitioning support
- ✅ **Split optimization**: SQL-based merge commands with MAX GROUPS limits and partition-aware operations
- ✅ **Broadcast locality**: Cluster-wide cache locality management
- ✅ **Schema validation**: Field type compatibility checks prevent configuration conflicts
- **Next**: Complete date filtering edge cases, performance optimizations

## Transaction Log Behavior
**Overwrite Operations**: Reset visible data completely, removing all previous files from transaction log
**Merge Operations**: Consolidate only files visible at merge time (respects overwrite boundaries)
**Read Behavior**: Only accesses merged splits, not original constituent files

**Example sequence:**
1. `add1(append)` + `add2(append)` → visible: add1+add2
2. `add3(overwrite)` → visible: add3 only (add1+add2 invisible) 
3. `add4(append)` → visible: add3+add4
4. `merge()` → consolidates add3+add4 into single split
5. `add5(append)` → visible: merged(add3+add4)+add5

## Breaking Changes & Migration

### v1.1 Field Type Changes
- **Default string field type changed from `text` to `string`** for exact matching behavior
- **Existing tables**: Continue to work with their original field type configuration
- **New tables**: Use `string` fields by default unless explicitly configured

### Migration Guide
```scala
// Pre-v1.1 behavior (text fields by default)
// No configuration needed - was automatic

// v1.1+ equivalent behavior
df.write.format("tantivy4spark")
  .option("spark.tantivy4spark.indexing.typemap.content", "text")  // Explicit text type
  .save("path")

// v1.1+ recommended (new default)
df.write.format("tantivy4spark")
  // No configuration - defaults to string fields for exact matching
  .save("path")
```

## Important Notes
- **tantivy4java integration**: Pure Java bindings, no Rust compilation needed
- **AWS support**: Full session token support for temporary credentials
- **Merge compression**: Tantivy achieves 30-70% size reduction through deduplication
- **Distributed operations**: Serializable AWS configs for executor-based merge operations
- **Error handling**: Comprehensive validation with descriptive error messages
- **Performance**: Batch processing, predictive I/O, smart caching, broadcast locality

---

**Instructions**: Do exactly what's asked, nothing more. Prefer editing existing files over creating new ones. Never create documentation files unless explicitly requested.