# Section 1: Introduction & Architecture Overview

**Document Version:** 1.0
**Last Updated:** 2025-10-06
**Target Audience:** Developers familiar with Apache Spark but not necessarily with Spark internals

---

## Table of Contents

1. [System Overview](#11-system-overview)
2. [Core Architectural Patterns](#12-core-architectural-patterns)
3. [High-Level Data Flow](#13-high-level-data-flow)
4. [Component Relationship Diagram](#14-component-relationship-diagram)
5. [Design Philosophy](#15-design-philosophy)
6. [Key Features Summary](#16-key-features-summary)

---

## 1.1 System Overview

### Purpose

**IndexTables4Spark** is a high-performance Apache Spark DataSource V2 implementation that provides full-text search capabilities using the Tantivy search engine. Unlike traditional search solutions that require separate server infrastructure (like Elasticsearch or Solr), IndexTables4Spark runs entirely embedded within Spark executors, providing:

- **Zero infrastructure overhead**: No separate search servers to manage
- **Native Spark integration**: First-class DataFrame and SQL API support
- **Distributed execution**: Search operations scale horizontally with your Spark cluster
- **ACID transactions**: Delta Lake-inspired transaction log ensures data consistency

### Key Value Proposition

The primary value of IndexTables4Spark comes from eliminating the operational complexity and data synchronization challenges of maintaining separate search infrastructure:

**Traditional Architecture:**
```
Spark DataFrame → ETL Pipeline → Elasticsearch/Solr → Search Results
                     ↓
              Sync Delays, Consistency Issues, Duplicate Infrastructure
```

**IndexTables4Spark Architecture:**
```
Spark DataFrame → IndexTables4Spark → Search Results
                      ↓
              Same Cluster, No Sync, ACID Guarantees
```

### Target Use Cases

IndexTables4Spark is designed for:

1. **Log Analytics**: Fast text search across application logs, security logs, audit trails
2. **Document Search**: Full-text search of documents, articles, or content repositories
3. **Event Processing**: Search and filter high-volume event streams
4. **Data Lake Exploration**: Interactive search capabilities over data lake storage
5. **Compliance & Audit**: Searchable archives with ACID transaction guarantees

### What IndexTables4Spark Is NOT

To set proper expectations:

- **Not a replacement for transactional databases**: Use Spark SQL DataSources for OLTP workloads
- **Not a real-time search engine**: Writes are batch-oriented (Spark's execution model)
- **Not a vector database**: Optimized for full-text search, not embeddings or similarity search
- **Not a streaming platform**: Best suited for batch and micro-batch processing

---

## 1.2 Core Architectural Patterns

IndexTables4Spark is built on four foundational architectural patterns that define its behavior and capabilities:

### 1.2.1 DataSource V2 API Pattern

IndexTables4Spark implements the modern **Spark DataSource V2 API** (introduced in Spark 3.0), providing:

**V2 API Advantages:**
- **Push-down optimizations**: Filters, projections, limits, and aggregates pushed to the data source
- **Metadata columns**: Virtual columns like `_indexall` for cross-field search
- **Better partition handling**: Transform-based partitioning with proper schema integration
- **Statistics reporting**: Table statistics for Spark's cost-based optimizer

**V1 API Deprecation:**
> **IMPORTANT:** The legacy V1 DataSource API (`format("io.indextables.spark.core.IndexTables4SparkTableProvider")`) is deprecated and scheduled for removal. All new code should use the V2 API (`format("io.indextables.spark.core.IndexTables4SparkTableProvider")`). See Section 2.3 for migration details.

**Key Difference:** V2 API **indexes partition columns**, while V1 did not. This enables full-text search across partitioned datasets.

### 1.2.2 Split-Based Storage Pattern

IndexTables4Spark uses an **immutable split-based architecture** inspired by Apache Quickwit:

**What is a Split?**
A split is a self-contained, immutable Tantivy index stored as a single `.split` file:

```
s3://my-bucket/my-table/                           # AWS S3 example
abfss://container@account.dfs.core.windows.net/    # Azure ADLS Gen2 example
├── _transaction_log/
│   ├── 00000000000000000000.json       # Transaction log files
│   ├── 00000000000000000001.json
│   └── 00000000000000000002.checkpoint.json
└── partition=2024-01-01/
    ├── 3f2504e0-4f89-11d3-9a0c-0305e82c3301.split  # Immutable split files
    ├── 6ba7b810-9dad-11d1-80b4-00c04fd430c8.split
    └── 9c4e1e20-2b56-4c3d-8f2a-7e9f3a8c1d4e.split
```

**Split Characteristics:**
- **Immutable**: Once written, never modified (copy-on-write semantics)
- **Self-contained**: Complete Tantivy index with schema, documents, and search structures
- **Portable**: Can be cached locally, uploaded to S3/Azure, or stored on HDFS
- **Independently searchable**: Each split can be queried in parallel

**Benefits:**
- **Horizontal scalability**: Each Spark partition processes one or more splits
- **Efficient caching**: Splits can be cached on executor local disks
- **Simple compaction**: Merge small splits into larger ones without rewriting entire table
- **Multi-cloud storage friendly**: Large files optimize S3/Azure Blob Storage performance

### 1.2.3 Transaction Log Pattern (Delta Lake-Inspired)

IndexTables4Spark uses a **Delta Lake-compatible transaction log** to track table state:

**Transaction Log Structure:**
```
_transaction_log/
├── 00000000000000000000.json          # Protocol + Metadata
├── 00000000000000000001.json          # ADD actions (first write)
├── 00000000000000000002.json          # ADD actions (append)
├── 00000000000000000010.checkpoint.json  # Checkpoint (compacted state)
└── _last_checkpoint                   # Points to latest checkpoint
```

**ACID Guarantees:**

| Property | Implementation |
|----------|----------------|
| **Atomicity** | All changes in a transaction commit together or not at all |
| **Consistency** | Schema validation and protocol versioning ensure valid states |
| **Isolation** | Optimistic concurrency with version counters |
| **Durability** | Writes to cloud storage with conditional PUT (no overwrites) |

**Transaction Log Actions:**

| Action Type | Purpose | Example |
|-------------|---------|---------|
| `ProtocolAction` | Version compatibility | Reader v2, Writer v2 |
| `MetadataAction` | Schema and table properties | Schema, partition columns, config |
| `AddAction` | File additions | Add split with metadata (size, rows, min/max) |
| `RemoveAction` | File removals | Remove split during merge or overwrite |
| `SkipAction` | Corrupted file tracking | Record failed merge with cooldown |

**Checkpoint Compaction:**
To prevent unbounded transaction log growth, IndexTables4Spark periodically creates **checkpoint files** that consolidate the table state:

```
Every N transactions → Create checkpoint → Clean up old transaction files
```

**Performance Impact:** Checkpoints provide **60% faster reads** by loading base state from a single file instead of replaying hundreds of individual transactions. See Section 3.2 for details.

### 1.2.4 Embedded Search Engine Pattern

IndexTables4Spark embeds **Tantivy** (a Rust-based search engine) directly in Spark executors via **tantivy4java** JNI bindings:

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│ Spark Executor JVM                                      │
│  ┌──────────────────────────────────────────────────┐  │
│  │ IndexTables4Spark (Scala)                        │  │
│  │   ├── Split Cache Manager                        │  │
│  │   ├── Query Converter (Spark → Tantivy)         │  │
│  │   └── Result Processor (Tantivy → Spark)        │  │
│  └──────────────────┬───────────────────────────────┘  │
│                     │ JNI                               │
│  ┌──────────────────▼───────────────────────────────┐  │
│  │ tantivy4java (Java → Rust bridge)                │  │
│  └──────────────────┬───────────────────────────────┘  │
│                     │ JNI                               │
│  ┌──────────────────▼───────────────────────────────┐  │
│  │ Tantivy Search Engine (Rust)                     │  │
│  │   ├── Inverted Index                             │  │
│  │   ├── Query Parser                               │  │
│  │   ├── Fast Fields (column storage)               │  │
│  │   └── Aggregation Engine                         │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Benefits:**
- **No network overhead**: Search executes in-process
- **Data locality**: Splits cached on local disk for fast access
- **Native performance**: Rust-based search engine with zero-copy operations
- **Resource sharing**: Uses Spark executor memory and CPU

**Integration Points:**
- **Schema mapping**: Spark `StructType` → Tantivy `Schema`
- **Document conversion**: Spark `InternalRow` → Tantivy `Document`
- **Query conversion**: Spark `Filter` → Tantivy `Query`
- **Result streaming**: Tantivy search results → Spark `Iterator[InternalRow]`

---

## 1.3 High-Level Data Flow

### 1.3.1 Write Path

The write path converts DataFrames into searchable split files:

```
┌─────────────────┐
│ User DataFrame  │
│ df.write.format │
│ ("...Provider") │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│ IndexTables4SparkWriteBuilder   │ ◄── Determine write mode
│  • Check overwrite vs append    │     (overwrite/append)
│  • Select write strategy         │
│    (Standard vs Optimized)       │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Auto-Sizing (Optional)          │ ◄── Intelligent repartitioning
│  • Analyze historical splits    │     based on target split size
│  • Calculate bytes/record        │
│  • Repartition DataFrame         │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Distributed Write Execution     │ ◄── Parallel split creation
│ IndexTables4SparkBatchWrite     │     across Spark executors
│  • Create per-partition writers │
│  • Build Tantivy indexes        │
│  • Upload splits to storage     │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Commit to Transaction Log       │ ◄── Atomic transaction
│  • Collect split metadata       │     log update
│  • Write ADD/REMOVE actions     │
│  • Create checkpoint if needed  │
└─────────────────────────────────┘
```

**Key Steps:**

1. **Write Builder**: Determines write mode (append/overwrite) and selects write strategy
2. **Auto-Sizing**: Optionally repartitions DataFrame based on historical split analysis
3. **Split Creation**: Each executor partition creates Tantivy indexes and uploads splits
4. **Transaction Commit**: Driver collects metadata and atomically updates transaction log

**Write Strategies:**

| Strategy | Use Case | Features |
|----------|----------|----------|
| **Standard** | Simple writes | Direct execution, manual partitioning |
| **Optimized** | Production workloads | Auto-sizing, intelligent shuffle, historical analysis |

### 1.3.2 Read Path

The read path plans queries and distributes search operations:

```
┌─────────────────┐
│ User Query      │
│ df.filter(...)  │
│   .select(...)  │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────┐
│ IndexTables4SparkScanBuilder     │ ◄── Catalyst optimizer
│  • Push down filters              │     pushes down predicates
│  • Push down projections          │
│  • Push down aggregates           │
│  • Push down limit                │
└────────┬─────────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│ Scan Strategy Selection          │ ◄── Choose scan type
│  • Regular scan (IndexQuery)     │     based on query
│  • Simple aggregate scan          │
│  • GROUP BY aggregate scan        │
│  • Transaction log COUNT scan    │
└────────┬─────────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│ Partition Planning                │ ◄── Data skipping
│  • Read transaction log           │     optimization
│  • Apply partition pruning        │
│  • Apply min/max filtering        │
│  • Determine preferred locations  │
└────────┬─────────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│ Distributed Search Execution     │ ◄── Parallel search
│  • Cache splits on executors     │     across cluster
│  • Execute Tantivy queries       │
│  • Stream results back           │
└──────────────────────────────────┘
```

**Key Steps:**

1. **ScanBuilder**: Catalyst optimizer pushes down filters, projections, limits, and aggregates
2. **Scan Selection**: Choose appropriate scan type (regular, aggregate, or COUNT-only)
3. **Partition Planning**: Apply data skipping (partition pruning + min/max filtering)
4. **Search Execution**: Distributed Tantivy queries across cached splits

**Data Skipping Optimizations:**

| Optimization | Description | Performance Impact |
|--------------|-------------|--------------------|
| **Partition Pruning** | Eliminate partitions based on partition column filters | 50-99% reduction in files scanned |
| **Min/Max Filtering** | Skip splits based on min/max values in footer metadata | 10-50% reduction in remaining files |
| **IndexQuery Pushdown** | Native Tantivy query execution (no Spark post-filter) | 10-100x speedup for text search |
| **Aggregate Pushdown** | Tantivy-native aggregation (no data transfer) | 10-100x speedup for COUNT/SUM/etc |

### 1.3.3 Query Path (IndexQuery Operations)

IndexQuery operations provide native Tantivy query syntax:

```
┌─────────────────────────────────────┐
│ SQL Query with IndexQuery           │
│ SELECT * FROM logs                  │
│ WHERE message indexquery            │
│   'ERROR AND (timeout OR crash)'    │
└────────┬────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ Custom SQL Parser                    │ ◄── Parse indexquery
│ IndexTables4SparkSqlParser           │     operator
│  • Detect 'indexquery' operator      │
│  • Create IndexQueryExpression       │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ Catalyst Resolution Rule             │ ◄── Store IndexQuery
│ V2IndexQueryExpressionRule           │     in cache
│  • Extract IndexQuery from plan      │
│  • Store in schema-based cache       │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ ScanBuilder Retrieves IndexQuery    │ ◄── Pass to scan
│  • Check cache using schema hash     │
│  • Pass to scan as filter            │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ Tantivy Query Execution              │ ◄── Native search
│  • Parse query string                │
│  • Execute inverted index lookup     │
│  • Return matching documents         │
└──────────────────────────────────────┘
```

**IndexQuery Syntax Examples:**

```sql
-- Single term search
WHERE content indexquery 'error'

-- Boolean queries
WHERE content indexquery 'ERROR AND (timeout OR crash)'

-- Phrase search
WHERE content indexquery '"connection timeout"'

-- Field-specific search (cross-field via _indexall)
WHERE _indexall indexquery 'user:john AND status:active'

-- Complex nested queries
WHERE content indexquery '(apache OR nginx) AND NOT test'
```

---

## 1.4 Component Relationship Diagram

IndexTables4Spark is organized into four architectural layers:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          API LAYER                                      │
│  ┌──────────────────────┐  ┌─────────────────────┐                    │
│  │ DataSource V2 API    │  │ SQL Extensions      │                    │
│  │                      │  │                      │                    │
│  │ • TableProvider      │  │ • Custom Parser     │                    │
│  │ • Table              │  │ • IndexQuery UDFs   │                    │
│  │ • ScanBuilder        │  │ • MERGE SPLITS CMD  │                    │
│  │ • WriteBuilder       │  │ • Cache Commands    │                    │
│  └──────────┬───────────┘  └──────────┬──────────┘                    │
└─────────────┼──────────────────────────┼──────────────────────────────┘
              │                          │
┌─────────────▼──────────────────────────▼──────────────────────────────┐
│                       PLANNING LAYER                                   │
│  ┌──────────────────────┐  ┌──────────────────────┐                  │
│  │ Scan Planning        │  │ Write Planning       │                  │
│  │                      │  │                       │                  │
│  │ • Filter Pushdown    │  │ • Auto-Sizing        │                  │
│  │ • Aggregate Pushdown │  │ • Partition Planning │                  │
│  │ • Data Skipping      │  │ • Shuffle Strategy   │                  │
│  │ • Partition Pruning  │  │ • Config Hierarchy   │                  │
│  └──────────┬───────────┘  └──────────┬───────────┘                  │
└─────────────┼──────────────────────────┼──────────────────────────────┘
              │                          │
┌─────────────▼──────────────────────────▼──────────────────────────────┐
│                      EXECUTION LAYER                                   │
│  ┌──────────────────────┐  ┌──────────────────────┐                  │
│  │ Read Execution       │  │ Write Execution      │                  │
│  │                      │  │                       │                  │
│  │ • Scan Types         │  │ • Split Creation     │                  │
│  │ • Partition Readers  │  │ • Batch Write        │                  │
│  │ • IndexQuery Cache   │  │ • Transaction Commit │                  │
│  │ • Result Streaming   │  │ • Merge Operations   │                  │
│  └──────────┬───────────┘  └──────────┬───────────┘                  │
└─────────────┼──────────────────────────┼──────────────────────────────┘
              │                          │
┌─────────────▼──────────────────────────▼──────────────────────────────┐
│                       STORAGE LAYER                                    │
│  ┌──────────────────────┐  ┌──────────────────────┐                  │
│  │ Transaction Log      │  │ Split Storage        │                  │
│  │                      │  │                       │                  │
│  │ • Checkpoint System  │  │ • Split Cache        │                  │
│  │ • Action Types       │  │ • S3 Optimization    │                  │
│  │ • Protocol Versioning│  │ • Locality Tracking  │                  │
│  │ • Cache & TTL        │  │ • Cloud Providers    │                  │
│  └──────────────────────┘  └──────────────────────┘                  │
│                                                                         │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ Tantivy Search Engine (via tantivy4java)        │                 │
│  │                                                   │                 │
│  │ • Inverted Index • Query Parser                  │                 │
│  │ • Fast Fields    • Aggregation Engine            │                 │
│  └──────────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Layer Responsibilities:**

| Layer | Responsibility | Key Components |
|-------|---------------|----------------|
| **API Layer** | User-facing interfaces | TableProvider, ScanBuilder, WriteBuilder, SQL Parser |
| **Planning Layer** | Query optimization and planning | Filter pushdown, Data skipping, Auto-sizing |
| **Execution Layer** | Distributed execution | Scan types, Split creation, Partition readers |
| **Storage Layer** | Data persistence and retrieval | Transaction log, Split cache, Cloud storage |

---

## 1.5 Design Philosophy

IndexTables4Spark is guided by several key design principles:

### 1.5.1 Simplicity Over Magic

**Principle:** Favor explicit, understandable behavior over implicit "magic."

**Examples:**
- **Auto-sizing is opt-in**: Users must explicitly enable and configure auto-sizing
- **Explicit row counts**: V2 API recommends explicit row count hints for optimal partitioning
- **Clear error messages**: Fast field validation errors explain exactly what's missing
- **No hidden conversions**: Field types (string vs text) must be explicitly configured

**Rationale:** Predictable behavior reduces debugging time and operational surprises.

### 1.5.2 Delta Lake Compatibility

**Principle:** Transaction log format and semantics should be Delta Lake-compatible where possible.

**Examples:**
- **Action types**: `ProtocolAction`, `MetadataAction`, `AddAction`, `RemoveAction`
- **Checkpoint format**: JSON snapshots with similar structure
- **ACID guarantees**: Optimistic concurrency and atomic commits
- **File naming**: Zero-padded version numbers (e.g., `00000000000000000001.json`)

**Rationale:** Familiarity for Delta Lake users and potential future interoperability.

### 1.5.3 Performance Through Pushdown

**Principle:** Move computation to the data, not data to the computation.

**Examples:**
- **Filter pushdown**: Tantivy executes filters natively (no Spark post-processing)
- **Aggregate pushdown**: COUNT/SUM/AVG execute in Tantivy (no data transfer)
- **Partition pruning**: Eliminate files before reading
- **Min/max filtering**: Skip splits using footer metadata

**Rationale:** Minimize data movement and leverage native search engine performance.

### 1.5.4 Graceful Degradation

**Principle:** System should continue operating even when optimizations fail.

**Examples:**
- **Checkpoint failures**: Writes succeed even if checkpoint creation fails
- **Cache misses**: Queries work without cache optimization
- **Skipped files**: Merge operations continue despite corrupted files
- **Pre-warm failures**: Query execution proceeds without cache warming

**Rationale:** Robustness in production environments with imperfect infrastructure.

### 1.5.5 Cloud-First Storage

**Principle:** Design for cloud object storage (S3, Azure Blob Storage, GCS) as the primary storage tier.

**Examples:**
- **Large split files**: Optimize S3/Azure upload performance
- **Parallel streaming uploads**: Multi-threaded uploads for large files
- **Retry logic**: Handle transient S3/Azure failures
- **Multi-cloud authentication**: Support AWS credentials, Azure OAuth, and custom providers

**Rationale:** Modern data infrastructure is cloud-based; optimize for that reality.

### 1.5.6 Immutability

**Principle:** Data files should be immutable (copy-on-write semantics).

**Examples:**
- **Split files**: Never modified after creation
- **Transaction log files**: Never overwritten (conditional PUT)
- **Merge operations**: Create new splits, mark old splits as removed
- **Overwrite mode**: REMOVE old files + ADD new files in single transaction

**Rationale:** Immutability enables caching, concurrent reads, and ACID guarantees.

---

## 1.6 Key Features Summary

This section provides a high-level summary of IndexTables4Spark's major capabilities. Detailed documentation for each feature is provided in subsequent sections.

### 1.6.1 DataSource Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **V2 DataSource API** | Modern Spark 3.x DataSource implementation | Section 2 |
| **V1 API (Deprecated)** | Legacy API maintained for backward compatibility | Section 2.3 |
| **Partition Support** | Full partitioned dataset support with pruning | Section 6 |
| **Schema Evolution** | Protocol versioning for future schema changes | Section 12.1 |

### 1.6.2 Query Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **Filter Pushdown** | Standard Spark filters pushed to Tantivy | Section 4.2 |
| **Aggregate Pushdown** | COUNT/SUM/AVG/MIN/MAX in Tantivy | Section 4.3 |
| **IndexQuery Operator** | Native Tantivy query syntax in SQL | Section 7 |
| **Virtual Columns** | `_indexall` for cross-field search | Section 7.1 |
| **Data Skipping** | Partition pruning + min/max filtering | Section 4.5 |
| **Limit Pushdown** | Early termination for LIMIT queries | Section 4.1 |

### 1.6.3 Write Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **Optimized Write** | Auto-sizing with historical analysis | Section 5.3 |
| **Standard Write** | Simple append/overwrite mode | Section 5.2 |
| **Partition-Aware Writes** | Automatic partition directory creation | Section 6 |
| **Batch Processing** | Efficient document batching | Section 5.5 |

### 1.6.4 Transaction Log Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **ACID Guarantees** | Atomic commits, isolation, durability | Section 3.1 |
| **Checkpoint Compaction** | 60% faster reads via consolidation | Section 3.2 |
| **Parallel I/O** | Concurrent transaction file reading | Section 3.2 |
| **Multi-Level Caching** | Versions, files, metadata, protocol | Section 3.4 |
| **Retention Policies** | Automatic cleanup with safety gates | Section 3.3 |
| **Protocol Versioning** | Forward/backward compatibility | Section 12.1 |

### 1.6.5 Performance Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **Auto-Sizing** | Intelligent DataFrame repartitioning | Section 11.1 |
| **Split Caching** | JVM-wide split cache with locality | Section 8.1 |
| **Broadcast Locality** | Cluster-wide cache tracking | Section 8.2 |
| **Pre-Warm Manager** | Proactive cache warming | Section 11.4 |
| **S3 Upload Optimization** | Parallel streaming uploads | Section 8.4 |
| **Transaction Log COUNT** | Metadata-only COUNT queries | Section 4.3 |

### 1.6.6 Operational Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **MERGE SPLITS Command** | Split consolidation via SQL | Section 9.2 |
| **Cache Management** | FLUSH/INVALIDATE commands | Section 9.3 |
| **Skipped Files Tracking** | Corrupted file handling with cooldown | Section 3.6 |
| **Working Directory Config** | Custom temp directories | Section 10.4 |
| **Custom Credentials** | Enterprise credential provider integration | Section 8.5 |
| **Configuration Hierarchy** | Options → Spark → Hadoop → defaults | Section 10 |

### 1.6.7 Field Indexing Features

| Feature | Description | Section Reference |
|---------|-------------|-------------------|
| **String Fields** | Exact matching with raw tokenizer | Section 4.2 |
| **Text Fields** | Full-text search with tokenization | Section 4.2 |
| **Fast Fields** | Column storage for aggregations | Section 4.3 |
| **Store-Only Fields** | Stored but not indexed | Section 10.2 |
| **Index-Only Fields** | Indexed but not stored | Section 10.2 |

---

## Summary

This section introduced IndexTables4Spark's architecture and design philosophy:

- **Purpose**: Embedded full-text search for Spark with zero infrastructure overhead
- **Core Patterns**: V2 API, split-based storage, transaction log, embedded Tantivy engine
- **Data Flows**: Write path (DataFrame → splits → transaction log), Read path (query → scan planning → distributed search)
- **Design Philosophy**: Simplicity, Delta Lake compatibility, pushdown optimization, graceful degradation, cloud-first storage, immutability

The following sections provide detailed documentation for each architectural component and feature.

---

**Next Section:** [Section 2: DataSource V2 API Implementation](02_datasource_v2_api.md)
