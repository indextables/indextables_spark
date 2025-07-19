# Spark Tantivy Handler - Design Documentation

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Principles](#architecture-principles)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [UML Diagrams](#uml-diagrams)
6. [Deployment Architecture](#deployment-architecture)
7. [Performance Considerations](#performance-considerations)
8. [Security Design](#security-design)

## System Overview

The Spark Tantivy Handler is a high-performance Apache Spark DataSource that embeds Tantivy search capabilities directly into Spark executors. This design eliminates the need for separate search infrastructure while providing sub-second search performance on data stored in cloud storage systems like S3.

### Key Design Goals

- **Embedded Architecture**: No external dependencies or separate services required
- **High Performance**: Sub-second search with aggressive optimization for cloud storage
- **ACID Compliance**: Delta-style transaction logging ensures data consistency
- **Scalability**: Designed to handle petabyte-scale datasets efficiently
- **Cross-Platform**: Native library support for Linux, macOS, and Windows

### System Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                   Apache Spark                         │
├─────────────────────────────────────────────────────────┤
│              Spark DataSource V1 API                   │
├─────────────────────────────────────────────────────────┤
│            TantivyFileFormat (Scala)                  │
├─────────────────────────────────────────────────────────┤
│     Search Engine    │    Storage Layer    │   Config   │
│   (JNI → Rust)      │   (S3 Optimized)   │ Management │
├─────────────────────────────────────────────────────────┤
│              Tantivy Native Library (Rust)            │
├─────────────────────────────────────────────────────────┤
│         Cloud Storage (S3/GCS/Azure Blob)              │
└─────────────────────────────────────────────────────────┘
```

## Architecture Principles

### 1. Embedded-First Design
- **Principle**: All search functionality embedded within Spark executors
- **Benefit**: Eliminates network latency and infrastructure complexity
- **Implementation**: JNI bridge to native Rust Tantivy library

### 2. Storage-Compute Separation
- **Principle**: Compute and storage are completely decoupled
- **Benefit**: Independent scaling and cost optimization
- **Implementation**: Direct cloud storage access with intelligent caching

### 3. Predictive I/O Optimization
- **Principle**: Anticipate data access patterns to minimize latency
- **Benefit**: Sub-second query performance on cloud storage
- **Implementation**: Read-ahead caching and concurrent prefetching

### 4. Transaction Consistency
- **Principle**: ACID guarantees for all write operations
- **Benefit**: Data integrity and consistency across failures
- **Implementation**: Delta-style append-only transaction logging

### 5. Schema Evolution Support
- **Principle**: Backward-compatible schema changes without data migration
- **Benefit**: Continuous operation during schema updates
- **Implementation**: Automated compatibility checking and validation

## Core Components

### 1. Spark Integration Layer

#### TantivyFileFormat
- **Purpose**: Main entry point implementing Spark DataSource V1 API
- **Responsibilities**:
  - Schema inference and validation
  - Reader/Writer factory creation
  - Partition planning and optimization
- **Key Features**:
  - Automatic file splitting for parallel processing
  - Schema compatibility checking
  - Custom option handling

#### TantivyFileReader/Writer
- **Purpose**: Handle actual data I/O operations
- **Responsibilities**:
  - Data serialization/deserialization
  - Filter pushdown optimization
  - Transaction coordination
- **Key Features**:
  - Streaming data processing
  - Memory-efficient operations
  - Error handling and recovery

### 2. Search Engine Layer

#### TantivySearchEngine
- **Purpose**: High-level search interface and query processing
- **Responsibilities**:
  - Query parsing and optimization
  - Result aggregation and ranking
  - Index management
- **Key Features**:
  - Full-text search capabilities
  - Complex query support (filters, aggregations)
  - Real-time search on streaming data

#### TantivyIndexWriter
- **Purpose**: Document indexing and storage management
- **Responsibilities**:
  - Document processing and transformation
  - Index segment creation and optimization
  - Batch processing for efficiency
- **Key Features**:
  - Automatic schema mapping
  - Incremental indexing
  - Compression and optimization

### 3. Storage Optimization Layer

#### S3OptimizedReader
- **Purpose**: High-performance cloud storage access
- **Responsibilities**:
  - Predictive I/O and caching
  - Concurrent data fetching
  - Error handling and retry logic
- **Key Features**:
  - LRU cache with configurable size
  - Read-ahead prediction algorithms
  - Parallel download optimization

### 4. Transaction Management

#### TransactionLog
- **Purpose**: ACID compliance and data consistency
- **Responsibilities**:
  - Operation logging and tracking
  - Commit/rollback coordination
  - Conflict detection and resolution
- **Key Features**:
  - Append-only log structure
  - Atomic operations
  - Recovery and replay capabilities

### 5. Configuration Management

#### TantivyConfig & SchemaManager
- **Purpose**: System configuration and schema evolution
- **Responsibilities**:
  - Configuration validation and management
  - Schema compatibility checking
  - Migration planning and execution
- **Key Features**:
  - Automatic type mapping
  - Version control for schemas
  - Backward compatibility validation

### 6. Native Integration Layer

#### TantivyNative (JNI Bridge)
- **Purpose**: Interface between JVM and native Rust library
- **Responsibilities**:
  - Native library loading and management
  - Memory management across language boundaries
  - Error propagation and handling
- **Key Features**:
  - Cross-platform library support
  - Efficient data marshaling
  - Resource lifecycle management

## Data Flow

### Write Operations Flow

```
Spark DataFrame
       ↓
TantivyFileFormat.prepareWrite()
       ↓
TantivyOutputWriter.write()
       ↓
TantivyIndexWriter.writeRow()
       ↓
[Schema Mapping & Validation]
       ↓
[Batch Accumulation]
       ↓
TantivyNative.indexDocument()
       ↓
[JNI → Rust Tantivy]
       ↓
[Index Segment Creation]
       ↓
TransactionLog.appendEntry()
       ↓
[S3 Storage Write]
       ↓
TransactionLog.commit()
```

### Read Operations Flow

```
Spark SQL Query
       ↓
TantivyFileFormat.buildReader()
       ↓
TantivyFileReader.read()
       ↓
[Filter Analysis & Pushdown]
       ↓
TantivySearchEngine.search()
       ↓
TantivyNative.search()
       ↓
[JNI → Rust Tantivy]
       ↓
[Index Search & Scoring]
       ↓
[Result Data Locations]
       ↓
S3OptimizedReader.readWithPredictiveIO()
       ↓
[Concurrent Data Fetch]
       ↓
[LRU Cache Check]
       ↓
[Data Deserialization]
       ↓
Spark InternalRow Iterator
```

### Schema Evolution Flow

```
New Spark Schema
       ↓
SchemaManager.validateCompatibility()
       ↓
[Compare with Existing Schema]
       ↓
[Generate Compatibility Report]
       ↓
TantivyConfig.fromSpark()
       ↓
[Automatic Type Mapping]
       ↓
[Field Option Resolution]
       ↓
SchemaManager.saveSchema()
       ↓
[JSON Serialization]
       ↓
[File System Persistence]
```

## Performance Considerations

### 1. Caching Strategy
- **L1 Cache**: In-memory LRU cache for frequently accessed data segments
- **L2 Cache**: Local disk cache for intermediate results
- **L3 Cache**: Distributed cache across Spark executors

### 2. Parallelization
- **Index Segments**: Multiple segments processed in parallel
- **I/O Operations**: Concurrent reads from cloud storage
- **Query Processing**: Parallel search across index partitions

### 3. Memory Management
- **Streaming Processing**: Process data in fixed-size batches
- **Off-heap Storage**: Use native memory for large objects
- **Garbage Collection**: Minimize JVM GC pressure through careful object lifecycle

### 4. Network Optimization
- **Connection Pooling**: Reuse HTTP connections to cloud storage
- **Compression**: Automatic compression for data transfer
- **Regional Affinity**: Process data in same region as storage

## Security Design

### 1. Data Protection
- **Encryption at Rest**: Leverage cloud storage encryption
- **Encryption in Transit**: TLS for all network communications
- **Access Control**: Integration with cloud IAM systems

### 2. Credential Management
- **No Embedded Secrets**: Use IAM roles and temporary credentials
- **Audit Logging**: Track all data access operations
- **Fine-grained Permissions**: Minimum required access principles

### 3. Multi-tenancy
- **Namespace Isolation**: Separate indexes per tenant
- **Resource Quotas**: Configurable limits per tenant
- **Audit Trails**: Complete operation history per tenant

## Scalability Design

### 1. Horizontal Scaling
- **Stateless Design**: No shared state between executors
- **Dynamic Partitioning**: Automatic data distribution
- **Elastic Compute**: Scale based on workload demands

### 2. Storage Scaling
- **Unlimited Capacity**: Leverage cloud storage elasticity
- **Intelligent Tiering**: Automatic data lifecycle management
- **Global Distribution**: Multi-region data replication

### 3. Performance Scaling
- **Index Optimization**: Automatic segment merging and optimization
- **Query Caching**: Intelligent result caching strategies
- **Resource Management**: Dynamic resource allocation based on workload