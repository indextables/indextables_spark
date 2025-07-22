# Component Interactions and Data Flow Documentation

## Table of Contents

1. [Component Interaction Overview](#component-interaction-overview)
2. [Data Flow Patterns](#data-flow-patterns)
3. [Inter-Component Communication](#inter-component-communication)
4. [Event-Driven Interactions](#event-driven-interactions)
5. [Error Handling and Recovery](#error-handling-and-recovery)
6. [Performance Optimization Flows](#performance-optimization-flows)

## Component Interaction Overview

### Primary Component Layers and Their Interactions

```mermaid
graph TB
    subgraph "Client Layer"
        SparkApp[Spark Application<br/>DataFrame Operations]
    end
    
    subgraph "Integration Layer"
        DataSource[DataSource V2<br/>Registry & Discovery]
        TableProvider[Table Provider<br/>Schema & Metadata]
    end
    
    subgraph "Query Planning Layer"
        ScanBuilder[Scan Builder<br/>Filter Pushdown]
        WriteBuilder[Write Builder<br/>Batch Operations]
    end
    
    subgraph "Execution Layer"
        Scan[Scan Implementation<br/>Partition Planning]
        BatchWrite[Batch Write<br/>Parallel Processing]
    end
    
    subgraph "Core Engine Layer"
        SearchEngine[Search Engine<br/>Index Management]
        TxLog[Transaction Log<br/>ACID Operations]
    end
    
    subgraph "Storage Layer"
        StorageStrategy[Storage Strategy<br/>Protocol Selection]
        FileReaders[File Readers<br/>I/O Operations]
    end
    
    subgraph "Native Layer"
        JNI[JNI Bridge<br/>Type Conversion]
        TantivyCore[Tantivy Core<br/>Search Operations]
    end

    %% Interaction Flow
    SparkApp --> DataSource
    DataSource --> TableProvider
    TableProvider --> ScanBuilder
    TableProvider --> WriteBuilder
    
    ScanBuilder --> Scan
    WriteBuilder --> BatchWrite
    
    Scan --> SearchEngine
    BatchWrite --> SearchEngine
    SearchEngine --> TxLog
    
    SearchEngine --> StorageStrategy
    StorageStrategy --> FileReaders
    
    SearchEngine --> JNI
    JNI --> TantivyCore
    
    %% Feedback loops
    TxLog -.-> TableProvider
    StorageStrategy -.-> SearchEngine
    TantivyCore -.-> JNI
```

## Data Flow Patterns

### 1. Write Data Flow

#### Document Indexing Pipeline

```mermaid
flowchart TD
    A[DataFrame Input] --> B[Schema Validation]
    B --> C[Partition Distribution]
    C --> D[Row Serialization]
    D --> E[Document JSON Creation]
    E --> F[Native Index Addition]
    F --> G[Index Commit]
    G --> H[Archive Creation]
    H --> I[Storage Upload]
    I --> J[Transaction Log Update]
    J --> K[Statistics Collection]
    K --> L[Write Complete]

    %% Error paths
    B -.->|Schema Mismatch| ERR1[Schema Error]
    E -.->|JSON Error| ERR2[Serialization Error]
    F -.->|Native Error| ERR3[Index Error]
    I -.->|Storage Error| ERR4[Upload Error]
    
    %% Recovery paths
    ERR1 -.->|Auto Schema Evolution| B
    ERR2 -.->|Fallback Conversion| E
    ERR3 -.->|Retry with Cleanup| F
    ERR4 -.->|Exponential Backoff| I
```

#### Detailed Write Component Interactions

1. **Initial Setup Phase**
   ```
   SparkApp → DataSource.write() → WriteBuilder → BatchWrite
   ```
   - Schema inference and validation
   - Transaction log initialization
   - Partition strategy determination

2. **Per-Partition Processing**
   ```
   BatchWrite → SearchEngine.create() → TantivyNative.createIndex()
   ```
   - Native index creation per partition
   - Schema mapping Spark → Tantivy
   - Index handle registration

3. **Document Processing Loop**
   ```
   For each row:
     RowConverter.toJSON() → SearchEngine.addDocument() → JNI.addDocument()
   ```
   - Row-by-row conversion to JSON
   - Native document addition
   - Incremental statistics collection

4. **Finalization Phase**
   ```
   SearchEngine.commit() → ArchiveFormat.create() → Storage.upload() → TxLog.addFile()
   ```
   - Index commitment and optimization
   - Archive file creation with compression
   - Storage upload with retry logic
   - Transaction log entry creation

### 2. Read Data Flow

#### Query Execution Pipeline

```mermaid
flowchart TD
    A[SQL Query] --> B[Filter Analysis]
    B --> C[Query Plan Generation]
    C --> D[File Pruning]
    D --> E[Partition Assignment]
    E --> F[Index Loading]
    F --> G[Query Execution]
    G --> H[Result Conversion]
    H --> I[Row Assembly]
    I --> J[DataFrame Return]

    %% Optimization paths
    B -.->|Pushdown Filters| C
    D -.->|Min/Max Pruning| E
    F -.->|Parallel Loading| G
    G -.->|Result Streaming| H
    
    %% Caching paths  
    F -.->|Cache Hit| CACHE[Local Cache]
    CACHE -.->|Cached Results| G
```

#### Detailed Read Component Interactions

1. **Query Planning Phase**
   ```
   SparkSQL → ScanBuilder.pushFilters() → FiltersToQueryConverter.convert()
   ```
   - Filter pushdown optimization
   - Spark filters → Tantivy query conversion
   - Query optimization and validation

2. **File Discovery Phase**
   ```
   Scan → TransactionLog.listFiles() → Statistics-based pruning
   ```
   - Transaction log reading
   - File metadata extraction
   - Min/max statistics-based pruning

3. **Per-File Processing**
   ```
   For each index file:
     StorageStrategy.read() → SearchEngine.load() → TantivyNative.search()
   ```
   - Optimal storage reader selection
   - Index loading from archive format
   - Native search execution

4. **Result Processing**
   ```
   Native results → RowConverter.fromJSON() → InternalRow assembly
   ```
   - JSON result deserialization
   - Type conversion and validation
   - Spark InternalRow creation

### 3. Storage Access Patterns

#### Intelligent Storage Selection

```mermaid
flowchart LR
    subgraph "Path Analysis"
        PathInput[File Path Input]
        ProtocolDetector[Protocol Detector]
    end
    
    subgraph "Strategy Selection"
        S3Strategy[S3 Optimized Strategy]
        StandardStrategy[Standard Strategy]
        ForceStandard{Force Standard?}
    end
    
    subgraph "Reader Implementation"
        S3Reader[S3 Optimized Reader<br/>+ Chunked Caching<br/>+ Predictive Prefetch]
        StandardReader[Standard File Reader<br/>+ Hadoop Integration<br/>+ HDFS Support]
    end
    
    subgraph "Performance Features"
        ChunkCache[Chunk Cache Manager]
        Prefetcher[Predictive Prefetcher]
        ConnectionPool[Connection Pool]
    end

    PathInput --> ProtocolDetector
    ProtocolDetector -->|s3://,s3a://,s3n://| S3Strategy
    ProtocolDetector -->|hdfs://,file://| StandardStrategy
    ProtocolDetector --> ForceStandard
    ForceStandard -->|Yes| StandardStrategy
    ForceStandard -->|No| S3Strategy
    
    S3Strategy --> S3Reader
    StandardStrategy --> StandardReader
    
    S3Reader --> ChunkCache
    S3Reader --> Prefetcher  
    S3Reader --> ConnectionPool
```

#### Storage Optimization Flows

1. **S3 Optimized Access Pattern**
   ```
   Range Request → Chunk Cache Check → Prefetch Next Chunks → Parallel Downloads
   ```
   - Intelligent chunk-based reading
   - LRU cache with size limits
   - Predictive prefetching based on access patterns
   - Connection pooling for concurrent requests

2. **Standard Hadoop Access Pattern**
   ```
   Path Resolution → FileSystem API → Direct Read → Hadoop Caching
   ```
   - Standard Hadoop FileSystem abstraction
   - Built-in Hadoop caching mechanisms
   - Direct file system integration

## Inter-Component Communication

### 1. Synchronous Communication Patterns

#### Direct Method Invocation
```
Component A → Component B.method() → Result/Exception
```

**Examples:**
- `DataSource → TableProvider.getTable()`
- `SearchEngine → TantivyNative.search()`
- `TransactionLog → FileSystem.write()`

#### Request-Response with Error Handling
```mermaid
sequenceDiagram
    participant A as Component A
    participant B as Component B
    
    A->>B: method(params)
    
    alt Success Case
        B-->>A: Result
    else Error Case
        B-->>A: Exception
        A->>A: Error Handling
        A->>B: retry(params)
        B-->>A: Result/Exception
    end
```

### 2. Asynchronous Communication Patterns

#### Future-Based Operations
```scala
// Example: Parallel index loading
val futures = indexFiles.map { file =>
  Future {
    searchEngine.loadFromArchive(file)
  }
}
val results = Future.sequence(futures)
```

#### Iterator-Based Streaming
```scala
// Example: Streaming search results
def search(query: String): Iterator[InternalRow] = {
  val nativeResults = tantivyNative.search(indexHandle, query)
  nativeResults.map(jsonResult => convertToInternalRow(jsonResult))
}
```

### 3. Event-Driven Patterns

#### Transaction Log Events
```mermaid
graph LR
    subgraph "Event Producers"
        WriteOp[Write Operation]
        ReadOp[Read Operation]
        SchemaChange[Schema Evolution]
    end
    
    subgraph "Event Types"
        AddFile[ADD File Event]
        RemoveFile[REMOVE File Event]
        Metadata[METADATA Event]
        Commit[COMMIT Event]
    end
    
    subgraph "Event Consumers"
        StatsCollector[Statistics Collector]
        CacheInvalidator[Cache Invalidator]
        MetadataUpdater[Metadata Updater]
    end

    WriteOp --> AddFile
    WriteOp --> Commit
    ReadOp --> Metadata
    SchemaChange --> Metadata
    
    AddFile --> StatsCollector
    AddFile --> CacheInvalidator
    Metadata --> MetadataUpdater
    Commit --> StatsCollector
```

## Error Handling and Recovery

### 1. Error Propagation Hierarchy

```mermaid
flowchart TD
    subgraph "Native Layer Errors"
        RustPanic[Rust Panic<br/>Memory Safety Violations]
        IndexError[Index Corruption<br/>Invalid Operations]
        SerializationError[JSON Serialization<br/>Type Mismatches]
    end
    
    subgraph "JNI Layer Translation"
        JNIExceptionHandler[JNI Exception Handler<br/>Native → Java Translation]
    end
    
    subgraph "Scala/Java Layer Errors"
        RuntimeException[Runtime Exceptions<br/>Invalid States]
        IOExceptions[I/O Exceptions<br/>Storage Failures]
        SparkExceptions[Spark Exceptions<br/>Query Planning Errors]
    end
    
    subgraph "Application Layer Recovery"
        RetryLogic[Retry Logic<br/>Exponential Backoff]
        GracefulDegradation[Graceful Degradation<br/>Fallback Mechanisms]
        ErrorReporting[Error Reporting<br/>Logging & Monitoring]
    end

    RustPanic --> JNIExceptionHandler
    IndexError --> JNIExceptionHandler
    SerializationError --> JNIExceptionHandler
    
    JNIExceptionHandler --> RuntimeException
    JNIExceptionHandler --> IOExceptions
    
    RuntimeException --> RetryLogic
    IOExceptions --> RetryLogic
    SparkExceptions --> GracefulDegradation
    
    RetryLogic --> ErrorReporting
    GracefulDegradation --> ErrorReporting
```

### 2. Recovery Strategies by Component

#### Search Engine Recovery
```scala
// Graceful degradation when native library unavailable
class TantivySearchEngine {
  private val nativeAvailable = TantivyNative.ensureLibraryLoaded()
  
  def search(query: String): Iterator[InternalRow] = {
    if (nativeAvailable) {
      performNativeSearch(query)
    } else {
      // Fallback to all-document scan with manual filtering
      performFallbackSearch(query)
    }
  }
}
```

#### Storage Layer Recovery
```scala
// S3 with fallback to standard operations
class S3OptimizedReader {
  def read(path: Path, offset: Long, length: Int): Array[Byte] = {
    try {
      optimizedRangeRead(path, offset, length)
    } catch {
      case _: S3Exception => 
        logger.warn(s"S3 optimization failed for $path, falling back to standard read")
        standardFileRead(path, offset, length)
    }
  }
}
```

#### Transaction Log Recovery
```scala
// Version conflict resolution
class TransactionLog {
  def addFile(action: AddAction): Long = {
    var retries = 0
    while (retries < maxRetries) {
      try {
        val version = getLatestVersion() + 1
        writeAction(version, action)
        return version
      } catch {
        case _: FileAlreadyExistsException =>
          retries += 1
          Thread.sleep(backoffDelay(retries))
      }
    }
    throw new RuntimeException("Failed to add file after max retries")
  }
}
```

## Performance Optimization Flows

### 1. Query Optimization Pipeline

```mermaid
flowchart TD
    A[Original Query] --> B[Filter Analysis]
    B --> C{Pushdown Possible?}
    C -->|Yes| D[Convert to Tantivy Query]
    C -->|No| E[Post-Processing Filter]
    D --> F[Statistics-Based Pruning]
    F --> G[Partition Assignment]
    G --> H[Parallel Execution]
    H --> I[Result Streaming]
    E --> I
    
    subgraph "Optimization Techniques"
        F1[Min/Max Pruning<br/>Skip irrelevant files]
        F2[Bloom Filter<br/>Existence checks]
        F3[Index Selectivity<br/>Cost-based decisions]
    end
    
    F --> F1
    F --> F2
    F --> F3
```

### 2. I/O Optimization Patterns

#### Predictive Caching Flow
```mermaid
sequenceDiagram
    participant App as Application
    participant Cache as Chunk Cache
    participant Predictor as Prefetcher
    participant Storage as S3 Storage

    App->>Cache: request chunk(file, offset)
    
    alt Cache Hit
        Cache-->>App: cached data
    else Cache Miss
        Cache->>Storage: fetch chunk(file, offset)
        Cache->>Predictor: predict next access(file, offset)
        Predictor->>Storage: prefetch next chunks
        Storage-->>Cache: chunk data
        Cache-->>App: chunk data
        
        par Background Prefetch
            Storage-->>Cache: prefetched chunks
        end
    end
```

#### Connection Pool Management
```mermaid
stateDiagram-v2
    [*] --> PoolInitialized : Create Pool
    PoolInitialized --> ConnectionAvailable : Get Connection
    ConnectionAvailable --> ConnectionInUse : Borrow
    ConnectionInUse --> ConnectionAvailable : Return
    ConnectionAvailable --> ConnectionClosed : Idle Timeout
    ConnectionClosed --> ConnectionAvailable : Recreate
    ConnectionAvailable --> PoolShutdown : Pool Close
    PoolShutdown --> [*]
    
    note right of ConnectionInUse
        Max concurrent connections: 50
        Connection timeout: 30s
        Idle timeout: 5min
    end note
```

### 3. Memory Management Flows

#### Native Memory Lifecycle
```mermaid
flowchart LR
    subgraph "JVM Heap"
        SearchEngine[SearchEngine Object]
        IndexHandle[Index Handle<br/>Long value]
    end
    
    subgraph "Native Heap" 
        IndexRegistry[Index Registry<br/>HashMap<i64, IndexManager>]
        TantivyIndex[Tantivy Index<br/>Native Memory]
    end
    
    subgraph "Lifecycle Events"
        Create[Index Creation<br/>JNI Call]
        Use[Search Operations<br/>JNI Calls]
        Close[Resource Cleanup<br/>JNI Call]
    end
    
    SearchEngine --> IndexHandle
    IndexHandle --> IndexRegistry
    IndexRegistry --> TantivyIndex
    
    Create --> TantivyIndex
    Use --> TantivyIndex
    Close --> TantivyIndex
    
    TantivyIndex -.->|Finalize| Close
    SearchEngine -.->|GC| Close
```

This comprehensive documentation shows how all components interact within the Tantivy4Spark system, providing clear visibility into data flows, communication patterns, and optimization strategies.