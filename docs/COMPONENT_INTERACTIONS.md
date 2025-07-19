# Component Interaction Diagrams

## System Component Overview

```mermaid
graph TB
    subgraph "Spark Cluster"
        subgraph "Driver"
            SparkSQL[Spark SQL Engine]
            Catalyst[Catalyst Optimizer]
        end
        
        subgraph "Executor 1"
            QFF1[TantivyFileFormat]
            QSE1[TantivySearchEngine]
            QIW1[TantivyIndexWriter]
            S3OR1[S3OptimizedReader]
            QN1[TantivyNative]
        end
        
        subgraph "Executor 2"
            QFF2[TantivyFileFormat]
            QSE2[TantivySearchEngine]
            QIW2[TantivyIndexWriter]
            S3OR2[S3OptimizedReader]
            QN2[TantivyNative]
        end
    end
    
    subgraph "Native Layer"
        QWRust1[Tantivy Rust - Ex1]
        QWRust2[Tantivy Rust - Ex2]
    end
    
    subgraph "Storage Layer"
        S3[Cloud Storage S3/GCS/Azure]
        MetaStore[(Metadata Store)]
        TxLogs[(Transaction Logs)]
    end
    
    subgraph "Configuration"
        SchemaStore[(Schema Registry)]
        ConfigFiles[Configuration Files]
    end

    %% Driver connections
    SparkSQL --> Catalyst
    Catalyst --> QFF1
    Catalyst --> QFF2
    
    %% Executor 1 connections
    QFF1 --> QSE1
    QFF1 --> QIW1
    QSE1 --> S3OR1
    QIW1 --> QN1
    QSE1 --> QN1
    QN1 --> QWRust1
    
    %% Executor 2 connections
    QFF2 --> QSE2
    QFF2 --> QIW2
    QSE2 --> S3OR2
    QIW2 --> QN2
    QSE2 --> QN2
    QN2 --> QWRust2
    
    %% Storage connections
    S3OR1 --> S3
    S3OR2 --> S3
    QWRust1 --> S3
    QWRust2 --> S3
    QIW1 --> TxLogs
    QIW2 --> TxLogs
    QWRust1 --> MetaStore
    QWRust2 --> MetaStore
    
    %% Configuration connections
    QFF1 --> SchemaStore
    QFF2 --> SchemaStore
    QN1 --> ConfigFiles
    QN2 --> ConfigFiles
```

## Data Flow Interaction Patterns

### 1. Write Path Interactions

```mermaid
graph LR
    subgraph "Application Layer"
        App[Spark Application]
        DF[DataFrame API]
    end
    
    subgraph "Spark Engine"
        Planner[Query Planner]
        Optimizer[Catalyst Optimizer]
        Executor[Spark Executor]
    end
    
    subgraph "Tantivy Handler"
        FileFormat[TantivyFileFormat]
        Writer[TantivyOutputWriter]
        IndexWriter[TantivyIndexWriter]
        TxLog[TransactionLog]
    end
    
    subgraph "Native Integration"
        JNI[JNI Bridge]
        RustLib[Tantivy Rust]
    end
    
    subgraph "Storage Systems"
        IndexStore[Index Storage]
        DataStore[Data Storage]
        LogStore[Log Storage]
    end

    App --> DF
    DF --> Planner
    Planner --> Optimizer
    Optimizer --> Executor
    Executor --> FileFormat
    FileFormat --> Writer
    Writer --> IndexWriter
    Writer --> TxLog
    IndexWriter --> JNI
    JNI --> RustLib
    RustLib --> IndexStore
    RustLib --> DataStore
    TxLog --> LogStore
    
    %% Feedback loops
    TxLog -.-> Writer
    IndexWriter -.-> Writer
    RustLib -.-> IndexWriter
```

### 2. Read Path Interactions

```mermaid
graph LR
    subgraph "Query Layer"
        SQL[Spark SQL]
        Filters[Filter Pushdown]
        Projection[Column Pruning]
    end
    
    subgraph "Tantivy Handler"
        FileFormat[TantivyFileFormat]
        Reader[TantivyFileReader]
        SearchEngine[TantivySearchEngine]
        S3Reader[S3OptimizedReader]
    end
    
    subgraph "Native Search"
        JNI[JNI Bridge]
        TantivyCore[Tantivy Engine]
        IndexEngine[Index Engine]
    end
    
    subgraph "Storage Layer"
        IndexFiles[Index Files]
        DataFiles[Data Files]
        Cache[Predictive Cache]
    end
    
    subgraph "Results"
        Rows[InternalRow Iterator]
        ResultSet[Spark ResultSet]
    end

    SQL --> Filters
    Filters --> Projection
    Projection --> FileFormat
    FileFormat --> Reader
    Reader --> SearchEngine
    SearchEngine --> JNI
    JNI --> TantivyCore
    TantivyCore --> IndexEngine
    IndexEngine --> IndexFiles
    
    %% Data retrieval path
    SearchEngine --> S3Reader
    S3Reader --> Cache
    Cache --> DataFiles
    DataFiles --> S3Reader
    S3Reader --> Rows
    Rows --> ResultSet
    
    %% Search results path
    IndexFiles --> IndexEngine
    IndexEngine --> TantivyCore
    TantivyCore --> JNI
    JNI --> SearchEngine
```

## Component Communication Protocols

### 1. JNI Communication Protocol

```mermaid
sequenceDiagram
    participant Scala as Scala Layer
    participant JNI as JNI Bridge
    participant Rust as Rust Library
    participant QW as Tantivy Core

    Note over Scala,QW: Initialization Phase
    Scala->>JNI: loadLibrary()
    JNI->>Rust: dlopen()
    
    Note over Scala,QW: Configuration Phase
    Scala->>JNI: createConfig(json)
    JNI->>Rust: parse_config()
    Rust->>QW: initialize()
    QW-->>Rust: handle
    Rust-->>JNI: config_id
    JNI-->>Scala: Long
    
    Note over Scala,QW: Operation Phase
    Scala->>JNI: operation(params)
    JNI->>Rust: validate_params()
    Rust->>QW: execute()
    QW-->>Rust: result
    Rust->>Rust: serialize_result()
    Rust-->>JNI: json_string
    JNI-->>Scala: String
    
    Note over Scala,QW: Cleanup Phase
    Scala->>JNI: destroy(id)
    JNI->>Rust: cleanup(handle)
    Rust->>QW: drop_resource()
```

### 2. Cache Interaction Protocol

```mermaid
graph TB
    subgraph "Application Request"
        App[Read Request]
        Location[Data Location]
    end
    
    subgraph "Cache Hierarchy"
        L1[L1 - Memory Cache]
        L2[L2 - Local Disk]
        L3[L3 - Distributed Cache]
    end
    
    subgraph "Storage Backend"
        S3[Cloud Storage]
        Prefetch[Predictive Prefetch]
    end
    
    subgraph "Cache Policies"
        LRU[LRU Eviction]
        TTL[TTL Expiration]
        Size[Size Limits]
    end

    App --> Location
    Location --> L1
    L1 --> |Cache Miss| L2
    L2 --> |Cache Miss| L3
    L3 --> |Cache Miss| S3
    
    S3 --> Prefetch
    Prefetch --> L1
    
    L1 --> LRU
    L2 --> TTL
    L3 --> Size
    
    LRU --> |Evicted| L2
    TTL --> |Expired| S3
    Size --> |Overflow| L2
```

### 3. Transaction Coordination

```mermaid
stateDiagram-v2
    [*] --> Idle
    
    Idle --> Writing : startTransaction()
    Writing --> Writing : writeOperation()
    Writing --> Committing : commit()
    Writing --> RollingBack : error/rollback()
    
    Committing --> Committed : success
    Committing --> RollingBack : failure
    
    RollingBack --> RolledBack : cleanup complete
    
    Committed --> Idle : reset
    RolledBack --> Idle : reset
    
    note right of Writing
        - Log operations
        - Track changes
        - Validate writes
    end note
    
    note right of Committing
        - Flush indexes
        - Write metadata
        - Update logs
    end note
    
    note right of RollingBack
        - Delete partial files
        - Clean up resources
        - Restore state
    end note
```

## Cross-Component Dependencies

### 1. Configuration Dependencies

```mermaid
graph TB
    subgraph "Configuration Sources"
        SparkConf[Spark Configuration]
        AppProps[Application Properties]
        EnvVars[Environment Variables]
        ConfigFiles[Configuration Files]
    end
    
    subgraph "Configuration Management"
        ConfigResolver[Configuration Resolver]
        SchemaManager[Schema Manager]
        Validator[Configuration Validator]
    end
    
    subgraph "Component Configuration"
        SearchConfig[Search Engine Config]
        StorageConfig[Storage Config]
        JNIConfig[JNI Config]
        CacheConfig[Cache Config]
    end
    
    subgraph "Runtime Components"
        SearchEngine[Search Engine]
        StorageLayer[Storage Layer]
        NativeBridge[Native Bridge]
        CacheManager[Cache Manager]
    end

    SparkConf --> ConfigResolver
    AppProps --> ConfigResolver
    EnvVars --> ConfigResolver
    ConfigFiles --> SchemaManager
    
    ConfigResolver --> Validator
    SchemaManager --> Validator
    
    Validator --> SearchConfig
    Validator --> StorageConfig
    Validator --> JNIConfig
    Validator --> CacheConfig
    
    SearchConfig --> SearchEngine
    StorageConfig --> StorageLayer
    JNIConfig --> NativeBridge
    CacheConfig --> CacheManager
```

### 2. Error Handling and Recovery

```mermaid
graph TB
    subgraph "Error Sources"
        AppError[Application Errors]
        JNIError[JNI Errors]
        RustError[Rust Errors]
        StorageError[Storage Errors]
        NetworkError[Network Errors]
    end
    
    subgraph "Error Detection"
        ExceptionHandler[Exception Handler]
        HealthChecker[Health Checker]
        Monitor[System Monitor]
    end
    
    subgraph "Recovery Strategies"
        Retry[Retry Logic]
        Fallback[Fallback Mechanisms]
        CircuitBreaker[Circuit Breaker]
        GracefulDegradation[Graceful Degradation]
    end
    
    subgraph "Error Reporting"
        Logging[Structured Logging]
        Metrics[Error Metrics]
        Alerting[Alert System]
    end

    AppError --> ExceptionHandler
    JNIError --> ExceptionHandler
    RustError --> Monitor
    StorageError --> HealthChecker
    NetworkError --> HealthChecker
    
    ExceptionHandler --> Retry
    HealthChecker --> CircuitBreaker
    Monitor --> Fallback
    
    Retry --> |Success| Logging
    CircuitBreaker --> GracefulDegradation
    Fallback --> Logging
    GracefulDegradation --> Metrics
    
    Logging --> Alerting
    Metrics --> Alerting
```

### 3. Resource Management

```mermaid
graph TB
    subgraph "Resource Types"
        Memory[Memory Pools]
        NativeHandles[Native Handles]
        FileDescriptors[File Descriptors]
        NetworkConns[Network Connections]
        Threads[Thread Pools]
    end
    
    subgraph "Resource Managers"
        MemoryManager[Memory Manager]
        HandleManager[Handle Manager]
        IOManager[I/O Manager]
        ConnPool[Connection Pool]
        ThreadPool[Thread Pool Manager]
    end
    
    subgraph "Lifecycle Management"
        Allocator[Resource Allocator]
        Tracker[Resource Tracker]
        Cleaner[Resource Cleaner]
        Monitor[Resource Monitor]
    end
    
    subgraph "Policies"
        Quotas[Resource Quotas]
        Limits[Usage Limits]
        GC[Garbage Collection]
        Timeout[Timeout Policies]
    end

    Memory --> MemoryManager
    NativeHandles --> HandleManager
    FileDescriptors --> IOManager
    NetworkConns --> ConnPool
    Threads --> ThreadPool
    
    MemoryManager --> Allocator
    HandleManager --> Tracker
    IOManager --> Cleaner
    ConnPool --> Monitor
    ThreadPool --> Monitor
    
    Allocator --> Quotas
    Tracker --> Limits
    Cleaner --> GC
    Monitor --> Timeout
```

## Performance Optimization Interactions

### 1. Query Optimization Pipeline

```mermaid
graph LR
    subgraph "Query Input"
        SQL[SQL Query]
        Filters[WHERE Clauses]
        Projections[SELECT Columns]
    end
    
    subgraph "Optimization Stages"
        Parser[SQL Parser]
        Analyzer[Logical Analyzer]
        Optimizer[Rule-based Optimizer]
        Planner[Physical Planner]
    end
    
    subgraph "Tantivy Optimizations"
        FilterPushdown[Filter Pushdown]
        ColumnPruning[Column Pruning]
        IndexSelection[Index Selection]
        QueryRewrite[Query Rewrite]
    end
    
    subgraph "Execution"
        ParallelExec[Parallel Execution]
        CacheUtilization[Cache Utilization]
        PredictiveIO[Predictive I/O]
    end

    SQL --> Parser
    Filters --> Parser
    Projections --> Parser
    
    Parser --> Analyzer
    Analyzer --> Optimizer
    Optimizer --> Planner
    
    Planner --> FilterPushdown
    Planner --> ColumnPruning
    FilterPushdown --> IndexSelection
    ColumnPruning --> QueryRewrite
    
    IndexSelection --> ParallelExec
    QueryRewrite --> CacheUtilization
    ParallelExec --> PredictiveIO
```

### 2. Caching Strategy Coordination

```mermaid
graph TB
    subgraph "Cache Coordination"
        CacheCoordinator[Cache Coordinator]
        HitRateMonitor[Hit Rate Monitor]
        EvictionManager[Eviction Manager]
        PrefetchScheduler[Prefetch Scheduler]
    end
    
    subgraph "Cache Layers"
        QueryCache[Query Result Cache]
        IndexCache[Index Segment Cache]
        DataCache[Data Block Cache]
        MetadataCache[Metadata Cache]
    end
    
    subgraph "Cache Policies"
        LRUPolicy[LRU Policy]
        TTLPolicy[TTL Policy]
        SizePolicy[Size-based Policy]
        CostPolicy[Cost-based Policy]
    end
    
    subgraph "Performance Metrics"
        HitRate[Cache Hit Rate]
        Latency[Access Latency]
        Throughput[I/O Throughput]
        CostSavings[Cost Savings]
    end

    CacheCoordinator --> QueryCache
    CacheCoordinator --> IndexCache
    CacheCoordinator --> DataCache
    CacheCoordinator --> MetadataCache
    
    HitRateMonitor --> QueryCache
    EvictionManager --> IndexCache
    PrefetchScheduler --> DataCache
    
    QueryCache --> LRUPolicy
    IndexCache --> TTLPolicy
    DataCache --> SizePolicy
    MetadataCache --> CostPolicy
    
    LRUPolicy --> HitRate
    TTLPolicy --> Latency
    SizePolicy --> Throughput
    CostPolicy --> CostSavings
```