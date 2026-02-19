# UML Diagrams for IndexTables4Spark

## Table of Contents

1. [Class Diagrams](#class-diagrams)
2. [Sequence Diagrams](#sequence-diagrams)
3. [Component Diagrams](#component-diagrams)
4. [Deployment Diagrams](#deployment-diagrams)

## Class Diagrams

### Core DataSource V2 Implementation

```mermaid
classDiagram
    %% Spark Interfaces
    class DataSourceRegister {
        <<interface>>
        +shortName() String
    }

    class RelationProvider {
        <<interface>>
        +createRelation(SQLContext, Map[String,String]) BaseRelation
    }

    class CreatableRelationProvider {
        <<interface>>
        +createRelation(SQLContext, SaveMode, Map[String,String], DataFrame) BaseRelation
    }

    class TableProvider {
        <<interface>>
        +inferSchema(CaseInsensitiveStringMap) StructType
        +getTable(StructType, Transform[], CaseInsensitiveStringMap) Table
    }

    class Table {
        <<interface>>
        +name() String
        +schema() StructType
        +capabilities() Set[TableCapability]
        +newScanBuilder(CaseInsensitiveStringMap) ScanBuilder
        +newWriteBuilder(LogicalWriteInfo) WriteBuilder
    }

    class ScanBuilder {
        <<interface>>
        +build() Scan
        +pushFilters(Filter[]) Filter[]
        +pushedFilters() Filter[]
    }

    class WriteBuilder {
        <<interface>>
        +build() Write
    }

    %% IndexTables4Spark Implementation Classes
    class IndexTables4SparkDataSource {
        +shortName() String
        +createRelation(SQLContext, Map[String,String]) BaseRelation
        +createRelation(SQLContext, SaveMode, Map[String,String], DataFrame) BaseRelation
    }

    class IndexTables4SparkTableProvider {
        +inferSchema(CaseInsensitiveStringMap) StructType
        +getTable(StructType, Transform[], CaseInsensitiveStringMap) Table
        -readSchemaFromPath(String) StructType
    }

    class IndexTables4SparkTable {
        -schema: StructType
        -path: String
        -properties: Map[String, String]
        +name() String
        +schema() StructType
        +capabilities() Set[TableCapability]
        +newScanBuilder(CaseInsensitiveStringMap) ScanBuilder
        +newWriteBuilder(LogicalWriteInfo) WriteBuilder
    }

    class IndexTables4SparkScanBuilder {
        -schema: StructType
        -path: String
        -pushedFilters: Array[Filter]
        +build() Scan
        +pushFilters(Filter[]) Filter[]
        +pushedFilters() Filter[]
        -convertFiltersToQuery(Filter[]) String
    }

    class IndexTables4SparkScan {
        -schema: StructType
        -path: String
        -query: String
        +readSchema() StructType
        +toBatch() Batch
        +planInputPartitions() Array[InputPartition]
    }

    class IndexTables4SparkWriteBuilder {
        -schema: StructType
        -path: String
        -options: CaseInsensitiveStringMap
        +build() Write
    }

    class IndexTables4SparkBatchWrite {
        -schema: StructType
        -path: String
        +createBatchTask(Int) DataWriterFactory
        +commit(WriterCommitMessage[]) Unit
        +abort(WriterCommitMessage[]) Unit
    }

    %% Relationships
    DataSourceRegister <|-- IndexTables4SparkDataSource
    RelationProvider <|-- IndexTables4SparkDataSource
    CreatableRelationProvider <|-- IndexTables4SparkDataSource

    TableProvider <|.. IndexTables4SparkTableProvider
    Table <|.. IndexTables4SparkTable
    ScanBuilder <|.. IndexTables4SparkScanBuilder
    WriteBuilder <|.. IndexTables4SparkWriteBuilder

    IndexTables4SparkDataSource --> IndexTables4SparkTableProvider : creates
    IndexTables4SparkTableProvider --> IndexTables4SparkTable : creates
    IndexTables4SparkTable --> IndexTables4SparkScanBuilder : creates
    IndexTables4SparkTable --> IndexTables4SparkWriteBuilder : creates
    IndexTables4SparkScanBuilder --> IndexTables4SparkScan : builds
    IndexTables4SparkWriteBuilder --> IndexTables4SparkBatchWrite : builds
```

### Search Engine and Native Integration

```mermaid
classDiagram
    %% Search Layer Classes
    class TantivySearchEngine {
        -logger: Logger
        -mapper: ObjectMapper
        -schema: StructType
        -indexHandle: Long
        +TantivySearchEngine(StructType)
        +TantivySearchEngine(StructType, Long)
        +addDocument(InternalRow) Unit
        +addDocuments(Iterator[InternalRow]) Unit
        +search(String) Iterator[InternalRow]
        +searchWithLimit(String, Int) Iterator[InternalRow]
        +commit() Unit
        +saveToArchive(String) Unit
        +loadFromArchive(String) TantivySearchEngine
        +close() Unit
    }

    class TantivyNative {
        <<object>>
        -logger: Logger
        -libraryLoaded: AtomicBoolean
        +ensureLibraryLoaded() Boolean
        +createIndex(String) Long
        +addDocument(Long, String) Boolean
        +commit(Long) Boolean
        +search(Long, String) String
        +searchWithLimit(Long, String, Int) String
        +saveIndex(Long, String) Boolean
        +loadIndex(String) Long
        +closeIndex(Long) Unit
        -loadNativeLibrary() Unit
        -extractLibraryFromJar() File
    }

    class SchemaConverter {
        <<object>>
        -logger: Logger
        +sparkToTantivySchema(StructType) String
        +tantivyToSparkSchema(String) StructType
        +convertDataType(DataType) String
        +convertTantivyType(String) DataType
        -mapSparkFieldToTantivy(StructField) Map[String, Any]
        -mapTantivyFieldToSpark(Map[String, Any]) StructField
    }

    class RowConverter {
        <<object>>
        +internalRowToJson(InternalRow, StructType, ObjectMapper) String
        +jsonToInternalRow(String, StructType, ObjectMapper) InternalRow
        +convertValue(Any, DataType) Any
        -handleNullValue(DataType) Any
    }

    class FiltersToQueryConverter {
        <<object>>
        +convertFilters(Array[Filter]) String
        +convertSingleFilter(Filter) String
        -convertEqualTo(EqualTo) String
        -convertIsNotNull(IsNotNull) String
        -convertIsNull(IsNull) String
        -convertIn(In) String
        -convertStringContains(StringContains) String
        -convertAnd(And) String
        -convertOr(Or) String
        -convertNot(Not) String
        -escapeQueryString(String) String
    }

    %% Relationships
    TantivySearchEngine --> TantivyNative : uses
    TantivySearchEngine --> SchemaConverter : uses
    TantivySearchEngine --> RowConverter : uses
    IndexTables4SparkScanBuilder --> FiltersToQueryConverter : uses
    IndexTables4SparkScan --> TantivySearchEngine : creates
    IndexTables4SparkBatchWrite --> TantivySearchEngine : creates
```

### Storage Strategy Pattern

```mermaid
classDiagram
    %% Storage Abstraction
    class FileReader {
        <<interface>>
        +read(Path, Long, Int) Array[Byte]
        +readFully(Path) Array[Byte]
        +exists(Path) Boolean
        +getFileSize(Path) Long
        +listFiles(Path) Array[Path]
    }

    class StorageStrategy {
        <<abstract>>
        +canHandle(String) Boolean
        +createReader(Path, Configuration) FileReader
        +isOptimized() Boolean
        +getProtocolName() String
    }

    %% Concrete Storage Implementations
    class S3StorageStrategy {
        +canHandle(String) Boolean
        +createReader(Path, Configuration) FileReader
        +isOptimized() Boolean
        +getProtocolName() String
        -isS3Protocol(String) Boolean
    }

    class StandardStorageStrategy {
        +canHandle(String) Boolean
        +createReader(Path, Configuration) FileReader
        +isOptimized() Boolean
        +getProtocolName() String
    }

    class S3OptimizedReader {
        -logger: Logger
        -s3Client: S3Client
        -bucketName: String
        -cacheManager: ChunkCacheManager
        -config: Configuration
        +read(Path, Long, Int) Array[Byte]
        +readFully(Path) Array[Byte]
        +exists(Path) Boolean
        +getFileSize(Path) Long
        +listFiles(Path) Array[Path]
        +readChunk(Path, Long, Int) Array[Byte]
        +prefetchNext(Path, Long) Unit
        -createS3Client() S3Client
        -parseS3Path(String) (String, String)
    }

    class StandardFileReader {
        -logger: Logger
        -fs: FileSystem
        -config: Configuration
        +read(Path, Long, Int) Array[Byte]
        +readFully(Path) Array[Byte]
        +exists(Path) Boolean
        +getFileSize(Path) Long
        +listFiles(Path) Array[Path]
    }

    %% Factory and Cache
    class ProtocolBasedIOFactory {
        <<object>>
        -strategies: Array[StorageStrategy]
        +getStrategy(String) StorageStrategy
        +createReader(String, Configuration) FileReader
        +isOptimizedPath(String) Boolean
        -detectProtocol(String) String
    }

    class ChunkCacheManager {
        -cache: ConcurrentHashMap[String, CacheEntry]
        -maxCacheSize: Long
        -chunkSize: Int
        +get(String, Long) Option[Array[Byte]]
        +put(String, Long, Array[Byte]) Unit
        +evictOldest() Unit
        +clear() Unit
        -generateCacheKey(String, Long) String
    }

    %% Archive Format
    class TantivyArchiveFormat {
        <<object>>
        +createArchive(Array[Byte], Map[String, Array[Byte]]) Array[Byte]
        +extractComponents(Array[Byte]) Map[String, Array[Byte]]
        +readFooter(Array[Byte]) ArchiveFooter
        +writeFooter(Map[String, Long]) Array[Byte]
        -calculateChecksum(Array[Byte]) Long
    }

    class ArchiveFooter {
        +componentOffsets: Map[String, Long]
        +componentSizes: Map[String, Long]
        +totalSize: Long
        +checksum: Long
        +version: Int
    }

    %% Relationships
    StorageStrategy <|-- S3StorageStrategy
    StorageStrategy <|-- StandardStorageStrategy
    FileReader <|.. S3OptimizedReader
    FileReader <|.. StandardFileReader

    S3StorageStrategy --> S3OptimizedReader : creates
    StandardStorageStrategy --> StandardFileReader : creates
    
    ProtocolBasedIOFactory --> StorageStrategy : uses
    ProtocolBasedIOFactory --> FileReader : creates
    
    S3OptimizedReader --> ChunkCacheManager : uses
    TantivySearchEngine --> TantivyArchiveFormat : uses
    TantivyArchiveFormat --> ArchiveFooter : creates
```

### Transaction Log System

```mermaid
classDiagram
    %% Transaction Log Core
    class TransactionLog {
        -logger: Logger
        -tablePath: Path
        -spark: SparkSession
        -fs: FileSystem
        -transactionLogPath: Path
        -mapper: ObjectMapper
        +initialize(StructType) Unit
        +addFile(AddAction) Long
        +listFiles() Seq[AddAction]
        +getSchema() StructType
        +getLatestVersion() Long
        +readVersion(Long) Seq[Action]
        +writeAction(Long, Action) Unit
        +getVersions() Seq[Long]
        -createVersionFile(Long) Path
        -parseVersionFromFileName(String) Option[Long]
    }

    %% Action Types
    class Action {
        <<abstract>>
        +toJson(ObjectMapper) String
    }

    class MetadataAction {
        +id: String
        +name: Option[String]
        +description: Option[String]
        +format: FileFormat
        +schemaString: String
        +partitionColumns: Seq[String]
        +configuration: Map[String, String]
        +createdTime: Option[Long]
        +toJson(ObjectMapper) String
    }

    class AddAction {
        +path: String
        +partitionValues: Map[String, String]
        +size: Long
        +modificationTime: Long
        +dataChange: Boolean
        +stats: Option[String]
        +tags: Option[Map[String, String]]
        +toJson(ObjectMapper) String
        +getMinValues() Map[String, Any]
        +getMaxValues() Map[String, Any]
        +getRowCount() Long
    }

    class RemoveAction {
        +path: String
        +deletionTimestamp: Long
        +dataChange: Boolean
        +toJson(ObjectMapper) String
    }

    class CommitInfo {
        +version: Long
        +timestamp: Long
        +userId: Option[String]
        +userName: Option[String]
        +operation: String
        +operationParameters: Map[String, String]
        +readVersion: Option[Long]
        +isBlindAppend: Boolean
        +toJson(ObjectMapper) String
    }

    %% Supporting Classes
    class FileFormat {
        +provider: String
        +options: Map[String, String]
    }

    class Statistics {
        +numRecords: Long
        +minValues: Map[String, Any]
        +maxValues: Map[String, Any]
        +nullCount: Map[String, Long]
        +toJson() String
        +fromJson(String) Statistics
    }

    %% Relationships
    Action <|-- MetadataAction
    Action <|-- AddAction
    Action <|-- RemoveAction
    Action <|-- CommitInfo

    MetadataAction --> FileFormat : contains
    AddAction --> Statistics : contains
    TransactionLog --> Action : manages
    TransactionLog --> MetadataAction : creates
    TransactionLog --> AddAction : creates
    TransactionLog --> RemoveAction : creates
    TransactionLog --> CommitInfo : creates
```

## Sequence Diagrams

### Write Operation Sequence

```mermaid
sequenceDiagram
    participant Client as Spark Application
    participant DS as IndexTables4SparkDataSource
    participant Table as IndexTables4SparkTable
    participant WB as WriteBuilder
    participant BatchWrite as BatchWrite
    participant TL as TransactionLog
    participant SE as SearchEngine
    participant JNI as TantivyNative
    participant Storage as StorageStrategy

    Client->>DS: df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)
    DS->>Table: newWriteBuilder(writeInfo)
    Table->>WB: create WriteBuilder
    WB->>BatchWrite: build()
    BatchWrite->>TL: initialize(schema)
    
    alt First time initialization
        TL->>Storage: create transaction log directory
        TL->>TL: write MetadataAction(schema)
    end

    loop For each partition
        BatchWrite->>SE: createSearchEngine(schema)
        SE->>JNI: createIndex(schemaJson)
        JNI-->>SE: indexHandle
        
        loop For each row in partition
            BatchWrite->>SE: addDocument(row)
            SE->>JNI: addDocument(handle, docJson)
        end
        
        SE->>JNI: commit()
        SE->>Storage: saveToArchive(partitionPath)
        BatchWrite->>TL: createAddAction(path, stats)
    end

    BatchWrite->>TL: addFile(AddAction)
    TL->>Storage: write transaction log entry
    TL-->>BatchWrite: version number
    BatchWrite-->>Client: write complete
```

### Read/Search Operation Sequence

```mermaid
sequenceDiagram
    participant Client as Spark SQL Engine
    participant DS as DataSource
    participant Table as IndexTables4SparkTable
    participant SB as ScanBuilder
    participant Scan as IndexTables4SparkScan
    participant TL as TransactionLog
    participant SE as SearchEngine
    participant JNI as TantivyNative
    participant Storage as StorageStrategy

    Client->>DS: spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    DS->>Table: getTable()
    Table->>TL: getSchema()
    TL-->>Table: schema from transaction log
    Table-->>DS: table with schema
    
    Client->>Table: .filter(conditions).select(columns)
    Table->>SB: newScanBuilder(options)
    SB->>SB: pushFilters(sparkFilters)
    SB->>SB: convertFiltersToTantivyQuery()
    
    SB->>Scan: build()
    Scan->>TL: listFiles()
    TL-->>Scan: list of AddActions
    
    Scan->>Scan: pruneFiles(query, minMaxStats)
    
    loop For each relevant index file
        Scan->>SE: createSearchEngine()
        SE->>Storage: loadFromArchive(filePath)
        Storage-->>SE: index components
        SE->>JNI: loadIndex(indexData)
        JNI-->>SE: indexHandle
        
        SE->>JNI: search(handle, queryJson)
        JNI-->>SE: resultDocumentsJson
        SE->>SE: convertToInternalRows()
        SE-->>Scan: Iterator[InternalRow]
    end
    
    Scan-->>Client: Combined search results as DataFrame
```

### Native Library Loading Sequence

```mermaid
sequenceDiagram
    participant SE as SearchEngine
    participant Native as TantivyNative
    participant JVM as Java Runtime
    participant FS as File System
    participant JAR as JAR Resources

    SE->>Native: ensureLibraryLoaded()
    
    alt Library not loaded
        Native->>Native: detectPlatform()
        Native->>JAR: getResourceAsStream(libraryPath)
        JAR-->>Native: library bytes
        
        Native->>FS: createTempFile(.dylib/.so/.dll)
        Native->>FS: write library bytes to temp file
        Native->>JVM: System.load(tempFilePath)
        
        alt Load successful
            Native->>Native: set libraryLoaded = true
            Native->>JVM: addShutdownHook(cleanup)
        else Load failed
            Native->>FS: delete temp file
            Native-->>SE: false
        end
    end
    
    Native-->>SE: true (library ready)
    
    SE->>Native: createIndex(schemaJson)
    Native->>JVM: call native method via JNI
    JVM-->>Native: index handle
    Native-->>SE: handle
```

### Transaction Log Versioning Sequence

```mermaid
sequenceDiagram
    participant Writer as Write Operation
    participant TL as TransactionLog
    participant FS as File System
    participant Reader as Read Operation

    Writer->>TL: addFile(AddAction)
    TL->>TL: getLatestVersion()
    TL->>FS: list transaction log files
    FS-->>TL: [000000.json, 000001.json, ...]
    TL->>TL: parse latest version number
    
    TL->>TL: nextVersion = latest + 1
    TL->>FS: write nextVersion.json with AddAction
    FS-->>TL: write complete
    TL-->>Writer: version number
    
    Note over TL: Concurrent read can happen
    
    Reader->>TL: listFiles()
    TL->>FS: list all transaction log files
    FS-->>TL: all version files
    
    loop For each version file
        TL->>FS: read version file
        FS-->>TL: actions in file
        TL->>TL: filter for AddActions
    end
    
    TL->>TL: combine all AddActions
    TL-->>Reader: complete file list with metadata
```

## Component Diagrams

### High-Level Component Architecture

```mermaid
graph TB
    subgraph "Spark Integration Layer"
        DataSource[DataSource V2<br/>Implementation]
        TableProvider[Table Provider]
        ScanBuilder[Scan Builder]
        WriteBuilder[Write Builder]
    end

    subgraph "Core Engine Layer"
        SearchEngine[Tantivy Search Engine]
        SchemaConv[Schema Converter]
        RowConv[Row Converter]
        FilterConv[Filter Converter]
    end

    subgraph "Storage Abstraction Layer"
        StorageFactory[Storage Factory]
        S3Reader[S3 Optimized Reader]
        StdReader[Standard File Reader]
        CacheManager[Chunk Cache Manager]
    end

    subgraph "Transaction Management Layer"
        TxLog[Transaction Log]
        Actions[Action Types]
        Statistics[Statistics Manager]
    end

    subgraph "Native Integration Layer"
        JNIBridge[JNI Bridge]
        NativeLib[Native Library Loader]
        IndexManager[Index Manager<br/>(Rust)]
        TantivyCore[Tantivy Core<br/>(Rust)]
    end

    subgraph "Archive & Serialization"
        ArchiveFormat[Archive Format]
        Compression[Compression Utils]
        Serialization[JSON Serialization]
    end

    %% Connections
    DataSource --> TableProvider
    TableProvider --> ScanBuilder
    TableProvider --> WriteBuilder
    ScanBuilder --> SearchEngine
    WriteBuilder --> SearchEngine

    SearchEngine --> SchemaConv
    SearchEngine --> RowConv
    ScanBuilder --> FilterConv
    SearchEngine --> JNIBridge

    SearchEngine --> StorageFactory
    StorageFactory --> S3Reader
    StorageFactory --> StdReader
    S3Reader --> CacheManager

    SearchEngine --> TxLog
    TxLog --> Actions
    TxLog --> Statistics
    TxLog --> ArchiveFormat

    JNIBridge --> NativeLib
    JNIBridge --> IndexManager
    IndexManager --> TantivyCore

    ArchiveFormat --> Compression
    TxLog --> Serialization
    SearchEngine --> Serialization
```

### Storage Layer Component Detail

```mermaid
graph LR
    subgraph "Storage Strategy Pattern"
        Factory[Protocol Based<br/>IO Factory]
        S3Strategy[S3 Storage<br/>Strategy]
        StdStrategy[Standard Storage<br/>Strategy]
    end

    subgraph "File Readers"
        S3Reader[S3 Optimized Reader]
        StdReader[Standard File Reader]
    end

    subgraph "Optimization Components"
        CacheManager[Chunk Cache<br/>Manager]
        Prefetcher[Predictive<br/>Prefetcher]
        ConnectionPool[S3 Connection<br/>Pool]
    end

    subgraph "Storage Backends"
        S3[Amazon S3<br/>Compatible Storage]
        HDFS[Hadoop Distributed<br/>File System]
        LocalFS[Local File System]
        Azure[Azure Blob<br/>Storage]
        GCS[Google Cloud<br/>Storage]
    end

    Factory --> S3Strategy
    Factory --> StdStrategy
    S3Strategy --> S3Reader
    StdStrategy --> StdReader

    S3Reader --> CacheManager
    S3Reader --> Prefetcher
    S3Reader --> ConnectionPool

    S3Reader --> S3
    S3Reader --> Azure
    S3Reader --> GCS
    StdReader --> HDFS
    StdReader --> LocalFS
```

## Deployment Diagrams

### Single Spark Cluster Deployment

```mermaid
graph TB
    subgraph "Spark Cluster"
        subgraph "Driver Node"
            Driver[Spark Driver JVM]
            DriverLibs[IndexTables4Spark JAR<br/>+ Dependencies]
        end
        
        subgraph "Executor Node 1"
            Executor1[Spark Executor JVM]
            Native1[Native Library<br/>libtantivy4spark.so]
            Cache1[Local File Cache<br/>Chunk Manager]
        end
        
        subgraph "Executor Node 2"
            Executor2[Spark Executor JVM]
            Native2[Native Library<br/>libtantivy4spark.so]
            Cache2[Local File Cache<br/>Chunk Manager]
        end
        
        subgraph "Executor Node N"
            ExecutorN[Spark Executor JVM]
            NativeN[Native Library<br/>libtantivy4spark.so]
            CacheN[Local File Cache<br/>Chunk Manager]
        end
    end

    subgraph "Distributed Storage"
        S3[Amazon S3<br/>Buckets]
        HDFS[HDFS Cluster<br/>NameNode + DataNodes]
    end

    subgraph "Network Infrastructure"
        LoadBalancer[Load Balancer<br/>S3 Endpoint]
        VPC[Virtual Private Cloud<br/>Network Isolation]
    end

    Driver --> Executor1
    Driver --> Executor2
    Driver --> ExecutorN

    Executor1 --> Native1
    Executor2 --> Native2
    ExecutorN --> NativeN

    Native1 --> Cache1
    Native2 --> Cache2
    NativeN --> CacheN

    Cache1 --> LoadBalancer
    Cache2 --> LoadBalancer
    CacheN --> LoadBalancer

    Cache1 --> HDFS
    Cache2 --> HDFS
    CacheN --> HDFS

    LoadBalancer --> S3
    
    VPC -.-> LoadBalancer
    VPC -.-> HDFS
```

### Multi-Cluster Cloud Deployment

```mermaid
graph TB
    subgraph "Region 1 - Primary"
        subgraph "Spark Cluster 1"
            SC1[Spark Cluster<br/>Master + Workers]
            T4S1[IndexTables4Spark<br/>Deployment]
        end
        
        subgraph "Storage Layer 1"
            S3R1[S3 Bucket<br/>Primary Region]
            Cache1[Redis Cluster<br/>Metadata Cache]
        end
    end

    subgraph "Region 2 - Secondary"
        subgraph "Spark Cluster 2" 
            SC2[Spark Cluster<br/>Master + Workers]
            T4S2[IndexTables4Spark<br/>Deployment]
        end
        
        subgraph "Storage Layer 2"
            S3R2[S3 Bucket<br/>Cross-Region Replica]
            Cache2[Redis Cluster<br/>Metadata Cache]
        end
    end

    subgraph "Control Plane"
        K8s[Kubernetes<br/>Orchestration]
        Prometheus[Prometheus<br/>Monitoring]
        Grafana[Grafana<br/>Dashboards]
    end

    subgraph "Data Pipeline"
        Airflow[Apache Airflow<br/>Workflow Management]
        DataCatalog[AWS Glue<br/>Data Catalog]
    end

    SC1 --> T4S1
    SC2 --> T4S2
    T4S1 --> S3R1
    T4S2 --> S3R2
    T4S1 --> Cache1
    T4S2 --> Cache2

    S3R1 -.->|Cross-Region<br/>Replication| S3R2

    K8s --> SC1
    K8s --> SC2
    Prometheus --> SC1
    Prometheus --> SC2
    Prometheus --> Grafana

    Airflow --> SC1
    Airflow --> SC2
    DataCatalog --> S3R1
    DataCatalog --> S3R2
```

### Container-Based Deployment

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Spark Application Pod"
            SparkContainer[Spark Container<br/>openjdk:11-jre]
            T4SJar[tantivy4spark.jar<br/>Application JAR]
            NativeLibs[Native Libraries<br/>/native/*.so]
        end
        
        subgraph "Supporting Services"
            ConfigMap[ConfigMap<br/>Spark Configuration]
            Secret[Secret<br/>Storage Credentials]
            PVC[PersistentVolumeClaim<br/>Local Cache Storage]
        end
    end

    subgraph "Container Registry"
        DockerHub[Docker Registry<br/>Spark Base Images]
        ArtifactRepo[Artifact Repository<br/>JAR Distribution]
    end

    subgraph "External Storage"
        ObjectStore[Object Storage<br/>S3/GCS/Azure]
        MetadataStore[Metadata Store<br/>Transaction Logs]
    end

    SparkContainer --> T4SJar
    T4SJar --> NativeLibs
    SparkContainer --> ConfigMap
    SparkContainer --> Secret
    SparkContainer --> PVC

    SparkContainer -.->|Pull Images| DockerHub
    T4SJar -.->|Download JARs| ArtifactRepo

    NativeLibs --> ObjectStore
    NativeLibs --> MetadataStore
```

This comprehensive UML documentation provides detailed views of the system architecture from multiple perspectives, showing how the various components interact and are deployed in different environments.