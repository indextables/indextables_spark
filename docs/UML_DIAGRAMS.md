# UML Diagrams - Spark Tantivy Handler

## Class Diagrams

### 1. Core Spark Integration Classes

```mermaid
classDiagram
    class FileFormat {
        <<interface>>
        +shortName() String
        +toString() String
        +inferSchema() Option[StructType]
        +prepareWrite() OutputWriterFactory
        +buildReader() Function
        +supportBatch() Boolean
        +isSplitable() Boolean
    }
    
    class TantivyFileFormat {
        +shortName() String
        +toString() String
        +inferSchema() Option[StructType]
        +prepareWrite() OutputWriterFactory
        +buildReader() Function
        +supportBatch() Boolean
        +isSplitable() Boolean
    }
    
    class TantivyOutputWriterFactory {
        -options: Map[String, String]
        -dataSchema: StructType
        +getFileExtension() String
        +newInstance() OutputWriter
    }
    
    class TantivyOutputWriter {
        -path: String
        -dataSchema: StructType
        -options: Map[String, String]
        -context: TaskAttemptContext
        -transactionLog: TransactionLog
        -indexWriter: TantivyIndexWriter
        +write(row: InternalRow)
        +close()
    }
    
    class TantivyFileReader {
        -partitionedFile: PartitionedFile
        -requiredSchema: StructType
        -filters: Seq[Filter]
        -options: Map[String, String]
        -hadoopConf: Configuration
        -searchEngine: TantivySearchEngine
        -s3Reader: S3OptimizedReader
        +read() Iterator[InternalRow]
        -buildQueryFromFilters() String
    }
    
    FileFormat <|-- TantivyFileFormat
    TantivyFileFormat --> TantivyOutputWriterFactory : creates
    TantivyOutputWriterFactory --> TantivyOutputWriter : creates
    TantivyFileFormat --> TantivyFileReader : creates
```

### 2. Search Engine Classes

```mermaid
classDiagram
    class TantivySearchEngine {
        -indexCache: LRUMap[String, TantivyIndex]
        -maxResults: Int
        -objectMapper: ObjectMapper
        -configId: Option[Long]
        -engineId: Option[Long]
        +search(query: String, indexPath: String) Iterator[SearchResult]
        +searchWithFilters() Iterator[SearchResult]
        +refreshIndex(indexPath: String)
        +close()
        -ensureInitialized()
        -parseSearchResults() Iterator[SearchResult]
        -initializeConfig() Long
    }
    
    class TantivyIndexWriter {
        -recordCount: AtomicLong
        -bytesWritten: AtomicLong
        -segmentSize: Long
        -currentSegmentPath: String
        -objectMapper: ObjectMapper
        -configId: Option[Long]
        -writerId: Option[Long]
        -documentBuffer: ListBuffer[Map[String, Any]]
        -batchSize: Int
        +writeRow(row: InternalRow) WriteResult
        +writeBatch(rows: Array[InternalRow]) List[WriteResult]
        +commit() Boolean
        +close()
        -ensureInitialized()
        -convertRowToDocument() Map[String, Any]
        -flushBatch()
    }
    
    class SearchResult {
        +docId: String
        +score: Float
        +dataLocation: DataLocation
        +highlights: Map[String, String]
    }
    
    class TantivyIndex {
        +name: String
        +fields: Map[String, String]
        +segmentPaths: List[String]
    }
    
    class TantivySearchHit {
        +score: Double
        +document: Map[String, Any]
        +snippet: Map[String, String]
    }
    
    class TantivySearchResponse {
        +hits: List[TantivySearchHit]
        +totalHits: Long
        +elapsedTimeMicros: Long
    }
    
    TantivySearchEngine --> SearchResult : produces
    TantivySearchEngine --> TantivyIndex : caches
    TantivySearchEngine --> TantivySearchResponse : parses
    TantivySearchResponse --> TantivySearchHit : contains
    SearchResult --> DataLocation : references
```

### 3. Storage and Transaction Classes

```mermaid
classDiagram
    class S3OptimizedReader {
        -s3Client: AmazonS3
        -readAheadCache: LRUMap[String, Array[Byte]]
        -executor: ExecutorService
        -predictiveReadSize: Long
        -maxConcurrentReads: Int
        +readWithPredictiveIO() Iterator[InternalRow]
        +close()
        -fetchDataWithReadAhead() Array[Byte]
        -triggerPredictiveReads()
        -parseDataToRows() Iterator[InternalRow]
    }
    
    class DataLocation {
        +bucket: String
        +key: String
        +offset: Long
        +length: Long
    }
    
    class TransactionLog {
        -transactionId: AtomicLong
        -entries: ListBuffer[TransactionEntry]
        -logPath: String
        +appendEntry(writeResult: WriteResult)
        +commit()
        +rollback()
        +getEntries() List[TransactionEntry]
        -writeLogToStorage()
        -cleanupFiles()
    }
    
    class TransactionEntry {
        +timestamp: Long
        +operation: String
        +filePath: String
        +metadata: Map[String, String]
    }
    
    class WriteResult {
        +filePath: String
        +bytesWritten: Long
        +recordCount: Long
        +checksum: String
    }
    
    S3OptimizedReader --> DataLocation : uses
    TransactionLog --> TransactionEntry : contains
    TransactionLog --> WriteResult : logs
```

### 4. Configuration Management Classes

```mermaid
classDiagram
    class TantivyConfig {
        <<object>>
        -objectMapper: ObjectMapper
        +fromSpark() TantivyGlobalConfig
        +toJson() String
        +fromJson() Try[TantivyGlobalConfig]
        +validateConfig() List[String]
        -generateFieldMappings() Map[String, TantivyFieldMapping]
        -mapSparkTypeToTantivy() String
        -extractDefaultSearchFields() List[String]
    }
    
    class TantivyGlobalConfig {
        +basePath: String
        +metastore: TantivyMetastoreConfig
        +storageUri: String
        +indexes: List[TantivyIndexConfig]
    }
    
    class TantivyIndexConfig {
        +indexId: String
        +indexUri: String
        +docMapping: TantivyDocMapping
        +searchSettings: TantivySearchSettings
        +indexingSettings: TantivyIndexingSettings
    }
    
    class TantivyDocMapping {
        +mode: String
        +fieldMappings: Map[String, TantivyFieldMapping]
        +timestampField: Option[String]
        +defaultSearchFields: List[String]
    }
    
    class TantivyFieldMapping {
        +fieldType: String
        +indexed: Boolean
        +stored: Boolean
        +fast: Boolean
        +fieldNorms: Boolean
    }
    
    class SchemaManager {
        -objectMapper: ObjectMapper
        -schemaPath: String
        +saveSchema() Try[Unit]
        +loadSchema() Try[TantivyGlobalConfig]
        +schemaExists() Boolean
        +listSchemas() List[String]
        +deleteSchema() Try[Unit]
        +validateSchemaCompatibility() Try[List[String]]
        +generateSampleConfig() String
        +getSchemaStatistics() Try[Map[String, Any]]
        -compareSchemas() List[String]
    }
    
    TantivyConfig --> TantivyGlobalConfig : creates
    TantivyGlobalConfig --> TantivyIndexConfig : contains
    TantivyIndexConfig --> TantivyDocMapping : contains
    TantivyDocMapping --> TantivyFieldMapping : contains
    SchemaManager --> TantivyGlobalConfig : manages
```

### 5. Native Integration Classes

```mermaid
classDiagram
    class TantivyNativeLibrary {
        <<interface>>
        +createConfig(configJson: String) Long
        +destroyConfig(configId: Long)
        +createSearchEngine(configId: Long, indexPath: String) Long
        +search(engineId: Long, query: String, maxHits: Long) String
        +destroySearchEngine(engineId: Long)
        +createIndexWriter() Long
        +indexDocument(writerId: Long, documentJson: String) Boolean
        +commitIndex(writerId: Long) Boolean
        +destroyIndexWriter(writerId: Long)
    }
    
    class TantivyNative {
        <<object>>
        -LIBRARY_NAME: String
        -nativeLib: TantivyNativeLibrary
        +createConfig() Long
        +destroyConfig()
        +createSearchEngine() Long
        +search() String
        +destroySearchEngine()
        +createIndexWriter() Long
        +indexDocument() Boolean
        +commitIndex() Boolean
        +destroyIndexWriter()
        -extractNativeLibrary() String
    }
    
    TantivyNative --> TantivyNativeLibrary : uses
```

## Package Dependencies

```mermaid
graph TB
    subgraph "com.tantivy4spark.core"
        A[TantivyFileFormat]
        B[TantivyFileReader]
        C[TantivyOutputWriter]
    end
    
    subgraph "com.tantivy4spark.search"
        D[TantivySearchEngine]
        E[TantivyIndexWriter]
    end
    
    subgraph "com.tantivy4spark.storage"
        F[S3OptimizedReader]
        G[DataLocation]
    end
    
    subgraph "com.tantivy4spark.transaction"
        H[TransactionLog]
        I[WriteResult]
    end
    
    subgraph "com.tantivy4spark.config"
        J[TantivyConfig]
        K[SchemaManager]
    end
    
    subgraph "com.tantivy4spark.native"
        L[TantivyNative]
    end
    
    A --> D
    A --> B
    A --> C
    B --> D
    B --> F
    C --> E
    C --> H
    E --> L
    D --> L
    E --> J
    D --> J
    K --> J
    H --> I
    F --> G
```

## Class Relationship Summary

### Inheritance Relationships
- `TantivyFileFormat` implements Spark's `FileFormat` interface
- `TantivyNative` implements `TantivyNativeLibrary` interface

### Composition Relationships
- `TantivyFileFormat` creates `TantivyOutputWriterFactory` and `TantivyFileReader`
- `TantivyOutputWriter` contains `TransactionLog` and `TantivyIndexWriter`
- `TantivyFileReader` contains `TantivySearchEngine` and `S3OptimizedReader`
- `TantivySearchEngine` caches `TantivyIndex` objects

### Dependency Relationships
- Search and indexing components depend on `TantivyNative` for JNI operations
- Configuration components provide settings to all other layers
- Storage components are used by both read and write operations
- Transaction logging is integrated into write operations

### Key Design Patterns
- **Factory Pattern**: `TantivyOutputWriterFactory` creates writers
- **Singleton Pattern**: `TantivyNative` object manages native library
- **Observer Pattern**: Transaction logging observes write operations
- **Strategy Pattern**: Different storage strategies for various cloud providers
- **Builder Pattern**: Configuration objects built from various sources