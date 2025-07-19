# Sequence Diagrams - Spark Tantivy Handler

## Key Workflow Sequences

### 1. Data Write Operation Sequence

```mermaid
sequenceDiagram
    participant SparkDF as Spark DataFrame
    participant QFF as TantivyFileFormat
    participant QOWF as TantivyOutputWriterFactory
    participant QOW as TantivyOutputWriter
    participant QIW as TantivyIndexWriter
    participant QN as TantivyNative
    participant TxLog as TransactionLog
    participant S3 as Cloud Storage

    SparkDF->>QFF: write.format("tantivy").save()
    QFF->>QOWF: prepareWrite(spark, job, options, schema)
    QOWF->>QOW: newInstance(path, schema, context)
    
    loop For each row batch
        SparkDF->>QOW: write(InternalRow)
        QOW->>QIW: writeRow(row)
        
        QIW->>QIW: convertRowToDocument(row)
        QIW->>QIW: addToBuffer(document)
        
        alt Buffer full
            QIW->>QN: createConfig(configJson)
            QN-->>QIW: configId
            QIW->>QN: createIndexWriter(configId, path, schema)
            QN-->>QIW: writerId
            
            loop For each document in buffer
                QIW->>QN: indexDocument(writerId, documentJson)
                QN-->>QIW: success
            end
            QIW->>QIW: clearBuffer()
        end
        
        QIW-->>QOW: WriteResult
        QOW->>TxLog: appendEntry(writeResult)
    end
    
    SparkDF->>QOW: close()
    QOW->>QIW: commit()
    QIW->>QN: commitIndex(writerId)
    QN->>S3: writeIndexSegments()
    QOW->>TxLog: commit()
    TxLog->>S3: writeTransactionLog()
    QOW->>QN: destroyIndexWriter(writerId)
    QOW->>QN: destroyConfig(configId)
```

### 2. Data Read/Search Operation Sequence

```mermaid
sequenceDiagram
    participant Spark as Spark SQL
    participant QFF as TantivyFileFormat
    participant QFR as TantivyFileReader
    participant QSE as TantivySearchEngine
    participant QN as TantivyNative
    participant S3OR as S3OptimizedReader
    participant S3 as Cloud Storage

    Spark->>QFF: spark.read.format("tantivy").load()
    QFF->>QFR: buildReader(schema, filters, options)
    
    Spark->>QFR: read() Iterator[InternalRow]
    QFR->>QFR: buildQueryFromFilters(filters)
    QFR->>QSE: search(query, indexPath)
    
    QSE->>QSE: ensureInitialized(indexPath)
    QSE->>QN: createConfig(configJson)
    QN-->>QSE: configId
    QSE->>QN: createSearchEngine(configId, indexPath)
    QN-->>QSE: engineId
    
    QSE->>QN: search(engineId, query, maxHits)
    QN->>S3: readIndexSegments()
    QN->>QN: executeTantivySearch()
    QN-->>QSE: searchResultsJson
    
    QSE->>QSE: parseSearchResults(json)
    QSE-->>QFR: Iterator[SearchResult]
    
    loop For each SearchResult
        QFR->>S3OR: readWithPredictiveIO(dataLocation, schema)
        
        S3OR->>S3OR: checkCache(dataLocation)
        alt Cache miss
            S3OR->>S3: fetchDataWithReadAhead(location)
            S3OR->>S3OR: triggerPredictiveReads(nextLocation)
            S3OR->>S3OR: updateCache(data)
        end
        
        S3OR->>S3OR: parseDataToRows(data, schema)
        S3OR-->>QFR: Iterator[InternalRow]
    end
    
    QFR-->>Spark: Iterator[InternalRow]
    
    Spark->>QSE: close()
    QSE->>QN: destroySearchEngine(engineId)
    QSE->>QN: destroyConfig(configId)
```

### 3. Schema Management Sequence

```mermaid
sequenceDiagram
    participant User as User/Application
    participant SM as SchemaManager
    participant QC as TantivyConfig
    participant FS as File System
    participant Spark as Spark Schema

    User->>SM: saveSchema(indexId, sparkSchema)
    SM->>QC: fromSpark(sparkSchema, options)
    
    QC->>QC: generateFieldMappings(schema)
    loop For each field
        QC->>QC: mapSparkTypeToTantivy(dataType)
    end
    
    QC->>QC: extractDefaultSearchFields(schema)
    QC-->>SM: TantivyGlobalConfig
    
    SM->>SM: toJson(config)
    SM->>FS: write(schemaFile, jsonContent)
    FS-->>SM: success
    SM-->>User: Try[Unit]

    User->>SM: validateSchemaCompatibility(indexId, newSchema)
    SM->>FS: read(existingSchemaFile)
    FS-->>SM: existingConfigJson
    SM->>QC: fromJson(existingConfigJson)
    QC-->>SM: existingConfig
    
    SM->>QC: fromSpark(newSchema, options)
    QC-->>SM: newConfig
    
    SM->>SM: compareSchemas(existing, new)
    SM->>SM: detectFieldChanges()
    SM->>SM: detectTypeChanges()
    SM->>SM: generateWarnings()
    SM-->>User: Try[List[String]] warnings
```

### 4. Native Library Lifecycle Sequence

```mermaid
sequenceDiagram
    participant App as Application
    participant QN as TantivyNative
    participant JNI as JNI Layer
    participant Rust as Rust Library
    participant QW as Tantivy Core

    App->>QN: First native call
    QN->>QN: loadNativeLibrary()
    
    alt Library not in system path
        QN->>QN: extractNativeLibrary()
        QN->>FS: extractFromResources()
        QN->>JNI: Native.load(tempFile)
    else Library in system path
        QN->>JNI: Native.load("tantivy_jni")
    end
    
    JNI->>Rust: dlopen(library)
    Rust-->>JNI: success
    JNI-->>QN: nativeLib instance

    App->>QN: createConfig(configJson)
    QN->>JNI: Java_createConfig(env, configJson)
    JNI->>Rust: create_tantivy_config(json_str)
    Rust->>QW: initialize_config(config)
    QW-->>Rust: config_handle
    Rust-->>JNI: config_id
    JNI-->>QN: configId
    QN-->>App: configId

    App->>QN: createSearchEngine(configId, indexPath)
    QN->>JNI: Java_createSearchEngine(env, configId, indexPath)
    JNI->>Rust: create_search_engine(config_id, path)
    Rust->>QW: create_searcher(config, index_path)
    QW-->>Rust: searcher_handle
    Rust-->>JNI: engine_id
    JNI-->>QN: engineId
    QN-->>App: engineId

    App->>QN: search(engineId, query, maxHits)
    QN->>JNI: Java_search(env, engineId, query, maxHits)
    JNI->>Rust: execute_search(engine_id, query_str, max_hits)
    Rust->>QW: search(query, limit)
    QW->>QW: parse_query(query_str)
    QW->>QW: execute_search(parsed_query)
    QW->>QW: collect_results(hits)
    QW-->>Rust: search_results
    Rust->>Rust: serialize_results_to_json(results)
    Rust-->>JNI: json_string
    JNI-->>QN: resultsJson
    QN-->>App: resultsJson

    App->>QN: close/cleanup
    QN->>JNI: Java_destroySearchEngine(env, engineId)
    JNI->>Rust: destroy_search_engine(engine_id)
    Rust->>QW: drop_searcher(handle)
    QN->>JNI: Java_destroyConfig(env, configId)
    JNI->>Rust: destroy_config(config_id)
    Rust->>QW: drop_config(handle)
```

### 5. Transaction Commit/Rollback Sequence

```mermaid
sequenceDiagram
    participant App as Application
    participant TxLog as TransactionLog
    participant QIW as TantivyIndexWriter
    participant QN as TantivyNative
    participant S3 as Cloud Storage

    App->>TxLog: new TransactionLog(basePath, options)
    TxLog->>TxLog: generateTransactionId()

    loop Write operations
        App->>QIW: writeRow(row)
        QIW-->>App: WriteResult
        App->>TxLog: appendEntry(writeResult)
        TxLog->>TxLog: addToEntries(TransactionEntry)
    end

    alt Successful completion
        App->>QIW: commit()
        QIW->>QN: commitIndex(writerId)
        QN->>S3: flushIndexSegments()
        QN->>S3: writeIndexMetadata()
        QN-->>QIW: success
        QIW-->>App: success

        App->>TxLog: commit()
        TxLog->>TxLog: createCommitEntry()
        TxLog->>TxLog: addToEntries(commitEntry)
        TxLog->>TxLog: writeLogToStorage()
        TxLog->>S3: writeTransactionLog(entries)
        S3-->>TxLog: success
        TxLog-->>App: success

    else Error occurred
        App->>TxLog: rollback()
        TxLog->>TxLog: createRollbackEntry()
        TxLog->>TxLog: addToEntries(rollbackEntry)
        TxLog->>TxLog: writeLogToStorage()
        TxLog->>S3: writeTransactionLog(entries)
        TxLog->>TxLog: cleanupFiles()
        
        loop For each WriteResult in entries
            TxLog->>S3: deleteFile(writeResult.filePath)
        end
        
        TxLog-->>App: rollback complete
    end
```

### 6. Predictive I/O Sequence

```mermaid
sequenceDiagram
    participant QFR as TantivyFileReader
    participant S3OR as S3OptimizedReader
    participant Cache as LRU Cache
    participant S3 as Cloud Storage
    participant Executor as ExecutorService

    QFR->>S3OR: readWithPredictiveIO(dataLocation, schema)
    S3OR->>S3OR: generateCacheKey(location)
    S3OR->>Cache: get(cacheKey)
    
    alt Cache hit
        Cache-->>S3OR: cachedData
        S3OR->>S3OR: parseDataToRows(cachedData, schema)
        S3OR-->>QFR: Iterator[InternalRow]
    else Cache miss
        S3OR->>S3: fetchDataWithReadAhead(location)
        S3->>S3: getObject(bucket, key, range)
        S3-->>S3OR: dataBytes
        
        S3OR->>Cache: put(cacheKey, dataBytes)
        S3OR->>S3OR: parseDataToRows(dataBytes, schema)
        
        par Predictive reads
            S3OR->>S3OR: calculateNextLocation(currentLocation)
            S3OR->>Executor: submit(predictiveReadTask)
            Executor->>S3: getObject(bucket, nextKey, nextRange)
            Executor->>Cache: put(nextCacheKey, nextData)
        end
        
        S3OR-->>QFR: Iterator[InternalRow]
    end
```

## Sequence Diagram Patterns

### 1. Error Handling Pattern
- All sequences include error propagation paths
- Cleanup operations are performed in reverse order
- Transaction rollback undoes partial operations
- Native resources are always cleaned up

### 2. Resource Management Pattern
- Native handles created in pairs (config + engine/writer)
- Lifecycle management through reference counting
- Automatic cleanup in finally blocks
- Memory management across JNI boundary

### 3. Asynchronous Operations Pattern
- Predictive I/O runs concurrently with main operations
- Background tasks don't block main workflow
- Future/Promise pattern for async results
- Timeout handling for long-running operations

### 4. Caching Strategy Pattern
- Multi-level cache checking (L1, L2, L3)
- Cache invalidation on updates
- LRU eviction policy
- Cache warming through predictive reads

### 5. Batch Processing Pattern
- Accumulate operations until threshold reached
- Batch commit for efficiency
- Streaming processing for large datasets
- Backpressure handling for flow control