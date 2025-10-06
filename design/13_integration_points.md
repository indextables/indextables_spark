# Section 13: Integration Points (Outline)

## 13.1 Overview

IndexTables4Spark integrates with multiple components of the Apache Spark and Hadoop ecosystems, as well as external libraries and cloud services. This section outlines all major integration points and their implementation details.

## 13.2 Apache Spark Integration

### 13.2.1 DataSource V2 API
- **TableProvider interface**: Entry point for table discovery
- **Table interface**: Capabilities and metadata exposure
- **ScanBuilder interface**: Scan planning and filter pushdown
- **WriteBuilder interface**: Write planning and mode selection
- **PartitionReader interface**: Split-level data reading
- **DataWriter interface**: Partition-level data writing

### 13.2.2 Catalyst Optimizer Integration
- **Resolution rules**: V2IndexQueryExpressionRule for IndexQuery detection
- **Physical planning**: Scan type selection based on aggregations
- **Statistics integration**: File-level and partition-level statistics
- **Filter pushdown**: SupportsPushDownFilters and SupportsPushDownV2Filters
- **Aggregate pushdown**: SupportsPushDownAggregates interface

### 13.2.3 Spark SQL Extensions
- **SparkSessionExtensions API**: Custom parser and rule registration
- **Custom parser injection**: IndexTables4SparkSqlParser
- **Function registration**: tantivy4spark_indexquery and tantivy4spark_indexqueryall
- **Resolution rule injection**: V2IndexQueryExpressionRule
- **Planner strategy injection**: (Future expansion point)

### 13.2.4 RDD and DataFrame API
- **RDD creation**: Through PartitionReader implementations
- **DataFrame operations**: Full compatibility with Spark SQL operations
- **Partition discovery**: Hive-style partition detection
- **Schema inference**: Automatic schema discovery from transaction log

## 13.3 Hadoop Ecosystem Integration

### 13.3.1 Hadoop Configuration
- **Configuration hierarchy**: Integration with core-site.xml, hdfs-site.xml
- **Credential propagation**: Hadoop configs propagated to executors
- **FileSystem API**: Support for HDFS, S3A, and local filesystems
- **Path handling**: Hadoop Path objects for URI manipulation

### 13.3.2 S3A Filesystem
- **S3A URI support**: s3a:// scheme handling
- **AWS credential chain**: Integration with DefaultAWSCredentialsProviderChain
- **Session token support**: Temporary credentials via STS
- **Configuration compatibility**: Respects spark.hadoop.fs.s3a.* settings

### 13.3.3 HDFS Integration
- **HDFS client**: Native Hadoop FileSystem API
- **Distributed storage**: Full support for HDFS clusters
- **Replication**: Respects HDFS replication settings
- **Locality**: HDFS block locality for task scheduling

## 13.4 tantivy4java Integration

### 13.4.1 Core tantivy4java APIs
- **Index**: Split opening and query execution
- **IndexReader**: Document retrieval and metadata access
- **IndexWriter**: Document indexing and split creation
- **BatchDocumentBuilder**: Batch document addition
- **Schema**: Field definitions and type mappings

### 13.4.2 Split Operations
- **QuickwitSplit**: Split file format and operations
- **SplitQuery**: Query representation for filter pushdown
- **SplitMerge**: Split consolidation operations
- **SplitCacheManager**: Native cache management (tantivy4java-provided)

### 13.4.3 Query Execution
- **parseQuery**: Tantivy query string parsing
- **searchSplit**: Query execution on single split
- **aggregations**: COUNT, SUM, AVG, MIN, MAX support
- **fast fields**: Columnar field access for aggregations

### 13.4.4 JNI and Native Integration
- **JNI bindings**: Java-Rust interop layer
- **Memory management**: Native memory allocation and cleanup
- **Exception handling**: Rust panic translation to Java exceptions
- **Resource cleanup**: Explicit close() calls for native resources

## 13.5 AWS Integration

### 13.5.1 S3 Client
- **AWS SDK**: AmazonS3 client for object storage
- **Multipart uploads**: S3 multipart upload API
- **Streaming downloads**: S3 GetObject with streaming
- **Retry logic**: SDK retry policy integration

### 13.5.2 Credential Providers
- **DefaultAWSCredentialsProviderChain**: Standard provider chain
- **BasicAWSCredentials**: Explicit access key/secret key
- **BasicSessionCredentials**: Temporary credentials with session token
- **Custom providers** (v1.9+): Reflection-based custom credential provider support

### 13.5.3 S3 Operations
- **PutObject**: Single-part uploads for small files
- **CreateMultipartUpload**: Initiate large file uploads
- **UploadPart**: Individual part uploads
- **CompleteMultipartUpload**: Finalize multipart uploads
- **AbortMultipartUpload**: Cleanup failed uploads

### 13.5.4 Custom Credential Provider Integration (v1.9+)
- **Reflection-based loading**: Dynamic class loading without compile-time dependencies
- **AWS SDK v1 support**: AWSCredentialsProvider interface
- **AWS SDK v2 support**: AwsCredentialsProvider interface
- **URI context**: Table-level URIs passed to providers for caching
- **Configuration hierarchy**: Full support for DataFrame options, Spark config, Hadoop config

## 13.6 Delta Lake Compatibility

### 13.6.1 Transaction Log Format
- **JSON structure**: Delta Lake-compatible JSON format
- **Action types**: Similar ProtocolAction, MetadataAction, AddAction, RemoveAction
- **Checkpoint format**: Parquet checkpoints (similar to Delta)
- **Version numbering**: Monotonic transaction versions

### 13.6.2 ACID Semantics
- **Atomicity**: All-or-nothing transaction commits
- **Consistency**: Schema validation and metadata enforcement
- **Isolation**: Optimistic concurrency control
- **Durability**: S3 persistence with eventual consistency handling

### 13.6.3 Time Travel (Future)
- **Version-based reads**: Read table at specific transaction version
- **Timestamp-based reads**: Read table at specific timestamp
- **Version history**: List all transaction versions
- **Schema evolution**: Track schema changes over time

## 13.7 Cloud Platform Integration

### 13.7.1 Databricks
- **Unity Catalog**: Compatible with Delta Lake tables
- **/local_disk0 detection**: Automatic high-performance storage usage
- **Cluster libraries**: JAR deployment and dependency management
- **Notebook integration**: Interactive query and DataFrame API usage

### 13.7.2 AWS EMR
- **S3 as primary storage**: Native S3 integration
- **Instance store**: /local_disk0 detection for ephemeral storage
- **EMRFS configuration**: Compatibility with EMR-specific S3 settings
- **Bootstrap actions**: Cluster initialization for tantivy4java

### 13.7.3 Google Cloud Dataproc
- **GCS support** (via Hadoop connector): Google Cloud Storage integration
- **Persistent disks**: Custom temp directory configuration
- **Cluster initialization**: Startup scripts for dependencies

### 13.7.4 Azure HDInsight / Synapse
- **WASB/ABFS support**: Azure Blob Storage integration
- **Managed identities**: Azure credential integration
- **Synapse Spark pools**: Compatibility with serverless Spark

## 13.8 Build and Dependency Integration

### 13.8.1 Maven Build System
- **pom.xml configuration**: Maven Central dependencies
- **Scala 2.12 compilation**: Cross-compilation support
- **ANTLR code generation**: Parser grammar compilation
- **Assembly plugin**: Fat JAR creation with dependencies

### 13.8.2 Dependency Management
- **Spark dependencies**: Provided scope for cluster deployment
- **tantivy4java**: Platform-specific native libraries
- **AWS SDK**: Optional dependency based on storage
- **Guava**: Caching and utilities

### 13.8.3 Native Library Packaging
- **Platform-specific JARs**: macOS, Linux x86_64, Linux ARM64
- **JNI library loading**: Automatic extraction and loading
- **Fallback mechanisms**: Graceful degradation if native library unavailable
- **Version compatibility**: Ensure tantivy4java version matches

## 13.9 Monitoring and Observability Integration

### 13.9.1 Spark UI Integration
- **Stage metrics**: Visible in Spark UI stages
- **Task metrics**: Custom metrics for split processing
- **SQL tab**: Query plans show IndexQuery pushdown
- **Storage tab**: Cached RDD visibility

### 13.9.2 Logging Integration
- **SLF4J**: Standard Spark logging framework
- **Log4j configuration**: Configurable log levels
- **Structured logging**: Consistent log format
- **Error context**: File paths, sizes, configurations in logs

### 13.9.3 Metrics Integration
- **Spark metrics system**: Custom metrics registration
- **Dropwizard metrics**: Counter, gauge, histogram support
- **JMX exposure**: Metrics visible via JMX
- **Prometheus export**: (Future) Prometheus integration

## 13.10 Testing Integration

### 13.10.1 ScalaTest Framework
- **FunSuite style**: Standard ScalaTest test suites
- **BeforeAndAfter**: Test fixture setup and teardown
- **Matchers**: Rich assertion library
- **Tag-based filtering**: Run specific test categories

### 13.10.2 Spark Test Utilities
- **SharedSparkSession**: Shared Spark context across tests
- **TempDir management**: Temporary directory creation and cleanup
- **LocalSparkSession**: Local mode Spark for unit tests
- **QueryTest**: SQL query comparison utilities

### 13.10.3 Mock and Stub Integration
- **Mockito**: Java mocking framework
- **Embedded S3**: MinIO or LocalStack for S3 testing
- **Embedded Hadoop**: MiniDFSCluster for HDFS testing
- **Test data generation**: ScalaCheck for property-based testing

## 13.11 Catalog Integration

### 13.11.1 Hive Metastore
- **Table registration**: CREATE TABLE ... USING indextables
- **Metadata storage**: Table location, schema, properties
- **Partition discovery**: Hive-style partition detection
- **Statistics**: Table and column statistics

### 13.11.2 Unity Catalog (Databricks)
- **Managed tables**: Unity Catalog-managed IndexTables4Spark tables
- **External tables**: Pointer to existing S3/ADLS data
- **Access control**: Unity Catalog permissions
- **Data lineage**: Automatic lineage tracking

### 13.11.3 AWS Glue Data Catalog
- **Table definition**: Glue table with SERDE properties
- **Partition management**: Automatic partition detection
- **Crawler support**: Schema discovery via Glue crawlers
- **Integration with Athena**: (Limited) Read-only via Athena

## 13.12 Security Integration

### 13.12.1 Authentication
- **AWS IAM**: Role-based S3 access
- **Kerberos**: HDFS authentication
- **Custom credential providers**: Enterprise identity integration
- **Session tokens**: Temporary credential support

### 13.12.2 Authorization
- **File-level permissions**: Respect storage-level ACLs
- **Catalog permissions**: Hive/Unity Catalog access control
- **Column-level security**: (Future) Fine-grained access control
- **Row-level security**: (Future) Predicate-based filtering

### 13.12.3 Encryption
- **Encryption at rest**: S3 SSE-S3, SSE-KMS support
- **Encryption in transit**: HTTPS for S3 access
- **Client-side encryption**: (Future) Application-level encryption
- **Key management**: AWS KMS integration

## 13.13 Third-Party Tool Integration

### 13.13.1 Jupyter Notebooks
- **PySpark integration**: DataFrame API via PySpark
- **SQL magic commands**: %%sql cells for IndexQuery
- **Visualization**: DataFrame display and plotting
- **Interactive exploration**: Ad-hoc query development

### 13.13.2 Apache Zeppelin
- **Spark interpreter**: Full IndexTables4Spark support
- **SQL interpreter**: Native IndexQuery syntax
- **Dynamic forms**: Parameterized queries
- **Visualization**: Built-in charting

### 13.13.3 BI Tools
- **Tableau**: JDBC connection via Spark Thrift Server
- **Power BI**: Spark connector support
- **Looker**: SQL-based exploration
- **Limitations**: IndexQuery syntax requires views or pre-filtering

## 13.14 Data Pipeline Integration

### 13.14.1 Apache Airflow
- **SparkSubmitOperator**: Spark job submission
- **PythonOperator**: PySpark scripts
- **Sensor support**: Table availability sensors
- **Retry logic**: Task retry on failure

### 13.14.2 Apache NiFi
- **PutSparkDataFrame**: Write DataFrames to IndexTables4Spark
- **ExecuteSparkInteractive**: Read and process data
- **Flow file integration**: DataFrame conversion
- **Error handling**: Failure routing

### 13.14.3 dbt (data build tool)
- **Spark adapter**: Compatible with dbt-spark
- **Incremental models**: Append-mode writes
- **Snapshots**: Version-based reads (Future)
- **Testing**: Data quality testing

## 13.15 Summary

IndexTables4Spark integrates seamlessly with:

✅ **Apache Spark**: DataSource V2, Catalyst, SQL extensions, RDD/DataFrame APIs
✅ **Hadoop ecosystem**: Hadoop configuration, S3A, HDFS
✅ **tantivy4java**: Core APIs, JNI bindings, native integration
✅ **AWS**: S3 client, credential providers, custom provider support (v1.9+)
✅ **Delta Lake**: Compatible transaction log format and ACID semantics
✅ **Cloud platforms**: Databricks, EMR, Dataproc, HDInsight
✅ **Build tools**: Maven, dependency management, native library packaging
✅ **Monitoring**: Spark UI, logging, metrics
✅ **Testing**: ScalaTest, Spark test utilities, mocking frameworks
✅ **Catalogs**: Hive Metastore, Unity Catalog, AWS Glue
✅ **Security**: IAM, Kerberos, encryption
✅ **Third-party tools**: Jupyter, Zeppelin, BI tools
✅ **Data pipelines**: Airflow, NiFi, dbt

**Key Integration Strengths**:
- **Native Spark integration** via DataSource V2 API
- **Minimal external dependencies** for easy deployment
- **Cloud-native design** optimized for S3 and cloud platforms
- **Enterprise-ready security** with custom credential provider support
- **Standard tooling support** for development and operations
- **Extensibility** through well-defined interfaces
