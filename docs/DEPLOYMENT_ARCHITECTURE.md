# Deployment Architecture - Spark Tantivy Handler

## Deployment Overview

The Spark Tantivy Handler can be deployed in various configurations to meet different performance, scalability, and operational requirements. This document outlines the primary deployment patterns and architectural considerations.

## Deployment Topologies

### 1. Single-Node Development Deployment

```mermaid
graph TB
    subgraph "Development Machine"
        subgraph "Spark Local Mode"
            Driver[Spark Driver]
            LocalExecutor[Local Executor]
        end
        
        subgraph "Tantivy Handler"
            QFF[TantivyFileFormat]
            QSE[TantivySearchEngine]
            QIW[TantivyIndexWriter]
        end
        
        subgraph "Native Layer"
            QN[TantivyNative JNI]
            RustLib[Tantivy Rust Library]
        end
        
        subgraph "Local Storage"
            LocalFS[Local File System]
            TestData[Test Data]
        end
    end

    Driver --> LocalExecutor
    LocalExecutor --> QFF
    QFF --> QSE
    QFF --> QIW
    QSE --> QN
    QIW --> QN
    QN --> RustLib
    RustLib --> LocalFS
    LocalFS --> TestData
```

**Use Cases:**
- Development and testing
- Proof of concept implementations
- Small dataset experimentation
- Feature development and debugging

**Configuration:**
```properties
spark.master=local[*]
spark.serializer=org.apache.spark.serializer.KryoSerializer
tantivy.base.path=./local-tantivy-data
storage.uri=file://./data
```

### 2. Standalone Cluster Deployment

```mermaid
graph TB
    subgraph "Spark Standalone Cluster"
        subgraph "Master Node"
            SparkMaster[Spark Master]
            ClusterManager[Cluster Manager]
        end
        
        subgraph "Worker Node 1"
            Worker1[Spark Worker]
            Executor1[Executor JVM]
            QWHandler1[Tantivy Handler]
            NativeLib1[Native Library]
        end
        
        subgraph "Worker Node 2"
            Worker2[Spark Worker]
            Executor2[Executor JVM]
            QWHandler2[Tantivy Handler]
            NativeLib2[Native Library]
        end
        
        subgraph "Worker Node N"
            WorkerN[Spark Worker]
            ExecutorN[Executor JVM]
            QWHandlerN[Tantivy Handler]
            NativeLibN[Native Library]
        end
    end
    
    subgraph "Shared Storage"
        CloudStorage[Cloud Object Storage]
        MetadataStore[Metadata Store]
        ConfigStore[Configuration Store]
    end
    
    subgraph "Client Applications"
        SparkApp[Spark Application]
        Notebook[Jupyter Notebook]
        BI[BI Tools]
    end

    SparkMaster --> Worker1
    SparkMaster --> Worker2
    SparkMaster --> WorkerN
    
    Worker1 --> Executor1
    Worker2 --> Executor2
    WorkerN --> ExecutorN
    
    Executor1 --> QWHandler1
    Executor2 --> QWHandler2
    ExecutorN --> QWHandlerN
    
    QWHandler1 --> NativeLib1
    QWHandler2 --> NativeLib2
    QWHandlerN --> NativeLibN
    
    NativeLib1 --> CloudStorage
    NativeLib2 --> CloudStorage
    NativeLibN --> CloudStorage
    
    QWHandler1 --> MetadataStore
    QWHandler2 --> MetadataStore
    QWHandlerN --> MetadataStore
    
    SparkApp --> SparkMaster
    Notebook --> SparkMaster
    BI --> SparkMaster
```

**Use Cases:**
- Production workloads with dedicated infrastructure
- On-premises deployments
- Complete control over cluster configuration
- Custom security and networking requirements

### 3. Kubernetes Deployment

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Spark Operator"
            SparkOperator[Spark Operator]
            SparkApp[SparkApplication CRD]
        end
        
        subgraph "Driver Pod"
            DriverContainer[Spark Driver Container]
            ConfigMap[ConfigMap]
            Secrets[Secrets]
        end
        
        subgraph "Executor Pods"
            subgraph "Executor Pod 1"
                ExecutorContainer1[Executor Container]
                TantivyHandler1[Tantivy Handler]
                NativeLibs1[Native Libraries]
                TmpVolume1[Tmp Volume]
            end
            
            subgraph "Executor Pod 2"
                ExecutorContainer2[Executor Container]
                TantivyHandler2[Tantivy Handler]
                NativeLibs2[Native Libraries]
                TmpVolume2[Tmp Volume]
            end
        end
        
        subgraph "Storage"
            PVCs[Persistent Volume Claims]
            StorageClass[Storage Class]
        end
        
        subgraph "Services"
            DriverService[Driver Service]
            UIService[Spark UI Service]
        end
    end
    
    subgraph "External Dependencies"
        CloudStorage[Cloud Storage]
        Monitoring[Monitoring Stack]
        LogAggregation[Log Aggregation]
    end

    SparkOperator --> SparkApp
    SparkApp --> DriverContainer
    DriverContainer --> ConfigMap
    DriverContainer --> Secrets
    
    DriverContainer --> ExecutorContainer1
    DriverContainer --> ExecutorContainer2
    
    ExecutorContainer1 --> TantivyHandler1
    ExecutorContainer2 --> TantivyHandler2
    
    TantivyHandler1 --> NativeLibs1
    TantivyHandler2 --> NativeLibs2
    
    NativeLibs1 --> TmpVolume1
    NativeLibs2 --> TmpVolume2
    
    TmpVolume1 --> PVCs
    TmpVolume2 --> PVCs
    PVCs --> StorageClass
    
    DriverContainer --> DriverService
    DriverService --> UIService
    
    TantivyHandler1 --> CloudStorage
    TantivyHandler2 --> CloudStorage
    
    ExecutorContainer1 --> Monitoring
    ExecutorContainer2 --> Monitoring
    ExecutorContainer1 --> LogAggregation
    ExecutorContainer2 --> LogAggregation
```

**Kubernetes Manifest Example:**
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: tantivy-spark-job
spec:
  type: Scala
  mode: cluster
  image: "tantivy-spark:latest"
  imagePullPolicy: Always
  mainClass: com.example.TantivySparkApp
  mainApplicationFile: "local:///opt/spark/jars/tantivy-spark-handler.jar"
  sparkVersion: "3.4.1"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "2g"
    serviceAccount: spark-driver
  executor:
    cores: 2
    instances: 4
    memory: "4g"
    serviceAccount: spark-executor
  volumes:
    - name: native-libs
      emptyDir: {}
  volumeMounts:
    - name: native-libs
      mountPath: /tmp/native-libs
```

### 4. Cloud-Managed Deployments

#### AWS EMR Deployment

```mermaid
graph TB
    subgraph "AWS EMR Cluster"
        subgraph "Master Node"
            EMRMaster[EMR Master]
            SparkDriver[Spark Driver]
            ResourceManager[YARN Resource Manager]
        end
        
        subgraph "Core Nodes"
            CoreNode1[Core Node 1]
            CoreNode2[Core Node 2]
            CoreNodeN[Core Node N]
        end
        
        subgraph "Task Nodes"
            TaskNode1[Task Node 1]
            TaskNode2[Task Node 2]
            TaskNodeN[Task Node N]
        end
    end
    
    subgraph "AWS Services"
        S3[Amazon S3]
        IAMRoles[IAM Roles]
        CloudWatch[CloudWatch]
        VPC[VPC/Security Groups]
    end
    
    subgraph "Client Access"
        EMRNotebooks[EMR Notebooks]
        Zeppelin[Zeppelin]
        SparkSubmit[spark-submit]
    end

    EMRMaster --> CoreNode1
    EMRMaster --> CoreNode2
    EMRMaster --> CoreNodeN
    
    EMRMaster --> TaskNode1
    EMRMaster --> TaskNode2
    EMRMaster --> TaskNodeN
    
    CoreNode1 --> S3
    CoreNode2 --> S3
    CoreNodeN --> S3
    TaskNode1 --> S3
    TaskNode2 --> S3
    TaskNodeN --> S3
    
    EMRMaster --> IAMRoles
    CoreNode1 --> CloudWatch
    CoreNode2 --> CloudWatch
    
    EMRNotebooks --> EMRMaster
    Zeppelin --> EMRMaster
    SparkSubmit --> EMRMaster
```

#### Databricks Deployment

```mermaid
graph TB
    subgraph "Databricks Workspace"
        subgraph "Cluster Configuration"
            ClusterManager[Cluster Manager]
            AutoScaling[Auto Scaling]
            RuntimeConfig[Runtime Config]
        end
        
        subgraph "Compute Clusters"
            DriverInstance[Driver Instance]
            WorkerInstances[Worker Instances]
        end
        
        subgraph "Tantivy Integration"
            TantivyLibrary[Tantivy Library]
            NativeLibraries[Native Libraries]
            ConfigManagement[Config Management]
        end
    end
    
    subgraph "Databricks Services"
        DBFS[Databricks File System]
        Notebooks[Databricks Notebooks]
        Jobs[Databricks Jobs]
        MLflow[MLflow]
    end
    
    subgraph "External Storage"
        CloudStorage[Cloud Storage]
        ExternalTables[External Tables]
        DataLakes[Data Lakes]
    end

    ClusterManager --> DriverInstance
    ClusterManager --> WorkerInstances
    AutoScaling --> WorkerInstances
    RuntimeConfig --> TantivyLibrary
    
    TantivyLibrary --> NativeLibraries
    NativeLibraries --> ConfigManagement
    
    DriverInstance --> DBFS
    WorkerInstances --> DBFS
    
    Notebooks --> DriverInstance
    Jobs --> ClusterManager
    MLflow --> DriverInstance
    
    TantivyLibrary --> CloudStorage
    ConfigManagement --> ExternalTables
    DBFS --> DataLakes
```

## Container Images and Packaging

### 1. Base Container Image Structure

```dockerfile
# Multi-stage build for Tantivy Spark Handler
FROM rust:1.70 AS rust-builder

# Build native Tantivy library
WORKDIR /build
COPY src/main/rust/ .
RUN cargo build --release

FROM openjdk:11-jre-slim AS java-builder

# Install Scala and SBT
RUN apt-get update && apt-get install -y curl
RUN curl -L https://github.com/coursier/launchers/raw/master/scala-cli.sh | bash

# Build Scala application
WORKDIR /build
COPY pom.xml .
COPY src/ src/
RUN mvn clean package -DskipTests

FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

# Copy native libraries
COPY --from=rust-builder /build/target/release/libtantivy_jni.so /opt/spark/native/
COPY --from=rust-builder /build/target/release/libtantivy_jni.dylib /opt/spark/native/
COPY --from=rust-builder /build/target/release/tantivy_jni.dll /opt/spark/native/

# Copy JAR files
COPY --from=java-builder /build/target/tantivy-spark-handler-*.jar /opt/spark/jars/

# Set environment variables
ENV QUICKWIT_NATIVE_LIB_PATH=/opt/spark/native
ENV LD_LIBRARY_PATH=/opt/spark/native:$LD_LIBRARY_PATH

# Create non-root user
RUN groupadd -r tantivy && useradd -r -g tantivy tantivy
USER tantivy

ENTRYPOINT ["/opt/entrypoint.sh"]
```

### 2. Multi-Architecture Support

```mermaid
graph TB
    subgraph "Build Pipeline"
        SourceCode[Source Code]
        MultiArchBuilder[Multi-Architecture Builder]
    end
    
    subgraph "Architecture Builds"
        AMD64Build[AMD64 Build]
        ARM64Build[ARM64 Build]
        S390XBuild[S390X Build]
    end
    
    subgraph "Native Libraries"
        AMD64Native[AMD64 Native Libs]
        ARM64Native[ARM64 Native Libs]
        S390XNative[S390X Native Libs]
    end
    
    subgraph "Container Registry"
        ManifestList[Manifest List]
        AMD64Image[AMD64 Image]
        ARM64Image[ARM64 Image]
        S390XImage[S390X Image]
    end

    SourceCode --> MultiArchBuilder
    
    MultiArchBuilder --> AMD64Build
    MultiArchBuilder --> ARM64Build
    MultiArchBuilder --> S390XBuild
    
    AMD64Build --> AMD64Native
    ARM64Build --> ARM64Native
    S390XBuild --> S390XNative
    
    AMD64Native --> AMD64Image
    ARM64Native --> ARM64Image
    S390XNative --> S390XImage
    
    AMD64Image --> ManifestList
    ARM64Image --> ManifestList
    S390XImage --> ManifestList
```

## Security Architecture

### 1. Security Layers

```mermaid
graph TB
    subgraph "Network Security"
        TLS[TLS Encryption]
        VPN[VPN/Private Networks]
        Firewall[Firewall Rules]
        NetworkPolicies[Network Policies]
    end
    
    subgraph "Authentication & Authorization"
        RBAC[Role-Based Access Control]
        ServiceAccounts[Service Accounts]
        IAMRoles[IAM Roles]
        TokenManagement[Token Management]
    end
    
    subgraph "Data Security"
        EncryptionAtRest[Encryption at Rest]
        EncryptionInTransit[Encryption in Transit]
        KeyManagement[Key Management]
        DataMasking[Data Masking]
    end
    
    subgraph "Runtime Security"
        PodSecurityPolicies[Pod Security Policies]
        ResourceLimits[Resource Limits]
        SecurityContexts[Security Contexts]
        AdmissionControllers[Admission Controllers]
    end
    
    subgraph "Monitoring & Auditing"
        SecurityLogs[Security Logs]
        AuditTrails[Audit Trails]
        Compliance[Compliance Monitoring]
        ThreatDetection[Threat Detection]
    end

    TLS --> EncryptionInTransit
    RBAC --> ServiceAccounts
    EncryptionAtRest --> KeyManagement
    PodSecurityPolicies --> SecurityContexts
    SecurityLogs --> AuditTrails
```

### 2. Secrets Management

```mermaid
graph TB
    subgraph "Secret Sources"
        K8sSecrets[Kubernetes Secrets]
        ExternalSecrets[External Secret Store]
        ConfigMaps[ConfigMaps]
        EnvVars[Environment Variables]
    end
    
    subgraph "Secret Management Tools"
        VaultOperator[Vault Operator]
        CSIDriver[CSI Secret Driver]
        ExternalSecretsOperator[External Secrets Operator]
    end
    
    subgraph "Runtime Integration"
        VolumeMount[Volume Mount]
        EnvInjection[Environment Injection]
        InitContainer[Init Container]
    end
    
    subgraph "Applications"
        SparkDriver[Spark Driver]
        SparkExecutor[Spark Executor]
        TantivyHandler[Tantivy Handler]
    end

    ExternalSecrets --> VaultOperator
    K8sSecrets --> CSIDriver
    ConfigMaps --> ExternalSecretsOperator
    
    VaultOperator --> VolumeMount
    CSIDriver --> EnvInjection
    ExternalSecretsOperator --> InitContainer
    
    VolumeMount --> SparkDriver
    EnvInjection --> SparkExecutor
    InitContainer --> TantivyHandler
```

## Performance and Scaling Considerations

### 1. Horizontal Scaling Pattern

```mermaid
graph TB
    subgraph "Load Balancing"
        LoadBalancer[Load Balancer]
        HealthChecks[Health Checks]
        RoutingRules[Routing Rules]
    end
    
    subgraph "Auto Scaling"
        HPA[Horizontal Pod Autoscaler]
        VPA[Vertical Pod Autoscaler]
        ClusterAutoscaler[Cluster Autoscaler]
        CustomMetrics[Custom Metrics]
    end
    
    subgraph "Resource Management"
        ResourceQuotas[Resource Quotas]
        LimitRanges[Limit Ranges]
        PriorityClasses[Priority Classes]
        NodeAffinity[Node Affinity]
    end
    
    subgraph "Performance Optimization"
        CPUOptimization[CPU Optimization]
        MemoryTuning[Memory Tuning]
        NetworkOptimization[Network Optimization]
        StorageOptimization[Storage Optimization]
    end

    LoadBalancer --> HealthChecks
    HealthChecks --> RoutingRules
    
    HPA --> CustomMetrics
    VPA --> ResourceQuotas
    ClusterAutoscaler --> NodeAffinity
    
    ResourceQuotas --> CPUOptimization
    LimitRanges --> MemoryTuning
    PriorityClasses --> NetworkOptimization
    NodeAffinity --> StorageOptimization
```

### 2. Resource Allocation Strategy

```yaml
# Resource allocation configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: tantivy-resource-config
data:
  driver-resources.yaml: |
    spark.driver.cores: 2
    spark.driver.memory: 4g
    spark.driver.memoryFraction: 0.8
    spark.driver.maxResultSize: 2g
    
  executor-resources.yaml: |
    spark.executor.cores: 4
    spark.executor.memory: 8g
    spark.executor.memoryFraction: 0.8
    spark.executor.instances: auto
    
  tantivy-config.yaml: |
    tantivy.jni.memory.max: 2g
    tantivy.cache.size: 1g
    tantivy.batch.size: 1000
    tantivy.segment.size: 128MB
```

## Monitoring and Observability

### 1. Metrics Collection Architecture

```mermaid
graph TB
    subgraph "Metric Sources"
        SparkMetrics[Spark Metrics]
        JVMMetrics[JVM Metrics]
        TantivyMetrics[Tantivy Metrics]
        SystemMetrics[System Metrics]
    end
    
    subgraph "Collection Layer"
        PrometheusAgent[Prometheus Agent]
        MetricsEndpoint[Metrics Endpoint]
        CustomCollectors[Custom Collectors]
    end
    
    subgraph "Storage & Processing"
        PrometheusServer[Prometheus Server]
        InfluxDB[InfluxDB]
        ElasticSearch[ElasticSearch]
    end
    
    subgraph "Visualization"
        Grafana[Grafana Dashboards]
        Kibana[Kibana Dashboards]
        CustomUI[Custom UI]
    end
    
    subgraph "Alerting"
        AlertManager[Alert Manager]
        PagerDuty[PagerDuty]
        Slack[Slack Notifications]
    end

    SparkMetrics --> PrometheusAgent
    JVMMetrics --> MetricsEndpoint
    TantivyMetrics --> CustomCollectors
    SystemMetrics --> PrometheusAgent
    
    PrometheusAgent --> PrometheusServer
    MetricsEndpoint --> InfluxDB
    CustomCollectors --> ElasticSearch
    
    PrometheusServer --> Grafana
    InfluxDB --> Grafana
    ElasticSearch --> Kibana
    
    PrometheusServer --> AlertManager
    AlertManager --> PagerDuty
    AlertManager --> Slack
```

### 2. Logging Architecture

```mermaid
graph TB
    subgraph "Log Sources"
        ApplicationLogs[Application Logs]
        SparkLogs[Spark Logs]
        TantivyLogs[Tantivy Native Logs]
        SystemLogs[System Logs]
        AuditLogs[Audit Logs]
    end
    
    subgraph "Log Processing"
        Fluentd[Fluentd/Fluent Bit]
        Logstash[Logstash]
        VectorLogs[Vector]
    end
    
    subgraph "Log Storage"
        ElasticSearch[ElasticSearch]
        S3Logs[S3 Log Storage]
        Loki[Grafana Loki]
    end
    
    subgraph "Log Analysis"
        Kibana[Kibana]
        Grafana[Grafana]
        Splunk[Splunk]
    end

    ApplicationLogs --> Fluentd
    SparkLogs --> Fluentd
    TantivyLogs --> Logstash
    SystemLogs --> VectorLogs
    AuditLogs --> VectorLogs
    
    Fluentd --> ElasticSearch
    Logstash --> S3Logs
    VectorLogs --> Loki
    
    ElasticSearch --> Kibana
    S3Logs --> Splunk
    Loki --> Grafana
```

## Disaster Recovery and Backup

```mermaid
graph TB
    subgraph "Backup Sources"
        IndexData[Index Data]
        Metadata[Metadata]
        Configuration[Configuration]
        TransactionLogs[Transaction Logs]
    end
    
    subgraph "Backup Strategy"
        IncrementalBackup[Incremental Backup]
        FullBackup[Full Backup]
        ContinuousReplication[Continuous Replication]
        Snapshotting[Snapshotting]
    end
    
    subgraph "Storage Tiers"
        HotStorage[Hot Storage]
        WarmStorage[Warm Storage]
        ColdStorage[Cold Storage]
        ArchiveStorage[Archive Storage]
    end
    
    subgraph "Recovery Procedures"
        PointInTimeRecovery[Point-in-Time Recovery]
        DisasterRecovery[Disaster Recovery]
        DataValidation[Data Validation]
        ServiceRestoration[Service Restoration]
    end

    IndexData --> IncrementalBackup
    Metadata --> FullBackup
    Configuration --> ContinuousReplication
    TransactionLogs --> Snapshotting
    
    IncrementalBackup --> HotStorage
    FullBackup --> WarmStorage
    ContinuousReplication --> ColdStorage
    Snapshotting --> ArchiveStorage
    
    HotStorage --> PointInTimeRecovery
    WarmStorage --> DisasterRecovery
    ColdStorage --> DataValidation
    ArchiveStorage --> ServiceRestoration
```

This deployment architecture provides comprehensive guidance for deploying the Spark Tantivy Handler across various environments while maintaining security, performance, and operational excellence.