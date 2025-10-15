# Scala 2.13 Test Execution Plan

## Overview
After successfully migrating from Scala 2.12 to 2.13, this plan systematically validates all 176 test suites to identify and fix any regressions before considering the migration complete.

## Execution Strategy
- **One test at a time**: Execute each test suite individually to isolate failures
- **Fix before proceeding**: Address any failures completely before moving to the next test
- **Track progress**: Use checkboxes to maintain clear progress visibility
- **Command template**: `mvn test-compile scalatest:test -DwildcardSuites='<TestClassName>'`

## Test Execution Progress

### Phase 1: Core Functionality Tests (Critical Path)
Tests related to basic read/write operations and core DataSource functionality.

- [ ] **IndexTables4SparkIntegrationTest** - Core integration test
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexTables4SparkIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2SimpleTest** - V2 DataSource API basic functionality
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2SimpleTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2TableProviderTest** - V2 table provider tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2TableProviderTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexTablesValidationTest** - Core validation logic
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexTablesValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2ReadPathTest** - V2 read path validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2ReadPathTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 2: Schema and Type Tests
Tests for schema handling and data type conversions.

- [ ] **SchemaConverterTest** - Schema conversion logic
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.search.SchemaConverterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SchemaMappingSimpleTest** - Schema mapping tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.schema.SchemaMappingSimpleTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DateTimeTypeConversionTest** - Date/time type handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.schema.DateTimeTypeConversionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **UnsupportedTypesTest** - Unsupported type validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.UnsupportedTypesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2SchemaEvolutionTest** - Schema evolution tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2SchemaEvolutionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 3: Transaction Log Tests
Critical tests for transaction log functionality (checkpoint, compression, etc.).

- [ ] **TransactionLogTest** - Basic transaction log operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogEnhancedTest** - Enhanced transaction log features
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogEnhancedTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogPerformanceTest** - Performance validation (60% improvement expected)
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogPerformanceTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogCacheTest** - Transaction log caching
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogCacheTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogDeletionTest** - Transaction log cleanup
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogDeletionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CompressionUtilsTest** - Transaction log compression utilities
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.compression.CompressionUtilsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CompressedTransactionLogIntegrationTest** - Compression integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.compression.CompressedTransactionLogIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **OptimizedTransactionLogTest** - Optimized transaction log
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.OptimizedTransactionLogTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **BatchTransactionLogTest** - Batch transaction operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.BatchTransactionLogTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CheckpointStatisticsTruncationTest** - Checkpoint statistics truncation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.CheckpointStatisticsTruncationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 4: IndexQuery Tests
Tests for IndexQuery and IndexQueryAll operators.

- [ ] **IndexQueryExpressionTest** - IndexQuery expression parsing
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.expressions.IndexQueryExpressionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryAllExpressionTest** - IndexQueryAll expression parsing
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.expressions.IndexQueryAllExpressionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryIntegrationTest** - IndexQuery integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.IndexQueryIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryAllIntegrationTest** - IndexQueryAll integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.IndexQueryAllIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryCacheTest** - IndexQuery cache validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexQueryCacheTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryFilterTest** - IndexQuery filter tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.filters.IndexQueryFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2IndexQueryPushdownTest** - V2 IndexQuery pushdown
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.V2IndexQueryPushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 5: Aggregate Pushdown Tests
Tests for COUNT/SUM/AVG/MIN/MAX aggregate pushdown.

- [ ] **BasicAggregatePushdownTest** - Basic aggregate pushdown
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.BasicAggregatePushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SimpleAggregatePushdownTest** - Simple aggregate tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.SimpleAggregatePushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AggregatePushdownIntegrationTest** - Aggregate integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AggregatePushdownIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AggregatePushdownValidationTest** - Aggregate validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AggregatePushdownValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AggregatePushdownCompatibilityTest** - Aggregate compatibility
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AggregatePushdownCompatibilityTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **GroupByAggregatePushdownTest** - GroupBy aggregate pushdown
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.GroupByAggregatePushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **GroupByIntegrationTest** - GroupBy integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.GroupByIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AvgAggregationTest** - AVG aggregation tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AvgAggregationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FastFieldValidationTest** - Fast field validation for aggregates
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.FastFieldValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SupportsPushDownAggregatesTest** - Aggregate pushdown interface
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.SupportsPushDownAggregatesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AggregationCacheLocalityValidationTest** - Aggregate cache locality
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AggregationCacheLocalityValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 6: Merge Splits Tests
Tests for split optimization and merge operations.

- [ ] **MergeSplitsCommandTest** - Basic merge splits command
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsCommandTest'`
  - Status: Not started
  - Failures: None
  - Notes: **Critical - uses sequential processing after parallel collections removal**

- [ ] **MergeSplitsBatchTest** - Batch merge operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsBatchTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MergeSplitsValidationTest** - Merge validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MergeSplitsPartitionTest** - Partition-aware merges
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsPartitionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MergeSplitsSkippedFilesTest** - Skipped files handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsSkippedFilesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MergeSplitsNumRecordsTest** - Record count validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsNumRecordsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MergeSplitsTempDirectoryTest** - Temporary directory handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsTempDirectoryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DocumentPreservationValidationTest** - Document preservation during merge
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.DocumentPreservationValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 7: Partitioned Dataset Tests
Tests for partition pruning and partitioned table operations.

- [ ] **PartitionedTableTest** - Partitioned table basic operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.PartitionedTableTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PartitionedDatasetTest** - Partitioned dataset tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.PartitionedDatasetTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PartitionPruningDateFilterTest** - Partition pruning with date filters
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.PartitionPruningDateFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PartitionVsDataDateTest** - Partition vs data date handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.PartitionVsDataDateTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PartitionGroupByCountOptimizationTest** - Partition GroupBy optimization
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.PartitionGroupByCountOptimizationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 8: S3 and Storage Tests
Tests for S3 operations, multipart uploads, and storage providers.

- [ ] **CloudStorageProviderTest** - Cloud storage provider tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.CloudStorageProviderTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3MultipartUploaderTest** - S3 multipart upload tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.S3MultipartUploaderTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3CloudStorageProviderMultipartTest** - S3 provider multipart tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.S3CloudStorageProviderMultipartTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MultipartUploadVerificationTest** - Multipart upload verification
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.MultipartUploadVerificationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **LargeFileUploadTest** - Large file upload tests (4GB+)
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.LargeFileUploadTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3SplitReadWriteTest** - S3 split read/write
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.S3SplitReadWriteTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3DirectTest** - S3 direct operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.S3DirectTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 9: Configuration and Options Tests
Tests for configuration parsing and validation.

- [ ] **IndexWriterConfigTest** - Index writer configuration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.config.IndexWriterConfigTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SizeParsingTest** - Size parsing tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.config.SizeParsingTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SizeParserTest** - Size parser utility tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.util.SizeParserTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SizeParserUnitTest** - Size parser unit tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.util.SizeParserUnitTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AutoSizeOptionsUnitTest** - Auto-size options tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AutoSizeOptionsUnitTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexTables4SparkOptionsAutoSizeTest** - Auto-size integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexTables4SparkOptionsAutoSizeTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AutoSizingIntegrationTest** - Auto-sizing integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.autosize.AutoSizingIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 10: Credential and Authentication Tests
Tests for custom credential providers and authentication.

- [ ] **CredentialProviderFactoryTest** - Credential provider factory
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.utils.CredentialProviderFactoryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CredentialExtractionTest** - Credential extraction logic
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.CredentialExtractionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CredentialPropagationTest** - Credential propagation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.CredentialPropagationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CustomCredentialProviderIntegrationTest** - Custom provider integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.CustomCredentialProviderIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2S3CredentialTest** - V2 S3 credential tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2S3CredentialTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 11: Filter and Pushdown Tests
Tests for filter pushdown and data skipping.

- [ ] **SqlPushdownTest** - SQL filter pushdown
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.SqlPushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2PushdownValidationTest** - V2 pushdown validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2PushdownValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DateStringFilterValidationTest** - Date string filter validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.DateStringFilterValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2LocalDateFilterTest** - V2 local date filter tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2LocalDateFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DataSkippingVerificationTest** - Data skipping verification
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.DataSkippingVerificationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **LimitPushdownTest** - Limit pushdown tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.LimitPushdownTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ZeroRecordsFilterTest** - Zero records filter handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.ZeroRecordsFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 12: Statistics and Metadata Tests
Tests for statistics collection and metadata handling.

- [ ] **StatisticsIntegrationTest** - Statistics integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.StatisticsIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **StatisticsTruncationSuite** - Statistics truncation (98% reduction)
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.StatisticsTruncationSuite'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogStatisticsTest** - Transaction log statistics
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.TransactionLogStatisticsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **MetadataColumnTest** - Metadata column tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.MetadataColumnTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 13: V2 Advanced Tests
Advanced V2 DataSource API tests.

- [ ] **V2AdvancedScanTest** - V2 advanced scan operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2AdvancedScanTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2AdvancedWriteTest** - V2 advanced write operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2AdvancedWriteTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2OptimizeWriteTest** - V2 optimized write tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2OptimizeWriteTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2WriteDebugTest** - V2 write debug tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2WriteDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2ConfigurationEdgeCaseTest** - V2 configuration edge cases
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2ConfigurationEdgeCaseTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2ErrorScenarioTest** - V2 error scenario handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2ErrorScenarioTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2MultiPathTest** - V2 multi-path operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2MultiPathTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 14: Indexing Configuration Tests
Tests for field indexing configuration and tokenization.

- [ ] **IndexingConfigurationTest** - Indexing configuration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexing.IndexingConfigurationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FieldTypeTest** - Field type handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexing.FieldTypeTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TokenizationTest** - Tokenization tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexing.TokenizationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SimpleIndexingTest** - Simple indexing tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexing.SimpleIndexingTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 15: Optimization Tests
Tests for optimized writes and performance features.

- [ ] **OptimizedWriteTest** - Optimized write tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.optimize.OptimizedWriteTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **BulkLoadValidationTest** - Bulk load validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.performance.BulkLoadValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FooterOffsetOptimizationTest** - Footer offset optimization
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.FooterOffsetOptimizationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 16: Cache and Locality Tests
Tests for split caching and locality management.

- [ ] **BroadcastLocalityTest** - Broadcast locality management
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.locality.BroadcastLocalityTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SplitLocationTrackingTest** - Split location tracking
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.locality.SplitLocationTrackingTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PrewarmCacheTest** - Prewarm cache tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.PrewarmCacheTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PreWarmManagerTest** - Prewarm manager tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.prewarm.PreWarmManagerTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PreWarmManagerSimpleTest** - Prewarm manager simple tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.prewarm.PreWarmManagerSimpleTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PreWarmIntegrationTest** - Prewarm integration tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.prewarm.PreWarmIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 17: Search Engine Tests
Tests for split search engine functionality.

- [ ] **SplitSearchEngineValidationTest** - Search engine validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.search.SplitSearchEngineValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes: **Critical - contains docBatch conversion changes**

### Phase 18: SQL Extensions Tests
Tests for SQL extensions and custom commands.

- [ ] **IndexTablesSparkExtensionsTest** - SQL extensions tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.extensions.IndexTablesSparkExtensionsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryParserTest** - IndexQuery SQL parser
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.IndexQueryParserTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FlushIndexTablesCacheCommandTest** - Flush cache command
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.FlushIndexTablesCacheCommandTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **InvalidateTransactionLogCacheCommandTest** - Invalidate cache command
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.InvalidateTransactionLogCacheCommandTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **RepairIndexFilesTransactionLogReplacementSuite** - Repair command
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.RepairIndexFilesTransactionLogReplacementSuite'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 19: Utility Tests
Tests for utility classes and helper functions.

- [ ] **ExpressionUtilsTest** - Expression utilities
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.util.ExpressionUtilsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ExpressionUtilsIndexQueryAllTest** - IndexQueryAll expression utilities
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.util.ExpressionUtilsIndexQueryAllTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SplitSizeAnalyzerTest** - Split size analyzer
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.util.SplitSizeAnalyzerTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **PathParameterTest** - Path parameter handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.PathParameterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 20: AQE (Adaptive Query Execution) Tests
Tests for Adaptive Query Execution integration.

- [ ] **AQEFooterMetadataValidationTest** - AQE footer metadata
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AQEFooterMetadataValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AQEInputPartitionValidationTest** - AQE input partition validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AQEInputPartitionValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AQEMetadataSerializationTest** - AQE metadata serialization
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AQEMetadataSerializationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 21: Debug and Diagnostic Tests
Comprehensive debug and diagnostic test suite.

- [ ] **MinimalSplitTest** - Minimal split tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.MinimalSplitTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **NativeLibraryTest** - Native library (tantivy4java) tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.NativeLibraryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DirectInterfaceTest** - Direct interface tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.DirectInterfaceTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SimpleTermQueryTest** - Simple term query tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.SimpleTermQueryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SimpleIndexQueryTest** - Simple IndexQuery tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.SimpleIndexQueryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DirectDataFrameIndexQueryTest** - Direct DataFrame IndexQuery
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.DirectDataFrameIndexQueryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryPushdownDebugTest** - IndexQuery pushdown debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.IndexQueryPushdownDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryParsingDebugTest** - IndexQuery parsing debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.IndexQueryParsingDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ComplexIndexQueryDebugTest** - Complex IndexQuery debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.ComplexIndexQueryDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ComplexCompoundQueriesTest** - Complex compound queries
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.ComplexCompoundQueriesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ComprehensiveFiltersTest** - Comprehensive filter tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.ComprehensiveFiltersTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **BooleanDebugLoggingTest** - Boolean debug logging
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.BooleanDebugLoggingTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DataSourceApiComparisonTest** - DataSource API comparison
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.DataSourceApiComparisonTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DateAndTimestampQueriesTest** - Date and timestamp query tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.DateAndTimestampQueriesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **DocumentExtractionTest** - Document extraction tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.DocumentExtractionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **EqualityAndInQueriesTest** - Equality and IN query tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.EqualityAndInQueriesTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FieldExtractionDebugTest** - Field extraction debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.FieldExtractionDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **NumericFieldFilteringTest** - Numeric field filtering
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.NumericFieldFilteringTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **OverwriteDebugTest** - Overwrite operation debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.OverwriteDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ParseQueryApiTest** - Parse query API tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.ParseQueryApiTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **StandaloneParseQueryTest** - Standalone parse query
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.StandaloneParseQueryTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SqlVsDataFrameLikeTest** - SQL vs DataFrame comparison
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.SqlVsDataFrameLikeTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TableExistsTest** - Table exists check tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.TableExistsTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TimestampRoundTripTest** - Timestamp round trip tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.TimestampRoundTripTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **CredentialDebugTest** - Credential debug tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.CredentialDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3FlowDebugTest** - S3 flow debug tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.S3FlowDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 22: IndexQuery Additional Tests
Additional IndexQuery tests from various packages.

- [ ] **NotFilterTest** - NOT filter tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.NotFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ComplexNotFilterTest** - Complex NOT filter tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.ComplexNotFilterTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryDiagnosticTest** - IndexQuery diagnostic tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.IndexQueryDiagnosticTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SimpleIndexQueryDebugTest** - Simple IndexQuery debug
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.SimpleIndexQueryDebugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SQLIndexQueryValidationTest** - SQL IndexQuery validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.SQLIndexQueryValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2IndexQueryBugTest** - V2 IndexQuery bug tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.indexquery.V2IndexQueryBugTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **V2IndexQueryAllValidationTest** - V2 IndexQueryAll validation
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.debug.V2IndexQueryAllValidationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryAllSimpleTest** - IndexQueryAll simple tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.IndexQueryAllSimpleTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryAggregateTest** - IndexQuery with aggregates
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexQueryAggregateTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **IndexQueryBehaviorTest** - IndexQuery behavior tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.IndexQueryBehaviorTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 23: Integration Tests
Full end-to-end integration tests.

- [ ] **IndexTables4SparkFullIntegrationTest** - Full integration test
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.IndexTables4SparkFullIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **S3EndToEndIntegrationTest** - S3 end-to-end test
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.integration.S3EndToEndIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **SkippedFilesIntegrationTest** - Skipped files integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.SkippedFilesIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes:

### Phase 24: Real S3 Integration Tests
Tests requiring real AWS S3 access (may be skipped in CI).

- [ ] **RealS3IntegrationTest** - Basic real S3 integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.RealS3IntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **RealS3IntegrationIndexTablesTest** - Real S3 IndexTables integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.RealS3IntegrationIndexTablesTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **RealS3IntegrationJustMergeTest** - Real S3 merge operations
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.RealS3IntegrationJustMergeTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **RealS3MultipartUploaderTest** - Real S3 multipart uploads
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.RealS3MultipartUploaderTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **RealS3CompressionIntegrationTest** - Real S3 compression integration
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.compression.RealS3CompressionIntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **CustomCredentialProviderRealS3IntegrationTest** - Real S3 custom credentials
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.storage.CustomCredentialProviderRealS3IntegrationTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **RealS3MultiDimensionalGroupByTest** - Real S3 multi-dimensional GroupBy
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.RealS3MultiDimensionalGroupByTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **MergeSplitsS3Test** - Merge splits S3 tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsS3Test'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **MergeSplitsS3DirectTest** - Merge splits S3 direct tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MergeSplitsS3DirectTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **V2S3ReviewDataTest** - V2 S3 review data tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.V2S3ReviewDataTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

- [ ] **S3TrailingSlashTest** - S3 trailing slash handling
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.io.S3TrailingSlashTest'`
  - Status: Not started
  - Failures: None
  - Notes: Requires AWS credentials

### Phase 25: Remaining Tests
All remaining test files not categorized above.

- [ ] **FastFieldLocalReproTest** - Fast field local reproduction
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.FastFieldLocalReproTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FastFieldMergeTest** - Fast field merge tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.FastFieldMergeTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **FastFieldScalaReproTest** - Fast field Scala reproduction
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.FastFieldScalaReproTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **AggregateExpressionTest** - Aggregate expression tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.core.AggregateExpressionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **ProtocolVersionTest** - Protocol version tests
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.ProtocolVersionTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogFileNamingTest** - Transaction log file naming
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogFileNamingTest'`
  - Status: Not started
  - Failures: None
  - Notes:

- [ ] **TransactionLogLargeBatchTest** - Transaction log large batch
  - Command: `mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.transaction.TransactionLogLargeBatchTest'`
  - Status: Not started
  - Failures: None
  - Notes:

---

## Progress Summary

### Overall Status
- **Total Tests**: 176
- **Completed**: 0
- **In Progress**: 0
- **Failed**: 0
- **Skipped**: 0
- **Remaining**: 176

### Phase Completion
- Phase 1 (Core): 0/5
- Phase 2 (Schema): 0/5
- Phase 3 (Transaction Log): 0/10
- Phase 4 (IndexQuery): 0/7
- Phase 5 (Aggregates): 0/11
- Phase 6 (Merge Splits): 0/8
- Phase 7 (Partitions): 0/5
- Phase 8 (S3/Storage): 0/7
- Phase 9 (Config): 0/7
- Phase 10 (Credentials): 0/5
- Phase 11 (Filters): 0/7
- Phase 12 (Statistics): 0/4
- Phase 13 (V2 Advanced): 0/7
- Phase 14 (Indexing): 0/4
- Phase 15 (Optimization): 0/3
- Phase 16 (Cache): 0/6
- Phase 17 (Search): 0/1
- Phase 18 (SQL Extensions): 0/5
- Phase 19 (Utilities): 0/4
- Phase 20 (AQE): 0/3
- Phase 21 (Debug): 0/28
- Phase 22 (IndexQuery Additional): 0/10
- Phase 23 (Integration): 0/3
- Phase 24 (Real S3): 0/11
- Phase 25 (Remaining): 0/7

---

## Known Scala 2.13 Migration Changes to Watch For

### 1. Java Collection Conversions
- **What changed**: `docBatch()` return type conversion in `SplitSearchEngine.scala`
- **Watch for**: Type inference issues with `.asScala` conversions
- **Fix applied**: Explicit `java.util.List` casting with `.toSeq`

### 2. Future.sequence Execution Context
- **What changed**: Removed `breakOut` parameter, added implicit execution contexts
- **Watch for**: Missing execution context errors
- **Fix applied**: Local implicit declarations in 4 locations

### 3. Parallel Collections
- **What changed**: Removed `.par` from `MergeSplitsCommand.scala`
- **Watch for**: Performance changes in merge operations (now sequential at batch level)
- **Fix applied**: Sequential batch processing, parallelization handled by Spark RDD

### 4. ArrayBuffer to Seq Conversions
- **What changed**: Explicit `.toSeq` conversions required
- **Watch for**: Type mismatch errors in transaction log operations
- **Fix applied**: Added `.toSeq` conversions in `commitMergeSplits` calls

---

## Regression Categories to Monitor

### High Risk Areas
1. **Merge Splits Operations** - Sequential processing after parallel collections removal
2. **docBatch Operations** - Java collection conversion changes
3. **Transaction Log** - Future.sequence execution context changes
4. **Type Inference** - Scala 2.13 stricter type inference

### Medium Risk Areas
1. **Collection API Usage** - MapView, FilterKeys behavior changes
2. **Implicit Conversions** - Changes in implicit resolution
3. **Pattern Matching** - Exhaustiveness checking changes

### Low Risk Areas
1. **Import Statements** - Pure rename (JavaConverters → CollectionConverters)
2. **String Operations** - No API changes
3. **Numerical Operations** - No API changes

---

## Test Execution Guidelines

### Before Running Each Test
1. Ensure compilation is clean: `mvn clean compile -DskipTests`
2. Review test file for any Scala 2.12 patterns
3. Note test category and expected behavior

### When a Test Fails
1. **Document the failure**: Capture full error message and stack trace
2. **Identify root cause**: Determine if it's Scala 2.13 related or pre-existing
3. **Fix the issue**: Make minimal, targeted changes
4. **Verify the fix**: Re-run only the failing test
5. **Regression check**: Re-run any related tests
6. **Update this plan**: Mark test as completed with notes

### After Each Test Passes
1. Update checkbox in this document
2. Mark status as "Completed"
3. Add any relevant notes about the test
4. Commit changes to preserve progress

---

## Success Criteria

The Scala 2.13 migration is complete when:
- ✅ All 176 tests pass (or documented skips for real S3 tests without credentials)
- ✅ No Scala 2.12 compatibility code remains
- ✅ Performance benchmarks meet expectations (especially transaction log 60% improvement)
- ✅ All regressions fixed and documented

---

## Notes and Observations

### Test Execution Log
(Add notes here as tests are executed)

---

**Last Updated**: 2025-10-15
**Migration Status**: Tests not yet started - compilation successful
