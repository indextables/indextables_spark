# Code Review: Redundant Code and Duplicate Algorithms

**Date**: 2025-10-27
**Project**: IndexTables4Spark
**Reviewer**: Claude Code
**Scope**: Full codebase analysis for redundant code and duplicate algorithms

---

## Executive Summary

This production Scala/Java codebase contains **significant code duplication** across multiple domains including:
- URL/protocol normalization logic (appears in 8+ files)
- Cloud configuration extraction (600+ lines duplicated in CloudStorageProvider.scala)
- Cache configuration creation (duplicated across 3 scan types before recent refactoring)
- SplitMetadata creation (duplicated in 4 files)
- Field name extraction from expressions (duplicated in 3 aggregate scan files)
- Credential resolution patterns (duplicated across multiple storage providers)

**Total Estimated Redundancy**: ~1,848 lines across 20+ files
**Potential Reduction**: ~1,067 lines (58% reduction)

---

## Critical Findings

### 1. URL Protocol Normalization - MASSIVE DUPLICATION

**Severity**: **HIGH**
**Estimated Effort**: 3-4 hours
**Lines Duplicated**: ~150 lines across 8+ files

#### Location

Appears in 8+ files:
- `src/main/scala/io/indextables/spark/io/CloudStorageProvider.scala:568-635`
- `src/main/scala/io/indextables/spark/io/S3CloudStorageProvider.scala:84-89, 107-129`
- `src/main/scala/io/indextables/spark/io/AzureCloudStorageProvider.scala:78-113`
- `src/main/scala/io/indextables/spark/io/HadoopCloudStorageProvider.scala`
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala`
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkDataSource.scala`
- `src/main/scala/io/indextables/spark/sql/MergeSplitsCommand.scala`
- `src/main/scala/io/indextables/spark/prewarm/PreWarmManager.scala`

#### Description

The same S3 protocol normalization logic (`s3a://`, `s3n://` → `s3://`) and Azure protocol normalization (`wasb://`, `wasbs://`, `abfs://`, `abfss://` → `azure://`) appears in multiple places with slight variations:

```scala
// Pattern 1: In CloudStorageProvider.scala (static method)
val protocolNormalized = if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
  path.replaceFirst("^s3[an]://", "s3://")
} else if (path.startsWith("wasb://") || path.startsWith("wasbs://") ||
           path.startsWith("abfs://") || path.startsWith("abfss://")) {
  // Azure normalization logic...
} else { path }

// Pattern 2: In S3CloudStorageProvider.scala (private method)
private def normalizeProtocolForTantivy(path: String): String =
  if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
    path.replaceFirst("^s3[an]://", "s3://")
  } else { path }

// Pattern 3: Override methods in each storage provider
override def normalizePathForTantivy(path: String): String = { /* custom logic */ }
```

#### Recommendation

1. Create a single `ProtocolNormalizer` utility object with:
   - `normalizeS3Protocol(path: String): String`
   - `normalizeAzureProtocol(path: String): String`
   - `normalizeAllProtocols(path: String): String`
2. Update all 8+ files to use the centralized utility
3. Remove all duplicate implementations

#### Expected Impact

- **Lines removed**: ~120 lines (80% reduction)
- **Files simplified**: 8+ files
- **Maintenance benefit**: Single source of truth for protocol normalization

---

### 2. Cloud Configuration Extraction - 636 LINES OF DUPLICATION

**Severity**: **HIGH**
**Estimated Effort**: 4-6 hours
**Lines Duplicated**: ~350 lines (internal duplication within CloudStorageProvider.scala)

#### Location

- `src/main/scala/io/indextables/spark/io/CloudStorageProvider.scala:210-559`
- Similar patterns in individual storage providers (S3CloudStorageProvider, AzureCloudStorageProvider)

#### Description

The `enrichHadoopConfWithSparkConf` method (lines 210-304) and `extractCloudConfig` method (lines 306-559) contain extensive duplication:

**Issue 1: Duplicate credential extraction chains** (appears 6+ times):

```scala
// Access key extraction (duplicated for access key, secret key, session token, region, etc.)
val accessKeyFromOptions           = Option(options.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopTantivy     = Option(hadoopConf.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopIndexTables = Option(hadoopConf.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopS3a         = Option(hadoopConf.get("spark.hadoop.fs.s3a.access.key"))
val accessKeyFromS3a               = Option(hadoopConf.get("fs.s3a.access.key"))

// Repeated for secretKey (lines 345-356)
// Repeated for sessionToken (lines 371-379)
// Repeated for region (lines 410-440)
// etc.
```

**Issue 2: Duplicate configuration copying** (lines 224-293):

```scala
// String configurations copied twice (lines 224-243 duplicate lines 231-236)
stringConfigs.foreach { key =>
  // Same extraction logic repeated
}

// Boolean configurations copied twice (lines 240-293 duplicate pattern)
booleanConfigs.foreach { key =>
  // Same extraction logic repeated
}
```

**Issue 3: Duplicate pathStyleAccess logic** (lines 450-505):
Multiple extraction attempts for the same boolean value from 7+ sources

#### Recommendation

1. Create `ConfigurationResolver` utility with generic methods:
   - `resolveString(key: String, sources: Seq[ConfigSource]): Option[String]`
   - `resolveBoolean(key: String, sources: Seq[ConfigSource]): Boolean`
   - `resolveLong(key: String, sources: Seq[ConfigSource]): Long`
2. Define configuration keys once in a sealed trait hierarchy
3. Consolidate all credential extraction into single parameterized method

#### Expected Impact

- **Lines removed**: ~270 lines (78% reduction)
- **Code clarity**: Significantly improved with declarative configuration definitions
- **Maintenance benefit**: Single location for credential resolution logic

---

### 3. Cache Configuration Creation - RECENTLY REFACTORED BUT STILL DUPLICATED

**Severity**: **MEDIUM** (Partially fixed)
**Estimated Effort**: 2 hours
**Lines Duplicated**: ~270 lines (before refactoring), ~135 lines remaining

#### Location

- `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala:190-325` (~135 lines of custom logic)
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkSimpleAggregateScan.scala` (uses utility ✅)
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkGroupByAggregateScan.scala:796-811` (uses utility ✅)

#### Description

Good news: `ConfigUtils.createSplitCacheConfig()` was created to consolidate this! However:

**IndexTables4SparkPartitions still has ~135 lines of custom cache config logic** instead of using the utility:

```scala
// In IndexTables4SparkPartitions.scala - SHOULD USE UTILITY
private def createCacheConfig(): SplitCacheConfig = {
  // 135 lines of manual config extraction that duplicates ConfigUtils logic
  val configMap = broadcasted
  def getBroadcastConfig(...) { ... } // Duplicates ConfigUtils logic
  def getBroadcastConfigOption(...) { ... } // Duplicates ConfigUtils logic
  SplitCacheConfig(
    cacheName = { /* manual logic */ },
    maxCacheSize = { /* manual parsing */ },
    // ... 100+ more lines
  )
}
```

**GroupByAggregateScan properly uses the utility** (lines 796-811):

```scala
// CORRECT PATTERN - should be used everywhere
private def createCacheConfig(): SplitCacheConfig = {
  io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
    partition.config,
    Some(partition.tablePath.toString)
  )
}
```

#### Recommendation

1. **Refactor IndexTables4SparkPartitions.createCacheConfig()** to use `ConfigUtils.createSplitCacheConfig()`
2. Remove the 135 lines of duplicate config extraction logic
3. Add unit tests to ensure behavior parity

#### Expected Impact

- **Lines removed**: ~135 lines (50% reduction)
- **Consistency**: All scan types use same utility
- **Maintenance benefit**: Single source of truth for cache configuration

---

### 4. SplitMetadata Creation from AddAction

**Severity**: **MEDIUM**
**Estimated Effort**: 3-4 hours
**Lines Duplicated**: ~200 lines across 4 files

#### Location

- `src/main/scala/io/indextables/spark/core/IndexTables4SparkGroupByAggregateScan.scala:814-866`
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkSimpleAggregateScan.scala` (similar pattern)
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala` (similar pattern)
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkDataSource.scala`

#### Description

The same ~50 line method for creating `QuickwitSplit.SplitMetadata` from `AddAction` is duplicated across 4 files:

```scala
private def createSplitMetadataFromSplit(): QuickwitSplit.SplitMetadata = {
  val splitId = partition.split.path.split("/").last.replace(".split", "")
  val addAction = partition.split

  val (footerStartOffset, footerEndOffset) =
    if (addAction.footerStartOffset.isDefined && addAction.footerEndOffset.isDefined) {
      (addAction.footerStartOffset.get, addAction.footerEndOffset.get)
    } else {
      // Fallback logic with try-catch...
    }

  new QuickwitSplit.SplitMetadata(
    splitId,
    "tantivy4spark-index",
    0L,
    // ... 15 more parameters
  )
}
```

#### Recommendation

1. Create `SplitMetadataFactory.fromAddAction(addAction: AddAction): QuickwitSplit.SplitMetadata`
2. Consolidate the footer offset fallback logic
3. Remove duplicates from all 4 files

#### Expected Impact

- **Lines removed**: ~140 lines (70% reduction)
- **Files simplified**: 4 files
- **Maintenance benefit**: Consistent SplitMetadata creation across all components

---

### 5. Field Name Extraction from Spark Expressions

**Severity**: **LOW-MEDIUM**
**Estimated Effort**: 1-2 hours
**Lines Duplicated**: ~48 lines across 3 files

#### Location

- `src/main/scala/io/indextables/spark/core/IndexTables4SparkSimpleAggregateScan.scala:184-200`
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkGroupByAggregateScan.scala:869-885`
- `src/main/scala/io/indextables/spark/core/IndexTables4SparkScanBuilder.scala`

#### Description

The same regex-based field name extraction appears in 3 files with identical logic:

```scala
private def extractFieldNameFromExpression(expression: Expression): String = {
  val exprStr = expression.toString
  if (exprStr.startsWith("FieldReference(")) {
    val pattern = """FieldReference\(([^)]+)\)""".r
    pattern.findFirstMatchIn(exprStr) match {
      case Some(m) => m.group(1)
      case None =>
        logger.warn(s"Could not extract field name from expression: $expression")
        "unknown_field"
    }
  } else {
    logger.warn(s"Unsupported expression type for field extraction: $expression")
    "unknown_field"
  }
}
```

#### Recommendation

1. Create `ExpressionUtils.extractFieldName(expression: Expression): String` in `src/main/scala/io/indextables/spark/util/ExpressionUtils.scala`
2. Remove duplicates from all 3 files

#### Expected Impact

- **Lines removed**: ~32 lines (67% reduction)
- **Files simplified**: 3 files
- **Maintenance benefit**: Single regex pattern definition

---

### 6. Credential Resolution Priority Logic

**Severity**: **MEDIUM**
**Estimated Effort**: 3 hours
**Lines Duplicated**: ~230 lines (internal duplication)

#### Location

- AWS credentials: `CloudStorageProvider.scala:328-388`
- Azure credentials: `CloudStorageProvider.scala:508-530`
- Similar patterns in `CredentialProviderFactory.scala`

#### Description

The same priority-based credential resolution pattern is duplicated for multiple credential types:

```scala
// AWS Access Key (lines 328-343)
val accessKeyFromOptions           = Option(options.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopTantivy     = Option(hadoopConf.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopIndexTables = Option(hadoopConf.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopS3a         = Option(hadoopConf.get("spark.hadoop.fs.s3a.access.key"))
val accessKeyFromS3a               = Option(hadoopConf.get("fs.s3a.access.key"))
val finalAccessKey = accessKeyFromOptions.orElse(...).orElse(...) // 5 fallbacks

// AWS Secret Key (lines 345-368) - IDENTICAL PATTERN
// AWS Session Token (lines 371-379) - IDENTICAL PATTERN
// AWS Region (lines 410-440) - IDENTICAL PATTERN with 6 sources
// Azure Account Name (line 508) - SIMILAR PATTERN
// Azure Account Key (line 511) - SIMILAR PATTERN
// etc.
```

#### Recommendation

1. Create generic `CredentialResolver.resolve(key: String, sources: Seq[ConfigSource], logMask: Boolean): Option[String]`
2. Define credential keys once with their resolution chains
3. Support masked logging for sensitive credentials

#### Expected Impact

- **Lines removed**: ~170 lines (74% reduction)
- **Security**: Consistent credential masking
- **Maintenance benefit**: Single resolution logic for all credentials

---

### 7. Transaction Log Operations - Dual Implementation

**Severity**: **MEDIUM**
**Estimated Effort**: 6-8 hours
**Lines Duplicated**: ~600 lines with ~60% overlap

#### Location

- `src/main/scala/io/indextables/spark/transaction/TransactionLog.scala` (300+ lines)
- `src/main/scala/io/indextables/spark/transaction/OptimizedTransactionLog.scala` (300+ lines)

#### Description

Two separate transaction log implementations exist with ~60% overlapping logic:

**Common operations duplicated:**
- `listFiles()`: Appears in both with similar checkpoint + incremental logic
- `addFiles()`: Similar version management
- `overwriteFiles()`: Near-identical REMOVE+ADD logic
- `initialize()`: Identical schema validation

#### Recommendation

1. Create abstract base class `BaseTransactionLog` with common operations
2. Extract shared logic: version management, protocol checking, schema validation
3. Keep optimization-specific code in `OptimizedTransactionLog`

#### Expected Impact

- **Lines removed**: ~200 lines (33% reduction)
- **Architecture**: Clear separation between base and optimized implementations
- **Maintenance benefit**: Bug fixes propagate to both implementations

---

## Summary of Redundancy

| **Area** | **Files Affected** | **Lines Duplicated** | **Severity** | **Estimated Savings** |
|----------|-------------------|---------------------|--------------|----------------------|
| URL Protocol Normalization | 8+ files | ~150 lines | HIGH | 120 lines (80%) |
| Cloud Config Extraction | 1 file (internal duplication) | ~350 lines | HIGH | 270 lines (78%) |
| Cache Config Creation | 3 files | ~270 lines | MEDIUM | 135 lines (50% - partially fixed) |
| SplitMetadata Creation | 4 files | ~200 lines | MEDIUM | 140 lines (70%) |
| Field Name Extraction | 3 files | ~48 lines | LOW-MEDIUM | 32 lines (67%) |
| Credential Resolution | 1 file (internal duplication) | ~230 lines | MEDIUM | 170 lines (74%) |
| Transaction Log Ops | 2 files | ~600 lines | MEDIUM | 200 lines (33%) |
| **TOTAL** | **20+ files** | **~1,848 lines** | - | **~1,067 lines (58%)** |

---

## Recommended Refactoring Priority

### Phase 1: High Impact (Week 1)

**Estimated Time**: 9-12 hours

1. **URL Protocol Normalization** → Create `ProtocolNormalizer` utility
   - Create `src/main/scala/io/indextables/spark/util/ProtocolNormalizer.scala`
   - Update 8+ files to use utility
   - Add unit tests

2. **Cloud Config Extraction** → Create `ConfigurationResolver` utility
   - Create `src/main/scala/io/indextables/spark/util/ConfigurationResolver.scala`
   - Refactor CloudStorageProvider.scala
   - Add comprehensive tests

3. **Cache Config Creation** → Update IndexTables4SparkPartitions to use ConfigUtils
   - Replace 135 lines with utility call
   - Add unit tests for behavior parity

**Expected Impact**: ~525 lines removed, 8+ files simplified

---

### Phase 2: Medium Impact (Week 2)

**Estimated Time**: 7-9 hours

4. **SplitMetadata Creation** → Create `SplitMetadataFactory`
   - Create `src/main/scala/io/indextables/spark/util/SplitMetadataFactory.scala`
   - Update 4 files
   - Add unit tests

5. **Credential Resolution** → Create `CredentialResolver` utility
   - Create `src/main/scala/io/indextables/spark/util/CredentialResolver.scala`
   - Refactor CloudStorageProvider.scala
   - Add tests with credential masking

6. **Field Name Extraction** → Add to `ExpressionUtils`
   - Create or update `src/main/scala/io/indextables/spark/util/ExpressionUtils.scala`
   - Update 3 files
   - Add tests

**Expected Impact**: ~342 lines removed, 7+ files simplified

---

### Phase 3: Architectural (Week 3-4)

**Estimated Time**: 6-8 hours

7. **Transaction Log Consolidation** → Create `BaseTransactionLog` abstract class
   - Create abstract base class
   - Refactor TransactionLog and OptimizedTransactionLog
   - Comprehensive testing

**Expected Impact**: ~200 lines removed, improved maintainability

---

## Additional Observations

### Positive Findings

✅ **Recent refactoring detected**: `ConfigUtils.createSplitCacheConfig()` shows good consolidation effort
✅ **GroupByAggregateScan** properly uses the utility (good pattern to follow)
✅ **Factory pattern used**: `TransactionLogFactory` shows architectural awareness
✅ **Comprehensive test coverage**: 228+ tests passing indicates good testing discipline

### Code Quality Concerns

⚠️ **Extensive logging duplication**: Debug logging patterns repeated across many files
⚠️ **Magic strings**: Configuration keys hardcoded in multiple places
⚠️ **Try-catch patterns**: Error handling logic duplicated across storage providers
⚠️ **Incomplete refactoring**: ConfigUtils utility exists but not used everywhere

---

## Testing Recommendations

For each consolidation:

1. **Create comprehensive unit tests** for new utility classes
   - Test all edge cases
   - Test configuration priority chains
   - Test error handling

2. **Run regression tests** on all affected components
   - Existing test suite (228+ tests)
   - Integration tests for cloud storage
   - Performance benchmarks

3. **Verify behavior parity** between old and new implementations
   - Compare outputs for identical inputs
   - Validate error messages unchanged
   - Check logging consistency

4. **Add integration tests** for credential resolution chains
   - Test all credential sources
   - Verify priority ordering
   - Test fallback behavior

---

## Implementation Notes

### New Utility Classes to Create

1. `src/main/scala/io/indextables/spark/util/ProtocolNormalizer.scala`
2. `src/main/scala/io/indextables/spark/util/ConfigurationResolver.scala`
3. `src/main/scala/io/indextables/spark/util/SplitMetadataFactory.scala`
4. `src/main/scala/io/indextables/spark/util/CredentialResolver.scala`
5. `src/main/scala/io/indextables/spark/util/ExpressionUtils.scala`
6. `src/main/scala/io/indextables/spark/transaction/BaseTransactionLog.scala` (abstract class)

### Breaking Changes

**None expected** - All changes should be internal refactoring with identical external behavior.

### Backward Compatibility

All refactoring should maintain:
- Identical public APIs
- Same configuration keys and values
- Same error messages and logging
- Same performance characteristics (or better)

---

## Conclusion

This codebase has **significant opportunities for consolidation** that would:

1. **Reduce maintenance burden** by eliminating ~1,067 lines of duplicate code
2. **Improve consistency** through single sources of truth
3. **Reduce bug surface area** by consolidating logic
4. **Enhance testability** with focused utility classes
5. **Improve onboarding** for new developers with clearer code organization

The refactoring is **low-risk** due to comprehensive test coverage and can be done incrementally over 3-4 weeks without disrupting ongoing development.

---

**Report Generated**: 2025-10-27
**Total Files Analyzed**: 80+ source files
**Analysis Method**: Pattern matching, grep analysis, manual code review
**Recommended Total Effort**: 22-29 hours (3-4 weeks part-time)
