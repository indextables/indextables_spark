# Comprehensive Refactoring Implementation Plan

**Project**: IndexTables4Spark Code Consolidation
**Created**: 2025-10-27
**Based On**: CODE_REVIEW_REDUNDANCY.md
**Total Duration**: 3-4 weeks (22-29 hours)
**Goal**: Eliminate ~1,067 lines of duplicate code (58% reduction)

---

## Table of Contents

1. [Overview](#overview)
2. [Phase 1: High Impact Refactoring (Week 1)](#phase-1-high-impact-refactoring-week-1)
3. [Phase 2: Medium Impact Refactoring (Week 2)](#phase-2-medium-impact-refactoring-week-2)
4. [Phase 3: Architectural Refactoring (Week 3-4)](#phase-3-architectural-refactoring-week-3-4)
5. [Testing Strategy](#testing-strategy)
6. [Risk Management](#risk-management)
7. [Rollback Plan](#rollback-plan)
8. [Success Metrics](#success-metrics)

---

## Overview

### Objectives

1. **Eliminate code duplication**: Remove ~1,067 lines of redundant code
2. **Improve maintainability**: Create single sources of truth for common operations
3. **Reduce bug surface area**: Consolidate logic to prevent divergent implementations
4. **Enhance testability**: Create focused utility classes with clear responsibilities
5. **Zero breaking changes**: Maintain backward compatibility throughout

### Approach

- **Incremental refactoring**: Small, testable changes with continuous validation
- **Feature branch per phase**: Each phase gets its own branch for easy rollback
- **Test-driven**: Write tests before refactoring, ensure green before moving on
- **Backward compatibility**: All changes maintain existing APIs and behavior

### Key Principles

✅ **Safety First**: No production impact, comprehensive testing
✅ **Incremental Progress**: Small PRs that can be reviewed independently
✅ **Continuous Validation**: Run full test suite after each change
✅ **Documentation**: Update docs as utilities are created

---

## Phase 1: High Impact Refactoring (Week 1)

**Duration**: 9-12 hours
**Expected Impact**: ~525 lines removed, 8+ files simplified
**Branch**: `refactor/phase1-high-impact`

---

### Task 1.1: Create ProtocolNormalizer Utility

**Duration**: 3-4 hours
**Priority**: HIGH
**Lines Saved**: ~120 lines (80% reduction)

#### Step 1: Create Utility Class

**File**: `src/main/scala/io/indextables/spark/util/ProtocolNormalizer.scala`

```scala
package io.indextables.spark.util

import org.slf4j.LoggerFactory

/**
 * Utility for normalizing cloud storage protocol schemes to tantivy4java-compatible formats.
 *
 * Supported normalizations:
 * - S3: s3a://, s3n:// → s3://
 * - Azure: wasb://, wasbs://, abfs://, abfss:// → azure://
 */
object ProtocolNormalizer {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Normalizes S3 protocol schemes to s3:// format.
   *
   * @param path The path to normalize
   * @return Normalized path with s3:// scheme, or original path if not S3
   */
  def normalizeS3Protocol(path: String): String = {
    if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
      val normalized = path.replaceFirst("^s3[an]://", "s3://")
      logger.debug(s"Normalized S3 protocol: $path → $normalized")
      normalized
    } else {
      path
    }
  }

  /**
   * Normalizes Azure protocol schemes to azure:// format.
   *
   * Handles: wasb://, wasbs://, abfs://, abfss://
   * Extracts container name and path from full Azure URLs
   *
   * @param path The path to normalize
   * @return Normalized path with azure:// scheme, or original path if not Azure
   */
  def normalizeAzureProtocol(path: String): String = {
    if (path.startsWith("wasb://") || path.startsWith("wasbs://")) {
      // Extract: wasb[s]://container@account.blob.core.windows.net/path → azure://container/path
      val pattern = """wasbs?://([^@]+)@[^/]+/(.*)""".r
      path match {
        case pattern(container, pathPart) =>
          val normalized = s"azure://$container/$pathPart"
          logger.debug(s"Normalized Azure WASB protocol: $path → $normalized")
          normalized
        case _ =>
          logger.warn(s"Could not parse Azure WASB URL: $path")
          path
      }
    } else if (path.startsWith("abfs://") || path.startsWith("abfss://")) {
      // Extract: abfs[s]://container@account.dfs.core.windows.net/path → azure://container/path
      val pattern = """abfss?://([^@]+)@[^/]+/(.*)""".r
      path match {
        case pattern(container, pathPart) =>
          val normalized = s"azure://$container/$pathPart"
          logger.debug(s"Normalized Azure ABFS protocol: $path → $normalized")
          normalized
        case _ =>
          logger.warn(s"Could not parse Azure ABFS URL: $path")
          path
      }
    } else {
      path
    }
  }

  /**
   * Normalizes all supported cloud storage protocols.
   *
   * @param path The path to normalize
   * @return Normalized path, or original if no normalization needed
   */
  def normalizeAllProtocols(path: String): String = {
    val afterS3 = normalizeS3Protocol(path)
    val afterAzure = normalizeAzureProtocol(afterS3)
    afterAzure
  }

  /**
   * Checks if a path uses an S3 protocol scheme.
   *
   * @param path The path to check
   * @return true if path starts with s3://, s3a://, or s3n://
   */
  def isS3Path(path: String): Boolean = {
    path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")
  }

  /**
   * Checks if a path uses an Azure protocol scheme.
   *
   * @param path The path to check
   * @return true if path starts with azure://, wasb://, wasbs://, abfs://, or abfss://
   */
  def isAzurePath(path: String): Boolean = {
    path.startsWith("azure://") ||
    path.startsWith("wasb://") ||
    path.startsWith("wasbs://") ||
    path.startsWith("abfs://") ||
    path.startsWith("abfss://")
  }
}
```

#### Step 2: Create Unit Tests

**File**: `src/test/scala/io/indextables/spark/util/ProtocolNormalizerTest.scala`

```scala
package io.indextables.spark.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProtocolNormalizerTest extends AnyFunSuite with Matchers {

  test("normalizeS3Protocol should convert s3a:// to s3://") {
    ProtocolNormalizer.normalizeS3Protocol("s3a://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should convert s3n:// to s3://") {
    ProtocolNormalizer.normalizeS3Protocol("s3n://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should leave s3:// unchanged") {
    ProtocolNormalizer.normalizeS3Protocol("s3://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should leave non-S3 paths unchanged") {
    ProtocolNormalizer.normalizeS3Protocol("hdfs://path") shouldBe "hdfs://path"
  }

  test("normalizeAzureProtocol should convert wasb:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol("wasb://container@account.blob.core.windows.net/path") shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert wasbs:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol("wasbs://container@account.blob.core.windows.net/path") shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert abfs:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol("abfs://container@account.dfs.core.windows.net/path") shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert abfss:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol("abfss://container@account.dfs.core.windows.net/path") shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should leave azure:// unchanged") {
    ProtocolNormalizer.normalizeAzureProtocol("azure://container/path") shouldBe "azure://container/path"
  }

  test("normalizeAllProtocols should normalize S3 paths") {
    ProtocolNormalizer.normalizeAllProtocols("s3a://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeAllProtocols should normalize Azure paths") {
    ProtocolNormalizer.normalizeAllProtocols("abfss://container@account.dfs.core.windows.net/path") shouldBe "azure://container/path"
  }

  test("isS3Path should detect S3 schemes") {
    ProtocolNormalizer.isS3Path("s3://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("s3a://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("s3n://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("azure://container/path") shouldBe false
  }

  test("isAzurePath should detect Azure schemes") {
    ProtocolNormalizer.isAzurePath("azure://container/path") shouldBe true
    ProtocolNormalizer.isAzurePath("wasb://container@account.blob.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("abfss://container@account.dfs.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("s3://bucket/path") shouldBe false
  }
}
```

#### Step 3: Update All Files to Use Utility

**Files to Update** (8+ files):

1. `src/main/scala/io/indextables/spark/io/CloudStorageProvider.scala:568-635`
   - Replace static `normalizePathForTantivy` method with call to `ProtocolNormalizer.normalizeAllProtocols`

2. `src/main/scala/io/indextables/spark/io/S3CloudStorageProvider.scala:84-89, 107-129`
   - Remove private `normalizeProtocolForTantivy` method
   - Replace calls with `ProtocolNormalizer.normalizeS3Protocol`

3. `src/main/scala/io/indextables/spark/io/AzureCloudStorageProvider.scala:78-113`
   - Remove override `normalizePathForTantivy` method
   - Replace calls with `ProtocolNormalizer.normalizeAzureProtocol`

4. `src/main/scala/io/indextables/spark/io/HadoopCloudStorageProvider.scala`
   - Update any normalization logic

5. `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala`
   - Replace inline normalization with utility calls

6. `src/main/scala/io/indextables/spark/core/IndexTables4SparkDataSource.scala`
   - Replace inline normalization with utility calls

7. `src/main/scala/io/indextables/spark/sql/MergeSplitsCommand.scala`
   - Replace inline normalization with utility calls

8. `src/main/scala/io/indextables/spark/prewarm/PreWarmManager.scala`
   - Replace inline normalization with utility calls

#### Step 4: Validation

- [ ] Run `ProtocolNormalizerTest` - all tests pass
- [ ] Run full test suite (`mvn test`) - 228+ tests pass
- [ ] Run integration tests with S3 and Azure
- [ ] Verify no behavior changes with debug logging

**Acceptance Criteria**:
- ✅ All 228+ existing tests pass
- ✅ New utility has 100% test coverage
- ✅ No changes to external behavior
- ✅ ~120 lines of duplicate code removed

---

### Task 1.2: Create ConfigurationResolver Utility

**Duration**: 4-6 hours
**Priority**: HIGH
**Lines Saved**: ~270 lines (78% reduction)

#### Step 1: Design Configuration Source Abstraction

**File**: `src/main/scala/io/indextables/spark/util/ConfigurationResolver.scala`

```scala
package io.indextables.spark.util

import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Configuration source abstraction for priority-based resolution.
 */
sealed trait ConfigSource {
  def get(key: String): Option[String]
  def name: String
}

case class OptionsConfigSource(options: java.util.Map[String, String]) extends ConfigSource {
  override def get(key: String): Option[String] = Option(options.get(key))
  override def name: String = "DataFrame options"
}

case class HadoopConfigSource(hadoopConf: Configuration, prefix: String = "") extends ConfigSource {
  override def get(key: String): Option[String] = {
    val fullKey = if (prefix.isEmpty) key else s"$prefix.$key"
    Option(hadoopConf.get(fullKey))
  }
  override def name: String = if (prefix.isEmpty) "Hadoop config" else s"Hadoop config ($prefix)"
}

case class SparkConfigSource(sparkConf: org.apache.spark.SparkConf) extends ConfigSource {
  override def get(key: String): Option[String] = sparkConf.getOption(key)
  override def name: String = "Spark config"
}

case class EnvironmentConfigSource() extends ConfigSource {
  override def get(key: String): Option[String] = sys.env.get(key)
  override def name: String = "Environment variables"
}

/**
 * Utility for resolving configuration values from multiple sources with priority ordering.
 *
 * Provides consistent credential resolution across AWS, Azure, and other configuration needs.
 */
object ConfigurationResolver {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Resolves a string configuration value from multiple sources in priority order.
   *
   * @param key Configuration key to resolve
   * @param sources Ordered sequence of configuration sources (first match wins)
   * @param logMask If true, masks the value in logs (for credentials)
   * @return Resolved value, or None if not found in any source
   */
  def resolveString(
    key: String,
    sources: Seq[ConfigSource],
    logMask: Boolean = false
  ): Option[String] = {
    sources.foreach { source =>
      source.get(key) match {
        case Some(value) if value.nonEmpty =>
          val displayValue = if (logMask) "****" else value
          logger.debug(s"Resolved '$key' from ${source.name}: $displayValue")
          return Some(value)
        case _ =>
      }
    }
    logger.debug(s"Could not resolve '$key' from any source")
    None
  }

  /**
   * Resolves a boolean configuration value from multiple sources.
   *
   * @param key Configuration key to resolve
   * @param sources Ordered sequence of configuration sources
   * @param default Default value if not found
   * @return Resolved boolean value
   */
  def resolveBoolean(
    key: String,
    sources: Seq[ConfigSource],
    default: Boolean = false
  ): Boolean = {
    resolveString(key, sources, logMask = false) match {
      case Some(value) =>
        val boolValue = value.toLowerCase match {
          case "true" | "1" | "yes" | "on" => true
          case "false" | "0" | "no" | "off" => false
          case _ =>
            logger.warn(s"Invalid boolean value for '$key': '$value', using default: $default")
            default
        }
        logger.debug(s"Resolved boolean '$key': $boolValue")
        boolValue
      case None =>
        logger.debug(s"Using default for '$key': $default")
        default
    }
  }

  /**
   * Resolves a long configuration value from multiple sources.
   *
   * @param key Configuration key to resolve
   * @param sources Ordered sequence of configuration sources
   * @param default Default value if not found
   * @return Resolved long value
   */
  def resolveLong(
    key: String,
    sources: Seq[ConfigSource],
    default: Long
  ): Long = {
    resolveString(key, sources, logMask = false) match {
      case Some(value) =>
        try {
          val longValue = value.toLong
          logger.debug(s"Resolved long '$key': $longValue")
          longValue
        } catch {
          case e: NumberFormatException =>
            logger.warn(s"Invalid long value for '$key': '$value', using default: $default", e)
            default
        }
      case None =>
        logger.debug(s"Using default for '$key': $default")
        default
    }
  }

  /**
   * Resolves multiple configuration keys as a batch.
   *
   * @param keys Configuration keys to resolve
   * @param sources Ordered sequence of configuration sources
   * @param logMask If true, masks values in logs
   * @return Map of resolved key-value pairs (only includes found keys)
   */
  def resolveBatch(
    keys: Seq[String],
    sources: Seq[ConfigSource],
    logMask: Boolean = false
  ): Map[String, String] = {
    keys.flatMap { key =>
      resolveString(key, sources, logMask).map(key -> _)
    }.toMap
  }

  /**
   * Creates standard AWS credential resolution sources in priority order.
   *
   * Priority: DataFrame options > Spark config > Hadoop tantivy > Hadoop S3a > Environment
   */
  def createAWSCredentialSources(
    options: java.util.Map[String, String],
    hadoopConf: Configuration
  ): Seq[ConfigSource] = Seq(
    OptionsConfigSource(options),
    HadoopConfigSource(hadoopConf, "spark.indextables.aws"),
    HadoopConfigSource(hadoopConf, "spark.hadoop.fs.s3a"),
    HadoopConfigSource(hadoopConf, "fs.s3a"),
    EnvironmentConfigSource()
  )

  /**
   * Creates standard Azure credential resolution sources in priority order.
   *
   * Priority: DataFrame options > Spark config > Hadoop config > Environment
   */
  def createAzureCredentialSources(
    options: java.util.Map[String, String],
    hadoopConf: Configuration
  ): Seq[ConfigSource] = Seq(
    OptionsConfigSource(options),
    HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
    HadoopConfigSource(hadoopConf),
    EnvironmentConfigSource()
  )
}
```

#### Step 2: Create Unit Tests

**File**: `src/test/scala/io/indextables/spark/util/ConfigurationResolverTest.scala`

```scala
package io.indextables.spark.util

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ConfigurationResolverTest extends AnyFunSuite with Matchers {

  test("resolveString should return value from first source that has it") {
    val options = Map("key1" -> "value1").asJava
    val hadoopConf = new Configuration()
    hadoopConf.set("key2", "value2")

    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    ConfigurationResolver.resolveString("key1", sources) shouldBe Some("value1")
    ConfigurationResolver.resolveString("key2", sources) shouldBe Some("value2")
  }

  test("resolveString should prioritize earlier sources") {
    val options = Map("key" -> "value1").asJava
    val hadoopConf = new Configuration()
    hadoopConf.set("key", "value2")

    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    ConfigurationResolver.resolveString("key", sources) shouldBe Some("value1")
  }

  test("resolveString should return None if key not found") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveString("missing", sources) shouldBe None
  }

  test("resolveBoolean should parse boolean values") {
    val options = Map(
      "true1" -> "true",
      "true2" -> "1",
      "true3" -> "yes",
      "true4" -> "on",
      "false1" -> "false",
      "false2" -> "0",
      "false3" -> "no",
      "false4" -> "off"
    ).asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("true1", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true2", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true3", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true4", sources) shouldBe true

    ConfigurationResolver.resolveBoolean("false1", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false2", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false3", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false4", sources) shouldBe false
  }

  test("resolveBoolean should use default for missing keys") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("missing", sources, default = true) shouldBe true
    ConfigurationResolver.resolveBoolean("missing", sources, default = false) shouldBe false
  }

  test("resolveLong should parse long values") {
    val options = Map("num" -> "12345").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveLong("num", sources, default = 0L) shouldBe 12345L
  }

  test("resolveLong should use default for invalid values") {
    val options = Map("invalid" -> "not-a-number").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveLong("invalid", sources, default = 999L) shouldBe 999L
  }

  test("resolveBatch should resolve multiple keys") {
    val options = Map(
      "key1" -> "value1",
      "key2" -> "value2"
    ).asJava
    val sources = Seq(OptionsConfigSource(options))

    val result = ConfigurationResolver.resolveBatch(Seq("key1", "key2", "missing"), sources)

    result shouldBe Map("key1" -> "value1", "key2" -> "value2")
  }
}
```

#### Step 3: Refactor CloudStorageProvider.scala

Replace lines 328-559 in `CloudStorageProvider.scala`:

**Before** (~230 lines of duplicate credential extraction):
```scala
// AWS Access Key (lines 328-343)
val accessKeyFromOptions = Option(options.get("spark.indextables.aws.accessKey"))
val accessKeyFromHadoopTantivy = Option(hadoopConf.get("spark.indextables.aws.accessKey"))
// ... 5 more sources ...
val finalAccessKey = accessKeyFromOptions.orElse(...).orElse(...)

// AWS Secret Key (lines 345-368) - DUPLICATE PATTERN
// AWS Session Token (lines 371-379) - DUPLICATE PATTERN
// AWS Region (lines 410-440) - DUPLICATE PATTERN
// Azure credentials - DUPLICATE PATTERN
```

**After** (~60 lines with ConfigurationResolver):
```scala
import io.indextables.spark.util.ConfigurationResolver

// Create source hierarchies once
private val awsSources = ConfigurationResolver.createAWSCredentialSources(options, hadoopConf)
private val azureSources = ConfigurationResolver.createAzureCredentialSources(options, hadoopConf)

// Resolve AWS credentials with masking
val accessKey = ConfigurationResolver.resolveString("spark.indextables.aws.accessKey", awsSources, logMask = true)
val secretKey = ConfigurationResolver.resolveString("spark.indextables.aws.secretKey", awsSources, logMask = true)
val sessionToken = ConfigurationResolver.resolveString("spark.indextables.aws.sessionToken", awsSources, logMask = true)
val region = ConfigurationResolver.resolveString("spark.indextables.aws.region", awsSources)

// Resolve Azure credentials with masking
val azureAccountName = ConfigurationResolver.resolveString("spark.indextables.azure.accountName", azureSources)
val azureAccountKey = ConfigurationResolver.resolveString("spark.indextables.azure.accountKey", azureSources, logMask = true)

// Resolve boolean configurations
val pathStyleAccess = ConfigurationResolver.resolveBoolean(
  "spark.indextables.s3.pathStyleAccess",
  awsSources,
  default = false
)
```

#### Step 4: Validation

- [ ] Run `ConfigurationResolverTest` - all tests pass
- [ ] Run full test suite - 228+ tests pass
- [ ] Verify credential resolution with multiple sources
- [ ] Check debug logs show proper masking

**Acceptance Criteria**:
- ✅ All tests pass
- ✅ ~270 lines removed from CloudStorageProvider.scala
- ✅ Credential masking works correctly
- ✅ Same resolution priority maintained

---

### Task 1.3: Refactor Cache Configuration in IndexTables4SparkPartitions

**Duration**: 2 hours
**Priority**: MEDIUM
**Lines Saved**: ~135 lines (50% reduction)

#### Step 1: Identify Current Implementation

**File**: `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala:190-325`

Current: ~135 lines of custom `createCacheConfig()` method

#### Step 2: Replace with ConfigUtils Call

**Before** (lines 190-325):
```scala
private def createCacheConfig(): SplitCacheConfig = {
  val configMap = broadcasted
  def getBroadcastConfig(...) { ... }
  def getBroadcastConfigOption(...) { ... }
  SplitCacheConfig(
    cacheName = { /* 100+ lines of manual logic */ },
    // ...
  )
}
```

**After** (~10 lines):
```scala
private def createCacheConfig(): SplitCacheConfig = {
  io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
    partition.config,
    Some(partition.tablePath.toString)
  )
}
```

#### Step 3: Validation

- [ ] Run partition-related tests
- [ ] Verify cache configuration matches previous behavior
- [ ] Check cache hits/misses in integration tests

**Acceptance Criteria**:
- ✅ All partition tests pass
- ✅ Cache behavior identical to before
- ✅ ~135 lines removed

---

### Phase 1 Completion Checklist

- [ ] Task 1.1: ProtocolNormalizer created and tested
- [ ] Task 1.2: ConfigurationResolver created and tested
- [ ] Task 1.3: Cache config refactored
- [ ] All 228+ tests passing
- [ ] Integration tests with S3 passing
- [ ] Integration tests with Azure passing
- [ ] Code review completed
- [ ] Documentation updated
- [ ] PR created and merged

**Phase 1 Deliverables**:
- ✅ 2 new utility classes
- ✅ ~525 lines of duplicate code removed
- ✅ 8+ files simplified
- ✅ 100% test coverage for new utilities
- ✅ Zero breaking changes

---

## Phase 2: Medium Impact Refactoring (Week 2)

**Duration**: 7-9 hours
**Expected Impact**: ~342 lines removed, 7+ files simplified
**Branch**: `refactor/phase2-medium-impact`

---

### Task 2.1: Create SplitMetadataFactory Utility

**Duration**: 3-4 hours
**Priority**: MEDIUM
**Lines Saved**: ~140 lines (70% reduction)

#### Step 1: Create Factory Class

**File**: `src/main/scala/io/indextables/spark/util/SplitMetadataFactory.scala`

```scala
package io.indextables.spark.util

import com.tantivy4java.quickwit.QuickwitSplit
import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

import java.net.URI

/**
 * Factory for creating QuickwitSplit.SplitMetadata from AddAction transaction log entries.
 *
 * Handles footer offset extraction with fallback logic for splits that don't have
 * pre-computed offsets stored in the transaction log.
 */
object SplitMetadataFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates SplitMetadata from an AddAction.
   *
   * @param addAction The transaction log add action
   * @param tablePath The base table path (for relative split path resolution)
   * @return QuickwitSplit.SplitMetadata instance
   */
  def fromAddAction(
    addAction: AddAction,
    tablePath: String
  ): QuickwitSplit.SplitMetadata = {

    val splitId = extractSplitId(addAction.path)

    // Extract or compute footer offsets
    val (footerStartOffset, footerEndOffset) = extractFooterOffsets(addAction, tablePath)

    new QuickwitSplit.SplitMetadata(
      splitId,
      "tantivy4spark-index",  // index_uid
      0L,                       // time_range_start (not used)
      0L,                       // time_range_end (not used)
      null,                     // tags (not used)
      footerStartOffset,
      footerEndOffset,
      addAction.size.getOrElse(0L),
      addAction.size.getOrElse(0L),  // uncompressed_docs_size_bytes
      addAction.numRecords.getOrElse(0L),
      addAction.numRecords.getOrElse(0L),  // num_docs
      0,                        // num_merge_ops (not used)
      null                      // delete_opstamp_range (not used)
    )
  }

  /**
   * Extracts split ID from split path.
   *
   * @param splitPath Full or relative split path
   * @return Split ID (filename without .split extension)
   */
  private def extractSplitId(splitPath: String): String = {
    splitPath.split("/").last.replace(".split", "")
  }

  /**
   * Extracts footer offsets from AddAction, with fallback to file-based extraction.
   *
   * @param addAction The add action containing split metadata
   * @param tablePath The table base path for constructing full split URL
   * @return Tuple of (footerStartOffset, footerEndOffset)
   */
  private def extractFooterOffsets(
    addAction: AddAction,
    tablePath: String
  ): (Long, Long) = {

    // Try to use pre-computed offsets from transaction log
    if (addAction.footerStartOffset.isDefined && addAction.footerEndOffset.isDefined) {
      val start = addAction.footerStartOffset.get
      val end = addAction.footerEndOffset.get
      logger.debug(s"Using pre-computed footer offsets for ${addAction.path}: $start-$end")
      return (start, end)
    }

    // Fallback: Extract from split file
    logger.debug(s"Footer offsets not in transaction log for ${addAction.path}, extracting from file")

    try {
      val fullSplitPath = if (addAction.path.contains("://")) {
        addAction.path
      } else {
        s"$tablePath/${addAction.path}"
      }

      val splitUri = new URI(fullSplitPath)
      val quickwitSplit = new QuickwitSplit(splitUri)
      val metadata = quickwitSplit.splitMetadata()

      val start = metadata.footerOffsetsRange().start
      val end = metadata.footerOffsetsRange().end

      logger.debug(s"Extracted footer offsets from file for ${addAction.path}: $start-$end")
      (start, end)

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to extract footer offsets for ${addAction.path}, using defaults", e)
        (0L, 0L)  // Default fallback
    }
  }

  /**
   * Batch creates SplitMetadata from multiple AddActions.
   *
   * @param addActions Sequence of add actions
   * @param tablePath The base table path
   * @return Sequence of SplitMetadata instances
   */
  def fromAddActions(
    addActions: Seq[AddAction],
    tablePath: String
  ): Seq[QuickwitSplit.SplitMetadata] = {
    addActions.map(action => fromAddAction(action, tablePath))
  }
}
```

#### Step 2: Create Unit Tests

**File**: `src/test/scala/io/indextables/spark/util/SplitMetadataFactoryTest.scala`

```scala
package io.indextables.spark.util

import io.indextables.spark.transaction.AddAction
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SplitMetadataFactoryTest extends AnyFunSuite with Matchers {

  test("fromAddAction should create metadata with pre-computed offsets") {
    val addAction = AddAction(
      path = "partition1/split-12345.split",
      size = Some(1000L),
      numRecords = Some(100L),
      footerStartOffset = Some(800L),
      footerEndOffset = Some(1000L),
      partitionValues = Map.empty,
      minValues = Map.empty,
      maxValues = Map.empty
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    metadata.splitId() shouldBe "split-12345"
    metadata.footerOffsetsRange().start shouldBe 800L
    metadata.footerOffsetsRange().end shouldBe 1000L
    metadata.numBytes() shouldBe 1000L
    metadata.numDocs() shouldBe 100L
  }

  test("fromAddAction should extract split ID correctly") {
    val addAction = AddAction(
      path = "nested/path/to/split-67890.split",
      size = Some(500L),
      numRecords = Some(50L),
      footerStartOffset = Some(400L),
      footerEndOffset = Some(500L),
      partitionValues = Map.empty,
      minValues = Map.empty,
      maxValues = Map.empty
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    metadata.splitId() shouldBe "split-67890"
  }

  test("fromAddActions should create metadata for multiple actions") {
    val actions = Seq(
      AddAction(
        path = "split-1.split",
        size = Some(1000L),
        numRecords = Some(100L),
        footerStartOffset = Some(800L),
        footerEndOffset = Some(1000L),
        partitionValues = Map.empty,
        minValues = Map.empty,
        maxValues = Map.empty
      ),
      AddAction(
        path = "split-2.split",
        size = Some(2000L),
        numRecords = Some(200L),
        footerStartOffset = Some(1600L),
        footerEndOffset = Some(2000L),
        partitionValues = Map.empty,
        minValues = Map.empty,
        maxValues = Map.empty
      )
    )

    val metadataList = SplitMetadataFactory.fromAddActions(actions, "s3://bucket/table")

    metadataList should have size 2
    metadataList(0).splitId() shouldBe "split-1"
    metadataList(1).splitId() shouldBe "split-2"
  }
}
```

#### Step 3: Update All Files

**Files to Update** (4 files):

1. `src/main/scala/io/indextables/spark/core/IndexTables4SparkGroupByAggregateScan.scala:814-866`
2. `src/main/scala/io/indextables/spark/core/IndexTables4SparkSimpleAggregateScan.scala`
3. `src/main/scala/io/indextables/spark/core/IndexTables4SparkPartitions.scala`
4. `src/main/scala/io/indextables/spark/core/IndexTables4SparkDataSource.scala`

Replace private `createSplitMetadataFromSplit()` methods with:
```scala
import io.indextables.spark.util.SplitMetadataFactory

val metadata = SplitMetadataFactory.fromAddAction(partition.split, partition.tablePath.toString)
```

#### Step 4: Validation

- [ ] Run `SplitMetadataFactoryTest` - all tests pass
- [ ] Run aggregate scan tests
- [ ] Run partition tests
- [ ] Verify split metadata creation in integration tests

**Acceptance Criteria**:
- ✅ All tests pass
- ✅ ~140 lines removed across 4 files
- ✅ Consistent metadata creation

---

### Task 2.2: Create CredentialResolver Utility

**Duration**: 3 hours
**Priority**: MEDIUM
**Lines Saved**: ~170 lines (74% reduction)

**Note**: This task is closely related to Task 1.2 (ConfigurationResolver) and may be merged or extracted from it.

#### Approach

If `ConfigurationResolver` from Task 1.2 handles credential resolution adequately, this task becomes:
1. Extract credential-specific methods to `CredentialResolver` wrapper
2. Add credential-specific features (masking, validation, fallbacks)
3. Update CloudStorageProvider to use the specialized resolver

Otherwise, follow similar pattern to ConfigurationResolver with credential-specific logic.

---

### Task 2.3: Create ExpressionUtils for Field Name Extraction

**Duration**: 1-2 hours
**Priority**: LOW-MEDIUM
**Lines Saved**: ~32 lines (67% reduction)

#### Step 1: Create Utility Class

**File**: `src/main/scala/io/indextables/spark/util/ExpressionUtils.scala`

```scala
package io.indextables.spark.util

import org.apache.spark.sql.connector.expressions.Expression
import org.slf4j.LoggerFactory

/**
 * Utility methods for working with Spark SQL expressions.
 */
object ExpressionUtils {

  private val logger = LoggerFactory.getLogger(getClass)
  private val fieldReferencePattern = """FieldReference\(([^)]+)\)""".r

  /**
   * Extracts field name from a Spark SQL expression.
   *
   * Currently supports FieldReference expressions. Returns "unknown_field" for
   * unsupported expression types.
   *
   * @param expression The Spark SQL expression
   * @return Extracted field name, or "unknown_field" if extraction fails
   */
  def extractFieldName(expression: Expression): String = {
    val exprStr = expression.toString

    if (exprStr.startsWith("FieldReference(")) {
      fieldReferencePattern.findFirstMatchIn(exprStr) match {
        case Some(m) =>
          val fieldName = m.group(1)
          logger.debug(s"Extracted field name from expression: $fieldName")
          fieldName
        case None =>
          logger.warn(s"Could not extract field name from FieldReference expression: $expression")
          "unknown_field"
      }
    } else {
      logger.warn(s"Unsupported expression type for field extraction: $expression")
      "unknown_field"
    }
  }

  /**
   * Extracts field names from multiple expressions.
   *
   * @param expressions Sequence of Spark SQL expressions
   * @return Sequence of extracted field names
   */
  def extractFieldNames(expressions: Seq[Expression]): Seq[String] = {
    expressions.map(extractFieldName)
  }
}
```

#### Step 2: Create Unit Tests

**File**: `src/test/scala/io/indextables/spark/util/ExpressionUtilsTest.scala`

#### Step 3: Update Files

**Files to Update** (3 files):
1. `src/main/scala/io/indextables/spark/core/IndexTables4SparkSimpleAggregateScan.scala:184-200`
2. `src/main/scala/io/indextables/spark/core/IndexTables4SparkGroupByAggregateScan.scala:869-885`
3. `src/main/scala/io/indextables/spark/core/IndexTables4SparkScanBuilder.scala`

#### Step 4: Validation

- [ ] Unit tests pass
- [ ] Aggregate scan tests pass
- [ ] Field names extracted correctly

---

### Phase 2 Completion Checklist

- [ ] Task 2.1: SplitMetadataFactory completed
- [ ] Task 2.2: CredentialResolver completed (or merged with Task 1.2)
- [ ] Task 2.3: ExpressionUtils completed
- [ ] All 228+ tests passing
- [ ] Integration tests passing
- [ ] Code review completed
- [ ] Documentation updated
- [ ] PR created and merged

**Phase 2 Deliverables**:
- ✅ 3 new utility classes (or 2 if merged)
- ✅ ~342 lines of duplicate code removed
- ✅ 7+ files simplified
- ✅ Zero breaking changes

---

## Phase 3: Architectural Refactoring (Week 3-4)

**Duration**: 6-8 hours
**Expected Impact**: ~200 lines removed, improved architecture
**Branch**: `refactor/phase3-architectural`

---

### Task 3.1: Create BaseTransactionLog Abstract Class

**Duration**: 6-8 hours
**Priority**: MEDIUM
**Lines Saved**: ~200 lines (33% reduction)

#### Step 1: Analyze Commonalities

**Common Operations**:
- Version management
- Schema validation
- Protocol checking
- File path construction
- Error handling patterns

**Differences**:
- `TransactionLog`: Simple sequential implementation
- `OptimizedTransactionLog`: Caching, parallelization, async operations

#### Step 2: Design Abstract Base Class

**File**: `src/main/scala/io/indextables/spark/transaction/BaseTransactionLog.scala`

```scala
package io.indextables.spark.transaction

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Abstract base class for transaction log implementations.
 *
 * Provides common functionality for version management, schema validation,
 * and protocol checking that is shared across all transaction log implementations.
 */
abstract class BaseTransactionLog(
  val tablePath: String,
  val hadoopConf: Configuration
) extends Serializable {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val transactionLogPath = s"$tablePath/_transaction_log"

  /**
   * Initializes the transaction log with a schema.
   * Must be implemented by subclasses.
   */
  def initialize(schema: StructType): Unit

  /**
   * Adds files to the transaction log.
   * Must be implemented by subclasses.
   */
  def addFiles(
    files: Seq[AddAction],
    commitId: Option[String] = None
  ): Long

  /**
   * Overwrites the transaction log by removing all files and adding new ones.
   * Must be implemented by subclasses.
   */
  def overwriteFiles(
    files: Seq[AddAction],
    commitId: Option[String] = None
  ): Long

  /**
   * Lists all visible files in the transaction log.
   * Must be implemented by subclasses.
   */
  def listFiles(): Seq[AddAction]

  /**
   * Gets the current transaction log version.
   * Must be implemented by subclasses.
   */
  def currentVersion(): Long

  /**
   * Common schema validation logic.
   *
   * @param schema Schema to validate
   * @throws IllegalArgumentException if schema contains unsupported types
   */
  protected def validateSchema(schema: StructType): Unit = {
    import org.apache.spark.sql.types._

    schema.fields.foreach { field =>
      field.dataType match {
        case _: StringType | _: IntegerType | _: LongType | _: DoubleType |
             _: FloatType | _: BooleanType | _: DateType | _: TimestampType |
             _: BinaryType =>
          // Supported types
        case _: ArrayType =>
          throw new IllegalArgumentException(
            s"Array types are not supported: ${field.name}"
          )
        case _: MapType =>
          throw new IllegalArgumentException(
            s"Map types are not supported: ${field.name}"
          )
        case _: StructType =>
          throw new IllegalArgumentException(
            s"Struct types are not supported: ${field.name}"
          )
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported type for field ${field.name}: ${other}"
          )
      }
    }

    logger.info(s"Schema validated: ${schema.fields.length} fields")
  }

  /**
   * Common protocol validation logic.
   *
   * @param path Path to validate
   * @return true if path uses a supported protocol
   */
  protected def isSupportedProtocol(path: String): Boolean = {
    path.startsWith("s3://") ||
    path.startsWith("s3a://") ||
    path.startsWith("s3n://") ||
    path.startsWith("azure://") ||
    path.startsWith("wasb://") ||
    path.startsWith("wasbs://") ||
    path.startsWith("abfs://") ||
    path.startsWith("abfss://") ||
    path.startsWith("file://") ||
    path.startsWith("hdfs://") ||
    !path.contains("://")  // Relative paths
  }

  /**
   * Constructs version file path.
   *
   * @param version Version number
   * @return Full path to version file
   */
  protected def versionFilePath(version: Long): String = {
    f"$transactionLogPath/$version%020d.json"
  }

  /**
   * Constructs checkpoint file path.
   *
   * @param version Version number
   * @return Full path to checkpoint file
   */
  protected def checkpointFilePath(version: Long): String = {
    f"$transactionLogPath/$version%020d.checkpoint.json"
  }

  /**
   * Extracts version number from transaction log file name.
   *
   * @param fileName File name (e.g., "00000000000000000042.json")
   * @return Version number, or None if filename doesn't match pattern
   */
  protected def extractVersionFromFileName(fileName: String): Option[Long] = {
    try {
      if (fileName.endsWith(".json") && !fileName.contains("checkpoint")) {
        Some(fileName.replace(".json", "").toLong)
      } else {
        None
      }
    } catch {
      case _: NumberFormatException => None
    }
  }

  /**
   * Logs transaction operation.
   *
   * @param operation Operation name
   * @param version Version number
   * @param fileCount Number of files affected
   */
  protected def logOperation(
    operation: String,
    version: Long,
    fileCount: Int
  ): Unit = {
    logger.info(s"$operation: version=$version, files=$fileCount, path=$tablePath")
  }
}
```

#### Step 3: Refactor TransactionLog

Update `TransactionLog.scala` to extend `BaseTransactionLog`:

```scala
class TransactionLog(
  tablePath: String,
  hadoopConf: Configuration
) extends BaseTransactionLog(tablePath, hadoopConf) {

  // Remove duplicate code (schema validation, protocol checking, etc.)
  // Keep sequential implementation-specific code

  override def initialize(schema: StructType): Unit = {
    validateSchema(schema)  // Use base class method
    // ... rest of initialization
  }

  // ... other methods using base class utilities
}
```

#### Step 4: Refactor OptimizedTransactionLog

Update `OptimizedTransactionLog.scala` to extend `BaseTransactionLog`:

```scala
class OptimizedTransactionLog(
  tablePath: String,
  hadoopConf: Configuration
) extends BaseTransactionLog(tablePath, hadoopConf) {

  // Remove duplicate code
  // Keep optimization-specific code (caching, parallelization, etc.)

  override def initialize(schema: StructType): Unit = {
    validateSchema(schema)  // Use base class method
    // ... optimized initialization
  }

  // ... other methods using base class utilities
}
```

#### Step 5: Validation

- [ ] All transaction log tests pass
- [ ] Both implementations work correctly
- [ ] No behavior changes
- [ ] Performance maintained

**Acceptance Criteria**:
- ✅ ~200 lines removed
- ✅ Clear separation of concerns
- ✅ All tests passing

---

### Phase 3 Completion Checklist

- [ ] Task 3.1: BaseTransactionLog completed
- [ ] All 228+ tests passing
- [ ] Performance benchmarks unchanged
- [ ] Code review completed
- [ ] Documentation updated
- [ ] PR created and merged

**Phase 3 Deliverables**:
- ✅ 1 new abstract base class
- ✅ ~200 lines of duplicate code removed
- ✅ Improved architectural clarity
- ✅ Zero breaking changes

---

## Testing Strategy

### Unit Testing

**For Each New Utility Class**:
1. Test all public methods
2. Test edge cases (null, empty, invalid input)
3. Test error handling
4. Achieve 100% code coverage

**Test Files to Create**:
- `ProtocolNormalizerTest.scala`
- `ConfigurationResolverTest.scala`
- `SplitMetadataFactoryTest.scala`
- `ExpressionUtilsTest.scala`
- `BaseTransactionLogTest.scala` (if needed)

### Integration Testing

**After Each Phase**:
1. Run full test suite: `mvn test`
2. Run S3 integration tests
3. Run Azure integration tests
4. Run partitioned dataset tests
5. Run aggregate pushdown tests

**Validation Commands**:
```bash
# Full test suite
mvn clean test

# Specific test suite
mvn test -Dtest="ProtocolNormalizerTest"

# Integration tests with S3
mvn test -Dtest="*S3*"

# Integration tests with Azure
mvn test -Dtest="*Azure*"
```

### Regression Testing

**Before Merging Each Phase**:
1. Compare test results with baseline (228+ tests)
2. Run performance benchmarks
3. Verify no new warnings or errors
4. Check debug logs for consistency

### Performance Testing

**Key Metrics to Monitor**:
- Transaction log read times
- Write operation throughput
- Cache hit rates
- S3/Azure operation latency

**Benchmark Commands**:
```bash
# Transaction log performance
mvn test -Dtest="TransactionLogPerformanceTest"

# Overall performance regression test
mvn test -Dtest="*Performance*"
```

---

## Risk Management

### Identified Risks

| **Risk** | **Severity** | **Mitigation** |
|----------|-------------|----------------|
| Breaking existing functionality | HIGH | Comprehensive test suite, incremental changes |
| Performance regression | MEDIUM | Benchmark before/after, profile hotspots |
| Merge conflicts | MEDIUM | Small PRs, frequent rebasing, clear communication |
| Incomplete refactoring | LOW | Detailed checklist, code review |
| Test coverage gaps | LOW | 100% coverage for new utilities, verify existing tests |

### Risk Mitigation Strategies

#### 1. Breaking Functionality

**Mitigation**:
- Run full test suite after each change
- Compare behavior with debug logging
- Use feature branches for easy rollback
- Get code review before merging

#### 2. Performance Regression

**Mitigation**:
- Benchmark before/after each phase
- Profile new utility methods
- Compare transaction log performance
- Monitor cache hit rates

#### 3. Merge Conflicts

**Mitigation**:
- Create small, focused PRs
- Rebase frequently from main
- Communicate with team about changes
- Use clear commit messages

#### 4. Incomplete Refactoring

**Mitigation**:
- Follow detailed checklist for each task
- Search codebase for remaining duplicates
- Code review with focus on completeness
- Update documentation

---

## Rollback Plan

### Phase-Level Rollback

**If Issues Arise in a Phase**:
1. Identify the problematic commit
2. Revert to previous stable state
3. Analyze root cause
4. Fix issues
5. Resume refactoring

### Task-Level Rollback

**If a Specific Task Causes Issues**:
1. Revert only that task's changes
2. Keep other completed tasks
3. Fix and retry the problematic task

### Emergency Rollback

**If Critical Production Issue**:
1. Immediately revert entire feature branch
2. Merge hotfix to main
3. Investigate issue offline
4. Resume refactoring after fix

### Rollback Commands

```bash
# Revert last commit
git revert HEAD

# Revert specific commit
git revert <commit-hash>

# Reset branch to main
git reset --hard origin/main

# Cherry-pick working commits
git cherry-pick <commit-hash>
```

---

## Success Metrics

### Quantitative Metrics

| **Metric** | **Baseline** | **Target** | **Measurement** |
|-----------|-------------|-----------|----------------|
| Lines of Code | ~1,848 duplicates | ~781 remaining | Count duplicate lines |
| Number of Files with Duplicates | 20+ files | < 5 files | File analysis |
| Test Coverage (New Utilities) | N/A | 100% | JaCoCo report |
| Test Pass Rate | 228+ tests | 228+ tests | Maven test output |
| Build Time | Baseline | ≤ Baseline + 5% | Maven build logs |
| Transaction Log Read Time | Baseline | ≤ Baseline | Performance tests |

### Qualitative Metrics

| **Metric** | **Success Criteria** |
|-----------|---------------------|
| Code Maintainability | Reduced cyclomatic complexity in affected files |
| Code Readability | Positive code review feedback on clarity |
| Developer Experience | Faster onboarding for new team members |
| Bug Surface Area | Fewer potential locations for bugs |
| Test Isolation | Utilities independently testable |

### Validation Checklist

**After Project Completion**:
- [ ] All 228+ tests passing
- [ ] No performance regressions
- [ ] ~1,067 lines of duplicate code removed
- [ ] 6 new utility classes created
- [ ] 100% test coverage for utilities
- [ ] Zero breaking changes
- [ ] Documentation updated
- [ ] Team training completed

---

## Timeline and Milestones

### Week 1: Phase 1 (High Impact)

| **Day** | **Task** | **Duration** | **Deliverable** |
|---------|----------|-------------|----------------|
| Mon | Task 1.1: ProtocolNormalizer | 3-4 hours | Utility + tests |
| Tue | Task 1.2: ConfigurationResolver (Part 1) | 4 hours | Design + partial implementation |
| Wed | Task 1.2: ConfigurationResolver (Part 2) | 2 hours | Complete + tests |
| Thu | Task 1.3: Cache Config Refactor | 2 hours | Refactored code + tests |
| Fri | Phase 1 validation + PR | 2 hours | Merged PR |

**Milestone**: Phase 1 Complete - ~525 lines removed

---

### Week 2: Phase 2 (Medium Impact)

| **Day** | **Task** | **Duration** | **Deliverable** |
|---------|----------|-------------|----------------|
| Mon | Task 2.1: SplitMetadataFactory | 3-4 hours | Utility + tests |
| Tue | Task 2.2: CredentialResolver | 3 hours | Utility + tests |
| Wed | Task 2.3: ExpressionUtils | 2 hours | Utility + tests |
| Thu | Phase 2 validation | 2 hours | All tests passing |
| Fri | Code review + PR | 2 hours | Merged PR |

**Milestone**: Phase 2 Complete - ~342 additional lines removed

---

### Week 3-4: Phase 3 (Architectural)

| **Week** | **Task** | **Duration** | **Deliverable** |
|----------|----------|-------------|----------------|
| Week 3 | Task 3.1: BaseTransactionLog design | 2 hours | Design document |
| Week 3 | Task 3.1: Implementation | 4 hours | Abstract base class |
| Week 3 | Task 3.1: Refactor subclasses | 3 hours | Updated implementations |
| Week 4 | Phase 3 validation | 2 hours | All tests passing |
| Week 4 | Final code review + PR | 2 hours | Merged PR |

**Milestone**: Phase 3 Complete - ~200 additional lines removed

---

### Project Completion

**Total Duration**: 3-4 weeks (22-29 hours)
**Total Impact**: ~1,067 lines removed (58% reduction)

---

## Communication Plan

### Stakeholder Updates

**Weekly Status Report**:
- Progress summary
- Metrics achieved
- Risks identified
- Next week's plan

**Daily Standup**:
- Yesterday's accomplishments
- Today's goals
- Blockers

### Code Review Process

**Pull Request Template**:
```markdown
## Summary
Brief description of changes

## Phase
Phase 1 / Phase 2 / Phase 3

## Changes
- List of files modified
- List of utility classes created
- Lines of duplicate code removed

## Testing
- Unit tests: X/X passing
- Integration tests: X/X passing
- Regression tests: ✅ No regressions

## Performance
- Benchmarks: ✅ No degradation
- Build time: ✅ Within acceptable range

## Checklist
- [ ] All tests passing
- [ ] Code reviewed
- [ ] Documentation updated
- [ ] No breaking changes
```

### Team Training

**After Each Phase**:
- Demo new utility classes
- Show usage examples
- Answer questions
- Update team wiki

---

## Post-Refactoring Actions

### Documentation Updates

**Files to Update**:
- [ ] `CLAUDE.md` - Add utility classes section
- [ ] `README.md` - Update architecture section
- [ ] Javadoc/Scaladoc - Comprehensive API docs
- [ ] Team wiki - New utilities guide

### Knowledge Sharing

**Training Sessions**:
1. Utility classes overview
2. Configuration resolution patterns
3. Best practices for future development

### Continuous Improvement

**Follow-up Tasks**:
- Monitor for new duplication patterns
- Establish code review guidelines
- Add linting rules for duplication detection
- Schedule quarterly refactoring reviews

---

## Appendix

### A. File Modification Summary

**Phase 1 Files**:
- CREATE: `ProtocolNormalizer.scala`, `ConfigurationResolver.scala`
- MODIFY: 8+ files using protocol normalization
- MODIFY: `CloudStorageProvider.scala` (major refactor)
- MODIFY: `IndexTables4SparkPartitions.scala` (cache config)

**Phase 2 Files**:
- CREATE: `SplitMetadataFactory.scala`, `ExpressionUtils.scala`, `CredentialResolver.scala`
- MODIFY: 4 files using SplitMetadata creation
- MODIFY: 3 files using field name extraction

**Phase 3 Files**:
- CREATE: `BaseTransactionLog.scala`
- MODIFY: `TransactionLog.scala`, `OptimizedTransactionLog.scala`

### B. Testing Checklist

**Per-Task Testing**:
- [ ] Unit tests written and passing
- [ ] Integration tests passing
- [ ] Edge cases covered
- [ ] Error handling tested

**Per-Phase Testing**:
- [ ] Full test suite passing (228+ tests)
- [ ] S3 integration tests passing
- [ ] Azure integration tests passing
- [ ] Performance benchmarks acceptable

**Final Testing**:
- [ ] End-to-end testing
- [ ] Load testing
- [ ] Security testing
- [ ] Documentation review

### C. Useful Commands

**Build & Test**:
```bash
# Clean build
mvn clean compile

# Run all tests
mvn test

# Run specific test
mvn test -Dtest="ClassName"

# Run with coverage
mvn clean test jacoco:report

# Skip tests (build only)
mvn clean install -DskipTests
```

**Git Workflow**:
```bash
# Create feature branch
git checkout -b refactor/phase1-high-impact

# Commit with descriptive message
git commit -m "feat: add ProtocolNormalizer utility"

# Push and create PR
git push origin refactor/phase1-high-impact

# Rebase from main
git rebase origin/main
```

**Code Analysis**:
```bash
# Find duplicate code
grep -r "normalizeProtocol" src/

# Count lines in file
wc -l src/main/scala/path/to/File.scala

# Search for TODO comments
grep -r "TODO" src/
```

---

**Plan Created**: 2025-10-27
**Plan Owner**: Development Team
**Review Schedule**: Weekly
**Next Review**: End of Phase 1
