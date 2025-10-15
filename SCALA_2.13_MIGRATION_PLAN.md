# Scala 2.12 to 2.13 Upgrade Plan for IndexTables4Spark

## Overview
Upgrade from Scala 2.12.20 to Scala 2.13.15 for compatibility with Spark 3.5.3 compiled with Scala 2.13, using pure Scala 2.13 APIs without backward compatibility layers.

---

## Phase 1: Update Build Configuration (pom.xml)

### 1.1 Update Scala Version Properties
- Change `scala.version` from `2.12.20` to `2.13.15`
- Change `scala.compat.version` from `2.12` to `2.13`

### 1.2 Update Core Dependencies
**Spark Dependencies** (keep as `provided` scope):
- `spark-core_2.13` version `3.5.3`
- `spark-sql_2.13` version `3.5.3`
- `spark-catalyst_2.13` version `3.5.3`

**Jackson Dependencies** (align with Spark 3.5.3):
- `jackson-databind`: `2.15.2`
- `jackson-module-scala_2.13`: `2.15.2`

**Testing Dependencies**:
- `scalatest_2.13`: `3.2.19` (latest stable for Scala 2.13)
- `scalatestplus-mockito-3-4_2.13`: `3.2.10.0`

### 1.3 Update Test-Scope Dependencies
- `s3mock_2.13`: `0.2.6` (confirmed available for Scala 2.13)
- `spark-sql_2.13` (tests classifier): `3.5.3`
- `spark-core_2.13` (tests classifier): `3.5.3`

### 1.4 Update Build Plugin Configuration
**scala-maven-plugin**:
- Update compiler args: Remove `-Ywarn-unused:imports`, add `-Wunused:imports` (Scala 2.13 syntax)
- Consider adding `-Xlint:deprecation` for migration warnings

**scoverage-maven-plugin**:
- Update to version `2.0.11` or later for Scala 2.13 support

---

## Phase 2: Code Migration - Import Statements

### 2.1 Replace JavaConverters (17 files affected)
**Search pattern**: `scala.collection.JavaConverters`

**Replace with**: `scala.jdk.CollectionConverters`

**Affected files**:
- All files in src/main/scala/io/indextables/spark/core/
- All files in src/test/scala/io/indextables/spark/
- src/main/scala/io/indextables/spark/util/ConfigNormalization.scala

**Migration**:
```scala
// OLD (Scala 2.12)
import scala.collection.JavaConverters._

// NEW (Scala 2.13)
import scala.jdk.CollectionConverters._
```

**Note**: One file (`ParallelTransactionLogOperations.scala`) already uses the correct import.

### 2.2 Verify Collection Imports
Check for deprecated imports:
- `scala.collection.Traversable` → `scala.collection.Iterable`
- `scala.collection.TraversableOnce` → `scala.collection.IterableOnce`

---

## Phase 3: Code Migration - Collection API Changes

### 3.1 Replace `breakOut` Usage (1 file)
**File**: `src/main/scala/io/indextables/spark/transaction/ParallelTransactionLogOperations.scala`

**Migration strategy**:
```scala
// OLD (Scala 2.12)
val result: Map[K, V] = list.map(f)(collection.breakOut)

// NEW (Scala 2.13)
val result: Map[K, V] = list.view.map(f).to(Map)
```

### 3.2 Update `to[Collection]` to `to(Collection)`
**Search pattern**: `.to\[` (though grep found 0 occurrences)

If found, update:
```scala
// OLD (Scala 2.12)
list.to[Set]

// NEW (Scala 2.13)
list.to(Set)
```

### 3.3 Review `mapValues` and `filterKeys` Usage
These now return `MapView` instead of `Map`:
```scala
// If strict Map is needed, add .toMap
val result = map.mapValues(f).toMap  // Scala 2.13
```

### 3.4 Check for Stream Usage
Replace deprecated `Stream` with `LazyList` if found:
```scala
// OLD
Stream.continually(...)

// NEW
LazyList.continually(...)
```

---

## Phase 4: Compilation and Error Resolution

### 4.1 Initial Compilation
```bash
mvn clean compile
```

### 4.2 Expected Compilation Issues
- **Implicit conversions**: May need explicit `.asScala` or `.asJava` calls
- **Type inference**: Scala 2.13 has stricter type inference; may need explicit type annotations
- **Varargs**: Type changes from `Seq` to `immutable.Seq`

### 4.3 Common Fixes
**Issue**: Mutable/Immutable collection ambiguity
```scala
// Solution: Use explicit imports
import scala.collection.immutable.Seq
import scala.collection.mutable.{Map => MutableMap}
```

**Issue**: Missing implicit conversions
```scala
// Solution: Use explicit conversion
javaList.asScala  // instead of implicit conversion
```

### 4.4 Compiler Flag Validation
Ensure NO backward compatibility flags are used:
- ❌ Do NOT use `-Xsource:2.12`
- ❌ Do NOT use `-Xmigration`
- ✅ Use `-Xlint:deprecation` to catch issues

---

## Phase 5: Dependency Compatibility Verification

### 5.1 Third-Party Dependencies to Check
**tantivy4java**:
- Current version: `0.24.1`
- Verify Scala 2.13 compatibility (likely Scala-agnostic as it's Java-based)

**AWS SDK**:
- Current: `2.34.8`
- No Scala version dependency (Java library)

**ANTLR**:
- Current: `4.9.3`
- Update to `4.13.2` (latest stable)

**Databricks SDK**:
- Current: `0.65.0`
- Verify Scala 2.13 compatibility

### 5.2 S3Mock Dependency
**Status**: ✅ CONFIRMED AVAILABLE
- `s3mock_2.13` version `0.2.6` available on Maven Central

---

## Phase 6: Testing Strategy

### 6.1 Compilation Test
```bash
mvn clean compile -DskipTests
```

### 6.2 Test Compilation
```bash
mvn test-compile
```

### 6.3 Code Quality Checks
```bash
# Scalafmt check
mvn spotless:check

# Apply formatting if needed
mvn spotless:apply
```

---

## Phase 7: Documentation Updates

### 7.1 Update README/CLAUDE.md
- Update build instructions to specify Scala 2.13
- Update dependency version references
- Note Scala 2.13 requirement

### 7.2 Update Version Artifact ID
Consider updating artifact version:
- From: `0.3.0_spark_3.5.3` (implied 2.12)
- To: `0.3.0_spark_3.5.3_scala_2.13` (explicit 2.13)

---

## Phase 8: Risk Mitigation

### 8.1 Known Risks
1. ~~**S3Mock test dependency**: May not have 2.13 version~~ ✅ RESOLVED - version available
2. **Binary compatibility**: Downstream users must recompile
3. **Collection semantics**: Subtle behavioral changes in edge cases

### 8.2 Mitigation Strategies
1. ~~**S3Mock**: Have alternative test framework ready~~ ✅ NOT NEEDED
2. **Documentation**: Clearly communicate breaking change
3. **Testing**: Run full test suite after migration

---

## Phase 9: Migration Checklist

### Pre-Migration
- [x] Create feature branch for migration
- [ ] Document current test pass rate (baseline)
- [ ] Backup current working configuration

### Core Migration
- [x] Update pom.xml Scala properties (2.13.15)
- [x] Update all _2.12 dependencies to _2.13
- [x] Update Jackson versions to 2.15.2
- [x] Update ScalaTest to 3.2.19
- [x] Update ANTLR to 4.13.2
- [x] Update s3mock to _2.13 version
- [x] Replace all JavaConverters imports (17 files) - 120 occurrences updated
- [x] Fix breakOut usage (1 file) - 4 occurrences fixed
- [x] Update compiler flags in pom.xml
- [x] Fix Java collection conversion in SplitSearchEngine.scala
- [x] Remove parallel collections (.par) from MergeSplitsCommand.scala
- [x] Fix Future.sequence execution context parameters (4 occurrences)
- [x] Fix ArrayBuffer to Seq type mismatches

### Validation
- [x] Run `mvn clean compile` - **SUCCESS** ✅
- [ ] Run `mvn test-compile` - verify success
- [x] Check for deprecation warnings - 100 warnings (acceptable, mostly unused imports)
- [x] Verify no `-Xsource:2.12` flags remain - confirmed

### Final Steps
- [ ] Update documentation
- [ ] Update artifact version
- [ ] Create migration notes for users

---

## Expected Outcome

**Pure Scala 2.13 codebase** compatible with Spark 3.5.3 compiled with Scala 2.13, with:
- ✅ All code using Scala 2.13 APIs
- ✅ No backward compatibility shims
- ✅ Clean compilation (test execution separate)
- ✅ Updated dependencies aligned with Spark 3.5.3
- ✅ Documentation updated for Scala 2.13 requirement

**Note**: Test execution will be performed separately after successful compilation.

---

## Progress Tracking

### Completed Steps
- ✅ Phase 1.1: Updated Scala version properties (2.13.15)
- ✅ Phase 1.2: Updated Jackson to 2.15.2
- ✅ Phase 1.3: Updated ScalaTest to 3.2.19
- ✅ Phase 1.3: Updated ANTLR to 4.13.2
- ✅ Phase 1.3: Updated s3mock to _2.13 version

### In Progress
- 🔄 Phase 1.4: Update build plugin configurations
- 🔄 Phase 2.1: Replace JavaConverters imports

### Pending
- ⏳ Phase 7: Documentation updates

---

## Migration Summary (Completion Status: ✅ SUCCESS)

### Code Changes Made

#### 1. Import Statement Migration
- **Files affected**: 66 files
- **Occurrences changed**: 120 instances
- **Change**: `scala.collection.JavaConverters._` → `scala.jdk.CollectionConverters._`

#### 2. Collection API Updates
- **File**: ParallelTransactionLogOperations.scala
  - Removed 4 occurrences of `collection.breakOut` from `Future.sequence` calls
  - Added implicit execution context declarations for proper Scala 2.13 syntax

#### 3. Java Collection Conversion
- **File**: SplitSearchEngine.scala
  - Fixed type conversion for `docBatch()` return value
  - Added explicit `java.util.List` casting and `.toSeq` conversion
  - Solution: `asScala(javaList).toSeq` to ensure proper type inference

#### 4. Parallel Collections Removal
- **File**: MergeSplitsCommand.scala
  - Removed `.par` parallel collection usage (moved to separate library in Scala 2.13)
  - Simplified to sequential processing (parallelization handled by Spark RDD)
  - Removed ForkJoinTaskSupport and related cleanup code

#### 5. Type Inference Fixes
- **File**: MergeSplitsCommand.scala
  - Added `.toSeq` conversions for `ArrayBuffer` types
  - Fixed type mismatches in `commitMergeSplits` calls

### Compilation Results
- **Status**: ✅ BUILD SUCCESS
- **Warnings**: 100 warnings (mostly unused imports - acceptable)
- **Errors**: 0 errors
- **Pure Scala 2.13**: No backward compatibility flags used

### Key Technical Decisions
1. **No external dependencies added**: Avoided scala-parallel-collections library
2. **Explicit type conversions**: Used explicit `.toSeq` conversions for clarity
3. **Implicit execution contexts**: Used local implicit declarations for Future.sequence
4. **Sequential batch processing**: Leveraged Spark's RDD parallelization instead of parallel collections

### Next Steps
1. Run test compilation: `mvn test-compile`
2. Execute test suite (separate from this migration)
3. Update project documentation for Scala 2.13 requirement
