# Cross-Reference Indexing Design Document

**Version:** 1.0
**Date:** December 2024
**Status:** Draft

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture](#3-architecture)
4. [Transaction Log Changes](#4-transaction-log-changes)
5. [XRef Split Lifecycle](#5-xref-split-lifecycle)
6. [SQL Commands](#6-sql-commands)
7. [Query-Time Integration](#7-query-time-integration)
8. [Configuration](#8-configuration)
9. [Implementation Plan](#9-implementation-plan)
10. [Testing Strategy](#10-testing-strategy)
11. [Migration & Compatibility](#11-migration--compatibility)

---

## 1. Overview

### 1.1 Problem Statement

Large IndexTables4Spark tables with hundreds or thousands of splits suffer from query performance issues:
- Every split must be evaluated for potential matches even when the query is highly selective
- Data skipping via min/max statistics helps but is limited to simple range queries
- For rare term searches, most splits are opened only to find zero matches

### 1.2 Solution: Cross-Reference (XRef) Splits

XRef splits leverage tantivy4java's cross-reference capability to consolidate term dictionaries from N source splits into a single lightweight index. Each source split becomes one document in the XRef split, enabling:

- **10-100x faster split identification** for selective queries
- **Sub-millisecond query routing** - searching N documents vs opening N splits
- **Footer-only downloads** - source splits accessed using only footer data during XRef build
- **Bounded size** - XRef with 1,000 splits ≈ 15-70 MB

### 1.3 Example

```
Source splits (1,000 files, 50GB total):
  split-001.split: terms ["error", "warning", "api"]
  split-002.split: terms ["error", "debug"]
  split-003.split: terms ["warning", "debug"]
  ...

XRef split (1 file, ~30MB):
  doc_0: represents split-001, indexed terms ["error", "warning", "api"]
  doc_1: represents split-002, indexed terms ["error", "debug"]
  doc_2: represents split-003, indexed terms ["warning", "debug"]
  ...

Query: "error AND critical"
  Without XRef: Open 1,000 splits → find 15 matches → 3-10 seconds
  With XRef:    Open 1 XRef → identify 15 splits → Open 15 splits → 100-500ms
```

---

## 2. Goals & Non-Goals

### 2.1 Goals

1. **Dramatically reduce split scanning** for selective queries on large tables
2. **Automatic XRef maintenance** - create/update XRef on transaction commit (configurable)
3. **Manual XRef control** via SQL command for explicit indexing
4. **Seamless query integration** - transparent to users, no query syntax changes
5. **Partition-agnostic** - XRef splits can span multiple partitions, avoiding fragmentation
6. **Compatible with existing features** - works with MERGE SPLITS, PURGE, etc.
7. **Cloud storage efficient** - minimize S3/Azure requests during build and query
8. **Easily disableable** - query-time configuration to bypass XRef if needed

### 2.2 Non-Goals

1. **Real-time updates** - XRef is rebuilt periodically, not on every write
2. **Range query optimization in XRef** - XRef transforms range queries to match-all (conservative)
3. **Phrase query support in XRef** - positions not stored by default

---

## 3. Architecture

### 3.1 Storage Layout

XRef splits are stored in a hash-based directory structure to avoid fragmentation when tables have many partitions (most partitions have < 1000 files). The directory name is derived from the XRef filename's hash code.

```
{table_root}/
├── _transaction_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── ...
├── _xrefsplits/                         # NEW: XRef storage directory
│   ├── aaaa/                            # Hash-based subdirectories (aaaa-zzzz)
│   │   └── xref-{uuid}.split
│   ├── abcd/
│   │   └── xref-{uuid}.split
│   ├── mxyz/
│   │   └── xref-{uuid}.split
│   └── zzzz/
│       └── xref-{uuid}.split
├── part-00000-{uuid}.split              # Data splits (existing)
├── partition_col=value/                 # Partitioned data (existing)
│   └── part-00000-{uuid}.split
└── ...
```

#### Hash Directory Derivation

The 4-character directory name is computed from the XRef filename:

```scala
object XRefStorageUtils {
  /**
   * Derive hash directory from XRef filename.
   * Maps filename hash to a 4-character string in range [aaaa, zzzz].
   */
  def getHashDirectory(xrefFileName: String): String = {
    val hash = Math.abs(xrefFileName.hashCode)
    // Map to 4 characters, each in range [a-z] (26 options)
    // Total buckets: 26^4 = 456,976
    val chars = Array.fill(4)('a')
    var remaining = hash
    for (i <- 3 to 0 by -1) {
      chars(i) = ('a' + (remaining % 26)).toChar
      remaining = remaining / 26
    }
    new String(chars)
  }

  /** Build full relative path for XRef file */
  def getXRefPath(xrefId: String): String = {
    val fileName = s"xref-$xrefId.split"
    val hashDir = getHashDirectory(fileName)
    s"_xrefsplits/$hashDir/$fileName"
  }
}

// Examples:
// xref-abc123.split  → _xrefsplits/kmpq/xref-abc123.split
// xref-def456.split  → _xrefsplits/wxyz/xref-def456.split
```

**Benefits of hash-based storage:**
- Avoids directory proliferation for tables with many partitions
- Even distribution across ~457K possible directories
- Simple, deterministic path computation
- No partition-specific directories to manage

### 3.2 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              SQL Layer                                   │
│  ┌──────────────────────┐  ┌────────────────────────────────────────┐   │
│  │ INDEX CROSSREFERENCES│  │          Query Planning                 │   │
│  │      Command         │  │  (XRef-aware split selection)          │   │
│  └──────────────────────┘  └────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                          Transaction Log                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────┐   │
│  │  AddAction   │  │ RemoveAction │  │  AddXRefAction (NEW)         │   │
│  │ (data splits)│  │              │  │  RemoveXRefAction (NEW)      │   │
│  └──────────────┘  └──────────────┘  └──────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                          XRef Manager                                    │
│  ┌──────────────────────┐  ┌────────────────────────────────────────┐   │
│  │  XRefBuildManager    │  │      XRefQueryRouter                   │   │
│  │  - Build/rebuild     │  │  - Select relevant XRefs               │   │
│  │  - Partition grouping│  │  - Query XRefs for split selection     │   │
│  │  - Transaction commit│  │  - Combine with data skipping          │   │
│  └──────────────────────┘  └────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                          tantivy4java                                    │
│  ┌──────────────────────┐  ┌────────────────────────────────────────┐   │
│  │    XRefSplit.build   │  │  XRefSearcher / SplitSearcher          │   │
│  │ (term consolidation) │  │  (query execution)                     │   │
│  └──────────────────────┘  └────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Key Classes

| Class | Responsibility |
|-------|---------------|
| `AddXRefAction` | Transaction log action for XRef creation |
| `RemoveXRefAction` | Transaction log action for XRef removal |
| `XRefBuildManager` | Orchestrates XRef build/rebuild operations |
| `XRefQueryRouter` | Determines which splits to scan using XRef |
| `IndexCrossReferencesCommand` | SQL command implementation |
| `IndexCrossReferencesExecutor` | Distributed XRef build execution |

---

## 4. Transaction Log Changes

### 4.1 New Action: AddXRefAction

```scala
case class AddXRefAction(
  // Core identification
  path: String,                              // e.g., "_xrefsplits/kmpq/xref-{uuid}.split"
  xrefId: String,                            // Unique XRef identifier

  // Source split references (partition-agnostic - can span multiple partitions)
  sourceSplitPaths: Seq[String],             // Paths of splits included in this XRef
  sourceSplitCount: Int,                     // Count for quick access

  // XRef metadata (from XRefMetadata)
  size: Long,                                // File size in bytes
  totalTerms: Long,                          // Total unique terms indexed
  footerStartOffset: Long,                   // For efficient opening
  footerEndOffset: Long,

  // Build metadata
  createdTime: Long,                         // Build timestamp
  buildDurationMs: Long,                     // How long build took

  // Configuration used
  maxSourceSplits: Int                       // Config value used for this build
) extends Action
```

**JSON representation:**
```json
{
  "addXRef": {
    "path": "_xrefsplits/kmpq/xref-abc123.split",
    "xrefId": "xref-abc123",
    "sourceSplitPaths": ["part-00000-def.split", "date=2024-01-15/part-00001-ghi.split"],
    "sourceSplitCount": 2,
    "size": 15728640,
    "totalTerms": 125000,
    "footerStartOffset": 15700000,
    "footerEndOffset": 15728640,
    "createdTime": 1702600000000,
    "buildDurationMs": 5432,
    "maxSourceSplits": 1024
  }
}
```

Note: `sourceSplitPaths` can include splits from multiple partitions - XRefs are partition-agnostic.

### 4.2 New Action: RemoveXRefAction

```scala
case class RemoveXRefAction(
  path: String,                              // Path of XRef to remove
  xrefId: String,                            // XRef identifier
  deletionTimestamp: Long,                   // When marked for removal
  reason: String                             // "replaced", "source_changed", "explicit"
) extends Action
```

**JSON representation:**
```json
{
  "removeXRef": {
    "path": "_xrefsplits/xref-abc123.split",
    "xrefId": "xref-abc123",
    "deletionTimestamp": 1702600500000,
    "reason": "replaced"
  }
}
```

### 4.3 Protocol Version Update

```scala
// New protocol version for XRef support
CURRENT_READER_VERSION = 3  // Up from 2
CURRENT_WRITER_VERSION = 3  // Up from 2

// New features
SUPPORTED_READER_FEATURES += "crossReferenceIndex"
SUPPORTED_WRITER_FEATURES += "crossReferenceIndex"
```

### 4.4 TransactionLog API Extensions

```scala
class TransactionLog {
  // Existing methods...

  // NEW: XRef-specific methods

  /** Add XRef to transaction log */
  def addXRef(xrefAction: AddXRefAction): Long

  /** Remove XRef from transaction log */
  def removeXRef(path: String, reason: String): Long

  /** Atomic XRef replacement (remove old + add new) */
  def replaceXRef(
    removeAction: RemoveXRefAction,
    addAction: AddXRefAction
  ): Long

  /** List all active XRefs (partition-agnostic) */
  def listXRefs(): Seq[AddXRefAction]

  /** Get XRef that covers a specific split (by filename) */
  def getXRefForSplit(splitPath: String): Option[AddXRefAction]

  /** Get all splits NOT covered by any XRef */
  def getSplitsWithoutXRef(): Seq[AddAction]
}
```

---

## 5. XRef Split Lifecycle

### 5.1 Creation Scenarios

#### Scenario A: Initial Build (No XRefs Exist)

```
State: 500 splits, no XRefs
Config: maxSourceSplits = 1024

Action: INDEX CROSSREFERENCES or auto-build on commit

Result:
  1. Group all 500 splits (within maxSourceSplits limit)
  2. Build single XRef containing all 500 splits
  3. Transaction log: AddXRefAction(sourceSplitCount=500)
```

#### Scenario B: Incremental Update (XRef Exists, New Splits Added)

```
State: XRef with 500 splits, 50 new splits added
Config: maxSourceSplits = 1024

Action: Auto-build on commit (if enabled)

Decision logic:
  - Current XRef has 500 splits (< maxSourceSplits)
  - Adding 50 would make 550 (< maxSourceSplits)
  - Rebuild XRef with all 550 splits

Result:
  1. RemoveXRefAction(old xref, reason="replaced")
  2. AddXRefAction(new xref with 550 splits)
```

#### Scenario C: Max Capacity (XRef at Limit)

```
State: XRef with 1024 splits (at maxSourceSplits), 50 new splits added
Config: maxSourceSplits = 1024

Action: Auto-build on commit

Decision logic:
  - Current XRef at capacity (1024)
  - 50 new splits cannot fit
  - Create new XRef for uncovered splits

Result:
  1. Keep existing XRef unchanged
  2. AddXRefAction(new xref with 50 splits)

Note: Now have 2 XRefs for this partition
```

#### Scenario D: Source Splits Removed (Compaction/Merge)

```
State: XRef references splits A,B,C,D,E; MERGE SPLITS combined B+C → BC
Config: rebuildOnSourceChange = true

Action: Auto-rebuild triggered by MERGE SPLITS

Decision logic:
  - XRef references B and C which no longer exist
  - Must rebuild with valid splits only

Result:
  1. RemoveXRefAction(old xref, reason="source_changed")
  2. AddXRefAction(new xref with A,BC,D,E)
```

### 5.2 Rebuild Algorithm

```scala
def evaluateXRefRebuild(
    existingXRefs: Seq[AddXRefAction],
    activeSplits: Seq[AddAction],
    config: XRefConfig
): XRefRebuildPlan = {

  // Step 1: Identify stale XRefs (reference non-existent splits)
  val activeSplitPaths = activeSplits.map(_.path).toSet
  val staleXRefs = existingXRefs.filter { xref =>
    xref.sourceSplitPaths.exists(!activeSplitPaths.contains(_))
  }

  // Step 2: Identify uncovered splits
  val coveredSplitPaths = existingXRefs
    .filterNot(staleXRefs.contains)
    .flatMap(_.sourceSplitPaths)
    .toSet
  val uncoveredSplits = activeSplits.filterNot(s => coveredSplitPaths.contains(s.path))

  // Step 3: Identify under-capacity XRefs that can absorb uncovered splits
  val underCapacityXRefs = existingXRefs
    .filterNot(staleXRefs.contains)
    .filter(_.sourceSplitCount < config.maxSourceSplits)

  // Step 4: Build plan
  XRefRebuildPlan(
    xrefsToRemove = staleXRefs,
    xrefsToRebuild = determineRebuilds(staleXRefs, activeSplits, config),
    newXrefsNeeded = determineNewXrefs(uncoveredSplits, underCapacityXRefs, config)
  )
}
```

### 5.3 XRef and Partition Handling

XRef splits are **partition-agnostic**. A single XRef can contain splits from multiple partitions - there is no relationship between XRef organization and table partitioning.

**Design rationale:**
- Avoids directory fragmentation for tables with many partitions
- Many partitions have < 1000 splits, so partition-wise XRefs would be wasteful
- Simplifies XRef management - no need to track partition→XRef relationships
- Hash-based storage provides even distribution

**Query-time behavior:**
- After partition pruning narrows candidate splits, the query router loads all active XRefs
- XRef query returns matching split filenames
- Filenames are matched against candidate splits (by filename, ignoring path)
- This allows XRef results to work regardless of which partitions the splits belong to

---

## 6. SQL Commands

### 6.1 INDEX CROSSREFERENCES Command

#### Syntax

```sql
INDEX CROSSREFERENCES FOR '<table_path>'
  [WHERE partition_predicates]
  [FORCE REBUILD]
  [DRY RUN]
```

#### Examples

```sql
-- Build/update XRefs for entire table
INDEX CROSSREFERENCES FOR 's3://bucket/my_table';

-- Build/update XRefs for specific partition
INDEX CROSSREFERENCES FOR 's3://bucket/my_table' WHERE date = '2024-01-15';

-- Force rebuild even if XRef exists and is valid
INDEX CROSSREFERENCES FOR 's3://bucket/my_table' FORCE REBUILD;

-- Preview what would be done
INDEX CROSSREFERENCES FOR 's3://bucket/my_table' DRY RUN;
```

#### Output Schema

```scala
case class IndexCrossReferencesResult(
  table_path: String,
  partition: String,                    // Empty for non-partitioned
  action: String,                       // "created", "rebuilt", "unchanged"
  xref_path: String,
  source_splits_count: Int,
  total_terms: Long,
  xref_size_bytes: Long,
  build_duration_ms: Long
)
```

#### Output Example

```
+----------------------+-----------------+---------+----------------------------------+--------------------+-------------+----------------+------------------+
|table_path            |partition        |action   |xref_path                         |source_splits_count |total_terms  |xref_size_bytes |build_duration_ms |
+----------------------+-----------------+---------+----------------------------------+--------------------+-------------+----------------+------------------+
|s3://bucket/my_table  |date=2024-01-15  |created  |_xrefsplits/date=2024.../xref-... |125                 |1250000      |31457280        |8543              |
|s3://bucket/my_table  |date=2024-01-16  |rebuilt  |_xrefsplits/date=2024.../xref-... |89                  |890000       |22020096        |5210              |
|s3://bucket/my_table  |date=2024-01-17  |unchanged|_xrefsplits/date=2024.../xref-... |156                 |1560000      |39321600        |0                 |
+----------------------+-----------------+---------+----------------------------------+--------------------+-------------+----------------+------------------+
```

### 6.2 ANTLR Grammar Additions

```antlr
// In IndexTables4SparkSqlBase.g4

indexCrossReferences
    : INDEX CROSSREFERENCES FOR tablePathOrName
      (WHERE predicate)?
      (FORCE REBUILD)?
      (DRY RUN)?
    ;

tablePathOrName
    : STRING                              // '/path/to/table' or 's3://bucket/table'
    | qualifiedName                       // catalog.schema.table
    ;

// New tokens
INDEX: 'INDEX';
CROSSREFERENCES: 'CROSSREFERENCES';
FORCE: 'FORCE';
REBUILD: 'REBUILD';
```

### 6.3 Command Implementation

```scala
case class IndexCrossReferencesCommand(
  tableIdentifier: Either[String, Seq[String]],  // Path or qualified name
  whereClause: Option[Expression],
  forceRebuild: Boolean,
  dryRun: Boolean
) extends RunnableCommand with UnaryNode {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tablePath = resolveTablePath(tableIdentifier, sparkSession)
    val transactionLog = TransactionLog.forPath(tablePath, sparkSession)

    // Validate WHERE clause references only partition columns
    whereClause.foreach { predicate =>
      validatePartitionPredicate(predicate, transactionLog.metadata)
    }

    // Execute via XRefBuildManager
    val results = XRefBuildManager.buildCrossReferences(
      tablePath = tablePath,
      transactionLog = transactionLog,
      whereClause = whereClause,
      forceRebuild = forceRebuild,
      dryRun = dryRun,
      sparkSession = sparkSession
    )

    results.map(_.toRow)
  }
}
```

### 6.4 DESCRIBE TRANSACTION LOG Integration

The existing `DESCRIBE INDEXTABLES TRANSACTION LOG` command must be updated to display XRef actions alongside regular split actions.

#### Updated Output Format

```sql
DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/my_table' INCLUDE ALL;
```

**Output with XRef actions:**
```
+--------+------------+-------------------------------------------+-------------+---------+----------------+
|version |action_type |path                                       |source_count |size     |partition       |
+--------+------------+-------------------------------------------+-------------+---------+----------------+
|15      |add         |part-00050-abc.split                       |null         |52428800 |date=2024-01-20 |
|15      |add         |part-00051-def.split                       |null         |48234567 |date=2024-01-20 |
|15      |addXRef     |_xrefsplits/kmpq/xref-xyz789.split         |550          |31457280 |null            |
|14      |removeXRef  |_xrefsplits/abcd/xref-abc123.split         |500          |null     |null            |
|13      |add         |part-00045-ghi.split                       |null         |61234567 |date=2024-01-19 |
|12      |remove      |part-00020-old.split                       |null         |null     |date=2024-01-15 |
+--------+------------+-------------------------------------------+-------------+---------+----------------+
```

#### XRef Summary View

```sql
DESCRIBE INDEXTABLES TRANSACTION LOG 's3://bucket/my_table' XREFS;
```

**Output:**
```
+-------------------------------------------+-------------+-----------+-------------+-----------------------+------------------+
|xref_path                                  |source_count |total_terms|size_bytes   |created_time           |status            |
+-------------------------------------------+-------------+-----------+-------------+-----------------------+------------------+
|_xrefsplits/kmpq/xref-xyz789.split         |550          |1250000    |31457280     |2024-01-20 10:30:00    |active            |
|_xrefsplits/wxyz/xref-def456.split         |200          |450000     |12582912     |2024-01-18 14:22:00    |active            |
|_xrefsplits/abcd/xref-abc123.split         |500          |1100000    |28311552     |2024-01-15 09:15:00    |removed           |
+-------------------------------------------+-------------+-----------+-------------+-----------------------+------------------+

XRef Coverage Summary:
  Active XRefs: 2
  Total splits covered: 750
  Total splits in table: 800
  Coverage: 93.75%
  Uncovered splits: 50
```

#### Implementation Changes

```scala
// In DescribeTransactionLogCommand.scala

case class DescribeTransactionLogCommand(
  tablePath: String,
  includeAll: Boolean,
  xrefsOnly: Boolean,           // NEW: show only XRef info
  limit: Option[Int]
) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val transactionLog = TransactionLog.forPath(tablePath, sparkSession)

    if (xrefsOnly) {
      // Return XRef summary view
      describeXRefs(transactionLog)
    } else {
      // Existing logic + XRef actions
      describeAllActions(transactionLog, includeAll)
    }
  }

  private def describeXRefs(transactionLog: TransactionLog): Seq[Row] = {
    val activeXRefs = transactionLog.listXRefs()
    val removedXRefs = transactionLog.listRemovedXRefs()  // NEW method
    val totalSplits = transactionLog.listFiles().size
    val coveredSplits = activeXRefs.flatMap(_.sourceSplitPaths).distinct.size

    // Build result rows
    val xrefRows = (activeXRefs.map(x => xrefToRow(x, "active")) ++
                   removedXRefs.map(x => xrefToRow(x, "removed")))

    // Add summary row
    val summaryRow = Row(
      s"Coverage: ${coveredSplits}/${totalSplits} (${(coveredSplits * 100.0 / totalSplits).formatted("%.2f")}%)"
    )

    xrefRows :+ summaryRow
  }
}
```

#### ANTLR Grammar Update

```antlr
describeTransactionLog
    : DESCRIBE INDEXTABLES TRANSACTION LOG tablePathOrName
      (INCLUDE ALL)?
      (XREFS)?                    // NEW: XRef-only view
      (LIMIT INTEGER_VALUE)?
    ;

// New token
XREFS: 'XREFS';
```

---

## 7. Query-Time Integration

### 7.1 Query Flow with XRef

```
User Query: SELECT * FROM table WHERE title = 'specific_value'

┌─────────────────────────────────────────────────────────────────────────┐
│ Step 1: Parse & Plan                                                     │
│   - Standard Spark planning                                              │
│   - Filter pushdown to DataSource                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 2: List Files (IndexTables4SparkScanBuilder.planFiles)             │
│   - Load all AddActions from transaction log                            │
│   - Apply partition pruning (if partitioned)                            │
│   - Apply data skipping via min/max stats                               │
│   Result: 500 candidate splits                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 3: XRef Pre-Scan (NEW - XRefQueryRouter)                           │
│   IF candidate splits > threshold (default 128):                        │
│     a. Load relevant XRefs from transaction log                         │
│     b. Convert Spark filter to SplitQuery                               │
│     c. Search XRefs for matching splits                                 │
│     d. Filter candidates to only XRef-matched + uncovered splits        │
│   Result: 45 candidate splits (90% reduction)                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 4: Create Scan Tasks                                               │
│   - Create InputPartition per remaining split                           │
│   - Distribute to executors via locality-aware scheduling               │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 5: Execute Scans (existing logic)                                  │
│   - Executors open splits and execute queries                           │
│   - Return matching documents                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 7.2 XRefQueryRouter Implementation

```scala
class XRefQueryRouter(
    transactionLog: TransactionLog,
    config: XRefQueryConfig,
    sparkSession: SparkSession
) {

  /**
   * Filter candidate splits using XRef pre-scan.
   *
   * @param candidateSplits Splits after partition pruning and data skipping
   * @param filters Spark filters to evaluate
   * @return Filtered splits that may contain matching documents
   */
  def filterSplitsWithXRef(
      candidateSplits: Seq[AddAction],
      filters: Seq[Expression]
  ): Seq[AddAction] = {

    // Check if XRef query is enabled
    if (!config.enabled) {
      return candidateSplits
    }

    // Skip XRef check if below threshold
    if (candidateSplits.size < config.minSplitsForXRef) {
      return candidateSplits
    }

    // Get candidate split filenames for filtering
    val candidateFileNames = candidateSplits.map(s => extractFileName(s.path)).toSet

    // Load all active XRefs and filter to only those covering candidate splits
    val allXRefs = transactionLog.listXRefs()
    val relevantXRefs = allXRefs.filter { xref =>
      xref.sourceSplitPaths.exists(p => candidateFileNames.contains(extractFileName(p)))
    }

    if (relevantXRefs.isEmpty) {
      return candidateSplits  // No relevant XRefs - return all candidates
    }

    // Convert Spark filters to SplitQuery
    val splitQuery = FilterToSplitQueryConverter.convert(filters)

    // Query all relevant XRefs (no upper limit)
    val matchingSplitFileNames = queryXRefs(relevantXRefs, splitQuery)

    // Identify splits not covered by any relevant XRef (by filename)
    val coveredSplitFileNames = relevantXRefs
      .flatMap(_.sourceSplitPaths)
      .map(extractFileName)
      .toSet
    val uncoveredSplits = candidateSplits.filterNot(s =>
      coveredSplitFileNames.contains(extractFileName(s.path)))

    // Return: XRef-matched splits + uncovered splits
    val matchedSplits = candidateSplits.filter(s =>
      matchingSplitFileNames.contains(extractFileName(s.path)))

    matchedSplits ++ uncoveredSplits
  }

  private def queryXRefs(
      xrefs: Seq[AddXRefAction],
      query: SplitQuery
  ): Set[String] = {

    // Use SplitCacheManager for efficient XRef access
    val cacheManager = SplitCacheManager.getInstance(cacheConfig)

    xrefs.flatMap { xref =>
      val xrefUri = resolveXRefUri(xref.path)
      val splitMeta = createSplitMetadata(xref)

      try {
        val searcher = XRefSearcher.open(cacheManager, xrefUri, splitMeta)
        try {
          // Search returns matching split URIs
          val result = searcher.search(query, xref.sourceSplitCount)
          result.getMatchingSplits.asScala.map { matchingSplit =>
            extractFileName(matchingSplit.getUri)
          }
        } finally {
          searcher.close()
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to query XRef ${xref.path}: ${e.getMessage}")
          // Fallback: return all source splits for this XRef
          xref.sourceSplitPaths.map(extractFileName)
      }
    }.toSet
  }

  /**
   * Extract file name from path for matching.
   * XRef returns full URIs, transaction log may have relative paths.
   */
  private def extractFileName(path: String): String = {
    path.split("/").last  // e.g., "part-00000-abc.split"
  }
}
```

### 7.3 Filter to SplitQuery Conversion

```scala
object FilterToSplitQueryConverter {

  def convert(filters: Seq[Expression]): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery()
    }

    val queries = filters.map(convertSingle)

    if (queries.size == 1) {
      queries.head
    } else {
      // AND all filters together
      val builder = new SplitBooleanQuery.Builder()
      queries.foreach(q => builder.must(q))
      builder.build()
    }
  }

  private def convertSingle(filter: Expression): SplitQuery = filter match {
    // Equality: column = 'value'
    case EqualTo(attr: AttributeReference, Literal(value, _)) =>
      new SplitTermQuery(attr.name, value.toString)

    case EqualTo(Literal(value, _), attr: AttributeReference) =>
      new SplitTermQuery(attr.name, value.toString)

    // AND: filter1 AND filter2
    case And(left, right) =>
      val builder = new SplitBooleanQuery.Builder()
      builder.must(convertSingle(left))
      builder.must(convertSingle(right))
      builder.build()

    // OR: filter1 OR filter2
    case Or(left, right) =>
      val builder = new SplitBooleanQuery.Builder()
      builder.should(convertSingle(left))
      builder.should(convertSingle(right))
      builder.minimumShouldMatch(1)
      builder.build()

    // NOT: NOT filter
    case Not(child) =>
      val builder = new SplitBooleanQuery.Builder()
      builder.mustNot(convertSingle(child))
      builder.build()

    // IN: column IN ('a', 'b', 'c')
    case In(attr: AttributeReference, values) =>
      val builder = new SplitBooleanQuery.Builder()
      values.foreach {
        case Literal(v, _) => builder.should(new SplitTermQuery(attr.name, v.toString))
        case _ => // Skip non-literals
      }
      builder.minimumShouldMatch(1)
      builder.build()

    // Range queries → match_all (XRef limitation)
    case _: GreaterThan | _: GreaterThanOrEqual |
         _: LessThan | _: LessThanOrEqual =>
      new SplitMatchAllQuery()

    // Unsupported → match_all (conservative)
    case _ =>
      new SplitMatchAllQuery()
  }
}
```

### 7.4 Integration Point: IndexTables4SparkScanBuilder

```scala
class IndexTables4SparkScanBuilder(...) {

  override def planInputPartitions(): Array[InputPartition] = {
    // Step 1: Get all files from transaction log
    var candidateSplits = transactionLog.listFiles()

    // Step 2: Apply partition pruning (existing)
    candidateSplits = applyPartitionPruning(candidateSplits, partitionFilters)

    // Step 3: Apply data skipping (existing)
    candidateSplits = applyDataSkipping(candidateSplits, dataFilters)

    // Step 4: Apply XRef pre-scan (NEW)
    if (xrefConfig.enabled && candidateSplits.size >= xrefConfig.minSplitsForXRef) {
      val router = new XRefQueryRouter(transactionLog, xrefConfig, sparkSession)
      candidateSplits = router.filterSplitsWithXRef(candidateSplits, pushedFilters)

      logInfo(s"XRef pre-scan: ${candidateSplits.size} splits after XRef filtering")
    }

    // Step 5: Create partitions (existing)
    createInputPartitions(candidateSplits)
  }
}
```

---

## 8. Configuration

### 8.1 Build-Time Configuration

```scala
// XRef auto-build on commit
spark.indextables.xref.autoIndex.enabled: true              // Enable auto-build
spark.indextables.xref.autoIndex.maxSourceSplits: 1024      // Max splits per XRef
spark.indextables.xref.autoIndex.minSplitsToTrigger: 10     // Min splits to build XRef
spark.indextables.xref.autoIndex.rebuildOnSourceChange: true // Rebuild when sources change

// XRef build performance
spark.indextables.xref.build.includePositions: false        // Skip positions (faster)
spark.indextables.xref.build.parallelism: <auto>            // Parallel XRef builds
spark.indextables.xref.build.tempDirectoryPath: <auto>      // Falls back to indexWriter.tempDirectoryPath
spark.indextables.xref.build.heapSize: <auto>               // Falls back to indexWriter.heapSize, then 50MB
```

### 8.2 Query-Time Configuration

```scala
// XRef query routing
spark.indextables.xref.query.enabled: true                  // Enable/disable XRef pre-scan at query time
spark.indextables.xref.query.minSplitsForXRef: 128          // Min splits to trigger XRef check
spark.indextables.xref.query.timeoutMs: 5000                // XRef query timeout
spark.indextables.xref.query.fallbackOnError: true          // Fallback to all splits on error
```

Note: All XRefs that cover any candidate split (after partition pruning and data skipping) are queried - there is no upper limit.

#### Disabling XRef at Query Time

The `spark.indextables.xref.query.enabled` configuration (default: `true`) allows users to disable XRef usage at query time without removing XRef data. This is useful for:

- **Debugging**: Comparing query performance with/without XRef
- **Troubleshooting**: Bypassing XRef if it's causing issues
- **Testing**: Verifying query correctness without XRef optimization

```scala
// Disable XRef for a specific session
spark.conf.set("spark.indextables.xref.query.enabled", "false")

// Or per-read operation
val df = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.xref.query.enabled", "false")
  .load("s3://bucket/my_table")

// XRef data remains intact - can be re-enabled anytime
spark.conf.set("spark.indextables.xref.query.enabled", "true")
```

When disabled:
- XRef files are not queried
- All candidate splits (after partition pruning and data skipping) are scanned
- No overhead from XRef lookup
- XRef data remains in transaction log and can be used when re-enabled

### 8.3 Storage Configuration

```scala
// XRef storage
spark.indextables.xref.storage.directory: "_xrefsplits"     // Relative to table root
spark.indextables.xref.storage.compressionEnabled: true     // Compress XRef splits
```

### 8.4 Configuration Example

```scala
// Enable XRef with custom settings
spark.conf.set("spark.indextables.xref.autoIndex.enabled", "true")
spark.conf.set("spark.indextables.xref.autoIndex.maxSourceSplits", "2048")
spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "64")

// Write data (XRef auto-built if conditions met)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/my_table")

// Query uses XRef automatically
val result = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/my_table")
  .filter($"status" === "error")
  .show()
```

---

## 9. Implementation Plan

### Phase 1: Transaction Log Foundation (Week 1-2)

**Tasks:**
1. Define `AddXRefAction` and `RemoveXRefAction` in `Actions.scala`
2. Update `ActionSerDe` for JSON serialization
3. Add TransactionLog methods: `addXRef`, `removeXRef`, `replaceXRef`, `listXRefs`
4. Update protocol version to v3
5. Write unit tests for new actions

**Files to modify:**
- `src/main/scala/io/indextables/spark/transaction/Actions.scala`
- `src/main/scala/io/indextables/spark/transaction/ActionSerDe.scala`
- `src/main/scala/io/indextables/spark/transaction/TransactionLog.scala`
- `src/main/scala/io/indextables/spark/transaction/ProtocolVersion.scala`

### Phase 2: XRef Build Manager (Week 2-3)

**Tasks:**
1. Create `XRefBuildManager` class
2. Implement `evaluateXRefRebuild` algorithm
3. Implement distributed XRef build execution
4. Add partition-aware XRef creation
5. Write integration tests

**New files:**
- `src/main/scala/io/indextables/spark/xref/XRefBuildManager.scala`
- `src/main/scala/io/indextables/spark/xref/XRefBuildExecutor.scala`
- `src/main/scala/io/indextables/spark/xref/XRefConfig.scala`

### Phase 3: SQL Command (Week 3-4)

**Tasks:**
1. Add ANTLR grammar rules
2. Implement `IndexCrossReferencesCommand`
3. Add AST builder visitor
4. Write SQL parsing tests
5. Write command execution tests

**Files to modify:**
- `src/main/antlr4/.../IndexTables4SparkSqlBase.g4`
- `src/main/scala/.../parser/IndexTables4SparkSqlAstBuilder.scala`

**New files:**
- `src/main/scala/.../sql/IndexCrossReferencesCommand.scala`

### Phase 4: Query Integration (Week 4-5)

**Tasks:**
1. Create `XRefQueryRouter` class
2. Implement `FilterToSplitQueryConverter`
3. Integrate into `IndexTables4SparkScanBuilder`
4. Add metrics/logging for XRef query routing
5. Write query integration tests

**New files:**
- `src/main/scala/io/indextables/spark/xref/XRefQueryRouter.scala`
- `src/main/scala/io/indextables/spark/xref/FilterToSplitQueryConverter.scala`

**Files to modify:**
- `src/main/scala/.../core/IndexTables4SparkScanBuilder.scala`

### Phase 5: Auto-Indexing Integration (Week 5-6)

**Tasks:**
1. Add auto-index trigger in transaction commit path
2. Implement rebuild triggers for MERGE SPLITS
3. Add XRef cleanup to PURGE command
4. Update DESCRIBE command to show XRef info
5. End-to-end testing

**Files to modify:**
- `src/main/scala/.../transaction/TransactionLog.scala` (commit hooks)
- `src/main/scala/.../sql/MergeSplitsCommand.scala`
- `src/main/scala/.../sql/PurgeOrphanedSplitsCommand.scala`
- `src/main/scala/.../sql/DescribeTransactionLogCommand.scala`

### Phase 6: Testing & Documentation (Week 6-7)

**Tasks:**
1. Comprehensive unit tests
2. Integration tests with S3/Azure
3. Performance benchmarks
4. Update CLAUDE.md documentation
5. Create user guide

---

## 10. Testing Strategy

### 10.1 Unit Tests

```scala
class AddXRefActionTest extends AnyFunSuite {
  test("AddXRefAction serializes to JSON correctly")
  test("AddXRefAction deserializes from JSON correctly")
  test("RemoveXRefAction round-trips through JSON")
}

class XRefBuildManagerTest extends AnyFunSuite {
  test("evaluates rebuild when XRef references removed splits")
  test("creates new XRef for uncovered splits")
  test("respects maxSourceSplits limit")
  test("handles partition-aware XRef creation")
}

class FilterToSplitQueryConverterTest extends AnyFunSuite {
  test("converts equality filter")
  test("converts AND filter")
  test("converts OR filter")
  test("converts IN filter")
  test("converts range filter to match-all")
}
```

### 10.2 Integration Tests

```scala
class XRefIntegrationTest extends AnyFunSuite with SparkSessionTestWrapper {
  test("INDEX CROSSREFERENCES creates XRef split") {
    // Write data
    // Execute INDEX CROSSREFERENCES
    // Verify XRef in transaction log
    // Verify XRef file exists
  }

  test("query uses XRef to skip splits") {
    // Write 500 splits
    // Build XRef
    // Query with selective filter
    // Verify <50 splits scanned (via metrics)
  }

  test("PURGE removes orphaned XRef files") {
    // Create XRef
    // Remove source splits
    // Execute PURGE
    // Verify XRef file deleted
  }
}
```

### 10.3 Performance Benchmarks

```scala
class XRefPerformanceBenchmark {
  // Benchmark: 1000 splits, selective query
  // Without XRef: measure scan time
  // With XRef: measure scan time
  // Report: % improvement

  // Benchmark: XRef build time vs split count
  // Measure: 100, 500, 1000, 2000 splits
  // Report: build time, XRef size
}
```

---

## 11. Migration & Compatibility

### 11.1 Backward Compatibility

- **Reader v2 clients**: Cannot read tables with XRef (protocol v3 required)
- **Writer v2 clients**: Cannot write to tables with XRef
- **Tables without XRef**: Continue to work with all clients

### 11.2 Upgrade Path

```scala
// Upgrade table to support XRef
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('minReaderVersion' = '3')")

// Or let auto-upgrade handle it (default enabled)
spark.conf.set("spark.indextables.protocol.autoUpgrade", "true")
// First INDEX CROSSREFERENCES will upgrade protocol
```

### 11.3 Rollback

```scala
// Remove all XRefs to rollback (protocol stays at v3 but XRef not used)
// Manual: Delete _xrefsplits directory and clean transaction log
// Or: PURGE will eventually clean orphaned XRef files
```

---

## Appendix A: API Reference

### AddXRefAction Fields

| Field | Type | Description |
|-------|------|-------------|
| `path` | String | Relative path to XRef file (includes hash directory) |
| `xrefId` | String | Unique identifier |
| `sourceSplitPaths` | Seq[String] | Source split paths (partition-agnostic) |
| `sourceSplitCount` | Int | Count of source splits |
| `size` | Long | File size in bytes |
| `totalTerms` | Long | Total indexed terms |
| `footerStartOffset` | Long | Footer start byte |
| `footerEndOffset` | Long | Footer end byte |
| `createdTime` | Long | Build timestamp |
| `buildDurationMs` | Long | Build duration |
| `maxSourceSplits` | Int | Config used |

### Configuration Reference

| Config | Default | Description |
|--------|---------|-------------|
| **Auto-Index** | | |
| `spark.indextables.xref.autoIndex.enabled` | true | Enable auto-build on commit |
| `spark.indextables.xref.autoIndex.maxSourceSplits` | 1024 | Max splits per XRef |
| `spark.indextables.xref.autoIndex.minSplitsToTrigger` | 10 | Min splits to build XRef |
| `spark.indextables.xref.autoIndex.rebuildOnSourceChange` | true | Rebuild when sources change |
| **Build Performance** | | |
| `spark.indextables.xref.build.includePositions` | false | Include positions (slower but enables phrase queries) |
| `spark.indextables.xref.build.parallelism` | auto | Parallel XRef builds |
| `spark.indextables.xref.build.tempDirectoryPath` | auto | Falls back to indexWriter.tempDirectoryPath |
| `spark.indextables.xref.build.heapSize` | auto | Falls back to indexWriter.heapSize, then 50MB |
| **Query-Time** | | |
| `spark.indextables.xref.query.enabled` | **true** | **Enable/disable XRef at query time** |
| `spark.indextables.xref.query.minSplitsForXRef` | 128 | Min splits to trigger XRef check |
| `spark.indextables.xref.query.timeoutMs` | 5000 | XRef query timeout |
| `spark.indextables.xref.query.fallbackOnError` | true | Fallback on error |
| **Storage** | | |
| `spark.indextables.xref.storage.directory` | "_xrefsplits" | XRef storage directory |

---

## Appendix B: Example Workflows

### B.1 Initial Setup

```scala
// Configure XRef
spark.conf.set("spark.indextables.xref.autoIndex.enabled", "true")

// Write initial data (500 splits)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://bucket/events")

// XRef auto-built during commit (500 < 1024 maxSourceSplits)
// Transaction log shows:
//   Version N: AddAction × 500
//   Version N: AddXRefAction (sourceSplitCount=500)
```

### B.2 Query Execution

```scala
// Load table
val events = spark.read
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://bucket/events")

// Selective query
events.filter($"error_code" === "E500").count()

// Execution:
// 1. List 500 splits from transaction log
// 2. 500 > 128 threshold → XRef pre-scan
// 3. Query XRef for "error_code:E500"
// 4. XRef returns 23 matching splits
// 5. Create scan tasks for 23 splits only
// 6. Execute scans → return count
```

### B.3 Incremental Update

```scala
// Add more data
newData.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .mode("append")
  .save("s3://bucket/events")

// If 50 new splits added and existing XRef has 500 (total 550 < 1024):
//   Version N+1: AddAction × 50
//   Version N+1: RemoveXRefAction (old XRef)
//   Version N+1: AddXRefAction (new XRef with 550 splits)
```

### B.4 After MERGE SPLITS

```scala
// Compact table
spark.sql("MERGE SPLITS 's3://bucket/events' TARGET SIZE 1G")

// If merge combines splits B+C → BC:
//   Version N+2: RemoveAction (B), RemoveAction (C), AddAction (BC)
//   Version N+2: RemoveXRefAction (old XRef, reason="source_changed")
//   Version N+2: AddXRefAction (new XRef with updated split list)
```

---

*End of Design Document*
