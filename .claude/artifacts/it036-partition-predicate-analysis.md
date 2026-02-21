# IT-036: Adversarial Analysis — Partition Predicate Numeric Comparison Bug

**GitHub**: [#85](https://github.com/indextables/indextables_spark/issues/85)
**Date**: 2026-02-20
**Status**: Analysis Complete
**Priority Assigned**: High
**Tier**: Moderate

## Summary

GitHub issue #85 reports that partition predicate WHERE clauses in custom SQL commands have broken numeric comparisons due to lexicographic string comparison. Two expert investigations confirm the bug is real, the fix approach is feasible, and the priority is High.

## Bug Confirmation

**Confirmed.** `PartitionPredicateUtils.resolveExpression` (line 166-173) converts all non-string literals to `UTF8String`, causing Catalyst's comparison operators to use lexicographic ordering. This means `"10" < "2"` evaluates to `true` because `'1' < '2'` in character comparison.

### Root Cause

```
User writes: WHERE month BETWEEN 2 AND 11
Catalyst creates: Literal(2, IntegerType), Literal(11, IntegerType)
resolveExpression converts to: Literal(UTF8String("2"), StringType), Literal(UTF8String("11"), StringType)
buildPartitionSchema types all columns as: StringType
Result: All comparisons are lexicographic string comparisons
```

### Affected Paths (7 total)

| # | Command / Path | File | Risk |
|---|---------------|------|------|
| 1 | DROP PARTITIONS | `DropPartitionsCommand.scala` | **DESTRUCTIVE** — wrong partitions removed |
| 2 | MERGE SPLITS | `MergeSplitsCommand.scala` | Wrong partitions merged (wasteful, not destructive) |
| 3 | MERGE SPLITS (countMergeGroups) | `MergeSplitsCommand.scala` | Wrong count returned |
| 4 | PREWARM CACHE | `PrewarmCacheCommand.scala` | Wrong partitions prewarmed |
| 5 | DESCRIBE COMPONENT SIZES | `DescribeComponentSizesCommand.scala` | Wrong partitions described |
| 6 | BUILD COMPANION | `SyncToExternalCommand.scala` | Wrong partitions synced |
| 7 | Prewarm partition filter | `IndexTables4SparkScan.scala` | Wrong partitions prewarmed on read |

### NOT Affected

- **Standard read path**: `PartitionPruning.compareValues` already has type-aware comparison (Int, Long, Float, Double, Boolean, Date, Timestamp, BigDecimal)
- **Aggregate pushdown COUNT**: `TransactionLogCountScan.compareValues` uses numeric-first comparison heuristic
- **Avro manifest pruning**: `expressionToFilter` preserves original literal types (needs verification of downstream evaluation)

## Severity Assessment

**HIGH — Silent data correctness.** The bug produces wrong results with no error or warning. The `evaluatePredicates` catch block swallows exceptions and returns `false`, meaning a failing comparison silently excludes partitions rather than erroring.

**Worst case**: `DROP INDEXTABLES PARTITIONS ... WHERE month < 9` would delete months 1-8 AND also 10, 11, 12 (because `"10" < "9"`, `"11" < "9"`, `"12" < "9"` lexicographically).

### Mitigating Factors

- Most real-world partition columns are date strings (`YYYY-MM-DD`) where lexicographic ordering equals chronological ordering
- Zero-padded numeric partitions (e.g., `month=01`, `month=02`) are not affected
- Equality (`=`) and `IN` operators are not affected (string equality is correct)
- Only manifests when numeric partition values cross digit boundaries (9→10, 99→100)
- Time travel provides recovery path for accidental DROP PARTITIONS (if retention hasn't expired)

### Aggravating Factors

- The inconsistency between read path (correct) and SQL command path (broken) means the same logical predicate can produce different results
- DROP PARTITIONS is destructive — no undo without time travel
- Bug is completely silent with no warning

## Recommended Fix: Solution 3 (Type-Aware Literal Handling)

### Feasibility: Confirmed

`MetadataAction.schemaString` stores the full schema as JSON (set at `TransactionLog.scala:231` via `schema.json`). Partition column types can be extracted:

```scala
val fullSchema = DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
val partitionSchema = StructType(
  metadata.partitionColumns.map(name => fullSchema(fullSchema.fieldIndex(name)))
)
```

### Fix Scope

| File | Change | Complexity |
|------|--------|------------|
| `PartitionPredicateUtils.scala` | Rework `buildPartitionSchema`, `createRowFromPartitionValues`, `resolveExpression` to use real types | Medium |
| `MergeSplitsCommand.scala` | Remove duplicate methods, delegate to `PartitionPredicateUtils` | Medium (refactor) |
| `DropPartitionsCommand.scala` | Pass full schema to `buildPartitionSchema` | Low |
| `PrewarmCacheCommand.scala` | Pass full schema to `buildPartitionSchema` | Low |
| `DescribeComponentSizesCommand.scala` | Pass full schema to `buildPartitionSchema` | Low |
| `SyncToExternalCommand.scala` | Pass full schema to `buildPartitionSchema` | Low |
| `IndexTables4SparkScan.scala` | Pass full schema to `buildPartitionSchema` | Low |
| New/updated tests | Numeric partition tests with multi-digit values | Medium |

### Architectural Debt Identified

Three independent implementations of partition value comparison exist:
1. `PartitionPruning.compareValues` — correct, type-aware (most complete)
2. `PartitionPredicateUtils.resolveExpression` — broken, string-only
3. `TransactionLogCountScan.compareValues` — correct, numeric-first heuristic

The fix should consolidate these into a single canonical implementation.

## Rejected Alternatives

| Solution | Why Rejected |
|----------|-------------|
| Solution 1: Numeric-aware string comparison | Modifies Catalyst internals; fragile, violates type system |
| Solution 2: Explicit CAST in predicates | Shifts burden to users; unacceptable UX |
| Solution 4: Detect and warn | Unacceptable for data-correctness issue — silent wrong results are worse than errors |
| Numeric detection heuristic | Fragile (what about "00123" vs "123"?); schema info is available, so use it |

## Test Gaps

- Existing BETWEEN test uses months 1-6 (all single digit) — does not trigger the bug
- No tests with multi-digit numeric partition values (1-12, 1-100)
- No tests for `>`, `<`, `>=`, `<=` with numeric values crossing digit boundaries
- MergeSplitsCommand duplicate code path has no independent partition predicate tests

## Open Questions

1. Does the Avro manifest pruning evaluation path have the same bug? (Needs trace of `expressionToFilter` downstream evaluation)
2. Are there protocol version edge cases where `MetadataAction.schemaString` might not include partition column fields?

## Dissenting View

The Distinguished Engineer argued for MEDIUM priority based on LOW-MEDIUM likelihood of triggering in common usage patterns (most partitions are date strings). This is overruled because: (1) the fix is low-complexity with a known approach, (2) IT-009 (ReplaceWhere) will build more partition predicate functionality on this foundation, and (3) the destructive nature of DROP PARTITIONS warrants proactive remediation.
