# IndexTables4Spark Development Backlog

## Session Carryover (2026-03-03)

Items requiring attention in the next session. Remove entries as they are addressed.

### Open PRs Awaiting Action
- **PR #191** (DRAFT): Document companion/sync feature — branch `docs/it011-companion-sync-feature`. IT-011 content complete; PR needs to be taken out of draft and reviewed.
- **PR #184** (OPEN): Rewrite table-protocol.md for V4 Avro state — branch `docs/table-protocol-v4-rewrite-175`. IT-043 marked complete; PR awaiting Scott's review.
- **IT-055 PR** (NEW): Multi-region table roots — branch `feature/multi-region-table-roots`. Ready for review.

### Incomplete Acceptance Criteria on Merged Items
- **IT-036** (PR #173 merged): Two items remain unchecked — "Follow-up posted to GitHub issue #85" and "Full test suite validation." Both are verification/admin tasks, not code work.

### Needs Evaluation Queue (5 items, no adversarial analysis done)
- **IT-037** (#146): Text/JSON exists queries broken in companion splits — initial signal High (bug)
- **IT-038** (#133): Replace indexquery with textsearch/fieldmatch — initial signal Medium (breaking API)
- **IT-039** (#129): Structured streaming support — initial signal Medium (overlaps IT-013)
- **IT-040** (#140): Fast fields GROUP BY guidance — initial signal Low-Medium (UX/docs)
- **IT-041** (#10): Scala compiler warnings cleanup — initial signal Low (tech debt)

### Ready to Work (no blockers)
- **IT-057**: Concurrent-safe metadata-only transaction log writes — Medium priority (discovered during IT-055)
- **IT-056**: exact_only IndexQuery validation remainder — Medium priority
- **IT-012**: Fix deprecated config in features.md — Medium, quick fix
- **IT-013**: Document IP address fields and structured streaming — Medium

### Blocked (Upstream) — Monitor Only
- IT-048–053 (Arrow FFI roadmap): All blocked on tantivy4java #100–#106
- IT-054 (Streaming sync): Blocked on tantivy4java #107

---

## Blocking

### ~~IT-001: OOM running full test suite via `mvn test`~~ **DONE**
Moved to Completed.

### ~~IT-035: Separate cloud tests from local test suite~~ **DONE**
Moved to Completed.

### IT-002: JVM shutdown crashes in tantivy4java tests
**Reporter**: Scott
**Upstream**: tantivy4java
**Description**: Several tantivy4java test cases crash on JVM shutdown after tests pass. Root cause unknown.
**Acceptance Criteria**:
- [ ] Root cause identified
- [ ] Fix applied upstream in tantivy4java or workaround documented

---

## Critical

### ~~IT-003: Fix IndexQuery usage in CLAUDE.md~~ **DONE**
Moved to Completed.

### ~~IT-004: Fix extensions registration syntax~~ **DONE**
Moved to Completed.

### ~~IT-005: Remove or annotate unwired data skipping SQL commands~~ **DONE**
Moved to Completed.

---

## High

### ~~IT-006: Document both class name aliases in CLAUDE.md~~ **DONE**
Moved to Completed.

### ~~IT-007: Fix storage class names in CLAUDE.md~~ **DONE**
Moved to Completed.

### ~~IT-008: Document missing SQL commands~~ **DONE**
Moved to Completed.

### IT-009: ReplaceWhere with Partition Predicates
**Status**: Design Complete
**Design**: [docs/design/replace-where.md](../docs/design/replace-where.md)
**Description**: Delta Lake-style replaceWhere for selective partition replacement.
**Acceptance Criteria**:
- [ ] Predicate parser for `=`, `IN`, `AND` on partition columns
- [ ] Transaction log ReplaceWhereAction integration
- [ ] Validation and error handling
- [ ] Test coverage

### IT-010: Transaction Log Compaction (Parquet-based) — Needs Re-triage
**Design**: [docs/design/log-compaction.md](../docs/design/log-compaction.md)
**Description**: Parquet-based checkpoint system. May be partially superseded by existing Avro state format (Protocol V4).
**Acceptance Criteria**:
- [ ] Determine if Parquet-based approach is still needed on top of Avro state
- [ ] If yes, implement. If no, close and archive design doc.

### IT-036: Partition predicate WHERE clause — broken numeric comparisons
**GitHub**: [#85](https://github.com/indextables/indextables_spark/issues/85)
**Reporter**: Issue filer (follow-up needed)
**Type**: Bug
**Priority**: High (assigned 2026-02-20)
**Analysis**: [.claude/artifacts/it036-partition-predicate-analysis.md](artifacts/it036-partition-predicate-analysis.md)
**Description**: `PartitionPredicateUtils.resolveExpression` converts all non-string literals to UTF8String, causing lexicographic comparison where `"10" < "2"`. Affects 7 code paths including DROP PARTITIONS (destructive), MERGE SPLITS, PREWARM CACHE, DESCRIBE COMPONENT SIZES, BUILD COMPANION. Standard read path is NOT affected. Silent data correctness bug — no error or warning on wrong results.
**Recommended Fix**: Type-aware literal handling (Solution 3). Partition column types are available via `MetadataAction.schemaString`. Also consolidate 3-way duplicated comparison logic and remove code duplication in `MergeSplitsCommand`.
**Blocks**: IT-009 (ReplaceWhere builds on the same partition predicate infrastructure)
**Acceptance Criteria**:
- [x] Adversarial analysis complete
- [x] Priority assigned
- [ ] Follow-up posted to GitHub issue #85
- [x] Fix implemented in `PartitionPredicateUtils.resolveExpression`
- [x] MergeSplitsCommand duplicate code consolidated
- [x] Regression tests with multi-digit numeric partition values (27 unit + 3 integration)
- [x] All 7 affected call sites updated
- [ ] Full test suite validation
- **PR**: [#173](https://github.com/indextables/indextables_spark/pull/173) — **MERGED** (2026-02-22)

---

## Medium

### ~~IT-011: Document companion/sync feature~~ **DONE**
Moved to Completed.

### IT-012: Fix deprecated config in features.md
**Description**: `docs/reference/features.md` line 185 uses `mergeOnWrite.mergeGroupMultiplier` which is deprecated.
**Acceptance Criteria**:
- [ ] Replace with current config key or remove

### IT-013: Document IP address fields and structured streaming
**Description**: Test files exist (`IpAddressFieldTest.scala`, `IpAddressIndexQueryTest.scala`, `StructuredStreamingTest.scala`) but neither feature is documented.
**Acceptance Criteria**:
- [ ] IP address field type documented in `docs/reference/field-indexing.md`
- [ ] Structured streaming documented in CLAUDE.md or `docs/reference/features.md`

---

## Needs Evaluation

Items imported from GitHub issues. Each requires adversarial analysis before prioritization, followed by follow-up with the issue filer.

### IT-037: Text/JSON exists queries broken in companion splits
**GitHub**: [#146](https://github.com/indextables/indextables_spark/issues/146)
**Reporter**: Issue filer (follow-up needed)
**Type**: Bug
**Description**: `SplitExistsQuery` on text and JSON fields returns 0 hits in companion mode. Companion transcode pipeline excludes text/JSON from fast fields; exists queries rely on fast field metadata. Standard (non-companion) splits unaffected.
**Initial Signal**: High — bug in documented feature (companion/sync). Upstream tantivy4java transcode implications.
**Acceptance Criteria**:
- [ ] Adversarial analysis complete
- [ ] Priority assigned
- [ ] Follow-up posted to GitHub issue #146

### IT-038: Replace indexquery with textsearch/fieldmatch operators
**GitHub**: [#133](https://github.com/indextables/indextables_spark/issues/133)
**Reporter**: Issue filer (follow-up needed)
**Type**: Enhancement
**Description**: Split `indexquery` into two operators: `textsearch` (tokenized text fields, phrase queries) and `fieldmatch` (non-tokenized fields, exact match). Adds type validation with clear error messages when operator/field type mismatch.
**Initial Signal**: Medium — API design change, potentially breaking. Needs design doc and backward compatibility analysis.
**Acceptance Criteria**:
- [ ] Adversarial analysis complete
- [ ] Priority assigned
- [ ] Follow-up posted to GitHub issue #133

### IT-039: Support Spark structured streaming
**GitHub**: [#129](https://github.com/indextables/indextables_spark/issues/129)
**Reporter**: Issue filer (follow-up needed)
**Type**: Enhancement
**Description**: Full structured streaming support similar to Delta Lake. Note: `StructuredStreamingTest.scala` already exists — need to determine current implementation status vs. what's being requested.
**Initial Signal**: Medium — overlaps IT-013 (documentation). Need to assess if this is "document what exists" or "build new functionality."
**Acceptance Criteria**:
- [ ] Adversarial analysis complete (including current implementation assessment)
- [ ] Priority assigned
- [ ] Relationship to IT-013 clarified
- [ ] Follow-up posted to GitHub issue #129

### IT-040: Fast fields missing for GROUP BY — best practices guidance
**GitHub**: [#140](https://github.com/indextables/indextables_spark/issues/140)
**Reporter**: Issue filer (follow-up needed)
**Type**: Enhancement / Documentation
**Description**: User hit `IllegalArgumentException` when GROUP BY field not configured as fast field. Error message already provides the fix. Requests: default fast field templates for security datasets, pre-deployment config validation, best practices documentation.
**Initial Signal**: Low-Medium — not a code bug (config issue), but highlights UX gap in onboarding and configuration guidance.
**Acceptance Criteria**:
- [ ] Adversarial analysis complete
- [ ] Priority assigned
- [ ] Follow-up posted to GitHub issue #140

### IT-041: Clean up Scala compiler warnings
**GitHub**: [#10](https://github.com/indextables/indextables_spark/issues/10)
**Reporter**: Issue filer (follow-up needed)
**Type**: Tech Debt
**Description**: Systematic elimination of all Scala compiler warnings. Phased approach: audit/categorize, cleanup by priority, then add `-Xfatal-warnings` to prevent regressions.
**Initial Signal**: Low — code quality improvement, no functional impact. Good housekeeping.
**Acceptance Criteria**:
- [ ] Adversarial analysis complete
- [ ] Priority assigned
- [ ] Follow-up posted to GitHub issue #10

### ~~IT-044: Testing excellence — fix exception-swallowing tests (Phase 0A)~~ **DONE**
Moved to Completed.

### ~~IT-045: setup.sh builds tantivy at wrong revision~~ **DONE**
Moved to Completed.

### ~~IT-046: SyncToExternalCommand compile error on main~~ **DONE**
Moved to Completed.

### IT-047: Avro state format corruption tests (coverage gap)
**Type**: Tech Debt / Test Quality
**Priority**: Low
**Description**: StorageErrorHandlingTest only tests JSON format transaction log corruption. Avro is the default since Protocol V4 and is more representative of production. Discovered during adversarial review of IT-044.
**Acceptance Criteria**:
- [ ] Equivalent corruption tests for Avro state format

---

## Blocked (Upstream)

Items with fully specified designs from Scott. Blocked on tantivy4java upstream dependencies. No adversarial analysis needed — project owner authored and prioritized.

### Arrow FFI Performance Migration

Systematic replacement of per-row/per-field JNI with zero-copy Arrow FFI across all code paths. Companion reads already use Arrow FFI (shipped in v0.5.2/v0.5.3). This roadmap extends the pattern to all remaining paths.

#### P0 (Prerequisites / Safety Gates)

### IT-048: Integrate Spark TaskMemoryManager with tantivy4java native memory
**GitHub**: [#206](https://github.com/indextables/indextables_spark/issues/206)
**Type**: Enhancement
**Priority**: P0 (prerequisite for safe production use of all FFI features)
**Upstream**: tantivy4java [#105](https://github.com/indextables/tantivy4java/issues/105)
**Description**: Bridge Spark's `TaskMemoryManager` with tantivy4java's native memory pool, following DataFusion Comet's pattern. Gives Spark visibility into native memory, prevents OOM from untracked off-heap allocations. Creates `IndexTablesNativeMemoryAccountant` Java class with per-task lifecycle.
**Key Files**: New `IndexTablesNativeMemoryAccountant.java`; modify `SplitReaderContext`, `ArrowFfiBridge`, `WorkerLocalSplitMerger`, `IndexTables4SparkDataWriter`
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#105 landed
- [ ] `IndexTablesNativeMemoryAccountant` integrated with TaskMemoryManager
- [ ] Per-task lifecycle with leak detection
- [ ] Metrics in `DESCRIBE INDEXTABLES ENVIRONMENT`
- [ ] Test coverage

### IT-049: Integrate native transaction log state reader (Arrow FFI)
**GitHub**: [#201](https://github.com/indextables/indextables_spark/issues/201)
**Type**: Enhancement
**Priority**: P0
**Upstream**: tantivy4java [#100](https://github.com/indextables/tantivy4java/issues/100)
**Description**: Replace JVM-side Avro manifest reading with single native call to tantivy4java's `TransactionLogReader`, receiving materialized state as Arrow columnar batches via FFI. Eliminates per-manifest GZIP decompression and Avro deserialization on the driver. Expected 2-5x faster cold startup on large tables.
**Key Files**: Modify `TransactionLog.scala`, `AvroManifestReader.scala`, `StateManifestIO.scala`; reuse `ArrowFfiBridge`
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#100 landed
- [ ] Native state read path with feature flag
- [ ] Correctness parity with JVM reader
- [ ] Benchmark on tables with 100/1000/5000+ splits

### IT-050: Extend Arrow FFI columnar reads to standard (non-companion) read path
**GitHub**: [#202](https://github.com/indextables/indextables_spark/issues/202)
**Type**: Enhancement
**Priority**: P0
**Upstream**: tantivy4java [#101](https://github.com/indextables/tantivy4java/issues/101)
**Description**: Use `docBatchArrowFfi` for standard IndexTables reads (not just companion mode). Eliminates per-document JNI overhead. Expected 30-60% faster reads for wide schemas and large result sets.
**Key Files**: Modify `SplitSearchEngine.scala`, `IndexTables4SparkPartitionReader.scala`, `IndexTables4SparkMultiSplitPartitionReader.scala`; potentially new columnar reader
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#101 landed
- [ ] Arrow FFI read path with feature flag
- [ ] Correctness parity across all field types including JSON
- [ ] LIMIT pushdown still works with columnar batches
- [ ] Benchmark on wide schemas and large result sets

#### P1 (Performance Improvements)

### IT-051: Arrow FFI columnar ingestion on write path
**GitHub**: [#199](https://github.com/indextables/indextables_spark/issues/199), [#204](https://github.com/indextables/indextables_spark/issues/204)
**Type**: Enhancement
**Priority**: P1
**Upstream**: tantivy4java [#103](https://github.com/indextables/tantivy4java/issues/103)
**Description**: Send data to tantivy4java as Arrow columnar batches on the write path, replacing per-row JNI document construction. Rust handles partition routing from full batches. Developer guide available in tantivy4java repo. Expected 20-40% faster write throughput.
**Key Files**: New `IndexTables4SparkColumnarDataWriter`; modify `IndexTables4SparkWriterFactory`; reuse `ArrowFfiBridge`
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#103 landed
- [ ] `DataWriter[ColumnarBatch]` implementation with feature flag
- [ ] Write + read roundtrip correctness for all field types
- [ ] Optimized write (shuffle) compatibility
- [ ] Benchmark: narrow (5 cols) vs wide (30+ cols) schemas

### IT-052: Arrow FFI for aggregate pushdown
**GitHub**: [#207](https://github.com/indextables/indextables_spark/issues/207)
**Type**: Enhancement
**Priority**: P1
**Upstream**: tantivy4java [#106](https://github.com/indextables/tantivy4java/issues/106)
**Description**: Use Arrow FFI to receive aggregation results from tantivy4java, replacing per-aggregation JNI object extraction. Optionally batch aggregations across multiple splits. Expected 10-50x fewer JNI crossings for bucket aggregations.
**Key Files**: Modify `IndexTables4SparkSimpleAggregateScan.scala`, `SplitSearchEngine.scala`; reuse `ArrowFfiBridge`
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#106 landed
- [ ] Arrow FFI aggregate path for all aggregation types
- [ ] NULL handling verified
- [ ] Benchmark: bucket aggregations (365 daily buckets × 5 metrics)

### IT-053: Native data skipping and partition pruning via PartitionFilter
**GitHub**: [#203](https://github.com/indextables/indextables_spark/issues/203)
**Type**: Enhancement
**Priority**: P1
**Upstream**: tantivy4java [#102](https://github.com/indextables/tantivy4java/issues/102)
**Description**: Move partition pruning and column-statistics-based data skipping from JVM driver to native Rust code. Ideally done after IT-049 (native tx log reader) so pruning happens during state materialization. Expected 5-10x faster scan planning on highly-partitioned tables.
**Key Files**: Modify `PartitionPruning.scala`, `SparkPredicateToPartitionFilter.scala`, `IndexTables4SparkScan.scala`
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#102 landed
- [ ] Native pruning correctness parity with JVM
- [ ] All filter types: equality, range, IN, IS NULL, IS NOT NULL, AND, OR, NOT
- [ ] Benchmark on 100/500/1000+ partition tables

---

## High (New)

### IT-054: Streaming companion sync
**GitHub**: [#208](https://github.com/indextables/indextables_spark/issues/208)
**Type**: Enhancement
**Priority**: P1 (Scott)
**Upstream**: tantivy4java [#107](https://github.com/indextables/tantivy4java/issues/107) (lightweight version-change detection)
**Description**: Add `WITH STREAMING` option to `BUILD INDEXTABLES COMPANION` that continuously monitors the source table and automatically triggers incremental sync. Blocking command with polling loop, Spark UI metrics, and graceful shutdown. Supports Delta version tracking, Iceberg snapshot tracking, and Parquet directory fingerprinting.
**Key Files**: New `CompanionStreamingSyncManager.scala`, `StreamingSyncMetrics.scala`; modify `SyncToExternalCommand.scala`, SQL parser
**Acceptance Criteria**:
- [ ] Upstream tantivy4java#107 landed
- [ ] `WITH STREAMING [POLL INTERVAL]` SQL syntax
- [ ] Polling loop with version-change detection per source format
- [ ] Metrics and observability (Spark UI, structured logging)
- [ ] Graceful shutdown on interrupt/cancel
- [ ] Failure recovery with exponential backoff
- [ ] Resume from `lastSyncedVersion` across sessions
- [ ] Integration tests with background thread + Delta writes

### ~~IT-055: Multi-region table roots for companion splits~~ **DONE**
Moved to Completed.

---

## Medium (New)

### IT-057: Concurrent-safe metadata-only transaction log writes
**Type**: Tech Debt / Correctness
**Priority**: Medium
**Description**: Discovered during IT-055 adversarial review. `commitSyncActions` → `writeActionsWithRetry` does not re-read metadata between retry attempts. For metadata-only operations like SET/UNSET TABLE ROOT, concurrent writers can silently overwrite each other's changes. The fix is to add a `commitMetadataUpdate(transform: MetadataAction => MetadataAction)` method that re-reads metadata on each retry, or implement a manual compare-and-swap loop at the command level.
**Affected Commands**: `SetTableRootCommand`, `UnsetTableRootCommand`
**Acceptance Criteria**:
- [ ] Concurrent SET TABLE ROOT operations preserve both writers' root entries
- [ ] Test with parallel metadata writes
- [ ] No regression in existing commitSyncActions callers

### IT-056: exact_only IndexQuery validation (remainder)
**GitHub**: [#186](https://github.com/indextables/indextables_spark/issues/186)
**Type**: Enhancement
**Priority**: Medium
**Description**: Range filter validation for `exact_only` fields already shipped (PR #188, commit 4876a42). Remaining work: validate IndexQuery wildcard/range patterns on `exact_only` fields before they reach the native layer. Best-effort pattern detection for `*`, `?`, `[... TO ...]` in query text. Currently these produce cryptic JNI errors after exhausting Spark task retries.
**Key Files**: Modify `IndexingModes.scala` (add `supportsWildcardQuery`), IndexQuery filter path
**Acceptance Criteria**:
- [ ] Wildcard IndexQuery on `exact_only` returns clear error message
- [ ] Range IndexQuery on `exact_only` returns clear error message
- [ ] EqualTo on `exact_only` still works (regression test)
- [ ] Range/wildcard on `text_*` fields still work (regression test)

---

## Low

| ID | Item | Description |
|----|------|-------------|
| IT-014 | Bloom filters | Better file skipping via bloom filters |
| IT-015 | Column statistics | Improved min/max tracking for pruning |
| IT-016 | Join optimization | Better Spark join planning integration |
| IT-017 | Compression | Evaluate additional compression codecs |
| IT-018 | Cache eviction | Smarter cache eviction policies |
| IT-019 | Async I/O | Background prefetching for read performance |
| IT-020 | Metrics dashboard | Comprehensive performance metrics |
| IT-021 | Query profiling | Detailed query execution statistics |
| IT-022 | Health checks | Automated table integrity validation |
| IT-023 | Fuzzy search | Approximate string matching |
| IT-024 | Synonym support | Query expansion with synonyms |
| IT-025 | Multi-language | Enhanced tokenization for different languages |
| IT-026 | Schema evolution | Column addition, type changes, removal |
| IT-027 | Iceberg interop | Cross-format compatibility layer |
| IT-028 | Hudi interop | Cross-format compatibility layer |

---

## Completed

| ID | Item | Date |
|----|------|------|
| IT-029 | Archive stale design documents | 2026-02 |
| IT-030 | Remove performance-tuning.md stub | 2026-02 |
| IT-031 | Restructure llms.txt with proper hierarchy | 2026-02 |
| IT-032 | Reorganize docs/ into reference/design/archive | 2026-02 |
| IT-033 | Move CLAUDE.md and BACKLOG.md to .claude/ | 2026-02 |
| IT-034 | Move PROTOCOL.md and TABLE_PROTOCOL.md to docs/reference/ | 2026-02 |
| IT-001 | Add parallel execution to test runner script | 2026-02 |
| IT-035 | Separate cloud tests from local test suite | 2026-02 |
| IT-003 | Fix IndexQuery usage in CLAUDE.md | 2026-02 |
| IT-004 | Fix extensions registration syntax | 2026-02 |
| IT-005 | Annotate unwired data skipping SQL commands | 2026-02 |
| IT-007 | Fix storage class names in CLAUDE.md | 2026-02 |
| IT-006 | Document both class name aliases in CLAUDE.md | 2026-02 |
| IT-008 | Document missing SQL commands | 2026-02 |
| IT-042 | Fix broken README.md links ([#176](https://github.com/indextables/indextables_spark/issues/176)) | 2026-02 |
| IT-043 | Rewrite table-protocol.md for V4 Avro state ([#175](https://github.com/indextables/indextables_spark/issues/175)) | 2026-02 |
| IT-044 | Fix exception-swallowing tests — Phase 0A testing excellence | 2026-02 |
| IT-045 | Fix setup.sh tantivy pinned rev +1 bug | 2026-02 |
| IT-046 | Fix SyncToExternalCommand compile error on main | 2026-02 |
| IT-011 | Document companion/sync feature ([PR #191](https://github.com/indextables/indextables_spark/pull/191)) | 2026-02 |
| IT-036 | Partition predicate numeric comparisons ([PR #173](https://github.com/indextables/indextables_spark/pull/173)) | 2026-02 |
| IT-055 | Multi-region table roots for companion splits ([#179](https://github.com/indextables/indextables_spark/issues/179)) | 2026-03 |
