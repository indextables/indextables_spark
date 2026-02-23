# IndexTables4Spark Development Backlog

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
- [ ] Follow-up posted to GitHub issue #85 (pending Scott's review of PR #173)
- [x] Fix implemented in `PartitionPredicateUtils.resolveExpression`
- [x] MergeSplitsCommand duplicate code consolidated
- [x] Regression tests with multi-digit numeric partition values (27 unit + 3 integration)
- [x] All 7 affected call sites updated
- [ ] Full test suite validation
- **PR**: [#173](https://github.com/indextables/indextables_spark/pull/173) — awaiting Scott's review

---

## Medium

### IT-011: Document companion/sync feature
**Description**: 7 source files in `src/main/scala/io/indextables/spark/sync/`, 14+ test files, `BUILD INDEXTABLES COMPANION` SQL command — entirely undocumented.
**Acceptance Criteria**:
- [ ] Feature documented in CLAUDE.md
- [ ] SQL command documented in `docs/reference/sql-commands.md`
- [ ] Usage examples in `docs/reference/features.md`

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

### IT-044: Testing excellence — fix exception-swallowing tests (Phase 0A)
**Type**: Tech Debt / Test Quality
**Priority**: Medium
**Analysis**: [.claude/artifacts/testing-excellence-review.md](artifacts/testing-excellence-review.md)
**Description**: StorageErrorHandlingTest had 7 tests using try/catch patterns that passed regardless of behavior. Root cause: `count()` bypasses split files via TransactionLogCountScan aggregate pushdown, and JVM-wide caches retain stale data from writes. Tests appeared to "handle errors gracefully" but tested nothing. SizeParsingTest had 1 edge-case test with weak assertions.
**Acceptance Criteria**:
- [x] Root cause identified (aggregate pushdown + cache masking)
- [x] Actual runtime behavior verified (all corruption scenarios throw exceptions, no partial results)
- [x] Tests rewritten with `select().collect()`, cache clearing, and `intercept[]` assertions
- [x] Peer review completed (2 iterations)
- [x] All 14 tests pass (12 StorageErrorHandlingTest + 2 SizeParsingTest)

### IT-045: setup.sh builds tantivy at wrong revision
**Type**: Bug
**Priority**: Medium
**Description**: `scripts/setup.sh` advances tantivy one commit past pinned rev (4b6d7d49 → 696b8477), which introduces a `BucketResult::Filter` variant that quickwit-query doesn't handle. Causes 3 Rust compilation errors (E0605, E0308, E0004). Workaround: build manually with exact pinned rev.
**Acceptance Criteria**:
- [x] setup.sh uses exact pinned rev without +1 advancement
- [ ] Build succeeds on clean checkout (needs verification on clean machine)

### IT-046: SyncToExternalCommand compile error on main
**Type**: Bug
**Priority**: High
**Description**: `SyncToExternalCommand.scala:426` passes `sourceSchema` (StructType) where `applyWhereFilter` expects `Option[StructType]`. Pre-existing on main, blocks compilation.
**Acceptance Criteria**:
- [x] Fixed: wrapped in `Some(sourceSchema)`

### IT-047: Avro state format corruption tests (coverage gap)
**Type**: Tech Debt / Test Quality
**Priority**: Low
**Description**: StorageErrorHandlingTest only tests JSON format transaction log corruption. Avro is the default since Protocol V4 and is more representative of production. Discovered during adversarial review of IT-044.
**Acceptance Criteria**:
- [ ] Equivalent corruption tests for Avro state format

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
