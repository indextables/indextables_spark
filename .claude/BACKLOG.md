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

### IT-008: Document missing SQL commands
**Description**: 8 commands exist in ANTLR grammar but are absent from docs: `BUILD COMPANION`, `DESCRIBE COMPONENT SIZES`, `DESCRIBE PREWARM JOBS`, `WAIT FOR PREWARM JOBS`, `DESCRIBE TRANSACTION LOG`, `REPAIR INDEXFILES TRANSACTION LOG`, `FLUSH SEARCHER CACHE`, `INVALIDATE TRANSACTION LOG CACHE`.
**Acceptance Criteria**:
- [ ] All grammar-wired commands documented in `docs/reference/sql-commands.md`
- [ ] CLAUDE.md SQL Extensions section updated

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
