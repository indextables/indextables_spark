> **Archived**: This document references the pre-rename `MATCHES` keyword.
> The final implementation uses `TEXTSEARCH` (tokenized) and `FIELDMATCH` (non-tokenized).
> See `TextSearchFieldMatchParserTest.scala` for current tests.

# MATCHES / * MATCHES SQL Syntax - Test Suite & Results

## Overview

Test suite validating the new `MATCHES` and `* MATCHES` SQL syntax for full-text search, alongside backward compatibility with legacy `indexquery` and `indexqueryall` syntax.

**New syntax:**
```sql
WHERE content MATCHES 'machine learning'   -- single column
WHERE * MATCHES 'error timeout'            -- all columns
```

**Legacy syntax (still supported):**
```sql
WHERE content indexquery 'machine learning'
WHERE indexqueryall('error timeout')
```

## Test File

`src/test/scala/io/indextables/spark/sql/MatchesSyntaxParserTest.scala`

Run with:
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.MatchesSyntaxParserTest'
```

## Test Categories

### 1. Parser-Level Tests (7 tests)
### 2. End-to-End Small Dataset Tests (2 tests)
### 3. Backward Compatibility Parser Tests (2 tests)
### 4. End-to-End All-Syntax Comparison (1 test, 3-row dataset)
### 5. End-to-End 100k-Row Scale Test (1 test, 100k-row dataset)
### 6. Compound WHERE Clause Tests (1 test, 6 sub-cases)

---

## Test Implementation

### Test 1: MATCHES Basic Parsing

**Objective**: Verify `MATCHES` parses to `IndexQueryExpression` with correct column and query extraction.

```scala
test("MATCHES should parse simple expression") {
  val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)
  val expr = parser.parseExpression("title MATCHES 'spark AND sql'")

  assert(expr.isInstanceOf[IndexQueryExpression])
  assert(expr.asInstanceOf[IndexQueryExpression].getColumnName.contains("title"))
  assert(expr.asInstanceOf[IndexQueryExpression].getQueryString.contains("spark AND sql"))
}
```

**Result**: PASS

---

### Test 2: MATCHES Case-Insensitivity

**Objective**: Verify `MATCHES`, `matches`, and `Matches` all parse identically.

```scala
test("MATCHES should be case-insensitive") {
  val parser = new IndexTables4SparkSqlParser(CatalystSqlParser)

  val expr1 = parser.parseExpression("title MATCHES 'query'")
  val expr2 = parser.parseExpression("title matches 'query'")
  val expr3 = parser.parseExpression("title Matches 'query'")

  // All three produce IndexQueryExpression with the same query string
}
```

**Result**: PASS

---

### Test 3: MATCHES with Backtick Columns

**Objective**: Verify backtick-quoted column names work with `MATCHES`.

```scala
test("MATCHES should handle backtick columns") {
  val expr = parser.parseExpression("`my column` MATCHES 'test'")
  assert(expr.isInstanceOf[IndexQueryExpression])
}
```

**Result**: PASS

---

### Test 4: MATCHES with Complex Tantivy Syntax

**Objective**: Verify boolean operators and parentheses in the query string are preserved.

```scala
test("MATCHES should handle complex Tantivy syntax") {
  val expr = parser.parseExpression(
    "content MATCHES '(apache AND spark) OR (hadoop AND mapreduce)'"
  )
  assert(expr.asInstanceOf[IndexQueryExpression]
    .getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
}
```

**Result**: PASS

---

### Test 5: MATCHES with Qualified Column Names

**Objective**: Verify dotted column names (e.g., `table.column`) work.

```scala
test("MATCHES should handle qualified column names") {
  val expr = parser.parseExpression("table.columnName MATCHES 'search term'")
  assert(expr.isInstanceOf[IndexQueryExpression])
}
```

**Result**: PASS

---

### Test 6: * MATCHES All-Fields Parsing

**Objective**: Verify `* MATCHES` parses to `IndexQueryAllExpression`.

```scala
test("* MATCHES should parse all-fields expression") {
  val expr = parser.parseExpression("* MATCHES 'error timeout'")

  assert(expr.isInstanceOf[IndexQueryAllExpression])
  assert(expr.asInstanceOf[IndexQueryAllExpression].getQueryString.contains("error timeout"))
}
```

**Result**: PASS

---

### Test 7: * MATCHES Case-Insensitivity

**Objective**: Verify `* MATCHES`, `* matches`, and `* Matches` all parse identically.

**Result**: PASS

---

### Test 8: End-to-End `WHERE col MATCHES 'query'`

**Objective**: Verify MATCHES works through the full Spark SQL pipeline against a real IndexTables4Spark dataset.

```scala
test("End-to-end: WHERE col MATCHES 'query' via spark.sql") {
  // Write 3 rows, query with: WHERE title MATCHES 'spark'
  // Assert results contain the matching document
}
```

**Result**: PASS - Returned 1 row: `id=1, title='apache spark documentation'`

---

### Test 9: End-to-End `WHERE * MATCHES 'query'`

**Objective**: Verify `* MATCHES` works through the full Spark SQL pipeline.

**Result**: PASS - Returned 1 row: `id=1, title='apache spark documentation'`

---

### Test 10: Legacy `indexquery` Backward Compatibility

**Objective**: Verify legacy `indexquery` still parses correctly.

```scala
test("Legacy indexquery syntax should still work") {
  val expr = parser.parseExpression("title indexquery 'spark AND sql'")
  assert(expr.isInstanceOf[IndexQueryExpression])
}
```

**Result**: PASS

---

### Test 11: Legacy `indexqueryall` Backward Compatibility

**Objective**: Verify legacy `indexqueryall()` still parses correctly.

```scala
test("Legacy indexqueryall syntax should still work") {
  val expr = parser.parseExpression("indexqueryall('spark AND sql')")
  assert(expr.isInstanceOf[IndexQueryAllExpression])
}
```

**Result**: PASS

---

### Test 12: All 4 Syntax Variants (3-Row Dataset)

**Objective**: Verify all 4 syntax variants produce identical results against the same dataset.

**Dataset**: 3 rows

| id | title | category |
|----|-------|----------|
| 1 | apache spark documentation | technology |
| 2 | machine learning algorithms | ai |
| 3 | big data processing | technology |

**Results**:

| Syntax | SQL | Rows | IDs |
|--------|-----|------|-----|
| `MATCHES` | `WHERE title MATCHES 'spark'` | 1 | {1} |
| `* MATCHES` | `WHERE * MATCHES 'spark'` | 1 | {1} |
| `indexquery` | `WHERE title indexquery 'spark'` | 1 | {1} |
| `indexqueryall` | `WHERE indexqueryall('spark')` | 1 | {1} |

**Cross-check**:
- `MATCHES` ids == `indexquery` ids: **true**
- `* MATCHES` ids == `indexqueryall` ids: **true**

**Result**: PASS

---

### Test 13: All Syntax Variants at 100k-Row Scale

**Objective**: Verify all syntax variants produce identical results at scale with multiple query types.

**Dataset**: 100,000 rows generated from 10 rotating titles and 5 categories.

**Configuration**: `spark.indextables.read.defaultLimit` set to `200000` and data written with `repartition(1)` to ensure all rows are returned.

#### Search 1: Simple term `'spark'`

| Syntax | Rows Returned | Time |
|--------|---------------|------|
| `title MATCHES 'spark'` | 10,000 | 558ms |
| `* MATCHES 'spark'` | 10,000 | 559ms |
| `title indexquery 'spark'` | 10,000 | 526ms |
| `indexqueryall('spark')` | 10,000 | 475ms |

- `MATCHES` == `indexquery`: **true** (identical row sets)
- `* MATCHES` == `indexqueryall`: **true** (identical row sets)
- All returned rows contain "spark" in title: **verified**
- Row count matches expected (1 in 10 titles contain "spark"): **verified**

Sample output:
```
id=10, title='apache spark documentation'
id=20, title='apache spark documentation'
id=30, title='apache spark documentation'
id=40, title='apache spark documentation'
id=50, title='apache spark documentation'
```

#### Search 2: Boolean query `'learning AND neural'`

| Syntax | Rows Returned |
|--------|---------------|
| `title MATCHES 'learning AND neural'` | 10,000 |
| `title indexquery 'learning AND neural'` | 10,000 |

- Identical result sets: **true**

#### Search 3: Phrase query `'"machine learning"'`

| Syntax | Rows Returned |
|--------|---------------|
| `title MATCHES '"machine learning"'` | 10,000 |
| `title indexquery '"machine learning"'` | 10,000 |

- Identical result sets: **true**
- Row count matches expected (1 in 10 titles = "machine learning algorithms"): **verified**

**Result**: PASS

---

### Test 14: MATCHES in Compound WHERE Clauses

**Objective**: Verify MATCHES and * MATCHES work correctly when combined with other filters using AND/OR.

**Dataset**: 5 rows

| id | title | category |
|----|-------|----------|
| 1 | apache spark documentation | technology |
| 2 | machine learning algorithms | ai |
| 3 | spark streaming tutorial | technology |
| 4 | deep learning neural networks | ai |
| 5 | big data processing | technology |

#### Sub-case 1: `MATCHES AND` standard filter

```sql
WHERE title MATCHES 'spark' AND category = 'technology'
```

| id | title | category |
|----|-------|----------|
| 1 | apache spark documentation | technology |
| 3 | spark streaming tutorial | technology |

- Rows: 2
- All results have category=technology: **verified**
- All results match 'spark': **verified**
- `MATCHES AND` == `indexquery AND`: **true**

#### Sub-case 2: `MATCHES OR MATCHES`

```sql
WHERE title MATCHES 'spark' OR title MATCHES 'learning'
```

| id | title |
|----|-------|
| 1 | apache spark documentation |
| 2 | machine learning algorithms |
| 3 | spark streaming tutorial |
| 4 | deep learning neural networks |

- Rows: 4
- All results match 'spark' or 'learning': **verified**
- `MATCHES OR` == `indexquery OR`: **true**

#### Sub-case 3: `* MATCHES AND` standard filter

```sql
WHERE * MATCHES 'learning' AND category = 'ai'
```

| id | title | category |
|----|-------|----------|
| 2 | machine learning algorithms | ai |
| 4 | deep learning neural networks | ai |

- Rows: 2
- All results have category=ai: **verified**
- `* MATCHES AND` == `indexqueryall AND`: **true**

#### Sub-case 4: `MATCHES AND MATCHES`

```sql
WHERE title MATCHES 'spark' AND title MATCHES 'documentation'
```

| id | title |
|----|-------|
| 1 | apache spark documentation |

- Rows: 1
- Row 1 has both "spark" and "documentation": **verified**
- Row 3 ("spark streaming tutorial") correctly excluded (lacks "documentation"): **verified**
- `MATCHES AND MATCHES` == `indexquery AND indexquery`: **true**

#### Sub-case 5: `MATCHES OR` standard filter

```sql
WHERE title MATCHES 'spark' OR category = 'ai'
```

| id | title | category |
|----|-------|----------|
| 1 | apache spark documentation | technology |
| 2 | machine learning algorithms | ai |
| 3 | spark streaming tutorial | technology |
| 4 | deep learning neural networks | ai |

- Rows: 4
- Rows 1+3 match 'spark', rows 2+4 match category=ai: **verified**
- `MATCHES OR filter` == `indexquery OR filter`: **true**

#### Sub-case 6: `* MATCHES OR` standard filter

```sql
WHERE * MATCHES 'neural' OR category = 'technology'
```

| id | title | category |
|----|-------|----------|
| 1 | apache spark documentation | technology |
| 3 | spark streaming tutorial | technology |
| 4 | deep learning neural networks | ai |
| 5 | big data processing | technology |

- Rows: 4
- Row 4 matches 'neural', rows 1+3+5 match category=technology: **verified**
- `* MATCHES OR filter` == `indexqueryall OR filter`: **true**

**Result**: PASS

---

## Updated Test Files (Assertion Changes)

The following existing tests were updated to reflect the new `prettyName`, `sql`, and `toString` output:

| File | Changes |
|------|---------|
| `expressions/IndexQueryExpressionTest.scala` | `"indexquery"` -> `"matches"`, `contains("indexquery")` -> `contains("MATCHES")` |
| `expressions/IndexQueryAllExpressionTest.scala` | `"indexqueryall"` -> `"matches_all"`, `contains("indexqueryall")` -> `contains("MATCHES")` |
| `integration/IndexQueryAllSimpleTest.scala` | `"indexqueryall"` -> `"matches_all"`, `contains("indexqueryall")` -> `contains("MATCHES")` |
| `integration/IndexQueryIntegrationTest.scala` | `contains("indexquery")` -> `contains("MATCHES")` |

All existing tests continue to pass — SQL strings using legacy `indexquery` syntax are exercised for backward compatibility.

## Summary

```
Run completed in 14 seconds, 178 milliseconds.
Total number of tests run: 14
Suites: completed 2, aborted 0
Tests: succeeded 14, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
```

| Category | Tests | Status |
|----------|-------|--------|
| Parser: MATCHES | 5 | PASS |
| Parser: * MATCHES | 2 | PASS |
| End-to-end: MATCHES / * MATCHES | 2 | PASS |
| Backward compat: indexquery / indexqueryall | 2 | PASS |
| All-syntax comparison (3 rows) | 1 | PASS |
| All-syntax comparison (100k rows, 3 query types) | 1 | PASS |
| Compound WHERE clauses (6 sub-cases: AND, OR, * AND, AND+AND, OR+filter, * OR+filter) | 1 | PASS |
| **Total** | **14** | **ALL PASS** |
