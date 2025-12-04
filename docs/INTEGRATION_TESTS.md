# IndexTables4Spark Integration Tests

This document describes the comprehensive integration tests for IndexTables4Spark that validate the complete write/read/query cycle.

## Test Structure

### Full Integration Tests (`IndexTables4SparkFullIntegrationTest.scala`)

These tests require the native Tantivy JNI library to be built and available. When the native library is not available, the tests are automatically skipped using ScalaTest's `assume()` function.

#### Test Categories:

1. **Full Write/Read Cycle** - `should perform full write/read cycle with comprehensive test data`
   - Tests complete data round-trip integrity
   - Validates schema preservation  
   - Verifies row count consistency
   - Tests basic filtering on read data

2. **Text Search Operations** - `should handle text search queries with various operators`
   - `contains()` operations
   - `startsWith()` operations  
   - `endsWith()` operations
   - `rlike()` regex operations
   - Combined text + categorical filters
   - OR conditions with text search

3. **Numeric Range Queries** - `should handle numeric range queries`
   - Greater than (`>`) operations with min/max validation
   - Greater than or equal (`>=`) operations
   - Less than (`<`) operations  
   - Between range operations with boundary validation
   - Mixed integer, double, and long comparisons
   - Result set validation (ensures only matching rows returned)

4. **Equality and IN Queries** - `should handle equality and IN queries`
   - Exact equality (`===`) matching
   - IN clause operations (`isin()`)
   - NOT IN operations (`!col().isin()`)
   - Category validation (ensures only specified values returned)
   - Result set completeness validation

5. **Null/Non-Null Queries** - `should handle null/non-null queries`
   - `isNull()` operations with null value validation
   - `isNotNull()` operations with non-null validation
   - Complementary result validation (null + not-null = total count)

6. **Boolean Queries** - `should handle boolean queries`  
   - Boolean equality (`=== true/false`)
   - Combined boolean AND operations
   - Combined boolean OR operations
   - Logical consistency validation

7. **Date and Timestamp Queries** - `should handle date and timestamp queries`
   - Date range comparisons
   - Timestamp range operations
   - Date function operations (`year()`, `month()`, `dayofweek()`)
   - Temporal boundary validation

8. **Complex Compound Queries** - `should handle complex compound queries`
   - Multi-condition AND operations
   - Multi-condition OR operations  
   - Mixed AND/OR with text search
   - Nested conditional logic
   - Cross-field validation

9. **Aggregation Queries** - `should handle aggregation queries with filters`
   - COUNT operations with filters
   - GROUP BY with aggregations (`count()`, `avg()`, `max()`)
   - Multi-field grouping
   - Aggregation result validation

10. **Partitioned Data Operations** - `should handle partitioned data operations`
    - Partitioned writes (`partitionBy()`)
    - Partition-aware reads
    - Partition pruning validation

11. **Schema Evolution** - `should handle schema evolution scenarios`
    - Adding columns to existing datasets
    - Schema compatibility validation
    - New field query operations

12. **Comprehensive Integration** - `should complete full write/read/query cycle with comprehensive filters`
    - Tests all major query types on single dataset
    - Cross-validates different operation types
    - End-to-end query pipeline validation

## Test Data Generation

### Deterministic Data Sets
Each test uses deterministic data generation with fixed seeds to ensure reproducible results:

- **Text Search Data**: 5 rows with known content for precise text matching validation
- **Numeric Data**: 100 rows with controlled ranges for boundary testing  
- **Categorical Data**: 200 rows with known distribution across categories
- **Nullable Data**: 100 rows with controlled null distribution (~30-60% null rate)
- **Boolean Data**: 100 rows with random but reproducible boolean values
- **DateTime Data**: 100 rows with controlled timestamp ranges
- **Comprehensive Data**: 1000 rows with realistic employee data for complex queries

### Validation Strategy

Each test follows this pattern:
1. **Generate Data**: Create DataFrame with known characteristics
2. **Write Data**: Use `df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)`  
3. **Read Data**: Use `spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)`
4. **Execute Queries**: Run specific query types on read DataFrame
5. **Validate Results**: Assert that:
   - Only matching rows are returned (min/max/exact validation)
   - Result counts are reasonable (> 0 for expected matches)
   - Data values match expected criteria
   - Cross-field consistency is maintained

## Running the Tests

### When Native Library is Available:
```bash
mvn test -Dtest=IndexTables4SparkFullIntegrationTest
```
All integration tests will execute and validate query results.

### When Native Library is Not Available:
```bash  
mvn test -Dtest=IndexTables4SparkFullIntegrationTest
```
Integration tests will be skipped with "CANCELED" status, but compilation and structure are validated.

## Test Benefits

1. **Correctness Validation**: Ensures queries return only matching data
2. **Completeness Testing**: Validates that all expected results are returned  
3. **Boundary Testing**: Tests edge cases and range boundaries
4. **Integration Verification**: Tests full Spark DataSource V2 integration
5. **Regression Prevention**: Detects breaking changes in query behavior
6. **Performance Insight**: Provides baseline for query execution patterns

## Future Enhancements

When the native Tantivy library is implemented, these tests will:
- Provide immediate validation of JNI integration
- Catch query translation errors
- Validate index efficiency
- Ensure filter pushdown correctness
- Test transaction log integration
- Verify archive format compatibility