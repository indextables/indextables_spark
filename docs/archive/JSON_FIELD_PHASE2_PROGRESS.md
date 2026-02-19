# JSON Field Integration - Phase 2 Progress Report

**Date**: 2025-10-30
**Status**: In Progress - Schema Integration Complete ✅
**Completion**: ~30% of Phase 2

---

## Completed Tasks

### ✅ 1. Upgrade tantivy4java Dependency

**Action**: Built tantivy4java 0.25.2 locally and updated search_test project dependency
- Built `../tantivy4java` from source
- Updated `pom.xml` from version 0.25.1 → 0.25.2
- Verified JSON query methods available: `jsonTermQuery`, `jsonRangeQuery`, `jsonExistsQuery`
- **Result**: ✅ BUILD SUCCESS - All Phase 1 components now compile

**Files Modified**:
- `/Users/schenksj/tmp/x/search_test/pom.xml` (line 167-173)

---

### ✅ 2. Integrate SparkSchemaToTantivyMapper into Schema Creation

**Action**: Integrated JSON field mapper into `TantivyDirectInterface.createSchemaThreadSafe()`
- Added import for `SparkSchemaToTantivyMapper`
- Created mapper instance in schema creation function
- Added `validateJsonFieldConfiguration()` call before schema building
- Implemented `shouldUseJsonField()` check for automatic JSON detection
- Updated field processing to use `addJsonField()` for Struct/Array types
- Improved error messages for unsupported types

**Files Modified**:
- `src/main/scala/io/indextables/spark/search/IndexTablesDirectInterface.scala`
  - Line 31: Added import for `SparkSchemaToTantivyMapper`
  - Lines 167-170: Created mapper and validation
  - Lines 192-243: Updated field processing logic with JSON detection

**Key Changes**:
```scala
// Create JSON field mapper for automatic JSON field detection
val jsonFieldMapper = new SparkSchemaToTantivyMapper(finalTantivyOptions)

// Validate JSON field configuration before building schema
jsonFieldMapper.validateJsonFieldConfiguration(sparkSchema)

// Check if this field should use JSON field type
val shouldUseJson = jsonFieldMapper.shouldUseJsonField(field)

if (shouldUseJson) {
  // Use JSON field for Struct, Array, or explicitly configured StringType
  val tokenizer = indexingConfig.tokenizerOverride.getOrElse("default")
  builder.addJsonField(fieldName, stored, tokenizer, "position")
} else {
  // Handle non-JSON fields normally...
}
```

**Impact**:
- ✅ Struct fields automatically detected and mapped to JSON fields
- ✅ Array fields automatically detected and mapped to JSON fields
- ✅ StringType fields with "json" configuration mapped to JSON fields
- ✅ Schema validation prevents conflicting type configurations
- ✅ Better error messages for debugging

---

### ✅ 3. tantivy4java Schema Builder Integration

**Status**: Already integrated via `builder.addJsonField()` calls
- Schema creation now uses tantivy4java's `addJsonField()` method
- Automatic detection eliminates manual configuration for complex types
- Fast field support for JSON fields
- Tokenizer configuration for JSON fields

---

## Remaining Phase 2 Tasks

### 🔲 4. Integrate SparkToTantivyConverter into Write Path

**Status**: Not Started
**Location**: Document creation in `TantivyDirectInterface`
**Required Actions**:
1. Find where `Document.add*()` methods are called
2. Detect JSON fields during document creation
3. Use `SparkToTantivyConverter` to convert Row/Seq to Java Map/List
4. Call `Document.addJson(field, javaMap)` for JSON fields

**Files to Modify**:
- `src/main/scala/io/indextables/spark/search/IndexTablesDirectInterface.scala` (document creation methods)

---

### 🔲 5. Integrate TantivyToSparkConverter into Read Path

**Status**: Not Started
**Location**: Document reading in scan/reader classes
**Required Actions**:
1. Find where `Document.getFirst()` is called for field values
2. Detect JSON fields during document reading
3. Use `TantivyToSparkConverter` to convert Java Map/List to Spark Row/Seq
4. Integrate with `SchemaMapping.Read.convertDocument()`

**Files to Modify**:
- `src/main/scala/io/indextables/spark/schema/SchemaMapping.scala` (Read.convertDocument)
- Scan/reader classes that materialize rows from documents

---

### 🔲 6. Hook JsonPredicateTranslator into Filter Pushdown

**Status**: Not Started
**Location**: `FiltersToQueryConverter`
**Required Actions**:
1. Integrate `JsonPredicateTranslator` into filter conversion logic
2. Detect nested field predicates (e.g., `$"user.name" === "Alice"`)
3. Translate to JSON queries when applicable
4. Fall back to Spark filtering for unsupported predicates

**Files to Modify**:
- `src/main/scala/io/indextables/spark/core/FiltersToQueryConverter.scala`

---

### 🔲 7. Update SchemaMapping for JSON Field Types

**Status**: Not Started
**Location**: `SchemaMapping.scala`
**Required Actions**:
1. Update `isSupportedSparkType()` to accept Struct/Array types
2. Update `getSupportedTypes()` list
3. Add `sparkTypeToTantivyFieldType()` case for Struct/Array → JSON
4. Integrate converters into `Read.convertDocument()` for JSON fields

**Files to Modify**:
- `src/main/scala/io/indextables/spark/schema/SchemaMapping.scala`

---

## Technical Details

### Current Architecture Flow

**Write Path (Schema Creation) - ✅ DONE**:
1. User calls `df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(...)`
2. `TantivyDirectInterface.createSchemaThreadSafe()` is called
3. `SparkSchemaToTantivyMapper` detects JSON fields
4. Schema validates field configurations
5. `builder.addJsonField()` called for Struct/Array/JSON-configured fields
6. tantivy4java schema created with JSON field support

**Write Path (Document Creation) - 🔲 TODO**:
1. For each row in DataFrame
2. Create `Document` instance
3. **[TODO]** Detect JSON fields
4. **[TODO]** Use `SparkToTantivyConverter` to convert Spark data
5. **[TODO]** Call `document.addJson(field, javaMap)`
6. Write document to index

**Read Path (Document Reading) - 🔲 TODO**:
1. Query returns documents from tantivy4java
2. **[TODO]** Detect JSON fields
3. **[TODO]** Use `document.getFirst(fieldName)` to get Java Map
4. **[TODO]** Use `TantivyToSparkConverter` to convert to Spark types
5. Create InternalRow with converted values
6. Return to Spark

**Filter Pushdown - 🔲 TODO**:
1. Spark Catalyst generates filters
2. **[TODO]** `JsonPredicateTranslator` detects nested field predicates
3. **[TODO]** Translate to `jsonTermQuery`, `jsonRangeQuery`, etc.
4. Push down to tantivy4java
5. Execute query

---

## Testing Strategy

### Unit Tests (Phase 1) - ✅ Complete
- ✅ JsonSchemaConversionTest (18 tests)
- ✅ JsonDataConversionTest (20 tests)
- ✅ JsonPredicatePushdownTest (15 tests)

### Integration Tests (Phase 2) - 🔲 Planned
- 🔲 End-to-end Struct field write/read test
- 🔲 End-to-end Array field write/read test
- 🔲 JSON string field write/read test
- 🔲 Nested struct write/read test
- 🔲 Array of structs write/read test
- 🔲 Filter pushdown on nested fields test
- 🔲 Round-trip data integrity test
- 🔲 Null handling test
- 🔲 Performance benchmark test

---

## Next Steps - Priority Order

### Immediate (Complete Phase 2 Week 3)

1. **Integrate Write Converter** (Highest Priority)
   - Find document creation code in `TantivyDirectInterface`
   - Add `SparkToTantivyConverter` usage
   - Handle JSON field data conversion during document creation

2. **Integrate Read Converter**
   - Update `SchemaMapping.Read.convertDocument()`
   - Add JSON field detection and conversion
   - Handle nested structures in read path

3. **Write First Integration Test**
   - Simple Struct field write/read test
   - Verify schema creation, data writing, data reading
   - Validate round-trip conversion

### Follow-up (Complete Phase 2 Week 4)

4. **Filter Pushdown Integration**
   - Hook `JsonPredicateTranslator` into `FiltersToQueryConverter`
   - Test nested field predicates

5. **Comprehensive Testing**
   - All integration test suites
   - Performance benchmarks

6. **Documentation**
   - Update CLAUDE.md with JSON field usage
   - Add examples for Struct/Array fields

---

## Risks & Mitigation

### Risk 1: Document Creation Complexity
**Risk**: Document creation code may be complex with multiple paths
**Mitigation**: Start with simple test case, iterate incrementally

### Risk 2: Schema Version Compatibility
**Risk**: Existing tables may not have JSON field metadata
**Mitigation**: Schema validation catches conflicts, existing tables continue to work

### Risk 3: Performance Impact
**Risk**: JSON conversion may add overhead
**Mitigation**: Measure with benchmarks, optimize if needed

---

## Files Modified Summary

**Modified (2)**:
1. `pom.xml` - Updated tantivy4java version to 0.25.2
2. `IndexTablesDirectInterface.scala` - Integrated JSON field mapper into schema creation

**Not Modified Yet (3)**:
1. `SchemaMapping.scala` - Needs JSON field type support
2. `FiltersToQueryConverter.scala` - Needs predicate translator integration
3. Document creation/reading code - Needs converter integration

---

## Compilation Status

✅ **All code compiles successfully**
- tantivy4java 0.25.2 built and installed
- Phase 1 components compile
- Schema integration compiles
- No errors, only warnings (existing codebase warnings)

---

## Conclusion

**Phase 2 Progress**: ~30% Complete

**Completed**:
- ✅ tantivy4java upgrade
- ✅ Schema mapper integration
- ✅ Schema builder JSON field support

**Remaining**:
- 🔲 Write converter integration (~3-4 hours estimated)
- 🔲 Read converter integration (~3-4 hours estimated)
- 🔲 Filter pushdown integration (~2-3 hours estimated)
- 🔲 Comprehensive testing (~4-6 hours estimated)

**Total Estimated Time Remaining**: 12-17 hours for full Phase 2 completion

**Status**: On track, good progress, clear path forward

---

**Next Action**: Integrate `SparkToTantivyConverter` into document write path

