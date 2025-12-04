# ReplaceWhere Design Document

## Overview

ReplaceWhere is a Delta Lake feature that allows selective replacement of data in specific partitions based on predicate conditions. Instead of rewriting the entire table (overwrite) or adding to all partitions (append), replaceWhere enables targeted updates to only the partitions that match the specified predicates.

## Current State

IndexTables4Spark currently supports:
- **Overwrite mode**: Removes all existing files and replaces with new data
- **Append mode**: Adds new files alongside existing data
- **Partition tracking**: Transaction log captures partition values in ADD actions
- **Partition pruning**: Query optimization based on partition metadata

## Proposed Architecture

### Core Components

#### 1. ReplaceWhereAction
```scala
case class ReplaceWhereAction(
  predicate: String,              // SQL predicate string (e.g., "year=2023 AND month=12")
  timestamp: Long,                // When the operation occurred
  dataChange: Boolean = true,     // Always true for replace operations
  operationMetrics: Option[Map[String, String]] = None
)
```

#### 2. Predicate Parser
```scala
class PartitionPredicateParser {
  def parsePredicates(predicateStr: String, partitionColumns: Seq[String]): Seq[PartitionPredicate]
  def validatePredicates(predicates: Seq[PartitionPredicate], schema: StructType): Unit
  def matchesPartition(predicates: Seq[PartitionPredicate], partitionValues: Map[String, String]): Boolean
}

sealed trait PartitionPredicate
case class EqualsPredicate(column: String, value: String) extends PartitionPredicate
case class InPredicate(column: String, values: Seq[String]) extends PartitionPredicate
case class AndPredicate(left: PartitionPredicate, right: PartitionPredicate) extends PartitionPredicate
```

#### 3. TransactionLog Enhancement
```scala
def replaceWhere(addActions: Seq[AddAction], predicate: String): Long = {
  val partitionColumns = getPartitionColumns()
  if (partitionColumns.isEmpty) {
    throw new IllegalArgumentException("replaceWhere requires a partitioned table")
  }
  
  val predicates = PartitionPredicateParser.parsePredicates(predicate, partitionColumns)
  val existingFiles = listFiles()
  
  // Find files to remove based on partition predicate
  val filesToRemove = existingFiles.filter { file =>
    PartitionPredicateParser.matchesPartition(predicates, file.partitionValues)
  }
  
  val removeActions = filesToRemove.map(createRemoveAction)
  val replaceWhereAction = ReplaceWhereAction(predicate, System.currentTimeMillis())
  
  val version = getLatestVersion() + 1
  val allActions = Seq(replaceWhereAction) ++ removeActions ++ addActions
  writeActions(version, allActions)
  
  logger.info(s"ReplaceWhere operation: removed ${removeActions.length} files, added ${addActions.length} files matching predicate: $predicate")
  version
}
```

### API Design

#### DataFrame Writer API
```scala
// User-facing API
df.write
  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .mode("replaceWhere")
  .option("replaceWhere", "year=2023 AND month IN ('11', '12')")
  .save("/path/to/table")
```

#### Internal DataSource Integration
```scala
// In IndexTables4SparkDataSource
override def createWriter(
    jobId: String,
    schema: StructType,
    mode: SaveMode,
    options: CaseInsensitiveStringMap): DataSourceWriter = {
    
  val replaceWhereOption = options.get("replaceWhere")
  
  mode match {
    case SaveMode.Append if replaceWhereOption != null =>
      new IndexTables4SparkReplaceWhereWriter(path, schema, replaceWhereOption, options)
    case _ =>
      // Existing logic for other modes
  }
}
```

### Validation Rules

1. **Partitioned Tables Only**: ReplaceWhere only works on partitioned tables
2. **Partition Column Predicates**: Predicates must reference only partition columns
3. **Supported Operators**: `=`, `IN`, `AND` (no `OR`, `NOT`, or complex expressions)
4. **Type Safety**: Predicate values must match partition column data types
5. **Schema Compatibility**: New data schema must be compatible with existing table schema

### Error Handling

```scala
sealed trait ReplaceWhereError extends Exception
case class NonPartitionedTableError(tablePath: String) extends ReplaceWhereError
case class InvalidPredicateError(predicate: String, reason: String) extends ReplaceWhereError
case class UnsupportedOperatorError(operator: String) extends ReplaceWhereError
case class PartitionColumnNotFoundError(column: String, availableColumns: Seq[String]) extends ReplaceWhereError
case class TypeMismatchError(column: String, expectedType: String, actualType: String) extends ReplaceWhereError
```

### Transaction Log Format

ReplaceWhere operations will be recorded in the transaction log with this format:

```json
{"replaceWhere": {"predicate": "year=2023 AND month=12", "timestamp": 1703123456789, "dataChange": true}}
{"remove": {"path": "year=2023/month=12/part-00001.split", "deletionTimestamp": 1703123456789, "dataChange": true, "partitionValues": {"year": "2023", "month": "12"}}}
{"add": {"path": "year=2023/month=12/part-00003.split", "partitionValues": {"year": "2023", "month": "12"}, "size": 1048576, "numRecords": 25000}}
```

## Implementation Plan

### Phase 1: Core Infrastructure
1. Implement `ReplaceWhereAction` case class
2. Create `PartitionPredicateParser` with support for `=` and `AND`
3. Add `replaceWhere()` method to `TransactionLog`
4. Unit tests for predicate parsing and partition matching

### Phase 2: DataSource Integration
1. Extend `IndexTables4SparkDataSource` to recognize replaceWhere mode
2. Create `IndexTables4SparkReplaceWhereWriter` 
3. Add validation for partitioned tables and predicate format
4. Integration tests with simple partition predicates

### Phase 3: Advanced Features
1. Add support for `IN` predicates
2. Implement comprehensive error handling and validation
3. Add SQL metrics for monitoring replace operations
4. Performance optimization for large partition pruning

### Phase 4: Testing & Documentation
1. End-to-end integration tests covering various scenarios
2. Performance benchmarks comparing to overwrite mode
3. User documentation and examples
4. Error message improvements

## Testing Strategy

### Unit Tests
- Predicate parsing for valid/invalid expressions
- Partition matching logic with various data types
- Transaction log serialization/deserialization
- Error handling for edge cases

### Integration Tests
```scala
test("should replace specific partitions with replaceWhere") {
  // Create partitioned table with multiple years/months
  val initialData = spark.range(1000).select(
    $"id",
    ($"id" % 3).cast("string").as("year"),
    ($"id" % 2).cast("string").as("month"),
    lit("initial").as("data")
  )
  
  initialData.write
    .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
    .partitionBy("year", "month")
    .save(tablePath)
    
  // Replace specific partition
  val newData = spark.range(100, 200).select(
    $"id",
    lit("1").as("year"),
    lit("0").as("month"), 
    lit("replaced").as("data")
  )
  
  newData.write
    .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
    .mode("replaceWhere")
    .option("replaceWhere", "year=1 AND month=0")
    .save(tablePath)
    
  // Verify only the specified partition was replaced
  val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
  val year1Month0Count = result.filter($"year" === "1" && $"month" === "0" && $"data" === "replaced").count()
  val otherPartitionsCount = result.filter(!($"year" === "1" && $"month" === "0")).count()
  
  year1Month0Count shouldBe 100
  otherPartitionsCount shouldBe > 0L  // Other partitions unchanged
}
```

## Performance Considerations

1. **Partition Pruning**: Leverage existing min/max statistics for efficient file selection
2. **Parallel Processing**: Remove and add operations can be parallelized
3. **Memory Usage**: Stream processing for large partition replacements
4. **I/O Optimization**: Reuse S3OptimizedReader patterns for cloud storage

## Compatibility

- **Delta Lake**: Follow Delta Lake's replaceWhere semantics for consistency
- **Spark Versions**: Compatible with Spark 3.0+ DataSource V2 API
- **Storage**: Works with all supported storage backends (local, HDFS, S3)
- **Existing Tables**: Can be applied to any partitioned IndexTables4Spark table

## Limitations

1. **Partition Columns Only**: Predicates must reference partition columns exclusively
2. **Simple Operators**: No support for complex expressions, functions, or subqueries  
3. **No Cross-Partition Dependencies**: Cannot reference data across multiple partitions
4. **Single Transaction**: Entire replaceWhere operation must complete atomically

## Future Enhancements

- Support for `OR` conditions in predicates
- Range-based partition predicates (`year BETWEEN 2022 AND 2024`)
- Partition-level statistics for more efficient pruning
- Integration with Spark's dynamic partition pruning
- Batch replaceWhere operations across multiple predicates