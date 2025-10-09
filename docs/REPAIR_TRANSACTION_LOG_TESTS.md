# REPAIR INDEXFILES TRANSACTION LOG - Test Suite

## Overview
Comprehensive test suite validating that repaired transaction logs are functionally equivalent to original logs and that tables remain fully readable after replacement.

## Test Categories

### 1. Basic Repair & Replace Tests
### 2. Missing Split File Handling
### 3. Checkpoint Reconstruction Tests
### 4. Partition Preservation Tests
### 5. Multiple Transaction Consolidation Tests
### 6. Edge Cases & Error Handling

---

## Test Implementation

### Test 1: Basic Repair with Healthy Transaction Log

**Objective**: Verify repaired transaction log produces identical read results as original.

```scala
test("repaired transaction log should produce identical results after replacement") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create table with sample data
    val data = Seq(
      ("doc1", "content one", 100),
      ("doc2", "content two", 200),
      ("doc3", "content three", 300)
    ).toDF("id", "content", "score")

    data.write.format("indextables")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Read original table and collect results
    val originalDf = spark.read.format("indextables").load(tablePath)
    val originalResults = originalDf.orderBy("id").collect()
    val originalCount = originalDf.count()

    // 3. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    // 4. Validate repair statistics
    assert(repairResult.length === 1)
    assert(repairResult(0).getAs[String]("status") === "SUCCESS")
    assert(repairResult(0).getAs[Int]("missing_splits") === 0)
    val validSplits = repairResult(0).getAs[Int]("valid_splits")
    assert(validSplits > 0)

    // 5. Replace original transaction log with repaired version
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")

    // Backup and replace
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")
    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 6. Read table with repaired transaction log
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedResults = repairedDf.orderBy("id").collect()
    val repairedCount = repairedDf.count()

    // 7. Validate results are identical
    assert(repairedCount === originalCount)
    assert(repairedResults.length === originalResults.length)

    repairedResults.zip(originalResults).foreach { case (repaired, original) =>
      assert(repaired.getAs[String]("id") === original.getAs[String]("id"))
      assert(repaired.getAs[String]("content") === original.getAs[String]("content"))
      assert(repaired.getAs[Int]("score") === original.getAs[Int]("score"))
    }

    // 8. Validate checkpoint was created
    val checkpointFile = new Path(originalLogPath, "000000000000000001.checkpoint.json")
    assert(fs.exists(checkpointFile), "Checkpoint file should exist")

    val lastCheckpointFile = new Path(originalLogPath, "_last_checkpoint")
    assert(fs.exists(lastCheckpointFile), "_last_checkpoint file should exist")
  }
}
```

---

### Test 2: Repair with Missing Split Files

**Objective**: Verify repaired log excludes missing splits and table remains readable.

```scala
test("repaired transaction log should exclude missing splits and remain readable") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create table with multiple splits
    val data = (1 to 100).map(i => (s"doc$i", s"content $i", i))
      .toDF("id", "content", "score")

    data.repartition(5).write.format("indextables")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Read original count
    val originalCount = spark.read.format("indextables").load(tablePath).count()
    assert(originalCount === 100)

    // 3. Delete 2 split files to simulate missing files
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val splitFiles = fs.listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
    assert(splitFiles.length >= 2, "Should have at least 2 split files")

    val deletedSplits = splitFiles.take(2)
    deletedSplits.foreach { file =>
      fs.delete(file.getPath, false)
      println(s"Deleted split file: ${file.getPath}")
    }

    // 4. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    // 5. Validate repair detected missing splits
    assert(repairResult.length === 1)
    assert(repairResult(0).getAs[String]("status") === "SUCCESS")
    val missingSplits = repairResult(0).getAs[Int]("missing_splits")
    assert(missingSplits === 2, "Should report 2 missing splits")

    val validSplits = repairResult(0).getAs[Int]("valid_splits")
    val totalSplits = repairResult(0).getAs[Int]("total_splits")
    assert(validSplits === totalSplits - 2)

    // 6. Replace transaction log
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")

    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 7. Read table with repaired transaction log (should succeed)
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedCount = repairedDf.count()

    // 8. Validate reduced count (fewer documents due to missing splits)
    assert(repairedCount < originalCount, "Count should be less after excluding missing splits")
    assert(repairedCount > 0, "Should still have readable data")

    // 9. Validate all remaining data is readable
    val results = repairedDf.select("id", "content", "score").collect()
    assert(results.nonEmpty)
    results.foreach { row =>
      assert(row.getAs[String]("id").startsWith("doc"))
      assert(row.getAs[String]("content").startsWith("content"))
      assert(row.getAs[Int]("score") > 0)
    }
  }
}
```

---

### Test 3: Repair with Multiple Transactions and Checkpoint

**Objective**: Verify repair consolidates multiple transactions correctly.

```scala
test("repaired transaction log should consolidate multiple append transactions") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create initial table
    val data1 = Seq(("doc1", "content1", 100)).toDF("id", "content", "score")
    data1.write.format("indextables")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Append more data (transaction 2)
    val data2 = Seq(("doc2", "content2", 200)).toDF("id", "content", "score")
    data2.write.format("indextables").mode("append").save(tablePath)

    // 3. Append more data (transaction 3)
    val data3 = Seq(("doc3", "content3", 300)).toDF("id", "content", "score")
    data3.write.format("indextables").mode("append").save(tablePath)

    // 4. Verify original table has 3 documents
    val originalDf = spark.read.format("indextables").load(tablePath)
    val originalCount = originalDf.count()
    assert(originalCount === 3)
    val originalResults = originalDf.orderBy("id").collect()

    // 5. Check transaction log has multiple files
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val txnFiles = fs.listStatus(originalLogPath)
      .filter(_.getPath.getName.matches("\\d{20}\\.json"))
    assert(txnFiles.length >= 3, "Should have at least 3 transaction files")

    // 6. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(repairResult(0).getAs[String]("status") === "SUCCESS")
    assert(repairResult(0).getAs[Int]("valid_splits") === 3)

    // 7. Verify repaired log is consolidated (2 files + checkpoint + _last_checkpoint)
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val repairedFiles = fs.listStatus(repairedLogPath)
    val repairedTxnFiles = repairedFiles.filter(_.getPath.getName.matches("\\d{20}\\.json"))
    assert(repairedTxnFiles.length === 2, "Should have exactly 2 transaction files (metadata + adds)")

    val checkpointFile = repairedFiles.find(_.getPath.getName.endsWith(".checkpoint.json"))
    assert(checkpointFile.isDefined, "Should have checkpoint file")

    val lastCheckpoint = repairedFiles.find(_.getPath.getName === "_last_checkpoint")
    assert(lastCheckpoint.isDefined, "Should have _last_checkpoint file")

    // 8. Replace transaction log
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")
    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 9. Read table with consolidated transaction log
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedCount = repairedDf.count()
    val repairedResults = repairedDf.orderBy("id").collect()

    // 10. Validate identical results
    assert(repairedCount === originalCount)
    assert(repairedResults.length === 3)

    repairedResults.zip(originalResults).foreach { case (repaired, original) =>
      assert(repaired.getAs[String]("id") === original.getAs[String]("id"))
      assert(repaired.getAs[String]("content") === original.getAs[String]("content"))
      assert(repaired.getAs[Int]("score") === original.getAs[Int]("score"))
    }
  }
}
```

---

### Test 4: Repair Partitioned Table

**Objective**: Verify partition structure is preserved after repair.

```scala
test("repaired transaction log should preserve partition structure") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create partitioned table
    val data = Seq(
      ("2024-01-01", 10, "doc1", "content1", 100),
      ("2024-01-01", 11, "doc2", "content2", 200),
      ("2024-01-02", 10, "doc3", "content3", 300),
      ("2024-01-02", 11, "doc4", "content4", 400)
    ).toDF("load_date", "load_hour", "id", "content", "score")

    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("load_date", "load_hour")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Read original with partition pruning
    val originalDf = spark.read.format("indextables").load(tablePath)
    val filteredOriginal = originalDf
      .filter($"load_date" === "2024-01-01" && $"load_hour" === 10)
    val originalCount = filteredOriginal.count()
    assert(originalCount === 1)

    // 3. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(repairResult(0).getAs[String]("status") === "SUCCESS")
    assert(repairResult(0).getAs[Int]("valid_splits") === 4)

    // 4. Replace transaction log
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")

    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 5. Read table with repaired transaction log
    val repairedDf = spark.read.format("indextables").load(tablePath)

    // 6. Validate partition pruning still works
    val filteredRepaired = repairedDf
      .filter($"load_date" === "2024-01-01" && $"load_hour" === 10)
    val repairedCount = filteredRepaired.count()
    assert(repairedCount === 1)

    val result = filteredRepaired.collect()(0)
    assert(result.getAs[String]("id") === "doc1")
    assert(result.getAs[String]("load_date") === "2024-01-01")
    assert(result.getAs[Int]("load_hour") === 10)

    // 7. Validate all partitions are accessible
    val allPartitions = repairedDf.select("load_date", "load_hour").distinct().collect()
    assert(allPartitions.length === 4, "Should have 4 partition combinations")

    // 8. Validate total count matches
    assert(repairedDf.count() === 4)
  }
}
```

---

### Test 5: Repair After Overwrite Operation

**Objective**: Verify repair handles overwrite transaction boundaries correctly.

```scala
test("repaired transaction log should respect overwrite boundaries") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create initial table
    val data1 = Seq(("old1", "old content", 100)).toDF("id", "content", "score")
    data1.write.format("indextables")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Append data
    val data2 = Seq(("old2", "old content 2", 200)).toDF("id", "content", "score")
    data2.write.format("indextables").mode("append").save(tablePath)

    // 3. Overwrite table (should make old1/old2 invisible)
    val data3 = Seq(
      ("new1", "new content", 300),
      ("new2", "new content 2", 400)
    ).toDF("id", "content", "score")
    data3.write.format("indextables").mode("overwrite").save(tablePath)

    // 4. Verify only new data is visible
    val originalDf = spark.read.format("indextables").load(tablePath)
    val originalCount = originalDf.count()
    assert(originalCount === 2)

    val originalIds = originalDf.select("id").collect().map(_.getString(0)).sorted
    assert(originalIds === Array("new1", "new2"))

    // 5. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(repairResult(0).getAs[String]("status") === "SUCCESS")
    val validSplits = repairResult(0).getAs[Int]("valid_splits")
    assert(validSplits === 2, "Should only include splits from overwrite, not old splits")

    // 6. Replace transaction log
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")

    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 7. Read table with repaired transaction log
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedCount = repairedDf.count()
    assert(repairedCount === 2)

    val repairedIds = repairedDf.select("id").collect().map(_.getString(0)).sorted
    assert(repairedIds === Array("new1", "new2"))

    // 8. Validate old data is NOT accessible
    val allIds = repairedDf.select("id").collect().map(_.getString(0))
    assert(!allIds.contains("old1"))
    assert(!allIds.contains("old2"))
  }
}
```

---

### Test 6: Repair with Query Operations

**Objective**: Verify IndexQuery operations work correctly after repair.

```scala
test("repaired transaction log should support IndexQuery operations") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create table with text fields
    val data = Seq(
      ("doc1", "apache spark machine learning", 100),
      ("doc2", "python pandas data science", 200),
      ("doc3", "spark sql and dataframes", 300)
    ).toDF("id", "content", "score")

    data.write.format("indextables")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Test IndexQuery on original table
    val originalDf = spark.read.format("indextables").load(tablePath)
    val originalSparkQuery = originalDf
      .filter($"content" indexquery "spark")
      .select("id")
      .collect()
      .map(_.getString(0))
      .sorted

    assert(originalSparkQuery === Array("doc1", "doc3"))

    // 3. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(repairResult(0).getAs[String]("status") === "SUCCESS")

    // 4. Replace transaction log
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")

    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 5. Test IndexQuery on repaired table
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedSparkQuery = repairedDf
      .filter($"content" indexquery "spark")
      .select("id")
      .collect()
      .map(_.getString(0))
      .sorted

    assert(repairedSparkQuery === Array("doc1", "doc3"))

    // 6. Test complex IndexQuery
    val complexQuery = repairedDf
      .filter($"content" indexquery "spark AND (sql OR machine)")
      .select("id")
      .collect()
      .map(_.getString(0))
      .sorted

    assert(complexQuery === Array("doc1", "doc3"))

    // 7. Test _indexall query
    val indexAllQuery = repairedDf
      .filter($"_indexall" indexquery "python OR dataframes")
      .select("id")
      .collect()
      .map(_.getString(0))
      .sorted

    assert(indexAllQuery === Array("doc2", "doc3"))
  }
}
```

---

### Test 7: Repair with Aggregate Operations

**Objective**: Verify aggregate pushdown works after repair.

```scala
test("repaired transaction log should support aggregate pushdown") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create table with numeric fast field
    val data = Seq(
      ("doc1", "content1", 100),
      ("doc2", "content2", 200),
      ("doc3", "content3", 300),
      ("doc4", "content4", 400)
    ).toDF("id", "content", "score")

    data.write.format("indextables")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tablePath)

    // 2. Test aggregations on original table
    val originalDf = spark.read.format("indextables").load(tablePath)
    val originalCount = originalDf.count()
    val originalSum = originalDf.agg(sum("score")).collect()(0).getLong(0)
    val originalAvg = originalDf.agg(avg("score")).collect()(0).getDouble(0)

    assert(originalCount === 4)
    assert(originalSum === 1000)
    assert(originalAvg === 250.0)

    // 3. Run repair command
    val repairResult = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(repairResult(0).getAs[String]("status") === "SUCCESS")

    // 4. Replace transaction log
    val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
    val originalLogPath = new Path(tablePath, "_transaction_log")
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    val backupPath = new Path(tempDir.getAbsolutePath, "_transaction_log_backup")

    fs.rename(originalLogPath, backupPath)
    fs.rename(repairedLogPath, originalLogPath)

    // 5. Test aggregations on repaired table
    val repairedDf = spark.read.format("indextables").load(tablePath)
    val repairedCount = repairedDf.count()
    val repairedSum = repairedDf.agg(sum("score")).collect()(0).getLong(0)
    val repairedAvg = repairedDf.agg(avg("score")).collect()(0).getDouble(0)

    assert(repairedCount === originalCount)
    assert(repairedSum === originalSum)
    assert(repairedAvg === originalAvg)

    // 6. Test multiple aggregations
    val multiAgg = repairedDf.agg(
      count("*").as("count"),
      sum("score").as("sum"),
      avg("score").as("avg"),
      min("score").as("min"),
      max("score").as("max")
    ).collect()(0)

    assert(multiAgg.getAs[Long]("count") === 4)
    assert(multiAgg.getAs[Long]("sum") === 1000)
    assert(multiAgg.getAs[Double]("avg") === 250.0)
    assert(multiAgg.getAs[Int]("min") === 100)
    assert(multiAgg.getAs[Int]("max") === 400)
  }
}
```

---

### Test 8: Error Handling - Target Already Exists

**Objective**: Verify repair rejects overwrites of existing target directories.

```scala
test("repair should reject target location that already exists") {
  withTempDir { tempDir =>
    val tablePath = new File(tempDir, "test_table").getAbsolutePath
    val repairedPath = new File(tempDir, "test_table_repaired").getAbsolutePath

    // 1. Create original table
    val data = Seq(("doc1", "content1", 100)).toDF("id", "content", "score")
    data.write.format("indextables").save(tablePath)

    // 2. Create existing target directory with content
    val fs = new Path(repairedPath).getFileSystem(spark.sessionState.newHadoopConf())
    val repairedLogPath = new Path(repairedPath, "_transaction_log")
    fs.mkdirs(repairedLogPath)

    // Write dummy file to make directory non-empty
    val dummyFile = new Path(repairedLogPath, "dummy.txt")
    val out = fs.create(dummyFile)
    out.writeBytes("dummy content")
    out.close()

    // 3. Attempt repair (should fail)
    val exception = intercept[Exception] {
      spark.sql(
        s"""REPAIR INDEXFILES TRANSACTION LOG '$tablePath/_transaction_log'
            AT LOCATION '$repairedPath/_transaction_log'"""
      ).collect()
    }

    assert(exception.getMessage.contains("already exists") ||
           exception.getMessage.contains("not empty"))
  }
}
```

---

### Test 9: Error Handling - Source Not Found

**Objective**: Verify repair handles missing source transaction log gracefully.

```scala
test("repair should handle missing source transaction log gracefully") {
  withTempDir { tempDir =>
    val nonExistentPath = new File(tempDir, "does_not_exist").getAbsolutePath
    val repairedPath = new File(tempDir, "repaired").getAbsolutePath

    // Attempt repair on non-existent source
    val result = spark.sql(
      s"""REPAIR INDEXFILES TRANSACTION LOG '$nonExistentPath/_transaction_log'
          AT LOCATION '$repairedPath/_transaction_log'"""
    ).collect()

    assert(result.length === 1)
    val status = result(0).getAs[String]("status")
    assert(status.startsWith("ERROR"))
    assert(status.contains("not found") || status.contains("does not exist"))
  }
}
```

---

## Test Execution Plan

### Phase 1: Basic Functionality
- ✅ Test 1: Basic repair with healthy log
- ✅ Test 3: Multiple transaction consolidation
- ✅ Test 4: Partitioned table preservation

### Phase 2: Resilience & Recovery
- ✅ Test 2: Missing split file handling
- ✅ Test 5: Overwrite boundary respect

### Phase 3: Feature Validation
- ✅ Test 6: IndexQuery operations
- ✅ Test 7: Aggregate pushdown

### Phase 4: Error Handling
- ✅ Test 8: Target exists rejection
- ✅ Test 9: Source not found handling

---

## Success Criteria

**All tests must validate:**
1. ✅ Repaired transaction log produces identical read results
2. ✅ Row counts match between original and repaired tables
3. ✅ Partition pruning works correctly after replacement
4. ✅ IndexQuery operations function identically
5. ✅ Aggregate pushdown maintains correctness
6. ✅ Checkpoint and _last_checkpoint files are created
7. ✅ Missing splits are excluded gracefully
8. ✅ Overwrite boundaries are respected
9. ✅ Error cases fail safely without corruption

---

## Test File Location

**Path**: `spark/src/test/scala/io/delta/sql/parser/RepairIndexFilesTransactionLogReplacementSuite.scala`

**Base Class**: Extends `QueryTest` with `SharedSparkSession`

**Test Utilities**:
- `withTempDir`: Automatic cleanup of test directories
- Transaction log inspection helpers
- File system manipulation utilities
- Result comparison utilities

---

## Performance Validation

Additional tests should measure:
- **Repair operation time** for large transaction logs (1000+ transactions)
- **Read performance** comparison (original vs repaired)
- **Checkpoint effectiveness** (single file load vs multiple transaction reads)
- **Memory usage** during repair operation

---

## Integration with CI/CD

```bash
# Run repair tests specifically
mvn test -Dsuites="*RepairIndexFilesTransactionLogReplacementSuite"

# Run with coverage
mvn test -Dsuites="*RepairIndexFilesTransactionLogReplacementSuite" \
  -Dcoverage=true
```
