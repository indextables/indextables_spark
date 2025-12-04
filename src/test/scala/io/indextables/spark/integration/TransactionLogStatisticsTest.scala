/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.integration

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Integration tests specifically focused on verifying that statistics are correctly written to and read from the
 * transaction log.
 */
class TransactionLogStatisticsTest extends TestBase with BeforeAndAfterEach {

  private var testTablePath: Path            = _
  private var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testTablePath = new Path(tempDir, "txn_log_stats_test")
    transactionLog = TransactionLogFactory.create(testTablePath, spark)
  }

  test("should write complete statistics for all supported data types") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create dataset with various data types
    val complexData = Seq(
      (
        1L,
        "Alice",
        25,
        75000.50,
        true,
        java.sql.Date.valueOf("2023-01-15"),
        java.sql.Timestamp.valueOf("2023-01-15 10:30:00")
      ),
      (
        2L,
        "Bob",
        30,
        80000.75,
        false,
        java.sql.Date.valueOf("2023-02-20"),
        java.sql.Timestamp.valueOf("2023-02-20 14:45:00")
      ),
      (
        3L,
        "Charlie",
        35,
        85000.25,
        true,
        java.sql.Date.valueOf("2023-03-10"),
        java.sql.Timestamp.valueOf("2023-03-10 09:15:00")
      ),
      (
        4L,
        "Diana",
        28,
        78000.00,
        false,
        java.sql.Date.valueOf("2023-04-05"),
        java.sql.Timestamp.valueOf("2023-04-05 16:20:00")
      )
    ).toDF("id", "name", "age", "salary", "active", "hire_date", "last_login")

    println("📊 Writing data with multiple data types...")

    complexData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)

    // Verify statistics are written for all columns
    val addActions = transactionLog.listFiles()
    assert(addActions.length == 1, "Should have exactly one file")

    val addAction = addActions.head
    assert(addAction.minValues.isDefined, "MinValues should be present")
    assert(addAction.maxValues.isDefined, "MaxValues should be present")
    assert(addAction.numRecords.isDefined, "NumRecords should be present")

    val minVals = addAction.minValues.get
    val maxVals = addAction.maxValues.get

    // Verify statistics for each data type
    println(s"📈 Verifying statistics for all columns:")

    // Long type (id)
    assert(minVals.contains("id"), "Missing id min value")
    assert(maxVals.contains("id"), "Missing id max value")
    assert(minVals("id") == "1", s"Expected id min=1, got ${minVals("id")}")
    assert(maxVals("id") == "4", s"Expected id max=4, got ${maxVals("id")}")
    println(s"   ✓ id (Long): [${minVals("id")} - ${maxVals("id")}]")

    // String type (name)
    assert(minVals.contains("name"), "Missing name min value")
    assert(maxVals.contains("name"), "Missing name max value")
    assert(minVals("name") == "Alice", s"Expected name min=Alice, got ${minVals("name")}")
    assert(maxVals("name") == "Diana", s"Expected name max=Diana, got ${maxVals("name")}")
    println(s"   ✓ name (String): [${minVals("name")} - ${maxVals("name")}]")

    // Int type (age)
    assert(minVals.contains("age"), "Missing age min value")
    assert(maxVals.contains("age"), "Missing age max value")
    assert(minVals("age") == "25", s"Expected age min=25, got ${minVals("age")}")
    assert(maxVals("age") == "35", s"Expected age max=35, got ${maxVals("age")}")
    println(s"   ✓ age (Int): [${minVals("age")} - ${maxVals("age")}]")

    // Double type (salary)
    assert(minVals.contains("salary"), "Missing salary min value")
    assert(maxVals.contains("salary"), "Missing salary max value")
    assert(minVals("salary") == "75000.5", s"Expected salary min=75000.5, got ${minVals("salary")}")
    assert(maxVals("salary") == "85000.25", s"Expected salary max=85000.25, got ${maxVals("salary")}")
    println(s"   ✓ salary (Double): [${minVals("salary")} - ${maxVals("salary")}]")

    // Boolean type (active)
    assert(minVals.contains("active"), "Missing active min value")
    assert(maxVals.contains("active"), "Missing active max value")
    assert(minVals("active") == "false", s"Expected active min=false, got ${minVals("active")}")
    assert(maxVals("active") == "true", s"Expected active max=true, got ${maxVals("active")}")
    println(s"   ✓ active (Boolean): [${minVals("active")} - ${maxVals("active")}]")

    // Record count
    assert(addAction.numRecords.get.toString.toLong == 4L, s"Expected 4 records, got ${addAction.numRecords.get}")
    println(s"   ✓ Record count: ${addAction.numRecords.get}")

    println("✅ All data types have correct statistics")
  }

  test("should maintain statistics isolation across multiple files") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Write three separate batches with distinct ranges
    val batch1 = createBatchData(1, 100, "Batch1")
    val batch2 = createBatchData(1001, 1100, "Batch2")
    val batch3 = createBatchData(2001, 2100, "Batch3")

    println("📝 Writing multiple batches with distinct ranges...")

    batch1
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)
    batch2
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(testTablePath.toString)
    batch3
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(testTablePath.toString)

    // Verify each file has isolated statistics
    val addActions = transactionLog.listFiles().sortBy(_.modificationTime)
    assert(addActions.length == 3, s"Expected 3 files, got ${addActions.length}")

    // Check each file's statistics are isolated and correct
    verifyBatchStatistics(addActions(0), 1, 100, "Batch1", "File 1")
    verifyBatchStatistics(addActions(1), 1001, 1100, "Batch2", "File 2")
    verifyBatchStatistics(addActions(2), 2001, 2100, "Batch3", "File 3")

    println("✅ Statistics correctly isolated across multiple files")
  }

  test("should handle statistics for files with identical values") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create dataset where some columns have identical values
    val uniformData = Seq(
      (1L, "SameName", 30, 75000.0, "Engineering"),
      (2L, "SameName", 30, 75000.0, "Engineering"),
      (3L, "SameName", 30, 75000.0, "Engineering"),
      (4L, "SameName", 30, 75000.0, "Engineering")
    ).toDF("id", "name", "age", "salary", "department")

    println("📊 Writing data with uniform values...")

    uniformData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)

    val addActions = transactionLog.listFiles()
    val addAction  = addActions.head
    val minVals    = addAction.minValues.get
    val maxVals    = addAction.maxValues.get

    // Verify that min equals max for uniform columns
    assert(minVals("name") == "SameName" && maxVals("name") == "SameName", "Uniform string column should have min=max")
    assert(minVals("age") == "30" && maxVals("age") == "30", "Uniform int column should have min=max")
    assert(minVals("salary") == "75000.0" && maxVals("salary") == "75000.0", "Uniform double column should have min=max")
    assert(
      minVals("department") == "Engineering" && maxVals("department") == "Engineering",
      "Uniform string column should have min=max"
    )

    // But id should still have different min/max
    assert(minVals("id") == "1" && maxVals("id") == "4", "Varying column should have different min/max")

    println("✅ Uniform value statistics handled correctly")
  }

  test("should handle edge cases in statistics collection") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Test with single record
    val singleRecord = Seq((42L, "Single", 50, 100000.0)).toDF("id", "name", "age", "salary")

    singleRecord
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)

    val addActions = transactionLog.listFiles()
    val addAction  = addActions.head
    val minVals    = addAction.minValues.get
    val maxVals    = addAction.maxValues.get

    // For single record, min should equal max
    assert(minVals("id") == "42" && maxVals("id") == "42")
    assert(minVals("name") == "Single" && maxVals("name") == "Single")
    assert(minVals("age") == "50" && maxVals("age") == "50")
    assert(minVals("salary") == "100000.0" && maxVals("salary") == "100000.0")
    assert(addAction.numRecords.get.toString.toLong == 1L)

    println("✅ Single record statistics handled correctly")
  }

  test("should verify statistics persistence and retrieval") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Write data
    val testData = createBatchData(500, 600, "PersistenceTest")
    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)

    // Create a new transaction log instance to test persistence
    val newTransactionLog = TransactionLogFactory.create(testTablePath, spark)
    val retrievedActions  = newTransactionLog.listFiles()

    assert(retrievedActions.length == 1, "Should retrieve the written file")

    val retrievedAction = retrievedActions.head
    assert(retrievedAction.minValues.isDefined, "MinValues should persist")
    assert(retrievedAction.maxValues.isDefined, "MaxValues should persist")
    assert(retrievedAction.numRecords.isDefined, "NumRecords should persist")

    val minVals = retrievedAction.minValues.get
    val maxVals = retrievedAction.maxValues.get

    // Verify persisted statistics are correct
    assert(minVals("id") == "500", s"Persisted id min should be 500, got ${minVals("id")}")
    assert(maxVals("id") == "600", s"Persisted id max should be 600, got ${maxVals("id")}")
    assert(minVals("batch") == "PersistenceTest", s"Persisted batch should be PersistenceTest")
    assert(retrievedAction.numRecords.get.toString.toLong == 101L, s"Persisted count should be 101")

    println("✅ Statistics correctly persist and can be retrieved")
  }

  test("should write statistics incrementally for append operations") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Initial write
    val initial = createBatchData(1, 50, "Initial")
    initial
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(testTablePath.toString)

    // Invalidate cache after write since Spark creates separate TransactionLog instances
    transactionLog.invalidateCache()

    val initialActions = transactionLog.listFiles()
    assert(initialActions.length == 1, "Should have 1 file after initial write")

    // First append
    val append1 = createBatchData(51, 100, "Append1")
    append1
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(testTablePath.toString)

    // Invalidate cache after write since Spark creates separate TransactionLog instances
    transactionLog.invalidateCache()

    val afterAppend1 = transactionLog.listFiles()
    assert(afterAppend1.length == 2, "Should have 2 files after first append")

    // Second append
    val append2 = createBatchData(101, 150, "Append2")
    append2
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(testTablePath.toString)

    // Invalidate cache after write since Spark creates separate TransactionLog instances
    transactionLog.invalidateCache()

    val finalActions = transactionLog.listFiles().sortBy(_.modificationTime)
    assert(finalActions.length == 3, "Should have 3 files after second append")

    // Verify each append operation created correct statistics
    verifyBatchStatistics(finalActions(0), 1, 50, "Initial", "Initial file")
    verifyBatchStatistics(finalActions(1), 51, 100, "Append1", "First append file")
    verifyBatchStatistics(finalActions(2), 101, 150, "Append2", "Second append file")

    println("✅ Incremental statistics writing works correctly")
  }

  private def createBatchData(
    idStart: Long,
    idEnd: Long,
    batchName: String
  ): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val data = ArrayBuffer[(Long, String, Int, String)]()
    for (id <- idStart.to(idEnd))
      data += ((id, s"User$id", (20 + id % 30).toInt, batchName))
    data.toDF("id", "name", "age", "batch")
  }

  private def verifyBatchStatistics(
    addAction: AddAction,
    expectedMinId: Long,
    expectedMaxId: Long,
    expectedBatch: String,
    description: String
  ): Unit = {
    assert(addAction.minValues.isDefined, s"$description should have minValues")
    assert(addAction.maxValues.isDefined, s"$description should have maxValues")

    val minVals = addAction.minValues.get
    val maxVals = addAction.maxValues.get

    assert(
      minVals("id") == expectedMinId.toString,
      s"$description: expected id min=$expectedMinId, got ${minVals("id")}"
    )
    assert(
      maxVals("id") == expectedMaxId.toString,
      s"$description: expected id max=$expectedMaxId, got ${maxVals("id")}"
    )
    assert(minVals("batch") == expectedBatch, s"$description: expected batch=$expectedBatch, got ${minVals("batch")}")
    assert(maxVals("batch") == expectedBatch, s"$description: expected batch=$expectedBatch, got ${maxVals("batch")}")

    val expectedCount = expectedMaxId - expectedMinId + 1L
    assert(
      addAction.numRecords.get.toString.toLong == expectedCount,
      s"$description: expected count=$expectedCount, got ${addAction.numRecords.get}"
    )

    println(s"   ✓ $description: id=[$expectedMinId-$expectedMaxId], batch=$expectedBatch, count=$expectedCount")
  }
}
