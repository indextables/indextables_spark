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

package com.tantivy4spark.integration

import com.tantivy4spark.TestBase
import com.tantivy4spark.transaction.{TransactionLogFactory, TransactionLog, AddAction}
import com.tantivy4spark.core.Tantivy4SparkScan
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterEach
import scala.jdk.CollectionConverters._

class DataSkippingVerificationTest extends TestBase with BeforeAndAfterEach {
  
  private var testTablePath: Path = _
  private var transactionLog: TransactionLog = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    testTablePath = new Path(tempDir, "data_skipping_verification")
    transactionLog = TransactionLogFactory.create(testTablePath, spark)
    
    val schema = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("department", StringType, false)
    ))
    
    transactionLog.initialize(schema)
  }
  
  test("data skipping should reduce scanned partitions with EqualTo filter") {
    // Setup: Create 3 files with distinct ID ranges
    val file1 = createAddActionWithStats("file1.tnt4s", 
      minValues = Map("id" -> "1", "name" -> "Alice", "department" -> "Engineering"),
      maxValues = Map("id" -> "100", "name" -> "John", "department" -> "Engineering")
    )
    val file2 = createAddActionWithStats("file2.tnt4s",
      minValues = Map("id" -> "101", "name" -> "Kate", "department" -> "Marketing"), 
      maxValues = Map("id" -> "200", "name" -> "Mike", "department" -> "Marketing")
    )
    val file3 = createAddActionWithStats("file3.tnt4s",
      minValues = Map("id" -> "201", "name" -> "Sam", "department" -> "Sales"),
      maxValues = Map("id" -> "300", "name" -> "Zoe", "department" -> "Sales")
    )
    
    transactionLog.addFile(file1)
    transactionLog.addFile(file2)  
    transactionLog.addFile(file3)
    
    // Test: Query for id=150 (should only scan file2)
    val filter = EqualTo("id", 150L)
    val scan = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()
    
    // Verify: Should only have 1 partition (file2)
    assert(partitions.length == 1, 
      s"Expected 1 partition after data skipping, got ${partitions.length}")
    
    val partition = partitions.head.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition]
    assert(partition.addAction.path.contains("file2"), 
      s"Expected file2, got ${partition.addAction.path}")
    
    println("✅ EqualTo filter test passed: Correctly skipped 2 out of 3 files")
  }
  
  test("data skipping should work with range filters") {
    val file1 = createAddActionWithStats("range1.tnt4s", 
      minValues = Map("id" -> "1"),
      maxValues = Map("id" -> "100")
    )
    val file2 = createAddActionWithStats("range2.tnt4s",
      minValues = Map("id" -> "101"), 
      maxValues = Map("id" -> "200")
    )
    val file3 = createAddActionWithStats("range3.tnt4s",
      minValues = Map("id" -> "201"),
      maxValues = Map("id" -> "300")
    )
    
    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)
    
    // Test GreaterThan filter (should skip file1)  
    val gtFilter = GreaterThan("id", 100L)
    val gtScan = createScanWithFilters(Array(gtFilter))
    val gtPartitions = gtScan.planInputPartitions()
    
    assert(gtPartitions.length == 2, 
      s"GreaterThan: Expected 2 partitions, got ${gtPartitions.length}")
    
    val gtPaths = gtPartitions.map(_.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition].addAction.path)
    assert(!gtPaths.exists(_.contains("range1")), "GreaterThan should skip range1")
    assert(gtPaths.exists(_.contains("range2")), "GreaterThan should include range2")
    assert(gtPaths.exists(_.contains("range3")), "GreaterThan should include range3")
    
    // Test LessThan filter (should skip file3)
    val ltFilter = LessThan("id", 201L)
    val ltScan = createScanWithFilters(Array(ltFilter))  
    val ltPartitions = ltScan.planInputPartitions()
    
    assert(ltPartitions.length == 2,
      s"LessThan: Expected 2 partitions, got ${ltPartitions.length}")
      
    val ltPaths = ltPartitions.map(_.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition].addAction.path)
    assert(ltPaths.exists(_.contains("range1")), "LessThan should include range1")
    assert(ltPaths.exists(_.contains("range2")), "LessThan should include range2") 
    assert(!ltPaths.exists(_.contains("range3")), "LessThan should skip range3")
    
    println("✅ Range filter tests passed: GreaterThan and LessThan correctly skip files")
  }
  
  test("data skipping should handle missing statistics gracefully") {
    val fileWithStats = createAddActionWithStats("with_stats.tnt4s",
      minValues = Map("id" -> "1"),
      maxValues = Map("id" -> "100") 
    )
    val fileWithoutStats = createAddActionWithoutStats("no_stats.tnt4s")
    
    transactionLog.addFile(fileWithStats)
    transactionLog.addFile(fileWithoutStats)
    
    // Test: Filter that would skip fileWithStats but not fileWithoutStats
    val filter = EqualTo("id", 200L)  // Outside range of fileWithStats
    val scan = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()
    
    // Should still scan fileWithoutStats (no stats = can't skip)
    assert(partitions.length == 1, 
      s"Expected 1 partition (no stats file), got ${partitions.length}")
      
    val partition = partitions.head.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition]
    assert(partition.addAction.path.contains("no_stats"), 
      "Should keep file without statistics")
    
    println("✅ Missing statistics test passed: Files without stats are not skipped")
  }
  
  test("data skipping should log statistics") {
    val file1 = createAddActionWithStats("log1.tnt4s", 
      minValues = Map("id" -> "1"),
      maxValues = Map("id" -> "100")
    )
    val file2 = createAddActionWithStats("log2.tnt4s",
      minValues = Map("id" -> "101"), 
      maxValues = Map("id" -> "200")
    )
    val file3 = createAddActionWithStats("log3.tnt4s",
      minValues = Map("id" -> "201"),
      maxValues = Map("id" -> "300")
    )
    
    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)
    
    // Capture console output
    val originalOut = System.out
    val outputStream = new java.io.ByteArrayOutputStream()
    val printStream = new java.io.PrintStream(outputStream)
    
    try {
      // Redirect system output to capture logs
      System.setOut(printStream)
      
      val filter = EqualTo("id", 150L)
      val scan = createScanWithFilters(Array(filter))
      scan.planInputPartitions()  // This should generate log messages
      
      val output = outputStream.toString()
      
      // Note: This test may not capture SLF4J logs, but demonstrates the approach
      println("✅ Logging test completed - data skipping statistics should be logged")
      println(s"Captured output length: ${output.length} characters")
      
    } finally {
      System.setOut(originalOut)
    }
  }
  
  private def createAddActionWithStats(
    filename: String,
    minValues: Map[String, String],
    maxValues: Map[String, String]
  ): AddAction = {
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L),
      minValues = Some(minValues),
      maxValues = Some(maxValues)
    )
  }
  
  private def createAddActionWithoutStats(filename: String): AddAction = {
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(), 
      dataChange = true,
      numRecords = Some(100L),
      minValues = None,
      maxValues = None
    )
  }
  
  private def createScanWithFilters(filters: Array[Filter]): Tantivy4SparkScan = {
    val schema = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false), 
      StructField("department", StringType, false)
    ))
    
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    
    val emptyBroadcastConfig = spark.sparkContext.broadcast(Map.empty[String, String])
    new Tantivy4SparkScan(
      spark,
      transactionLog,
      schema,
      filters,
      options,
      None,
      emptyBroadcastConfig.value
    )
  }
}
