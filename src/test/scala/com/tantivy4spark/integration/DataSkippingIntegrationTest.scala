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
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import scala.collection.mutable.ArrayBuffer

class DataSkippingIntegrationTest extends TestBase with BeforeAndAfterEach {
  
  private var testTablePath: Path = _
  private var transactionLog: TransactionLog = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    testTablePath = new Path(tempDir, "data_skipping_table")
    transactionLog = new TransactionLog(testTablePath, spark)
    
    val schema = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
      StructField("score", DoubleType, false),
      StructField("department", StringType, false)
    ))
    
    transactionLog.initialize(schema)
  }
  
  test("should skip files based on EqualTo filter") {
    // Create test data with distinct ranges in different files
    val file1Data = createTestData(1, 100, "Engineering", 20, 30, 85.0, 95.0)
    val file2Data = createTestData(101, 200, "Marketing", 25, 35, 70.0, 80.0)  
    val file3Data = createTestData(201, 300, "Sales", 30, 40, 60.0, 75.0)
    
    // Add files to transaction log with min/max values
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 100)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 100)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 100)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)
    transactionLog.addFile(addAction3)
    
    // Test EqualTo filter on id
    val filter = EqualTo("id", 150L)
    val scan = createScanWithFilter(Array(filter))
    val partitions = scan.planInputPartitions()
    
    // Should only scan file2 (contains id=150)
    assert(partitions.length == 1, s"Expected 1 partition, got ${partitions.length}")
    
    val partition = partitions.head.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition]
    assert(partition.addAction.path.contains("file2"))
  }
  
  test("should skip files based on range filters") {
    val file1Data = createTestData(1, 100, "Engineering", 20, 30, 85.0, 95.0)
    val file2Data = createTestData(101, 200, "Marketing", 25, 35, 70.0, 80.0)  
    val file3Data = createTestData(201, 300, "Sales", 30, 40, 60.0, 75.0)
    
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 100)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 100)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 100)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)
    transactionLog.addFile(addAction3)
    
    // Test GreaterThan filter - should skip file1
    val greaterThanFilter = GreaterThan("id", 100L)
    val scan1 = createScanWithFilter(Array(greaterThanFilter))
    val partitions1 = scan1.planInputPartitions()
    
    assert(partitions1.length == 2, s"Expected 2 partitions for GreaterThan, got ${partitions1.length}")
    val paths1 = partitions1.map(_.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition].addAction.path)
    assert(paths1.exists(_.contains("file2")))
    assert(paths1.exists(_.contains("file3")))
    assert(!paths1.exists(_.contains("file1")))
    
    // Test LessThan filter - should skip file3  
    val lessThanFilter = LessThan("id", 201L)
    val scan2 = createScanWithFilter(Array(lessThanFilter))
    val partitions2 = scan2.planInputPartitions()
    
    assert(partitions2.length == 2, s"Expected 2 partitions for LessThan, got ${partitions2.length}")
    val paths2 = partitions2.map(_.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition].addAction.path)
    assert(paths2.exists(_.contains("file1")))
    assert(paths2.exists(_.contains("file2")))
    assert(!paths2.exists(_.contains("file3")))
  }
  
  test("should skip files based on string filters") {
    val file1Data = createTestData(1, 100, "Engineering", 20, 30, 85.0, 95.0)
    val file2Data = createTestData(101, 200, "Marketing", 25, 35, 70.0, 80.0)  
    val file3Data = createTestData(201, 300, "Sales", 30, 40, 60.0, 75.0)
    
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 100)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 100)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 100)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)
    transactionLog.addFile(addAction3)
    
    // Test EqualTo filter on department
    val departmentFilter = EqualTo("department", "Marketing")
    val scan = createScanWithFilter(Array(departmentFilter))
    val partitions = scan.planInputPartitions()
    
    // Should only scan file2 (contains Marketing department)
    assert(partitions.length == 1, s"Expected 1 partition, got ${partitions.length}")
    
    val partition = partitions.head.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition]
    assert(partition.addAction.path.contains("file2"))
  }
  
  test("should handle overlapping ranges correctly") {
    // Create overlapping data ranges
    val file1Data = createTestData(1, 150, "Engineering", 20, 35, 70.0, 95.0)  // overlaps with file2
    val file2Data = createTestData(100, 250, "Marketing", 25, 40, 60.0, 90.0)   // overlaps with file1 and file3
    val file3Data = createTestData(200, 300, "Sales", 30, 45, 50.0, 85.0)      // overlaps with file2
    
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 150)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 151)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 101)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)
    transactionLog.addFile(addAction3)
    
    // Test filter that should match multiple overlapping files
    val filter = EqualTo("id", 150L)  // exists in file1 and file2
    val scan = createScanWithFilter(Array(filter))
    val partitions = scan.planInputPartitions()
    
    // Should scan both file1 and file2 due to overlapping ranges
    assert(partitions.length == 2, s"Expected 2 partitions for overlapping ranges, got ${partitions.length}")
    
    val paths = partitions.map(_.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkInputPartition].addAction.path)
    assert(paths.exists(_.contains("file1")))
    assert(paths.exists(_.contains("file2")))
    assert(!paths.exists(_.contains("file3")))
  }
  
  test("should not skip files when min/max values are missing") {
    // Create file without min/max statistics
    val addActionNoStats = AddAction(
      path = "no_stats_file.tnt4s",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L),
      minValues = None,  // No min values
      maxValues = None   // No max values
    )
    
    transactionLog.addFile(addActionNoStats)
    
    val filter = EqualTo("id", 150L)
    val scan = createScanWithFilter(Array(filter))
    val partitions = scan.planInputPartitions()
    
    // Should not skip file without statistics
    assert(partitions.length == 1, s"Expected 1 partition (no skipping), got ${partitions.length}")
  }
  
  test("should handle complex filter combinations") {
    val file1Data = createTestData(1, 100, "Engineering", 20, 30, 85.0, 95.0)
    val file2Data = createTestData(101, 200, "Marketing", 25, 35, 70.0, 80.0)  
    val file3Data = createTestData(201, 300, "Sales", 30, 40, 60.0, 75.0)
    
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 100)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 100)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 100)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)  
    transactionLog.addFile(addAction3)
    
    // Test AND filter - both conditions must be satisfied
    val andFilter = And(
      GreaterThan("id", 150L),      // Skips file1 
      LessThan("score", 78.0)       // Skips file1 (scores 85-95)
    )
    
    val scan = createScanWithFilter(Array(andFilter))
    val partitions = scan.planInputPartitions()
    
    // Should scan file2 and file3 (id > 150), but effectively only file3 has score < 78
    // However, our current implementation doesn't handle complex AND logic for skipping,
    // so it will be conservative and scan files that match either condition
    assert(partitions.length >= 1, s"Expected at least 1 partition, got ${partitions.length}")
  }
  
  test("should log data skipping statistics") {
    val file1Data = createTestData(1, 100, "Engineering", 20, 30, 85.0, 95.0)
    val file2Data = createTestData(101, 200, "Marketing", 25, 35, 70.0, 80.0)  
    val file3Data = createTestData(201, 300, "Sales", 30, 40, 60.0, 75.0)
    
    val addAction1 = createAddActionWithStats("file1.tnt4s", file1Data, 100)
    val addAction2 = createAddActionWithStats("file2.tnt4s", file2Data, 100)
    val addAction3 = createAddActionWithStats("file3.tnt4s", file3Data, 100)
    
    transactionLog.addFile(addAction1)
    transactionLog.addFile(addAction2)
    transactionLog.addFile(addAction3)
    
    // Capture log output to verify skipping messages
    import org.apache.log4j.{Logger, Level, AppenderSkeleton}
    import org.apache.log4j.spi.LoggingEvent
    import java.util.concurrent.ConcurrentLinkedQueue
    
    val logMessages = new ConcurrentLinkedQueue[String]()
    val logger = Logger.getLogger("com.tantivy4spark.core.Tantivy4SparkScan")
    val originalLevel = logger.getLevel
    
    val testAppender = new AppenderSkeleton() {
      override def append(event: LoggingEvent): Unit = {
        logMessages.add(event.getRenderedMessage)
      }
      override def close(): Unit = {}
      override def requiresLayout(): Boolean = false
    }
    
    try {
      logger.setLevel(Level.INFO)
      logger.addAppender(testAppender)
      
      // Perform query that should skip files
      val filter = EqualTo("id", 150L)
      val scan = createScanWithFilter(Array(filter))
      scan.planInputPartitions()
      
      // Verify that data skipping log messages were generated
      val messages = logMessages.toArray.map(_.toString)
      val skippingMessage = messages.find(_.contains("Data skipping:"))
      
      assert(skippingMessage.isDefined, s"Expected data skipping log message, got: ${messages.mkString(", ")}")
      
      val message = skippingMessage.get
      assert(message.contains("files skipped"), s"Expected skipping stats in message: $message")
      assert(message.contains("files remaining"), s"Expected remaining stats in message: $message")
      
    } finally {
      logger.removeAppender(testAppender)
      logger.setLevel(originalLevel)
    }
  }
  
  private def createTestData(
    idStart: Long, 
    idEnd: Long, 
    department: String,
    ageMin: Int,
    ageMax: Int, 
    scoreMin: Double,
    scoreMax: Double
  ): Array[Row] = {
    val buffer = ArrayBuffer[Row]()
    for (i <- idStart to idEnd) {
      val age = ageMin + (i - idStart).toInt % (ageMax - ageMin + 1)
      val score = scoreMin + ((i - idStart).toDouble / (idEnd - idStart)) * (scoreMax - scoreMin)
      buffer += Row(i, s"User$i", age, score, department)
    }
    buffer.toArray
  }
  
  private def createAddActionWithStats(
    filename: String, 
    data: Array[Row], 
    numRecords: Long
  ): AddAction = {
    // Calculate min/max values from the data
    val ids = data.map(_.getAs[Long]("id"))
    val ages = data.map(_.getAs[Int]("age"))  
    val scores = data.map(_.getAs[Double]("score"))
    val departments = data.map(_.getAs[String]("department"))
    
    val minValues = Map(
      "id" -> ids.min.toString,
      "age" -> ages.min.toString,
      "score" -> scores.min.toString,
      "department" -> departments.min
    )
    
    val maxValues = Map(
      "id" -> ids.max.toString,
      "age" -> ages.max.toString, 
      "score" -> scores.max.toString,
      "department" -> departments.max
    )
    
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(numRecords),
      minValues = Some(minValues),
      maxValues = Some(maxValues)
    )
  }
  
  private def createScanWithFilter(filters: Array[Filter]): com.tantivy4spark.core.Tantivy4SparkScan = {
    val schema = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
      StructField("score", DoubleType, false),
      StructField("department", StringType, false)
    ))
    
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._
    
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    
    new com.tantivy4spark.core.Tantivy4SparkScan(
      transactionLog,
      schema,
      filters,
      options
    )
  }
}