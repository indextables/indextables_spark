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

package com.tantivy4spark.transaction

import com.tantivy4spark.{TantivyTestBase, TestOptions, FileTestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Random

class TransactionLogTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TransactionLog" should "initialize with base path and options" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    txLog shouldNot be(null)
  }
  
  it should "append write result entries" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    val writeResult = WriteResult(
      filePath = s"${testDir}/test.qwt",
      bytesWritten = 1024L,
      recordCount = 10L,
      checksum = "abc123"
    )
    
    txLog.appendEntry(writeResult)
    
    val entries = txLog.getEntries
    entries should have length 1
    
    val entry = entries.head
    entry.operation shouldBe "WRITE"
    entry.filePath shouldBe writeResult.filePath
    entry.metadata("bytes_written") shouldBe "1024"
    entry.metadata("record_count") shouldBe "10"
    entry.metadata("checksum") shouldBe "abc123"
  }
  
  it should "append multiple entries" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    val writeResults = (1 to 5).map { i =>
      WriteResult(
        filePath = s"${testDir}/test$i.qwt",
        bytesWritten = i * 1024L,
        recordCount = i * 10L,
        checksum = s"checksum$i"
      )
    }
    
    writeResults.foreach(txLog.appendEntry)
    
    val entries = txLog.getEntries
    entries should have length 5
    
    entries.zipWithIndex.foreach { case (entry, index) =>
      entry.operation shouldBe "WRITE"
      entry.filePath shouldBe s"${testDir}/test${index + 1}.qwt"
      entry.metadata("bytes_written") shouldBe ((index + 1) * 1024).toString
    }
  }
  
  it should "commit transaction successfully" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Add some entries
    val writeResult = WriteResult(
      filePath = s"${testDir}/commit_test.qwt",
      bytesWritten = 2048L,
      recordCount = 20L,
      checksum = "commit123"
    )
    txLog.appendEntry(writeResult)
    
    // Commit transaction
    txLog.commit()
    
    val entries = txLog.getEntries
    entries should have length 2 // Original entry + commit entry
    
    val commitEntry = entries.last
    commitEntry.operation shouldBe "COMMIT"
    commitEntry.metadata should contain key "transaction_id"
    commitEntry.metadata("entries_count") shouldBe "1"
  }
  
  it should "rollback transaction successfully" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Add some entries
    val writeResults = (1 to 3).map { i =>
      WriteResult(
        filePath = s"${testDir}/rollback_test$i.qwt",
        bytesWritten = i * 512L,
        recordCount = i * 5L,
        checksum = s"rollback$i"
      )
    }
    writeResults.foreach(txLog.appendEntry)
    
    // Rollback transaction
    txLog.rollback()
    
    val entries = txLog.getEntries
    entries should have length 4 // 3 original entries + rollback entry
    
    val rollbackEntry = entries.last
    rollbackEntry.operation shouldBe "ROLLBACK"
    rollbackEntry.metadata should contain key "transaction_id"
    rollbackEntry.metadata("entries_count") shouldBe "3"
  }
  
  it should "generate unique transaction IDs" in {
    val txLog1 = new TransactionLog(testDir.toString, testOptions.toMap)
    val txLog2 = new TransactionLog(testDir.resolve("other").toString, testOptions.toMap)
    
    val writeResult = WriteResult(
      filePath = s"${testDir}/unique_test.qwt",
      bytesWritten = 1024L,
      recordCount = 10L,
      checksum = "unique123"
    )
    
    txLog1.appendEntry(writeResult)
    txLog2.appendEntry(writeResult)
    
    val entries1 = txLog1.getEntries
    val entries2 = txLog2.getEntries
    
    entries1 should have length 1
    entries2 should have length 1
    
    val txId1 = entries1.head.metadata("transaction_id")
    val txId2 = entries2.head.metadata("transaction_id")
    
    txId1 should not equal txId2
  }
  
  it should "include timestamps in entries" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    val startTime = System.currentTimeMillis()
    
    val writeResult = WriteResult(
      filePath = s"${testDir}/timestamp_test.qwt",
      bytesWritten = 1024L,
      recordCount = 10L,
      checksum = "timestamp123"
    )
    txLog.appendEntry(writeResult)
    
    val endTime = System.currentTimeMillis()
    
    val entries = txLog.getEntries
    entries should have length 1
    
    val entry = entries.head
    entry.timestamp should be >= startTime
    entry.timestamp should be <= endTime
  }
  
  it should "handle empty transaction commit" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Commit without any entries
    txLog.commit()
    
    val entries = txLog.getEntries
    entries should have length 1
    
    val commitEntry = entries.head
    commitEntry.operation shouldBe "COMMIT"
    commitEntry.metadata("entries_count") shouldBe "0"
  }
  
  it should "handle empty transaction rollback" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Rollback without any entries
    txLog.rollback()
    
    val entries = txLog.getEntries
    entries should have length 1
    
    val rollbackEntry = entries.head
    rollbackEntry.operation shouldBe "ROLLBACK"
    rollbackEntry.metadata("entries_count") shouldBe "0"
  }
}

class WriteResultTest extends AnyFlatSpec with Matchers {
  
  "WriteResult" should "be created with all parameters" in {
    val result = WriteResult(
      filePath = "/path/to/file.qwt",
      bytesWritten = 2048L,
      recordCount = 25L,
      checksum = "sha256hash"
    )
    
    result.filePath shouldBe "/path/to/file.qwt"
    result.bytesWritten shouldBe 2048L
    result.recordCount shouldBe 25L
    result.checksum shouldBe "sha256hash"
  }
  
  it should "support equality comparison" in {
    val result1 = WriteResult("/path/file.qwt", 1024L, 10L, "hash1")
    val result2 = WriteResult("/path/file.qwt", 1024L, 10L, "hash1")
    val result3 = WriteResult("/path/file.qwt", 1024L, 10L, "hash2")
    
    result1 shouldBe result2
    result1 should not be result3
  }
  
  it should "have meaningful toString" in {
    val result = WriteResult("/test/file.qwt", 512L, 5L, "abc123")
    val stringRepr = result.toString
    
    stringRepr should include("/test/file.qwt")
    stringRepr should include("512")
    stringRepr should include("5")
    stringRepr should include("abc123")
  }
}

class TransactionEntryTest extends AnyFlatSpec with Matchers {
  
  "TransactionEntry" should "be created with all parameters" in {
    val metadata = Map("key1" -> "value1", "key2" -> "value2")
    val timestamp = System.currentTimeMillis()
    
    val entry = TransactionEntry(
      timestamp = timestamp,
      operation = "WRITE",
      filePath = "/path/to/file.qwt",
      metadata = metadata
    )
    
    entry.timestamp shouldBe timestamp
    entry.operation shouldBe "WRITE"
    entry.filePath shouldBe "/path/to/file.qwt"
    entry.metadata shouldBe metadata
  }
  
  it should "support different operation types" in {
    val operations = Seq("WRITE", "COMMIT", "ROLLBACK", "DELETE")
    
    operations.foreach { op =>
      val entry = TransactionEntry(
        timestamp = System.currentTimeMillis(),
        operation = op,
        filePath = "/test/file.qwt",
        metadata = Map.empty
      )
      
      entry.operation shouldBe op
    }
  }
  
  it should "handle empty metadata" in {
    val entry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "TEST",
      filePath = "/empty/metadata.qwt",
      metadata = Map.empty
    )
    
    entry.metadata shouldBe empty
  }
  
  it should "handle large metadata" in {
    val largeMetadata = (1 to 100).map { i =>
      s"key$i" -> s"value$i"
    }.toMap
    
    val entry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "LARGE_WRITE",
      filePath = "/large/metadata.qwt",
      metadata = largeMetadata
    )
    
    entry.metadata should have size 100
    entry.metadata("key50") shouldBe "value50"
  }
}

class TransactionLogIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TransactionLog integration" should "handle realistic transaction scenarios" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Simulate a batch write operation
    val batchSize = 10
    val writeResults = (1 to batchSize).map { i =>
      WriteResult(
        filePath = s"${testDir}/batch_$i.qwt",
        bytesWritten = Random.nextLong() % 10000 + 1000,
        recordCount = Random.nextLong() % 1000 + 100,
        checksum = s"checksum_$i"
      )
    }
    
    // Add all entries
    writeResults.foreach(txLog.appendEntry)
    
    // Verify all entries are recorded
    val entries = txLog.getEntries
    entries should have length batchSize
    
    // Commit the transaction
    txLog.commit()
    
    val finalEntries = txLog.getEntries
    finalEntries should have length (batchSize + 1)
    finalEntries.last.operation shouldBe "COMMIT"
  }
  
  it should "handle concurrent transaction scenarios" in {
    // Create multiple transaction logs for different paths
    val txLogs = (1 to 3).map { i =>
      new TransactionLog(testDir.resolve(s"tx_$i").toString, testOptions.toMap)
    }
    
    // Simulate concurrent writes
    txLogs.zipWithIndex.foreach { case (txLog, index) =>
      val writeResult = WriteResult(
        filePath = s"${testDir}/concurrent_${index}.qwt",
        bytesWritten = (index + 1) * 1024L,
        recordCount = (index + 1) * 10L,
        checksum = s"concurrent_$index"
      )
      txLog.appendEntry(writeResult)
    }
    
    // Commit all transactions
    txLogs.foreach(_.commit())
    
    // Verify each transaction log
    txLogs.zipWithIndex.foreach { case (txLog, index) =>
      val entries = txLog.getEntries
      entries should have length 2 // write + commit
      entries.head.operation shouldBe "WRITE"
      entries.last.operation shouldBe "COMMIT"
    }
  }
  
  it should "maintain transaction consistency under failure scenarios" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    
    // Add some successful writes
    val successfulWrites = (1 to 3).map { i =>
      WriteResult(
        filePath = s"${testDir}/success_$i.qwt",
        bytesWritten = i * 1024L,
        recordCount = i * 10L,
        checksum = s"success_$i"
      )
    }
    successfulWrites.foreach(txLog.appendEntry)
    
    // Simulate a failure and rollback
    txLog.rollback()
    
    val entries = txLog.getEntries
    entries should have length 4 // 3 writes + 1 rollback
    entries.last.operation shouldBe "ROLLBACK"
    entries.last.metadata("entries_count") shouldBe "3"
  }
}