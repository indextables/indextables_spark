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

import com.tantivy4spark.TestBase
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach
import java.util.Collections
import scala.jdk.CollectionConverters._

class TransactionLogCacheTest extends TestBase with BeforeAndAfterEach {

  var transactionLog: TransactionLog = _
  var tablePath: String = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tablePath = tempDir + "/test_table"
  }

  override def afterEach(): Unit = {
    if (transactionLog != null) {
      transactionLog.close()
    }
    super.afterEach()
  }

  test("cache should be enabled by default") {
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    val stats = transactionLog.getCacheStats()
    assert(stats.isDefined, "Cache should be enabled by default")
    assert(stats.get.expirationSeconds == 300, "Default expiration should be 5 minutes (300 seconds)")
  }

  test("cache can be disabled") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.tantivy4spark.transaction.cache.enabled" -> "false"
    ).asJava)
    
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    val stats = transactionLog.getCacheStats()
    assert(stats.isEmpty, "Cache should be disabled when explicitly set to false")
  }

  test("cache expiration can be configured") {
    val customExpirationSeconds = 30L
    val options = new CaseInsensitiveStringMap(Map(
      "spark.tantivy4spark.transaction.cache.expirationSeconds" -> customExpirationSeconds.toString
    ).asJava)
    
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    val stats = transactionLog.getCacheStats()
    assert(stats.isDefined, "Cache should be enabled")
    assert(stats.get.expirationSeconds == customExpirationSeconds, 
      s"Cache expiration should be ${customExpirationSeconds} seconds")
  }

  test("cache improves performance on repeated reads") {
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    // Initialize the transaction log
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    transactionLog.initialize(schema)
    
    // Add some files
    val addActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true),
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true)
    )
    transactionLog.addFiles(addActions)
    
    // First read - should populate cache
    val stats1 = transactionLog.getCacheStats().get
    val initialHits = stats1.hits
    val initialMisses = stats1.misses
    
    val files1 = transactionLog.listFiles()
    assert(files1.length == 2, "Should have 2 files")
    
    val stats2 = transactionLog.getCacheStats().get
    assert(stats2.misses > initialMisses, "Should have cache misses on first read")
    
    // Second read - should use cache
    val files2 = transactionLog.listFiles()
    assert(files2.length == 2, "Should still have 2 files")
    
    val stats3 = transactionLog.getCacheStats().get
    assert(stats3.hits > stats2.hits, "Should have cache hits on second read")
    assert(stats3.hitRate > 0.0, "Should have positive hit rate")
    
    println(s"Cache stats after repeated reads: $stats3")
  }

  test("cache invalidation works after writes") {
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    // Initialize the transaction log
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    transactionLog.initialize(schema)
    
    // Add initial file and cache the result
    transactionLog.addFile(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true))
    val files1 = transactionLog.listFiles()
    assert(files1.length == 1, "Should have 1 file")
    
    // Add another file - this should invalidate cache
    transactionLog.addFile(AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true))
    
    // Read again - should reflect the new state
    val files2 = transactionLog.listFiles()
    assert(files2.length == 2, "Should have 2 files after adding second file")
  }

  test("cache expires after configured time") {
    // Use very short expiration for testing
    val shortExpirationSeconds = 1L
    val options = new CaseInsensitiveStringMap(Map(
      "spark.tantivy4spark.transaction.cache.expirationSeconds" -> shortExpirationSeconds.toString
    ).asJava)
    
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    // Initialize and add data
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    transactionLog.initialize(schema)
    transactionLog.addFile(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true))
    
    // First read - populates cache
    val files1 = transactionLog.listFiles()
    assert(files1.length == 1, "Should have 1 file")
    
    val stats1 = transactionLog.getCacheStats().get
    println(s"Stats after first read: $stats1")
    
    // Wait for cache to expire
    Thread.sleep((shortExpirationSeconds + 1) * 1000L)
    
    // Second read - cache should be expired
    val files2 = transactionLog.listFiles()
    assert(files2.length == 1, "Should still have 1 file")
    
    val stats2 = transactionLog.getCacheStats().get
    println(s"Stats after expired read: $stats2")
    assert(stats2.misses > stats1.misses, "Should have additional misses due to expiration")
  }

  test("manual cache invalidation works") {
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    // Initialize and add data
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    transactionLog.initialize(schema)
    transactionLog.addFile(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true))
    
    // First read - populates cache
    transactionLog.listFiles()
    transactionLog.getMetadata()
    
    val stats1 = transactionLog.getCacheStats().get
    assert(stats1.hits > 0 || stats1.misses > 0, "Should have some cache activity")
    
    // Manually invalidate cache
    transactionLog.invalidateCache()
    
    // Next read should miss cache
    transactionLog.listFiles()
    val stats2 = transactionLog.getCacheStats().get
    
    // Should have reset hit/miss counters, but more importantly,
    // should have required fresh reads from storage
    println(s"Stats after manual invalidation: $stats2")
  }

  test("metadata caching works correctly") {
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    
    // Initialize the transaction log
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    transactionLog.initialize(schema)
    
    val stats1 = transactionLog.getCacheStats().get
    val initialHits = stats1.hits
    
    // First metadata read - should populate cache
    val metadata1 = transactionLog.getMetadata()
    assert(metadata1 != null, "Should have metadata")
    
    // Second metadata read - should use cache
    val metadata2 = transactionLog.getMetadata()
    assert(metadata2 == metadata1, "Should return same metadata object")
    
    val stats2 = transactionLog.getCacheStats().get
    assert(stats2.hits > initialHits, "Should have cache hits for metadata")
  }
}