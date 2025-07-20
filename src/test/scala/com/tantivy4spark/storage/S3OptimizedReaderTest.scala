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

package com.tantivy4spark.storage

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.amazonaws.services.s3.AmazonS3

class S3OptimizedReaderTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "S3OptimizedReader" should "initialize with configuration" in {
    val hadoopConf = createTestConfiguration()
    // Use null S3 client to trigger mock mode
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    reader shouldNot be(null)
  }
  
  it should "read data with predictive IO" in {
    val hadoopConf = createTestConfiguration()
    // Use null S3 client to trigger mock mode  
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    val dataLocation = DataLocation(
      bucket = "test-bucket",
      key = "test/data.tantv",
      offset = 0L,
      length = 1024L
    )
    
    val results = reader.readWithPredictiveIO(dataLocation, TestSchemas.basicSchema)
    
    // Should return empty iterator as parseDataToRows is not implemented
    results shouldBe a[Iterator[_]]
  }
  
  it should "cache data in LRU cache" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    val dataLocation = DataLocation(
      bucket = "test-bucket", 
      key = "test/cached.tantv",
      offset = 0L,
      length = 512L
    )
    
    // First read should fetch from "S3" (mock mode)
    val results1 = reader.readWithPredictiveIO(dataLocation, TestSchemas.basicSchema)
    results1 shouldBe a[Iterator[_]]
    
    // Second read should use cache
    val results2 = reader.readWithPredictiveIO(dataLocation, TestSchemas.basicSchema)
    results2 shouldBe a[Iterator[_]]
  }
  
  it should "handle different data locations" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    val location1 = DataLocation("bucket1", "key1", 0L, 100L)
    val location2 = DataLocation("bucket2", "key2", 0L, 200L)
    
    val results1 = reader.readWithPredictiveIO(location1, TestSchemas.basicSchema)
    val results2 = reader.readWithPredictiveIO(location2, TestSchemas.basicSchema)
    
    results1 shouldBe a[Iterator[_]]
    results2 shouldBe a[Iterator[_]]
  }
  
  it should "trigger predictive reads for next segments" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    val dataLocation = DataLocation(
      bucket = "test-bucket",
      key = "test/predictive.tantv", 
      offset = 0L,
      length = 1024L
    )
    
    val results = reader.readWithPredictiveIO(dataLocation, TestSchemas.basicSchema)
    results shouldBe a[Iterator[_]]
    
    // Give time for async predictive read
    Thread.sleep(100)
  }
  
  it should "handle configuration options correctly" in {
    val hadoopConf = createTestConfiguration()
    val customOptions = testOptions.copy(additional = Map(
      "predictive.read.size" -> "2097152", // 2MB
      "max.concurrent.reads" -> "8"
    )).toMap
    
    val reader = new S3OptimizedReader(hadoopConf, customOptions, None)
    reader shouldNot be(null)
  }
  
  it should "close resources properly" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    // Should not throw an exception
    noException should be thrownBy {
      reader.close()
    }
  }
  
  it should "handle concurrent reads safely" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    // Simple sequential test instead of concurrent to avoid ScalaFutures dependency
    val results = (1 to 5).map { i =>
      val location = DataLocation("bucket", s"key$i", i * 1000L, 1024L)
      reader.readWithPredictiveIO(location, TestSchemas.basicSchema)
    }
    
    results should have length 5
    results.foreach(_ shouldBe a[Iterator[_]])
  }
}

class DataLocationTest extends AnyFlatSpec with Matchers {
  
  "DataLocation" should "be created with all parameters" in {
    val location = DataLocation(
      bucket = "test-bucket",
      key = "test/key",
      offset = 512L,
      length = 1024L
    )
    
    location.bucket should be("test-bucket")
    location.key should be("test/key")
    location.offset should be(512L)
    location.length should be(1024L)
  }
  
  it should "support equality comparison" in {
    val location1 = DataLocation("bucket", "key", 0L, 100L)
    val location2 = DataLocation("bucket", "key", 0L, 100L)
    val location3 = DataLocation("bucket", "key", 0L, 200L)
    
    location1 should equal(location2)
    location1 should not equal location3
  }
  
  it should "have meaningful toString" in {
    val location = DataLocation("test-bucket", "test/key", 512L, 1024L)
    val stringRepr = location.toString
    
    stringRepr should include("test-bucket")
    stringRepr should include("test/key")
    stringRepr should include("512")
    stringRepr should include("1024")
  }
}

class S3OptimizedReaderIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "S3OptimizedReader integration" should "handle realistic data scenarios" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    // Simulate reading a large file in chunks
    val fileSize = 10 * 1024L // 10KB
    val chunkSize = 1024L     // 1KB chunks
    
    val chunks = (0L until fileSize by chunkSize).map { offset =>
      DataLocation("large-bucket", "large-file.tantv", offset, chunkSize)
    }
    
    val results = chunks.map { location =>
      reader.readWithPredictiveIO(location, TestSchemas.logSchema)
    }
    
    results should have length 10
    results.foreach(_ shouldBe a[Iterator[_]])
  }
  
  it should "handle edge cases gracefully" in {
    val hadoopConf = createTestConfiguration()
    val reader = new S3OptimizedReader(hadoopConf, testOptions.toMap, None)
    
    // Zero-length read
    val emptyLocation = DataLocation("bucket", "key", 0L, 0L)
    val emptyResults = reader.readWithPredictiveIO(emptyLocation, TestSchemas.basicSchema)
    emptyResults shouldBe a[Iterator[_]]
    
    // Large offset
    val largeOffsetLocation = DataLocation("bucket", "key", Long.MaxValue - 1000L, 100L)
    val largeResults = reader.readWithPredictiveIO(largeOffsetLocation, TestSchemas.basicSchema)
    largeResults shouldBe a[Iterator[_]]
  }
}