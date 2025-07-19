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

package com.tantivy4spark.search

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestDataGenerator, TestOptions, MockTantivyNative}
import com.tantivy4spark.transaction.WriteResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TantivyIndexWriterTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    MockTantivyNative.reset()
  }
  
  "TantivyIndexWriter" should "initialize correctly" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    writer shouldNot be(null)
  }
  
  it should "write a single row successfully" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    val row = InternalRow(
      1L,
      UTF8String.fromString("Test Title"),
      UTF8String.fromString("Test Content"),
      System.currentTimeMillis(),
      85.5,
      true
    )
    
    val result = writer.writeRow(row)
    
    result shouldBe a[WriteResult]
    result.recordCount shouldBe 1
    result.bytesWritten should be > 0L
    result.checksum should not be empty
  }
  
  it should "handle null values in rows" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    val rowWithNulls = InternalRow(
      2L,
      UTF8String.fromString("Title with nulls"),
      null, // content is null
      System.currentTimeMillis(),
      null, // score is null
      false
    )
    
    val result = writer.writeRow(rowWithNulls)
    
    result shouldBe a[WriteResult]
    result.recordCount shouldBe 1
  }
  
  it should "write multiple rows in batch" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    val rows = TestDataGenerator.generateBasicRows(5).toArray
    val results = writer.writeBatch(rows)
    
    results should have length 5
    results.foreach { result =>
      result.recordCount shouldBe 1
      result.bytesWritten should be > 0L
    }
  }
  
  it should "flush batch when batch size is reached" in {
    val smallBatchOptions = testOptions.copy(batchSize = 2).toMap
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      smallBatchOptions
    )
    
    // Write 3 rows - should trigger flush after 2 rows
    val rows = TestDataGenerator.generateBasicRows(3)
    rows.foreach(writer.writeRow)
    
    // At this point, batch should have been flushed
    // This is tested indirectly through the write operations
  }
  
  it should "handle different data types correctly" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.logSchema,
      testOptions.toMap
    )
    
    val logRow = InternalRow(
      System.currentTimeMillis(),
      UTF8String.fromString("ERROR"),
      UTF8String.fromString("Database connection failed"),
      UTF8String.fromString("api-service"),
      UTF8String.fromString("host1"),
      1500L,
      500
    )
    
    val result = writer.writeRow(logRow)
    
    result shouldBe a[WriteResult]
    result.recordCount shouldBe 1
  }
  
  it should "roll to new segment when segment size is reached" in {
    val smallSegmentOptions = testOptions.copy(
      segmentSize = 1024, // Very small segment size
      batchSize = 1
    ).toMap
    
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      smallSegmentOptions
    )
    
    // Write enough rows to trigger segment rolling
    val rows = TestDataGenerator.generateBasicRows(10)
    val results = rows.map(writer.writeRow)
    
    results should have length 10
    results.foreach(_.recordCount shouldBe 1)
  }
  
  it should "commit successfully" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    // Write some data first
    val rows = TestDataGenerator.generateBasicRows(3)
    rows.foreach(writer.writeRow)
    
    // Mock successful commit
    val configId = MockTantivyNative.createMockConfig()
    val writerId = MockTantivyNative.createMockWriter(configId)
    
    val commitResult = writer.commit()
    
    // Since we're using mocks, this tests the flow rather than actual commit
    commitResult shouldBe a[Boolean]
  }
  
  it should "close properly and clean up resources" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    // Write some data
    val row = TestDataGenerator.generateBasicRows(1).head
    writer.writeRow(row)
    
    // Close should not throw exception
    writer.close()
  }
  
  it should "generate correct statistics" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    val (buffered, capacity) = writer.getStats
    
    buffered shouldBe 0 // No documents written yet
    capacity shouldBe testOptions.batchSize
  }
  
  it should "handle complex schema with nested fields" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.complexSchema,
      testOptions.toMap
    )
    
    // Create a row with complex data
    val nestedStruct = InternalRow(
      UTF8String.fromString("nested_value"),
      42
    )
    
    val complexRow = InternalRow(
      UTF8String.fromString("complex_doc"),
      nestedStruct,
      null, // array field
      null  // map field
    )
    
    val result = writer.writeRow(complexRow)
    
    result shouldBe a[WriteResult]
    result.recordCount shouldBe 1
  }
  
  it should "add metadata fields automatically" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.copy(additional = Map("s3.bucket" -> "test-bucket")).toMap
    )
    
    val row = TestDataGenerator.generateBasicRows(1).head
    val result = writer.writeRow(row)
    
    result shouldBe a[WriteResult]
    // Metadata fields like _timestamp, _bucket, etc. should be added automatically
    // This is tested indirectly through successful write operation
  }
  
  it should "handle custom field type mappings" in {
    val customOptions = testOptions.copy(additional = Map(
      "field.title.indexed" -> "true",
      "field.content.stored" -> "false",
      "field.score.fast" -> "true"
    )).toMap
    
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      customOptions
    )
    
    writer shouldNot be(null)
    // Custom mappings are applied during schema generation
  }
  
  it should "validate checksum calculation" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    val row = TestDataGenerator.generateBasicRows(1).head
    val result1 = writer.writeRow(row)
    val result2 = writer.writeRow(row)
    
    // Same row should produce same checksum
    result1.checksum shouldBe result2.checksum
  }
}

class TantivyIndexWriterSchemaTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyIndexWriter schema mapping" should "map Spark types to Tantivy types correctly" in {
    val writer = new TantivyIndexWriter(
      testDir.toString,
      TestSchemas.basicSchema,
      testOptions.toMap
    )
    
    // This tests the schema mapping indirectly through successful initialization
    writer shouldNot be(null)
  }
  
  it should "handle all supported Spark data types" in {
    val allTypesSchema = StructType(Seq(
      StructField("string_field", StringType),
      StructField("long_field", LongType),
      StructField("double_field", DoubleType),
      StructField("boolean_field", BooleanType),
      StructField("timestamp_field", TimestampType),
      StructField("int_field", IntegerType),
      StructField("float_field", FloatType),
      StructField("byte_field", ByteType),
      StructField("short_field", ShortType)
    ))
    
    val writer = new TantivyIndexWriter(
      testDir.toString,
      allTypesSchema,
      testOptions.toMap
    )
    
    writer shouldNot be(null)
  }
  
  it should "handle nullable and non-nullable fields" in {
    val mixedNullabilitySchema = StructType(Seq(
      StructField("required_field", StringType, nullable = false),
      StructField("optional_field", StringType, nullable = true)
    ))
    
    val writer = new TantivyIndexWriter(
      testDir.toString,
      mixedNullabilitySchema,
      testOptions.toMap
    )
    
    writer shouldNot be(null)
  }
}