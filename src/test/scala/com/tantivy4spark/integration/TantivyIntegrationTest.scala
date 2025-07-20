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

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestDataGenerator, TestOptions, MockTantivyNative}
import com.tantivy4spark.core.{TantivyFileFormat, TantivyOutputWriter, TantivyOutputWriterFactory, TantivyFileReader}
import com.tantivy4spark.search.{TantivySearchEngine, TantivyIndexWriter}
import com.tantivy4spark.storage.{S3OptimizedReader, DataLocation}
import com.tantivy4spark.transaction.{TransactionLog, WriteResult}
import com.tantivy4spark.config.{TantivyConfig, SchemaManager}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar
import org.mockito.Mockito._
import com.amazonaws.services.s3.AmazonS3
import scala.util.Success

class TantivyIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase with MockitoSugar {
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    MockTantivyNative.reset()
  }
  
  "Tantivy integration" should "perform end-to-end write and read operations" in {
    val indexPath = testDir.resolve("integration_test").toString
    
    // 1. Write data using TantivyIndexWriter
    val writer = new TantivyIndexWriter(indexPath, TestSchemas.basicSchema, testOptions.toMap)
    val testRows = TestDataGenerator.generateBasicRows(10)
    
    val writeResults = testRows.map(writer.writeRow)
    writeResults should have length 10
    writeResults.foreach(_.recordCount shouldBe 1)
    
    writer.commit()
    writer.close()
    
    // 2. Read data using TantivySearchEngine
    val searchEngine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    val searchResults = searchEngine.search("*", indexPath).toList
    
    // Since we're using mocks, we won't get actual results,
    // but the integration flow should complete without errors
    searchEngine.close()
  }
  
  it should "handle schema management workflow" in {
    val schemaManager = new SchemaManager(testOptions.copy(
      additional = Map("schema.path" -> testDir.resolve("schemas").toString)
    ).toMap)
    
    val indexId = "integration_schema_test"
    
    // 1. Save schema
    val saveResult = schemaManager.saveSchema(indexId, TestSchemas.logSchema)
    saveResult shouldBe a[Success[_]]
    
    // 2. Verify schema exists
    schemaManager.schemaExists(indexId) shouldBe true
    
    // 3. Load schema
    val loadResult = schemaManager.loadSchema(indexId)
    loadResult shouldBe a[Success[_]]
    
    // 4. Get statistics
    val statsResult = schemaManager.getSchemaStatistics(indexId)
    statsResult shouldBe a[Success[_]]
    
    val stats = statsResult.get
    stats("index_id") shouldBe indexId
    stats("field_count") shouldBe TestSchemas.logSchema.fields.length
  }
  
  it should "handle transaction log operations with writes" in {
    val txLog = new TransactionLog(testDir.toString, testOptions.toMap)
    val writer = new TantivyIndexWriter(testDir.toString, TestSchemas.basicSchema, testOptions.toMap)
    
    // Simulate writing with transaction logging
    val testRows = TestDataGenerator.generateBasicRows(5)
    testRows.foreach { row =>
      val writeResult = writer.writeRow(row)
      txLog.appendEntry(writeResult)
    }
    
    // Commit transaction
    writer.commit()
    txLog.commit()
    
    val entries = txLog.getEntries
    entries should have length 6 // 5 writes + 1 commit
    entries.last.operation shouldBe "COMMIT"
    
    writer.close()
  }
  
  it should "handle configuration generation and validation" in {
    // Generate configuration from schema
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    
    // Validate configuration
    val errors = TantivyConfig.validateConfig(config)
    errors shouldBe empty
    
    // Serialize and deserialize
    val json = TantivyConfig.toJson(config)
    val deserializedResult = TantivyConfig.fromJson(json)
    
    deserializedResult shouldBe a[Success[_]]
    val deserializedConfig = deserializedResult.get
    deserializedConfig.basePath shouldBe config.basePath
    deserializedConfig.indexes.head.indexId shouldBe config.indexes.head.indexId
  }
  
  it should "perform file format operations" in {
    val format = new TantivyFileFormat()
    val job = mock[org.apache.hadoop.mapreduce.Job]
    when(job.getConfiguration).thenReturn(createTestConfiguration())
    
    // Test output writer factory creation
    val writerFactory = format.prepareWrite(spark, job, testOptions.toMap, TestSchemas.basicSchema)
    writerFactory shouldBe a[TantivyOutputWriterFactory]
    
    // Test reader function creation
    val readerFunction = format.buildReader(
      spark,
      TestSchemas.basicSchema,
      org.apache.spark.sql.types.StructType(Seq.empty),
      TestSchemas.basicSchema,
      Seq.empty,
      testOptions.toMap,
      createTestConfiguration()
    )
    
    readerFunction shouldBe a[Function1[_, _]]
  }
  
  it should "handle error scenarios gracefully" in {
    // Test writer with invalid path
    val invalidWriter = new TantivyIndexWriter("/invalid/path", TestSchemas.basicSchema, testOptions.toMap)
    val testRow = TestDataGenerator.generateBasicRows(1).head
    
    // Should not throw exception even with invalid path
    val writeResult = invalidWriter.writeRow(testRow)
    writeResult shouldBe a[WriteResult]
    
    // Test search engine with invalid query
    val searchEngine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    val invalidResults = searchEngine.search("", "/invalid/path")
    
    // Should return empty iterator for invalid queries
    invalidResults.hasNext shouldBe false
    
    searchEngine.close()
    invalidWriter.close()
  }
  
  it should "handle concurrent operations safely" in {
    import scala.concurrent.{Future, ExecutionContext}
    import scala.concurrent.duration._
    
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    val indexPath = testDir.resolve("concurrent_test").toString
    
    // Create multiple writers concurrently
    val writerFutures = (1 to 3).map { i =>
      Future {
        val writer = new TantivyIndexWriter(s"${indexPath}_$i", TestSchemas.basicSchema, testOptions.toMap)
        val rows = TestDataGenerator.generateBasicRows(5)
        val results = rows.map(writer.writeRow)
        writer.commit()
        writer.close()
        results
      }
    }
    
    // Wait for all writers to complete
    val allResults = scala.concurrent.Await.result(Future.sequence(writerFutures), 30.seconds)
    
    allResults should have length 3
    allResults.foreach { results =>
      results should have length 5
      results.foreach(_.recordCount shouldBe 1)
    }
  }
}

class TantivyWorkflowIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "Complete Tantivy workflow" should "simulate realistic data pipeline" in {
    val pipelinePath = testDir.resolve("pipeline").toString
    
    // 1. Schema Management Phase
    val schemaManager = new SchemaManager(testOptions.copy(
      additional = Map("schema.path" -> testDir.resolve("schemas").toString)
    ).toMap)
    
    val indexId = "pipeline_index"
    schemaManager.saveSchema(indexId, TestSchemas.logSchema)
    
    // 2. Data Ingestion Phase
    val writer = new TantivyIndexWriter(pipelinePath, TestSchemas.logSchema, testOptions.toMap)
    val txLog = new TransactionLog(pipelinePath, testOptions.toMap)
    
    // Simulate batch ingestion
    val logData = TestDataGenerator.generateLogRows(100)
    val batchSize = 10
    
    logData.grouped(batchSize).foreach { batch =>
      batch.foreach { row =>
        val writeResult = writer.writeRow(row)
        txLog.appendEntry(writeResult)
      }
      
      // Simulate periodic commits
      writer.commit()
    }
    
    // Final commit
    txLog.commit()
    writer.close()
    
    // 3. Search and Retrieval Phase
    val searchEngine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    
    // Perform various searches
    val searches = List(
      "ERROR",
      "level:WARN",
      "service:api",
      "*"
    )
    
    searches.foreach { query =>
      val results = searchEngine.search(query, pipelinePath)
      results shouldBe a[Iterator[_]]
    }
    
    searchEngine.close()
    
    // 4. Verification Phase
    val finalEntries = txLog.getEntries
    finalEntries.last.operation shouldBe "COMMIT"
    
    val stats = schemaManager.getSchemaStatistics(indexId)
    stats shouldBe a[Success[_]]
  }
  
  it should "handle data type conversions correctly" in {
    val conversionPath = testDir.resolve("conversion").toString
    
    // Create schema with all supported types
    val allTypesSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType),
      org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("score", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField("active", org.apache.spark.sql.types.BooleanType),
      org.apache.spark.sql.types.StructField("created", org.apache.spark.sql.types.TimestampType)
    ))
    
    val writer = new TantivyIndexWriter(conversionPath, allTypesSchema, testOptions.toMap)
    
    // Create test row with all data types
    val testRow = InternalRow(
      12345L,
      org.apache.spark.unsafe.types.UTF8String.fromString("Test Name"),
      99.99,
      true,
      System.currentTimeMillis()
    )
    
    val writeResult = writer.writeRow(testRow)
    writeResult shouldBe a[WriteResult]
    writeResult.recordCount shouldBe 1
    
    writer.commit()
    writer.close()
  }
  
  it should "validate schema evolution scenarios" in {
    val evolutionPath = testDir.resolve("evolution").toString
    val schemaManager = new SchemaManager(testOptions.copy(
      additional = Map("schema.path" -> testDir.resolve("evolution_schemas").toString)
    ).toMap)
    
    val indexId = "evolving_index"
    
    // Phase 1: Initial schema
    val initialSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType),
      org.apache.spark.sql.types.StructField("title", org.apache.spark.sql.types.StringType)
    ))
    
    schemaManager.saveSchema(indexId, initialSchema)
    
    // Phase 2: Add compatible field
    val extendedSchema = org.apache.spark.sql.types.StructType(initialSchema.fields ++
      Seq(org.apache.spark.sql.types.StructField("description", org.apache.spark.sql.types.StringType))
    )
    
    val compatibilityResult = schemaManager.validateSchemaCompatibility(indexId, extendedSchema)
    compatibilityResult shouldBe a[Success[_]]
    compatibilityResult.get shouldBe empty // No warnings for adding fields
    
    // Phase 3: Change field type (incompatible)
    val incompatibleSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.StringType), // Changed type
      org.apache.spark.sql.types.StructField("title", org.apache.spark.sql.types.StringType)
    ))
    
    val incompatibilityResult = schemaManager.validateSchemaCompatibility(indexId, incompatibleSchema)
    incompatibilityResult shouldBe a[Success[_]]
    incompatibilityResult.get should not be empty // Should have warnings
  }
}

class TantivyPerformanceIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "Tantivy performance integration" should "handle large batch operations efficiently" in {
    val largeBatchPath = testDir.resolve("large_batch").toString
    val writer = new TantivyIndexWriter(largeBatchPath, TestSchemas.basicSchema, testOptions.copy(
      batchSize = 100,
      segmentSize = 1024 * 1024 // 1MB segments
    ).toMap)
    
    val startTime = System.currentTimeMillis()
    
    // Write 1000 rows
    val largeDataset = TestDataGenerator.generateBasicRows(1000)
    val writeResults = largeDataset.map(writer.writeRow)
    
    val writeTime = System.currentTimeMillis() - startTime
    
    writeResults should have length 1000
    writeResults.foreach(_.recordCount shouldBe 1)
    
    // Commit should be fast
    val commitStartTime = System.currentTimeMillis()
    writer.commit()
    val commitTime = System.currentTimeMillis() - commitStartTime
    
    writer.close()
    
    // Performance assertions (very lenient for test environment)
    writeTime should be < 30000L // Less than 30 seconds for 1000 writes
    commitTime should be < 10000L // Less than 10 seconds for commit
  }
  
  it should "handle concurrent readers efficiently" in {
    import scala.concurrent.{Future, ExecutionContext}
    import scala.concurrent.duration._
    
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    val concurrentPath = testDir.resolve("concurrent_reads").toString
    val hadoopConf = createTestConfiguration()
    
    // Create multiple readers with mock S3 client
    val mockS3 = mock[AmazonS3]
    val readers = (1 to 5).map { _ =>
      new S3OptimizedReader(hadoopConf, testOptions.toMap, Some(mockS3))
    }
    
    val startTime = System.currentTimeMillis()
    
    // Simulate concurrent reads
    val readFutures = readers.zipWithIndex.map { case (reader, index) =>
      Future {
        val location = DataLocation(
          bucket = "test-bucket",
          key = s"concurrent/file_$index.qwt",
          offset = index * 1024L,
          length = 1024L
        )
        reader.readWithPredictiveIO(location, TestSchemas.basicSchema)
      }
    }
    
    val results = scala.concurrent.Await.result(Future.sequence(readFutures), 30.seconds)
    val totalTime = System.currentTimeMillis() - startTime
    
    results should have length 5
    results.foreach(_ shouldBe a[Iterator[_]])
    
    // Clean up
    readers.foreach(_.close())
    
    // Should complete quickly even with concurrent access
    totalTime should be < 15000L // Less than 15 seconds
  }
}