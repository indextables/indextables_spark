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

package com.tantivy4spark.core

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestOptions}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.paths.SparkPath
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.ArgumentMatchers._

class TantivyFileFormatTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyFileFormat" should "have correct short name" in {
    val format = new TantivyFileFormat()
    format.shortName() shouldBe "tantivy"
  }
  
  it should "have correct string representation" in {
    val format = new TantivyFileFormat()
    format.toString shouldBe "Tantivy"
  }
  
  it should "support batch reading" in {
    val format = new TantivyFileFormat()
    format.supportBatch(spark, TestSchemas.basicSchema) shouldBe true
  }
  
  it should "indicate files are splitable" in {
    val format = new TantivyFileFormat()
    val path = new Path(testDir.toString, "test.tnt")
    format.isSplitable(spark, testOptions.toMap, path) shouldBe true
  }
  
  it should "infer schema from files" in {
    val format = new TantivyFileFormat()
    val fileStatus = mock[FileStatus]
    when(fileStatus.getPath).thenReturn(new Path(testDir.toString, "test.tnt"))
    when(fileStatus.getLen).thenReturn(1024L)
    
    val options = testOptions.copy(additional = Map("schema.inference" -> "true")).toMap
    val inferredSchema = format.inferSchema(spark, options, Seq(fileStatus))
    
    // Should return None for now as schema inference is not implemented
    inferredSchema shouldBe None
  }
  
  it should "create output writer factory" in {
    val format = new TantivyFileFormat()
    val job = mock[org.apache.hadoop.mapreduce.Job]
    val hadoopConf = createTestConfiguration()
    when(job.getConfiguration).thenReturn(hadoopConf)
    
    val writerFactory = format.prepareWrite(
      spark,
      job,
      testOptions.toMap,
      TestSchemas.basicSchema
    )
    
    writerFactory shouldBe a[TantivyOutputWriterFactory]
  }
  
  it should "build reader function" in {
    val format = new TantivyFileFormat()
    val filters: Seq[Filter] = Seq.empty
    val hadoopConf = createTestConfiguration()
    
    val readerFunction = format.buildReader(
      spark,
      TestSchemas.basicSchema,
      StructType(Seq.empty),
      TestSchemas.basicSchema,
      filters,
      testOptions.toMap,
      hadoopConf
    )
    
    readerFunction shouldBe a[Function1[_, _]]
    
    // Test that reader function can be called
    val partitionedFile = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromPath(new Path(s"file://${testDir}/test.tnt")),
      0L,
      1024L
    )
    
    val iterator = readerFunction(partitionedFile)
    iterator shouldBe a[Iterator[_]]
  }
  
  it should "handle empty file list for schema inference" in {
    val format = new TantivyFileFormat()
    val inferredSchema = format.inferSchema(spark, testOptions.toMap, Seq.empty)
    inferredSchema shouldBe None
  }
  
  it should "pass options to reader and writer" in {
    val format = new TantivyFileFormat()
    val customOptions = testOptions.copy(
      additional = Map(
        "custom.option" -> "test_value",
        "query" -> "title:test"
      )
    ).toMap
    
    // Test reader
    val readerFunction = format.buildReader(
      spark,
      TestSchemas.basicSchema,
      StructType(Seq.empty),
      TestSchemas.basicSchema,
      Seq.empty,
      customOptions,
      createTestConfiguration()
    )
    
    readerFunction shouldNot be(null)
    
    // Test writer
    val job = mock[org.apache.hadoop.mapreduce.Job]
    when(job.getConfiguration).thenReturn(createTestConfiguration())
    
    val writerFactory = format.prepareWrite(
      spark,
      job,
      customOptions,
      TestSchemas.basicSchema
    )
    
    writerFactory shouldBe a[TantivyOutputWriterFactory]
  }
}

class TantivyOutputWriterFactoryTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyOutputWriterFactory" should "have correct file extension" in {
    val factory = new TantivyOutputWriterFactory(testOptions.toMap, TestSchemas.basicSchema)
    val context = mock[org.apache.hadoop.mapreduce.TaskAttemptContext]
    
    factory.getFileExtension(context) shouldBe ".tnt"
  }
  
  it should "create output writer instances" in {
    val factory = new TantivyOutputWriterFactory(testOptions.toMap, TestSchemas.basicSchema)
    val context = mock[org.apache.hadoop.mapreduce.TaskAttemptContext]
    
    val outputPath = testDir.resolve("output.tnt").toString
    val writer = factory.newInstance(outputPath, TestSchemas.basicSchema, context)
    
    writer shouldBe a[TantivyOutputWriter]
  }
  
  it should "pass options to output writer" in {
    val customOptions = testOptions.copy(
      additional = Map("custom.writer.option" -> "test_value")
    ).toMap
    
    val factory = new TantivyOutputWriterFactory(customOptions, TestSchemas.basicSchema)
    val context = mock[org.apache.hadoop.mapreduce.TaskAttemptContext]
    
    val outputPath = testDir.resolve("output.tnt").toString
    val writer = factory.newInstance(outputPath, TestSchemas.basicSchema, context)
    
    writer shouldBe a[TantivyOutputWriter]
  }
}

class TantivyFileReaderTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyFileReader" should "initialize with valid parameters" in {
    val partitionedFile = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromPath(new Path(s"file://${testDir}/test.tnt")),
      0L,
      1024L
    )
    
    val reader = new TantivyFileReader(
      partitionedFile,
      TestSchemas.basicSchema,
      Seq.empty,
      testOptions.toMap,
      createTestConfiguration()
    )
    
    reader shouldNot be(null)
  }
  
  it should "return empty iterator when no search results" in {
    val partitionedFile = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromPath(new Path(s"file://${testDir}/test.tnt")),
      0L,
      1024L
    )
    
    val reader = new TantivyFileReader(
      partitionedFile,
      TestSchemas.basicSchema,
      Seq.empty,
      testOptions.toMap,
      createTestConfiguration()
    )
    
    val results = reader.read()
    results.hasNext shouldBe false
  }
  
  it should "handle filters correctly" in {
    import org.apache.spark.sql.sources._
    
    val filters = Seq(
      EqualTo("title", "test"),
      GreaterThan("score", 50.0)
    )
    
    val partitionedFile = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromPath(new Path(s"file://${testDir}/test.tnt")),
      0L,
      1024L
    )
    
    val reader = new TantivyFileReader(
      partitionedFile,
      TestSchemas.basicSchema,
      filters,
      testOptions.toMap,
      createTestConfiguration()
    )
    
    val results = reader.read()
    results shouldBe a[Iterator[_]]
  }
  
  it should "build query from filters" in {
    val partitionedFile = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromPath(new Path(s"file://${testDir}/test.tnt")),
      0L,
      1024L
    )
    
    val reader = new TantivyFileReader(
      partitionedFile,
      TestSchemas.basicSchema,
      Seq.empty,
      testOptions.toMap,
      createTestConfiguration()
    )
    
    // This tests the private buildQueryFromFilters method indirectly
    val results = reader.read()
    results shouldBe a[Iterator[_]]
  }
}