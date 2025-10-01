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

package io.indextables.spark.core

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{AddAction, TransactionLog, TransactionLogFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{Filter, EqualTo}
import org.apache.spark.sql.types.{StructType, StringType, StructField, LongType}
import org.scalatest.matchers.should.Matchers
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
 * Tests to validate that Tantivy4SparkInputPartition correctly preserves footer metadata during AQE stage-wise
 * execution. InputPartitions are serialized and sent to executors, so this test ensures footer metadata survives this
 * process.
 */
class AQEInputPartitionValidationTest extends TestBase with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    println("🔧 AQE enabled for InputPartition validation tests")
  }

  test("should serialize Tantivy4SparkInputPartition with footer metadata") {
    withTempPath { tempPath =>
      // Create test data
      val data = spark
        .range(150)
        .select(
          col("id"),
          (col("id") % 6).cast("string").as("category"),
          (col("id") * 3).as("value")
        )

      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "30")
        .save(tempPath)

      // Get AddActions from transaction log
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      addActions.length should be > 0
      println(s"📊 Testing InputPartition serialization with ${addActions.length} AddActions")

      val readSchema = StructType(
        Array(
          StructField("id", LongType),
          StructField("category", StringType),
          StructField("value", LongType)
        )
      )

      val filters = Array[Filter](EqualTo("category", "1"))

      addActions.zipWithIndex.foreach {
        case (addAction, index) =>
          println(s"\n🧪 Testing InputPartition $index:")

          // Validate AddAction has footer metadata
          addAction.hasFooterOffsets shouldBe true
          addAction.footerStartOffset shouldBe defined
          addAction.footerEndOffset shouldBe defined
          addAction.docMappingJson shouldBe defined

          val originalFooterStart = addAction.footerStartOffset.get.asInstanceOf[Number].longValue()
          val originalFooterEnd   = addAction.footerEndOffset.get.asInstanceOf[Number].longValue()
          val originalDocMapping  = addAction.docMappingJson.get

          println(s"   AddAction footer: $originalFooterStart-$originalFooterEnd")
          println(s"   AddAction docMapping: ${originalDocMapping.length} chars")

          // Create InputPartition
          val inputPartition = new Tantivy4SparkInputPartition(
            addAction = addAction,
            readSchema = readSchema,
            filters = filters,
            partitionId = index,
            limit = Some(10),
            indexQueryFilters = Array.empty
          )

          // Test serialization
          val deserializedPartition = serializeDeserializeInputPartition(inputPartition)

          // Validate deserialized InputPartition
          deserializedPartition.addAction.hasFooterOffsets shouldBe true // Deserialized partition should have footer offsets
          deserializedPartition.addAction.footerStartOffset shouldBe defined // Deserialized partition should have footer start
          deserializedPartition.addAction.footerEndOffset shouldBe defined // Deserialized partition should have footer end
          deserializedPartition.addAction.docMappingJson shouldBe defined // Deserialized partition should have doc mapping

          // Validate exact values are preserved
          val deserializedFooterStart =
            deserializedPartition.addAction.footerStartOffset.get.asInstanceOf[Number].longValue()
          val deserializedFooterEnd =
            deserializedPartition.addAction.footerEndOffset.get.asInstanceOf[Number].longValue()
          deserializedFooterStart shouldBe originalFooterStart
          deserializedFooterEnd shouldBe originalFooterEnd
          deserializedPartition.addAction.docMappingJson.get shouldBe originalDocMapping
          deserializedPartition.addAction.path shouldBe addAction.path
          deserializedPartition.addAction.size shouldBe addAction.size

          // Validate other InputPartition fields
          deserializedPartition.partitionId shouldBe index
          deserializedPartition.readSchema shouldBe readSchema
          deserializedPartition.filters.length shouldBe filters.length
          deserializedPartition.limit shouldBe Some(10)

          println(s"   ✅ InputPartition $index serialization preserved all metadata")
      }

      println(s"✅ All ${addActions.length} InputPartitions serialized correctly with footer metadata")
    }
  }

  test("should create PartitionReader with preserved footer metadata") {
    withTempPath { tempPath =>
      // Create test data
      val data = spark
        .range(120)
        .select(
          col("id"),
          (col("id") % 4).cast("string").as("group"),
          (rand() * 50).as("score")
        )

      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "25")
        .save(tempPath)

      // Get transaction log
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      val readSchema = StructType(
        Array(
          StructField("id", LongType),
          StructField("group", StringType),
          StructField("score", org.apache.spark.sql.types.DoubleType)
        )
      )

      // Create reader factory with broadcast config
      val emptyBroadcastConfig = spark.sparkContext.broadcast(Map.empty[String, String])
      val readerFactory = new Tantivy4SparkReaderFactory(
        readSchema = readSchema,
        limit = Some(15),
        config = emptyBroadcastConfig.value,
        tablePath = new Path(tempPath)
      )

      addActions.zipWithIndex.foreach {
        case (addAction, index) =>
          println(s"\n🧪 Testing PartitionReader creation for partition $index:")

          // Create InputPartition
          val inputPartition = new Tantivy4SparkInputPartition(
            addAction = addAction,
            readSchema = readSchema,
            filters = Array.empty,
            partitionId = index,
            limit = Some(15)
          )

          // Serialize and deserialize to simulate AQE behavior
          val serializedPartition = serializeDeserializeInputPartition(inputPartition)

          // Create PartitionReader - this will validate footer metadata internally
          try {
            val partitionReader = readerFactory.createReader(serializedPartition)

            // Read some records to validate the reader works
            var recordCount = 0
            while (partitionReader.next() && recordCount < 10) {
              val row = partitionReader.get()
              row should not be null
              recordCount += 1
            }

            partitionReader.close()

            println(s"   ✅ PartitionReader created and read $recordCount records successfully")

          } catch {
            case e: Exception if e.getMessage.contains("footer") || e.getMessage.contains("metadata") =>
              fail(s"PartitionReader creation failed due to metadata issue: ${e.getMessage}")
            case e: Exception =>
              // Re-throw non-metadata exceptions
              throw e
          }
      }

      println(s"✅ All ${addActions.length} PartitionReaders created successfully after serialization")
    }
  }

  test("should handle InputPartition preferred locations with footer metadata") {
    withTempPath { tempPath =>
      // Create test data
      val data = spark
        .range(100)
        .select(
          col("id"),
          (col("id") % 3).cast("string").as("type"),
          (col("id") + 100).as("modified_id")
        )

      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "20")
        .save(tempPath)

      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      val readSchema = StructType(
        Array(
          StructField("id", LongType),
          StructField("type", StringType),
          StructField("modified_id", LongType)
        )
      )

      addActions.zipWithIndex.foreach {
        case (addAction, index) =>
          // Create InputPartition
          val inputPartition = new Tantivy4SparkInputPartition(
            addAction = addAction,
            readSchema = readSchema,
            filters = Array.empty,
            partitionId = index
          )

          // Test preferred locations (should work without throwing exceptions)
          val preferredLocations = inputPartition.preferredLocations()
          preferredLocations should not be null

          println(s"   Partition $index preferred locations: ${preferredLocations.mkString(", ")}")

          // Serialize and test preferred locations after deserialization
          val deserializedPartition = serializeDeserializeInputPartition(inputPartition)
          val deserializedLocations = deserializedPartition.preferredLocations()

          // Locations may be empty (no cache history), but should be the same before/after serialization
          deserializedLocations.length shouldBe preferredLocations.length

          println(s"   ✅ Partition $index preferred locations preserved after serialization")
      }
    }
  }

  test("should validate footer metadata in complex AQE scenarios") {
    withTempPath { tempPath =>
      // Create partitioned data
      val complexData = spark
        .range(240)
        .select(
          col("id"),
          (col("id") % 8).cast("string").as("partition_key"),
          (col("id") % 12).cast("string").as("sort_key"),
          (rand() * 200).as("metric")
        )

      complexData.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "30")
        .partitionBy("partition_key")
        .save(tempPath)

      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      // Complex query that will create many InputPartitions
      val complexQuery = df
        .filter(col("metric") > 50)
        .groupBy("partition_key", "sort_key")
        .agg(
          count("*").as("record_count"),
          avg("metric").as("avg_metric")
        )
        .filter(col("record_count") > 2)
        .orderBy(col("avg_metric").desc)
        .limit(20)

      // Execute query to trigger AQE and InputPartition serialization
      val result = complexQuery.collect()

      result.length should be <= 20
      println(s"✅ Complex AQE query returned ${result.length} rows")

      // Validate results show proper data processing
      result.foreach { row =>
        val partitionKey = row.getString(0)
        val sortKey      = row.getString(1)
        val recordCount  = row.getLong(2)
        val avgMetric    = row.getDouble(3)

        partitionKey should not be null
        sortKey should not be null
        recordCount should be > 2L // Filter condition
        avgMetric should be >= 0.0
      }

      println("✅ Footer metadata preserved during complex AQE scenario execution")
    }
  }

  /** Helper method to serialize and deserialize InputPartition */
  private def serializeDeserializeInputPartition(partition: Tantivy4SparkInputPartition)
    : Tantivy4SparkInputPartition = {
    val baos = new ByteArrayOutputStream()
    val oos  = new ObjectOutputStream(baos)

    try {
      oos.writeObject(partition)
      oos.flush()

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois  = new ObjectInputStream(bais)

      val deserialized = ois.readObject().asInstanceOf[Tantivy4SparkInputPartition]
      ois.close()
      deserialized
    } finally
      oos.close()
  }

  test("should preserve all AddAction metadata fields during serialization") {
    withTempPath { tempPath =>
      // Create comprehensive test data
      val data = spark
        .range(80)
        .select(
          col("id"),
          (col("id") % 2).cast("string").as("even_odd"),
          current_timestamp().as("timestamp_col"),
          (col("id") * col("id")).as("squared")
        )

      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "16")
        .save(tempPath)

      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      addActions.foreach { addAction =>
        // Validate comprehensive metadata
        println(s"\n🔍 Comprehensive metadata validation for: ${addAction.path}")

        // Core metadata
        addAction.path should not be null
        addAction.size should be > 0L
        addAction.modificationTime should be > 0L

        // Footer metadata
        addAction.hasFooterOffsets shouldBe true
        addAction.footerStartOffset shouldBe defined
        addAction.footerEndOffset shouldBe defined
        // Hotcache fields are deprecated in v0.24.1 and set to None
        addAction.hotcacheStartOffset shouldBe None
        addAction.hotcacheLength shouldBe None
        addAction.docMappingJson shouldBe defined

        // Additional metadata
        addAction.numRecords shouldBe defined
        addAction.uncompressedSizeBytes shouldBe defined

        val originalMetadata = Map(
          "path"             -> addAction.path,
          "size"             -> addAction.size,
          "footerStart"      -> addAction.footerStartOffset.get.asInstanceOf[Number].longValue(),
          "footerEnd"        -> addAction.footerEndOffset.get.asInstanceOf[Number].longValue(),
          "docMappingLength" -> addAction.docMappingJson.get.length,
          "numRecords"       -> addAction.numRecords.get.asInstanceOf[Number].longValue(),
          "uncompressedSize" -> addAction.uncompressedSizeBytes.get.asInstanceOf[Number].longValue()
        )

        // Test serialization of InputPartition containing this AddAction
        val inputPartition = new Tantivy4SparkInputPartition(
          addAction = addAction,
          readSchema = data.schema,
          filters = Array.empty,
          partitionId = 0
        )

        val deserializedPartition = serializeDeserializeInputPartition(inputPartition)
        val deserializedAction    = deserializedPartition.addAction

        // Validate all metadata preserved
        deserializedAction.path shouldBe originalMetadata("path")
        deserializedAction.size shouldBe originalMetadata("size")
        val finalDeserializedFooterStart = deserializedAction.footerStartOffset.get.asInstanceOf[Number].longValue()
        val finalDeserializedFooterEnd   = deserializedAction.footerEndOffset.get.asInstanceOf[Number].longValue()
        finalDeserializedFooterStart shouldBe originalMetadata("footerStart")
        finalDeserializedFooterEnd shouldBe originalMetadata("footerEnd")
        // Validate hotcache fields remain None after serialization (deprecated in v0.24.1)
        deserializedAction.hotcacheStartOffset shouldBe None
        deserializedAction.hotcacheLength shouldBe None
        deserializedAction.docMappingJson.get.length shouldBe originalMetadata("docMappingLength")
        val deserializedNumRecords       = deserializedAction.numRecords.get.asInstanceOf[Number].longValue()
        val deserializedUncompressedSize = deserializedAction.uncompressedSizeBytes.get.asInstanceOf[Number].longValue()
        deserializedNumRecords shouldBe originalMetadata("numRecords")
        deserializedUncompressedSize shouldBe originalMetadata("uncompressedSize")

        println(s"   ✅ All comprehensive metadata preserved through serialization")
      }

      println(s"✅ Comprehensive metadata validation completed for ${addActions.length} AddActions")
    }
  }
}
