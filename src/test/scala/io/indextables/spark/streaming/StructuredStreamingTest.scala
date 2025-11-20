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

package io.indextables.spark.streaming

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for Spark Structured Streaming integration with IndexTables4Spark V2 DataSource.
 *
 * This test demonstrates:
 *   - Reading CSV files via structured streaming
 *   - Using availableNow trigger for micro-batch processing
 *   - Writing via foreachBatch to IndexTables4Spark
 *   - Verifying data integrity across streaming batches
 */
class StructuredStreamingTest extends AnyFunSuite with TestBase {

  test("Structured streaming with availableNow trigger and foreachBatch to V2 DataSource") {
    // Create directories for CSV input and IndexTables output
    val csvInputDir   = Files.createTempDirectory("csv_input_").toFile.getAbsolutePath
    val outputDir     = Files.createTempDirectory("streaming_output_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_").toFile.getAbsolutePath

    try {
      // Step 1: Write CSV files to disk (simulating incoming data)
      println(s"📁 Writing CSV files to: $csvInputDir")

      // Batch 1: Initial data (100 records)
      writeCsvFile(
        path = s"$csvInputDir/batch1.csv",
        data = (1 to 100).map(id => s"$id,user_$id,content for record $id,${id * 10}")
      )

      // Batch 2: Additional data (50 records)
      writeCsvFile(
        path = s"$csvInputDir/batch2.csv",
        data = (101 to 150).map(id => s"$id,user_$id,content for record $id,${id * 10}")
      )

      // Batch 3: More data (75 records)
      writeCsvFile(
        path = s"$csvInputDir/batch3.csv",
        data = (151 to 225).map(id => s"$id,user_$id,content for record $id,${id * 10}")
      )

      println(s"✅ Created 3 CSV files with 225 total records")

      // Step 2: Set up structured streaming with availableNow trigger
      println(s"🚀 Starting structured streaming job...")

      val streamingQuery = spark.readStream
        .format("csv")
        .option("header", "false")
        .schema("id INT, username STRING, content STRING, score INT")
        .load(csvInputDir)
        .writeStream
        .trigger(Trigger.AvailableNow()) // Process all available data then stop
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          println(s"📦 Processing batch $batchId with ${batchDF.count()} records")

          // Write each batch to IndexTables4Spark using V2 DataSource
          batchDF.write
            .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
            .mode("append") // Append each batch
            .option("spark.indextables.indexing.typemap.content", "text")
            .option("spark.indextables.indexing.fastfields", "score")
            .save(outputDir)

          println(s"✅ Batch $batchId written to IndexTables4Spark")
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      // Wait for streaming query to complete (availableNow will stop after processing all files)
      streamingQuery.awaitTermination()
      println(s"✅ Streaming query completed")

      // Step 3: Verify all data was written correctly
      println(s"🔍 Verifying data integrity...")

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputDir)

      // Verify record count
      val actualCount = resultDf.count()
      assert(actualCount == 225, s"Expected 225 records, got $actualCount")
      println(s"✅ Record count verified: $actualCount records")

      // Verify all IDs are present (1 to 225)
      val readIds     = resultDf.select("id").collect().map(_.getInt(0)).sorted
      val expectedIds = (1 to 225).toArray
      assert(readIds.sameElements(expectedIds), "Not all IDs are present or duplicates exist")
      println(s"✅ All IDs present (1-225) with no duplicates")

      // Verify schema
      val schema = resultDf.schema
      assert(schema.fieldNames.contains("id"), "Missing 'id' field")
      assert(schema.fieldNames.contains("username"), "Missing 'username' field")
      assert(schema.fieldNames.contains("content"), "Missing 'content' field")
      assert(schema.fieldNames.contains("score"), "Missing 'score' field")
      println(s"✅ Schema verified: ${schema.fieldNames.mkString(", ")}")

      // Verify content search (text field) using SQL
      resultDf.createOrReplaceTempView("streaming_data")
      val searchResult = spark.sql("SELECT * FROM streaming_data WHERE content indexquery 'record AND 100'")
      assert(searchResult.count() > 0, "Text search should find results")
      println(s"✅ Text search working: found ${searchResult.count()} results for 'record AND 100'")

      // Verify data values
      val sampleRow = resultDf.filter("id = 100").collect()(0)
      assert(sampleRow.getInt(0) == 100, "ID should be 100")
      assert(sampleRow.getString(1) == "user_100", "Username should be user_100")
      assert(sampleRow.getInt(3) == 1000, "Score should be 1000")
      println(s"✅ Sample row verified: id=100, username=user_100, score=1000")

      println(s"✅ All streaming tests passed!")

    } finally {
      // Cleanup
      deleteDirectory(new File(csvInputDir))
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Structured streaming with multiple batches and deduplication") {
    val csvInputDir   = Files.createTempDirectory("csv_input_dedup_").toFile.getAbsolutePath
    val outputDir     = Files.createTempDirectory("streaming_output_dedup_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_dedup_").toFile.getAbsolutePath

    try {
      println(s"📁 Writing CSV files with potential duplicates...")

      // Write multiple batches with overlapping IDs
      writeCsvFile(
        path = s"$csvInputDir/batch1.csv",
        data = (1 to 50).map(id => s"$id,user_$id,batch1 content $id,${id * 5}")
      )

      writeCsvFile(
        path = s"$csvInputDir/batch2.csv",
        data = (40 to 80).map(id => s"$id,user_${id}_updated,batch2 content $id,${id * 6}")
      )

      println(s"✅ Created CSV files with overlapping IDs (40-50)")

      // Set up streaming with foreachBatch
      val streamingQuery = spark.readStream
        .format("csv")
        .option("header", "false")
        .schema("id INT, username STRING, content STRING, score INT")
        .load(csvInputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          println(s"📦 Processing batch $batchId")

          // For this test, we use append mode which will create duplicates
          // In production, you'd handle deduplication in the foreachBatch logic
          batchDF.write
            .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
            .mode("append")
            .option("spark.indextables.indexing.typemap.content", "text")
            .save(outputDir)
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination()

      // Verify data
      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputDir)

      val totalCount = resultDf.count()
      println(s"📊 Total records written: $totalCount")

      // We expect 91 records (50 from batch1 + 41 from batch2, with 11 overlapping)
      // Since we're appending, overlapping IDs will be duplicated
      assert(totalCount == 91, s"Expected 91 records (50 + 41), got $totalCount")

      // Check for duplicates
      val distinctIds = resultDf.select("id").distinct().count()
      println(s"📊 Distinct IDs: $distinctIds")
      assert(distinctIds == 80, s"Expected 80 distinct IDs (1-80), got $distinctIds")

      println(s"✅ Streaming with overlapping batches verified!")

    } finally {
      deleteDirectory(new File(csvInputDir))
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Structured streaming with partitioned output") {
    val csvInputDir   = Files.createTempDirectory("csv_input_partitioned_").toFile.getAbsolutePath
    val outputDir     = Files.createTempDirectory("streaming_output_partitioned_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_partitioned_").toFile.getAbsolutePath

    try {
      println(s"📁 Writing CSV files with date partitions...")

      // Write data with date column for partitioning
      writeCsvFile(
        path = s"$csvInputDir/data.csv",
        data = Seq(
          "1,user1,content 1,100,2024-01-01",
          "2,user2,content 2,200,2024-01-01",
          "3,user3,content 3,300,2024-01-02",
          "4,user4,content 4,400,2024-01-02",
          "5,user5,content 5,500,2024-01-03"
        )
      )

      // Set up streaming with partitioned output
      val streamingQuery = spark.readStream
        .format("csv")
        .option("header", "false")
        .schema("id INT, username STRING, content STRING, score INT, date STRING")
        .load(csvInputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          println(s"📦 Processing batch $batchId with partitioning by date")

          batchDF.write
            .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
            .mode("append")
            .partitionBy("date") // Partition by date
            .option("spark.indextables.indexing.typemap.content", "text")
            .option("spark.indextables.indexing.fastfields", "score")
            .save(outputDir)
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination()

      // Verify partitioned data
      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputDir)

      val totalCount = resultDf.count()
      assert(totalCount == 5, s"Expected 5 records, got $totalCount")

      // Test partition pruning
      val date1Count = resultDf.filter("date = '2024-01-01'").count()
      assert(date1Count == 2, s"Expected 2 records for 2024-01-01, got $date1Count")

      val date2Count = resultDf.filter("date = '2024-01-02'").count()
      assert(date2Count == 2, s"Expected 2 records for 2024-01-02, got $date2Count")

      val date3Count = resultDf.filter("date = '2024-01-03'").count()
      assert(date3Count == 1, s"Expected 1 record for 2024-01-03, got $date3Count")

      println(s"✅ Partitioned streaming output verified!")

    } finally {
      deleteDirectory(new File(csvInputDir))
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  // Helper method to write CSV files
  private def writeCsvFile(path: String, data: Seq[String]): Unit = {
    val writer = new PrintWriter(new File(path))
    try
      // Write data only (no header - schema is inferred in streaming reader)
      data.foreach(writer.println)
    finally
      writer.close()
  }

  // Helper method to recursively delete directory
  private def deleteDirectory(directory: File): Unit =
    if (directory.exists()) {
      Option(directory.listFiles()).foreach(_.foreach { file =>
        if (file.isDirectory) deleteDirectory(file)
        else file.delete()
      })
      directory.delete()
    }
}
