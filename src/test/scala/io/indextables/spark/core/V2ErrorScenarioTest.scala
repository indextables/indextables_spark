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

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test error scenarios in V2 DataSource API including:
 *   - Non-existent tables/paths
 *   - Corrupted transaction logs
 *   - Permission errors
 *   - Invalid configurations
 *   - Schema validation errors
 */
class V2ErrorScenarioTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should handle non-existent table gracefully") {
    val nonExistentPath = tempDir + "/does-not-exist"

    // Verify directory doesn't exist
    assert(!new File(nonExistentPath).exists())

    // Attempt to read non-existent table using V2 API
    val exception = intercept[RuntimeException] {
      spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(nonExistentPath)
        .count()
    }

    // Verify proper error message
    exception.getMessage should include("Path does not exist")
    exception.getMessage should include(nonExistentPath)
  }

  ignore("should handle corrupted transaction log") {
    withTempPath { path =>
      // Create valid table first
      val data = spark
        .range(10)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("name")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Verify table works initially
      val initialResult = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
      initialResult.count() shouldBe 10

      // Corrupt the table by deleting the transaction log (cleaner error path)
      val transactionLogDir = new File(path, "_transaction_log")
      val logFiles          = transactionLogDir.listFiles().filter(_.getName.endsWith(".json"))
      logFiles.length should be > 0

      // Delete the transaction log files to simulate corruption
      logFiles.foreach(_.delete())
      // Verify the transaction log directory is now empty
      transactionLogDir.listFiles().filter(_.getName.endsWith(".json")).length shouldBe 0

      // Attempt to read corrupted table
      val exception = intercept[Exception] {
        spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(path)
          .count()
      }

      // Verify error handling
      val message = exception.getMessage
      message should (include("Path does not exist") or include("No transaction log found") or include(
        "Table does not exist"
      ))
    }
  }

  ignore("should handle invalid configuration gracefully") {
    withTempPath { path =>
      val data = spark.range(5).select(col("id"))

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test with invalid cache size
      val exception1 = intercept[Exception] {
        spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.cache.maxSize", "invalid-number")
          .load(path)
          .count()
      }

      exception1.getMessage should include("NumberFormat")
    }
  }

  ignore("should handle schema validation errors") {
    withTempPath { path =>
      // Create table with unsupported data type
      val data = spark
        .range(5)
        .select(
          col("id"),
          array(lit("a"), lit("b")).as("unsupported_array") // Array type not supported
        )

      // This should fail during write with clear error message
      val exception = intercept[UnsupportedOperationException] {
        data.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(path)
      }

      // Verify error message mentions unsupported type
      exception.getMessage should (include("ArrayType") or include("array") or include("unsupported"))
    }
  }

  ignore("should handle empty dataset gracefully") {
    withTempPath { path =>
      // Create empty dataset
      val emptyData = spark.emptyDataFrame
        .select(
          lit(1).as("id"),
          lit("test").as("name")
        )
        .filter(lit(false)) // Filter out all rows

      // Write empty dataset
      emptyData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read empty dataset
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 0
      result.schema.fieldNames should contain theSameElementsAs Array("id", "name")
    }
  }

  ignore("should handle concurrent access gracefully") {
    withTempPath { path =>
      val data = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Value "), col("id")).as("value")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Simulate concurrent reads
      val futures = (1 to 3).map { _ =>
        scala.concurrent.Future {
          val result = spark.read
            .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
            .load(path)
          result.count()
        }(scala.concurrent.ExecutionContext.global)
      }

      // Wait for all concurrent reads to complete
      import scala.concurrent.duration._
      import scala.concurrent.Await

      val results = futures.map(Await.result(_, 30.seconds))

      // All reads should succeed with same count
      results.foreach(_ shouldBe 100)
    }
  }

  ignore("should handle malformed split files") {
    withTempPath { path =>
      // Create valid table first
      val data = spark
        .range(5)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("name")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Find split files
      val splitFiles = new File(path)
        .listFiles()
        .filter(_.getName.endsWith(".split"))

      splitFiles.length should be > 0

      // Corrupt a split file by truncating it
      val splitFile        = splitFiles.head
      val originalSize     = splitFile.length()
      val truncatedContent = Files.readAllBytes(splitFile.toPath).take(10) // Keep only first 10 bytes
      Files.write(splitFile.toPath, truncatedContent)

      splitFile.length() should be < originalSize

      // Attempt to read corrupted table
      val exception = intercept[Exception] {
        spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(path)
          .collect()
      }

      // Should fail with split-related error
      val message = exception.getMessage
      message should (include("split") or include("Failed to read") or include("corrupted"))
    }
  }

  ignore("should handle invalid path characters") {
    // Test various invalid path scenarios
    val invalidPaths = Seq(
      "hdfs://invalid-host:999999/path", // Invalid HDFS host
      "s3://",                           // Incomplete S3 path
      "invalid-protocol://bucket/path"   // Invalid protocol
    )

    invalidPaths.foreach { invalidPath =>
      val exception = intercept[Exception] {
        spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(invalidPath)
          .count()
      }

      // Should fail with appropriate error
      val message = exception.getMessage.toLowerCase
      message should (include("path") or include("invalid") or include("not found") or include("protocol"))
    }
  }

  ignore("should handle permission denied scenarios") {
    withTempPath { path =>
      val data = spark.range(5).select(col("id"))

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Create a directory that simulates permission issues
      val restrictedPath = tempDir + "/restricted"
      new File(restrictedPath).mkdirs()

      // Try to write to a path that doesn't allow writes (this is OS-dependent)
      // This test may not always trigger a permission error in all environments
      try
        spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load("/root/cannot-access-this-path")
          .count()

      // If no exception, this is acceptable as permission restrictions vary by OS
      catch {
        case e: Exception =>
          // Should fail with permission-related error
          val message = e.getMessage.toLowerCase
          message should (include("permission") or include("access") or include("denied") or
            include("not found") or include("cannot"))
      }
    }
  }

  test("should fail job when split reading fails rather than returning partial results") {
    withTempPath { path =>
      // Create valid data and write it
      val data = spark
        .range(3)
        .select(
          concat(lit("doc"), col("id").cast("string")).as("id"),
          concat(lit("content"), col("id").cast("string")).as("content"),
          (col("id") * 100 + 100).cast("int").as("score")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Verify data is written correctly
      val validRead = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
      validRead.count() shouldBe 3

      // Now corrupt the split file to simulate a reading failure
      val splitFiles = new File(path).listFiles().filter(_.getName.endsWith(".split"))
      splitFiles.length should be > 0

      val splitFile = splitFiles(0)
      println(s"Corrupting split file: ${splitFile.getAbsolutePath}")

      // Corrupt the split by truncating it (simulates corrupted/incomplete file)
      val writer = new FileWriter(splitFile, false)
      writer.write("CORRUPTED DATA")
      writer.close()

      // Attempt to read the corrupted table - should fail rather than return partial results
      val exception = intercept[Exception] {
        val corruptedRead = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(path)

        // Try to collect the data - this should fail
        corruptedRead.collect()
      }

      // Verify that the exception indicates a reading failure
      val message = exception.getMessage
      println(s"Exception message: $message")

      // Should contain error message about failed read
      // The exact message depends on where the failure occurs (could be RuntimeException, IOException, etc.)
      message should (
        include("Failed to read") or
          include("Failed to initialize") or
          include("Failed to read partition") or
          include("Failed to read Tantivy index") or
          include("Error reading") or
          include("Cannot read")
      )

      println("✅ Job correctly failed when split reading encountered error")
    }
  }
}
