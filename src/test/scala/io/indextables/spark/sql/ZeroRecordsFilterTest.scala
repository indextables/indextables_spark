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

package io.indextables.spark.sql

import io.indextables.spark.transaction.{AddAction, TransactionLogFactory, TransactionLog}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

/** Test to validate that files with zero records are not inserted into the transaction log */
class ZeroRecordsFilterTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: File       = _

  override def beforeEach(): Unit = {
    // Create temporary directory for this test
    tempDir = Files.createTempDirectory("tantivy4spark_zero_records_test").toFile
    tempDir.deleteOnExit()

    // Create Spark session
    spark = SparkSession
      .builder()
      .appName("ZeroRecordsFilterTest")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.Tantivy4SparkExtensions")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  "Tantivy4Spark write operations" should "not create transaction log entries for files with zero records" in {
    val tablePath = new File(tempDir, "zero_records_test").getAbsolutePath

    // Create an empty DataFrame (zero records)
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val schema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("content", StringType, true),
        StructField("score", IntegerType, true)
      )
    )

    val emptyRDD  = spark.sparkContext.emptyRDD[Row]
    val emptyData = spark.createDataFrame(emptyRDD, schema)

    // Verify the DataFrame is indeed empty
    emptyData.count() shouldBe 0

    // Write the empty DataFrame
    emptyData.write
      .format("tantivy4spark")
      .option("spark.indextables.indexing.typemap.content", "text")
      .save(tablePath)

    // Check transaction log to ensure no AddActions were created
    val hadoopPath     = new org.apache.hadoop.fs.Path(tablePath)
    val emptyOptions   = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    val transactionLog = TransactionLogFactory.create(hadoopPath, spark, emptyOptions)

    // Get AddActions directly
    val addActions     = transactionLog.listFiles()
    val metadataAction = transactionLog.getMetadata()

    println(s"AddActions: ${addActions.length}")
    println(s"MetadataAction found: ${if (metadataAction != null) "yes" else "no"}")

    // Should have metadata action but no add actions
    metadataAction should not be null
    addActions.length shouldBe 0

    println("✅ Verified: No AddActions created for empty dataset")

    // Verify we can still read from the table (should return empty)
    val readBack = spark.read.format("tantivy4spark").load(tablePath)
    readBack.count() shouldBe 0

    println("✅ Verified: Empty table can be read successfully")
  }

  "Tantivy4Spark write operations" should "handle mixed empty and non-empty partitions correctly" in {
    val tablePath = new File(tempDir, "mixed_partitions_test").getAbsolutePath

    // Create a DataFrame with some data
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val schema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("content", StringType, true),
        StructField("score", IntegerType, true)
      )
    )

    val testDataRows = Seq(
      Row("doc1", "first document", 1),
      Row("doc2", "second document", 2)
    )

    val testDataRDD = spark.sparkContext.parallelize(testDataRows)
    val testData    = spark.createDataFrame(testDataRDD, schema)

    // Write initial data
    testData.write
      .format("tantivy4spark")
      .option("spark.indextables.indexing.typemap.content", "text")
      .save(tablePath)

    // Get initial transaction log state
    val hadoopPath        = new org.apache.hadoop.fs.Path(tablePath)
    val emptyOptions      = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    val transactionLog    = TransactionLogFactory.create(hadoopPath, spark, emptyOptions)
    val initialAddActions = transactionLog.listFiles()

    println(s"Initial AddActions: ${initialAddActions.length}")
    initialAddActions.foreach(add => println(s"  Path: ${add.path}, NumRecords: ${add.numRecords}"))

    // Verify we have at least one AddAction for the non-empty data
    initialAddActions.length should be > 0
    initialAddActions.foreach { add =>
      add.numRecords shouldBe defined
      add.numRecords.get should be > 0L
    }

    // Now append empty data (should not create additional AddActions)
    val emptyRDD2 = spark.sparkContext.emptyRDD[Row]
    val emptyData = spark.createDataFrame(emptyRDD2, schema)

    emptyData.write
      .format("tantivy4spark")
      .mode("append")
      .option("spark.indextables.indexing.typemap.content", "text")
      .save(tablePath)

    // Check final transaction log state
    val finalAddActions = transactionLog.listFiles()

    println(s"Final AddActions: ${finalAddActions.length}")
    finalAddActions.foreach(add => println(s"  Path: ${add.path}, NumRecords: ${add.numRecords}"))

    // Should have same number of AddActions (empty append didn't add any)
    finalAddActions.length shouldBe initialAddActions.length

    // Verify data integrity
    val readBack = spark.read.format("tantivy4spark").load(tablePath)
    readBack.count() shouldBe 2 // Original 2 records should still be there

    println("✅ Verified: Empty append operation did not create unnecessary AddActions")
  }
}
