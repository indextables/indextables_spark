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

package io.indextables.spark.demo

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.core.IndexTables4SparkScan
import io.indextables.spark.transaction.{AddAction, TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase

/**
 * Demonstration of data skipping functionality in IndexTables4Spark.
 *
 * This demo shows how the query engine automatically skips reading files that cannot contain matching data based on
 * min/max statistics.
 */
object DataSkippingDemo extends TestBase {

  def main(args: Array[String]): Unit = {
    println("🚀 IndexTables4Spark Data Skipping Demo")
    println("=====================================")

    // Initialize Spark and test environment
    beforeAll()
    beforeEach()

    try
      demonstrateDataSkipping()
    finally {
      afterEach()
      afterAll()
    }
  }

  private def demonstrateDataSkipping(): Unit = {
    val testTablePath  = new Path(tempDir, "data_skipping_demo")
    val transactionLog = TransactionLogFactory.create(testTablePath, spark)

    val schema = StructType(
      Array(
        StructField("customer_id", LongType, false),
        StructField("order_amount", DoubleType, false),
        StructField("region", StringType, false)
      )
    )

    transactionLog.initialize(schema)

    println("\n📁 Setting up test data with 5 files containing different ranges:")

    // Create 5 files with distinct data ranges
    val files = Seq(
      (
        "Q1_orders.tnt4s",
        Map("customer_id" -> "1", "order_amount"    -> "10.50", "region"  -> "East"),
        Map("customer_id" -> "1000", "order_amount" -> "999.99", "region" -> "East")
      ),
      (
        "Q2_orders.tnt4s",
        Map("customer_id" -> "1001", "order_amount" -> "25.00", "region"   -> "West"),
        Map("customer_id" -> "2000", "order_amount" -> "1500.00", "region" -> "West")
      ),
      (
        "Q3_orders.tnt4s",
        Map("customer_id" -> "2001", "order_amount" -> "15.75", "region"   -> "North"),
        Map("customer_id" -> "3000", "order_amount" -> "1200.50", "region" -> "North")
      ),
      (
        "Q4_orders.tnt4s",
        Map("customer_id" -> "3001", "order_amount" -> "5.99", "region"    -> "South"),
        Map("customer_id" -> "4000", "order_amount" -> "2000.00", "region" -> "South")
      ),
      (
        "Special_orders.tnt4s",
        Map("customer_id" -> "10001", "order_amount" -> "50000.00", "region"  -> "Global"),
        Map("customer_id" -> "10010", "order_amount" -> "100000.00", "region" -> "Global")
      )
    )

    files.foreach {
      case (filename, minVals, maxVals) =>
        val addAction = AddAction(
          path = filename,
          partitionValues = Map.empty,
          size = 1024L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(1000L),
          minValues = Some(minVals),
          maxValues = Some(maxVals)
        )
        transactionLog.addFile(addAction)
        println(
          s"   ✓ $filename: customer_id [${minVals("customer_id")}-${maxVals("customer_id")}], " +
            s"amount [${minVals("order_amount")}-${maxVals("order_amount")}], region=${minVals("region")}"
        )
    }

    println(s"\n📊 Total files in table: ${files.length}")

    // Demonstrate different skipping scenarios
    demonstrateScenario1_EqualTo(transactionLog)
    demonstrateScenario2_Range(transactionLog)
    demonstrateScenario3_StringFilter(transactionLog)
    demonstrateScenario4_NoSkipping(transactionLog)

    println("\n🎉 Data Skipping Demo completed successfully!")
    println("\nKey Takeaways:")
    println("• Data skipping reduces I/O by avoiding reads of irrelevant files")
    println("• Min/max statistics enable intelligent file pruning")
    println("• Query performance improves dramatically on large datasets")
    println("• Statistics are automatically collected during write operations")
  }

  private def demonstrateScenario1_EqualTo(transactionLog: TransactionLog): Unit = {
    println("\n🎯 Scenario 1: EqualTo Filter - Precise Match")
    println("Query: SELECT * FROM orders WHERE customer_id = 2500")

    val filter                     = EqualTo("customer_id", 2500L)
    val (scannedFiles, totalFiles) = executeQuery(transactionLog, Array(filter))

    println(s"   📈 Result: Scanned $scannedFiles out of $totalFiles files")
    println(
      s"   💡 Skipped ${totalFiles - scannedFiles} files (${((totalFiles - scannedFiles) * 100.0 / totalFiles).toInt}% reduction)"
    )
    println(s"   ℹ️  Customer 2500 exists only in Q3_orders.tnt4s (range 2001-3000)")
  }

  private def demonstrateScenario2_Range(transactionLog: TransactionLog): Unit = {
    println("\n📈 Scenario 2: Range Filter - High Value Orders")
    println("Query: SELECT * FROM orders WHERE order_amount > 1000.00")

    val filter                     = GreaterThan("order_amount", 1000.0)
    val (scannedFiles, totalFiles) = executeQuery(transactionLog, Array(filter))

    println(s"   📈 Result: Scanned $scannedFiles out of $totalFiles files")
    println(
      s"   💡 Skipped ${totalFiles - scannedFiles} files (${((totalFiles - scannedFiles) * 100.0 / totalFiles).toInt}% reduction)"
    )
    println(s"   ℹ️  Files with max_amount <= 1000 are automatically skipped")
  }

  private def demonstrateScenario3_StringFilter(transactionLog: TransactionLog): Unit = {
    println("\n🌎 Scenario 3: String Filter - Regional Analysis")
    println("Query: SELECT * FROM orders WHERE region = 'Global'")

    val filter                     = EqualTo("region", "Global")
    val (scannedFiles, totalFiles) = executeQuery(transactionLog, Array(filter))

    println(s"   📈 Result: Scanned $scannedFiles out of $totalFiles files")
    println(
      s"   💡 Skipped ${totalFiles - scannedFiles} files (${((totalFiles - scannedFiles) * 100.0 / totalFiles).toInt}% reduction)"
    )
    println(s"   ℹ️  Only Special_orders.tnt4s contains Global region data")
  }

  private def demonstrateScenario4_NoSkipping(transactionLog: TransactionLog): Unit = {
    println("\n🚫 Scenario 4: No Skipping - Wide Range Query")
    println("Query: SELECT * FROM orders WHERE customer_id > 0")

    val filter                     = GreaterThan("customer_id", 0L)
    val (scannedFiles, totalFiles) = executeQuery(transactionLog, Array(filter))

    println(s"   📈 Result: Scanned $scannedFiles out of $totalFiles files")
    println(
      s"   💡 Skipped ${totalFiles - scannedFiles} files (${((totalFiles - scannedFiles) * 100.0 / totalFiles).toInt}% reduction)"
    )
    println(s"   ℹ️  All files contain customer_id > 0, so no files can be skipped")
  }

  private def executeQuery(transactionLog: TransactionLog, filters: Array[Filter]): (Int, Int) = {
    val schema = StructType(
      Array(
        StructField("customer_id", LongType, false),
        StructField("order_amount", DoubleType, false),
        StructField("region", StringType, false)
      )
    )

    val options              = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val emptyBroadcastConfig = spark.sparkContext.broadcast(Map.empty[String, String])
    val scan =
      new IndexTables4SparkScan(spark, transactionLog, schema, filters, options, None, emptyBroadcastConfig.value)

    val totalFiles   = transactionLog.listFiles().length
    val partitions   = scan.planInputPartitions()
    val scannedFiles = partitions.length

    (scannedFiles, totalFiles)
  }
}
