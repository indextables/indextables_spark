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

import java.sql.Date

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers

class PartitionVsDataDateTest extends TestBase with Matchers {

  test("Data date fields (non-partition) should work correctly") {
    withTempPath { tempPath =>
      val tablePath = tempPath.toString

      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with both partition date and data date
      // Added seq_num (IntegerType) for COUNT aggregation with filters
      val testData = Seq(
        ("test1", Date.valueOf("2023-02-10"), "content1", Date.valueOf("2023-02-15"), 1),
        ("test2", Date.valueOf("2023-02-10"), "content2", Date.valueOf("2023-02-16"), 2)
      ).toDF("id", "partition_date", "description", "data_date", "seq_num")

      // Write with partition_date as partition column, data_date as regular indexed field
      // seq_num provides fast field for COUNT aggregation, data_date for filtering
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "seq_num,data_date")
        .partitionBy("partition_date")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Test data date query (should work - field is stored in split)
      println("Creating filter for data_date === '2023-02-15'")
      val dataQuery = df.filter($"data_date" === "2023-02-15")
      // Let's see what actual data is being returned first
      println("=== Actual rows returned by data_date filter ===")
      val filteredRows = dataQuery.collect()
      filteredRows.foreach { row =>
        println(s"Filtered row: id=${row.getAs[String]("id")}, data_date=${row.getAs[java.sql.Date]("data_date")}, partition_date=${row.getAs[java.sql.Date]("partition_date")}")
      }

      println("About to call count() on filtered query")
      val dataCount = dataQuery.count()
      println(s"Data date query count: $dataCount")
      println(s"Collected rows length: ${filteredRows.length}")

      // Compare count() vs collect().length
      if (dataCount != filteredRows.length) {
        println(s"⚠️  MISMATCH: count()=$dataCount but collect().length=${filteredRows.length}")
      }

      // Let's also check explain to see if filter is pushed down
      println("=== Query explain ===")
      dataQuery.explain(true)

      assert(dataCount == 1, "Should find single record with data_date = 2023-02-15")

      // Test partition date query (this should work if partition values are stored in splits)
      val partitionQuery = df.filter($"partition_date" === "2023-02-10")
      val partitionCount = partitionQuery.count()
      println(s"Partition date query count: $partitionCount")

      // Log all data for debugging
      println("=== All data without filters ===")
      val allData = df.collect()
      allData.foreach(row => println(s"Row: $row"))

      // This should work if our fix is correct
      assert(partitionCount == 2, "Should find 2 records with partition_date = 2023-02-10")
    }
  }
}
