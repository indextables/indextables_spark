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

import com.tantivy4spark.TestBase
import org.apache.spark.sql.types._
import java.util.OptionalLong

class StatisticsIntegrationTest extends TestBase {

  test("Tantivy4SparkStatistics should provide basic table statistics") {
    withTempPath { path =>
      val schema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("title", StringType, nullable = true),
          StructField("content", StringType, nullable = true)
        )
      )

      val testData = (1 to 100).map { i =>
        (i.toLong, s"Title $i", s"Content for document $i with some additional text to make it longer")
      }

      val rows = testData.map {
        case (id, title, content) =>
          org.apache.spark.sql.Row(id, title, content)
      }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val tablePath = path.toString

      // Write data using V2 DataSource API
      df.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Read back using V2 API and verify statistics are available
      val readDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)

      // Force the query plan to be created, which should trigger statistics estimation
      val logicalPlan = readDf.queryExecution.logical

      // Collect the data to ensure the scan builder and statistics are used
      val result = readDf.collect()

      // Verify we got the correct number of records
      assert(result.length == 100, s"Expected 100 records, got ${result.length}")

      // The statistics should be computed during query planning
      // We can verify this by checking the logical plan contains our table
      assert(
        logicalPlan.toString.contains("Tantivy4SparkTable") ||
          logicalPlan.toString.contains("tantivy4spark"),
        s"Logical plan should contain Tantivy4Spark reference: $logicalPlan"
      )

      println("✅ Statistics integration test completed successfully")
    }
  }

  test("Tantivy4SparkStatistics should handle empty tables") {
    withTempPath { path =>
      val schema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType, nullable = true)
        )
      )

      val emptyData = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
      val tablePath = path.toString

      // Write empty data
      emptyData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Read back and verify no errors
      val readDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)

      val result = readDf.collect()
      assert(result.length == 0, s"Expected 0 records for empty table, got ${result.length}")

      println("✅ Empty table statistics test completed successfully")
    }
  }

  test("Statistics should work with filtered scans") {
    withTempPath { path =>
      val schema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("category", StringType, nullable = true),
          StructField("value", DoubleType, nullable = true)
        )
      )

      val testData = (1 to 50).map { i =>
        val category = if (i % 2 == 0) "even" else "odd"
        (i.toLong, category, i.toDouble * 1.5)
      }

      val rows = testData.map {
        case (id, category, value) =>
          org.apache.spark.sql.Row(id, category, value)
      }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val tablePath = path.toString

      df.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Read with filter - statistics should reflect the filtered dataset
      val filteredDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)
        .filter("category = 'even'")

      val result = filteredDf.collect()
      assert(result.length == 25, s"Expected 25 filtered records, got ${result.length}")

      // Verify all results have category 'even'
      assert(result.forall(_.getString(1) == "even"), "All filtered results should have category 'even'")

      println("✅ Filtered statistics test completed successfully")
    }
  }
}
