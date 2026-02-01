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

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

/**
 * Test to reproduce the "object must have at least one field mapping" bug.
 *
 * When a StructType/ArrayType field is always null during writes, the docMappingJson ends up with an empty
 * field_mappings array for that field. When aggregation queries are run (even on unrelated fields), the
 * DocMapperBuilder parsing fails because Quickwit requires object fields to have at least one field mapping.
 *
 * This bug occurs because:
 *   1. json_discovery.rs discovers sub-fields by sampling actual document data 2. If all values are null, no sub-fields
 *      are discovered 3. The docMappingJson has {"type": "object", "field_mappings": []} 4. Quickwit's DocMapperBuilder
 *      rejects empty field_mappings
 *
 * The fix filters out object fields with empty field_mappings when restoring schemas from the transaction log.
 */
class EmptyFieldMappingsAggregationBugTest extends TestBase {

  test("REPRODUCE BUG: aggregation fails when table has always-null struct field") {
    withTempPath { path =>
      // Schema with a struct field that will always be null
      val parsingErrorsSchema = StructType(
        Seq(
          StructField("errorCode", IntegerType, nullable = true),
          StructField("errorMessage", StringType, nullable = true)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("host_hostname", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          // This field will always be null - simulates the user's event_ParsingErrors field
          StructField("event_ParsingErrors", parsingErrorsSchema, nullable = true)
        )
      )

      // All rows have null for event_ParsingErrors
      val data = Seq(
        Row(1, "host1.example.com", 100, null),
        Row(2, "host2.example.com", 200, null),
        Row(3, "host1.example.com", 150, null),
        Row(4, "host3.example.com", 300, null),
        Row(5, "host2.example.com", 250, null)
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"Writing ${df.count()} rows with always-null struct field 'event_ParsingErrors'")

      // Write the data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println("Write completed successfully")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Regular query should work (doesn't use DocMapper)
      val regularResult = result.filter("host_hostname = 'host1.example.com'").collect()
      regularResult.length shouldBe 2
      println(s"Regular filter query works: ${regularResult.length} rows returned")

      // Aggregation query that doesn't touch event_ParsingErrors should still fail
      // because the entire docMappingJson is parsed
      println("Attempting aggregation query (this should fail before the fix)...")

      // This query is similar to: SELECT host_hostname, count(host_hostname) FROM table GROUP BY 1
      val aggregationResult = result
        .groupBy("host_hostname")
        .count()
        .collect()

      // If we get here, the bug is fixed
      aggregationResult.length shouldBe 3 // host1, host2, host3
      println(s"Aggregation query succeeded with ${aggregationResult.length} groups")
      aggregationResult.foreach(row => println(s"  ${row.getString(0)}: ${row.getLong(1)} rows"))
    }
  }

  test("REPRODUCE BUG: aggregation fails when table has always-null array field") {
    withTempPath { path =>
      // Schema with an array field that will always be null
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("host_hostname", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          // Array field that will always be null
          StructField("tags", ArrayType(StringType), nullable = true)
        )
      )

      // All rows have null for tags
      val data = Seq(
        Row(1, "host1.example.com", 100, null),
        Row(2, "host2.example.com", 200, null),
        Row(3, "host1.example.com", 150, null),
        Row(4, "host3.example.com", 300, null),
        Row(5, "host2.example.com", 250, null)
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"Writing ${df.count()} rows with always-null array field 'tags'")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println("Write completed successfully")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Aggregation query
      println("Attempting aggregation query with always-null array field...")

      val aggregationResult = result
        .groupBy("host_hostname")
        .count()
        .collect()

      aggregationResult.length shouldBe 3
      println(s"Aggregation query succeeded with ${aggregationResult.length} groups")
    }
  }

  test("aggregation works when struct field has some non-null values") {
    withTempPath { path =>
      // Schema with a struct field that has SOME non-null values
      val parsingErrorsSchema = StructType(
        Seq(
          StructField("errorCode", IntegerType, nullable = true),
          StructField("errorMessage", StringType, nullable = true)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("host_hostname", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("event_ParsingErrors", parsingErrorsSchema, nullable = true)
        )
      )

      // Some rows have actual data for event_ParsingErrors
      val data = Seq(
        Row(1, "host1.example.com", 100, Row(404, "Not found")),
        Row(2, "host2.example.com", 200, null),
        Row(3, "host1.example.com", 150, Row(500, "Server error")),
        Row(4, "host3.example.com", 300, null),
        Row(5, "host2.example.com", 250, null)
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"Writing ${df.count()} rows with struct field that has SOME non-null values")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // This should work because the struct field has actual data to discover sub-fields from
      val aggregationResult = result
        .groupBy("host_hostname")
        .count()
        .collect()

      aggregationResult.length shouldBe 3
      println(s"Aggregation with non-null struct data succeeded: ${aggregationResult.length} groups")
    }
  }

  test("simple count aggregation fails with always-null struct field") {
    withTempPath { path =>
      val parsingErrorsSchema = StructType(
        Seq(
          StructField("errorCode", IntegerType, nullable = true),
          StructField("errorMessage", StringType, nullable = true)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("host_hostname", StringType, nullable = false),
          StructField("event_ParsingErrors", parsingErrorsSchema, nullable = true)
        )
      )

      val data = Seq(
        Row(1, "host1", null),
        Row(2, "host2", null),
        Row(3, "host1", null)
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Even a simple COUNT(*) fails due to the empty field_mappings
      println("Attempting simple count() aggregation...")
      val count = result.count()
      count shouldBe 3
      println(s"Simple count succeeded: $count rows")
    }
  }
}
