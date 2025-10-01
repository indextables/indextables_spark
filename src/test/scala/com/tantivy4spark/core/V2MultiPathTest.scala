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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test multi-path operations in V2 DataSource API including:
 *   - Reading from multiple paths/tables
 *   - Path globbing and pattern matching
 *   - Mixed protocol access (local + HDFS)
 *   - Path resolution and validation
 *   - Union operations across multiple tables
 */
class V2MultiPathTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should read from multiple individual paths") {
    withTempPath { basePath =>
      val path1 = s"$basePath/table1"
      val path2 = s"$basePath/table2"
      val path3 = s"$basePath/table3"

      // Create multiple tables with different data
      val data1 = spark.range(10, 20).select(col("id"), lit("table1").as("source"))
      val data2 = spark.range(20, 30).select(col("id"), lit("table2").as("source"))
      val data3 = spark.range(30, 40).select(col("id"), lit("table3").as("source"))

      // Write to multiple paths
      data1.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path1)
      data2.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path2)
      data3.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path3)

      // Read from individual paths
      val result1 = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(path1)
      val result2 = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(path2)
      val result3 = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(path3)

      // Verify each table individually
      result1.count() shouldBe 10
      result2.count() shouldBe 10
      result3.count() shouldBe 10

      result1.select("source").distinct().collect().head.getString(0) shouldBe "table1"
      result2.select("source").distinct().collect().head.getString(0) shouldBe "table2"
      result3.select("source").distinct().collect().head.getString(0) shouldBe "table3"

      // Union multiple paths programmatically
      val unionResult = result1.union(result2).union(result3)
      unionResult.count() shouldBe 30
      unionResult.select("source").distinct().count() shouldBe 3
    }
  }

  ignore("should handle path arrays in load operation") {
    withTempPath { basePath =>
      val path1 = s"$basePath/dataset_a"
      val path2 = s"$basePath/dataset_b"

      // Create datasets with same schema
      val schema = StructType(
        Array(
          StructField("id", LongType, false),
          StructField("category", StringType, true),
          StructField("value", DoubleType, true)
        )
      )

      val data1 = spark
        .range(0, 50)
        .select(
          col("id"),
          when(col("id") % 2 === 0, "even").otherwise("odd").as("category"),
          (col("id") * 1.5).as("value")
        )

      val data2 = spark
        .range(50, 100)
        .select(
          col("id"),
          when(col("id") % 3 === 0, "multiple_of_3").otherwise("other").as("category"),
          (col("id") * 2.0).as("value")
        )

      data1.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path1)
      data2.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path2)

      // Read from multiple paths using array notation
      val multiPathResult = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path1, path2)

      // Verify combined results
      multiPathResult.count() shouldBe 100
      multiPathResult.schema should equal(schema)

      // Check data ranges
      val ids = multiPathResult.select("id").collect().map(_.getLong(0)).sorted
      ids should have length 100
    }
  }

  ignore("should handle mixed data types across multiple paths") {
    withTempPath { basePath =>
      val numericPath = s"$basePath/numeric_data"
      val textPath    = s"$basePath/text_data"

      // Create datasets with compatible but different content
      val numericData = spark
        .range(0, 25)
        .select(
          col("id"),
          col("id").cast(StringType).as("content"), // Numbers as strings
          lit("numeric").as("data_type")
        )

      val textData = spark
        .range(25, 50)
        .select(
          col("id"),
          concat(lit("text_"), col("id")).as("content"), // Text content
          lit("text").as("data_type")
        )

      numericData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(numericPath)
      textData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(textPath)

      // Read both paths together
      val combinedResult = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(numericPath, textPath)

      combinedResult.count() shouldBe 50

      // Verify both data types are present
      val dataTypes = combinedResult.select("data_type").distinct().collect().map(_.getString(0))
      dataTypes.toSeq should contain theSameElementsAs Seq("numeric", "text")

      // Test queries across both datasets
      val numericRows = combinedResult.filter(col("data_type") === "numeric").count()
      val textRows    = combinedResult.filter(col("data_type") === "text").count()

      numericRows shouldBe 25
      textRows shouldBe 25
    }
  }

  ignore("should handle overlapping path hierarchies") {
    withTempPath { basePath =>
      val parentPath  = s"$basePath/parent"
      val childPath   = s"$basePath/parent/child"
      val siblingPath = s"$basePath/sibling"

      // Create hierarchical structure
      val parentData = spark
        .range(0, 30)
        .select(
          col("id"),
          lit("parent").as("location")
        )

      val childData = spark
        .range(30, 45)
        .select(
          col("id"),
          lit("child").as("location")
        )

      val siblingData = spark
        .range(45, 60)
        .select(
          col("id"),
          lit("sibling").as("location")
        )

      parentData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(parentPath)
      childData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(childPath)
      siblingData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(siblingPath)

      // Read each path individually to verify separation
      val parentResult  = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(parentPath)
      val childResult   = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(childPath)
      val siblingResult = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(siblingPath)

      // Verify each dataset is distinct
      parentResult.count() shouldBe 30
      childResult.count() shouldBe 15
      siblingResult.count() shouldBe 15

      parentResult.select("location").distinct().collect().head.getString(0) shouldBe "parent"
      childResult.select("location").distinct().collect().head.getString(0) shouldBe "child"
      siblingResult.select("location").distinct().collect().head.getString(0) shouldBe "sibling"

      // Read multiple non-overlapping paths
      val combinedResult = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(childPath, siblingPath)

      combinedResult.count() shouldBe 30 // child + sibling
      val locations = combinedResult.select("location").distinct().collect().map(_.getString(0))
      locations.toSeq should contain theSameElementsAs Seq("child", "sibling")
    }
  }

  ignore("should handle empty paths in multi-path operations") {
    withTempPath { basePath =>
      val dataPath  = s"$basePath/with_data"
      val emptyPath = s"$basePath/empty_data"

      // Create one path with data
      val data = spark
        .range(0, 20)
        .select(
          col("id"),
          concat(lit("item_"), col("id")).as("name")
        )
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(dataPath)

      // Create empty path
      val emptyData = spark.emptyDataFrame
        .select(
          lit(0L).as("id"),
          lit("").as("name")
        )
        .filter(lit(false)) // Filter out all rows
      emptyData.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(emptyPath)

      // Read both paths together
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(dataPath, emptyPath)

      // Should get data from non-empty path only
      result.count() shouldBe 20
      result.select("id").collect().length shouldBe 20
    }
  }

  ignore("should handle path resolution edge cases") {
    withTempPath { basePath =>
      val normalPath           = s"$basePath/normal"
      val pathWithSpaces       = s"$basePath/path with spaces"
      val pathWithSpecialChars = s"$basePath/path-with_special.chars"

      val data = spark.range(0, 10).select(col("id"))

      // Write to paths with various naming patterns
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(normalPath)
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(pathWithSpaces)
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(pathWithSpecialChars)

      // Read from each path
      val normalResult = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(normalPath)
      val spacesResult = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(pathWithSpaces)
      val specialResult =
        spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(pathWithSpecialChars)

      // All should work
      normalResult.count() shouldBe 10
      spacesResult.count() shouldBe 10
      specialResult.count() shouldBe 10

      // Multi-path read with various naming patterns
      val multiResult = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(normalPath, pathWithSpaces, pathWithSpecialChars)

      multiResult.count() shouldBe 30
    }
  }

  ignore("should support different file protocols in theory") {
    // Note: This test verifies that the multi-path logic can handle different protocol prefixes
    // Actual HDFS/S3 testing would require infrastructure setup

    withTempPath { basePath =>
      val localPath = s"file://$basePath/local_data"

      val data = spark
        .range(0, 15)
        .select(
          col("id"),
          lit("local").as("protocol")
        )

      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(localPath)

      // Read with explicit file:// protocol
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(localPath)

      result.count() shouldBe 15
      result.select("protocol").distinct().collect().head.getString(0) shouldBe "local"
    }
  }

  ignore("should handle schema consistency across multiple paths") {
    withTempPath { basePath =>
      val path1 = s"$basePath/schema_consistent_1"
      val path2 = s"$basePath/schema_consistent_2"

      // Define consistent schema
      val schema = StructType(
        Array(
          StructField("id", LongType, false),
          StructField("name", StringType, true),
          StructField("active", BooleanType, true)
        )
      )

      val data1 = spark
        .range(0, 25)
        .select(
          col("id"),
          concat(lit("user_"), col("id")).as("name"),
          (col("id") % 2 === 0).as("active")
        )

      val data2 = spark
        .range(25, 50)
        .select(
          col("id"),
          concat(lit("admin_"), col("id")).as("name"),
          lit(true).as("active")
        )

      data1.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path1)
      data2.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path2)

      // Read from multiple paths
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path1, path2)

      // Verify schema consistency
      result.schema should equal(schema)
      result.count() shouldBe 50

      // Verify data from both sources is present
      val userCount  = result.filter(col("name").startsWith("user_")).count()
      val adminCount = result.filter(col("name").startsWith("admin_")).count()

      userCount shouldBe 25
      adminCount shouldBe 25
    }
  }

  ignore("should handle large number of paths efficiently") {
    withTempPath { basePath =>
      val numPaths       = 10
      val recordsPerPath = 50

      // Create multiple small datasets
      val paths = (0 until numPaths).map { i =>
        val path = s"$basePath/dataset_$i"
        val data = spark
          .range(i * recordsPerPath, (i + 1) * recordsPerPath)
          .select(
            col("id"),
            lit(i).as("dataset_number"),
            concat(lit("value_"), col("id")).as("description")
          )
        data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(path)
        path
      }

      // Read from all paths at once
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(paths: _*)

      // Verify all data is present
      result.count() shouldBe numPaths * recordsPerPath

      // Verify all datasets are represented
      val datasetNumbers = result.select("dataset_number").distinct().collect().map(_.getInt(0)).sorted
      datasetNumbers should equal((0 until numPaths).toArray)

      // Test aggregation across all paths
      val totalsByDataset = result.groupBy("dataset_number").count().collect()
      totalsByDataset.foreach(row => row.getLong(1) shouldBe recordsPerPath)
    }
  }
}
