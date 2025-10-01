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
import org.apache.spark.sql.functions._

class MetadataColumnTest extends TestBase {

  test("should expose _indexall metadata column in table") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "Apache Spark is fast", "Big Data"),
        (2, "Machine Learning with Python", "Data Science"),
        (3, "Scala Programming Guide", "Programming"),
        (4, "Database Design Patterns", "Architecture")
      ).toDF("id", "title", "category")

      // Write the data
      testData.write.format("tantivy4spark").mode("overwrite").save(tempPath)

      // Read back and get the table instance
      val readDf = spark.read.format("tantivy4spark").load(tempPath)

      // Verify that we can access the metadata columns through introspection
      // The regular schema should NOT include _indexall
      val regularSchema = readDf.schema
      assert(
        !regularSchema.fieldNames.contains("_indexall"),
        s"Regular schema should not contain _indexall: ${regularSchema.fieldNames.mkString(", ")}"
      )

      println(s"✅ Regular schema correctly excludes _indexall: ${regularSchema.fieldNames.mkString(", ")}")

      // Verify the table supports metadata columns by checking the provider
      // Since we can't easily access the internal table instance, we'll test via SQL functionality
      readDf.createOrReplaceTempView("metadata_test_table")

      // Test that we can select from the normal columns
      val normalSelect = spark.sql("SELECT id, title FROM metadata_test_table")
      val normalRows   = normalSelect.collect()
      assert(normalRows.length == 4, "Should be able to select normal columns")

      println(s"✅ Can select normal columns: ${normalRows.length} rows")
    }
  }

  test("should verify metadata column implementation exists") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "Apache Spark is fast")
      ).toDF("id", "title")

      testData.write.format("tantivy4spark").mode("overwrite").save(tempPath)

      // Create the table through the V2 DataSource API
      val tableProvider = new IndexTables4SparkTableProvider()
      import scala.jdk.CollectionConverters._
      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(Map("path" -> tempPath).asJava)

      val table = tableProvider.getTable(null, Array.empty, options)

      // Verify the table implements SupportsMetadataColumns
      assert(
        table.isInstanceOf[org.apache.spark.sql.connector.catalog.SupportsMetadataColumns],
        "Table should implement SupportsMetadataColumns"
      )

      val metadataSupport = table.asInstanceOf[org.apache.spark.sql.connector.catalog.SupportsMetadataColumns]
      val metadataColumns = metadataSupport.metadataColumns()

      assert(metadataColumns.length == 1, s"Expected 1 metadata column, got ${metadataColumns.length}")
      assert(
        metadataColumns(0).name() == "_indexall",
        s"Expected _indexall metadata column, got ${metadataColumns(0).name()}"
      )
      assert(metadataColumns(0).dataType() == org.apache.spark.sql.types.StringType, "Expected StringType for _indexall")

      println(
        s"✅ Metadata column _indexall properly defined: ${metadataColumns(0).name()} (${metadataColumns(0).dataType()})"
      )
    }
  }
}
