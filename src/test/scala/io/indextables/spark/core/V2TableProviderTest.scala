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
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog.TableCapability

/** Tests for V2 TableProvider methods and capabilities that weren't covered in the basic V2 tests. */
class V2TableProviderTest extends TestBase {

  ignore("should test inferSchema method directly with V2 TableProvider") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // First write some data to create a table
      val testData = Seq(
        (1, "Schema Test", "inference", 42.0, true),
        (2, "Another Test", "validation", 84.0, false)
      ).toDF("id", "name", "category", "value", "flag")

      testData.write.format("tantivy4spark").save(tempPath)

      // Test inferSchema method directly
      val tableProvider = new Tantivy4SparkTableProvider()

      import scala.jdk.CollectionConverters._
      val options = new CaseInsensitiveStringMap(Map("path" -> tempPath).asJava)

      val inferredSchema = tableProvider.inferSchema(options)

      inferredSchema should not be null
      inferredSchema.fields.length should be(5)

      // Verify specific field types
      val fieldNames = inferredSchema.fieldNames
      fieldNames should contain("id")
      fieldNames should contain("name")
      fieldNames should contain("category")
      fieldNames should contain("value")
      fieldNames should contain("flag")
    }
  }

  ignore("should handle multiple paths with JSON format") {
    withTempPath { tempPath1 =>
      withTempPath { tempPath2 =>
        val spark = this.spark
        import spark.implicits._

        // Create data in two different paths
        val data1 = Seq((1, "Path1 Data", "test")).toDF("id", "content", "source")
        val data2 = Seq((2, "Path2 Data", "test")).toDF("id", "content", "source")

        data1.write.format("tantivy4spark").save(tempPath1)
        data2.write.format("tantivy4spark").save(tempPath2)

        // Test reading from multiple paths using JSON format
        val pathsJson = s"""["$tempPath1", "$tempPath2"]"""

        val multiPathData = spark.read
          .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
          .option("paths", pathsJson)
          .load()

        val results = multiPathData.collect()
        results.length should be(2)

        val ids = results.map(_.getAs[Int]("id")).sorted
        ids should be(Array(1, 2))
      }
    }
  }

  ignore("should handle invalid JSON paths format") {
    val tableProvider = new Tantivy4SparkTableProvider()

    import scala.jdk.CollectionConverters._
    val invalidOptions = new CaseInsensitiveStringMap(
      Map(
        "paths" -> "invalid-json-format"
      ).asJava
    )

    assertThrows[IllegalArgumentException] {
      tableProvider.inferSchema(invalidOptions)
    }
  }

  ignore("should test table capabilities") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create a table first
      val testData = Seq((1, "Capability Test")).toDF("id", "name")
      testData.write.format("tantivy4spark").save(tempPath)

      // Get table via V2 API
      val tableProvider = new Tantivy4SparkTableProvider()
      import scala.jdk.CollectionConverters._
      val options = new CaseInsensitiveStringMap(Map("path" -> tempPath).asJava)

      val table        = tableProvider.getTable(null, Array.empty, options)
      val capabilities = table.capabilities()

      import scala.jdk.CollectionConverters._
      val capabilityList = capabilities.asScala.toSet

      // Verify expected capabilities
      capabilityList should contain(TableCapability.BATCH_READ)
      capabilityList should contain(TableCapability.BATCH_WRITE)
      capabilityList should contain(TableCapability.OVERWRITE_BY_FILTER)
      capabilityList should contain(TableCapability.TRUNCATE)
    }
  }

  ignore("should handle missing path parameter") {
    val tableProvider = new Tantivy4SparkTableProvider()

    import scala.jdk.CollectionConverters._
    val emptyOptions = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    assertThrows[IllegalArgumentException] {
      tableProvider.inferSchema(emptyOptions)
    }
  }
}
