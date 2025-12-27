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

package io.indextables.spark.json

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for MapType field support via JSON fields. Tests end-to-end write/read paths with various Map
 * configurations.
 */
class JsonMapFieldIntegrationTest extends TestBase with Matchers {

  test("should write and read simple string map") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("color" -> "red", "size" -> "large")),
        (2, Map("color" -> "blue", "size" -> "small", "category" -> "clothing"))
      )
      val df = spark.createDataFrame(data).toDF("id", "attributes")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify first row
      rows(0).getInt(0) shouldBe 1
      val attrs1 = rows(0).getMap[String, String](1)
      attrs1.get("color").get shouldBe "red"
      attrs1.get("size").get shouldBe "large"

      // Verify second row
      rows(1).getInt(0) shouldBe 2
      val attrs2 = rows(1).getMap[String, String](1)
      attrs2.get("color").get shouldBe "blue"
      attrs2.get("size").get shouldBe "small"
      attrs2.get("category").get shouldBe "clothing"
    }
  }

  test("should write and read map with integer values") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("count" -> 42, "total" -> 1000, "errors" -> 0)),
        (2, Map("count" -> 15, "total" -> 500))
      )
      val df = spark.createDataFrame(data).toDF("id", "counters")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify first row
      rows(0).getInt(0) shouldBe 1
      val counters1 = rows(0).getMap[String, Int](1)
      counters1.get("count").get shouldBe 42
      counters1.get("total").get shouldBe 1000
      counters1.get("errors").get shouldBe 0

      // Verify second row
      rows(1).getInt(0) shouldBe 2
      val counters2 = rows(1).getMap[String, Int](1)
      counters2.get("count").get shouldBe 15
      counters2.get("total").get shouldBe 500
    }
  }

  test("should write and read map with mixed types") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("count" -> 42L, "total" -> 1000L)),
        (2, Map("count" -> 15L, "total" -> 500L, "errors" -> 3L))
      )
      val df = spark.createDataFrame(data).toDF("id", "metrics")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify first row
      rows(0).getInt(0) shouldBe 1
      val metrics1 = rows(0).getMap[String, Long](1)
      metrics1.get("count").get shouldBe 42L
      metrics1.get("total").get shouldBe 1000L

      // Verify second row
      rows(1).getInt(0) shouldBe 2
      val metrics2 = rows(1).getMap[String, Long](1)
      metrics2.get("count").get shouldBe 15L
      metrics2.get("total").get shouldBe 500L
      metrics2.get("errors").get shouldBe 3L
    }
  }

  test("should handle empty maps") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("key1" -> "value1")),
        (2, Map.empty[String, String]),
        (3, Map("key2" -> "value2", "key3" -> "value3"))
      )
      val df = spark.createDataFrame(data).toDF("id", "attributes")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 3
      val rows = result.orderBy("id").collect()

      rows(0).getMap[String, String](1).size shouldBe 1
      rows(1).getMap[String, String](1).size shouldBe 0 // Empty map
      rows(2).getMap[String, String](1).size shouldBe 2
    }
  }

  test("should write and read multiple map fields") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("color" -> "red"), Map("count" -> 42)),
        (2, Map("color" -> "blue", "size" -> "large"), Map("count" -> 15, "total" -> 100))
      )
      val df = spark.createDataFrame(data).toDF("id", "attributes", "counters")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify first row
      rows(0).getInt(0) shouldBe 1
      rows(0).getMap[String, String](1).get("color").get shouldBe "red"
      rows(0).getMap[String, Int](2).get("count").get shouldBe 42

      // Verify second row
      rows(1).getInt(0) shouldBe 2
      rows(1).getMap[String, String](1).get("color").get shouldBe "blue"
      rows(1).getMap[String, Int](2).get("count").get shouldBe 15
      rows(1).getMap[String, Int](2).get("total").get shouldBe 100
    }
  }

  test("should handle map with integer keys") {
    withTempPath { path =>
      val data = Seq(
        (1, Map(1 -> "first", 2 -> "second", 3 -> "third")),
        (2, Map(10 -> "ten", 20 -> "twenty"))
      )
      val df = spark.createDataFrame(data).toDF("id", "lookup")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify first row
      rows(0).getInt(0) shouldBe 1
      val lookup1 = rows(0).getMap[Int, String](1)
      lookup1.get(1).get shouldBe "first"
      lookup1.get(2).get shouldBe "second"
      lookup1.get(3).get shouldBe "third"

      // Verify second row
      rows(1).getInt(0) shouldBe 2
      val lookup2 = rows(1).getMap[Int, String](1)
      lookup2.get(10).get shouldBe "ten"
      lookup2.get(20).get shouldBe "twenty"
    }
  }

  test("should handle large maps") {
    withTempPath { path =>
      val largeMap = (1 to 100).map(i => s"key$i" -> s"value$i").toMap
      val data = Seq(
        (1, largeMap),
        (2, Map("single" -> "entry"))
      )
      val df = spark.createDataFrame(data).toDF("id", "attributes")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      rows(0).getMap[String, String](1).size shouldBe 100
      rows(1).getMap[String, String](1).size shouldBe 1
    }
  }

  test("should preserve map key-value types through roundtrip") {
    withTempPath { path =>
      val data = Seq(
        (1, Map("count" -> 42L, "total" -> 1000L)),
        (2, Map("count" -> 15L, "total" -> 500L, "errors" -> 3L))
      )
      val df = spark.createDataFrame(data).toDF("id", "metrics")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 2
      val rows = result.orderBy("id").collect()

      // Verify data types are preserved through roundtrip
      rows(0).getInt(0) shouldBe 1
      val metrics1 = rows(0).getMap[String, Long](1)
      metrics1.get("count").get shouldBe 42L
      metrics1.get("total").get shouldBe 1000L

      rows(1).getInt(0) shouldBe 2
      val metrics2 = rows(1).getMap[String, Long](1)
      metrics2.get("count").get shouldBe 15L
      metrics2.get("total").get shouldBe 500L
      metrics2.get("errors").get shouldBe 3L
    }
  }
}
