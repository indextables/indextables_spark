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

/**
 * Test for appending to tables with JSON fields configured via typemap.
 *
 * Bug: First write with typemap.field=json succeeds, but subsequent appends fail with:
 * "Field 'user_identity' type mismatch: existing table has object field, cannot append with json configuration"
 *
 * This is because JSON fields are stored as "object" type in tantivy, but the config specifies "json".
 * The validation needs to recognize these as equivalent.
 */
class JsonFieldAppendTest extends TestBase {

  test("should support append with JSON field configured via typemap") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // First write - create table with JSON field
      val df1 = Seq(
        (1, """{"username": "alice", "role": "admin"}"""),
        (2, """{"username": "bob", "role": "user"}""")
      ).toDF("id", "user_identity")

      println("✅ Writing initial data with JSON field configured via typemap")
      df1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.user_identity", "json")
        .mode("overwrite")
        .save(path)

      println("✅ Initial write succeeded")

      // Second write - append with same JSON field configuration
      val df2 = Seq(
        (3, """{"username": "charlie", "role": "admin"}"""),
        (4, """{"username": "diana", "role": "user"}""")
      ).toDF("id", "user_identity")

      println("✅ Appending more data with same JSON field configuration")
      df2.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.user_identity", "json")
        .mode("append")
        .save(path)

      println("✅ Append succeeded!")

      // Verify all data is present
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 4

      // Verify data integrity
      val ids = result.select("id").collect().map(_.getInt(0)).sorted
      ids shouldBe Array(1, 2, 3, 4)

      println("✅ All 4 records present after append")

      // Verify JSON queries work
      result.createOrReplaceTempView("users")
      val adminQuery = spark.sql("""
        SELECT id, user_identity
        FROM users
        WHERE _indexall indexquery 'user_identity.role:admin'
        ORDER BY id
      """)

      val admins = adminQuery.collect()
      admins.length shouldBe 2
      admins(0).getInt(0) shouldBe 1
      admins(1).getInt(0) shouldBe 3

      println("✅ JSON queries work correctly on appended data")
      println("🎉 Append with JSON field typemap configuration works!")
    }
  }

  test("should support multiple appends with JSON field") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Initial write
      val df1 = Seq((1, """{"status": "active"}""")).toDF("id", "metadata")
      df1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.metadata", "json")
        .mode("overwrite")
        .save(path)

      // First append
      val df2 = Seq((2, """{"status": "pending"}""")).toDF("id", "metadata")
      df2.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.metadata", "json")
        .mode("append")
        .save(path)

      // Second append
      val df3 = Seq((3, """{"status": "completed"}""")).toDF("id", "metadata")
      df3.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.metadata", "json")
        .mode("append")
        .save(path)

      // Third append
      val df4 = Seq((4, """{"status": "active"}""")).toDF("id", "metadata")
      df4.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.metadata", "json")
        .mode("append")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 4

      println("✅ Multiple appends with JSON field configuration work!")
    }
  }

  test("should support append with mixed-case JSON field names") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Initial write with mixed-case field name
      val df1 = Seq(
        (1, """{"city": "NYC"}"""),
        (2, """{"city": "SF"}""")
      ).toDF("id", "userLocation")

      df1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.userLocation", "json")
        .mode("overwrite")
        .save(path)

      // Append with same mixed-case field name
      val df2 = Seq(
        (3, """{"city": "LA"}"""),
        (4, """{"city": "SEA"}""")
      ).toDF("id", "userLocation")

      df2.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.userLocation", "json")
        .mode("append")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 4

      // Verify mixed-case field name is preserved
      val fieldNames = result.schema.fieldNames
      fieldNames should contain("userLocation")

      println("✅ Append with mixed-case JSON field names works!")
    }
  }
}
