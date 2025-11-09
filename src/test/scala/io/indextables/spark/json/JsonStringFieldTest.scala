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
 * Test for the exact usage pattern requested by user:
 * StringType field with JSON string content, configured as "json" type.
 */
class JsonStringFieldTest extends TestBase {

  test("should work with StringType field configured as json - exact user example") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Exact usage pattern from user
      val df = Seq((1, """{"one": "two", "three": "four"}""")).toDF("id", "val_field")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.val_field", "json")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote data with StringType field configured as 'json'")

      // Read back
      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("test_table")

      // Verify data can be read
      val data = resultDf.collect()
      println(s"✅ Read back ${data.length} rows")
      data.foreach { row =>
        println(s"   - ID: ${row.getInt(0)}, val_field: ${row.getString(1)}")
      }

      data.length shouldBe 1
      val jsonStr = data(0).getString(1)
      jsonStr should not be null
      println(s"✅ JSON data: $jsonStr")

      // Test IndexQuery: val_field.one:two
      println("\n🔍 Test: _indexall indexquery 'val_field.one:two'")
      val query = spark.sql("""
        SELECT id, val_field
        FROM test_table
        WHERE _indexall indexquery 'val_field.one:two'
      """)

      val results = query.collect()
      println(s"   Found ${results.length} matching documents")
      results.foreach { row =>
        println(s"   - ID: ${row.getInt(0)}, val_field: ${row.getString(1)}")
      }

      results.length shouldBe 1
      results(0).getInt(0) shouldBe 1

      println("\n🎉 StringType with 'json' configuration works perfectly!")
    }
  }

  test("should work with multiple JSON string records") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      val df = Seq(
        (1, """{"one": "two", "three": "four"}"""),
        (2, """{"one": "alpha", "three": "beta"}"""),
        (3, """{"one": "two", "three": "gamma"}""")
      ).toDF("id", "val_field")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.val_field", "json")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} records")

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("test_table")

      // Test: Find all records where one = "two"
      println("\n🔍 Test: Find all where val_field.one:two")
      val query = spark.sql("""
        SELECT id, val_field
        FROM test_table
        WHERE _indexall indexquery 'val_field.one:two'
        ORDER BY id
      """)

      val results = query.collect()
      println(s"   Found ${results.length} matching documents:")
      results.foreach { row =>
        println(s"   - ID: ${row.getInt(0)}, val_field: ${row.getString(1)}")
      }

      results.length shouldBe 2
      results(0).getInt(0) shouldBe 1
      results(1).getInt(0) shouldBe 3

      println("\n🎉 Multiple JSON string records work!")
    }
  }

  test("should support complex queries on JSON string fields") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      val df = Seq(
        (1, """{"name": "alice", "role": "engineer", "team": "search"}"""),
        (2, """{"name": "bob", "role": "manager", "team": "analytics"}"""),
        (3, """{"name": "charlie", "role": "engineer", "team": "analytics"}""")
      ).toDF("id", "user_data")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.user_data", "json")
        .mode("overwrite")
        .save(path)

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("users")

      // Test: AND query
      println("\n🔍 Test: user_data.role:engineer AND user_data.team:analytics")
      val andQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:engineer AND user_data.team:analytics'
      """)

      val andResults = andQuery.collect()
      println(s"   Found ${andResults.length} engineers in analytics:")
      andResults.foreach { row =>
        println(s"   - ID: ${row.getInt(0)}, user_data: ${row.getString(1)}")
      }

      andResults.length shouldBe 1
      andResults(0).getInt(0) shouldBe 3

      // Test: OR query
      println("\n🔍 Test: user_data.role:manager OR user_data.team:search")
      val orQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:manager OR user_data.team:search'
        ORDER BY id
      """)

      val orResults = orQuery.collect()
      println(s"   Found ${orResults.length} managers or search team members:")
      orResults.foreach { row =>
        println(s"   - ID: ${row.getInt(0)}, user_data: ${row.getString(1)}")
      }

      orResults.length shouldBe 2
      orResults(0).getInt(0) shouldBe 1
      orResults(1).getInt(0) shouldBe 2

      println("\n🎉 Complex queries on JSON string fields work!")
    }
  }
}
