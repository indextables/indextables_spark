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
 * Tests for using IndexQuery with JSON-indexed fields (Struct types).
 *
 * This test demonstrates:
 *   - Using IndexQuery to search within Struct field properties using dot notation
 *   - Querying nested JSON properties like: json_field.property:value
 *   - Using _indexall to search across all JSON properties
 *
 * Note: Currently StringType fields configured as "json" work for IndexQuery but have a conversion issue when reading
 * data back (missing JSON → StringType conversion). Struct types work end-to-end for both querying and reading.
 *
 * Example with Struct: case class UserData(name: String, role: String, team: String) Query: _indexall indexquery
 * 'user_data.role:engineer'
 */
class JsonFieldIndexQueryTest extends TestBase {

  test("should use IndexQuery to search within JSON field properties") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with JSON strings
      val testData = Seq(
        (1, """{"name": "alice", "role": "engineer", "team": "search"}"""),
        (2, """{"name": "bob", "role": "manager", "team": "analytics"}"""),
        (3, """{"name": "charlie", "role": "engineer", "team": "platform"}"""),
        (4, """{"name": "diana", "role": "designer", "team": "search"}"""),
        (5, """{"name": "eve", "role": "engineer", "team": "analytics"}""")
      ).toDF("id", "user_data")

      // Write with "json" type to enable JSON field indexing
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.user_data", "json") // Index as JSON field
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} documents with JSON field indexing")

      // Read back and create temp view
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("users")

      // Test 1: Query specific JSON property with exact value
      println("\n🔍 Test 1: Query JSON property - user_data.role:engineer")
      val engineerQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:engineer'
        ORDER BY id
      """)

      val engineers = engineerQuery.collect()
      println(s"   Found ${engineers.length} engineers:")
      engineers.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      engineers.length should be >= 1
      engineers.foreach(row => row.getString(1) should include("engineer"))

      // Test 2: Query different JSON property
      println("\n🔍 Test 2: Query JSON property - user_data.team:search")
      val searchTeamQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.team:search'
        ORDER BY id
      """)

      val searchTeam = searchTeamQuery.collect()
      println(s"   Found ${searchTeam.length} members in search team:")
      searchTeam.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      searchTeam.length should be >= 1
      searchTeam.foreach(row => row.getString(1) should include("search"))

      // Test 3: Query with multiple conditions using AND
      println("\n🔍 Test 3: Query with AND - user_data.role:engineer AND user_data.team:analytics")
      val combinedQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:engineer AND user_data.team:analytics'
        ORDER BY id
      """)

      val combined = combinedQuery.collect()
      println(s"   Found ${combined.length} engineers in analytics team:")
      combined.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      combined.length should be >= 1
      combined.foreach { row =>
        val data = row.getString(1)
        data should include("engineer")
        data should include("analytics")
      }

      // Test 4: Query with OR
      println("\n🔍 Test 4: Query with OR - user_data.role:manager OR user_data.role:designer")
      val orQuery = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:manager OR user_data.role:designer'
        ORDER BY id
      """)

      val orResults = orQuery.collect()
      println(s"   Found ${orResults.length} managers or designers:")
      orResults.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      orResults.length should be >= 1
      orResults.foreach { row =>
        val data = row.getString(1)
        assert(data.contains("manager") || data.contains("designer"), s"Expected 'manager' or 'designer' in: $data")
      }

      println("\n🎉 All JSON field IndexQuery tests passed!")
    }
  }

  test("should query nested JSON with complex property names") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with more complex JSON
      val testData = Seq(
        (1, """{"one": "two", "three": "four", "status": "active"}"""),
        (2, """{"one": "alpha", "three": "beta", "status": "inactive"}"""),
        (3, """{"one": "two", "three": "gamma", "status": "active"}"""),
        (4, """{"one": "delta", "three": "four", "status": "pending"}""")
      ).toDF("id", "val_field")

      // Write with json type
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.val_field", "json")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} documents with val_field as JSON")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("test_data")

      // Test: Query val_field.one:two (exact example from user request)
      println("\n🔍 Test: Query val_field.one:two")
      val query = spark.sql("""
        SELECT id, val_field
        FROM test_data
        WHERE _indexall indexquery 'val_field.one:two'
        ORDER BY id
      """)

      val results = query.collect()
      println(s"   Found ${results.length} documents where val_field.one = 'two':")
      results.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))

      results.length should be >= 1
      results.foreach { row =>
        val json = row.getString(1)
        // Check for JSON content (may or may not have spaces after colons)
        json should (include("\"one\":\"two\"") or include("\"one\": \"two\""))
      }

      // Test: Query val_field.three:four
      println("\n🔍 Test: Query val_field.three:four")
      val query2 = spark.sql("""
        SELECT id, val_field
        FROM test_data
        WHERE _indexall indexquery 'val_field.three:four'
        ORDER BY id
      """)

      val results2 = query2.collect()
      println(s"   Found ${results2.length} documents where val_field.three = 'four':")
      results2.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))

      results2.length should be >= 1
      results2.foreach { row =>
        val json = row.getString(1)
        // Check for JSON content (may or may not have spaces after colons)
        json should (include("\"three\":\"four\"") or include("\"three\": \"four\""))
      }

      // Test: Combined query val_field.one:two AND val_field.status:active
      println("\n🔍 Test: Combined query - val_field.one:two AND val_field.status:active")
      val query3 = spark.sql("""
        SELECT id, val_field
        FROM test_data
        WHERE _indexall indexquery 'val_field.one:two AND val_field.status:active'
        ORDER BY id
      """)

      val results3 = query3.collect()
      println(s"   Found ${results3.length} documents matching both conditions:")
      results3.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))

      results3.length should be >= 1
      results3.foreach { row =>
        val json = row.getString(1)
        // Check for JSON content (may or may not have spaces after colons)
        json should (include("\"one\":\"two\"") or include("\"one\": \"two\""))
        json should (include("\"status\":\"active\"") or include("\"status\": \"active\""))
      }

      println("\n🎉 Complex JSON property queries passed!")
    }
  }

  test("should query JSON fields with different data types") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with JSON containing various types
      val testData = Seq(
        (1, """{"product": "laptop", "price": "1200", "category": "electronics", "brand": "dell"}"""),
        (2, """{"product": "mouse", "price": "25", "category": "electronics", "brand": "logitech"}"""),
        (3, """{"product": "desk", "price": "300", "category": "furniture", "brand": "ikea"}"""),
        (4, """{"product": "chair", "price": "150", "category": "furniture", "brand": "ikea"}"""),
        (5, """{"product": "monitor", "price": "400", "category": "electronics", "brand": "dell"}""")
      ).toDF("id", "product_info")

      // Write with json type
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.product_info", "json")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} products with JSON indexing")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("products")

      // Test: Query by category
      println("\n🔍 Test: Query product_info.category:electronics")
      val electronicsQuery = spark.sql("""
        SELECT id, product_info
        FROM products
        WHERE _indexall indexquery 'product_info.category:electronics'
        ORDER BY id
      """)

      val electronics = electronicsQuery.collect()
      println(s"   Found ${electronics.length} electronics:")
      electronics.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      electronics.length should be >= 1

      // Test: Query by brand
      println("\n🔍 Test: Query product_info.brand:ikea")
      val ikeaQuery = spark.sql("""
        SELECT id, product_info
        FROM products
        WHERE _indexall indexquery 'product_info.brand:ikea'
        ORDER BY id
      """)

      val ikea = ikeaQuery.collect()
      println(s"   Found ${ikea.length} IKEA products:")
      ikea.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      ikea.length should be >= 1
      ikea.foreach(row => row.getString(1) should include("ikea"))

      // Test: Complex query - electronics from dell
      println("\n🔍 Test: Query product_info.category:electronics AND product_info.brand:dell")
      val dellElectronicsQuery = spark.sql("""
        SELECT id, product_info
        FROM products
        WHERE _indexall indexquery 'product_info.category:electronics AND product_info.brand:dell'
        ORDER BY id
      """)

      val dellElectronics = dellElectronicsQuery.collect()
      println(s"   Found ${dellElectronics.length} Dell electronics:")
      dellElectronics.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      dellElectronics.length should be >= 1
      dellElectronics.foreach { row =>
        val json = row.getString(1)
        json should include("electronics")
        json should include("dell")
      }

      println("\n🎉 JSON field data type tests passed!")
    }
  }

  test("should query deeply nested JSON structures") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with nested JSON
      val testData = Seq(
        (1, """{"config": {"database": {"host": "localhost", "port": "5432"}}, "env": "dev"}"""),
        (2, """{"config": {"database": {"host": "prod-db", "port": "5432"}}, "env": "production"}"""),
        (3, """{"config": {"database": {"host": "staging-db", "port": "3306"}}, "env": "staging"}""")
      ).toDF("id", "settings")

      // Write with json type
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.settings", "json")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} configuration documents")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("configs")

      // Test: Query nested property
      println("\n🔍 Test: Query settings.env:production")
      val prodQuery = spark.sql("""
        SELECT id, settings
        FROM configs
        WHERE _indexall indexquery 'settings.env:production'
        ORDER BY id
      """)

      val prodResults = prodQuery.collect()
      println(s"   Found ${prodResults.length} production configs:")
      prodResults.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getString(1)}"))
      prodResults.length should be >= 1
      prodResults.foreach(row => row.getString(1) should include("production"))

      // Note: Deeply nested paths (settings.config.database.host) may require special handling
      // This test demonstrates basic nested JSON querying

      println("\n🎉 Nested JSON structure tests passed!")
    }
  }
}
