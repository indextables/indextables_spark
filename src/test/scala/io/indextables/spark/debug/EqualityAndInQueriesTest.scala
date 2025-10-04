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

package io.indextables.spark.debug

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Isolated test for the failing "should handle equality and IN queries" test. This helps debug schema consistency and
 * dirty table issues.
 */
class EqualityAndInQueriesTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }

  private def createCategoricalDataFrame(): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val _random     = new Random(42)
    val departments = Array("Engineering", "Marketing", "Sales", "HR", "Legal", "Finance")
    val statuses    = Array("active", "inactive", "pending", "deleted", "archived")
    val countries   = Array("USA", "Canada", "UK", "Germany", "France", "Japan")
    val priorities  = Array(1, 2, 3, 4, 5)

    val categoricalData = 1.to(200).map { i =>
      (
        i,
        departments(_random.nextInt(departments.length)),
        statuses(_random.nextInt(statuses.length)),
        countries(_random.nextInt(countries.length)),
        priorities(_random.nextInt(priorities.length))
      )
    }

    spark.createDataFrame(categoricalData).toDF("id", "department", "status", "country", "priority")
  }

  test("should handle equality and IN queries") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")

    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")

      val testData = createCategoricalDataFrame()
      println(s"📊 Created test data with ${testData.count()} rows")
      println(s"📋 Schema: ${testData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample data
      println("📝 Sample data:")
      testData.show(5, false)

      // Write data
      println("💾 Writing data...")
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      println("✅ Data written successfully")

      // Read the data back
      println("📖 Reading data back...")
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      println(s"📊 Read back ${readData.count()} rows")
      println(s"📋 Read schema: ${readData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample read data
      println("📝 Sample read data:")
      readData.show(5, false)

      // Test: department === "Engineering" - should return only Engineering dept rows
      println("\n🔍 Testing department === 'Engineering'")
      val engineeringResults = readData.filter(col("department") === "Engineering").collect()
      println(s"Found ${engineeringResults.length} Engineering results")
      engineeringResults.take(3).foreach { row =>
        println(s"  Row: $row")
        row.getAs[String]("department") shouldBe "Engineering"
      }
      engineeringResults.length should be > 0

      // Test: status === "active" - should return only active status rows
      println("\n🔍 Testing status === 'active'")
      val activeResults = readData.filter(col("status") === "active").collect()
      println(s"Found ${activeResults.length} active results")
      activeResults.take(3).foreach { row =>
        println(s"  Row: $row")
        row.getAs[String]("status") shouldBe "active"
      }
      activeResults.length should be > 0

      // Test: country === "USA" - should return only USA rows
      println("\n🔍 Testing country === 'USA'")
      val usaResults = readData.filter(col("country") === "USA").collect()
      println(s"Found ${usaResults.length} USA results")
      usaResults.take(3).foreach { row =>
        println(s"  Row: $row")
        row.getAs[String]("country") shouldBe "USA"
      }
      usaResults.length should be > 0

      // Test: department IN ("Engineering", "Marketing", "Sales")
      println("\n🔍 Testing department IN ('Engineering', 'Marketing', 'Sales')")
      val deptInResults = readData.filter(col("department").isin("Engineering", "Marketing", "Sales")).collect()
      println(s"Found ${deptInResults.length} department IN results")
      val validDepts = Set("Engineering", "Marketing", "Sales")
      deptInResults.take(3).foreach { row =>
        println(s"  Row: $row")
        validDepts should contain(row.getAs[String]("department"))
      }
      deptInResults.length should be > 0

      // Test: status IN ("active", "pending")
      println("\n🔍 Testing status IN ('active', 'pending')")
      val statusInResults = readData.filter(col("status").isin("active", "pending")).collect()
      println(s"Found ${statusInResults.length} status IN results")
      val validStatuses = Set("active", "pending")
      statusInResults.take(3).foreach { row =>
        println(s"  Row: $row")
        validStatuses should contain(row.getAs[String]("status"))
      }
      statusInResults.length should be > 0

      // Test: priority IN (1, 2, 3)
      println("\n🔍 Testing priority IN (1, 2, 3)")
      val priorityInResults = readData.filter(col("priority").isin(1, 2, 3)).collect()
      println(s"Found ${priorityInResults.length} priority IN results")
      val validPriorities = Set(1, 2, 3)
      priorityInResults.take(3).foreach { row =>
        println(s"  Row: $row")
        validPriorities should contain(row.getAs[Int]("priority"))
      }
      priorityInResults.length should be > 0

      // Test: department NOT IN ("HR", "Legal") - should exclude these departments
      println("\n🔍 Testing department NOT IN ('HR', 'Legal')")
      val deptNotInResults = readData.filter(!col("department").isin("HR", "Legal")).collect()
      println(s"Found ${deptNotInResults.length} department NOT IN results")
      val excludedDepts = Set("HR", "Legal")
      deptNotInResults.take(3).foreach { row =>
        println(s"  Row: $row")
        excludedDepts should not contain (row.getAs[String]("department"))
      }
      deptNotInResults.length should be > 0

      // Test: status NOT IN ("deleted", "archived") - should exclude these statuses
      println("\n🔍 Testing status NOT IN ('deleted', 'archived')")
      val statusNotInResults = readData.filter(!col("status").isin("deleted", "archived")).collect()
      println(s"Found ${statusNotInResults.length} status NOT IN results")
      val excludedStatuses = Set("deleted", "archived")
      statusNotInResults.take(3).foreach { row =>
        println(s"  Row: $row")
        excludedStatuses should not contain (row.getAs[String]("status"))
      }
      statusNotInResults.length should be > 0

      println("\n✅ All equality and IN queries completed successfully")
    }
  }
}
