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

package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import scala.util.Random

/**
 * Isolated test for the failing "should complete full write/read/query cycle with comprehensive filters" test. This
 * helps debug the full end-to-end filtering workflow and comprehensive query execution.
 */
class ComprehensiveFiltersTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import com.tantivy4spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }

  private def createComprehensiveTestDataFrame(): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val _random     = new Random(42)
    val departments = Array("Engineering", "Data Science", "Product", "Marketing", "Sales", "HR")
    val locations   = Array("San Francisco", "New York", "Seattle", "Austin", "Denver", "Boston")
    val titles = Array(
      "Software Engineer",
      "Senior Engineer",
      "Staff Engineer",
      "Principal Engineer",
      "Data Scientist",
      "Senior Data Scientist",
      "Product Manager",
      "Marketing Manager"
    )

    def generateBio(
      title: String,
      department: String,
      random: Random
    ): String = {
      val skills = Array("machine learning", "data analysis", "software development", "project management", "leadership")
      val interests = Array("AI", "cloud computing", "mobile development", "DevOps", "analytics")
      val skill     = skills(random.nextInt(skills.length))
      val interest  = interests(random.nextInt(interests.length))
      s"Experienced $title in $department with expertise in $skill and passion for $interest."
    }

    val data = 1.to(1000).map { i =>
      val age             = 22 + _random.nextInt(40)
      val experienceYears = math.max(0, age - 22 - _random.nextInt(4))
      val department      = departments(_random.nextInt(departments.length))
      val location        = locations(_random.nextInt(locations.length))
      val title           = titles(_random.nextInt(titles.length))
      val salary          = 50000 + _random.nextInt(150000)
      val isActive        = _random.nextBoolean()
      val bio             = generateBio(title, department, _random)

      (
        i.toLong,
        s"Employee$i",
        age,
        experienceYears,
        department,
        location,
        title,
        salary.toDouble,
        isActive,
        bio
      )
    }

    spark
      .createDataFrame(data)
      .toDF(
        "id",
        "name",
        "age",
        "experience_years",
        "department",
        "location",
        "title",
        "salary",
        "is_active",
        "bio"
      )
  }

  test("should complete full write/read/query cycle with comprehensive filters") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")

    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")

      val testData = createComprehensiveTestDataFrame()
      println(s"📊 Created test data with ${testData.count()} rows")
      println(s"📋 Schema: ${testData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample data
      println("📝 Sample data:")
      testData.show(5, false)

      // Step 1: Write comprehensive test data
      println("💾 Writing comprehensive test data...")
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      println("✅ Data written successfully")

      // Step 2: Read data back and execute comprehensive queries
      println("📖 Reading data back...")
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      println(s"📊 Read back ${readData.count()} rows")
      println(s"📋 Read schema: ${readData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample read data
      println("📝 Sample read data:")
      readData.show(5, false)

      println("\n🔍 Testing comprehensive filters...")

      // Test: Text search - bio contains "machine learning"
      println("  🔎 Test: Text search - bio contains 'machine learning'")
      val mlBioResults = readData.filter(col("bio").contains("machine learning")).collect()
      println(s"    Found ${mlBioResults.length} results")
      mlBioResults.take(3).foreach { row =>
        val bio = row.getAs[String]("bio")
        println(s"    Bio: $bio")
        bio.toLowerCase should include("machine learning")
      }

      // Test: Text search - title starts with "Senior"
      println("  🔎 Test: Text search - title starts with 'Senior'")
      val seniorTitleResults = readData.filter(col("title").startsWith("Senior")).collect()
      println(s"    Found ${seniorTitleResults.length} results")
      seniorTitleResults.take(3).foreach { row =>
        val title = row.getAs[String]("title")
        println(s"    Title: $title")
        title should startWith("Senior")
      }

      // Test: Text search - location ends with "Francisco"
      println("  🔎 Test: Text search - location ends with 'Francisco'")
      val sfLocationResults = readData.filter(col("location").endsWith("Francisco")).collect()
      println(s"    Found ${sfLocationResults.length} results")
      sfLocationResults.take(3).foreach { row =>
        val location = row.getAs[String]("location")
        println(s"    Location: $location")
        location should endWith("Francisco")
      }

      // Test: Numeric range - age between 25 and 45
      println("  🔎 Test: Numeric range - age between 25 and 45")
      val ageRangeResults = readData.filter(col("age").between(25, 45)).collect()
      println(s"    Found ${ageRangeResults.length} results")
      ageRangeResults.take(3).foreach { row =>
        val age = row.getAs[Int]("age")
        println(s"    Age: $age")
        age should be >= 25
        age should be <= 45
      }

      // Test: Numeric comparison - salary > 70000
      println("  🔎 Test: Numeric comparison - salary > 70000")
      val highSalaryResults = readData.filter(col("salary") > 70000).collect()
      println(s"    Found ${highSalaryResults.length} results")
      highSalaryResults.take(3).foreach { row =>
        val salary = row.getAs[Double]("salary")
        println(s"    Salary: $salary")
        salary should be > 70000.0
      }

      // Test: Equality - department === "Engineering"
      println("  🔎 Test: Equality - department === 'Engineering'")
      val engineeringResults = readData.filter(col("department") === "Engineering").collect()
      println(s"    Found ${engineeringResults.length} results")
      engineeringResults.take(3).foreach { row =>
        val department = row.getAs[String]("department")
        println(s"    Department: $department")
        department shouldBe "Engineering"
      }

      // Test: IN query - department in ("Engineering", "Data Science")
      println("  🔎 Test: IN query - department in ('Engineering', 'Data Science')")
      val deptInResults = readData.filter(col("department").isin("Engineering", "Data Science")).collect()
      println(s"    Found ${deptInResults.length} results")
      val validDepts = Set("Engineering", "Data Science")
      deptInResults.take(3).foreach { row =>
        val department = row.getAs[String]("department")
        println(s"    Department: $department")
        validDepts should contain(department)
      }

      // Test: Complex compound query - Engineering + high salary + active
      println("  🔎 Test: Complex compound query - Engineering + high salary + active")
      val complexResults = readData
        .filter(
          col("department") === "Engineering" &&
            col("salary") > 80000 &&
            col("is_active") === true
        )
        .collect()

      println(s"    Found ${complexResults.length} results")
      complexResults.take(3).foreach { row =>
        val department = row.getAs[String]("department")
        val salary     = row.getAs[Double]("salary")
        val isActive   = row.getAs[Boolean]("is_active")
        println(s"    Department: $department, Salary: $salary, Active: $isActive")
        department shouldBe "Engineering"
        salary should be > 80000.0
        isActive shouldBe true
      }

      println("  🔎 Test: Aggregation data type dumps")
      readData.filter(col("department") === "Engineering").show()
      readData.filter(col("department") === "Engineering").printSchema()
      readData
        .filter(col("department") === "Engineering")
        .groupBy("location")
        .agg(count("*").alias("count"), avg("salary").alias("avg_salary"))
        .printSchema()

      // Test: Aggregation with filters
      println("  🔎 Test: Aggregation with filters")
      val aggResults = readData
        .filter(col("department") === "Engineering")
        .groupBy("location")
        .agg(count("*").alias("count"), avg("salary").alias("avg_salary"))
        .collect()

      println(s"    Found ${aggResults.length} aggregation results")
      aggResults.take(3).foreach { row =>
        val location  = row.getAs[String]("location")
        val count     = row.getAs[Long]("count")
        val avgSalary = row.getAs[Double]("avg_salary")
        println(s"    Location: $location, Count: $count, Avg Salary: $avgSalary")
        count should be > 0L
        avgSalary should be > 0.0
      }

      // Verify we got results for most queries (data should have variety)
      println(s"\n📈 Results summary:")
      println(s"  Engineering results: ${engineeringResults.length}")
      println(s"  High salary results: ${highSalaryResults.length}")
      println(s"  Age range results: ${ageRangeResults.length}")
      println(s"  Department IN results: ${deptInResults.length}")
      println(s"  Complex compound results: ${complexResults.length}")

      engineeringResults.length should be > 0
      highSalaryResults.length should be > 0
      ageRangeResults.length should be > 0
      deptInResults.length should be > 0

      println("✅ All comprehensive filters completed successfully")
    }
  }
}
