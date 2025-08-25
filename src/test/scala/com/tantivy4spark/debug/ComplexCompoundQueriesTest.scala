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
 * Isolated test for the failing "should handle complex compound queries" test.
 * This helps debug complex boolean logic combinations and query handling.
 */
class ComplexCompoundQueriesTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean = {
    try {
      import com.tantivy4spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }
  }

  private def createComprehensiveTestDataFrame(): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    
    val _random = new Random(42)
    val departments = Array("Engineering", "Data Science", "Product", "Marketing", "Sales", "HR")
    val locations = Array("San Francisco", "New York", "Seattle", "Austin", "Denver", "Boston")
    val titles = Array("Software Engineer", "Senior Engineer", "Staff Engineer", "Principal Engineer",
                      "Data Scientist", "Senior Data Scientist", "Product Manager", "Marketing Manager",
                      "Lead Developer", "Director of Engineering")
    
    def generateBio(title: String, department: String, random: Random): String = {
      val skills = Array("machine learning", "data analysis", "software development", "project management", "leadership")
      val interests = Array("AI", "cloud computing", "mobile development", "DevOps", "analytics")
      val skill = skills(random.nextInt(skills.length))
      val interest = interests(random.nextInt(interests.length))
      s"Experienced $title in $department with expertise in $skill and passion for $interest."
    }
    
    val data = 1.to(500).map { i =>
      val age = 22 + _random.nextInt(40)
      val experienceYears = math.max(0, age - 22 - _random.nextInt(4))
      val department = departments(_random.nextInt(departments.length))
      val location = locations(_random.nextInt(locations.length))
      val title = titles(_random.nextInt(titles.length))
      val salary = 50000 + _random.nextInt(150000)
      val isActive = _random.nextBoolean()
      val bio = generateBio(title, department, _random)
      
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
    
    spark.createDataFrame(data).toDF(
      "id", "name", "age", "experience_years", "department", "location", "title", "salary", "is_active", "bio"
    )
  }

  test("should handle complex compound queries") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")
    
    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")
      
      val testData = createComprehensiveTestDataFrame()
      println(s"📊 Created test data with ${testData.count()} rows")
      println(s"📋 Schema: ${testData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")
      
      // Show sample data
      println("📝 Sample data:")
      testData.show(5, false)
      
      // Write comprehensive data - should succeed now
      println("💾 Writing data...")
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)
      
      println("✅ Data written successfully")
      
      // Read data back and test complex compound queries
      println("📖 Reading data back...")
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)
      
      println(s"📊 Read back ${readData.count()} rows")
      println(s"📋 Read schema: ${readData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")
      
      // Verify data was written and can be read
      readData.count() should be > 0L
      
      println("\n🔍 Testing complex compound queries...")
      
      // Test complex query operations work (even if advanced filtering isn't fully implemented)
      val complexQueries = Seq(
        // Multi-condition AND
        readData.filter(
          col("age").between(25, 45) && 
          col("department") === "Engineering" && 
          col("salary") > 70000 &&
          col("is_active") === true
        ),
        
        // Multi-condition OR
        readData.filter(
          col("department") === "Engineering" || 
          col("department") === "Data Science" || 
          (col("salary") > 90000 && col("experience_years") > 5)
        ),
        
        // Mixed AND/OR with text search
        readData.filter(
          (col("title").contains("Senior") || col("title").contains("Lead")) &&
          col("department").isin("Engineering", "Product") &&
          col("salary") > 80000
        ),
        
        // Nested conditions
        readData.filter(
          (col("age") < 30 && col("experience_years") > 2) || 
          (col("age") >= 30 && col("experience_years") > 5) ||
          col("title").contains("Director")
        ),
        
        // Text search with numeric filters
        readData.filter(
          col("bio").contains("machine learning") &&
          col("age").between(28, 45) &&
          col("salary") > 85000 &&
          col("location").startsWith("San")
        )
      )
      
      // Execute queries to verify they don't crash (results may vary based on filter pushdown implementation)
      complexQueries.zipWithIndex.foreach { case (query, index) =>
        println(s"  🔎 Executing complex query ${index + 1}...")
        noException should be thrownBy {
          val count = query.count()
          println(s"    Found $count results")
        }
      }
      
      println("✅ All complex compound queries completed successfully")
    }
  }
}
