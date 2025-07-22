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

package com.tantivy4spark.integration

import com.tantivy4spark.TantivyTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.util.Random

class TantivySparkIntegrationTest extends AnyFlatSpec with Matchers with TantivyTestBase {

  "Tantivy Spark Integration" should "write and read data with full Spark context" in {
    val indexPath = s"${testDir}/spark_integration_index"
    
    // Create test data with various data types
    val testData = Seq(
      Row(1L, "Alice Johnson", "alice@example.com", 28, 95.5, true, new Timestamp(System.currentTimeMillis() - 86400000), "Engineering"),
      Row(2L, "Bob Smith", "bob@smith.com", 34, 87.2, true, new Timestamp(System.currentTimeMillis() - 172800000), "Marketing"),
      Row(3L, "Carol Davis", "carol@davis.net", 29, 92.8, false, new Timestamp(System.currentTimeMillis() - 259200000), "Engineering"),
      Row(4L, "David Wilson", "david@wilson.org", 45, 88.1, true, new Timestamp(System.currentTimeMillis() - 345600000), "Sales"),
      Row(5L, "Eve Brown", "eve@brown.co", 31, 94.3, false, new Timestamp(System.currentTimeMillis() - 432000000), "Engineering"),
      Row(6L, "Frank Miller", "frank@miller.io", 38, 85.7, true, new Timestamp(System.currentTimeMillis() - 518400000), "Marketing"),
      Row(7L, "Grace Lee", "grace@lee.dev", 26, 96.9, true, new Timestamp(System.currentTimeMillis() - 604800000), "Engineering"),
      Row(8L, "Henry Taylor", "henry@taylor.net", 42, 89.4, false, new Timestamp(System.currentTimeMillis() - 691200000), "Sales")
    )
    
    val schema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("email", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("created_at", TimestampType, nullable = false),
      StructField("department", StringType, nullable = false)
    ))
    
    // Create DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    
    // Write to Tantivy index using the Tantivy format
    df.write
      .format("tantivy")
      .option("index.id", "spark_integration_test")
      .option("tantivy.base.path", testDir.toString)
      .mode("overwrite")
      .save(indexPath)
    
    // Read back from Tantivy index
    val readDf = spark.read
      .format("tantivy")
      .option("index.id", "spark_integration_test")
      .option("tantivy.base.path", testDir.toString)
      .load(indexPath)
    
    // Verify data integrity
    val readData = readDf.collect().sortBy(_.getAs[Long]("user_id"))
    readData should have length testData.length
    
    // Test filtering operations on Tantivy data
    val engineeringUsers = readDf.filter(col("department") === "Engineering").collect()
    engineeringUsers.length should be > 0
    engineeringUsers.foreach(_.getAs[String]("department") should be("Engineering"))
    
    val youngUsers = readDf.filter(col("age") < 30).collect()
    youngUsers.foreach(_.getAs[Int]("age") should be < 30)
    
    val activeUsers = readDf.filter(col("active") === true).collect()
    activeUsers.foreach(_.getAs[Boolean]("active") should be(true))
    
    val highScoreUsers = readDf.filter(col("score") > 90.0).collect()
    highScoreUsers.foreach(_.getAs[Double]("score") should be > 90.0)
    
    // Verify specific records
    val aliceRecord = readData.find(_.getAs[String]("name") == "Alice Johnson")
    aliceRecord should be(defined)
    aliceRecord.get.getAs[String]("email") should be("alice@example.com")
    aliceRecord.get.getAs[Int]("age") should be(28)
    aliceRecord.get.getAs[String]("department") should be("Engineering")
    
    val bobRecord = readData.find(_.getAs[String]("name") == "Bob Smith")
    bobRecord should be(defined)
    bobRecord.get.getAs[Double]("score") should be(87.2 +- 0.01)
    bobRecord.get.getAs[Boolean]("active") should be(true)
    
    // Verify schema inference worked correctly
    val readSchema = readDf.schema
    readSchema.fieldNames should contain allOf("user_id", "name", "email", "age", "score", "active", "created_at", "department")
    
    println(s"Successfully tested Tantivy write/read cycle with ${testData.length} test records")
  }
  
  it should "support various query operations and where clauses" in {
    val indexPath = s"${testDir}/query_operations_index"
    
    // Create comprehensive test dataset using Spark DataFrame operations
    val random = new Random(42) // Fixed seed for reproducible tests
    val departments = Array("Engineering", "Marketing", "Sales", "HR", "Finance")
    val statuses = Array("Active", "Inactive", "Pending", "Suspended")
    val cities = Array("New York", "San Francisco", "Chicago", "Austin", "Seattle")
    
    val testData = (1 to 100).map { i =>
      Row(
        i.toLong,
        s"User${i}",
        s"user${i}@company${i % 10}.com",
        20 + random.nextInt(45), // age 20-64
        random.nextDouble() * 100, // score 0-100
        random.nextBoolean(), // active
        new Timestamp(System.currentTimeMillis() - random.nextInt(365) * 86400000L), // created within last year
        departments(random.nextInt(departments.length)),
        statuses(random.nextInt(statuses.length)),
        cities(random.nextInt(cities.length)),
        1000 + random.nextInt(9000), // salary 1000-10000
        random.nextFloat() * 5 // rating 0-5
      )
    }
    
    val schema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("email", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("created_at", TimestampType, nullable = false),
      StructField("department", StringType, nullable = false),
      StructField("status", StringType, nullable = false),
      StructField("city", StringType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("rating", FloatType, nullable = false)
    ))
    
    // Create DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    
    // Write to Tantivy index using the Tantivy format
    df.write
      .format("tantivy")
      .option("index.id", "query_operations_test")
      .option("tantivy.base.path", testDir.toString)
      .mode("overwrite")
      .save(indexPath)
    
    // Read back from Tantivy index
    val readDf = spark.read
      .format("tantivy")
      .option("index.id", "query_operations_test")
      .option("tantivy.base.path", testDir.toString)
      .load(indexPath)
    
    // Test query operations on the Tantivy data
    // This validates that the Tantivy file provider can perform filtering using Tantivy queries
    
    readDf.createOrReplaceTempView("users")
    
    // Test 1: Equality operations
    val engineeringUsers = readDf.filter(col("department") === "Engineering").collect()
    engineeringUsers.length should be > 0
    engineeringUsers.foreach(_.getAs[String]("department") should be("Engineering"))
    
    // Test 2: Numeric comparisons
    val youngUsers = readDf.filter(col("age") < 30).collect()
    youngUsers.foreach(_.getAs[Int]("age") should be < 30)
    
    val highScoreUsers = readDf.filter(col("score") >= 80.0).collect()
    highScoreUsers.foreach(_.getAs[Double]("score") should be >= 80.0)
    
    val salaryRangeUsers = readDf.filter(col("salary").between(5000, 8000)).collect()
    salaryRangeUsers.foreach { row =>
      val salary = row.getAs[Int]("salary")
      salary should be >= 5000
      salary should be <= 8000
    }
    
    // Test 3: Boolean operations
    val activeUsers = readDf.filter(col("active") === true).collect()
    activeUsers.foreach(_.getAs[Boolean]("active") should be(true))
    
    val inactiveUsers = readDf.filter(col("active") === false).collect()
    inactiveUsers.foreach(_.getAs[Boolean]("active") should be(false))
    
    // Test 4: String operations
    val emailPatternUsers = readDf.filter(col("email").contains("company1")).collect()
    emailPatternUsers.foreach(_.getAs[String]("email") should include("company1"))
    
    val nameStartsWithUsers = readDf.filter(col("name").startsWith("User1")).collect()
    nameStartsWithUsers.foreach(_.getAs[String]("name") should startWith("User1"))
    
    // Test 5: IN operations
    val techDepartments = readDf.filter(col("department").isin("Engineering", "Marketing")).collect()
    techDepartments.foreach { row =>
      val dept = row.getAs[String]("department")
      dept should (be("Engineering") or be("Marketing"))
    }
    
    val specificCities = readDf.filter(col("city").isin("New York", "San Francisco", "Austin")).collect()
    specificCities.foreach { row =>
      val city = row.getAs[String]("city")
      city should (be("New York") or be("San Francisco") or be("Austin"))
    }
    
    // Test 6: Complex AND/OR operations
    val complexQuery1 = readDf.filter(
      (col("department") === "Engineering") && 
      (col("age") > 25) && 
      (col("score") >= 70.0)
    ).collect()
    
    complexQuery1.foreach { row =>
      row.getAs[String]("department") should be("Engineering")
      row.getAs[Int]("age") should be > 25
      row.getAs[Double]("score") should be >= 70.0
    }
    
    val complexQuery2 = readDf.filter(
      ((col("department") === "Sales") || (col("department") === "Marketing")) &&
      (col("active") === true) &&
      (col("salary") > 5000)
    ).collect()
    
    complexQuery2.foreach { row =>
      val dept = row.getAs[String]("department")
      dept should (be("Sales") or be("Marketing"))
      row.getAs[Boolean]("active") should be(true)
      row.getAs[Int]("salary") should be > 5000
    }
    
    // Test 7: NOT operations
    val notEngineeringUsers = readDf.filter(col("department") =!= "Engineering").collect()
    notEngineeringUsers.foreach(_.getAs[String]("department") should not be "Engineering")
    
    // Test 8: NULL handling (using isNull and isNotNull)
    val nonNullNames = readDf.filter(col("name").isNotNull).collect()
    nonNullNames should have length testData.length // All names should be non-null
    
    // Test 9: Aggregations with filtering
    val deptAverages = readDf
      .filter(col("active") === true)
      .groupBy("department")
      .agg(
        avg("age").alias("avg_age"),
        avg("score").alias("avg_score"),
        count("*").alias("count")
      )
      .collect()
    
    deptAverages.length should be > 0
    deptAverages.foreach { row =>
      row.getAs[Double]("avg_age") should be >= 20.0
      row.getAs[Double]("avg_score") should be >= 0.0
      row.getAs[Long]("count") should be > 0L
    }
    
    // Test 10: SQL-style queries
    val sqlResult1 = spark.sql("""
      SELECT department, COUNT(*) as user_count, AVG(salary) as avg_salary
      FROM users 
      WHERE age BETWEEN 25 AND 40 AND active = true
      GROUP BY department
      ORDER BY avg_salary DESC
    """).collect()
    
    sqlResult1.length should be > 0
    
    val sqlResult2 = spark.sql("""
      SELECT name, email, score, department
      FROM users 
      WHERE (department = 'Engineering' OR department = 'Sales') 
        AND score > 85.0 
        AND status IN ('Active', 'Pending')
      ORDER BY score DESC
      LIMIT 10
    """).collect()
    
    sqlResult2.length should be <= 10
    sqlResult2.foreach { row =>
      val dept = row.getAs[String]("department")
      dept should (be("Engineering") or be("Sales"))
      row.getAs[Double]("score") should be > 85.0
    }
    
    // Test 11: Date/Timestamp operations
    val currentTime = System.currentTimeMillis()
    val thirtyDaysAgo = new Timestamp(currentTime - 30 * 86400000L)
    
    val recentUsers = df.filter(col("created_at") > lit(thirtyDaysAgo)).collect()
    recentUsers.foreach { row =>
      row.getAs[Timestamp]("created_at").getTime should be > thirtyDaysAgo.getTime
    }
    
    // Test 12: Float comparisons
    val highRatedUsers = readDf.filter(col("rating") >= 4.0f).collect()
    highRatedUsers.foreach(_.getAs[Float]("rating") should be >= 4.0f)
    
    // Test 13: Multiple column sorting and limiting
    val topPerformers = readDf
      .filter((col("score") >= 90.0) && (col("active") === true))
      .orderBy(col("score").desc, col("age").asc)
      .limit(5)
      .collect()
    
    topPerformers.length should be <= 5
    topPerformers.foreach { row =>
      row.getAs[Double]("score") should be >= 90.0
      row.getAs[Boolean]("active") should be(true)
    }
    
    // Verify results are properly sorted
    if (topPerformers.length > 1) {
      for (i <- 0 until topPerformers.length - 1) {
        val currentScore = topPerformers(i).getAs[Double]("score")
        val nextScore = topPerformers(i + 1).getAs[Double]("score")
        currentScore should be >= nextScore
      }
    }
    
    // Test 14: Case-insensitive string operations (if supported)
    val upperCaseQuery = readDf.filter(upper(col("department")) === "ENGINEERING").collect()
    upperCaseQuery.foreach(_.getAs[String]("department") should be("Engineering"))
    
    // Verify overall data integrity
    val totalCount = readDf.count()
    totalCount should be(testData.length)
    
    println(s"Successfully executed ${15} different query operation tests on ${testData.length} records")
  }
  
  it should "handle large datasets efficiently" in {
    val indexPath = s"${testDir}/large_dataset_index"
    val recordCount = 100000
    
    // Generate larger dataset
    val random = new Random(123)
    val companies = (1 to 50).map(i => s"Company${i}")
    val roles = Array("Developer", "Manager", "Analyst", "Designer", "Architect", "Lead", "Senior", "Junior")
    val skills = Array("Java", "Scala", "Python", "JavaScript", "Go", "Rust", "C++", "Swift")
    
    val largeTestData = (1 to recordCount).map { i =>
      Row(
        i.toLong,
        s"Employee${i}",
        s"emp${i}@${companies(random.nextInt(companies.length)).toLowerCase}.com",
        22 + random.nextInt(40), // age 22-61
        random.nextDouble() * 100, // performance score
        random.nextBoolean(), // is_manager
        new Timestamp(System.currentTimeMillis() - random.nextInt(1000) * 86400000L), // hire_date
        roles(random.nextInt(roles.length)),
        skills(random.nextInt(skills.length)),
        30000 + random.nextInt(120000), // salary 30k-150k
        random.nextFloat() * 10, // years_experience 0-10
        companies(random.nextInt(companies.length))
      )
    }
    
    val largeSchema = StructType(Seq(
      StructField("employee_id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("email", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("performance_score", DoubleType, nullable = false),
      StructField("is_manager", BooleanType, nullable = false),
      StructField("hire_date", TimestampType, nullable = false),
      StructField("role", StringType, nullable = false),
      StructField("primary_skill", StringType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("years_experience", FloatType, nullable = false),
      StructField("company", StringType, nullable = false)
    ))
    
    // Create large DataFrame
    val largeDf = spark.createDataFrame(spark.sparkContext.parallelize(largeTestData), largeSchema)
    
    // Write to Tantivy index using the Tantivy format
    val writeStart = System.currentTimeMillis()
    largeDf.write
      .format("tantivy")
      .option("index.id", "large_dataset_test")
      .option("tantivy.base.path", testDir.toString)
      .mode("overwrite")
      .save(indexPath)
    val writeTime = System.currentTimeMillis() - writeStart
    
    // Read back from Tantivy index
    val readStart = System.currentTimeMillis()
    val readDf = spark.read
      .format("tantivy")
      .option("index.id", "large_dataset_test")
      .option("tantivy.base.path", testDir.toString)
      .load(indexPath)
    val readTime = System.currentTimeMillis() - readStart
    
    // Test DataFrame creation performance
    val createStart = System.currentTimeMillis()
    readDf.count() should be(recordCount)
    val createTime = System.currentTimeMillis() - createStart
    
    // Complex query performance test on Tantivy data
    val queryStart = System.currentTimeMillis()
    val complexResult = readDf
      .filter(
        (col("performance_score") >= 75.0) &&
        (col("salary") > 80000) &&
        (col("years_experience") >= 3.0f) &&
        col("role").isin("Developer", "Architect", "Lead") &&
        (col("is_manager") === true || col("primary_skill").isin("Java", "Scala", "Python"))
      )
      .groupBy("company", "role")
      .agg(
        count("*").alias("employee_count"),
        avg("performance_score").alias("avg_performance"),
        avg("salary").alias("avg_salary"),
        max("years_experience").alias("max_experience")
      )
      .orderBy(col("avg_performance").desc)
      .collect()
    val queryTime = System.currentTimeMillis() - queryStart
    
    // Test that we can process large datasets and work with Tantivy components
    import com.tantivy4spark.config.TantivyConfig
    import com.tantivy4spark.search.TantivyIndexWriter
    
    val options = Map(
      "index.id" -> "large_dataset_test",
      "tantivy.base.path" -> testDir.toString
    )
    
    // Test schema generation with large schema
    val config = TantivyConfig.fromSpark(largeSchema, options)
    config.indexes.head.docMapping.fieldMappings should have size largeSchema.fields.length
    
    // Performance assertions for Tantivy operations
    writeTime should be < 30000L    // Should write to Tantivy reasonably quickly
    readTime should be < 5000L      // Should read from Tantivy quickly
    createTime should be < 5000L    // Should create DataFrame quickly
    queryTime should be < 15000L    // Should execute complex query on Tantivy data
    
    complexResult.length should be > 0
    
    println(s"Performance results for $recordCount records:")
    println(s"  Tantivy write time: ${writeTime}ms")
    println(s"  Tantivy read time: ${readTime}ms")
    println(s"  DataFrame creation time: ${createTime}ms")
    println(s"  Complex query time: ${queryTime}ms")
    println(s"  Complex query results: ${complexResult.length} groups")
    println(s"  Schema fields mapped: ${config.indexes.head.docMapping.fieldMappings.size}")
  }
}
