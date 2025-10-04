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

package io.indextables.spark

import java.io.File
import java.nio.file.Files

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait TestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _
  protected var tempDir: String     = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("IndexTables4Spark Tests")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Set default AWS parameters to non-null values for all tests
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  override def beforeEach(): Unit =
    tempDir = Files.createTempDirectory("tantivy4spark-test").toString

  override def afterEach(): Unit =
    if (tempDir != null) {
      deleteRecursively(new File(tempDir))
    }

  protected def createTestDataFrame(): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val data = Seq(
      (1, "John Doe", 30, "Engineer", 75000.0, true),
      (2, "Jane Smith", 25, "Data Scientist", 85000.0, false),
      (3, "Bob Johnson", 35, "Manager", 95000.0, true),
      (4, "Alice Brown", 28, "Designer", 70000.0, false),
      (5, "Charlie Wilson", 32, "Developer", 80000.0, true)
    )

    spark.createDataFrame(data).toDF("id", "name", "age", "role", "salary", "active")
  }

  protected def createLargeTestDataFrame(numRows: Int = 10000): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val random     = new Random(42) // Deterministic seed for tests
    val roles      = Array("Engineer", "Data Scientist", "Manager", "Designer", "Developer")
    val _names     = Array("John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank")
    val _lastNames = Array("Doe", "Smith", "Johnson", "Brown", "Wilson", "Davis", "Miller", "Moore")

    val data = (1 until (numRows + 1)).map { i =>
      val name   = s"${_names(random.nextInt(_names.length))} ${_lastNames(random.nextInt(_lastNames.length))}"
      val age    = 20 + random.nextInt(40)
      val role   = roles(random.nextInt(roles.length))
      val salary = 50000 + random.nextInt(100000)
      val active = random.nextBoolean()

      (i, name, age, role, salary.toDouble, active)
    }

    spark.createDataFrame(data).toDF("id", "name", "age", "role", "salary", "active")
  }

  protected def getTestSchema(): StructType =
    StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("role", StringType, nullable = true),
        StructField("salary", DoubleType, nullable = true),
        StructField("active", BooleanType, nullable = true)
      )
    )

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  protected def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark").toString
    try {
      // Clear global split cache before each test to avoid schema pollution
      try {
        import _root_.io.indextables.spark.storage.{GlobalSplitCacheManager, SplitLocationRegistry}
        GlobalSplitCacheManager.flushAllCaches()
        SplitLocationRegistry.clearAllLocations()
      } catch {
        case _: Exception => // Ignore if cache clearing fails
      }
      f(path)
    } finally
      deleteRecursively(new File(path))
  }
}
