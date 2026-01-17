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

package io.indextables.spark.sql

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for DESCRIBE INDEXTABLES ENVIRONMENT command. */
class DescribeEnvironmentCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeEnvironmentCommandTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Add some test properties
      .config("spark.test.property", "test_value")
      .config("spark.test.secretKey", "should_be_redacted")
      .config("spark.test.password", "should_also_be_redacted")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }

  // ===== SQL Parsing Tests =====

  test("DESCRIBE INDEXTABLES ENVIRONMENT should parse and execute") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("host")
    columns should contain("role")
    columns should contain("property_type")
    columns should contain("property_name")
    columns should contain("property_value")

    // Should return at least some rows (driver spark + hadoop properties)
    result.count() should be >= 1L
  }

  test("DESCRIBE TANTIVY4SPARK ENVIRONMENT should parse and execute") {
    val result = spark.sql("DESCRIBE TANTIVY4SPARK ENVIRONMENT")

    // Should work with alternate keyword
    result.columns should contain("host")
    result.columns should contain("role")
    result.count() should be >= 1L
  }

  test("describe indextables environment should be case insensitive") {
    val result = spark.sql("describe indextables environment")

    result.columns should contain("host")
    result.count() should be >= 1L
  }

  // ===== Result Content Tests =====

  test("DESCRIBE INDEXTABLES ENVIRONMENT should return driver rows") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    val rows   = result.collect()

    // Should have rows
    rows.length should be >= 1

    // Find driver rows
    val driverRows = rows.filter(_.getString(1) == "driver")
    driverRows.length should be >= 1

    // Driver rows should have both spark and hadoop property types
    val propertyTypes = driverRows.map(_.getString(2)).toSet
    propertyTypes should contain("spark")
    propertyTypes should contain("hadoop")
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT should include spark properties") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    result.createOrReplaceTempView("env_props_spark_test")

    // Filter to spark properties from driver using SQL
    val sparkProps = spark
      .sql(
        "SELECT * FROM env_props_spark_test WHERE role = 'driver' AND property_type = 'spark'"
      )
      .collect()

    sparkProps.length should be >= 1

    // Should include our test property
    val testProp = sparkProps.find(_.getString(3) == "spark.test.property")
    testProp shouldBe defined
    testProp.get.getString(4) shouldBe "test_value"
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT should redact sensitive values") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    result.createOrReplaceTempView("env_props_redact_test")

    // Filter to spark properties from driver using SQL
    val sparkProps = spark
      .sql(
        "SELECT * FROM env_props_redact_test WHERE role = 'driver' AND property_type = 'spark'"
      )
      .collect()

    // Find the secretKey property - should be redacted
    val secretKeyProp = sparkProps.find(_.getString(3) == "spark.test.secretKey")
    secretKeyProp shouldBe defined
    secretKeyProp.get.getString(4) shouldBe "***REDACTED***"

    // Find the password property - should be redacted
    val passwordProp = sparkProps.find(_.getString(3) == "spark.test.password")
    passwordProp shouldBe defined
    passwordProp.get.getString(4) shouldBe "***REDACTED***"
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT should include hadoop properties") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    result.createOrReplaceTempView("env_props_hadoop_test")

    // Filter to hadoop properties from driver using SQL
    val hadoopProps = spark
      .sql(
        "SELECT * FROM env_props_hadoop_test WHERE role = 'driver' AND property_type = 'hadoop'"
      )
      .collect()

    hadoopProps.length should be >= 1

    // Should include some known hadoop properties
    val propertyNames = hadoopProps.map(_.getString(3)).toSet
    // fs.defaultFS is a standard hadoop property
    propertyNames should contain("fs.defaultFS")
  }

  // ===== Schema Tests =====

  test("DESCRIBE INDEXTABLES ENVIRONMENT should have correct column types") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    val schema = result.schema

    schema("host").dataType.typeName shouldBe "string"
    schema("role").dataType.typeName shouldBe "string"
    schema("property_type").dataType.typeName shouldBe "string"
    schema("property_name").dataType.typeName shouldBe "string"
    schema("property_value").dataType.typeName shouldBe "string"
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT result should be queryable") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")

    // Register as temp view and query
    result.createOrReplaceTempView("env_properties")

    val driverProps = spark.sql("SELECT * FROM env_properties WHERE role = 'driver'")
    driverProps.count() should be >= 1L

    val sparkProps = spark.sql("SELECT * FROM env_properties WHERE property_type = 'spark'")
    sparkProps.count() should be >= 1L

    val hadoopProps = spark.sql("SELECT * FROM env_properties WHERE property_type = 'hadoop'")
    hadoopProps.count() should be >= 1L
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT should allow filtering by property name pattern") {
    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT")
    result.createOrReplaceTempView("env_properties")

    // Filter for indextables-related properties
    val indextablesProps = spark.sql(
      "SELECT * FROM env_properties WHERE property_name LIKE 'spark.indextables%'"
    )
    // May or may not have indextables properties configured
    indextablesProps.count() should be >= 0L
  }
}
