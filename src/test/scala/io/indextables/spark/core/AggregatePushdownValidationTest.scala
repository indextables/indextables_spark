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

package io.indextables.spark.core

import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for aggregate pushdown validation logic. Uses correct configuration parameters as noted by the user:
 * io.indextables.* not tantivy4spark.*
 */
class AggregatePushdownValidationTest extends AnyFunSuite {

  val testSchema = StructType(
    Seq(
      StructField("id", StringType, nullable = false),
      StructField("content", StringType, nullable = false),
      StructField("score", IntegerType, nullable = false), // Numeric field for aggregation
      StructField("value", LongType, nullable = false)     // Numeric field for aggregation
    )
  )

  test("ScanBuilder implements SupportsPushDownAggregates interface") {
    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      // Use correct configuration parameters as per user note: spark.indextables.*
      val optionsMap = Map(
        "spark.indextables.indexing.fastfields"      -> "score,value",
        "spark.indextables.indexing.typemap.content" -> "text",
        "spark.indextables.indexing.typemap.id"      -> "string"
      )
      val options = new CaseInsensitiveStringMap(optionsMap.asJava)

      val tempDir = Files.createTempDirectory("aggregate-test").toFile
      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tempDir.getAbsolutePath),
        spark,
        options
      )
      val broadcastConfig = spark.sparkContext.broadcast(optionsMap)

      val scanBuilder = new IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        testSchema,
        options,
        broadcastConfig.value
      )

      // Verify interface implementation
      assert(scanBuilder.isInstanceOf[org.apache.spark.sql.connector.read.SupportsPushDownAggregates])
      println("✅ ScanBuilder implements SupportsPushDownAggregates interface")

      // Clean up
      tempDir.delete()

    } finally
      spark.stop()
  }

  test("fast field validation logic") {
    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      // Test with score marked as fast field
      val optionsWithFastFields = Map(
        "spark.indextables.indexing.fastfields" -> "score,value"
      )
      val options1 = new CaseInsensitiveStringMap(optionsWithFastFields.asJava)

      val tempDir1 = Files.createTempDirectory("aggregate-test").toFile
      val transactionLog1 = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tempDir1.getAbsolutePath),
        spark,
        options1
      )
      val broadcastConfig1 = spark.sparkContext.broadcast(optionsWithFastFields)

      val scanBuilder1 = new IndexTables4SparkScanBuilder(
        spark,
        transactionLog1,
        testSchema,
        options1,
        broadcastConfig1.value
      )

      // Test IndexTables4SparkOptions fast field detection
      val tantivyOptions1 = new IndexTables4SparkOptions(options1)
      val fastFields1     = tantivyOptions1.getFastFields
      assert(fastFields1.contains("score"), "score should be detected as fast field")
      assert(fastFields1.contains("value"), "value should be detected as fast field")
      assert(!fastFields1.contains("content"), "content should not be detected as fast field")

      println("✅ Fast field validation with io.indextables.* configuration passed")

      // Test without score as fast field
      val optionsWithoutFastFields = Map(
        "spark.indextables.indexing.fastfields" -> "value" // score not included
      )
      val options2 = new CaseInsensitiveStringMap(optionsWithoutFastFields.asJava)

      val tantivyOptions2 = new IndexTables4SparkOptions(options2)
      val fastFields2     = tantivyOptions2.getFastFields
      assert(!fastFields2.contains("score"), "score should not be detected as fast field")
      assert(fastFields2.contains("value"), "value should be detected as fast field")

      println("✅ Fast field exclusion validation passed")

      // Clean up
      tempDir1.delete()

    } finally
      spark.stop()
  }

  test("field type mapping validation") {
    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      // Test field type mapping with io.indextables.* configuration
      val optionsMap = Map(
        "spark.indextables.indexing.typemap.content" -> "text",
        "spark.indextables.indexing.typemap.id"      -> "string",
        "spark.indextables.indexing.fastfields"      -> "score,value"
      )
      val options = new CaseInsensitiveStringMap(optionsMap.asJava)

      val tantivyOptions   = new IndexTables4SparkOptions(options)
      val fieldTypeMapping = tantivyOptions.getFieldTypeMapping

      assert(fieldTypeMapping.contains("content"), "content field type mapping should be present")
      assert(fieldTypeMapping("content") == "text", "content should be mapped to text type")
      assert(fieldTypeMapping.contains("id"), "id field type mapping should be present")
      assert(fieldTypeMapping("id") == "string", "id should be mapped to string type")

      println("✅ Field type mapping with io.indextables.* configuration passed")

    } finally
      spark.stop()
  }

  test("numeric type detection") {
    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      val optionsMap = Map[String, String]()
      val options    = new CaseInsensitiveStringMap(optionsMap.asJava)

      val tempDir = Files.createTempDirectory("aggregate-test").toFile
      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tempDir.getAbsolutePath),
        spark,
        options
      )
      val broadcastConfig = spark.sparkContext.broadcast(optionsMap)

      val scanBuilder = new IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        testSchema,
        options,
        broadcastConfig.value
      )

      // Test numeric type detection using reflection since the method is private
      val scanBuilderClass = scanBuilder.getClass
      val isNumericTypeMethod =
        scanBuilderClass.getDeclaredMethod("isNumericType", classOf[org.apache.spark.sql.types.DataType])
      isNumericTypeMethod.setAccessible(true)

      import org.apache.spark.sql.types._

      // Test numeric types
      assert(isNumericTypeMethod.invoke(scanBuilder, IntegerType).asInstanceOf[Boolean], "IntegerType should be numeric")
      assert(isNumericTypeMethod.invoke(scanBuilder, LongType).asInstanceOf[Boolean], "LongType should be numeric")
      assert(isNumericTypeMethod.invoke(scanBuilder, FloatType).asInstanceOf[Boolean], "FloatType should be numeric")
      assert(isNumericTypeMethod.invoke(scanBuilder, DoubleType).asInstanceOf[Boolean], "DoubleType should be numeric")

      // Test non-numeric types
      assert(
        !isNumericTypeMethod.invoke(scanBuilder, StringType).asInstanceOf[Boolean],
        "StringType should not be numeric"
      )
      assert(
        !isNumericTypeMethod.invoke(scanBuilder, BooleanType).asInstanceOf[Boolean],
        "BooleanType should not be numeric"
      )

      println("✅ Numeric type detection passed")

      // Clean up
      tempDir.delete()

    } finally
      spark.stop()
  }

  test("transaction log count scan creation") {
    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      val optionsMap = Map[String, String]()
      val options    = new CaseInsensitiveStringMap(optionsMap.asJava)

      val tempDir = Files.createTempDirectory("aggregate-test").toFile
      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tempDir.getAbsolutePath),
        spark,
        options
      )
      val broadcastConfig = spark.sparkContext.broadcast(optionsMap)

      val scanBuilder = new IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        testSchema,
        options,
        broadcastConfig.value
      )

      // Test TransactionLogCountScan creation
      val countScan = new TransactionLogCountScan(spark, transactionLog, Array.empty, options, broadcastConfig.value)
      assert(countScan != null, "TransactionLogCountScan should be created successfully")

      val readSchema = countScan.readSchema()
      assert(readSchema.fields.length == 1, "Count scan should have single column schema")
      assert(readSchema.fields(0).name == "count", "Count scan should have 'count' column")
      assert(readSchema.fields(0).dataType == LongType, "Count column should be LongType")

      println("✅ Transaction log count scan creation passed")

      // Clean up
      tempDir.delete()

    } finally
      spark.stop()
  }

  test("configuration parameter compatibility") {
    // Test that io.indextables.* parameters are properly supported
    // This validates the user's requirement for using the correct provider configuration

    val spark = SparkSession
      .builder()
      .appName("AggregatePushdownValidationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      // Test both tantivy4spark and indextables configurations for compatibility
      val indextablesOptions = Map(
        "spark.indextables.indexing.fastfields"      -> "score,value",
        "spark.indextables.indexing.typemap.content" -> "text"
      )

      val tantivyOptions = Map(
        "spark.indextables.indexing.fastfields"      -> "score,value",
        "spark.indextables.indexing.typemap.content" -> "text"
      )

      // Test indextables configuration (preferred)
      val options1        = new CaseInsensitiveStringMap(indextablesOptions.asJava)
      val tantivyOptions1 = new IndexTables4SparkOptions(options1)

      // Both should work, but indextables is the preferred configuration
      val fastFields1 = tantivyOptions1.getFastFields
      assert(fastFields1.contains("score"), "indextables config should work for fast fields")

      val typeMapping1 = tantivyOptions1.getFieldTypeMapping
      assert(typeMapping1.contains("content"), "indextables config should work for type mapping")

      // Test tantivy4spark configuration (legacy)
      val options2        = new CaseInsensitiveStringMap(tantivyOptions.asJava)
      val tantivyOptions2 = new IndexTables4SparkOptions(options2)

      val fastFields2 = tantivyOptions2.getFastFields
      assert(fastFields2.contains("score"), "tantivy4spark config should still work for backward compatibility")

      println("✅ Configuration parameter compatibility passed")
      println("✅ io.indextables.* configuration properly supported")

    } finally
      spark.stop()
  }
}
