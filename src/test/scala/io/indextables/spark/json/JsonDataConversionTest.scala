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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.indextables.spark.core.IndexTables4SparkOptions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class JsonDataConversionTest extends AnyFunSuite with Matchers {

  private def createOptions(configMap: Map[String, String] = Map.empty): IndexTables4SparkOptions = {
    new IndexTables4SparkOptions(new CaseInsensitiveStringMap(configMap.asJava))
  }

  test("structToJsonMap converts simple struct") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val row = Row("Alice", 30)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    jsonMap.get("age") shouldBe 30
  }

  test("structToJsonMap handles null fields") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val row = Row("Alice", null)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    // Null values are explicitly added as JSON nulls for proper round-tripping
    jsonMap.containsKey("age") shouldBe true
    jsonMap.get("age") shouldBe null
  }

  test("structToJsonMap converts nested struct") {
    val addressSchema = StructType(Seq(
      StructField("city", StringType),
      StructField("zip", StringType)
    ))
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("address", addressSchema)
    ))

    val row = Row("Alice", Row("NYC", "10001"))
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    val addressMap = jsonMap.get("address").asInstanceOf[java.util.Map[String, Object]]
    addressMap.get("city") shouldBe "NYC"
    addressMap.get("zip") shouldBe "10001"
  }

  test("arrayToJsonList converts simple string array") {
    val arrayType = ArrayType(StringType)
    val tags = Seq("tag1", "tag2", "tag3")

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(tags, arrayType)

    jsonList.size() shouldBe 3
    jsonList.get(0) shouldBe "tag1"
    jsonList.get(1) shouldBe "tag2"
    jsonList.get(2) shouldBe "tag3"
  }

  test("arrayToJsonList converts numeric array") {
    val arrayType = ArrayType(IntegerType)
    val numbers = Seq(10, 20, 30)

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(numbers, arrayType)

    jsonList.size() shouldBe 3
    jsonList.get(0) shouldBe 10
    jsonList.get(1) shouldBe 20
    jsonList.get(2) shouldBe 30
  }

  test("arrayToJsonList handles null elements") {
    val arrayType = ArrayType(StringType)
    val tags = Seq("tag1", null, "tag3")

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(tags, arrayType)

    // Null elements are filtered out
    jsonList.size() shouldBe 2
    jsonList.get(0) shouldBe "tag1"
    jsonList.get(1) shouldBe "tag3"
  }

  test("convertToJsonValue handles all primitive types") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    converter.convertToJsonValue("test", StringType) shouldBe "test"
    converter.convertToJsonValue(42, IntegerType) shouldBe 42
    converter.convertToJsonValue(123L, LongType) shouldBe 123L
    converter.convertToJsonValue(3.14f, FloatType) shouldBe 3.14f
    converter.convertToJsonValue(2.718, DoubleType) shouldBe 2.718
    converter.convertToJsonValue(true, BooleanType) shouldBe true
  }

  test("convertToJsonValue converts DateType to milliseconds") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val days = 100  // 100 days since epoch
    val result = converter.convertToJsonValue(days, DateType)

    result shouldBe (100L * 86400000L)  // Days to milliseconds
  }

  test("convertToJsonValue converts TimestampType to milliseconds") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val schema = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val micros = 1000000L  // 1 second in microseconds
    val result = converter.convertToJsonValue(micros, TimestampType)

    result shouldBe 1000L  // Microseconds to milliseconds
  }

  test("jsonMapToRow converts simple map to Row") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("name", "Alice")
    jsonMap.put("age", Integer.valueOf(30))

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val row = converter.jsonMapToRow(jsonMap, schema)

    row.getString(0) shouldBe "Alice"
    row.getInt(1) shouldBe 30
  }

  test("jsonMapToRow handles null values") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("name", "Alice")
    // age is null

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val row = converter.jsonMapToRow(jsonMap, schema)

    row.getString(0) shouldBe "Alice"
    row.isNullAt(1) shouldBe true
  }

  test("jsonListToArray converts simple list to Seq") {
    val arrayType = ArrayType(StringType)

    val jsonList = new java.util.ArrayList[Object]()
    jsonList.add("tag1")
    jsonList.add("tag2")
    jsonList.add("tag3")

    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val result = converter.jsonListToArray(jsonList, arrayType)

    result.numElements() shouldBe 3
    result.getUTF8String(0).toString shouldBe "tag1"
    result.getUTF8String(1).toString shouldBe "tag2"
    result.getUTF8String(2).toString shouldBe "tag3"
  }

  test("convertFromJsonValue handles all primitive types") {
    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    // String returns UTF8String for Spark internal format
    converter.convertFromJsonValue("test", StringType).toString shouldBe "test"
    converter.convertFromJsonValue(Integer.valueOf(42), IntegerType) shouldBe 42
    converter.convertFromJsonValue(java.lang.Long.valueOf(123L), LongType) shouldBe 123L
    converter.convertFromJsonValue(java.lang.Float.valueOf(3.14f), FloatType) shouldBe 3.14f
    converter.convertFromJsonValue(java.lang.Double.valueOf(2.718), DoubleType) shouldBe 2.718
    converter.convertFromJsonValue(java.lang.Boolean.valueOf(true), BooleanType) shouldBe true
  }

  test("convertFromJsonValue converts milliseconds to DateType") {
    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val millis = 100L * 86400000L  // 100 days in milliseconds
    val result = converter.convertFromJsonValue(java.lang.Long.valueOf(millis), DateType)

    result shouldBe 100  // Days since epoch
  }

  test("convertFromJsonValue converts milliseconds to TimestampType") {
    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val millis = 1000L  // 1 second in milliseconds
    val result = converter.convertFromJsonValue(java.lang.Long.valueOf(millis), TimestampType)

    result shouldBe 1000000L  // Microseconds
  }

  test("round-trip conversion preserves struct data") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("score", DoubleType)
    ))

    val originalRow = Row("Alice", 30, 95.5)

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val writeConverter = new SparkToTantivyConverter(schema, mapper)
    val readConverter = new TantivyToSparkConverter(schema, mapper)

    // Convert to JSON map
    val jsonMap = writeConverter.structToJsonMap(originalRow, schema)

    // Convert back to Row
    val resultRow = readConverter.jsonMapToRow(jsonMap, schema)

    resultRow.getString(0) shouldBe "Alice"
    resultRow.getInt(1) shouldBe 30
    resultRow.getDouble(2) shouldBe 95.5
  }

  test("round-trip conversion preserves array data") {
    val arrayType = ArrayType(IntegerType)
    val originalArray = Seq(10, 20, 30, 40, 50)

    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val writeConverter = new SparkToTantivyConverter(schema, mapper)
    val readConverter = new TantivyToSparkConverter(schema, mapper)

    // Convert to JSON list
    val jsonList = writeConverter.arrayToJsonList(originalArray, arrayType)

    // Convert back to ArrayData
    val resultArray = readConverter.jsonListToArray(jsonList, arrayType)

    // Verify ArrayData contents match original
    resultArray.numElements() shouldBe originalArray.length
    (0 until resultArray.numElements()).foreach { i =>
      resultArray.getInt(i) shouldBe originalArray(i)
    }
  }

  test("parseJsonString succeeds for valid JSON") {
    val jsonString = """{"user_id": "123", "action": "click"}"""
    val config = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = false)

    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.parseJsonString(jsonString, config)

    jsonMap.get("user_id") shouldBe "123"
    jsonMap.get("action") shouldBe "click"
  }

  test("parseJsonString handles invalid JSON with failOnInvalidJson=false") {
    val jsonString = "invalid json{"
    val config = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = false)

    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.parseJsonString(jsonString, config)

    jsonMap.containsKey("_raw") shouldBe true
    jsonMap.get("_raw") shouldBe "invalid json{"
  }

  test("parseJsonString throws exception for invalid JSON with failOnInvalidJson=true") {
    val jsonString = "invalid json{"
    val config = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = true)

    val schema = StructType(Seq.empty)
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    an[RuntimeException] should be thrownBy {
      converter.parseJsonString(jsonString, config)
    }
  }
}
