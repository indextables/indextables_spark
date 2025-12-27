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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.Row

import io.indextables.spark.core.IndexTables4SparkOptions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonDataConversionTest extends AnyFunSuite with Matchers {

  private def createOptions(configMap: Map[String, String] = Map.empty): IndexTables4SparkOptions =
    new IndexTables4SparkOptions(new CaseInsensitiveStringMap(configMap.asJava))

  test("structToJsonMap converts simple struct") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    val row       = Row("Alice", 30)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    jsonMap.get("age") shouldBe 30
  }

  test("structToJsonMap handles null fields") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    val row       = Row("Alice", null)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    // Null values are explicitly added as JSON nulls for proper round-tripping
    jsonMap.containsKey("age") shouldBe true
    jsonMap.get("age") shouldBe null
  }

  test("structToJsonMap converts nested struct") {
    val addressSchema = StructType(
      Seq(
        StructField("city", StringType),
        StructField("zip", StringType)
      )
    )
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("address", addressSchema)
      )
    )

    val row       = Row("Alice", Row("NYC", "10001"))
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.structToJsonMap(row, schema)

    jsonMap.get("name") shouldBe "Alice"
    val addressMap = jsonMap.get("address").asInstanceOf[java.util.Map[String, Object]]
    addressMap.get("city") shouldBe "NYC"
    addressMap.get("zip") shouldBe "10001"
  }

  test("arrayToJsonList converts simple string array") {
    val arrayType = ArrayType(StringType)
    val tags      = Seq("tag1", "tag2", "tag3")

    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(tags, arrayType)

    jsonList.size() shouldBe 3
    jsonList.get(0) shouldBe "tag1"
    jsonList.get(1) shouldBe "tag2"
    jsonList.get(2) shouldBe "tag3"
  }

  test("arrayToJsonList converts numeric array") {
    val arrayType = ArrayType(IntegerType)
    val numbers   = Seq(10, 20, 30)

    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(numbers, arrayType)

    jsonList.size() shouldBe 3
    jsonList.get(0) shouldBe 10
    jsonList.get(1) shouldBe 20
    jsonList.get(2) shouldBe 30
  }

  test("arrayToJsonList handles null elements") {
    val arrayType = ArrayType(StringType)
    val tags      = Seq("tag1", null, "tag3")

    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonList = converter.arrayToJsonList(tags, arrayType)

    // Null elements are filtered out
    jsonList.size() shouldBe 2
    jsonList.get(0) shouldBe "tag1"
    jsonList.get(1) shouldBe "tag3"
  }

  test("convertToJsonValue handles all primitive types") {
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    converter.convertToJsonValue("test", StringType) shouldBe "test"
    converter.convertToJsonValue(42, IntegerType) shouldBe 42
    converter.convertToJsonValue(123L, LongType) shouldBe 123L
    converter.convertToJsonValue(3.14f, FloatType) shouldBe 3.14f
    converter.convertToJsonValue(2.718, DoubleType) shouldBe 2.718
    converter.convertToJsonValue(true, BooleanType) shouldBe true
  }

  test("convertToJsonValue converts DateType to milliseconds") {
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val days   = 100 // 100 days since epoch
    val result = converter.convertToJsonValue(days, DateType)

    result shouldBe (100L * 86400000L) // Days to milliseconds
  }

  test("convertToJsonValue converts TimestampType to microseconds") {
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val schema    = StructType(Seq.empty)
    val converter = new SparkToTantivyConverter(schema, mapper)

    val micros = 1000000L // 1 second in microseconds
    val result = converter.convertToJsonValue(micros, TimestampType)

    result shouldBe 1000000L // Microseconds stored directly (no conversion)
  }

  test("jsonMapToRow converts simple map to Row") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("name", "Alice")
    jsonMap.put("age", Integer.valueOf(30))

    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val row = converter.jsonMapToRow(jsonMap, schema)

    row.getString(0) shouldBe "Alice"
    row.getInt(1) shouldBe 30
  }

  test("jsonMapToRow handles null values") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("name", "Alice")
    // age is null

    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
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

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val result = converter.jsonListToArray(jsonList, arrayType)

    result.numElements() shouldBe 3
    result.getUTF8String(0).toString shouldBe "tag1"
    result.getUTF8String(1).toString shouldBe "tag2"
    result.getUTF8String(2).toString shouldBe "tag3"
  }

  test("convertFromJsonValue handles all primitive types") {
    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
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
    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val millis = 100L * 86400000L // 100 days in milliseconds
    val result = converter.convertFromJsonValue(java.lang.Long.valueOf(millis), DateType)

    result shouldBe 100 // Days since epoch
  }

  test("convertFromJsonValue converts milliseconds to TimestampType") {
    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val millis = 1000L // 1 second in milliseconds
    val result = converter.convertFromJsonValue(java.lang.Long.valueOf(millis), TimestampType)

    result shouldBe 1000000L // Microseconds
  }

  test("round-trip conversion preserves struct data") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("score", DoubleType)
      )
    )

    val originalRow = Row("Alice", 30, 95.5)

    val mapper         = new SparkSchemaToTantivyMapper(createOptions())
    val writeConverter = new SparkToTantivyConverter(schema, mapper)
    val readConverter  = new TantivyToSparkConverter(schema, mapper)

    // Convert to JSON map
    val jsonMap = writeConverter.structToJsonMap(originalRow, schema)

    // Convert back to Row
    val resultRow = readConverter.jsonMapToRow(jsonMap, schema)

    resultRow.getString(0) shouldBe "Alice"
    resultRow.getInt(1) shouldBe 30
    resultRow.getDouble(2) shouldBe 95.5
  }

  test("round-trip conversion preserves array data") {
    val arrayType     = ArrayType(IntegerType)
    val originalArray = Seq(10, 20, 30, 40, 50)

    val schema         = StructType(Seq.empty)
    val mapper         = new SparkSchemaToTantivyMapper(createOptions())
    val writeConverter = new SparkToTantivyConverter(schema, mapper)
    val readConverter  = new TantivyToSparkConverter(schema, mapper)

    // Convert to JSON list
    val jsonList = writeConverter.arrayToJsonList(originalArray, arrayType)

    // Convert back to ArrayData
    val resultArray = readConverter.jsonListToArray(jsonList, arrayType)

    // Verify ArrayData contents match original
    resultArray.numElements() shouldBe originalArray.length
    (0 until resultArray.numElements()).foreach(i => resultArray.getInt(i) shouldBe originalArray(i))
  }

  test("parseJsonString succeeds for valid JSON") {
    val jsonString = """{"user_id": "123", "action": "click"}"""
    val config     = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = false)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.parseJsonString(jsonString, config)

    jsonMap.get("user_id") shouldBe "123"
    jsonMap.get("action") shouldBe "click"
  }

  test("parseJsonString handles invalid JSON with failOnInvalidJson=false") {
    val jsonString = "invalid json{"
    val config     = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = false)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.parseJsonString(jsonString, config)

    jsonMap.containsKey("_raw") shouldBe true
    jsonMap.get("_raw") shouldBe "invalid json{"
  }

  test("parseJsonString throws exception for invalid JSON with failOnInvalidJson=true") {
    val jsonString = "invalid json{"
    val config     = JsonFieldConfig(parseOnWrite = true, failOnInvalidJson = true)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    an[RuntimeException] should be thrownBy {
      converter.parseJsonString(jsonString, config)
    }
  }

  // MapType conversion tests

  test("mapToJsonMap converts simple string map") {
    val mapType  = MapType(StringType, StringType)
    val sparkMap = Map("color" -> "red", "size" -> "large", "category" -> "clothing")

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.mapToJsonMap(sparkMap, mapType)

    jsonMap.get("color") shouldBe "red"
    jsonMap.get("size") shouldBe "large"
    jsonMap.get("category") shouldBe "clothing"
    jsonMap.size() shouldBe 3
  }

  test("mapToJsonMap converts integer key-value map") {
    val mapType  = MapType(IntegerType, IntegerType)
    val sparkMap = Map(1 -> 100, 2 -> 200, 3 -> 300)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.mapToJsonMap(sparkMap, mapType)

    // Keys are converted to strings in JSON
    jsonMap.get("1") shouldBe 100
    jsonMap.get("2") shouldBe 200
    jsonMap.get("3") shouldBe 300
    jsonMap.size() shouldBe 3
  }

  test("mapToJsonMap handles mixed-type map") {
    val mapType  = MapType(StringType, IntegerType)
    val sparkMap = Map("count" -> 42, "total" -> 1000, "errors" -> 0)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.mapToJsonMap(sparkMap, mapType)

    jsonMap.get("count") shouldBe 42
    jsonMap.get("total") shouldBe 1000
    jsonMap.get("errors") shouldBe 0
    jsonMap.size() shouldBe 3
  }

  test("mapToJsonMap handles nested struct values") {
    val valueSchema = StructType(
      Seq(
        StructField("city", StringType),
        StructField("zip", StringType)
      )
    )
    val mapType = MapType(StringType, valueSchema)
    val sparkMap = Map(
      "home" -> Row("NYC", "10001"),
      "work" -> Row("SF", "94102")
    )

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.mapToJsonMap(sparkMap, mapType)

    val homeAddr = jsonMap.get("home").asInstanceOf[java.util.Map[String, Object]]
    homeAddr.get("city") shouldBe "NYC"
    homeAddr.get("zip") shouldBe "10001"

    val workAddr = jsonMap.get("work").asInstanceOf[java.util.Map[String, Object]]
    workAddr.get("city") shouldBe "SF"
    workAddr.get("zip") shouldBe "94102"
  }

  test("mapToJsonMap handles null values") {
    val mapType  = MapType(StringType, IntegerType)
    val sparkMap = scala.collection.Map[String, Any]("count" -> 42, "total" -> null)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonMap = converter.mapToJsonMap(sparkMap, mapType)

    jsonMap.get("count") shouldBe 42
    // Null values are filtered out during conversion
    jsonMap.size() shouldBe 1
  }

  test("jsonMapToMapData converts simple string map") {
    val mapType = MapType(StringType, StringType)

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("color", "red")
    jsonMap.put("size", "large")

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val mapData = converter.jsonMapToMapData(jsonMap, mapType)

    mapData.numElements() shouldBe 2
    // Verify map contains expected keys and values
    val keys   = mapData.keyArray().toSeq[Any](mapType.keyType)
    val values = mapData.valueArray().toSeq[Any](mapType.valueType)

    keys.map(_.toString) should contain allOf ("color", "size")
    values.map(_.toString) should contain allOf ("red", "large")
  }

  test("jsonMapToMapData converts integer-valued map") {
    val mapType = MapType(StringType, IntegerType)

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("count", Integer.valueOf(42))
    jsonMap.put("total", Integer.valueOf(1000))

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val mapData = converter.jsonMapToMapData(jsonMap, mapType)

    mapData.numElements() shouldBe 2
    val keys   = mapData.keyArray().toSeq[Any](mapType.keyType)
    val values = mapData.valueArray().toSeq[Any](mapType.valueType)

    keys.map(_.toString) should contain allOf ("count", "total")
    values should contain allOf (42, 1000)
  }

  test("convertToJsonValue handles MapType recursively") {
    val mapType  = MapType(StringType, IntegerType)
    val sparkMap = Map("count" -> 42, "total" -> 1000)

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new SparkToTantivyConverter(schema, mapper)

    val jsonValue = converter.convertToJsonValue(sparkMap, mapType)

    jsonValue.isInstanceOf[java.util.Map[_, _]] shouldBe true
    val jsonMap = jsonValue.asInstanceOf[java.util.Map[String, Object]]
    jsonMap.get("count") shouldBe 42
    jsonMap.get("total") shouldBe 1000
  }

  test("convertFromJsonValue handles MapType recursively") {
    val mapType = MapType(StringType, IntegerType)

    val jsonMap = new java.util.HashMap[String, Object]()
    jsonMap.put("count", Integer.valueOf(42))
    jsonMap.put("total", Integer.valueOf(1000))

    val schema    = StructType(Seq.empty)
    val mapper    = new SparkSchemaToTantivyMapper(createOptions())
    val converter = new TantivyToSparkConverter(schema, mapper)

    val mapData = converter.convertFromJsonValue(jsonMap, mapType)

    mapData.isInstanceOf[org.apache.spark.sql.catalyst.util.MapData] shouldBe true
    mapData.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData].numElements() shouldBe 2
  }

  test("roundtrip Map conversion maintains data integrity") {
    val mapType     = MapType(StringType, StringType)
    val originalMap = Map("color" -> "red", "size" -> "large", "category" -> "clothing")

    val schema         = StructType(Seq.empty)
    val mapper         = new SparkSchemaToTantivyMapper(createOptions())
    val writeConverter = new SparkToTantivyConverter(schema, mapper)
    val readConverter  = new TantivyToSparkConverter(schema, mapper)

    // Convert to JSON map
    val jsonMap = writeConverter.mapToJsonMap(originalMap, mapType)

    // Convert back to MapData
    val mapData = readConverter.jsonMapToMapData(jsonMap, mapType)

    // Verify data integrity
    mapData.numElements() shouldBe 3
    val keys   = mapData.keyArray().toSeq[Any](mapType.keyType)
    val values = mapData.valueArray().toSeq[Any](mapType.valueType)

    keys.map(_.toString) should contain allOf ("color", "size", "category")
    values.map(_.toString) should contain allOf ("red", "large", "clothing")
  }
}
