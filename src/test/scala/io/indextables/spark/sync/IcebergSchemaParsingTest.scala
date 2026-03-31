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

package io.indextables.spark.sync

import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the Iceberg schema parsing methods extracted to IcebergSourceReader companion object.
 * Covers: parseSchemaJson, convertIcebergSchemaToSpark, all primitive/complex type mappings, nullable semantics.
 */
class IcebergSchemaParsingTest extends AnyFunSuite with Matchers {

  // ── parseSchemaJson ────────────────────────────────────────────────────

  test("parseSchemaJson should parse Spark-format schema JSON") {
    val original = StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))
    val result = IcebergSourceReader.parseSchemaJson(original.json)
    result shouldBe original
  }

  test("parseSchemaJson should fall back to Iceberg format when Spark format fails") {
    val icebergJson =
      """{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":false,"type":"long"}]}"""
    val result = IcebergSourceReader.parseSchemaJson(icebergJson)
    result.fields.length shouldBe 1
    result.fields(0).name shouldBe "id"
    result.fields(0).dataType shouldBe LongType
    result.fields(0).nullable shouldBe true
  }

  // ── Iceberg primitive type mappings ────────────────────────────────────

  private def icebergSchemaWithType(fieldName: String, icebergType: String): String =
    s"""{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"$fieldName","required":false,"type":"$icebergType"}]}"""

  test("convertIcebergSchemaToSpark should map boolean to BooleanType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("flag", "boolean"))
    result.fields(0).dataType shouldBe BooleanType
  }

  test("convertIcebergSchemaToSpark should map int and integer to IntegerType") {
    val result1 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "int"))
    result1.fields(0).dataType shouldBe IntegerType

    val result2 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "integer"))
    result2.fields(0).dataType shouldBe IntegerType
  }

  test("convertIcebergSchemaToSpark should map long to LongType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "long"))
    result.fields(0).dataType shouldBe LongType
  }

  test("convertIcebergSchemaToSpark should map float to FloatType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "float"))
    result.fields(0).dataType shouldBe FloatType
  }

  test("convertIcebergSchemaToSpark should map double to DoubleType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "double"))
    result.fields(0).dataType shouldBe DoubleType
  }

  test("convertIcebergSchemaToSpark should map string to StringType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "string"))
    result.fields(0).dataType shouldBe StringType
  }

  test("convertIcebergSchemaToSpark should map binary and fixed to BinaryType") {
    val result1 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "binary"))
    result1.fields(0).dataType shouldBe BinaryType

    val result2 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "fixed"))
    result2.fields(0).dataType shouldBe BinaryType
  }

  test("convertIcebergSchemaToSpark should map fixed[N] to BinaryType") {
    val result1 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("uuid_col", "fixed[16]"))
    result1.fields(0).dataType shouldBe BinaryType

    val result2 = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("hash_col", "fixed[32]"))
    result2.fields(0).dataType shouldBe BinaryType
  }

  test("convertIcebergSchemaToSpark should map fixed[1] to BinaryType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("flag_col", "fixed[1]"))
    result.fields(0).dataType shouldBe BinaryType
  }

  test("convertIcebergSchemaToSpark should map date to DateType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "date"))
    result.fields(0).dataType shouldBe DateType
  }

  test("convertIcebergSchemaToSpark should map timestamp variants to TimestampType") {
    Seq("timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns").foreach { ts =>
      val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", ts))
      result.fields(0).dataType shouldBe TimestampType
    }
  }

  test("convertIcebergSchemaToSpark should map time to LongType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "time"))
    result.fields(0).dataType shouldBe LongType
  }

  test("convertIcebergSchemaToSpark should map uuid to StringType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "uuid"))
    result.fields(0).dataType shouldBe StringType
  }

  test("convertIcebergSchemaToSpark should map decimal with precision and scale") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "decimal(18,6)"))
    result.fields(0).dataType shouldBe DecimalType(18, 6)
  }

  test("convertIcebergSchemaToSpark should default unknown primitive type to StringType") {
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(icebergSchemaWithType("x", "foobar"))
    result.fields(0).dataType shouldBe StringType
  }

  // ── Iceberg complex type mappings ──────────────────────────────────────

  test("convertIcebergSchemaToSpark should handle nested struct type") {
    val json =
      """{
        |  "schema-id": 0, "type": "struct",
        |  "fields": [{
        |    "id": 1, "name": "address", "required": false,
        |    "type": {
        |      "type": "struct",
        |      "fields": [
        |        {"id": 2, "name": "city", "required": false, "type": "string"},
        |        {"id": 3, "name": "zip", "required": true, "type": "int"}
        |      ]
        |    }
        |  }]
        |}""".stripMargin
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    result.fields.length shouldBe 1
    val nested = result.fields(0).dataType.asInstanceOf[StructType]
    nested.fields.length shouldBe 2
    nested.fields(0).name shouldBe "city"
    nested.fields(0).dataType shouldBe StringType
    nested.fields(1).name shouldBe "zip"
    nested.fields(1).dataType shouldBe IntegerType
    nested.fields(1).nullable shouldBe false
  }

  test("convertIcebergSchemaToSpark should handle list type") {
    val json =
      """{
        |  "schema-id": 0, "type": "struct",
        |  "fields": [{
        |    "id": 1, "name": "tags", "required": false,
        |    "type": {
        |      "type": "list",
        |      "element-id": 2,
        |      "element": "string",
        |      "element-required": false
        |    }
        |  }]
        |}""".stripMargin
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    val arrType = result.fields(0).dataType.asInstanceOf[ArrayType]
    arrType.elementType shouldBe StringType
    arrType.containsNull shouldBe true
  }

  test("convertIcebergSchemaToSpark should handle map type") {
    val json =
      """{
        |  "schema-id": 0, "type": "struct",
        |  "fields": [{
        |    "id": 1, "name": "props", "required": false,
        |    "type": {
        |      "type": "map",
        |      "key-id": 2,
        |      "key": "string",
        |      "value-id": 3,
        |      "value": "long",
        |      "value-required": false
        |    }
        |  }]
        |}""".stripMargin
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    val mapType = result.fields(0).dataType.asInstanceOf[MapType]
    mapType.keyType shouldBe StringType
    mapType.valueType shouldBe LongType
    mapType.valueContainsNull shouldBe true
  }

  test("convertIcebergSchemaToSpark should default unknown complex type to StringType") {
    val json =
      """{
        |  "schema-id": 0, "type": "struct",
        |  "fields": [{
        |    "id": 1, "name": "x", "required": false,
        |    "type": {"type": "graph", "nodes": 5}
        |  }]
        |}""".stripMargin
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    result.fields(0).dataType shouldBe StringType
  }

  // ── Nullable semantics ─────────────────────────────────────────────────

  test("convertIcebergSchemaToSpark should set nullable=false when required=true") {
    val json =
      """{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}"""
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    result.fields(0).nullable shouldBe false
  }

  test("convertIcebergSchemaToSpark should set nullable=true when required=false") {
    val json =
      """{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":false,"type":"long"}]}"""
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    result.fields(0).nullable shouldBe true
  }

  test("convertIcebergSchemaToSpark should handle schema with multiple fields of mixed nullability") {
    val json =
      """{
        |  "schema-id":0,"type":"struct",
        |  "fields":[
        |    {"id":1,"name":"id","required":true,"type":"long"},
        |    {"id":2,"name":"name","required":false,"type":"string"},
        |    {"id":3,"name":"score","required":true,"type":"double"}
        |  ]
        |}""".stripMargin
    val result = IcebergSourceReader.convertIcebergSchemaToSpark(json)
    result.fields.length shouldBe 3
    result.fields(0).nullable shouldBe false
    result.fields(1).nullable shouldBe true
    result.fields(2).nullable shouldBe false
  }
}
