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

package io.indextables.spark.arrow

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrowFfiWriteBridgeTest extends AnyFunSuite with Matchers {

  // ===== Schema conversion tests: primitive types =====

  test("toArrowSchema maps StringType to Utf8") {
    val schema      = new StructType().add("name", StringType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    arrowSchema.getFields.size() shouldBe 1
    arrowSchema.getFields.get(0).getType shouldBe ArrowType.Utf8.INSTANCE
  }

  test("toArrowSchema maps IntegerType to Int64") {
    val schema      = new StructType().add("id", IntegerType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.Int]
    arrowType.getBitWidth shouldBe 64
    arrowType.getIsSigned shouldBe true
  }

  test("toArrowSchema maps LongType to Int64") {
    val schema      = new StructType().add("id", LongType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.Int]
    arrowType.getBitWidth shouldBe 64
    arrowType.getIsSigned shouldBe true
  }

  test("toArrowSchema maps FloatType to Float64") {
    val schema      = new StructType().add("score", FloatType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.FloatingPoint]
    arrowType.getPrecision shouldBe FloatingPointPrecision.DOUBLE
  }

  test("toArrowSchema maps DoubleType to Float64") {
    val schema      = new StructType().add("score", DoubleType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.FloatingPoint]
    arrowType.getPrecision shouldBe FloatingPointPrecision.DOUBLE
  }

  test("toArrowSchema maps BooleanType to Bool") {
    val schema      = new StructType().add("active", BooleanType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    arrowSchema.getFields.get(0).getType shouldBe ArrowType.Bool.INSTANCE
  }

  test("toArrowSchema maps TimestampType to Timestamp(MICROSECOND, UTC)") {
    val schema      = new StructType().add("ts", TimestampType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.Timestamp]
    arrowType.getUnit shouldBe TimeUnit.MICROSECOND
    arrowType.getTimezone shouldBe "UTC"
  }

  test("toArrowSchema maps DateType to Date(DAY)") {
    val schema      = new StructType().add("dt", DateType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val arrowType   = arrowSchema.getFields.get(0).getType.asInstanceOf[ArrowType.Date]
    arrowType.getUnit shouldBe DateUnit.DAY
  }

  test("toArrowSchema maps BinaryType to Binary") {
    val schema      = new StructType().add("data", BinaryType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    arrowSchema.getFields.get(0).getType shouldBe ArrowType.Binary.INSTANCE
  }

  // ===== Schema conversion tests: complex types (native Arrow) =====

  test("toArrowSchema maps StructType to Arrow Struct with child fields") {
    val innerStruct = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
    val schema      = new StructType().add("obj", innerStruct)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val field       = arrowSchema.getFields.get(0)

    field.getType shouldBe ArrowType.Struct.INSTANCE
    field.getChildren.size() shouldBe 2
    field.getChildren.get(0).getName shouldBe "name"
    field.getChildren.get(0).getType shouldBe ArrowType.Utf8.INSTANCE
    field.getChildren.get(1).getName shouldBe "age"
    field.getChildren.get(1).getType.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 64
  }

  test("toArrowSchema maps ArrayType to Arrow List with element child") {
    val schema      = new StructType().add("tags", ArrayType(StringType, containsNull = true))
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val field       = arrowSchema.getFields.get(0)

    field.getType shouldBe ArrowType.List.INSTANCE
    field.getChildren.size() shouldBe 1
    field.getChildren.get(0).getName shouldBe "item"
    field.getChildren.get(0).getType shouldBe ArrowType.Utf8.INSTANCE
    field.getChildren.get(0).isNullable shouldBe true
  }

  test("toArrowSchema maps MapType to Arrow Map with key/value entries") {
    val schema      = new StructType().add("attrs", MapType(StringType, LongType, valueContainsNull = true))
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val field       = arrowSchema.getFields.get(0)

    field.getType shouldBe a[ArrowType.Map]
    field.getChildren.size() shouldBe 1

    val entries = field.getChildren.get(0)
    entries.getName shouldBe "entries"
    entries.getType shouldBe ArrowType.Struct.INSTANCE
    entries.getChildren.size() shouldBe 2
    entries.getChildren.get(0).getName shouldBe "key"
    entries.getChildren.get(0).getType shouldBe ArrowType.Utf8.INSTANCE
    entries.getChildren.get(0).isNullable shouldBe false
    entries.getChildren.get(1).getName shouldBe "value"
    entries.getChildren.get(1).getType.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 64
    entries.getChildren.get(1).isNullable shouldBe true
  }

  test("toArrowSchema handles nested complex types (List of Structs)") {
    val innerStruct = new StructType()
      .add("x", IntegerType)
      .add("label", StringType)
    val schema      = new StructType().add("items", ArrayType(innerStruct))
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    val field       = arrowSchema.getFields.get(0)

    field.getType shouldBe ArrowType.List.INSTANCE
    val itemField = field.getChildren.get(0)
    itemField.getType shouldBe ArrowType.Struct.INSTANCE
    itemField.getChildren.size() shouldBe 2
    itemField.getChildren.get(0).getName shouldBe "x"
    itemField.getChildren.get(1).getName shouldBe "label"
  }

  test("toArrowSchema handles mixed schema correctly") {
    val schema = new StructType()
      .add("name", StringType)
      .add("id", LongType)
      .add("score", DoubleType)
      .add("active", BooleanType)
      .add("ts", TimestampType)
    val arrowSchema = ArrowFfiWriteBridge.toArrowSchema(schema)
    arrowSchema.getFields.size() shouldBe 5
  }

  // ===== Row buffering tests: primitives =====

  test("bufferRow returns false when batch not full") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 10)
    try {
      val row = new GenericInternalRow(Array[Any](UTF8String.fromString("hello")))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
      bridge.hasBufferedRows shouldBe true
    } finally
      bridge.close()
  }

  test("bufferRow returns true when batch full") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 3)
    try {
      val row = new GenericInternalRow(Array[Any](UTF8String.fromString("hello")))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferRow(row) shouldBe false
      bridge.bufferRow(row) shouldBe true // 3rd row fills the batch
      bridge.bufferedRowCount shouldBe 3
    } finally
      bridge.close()
  }

  test("bufferRow handles all primitive types") {
    val schema = new StructType()
      .add("str", StringType)
      .add("int_val", IntegerType)
      .add("long_val", LongType)
      .add("float_val", FloatType)
      .add("double_val", DoubleType)
      .add("bool_val", BooleanType)
      .add("ts", TimestampType)
      .add("dt", DateType)
      .add("bin", BinaryType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row = new GenericInternalRow(
        Array[Any](
          UTF8String.fromString("test"),
          42,                  // IntegerType
          100L,                // LongType
          3.14f,               // FloatType
          2.718,               // DoubleType
          true,                // BooleanType
          1000000L,            // TimestampType (micros since epoch)
          19000,               // DateType (days since epoch)
          Array[Byte](1, 2, 3) // BinaryType
        )
      )
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles null values") {
    val schema = new StructType()
      .add("name", StringType, nullable = true)
      .add("id", LongType, nullable = true)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row = new GenericInternalRow(Array[Any](null, null))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  // ===== Row buffering tests: complex types =====

  test("bufferRow handles StructType natively") {
    val innerStruct = new StructType()
      .add("name", StringType)
      .add("age", LongType)
    val schema = new StructType().add("obj", innerStruct)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val innerRow = new GenericInternalRow(Array[Any](UTF8String.fromString("Alice"), 30L))
      val row      = new GenericInternalRow(Array[Any](innerRow))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles ArrayType natively") {
    val schema = new StructType().add("tags", ArrayType(StringType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val arrayData = new GenericArrayData(
        Array(UTF8String.fromString("a"), UTF8String.fromString("b"), UTF8String.fromString("c"))
      )
      val row = new GenericInternalRow(Array[Any](arrayData))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles MapType natively") {
    val schema = new StructType().add("attrs", MapType(StringType, LongType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val mapData = ArrayBasedMapData(
        Array[Any](UTF8String.fromString("x"), UTF8String.fromString("y")),
        Array[Any](1L, 2L)
      )
      val row = new GenericInternalRow(Array[Any](mapData))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles null struct value") {
    val innerStruct = new StructType().add("name", StringType)
    val schema      = new StructType().add("obj", innerStruct, nullable = true)
    val bridge      = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row = new GenericInternalRow(Array[Any](null))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles struct with null fields") {
    val innerStruct = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", LongType, nullable = true)
    val schema = new StructType().add("obj", innerStruct)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val innerRow = new GenericInternalRow(Array[Any](null, 42L))
      val row      = new GenericInternalRow(Array[Any](innerRow))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles nested complex types (List of Structs)") {
    val innerStruct = new StructType()
      .add("x", LongType)
      .add("label", StringType)
    val schema = new StructType().add("items", ArrayType(innerStruct))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val struct1   = new GenericInternalRow(Array[Any](1L, UTF8String.fromString("first")))
      val struct2   = new GenericInternalRow(Array[Any](2L, UTF8String.fromString("second")))
      val arrayData = new GenericArrayData(Array[Any](struct1, struct2))
      val row       = new GenericInternalRow(Array[Any](arrayData))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles multiple rows with varying list sizes") {
    val schema = new StructType().add("nums", ArrayType(LongType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row1 = new GenericInternalRow(Array[Any](new GenericArrayData(Array(1L, 2L, 3L))))
      val row2 = new GenericInternalRow(Array[Any](new GenericArrayData(Array(4L))))
      val row3 = new GenericInternalRow(Array[Any](new GenericArrayData(Array(5L, 6L))))
      bridge.bufferRow(row1) shouldBe false
      bridge.bufferRow(row2) shouldBe false
      bridge.bufferRow(row3) shouldBe false
      bridge.bufferedRowCount shouldBe 3
    } finally
      bridge.close()
  }

  // ===== FFI export tests =====

  test("exportSchema produces non-zero address") {
    val schema = new StructType()
      .add("name", StringType)
      .add("id", LongType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val addr = bridge.exportSchema()
      addr should not be 0L
    } finally
      bridge.close()
  }

  test("exportBatch produces non-zero addresses") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row = new GenericInternalRow(Array[Any](UTF8String.fromString("hello")))
      bridge.bufferRow(row)

      val (arrayAddr, schemaAddr) = bridge.exportBatch()
      arrayAddr should not be 0L
      schemaAddr should not be 0L
    } finally
      bridge.close()
  }

  test("exportBatch throws on empty batch") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try
      an[IllegalStateException] should be thrownBy {
        bridge.exportBatch()
      }
    finally
      bridge.close()
  }

  test("buffer resets after exportBatch") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val row = new GenericInternalRow(Array[Any](UTF8String.fromString("hello")))
      bridge.bufferRow(row)
      bridge.bufferedRowCount shouldBe 1

      bridge.exportBatch()
      bridge.bufferedRowCount shouldBe 0
      bridge.hasBufferedRows shouldBe false
    } finally
      bridge.close()
  }

  test("multiple export cycles work correctly") {
    val schema = new StructType().add("name", StringType)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 2)
    try {
      val row1 = new GenericInternalRow(Array[Any](UTF8String.fromString("first")))
      val row2 = new GenericInternalRow(Array[Any](UTF8String.fromString("second")))
      val row3 = new GenericInternalRow(Array[Any](UTF8String.fromString("third")))

      // First batch
      bridge.bufferRow(row1)
      bridge.bufferRow(row2)
      val (a1, s1) = bridge.exportBatch()
      a1 should not be 0L
      s1 should not be 0L

      // Second batch (reuse)
      bridge.bufferRow(row3)
      bridge.bufferedRowCount shouldBe 1
      val (a2, s2) = bridge.exportBatch()
      a2 should not be 0L
      s2 should not be 0L
    } finally
      bridge.close()
  }

  test("bufferRow handles empty array") {
    val schema = new StructType().add("tags", ArrayType(StringType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val emptyArray = new GenericArrayData(Array.empty[Any])
      val row        = new GenericInternalRow(Array[Any](emptyArray))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles empty map") {
    val schema = new StructType().add("attrs", MapType(StringType, LongType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val emptyMap = ArrayBasedMapData(Array.empty[Any], Array.empty[Any])
      val row      = new GenericInternalRow(Array[Any](emptyMap))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles null elements within array") {
    val schema = new StructType().add("tags", ArrayType(StringType, containsNull = true))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val arrayWithNulls = new GenericArrayData(
        Array(UTF8String.fromString("a"), null, UTF8String.fromString("c"))
      )
      val row = new GenericInternalRow(Array[Any](arrayWithNulls))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles deeply nested structures (Struct of Array of Struct)") {
    val innerStruct = new StructType()
      .add("name", StringType)
      .add("value", LongType)
    val midStruct = new StructType()
      .add("items", ArrayType(innerStruct))
      .add("label", StringType)
    val schema = new StructType().add("outer", midStruct)
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val item1     = new GenericInternalRow(Array[Any](UTF8String.fromString("a"), 1L))
      val item2     = new GenericInternalRow(Array[Any](UTF8String.fromString("b"), 2L))
      val itemArray = new GenericArrayData(Array[Any](item1, item2))
      val midRow    = new GenericInternalRow(Array[Any](itemArray, UTF8String.fromString("test")))
      val row       = new GenericInternalRow(Array[Any](midRow))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("bufferRow handles Map with null values") {
    val schema = new StructType().add("attrs", MapType(StringType, LongType, valueContainsNull = true))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val mapData = ArrayBasedMapData(
        Array[Any](UTF8String.fromString("x"), UTF8String.fromString("y")),
        Array[Any](1L, null)
      )
      val row = new GenericInternalRow(Array[Any](mapData))
      bridge.bufferRow(row) shouldBe false
      bridge.bufferedRowCount shouldBe 1
    } finally
      bridge.close()
  }

  test("exportBatch with complex types produces non-zero addresses") {
    val innerStruct = new StructType()
      .add("name", StringType)
      .add("score", LongType)
    val schema = new StructType()
      .add("data", innerStruct)
      .add("tags", ArrayType(StringType))
      .add("attrs", MapType(StringType, LongType))
    val bridge = new ArrowFfiWriteBridge(schema, batchSize = 100)
    try {
      val structVal = new GenericInternalRow(Array[Any](UTF8String.fromString("Alice"), 95L))
      val arrayVal  = new GenericArrayData(Array(UTF8String.fromString("a"), UTF8String.fromString("b")))
      val mapVal = ArrayBasedMapData(
        Array[Any](UTF8String.fromString("k1")),
        Array[Any](10L)
      )
      val row = new GenericInternalRow(Array[Any](structVal, arrayVal, mapVal))
      bridge.bufferRow(row)

      val (arrayAddr, schemaAddr) = bridge.exportBatch()
      arrayAddr should not be 0L
      schemaAddr should not be 0L
    } finally
      bridge.close()
  }
}
