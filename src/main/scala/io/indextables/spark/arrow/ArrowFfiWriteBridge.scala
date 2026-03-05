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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
 * Export-direction Arrow FFI bridge for the write path. Buffers InternalRows into an Arrow VectorSchemaRoot and exports
 * to FFI structs for zero-copy handoff to Rust via tantivy4java.
 *
 * Complex types (Struct, Array, Map) are passed as native Arrow complex vectors (StructVector, ListVector, MapVector).
 * The Rust side converts these directly to tantivy OwnedValue without JSON serialization — see
 * tantivy4java's Arrow-Native Complex Type Indexing feature.
 *
 * ARCHITECTURE & MIGRATION PATH:
 *
 * This bridge currently buffers `InternalRow`s into Arrow vectors because Spark V2 `DataWriter[T]` restricts T to
 * `InternalRow` (confirmed in Spark 3.5, 4.0, and 4.1-preview). The data flow is:
 *
 * {{{
 *   InternalRow → bufferRow() → VectorSchemaRoot → exportBatch() → FFI (ArrowArray + ArrowSchema) → Rust
 * }}}
 *
 * When Spark adds `DataWriter[ColumnarBatch]` support, this class should be refactored to accept `ColumnarBatch`
 * directly. The `bufferRow()` method would be replaced by `exportColumnarBatch(batch: ColumnarBatch)` which extracts
 * the underlying `ArrowColumnVector`/`FieldVector` instances and exports them via `Data.exportVector()` — zero-copy, no
 * row-level buffering needed. See Comet's `NativeUtil.exportBatch()` pattern at
 * `datafusion-comet/common/src/main/scala/org/apache/comet/vector/NativeUtil.scala:111` for the target architecture.
 *
 * @param sparkSchema
 *   The Spark schema for the data being written
 * @param batchSize
 *   Maximum number of rows to buffer before a flush
 */
class ArrowFfiWriteBridge(
  sparkSchema: StructType,
  batchSize: Int)
    extends AutoCloseable {

  private val logger    = LoggerFactory.getLogger(classOf[ArrowFfiWriteBridge])
  private val allocator = ArrowFfiBridge.allocator

  // Arrow schema derived from Spark schema
  private val arrowSchema: Schema = ArrowFfiWriteBridge.toArrowSchema(sparkSchema)

  // VectorSchemaRoot for buffering rows — allocated once, reset between batches
  private val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)

  // Precomputed field accessors for fast row buffering
  private val fieldCount = sparkSchema.fields.length
  private val sparkTypes = sparkSchema.fields.map(_.dataType)

  // Current buffer position
  private var rowCount: Int = 0

  /**
   * Export the Arrow schema to an FFI ArrowSchema struct.
   *
   * @return
   *   memory address of the ArrowSchema FFI struct. Ownership is transferred to the caller (Rust side).
   */
  def exportSchema(): Long = {
    val ffiSchema = ArrowSchema.allocateNew(allocator)
    try {
      Data.exportSchema(allocator, arrowSchema, null, ffiSchema)
      val addr = ffiSchema.memoryAddress()
      logger.debug(s"Exported Arrow schema with ${sparkSchema.fields.length} fields to FFI address $addr")
      // Do NOT close ffiSchema — ownership transferred to Rust via the address
      addr
    } catch {
      case e: Exception =>
        ffiSchema.close()
        throw e
    }
  }

  /**
   * Buffer one InternalRow into the Arrow VectorSchemaRoot.
   *
   * MIGRATION: This method exists because Spark V2 delivers InternalRow, not ColumnarBatch. When migrating to columnar
   * writes, replace this with direct ColumnarBatch→FFI export via `Data.exportVectorSchemaRoot()`.
   *
   * @param row
   *   The InternalRow to buffer
   * @return
   *   true if the batch is full and should be flushed via exportBatch()
   */
  def bufferRow(row: InternalRow): Boolean = {
    val rowIdx = rowCount
    var i      = 0
    while (i < fieldCount) {
      if (row.isNullAt(i)) {
        // Arrow vectors track nulls via validity bitmap — just skip setting the value
        // The vector's null count is updated when setValueCount is called
      } else {
        setArrowValue(root.getVector(i), rowIdx, row, i, sparkTypes(i))
      }
      i += 1
    }
    rowCount += 1
    rowCount >= batchSize
  }

  /**
   * Export the buffered rows as FFI structs for handoff to Rust.
   *
   * This method's FFI export logic (Data.exportVectorSchemaRoot → address pair) is reusable regardless of whether the
   * input is buffered rows or a native ColumnarBatch — only the upstream data source changes. When migrating to
   * columnar writes, this same export pattern applies to ColumnarBatch's underlying VectorSchemaRoot.
   *
   * @return
   *   (arrayAddr, schemaAddr) — memory addresses of the FFI structs. Ownership is transferred to Rust.
   */
  def exportBatch(): (Long, Long) = {
    if (rowCount == 0) {
      throw new IllegalStateException("Cannot export empty batch")
    }

    // Set the value count on each vector (finalizes nulls bitmap)
    root.getFieldVectors.asScala.foreach(_.setValueCount(rowCount))
    root.setRowCount(rowCount)

    val ffiArray  = ArrowArray.allocateNew(allocator)
    val ffiSchema = ArrowSchema.allocateNew(allocator)
    try {
      Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema)
      val arrayAddr  = ffiArray.memoryAddress()
      val schemaAddr = ffiSchema.memoryAddress()

      logger.debug(s"Exported batch of $rowCount rows to FFI (array=$arrayAddr, schema=$schemaAddr)")

      // Reset for next batch — allocate fresh vectors since export consumed the buffers
      resetBuffer()

      // Do NOT close ffiArray/ffiSchema — ownership transferred to Rust
      (arrayAddr, schemaAddr)
    } catch {
      case e: Exception =>
        ffiArray.close()
        ffiSchema.close()
        throw e
    }
  }

  /** Returns the number of rows currently buffered. */
  def bufferedRowCount: Int = rowCount

  /** Returns true if there are buffered rows that haven't been exported. */
  def hasBufferedRows: Boolean = rowCount > 0

  override def close(): Unit =
    try
      root.close()
    catch {
      case e: Exception =>
        logger.warn("Error closing ArrowFfiWriteBridge VectorSchemaRoot", e)
    }

  // ---- Private helpers ----

  /** Reset the buffer for the next batch. After export, the VectorSchemaRoot buffers are consumed. */
  private def resetBuffer(): Unit = {
    root.allocateNew()
    rowCount = 0
  }

  /** Set a value in an Arrow vector from an InternalRow field. Uses type-specific getters for performance. */
  private def setArrowValue(
    vector: FieldVector,
    rowIdx: Int,
    row: InternalRow,
    colIdx: Int,
    dataType: DataType
  ): Unit =
    dataType match {
      case StringType =>
        val utf8  = row.getUTF8String(colIdx)
        val bytes = utf8.getBytes
        vector.asInstanceOf[VarCharVector].setSafe(rowIdx, bytes, 0, bytes.length)

      case IntegerType =>
        vector.asInstanceOf[BigIntVector].setSafe(rowIdx, row.getInt(colIdx).toLong)

      case LongType =>
        vector.asInstanceOf[BigIntVector].setSafe(rowIdx, row.getLong(colIdx))

      case FloatType =>
        vector.asInstanceOf[Float8Vector].setSafe(rowIdx, row.getFloat(colIdx).toDouble)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].setSafe(rowIdx, row.getDouble(colIdx))

      case BooleanType =>
        val bit = if (row.getBoolean(colIdx)) 1 else 0
        vector.asInstanceOf[BitVector].setSafe(rowIdx, bit)

      case TimestampType =>
        vector.asInstanceOf[TimeStampMicroTZVector].setSafe(rowIdx, row.getLong(colIdx))

      case DateType =>
        vector.asInstanceOf[DateDayVector].setSafe(rowIdx, row.getInt(colIdx))

      case BinaryType =>
        val bytes = row.getBinary(colIdx)
        vector.asInstanceOf[VarBinaryVector].setSafe(rowIdx, bytes, 0, bytes.length)

      case st: StructType =>
        writeStruct(vector.asInstanceOf[StructVector], rowIdx, row.getStruct(colIdx, st.fields.length), st)

      case at: ArrayType =>
        writeList(vector.asInstanceOf[ListVector], rowIdx, row.getArray(colIdx), at)

      case mt: MapType =>
        writeMap(vector.asInstanceOf[MapVector], rowIdx, row.getMap(colIdx), mt)

      case other =>
        val str   = row.get(colIdx, other).toString
        val bytes = str.getBytes(StandardCharsets.UTF_8)
        vector.asInstanceOf[VarCharVector].setSafe(rowIdx, bytes, 0, bytes.length)
    }

  /** Write a Spark InternalRow (struct value) into an Arrow StructVector at the given row index. */
  private def writeStruct(vector: StructVector, rowIdx: Int, struct: InternalRow, st: StructType): Unit = {
    var i = 0
    while (i < st.fields.length) {
      if (!struct.isNullAt(i)) {
        val field = st.fields(i)
        writeValueGeneric(
          vector.getChild(field.name).asInstanceOf[FieldVector],
          rowIdx,
          struct.get(i, field.dataType),
          field.dataType
        )
      }
      i += 1
    }
    vector.setIndexDefined(rowIdx)
  }

  /** Write a Spark ArrayData into an Arrow ListVector at the given row index. */
  private def writeList(vector: ListVector, rowIdx: Int, array: ArrayData, at: ArrayType): Unit = {
    val numElements = array.numElements()
    val startOffset = vector.startNewValue(rowIdx)
    val dataVector  = vector.getDataVector.asInstanceOf[FieldVector]
    var i = 0
    while (i < numElements) {
      if (!array.isNullAt(i)) {
        writeValueGeneric(dataVector, startOffset + i, array.get(i, at.elementType), at.elementType)
      }
      i += 1
    }
    vector.endValue(rowIdx, numElements)
  }

  /** Write a Spark MapData into an Arrow MapVector at the given row index. */
  private def writeMap(vector: MapVector, rowIdx: Int, mapData: MapData, mt: MapType): Unit = {
    val keys       = mapData.keyArray()
    val values     = mapData.valueArray()
    val numEntries = keys.numElements()

    val startOffset    = vector.startNewValue(rowIdx)
    val entriesVector  = vector.getDataVector.asInstanceOf[StructVector]
    val keyVector      = entriesVector.getChild("key").asInstanceOf[FieldVector]
    val valueVector    = entriesVector.getChild("value").asInstanceOf[FieldVector]

    var i = 0
    while (i < numEntries) {
      writeValueGeneric(keyVector, startOffset + i, keys.get(i, mt.keyType), mt.keyType)
      if (!values.isNullAt(i)) {
        writeValueGeneric(valueVector, startOffset + i, values.get(i, mt.valueType), mt.valueType)
      }
      entriesVector.setIndexDefined(startOffset + i)
      i += 1
    }
    vector.endValue(rowIdx, numEntries)
  }

  /**
   * Write a generic Spark internal-format value into an Arrow vector. Used recursively inside complex types where we
   * have already extracted the value via `InternalRow.get(i, dataType)` or `ArrayData.get(i, dataType)`.
   */
  private def writeValueGeneric(vector: FieldVector, rowIdx: Int, value: Any, dataType: DataType): Unit = {
    if (value == null) return
    dataType match {
      case StringType =>
        val bytes = value.asInstanceOf[UTF8String].getBytes
        vector.asInstanceOf[VarCharVector].setSafe(rowIdx, bytes, 0, bytes.length)

      case IntegerType =>
        vector.asInstanceOf[BigIntVector].setSafe(rowIdx, value.asInstanceOf[Int].toLong)

      case LongType =>
        vector.asInstanceOf[BigIntVector].setSafe(rowIdx, value.asInstanceOf[Long])

      case FloatType =>
        vector.asInstanceOf[Float8Vector].setSafe(rowIdx, value.asInstanceOf[Float].toDouble)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].setSafe(rowIdx, value.asInstanceOf[Double])

      case BooleanType =>
        val bit = if (value.asInstanceOf[Boolean]) 1 else 0
        vector.asInstanceOf[BitVector].setSafe(rowIdx, bit)

      case TimestampType =>
        vector.asInstanceOf[TimeStampMicroTZVector].setSafe(rowIdx, value.asInstanceOf[Long])

      case DateType =>
        vector.asInstanceOf[DateDayVector].setSafe(rowIdx, value.asInstanceOf[Int])

      case BinaryType =>
        val bytes = value.asInstanceOf[Array[Byte]]
        vector.asInstanceOf[VarBinaryVector].setSafe(rowIdx, bytes, 0, bytes.length)

      case st: StructType =>
        writeStruct(vector.asInstanceOf[StructVector], rowIdx, value.asInstanceOf[InternalRow], st)

      case at: ArrayType =>
        writeList(vector.asInstanceOf[ListVector], rowIdx, value.asInstanceOf[ArrayData], at)

      case mt: MapType =>
        writeMap(vector.asInstanceOf[MapVector], rowIdx, value.asInstanceOf[MapData], mt)

      case _ =>
        val str   = value.toString
        val bytes = str.getBytes(StandardCharsets.UTF_8)
        vector.asInstanceOf[VarCharVector].setSafe(rowIdx, bytes, 0, bytes.length)
    }
  }
}

object ArrowFfiWriteBridge {

  /**
   * Convert a Spark StructType to an Arrow Schema.
   *
   * Primitive type mapping follows tantivy4java's expected Arrow types:
   *   - StringType → Utf8 (VarCharVector)
   *   - IntegerType/LongType → Int64 (BigIntVector) — tantivy uses i64 for all integers
   *   - FloatType/DoubleType → Float64 (Float8Vector) — tantivy uses f64 for all floats
   *   - BooleanType → Bool (BitVector)
   *   - TimestampType → Timestamp(MICROSECOND, "UTC") (TimeStampMicroTZVector)
   *   - DateType → Date(DAY) (DateDayVector)
   *   - BinaryType → Binary (VarBinaryVector)
   *
   * Complex types are passed as native Arrow structures for direct Rust conversion:
   *   - StructType → Arrow Struct (StructVector) with recursive child fields
   *   - ArrayType → Arrow List (ListVector) with typed element child
   *   - MapType → Arrow Map (MapVector) with key/value entry struct child
   */
  def toArrowSchema(sparkSchema: StructType): Schema = {
    val fields = sparkSchema.fields.map { field =>
      sparkTypeToArrowField(field.name, field.dataType, field.nullable)
    }
    new Schema(fields.toList.asJava)
  }

  /** Convert a Spark field to an Arrow Field, recursively handling complex types. */
  def sparkTypeToArrowField(name: String, dataType: DataType, nullable: Boolean): Field =
    dataType match {
      case st: StructType =>
        val children = st.fields.map(f => sparkTypeToArrowField(f.name, f.dataType, f.nullable))
        new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), children.toList.asJava)

      case at: ArrayType =>
        val elementField = sparkTypeToArrowField("item", at.elementType, at.containsNull)
        new Field(
          name,
          new FieldType(nullable, ArrowType.List.INSTANCE, null),
          java.util.Collections.singletonList(elementField)
        )

      case mt: MapType =>
        val keyField     = sparkTypeToArrowField("key", mt.keyType, nullable = false)
        val valueField   = sparkTypeToArrowField("value", mt.valueType, mt.valueContainsNull)
        val entriesField = new Field(
          "entries",
          new FieldType(false, ArrowType.Struct.INSTANCE, null),
          java.util.Arrays.asList(keyField, valueField)
        )
        new Field(
          name,
          new FieldType(nullable, new ArrowType.Map(false), null),
          java.util.Collections.singletonList(entriesField)
        )

      case _ =>
        new Field(name, new FieldType(nullable, sparkTypeToArrowType(dataType), null), null)
    }

  private def sparkTypeToArrowType(dataType: DataType): ArrowType =
    dataType match {
      case StringType             => ArrowType.Utf8.INSTANCE
      case IntegerType | LongType => new ArrowType.Int(64, true)
      case FloatType | DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case BooleanType            => ArrowType.Bool.INSTANCE
      case TimestampType          => new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
      case DateType               => new ArrowType.Date(DateUnit.DAY)
      case BinaryType             => ArrowType.Binary.INSTANCE
      case _                      => ArrowType.Utf8.INSTANCE // Fallback
    }
}
