package com.tantivy4spark.search

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, ArrayBasedMapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.mutable

object RowConverter {

  def internalRowToJson(row: InternalRow, schema: StructType, mapper: ObjectMapper): String = {
    val map = mutable.Map[String, Any]()
    
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      if (!row.isNullAt(index)) {
        val value = extractValue(row, index, field.dataType)
        map(field.name) = value
      }
    }
    
    mapper.writeValueAsString(map.toMap)
  }

  def jsonToInternalRow(json: Map[String, Any], schema: StructType): InternalRow = {
    val values = schema.fields.map { field =>
      json.get(field.name) match {
        case Some(value) => convertToInternalValue(value, field.dataType)
        case None => null
      }
    }
    InternalRow.fromSeq(values)
  }

  private def extractValue(row: InternalRow, index: Int, dataType: DataType): Any = dataType match {
    case StringType => row.getUTF8String(index).toString
    case IntegerType => row.getInt(index)
    case LongType => row.getLong(index)
    case FloatType => row.getFloat(index)
    case DoubleType => row.getDouble(index)
    case BooleanType => row.getBoolean(index)
    case BinaryType => row.getBinary(index)
    case TimestampType => row.getLong(index) // Assuming microseconds since epoch
    case DateType => row.getInt(index) // Days since epoch
    case ArrayType(elementType, _) =>
      val arrayData = row.getArray(index)
      (0 until arrayData.numElements()).map { i =>
        if (arrayData.isNullAt(i)) null
        else extractArrayElement(arrayData, i, elementType)
      }.toArray
    case MapType(keyType, valueType, _) =>
      val mapData = row.getMap(index)
      val keyArray = mapData.keyArray()
      val valueArray = mapData.valueArray()
      (0 until mapData.numElements()).map { i =>
        val key = extractArrayElement(keyArray, i, keyType)
        val value = if (valueArray.isNullAt(i)) null else extractArrayElement(valueArray, i, valueType)
        key -> value
      }.toMap
    case StructType(fields) =>
      val struct = row.getStruct(index, fields.length)
      fields.zipWithIndex.map { case (field, i) =>
        field.name -> (if (struct.isNullAt(i)) null else extractValue(struct, i, field.dataType))
      }.toMap
    case _ => row.get(index, dataType)
  }

  private def extractArrayElement(arrayData: ArrayData, index: Int, dataType: DataType): Any = dataType match {
    case StringType => arrayData.getUTF8String(index).toString
    case IntegerType => arrayData.getInt(index)
    case LongType => arrayData.getLong(index)
    case FloatType => arrayData.getFloat(index)
    case DoubleType => arrayData.getDouble(index)
    case BooleanType => arrayData.getBoolean(index)
    case _ => arrayData.get(index, dataType)
  }

  private def convertToInternalValue(value: Any, dataType: DataType): Any = (value, dataType) match {
    case (s: String, StringType) => UTF8String.fromString(s)
    case (i: Int, IntegerType) => i
    case (l: Long, LongType) => l
    case (i: Int, LongType) => i.toLong  // Handle Integer -> Long conversion
    case (f: Float, FloatType) => f
    case (d: Double, DoubleType) => d
    case (i: Int, DoubleType) => i.toDouble  // Handle Integer -> Double conversion
    case (b: Boolean, BooleanType) => b
    case (i: Int, BooleanType) => i != 0  // Handle Integer (0/1) -> Boolean conversion
    case (bytes: Array[Byte], BinaryType) => bytes
    case (timestamp: Long, TimestampType) => timestamp
    case (timestamp: Int, TimestampType) => timestamp.toLong  // Handle Integer -> Long for timestamp
    case (date: Int, DateType) => date
    case (date: Long, DateType) => date.toInt  // Handle Long -> Int for date
    case (arr: Seq[_], ArrayType(elementType, _)) =>
      ArrayData.toArrayData(arr.map(convertToInternalValue(_, elementType)).toArray)
    case (map: Map[_, _], MapType(keyType, valueType, _)) =>
      val keys = map.keys.map(convertToInternalValue(_, keyType)).toArray
      val values = map.values.map(convertToInternalValue(_, valueType)).toArray
      ArrayBasedMapData(keys, values)
    case (struct: Map[String, _] @unchecked, StructType(fields)) =>
      val values = fields.map { field =>
        struct.get(field.name) match {
          case Some(v) => convertToInternalValue(v, field.dataType)
          case None => null
        }
      }
      InternalRow.fromSeq(values)
    case (null, _) => null
    case (v, _) => v // Fallback for unsupported types
  }
}