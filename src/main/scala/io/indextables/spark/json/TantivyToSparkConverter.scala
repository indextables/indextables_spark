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

import org.slf4j.LoggerFactory

/**
 * Converts tantivy4java JSON data back to Spark Row format.
 *
 * Handles conversion of:
 *   - Java Map[String, Object] → Spark Row (StructType)
 *   - Java List[Object] → Spark Seq (ArrayType)
 *   - Java Map[String, Object] → Spark Map (MapType)
 *   - JSON objects → JSON strings (for StringType with "json" config)
 *   - Recursive nested structures
 */
class TantivyToSparkConverter(
  schema: StructType,
  schemaMapper: SparkSchemaToTantivyMapper) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Converts a Java Map to a Spark InternalRow for StructType.
   *
   * @param jsonMap
   *   Java Map from tantivy4java JSON field
   * @param structType
   *   Target Spark StructType
   * @return
   *   Spark InternalRow
   */
  def jsonMapToRow(
    jsonMap: java.util.Map[String, Object],
    structType: StructType
  ): org.apache.spark.sql.catalyst.InternalRow = {
    val values = structType.fields.map { field =>
      val jsonValue = jsonMap.get(field.name)
      if (jsonValue == null) {
        null
      } else {
        convertFromJsonValue(jsonValue, field.dataType)
      }
    }

    logger.debug(s"Converted JSON map to InternalRow with ${values.length} fields")
    org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
  }

  /**
   * Converts a Java List to a Spark ArrayData for ArrayType.
   *
   * @param jsonList
   *   Java List from tantivy4java JSON field
   * @param arrayType
   *   Target Spark ArrayType
   * @return
   *   Spark ArrayData
   */
  def jsonListToArray(
    jsonList: java.util.List[Object],
    arrayType: ArrayType
  ): org.apache.spark.sql.catalyst.util.ArrayData = {
    val result = jsonList.asScala.map(element => convertFromJsonValue(element, arrayType.elementType)).toArray

    logger.debug(s"Converted JSON list to ArrayData with ${result.length} elements")
    org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(result)
  }

  /**
   * Converts a Java Map to a Spark MapData for MapType.
   *
   * @param jsonMap
   *   Java Map from tantivy4java JSON field
   * @param mapType
   *   Target Spark MapType
   * @return
   *   Spark MapData
   */
  def jsonMapToMapData(
    jsonMap: java.util.Map[String, Object],
    mapType: MapType
  ): org.apache.spark.sql.catalyst.util.MapData = {
    // Use entrySet() to maintain key-value pairing
    val entries = jsonMap.entrySet().asScala.toSeq

    val keys = entries.map { entry =>
      // Convert string key back to the original key type
      convertFromJsonValue(entry.getKey, mapType.keyType)
    }.toArray

    val values = entries.map(entry => convertFromJsonValue(entry.getValue, mapType.valueType)).toArray

    logger.debug(s"Converted JSON map to MapData with ${keys.length} entries")
    org.apache.spark.sql.catalyst.util.ArrayBasedMapData(keys, values)
  }

  /**
   * Recursively converts a JSON value to a Spark type.
   *
   * @param jsonValue
   *   Java Object from JSON
   * @param dataType
   *   Target Spark data type
   * @return
   *   Spark value
   */
  def convertFromJsonValue(jsonValue: Object, dataType: DataType): Any = {
    if (jsonValue == null) {
      return null
    }

    dataType match {
      case st: StructType =>
        jsonMapToRow(jsonValue.asInstanceOf[java.util.Map[String, Object]], st)

      case at: ArrayType =>
        jsonListToArray(jsonValue.asInstanceOf[java.util.List[Object]], at)

      case mt: MapType =>
        jsonMapToMapData(jsonValue.asInstanceOf[java.util.Map[String, Object]], mt)

      case StringType =>
        // Convert to UTF8String for Spark's internal format
        val stringValue = jsonValue match {
          case s: String => s
          case _         => jsonValue.toString
        }
        org.apache.spark.unsafe.types.UTF8String.fromString(stringValue)

      case IntegerType =>
        jsonValue match {
          case n: Number => n.intValue()
          case s: String => s.toInt
          case _         => jsonValue.toString.toInt
        }

      case LongType =>
        jsonValue match {
          case n: Number => n.longValue()
          case s: String => s.toLong
          case _         => jsonValue.toString.toLong
        }

      case FloatType =>
        jsonValue match {
          case n: Number => n.floatValue()
          case s: String => s.toFloat
          case _         => jsonValue.toString.toFloat
        }

      case DoubleType =>
        jsonValue match {
          case n: Number => n.doubleValue()
          case s: String => s.toDouble
          case _         => jsonValue.toString.toDouble
        }

      case BooleanType =>
        jsonValue match {
          case b: java.lang.Boolean => b.booleanValue()
          case _                    => jsonValue.toString.toLowerCase == "true"
        }

      case DateType =>
        // Convert milliseconds back to days since epoch
        val millis = jsonValue.asInstanceOf[Number].longValue()
        (millis / 86400000L).toInt

      case TimestampType =>
        // Convert milliseconds to microseconds
        val millis = jsonValue.asInstanceOf[Number].longValue()
        millis * 1000L

      case BinaryType =>
        // Decode base64 string back to bytes
        jsonValue match {
          case s: String          => java.util.Base64.getDecoder.decode(s)
          case bytes: Array[Byte] => bytes
          case _                  => throw new IllegalArgumentException(s"Cannot convert $jsonValue to BinaryType")
        }

      case _ =>
        logger.warn(s"Unsupported type for JSON conversion: $dataType")
        null
    }
  }

  /**
   * Retrieves JSON field from document and converts to Spark type.
   *
   * @param document
   *   tantivy4java Document
   * @param field
   *   Spark StructField
   * @return
   *   Converted Spark value
   */
  def retrieveJsonField(
    document: io.indextables.tantivy4java.core.Document,
    field: StructField
  ): Any = {
    // Get JSON value from document (returns Object - JSON fields are stored as strings)
    val jsonValue = document.getFirst(field.name)
    if (jsonValue == null) return null

    // tantivy4java stores JSON fields as strings
    val jsonString = jsonValue.asInstanceOf[String]

    field.dataType match {
      case at: ArrayType =>
        // Handle both raw JSON arrays and wrapped format:
        // - Raw JSON array: ["a","b","c"] (from companion splits / parquet)
        // - Wrapped format: {"_values": ["a","b","c"]} (from standard index writes)
        val trimmed = jsonString.trim
        if (trimmed.startsWith("[")) {
          // Raw JSON array - parse directly as List
          val jsonList = io.indextables.spark.util.JsonUtil.parseAsJava(
            jsonString,
            classOf[java.util.List[Object]]
          )
          jsonListToArray(jsonList, at)
        } else {
          // Wrapped format - parse as Map and extract "_values"
          val jsonMap = io.indextables.spark.util.JsonUtil.parseAsJava(
            jsonString,
            classOf[java.util.Map[String, Object]]
          )
          val jsonList = jsonMap.get("_values")
          if (jsonList == null) {
            org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(Array.empty[Any])
          } else {
            jsonListToArray(jsonList.asInstanceOf[java.util.List[Object]], at)
          }
        }

      case st: StructType =>
        val jsonMap = io.indextables.spark.util.JsonUtil.parseAsJava(
          jsonString,
          classOf[java.util.Map[String, Object]]
        )
        jsonMapToRow(jsonMap, st)

      case mt: MapType =>
        val jsonMap = io.indextables.spark.util.JsonUtil.parseAsJava(
          jsonString,
          classOf[java.util.Map[String, Object]]
        )
        jsonMapToMapData(jsonMap, mt)

      case StringType if schemaMapper.getFieldType(field.name) == "json" =>
        // JSON string type - return as UTF8String for Spark internal format
        org.apache.spark.unsafe.types.UTF8String.fromString(jsonString)

      case _ =>
        logger.warn(s"Unexpected JSON field retrieval for non-JSON type: ${field.dataType}")
        null
    }
  }

  /**
   * Unwraps an array from JSON object wrapper. Extracts the "_values" key that was used to wrap the array.
   *
   * @param jsonMap
   *   Java Map containing "_values" key
   * @return
   *   Java List from the "_values" key
   */
  def unwrapArrayFromObject(jsonMap: java.util.Map[String, Object]): java.util.List[Object] = {
    val values = jsonMap.get("_values")
    if (values == null) {
      new java.util.ArrayList[Object]()
    } else {
      values.asInstanceOf[java.util.List[Object]]
    }
  }
}
