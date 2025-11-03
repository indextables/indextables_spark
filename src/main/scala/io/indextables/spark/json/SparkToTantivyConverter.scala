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
import org.slf4j.LoggerFactory

/**
 * Converts Spark Row data to Java collections suitable for tantivy4java JSON fields.
 *
 * Handles conversion of:
 *   - Spark StructType → Java Map[String, Object]
 *   - Spark ArrayType → Java List[Object]
 *   - Spark MapType → Java Map[String, Object]
 *   - JSON string parsing → Java Map[String, Object]
 *   - Recursive nested structures
 */
class SparkToTantivyConverter(
  schema: StructType,
  schemaMapper: SparkSchemaToTantivyMapper
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Converts a Spark StructType Row to a Java Map for tantivy4java JSON field.
   *
   * @param row Spark Row containing struct data
   * @param structType Schema of the struct
   * @return Java Map representing the JSON object
   */
  def structToJsonMap(row: Row, structType: StructType): java.util.Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()

    structType.fields.zipWithIndex.foreach { case (field, idx) =>
      val value = row.get(idx)
      if (value != null) {
        val jsonValue = convertToJsonValue(value, field.dataType)
        logger.debug(s"  Adding field '${field.name}' (${field.name.getClass.getName}) = $jsonValue (${jsonValue.getClass.getName})")
        map.put(field.name, jsonValue)
      } else {
        // Explicitly add null values to JSON map so they can be detected on read
        map.put(field.name, null)
        logger.debug(s"  Adding null field '${field.name}'")
      }
    }

    logger.debug(s"Converted struct to JSON map with ${map.size()} fields: $map")
    map
  }

  /**
   * Converts a Spark ArrayType to a Java List for tantivy4java JSON field.
   *
   * @param value Spark Seq value
   * @param arrayType Schema of the array
   * @return Java List representing the JSON array
   */
  def arrayToJsonList(value: Any, arrayType: ArrayType): java.util.List[Object] = {
    val sparkSeq = value.asInstanceOf[Seq[_]]
    val javaList = new java.util.ArrayList[Object]()

    sparkSeq.foreach { element =>
      if (element != null) {
        val jsonValue = convertToJsonValue(element, arrayType.elementType)
        javaList.add(jsonValue)
      }
    }

    logger.debug(s"Converted array to JSON list with ${javaList.size()} elements")
    javaList
  }

  /**
   * Converts a Spark MapType to a Java Map for tantivy4java JSON field.
   *
   * @param value Spark Map value
   * @param mapType Schema of the map
   * @return Java Map representing the JSON object
   */
  def mapToJsonMap(value: Any, mapType: MapType): java.util.Map[String, Object] = {
    val sparkMap = value.asInstanceOf[scala.collection.Map[_, _]]
    val javaMap = new java.util.HashMap[String, Object]()

    sparkMap.foreach { case (key, mapValue) =>
      if (mapValue != null) {
        // Convert key to string (Spark maps can have non-string keys)
        val keyString = convertToJsonValue(key, mapType.keyType).toString
        val jsonValue = convertToJsonValue(mapValue, mapType.valueType)
        logger.debug(s"  Adding map entry '$keyString' = $jsonValue (${jsonValue.getClass.getName})")
        javaMap.put(keyString, jsonValue)
      }
    }

    logger.debug(s"Converted map to JSON map with ${javaMap.size()} entries")
    javaMap
  }

  /**
   * Recursively converts a Spark value to a Java object suitable for JSON serialization.
   *
   * @param value Spark value
   * @param dataType Spark data type
   * @return Java Object for JSON
   */
  def convertToJsonValue(value: Any, dataType: DataType): Object = {
    dataType match {
      case st: StructType =>
        structToJsonMap(value.asInstanceOf[Row], st)

      case at: ArrayType =>
        arrayToJsonList(value, at)

      case mt: MapType =>
        mapToJsonMap(value, mt)

      case StringType =>
        // Handle both UTF8String (from InternalRow) and regular String
        value match {
          case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
          case str: String => str
          case other => other.toString
        }

      case IntegerType =>
        Integer.valueOf(value.asInstanceOf[Int])

      case LongType =>
        java.lang.Long.valueOf(value.asInstanceOf[Long])

      case FloatType =>
        java.lang.Float.valueOf(value.asInstanceOf[Float])

      case DoubleType =>
        java.lang.Double.valueOf(value.asInstanceOf[Double])

      case BooleanType =>
        java.lang.Boolean.valueOf(value.asInstanceOf[Boolean])

      case DateType =>
        // Convert days since epoch to milliseconds for JSON
        val days = value.asInstanceOf[Int]
        java.lang.Long.valueOf(days.toLong * 86400000L)

      case TimestampType =>
        // Convert microseconds to milliseconds
        val micros = value.asInstanceOf[Long]
        java.lang.Long.valueOf(micros / 1000L)

      case BinaryType =>
        // Store binary data as base64 string in JSON
        val bytes = value.asInstanceOf[Array[Byte]]
        java.util.Base64.getEncoder.encodeToString(bytes)

      case _ =>
        logger.warn(s"Unsupported type for JSON conversion: $dataType, using toString")
        value.toString
    }
  }

  /**
   * Parses a JSON string into a Java Map.
   *
   * @param jsonString JSON string to parse
   * @param config JSON field configuration
   * @return Java Map representing the JSON object, or a map with "_raw" key if parsing fails
   */
  def parseJsonString(jsonString: String, config: JsonFieldConfig): java.util.Map[String, Object] = {
    try {
      JsonUtils.parseJson(jsonString)
    } catch {
      case e: Exception =>
        if (config.failOnInvalidJson) {
          throw new RuntimeException(s"Invalid JSON string: $jsonString", e)
        } else {
          logger.warn(s"Failed to parse JSON, storing as raw text: ${e.getMessage}")
          // Store unparsed string in special "_raw" field
          val map = new java.util.HashMap[String, Object]()
          map.put("_raw", jsonString)
          map
        }
    }
  }

  /**
   * Wraps an array in a JSON object for tantivy4java storage.
   * Since tantivy4java JSON fields expect objects, we wrap arrays with a "_values" key.
   *
   * @param jsonList Java List representing the array
   * @return Java Map with "_values" key containing the list
   */
  def wrapArrayInObject(jsonList: java.util.List[Object]): java.util.Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()
    map.put("_values", jsonList)
    map
  }
}

/**
 * JSON parsing and serialization utilities.
 */
object JsonUtils {
  private val mapper = new com.fasterxml.jackson.databind.ObjectMapper()

  /**
   * Parses a JSON string into a Java Map.
   *
   * @param jsonString JSON string
   * @return Java Map
   * @throws com.fasterxml.jackson.core.JsonParseException if JSON is invalid
   */
  def parseJson(jsonString: String): java.util.Map[String, Object] = {
    mapper.readValue(jsonString, classOf[java.util.Map[String, Object]])
  }

  /**
   * Serializes a Java Map to a JSON string.
   *
   * @param jsonMap Java Map
   * @return JSON string
   */
  def serializeToJson(jsonMap: java.util.Map[String, Object]): String = {
    mapper.writeValueAsString(jsonMap)
  }

  /**
   * Serializes a Java List to a JSON string.
   *
   * @param jsonList Java List
   * @return JSON string
   */
  def serializeListToJson(jsonList: java.util.List[Object]): String = {
    mapper.writeValueAsString(jsonList)
  }
}
