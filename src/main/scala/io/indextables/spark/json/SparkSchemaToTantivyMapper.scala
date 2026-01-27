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

import org.apache.spark.sql.types._

import io.indextables.spark.core.IndexTables4SparkOptions
import org.slf4j.LoggerFactory

/**
 * Maps Spark schemas to tantivy4java schemas with JSON field support.
 *
 * Determines if a Spark field should be mapped to a JSON field based on:
 *   - StructType fields (automatic)
 *   - ArrayType fields (automatic)
 *   - MapType fields (automatic)
 *   - StringType fields with explicit "json" type configuration
 */
class SparkSchemaToTantivyMapper(options: IndexTables4SparkOptions) {
  private val logger = LoggerFactory.getLogger(getClass)

  // Handle null options gracefully (can happen during MixedBooleanFilter conversion)
  private val fieldTypeMapping: Map[String, String] =
    if (options != null) options.getFieldTypeMapping else Map.empty[String, String]

  /**
   * Determines if a Spark field should be mapped to a JSON field in tantivy4java.
   *
   * @param field
   *   Spark StructField to check
   * @return
   *   true if field should use JSON field type
   */
  def shouldUseJsonField(field: StructField): Boolean = {
    val result = field.dataType match {
      case _: StructType =>
        logger.debug(s"Field '${field.name}' is StructType - using JSON field")
        true
      case _: ArrayType =>
        logger.debug(s"Field '${field.name}' is ArrayType - using JSON field")
        true
      case _: MapType =>
        logger.debug(s"Field '${field.name}' is MapType - using JSON field")
        true
      case StringType if getFieldType(field.name) == "json" =>
        logger.debug(s"Field '${field.name}' is StringType with 'json' configuration - using JSON field")
        true
      case _ =>
        false
    }
    result
  }

  /**
   * Gets the configured field type for a field name.
   *
   * @param fieldName
   *   Name of the field
   * @return
   *   Field type: "string", "text", or "json"
   */
  def getFieldType(fieldName: String): String =
    // Use lowercase for case-insensitive matching
    fieldTypeMapping.getOrElse(fieldName.toLowerCase, "string")

  /**
   * Determines if a field requires range query support (fast fields).
   *
   * @param fieldName
   *   Name of the field
   * @return
   *   true if field needs fast field configuration
   */
  def requiresRangeQueries(fieldName: String): Boolean =
    // Check if field is explicitly configured as fast field
    options.getFastFields.contains(fieldName)

  /**
   * Validates that JSON fields are properly configured. For Struct, Array, and Map types, ensures no conflicting type
   * mappings exist.
   *
   * @param schema
   *   Spark schema to validate
   * @throws IllegalArgumentException
   *   if validation fails
   */
  def validateJsonFieldConfiguration(schema: StructType): Unit =
    schema.fields.foreach { field =>
      field.dataType match {
        case _: StructType | _: ArrayType | _: MapType =>
          // These should automatically use JSON fields
          // Use lowercase for case-insensitive matching
          val configuredType = fieldTypeMapping.get(field.name.toLowerCase)
          if (configuredType.isDefined && configuredType.get != "json") {
            throw new IllegalArgumentException(
              s"Field '${field.name}' has type ${field.dataType} but is configured as '${configuredType.get}'. " +
                s"Struct, Array, and Map types must use 'json' type or have no explicit type configuration."
            )
          }
        case StringType =>
          // Valid to have "string", "text", or "json" configuration
          val configuredType = getFieldType(field.name)
          if (!Set("string", "text", "json").contains(configuredType)) {
            throw new IllegalArgumentException(
              s"Field '${field.name}' has invalid type configuration: '$configuredType'. " +
                s"Valid types are: string, text, json"
            )
          }
        case _ =>
          // Primitive types - should not have "json" configuration
          // Use lowercase for case-insensitive matching
          val configuredType = fieldTypeMapping.get(field.name.toLowerCase)
          if (configuredType.contains("json")) {
            throw new IllegalArgumentException(
              s"Field '${field.name}' has type ${field.dataType} but is configured as 'json'. " +
                s"Only StructType, ArrayType, MapType, and StringType can use 'json' type."
            )
          }
      }
    }

  /**
   * Gets JSON-specific configuration for a field.
   *
   * @param fieldName
   *   Name of the field
   * @return
   *   JsonFieldConfig with settings
   */
  def getJsonFieldConfig(fieldName: String): JsonFieldConfig =
    // Currently using simple configuration
    // Future: could add more granular JSON-specific settings
    JsonFieldConfig(
      parseOnWrite = true,
      failOnInvalidJson = false,
      enableRangeQueries = requiresRangeQueries(fieldName)
    )
}

/**
 * Configuration for JSON field behavior.
 *
 * @param parseOnWrite
 *   Whether to parse JSON strings on write (default: true)
 * @param failOnInvalidJson
 *   Whether to fail on invalid JSON or store as text (default: false)
 * @param enableRangeQueries
 *   Whether to enable range queries (requires fast fields) (default: false)
 */
case class JsonFieldConfig(
  parseOnWrite: Boolean = true,
  failOnInvalidJson: Boolean = false,
  enableRangeQueries: Boolean = false)
