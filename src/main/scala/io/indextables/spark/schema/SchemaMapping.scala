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

package io.indextables.spark.schema

import org.apache.spark.sql.types._

import io.indextables.spark.json.{SparkSchemaToTantivyMapper, TantivyToSparkConverter}
import io.indextables.spark.util.TimestampUtils
import io.indextables.tantivy4java.core.{FieldType, Schema}
import org.slf4j.LoggerFactory

/**
 * Comprehensive schema mapping system for IndexTables4Spark.
 *
 * Responsibilities:
 *   - Write-side: Handle schema for new vs existing datasets
 *   - Read-side: Convert split schema to Spark schema with explicit type conversion
 *   - Validation: Ensure schema compatibility and detect conflicts
 */
object SchemaMapping {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Exception thrown when schemas are incompatible */
  case class SchemaConflictException(message: String, cause: Throwable = null) extends Exception(message, cause)

  /** Represents a field mapping between Spark and Tantivy types */
  case class FieldMapping(
    sparkField: StructField,
    tantivyFieldType: FieldType,
    conversionRequired: Boolean)

  /** Write-side schema handling */
  object Write {

    /**
     * Handle schema for a new dataset (no existing transaction log)
     * @param sparkSchema
     *   Schema from the DataFrame being written
     * @return
     *   Schema to be written to transaction log
     */
    def handleNewDataset(sparkSchema: StructType): StructType = {
      logger.info(s"Creating new dataset with schema: ${sparkSchema.prettyJson}")

      // Validate that all fields are supported
      validateSupportedTypes(sparkSchema)

      // Return the schema as-is for transaction log
      sparkSchema
    }

    /**
     * Handle schema for an existing dataset
     * @param sparkSchema
     *   Schema from the DataFrame being written
     * @param transactionLogSchema
     *   Schema from the transaction log
     * @return
     *   Validated schema (should match transaction log)
     */
    def handleExistingDataset(sparkSchema: StructType, transactionLogSchema: StructType): StructType = {
      logger.info(s"Writing to existing dataset. Validating compatibility.")
      logger.debug(s"Spark schema: ${sparkSchema.prettyJson}")
      logger.debug(s"Transaction log schema: ${transactionLogSchema.prettyJson}")

      // Check for exact schema match
      if (sparkSchema != transactionLogSchema) {
        val conflicts = findSchemaConflicts(sparkSchema, transactionLogSchema)
        throw SchemaConflictException(
          s"Schema conflict detected. Cannot write DataFrame with different schema than existing dataset.\n" +
            s"Conflicts: ${conflicts.mkString(", ")}\n" +
            s"Existing schema: ${transactionLogSchema.prettyJson}\n" +
            s"Provided schema: ${sparkSchema.prettyJson}"
        )
      }

      transactionLogSchema
    }

    /** Validate that all Spark types are supported by Tantivy */
    private def validateSupportedTypes(schema: StructType): Unit =
      schema.fields.foreach { field =>
        if (!isSupportedSparkType(field.dataType)) {
          throw new UnsupportedOperationException(
            s"Field '${field.name}' has unsupported type '${field.dataType}'. " +
              s"Supported types: ${getSupportedTypes.mkString(", ")}"
          )
        }
      }

    /** Find specific conflicts between two schemas */
    private def findSchemaConflicts(schema1: StructType, schema2: StructType): List[String] = {
      val conflicts = scala.collection.mutable.ListBuffer[String]()

      // Check field count
      if (schema1.fields.length != schema2.fields.length) {
        conflicts += s"Field count mismatch: ${schema1.fields.length} vs ${schema2.fields.length}"
      }

      // Check each field
      val schema1Fields = schema1.fields.map(f => f.name -> f).toMap
      val schema2Fields = schema2.fields.map(f => f.name -> f).toMap

      (schema1Fields.keySet ++ schema2Fields.keySet).foreach { fieldName =>
        (schema1Fields.get(fieldName), schema2Fields.get(fieldName)) match {
          case (Some(field1), Some(field2)) =>
            if (field1.dataType != field2.dataType) {
              conflicts += s"Field '$fieldName' type mismatch: ${field1.dataType} vs ${field2.dataType}"
            }
            if (field1.nullable != field2.nullable) {
              conflicts += s"Field '$fieldName' nullability mismatch: ${field1.nullable} vs ${field2.nullable}"
            }
          case (Some(_), None) =>
            conflicts += s"Field '$fieldName' exists in first schema but not second"
          case (None, Some(_)) =>
            conflicts += s"Field '$fieldName' exists in second schema but not first"
          case (None, None) => // Should never happen
        }
      }

      conflicts.toList
    }
  }

  /** Read-side schema conversion */
  object Read {

    /**
     * Convert data from split schema to Spark schema
     * @param splitDocument
     *   Document from tantivy4java split
     * @param splitSchema
     *   Schema from the split file (tantivy4java Schema)
     * @param sparkSchema
     *   Target Spark schema (from transaction log)
     * @param options
     *   IndexTables4Spark options (optional, for JSON field support)
     * @return
     *   Array of values converted to Spark types
     */
    def convertDocument(
      splitDocument: io.indextables.tantivy4java.core.Document,
      splitSchema: Schema,
      sparkSchema: StructType,
      options: Option[io.indextables.spark.core.IndexTables4SparkOptions] = None
    ): Array[Any] = {
      logger.debug(s"SchemaMapping.convertDocument DEBUG:")
      logger.debug(s"  Spark schema fields: ${sparkSchema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")
      logger.debug(s"  Split schema fields: ${splitSchema.getFieldNames().toArray.mkString(", ")}")

      // Create JSON field mapper and converter if options provided
      val jsonMapper: Option[SparkSchemaToTantivyMapper] = options.map(new SparkSchemaToTantivyMapper(_))
      val jsonConverter: Option[TantivyToSparkConverter] =
        jsonMapper.map(mapper => new TantivyToSparkConverter(sparkSchema, mapper))

      val result = sparkSchema.fields.map { sparkField =>
        val convertedValue = convertField(splitDocument, sparkField, splitSchema, jsonMapper, jsonConverter)
        logger.debug(
          s"  Field '${sparkField.name}' -> $convertedValue (${if (convertedValue != null) convertedValue.getClass.getSimpleName
            else "null"})"
        )
        convertedValue
      }

      logger.debug(s"  Final result array: [${result.zipWithIndex.map { case (v, i) => s"$i:$v" }.mkString(", ")}]")
      result
    }

    /** Convert a single field from split to Spark format */
    private def convertField(
      document: io.indextables.tantivy4java.core.Document,
      sparkField: StructField,
      splitSchema: Schema,
      jsonMapper: Option[SparkSchemaToTantivyMapper],
      jsonConverter: Option[TantivyToSparkConverter]
    ): Any =
      try {
        // Check if this is a JSON field and we have a converter
        (jsonMapper, jsonConverter) match {
          case (Some(mapper), Some(converter)) if mapper.shouldUseJsonField(sparkField) =>
            // Use JSON converter for JSON fields (Struct, Array, JSON strings)
            logger.debug(s"Using JSON converter for field '${sparkField.name}' (type: ${sparkField.dataType})")
            converter.retrieveJsonField(document, sparkField)

          case _ =>
            // Handle regular fields with standard conversion
            val rawValue = document.getFirst(sparkField.name)

            if (rawValue == null) {
              return null
            }

            // Get the Tantivy field type from split schema
            val splitFieldInfo   = splitSchema.getFieldInfo(sparkField.name)
            val tantivyFieldType = splitFieldInfo.getType()

            // Convert based on explicit mapping
            convertValue(rawValue, tantivyFieldType, sparkField.dataType, sparkField.name)
        }

      } catch {
        case e: Exception =>
          logger.warn(s"Failed to convert field '${sparkField.name}': ${e.getMessage}")
          if (sparkField.nullable) {
            null
          } else {
            getDefaultValue(sparkField.dataType)
          }
      }

    /** Explicit type conversion from Tantivy to Spark */
    private def convertValue(
      rawValue: Any,
      tantivyType: FieldType,
      sparkType: DataType,
      fieldName: String
    ): Any = {
      logger.debug(s"Converting field '$fieldName': $rawValue ($tantivyType) -> $sparkType")

      (tantivyType, sparkType) match {
        // TEXT -> String
        case (FieldType.TEXT, StringType) =>
          org.apache.spark.unsafe.types.UTF8String.fromString(rawValue.toString)

        // INTEGER -> IntegerType
        case (FieldType.INTEGER, IntegerType) =>
          rawValue match {
            case i: java.lang.Integer => i.intValue()
            case l: java.lang.Long    => l.intValue()
            case s: String            => s.toInt
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Integer")
          }

        // INTEGER -> LongType
        case (FieldType.INTEGER, LongType) =>
          rawValue match {
            case i: java.lang.Integer => i.longValue()
            case l: java.lang.Long    => l.longValue()
            case s: String            => s.toLong
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Long")
          }

        // INTEGER -> DateType (stored as days since epoch)
        case (FieldType.INTEGER, DateType) =>
          rawValue match {
            case l: java.lang.Long    => l.intValue() // Days since epoch
            case i: java.lang.Integer => i.intValue()
            case s: String            => s.toInt
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Date")
          }

        // INTEGER -> TimestampType (stored as microseconds in tantivy, return directly)
        case (FieldType.INTEGER, TimestampType) =>
          rawValue match {
            case l: java.lang.Long    => l.longValue() // Already in microseconds, return as-is
            case i: java.lang.Integer => i.longValue() // Convert to Long
            case s: String            => s.toLong      // Parse as Long (microseconds)
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Timestamp")
          }

        // FLOAT -> DoubleType
        case (FieldType.FLOAT, DoubleType) =>
          rawValue match {
            case f: java.lang.Float  => f.doubleValue()
            case d: java.lang.Double => d.doubleValue()
            case s: String           => s.toDouble
            case other               => throw new IllegalArgumentException(s"Cannot convert $other to Double")
          }

        // FLOAT -> FloatType
        case (FieldType.FLOAT, FloatType) =>
          rawValue match {
            case f: java.lang.Float  => f.floatValue()
            case d: java.lang.Double => d.floatValue()
            case s: String           => s.toFloat
            case other               => throw new IllegalArgumentException(s"Cannot convert $other to Float")
          }

        // BOOLEAN -> BooleanType
        case (FieldType.BOOLEAN, BooleanType) =>
          rawValue match {
            case b: java.lang.Boolean => b.booleanValue()
            case i: java.lang.Integer => i != 0
            case s: String            => s.toLowerCase == "true" || s == "1"
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Boolean")
          }

        // DATE -> DateType
        case (FieldType.DATE, DateType) =>
          rawValue match {
            case ldt: java.time.LocalDateTime =>
              // Convert LocalDateTime back to days since epoch for Spark DateType
              val localDate = ldt.toLocalDate()
              val epochDate = java.time.LocalDate.of(1970, 1, 1)
              java.time.temporal.ChronoUnit.DAYS.between(epochDate, localDate).toInt
            case ld: java.time.LocalDate =>
              // Convert LocalDate back to days since epoch for Spark DateType
              val epochDate = java.time.LocalDate.of(1970, 1, 1)
              java.time.temporal.ChronoUnit.DAYS.between(epochDate, ld).toInt
            case l: java.lang.Long    => l.intValue() // Days since epoch
            case i: java.lang.Integer => i.intValue()
            case s: String            =>
              // Try to parse as numeric days first, then as LocalDateTime/LocalDate string
              try
                s.toInt // Parse as days since epoch
              catch {
                case _: NumberFormatException =>
                  // Parse as LocalDateTime/LocalDate string
                  try {
                    val epochDate = java.time.LocalDate.of(1970, 1, 1)
                    if (s.contains("T")) {
                      val localDateTime = java.time.LocalDateTime.parse(s)
                      java.time.temporal.ChronoUnit.DAYS.between(epochDate, localDateTime.toLocalDate).toInt
                    } else {
                      val localDate = java.time.LocalDate.parse(s)
                      java.time.temporal.ChronoUnit.DAYS.between(epochDate, localDate).toInt
                    }
                  } catch {
                    case _: Exception => throw new IllegalArgumentException(s"Cannot parse date string: $s")
                  }
              }
            case other                => throw new IllegalArgumentException(s"Cannot convert $other to Date")
          }

        // DATE -> TimestampType (DATE fields may return LocalDateTime or microseconds)
        case (FieldType.DATE, TimestampType) =>
          rawValue match {
            case l: java.lang.Long    => l.longValue() // Already in microseconds
            case i: java.lang.Integer => i.longValue() // Convert to Long
            case ldt: java.time.LocalDateTime =>
              // Convert LocalDateTime to microseconds since epoch
              TimestampUtils.toMicros(ldt)
            case ld: java.time.LocalDate =>
              // Convert LocalDate to microseconds since epoch (start of day)
              ld.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli * 1000L
            case s: String =>
              // Try to parse as numeric microseconds first, then as LocalDateTime string
              try
                s.toLong // Parse as microseconds
              catch {
                case _: NumberFormatException =>
                  // Parse as LocalDateTime/LocalDate string
                  try
                    if (s.contains("T")) {
                      val localDateTime = java.time.LocalDateTime.parse(s)
                      TimestampUtils.toMicros(localDateTime)
                    } else {
                      val localDate = java.time.LocalDate.parse(s)
                      localDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli * 1000L
                    }
                  catch {
                    case _: Exception => throw new IllegalArgumentException(s"Cannot parse timestamp string: $s")
                  }
              }
            case other => throw new IllegalArgumentException(s"Cannot convert $other to Timestamp")
          }

        // BYTES -> BinaryType
        case (FieldType.BYTES, BinaryType) =>
          rawValue match {
            case bytes: Array[Byte] => bytes
            case s: String          => s.getBytes("UTF-8")
            case other              => throw new IllegalArgumentException(s"Cannot convert $other to Binary")
          }

        // TEXT -> DateType (for tantivy4java addDateField that creates TEXT fields)
        case (FieldType.TEXT, DateType) =>
          rawValue match {
            case ldt: java.time.LocalDateTime =>
              // Convert LocalDateTime back to days since epoch for Spark DateType
              val localDate = ldt.toLocalDate()
              val epochDate = java.time.LocalDate.of(1970, 1, 1)
              java.time.temporal.ChronoUnit.DAYS.between(epochDate, localDate).toInt
            case ld: java.time.LocalDate =>
              // Convert LocalDate back to days since epoch for Spark DateType
              val epochDate = java.time.LocalDate.of(1970, 1, 1)
              java.time.temporal.ChronoUnit.DAYS.between(epochDate, ld).toInt
            case s: String =>
              // Parse date string and convert to days since epoch
              try {
                val localDate = java.time.LocalDate.parse(s)
                val epochDate = java.time.LocalDate.of(1970, 1, 1)
                java.time.temporal.ChronoUnit.DAYS.between(epochDate, localDate).toInt
              } catch {
                case _: Exception => throw new IllegalArgumentException(s"Cannot parse date string: $s")
              }
            case other => throw new IllegalArgumentException(s"Cannot convert $other to Date from TEXT field")
          }

        // TEXT -> TimestampType (for tantivy4java addDateField that creates TEXT fields)
        case (FieldType.TEXT, TimestampType) =>
          rawValue match {
            case ldt: java.time.LocalDateTime =>
              // Convert LocalDateTime to microseconds since epoch
              TimestampUtils.toMicros(ldt)
            case ld: java.time.LocalDate =>
              // Convert LocalDate to microseconds since epoch (start of day)
              ld.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli * 1000L
            case s: String =>
              // Parse date/timestamp string and convert to microseconds since epoch
              try
                if (s.contains("T")) {
                  // Parse as LocalDateTime and convert to microseconds
                  val localDateTime = java.time.LocalDateTime.parse(s)
                  TimestampUtils.toMicros(localDateTime)
                } else {
                  // Parse as LocalDate
                  val localDate = java.time.LocalDate.parse(s)
                  localDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli * 1000L
                }
              catch {
                case _: Exception => throw new IllegalArgumentException(s"Cannot parse timestamp string: $s")
              }
            case other => throw new IllegalArgumentException(s"Cannot convert $other to Timestamp from TEXT field")
          }

        // JSON -> StringType (for StringType fields indexed as "json")
        case (FieldType.JSON, StringType) =>
          // Convert JSON value back to stringified JSON
          val jsonString = rawValue match {
            case s: String =>
              // Already a string, return as-is
              s
            case _ =>
              // Use JsonUtil to serialize Map, List, or any other object to JSON
              io.indextables.spark.util.JsonUtil.toJson(rawValue)
          }
          org.apache.spark.unsafe.types.UTF8String.fromString(jsonString)

        // Unsupported conversion
        case (fromType, toType) =>
          throw new IllegalArgumentException(
            s"Unsupported conversion from Tantivy $fromType to Spark $toType for field '$fieldName'. " +
              s"Raw value: $rawValue (${rawValue.getClass.getSimpleName})"
          )
      }
    }

    /** Get default value for a Spark data type */
    def getDefaultValue(dataType: DataType): Any =
      dataType match {
        case StringType    => org.apache.spark.unsafe.types.UTF8String.fromString("")
        case IntegerType   => 0
        case LongType      => 0L
        case DoubleType    => 0.0
        case FloatType     => 0.0f
        case BooleanType   => false
        case DateType      => 0
        case TimestampType => 0L
        case BinaryType    => Array.empty[Byte]
        case _             => null
      }
  }

  /** Utility methods */

  /** Check if a Spark type is supported by Tantivy */
  def isSupportedSparkType(dataType: DataType): Boolean =
    dataType match {
      case StringType | IntegerType | LongType | DoubleType | FloatType | BooleanType | DateType | TimestampType |
          BinaryType =>
        true
      case _ => false
    }

  /** Get list of supported Spark types */
  def getSupportedTypes: List[String] =
    List(
      "StringType",
      "IntegerType",
      "LongType",
      "DoubleType",
      "FloatType",
      "BooleanType",
      "DateType",
      "TimestampType",
      "BinaryType"
    )

  /** Convert Spark type to Tantivy field type */
  def sparkTypeToTantivyFieldType(sparkType: DataType): FieldType =
    sparkType match {
      case StringType               => FieldType.TEXT
      case IntegerType | LongType   => FieldType.INTEGER
      case DoubleType | FloatType   => FieldType.FLOAT
      case BooleanType              => FieldType.BOOLEAN
      case DateType | TimestampType => FieldType.DATE
      case BinaryType               => FieldType.BYTES
      case _                        => throw new IllegalArgumentException(s"Unsupported Spark type: $sparkType")
    }
}
