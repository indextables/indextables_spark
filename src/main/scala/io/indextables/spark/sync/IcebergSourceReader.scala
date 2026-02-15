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

import scala.jdk.CollectionConverters._

import io.indextables.tantivy4java.iceberg.IcebergTableReader

import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory

/**
 * CompanionSourceReader for Apache Iceberg tables.
 * Wraps tantivy4java 0.29.6's IcebergTableReader (Rust iceberg-rust via JNI).
 *
 * @param tableIdentifier Iceberg table identifier (e.g., "namespace.table_name")
 * @param catalogName     Iceberg catalog name (default: "default")
 * @param icebergConfig   Configuration map for IcebergTableReader (catalog type, URI, credentials)
 * @param snapshotId      Optional snapshot ID for time travel
 */
class IcebergSourceReader(
    tableIdentifier: String,
    catalogName: String,
    icebergConfig: java.util.Map[String, String],
    snapshotId: Option[Long] = None
) extends CompanionSourceReader {

  private val logger = LoggerFactory.getLogger(classOf[IcebergSourceReader])

  // Parse "namespace.table" into separate components
  private val (namespace, tableName) = {
    val parts = tableIdentifier.split("\\.", 2)
    if (parts.length == 2) (parts(0), parts(1))
    else throw new IllegalArgumentException(
      s"Invalid Iceberg table identifier '$tableIdentifier': expected 'namespace.table_name'")
  }

  // Lazily list files and cache
  private lazy val fileEntries = {
    logger.info(s"Listing Iceberg table $catalogName.$namespace.$tableName" +
      snapshotId.map(id => s" at snapshot $id").getOrElse(""))

    val entries = snapshotId match {
      case Some(id) =>
        IcebergTableReader.listFiles(catalogName, namespace, tableName, icebergConfig, id)
      case None =>
        IcebergTableReader.listFiles(catalogName, namespace, tableName, icebergConfig)
    }
    logger.info(s"Iceberg table contains ${entries.size} file entries")
    entries
  }

  override def sourceVersion(): Option[Long] = {
    val entries = fileEntries
    if (entries.isEmpty) snapshotId
    else Some(entries.get(0).getSnapshotId)
  }

  override def getAllFiles(): Seq[CompanionSourceFile] = {
    val entries = fileEntries
    val parquetFiles = entries.asScala.toSeq.filter { entry =>
      val fmt = entry.getFileFormat
      fmt == null || fmt.equalsIgnoreCase("parquet")
    }

    val nonParquet = entries.size - parquetFiles.size
    if (nonParquet > 0) {
      throw new UnsupportedOperationException(
        s"Iceberg table contains $nonParquet non-parquet files (ORC/Avro). " +
        "BUILD COMPANION only supports parquet-format data files.")
    }

    parquetFiles.map { entry =>
      CompanionSourceFile(
        path = entry.getPath,
        partitionValues = entry.getPartitionValues.asScala.toMap,
        size = entry.getFileSizeBytes
      )
    }
  }

  override def partitionColumns(): Seq[String] =
    getAllFiles().headOption
      .map(_.partitionValues.keys.toSeq.sorted)
      .getOrElse(Seq.empty)

  override def schema(): StructType = {
    logger.info(s"Reading Iceberg schema for $catalogName.$namespace.$tableName")
    val icebergSchema = snapshotId match {
      case Some(id) =>
        IcebergTableReader.readSchema(catalogName, namespace, tableName, icebergConfig, id)
      case None =>
        IcebergTableReader.readSchema(catalogName, namespace, tableName, icebergConfig)
    }
    val schemaJson = icebergSchema.getSchemaJson()
    val sparkSchema = try {
      DataType.fromJson(schemaJson).asInstanceOf[StructType]
    } catch {
      case _: Exception =>
        logger.info("Iceberg schema JSON is not Spark-compatible, converting from Iceberg format")
        convertIcebergSchemaToSpark(schemaJson)
    }
    logger.info(s"Iceberg schema: ${sparkSchema.fields.length} fields")
    sparkSchema
  }

  override def schemaSourceParquetFile(): Option[String] = None // Iceberg provides schema via catalog

  /**
   * Convert Iceberg-format schema JSON to Spark StructType.
   * Iceberg format: {"schema-id":0, "type":"struct", "fields":[{"id":1, "name":"...", "required":false, "type":"long"}]}
   * Spark format:   {"type":"struct", "fields":[{"name":"...", "type":"long", "nullable":true, "metadata":{}}]}
   */
  private def convertIcebergSchemaToSpark(schemaJson: String): StructType = {
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    val root = mapper.readTree(schemaJson)
    val fields = root.get("fields")

    val sparkFields = (0 until fields.size()).map { i =>
      val field = fields.get(i)
      val name = field.get("name").asText()
      val nullable = !field.get("required").asBoolean(false)
      val icebergType = field.get("type")
      val dataType = icebergTypeToSparkType(icebergType)
      StructField(name, dataType, nullable)
    }
    StructType(sparkFields)
  }

  private def icebergTypeToSparkType(typeNode: com.fasterxml.jackson.databind.JsonNode): DataType = {
    if (typeNode.isTextual) {
      icebergPrimitiveToSpark(typeNode.asText())
    } else if (typeNode.isObject) {
      val typeName = typeNode.get("type").asText()
      typeName match {
        case "struct" =>
          val fields = typeNode.get("fields")
          val sparkFields = (0 until fields.size()).map { i =>
            val f = fields.get(i)
            val name = f.get("name").asText()
            val nullable = !f.get("required").asBoolean(false)
            StructField(name, icebergTypeToSparkType(f.get("type")), nullable)
          }
          StructType(sparkFields)
        case "list" =>
          val elementField = typeNode.get("element")
          val elementNullable = !typeNode.get("element-required").asBoolean(false)
          ArrayType(icebergTypeToSparkType(elementField), elementNullable)
        case "map" =>
          val keyType = icebergTypeToSparkType(typeNode.get("key"))
          val valueType = icebergTypeToSparkType(typeNode.get("value"))
          val valueNullable = !typeNode.get("value-required").asBoolean(false)
          MapType(keyType, valueType, valueNullable)
        case other =>
          logger.warn(s"Unknown Iceberg complex type '$other', defaulting to StringType")
          StringType
      }
    } else {
      logger.warn(s"Unexpected Iceberg type node: $typeNode, defaulting to StringType")
      StringType
    }
  }

  private val decimalPattern = """decimal\((\d+),\s*(\d+)\)""".r

  private def icebergPrimitiveToSpark(icebergType: String): DataType = icebergType match {
    case "boolean" => BooleanType
    case "int" | "integer" => IntegerType
    case "long" => LongType
    case "float" => FloatType
    case "double" => DoubleType
    case "string" => StringType
    case "binary" | "fixed" => BinaryType
    case "date" => DateType
    case "timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns" => TimestampType
    case "time" => LongType // Iceberg time stored as microseconds since midnight
    case "uuid" => StringType
    case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    case other =>
      logger.warn(s"Unknown Iceberg type '$other', defaulting to StringType")
      StringType
  }
}
