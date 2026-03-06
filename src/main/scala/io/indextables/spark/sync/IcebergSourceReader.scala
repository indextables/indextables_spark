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

import org.apache.spark.sql.types._

import io.indextables.tantivy4java.iceberg.IcebergTableReader
import io.indextables.tantivy4java.parquet.ParquetSchemaReader
import org.slf4j.LoggerFactory

object IcebergSourceReader {

  /**
   * Translate Iceberg catalog config keys to ParquetSchemaReader/object_store storage config keys. Iceberg uses:
   * s3.access-key-id, s3.secret-access-key, s3.session-token, s3.region ParquetSchemaReader/object_store uses:
   * aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region
   */
  def buildParquetReaderStorageConfig(icebergConfig: java.util.Map[String, String]): java.util.Map[String, String] = {
    val config     = new java.util.HashMap[String, String]()
    val icebergMap = icebergConfig.asScala
    icebergMap.get("s3.access-key-id").foreach(v => config.put("aws_access_key_id", v))
    icebergMap.get("s3.secret-access-key").foreach(v => config.put("aws_secret_access_key", v))
    icebergMap.get("s3.session-token").foreach(v => config.put("aws_session_token", v))
    icebergMap.get("s3.region").foreach(v => config.put("aws_region", v))
    icebergMap.get("s3.endpoint").foreach(v => config.put("aws_endpoint", v))
    icebergMap.get("s3.path-style-access").foreach(v => config.put("aws_force_path_style", v))
    // Azure credentials
    icebergMap.get("adls.account-name").foreach(v => config.put("azure_account_name", v))
    icebergMap.get("adls.account-key").foreach(v => config.put("azure_access_key", v))
    config
  }
}

/**
 * CompanionSourceReader for Apache Iceberg tables. Wraps tantivy4java 0.29.6's IcebergTableReader (Rust iceberg-rust
 * via JNI).
 *
 * @param tableIdentifier
 *   Iceberg table identifier (e.g., "namespace.table_name")
 * @param catalogName
 *   Iceberg catalog name (default: "default")
 * @param icebergConfig
 *   Configuration map for IcebergTableReader (catalog type, URI, credentials)
 * @param snapshotId
 *   Optional snapshot ID for time travel
 */
class IcebergSourceReader(
  tableIdentifier: String,
  catalogName: String,
  icebergConfig: java.util.Map[String, String],
  snapshotId: Option[Long] = None)
    extends CompanionSourceReader {

  private val logger = LoggerFactory.getLogger(classOf[IcebergSourceReader])

  // Parse "namespace.table" into separate components
  private val (namespace, tableName) = {
    val parts = tableIdentifier.split("\\.", 2)
    if (parts.length == 2) (parts(0), parts(1))
    else
      throw new IllegalArgumentException(
        s"Invalid Iceberg table identifier '$tableIdentifier': expected 'namespace.table_name'"
      )
  }

  // Lazily list files and cache
  private lazy val fileEntries = {
    logger.info(
      s"Listing Iceberg table $catalogName.$namespace.$tableName" +
        snapshotId.map(id => s" at snapshot $id").getOrElse("")
    )

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

  // Lazily compute the S3/Azure storage root from the first file's absolute path.
  // This is the base path with partition segments stripped, e.g.:
  //   s3://warehouse/db/table/data/region=us-east/file.parquet
  //   → s3://warehouse/db/table/data
  private lazy val computedStorageRoot: Option[String] = {
    val entries = fileEntries
    if (entries.isEmpty) None
    else {
      val root = SyncTaskExecutor.extractTableBasePath(entries.get(0).getPath)
      logger.info(s"Iceberg storage root: $root")
      Some(root)
    }
  }

  override def storageRoot(): Option[String] = computedStorageRoot

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
          "BUILD COMPANION only supports parquet-format data files."
      )
    }

    val root = computedStorageRoot

    // Build files with absolute paths first (needed for partition value extraction)
    val absoluteFiles = parquetFiles.map { entry =>
      CompanionSourceFile(
        path = entry.getPath,
        partitionValues = entry.getPartitionValues.asScala.toMap,
        size = entry.getFileSizeBytes
      )
    }

    // Fallback: if all partition values from the catalog are empty but file paths
    // contain Hive-style partitions (key=value/), extract partition values from the
    // absolute paths. Spark's Iceberg writer uses Hive-style directory layout by default.
    val enrichedFiles = {
      val allPartitionsEmpty = absoluteFiles.nonEmpty && absoluteFiles.forall(_.partitionValues.isEmpty)
      if (allPartitionsEmpty) {
        root match {
          case Some(basePath) =>
            val enriched = absoluteFiles.map { f =>
              val pv = extractPartitionValuesFromPath(f.path, basePath)
              if (pv.nonEmpty) f.copy(partitionValues = pv) else f
            }
            val partCols = enriched.headOption.flatMap(f =>
              if (f.partitionValues.nonEmpty) Some(f.partitionValues.keys.toSeq.sorted) else None
            )
            partCols.foreach(cols =>
              logger.info(
                s"Extracted partition columns from file paths (catalog returned empty): ${cols.mkString(", ")}"
              )
            )
            enriched
          case None => absoluteFiles
        }
      } else {
        absoluteFiles
      }
    }

    // Convert absolute paths to relative (bucket-independent for cross-region failover).
    // E.g., s3://bucket/warehouse/db/table/data/region=us-east/file.parquet
    //     → region=us-east/file.parquet
    root match {
      case Some(basePath) =>
        val normalizedBase = basePath.stripSuffix("/")
        enrichedFiles.map { f =>
          val normalizedPath = f.path.stripSuffix("/")
          val relativePath = if (normalizedPath.startsWith(normalizedBase)) {
            normalizedPath.substring(normalizedBase.length).stripPrefix("/")
          } else {
            f.path // shouldn't happen, but keep absolute as fallback
          }
          f.copy(path = relativePath)
        }
      case None => enrichedFiles
    }
  }

  override def partitionColumns(): Seq[String] =
    getAllFiles().headOption
      .map(_.partitionValues.keys.toSeq.sorted)
      .getOrElse(Seq.empty)

  /**
   * Extract Hive-style partition values from a file path relative to a base path. E.g., for base="s3://bucket/data" and
   * path="s3://bucket/data/region=us/year=2024/file.parquet", returns Map("region" -> "us", "year" -> "2024").
   */
  private def extractPartitionValuesFromPath(filePath: String, basePath: String): Map[String, String] = {
    val normalizedFile = filePath.stripSuffix("/")
    val normalizedBase = basePath.stripSuffix("/")
    val relative = if (normalizedFile.startsWith(normalizedBase)) {
      normalizedFile.substring(normalizedBase.length).stripPrefix("/")
    } else {
      return Map.empty
    }
    // Split into components, drop the filename (last), collect key=value segments
    val components = relative.split("/").dropRight(1)
    components.flatMap { component =>
      val eqIdx = component.indexOf('=')
      if (eqIdx > 0) Some(component.substring(0, eqIdx) -> component.substring(eqIdx + 1))
      else None
    }.toMap
  }

  // Lazily read and cache the IcebergTableSchema (used by schema() and columnNameMapping())
  private lazy val icebergTableSchema = {
    logger.info(s"Reading Iceberg schema for $catalogName.$namespace.$tableName")
    snapshotId match {
      case Some(id) =>
        IcebergTableReader.readSchema(catalogName, namespace, tableName, icebergConfig, id)
      case None =>
        IcebergTableReader.readSchema(catalogName, namespace, tableName, icebergConfig)
    }
  }

  override def schema(): StructType = {
    val schemaJson = icebergTableSchema.getSchemaJson()
    val sparkSchema =
      try
        DataType.fromJson(schemaJson).asInstanceOf[StructType]
      catch {
        case _: Exception =>
          logger.info("Iceberg schema JSON is not Spark-compatible, converting from Iceberg format")
          convertIcebergSchemaToSpark(schemaJson)
      }
    logger.info(s"Iceberg schema: ${sparkSchema.fields.length} fields")
    sparkSchema
  }

  override def schemaSourceParquetFile(): Option[String] = None // Iceberg provides schema via catalog

  override def columnNameMapping(): Map[String, String] = {
    val entries = fileEntries
    if (entries.isEmpty) return Map.empty

    val sampleUrl     = entries.get(0).getPath
    val fieldIdToName = icebergTableSchema.getFieldIdToNameMap

    try {
      // Translate icebergConfig (s3.access-key-id format) to ParquetSchemaReader format
      // (aws_access_key_id format) for reading the sample parquet file's metadata
      val storageConfig = buildParquetReaderStorageConfig()

      logger.info(s"Resolving Iceberg column name mapping from sample parquet: $sampleUrl")
      val mapping = ParquetSchemaReader.readColumnMapping(sampleUrl, fieldIdToName, storageConfig)

      if (mapping != null && !mapping.isEmpty) {
        val nonIdentity = mapping.asScala.count { case (k, v) => k != v }
        if (nonIdentity > 0) {
          logger.info(s"Iceberg column name mapping: ${mapping.size} columns, $nonIdentity non-identity mappings")
        } else {
          logger.info("Iceberg column names match parquet physical names (identity mapping)")
        }
        mapping.asScala.toMap
      } else {
        Map.empty
      }
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to resolve Iceberg column name mapping from sample parquet: ${e.getMessage}. " +
            "Will rely on auto-detection from parquet metadata at indexing time."
        )
        Map.empty
    }
  }

  private def buildParquetReaderStorageConfig(): java.util.Map[String, String] =
    IcebergSourceReader.buildParquetReaderStorageConfig(icebergConfig)

  /**
   * Convert Iceberg-format schema JSON to Spark StructType. Iceberg format: {"schema-id":0, "type":"struct",
   * "fields":[{"id":1, "name":"...", "required":false, "type":"long"}]} Spark format: {"type":"struct",
   * "fields":[{"name":"...", "type":"long", "nullable":true, "metadata":{}}]}
   */
  private def convertIcebergSchemaToSpark(schemaJson: String): StructType = {
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    val root   = mapper.readTree(schemaJson)
    val fields = root.get("fields")

    val sparkFields = (0 until fields.size()).map { i =>
      val field       = fields.get(i)
      val name        = field.get("name").asText()
      val nullable    = !field.get("required").asBoolean(false)
      val icebergType = field.get("type")
      val dataType    = icebergTypeToSparkType(icebergType)
      StructField(name, dataType, nullable)
    }
    StructType(sparkFields)
  }

  private def icebergTypeToSparkType(typeNode: com.fasterxml.jackson.databind.JsonNode): DataType =
    if (typeNode.isTextual) {
      icebergPrimitiveToSpark(typeNode.asText())
    } else if (typeNode.isObject) {
      val typeName = typeNode.get("type").asText()
      typeName match {
        case "struct" =>
          val fields = typeNode.get("fields")
          val sparkFields = (0 until fields.size()).map { i =>
            val f        = fields.get(i)
            val name     = f.get("name").asText()
            val nullable = !f.get("required").asBoolean(false)
            StructField(name, icebergTypeToSparkType(f.get("type")), nullable)
          }
          StructType(sparkFields)
        case "list" =>
          val elementField    = typeNode.get("element")
          val elementNullable = !typeNode.get("element-required").asBoolean(false)
          ArrayType(icebergTypeToSparkType(elementField), elementNullable)
        case "map" =>
          val keyType       = icebergTypeToSparkType(typeNode.get("key"))
          val valueType     = icebergTypeToSparkType(typeNode.get("value"))
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

  private val decimalPattern = """decimal\((\d+),\s*(\d+)\)""".r

  private def icebergPrimitiveToSpark(icebergType: String): DataType = icebergType match {
    case "boolean"                                                       => BooleanType
    case "int" | "integer"                                               => IntegerType
    case "long"                                                          => LongType
    case "float"                                                         => FloatType
    case "double"                                                        => DoubleType
    case "string"                                                        => StringType
    case "binary" | "fixed"                                              => BinaryType
    case "date"                                                          => DateType
    case "timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns" => TimestampType
    case "time"                           => LongType // Iceberg time stored as microseconds since midnight
    case "uuid"                           => StringType
    case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    case other =>
      logger.warn(s"Unknown Iceberg type '$other', defaulting to StringType")
      StringType
  }
}
