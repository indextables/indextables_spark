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

import scala.collection.mutable

import org.apache.spark.sql.types._

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.tantivy4java.parquet.ParquetSchemaReader
import org.slf4j.LoggerFactory

/**
 * CompanionSourceReader for bare Parquet directories (no table format). Discovers parquet files via
 * CloudStorageProvider (S3, Azure, local) and extracts Hive-style partition columns from directory structure
 * (col=value/). Schema is read via tantivy4java's ParquetSchemaReader (Arrow-based, JNI).
 *
 * @param directoryPath
 *   Root directory containing parquet files
 * @param credentials
 *   Credential map (spark.indextables.* keys)
 * @param schemaSourcePath
 *   Optional path to a specific parquet file for schema extraction
 */
class ParquetDirectoryReader(
  directoryPath: String,
  credentials: Map[String, String],
  schemaSourcePath: Option[String] = None)
    extends CompanionSourceReader {

  private val logger = LoggerFactory.getLogger(classOf[ParquetDirectoryReader])

  // Translate credentials for tantivy4java's ParquetSchemaReader (same format as DeltaLogReader)
  private val nativeConfig: java.util.Map[String, String] = {
    val config = new java.util.HashMap[String, String]()
    credentials
      .get("spark.indextables.aws.accessKey")
      .foreach(v => config.put("aws_access_key_id", v))
    credentials
      .get("spark.indextables.aws.secretKey")
      .foreach(v => config.put("aws_secret_access_key", v))
    credentials
      .get("spark.indextables.aws.sessionToken")
      .foreach(v => config.put("aws_session_token", v))
    credentials
      .get("spark.indextables.aws.region")
      .foreach(v => config.put("aws_region", v))
    credentials
      .get("spark.indextables.azure.accountName")
      .foreach(v => config.put("azure_account_name", v))
    credentials
      .get("spark.indextables.azure.accountKey")
      .foreach(v => config.put("azure_access_key", v))
    config
  }

  // Lazily discover files and cache
  private lazy val discoveredFiles: Seq[CompanionSourceFile] = discoverFiles()

  // Lazily discover partition columns from directory structure
  private lazy val discoveredPartitionColumns: Seq[String] = discoverPartitionColumns()

  // Lazily read and cache schema
  private lazy val discoveredSchema: StructType = readSchema()

  override def sourceVersion(): Option[Long] = None // Parquet directories are unversioned

  override def getAllFiles(): Seq[CompanionSourceFile] = discoveredFiles

  override def partitionColumns(): Seq[String] = discoveredPartitionColumns

  override def schema(): StructType = {
    // Augment parquet schema with Hive-style partition columns that exist only
    // in the directory structure, not in the parquet file itself.
    val baseSchema      = discoveredSchema
    val partCols        = discoveredPartitionColumns
    val missingPartCols = partCols.filterNot(c => baseSchema.fieldNames.contains(c))
    if (missingPartCols.isEmpty) {
      baseSchema
    } else {
      val partFields = missingPartCols.map(c => StructField(c, StringType, nullable = true))
      StructType(baseSchema.fields ++ partFields)
    }
  }

  override def schemaSourceParquetFile(): Option[String] =
    schemaSourcePath.orElse(discoveredFiles.headOption.map { f =>
      // Resolve relative path to absolute for ParquetSchemaReader
      val root = directoryPath.stripSuffix("/")
      if (
        f.path.startsWith("/") || f.path.startsWith("s3://") || f.path.startsWith("s3a://") ||
        f.path.startsWith("abfss://") || f.path.startsWith("wasbs://") || f.path.startsWith("abfs://")
      ) {
        f.path
      } else {
        s"$root/${f.path}"
      }
    })

  /**
   * Discover all *.parquet files using CloudStorageProvider for listing. Extracts Hive-style partition values from the
   * path structure.
   */
  private def discoverFiles(): Seq[CompanionSourceFile] = {
    val provider = CloudStorageProviderFactory.createProvider(directoryPath, credentials)
    try {
      val rootPath = directoryPath.stripSuffix("/")
      val allFiles = provider.listFiles(rootPath, recursive = true)

      val parquetFiles = allFiles
        .filter(f => !f.isDirectory && isParquetFile(f.path))
        .map { fileInfo =>
          val strippedPath    = stripFileScheme(fileInfo.path)
          val relativePath    = extractRelativePath(strippedPath, rootPath)
          val partitionValues = extractPartitionValuesFromRelative(relativePath)
          CompanionSourceFile(
            path = relativePath,
            partitionValues = partitionValues,
            size = fileInfo.size
          )
        }

      logger.info(s"Discovered ${parquetFiles.size} parquet files in $directoryPath")
      parquetFiles
    } finally
      provider.close()
  }

  /**
   * Extract relative path from an absolute path given the root. Handles scheme differences (e.g., s3:// vs s3a://) by
   * normalizing both paths.
   */
  private def extractRelativePath(absolutePath: String, rootPath: String): String = {
    val normalizedAbs  = normalizeCloudScheme(absolutePath.stripSuffix("/"))
    val normalizedRoot = normalizeCloudScheme(rootPath.stripSuffix("/"))
    if (normalizedAbs.startsWith(normalizedRoot)) {
      normalizedAbs.substring(normalizedRoot.length).stripPrefix("/")
    } else {
      // Fallback: find the common suffix by bucket/key structure
      val absKey  = extractObjectKey(absolutePath)
      val rootKey = extractObjectKey(rootPath)
      if (absKey.startsWith(rootKey)) {
        absKey.substring(rootKey.length).stripPrefix("/")
      } else {
        // Last resort: extract filename only
        logger.warn(s"Cannot extract relative path from '$absolutePath' with root '$rootPath', using filename only")
        absolutePath.substring(absolutePath.lastIndexOf('/') + 1)
      }
    }
  }

  /** Normalize cloud storage scheme for path comparison. */
  private def normalizeCloudScheme(path: String): String =
    if (path.startsWith("s3://")) "s3a://" + path.substring(5)
    else if (path.startsWith("abfs://")) "abfss://" + path.substring(7)
    else path

  /** Extract object key (path after bucket) for scheme-agnostic comparison. */
  private def extractObjectKey(path: String): String = {
    val stripped = path
      .replaceFirst("^s3a?://[^/]+/", "")
      .replaceFirst("^abfss?://[^/]+/", "")
      .replaceFirst("^wasbs?://[^/]+/", "")
      .replaceFirst("^azure://[^/]+/", "")
    stripped.stripSuffix("/")
  }

  private def isParquetFile(path: String): Boolean = {
    val name = path.substring(path.lastIndexOf('/') + 1).toLowerCase
    (name.endsWith(".parquet") || name.endsWith(".parq")) &&
    !name.startsWith("_") && !name.startsWith(".")
  }

  /**
   * Extract Hive-style partition values from a relative path. E.g., "year=2024/month=01/file.parquet" -> Map("year" ->
   * "2024", "month" -> "01")
   */
  private def extractPartitionValuesFromRelative(relativePath: String): Map[String, String] = {
    val partitions = mutable.LinkedHashMap[String, String]()
    // Split into components, excluding the filename
    val components = relativePath.split("/").dropRight(1)
    components.foreach { component =>
      val eqIdx = component.indexOf('=')
      if (eqIdx > 0) {
        val key   = component.substring(0, eqIdx)
        val value = component.substring(eqIdx + 1)
        partitions += (key -> value)
      }
    }
    partitions.toMap
  }

  /** Discover partition columns from the first file's relative path. */
  private def discoverPartitionColumns(): Seq[String] =
    discoveredFiles.headOption
      .map(_.partitionValues.keys.toSeq.sorted)
      .getOrElse(Seq.empty)

  /**
   * Read schema from a single parquet file via tantivy4java's ParquetSchemaReader.
   *
   * ParquetSchemaReader returns an Arrow-based schema JSON that isn't directly compatible with Spark's
   * DataType.fromJson(). For Spark-written parquet files, the Spark schema is embedded in parquet key-value metadata
   * under "org.apache.spark.sql.parquet.row.metadata". We extract and use that if present; otherwise, we convert the
   * Arrow format to Spark format.
   */
  private def readSchema(): StructType = {
    val schemaFile = schemaSourceParquetFile().getOrElse(
      throw new IllegalStateException(s"No parquet files found in $directoryPath for schema extraction")
    )

    // For wasbs:// paths, normalize to az:// for Rust's object_store crate.
    // tantivy4java 0.29.6+ uses HEAD + .with_file_size() to avoid suffix range requests.
    val effectiveSchemaFile = normalizeForObjectStore(schemaFile)
    logger.info(s"Reading schema from parquet file via ParquetSchemaReader: $effectiveSchemaFile")
    val parquetSchema = ParquetSchemaReader.readSchema(effectiveSchemaFile, nativeConfig)
    val schemaJson    = parquetSchema.getSchemaJson()

    // Try to extract the Spark schema from parquet metadata first
    val sparkSchema = extractSparkSchemaFromMetadata(schemaJson)
      .getOrElse {
        // Fallback: convert Arrow-format schema JSON to Spark StructType
        convertArrowSchemaToSpark(schemaJson)
      }

    logger.info(s"Parquet schema: ${sparkSchema.fields.length} fields")
    sparkSchema
  }

  /**
   * Extract Spark schema from parquet key-value metadata if present. Spark writes its schema under
   * "org.apache.spark.sql.parquet.row.metadata".
   */
  private def extractSparkSchemaFromMetadata(schemaJson: String): Option[StructType] =
    try {
      import com.fasterxml.jackson.databind.ObjectMapper
      val mapper   = new ObjectMapper()
      val root     = mapper.readTree(schemaJson)
      val metadata = root.get("metadata")
      if (metadata != null) {
        val sparkMeta = metadata.get("org.apache.spark.sql.parquet.row.metadata")
        if (sparkMeta != null) {
          val sparkSchemaJson = sparkMeta.asText()
          return Some(DataType.fromJson(sparkSchemaJson).asInstanceOf[StructType])
        }
      }
      None
    } catch {
      case e: Exception =>
        logger.debug(s"Could not extract Spark schema from metadata: ${e.getMessage}")
        None
    }

  /**
   * Convert Arrow-format schema JSON to Spark StructType. Arrow format: {"fields": [{"name": "...", "data_type":
   * "int64", "nullable": ...}]} Spark format: {"type": "struct", "fields": [{"name": "...", "type": "long", ...}]}
   */
  private def convertArrowSchemaToSpark(schemaJson: String): StructType = {
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    val root   = mapper.readTree(schemaJson)
    val fields = root.get("fields")

    val sparkFields = (0 until fields.size()).map { i =>
      val field    = fields.get(i)
      val name     = field.get("name").asText()
      val dataType = field.get("data_type").asText()
      val nullable = field.get("nullable").asBoolean(true)
      StructField(name, arrowTypeToSparkType(dataType), nullable)
    }
    StructType(sparkFields)
  }

  private val decimalPattern = """decimal\((\d+),\s*(\d+)\)""".r

  private def arrowTypeToSparkType(arrowType: String): DataType = arrowType.toLowerCase match {
    case "boolean" | "bool"               => BooleanType
    case "int8"                           => ByteType
    case "int16" | "short"                => ShortType
    case "int32" | "int" | "integer"      => IntegerType
    case "int64" | "long" | "bigint"      => LongType
    case "float" | "float32"              => FloatType
    case "double" | "float64"             => DoubleType
    case "string" | "utf8" | "large_utf8" => StringType
    case "binary" | "large_binary"        => BinaryType
    case "date32" | "date"                => DateType
    case "timestamp" | "timestamp_us" | "timestamp_ms" | "timestamp_ns" | "timestamp[us]" | "timestamp[ms]" |
        "timestamp[ns]" | "timestamp[us, tz=utc]" | "timestamp[ms, tz=utc]" | "timestamp[ns, tz=utc]" =>
      TimestampType
    case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    case "decimal128" | "decimal256"      => DecimalType(38, 18)
    case other =>
      logger.warn(s"Unknown Arrow type '$other', defaulting to StringType")
      StringType
  }

  /**
   * Normalize URL scheme for Rust's object_store crate. wasbs:// is not recognized by object_store; convert to az://
   * (Azure Blob). Same normalization as DeltaLogReader.normalizeForDeltaKernel().
   */
  private def normalizeForObjectStore(path: String): String = {
    val wasbsRegex = """^wasbs?://([^@]+)@[^/]+(?:/(.*))?$""".r
    path match {
      case wasbsRegex(container, rest) =>
        if (rest != null && rest.nonEmpty) s"az://$container/$rest" else s"az://$container"
      case _ => path
    }
  }

  /**
   * Strip `file:` URI scheme prefix from local filesystem paths. CloudStorageProvider returns paths with `file:` prefix
   * on local filesystem, but tantivy4java expects plain filesystem paths.
   */
  private def stripFileScheme(path: String): String =
    if (path.startsWith("file:///")) path.substring(7)
    else if (path.startsWith("file:/")) path.substring(5)
    else path
}
