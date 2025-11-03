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

package io.indextables.spark.search

import java.io.File
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import io.indextables.tantivy4java.batch.{BatchDocument, BatchDocumentBuilder}
import io.indextables.tantivy4java.core.{Document, Index, IndexWriter, Schema, SchemaBuilder, Tantivy}
import io.indextables.spark.json.{SparkSchemaToTantivyMapper, SparkToTantivyConverter}
import org.slf4j.LoggerFactory

/**
 * Direct tantivy4java interface that eliminates Long handles and thread safety issues. Each instance manages its own
 * Index and IndexWriter without cross-thread sharing. This is designed for per-executor usage in Spark where each
 * executor writes separate index files.
 */
object TantivyDirectInterface {
  private val initLock              = new Object()
  @volatile private var initialized = false
  private val logger                = LoggerFactory.getLogger(TantivyDirectInterface.getClass)

  // Global lock to prevent concurrent schema creation and field ID conflicts
  private val schemaCreationLock = new Object()

  private def ensureInitialized(): Unit =
    if (!initialized) {
      initLock.synchronized {
        if (!initialized) {
          Tantivy.initialize()
          initialized = true
        }
      }
    }

  // Thread-safe schema creation to prevent "Field already exists in schema id" race conditions
  /**
   * Auto-configure fast fields based on field types if none are explicitly configured. By default, makes all numeric,
   * string (raw tokenizer), and date fields fast unless:
   *   1. Fast fields are explicitly configured 2. Non-fast fields are explicitly configured to exclude specific fields
   */
  private def autoConfigureFastFields(
    sparkSchema: StructType,
    options: org.apache.spark.sql.util.CaseInsensitiveStringMap,
    tantivyOptions: io.indextables.spark.core.IndexTables4SparkOptions
  ): org.apache.spark.sql.util.CaseInsensitiveStringMap = {

    val currentFastFields    = tantivyOptions.getFastFields
    val currentNonFastFields = tantivyOptions.getNonFastFields

    // DEBUG: WHERE IS THE CONFIGURATION COMING FROM?
    logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: currentFastFields = ${currentFastFields.mkString(", ")}")
    logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: currentNonFastFields = ${currentNonFastFields.mkString(", ")}")
    logger.debug(
      s"🔍 AUTO-FAST-FIELD DEBUG: options raw value = ${options.get("spark.indextables.indexing.fastfields")}"
    )
    logger.debug(
      s"🔍 AUTO-FAST-FIELD DEBUG: ALL options keys = ${options.entrySet().asScala.map(_.getKey).mkString(", ")}"
    )

    // If fast fields are already configured, return original options
    if (currentFastFields.nonEmpty) {
      logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: Fast fields already configured, skipping auto-configuration")
      return options
    }

    logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: No fast fields configured, checking schema for eligible fields...")

    // Find all fields that should be fast by default
    val defaultFastFields = sparkSchema.fields
      .filter { field =>
        field.dataType match {
          // Numeric, boolean, and date fields are fast by default
          case org.apache.spark.sql.types.IntegerType | org.apache.spark.sql.types.LongType |
              org.apache.spark.sql.types.FloatType | org.apache.spark.sql.types.DoubleType |
              org.apache.spark.sql.types.BooleanType | org.apache.spark.sql.types.DateType |
              org.apache.spark.sql.types.TimestampType =>
            true
          // String fields with raw tokenizer (default behavior) are fast by default
          // Text fields with default tokenizer (explicit "text" type) are NOT fast by default
          case org.apache.spark.sql.types.StringType =>
            val fieldTypeOverride = tantivyOptions.getFieldTypeMapping.get(field.name)
            // String fields (default) should be fast by default
            // If no explicit configuration, it defaults to "string" (raw tokenizer) and should be fast
            // Only explicitly configured "text" fields should NOT be fast
            !fieldTypeOverride.contains("text")
          case _ => false
        }
      }
      .map(_.name)
      .toSet

    // Remove any fields explicitly configured as non-fast
    val finalFastFields = defaultFastFields -- currentNonFastFields

    logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: defaultFastFields = ${defaultFastFields.mkString(", ")}")
    logger.debug(s"🔍 AUTO-FAST-FIELD DEBUG: finalFastFields = ${finalFastFields.mkString(", ")}")

    if (finalFastFields.nonEmpty) {
      logger.info(s"🔧 AUTO-FAST-FIELD: No fast fields configured, automatically making fields fast by default: ${finalFastFields.mkString(", ")}")
      if (currentNonFastFields.nonEmpty) {
        logger.info(s"🔧 AUTO-FAST-FIELD: Excluding non-fast fields: ${currentNonFastFields.mkString(", ")}")
      }

      // Create new options map with auto-configured fast fields
      import scala.jdk.CollectionConverters._
      val existingOptions = options.asCaseSensitiveMap().asScala.toMap
      val newOptions      = existingOptions + ("spark.indextables.indexing.fastfields" -> finalFastFields.mkString(","))

      new org.apache.spark.sql.util.CaseInsensitiveStringMap(newOptions.asJava)
    } else {
      logger.debug("No fields qualify for auto-fast-field configuration after applying exclusions")
      options
    }
  }

  private def createSchemaThreadSafe(
    sparkSchema: StructType,
    options: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): (io.indextables.tantivy4java.core.Schema, SchemaBuilder) =
    schemaCreationLock.synchronized {
      logger.debug(s"🔍 CREATE SCHEMA CALLED: Creating schema with ${sparkSchema.fields.length} fields (thread: ${Thread.currentThread().getName})")
      logger.debug(
        s"Creating schema with ${sparkSchema.fields.length} fields (thread: ${Thread.currentThread().getName})"
      )

      val builder        = new SchemaBuilder()
      val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(options)

      // Auto-configure fast fields if none are configured
      val autoConfiguredOptions = autoConfigureFastFields(sparkSchema, options, tantivyOptions)

      // DEBUG: Verify what's in the options map after auto-configuration
      import scala.jdk.CollectionConverters._
      val autoConfiguredMap = autoConfiguredOptions.asCaseSensitiveMap().asScala.toMap
      logger.debug(s"🔍 AUTO-CONFIG MAP DEBUG: Keys in autoConfiguredOptions: ${autoConfiguredMap.keys.mkString(", ")}")
      logger.debug(s"🔍 AUTO-CONFIG MAP DEBUG: fastfields value = ${autoConfiguredMap.get("spark.indextables.indexing.fastfields")}")

      val finalTantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(autoConfiguredOptions)

      // DEBUG: Verify what getFastFields returns
      val retrievedFastFields = finalTantivyOptions.getFastFields
      logger.debug(s"🔍 FINAL OPTIONS DEBUG: getFastFields() returned: ${retrievedFastFields.mkString(", ")}")

      // Create JSON field mapper for automatic JSON field detection
      val jsonFieldMapper = new SparkSchemaToTantivyMapper(finalTantivyOptions)

      // Validate JSON field configuration before building schema
      jsonFieldMapper.validateJsonFieldConfiguration(sparkSchema)

      sparkSchema.fields.foreach { field =>
        val fieldName      = field.name
        val fieldType      = field.dataType
        val indexingConfig = finalTantivyOptions.getFieldIndexingConfig(fieldName)

        logger.debug(s"🔍 FIELD CONFIG DEBUG: Adding field: $fieldName of type: $fieldType with config: $indexingConfig")

        // Validate conflicting configurations
        if (indexingConfig.isStoreOnly && indexingConfig.isIndexOnly) {
          throw new IllegalArgumentException(s"Field $fieldName cannot be both store-only and index-only")
        }

        // Determine storage and indexing flags
        val stored  = !indexingConfig.isIndexOnly
        val indexed = !indexingConfig.isStoreOnly
        val fast    = indexingConfig.isFast

        logger.debug(s"🔍 BUILDER CALL DEBUG: Field $fieldName: stored=$stored, indexed=$indexed, fast=$fast")

        // Check if this field should use JSON field type
        val shouldUseJson = jsonFieldMapper.shouldUseJsonField(field)

        if (shouldUseJson) {
          // Use JSON field for Struct, Array, or explicitly configured StringType
          logger.debug(s"🔍 CALLING addJsonField: name=$fieldName (detected as JSON field)")
          val tokenizer = indexingConfig.tokenizerOverride.getOrElse("default")
          builder.addJsonField(fieldName, stored, tokenizer, "position")
        } else {
          // Handle non-JSON fields normally
          fieldType match {
            case org.apache.spark.sql.types.StringType =>
              // Default to "string" (raw tokenizer for exact matching). Only explicit "text" config uses default tokenizer.
              val fieldTypeOverride = indexingConfig.fieldType.getOrElse("string")

              // Store-only fields: tantivy4java now properly supports store-only string fields
              // after the "keyword" type bug was fixed in the schema conversion logic
              fieldTypeOverride match {
                case "string" =>
                  logger.debug(s"🔍 CALLING addStringField: name=$fieldName, stored=$stored, indexed=$indexed, fast=$fast")
                  builder.addStringField(fieldName, stored, indexed, fast)
                case "text" =>
                  val tokenizer = indexingConfig.tokenizerOverride.getOrElse("default")
                  builder.addTextField(fieldName, stored, fast, tokenizer, "position")
                case other =>
                  throw new IllegalArgumentException(
                    s"Unsupported field type override for field $fieldName: $other. Supported types: string, text"
                  )
              }
          case org.apache.spark.sql.types.LongType | org.apache.spark.sql.types.IntegerType =>
            logger.debug(s"🔍 CALLING addIntegerField: name=$fieldName, stored=$stored, indexed=$indexed, fast=$fast")
            builder.addIntegerField(fieldName, stored, indexed, fast)
          case org.apache.spark.sql.types.DoubleType | org.apache.spark.sql.types.FloatType =>
            builder.addFloatField(fieldName, stored, indexed, fast)
          case org.apache.spark.sql.types.BooleanType =>
            builder.addBooleanField(fieldName, stored, indexed, fast)
          case org.apache.spark.sql.types.BinaryType =>
            builder.addBytesField(fieldName, stored, indexed, fast, "position")
            case org.apache.spark.sql.types.TimestampType =>
              builder.addIntegerField(fieldName, stored, indexed, fast) // Store as microseconds since epoch
            case org.apache.spark.sql.types.DateType =>
              builder.addDateField(fieldName, stored, indexed, fast) // Use proper date field
            case org.apache.spark.sql.types.StructType(_) | org.apache.spark.sql.types.ArrayType(_, _) =>
              // This should have been caught by shouldUseJsonField check above
              throw new IllegalStateException(
                s"Field $fieldName has complex type $fieldType but was not mapped to JSON field. This is a bug."
              )
            case _ =>
              throw new UnsupportedOperationException(
                s"Unsupported field type for field $fieldName: $fieldType"
              )
          }
        }
      }

      val tantivySchema = builder.build()
      logger.debug(s"🔍 CREATE SCHEMA COMPLETED: Built schema with ${sparkSchema.fields.length} fields using new indexing configuration")
      logger.info(s"Successfully built schema with ${sparkSchema.fields.length} fields using new indexing configuration")

      (tantivySchema, builder)
    }

  // Note: fromIndexComponents and extractZipArchive removed - using splits instead of ZIP archives
}

class TantivyDirectInterface(
  val schema: StructType,
  restoredIndexPath: Option[Path] = None,
  options: org.apache.spark.sql.util.CaseInsensitiveStringMap =
    new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
  hadoopConf: org.apache.hadoop.conf.Configuration = new org.apache.hadoop.conf.Configuration(),
  existingDocMappingJson: Option[String] = None,
  workingDirectory: Option[String] = None)
    extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivyDirectInterface])

  // Initialize tantivy4java library only once
  TantivyDirectInterface.ensureInitialized()

  // Create JSON field mapper and converter for handling complex types
  private val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(options)
  private val jsonFieldMapper = new SparkSchemaToTantivyMapper(tantivyOptions)
  private val jsonConverter = new SparkToTantivyConverter(schema, jsonFieldMapper)

  // Validate indexing configuration against existing table if provided
  if (existingDocMappingJson.isDefined) {
    logger.debug(s"🔍 VALIDATION DEBUG: Running indexing configuration validation")
    validateIndexingConfiguration(existingDocMappingJson.get)
  } else {
    logger.debug(s"🔍 VALIDATION DEBUG: Skipping validation - no existing doc mapping provided")
  }

  // Keep schemaBuilder alive for the lifetime of this interface
  private var schemaBuilder: Option[SchemaBuilder] = None

  // Flag to prevent cleanup until split is created
  @volatile private var delayCleanup = false

  /**
   * Validate that the current indexing configuration matches the existing table configuration. This prevents schema
   * mismatches when writing to existing tables.
   */
  private def validateIndexingConfiguration(existingDocMapping: String): Unit =
    try {
      import com.fasterxml.jackson.databind.JsonNode
      import io.indextables.spark.util.JsonUtil
      import scala.jdk.CollectionConverters._

      // Parse existing doc mapping JSON to extract field configurations
      val existingMapping = JsonUtil.mapper.readTree(existingDocMapping)
      val existingFields  = Option(existingMapping.get("fields")).map(_.asInstanceOf[JsonNode])

      if (existingFields.isEmpty) {
        logger.warn("Existing docMappingJson does not contain 'fields' section - skipping validation")
        return
      }

      val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(options)
      val errors         = scala.collection.mutable.ListBuffer[String]()

      // For each field in the current schema, check if configuration matches existing
      schema.fields.foreach { field =>
        val fieldName           = field.name
        val currentConfig       = tantivyOptions.getFieldIndexingConfig(fieldName)
        val existingFieldConfig = Option(existingFields.get.get(fieldName))

        if (existingFieldConfig.isDefined) {
          val existing        = existingFieldConfig.get
          val existingType    = Option(existing.get("type")).map(_.asText())
          val existingIndexed = Option(existing.get("indexed")).exists(_.asBoolean())
          val existingStored  = Option(existing.get("stored")).exists(_.asBoolean())
          val existingFast    = Option(existing.get("fast")).exists(_.asBoolean())

          // Check field type configuration
          if (currentConfig.fieldType.isDefined) {
            val expectedTantivyType = currentConfig.fieldType.get match {
              case "string" => "text" // String fields map to text in tantivy
              case "text"   => "text"
              case "json"   => "json"
              case other    => other
            }
            if (existingType.contains(expectedTantivyType) == false) {
              errors += s"Field '$fieldName' type mismatch: configured as '${currentConfig.fieldType.get}' but existing table has '${existingType
                  .getOrElse("unknown")}'"
            }
          }

          // Check storage/indexing configuration
          val expectedStored  = !currentConfig.isIndexOnly
          val expectedIndexed = !currentConfig.isStoreOnly
          if (existingStored != expectedStored) {
            errors += s"Field '$fieldName' storage mismatch: configured stored=$expectedStored but existing table has stored=$existingStored"
          }
          if (existingIndexed != expectedIndexed) {
            errors += s"Field '$fieldName' indexing mismatch: configured indexed=$expectedIndexed but existing table has indexed=$existingIndexed"
          }

          // Check fast fields configuration
          if (currentConfig.isFast != existingFast) {
            errors += s"Field '$fieldName' fast field mismatch: configured fast=${currentConfig.isFast} but existing table has fast=$existingFast"
          }
        }
      }

      if (errors.nonEmpty) {
        val errorMessage = s"Indexing configuration mismatch with existing table:\n${errors.mkString("\n")}\n\n" +
          "To fix this, either:\n" +
          "1. Remove the conflicting indexing options to use the existing table configuration, or\n" +
          "2. Create a new table with the desired indexing configuration"
        throw new IllegalArgumentException(errorMessage)
      }

      logger.info("Indexing configuration validation passed - matches existing table configuration")

    } catch {
      case ex: IllegalArgumentException => throw ex // Re-throw validation errors
      case ex: Exception =>
        logger.warn(s"Failed to validate indexing configuration against existing table: ${ex.getMessage}", ex)
      // Don't fail the operation for parsing errors - just log warning
    }

  // Configuration resolution with proper hierarchy: options -> table props -> spark props -> defaults
  private def getConfigValue(key: String, defaultValue: String): String = {
    import io.indextables.spark.config.IndexTables4SparkSQLConf._

    // First try options (highest precedence)
    Option(options.get(key)).filter(_.nonEmpty).getOrElse {
      // Then try hadoop conf (includes table properties)
      Option(hadoopConf.get(key)).filter(_.nonEmpty).getOrElse {
        // Then try spark session config (if available)
        try {
          val sparkSession = org.apache.spark.sql.SparkSession.active
          sparkSession.conf.getOption(key).getOrElse(defaultValue)
        } catch {
          case _: Exception => defaultValue
        }
      }
    }
  }

  private def getConfigValueLong(key: String, defaultValue: Long): Long =
    try
      getConfigValue(key, defaultValue.toString).toLong
    catch {
      case _: NumberFormatException =>
        logger.warn(s"Invalid numeric value for $key, using default: $defaultValue")
        defaultValue
    }

  private def getConfigValueInt(key: String, defaultValue: Int): Int =
    try
      getConfigValue(key, defaultValue.toString).toInt
    catch {
      case _: NumberFormatException =>
        logger.warn(s"Invalid numeric value for $key, using default: $defaultValue")
        defaultValue
    }

  /**
   * Parse a size value that can include suffixes like K, M, G, T Examples: "100M", "2G", "512K", "1000000" (raw bytes)
   */
  private def parseSize(value: String): Long = {
    val trimmed = value.trim.toUpperCase

    if (trimmed.matches("\\d+[KMGT]?")) {
      val (numberPart, suffix) = if (trimmed.last.isLetter) {
        (trimmed.dropRight(1), trimmed.last.toString)
      } else {
        (trimmed, "")
      }

      val baseValue = numberPart.toLong
      suffix match {
        case "K" => baseValue * 1024L
        case "M" => baseValue * 1024L * 1024L
        case "G" => baseValue * 1024L * 1024L * 1024L
        case "T" => baseValue * 1024L * 1024L * 1024L * 1024L
        case ""  => baseValue // Raw bytes
        case _   => throw new IllegalArgumentException(s"Unknown size suffix: $suffix")
      }
    } else {
      throw new IllegalArgumentException(s"Invalid size format: $value")
    }
  }

  private def getConfigValueSize(key: String, defaultValue: Long): Long =
    try {
      val stringValue = getConfigValue(key, defaultValue.toString)
      parseSize(stringValue)
    } catch {
      case e: Exception =>
        logger.warn(s"Invalid size value for $key: ${e.getMessage}, using default: $defaultValue")
        defaultValue
    }

  // Resolve index writer configuration
  private val heapSize = getConfigValueSize("spark.indextables.indexWriter.heapSize", 100L * 1024 * 1024) // 100MB default
  private val threadCount = getConfigValueInt("spark.indextables.indexWriter.threads", 2) // 2 threads default
  private val batchSize = getConfigValueInt("spark.indextables.indexWriter.batchSize", 10000) // 10,000 records default
  private val useBatch = getConfigValue("spark.indextables.indexWriter.useBatch", "true").toBoolean // Use batch by default

  logger.info(s"Index writer configuration: heapSize=$heapSize bytes, threadCount=$threadCount, batchSize=$batchSize, useBatch=$useBatch")

  // Create appropriate index and schema based on whether this is a restored index or new one
  private val (index, tempIndexDir, needsCleanup, tantivySchema) = restoredIndexPath match {
    case Some(existingPath) =>
      // Open existing index from restored path following tantivy4java pattern
      val indexPath = existingPath.toAbsolutePath.toString
      logger.info(s"Opening existing tantivy index from: $indexPath")

      // Check if index exists first (following tantivy4java pattern)
      if (!Index.exists(indexPath)) {
        logger.error(s"Index does not exist at path: $indexPath")
        throw new IllegalStateException(s"Index does not exist at path: $indexPath")
      }

      val idx = Index.open(indexPath)
      // CRITICAL: Use the schema that's already stored in the index files
      // Do NOT create a new schema - this could cause field mismatches
      val tantivySchema = idx.getSchema()

      // CRITICAL: Reload the index after opening from extracted files
      // This ensures the index sees all committed documents
      idx.reload()
      logger.info(s"Successfully opened existing index, using stored schema, and reloaded")

      // Check documents are visible after reload
      Using.resource(idx.searcher()) { searcher =>
        val numDocs = searcher.getNumDocs()
        logger.info(s"Restored index contains $numDocs documents after reload")
        if (numDocs == 0) {
          logger.error(s"CRITICAL: Restored index has 0 documents after reload - this indicates a restoration problem")
        }
      }

      (idx, existingPath, true, tantivySchema) // Clean up extracted directory

    case None =>
      // Create new index in temporary directory following tantivy4java exact pattern
      val tempDir = workingDirectory match {
        case Some(customWorkDir) =>
          // Use custom working directory if specified
          val workDir = new File(customWorkDir)
          if (!workDir.exists()) {
            // Attempt to create the directory
            if (workDir.mkdirs()) {
              logger.info(s"Created custom working directory: $customWorkDir")
              Files.createTempDirectory(workDir.toPath, "tantivy4spark_idx_")
            } else {
              logger.warn(
                s"Failed to create custom working directory: $customWorkDir - falling back to system temp directory"
              )
              Files.createTempDirectory("tantivy4spark_idx_")
            }
          } else if (!workDir.isDirectory()) {
            logger.warn(s"Custom working directory path is not a directory: $customWorkDir - falling back to system temp directory")
            Files.createTempDirectory("tantivy4spark_idx_")
          } else if (!workDir.canWrite()) {
            logger.warn(
              s"Custom working directory is not writable: $customWorkDir - falling back to system temp directory"
            )
            Files.createTempDirectory("tantivy4spark_idx_")
          } else {
            logger.info(s"Using custom working directory: $customWorkDir")
            Files.createTempDirectory(workDir.toPath, "tantivy4spark_idx_")
          }
        case None =>
          // Use system default temporary directory
          Files.createTempDirectory("tantivy4spark_idx_")
      }

      logger.info(s"Creating new tantivy index at: ${tempDir.toAbsolutePath}")

      // Synchronize schema creation to prevent race conditions in field ID generation
      val (tantivySchema, builder) = TantivyDirectInterface.createSchemaThreadSafe(schema, options)
      schemaBuilder = Some(builder) // Store for cleanup later

      val idx = new Index(tantivySchema, tempDir.toAbsolutePath.toString, false)
      (idx, tempDir, true, tantivySchema)
  }

  // Use ThreadLocal to ensure each Spark task gets its own IndexWriter - no sharing between tasks
  private val threadLocalWriter = new ThreadLocal[IndexWriter]()

  // Batch writing support - ThreadLocal to ensure each Spark task gets its own batch
  private val threadLocalBatch      = new ThreadLocal[BatchDocumentBuilder]()
  private val threadLocalBatchCount = new ThreadLocal[Integer]()

  private def getOrCreateBatch(): BatchDocumentBuilder =
    Option(threadLocalBatch.get()) match {
      case Some(batch) => batch
      case None =>
        val batch = new BatchDocumentBuilder()
        threadLocalBatch.set(batch)
        threadLocalBatchCount.set(0)
        logger.debug(s"Created new BatchDocumentBuilder for Spark task thread ${Thread.currentThread().getName}")
        batch
    }

  private def flushBatchIfNeeded(forceBatch: Boolean = false): Unit = {
    val batch      = threadLocalBatch.get()
    val count: Int = Option(threadLocalBatchCount.get()).map(_.intValue()).getOrElse(0)

    if (batch != null && (count >= batchSize || forceBatch) && count > 0) {
      val writer = getOrCreateWriter()
      writer.addDocumentsBatch(batch)
      logger.debug(s"Flushed batch with $count documents")

      // Reset batch
      threadLocalBatch.set(new BatchDocumentBuilder())
      threadLocalBatchCount.set(0)
    }
  }

  private def getOrCreateWriter(): IndexWriter =
    Option(threadLocalWriter.get()) match {
      case Some(writer) => writer
      case None =>
        val heapSizeInt = Math.min(heapSize, Int.MaxValue).toInt // Convert to Int, capping at Int.MaxValue
        val writer      = index.writer(heapSizeInt, threadCount)
        threadLocalWriter.set(writer)
        logger.debug(s"Created new IndexWriter for Spark task thread ${Thread.currentThread().getName}")
        writer
    }

  def addDocument(row: InternalRow): Unit =
    if (useBatch) {
      addDocumentBatch(row)
    } else {
      addDocumentIndividual(row)
    }

  private def addDocumentIndividual(row: InternalRow): Unit = {
    // Each Spark task gets its own IndexWriter via ThreadLocal - no sharing between tasks
    val document = new Document()

    try {
      // Convert InternalRow directly to Document without JSON - protect against field processing errors
      logger.debug(s"Adding document with ${schema.fields.length} Spark schema fields")
      schema.fields.zipWithIndex.foreach {
        case (field, index) =>
          try {
            val value = row.get(index, field.dataType)
            if (value != null) {
              logger.debug(s"Adding field ${field.name} (type: ${field.dataType}) with value: $value")
              addFieldToDocument(document, field.name, value, field.dataType)
            } else {
              logger.debug(s"Skipping null field ${field.name}")
            }
          } catch {
            case ex: Exception =>
              logger.error(
                s"Failed to add field ${field.name} (type: ${field.dataType}) at index $index: ${ex.getMessage}"
              )
              logger.error(s"Document created from tantivy schema, Spark schema has ${schema.fields.length} fields")
              logger.error(s"Available tantivy schema: $tantivySchema")
              throw ex
          }
      }

      val writer = getOrCreateWriter()
      writer.addDocument(document)
      logger.debug(s"Added document to index using task-local writer")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to add document: ${ex.getMessage}")
        throw ex
    } finally
      // Only close the document - keep writer alive for more documents in this task
      try
        document.close()
      catch {
        case ex: Exception =>
          logger.warn(s"Failed to close document: ${ex.getMessage}")
      }
  }

  private def addDocumentBatch(row: InternalRow): Unit =
    try {
      val batchDocument = new BatchDocument()

      // Convert InternalRow to BatchDocument
      schema.fields.zipWithIndex.foreach {
        case (field, index) =>
          try {
            val value = row.get(index, field.dataType)
            if (value != null) {
              addFieldToBatchDocument(batchDocument, field.name, value, field.dataType)
            }
          } catch {
            case ex: Exception =>
              logger.error(
                s"Failed to add field ${field.name} (type: ${field.dataType}) at index $index: ${ex.getMessage}"
              )
              throw ex
          }
      }

      val batch = getOrCreateBatch()
      batch.addDocument(batchDocument)

      val currentCount = Option(threadLocalBatchCount.get()).map(_.intValue()).getOrElse(0) + 1
      threadLocalBatchCount.set(currentCount)
      logger.debug(s"Added document to batch ($currentCount/$batchSize)")

      // Flush if batch is full
      flushBatchIfNeeded()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to add document to batch: ${ex.getMessage}")
        throw ex
    }

  private def addFieldToDocument(
    document: Document,
    fieldName: String,
    value: Any,
    dataType: org.apache.spark.sql.types.DataType
  ): Unit = {
    // Check if this is a JSON field
    val sparkField = org.apache.spark.sql.types.StructField(fieldName, dataType)
    if (jsonFieldMapper.shouldUseJsonField(sparkField)) {
      // Handle JSON fields (Struct, Array, or explicitly configured StringType)
      logger.debug(s"Adding JSON field: $fieldName (type: $dataType)")

      dataType match {
        case st: org.apache.spark.sql.types.StructType =>
          // Value is an InternalRow for struct types
          val internalRow = value.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow]

          // Convert InternalRow to generic Row for easier processing
          val genericRow = org.apache.spark.sql.Row.fromSeq(
            st.fields.zipWithIndex.map { case (field, idx) =>
              internalRow.get(idx, field.dataType)
            }
          )

          val jsonMap = jsonConverter.structToJsonMap(genericRow, st)
          // Convert Java Map to JSON string
          val jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(jsonMap)
          document.addJson(fieldName, jsonString)

        case at: org.apache.spark.sql.types.ArrayType =>
          // Convert to Scala Seq
          val arrayData = value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
          val seq = (0 until arrayData.numElements()).map { i =>
            arrayData.get(i, at.elementType)
          }

          val jsonList = jsonConverter.arrayToJsonList(seq, at)
          // Wrap array in object with "_values" key
          val wrappedMap = jsonConverter.wrapArrayInObject(jsonList)
          // Convert Java Map to JSON string
          val jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(wrappedMap)
          document.addJson(fieldName, jsonString)

        case org.apache.spark.sql.types.StringType if jsonFieldMapper.getFieldType(fieldName) == "json" =>
          // Parse JSON string - it's already a string, just pass it through
          val jsonString = value.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString
          document.addJson(fieldName, jsonString)

        case other =>
          logger.warn(s"JSON field $fieldName has unexpected type: $other")
      }
    } else {
      // Handle regular fields
      dataType match {
        case org.apache.spark.sql.types.StringType =>
          val str = value.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString
          if (fieldName == "title") { // Debug logging for title field
            logger.debug(s"Adding title field: '$str'")
          }
          document.addText(fieldName, str)
        case org.apache.spark.sql.types.LongType =>
          document.addInteger(fieldName, value.asInstanceOf[Long])
        case org.apache.spark.sql.types.IntegerType =>
          document.addInteger(fieldName, value.asInstanceOf[Int].toLong)
        case org.apache.spark.sql.types.DoubleType =>
          document.addFloat(fieldName, value.asInstanceOf[Double])
        case org.apache.spark.sql.types.FloatType =>
          document.addFloat(fieldName, value.asInstanceOf[Float].toDouble)
        case org.apache.spark.sql.types.BooleanType =>
          document.addBoolean(fieldName, value.asInstanceOf[Boolean])
        case org.apache.spark.sql.types.BinaryType =>
          document.addBytes(fieldName, value.asInstanceOf[Array[Byte]])
        case org.apache.spark.sql.types.TimestampType =>
          // Spark uses microseconds internally, but we store as milliseconds for efficiency
          val microseconds = value.asInstanceOf[Long]
          val milliseconds = microseconds / 1000L
          document.addInteger(fieldName, milliseconds)
        case org.apache.spark.sql.types.DateType =>
          // Convert days since epoch to LocalDateTime for proper date storage
          import java.time.LocalDateTime
          import java.time.LocalDate
          val daysSinceEpoch = value.asInstanceOf[Int]
          val epochDate      = LocalDate.of(1970, 1, 1)
          val localDate      = epochDate.plusDays(daysSinceEpoch.toLong)
          val localDateTime  = localDate.atStartOfDay()
          document.addDate(fieldName, localDateTime)
        case _ =>
          logger.warn(s"Unsupported field type for $fieldName: $dataType")
      }
    }
  }

  private def addFieldToBatchDocument(
    batchDocument: BatchDocument,
    fieldName: String,
    value: Any,
    dataType: org.apache.spark.sql.types.DataType
  ): Unit = {
    // Check if this is a JSON field
    val sparkField = org.apache.spark.sql.types.StructField(fieldName, dataType)
    if (jsonFieldMapper.shouldUseJsonField(sparkField)) {
      // Handle JSON fields (Struct, Array, or explicitly configured StringType)
      logger.debug(s"Adding JSON field to batch: $fieldName (type: $dataType)")

      dataType match {
        case st: org.apache.spark.sql.types.StructType =>
          // Value is an InternalRow for struct types
          val internalRow = value.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow]

          // Convert InternalRow to generic Row for easier processing
          val genericRow = org.apache.spark.sql.Row.fromSeq(
            st.fields.zipWithIndex.map { case (field, idx) =>
              internalRow.get(idx, field.dataType)
            }
          )

          val jsonMap = jsonConverter.structToJsonMap(genericRow, st)
          // Convert Java Map to JSON string
          val jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(jsonMap)
          batchDocument.addJson(fieldName, jsonString)

        case at: org.apache.spark.sql.types.ArrayType =>
          // Convert to Scala Seq
          val arrayData = value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
          val seq = (0 until arrayData.numElements()).map { i =>
            arrayData.get(i, at.elementType)
          }

          val jsonList = jsonConverter.arrayToJsonList(seq, at)
          // Wrap array in object with "_values" key
          val wrappedMap = jsonConverter.wrapArrayInObject(jsonList)
          // Convert Java Map to JSON string
          val jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(wrappedMap)
          batchDocument.addJson(fieldName, jsonString)

        case org.apache.spark.sql.types.StringType if jsonFieldMapper.getFieldType(fieldName) == "json" =>
          // Parse JSON string - it's already a string, just pass it through
          val jsonString = value.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString
          batchDocument.addJson(fieldName, jsonString)

        case other =>
          logger.warn(s"JSON field $fieldName has unexpected type: $other")
      }
    } else {
      // Handle regular fields
      dataType match {
        case org.apache.spark.sql.types.StringType =>
          val str = value.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString
          batchDocument.addText(fieldName, str)
        case org.apache.spark.sql.types.LongType =>
          batchDocument.addInteger(fieldName, value.asInstanceOf[Long])
        case org.apache.spark.sql.types.IntegerType =>
          batchDocument.addInteger(fieldName, value.asInstanceOf[Int].toLong)
        case org.apache.spark.sql.types.DoubleType =>
          batchDocument.addFloat(fieldName, value.asInstanceOf[Double])
        case org.apache.spark.sql.types.FloatType =>
          batchDocument.addFloat(fieldName, value.asInstanceOf[Float].toDouble)
        case org.apache.spark.sql.types.BooleanType =>
          batchDocument.addBoolean(fieldName, value.asInstanceOf[Boolean])
        case org.apache.spark.sql.types.BinaryType =>
          batchDocument.addBytes(fieldName, value.asInstanceOf[Array[Byte]])
        case org.apache.spark.sql.types.TimestampType =>
          // Spark uses microseconds internally, but we store as milliseconds for efficiency
          val microseconds = value.asInstanceOf[Long]
          val milliseconds = microseconds / 1000L
          batchDocument.addInteger(fieldName, milliseconds)
        case org.apache.spark.sql.types.DateType =>
          // Convert days since epoch to LocalDateTime for proper date storage
          import java.time.LocalDateTime
          import java.time.LocalDate
          val daysSinceEpoch = value.asInstanceOf[Int]
          val epochDate      = LocalDate.of(1970, 1, 1)
          val localDate      = epochDate.plusDays(daysSinceEpoch.toLong)
          val localDateTime  = localDate.atStartOfDay()
          batchDocument.addDate(fieldName, localDateTime)
        case _ =>
          logger.warn(s"Unsupported field type for $fieldName: $dataType")
      }
    }
  }

  def addDocuments(rows: Iterator[InternalRow]): Unit =
    rows.foreach(addDocument)

  def commit(): Unit = {
    // Flush any remaining batch before committing
    if (useBatch) {
      flushBatchIfNeeded(forceBatch = true)
      // Clean up batch ThreadLocal
      threadLocalBatch.remove()
      threadLocalBatchCount.remove()
    }

    // Commit and close the ThreadLocal writer for this task
    Option(threadLocalWriter.get()).foreach { writer =>
      writer.commit()
      writer.close() // Close the writer after commit so files are fully flushed
      logger.info(s"Committed and closed IndexWriter for task thread ${Thread.currentThread().getName}")
    }
    threadLocalWriter.remove() // Remove from ThreadLocal since it's now closed

    // Reload the index to make committed documents visible
    index.reload()
    logger.info("Index reloaded after task writer commit and close")

    // Add a small delay to ensure all files are fully written to disk
    // This matches the pattern used in tantivy4java tests
    try {
      Thread.sleep(100)
      logger.debug("Waited 100ms for index files to be fully written to disk")
    } catch {
      case _: InterruptedException => // Ignore interruption
    }
  }

  /**
   * Close the index after commit for production use (write-only pattern). This is called by
   * TantivySearchEngine.commitAndCreateSplit() after split creation.
   */
  def commitAndClose(): Unit = {
    commit()
    // Close the index completely - we don't need it for reading since we use splits
    index.close()
    logger.info("Index closed after commit - all reading will be done from splits")
  }

  /**
   * Search operations are not supported on write-only indexes. Use SplitSearchEngine to read from split files instead.
   */
  @deprecated("Direct index search is not supported. Create splits and use SplitSearchEngine.", "split-migration")
  def searchAll(limit: Int = Int.MaxValue): Array[InternalRow] =
    throw new UnsupportedOperationException(
      "Direct index search is not supported in write-only architecture. " +
        "Use TantivySearchEngine.commitAndCreateSplit() to create a split, " +
        "then use SplitSearchEngine.fromSplitFile() to read from the split."
    )

  /**
   * Search operations are not supported on write-only indexes. Use SplitSearchEngine to read from split files instead.
   */
  @deprecated("Direct index search is not supported. Create splits and use SplitSearchEngine.", "split-migration")
  def search(queryString: String, limit: Int = 100): Array[InternalRow] =
    throw new UnsupportedOperationException(
      "Direct index search is not supported in write-only architecture. " +
        "Use TantivySearchEngine.commitAndCreateSplit() to create a split, " +
        "then use SplitSearchEngine.fromSplitFile() to read from the split."
    )

  // Note: getIndexComponents and createZipArchive removed - using splits instead of ZIP archives

  /** Get the path to the index directory for split creation. */
  def getIndexPath(): String =
    tempIndexDir.toAbsolutePath.toString

  /** Delay cleanup to allow split creation from the index directory. */
  def delayCleanupForSplit(): Unit =
    delayCleanup = true

  /** Allow cleanup after split creation is complete. */
  def allowCleanup(): Unit =
    delayCleanup = false

  /** Force cleanup of temporary directory (for use after split creation). */
  def forceCleanup(): Unit = {
    delayCleanup = false
    if (needsCleanup && tempIndexDir != null) {
      try {
        deleteRecursively(tempIndexDir.toFile)
        logger.debug(s"Force cleaned up temporary index directory: ${tempIndexDir.toAbsolutePath}")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to force clean up temporary directory: ${ex.getMessage}")
      }
    }
  }

  override def close(): Unit = {
    // Close ThreadLocal writer for current task if it exists
    Option(threadLocalWriter.get()).foreach { writer =>
      try {
        writer.close()
        logger.debug(s"Closed IndexWriter for task thread ${Thread.currentThread().getName}")
      } catch {
        case _: Exception => // Writer may already be closed
      }
    }
    threadLocalWriter.remove()

    // Close index if it hasn't been closed already
    try {
      index.close()
      logger.debug("Closed index in cleanup")
    } catch {
      case _: Exception => // Index may already be closed from commit()
    }

    // Close schemaBuilder if it exists
    schemaBuilder.foreach(_.close())
    schemaBuilder = None

    // Clean up temporary directory (unless cleanup is delayed for split creation)
    if (needsCleanup && tempIndexDir != null && !delayCleanup) {
      try {
        deleteRecursively(tempIndexDir.toFile)
        logger.debug(s"Cleaned up temporary index directory: ${tempIndexDir.toAbsolutePath}")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to clean up temporary directory: ${ex.getMessage}")
      }
    } else if (delayCleanup) {
      logger.debug(s"Delaying cleanup of temporary index directory for split creation: ${tempIndexDir.toAbsolutePath}")
    }
  }

  /**
   * Get the field indexing configuration for a specific field. This is used by the query converter to determine whether
   * to use tokenized or exact matching.
   */
  def getFieldIndexingConfig(fieldName: String): io.indextables.spark.core.FieldIndexingConfig = {
    val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(options)
    tantivyOptions.getFieldIndexingConfig(fieldName)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
