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

package com.tantivy4spark.core

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, Write, WriterCommitMessage, RequiresDistributionAndOrdering}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions, ClusteredDistribution}
import org.apache.spark.sql.connector.expressions.{Expressions, SortOrder, LogicalExpressions}
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.slf4j.LoggerFactory

/**
 * Standard write implementation for Tantivy4Spark tables.
 * This implementation does NOT include RequiresDistributionAndOrdering,
 * so it uses Spark's default partitioning without any optimization.
 * Used when optimizeWrite is disabled.
 */
class Tantivy4SparkStandardWrite(
    @transient transactionLog: TransactionLog,
    tablePath: Path,
    @transient writeInfo: LogicalWriteInfo,
    serializedOptions: Map[String, String],  // Use serializable Map instead of CaseInsensitiveStringMap
    @transient hadoopConf: org.apache.hadoop.conf.Configuration,
    isOverwrite: Boolean = false  // Track whether this is an overwrite operation
) extends Write with BatchWrite with RequiresDistributionAndOrdering with Serializable {

  @transient private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkStandardWrite])

  // Extract serializable values from transient fields during construction
  private val writeSchema = writeInfo.schema()
  private val serializedHadoopConf = {
    // Serialize only the tantivy4spark config properties from hadoopConf
    val props = scala.collection.mutable.Map[String, String]()
    val iter = hadoopConf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")) {
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        props.put(normalizedKey, entry.getValue)
      }
    }
    props.toMap
  }
  private val partitionColumns = {
    // Extract partition columns from write options (set by .partitionBy())
    // Spark sets this as a JSON array string like ["col1","col2"]
    serializedOptions.get("__partition_columns") match {
      case Some(partitionColumnsJson) =>
        try {
          // Parse JSON array to extract column names
          import com.fasterxml.jackson.module.scala.DefaultScalaModule
          import com.fasterxml.jackson.databind.ObjectMapper

          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val partitionCols = mapper.readValue(partitionColumnsJson, classOf[Array[String]]).toSeq

          println(s"🔍 PARTITION DEBUG: Extracted partition columns from options: $partitionCols")
          logger.info(s"🔍 PARTITION DEBUG: Extracted partition columns from options: $partitionCols")
          partitionCols
        } catch {
          case e: Exception =>
            println(s"🔍 PARTITION DEBUG: Failed to parse partition columns JSON: $partitionColumnsJson, error: ${e.getMessage}")
            logger.warn(s"Failed to parse partition columns from options: $partitionColumnsJson", e)
            Seq.empty
        }
      case None =>
        // Fallback: try to read from existing transaction log
        try {
          val cols = transactionLog.getPartitionColumns()
          println(s"🔍 PARTITION DEBUG: Fallback - read partition columns from transaction log: $cols")
          logger.info(s"🔍 PARTITION DEBUG: Fallback - read partition columns from transaction log: $cols")
          cols
        } catch {
          case ex: Exception =>
            println(s"🔍 PARTITION DEBUG: No partition columns found")
            logger.warn(s"Could not retrieve partition columns during construction: ${ex.getMessage}")
            Seq.empty[String]
        }
    }
  }

  override def toBatch: BatchWrite = this

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating standard batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"Using Spark's default partitioning (no optimization)")
    
    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }
    
    // Combine serialized hadoop config with tantivy4spark options (normalize spark.indextables to spark.tantivy4spark)
    val normalizedTantivyOptions = serializedOptions
      .filter(kv => kv._1.startsWith("spark.indextables.") || kv._1.startsWith("spark.indextables."))
      .map { case (key, value) =>
        val normalizedKey = if (key.startsWith("spark.indextables.")) {
          key.replace("spark.indextables.", "spark.indextables.")
        } else key
        normalizedKey -> value
      }
    val combinedHadoopConfig = serializedHadoopConf ++ normalizedTantivyOptions
      
    serializedOptions.foreach { case (key, value) =>
      if (key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")) {
        val normalizedKey = if (key.startsWith("spark.indextables.")) {
          key.replace("spark.indextables.", "spark.indextables.")
        } else key
        logger.info(s"Will copy DataFrame option to Hadoop config: $normalizedKey = ${if (key.contains("secretKey") || key.contains("sessionToken")) "***" else value}")
      }
    }
    
    new Tantivy4SparkWriterFactory(tablePath, writeSchema, serializedOptions, combinedHadoopConfig, partitionColumns)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.warn(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    println(s"🔍 DEBUG: serializedOptions keys: ${serializedOptions.keys.mkString(", ")}")
    serializedOptions.foreach { case (k, v) =>
      val redactedValue = if (k.toLowerCase.contains("secret") || k.toLowerCase.contains("key") || k.toLowerCase.contains("password") || k.toLowerCase.contains("token")) {
        "***REDACTED***"
      } else {
        v
      }
      println(s"🔍 DEBUG: serializedOption $k = $redactedValue")
    }

    // Validate indexing configuration for append operations
    if (!isOverwrite) {
      validateIndexingConfigurationForAppend()
    }

    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: Tantivy4SparkCommitMessage => msg.addActions
      case _ => Seq.empty[AddAction]
    }

    // Log how many empty partitions were filtered out
    val emptyPartitionsCount = messages.length - addActions.length
    if (emptyPartitionsCount > 0) {
      println(s"⚠️  Filtered out $emptyPartitionsCount empty partitions (0 records) from transaction log")
      logger.info(s"⚠️  Filtered out $emptyPartitionsCount empty partitions (0 records) from transaction log")
    }

    // Determine if this should be an overwrite based on existing table state and mode
    val shouldOverwrite = if (isOverwrite) {
      // Explicit overwrite flag from truncate() or overwrite() call
      println(s"🔍 DEBUG: Using explicit isOverwrite=true flag")
      logger.warn(s"🔍 DEBUG: Using explicit isOverwrite=true flag")
      true
    } else {
      // For DataSource V2, SaveMode.Overwrite might not trigger truncate()/overwrite() methods
      // Instead, we need to detect overwrite by checking the logical write info or options
      val saveMode = serializedOptions.get("saveMode") match {
        case Some("Overwrite") => true
        case Some("ErrorIfExists") => false
        case Some("Ignore") => false  
        case Some("Append") => false
        case None => {
          // Check if this looks like an initial write (no existing files) - treat as overwrite
          try {
            val existingFiles = transactionLog.listFiles()
            if (existingFiles.isEmpty) {
              logger.info("No existing files found - treating as initial write (overwrite semantics)")
              false // Initial write doesn't need overwrite semantics, just add files
            } else {
              logger.info(s"Found ${existingFiles.length} existing files - need to determine write mode")
              // Without explicit mode info, default to append to be safe
              false
            }
          } catch {
            case _: Exception => false // If we can't read transaction log, assume append
          }
        }
        case Some(other) => {
          logger.warn(s"Unknown saveMode: $other, defaulting to append")
          false
        }
      }
      println(s"🔍 DEBUG: Final saveMode decision: $saveMode")
      logger.warn(s"🔍 DEBUG: Final saveMode decision: $saveMode")
      saveMode
    }
    
    // Initialize transaction log with schema if this is the first commit  
    transactionLog.initialize(writeSchema, partitionColumns)
    
    // Commit the changes
    if (shouldOverwrite) {
      println(s"🔍 DEBUG: Performing OVERWRITE with ${addActions.length} new files")
      logger.warn(s"🔍 DEBUG: Performing OVERWRITE with ${addActions.length} new files")
      val version = transactionLog.overwriteFiles(addActions)
      logger.info(s"Overwrite completed in transaction version $version, added ${addActions.length} files")
    } else {
      println(s"🔍 DEBUG: Performing APPEND with ${addActions.length} new files")
      logger.warn(s"🔍 DEBUG: Performing APPEND with ${addActions.length} new files")
      // Standard append operation
      val version = transactionLog.addFiles(addActions)
      logger.info(s"Added ${addActions.length} files in transaction version $version")
    }
    
    logger.info(s"Successfully committed ${addActions.length} files")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")
    
    // Clean up any files that were created but not committed
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: Tantivy4SparkCommitMessage => msg.addActions
      case _ => Seq.empty[AddAction]
    }

    // TODO: In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }

  /**
   * Validate indexing configuration for append operations.
   * Checks that the new configuration is compatible with the existing table configuration.
   */
  private def validateIndexingConfigurationForAppend(): Unit = {
    try {
      logger.warn("🔍 VALIDATION DEBUG: Running append configuration validation")

      // Read existing doc mapping from latest add actions
      val existingFiles = transactionLog.listFiles()
      val existingDocMapping = existingFiles
        .flatMap(_.docMappingJson)
        .headOption // Get the first available doc mapping

      if (existingDocMapping.isDefined) {
        logger.warn("🔍 VALIDATION DEBUG: Found existing doc mapping, validating configuration")

        // Parse existing configuration
        import com.fasterxml.jackson.databind.JsonNode
        import com.tantivy4spark.util.JsonUtil
        import scala.jdk.CollectionConverters._

        val existingMapping = JsonUtil.mapper.readTree(existingDocMapping.get: String)
        logger.warn(s"🔍 VALIDATION DEBUG: Parsed existing mapping JSON: $existingMapping")

        // The docMappingJson is directly an array of field definitions
        if (existingMapping.isArray) {
          logger.warn(s"🔍 VALIDATION DEBUG: Found existing fields array with ${existingMapping.size()} fields, processing...")
          val tantivyOptions = com.tantivy4spark.core.Tantivy4SparkOptions(
            new org.apache.spark.sql.util.CaseInsensitiveStringMap(serializedOptions.asJava)
          )
          val errors = scala.collection.mutable.ListBuffer[String]()

          logger.warn(s"🔍 VALIDATION DEBUG: Schema has ${writeSchema.fields.length} fields")

          // Check each field in the current schema for configuration conflicts
          writeSchema.fields.foreach { field =>
            try {
              val fieldName = field.name
              logger.warn(s"🔍 VALIDATION DEBUG: Processing field '$fieldName'")

              val currentConfig = tantivyOptions.getFieldIndexingConfig(fieldName)

              // Find the field in the array by name
              val existingFieldConfig = existingMapping.asScala.find { fieldNode =>
                Option(fieldNode.get("name")).map(_.asText()).contains(fieldName)
              }

              logger.warn(s"🔍 VALIDATION DEBUG: Current config: $currentConfig")
              logger.warn(s"🔍 VALIDATION DEBUG: Existing field config present: ${existingFieldConfig.isDefined}")

              if (existingFieldConfig.isDefined) {
                val existing = existingFieldConfig.get
                val existingType = Option(existing.get("type")).map(_.asText())
                logger.warn(s"🔍 VALIDATION DEBUG: Existing field type: $existingType")

                // Check field type configuration conflicts
                if (currentConfig.fieldType.isDefined) {
                  val currentType = currentConfig.fieldType.get
                  logger.warn(s"🔍 VALIDATION DEBUG: Current type: $currentType")

                  // Strict validation: field types must match exactly
                  if (existingType.isDefined && existingType.get != currentType) {
                    logger.warn(s"🔍 VALIDATION DEBUG: CONFLICT DETECTED for field '$fieldName'!")
                    errors += s"Field '$fieldName' type mismatch: existing table has ${existingType.get} field, cannot append with $currentType configuration"
                  } else {
                    logger.warn(s"🔍 VALIDATION DEBUG: Compatible types for field '$fieldName' (existing: ${existingType.getOrElse("none")}, current: $currentType)")
                  }
                } else {
                  logger.warn(s"🔍 VALIDATION DEBUG: No current field type configured for '$fieldName'")
                }
              } else {
                logger.warn(s"🔍 VALIDATION DEBUG: Field '$fieldName' not found in existing configuration")
              }
            } catch {
              case e: Exception =>
                logger.warn(s"🔍 VALIDATION DEBUG: Exception processing field '${field.name}': ${e.getMessage}")
            }
          }

          logger.warn(s"🔍 VALIDATION DEBUG: Finished processing all fields. Errors found: ${errors.length}")
          if (errors.nonEmpty) {
            val errorMessage = s"Configuration validation failed for append operation:\n${errors.mkString("\n")}"
            logger.error(errorMessage)
            throw new IllegalArgumentException(errorMessage)
          }
        } else {
          logger.warn("🔍 VALIDATION DEBUG: Existing mapping is not an array - unexpected format")
        }
      } else {
        logger.warn("🔍 VALIDATION DEBUG: No existing doc mapping found, skipping validation")
      }
    } catch {
      case e: IllegalArgumentException => throw e // Re-throw validation errors
      case e: Exception =>
        logger.warn(s"🔍 VALIDATION DEBUG: Validation failed with exception: ${e.getMessage}")
        // Don't fail the write for other types of errors
    }
  }

  /**
   * RequiresDistributionAndOrdering implementation for partitioned tables.
   * This ensures Spark partitions data by partition columns before writing.
   */
  override def requiredDistribution(): Distribution = {
    if (partitionColumns.nonEmpty) {
      // For partitioned tables, cluster by partition columns using the correct constructor
      val clusteredColumns = partitionColumns.toArray
      logger.info(s"Standard write: requiring clustering by partition columns: ${partitionColumns.mkString(", ")}")
      Distributions.clustered(clusteredColumns.map(Expressions.identity))
    } else {
      // No partitioning required for non-partitioned tables
      logger.info("Standard write: no partition columns, using unspecified distribution")
      Distributions.unspecified()
    }
  }

  override def requiredOrdering(): Array[SortOrder] = {
    // No specific ordering required
    Array.empty
  }

  override def requiredNumPartitions(): Int = {
    if (partitionColumns.nonEmpty) {
      // For partitioned tables, let Spark determine the number of partitions based on data distribution
      // Return 0 to let Spark automatically determine the partition count
      logger.info("Standard write: letting Spark auto-determine partition count for partitioned table")
      0
    } else {
      // For non-partitioned tables, use default Spark behavior
      logger.info("Standard write: using default partition count for non-partitioned table")
      0
    }
  }
}