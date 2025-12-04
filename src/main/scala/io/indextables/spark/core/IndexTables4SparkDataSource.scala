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

package io.indextables.spark.core

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.slf4j.LoggerFactory

class IndexTables4SparkTable(
  path: String,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  tablePartitioning: Array[Transform] = Array.empty)
    extends SupportsRead
    with SupportsWrite
    with org.apache.spark.sql.connector.catalog.SupportsMetadataColumns {

  private val logger         = LoggerFactory.getLogger(classOf[IndexTables4SparkTable])
  private val spark          = SparkSession.active
  private val tablePath      = new Path(path)
  private val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

  // Aggressively populate cache on table initialization for faster first reads
  transactionLog.prewarmCache()

  override def name(): String = s"indextables.`$path`"

  override def schema(): StructType =
    transactionLog.getSchema().getOrElse(schema)

  override def capabilities(): util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE
    ).asJava

  override def metadataColumns(): Array[org.apache.spark.sql.connector.catalog.MetadataColumn] =
    Array(new org.apache.spark.sql.connector.catalog.MetadataColumn {
      override def name(): String                                  = "_indexall"
      override def dataType(): org.apache.spark.sql.types.DataType = org.apache.spark.sql.types.StringType
      override def isNullable(): Boolean                           = true
      override def comment(): String = "Virtual column for full-text search across all fields"
    })

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Create broadcast variable with proper precedence: read options > Spark config > Hadoop config
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Extract configurations from Hadoop config (lowest precedence)
    val hadoopTantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter { entry =>
        entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
      }
      .map { entry =>
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        normalizedKey -> entry.getValue
      }
      .filter(_._2 != null) // Filter out null values
      .toMap

    // Extract configurations from Spark session config (middle precedence)
    val sparkTantivyConfigs =
      try
        spark.conf.getAll
          .filter {
            case (key, _) =>
              key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
          }
          .map {
            case (key, value) =>
              val normalizedKey = if (key.startsWith("spark.indextables.")) {
                key.replace("spark.indextables.", "spark.indextables.")
              } else key
              normalizedKey -> value
          }
          .filter(_._2 != null)
          .toMap
      catch {
        case _: Exception => Map.empty[String, String]
      }

    // Extract configurations from read options (highest precedence)
    val readTantivyConfigs = options.asScala
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
      .filter(_._2 != null) // Filter out null values
      .toMap

    // Merge with proper precedence: Hadoop < Spark config < read options
    val tantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ readTantivyConfigs

    // Validate numeric configuration values early to provide better error messages
    def validateNumericConfig(
      key: String,
      value: String,
      expectedType: String
    ): Unit =
      try
        expectedType match {
          case "Long" =>
            value.toLong
          case "Int" =>
            value.toInt
          case _ => // No validation needed
        }
      catch {
        case e: NumberFormatException =>
          throw e
      }

    // Validate ALL configurations that might be numeric
    logger.debug(s"newScanBuilder called - validating ${tantivyConfigs.size} tantivy configs")
    tantivyConfigs.foreach {
      case (key, value) =>
        key.toLowerCase match {
          case k if k.contains("maxsize") =>
            validateNumericConfig(key, value, "Long")
          case k if k.contains("maxconcurrentloads") =>
            validateNumericConfig(key, value, "Int")
          case k if k.contains("targetrecordspersplit") =>
            validateNumericConfig(key, value, "Long")
          case _ =>
          // No validation needed
        }
    }

    // Debug: Log final configuration passed to scan builder
    logger.info(s"Passing ${tantivyConfigs.size} IndexTables4Spark configurations to scan builder")
    logger.info(s"Sources: Hadoop(${hadoopTantivyConfigs.size}), Spark(${sparkTantivyConfigs.size}), Options(${readTantivyConfigs.size})")

    new IndexTables4SparkScanBuilder(spark, transactionLog, schema(), options, tantivyConfigs)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Debug: Log existing hadoop config before modification (including normalized indextables configs)
    logger.info(s"V2 Write: Existing Hadoop config indextables properties:")
    import io.indextables.spark.util.ConfigNormalization
    val existingTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    existingTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Copy write options to Hadoop configuration so they're available in executors
    // Write options from info.options() should override any existing configuration
    // IMPORTANT: Create a COPY of hadoopConf to avoid polluting the shared Spark session configuration
    import scala.jdk.CollectionConverters._
    val enrichedHadoopConf = new org.apache.hadoop.conf.Configuration(hadoopConf)

    val writeOptions = info.options()
    logger.info(s"V2 Write: DataFrame options to copy:")
    val writeOptionTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromOptions(writeOptions)
    writeOptionTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Apply normalization to ensure both spark.indextables.* and spark.indextables.* are handled
    val normalizedWriteConfigs = ConfigNormalization.extractTantivyConfigsFromOptions(writeOptions)
    normalizedWriteConfigs.foreach {
      case (key, value) =>
        enrichedHadoopConf.set(key, value)
        logger.info(
          s"V2 Write: Set Hadoop config from write options: $key = ${if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***"
            else value}"
        )
    }

    // Debug: Log final hadoop config after modification (including normalized indextables configs)
    logger.info(s"V2 Write: Final Hadoop config indextables properties:")
    val finalTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(enrichedHadoopConf)
    finalTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Inject partition information into write options for V2 compatibility
    val enhancedOptions = if (tablePartitioning.nonEmpty) {
      // Extract partition column names from Transform objects
      val partitionColumnNames = tablePartitioning.map { transform =>
        // For identity transforms, extract the column name
        transform match {
          case identityTransform =>
            // Access the field reference and get the name
            try {
              // Use reflection to get the field reference from the identity transform
              val field = identityTransform.getClass.getDeclaredField("ref")
              field.setAccessible(true)
              val fieldRef = field.get(identityTransform)
              // Get the field names from the reference
              val fieldNames = fieldRef.getClass.getDeclaredField("fieldNames")
              fieldNames.setAccessible(true)
              val names = fieldNames.get(fieldRef).asInstanceOf[Array[String]]
              names.head // Take the first (and likely only) field name
            } catch {
              case _: Exception =>
                // Fallback: try to extract from toString
                val transformStr = transform.toString
                if (transformStr.contains("identity(") && transformStr.contains(")")) {
                  transformStr.substring(transformStr.indexOf("identity(") + 9, transformStr.lastIndexOf(")"))
                } else {
                  "unknown_column"
                }
            }
        }
      }

      logger.info(s"V2 Write: Injecting partition columns into options: ${partitionColumnNames.mkString(", ")}")

      // Create enhanced options with partition information
      import scala.jdk.CollectionConverters._
      val optionsMap = options.asScala.toMap

      // Serialize partition columns as JSON array
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val partitionColumnsJson = mapper.writeValueAsString(partitionColumnNames.toArray)

      val enhancedOptionsMap = optionsMap + ("__partition_columns" -> partitionColumnsJson)
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(enhancedOptionsMap.asJava)
    } else {
      options
    }

    new IndexTables4SparkWriteBuilder(transactionLog, tablePath, info, enhancedOptions, enrichedHadoopConf)
  }

  override def partitioning(): Array[org.apache.spark.sql.connector.expressions.Transform] =
    if (tablePartitioning.nonEmpty) {
      logger.info(s"Table partitioned by: ${tablePartitioning.mkString(", ")}")
      tablePartitioning
    } else {
      // Fallback: try to read from transaction log if it exists
      try {
        val partitionColumns = transactionLog.getPartitionColumns()
        if (partitionColumns.nonEmpty) {
          logger.info(s"Table partitioned by (from transaction log): ${partitionColumns.mkString(", ")}")
          // Convert partition column names to identity transforms
          partitionColumns.map(col => org.apache.spark.sql.connector.expressions.Expressions.identity(col)).toArray
        } else {
          logger.info("Table is not partitioned")
          Array.empty
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Could not read partition columns: ${e.getMessage}")
          Array.empty
      }
    }

}

class IndexTables4SparkTableProvider extends org.apache.spark.sql.connector.catalog.TableProvider {

  /**
   * Extracts paths from options, supporting both direct path parameters and multiple paths. Handles paths from
   * load("path") calls as well as .option("path", "value") and multiple paths.
   */
  protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(options.get("paths")).map(pathStr => parsePathsFromJson(pathStr)).getOrElse(Seq.empty)

    paths ++ Option(options.get("path")).toSeq
  }

  private def parsePathsFromJson(pathStr: String): Seq[String] =
    try {
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule

      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException(s"Invalid paths format: $pathStr. Expected JSON array of strings.")
    }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val paths = getPaths(options)
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark      = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Extract configurations with proper precedence hierarchy: Hadoop < Spark config < read options
    val hadoopTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    }

    val sparkTantivyConfigs =
      try {
        import io.indextables.spark.util.ConfigNormalization
        ConfigNormalization.extractTantivyConfigsFromSpark(spark)
      } catch {
        case _: Exception => Map.empty[String, String]
      }

    val readTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromOptions(options)
    }

    // Merge with proper precedence: Hadoop < Spark config < read options
    val mergedTantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ readTantivyConfigs

    // Create a serializable map first, then convert to CaseInsensitiveStringMap
    // This avoids serialization issues when the TransactionLog is used in distributed contexts
    import scala.jdk.CollectionConverters._
    val mergedConfigMap = options.asScala.toMap ++ mergedTantivyConfigs
    val mergedOptions   = new CaseInsensitiveStringMap(mergedConfigMap.asJava)

    val transactionLog = TransactionLogFactory.create(new Path(paths.head), spark, mergedOptions)

    transactionLog.getSchema().getOrElse {
      throw new RuntimeException(
        s"Path does not exist: ${paths.head}. No transaction log found. Use spark.write to create the table first."
      )
    }
  }

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): org.apache.spark.sql.connector.catalog.Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val paths   = getPaths(options)

    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark      = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Apply the same configuration hierarchy as inferSchema: Hadoop < Spark config < table properties
    val hadoopTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    }

    val sparkTantivyConfigs =
      try {
        import io.indextables.spark.util.ConfigNormalization
        ConfigNormalization.extractTantivyConfigsFromSpark(spark)
      } catch {
        case _: Exception => Map.empty[String, String]
      }

    val tableTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromOptions(options)
    }

    // Merge with proper precedence: Hadoop < Spark config < table properties
    val mergedTantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ tableTantivyConfigs

    // Create a serializable map first, then convert to CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._
    val mergedConfigMap = options.asScala.toMap ++ mergedTantivyConfigs
    val mergedOptions   = new CaseInsensitiveStringMap(mergedConfigMap.asJava)

    // Use the first path as the primary table path (support for multiple paths can be added later)
    new IndexTables4SparkTable(paths.head, schema, mergedOptions, partitioning)
  }

  override def supportsExternalMetadata(): Boolean = true
}
