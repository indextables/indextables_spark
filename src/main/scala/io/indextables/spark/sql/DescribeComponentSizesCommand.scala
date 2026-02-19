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

package io.indextables.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.transaction.{AddAction, PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils, ProtocolNormalizer, SplitMetadataFactory}
import org.slf4j.LoggerFactory

/**
 * SQL command to describe sub-component sizes for all splits in a table.
 *
 * Syntax:
 * {{{
 * DESCRIBE INDEXTABLES COMPONENT SIZES '/path/to/table'
 *   [WHERE partition_col = 'value']
 *
 * DESCRIBE TANTIVY4SPARK COMPONENT SIZES my_table
 *   WHERE year = '2024'
 * }}}
 *
 * This command outputs:
 *   - split_path: Path to the split file
 *   - partition_values: JSON map of partition key/values (null if unpartitioned)
 *   - component_key: Component identifier (e.g., `score.fastfield`, `_term_total`)
 *   - size_bytes: Size in bytes
 *   - component_type: Category: `fastfield`, `fieldnorm`, `term`, `postings`, `positions`, `store`
 *   - field_name: Field name (null for segment-level components)
 *
 * @param tablePath
 *   Table path or identifier
 * @param wherePredicates
 *   Partition predicates for filtering splits
 */
case class DescribeComponentSizesCommand(
  tablePath: String,
  wherePredicates: Seq[String])
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeComponentSizesCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("split_path", StringType, nullable = false)(),
    AttributeReference("partition_values", StringType, nullable = true)(),
    AttributeReference("component_key", StringType, nullable = false)(),
    AttributeReference("size_bytes", LongType, nullable = false)(),
    AttributeReference("component_type", StringType, nullable = false)(),
    AttributeReference("field_name", StringType, nullable = true)()
  )

  /**
   * Resolve AWS credentials on the driver and return a modified config. This eliminates executor-side HTTP calls for
   * credential providers like UnityCatalogAWSCredentialProvider.
   */
  private def resolveCredentialsOnDriver(config: Map[String, String], tablePath: String): Map[String, String] = {
    val providerClass = config
      .get("spark.indextables.aws.credentialsProviderClass")
      .orElse(config.get("spark.indextables.aws.credentialsproviderclass"))

    providerClass match {
      case Some(className) if className.nonEmpty =>
        try {
          val normalizedPath = io.indextables.spark.util.TablePathNormalizer.normalizeToTablePath(tablePath)
          val credentials = io.indextables.spark.utils.CredentialProviderFactory.resolveAWSCredentialsFromConfig(
            config,
            normalizedPath
          )

          credentials match {
            case Some(creds) =>
              logger.info(s"[DRIVER] Resolved AWS credentials from provider: $className")
              var newConfig = config -
                "spark.indextables.aws.credentialsProviderClass" -
                "spark.indextables.aws.credentialsproviderclass" +
                ("spark.indextables.aws.accessKey" -> creds.accessKey) +
                ("spark.indextables.aws.secretKey" -> creds.secretKey)

              creds.sessionToken.foreach(token => newConfig = newConfig + ("spark.indextables.aws.sessionToken" -> token))
              newConfig

            case None =>
              logger.warn(s"[DRIVER] Failed to resolve credentials from provider $className, passing to executors")
              config
          }
        } catch {
          case ex: Exception =>
            logger.warn(s"[DRIVER] Driver-side credential resolution failed: ${ex.getMessage}, passing to executors")
            config
        }

      case None => config
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"Describing component sizes for table: $resolvedPath")

      val sc = sparkSession.sparkContext

      // Get session config for credentials and cache settings
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs =
        ConfigNormalization.extractTantivyConfigsFromHadoop(sparkSession.sparkContext.hadoopConfiguration)
      val baseConfig = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // PERFORMANCE OPTIMIZATION: Resolve credentials on driver to avoid executor-side HTTP calls
      // Describe is read-only, request PATH_READ credentials
      val readConfig = baseConfig + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
      val mergedConfig = resolveCredentialsOnDriver(readConfig, resolvedPath.toString)

      // Create transaction log
      val transactionLog = TransactionLogFactory.create(
        resolvedPath,
        sparkSession,
        new CaseInsensitiveStringMap(mergedConfig.asJava)
      )

      try {
        // Get partition schema for predicate validation
        val metadata = transactionLog.getMetadata()
        val partitionSchema = StructType(
          metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
        )

        // Parse predicates and convert to Spark Filters for Avro manifest pruning
        val (parsedPredicates, partitionFilters) = if (wherePredicates.nonEmpty) {
          val parsed = PartitionPredicateUtils.parseAndValidatePredicates(wherePredicates, partitionSchema, sparkSession)
          val filters = PartitionPredicateUtils.expressionsToFilters(parsed)
          logger.info(
            s"Converted ${filters.length} of ${parsed.length} predicates to Spark Filters for manifest pruning"
          )
          (parsed, filters)
        } else {
          (Seq.empty, Seq.empty)
        }

        // Get active splits with manifest-level pruning if filters are available
        var addActions = if (partitionFilters.nonEmpty) {
          val actions = transactionLog.listFilesWithPartitionFilters(partitionFilters)
          logger.info(s"Found ${actions.length} splits using Avro manifest pruning")
          actions
        } else {
          val actions = transactionLog.listFiles()
          logger.info(s"Found ${actions.length} splits in transaction log")
          actions
        }

        // Apply in-memory partition filtering for any predicates not converted to Spark Filters
        if (parsedPredicates.nonEmpty) {
          addActions =
            PartitionPredicateUtils.filterAddActionsByPredicates(addActions, partitionSchema, parsedPredicates)
          logger.info(s"After in-memory partition filtering: ${addActions.length} splits")
        }

        if (addActions.isEmpty) {
          logger.info("No splits found matching the criteria")
          return Seq.empty
        }

        // Broadcast config for executor access
        val broadcastConfig = sc.broadcast(mergedConfig)
        val tablePathStr    = resolvedPath.toString

        // Create tasks with split metadata
        val tasks = addActions.map { addAction =>
          ComponentSizeTask(
            addAction = addAction,
            tablePath = tablePathStr
          )
        }

        logger.info(s"Processing ${tasks.length} splits for component sizes")

        // Execute tasks in parallel across executors
        val results = sc
          .parallelize(tasks, math.min(tasks.length, sc.defaultParallelism))
          .flatMap(task => executeComponentSizeTask(task, broadcastConfig.value))
          .collect()
          .toSeq

        logger.info(s"Collected ${results.length} component size entries")

        results

      } finally
        transactionLog.close()

    } catch {
      case e: Exception =>
        logger.error(s"Failed to describe component sizes: ${e.getMessage}", e)
        throw e
    }

  /** Execute component size task on an executor. */
  private def executeComponentSizeTask(
    task: ComponentSizeTask,
    config: Map[String, String]
  ): Seq[Row] = {
    val taskLogger = LoggerFactory.getLogger(classOf[DescribeComponentSizesCommand])

    try {
      // Normalize path for tantivy4java compatibility
      val fullPath =
        if (ProtocolNormalizer.isS3Path(task.addAction.path) || ProtocolNormalizer.isAzurePath(task.addAction.path)) {
          task.addAction.path
        } else {
          s"${task.tablePath}/${task.addAction.path}"
        }
      val actualPath = ProtocolNormalizer.normalizeAllProtocols(fullPath)

      // Create cache config and manager
      val cacheConfig  = ConfigUtils.createSplitCacheConfig(config, Some(task.tablePath))
      val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

      // Create split metadata from AddAction
      val splitMetadata = SplitMetadataFactory.fromAddAction(task.addAction, task.tablePath)

      // Create split searcher
      val splitSearcher = cacheManager.createSplitSearcher(actualPath, splitMetadata)

      try {
        // Get per-field component sizes from tantivy4java
        val componentSizes: java.util.Map[String, java.lang.Long] = splitSearcher.getPerFieldComponentSizes()

        // Convert partition values to JSON string
        val partitionValuesJson = if (task.addAction.partitionValues.nonEmpty) {
          val jsonPairs = task.addAction.partitionValues.map { case (k, v) => s""""$k":"$v"""" }.mkString(",")
          s"{$jsonPairs}"
        } else {
          null
        }

        // Convert each component size entry to a Row
        componentSizes.asScala.map {
          case (componentKey, sizeBytes) =>
            val (componentType, fieldName) = parseComponentKey(componentKey)
            Row(
              task.addAction.path,
              partitionValuesJson,
              componentKey,
              sizeBytes.toLong,
              componentType,
              fieldName
            )
        }.toSeq

      } finally
        try splitSearcher.close()
        catch { case _: Exception => }

    } catch {
      case e: Exception =>
        taskLogger.warn(s"Failed to get component sizes for split ${task.addAction.path}: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Parse component key to extract component type and field name.
   *
   * Key formats:
   *   - Per-field: `{field}.fastfield`, `{field}.fieldnorm`
   *   - Segment-level: `_term_total`, `_postings_total`, `_positions_total`, `_store`
   */
  private def parseComponentKey(componentKey: String): (String, String) =
    if (componentKey.startsWith("_")) {
      // Segment-level component
      val componentType = componentKey match {
        case "_term_total"      => "term"
        case "_postings_total"  => "postings"
        case "_positions_total" => "positions"
        case "_store"           => "store"
        case other              => other.stripPrefix("_").takeWhile(_ != '_')
      }
      (componentType, null)
    } else if (componentKey.endsWith(".fastfield")) {
      val fieldName = componentKey.stripSuffix(".fastfield")
      ("fastfield", fieldName)
    } else if (componentKey.endsWith(".fieldnorm")) {
      val fieldName = componentKey.stripSuffix(".fieldnorm")
      ("fieldnorm", fieldName)
    } else if (componentKey.contains(".")) {
      // Generic field.type format
      val lastDot       = componentKey.lastIndexOf('.')
      val fieldName     = componentKey.substring(0, lastDot)
      val componentType = componentKey.substring(lastDot + 1)
      (componentType, fieldName)
    } else {
      // Unknown format - use as-is
      (componentKey, null)
    }

  private def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path =
    if (
      pathOrTable.startsWith("/") || pathOrTable.startsWith("s3://") || pathOrTable.startsWith("s3a://") ||
      pathOrTable.startsWith("hdfs://") || pathOrTable.startsWith("file://") ||
      pathOrTable.startsWith("abfss://") || pathOrTable.startsWith("wasbs://")
    ) {
      new Path(pathOrTable)
    } else {
      try {
        val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(pathOrTable)
        val catalog         = sparkSession.sessionState.catalog
        if (catalog.tableExists(tableIdentifier)) {
          val tableMetadata = catalog.getTableMetadata(tableIdentifier)
          new Path(tableMetadata.location)
        } else {
          throw new IllegalArgumentException(s"Table not found: $pathOrTable")
        }
      } catch {
        case _: Exception =>
          new Path(pathOrTable)
      }
    }
}

/** Internal task representation for component size retrieval. */
private[sql] case class ComponentSizeTask(
  addAction: AddAction,
  tablePath: String)
    extends Serializable
