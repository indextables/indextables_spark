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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.filters.MixedBooleanFilter
import io.indextables.spark.expressions.{
  BucketAggregationConfig,
  DateHistogramConfig,
  HistogramConfig,
  RangeBucket,
  RangeConfig
}
import io.indextables.spark.transaction.TransactionLog
import io.indextables.spark.util.{PartitionUtils, SplitsPerTaskCalculator}
import io.indextables.tantivy4java.aggregation._
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitMatchAllQuery
import org.slf4j.LoggerFactory

/**
 * Specialized scan for GROUP BY aggregation operations. Implements distributed GROUP BY aggregation using tantivy's
 * terms aggregation capabilities.
 *
 * Also supports bucket aggregations (DateHistogram, Histogram, Range) when bucketConfig is provided. In bucket mode,
 * the groupByColumns array contains a single element with the bucket field name.
 */
class IndexTables4SparkGroupByAggregateScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast,
  aggregation: Aggregation,
  groupByColumns: Array[String],
  indexQueryFilters: Array[Any] = Array.empty,
  bucketConfig: Option[BucketAggregationConfig] = None,
  partitionColumns: Set[String] = Set.empty) // Partition columns for optimization
    extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateScan])

  // Separate GROUP BY columns into partition columns (values from split metadata) and data columns (need Tantivy)
  private val (partitionGroupByColumns, dataGroupByColumns) = {
    val (partCols, dataCols) = groupByColumns.partition(partitionColumns.contains)
    logger.info(s"GROUP BY OPTIMIZATION: Partition columns in GROUP BY: ${partCols.mkString(", ")}")
    logger.info(s"GROUP BY OPTIMIZATION: Data columns in GROUP BY (need Tantivy): ${dataCols.mkString(", ")}")
    (partCols, dataCols)
  }

  logger.debug(s"GROUP BY AGGREGATE SCAN: Created with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")
  pushedFilters.foreach(f => logger.debug(s"GROUP BY AGGREGATE SCAN: Filter: $f"))
  indexQueryFilters.foreach(f => logger.debug(s"GROUP BY AGGREGATE SCAN: IndexQuery Filter: $f"))

  // PERFORMANCE OPTIMIZATION: Resolve credentials on driver to avoid executor-side HTTP calls
  private lazy val resolvedConfig: Map[String, String] = {
    val tablePath = transactionLog.getTablePath()
    // Read path only needs PATH_READ credentials
    val readConfig = config + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    io.indextables.spark.utils.CredentialProviderFactory.resolveCredentialsOnDriver(readConfig, tablePath.toString)
  }

  override def readSchema(): StructType = {
    val resultSchema = createGroupBySchema(aggregation, groupByColumns, bucketConfig)
    logger.debug(s"GROUP BY readSchema(): Returning schema with ${resultSchema.fields.length} fields: ${resultSchema.fieldNames.mkString(", ")}")
    resultSchema
  }

  override def toBatch: Batch =
    // Note: Split locality is now managed by DriverSplitLocalityManager
    // Assignment happens during partition planning in the batch

    new IndexTables4SparkGroupByAggregateBatch(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      resolvedConfig, // Use resolved config with driver-side credentials
      aggregation,
      groupByColumns,
      indexQueryFilters,
      bucketConfig,
      partitionColumns
    )

  // Credential resolution centralized in CredentialProviderFactory.resolveCredentialsOnDriver()

  override def description(): String = {
    val groupByDesc = groupByColumns.mkString(", ")
    val aggDesc     = aggregation.aggregateExpressions.map(_.toString).mkString(", ")
    val bucketDesc  = bucketConfig.map(bc => s", bucket=${bc.description}").getOrElse("")
    s"IndexTables4SparkGroupByAggregateScan[groupBy=[$groupByDesc], aggregations=[$aggDesc]$bucketDesc]"
  }

  /** Create schema for GROUP BY aggregation results. */
  private def createGroupBySchema(
    aggregation: Aggregation,
    groupByColumns: Array[String],
    bucketConfig: Option[BucketAggregationConfig] = None
  ): StructType = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.debug(s"GROUP BY SCHEMA: Creating schema for GROUP BY on columns: ${groupByColumns.mkString(", ")}")
    bucketConfig.foreach(bc => logger.debug(s"GROUP BY SCHEMA: Bucket config present: ${bc.description}"))

    // Start with GROUP BY columns
    logger.info(
      s"GROUP BY SCHEMA: Looking up field types from schema with ${schema.fields.length} fields: ${schema.fields
          .map(f => s"${f.name}:${f.dataType}")
          .mkString(", ")}"
    )

    // IMPORTANT: Spark expects GROUP BY columns to be named group_col_0, group_col_1, etc.
    // See org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown lines 413-421
    val groupByFields = bucketConfig match {
      case Some(dhc: DateHistogramConfig) =>
        // DateHistogram returns TimestampType for bucket keys (epoch microseconds)
        // Position 0 is the bucket field, additional GROUP BY columns follow
        logger.debug(s"GROUP BY SCHEMA: DateHistogram bucket key type is TimestampType")
        val bucketField = StructField("group_col_0", TimestampType, nullable = true)
        val additionalFields = groupByColumns.drop(1).zipWithIndex.map {
          case (columnName, idx) =>
            val index = idx + 1 // Start at 1 since bucket is at 0
            schema.fields.find(_.name == columnName) match {
              case Some(field) =>
                logger.debug(s"GROUP BY SCHEMA: Additional field '$columnName' with type ${field.dataType}, naming as group_col_$index")
                StructField(s"group_col_$index", field.dataType, field.nullable)
              case None =>
                logger.warn(s"GROUP BY SCHEMA: Additional field '$columnName' not found, falling back to StringType")
                StructField(s"group_col_$index", StringType, nullable = true)
            }
        }
        Array(bucketField) ++ additionalFields

      case Some(hc: HistogramConfig) =>
        // Histogram returns DoubleType for bucket keys
        logger.debug(s"GROUP BY SCHEMA: Histogram bucket key type is DoubleType")
        val bucketField = StructField("group_col_0", DoubleType, nullable = true)
        val additionalFields = groupByColumns.drop(1).zipWithIndex.map {
          case (columnName, idx) =>
            val index = idx + 1
            schema.fields.find(_.name == columnName) match {
              case Some(field) =>
                logger.debug(s"GROUP BY SCHEMA: Additional field '$columnName' with type ${field.dataType}, naming as group_col_$index")
                StructField(s"group_col_$index", field.dataType, field.nullable)
              case None =>
                logger.warn(s"GROUP BY SCHEMA: Additional field '$columnName' not found, falling back to StringType")
                StructField(s"group_col_$index", StringType, nullable = true)
            }
        }
        Array(bucketField) ++ additionalFields

      case Some(rc: RangeConfig) =>
        // Range returns StringType for bucket keys (range names)
        logger.debug(s"GROUP BY SCHEMA: Range bucket key type is StringType")
        val bucketField = StructField("group_col_0", StringType, nullable = true)
        val additionalFields = groupByColumns.drop(1).zipWithIndex.map {
          case (columnName, idx) =>
            val index = idx + 1
            schema.fields.find(_.name == columnName) match {
              case Some(field) =>
                logger.debug(s"GROUP BY SCHEMA: Additional field '$columnName' with type ${field.dataType}, naming as group_col_$index")
                StructField(s"group_col_$index", field.dataType, field.nullable)
              case None =>
                logger.warn(s"GROUP BY SCHEMA: Additional field '$columnName' not found, falling back to StringType")
                StructField(s"group_col_$index", StringType, nullable = true)
            }
        }
        Array(bucketField) ++ additionalFields

      case None =>
        // Standard GROUP BY - use field types from schema
        groupByColumns.zipWithIndex.map {
          case (columnName, index) =>
            // Find the column type from the original schema
            schema.fields.find(_.name == columnName) match {
              case Some(field) =>
                logger.debug(
                  s"GROUP BY SCHEMA: Found field '$columnName' with type ${field.dataType}, naming as group_col_$index"
                )
                StructField(s"group_col_$index", field.dataType, field.nullable)
              case None =>
                // Fallback to string type
                logger.warn(s"GROUP BY SCHEMA: Field in schema! Falling back to StringType. Available fields: ${schema.fields.map(_.name).mkString(", ")}")
                StructField(s"group_col_$index", StringType, nullable = true)
            }
        }
    }

    // Add aggregation result columns
    // IMPORTANT: Spark expects aggregation columns to be named agg_func_0, agg_func_1, etc.
    // See org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown lines 417-420
    val aggregationFields = aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        val dataType = aggExpr match {
          case count: Count =>
            LongType
          case _: CountStar =>
            LongType
          case sum: Sum =>
            // For partial aggregations, return type must match Spark's accumulator type
            // IntegerType/LongType SUM -> LongType, FloatType/DoubleType SUM -> DoubleType
            val fieldType = getInputFieldType(sum, schema)
            fieldType match {
              case IntegerType | LongType => LongType
              case FloatType | DoubleType => DoubleType
              case _                      => DoubleType // Default to DoubleType for unknown types
            }
          case avg: Avg =>
            // AVG should not appear here if supportCompletePushDown=false
            throw new IllegalStateException(
              s"AVG aggregation should have been transformed by Spark into SUM + COUNT. " +
                s"This indicates supportCompletePushDown() may not be returning false correctly. " +
                s"Check the SupportsPushDownAggregates implementation in IndexTables4SparkScanBuilder."
            )
          case min: Min =>
            // MIN/MAX return the same type as the input field
            getInputFieldType(min, schema)
          case max: Max =>
            // MIN/MAX return the same type as the input field
            getInputFieldType(max, schema)
          case other =>
            logger.warn(s"Unknown aggregation type: ${other.getClass.getSimpleName}")
            LongType
        }
        StructField(s"agg_func_$index", dataType, nullable = true)
    }

    val resultSchema = StructType(groupByFields ++ aggregationFields)
    logger.debug(s"GROUP BY SCHEMA: Created schema with ${resultSchema.fields.length} fields: ${resultSchema.fieldNames.mkString(", ")}")
    resultSchema
  }

  /** Get the input field type for an aggregation expression. */
  private def getInputFieldType(
    aggExpr: org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc,
    schema: StructType
  ): DataType = {

    // Get the column reference from the aggregation
    val column = aggExpr.children().headOption.getOrElse {
      logger.warn(s"No children found for aggregation expression, defaulting to LongType")
      return LongType
    }

    // Extract field name (FieldReference is private, so check by class name)
    val fieldName = if (column.getClass.getSimpleName == "FieldReference") {
      column.toString
    } else {
      io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    }

    // Look up field type in schema
    schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.warn(s"Could not find field '$fieldName' in schema, defaulting to LongType")
        LongType
    }
  }
}

/** Batch implementation for GROUP BY aggregations. */
class IndexTables4SparkGroupByAggregateBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  groupByColumns: Array[String],
  indexQueryFilters: Array[Any] = Array.empty,
  bucketConfig: Option[BucketAggregationConfig] = None,
  partitionColumns: Set[String] = Set.empty) // Partition columns for optimization
    extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateBatch])

  logger.debug(s"GROUP BY BATCH: Created batch with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")

  override def planInputPartitions(): Array[InputPartition] = {
    logger.debug(s"GROUP BY BATCH: Planning input partitions for GROUP BY aggregation")

    // Get splitsPerTask configuration - aggregate scans can have separate configuration
    // Uses spark.indextables.read.aggregate.splitsPerTask if set, otherwise falls back to read.splitsPerTask
    // - "auto" or absent: auto-select based on cluster size and split count
    // - numeric value: use explicit value
    val configuredSplitsPerTask = config
      .get("spark.indextables.read.aggregate.splitsPerTask")
      .orElse(config.get("spark.indextables.read.splitsPerTask"))

    val maxSplitsPerTask = config
      .get("spark.indextables.read.aggregate.maxSplitsPerTask")
      .orElse(config.get("spark.indextables.read.maxSplitsPerTask"))
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .getOrElse(SplitsPerTaskCalculator.DefaultMaxSplitsPerTask)

    // Get available hosts for driver-based locality assignment
    val sparkContext   = sparkSession.sparkContext
    val availableHosts = io.indextables.spark.storage.DriverSplitLocalityManager.getAvailableHosts(sparkContext)
    logger.debug(s"GROUP BY BATCH: Available hosts: ${availableHosts.mkString(", ")}")

    // Get all splits from transaction log
    val allSplits = transactionLog.listFiles()
    logger.debug(s"GROUP BY BATCH: Found ${allSplits.length} total splits")

    // Apply data skipping using the same logic as simple aggregate scan
    // Use the full table schema to ensure proper field type detection for data skipping
    val helperScan = new IndexTables4SparkScan(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      None,
      config,
      indexQueryFilters
    )
    val filteredSplits = helperScan.applyDataSkipping(allSplits, pushedFilters)
    logger.debug(s"GROUP BY BATCH: After data skipping: ${filteredSplits.length} splits")

    // Calculate optimal splitsPerTask now that we know the split count
    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = filteredSplits.length,
      defaultParallelism = sparkContext.defaultParallelism,
      configuredValue = configuredSplitsPerTask,
      maxSplitsPerTask = maxSplitsPerTask
    )

    // Batch-assign all splits for this query using per-query load balancing
    val splitPaths = filteredSplits.map(_.path)
    val assignments =
      io.indextables.spark.storage.DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)
    logger.debug(s"GROUP BY BATCH: Assigned ${assignments.size} splits to hosts")

    // Group splits by host for locality-aware batching
    val splitsByHost: Map[String, Seq[io.indextables.spark.transaction.AddAction]] = filteredSplits
      .groupBy(a => assignments.getOrElse(a.path, "unknown"))

    // Create multi-split partitions (or single-split if splitsPerTask == 1)
    // Interleave partitions by host for better cluster utilization (h1,h2,h3,h1,h2,h3,... instead of h1,h1,h1,h2,h2,h2,...)
    val partitions = if (splitsPerTask == 1) {
      // Fallback to single-split behavior for backward compatibility
      // Still interleave by host for better distribution
      val batchesByHost = splitsByHost.map {
        case (host, hostSplits) =>
          host -> hostSplits.map { split =>
            val splitPartitionValues = Option(split.partitionValues).getOrElse(Map.empty[String, String])
            new IndexTables4SparkGroupByAggregatePartition(
              split,
              pushedFilters,
              config,
              aggregation,
              groupByColumns,
              transactionLog.getTablePath(),
              schema,
              indexQueryFilters,
              if (host == "unknown") None else Some(host),
              bucketConfig,
              partitionColumns,
              splitPartitionValues
            )
          }
      }
      PartitionUtils.interleaveByHost(batchesByHost)
    } else {
      // Group splits by host and batch them
      val batchesByHost = splitsByHost.map {
        case (host, hostSplits) =>
          host -> hostSplits
            .grouped(splitsPerTask)
            .map { batch =>
              new IndexTables4SparkMultiSplitGroupByAggregatePartition(
                splits = batch,
                pushedFilters = pushedFilters,
                config = config,
                aggregation = aggregation,
                groupByColumns = groupByColumns,
                tablePath = transactionLog.getTablePath(),
                schema = schema,
                indexQueryFilters = indexQueryFilters,
                preferredHost = if (host == "unknown") None else Some(host),
                bucketConfig = bucketConfig,
                partitionColumns = partitionColumns
              )
            }
            .toSeq
      }
      PartitionUtils.interleaveByHost(batchesByHost)
    }

    logger.info(s"GROUP BY BATCH: Planned ${partitions.length} partitions (${filteredSplits.length} splits, $splitsPerTask per task)")
    partitions.toArray
  }

  override def createReaderFactory(): org.apache.spark.sql.connector.read.PartitionReaderFactory = {
    logger.debug(s"GROUP BY BATCH: Creating reader factory for GROUP BY aggregation")

    new IndexTables4SparkGroupByAggregateReaderFactory(
      sparkSession,
      pushedFilters,
      config,
      aggregation,
      groupByColumns,
      schema,
      indexQueryFilters,
      bucketConfig
    )
  }

}

/** Input partition for GROUP BY aggregation processing. */
class IndexTables4SparkGroupByAggregatePartition(
  val split: io.indextables.spark.transaction.AddAction,
  val pushedFilters: Array[Filter],
  val config: Map[String, String], // Direct config instead of broadcast
  val aggregation: Aggregation,
  val groupByColumns: Array[String],
  val tablePath: org.apache.hadoop.fs.Path,
  val schema: StructType,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None,
  val bucketConfig: Option[BucketAggregationConfig] = None,
  val partitionColumns: Set[String] = Set.empty,             // Partition columns for optimization
  val splitPartitionValues: Map[String, String] = Map.empty) // Partition values from split metadata
    extends InputPartition {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregatePartition])

  logger.debug(s"GROUP BY PARTITION: Created partition for split: ${split.path}")
  logger.debug(s"GROUP BY PARTITION: Table path: $tablePath")
  logger.debug(s"GROUP BY PARTITION: GROUP BY columns: ${groupByColumns.mkString(", ")}")
  logger.debug(
    s"GROUP BY PARTITION: Aggregations: ${aggregation.aggregateExpressions.map(_.toString).mkString(", ")}"
  )
  logger.debug(s"GROUP BY PARTITION: IndexQuery filters: ${indexQueryFilters.length}")

  /**
   * Provide preferred locations for this GROUP BY aggregate partition based on driver-side split assignment. The
   * preferredHost is computed during partition planning using per-query load balancing while maintaining sticky
   * assignments for cache locality.
   */
  override def preferredLocations(): Array[String] =
    preferredHost.toArray
}

/**
 * Input partition holding multiple splits for batch GROUP BY aggregation processing. All splits share the same
 * preferredHost for cache locality. The reader will execute GROUP BY aggregations on each split and merge results by
 * group key.
 */
class IndexTables4SparkMultiSplitGroupByAggregatePartition(
  val splits: Seq[io.indextables.spark.transaction.AddAction],
  val pushedFilters: Array[Filter],
  val config: Map[String, String],
  val aggregation: Aggregation,
  val groupByColumns: Array[String],
  val tablePath: org.apache.hadoop.fs.Path,
  val schema: StructType,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None,
  val bucketConfig: Option[BucketAggregationConfig] = None,
  val partitionColumns: Set[String] = Set.empty)
    extends InputPartition {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkMultiSplitGroupByAggregatePartition])

  logger.debug(s"MULTI-SPLIT GROUP BY PARTITION: Created partition with ${splits.length} splits")
  logger.debug(s"MULTI-SPLIT GROUP BY PARTITION: GROUP BY columns: ${groupByColumns.mkString(", ")}")
  logger.info(
    s"MULTI-SPLIT GROUP BY PARTITION: Aggregations: ${aggregation.aggregateExpressions.map(_.toString).mkString(", ")}"
  )

  override def preferredLocations(): Array[String] = preferredHost.toArray
}

/** Reader factory for GROUP BY aggregation partitions. */
class IndexTables4SparkGroupByAggregateReaderFactory(
  sparkSession: SparkSession,
  pushedFilters: Array[Filter],
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  groupByColumns: Array[String],
  schema: StructType,
  indexQueryFilters: Array[Any] = Array.empty,
  bucketConfig: Option[BucketAggregationConfig] = None)
    extends org.apache.spark.sql.connector.read.PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateReaderFactory])

  logger.debug(s"GROUP BY READER FACTORY: Created with ${indexQueryFilters.length} IndexQuery filters")

  override def createReader(partition: org.apache.spark.sql.connector.read.InputPartition)
    : org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] =
    partition match {
      case multiSplitPartition: IndexTables4SparkMultiSplitGroupByAggregatePartition =>
        logger.debug(
          s"GROUP BY READER FACTORY: Creating multi-split reader with ${multiSplitPartition.splits.length} splits"
        )

        new IndexTables4SparkMultiSplitGroupByAggregateReader(
          multiSplitPartition,
          sparkSession,
          schema
        )

      case groupByPartition: IndexTables4SparkGroupByAggregatePartition =>
        logger.debug(s"GROUP BY READER FACTORY: Creating reader for GROUP BY partition")

        new IndexTables4SparkGroupByAggregateReader(
          groupByPartition,
          sparkSession,
          schema
        )

      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
}

/** Reader for GROUP BY aggregation partitions that executes terms aggregations using tantivy4java. */
class IndexTables4SparkGroupByAggregateReader(
  partition: IndexTables4SparkGroupByAggregatePartition,
  sparkSession: SparkSession,
  schema: StructType)
    extends org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateReader])
  private var groupByResults: Iterator[org.apache.spark.sql.catalyst.InternalRow] = _
  private var isInitialized                                                       = false

  // We need the schema from the scan to properly convert bucket keys
  private lazy val fieldSchema: StructType = partition.schema

  override def next(): Boolean = {
    if (!isInitialized) {
      initialize()
      isInitialized = true
    }
    groupByResults.hasNext
  }

  override def get(): org.apache.spark.sql.catalyst.InternalRow =
    groupByResults.next()

  override def close(): Unit = {
    logger.debug(s"GROUP BY READER: Closing GROUP BY reader")

    // Report bytesRead to Spark UI
    val bytesRead = partition.split.size
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)) {
      logger.debug(s"Reported input metrics for ${partition.split.path}: $bytesRead bytes")
    }
  }

  /** Initialize the GROUP BY aggregation by executing terms aggregation via tantivy4java. */
  private def initialize(): Unit = {
    logger.debug(s"GROUP BY READER: Initializing GROUP BY aggregation for split: ${partition.split.path}")

    try {
      // Execute GROUP BY aggregation using tantivy4java
      val results = executeGroupByAggregation()
      groupByResults = results.iterator
      logger.debug(s"GROUP BY READER: GROUP BY aggregation completed with ${results.length} groups")
    } catch {
      case e: IllegalArgumentException =>
        // Rethrow field validation errors - these should propagate to the user
        throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException =>
        // Rethrow IndexQuery parse errors - these should propagate to the user
        throw e
      case e: Exception =>
        logger.warn(s"GROUP BY READER: Failed to execute GROUP BY aggregation", e)
        // Return empty results on failure
        groupByResults = Iterator.empty
    }
  }

  /** Execute GROUP BY aggregation using tantivy4java terms or bucket aggregation. */
  private def executeGroupByAggregation(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    import io.indextables.tantivy4java.aggregation.TermsAggregation

    logger.debug(
      s"GROUP BY EXECUTION: Starting aggregation for GROUP BY columns: ${partition.groupByColumns.mkString(", ")}"
    )
    partition.bucketConfig.foreach(bc => logger.debug(s"GROUP BY EXECUTION: Bucket config: ${bc.description}"))
    logger.debug(s"GROUP BY EXECUTION: Split path: ${partition.split.path}")
    logger.debug(s"GROUP BY EXECUTION: Aggregation expressions: ${partition.aggregation.aggregateExpressions.length}")

    try {
      // Create cache configuration from broadcast config
      val splitCacheConfig = createCacheConfig()

      logger.debug(s"GROUP BY EXECUTION: Creating searcher for split: ${partition.split.path}")

      // Resolve relative path from AddAction against table path using utility
      val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        partition.split.path,
        partition.tablePath.toString
      )

      // Normalize path for tantivy4java compatibility (handles S3, Azure, etc.)
      // Uses Map-based fast path - no Hadoop Configuration creation needed
      val splitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
        resolvedPath,
        partition.config
      )

      logger.debug(s"GROUP BY EXECUTION: Resolved split path: $splitPath")

      // Create split metadata from the split
      val splitMetadata = createSplitMetadataFromSplit()

      // Create IndexTables4SparkOptions from config map for JSON field support
      val options = Some(IndexTables4SparkOptions(partition.config))

      // Create SplitSearchEngine for filter conversion (uses caching internally)
      val splitSearchEngine =
        io.indextables.spark.search.SplitSearchEngine.fromSplitFileWithMetadata(
          partition.schema,
          splitPath,
          splitMetadata,
          splitCacheConfig,
          options
        )

      logger.debug(s"GROUP BY EXECUTION: SplitSearchEngine created successfully")

      // Get the searcher from the SplitSearchEngine (NOT from cacheManager directly).
      // SplitSearchEngine handles companion mode by passing parquetTableRoot and
      // parquetStorageConfig to the native layer, which is required for fast field
      // access in companion (HYBRID/PARQUET_ONLY) mode.
      val searcher = splitSearchEngine.getSplitSearcher()

      // Dispatch to bucket aggregation if bucketConfig is present
      partition.bucketConfig match {
        case Some(bucketCfg) =>
          logger.debug(s"GROUP BY EXECUTION: Dispatching to bucket aggregation: ${bucketCfg.description}")
          executeBucketAggregation(searcher, splitSearchEngine, bucketCfg)

        case None =>
          // Standard terms aggregation
          executeTermsAggregation(searcher, splitSearchEngine)
      }

    } catch {
      case e: Exception =>
        logger.warn(s"GROUP BY EXECUTION: Failed to execute GROUP BY aggregation", e)
        throw e
    }
  }

  /** Execute standard terms aggregation for GROUP BY. */
  private def executeTermsAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    import io.indextables.tantivy4java.aggregation.TermsAggregation

    // Separate GROUP BY columns into partition columns (values from split metadata) and data columns (need Tantivy)
    val partitionColumns                        = partition.partitionColumns
    val allGroupByColumns                       = partition.groupByColumns
    val (partitionGroupByCols, dataGroupByCols) = allGroupByColumns.partition(partitionColumns.contains)

    if (partitionGroupByCols.nonEmpty) {
      logger.info(s"GROUP BY OPTIMIZATION: Partition columns in GROUP BY: ${partitionGroupByCols.mkString(", ")}")
      logger.info(
        s"GROUP BY OPTIMIZATION: Data columns requiring Tantivy aggregation: ${dataGroupByCols.mkString(", ")}"
      )
      logger.info(s"GROUP BY OPTIMIZATION: Split partition values: ${partition.splitPartitionValues}")
    }

    // Support both single and multi-column GROUP BY
    if (partition.groupByColumns.length >= 1) {
      // Use only data columns for Tantivy aggregation (optimization: skip partition columns)
      val groupByColumns = if (dataGroupByCols.isEmpty) {
        // All columns are partition columns - no Tantivy aggregation needed
        // Just return count for this split with partition values
        logger.info(s"GROUP BY OPTIMIZATION: All GROUP BY columns are partition columns - using simplified aggregation")
        Array.empty[String]
      } else {
        dataGroupByCols
      }

      // Handle case where all GROUP BY columns are partition columns
      if (groupByColumns.isEmpty) {
        return executePartitionOnlyAggregation(searcher, splitSearchEngine, allGroupByColumns)
      }

      logger.debug(s"GROUP BY EXECUTION: Creating TermsAggregation for ${groupByColumns.length} column(s): ${groupByColumns.mkString(", ")}")

      val (termsAgg, isMultiDimensional) = if (groupByColumns.length == 1) {
        // Single column GROUP BY - use field directly
        val groupByColumn = groupByColumns(0)
        logger.debug(s"GROUP BY EXECUTION: Single-dimensional GROUP BY on '$groupByColumn'")
        (new TermsAggregation("group_by_terms", groupByColumn, 1000, 0), false)
      } else {
        // Multi-column GROUP BY - use new MultiTermsAggregation
        logger.debug(s"GROUP BY EXECUTION: Multi-dimensional GROUP BY on ${groupByColumns.length} fields: ${groupByColumns.mkString(", ")}")
        logger.debug(s"GROUP BY EXECUTION: Using native MultiTermsAggregation")

        // Create MultiTermsAggregation with the field array
        val multiTermsAgg =
          new io.indextables.tantivy4java.aggregation.MultiTermsAggregation("group_by_terms", groupByColumns, 1000, 0)
        logger.info(
          s"GROUP BY EXECUTION: Created MultiTermsAggregation for fields: ${groupByColumns.mkString(", ")}"
        )
        (multiTermsAgg, true)
      }

      logger.debug(s"GROUP BY EXECUTION: Using TermsAggregation for GROUP BY with ${partition.aggregation.aggregateExpressions.length} aggregations")

      // Add sub-aggregations for each metric aggregation using the new API
      import org.apache.spark.sql.connector.expressions.aggregate._
      partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
        case (aggExpr, index) =>
          aggExpr match {
            case _: Count | _: CountStar =>
              // COUNT is handled via bucket doc count - no sub-aggregation needed
              logger.debug(s"GROUP BY EXECUTION: COUNT aggregation at index $index will use bucket doc count")

            case sum: Sum =>
              val fieldName = getFieldName(sum.column)
              logger.debug(s"GROUP BY EXECUTION: Adding SUM sub-aggregation for field '$fieldName' at index $index")
              termsAgg match {
                case terms: TermsAggregation =>
                  terms.addSubAggregation(
                    s"sum_$index",
                    new io.indextables.tantivy4java.aggregation.SumAggregation(fieldName)
                  )
                case multiTerms: io.indextables.tantivy4java.aggregation.MultiTermsAggregation =>
                  multiTerms.addSubAggregation(
                    s"sum_$index",
                    new io.indextables.tantivy4java.aggregation.SumAggregation(fieldName)
                  )
              }

            case avg: Avg =>
              // AVG should be automatically transformed by Spark into SUM + COUNT when supportCompletePushDown=false
              val fieldName = getFieldName(avg.column)
              throw new IllegalStateException(
                s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT. " +
                  s"This indicates supportCompletePushDown() may not be returning false correctly. " +
                  s"Check the SupportsPushDownAggregates implementation in IndexTables4SparkScanBuilder."
              )

            case min: Min =>
              val fieldName = getFieldName(min.column)
              logger.debug(s"GROUP BY EXECUTION: Adding MIN sub-aggregation for field '$fieldName' at index $index")
              termsAgg match {
                case terms: TermsAggregation =>
                  terms.addSubAggregation(
                    s"min_$index",
                    new io.indextables.tantivy4java.aggregation.MinAggregation(fieldName)
                  )
                case multiTerms: io.indextables.tantivy4java.aggregation.MultiTermsAggregation =>
                  multiTerms.addSubAggregation(
                    s"min_$index",
                    new io.indextables.tantivy4java.aggregation.MinAggregation(fieldName)
                  )
              }

            case max: Max =>
              val fieldName = getFieldName(max.column)
              logger.debug(s"GROUP BY EXECUTION: Adding MAX sub-aggregation for field '$fieldName' at index $index")
              termsAgg match {
                case terms: TermsAggregation =>
                  terms.addSubAggregation(
                    s"max_$index",
                    new io.indextables.tantivy4java.aggregation.MaxAggregation(fieldName)
                  )
                case multiTerms: io.indextables.tantivy4java.aggregation.MultiTermsAggregation =>
                  multiTerms.addSubAggregation(
                    s"max_$index",
                    new io.indextables.tantivy4java.aggregation.MaxAggregation(fieldName)
                  )
              }

            case other =>
              logger.debug(s"GROUP BY EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
          }
      }

      // Merge IndexQuery filters with pushed filters
      logger.debug(s"GROUP BY EXECUTION: Merging ${partition.pushedFilters.length} pushed filters and ${partition.indexQueryFilters.length} IndexQuery filters")
      partition.pushedFilters.foreach(f => logger.debug(s"GROUP BY EXECUTION: Pushed Filter: $f"))
      partition.indexQueryFilters.foreach(f => logger.debug(s"GROUP BY EXECUTION: IndexQuery Filter: $f"))

      // Strip partition-only filters - these are already handled by partition pruning and not indexed in Tantivy
      val nonPartitionPushedFilters = if (partition.partitionColumns.nonEmpty && partition.pushedFilters.nonEmpty) {
        val cleaned = MixedBooleanFilter.stripPartitionOnlyFilters(partition.pushedFilters, partition.partitionColumns)
        if (cleaned.length != partition.pushedFilters.length) {
          logger.info(s"GROUP BY EXECUTION: Stripped ${partition.pushedFilters.length - cleaned.length} partition-only pushed filter(s)")
        }
        cleaned
      } else {
        partition.pushedFilters
      }

      val cleanedIndexQueryFilters = if (partition.partitionColumns.nonEmpty && partition.indexQueryFilters.nonEmpty) {
        val cleaned = MixedBooleanFilter.stripPartitionFiltersFromArray(partition.indexQueryFilters, partition.partitionColumns)
        if (cleaned.length != partition.indexQueryFilters.length) {
          logger.info(s"GROUP BY EXECUTION: Stripped ${partition.indexQueryFilters.length - cleaned.length} partition-only IndexQuery filter(s)")
        }
        cleaned
      } else {
        partition.indexQueryFilters
      }

      // Combine pushed filters and IndexQuery filters
      val allFilters = nonPartitionPushedFilters ++ cleanedIndexQueryFilters

      val query = if (allFilters.nonEmpty) {
        logger.debug(s"GROUP BY EXECUTION: Converting ${allFilters.length} total filters to query")

        // Get the split field names for schema validation
        // CRITICAL: Schema must be closed to prevent native memory leak
        val splitFieldNames = {
          var schema: io.indextables.tantivy4java.core.Schema = null
          try {
            import scala.collection.JavaConverters._
            schema = splitSearchEngine.getSchema()
            if (schema != null) {
              Some(schema.getFieldNames().asScala.toSet)
            } else {
              logger.warn(s"GROUP BY EXECUTION: Schema is null, proceeding without field name validation")
              None
            }
          } catch {
            case e: Exception =>
              logger.warn(s"GROUP BY EXECUTION: Failed to get field names from schema: ${e.getMessage}")
              None
          } finally
            if (schema != null) {
              schema.close() // Prevent native memory leak
            }
        }

        // Create options from broadcast config
        import scala.jdk.CollectionConverters._
        val optionsFromBroadcast = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)

        // Convert filters to query with schema validation
        val convertedQuery = FiltersToQueryConverter.convertToSplitQuery(
          allFilters,
          splitSearchEngine,
          splitFieldNames,
          Some(optionsFromBroadcast)
        )
        logger.debug(s"GROUP BY EXECUTION: Converted filters to query: ${convertedQuery.getClass.getSimpleName}")
        convertedQuery
      } else {
        logger.debug(s"GROUP BY EXECUTION: No filters, using match-all query")
        new SplitMatchAllQuery()
      }

      logger.debug(s"GROUP BY EXECUTION: Executing TermsAggregation with sub-aggregations and filter query")
      logger.info(s"GroupBy aggregate pushdown for ${partition.split.path}: splitQuery='$query', groupBy=[${partition.groupByColumns.mkString(", ")}]")

      val result = searcher.search(query, 0, "group_by_terms", termsAgg)

      if (result.hasAggregations()) {
        logger.debug(s"GROUP BY EXECUTION: TermsAggregation completed successfully")

        // Debug: Try to get available aggregation names
        try {
          val aggregationNames = (0 until 10).map(i => s"agg_$i").filter(name => result.getAggregation(name) != null)
          logger.info(
            s"GROUP BY EXECUTION: Available aggregation names (agg_X): ${aggregationNames.mkString(", ")}"
          )
        } catch {
          case e: Exception =>
            logger.warn(s"GROUP BY EXECUTION: Error checking agg_X names: ${e.getMessage}")
        }

        // Try the standard name first, then fallback
        var aggregationResult = result.getAggregation("agg_0")
        if (aggregationResult == null) {
          logger.debug(s"GROUP BY EXECUTION: No aggregation result found for 'agg_0', trying 'group_by_terms'")
          aggregationResult = result.getAggregation("group_by_terms")
        }

        if (aggregationResult == null) {
          logger.debug(s"GROUP BY EXECUTION: No aggregation result found for 'agg_0' or 'group_by_terms'")
          return Array.empty[InternalRow]
        }

        if (isMultiDimensional) {
          // Handle MultiTermsResult for multi-dimensional GROUP BY
          logger.debug(s"GROUP BY EXECUTION: Processing MultiTermsResult for multi-dimensional GROUP BY")

          // The aggregationResult is a TermsResult containing the nested structure
          // We need to wrap it in a MultiTermsResult to flatten the nested buckets
          val termsResult = aggregationResult.asInstanceOf[TermsResult]
          val multiTermsResult =
            new io.indextables.tantivy4java.aggregation.MultiTermsResult("group_by_terms", termsResult, groupByColumns)
          val multiBuckets = multiTermsResult.getBuckets

          if (multiBuckets == null) {
            logger.debug(s"GROUP BY EXECUTION: MultiTermsResult.getBuckets() returned null")
            return Array.empty[InternalRow]
          }

          logger.debug(s"GROUP BY EXECUTION: Found ${multiBuckets.size()} multi-dimensional groups")

          // Convert multi-dimensional buckets to InternalRow
          val rows = multiBuckets.asScala
            .filter(_ != null)
            .map { multiBucket =>
              try {
                val fieldValues = multiBucket.getFieldValues()
                // Build full GROUP BY values with partition values injected
                val groupByValues = buildFullGroupByValues(
                  allGroupByColumns,
                  groupByColumns,
                  partitionGroupByCols,
                  fieldValues,
                  partition.splitPartitionValues
                )
                val aggregationValues =
                  calculateAggregationValuesFromMultiTermsBucket(multiBucket, partition.aggregation)

                val keyString = fieldValues.mkString("|")
                logger.debug(
                  s"GROUP BY EXECUTION: Multi-dimensional group '$keyString' has ${multiBucket.getDocCount} documents"
                )

                // Combine multi-dimensional GROUP BY values with aggregation results
                InternalRow.fromSeq(groupByValues ++ aggregationValues)
              } catch {
                case e: Exception =>
                  logger.warn(s"GROUP BY EXECUTION: Error processing multi-dimensional bucket: ${e.getMessage}", e)
                  // Return empty row in case of error
                  InternalRow.empty
              }
            }
            .toArray

          logger.debug(s"GROUP BY EXECUTION: Generated ${rows.length} multi-dimensional GROUP BY result rows")
          rows

        } else {
          // Handle regular TermsResult for single-dimensional GROUP BY
          val termsResult = aggregationResult.asInstanceOf[TermsResult]
          val buckets     = termsResult.getBuckets

          if (buckets == null) {
            logger.debug(s"GROUP BY EXECUTION: TermsResult.getBuckets() returned null")
            return Array.empty[InternalRow]
          }

          logger.debug(s"GROUP BY EXECUTION: Found ${buckets.size()} groups")

          // Convert buckets to InternalRow
          val rows = buckets.asScala
            .filter(_ != null)
            .map { bucket =>
              try {
                val bucketKeyValue = convertBucketKeyToSpark(bucket, groupByColumns(0))
                // Build full GROUP BY values with partition values injected
                val groupByValues = buildFullGroupByValues(
                  allGroupByColumns,
                  groupByColumns,
                  partitionGroupByCols,
                  Array(if (bucketKeyValue == null) "" else bucketKeyValue.toString),
                  partition.splitPartitionValues
                )
                val aggregationValues = calculateAggregationValuesFromSubAggregations(bucket, partition.aggregation)

                val keyString = if (bucket.getKeyAsString != null) bucket.getKeyAsString else "null"
                logger.debug(s"GROUP BY EXECUTION: Group '$keyString' has ${bucket.getDocCount} documents")

                // Combine GROUP BY value with aggregation results
                InternalRow.fromSeq(groupByValues ++ aggregationValues)
              } catch {
                case e: Exception =>
                  logger.warn(s"GROUP BY EXECUTION: Error processing bucket: ${e.getMessage}", e)
                  // Return empty row in case of error
                  InternalRow.empty
              }
            }
            .toArray

          logger.debug(s"GROUP BY EXECUTION: Generated ${rows.length} GROUP BY result rows")
          rows
        }

      } else {
        logger.debug(s"GROUP BY EXECUTION: No aggregation results returned")
        Array.empty[InternalRow]
      }

    } else {
      logger.debug(
        s"GROUP BY EXECUTION: Multi-column GROUP BY not yet implemented: ${partition.groupByColumns.mkString(", ")}"
      )
      Array.empty[InternalRow]
    }
  }

  /** Execute bucket aggregation (DateHistogram, Histogram, or Range). */
  private def executeBucketAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    bucketConfig: BucketAggregationConfig
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {

    logger.debug(s"Executing bucket aggregation: ${bucketConfig.description}")

    // Create the appropriate bucket aggregation based on config type
    bucketConfig match {
      case dhc: DateHistogramConfig =>
        executeDateHistogramAggregation(searcher, splitSearchEngine, dhc)

      case hc: HistogramConfig =>
        executeHistogramAggregationInternal(searcher, splitSearchEngine, hc)

      case rc: RangeConfig =>
        executeRangeAggregationInternal(searcher, splitSearchEngine, rc)
    }
  }

  /** Execute DateHistogram bucket aggregation. */
  private def executeDateHistogramAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    dhc: DateHistogramConfig
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {

    // Get additional GROUP BY columns (beyond the bucket field)
    val additionalGroupByColumns = partition.groupByColumns.drop(1)
    logger.debug(s"Executing DateHistogramAggregation for field '${dhc.fieldName}' with interval '${dhc.interval}'")
    if (additionalGroupByColumns.nonEmpty) {
      logger.debug(s"Additional GROUP BY columns (nested TermsAggregation): ${additionalGroupByColumns.mkString(", ")}")
    }

    val agg = new DateHistogramAggregation("bucket_agg", dhc.fieldName)
    agg.setFixedInterval(dhc.interval)
    dhc.offset.foreach(o => agg.setOffset(o))
    if (dhc.minDocCount > 0) agg.setMinDocCount(dhc.minDocCount)
    dhc.hardBoundsMin.foreach(min => dhc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
    dhc.extendedBoundsMin.foreach(min => dhc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))

    // Add nested TermsAggregation for additional GROUP BY columns (multi-key bucket aggregation)
    // Supports multiple additional columns via recursively nested TermsAggregations
    if (additionalGroupByColumns.nonEmpty) {
      val nestedTermsAgg = buildNestedTermsAggregation(additionalGroupByColumns.toList, 0)
      agg.addSubAggregation(nestedTermsAgg)
      logger.debug(s"Added nested TermsAggregation chain for columns: [${additionalGroupByColumns.mkString(", ")}]")
    } else {
      // No additional GROUP BY columns - add sub-aggregations directly to date histogram
      addSubAggregationsToDateHistogram(agg)
    }

    // Build and execute query
    val query  = buildFilterQuery(splitSearchEngine)
    val result = searcher.search(query, 10, "bucket_agg", agg)

    if (!result.hasAggregations()) {
      logger.debug("No aggregation results returned")
      return Array.empty[InternalRow]
    }

    val aggregationResult = result.getAggregation("bucket_agg")
    if (aggregationResult == null) {
      logger.debug("bucket_agg aggregation is null")
      return Array.empty[InternalRow]
    }

    // Process results differently based on whether there are additional GROUP BY columns
    if (additionalGroupByColumns.nonEmpty) {
      processDateHistogramWithNestedTermsResult(
        aggregationResult.asInstanceOf[DateHistogramResult],
        additionalGroupByColumns.toList
      )
    } else {
      processDateHistogramResult(aggregationResult.asInstanceOf[DateHistogramResult])
    }
  }

  /** Execute Histogram bucket aggregation. */
  private def executeHistogramAggregationInternal(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    hc: HistogramConfig
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {

    // Get additional GROUP BY columns (beyond the bucket field)
    val additionalGroupByColumns = partition.groupByColumns.drop(1)
    logger.debug(
      s"BUCKET EXECUTION: Creating HistogramAggregation for field '${hc.fieldName}' with interval ${hc.interval}"
    )
    if (additionalGroupByColumns.nonEmpty) {
      logger.debug(s"Additional GROUP BY columns (nested TermsAggregation): ${additionalGroupByColumns.mkString(", ")}")
    }

    val agg = new HistogramAggregation("bucket_agg", hc.fieldName, hc.interval)
    if (hc.offset != 0.0) agg.setOffset(hc.offset)
    if (hc.minDocCount > 0) agg.setMinDocCount(hc.minDocCount)
    hc.hardBoundsMin.foreach(min => hc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
    hc.extendedBoundsMin.foreach(min => hc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))

    // Add nested TermsAggregation for additional GROUP BY columns (multi-key bucket aggregation)
    // Supports multiple additional columns via recursively nested TermsAggregations
    if (additionalGroupByColumns.nonEmpty) {
      val nestedTermsAgg = buildNestedTermsAggregation(additionalGroupByColumns.toList, 0)
      agg.addSubAggregation(nestedTermsAgg)
      logger.debug(s"Added nested TermsAggregation chain for columns: [${additionalGroupByColumns.mkString(", ")}]")
    } else {
      // No additional GROUP BY columns - add sub-aggregations directly to histogram
      addSubAggregationsToHistogram(agg)
    }

    // Build and execute query
    val query = buildFilterQuery(splitSearchEngine)
    logger.debug(s"BUCKET EXECUTION: Executing Histogram aggregation")
    val result = searcher.search(query, 0, "bucket_agg", agg)

    if (!result.hasAggregations()) {
      logger.debug(s"BUCKET EXECUTION: No aggregation results returned")
      return Array.empty[InternalRow]
    }

    var aggregationResult = result.getAggregation("agg_0")
    if (aggregationResult == null) aggregationResult = result.getAggregation("bucket_agg")
    if (aggregationResult == null) return Array.empty[InternalRow]

    // Process results differently based on whether there are additional GROUP BY columns
    if (additionalGroupByColumns.nonEmpty) {
      processHistogramWithNestedTermsResult(
        aggregationResult.asInstanceOf[HistogramResult],
        additionalGroupByColumns.toList
      )
    } else {
      processHistogramResult(aggregationResult.asInstanceOf[HistogramResult])
    }
  }

  /** Execute Range bucket aggregation. */
  private def executeRangeAggregationInternal(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    rc: RangeConfig
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {

    logger.debug(s"Executing RangeAggregation for field '${rc.fieldName}' with ${rc.ranges.size} ranges")

    val agg = new RangeAggregation("bucket_agg", rc.fieldName)
    rc.ranges.foreach { range =>
      // RangeAggregation uses addRange(key, from, to) where null represents unbounded
      val fromVal: java.lang.Double = range.from.map(Double.box).orNull
      val toVal: java.lang.Double   = range.to.map(Double.box).orNull
      agg.addRange(range.key, fromVal, toVal)
    }

    // Build and execute query
    val query  = buildFilterQuery(splitSearchEngine)
    val result = searcher.search(query, 10, "bucket_agg", agg)

    if (!result.hasAggregations()) {
      logger.debug("No aggregation results returned")
      return Array.empty[InternalRow]
    }

    val aggregationResult = result.getAggregation("bucket_agg")
    if (aggregationResult == null) {
      logger.debug("bucket_agg aggregation is null")
      return Array.empty[InternalRow]
    }

    processRangeResult(aggregationResult.asInstanceOf[RangeResult])
  }

  /** Add sub-aggregations to DateHistogram aggregation. */
  private def addSubAggregationsToDateHistogram(agg: DateHistogramAggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            logger.debug(s"BUCKET EXECUTION: COUNT aggregation at index $index will use bucket doc count")

          case sum: Sum =>
            val fieldName = getFieldName(sum.column)
            logger.debug(s"BUCKET EXECUTION: Adding SUM sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new SumAggregation(s"sum_$index", fieldName))

          case avg: Avg =>
            val fieldName = getFieldName(avg.column)
            throw new IllegalStateException(
              s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT."
            )

          case min: Min =>
            val fieldName = getFieldName(min.column)
            logger.debug(s"BUCKET EXECUTION: Adding MIN sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new MinAggregation(s"min_$index", fieldName))

          case max: Max =>
            val fieldName = getFieldName(max.column)
            logger.debug(s"BUCKET EXECUTION: Adding MAX sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new MaxAggregation(s"max_$index", fieldName))

          case other =>
            logger.debug(s"BUCKET EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
        }
    }
  }

  /** Add sub-aggregations to Histogram aggregation. */
  private def addSubAggregationsToHistogram(agg: HistogramAggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            logger.debug(s"BUCKET EXECUTION: COUNT aggregation at index $index will use bucket doc count")

          case sum: Sum =>
            val fieldName = getFieldName(sum.column)
            logger.debug(s"BUCKET EXECUTION: Adding SUM sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new SumAggregation(s"sum_$index", fieldName))

          case avg: Avg =>
            val fieldName = getFieldName(avg.column)
            throw new IllegalStateException(
              s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT."
            )

          case min: Min =>
            val fieldName = getFieldName(min.column)
            logger.debug(s"BUCKET EXECUTION: Adding MIN sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new MinAggregation(s"min_$index", fieldName))

          case max: Max =>
            val fieldName = getFieldName(max.column)
            logger.debug(s"BUCKET EXECUTION: Adding MAX sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(new MaxAggregation(s"max_$index", fieldName))

          case other =>
            logger.debug(s"BUCKET EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
        }
    }
  }

  /** Add sub-aggregations to a nested TermsAggregation (for multi-key bucket aggregations). */
  /**
   * Build recursively nested TermsAggregations for multiple additional GROUP BY columns. The structure is: Terms(col1)
   * -> Terms(col2) -> ... -> Terms(colN) -> metric sub-aggregations
   *
   * @param columns
   *   The additional GROUP BY column names
   * @param depth
   *   The current nesting depth (used for naming)
   * @return
   *   The root TermsAggregation with nested children
   */
  private def buildNestedTermsAggregation(columns: List[String], depth: Int): TermsAggregation = {
    val columnName = columns.head
    val aggName    = if (depth == 0) "nested_terms" else s"nested_terms_$depth"
    val termsAgg   = new TermsAggregation(aggName, columnName, 1000, 0)

    if (columns.tail.isEmpty) {
      // Last column - add the metric sub-aggregations here
      addSubAggregationsToTerms(termsAgg)
      logger.debug(s"BUCKET EXECUTION: Created leaf TermsAggregation '$aggName' for column '$columnName' with metric sub-aggregations")
    } else {
      // More columns - add nested TermsAggregation
      val nestedAgg     = buildNestedTermsAggregation(columns.tail, depth + 1)
      val nestedAggName = if (depth + 1 == 1) "nested_terms_1" else s"nested_terms_${depth + 1}"
      termsAgg.addSubAggregation(nestedAggName, nestedAgg)
      logger.debug(s"BUCKET EXECUTION: Created TermsAggregation '$aggName' for column '$columnName' with nested terms")
    }

    termsAgg
  }

  private def addSubAggregationsToTerms(agg: TermsAggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            logger.debug(s"BUCKET EXECUTION: COUNT aggregation at index $index will use bucket doc count")

          case sum: Sum =>
            val fieldName = getFieldName(sum.column)
            logger.debug(s"BUCKET EXECUTION: Adding SUM sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(s"sum_$index", new SumAggregation(s"sum_$index", fieldName))

          case avg: Avg =>
            val fieldName = getFieldName(avg.column)
            throw new IllegalStateException(
              s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT."
            )

          case min: Min =>
            val fieldName = getFieldName(min.column)
            logger.debug(s"BUCKET EXECUTION: Adding MIN sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(s"min_$index", new MinAggregation(s"min_$index", fieldName))

          case max: Max =>
            val fieldName = getFieldName(max.column)
            logger.debug(s"BUCKET EXECUTION: Adding MAX sub-aggregation for field '$fieldName' at index $index")
            agg.addSubAggregation(s"max_$index", new MaxAggregation(s"max_$index", fieldName))

          case other =>
            logger.debug(s"BUCKET EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
        }
    }
  }

  /** Build filter query for bucket aggregation. */
  private def buildFilterQuery(splitSearchEngine: io.indextables.spark.search.SplitSearchEngine)
    : io.indextables.tantivy4java.split.SplitQuery = {
    // Strip partition-only filters - already handled by partition pruning, not indexed in Tantivy
    val nonPartitionPushedFilters = if (partition.partitionColumns.nonEmpty && partition.pushedFilters.nonEmpty) {
      val cleaned = MixedBooleanFilter.stripPartitionOnlyFilters(partition.pushedFilters, partition.partitionColumns)
      if (cleaned.length != partition.pushedFilters.length) {
        logger.info(s"BUCKET EXECUTION: Stripped ${partition.pushedFilters.length - cleaned.length} partition-only pushed filter(s)")
      }
      cleaned
    } else {
      partition.pushedFilters
    }
    val cleanedIndexQueryFilters = if (partition.partitionColumns.nonEmpty && partition.indexQueryFilters.nonEmpty) {
      val cleaned = MixedBooleanFilter.stripPartitionFiltersFromArray(partition.indexQueryFilters, partition.partitionColumns)
      if (cleaned.length != partition.indexQueryFilters.length) {
        logger.info(s"BUCKET EXECUTION: Stripped ${partition.indexQueryFilters.length - cleaned.length} partition-only IndexQuery filter(s)")
      }
      cleaned
    } else {
      partition.indexQueryFilters
    }
    val allFilters = nonPartitionPushedFilters ++ cleanedIndexQueryFilters
    if (allFilters.nonEmpty) {
      logger.debug(s"BUCKET EXECUTION: Converting ${allFilters.length} filters to query")
      val splitFieldNames = getSplitFieldNames(splitSearchEngine)
      import scala.jdk.CollectionConverters._
      val optionsFromBroadcast = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)
      val query = FiltersToQueryConverter.convertToSplitQuery(
        allFilters,
        splitSearchEngine,
        splitFieldNames,
        Some(optionsFromBroadcast)
      )
      logger.info(s"Bucket aggregate pushdown for ${partition.split.path}: splitQuery='$query'")
      query
    } else {
      logger.debug(s"BUCKET EXECUTION: No filters, using match-all query")
      new SplitMatchAllQuery()
    }
  }

  /** Get split field names for schema validation. */
  private def getSplitFieldNames(splitSearchEngine: io.indextables.spark.search.SplitSearchEngine)
    : Option[Set[String]] = {
    var schema: io.indextables.tantivy4java.core.Schema = null
    try {
      import scala.collection.JavaConverters._
      schema = splitSearchEngine.getSchema()
      if (schema != null) Some(schema.getFieldNames().asScala.toSet) else None
    } catch {
      case e: Exception =>
        logger.warn(s"BUCKET EXECUTION: Failed to get field names: ${e.getMessage}")
        None
    } finally
      if (schema != null) schema.close()
  }

  /** Process DateHistogram aggregation result into InternalRows (single-key bucket aggregation). */
  private def processDateHistogramResult(result: DateHistogramResult): Array[InternalRow] = {
    import scala.jdk.CollectionConverters._
    val buckets = result.getBuckets
    if (buckets == null) return Array.empty[InternalRow]

    buckets.asScala
      .filter(_ != null)
      .map { bucket =>
        // DateHistogram returns epoch milliseconds, Spark TimestampType expects microseconds
        val keyMillis         = bucket.getKey.toLong
        val keyMicros         = keyMillis * 1000L // Convert milliseconds to microseconds
        val aggregationValues = calculateBucketAggregationValues(bucket)
        InternalRow.fromSeq(Seq(keyMicros) ++ aggregationValues)
      }
      .toArray
  }

  /**
   * Process DateHistogram with nested TermsAggregation result into flattened InternalRows. Supports multiple levels of
   * nested TermsAggregation for multi-key bucket aggregation.
   *
   * @param result
   *   The DateHistogram aggregation result
   * @param nestedColumnNames
   *   The list of additional GROUP BY column names (corresponds to nested terms levels)
   */
  private def processDateHistogramWithNestedTermsResult(
    result: DateHistogramResult,
    nestedColumnNames: List[String]
  ): Array[InternalRow] = {
    import scala.jdk.CollectionConverters._
    val histBuckets = result.getBuckets
    if (histBuckets == null) return Array.empty[InternalRow]

    logger.debug(s"BUCKET EXECUTION: Processing DateHistogram with ${nestedColumnNames.length} nested terms columns: [${nestedColumnNames.mkString(", ")}]")

    // Flatten: for each histogram bucket, recursively iterate over nested terms buckets
    histBuckets.asScala
      .filter(_ != null)
      .flatMap { histBucket =>
        // DateHistogram returns epoch milliseconds, Spark TimestampType expects microseconds
        val keyMillis = histBucket.getKey.toLong
        val keyMicros = keyMillis * 1000L

        // Get first nested terms aggregation
        val nestedTermsResult = histBucket.getSubAggregation("nested_terms", classOf[TermsResult])
        if (nestedTermsResult == null || nestedTermsResult.getBuckets == null) {
          logger.debug(s"BUCKET EXECUTION: No nested terms results for histogram bucket at $keyMillis")
          Seq.empty[InternalRow]
        } else {
          // Recursively flatten all nested terms levels
          flattenNestedTermsBuckets(nestedTermsResult, nestedColumnNames.tail, 0)
            .map {
              case (termKeys, aggregationValues) =>
                val allKeys = Seq(keyMicros) ++ termKeys
                logger.debug(s"BUCKET EXECUTION: DateHistogram[$keyMicros] + Terms[${termKeys.mkString(", ")}] -> ${aggregationValues.mkString(", ")}")
                InternalRow.fromSeq(allKeys ++ aggregationValues)
            }
        }
      }
      .toArray
  }

  /**
   * Recursively flatten nested TermsAggregation buckets into (keys, aggregationValues) tuples. Returns a sequence of
   * (termKeys, aggregationValues) where termKeys has one value per nesting level.
   */
  private def flattenNestedTermsBuckets(
    termsResult: TermsResult,
    remainingColumnNames: List[String],
    depth: Int
  ): Seq[(Seq[Any], Array[Any])] = {
    import scala.jdk.CollectionConverters._

    if (termsResult == null || termsResult.getBuckets == null) {
      return Seq.empty
    }

    termsResult.getBuckets.asScala
      .filter(_ != null)
      .flatMap { termBucket =>
        val termKey = UTF8String.fromString(termBucket.getKey.toString)

        if (remainingColumnNames.isEmpty) {
          // This is the deepest level - extract aggregation values
          val aggregationValues = calculateNestedTermsBucketAggregationValues(termBucket)
          Seq((Seq(termKey), aggregationValues))
        } else {
          // More nesting levels - recurse into nested terms
          val nestedAggName     = s"nested_terms_${depth + 1}"
          val nestedTermsResult = termBucket.getSubAggregation(nestedAggName)
          if (nestedTermsResult == null) {
            logger.debug(s"BUCKET EXECUTION: No nested aggregation '$nestedAggName' found at depth $depth")
            Seq.empty[(Seq[Any], Array[Any])]
          } else {
            flattenNestedTermsBuckets(nestedTermsResult.asInstanceOf[TermsResult], remainingColumnNames.tail, depth + 1)
              .map {
                case (nestedKeys, aggregationValues) =>
                  (Seq(termKey) ++ nestedKeys, aggregationValues)
              }
          }
        }
      }
      .toSeq
  }

  /** Calculate aggregation values from a nested TermsBucket (within a bucket aggregation). */
  private def calculateNestedTermsBucketAggregationValues(
    bucket: io.indextables.tantivy4java.aggregation.TermsResult.TermsBucket
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            bucket.getDocCount.toLong

          case sum: Sum =>
            try {
              // TermsBucket.getSubAggregation returns AggregationResult, need to cast
              val sumResult = bucket.getSubAggregation(s"sum_$index").asInstanceOf[SumResult]
              if (sumResult != null) convertSumValue(sumResult.getSum, sum) else 0L
            } catch { case _: Exception => 0L }

          case min: Min =>
            try {
              val minResult = bucket.getSubAggregation(s"min_$index").asInstanceOf[MinResult]
              if (minResult != null) convertMinMaxValue(minResult.getMin, min) else 0.0
            } catch { case _: Exception => 0.0 }

          case max: Max =>
            try {
              val maxResult = bucket.getSubAggregation(s"max_$index").asInstanceOf[MaxResult]
              if (maxResult != null) convertMinMaxValue(maxResult.getMax, max) else 0.0
            } catch { case _: Exception => 0.0 }

          case _ => 0L
        }
    }
  }

  /** Process Histogram aggregation result into InternalRows (single-key bucket aggregation). */
  private def processHistogramResult(result: HistogramResult): Array[InternalRow] = {
    import scala.jdk.CollectionConverters._
    val buckets = result.getBuckets
    if (buckets == null) return Array.empty[InternalRow]

    logger.debug(s"BUCKET EXECUTION: Processing ${buckets.size()} histogram buckets")

    buckets.asScala
      .filter(_ != null)
      .map { bucket =>
        // Histogram returns double for bucket key
        val key               = bucket.getKey
        val aggregationValues = calculateBucketAggregationValues(bucket)
        logger.debug(s"BUCKET EXECUTION: Histogram bucket key=$key, docCount=${bucket.getDocCount}")
        InternalRow.fromSeq(Seq(key) ++ aggregationValues)
      }
      .toArray
  }

  /**
   * Process Histogram with nested TermsAggregation result into flattened InternalRows. Supports multiple levels of
   * nested TermsAggregation for multi-key bucket aggregation.
   *
   * @param result
   *   The Histogram aggregation result
   * @param nestedColumnNames
   *   The list of additional GROUP BY column names (corresponds to nested terms levels)
   */
  private def processHistogramWithNestedTermsResult(
    result: HistogramResult,
    nestedColumnNames: List[String]
  ): Array[InternalRow] = {
    import scala.jdk.CollectionConverters._
    val histBuckets = result.getBuckets
    if (histBuckets == null) return Array.empty[InternalRow]

    logger.debug(s"BUCKET EXECUTION: Processing Histogram with ${nestedColumnNames.length} nested terms columns: [${nestedColumnNames.mkString(", ")}]")

    // Flatten: for each histogram bucket, recursively iterate over nested terms buckets
    histBuckets.asScala
      .filter(_ != null)
      .flatMap { histBucket =>
        // Histogram returns double for bucket key
        val histKey = histBucket.getKey

        // Get first nested terms aggregation
        val nestedTermsResult = histBucket.getSubAggregation("nested_terms", classOf[TermsResult])
        if (nestedTermsResult == null || nestedTermsResult.getBuckets == null) {
          logger.debug(s"BUCKET EXECUTION: No nested terms results for histogram bucket at $histKey")
          Seq.empty[InternalRow]
        } else {
          // Recursively flatten all nested terms levels
          flattenNestedTermsBuckets(nestedTermsResult, nestedColumnNames.tail, 0)
            .map {
              case (termKeys, aggregationValues) =>
                val allKeys = Seq(histKey) ++ termKeys
                logger.debug(s"BUCKET EXECUTION: Histogram[$histKey] + Terms[${termKeys.mkString(", ")}] -> ${aggregationValues.mkString(", ")}")
                InternalRow.fromSeq(allKeys ++ aggregationValues)
            }
        }
      }
      .toArray
  }

  /** Process Range aggregation result into InternalRows. */
  private def processRangeResult(result: RangeResult): Array[InternalRow] = {
    import scala.jdk.CollectionConverters._
    val buckets = result.getBuckets
    if (buckets == null) return Array.empty[InternalRow]

    buckets.asScala
      .filter(_ != null)
      .map { bucket =>
        // Range returns string key (range name)
        val key               = UTF8String.fromString(bucket.getKey)
        val aggregationValues = calculateRangeBucketAggregationValues(bucket)
        InternalRow.fromSeq(Seq(key) ++ aggregationValues)
      }
      .toArray
  }

  /** Calculate aggregation values from a DateHistogram/Histogram bucket. */
  private def calculateBucketAggregationValues(
    bucket: io.indextables.tantivy4java.aggregation.DateHistogramResult.DateHistogramBucket
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            bucket.getDocCount.toLong

          case sum: Sum =>
            extractSumFromBucket(bucket, index, sum)

          case min: Min =>
            extractMinFromBucket(bucket, index, min)

          case max: Max =>
            extractMaxFromBucket(bucket, index, max)

          case _ => 0L
        }
    }
  }

  /** Calculate aggregation values from a Histogram bucket. */
  private def calculateBucketAggregationValues(
    bucket: io.indextables.tantivy4java.aggregation.HistogramResult.HistogramBucket
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            bucket.getDocCount.toLong

          case sum: Sum =>
            try {
              val sumResult = bucket.getSubAggregation(s"sum_$index", classOf[SumResult])
              if (sumResult != null) convertSumValue(sumResult.getSum, sum) else 0L
            } catch { case _: Exception => 0L }

          case min: Min =>
            try {
              val minResult = bucket.getSubAggregation(s"min_$index", classOf[MinResult])
              if (minResult != null) convertMinMaxValue(minResult.getMin, min) else 0.0
            } catch { case _: Exception => 0.0 }

          case max: Max =>
            try {
              val maxResult = bucket.getSubAggregation(s"max_$index", classOf[MaxResult])
              if (maxResult != null) convertMinMaxValue(maxResult.getMax, max) else 0.0
            } catch { case _: Exception => 0.0 }

          case _ => 0L
        }
    }
  }

  /** Calculate aggregation values from a Range bucket. */
  private def calculateRangeBucketAggregationValues(
    bucket: io.indextables.tantivy4java.aggregation.RangeResult.RangeBucket
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    // Note: RangeAggregation doesn't support sub-aggregations in current tantivy4java version
    // Only return COUNT values
    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            bucket.getDocCount.toLong

          case sum: Sum =>
            // Range buckets don't support sub-aggregations yet
            try {
              val sumResult = bucket.getSubAggregation(s"sum_$index", classOf[SumResult])
              if (sumResult != null) convertSumValue(sumResult.getSum, sum) else 0L
            } catch { case _: Exception => 0L }

          case min: Min =>
            try {
              val minResult = bucket.getSubAggregation(s"min_$index", classOf[MinResult])
              if (minResult != null) convertMinMaxValue(minResult.getMin, min) else 0.0
            } catch { case _: Exception => 0.0 }

          case max: Max =>
            try {
              val maxResult = bucket.getSubAggregation(s"max_$index", classOf[MaxResult])
              if (maxResult != null) convertMinMaxValue(maxResult.getMax, max) else 0.0
            } catch { case _: Exception => 0.0 }

          case _ => 0L
        }
    }
  }

  /** Extract SUM value from DateHistogram bucket. */
  private def extractSumFromBucket(
    bucket: io.indextables.tantivy4java.aggregation.DateHistogramResult.DateHistogramBucket,
    index: Int,
    sum: org.apache.spark.sql.connector.expressions.aggregate.Sum
  ): Any =
    try {
      val sumResult = bucket.getSubAggregation(s"sum_$index", classOf[SumResult])
      if (sumResult != null) convertSumValue(sumResult.getSum, sum) else 0L
    } catch { case _: Exception => 0L }

  /** Extract MIN value from DateHistogram bucket. */
  private def extractMinFromBucket(
    bucket: io.indextables.tantivy4java.aggregation.DateHistogramResult.DateHistogramBucket,
    index: Int,
    min: org.apache.spark.sql.connector.expressions.aggregate.Min
  ): Any =
    try {
      val minResult = bucket.getSubAggregation(s"min_$index", classOf[MinResult])
      if (minResult != null) convertMinMaxValue(minResult.getMin, min) else 0.0
    } catch { case _: Exception => 0.0 }

  /** Extract MAX value from DateHistogram bucket. */
  private def extractMaxFromBucket(
    bucket: io.indextables.tantivy4java.aggregation.DateHistogramResult.DateHistogramBucket,
    index: Int,
    max: org.apache.spark.sql.connector.expressions.aggregate.Max
  ): Any =
    try {
      val maxResult = bucket.getSubAggregation(s"max_$index", classOf[MaxResult])
      if (maxResult != null) convertMinMaxValue(maxResult.getMax, max) else 0.0
    } catch { case _: Exception => 0.0 }

  /** Convert SUM value to appropriate type. */
  private def convertSumValue(value: Double, sum: org.apache.spark.sql.connector.expressions.aggregate.Sum): Any = {
    val fieldName = getFieldName(sum.column)
    val fieldType = getFieldType(fieldName)
    fieldType match {
      case IntegerType | LongType => java.lang.Long.valueOf(Math.round(value))
      case _                      => java.lang.Double.valueOf(value)
    }
  }

  /** Convert MIN/MAX value to appropriate type. */
  private def convertMinMaxValue(
    value: Double,
    aggExpr: org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
  ): Any = {
    val column = aggExpr.children().headOption.getOrElse(return value)
    val fieldName =
      if (column.getClass.getSimpleName == "FieldReference") column.toString
      else io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    val fieldType = getFieldType(fieldName)
    fieldType match {
      case IntegerType => java.lang.Integer.valueOf(Math.round(value).toInt)
      case LongType    => java.lang.Long.valueOf(Math.round(value))
      case FloatType   => java.lang.Float.valueOf(value.toFloat)
      case _           => java.lang.Double.valueOf(value)
    }
  }

  /** Create cache configuration from broadcast config */
  private def createCacheConfig(): io.indextables.spark.storage.SplitCacheConfig = {
    // Use the centralized utility for consistent configuration
    val cacheConfig = io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
      partition.config,
      Some(partition.tablePath.toString)
    )

    logger.info(
      s"GROUP BY EXECUTION: Created cache config with AWS credentials - accessKey=${cacheConfig.awsAccessKey
          .map(k => s"${k.take(4)}***")
          .getOrElse("None")}"
    )

    // Return SplitCacheConfig directly (for SplitSearchEngine)
    cacheConfig
  }

  /** Create SplitMetadata from the existing split information. */
  private def createSplitMetadataFromSplit(): QuickwitSplit.SplitMetadata =
    io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
      partition.split,
      partition.tablePath.toString
    )

  /** Convert bucket key to appropriate Spark value. */
  private def convertBucketKeyToSpark(
    bucket: io.indextables.tantivy4java.aggregation.TermsResult.TermsBucket,
    fieldName: String
  ): Any = {
    import org.apache.spark.unsafe.types.UTF8String

    if (bucket == null) {
      logger.warn(s"GROUP BY EXECUTION: Bucket is null for field $fieldName")
      return UTF8String.fromString("")
    }

    val keyAsString = bucket.getKeyAsString
    if (keyAsString == null) {
      logger.warn(s"GROUP BY EXECUTION: Bucket key is null for field $fieldName")
      return UTF8String.fromString("")
    }

    // Get the field type from the field schema to determine conversion
    val fieldType = fieldSchema.fields.find(_.name == fieldName).map(_.dataType)

    fieldType match {
      case Some(org.apache.spark.sql.types.StringType) =>
        UTF8String.fromString(keyAsString)
      case Some(org.apache.spark.sql.types.IntegerType) =>
        try
          keyAsString.toInt
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$keyAsString' to Int: ${e.getMessage}")
            0
        }
      case Some(org.apache.spark.sql.types.LongType) =>
        try
          keyAsString.toLong
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$keyAsString' to Long: ${e.getMessage}")
            0L
        }
      case Some(org.apache.spark.sql.types.FloatType) =>
        try
          keyAsString.toFloat
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$keyAsString' to Float: ${e.getMessage}")
            0.0f
        }
      case Some(org.apache.spark.sql.types.DoubleType) =>
        try
          keyAsString.toDouble
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$keyAsString' to Double: ${e.getMessage}")
            0.0
        }
      case Some(org.apache.spark.sql.types.DateType) =>
        // Convert date string to days since epoch (Int) as Spark expects
        convertDateStringToDays(keyAsString)
      case Some(org.apache.spark.sql.types.TimestampType) =>
        // Convert timestamp to microseconds since epoch (Long) as Spark expects
        convertTimestampStringToMicros(keyAsString)
      case _ =>
        // Default to string
        UTF8String.fromString(keyAsString)
    }
  }

  /**
   * Convert a date string to days since epoch (Int) as Spark expects for DateType. Handles multiple formats:
   *   - YYYY-MM-DD format (e.g., "2024-01-15")
   *   - ISO datetime format (e.g., "2024-01-02T00:00:00Z") - extracts date part
   *   - LocalDateTime format (e.g., "2024-01-02T00:00:00") - extracts date part
   */
  private def convertDateStringToDays(dateStr: String): Int = {
    import java.time.LocalDate

    try
      if (dateStr.contains("T")) {
        // ISO datetime format - extract date part
        LocalDate.parse(dateStr.substring(0, 10)).toEpochDay.toInt
      } else if (dateStr.contains("-")) {
        // Simple date format YYYY-MM-DD
        LocalDate.parse(dateStr).toEpochDay.toInt
      } else {
        // Numeric string: already days-since-epoch (from Tantivy fast field
        // aggregation or Iceberg partition values)
        dateStr.toInt
      }
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Cannot convert date string '$dateStr' to days since epoch: ${e.getMessage}",
          e
        )
    }
  }

  /**
   * Convert a timestamp string to microseconds since epoch (Long) as Spark expects for TimestampType. Handles multiple
   * formats:
   *   - Numeric microseconds (already in correct format from tantivy)
   *   - ISO instant format (e.g., "2024-01-01T10:00:00Z")
   *   - URL-encoded strings (e.g., "2024-01-01T15%3A00%3A00Z") - partition paths encode colons
   *   - LocalDateTime format (e.g., "2024-01-01T10:00:00")
   */
  private def convertTimestampStringToMicros(tsStr: String): Long = {
    import java.time.{Instant, LocalDateTime, ZoneOffset}

    try {
      // Check if already numeric (microseconds from tantivy)
      if (tsStr.forall(c => c.isDigit || c == '-')) {
        return tsStr.toLong
      }

      // Check for URL-encoded strings (partition paths encode colons as %3A)
      val decodedStr = if (tsStr.contains("%")) {
        java.net.URLDecoder.decode(tsStr, "UTF-8")
      } else {
        tsStr
      }

      // Parse based on format
      val instant = if (decodedStr.endsWith("Z")) {
        // ISO instant format
        Instant.parse(decodedStr)
      } else if (decodedStr.contains("T")) {
        // LocalDateTime format - assume UTC
        LocalDateTime.parse(decodedStr).toInstant(ZoneOffset.UTC)
      } else {
        throw new IllegalArgumentException(s"Unrecognized timestamp format: $tsStr")
      }

      // Convert to microseconds
      instant.getEpochSecond * 1000000L + instant.getNano / 1000L
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Cannot convert timestamp string '$tsStr' to microseconds since epoch: ${e.getMessage}",
          e
        )
    }
  }

  /** Convert string value to appropriate Spark type */
  private def convertStringValueToSpark(value: String, dataType: org.apache.spark.sql.types.DataType): Any = {
    import org.apache.spark.unsafe.types.UTF8String
    import org.apache.spark.sql.types._

    dataType match {
      case StringType => UTF8String.fromString(value)
      case IntegerType =>
        try
          value.toInt
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$value' to Int: ${e.getMessage}")
            0
        }
      case LongType =>
        try
          value.toLong
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$value' to Long: ${e.getMessage}")
            0L
        }
      case FloatType =>
        try
          value.toFloat
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$value' to Float: ${e.getMessage}")
            0.0f
        }
      case DoubleType =>
        try
          value.toDouble
        catch {
          case e: NumberFormatException =>
            logger.warn(s"GROUP BY EXECUTION: Cannot convert '$value' to Double: ${e.getMessage}")
            0.0
        }
      case DateType =>
        // Convert date string to days since epoch (Int) as Spark expects
        convertDateStringToDays(value)
      case TimestampType =>
        // Convert timestamp to microseconds since epoch (Long) as Spark expects
        convertTimestampStringToMicros(value)
      case _ => UTF8String.fromString(value)
    }
  }

  /** Get the Spark DataType for a field from the schema */
  private def getFieldType(fieldName: String): DataType =
    partition.schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.debug(s"AGGREGATION TYPE: Field '$fieldName' not found in schema, defaulting to LongType")
        LongType
    }

  /** Get the input field type for an aggregation expression. */
  private def getInputFieldType(
    aggExpr: org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc,
    schema: StructType
  ): DataType = {

    // Get the column reference from the aggregation
    val column = aggExpr.children().headOption.getOrElse {
      logger.warn(s"No children found for aggregation expression, defaulting to LongType")
      return LongType
    }

    // Extract field name (FieldReference is private, so check by class name)
    val fieldName = if (column.getClass.getSimpleName == "FieldReference") {
      column.toString
    } else {
      val exprStr = column.toString
      if (exprStr.startsWith("FieldReference(")) {
        val pattern = """FieldReference\(([^)]+)\)""".r
        pattern.findFirstMatchIn(exprStr) match {
          case Some(m) => m.group(1)
          case None    => "unknown_field"
        }
      } else {
        "unknown_field"
      }
    }

    // Look up field type in schema
    schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.warn(s"Could not find field '$fieldName' in schema, defaulting to LongType")
        LongType
    }
  }

  /** Calculate aggregation values using sub-aggregations from MultiTermsBucket */
  private def calculateAggregationValuesFromMultiTermsBucket(
    multiBucket: io.indextables.tantivy4java.aggregation.MultiTermsResult.MultiTermsBucket,
    aggregation: Aggregation
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    if (multiBucket == null) {
      logger.warn(s"GROUP BY EXECUTION: MultiTermsBucket is null in calculateAggregationValuesFromMultiTermsBucket")
      return Array(0L)
    }

    aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        if (aggExpr == null) {
          logger.warn(s"GROUP BY EXECUTION: Aggregate expression is null at index $index")
          0L
        } else {
          aggExpr match {
            case _: Count | _: CountStar =>
              // For COUNT/COUNT(*), use the document count from the bucket
              multiBucket.getDocCount.toLong

            case sum: Sum =>
              // Extract SUM result from sub-aggregation
              try {
                val sumResult = multiBucket
                  .getSubAggregation(s"sum_$index")
                  .asInstanceOf[io.indextables.tantivy4java.aggregation.SumResult]
                if (sumResult != null) {
                  // tantivy4java returns double, convert to appropriate type based on input field
                  val sumValue  = sumResult.getSum
                  val fieldType = getInputFieldType(sum, schema)
                  val result: Any = fieldType match {
                    case IntegerType | LongType =>
                      val longVal: Long = Math.round(sumValue)
                      java.lang.Long.valueOf(longVal)
                    case _ => java.lang.Double.valueOf(sumValue)
                  }
                  result
                } else {
                  logger.warn(s"GROUP BY EXECUTION: SUM sub-aggregation result is null for index $index")
                  java.lang.Long.valueOf(0L)
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting SUM sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  java.lang.Long.valueOf(0L)
              }

            case _: Avg =>
              // AVG should not appear here if supportCompletePushDown=false
              throw new IllegalStateException(
                s"AVG aggregation should have been transformed by Spark into SUM + COUNT. " +
                  s"This indicates supportCompletePushDown() may not be returning false correctly. " +
                  s"Check the SupportsPushDownAggregates implementation in IndexTables4SparkScanBuilder."
              )

            case min: Min =>
              // Extract MIN result from sub-aggregation
              try {
                val minResult = multiBucket
                  .getSubAggregation(s"min_$index")
                  .asInstanceOf[io.indextables.tantivy4java.aggregation.MinResult]
                if (minResult != null) {
                  // tantivy4java returns double, convert to appropriate type based on input field
                  val minValue  = minResult.getMin
                  val fieldType = getInputFieldType(min, schema)
                  val result: Any = fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(minValue).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(minValue)
                      java.lang.Long.valueOf(longVal)
                    case FloatType => java.lang.Float.valueOf(minValue.toFloat)
                    case _         => java.lang.Double.valueOf(minValue)
                  }
                  result
                } else {
                  logger.warn(s"GROUP BY EXECUTION: MIN sub-aggregation result is null for index $index")
                  java.lang.Double.valueOf(0.0)
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting MIN sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  java.lang.Double.valueOf(0.0)
              }

            case max: Max =>
              // Extract MAX result from sub-aggregation
              try {
                val maxResult = multiBucket
                  .getSubAggregation(s"max_$index")
                  .asInstanceOf[io.indextables.tantivy4java.aggregation.MaxResult]
                if (maxResult != null) {
                  // tantivy4java returns double, convert to appropriate type based on input field
                  val maxValue  = maxResult.getMax
                  val fieldType = getInputFieldType(max, schema)
                  val result: Any = fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(maxValue).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(maxValue)
                      java.lang.Long.valueOf(longVal)
                    case FloatType => java.lang.Float.valueOf(maxValue.toFloat)
                    case _         => java.lang.Double.valueOf(maxValue)
                  }
                  result
                } else {
                  logger.warn(s"GROUP BY EXECUTION: MAX sub-aggregation result is null for index $index")
                  java.lang.Double.valueOf(0.0)
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting MAX sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  java.lang.Double.valueOf(0.0)
              }

            case other =>
              logger.debug(s"GROUP BY EXECUTION: Unknown aggregation type: ${other.getClass.getSimpleName}")
              0L
          }
        }
    }
  }

  /** Calculate aggregation values using sub-aggregations from bucket */
  private def calculateAggregationValuesFromSubAggregations(
    bucket: io.indextables.tantivy4java.aggregation.TermsResult.TermsBucket,
    aggregation: Aggregation
  ): Array[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    if (bucket == null) {
      logger.warn(s"GROUP BY EXECUTION: Bucket is null in calculateAggregationValuesFromSubAggregations")
      return Array(0L)
    }

    aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        if (aggExpr == null) {
          logger.warn(s"GROUP BY EXECUTION: Aggregate expression is null at index $index")
          0L
        } else {
          aggExpr match {
            case _: Count | _: CountStar =>
              // For COUNT/COUNT(*), use the document count from the bucket
              bucket.getDocCount.toLong

            case sum: Sum =>
              // Extract SUM result from sub-aggregation
              try {
                val sumResult =
                  bucket.getSubAggregation(s"sum_$index").asInstanceOf[io.indextables.tantivy4java.aggregation.SumResult]
                if (sumResult != null) {
                  val fieldName = getFieldName(sum.column)
                  val fieldType = getFieldType(fieldName)
                  // Return appropriate type based on OUTPUT type (SUM widens integers to Long)
                  fieldType match {
                    case IntegerType | LongType =>
                      // SUM widens to LongType in schema, always return Long
                      val longVal: Long = Math.round(sumResult.getSum)
                      java.lang.Long.valueOf(longVal)
                    case FloatType | DoubleType => sumResult.getSum.toDouble
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for SUM on '$fieldName': $fieldType, returning as Double")
                      sumResult.getSum.toDouble
                  }
                } else {
                  logger.warn(s"GROUP BY EXECUTION: SUM sub-aggregation result is null for index $index")
                  0L
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting SUM sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  0L
              }

            case _: Avg =>
              // AVG should not appear here if supportCompletePushDown=false
              throw new IllegalStateException(
                s"AVG aggregation should have been transformed by Spark into SUM + COUNT. " +
                  s"This indicates supportCompletePushDown() may not be returning false correctly. " +
                  s"Check the SupportsPushDownAggregates implementation in IndexTables4SparkScanBuilder."
              )

            case min: Min =>
              // Extract MIN result from sub-aggregation
              try {
                val minResult =
                  bucket.getSubAggregation(s"min_$index").asInstanceOf[io.indextables.tantivy4java.aggregation.MinResult]
                if (minResult != null) {
                  val fieldName = getFieldName(min.column)
                  val fieldType = getFieldType(fieldName)
                  // Return appropriate type based on field type
                  fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(minResult.getMin).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(minResult.getMin)
                      java.lang.Long.valueOf(longVal)
                    case FloatType | DoubleType => minResult.getMin.toDouble
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for MIN on '$fieldName': $fieldType, returning as Double")
                      minResult.getMin.toDouble
                  }
                } else {
                  logger.warn(s"GROUP BY EXECUTION: MIN sub-aggregation result is null for index $index")
                  0L
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting MIN sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  0L
              }

            case max: Max =>
              // Extract MAX result from sub-aggregation
              try {
                val maxResult =
                  bucket.getSubAggregation(s"max_$index").asInstanceOf[io.indextables.tantivy4java.aggregation.MaxResult]
                if (maxResult != null) {
                  val fieldName = getFieldName(max.column)
                  val fieldType = getFieldType(fieldName)
                  // Return appropriate type based on field type
                  fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(maxResult.getMax).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(maxResult.getMax)
                      java.lang.Long.valueOf(longVal)
                    case FloatType | DoubleType => maxResult.getMax.toDouble
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for MAX on '$fieldName': $fieldType, returning as Double")
                      maxResult.getMax.toDouble
                  }
                } else {
                  logger.warn(s"GROUP BY EXECUTION: MAX sub-aggregation result is null for index $index")
                  0L
                }
              } catch {
                case e: Exception =>
                  logger.warn(
                    s"GROUP BY EXECUTION: Error extracting MAX sub-aggregation result for index $index: ${e.getMessage}"
                  )
                  0L
              }

            case other =>
              logger.debug(s"GROUP BY EXECUTION: Unknown aggregation type: ${other.getClass.getSimpleName}")
              0L
          }
        }
    }
  }

  /** Extract field name from column expression for aggregations */
  private def getFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String =
    // Check if it's a FieldReference by class name (like in ScanBuilder)
    if (column.getClass.getSimpleName == "FieldReference") {
      // For FieldReference, toString() returns the field name directly
      val fieldName = column.toString
      logger.debug(s"FIELD EXTRACTION: Successfully extracted field name '$fieldName' from FieldReference")
      fieldName
    } else {
      // Fallback to ExpressionUtils
      val fieldName = io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
      if (fieldName == "unknown_field") {
        throw new UnsupportedOperationException(s"Complex column expressions not supported for aggregation: $column")
      }
      fieldName
    }

  /**
   * Build full GROUP BY values array by combining partition values from split metadata with data values from Tantivy
   * aggregation results. This maintains the correct column order as expected by Spark.
   *
   * @param allGroupByColumns
   *   All GROUP BY columns in original order
   * @param dataGroupByCols
   *   Data columns (non-partition) that were aggregated by Tantivy
   * @param partitionGroupByCols
   *   Partition columns whose values come from split metadata
   * @param dataFieldValues
   *   Values from Tantivy bucket for data columns
   * @param splitPartitionValues
   *   Partition values from split metadata
   * @return
   *   Array of Spark-compatible values in the correct column order
   */
  private def buildFullGroupByValues(
    allGroupByColumns: Array[String],
    dataGroupByCols: Array[String],
    partitionGroupByCols: Array[String],
    dataFieldValues: Array[String],
    splitPartitionValues: Map[String, String]
  ): Array[Any] = {
    var dataColIndex = 0
    allGroupByColumns.map { colName =>
      val fieldType = getFieldType(colName)
      if (partitionGroupByCols.contains(colName)) {
        // Get value from split metadata for partition columns
        val valueStr = splitPartitionValues.getOrElse(colName, "")
        logger.debug(s"GROUP BY OPTIMIZATION: Injecting partition value '$valueStr' for column '$colName'")
        convertStringValueToSpark(valueStr, fieldType)
      } else {
        // Get value from Tantivy bucket for data columns
        val valueStr = if (dataColIndex < dataFieldValues.length) dataFieldValues(dataColIndex) else ""
        dataColIndex += 1
        convertStringValueToSpark(valueStr, fieldType)
      }
    }
  }

  /**
   * Execute aggregation when all GROUP BY columns are partition columns. In this case, we don't need Tantivy terms
   * aggregation - just count documents and return a single row with partition values from split metadata.
   */
  private def executePartitionOnlyAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    allGroupByColumns: Array[String]
  ): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.info(s"GROUP BY OPTIMIZATION: Executing partition-only aggregation for split ${partition.split.path}")

    // Build filter query to count matching documents
    val query = buildFilterQuery(splitSearchEngine)

    // CountAggregation(name, fieldName) uses Tantivy's value_count on a fast field.
    // The single-arg constructor CountAggregation(fieldName) treats its argument as the
    // field name (not the aggregation name), so calling it as CountAggregation("count_agg")
    // would create value_count on a non-existent field "count_agg" and name the aggregation
    // "count_count_agg" — causing getAggregation("count_agg") to always return null → 0.
    //
    // Priority for the count field:
    //   1. Companion tracking fields (__pq_file_hash / __pq_row_in_file) — always non-null.
    //   2. The explicit COUNT(col) column, if it is a fast field.
    //   3. Any other fast field from the split's docMapping.
    val fastFields =
      io.indextables.spark.transaction.EnhancedTransactionLogCache
        .getDocMappingMetadata(partition.split)
        .fastFields

    val countFieldOpt: Option[String] =
      if (fastFields.contains("__pq_file_hash")) Some("__pq_file_hash")
      else if (fastFields.contains("__pq_row_in_file")) Some("__pq_row_in_file")
      else {
        val colFromAgg = partition.aggregation.aggregateExpressions.collectFirst {
          case count: Count =>
            val fieldName = getFieldName(count.column)
            if (fastFields.contains(fieldName)) Some(fieldName) else None
        }.flatten
        colFromAgg.orElse(if (fastFields.nonEmpty) Some(fastFields.head) else None)
      }

    val docCount = countFieldOpt match {
      case Some(countField) =>
        val countAgg = new io.indextables.tantivy4java.aggregation.CountAggregation("count_agg", countField)
        val result   = searcher.search(query, 0, "count_agg", countAgg)
        if (result.hasAggregations()) {
          val countResult = result.getAggregation("count_agg")
          if (countResult != null)
            countResult.asInstanceOf[io.indextables.tantivy4java.aggregation.CountResult].getCount
          else {
            logger.warn(s"GROUP BY OPTIMIZATION: count_agg result was null for split ${partition.split.path}")
            0L
          }
        } else {
          logger.warn(s"GROUP BY OPTIMIZATION: no aggregation results returned for split ${partition.split.path}")
          0L
        }
      case None =>
        logger.warn(
          s"GROUP BY OPTIMIZATION: no fast fields available in split ${partition.split.path} — " +
            s"document count may be inaccurate (returning partition numRecords)"
        )
        partition.split.numRecords.getOrElse(0L)
    }

    logger.info(s"GROUP BY OPTIMIZATION: Split has $docCount documents matching filters")

    // Build GROUP BY values from partition metadata
    val groupByValues = allGroupByColumns.map { colName =>
      val fieldType = getFieldType(colName)
      val valueStr  = partition.splitPartitionValues.getOrElse(colName, "")
      convertStringValueToSpark(valueStr, fieldType)
    }

    // Calculate aggregation values - for partition-only, we can compute simpler aggregations
    val aggregationValues = partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            docCount.toLong

          case sum: Sum =>
            // For SUM on partition-only aggregation, we need to run a SUM aggregation
            val fieldName = getFieldName(sum.column)
            try {
              val sumAgg    = new io.indextables.tantivy4java.aggregation.SumAggregation("sum_agg", fieldName)
              val sumResult = searcher.search(query, 0, "sum_agg", sumAgg)
              if (sumResult.hasAggregations()) {
                val agg = sumResult.getAggregation("sum_agg")
                if (agg != null) {
                  val sumValue  = agg.asInstanceOf[io.indextables.tantivy4java.aggregation.SumResult].getSum
                  val fieldType = getFieldType(fieldName)
                  fieldType match {
                    case IntegerType | LongType => java.lang.Long.valueOf(Math.round(sumValue))
                    case _                      => java.lang.Double.valueOf(sumValue)
                  }
                } else 0L
              } else 0L
            } catch { case _: Exception => 0L }

          case min: Min =>
            val fieldName = getFieldName(min.column)
            try {
              val minAgg    = new io.indextables.tantivy4java.aggregation.MinAggregation("min_agg", fieldName)
              val minResult = searcher.search(query, 0, "min_agg", minAgg)
              if (minResult.hasAggregations()) {
                val agg = minResult.getAggregation("min_agg")
                if (agg != null) {
                  val minValue = agg.asInstanceOf[io.indextables.tantivy4java.aggregation.MinResult].getMin
                  convertMinMaxValue(minValue, min)
                } else 0.0
              } else 0.0
            } catch { case _: Exception => 0.0 }

          case max: Max =>
            val fieldName = getFieldName(max.column)
            try {
              val maxAgg    = new io.indextables.tantivy4java.aggregation.MaxAggregation("max_agg", fieldName)
              val maxResult = searcher.search(query, 0, "max_agg", maxAgg)
              if (maxResult.hasAggregations()) {
                val agg = maxResult.getAggregation("max_agg")
                if (agg != null) {
                  val maxValue = agg.asInstanceOf[io.indextables.tantivy4java.aggregation.MaxResult].getMax
                  convertMinMaxValue(maxValue, max)
                } else 0.0
              } else 0.0
            } catch { case _: Exception => 0.0 }

          case _ => 0L
        }
    }

    // Return single row with partition values and aggregation results
    Array(InternalRow.fromSeq(groupByValues ++ aggregationValues))
  }
}

/**
 * Reader for multi-split GROUP BY aggregation partitions. Executes GROUP BY aggregations on each split and merges
 * results by group key:
 *   - Rows with same group key(s) are combined
 *   - COUNT/SUM: adds values together
 *   - MIN: takes the minimum across all splits
 *   - MAX: takes the maximum across all splits
 */
class IndexTables4SparkMultiSplitGroupByAggregateReader(
  partition: IndexTables4SparkMultiSplitGroupByAggregatePartition,
  sparkSession: SparkSession,
  schema: StructType)
    extends org.apache.spark.sql.connector.read.PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkMultiSplitGroupByAggregateReader])
  private var aggregateResults: Iterator[InternalRow] = _
  private var isInitialized                           = false

  override def next(): Boolean = {
    if (!isInitialized) {
      initialize()
      isInitialized = true
    }
    aggregateResults.hasNext
  }

  override def get(): InternalRow =
    aggregateResults.next()

  override def close(): Unit = {
    logger.debug(s"MULTI-SPLIT GROUP BY READER: Closing reader for ${partition.splits.length} splits")

    // Report bytesRead to Spark UI
    val totalBytesRead = partition.splits.map(_.size).sum
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(totalBytesRead, 0)) {
      logger.debug(s"Reported input metrics: $totalBytesRead bytes from ${partition.splits.length} splits")
    }
  }

  /** Initialize by executing GROUP BY aggregations on all splits and merging results by group key. */
  private def initialize(): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.debug(s"MULTI-SPLIT GROUP BY READER: Initializing with ${partition.splits.length} splits")

    try {
      // Execute GROUP BY on each split and collect all results
      val allSplitResults = partition.splits.flatMap { split =>
        // Create a temporary single-split partition for each split
        val splitPartitionValues = Option(split.partitionValues).getOrElse(Map.empty[String, String])
        val singlePartition = new IndexTables4SparkGroupByAggregatePartition(
          split,
          partition.pushedFilters,
          partition.config,
          partition.aggregation,
          partition.groupByColumns,
          partition.tablePath,
          partition.schema,
          partition.indexQueryFilters,
          None,
          partition.bucketConfig,
          partition.partitionColumns,
          splitPartitionValues
        )

        // Create a reader for this single split
        val singleReader = new IndexTables4SparkGroupByAggregateReader(singlePartition, sparkSession, schema)

        // Collect all results from this split
        val results = scala.collection.mutable.ArrayBuffer[InternalRow]()
        try
          while (singleReader.next())
            results += singleReader.get().copy()
        finally
          singleReader.close()
        results
      }

      if (allSplitResults.isEmpty) {
        logger.debug(s"MULTI-SPLIT GROUP BY READER: No results from any split")
        aggregateResults = Iterator.empty
        return
      }

      // Merge results by group key
      val mergedResults = mergeGroupByResults(allSplitResults, partition.aggregation, partition.groupByColumns.length)
      aggregateResults = mergedResults.iterator

      logger.debug(s"MULTI-SPLIT GROUP BY READER: Merged ${allSplitResults.length} rows into ${mergedResults.length} grouped rows from ${partition.splits.length} splits")

    } catch {
      case e: IllegalArgumentException =>
        throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException =>
        // Rethrow IndexQuery parse errors - these should propagate to the user
        throw e
      case e: Exception =>
        logger.error(s"MULTI-SPLIT GROUP BY READER: Failed to execute GROUP BY aggregation", e)
        aggregateResults = Iterator.empty
    }
  }

  /**
   * Merge GROUP BY results from multiple splits by group key. Rows with the same group key(s) are combined:
   *   - COUNT/SUM: adds values together
   *   - MIN: takes the minimum across all splits
   *   - MAX: takes the maximum across all splits
   */
  private def mergeGroupByResults(
    results: Seq[InternalRow],
    aggregation: Aggregation,
    numGroupByCols: Int
  ): Array[InternalRow] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    // Group rows by their group key(s)
    // Group key is the first numGroupByCols columns
    val groupedByKey = results.groupBy { row =>
      (0 until numGroupByCols).map(i => if (row.isNullAt(i)) null else row.get(i, null))
    }

    // Merge each group
    val numAggCols = aggregation.aggregateExpressions.length
    groupedByKey.map {
      case (groupKey, groupRows) =>
        // Start with group key values
        val mergedValues = new Array[Any](numGroupByCols + numAggCols)

        // Copy group key values from first row
        for (i <- 0 until numGroupByCols)
          mergedValues(i) = if (groupRows.head.isNullAt(i)) null else groupRows.head.get(i, null)

        // Merge aggregation values
        aggregation.aggregateExpressions.zipWithIndex.foreach {
          case (aggExpr, aggIdx) =>
            val colIdx = numGroupByCols + aggIdx

            aggExpr match {
              case _: Count | _: CountStar =>
                // COUNT: sum all counts
                val totalCount = groupRows.filter(!_.isNullAt(colIdx)).map(_.getLong(colIdx)).sum
                mergedValues(colIdx) = java.lang.Long.valueOf(totalCount)

              case _: Sum =>
                // SUM: sum all values
                val firstValue = groupRows.find(r => !r.isNullAt(colIdx)).map(_.get(colIdx, null))
                firstValue match {
                  case Some(_: java.lang.Long) =>
                    val totalSum = groupRows.filter(!_.isNullAt(colIdx)).map(_.getLong(colIdx)).sum
                    mergedValues(colIdx) = java.lang.Long.valueOf(totalSum)
                  case Some(_: java.lang.Double) =>
                    val totalSum = groupRows.filter(!_.isNullAt(colIdx)).map(_.getDouble(colIdx)).sum
                    mergedValues(colIdx) = java.lang.Double.valueOf(totalSum)
                  case _ =>
                    val totalSum = groupRows
                      .filter(!_.isNullAt(colIdx))
                      .map { row =>
                        try row.getLong(colIdx)
                        catch { case _: ClassCastException => row.getDouble(colIdx).toLong }
                      }
                      .sum
                    mergedValues(colIdx) = java.lang.Long.valueOf(totalSum)
                }

              case _: Min =>
                // MIN: take the minimum
                val validRows = groupRows.filter(!_.isNullAt(colIdx))
                if (validRows.isEmpty) {
                  mergedValues(colIdx) = null
                } else {
                  val firstValue = validRows.head.get(colIdx, null)
                  firstValue match {
                    case _: java.lang.Integer =>
                      mergedValues(colIdx) = java.lang.Integer.valueOf(validRows.map(_.getInt(colIdx)).min)
                    case _: java.lang.Long =>
                      mergedValues(colIdx) = java.lang.Long.valueOf(validRows.map(_.getLong(colIdx)).min)
                    case _: java.lang.Float =>
                      mergedValues(colIdx) = java.lang.Float.valueOf(validRows.map(_.getFloat(colIdx)).min)
                    case _: java.lang.Double =>
                      mergedValues(colIdx) = java.lang.Double.valueOf(validRows.map(_.getDouble(colIdx)).min)
                    case _ =>
                      mergedValues(colIdx) = java.lang.Double.valueOf(validRows.map(_.getDouble(colIdx)).min)
                  }
                }

              case _: Max =>
                // MAX: take the maximum
                val validRows = groupRows.filter(!_.isNullAt(colIdx))
                if (validRows.isEmpty) {
                  mergedValues(colIdx) = null
                } else {
                  val firstValue = validRows.head.get(colIdx, null)
                  firstValue match {
                    case _: java.lang.Integer =>
                      mergedValues(colIdx) = java.lang.Integer.valueOf(validRows.map(_.getInt(colIdx)).max)
                    case _: java.lang.Long =>
                      mergedValues(colIdx) = java.lang.Long.valueOf(validRows.map(_.getLong(colIdx)).max)
                    case _: java.lang.Float =>
                      mergedValues(colIdx) = java.lang.Float.valueOf(validRows.map(_.getFloat(colIdx)).max)
                    case _: java.lang.Double =>
                      mergedValues(colIdx) = java.lang.Double.valueOf(validRows.map(_.getDouble(colIdx)).max)
                    case _ =>
                      mergedValues(colIdx) = java.lang.Double.valueOf(validRows.map(_.getDouble(colIdx)).max)
                  }
                }

              case _ =>
                // Default: take from first row
                mergedValues(colIdx) = if (groupRows.head.isNullAt(colIdx)) null else groupRows.head.get(colIdx, null)
            }
        }

        InternalRow.fromSeq(mergedValues.toSeq)
    }.toArray
  }
}
