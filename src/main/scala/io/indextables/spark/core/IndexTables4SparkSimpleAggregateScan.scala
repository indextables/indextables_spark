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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.TransactionLog
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitCacheManager
import org.slf4j.LoggerFactory

/**
 * Specialized scan for simple aggregations (no GROUP BY). Handles queries like SELECT COUNT(*) FROM table, SELECT
 * SUM(price) FROM table, etc.
 */
class IndexTables4SparkSimpleAggregateScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  indexQueryFilters: Array[Any] = Array.empty)
    extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkSimpleAggregateScan])

  logger.debug(s"SIMPLE AGGREGATE SCAN: Created with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")
  pushedFilters.foreach(f => logger.debug(s"SIMPLE AGGREGATE SCAN: Filter: $f"))
  indexQueryFilters.foreach(f => logger.debug(s"SIMPLE AGGREGATE SCAN: IndexQuery Filter: $f"))

  override def readSchema(): StructType =
    createSimpleAggregateSchema(aggregation)

  override def toBatch: Batch = {
    logger.debug(s"SIMPLE AGGREGATE SCAN: toBatch() called, creating batch")

    // Update broadcast locality information before partition planning
    // This helps ensure preferred locations are accurate for aggregate operations
    try {
      val sparkContext = sparkSession.sparkContext
      logger.info("Updating broadcast locality before simple aggregate partition planning")
      io.indextables.spark.storage.BroadcastSplitLocalityManager.updateBroadcastLocality(sparkContext)
      logger.info("Broadcast locality update completed for simple aggregate scan")
    } catch {
      case ex: Exception =>
        logger.warn("Failed to update broadcast locality information for simple aggregate", ex)
    }

    new IndexTables4SparkSimpleAggregateBatch(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      config,
      aggregation,
      indexQueryFilters
    )
  }

  override def description(): String = {
    val aggDesc = aggregation.aggregateExpressions.map(_.toString).mkString(", ")
    s"IndexTables4SparkSimpleAggregateScan[aggregations=[$aggDesc]]"
  }

  /** Create schema for simple aggregation results. */
  private def createSimpleAggregateSchema(aggregation: Aggregation): StructType = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.info(
      s"SIMPLE AGGREGATE SCHEMA: Creating schema for ${aggregation.aggregateExpressions.length} aggregations"
    )

    val aggregationFields = aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        val (columnName, dataType) = aggExpr match {
          case _: Count =>
            (s"count", LongType)
          case _: CountStar =>
            (s"count(*)", LongType)
          case sum: Sum =>
            // For partial aggregations, return type must match Spark's accumulator type
            val fieldType = getInputFieldType(sum, schema)
            val sumType = fieldType match {
              case IntegerType | LongType => LongType
              case FloatType | DoubleType => DoubleType
              case _                      => DoubleType
            }
            (s"sum", sumType)
          case _: Avg =>
            // tantivy4java AverageResult.getAverage() returns double
            (s"avg", DoubleType)
          case min: Min =>
            // MIN returns the same type as the input field
            val fieldType = getInputFieldType(min, schema)
            (s"min", fieldType)
          case max: Max =>
            // MAX returns the same type as the input field
            val fieldType = getInputFieldType(max, schema)
            (s"max", fieldType)
          case other =>
            logger.warn(s"Unknown aggregation type: ${other.getClass.getSimpleName}")
            (s"agg_$index", LongType)
        }
        StructField(columnName, dataType, nullable = true)
    }

    val resultSchema = StructType(aggregationFields)
    logger.debug(s"SIMPLE AGGREGATE SCHEMA: Created schema with ${resultSchema.fields.length} fields: ${resultSchema.fieldNames.mkString(", ")}")
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

  /** Get the data type of a column from an expression. */
  private def getColumnDataType(column: org.apache.spark.sql.connector.expressions.Expression)
    : org.apache.spark.sql.types.DataType = {
    import org.apache.spark.sql.types.LongType

    // Extract field name and look it up in schema
    val fieldName = io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.warn(s"Could not find field '$fieldName' in schema, defaulting to LongType")
        LongType
    }
  }
}

/** Batch implementation for simple aggregations. */
class IndexTables4SparkSimpleAggregateBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  indexQueryFilters: Array[Any] = Array.empty)
    extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkSimpleAggregateBatch])

  logger.debug(s"SIMPLE AGGREGATE BATCH: Created batch with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")

  override def planInputPartitions(): Array[InputPartition] = {
    logger.debug(s"SIMPLE AGGREGATE BATCH: Planning input partitions for simple aggregation")

    // Get all splits from transaction log
    val allSplits = transactionLog.listFiles()
    logger.debug(s"SIMPLE AGGREGATE BATCH: Found ${allSplits.length} total splits")

    // Apply data skipping using the same logic as regular scan by creating a helper scan instance
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
    logger.debug(s"SIMPLE AGGREGATE BATCH: After data skipping: ${filteredSplits.length} splits")

    // Create one partition per filtered split for distributed aggregation processing
    filteredSplits.map { split =>
      new IndexTables4SparkSimpleAggregatePartition(
        split,
        schema,
        pushedFilters,
        config,
        aggregation,
        transactionLog.getTablePath(),
        indexQueryFilters
      )
    }.toArray
  }

  override def createReaderFactory(): org.apache.spark.sql.connector.read.PartitionReaderFactory = {
    logger.debug(s"SIMPLE AGGREGATE BATCH: Creating reader factory for simple aggregation")

    new IndexTables4SparkSimpleAggregateReaderFactory(
      sparkSession,
      pushedFilters,
      config,
      aggregation,
      indexQueryFilters
    )
  }
}

/** Input partition for simple aggregation processing. */
class IndexTables4SparkSimpleAggregatePartition(
  val split: io.indextables.spark.transaction.AddAction,
  val schema: StructType,
  val pushedFilters: Array[Filter],
  val config: Map[String, String], // Direct config instead of broadcast
  val aggregation: Aggregation,
  val tablePath: org.apache.hadoop.fs.Path,
  val indexQueryFilters: Array[Any] = Array.empty)
    extends InputPartition {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkSimpleAggregatePartition])

  logger.debug(s"SIMPLE AGGREGATE PARTITION: Created partition for split: ${split.path}")
  logger.debug(s"SIMPLE AGGREGATE PARTITION: Table path: $tablePath")
  logger.info(
    s"SIMPLE AGGREGATE PARTITION: Aggregations: ${aggregation.aggregateExpressions.map(_.toString).mkString(", ")}"
  )
  logger.debug(s"SIMPLE AGGREGATE PARTITION: IndexQuery filters: ${indexQueryFilters.length}")

  /**
   * Provide preferred locations for this aggregate partition based on split cache locality. Uses the same
   * broadcast-based locality information as regular scan partitions.
   */
  override def preferredLocations(): Array[String] = {
    import io.indextables.spark.storage.{BroadcastSplitLocalityManager, SplitLocationRegistry}

    logger.info(s"🎯 [SIMPLE-AGG] preferredLocations() called for split: ${split.path}")

    val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(split.path)
    if (preferredHosts.nonEmpty) {
      logger.info(s"🎯 [SIMPLE-AGG] Using broadcast preferred hosts: ${preferredHosts.mkString(", ")}")
      preferredHosts
    } else {
      logger.info(s"🎯 [SIMPLE-AGG] No broadcast hosts found, trying legacy registry")
      // Fallback to legacy registry for compatibility
      val legacyHosts = SplitLocationRegistry.getPreferredHosts(split.path)
      if (legacyHosts.nonEmpty) {
        logger.info(s"🎯 [SIMPLE-AGG] Using legacy preferred hosts: ${legacyHosts.mkString(", ")}")
        legacyHosts
      } else {
        logger.info(s"🎯 [SIMPLE-AGG] No preferred hosts found - letting Spark decide")
        // No cache history available, let Spark decide
        Array.empty[String]
      }
    }
  }
}

/** Reader factory for simple aggregation partitions. */
class IndexTables4SparkSimpleAggregateReaderFactory(
  sparkSession: SparkSession,
  pushedFilters: Array[Filter],
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  indexQueryFilters: Array[Any] = Array.empty)
    extends org.apache.spark.sql.connector.read.PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkSimpleAggregateReaderFactory])

  logger.debug(s"SIMPLE AGGREGATE READER FACTORY: Created with ${indexQueryFilters.length} IndexQuery filters")

  override def createReader(partition: org.apache.spark.sql.connector.read.InputPartition)
    : org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] =
    partition match {
      case simpleAggPartition: IndexTables4SparkSimpleAggregatePartition =>
        logger.debug(s"SIMPLE AGGREGATE READER FACTORY: Creating reader for simple aggregate partition: ${simpleAggPartition.split.path}")
        logger.debug(s"SIMPLE AGGREGATE READER FACTORY: Aggregations: ${simpleAggPartition.aggregation.aggregateExpressions.map(_.toString).mkString(", ")}")

        new IndexTables4SparkSimpleAggregateReader(
          simpleAggPartition,
          sparkSession
        )
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
}

/** Reader for simple aggregation partitions that executes aggregations using tantivy4java. */
class IndexTables4SparkSimpleAggregateReader(
  partition: IndexTables4SparkSimpleAggregatePartition,
  sparkSession: SparkSession)
    extends org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkSimpleAggregateReader])
  private var aggregateResults: Iterator[org.apache.spark.sql.catalyst.InternalRow] = _
  private var isInitialized                                                         = false

  // Helper function to get config with defaults
  private def getConfig(configKey: String, default: String = ""): String = {
    val value = partition.config.getOrElse(configKey, default)
    Option(value).getOrElse(default)
  }

  override def next(): Boolean = {
    if (!isInitialized) {
      initialize()
      isInitialized = true
    }
    aggregateResults.hasNext
  }

  override def get(): org.apache.spark.sql.catalyst.InternalRow =
    aggregateResults.next()

  override def close(): Unit =
    logger.debug(s"SIMPLE AGGREGATE READER: Closing simple aggregate reader")

  /** Initialize the simple aggregation by executing aggregation via tantivy4java. */
  private def initialize(): Unit = {
    logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: Called for split: ${partition.split.path}")
    logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: Aggregate expressions: ${partition.aggregation.aggregateExpressions.map(_.getClass.getSimpleName).mkString(", ")}")
    logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: Pushed filters: ${partition.pushedFilters.length}, IndexQuery filters: ${partition.indexQueryFilters.length}")

    try {
      // Execute simple aggregation using tantivy4java
      logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: About to call executeSimpleAggregation()")
      val results = executeSimpleAggregation()
      aggregateResults = results.iterator
      logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: Simple aggregation completed with ${results.length} result(s)")
      if (results.nonEmpty) {
        logger.warn(s"SIMPLE AGGREGATE READER INITIALIZE: First result contains ${results.head.numFields} fields")
      }
    } catch {
      case e: Exception =>
        logger.error(s"SIMPLE AGGREGATE READER INITIALIZE: Failed to execute simple aggregation", e)
        // Return empty results on failure
        aggregateResults = Iterator.empty
    }
  }

  /** Execute simple aggregation using tantivy4java aggregations. */
  private def executeSimpleAggregation(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    logger.warn(s"EXECUTE SIMPLE AGGREGATION: Starting execution")
    logger.warn(s"EXECUTE SIMPLE AGGREGATION: Split path: ${partition.split.path}")
    logger.warn(s"EXECUTE SIMPLE AGGREGATION: Aggregate expressions count: ${partition.aggregation.aggregateExpressions.length}")
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.unsafe.types.UTF8String
    import io.indextables.tantivy4java.split.{SplitMatchAllQuery, SplitAggregation}
    import io.indextables.tantivy4java.aggregation.{
      CountAggregation,
      SumAggregation,
      AverageAggregation,
      MinAggregation,
      MaxAggregation
    }
    import scala.collection.mutable.ArrayBuffer
    import scala.collection.JavaConverters._

    logger.debug(s"SIMPLE AGGREGATE EXECUTION: Starting simple aggregation")
    logger.debug(s"SIMPLE AGGREGATE EXECUTION: Split path: ${partition.split.path}")
    logger.info(
      s"SIMPLE AGGREGATE EXECUTION: Aggregation expressions: ${partition.aggregation.aggregateExpressions.length}"
    )

    try {
      // Create cache configuration from config
      val cacheConfig = io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
        partition.config,
        Some(partition.tablePath.toString)
      )

      logger.debug(s"SIMPLE AGGREGATE EXECUTION: Creating searcher for split: ${partition.split.path}")
      logger.debug(s"PATH DEBUG: partition.split.path = '${partition.split.path}'")
      logger.debug(s"PATH DEBUG: partition.tablePath = '${partition.tablePath}'")
      logger.debug(s"PATH DEBUG: startsWith('/') = ${partition.split.path.startsWith("/")}")
      logger.debug(s"PATH DEBUG: contains('://') = ${partition.split.path.contains("://")}")

      // Resolve relative path from AddAction against table path using utility
      val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        partition.split.path,
        partition.tablePath.toString
      )

      logger.debug(s"PATH DEBUG: resolvedPath = '$resolvedPath'")

      // Normalize s3a:// to s3:// for tantivy4java compatibility
      val splitPath = resolvedPath.replace("s3a://", "s3://")

      logger.debug(s"SIMPLE AGGREGATE EXECUTION: Resolved split path: $splitPath")
      logger.debug(s"PATH DEBUG: final splitPath = '$splitPath'")

      // Create split metadata from the split
      val splitMetadata = createSplitMetadataFromSplit()

      // Create IndexTables4SparkOptions from config map for JSON field support
      val options = Some(IndexTables4SparkOptions(partition.config))

      // Create SplitSearchEngine for filter conversion and schema access
      val splitSearchEngine = io.indextables.spark.search.SplitSearchEngine.fromSplitFileWithMetadata(
        partition.schema,
        splitPath,
        splitMetadata,
        cacheConfig,
        options
      )

      // Get the internal searcher for aggregation operations
      val searcher = splitSearchEngine.getSplitSearcher()

      logger.debug(s"SIMPLE AGGREGATE EXECUTION: Searcher created successfully")

      // Get schema field names for filter validation
      val splitSchema = splitSearchEngine.getSchema()
      val splitFieldNames =
        try {
          import scala.jdk.CollectionConverters._
          val fieldNames = splitSchema.getFieldNames().asScala.toSet
          logger.info(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
          fieldNames
        } catch {
          case e: Exception =>
            logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
            Set.empty[String]
        }

      // Merge IndexQuery filters with pushed filters
      logger.debug(s"SIMPLE AGGREGATE EXECUTION: Merging ${partition.pushedFilters.length} pushed filters and ${partition.indexQueryFilters.length} IndexQuery filters")
      partition.pushedFilters.foreach(f => logger.debug(s"SIMPLE AGGREGATE EXECUTION: Pushed Filter: $f"))
      partition.indexQueryFilters.foreach(f => logger.debug(s"SIMPLE AGGREGATE EXECUTION: IndexQuery Filter: $f"))

      // Combine pushed filters and IndexQuery filters
      val allFilters = partition.pushedFilters ++ partition.indexQueryFilters

      val splitQuery = if (allFilters.nonEmpty) {
        // Create options map from config for field configuration
        import scala.jdk.CollectionConverters._
        val optionsFromConfig = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)

        val queryObj = if (splitFieldNames.nonEmpty) {
          val validatedQuery = FiltersToQueryConverter.convertToSplitQuery(
            allFilters,
            splitSearchEngine,
            Some(splitFieldNames),
            Some(optionsFromConfig)
          )
          logger.debug(s"SIMPLE AGGREGATE EXECUTION: Created SplitQuery with schema validation: ${validatedQuery.getClass.getSimpleName}")
          validatedQuery
        } else {
          val fallbackQuery = FiltersToQueryConverter.convertToSplitQuery(
            allFilters,
            splitSearchEngine,
            None,
            Some(optionsFromConfig)
          )
          logger.debug(s"SIMPLE AGGREGATE EXECUTION: Created SplitQuery without schema validation: ${fallbackQuery.getClass.getSimpleName}")
          fallbackQuery
        }
        queryObj
      } else {
        logger.debug(s"SIMPLE AGGREGATE EXECUTION: No filters, using match-all query")
        new SplitMatchAllQuery()
      }

      // Create all aggregations and execute them in a single batch
      import org.apache.spark.sql.connector.expressions.aggregate._

      // Step 1: Build all aggregations into a single HashMap
      // For COUNT and COUNT(*), use StatsAggregation to get filtered counts
      val aggregations = new java.util.HashMap[String, io.indextables.tantivy4java.split.SplitAggregation]()
      val aggNames = ArrayBuffer[String]()

      partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
        case (aggExpr, index) =>
          aggExpr match {
            case count: Count =>
              val fieldName = getFieldName(count.column)
              val aggName = s"count_agg_$index"
              // Use CountAggregation which works on all field types (not just numeric)
              logger.debug(s"SIMPLE AGGREGATE EXECUTION: Adding COUNT aggregation for field '$fieldName' as '$aggName'")
              aggregations.put(aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, fieldName))
              aggNames += aggName

            case _: CountStar =>
              // For COUNT(*), we need a fast field since CountAggregation requires fast fields
              // Strategy: 1) fields from other aggregations, 2) fast fields from docMapping, 3) auto-fast-field
              logger.debug(s"COUNT(*) FIELD SELECTION: Looking for fast field for COUNT(*) aggregation")

              // Try to find a field from other aggregations first (guaranteed to be fast)
              val fieldFromAgg = partition.aggregation.aggregateExpressions.collectFirst {
                case sum: Sum => Some(getFieldName(sum.column))
                case min: Min => Some(getFieldName(min.column))
                case max: Max => Some(getFieldName(max.column))
                case count: Count => Some(getFieldName(count.column))
              }.flatten

              // If no field from aggregations, read fast fields from docMapping metadata
              val selectedField = fieldFromAgg.getOrElse {
                val fastFieldsFromDocMapping = getFastFieldsFromDocMapping()

                if (fastFieldsFromDocMapping.nonEmpty) {
                  val field = fastFieldsFromDocMapping.head
                  logger.debug(s"COUNT(*) FIELD SELECTION: Using fast field from docMapping: '$field'")
                  field
                } else {
                  // No docMapping fast fields - fall back to auto-fast-field (first string or numeric field)
                  val autoFastField = partition.schema.fields.find { f =>
                    f.dataType == StringType || f.dataType == IntegerType || f.dataType == LongType ||
                    f.dataType == FloatType || f.dataType == DoubleType
                  }.map(_.name)

                  autoFastField.getOrElse {
                    throw new IllegalArgumentException(
                      s"COUNT(*) aggregation requires at least one fast field. " +
                      s"Please configure a fast field using spark.indextables.indexing.fastfields."
                    )
                  }
                }
              }

              logger.debug(s"COUNT(*) FIELD SELECTION: Selected field '$selectedField' for COUNT(*)")

              val aggName = s"count_star_agg_$index"
              aggregations.put(aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, selectedField))
              aggNames += aggName

            case sum: Sum =>
              val fieldName = getFieldName(sum.column)
              val aggName = s"sum_agg_$index"
              logger.info(s"SIMPLE AGGREGATE EXECUTION: Adding SUM aggregation for field '$fieldName' as '$aggName'")
              aggregations.put(aggName, new io.indextables.tantivy4java.aggregation.SumAggregation(fieldName))
              aggNames += aggName

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
              val aggName = s"min_agg_$index"
              logger.info(s"SIMPLE AGGREGATE EXECUTION: Adding MIN aggregation for field '$fieldName' as '$aggName'")
              aggregations.put(aggName, new io.indextables.tantivy4java.aggregation.MinAggregation(fieldName))
              aggNames += aggName

            case max: Max =>
              val fieldName = getFieldName(max.column)
              val aggName = s"max_agg_$index"
              logger.info(s"SIMPLE AGGREGATE EXECUTION: Adding MAX aggregation for field '$fieldName' as '$aggName'")
              aggregations.put(aggName, new io.indextables.tantivy4java.aggregation.MaxAggregation(fieldName))
              aggNames += aggName

            case other =>
              logger.warn(s"SIMPLE AGGREGATE EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
              aggNames += s"unsupported_$index"
          }
      }

      // Step 2: Execute all aggregations in a single batch
      logger.warn(s"SIMPLE AGGREGATE EXECUTION: Executing ${aggregations.size()} aggregations in a single batch")
      logger.warn(s"SIMPLE AGGREGATE EXECUTION: Split path: ${partition.split.path}")
      logger.warn(s"SIMPLE AGGREGATE EXECUTION: Aggregation names: ${aggNames.mkString(", ")}")
      logger.warn(s"SIMPLE AGGREGATE EXECUTION: Query: ${splitQuery.getClass.getSimpleName}")
      val result = searcher.aggregate(splitQuery, aggregations)
      logger.warn(s"SIMPLE AGGREGATE EXECUTION: Aggregate call completed, hasAggregations: ${result.hasAggregations()}")

      // Step 4: Extract results in the same order as expressions
      val aggregationResults = ArrayBuffer[Any]()

      logger.warn(s"SIMPLE AGGREGATE EXECUTION: About to extract ${partition.aggregation.aggregateExpressions.length} aggregation results")

      partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
        case (aggExpr, index) =>
          val aggName = aggNames(index)
          logger.warn(s"SIMPLE AGGREGATE EXECUTION: Extracting result $index: ${aggExpr.getClass.getSimpleName}")

          aggExpr match {
            case count: Count =>
              // Extract count from CountResult
              if (result.hasAggregations()) {
                val countResult = result.getAggregation(aggName).asInstanceOf[io.indextables.tantivy4java.aggregation.CountResult]
                val countValue = if (countResult != null) countResult.getCount() else 0L
                logger.warn(s"SIMPLE AGGREGATE EXECUTION: COUNT result: $countValue (split: ${partition.split.path})")
                aggregationResults += countValue
              } else {
                logger.warn(s"SIMPLE AGGREGATE EXECUTION: No COUNT aggregation result for '$aggName'")
                aggregationResults += 0L
              }

            case _: CountStar =>
              // Extract count from CountResult
              if (result.hasAggregations()) {
                val countResult = result.getAggregation(aggName).asInstanceOf[io.indextables.tantivy4java.aggregation.CountResult]
                val countValue = if (countResult != null) countResult.getCount() else 0L
                logger.warn(s"SIMPLE AGGREGATE EXECUTION: COUNT(*) result: $countValue (split: ${partition.split.path})")
                aggregationResults += countValue
              } else {
                logger.warn(s"SIMPLE AGGREGATE EXECUTION: No COUNT(*) aggregation result")
                aggregationResults += 0L
              }

            case sum: Sum =>
              val fieldName = getFieldName(sum.column)
              val fieldType = getFieldType(fieldName)
              if (result.hasAggregations()) {
                val sumResult = result.getAggregation(aggName).asInstanceOf[io.indextables.tantivy4java.aggregation.SumResult]
                val sumValue: Any = if (sumResult != null) {
                  // tantivy4java returns double - convert based on OUTPUT type (which widens integers to Long)
                  fieldType match {
                    case IntegerType | LongType =>
                      // SUM widens to LongType in schema, always return Long
                      val longVal: Long = Math.round(sumResult.getSum)
                      java.lang.Long.valueOf(longVal)
                    case FloatType | DoubleType => sumResult.getSum
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for SUM on '$fieldName': $fieldType, returning as Double")
                      sumResult.getSum
                  }
                } else {
                  // SUM widens integer types to Long in schema, so always return Long
                  fieldType match {
                    case IntegerType | LongType => java.lang.Long.valueOf(0L)
                    case FloatType | DoubleType => 0.0
                    case _                      => java.lang.Long.valueOf(0L)
                  }
                }
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: SUM result for '$aggName': $sumValue")
                aggregationResults += sumValue
              } else {
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: No SUM aggregation result for '$aggName'")
                aggregationResults += (fieldType match {
                  case IntegerType | LongType => java.lang.Long.valueOf(0L)
                  case FloatType | DoubleType => 0.0
                  case _                      => 0L
                })
              }

            case avg: Avg =>
              // AVG should be automatically transformed by Spark into SUM + COUNT when supportCompletePushDown=false
              // If we receive AVG directly, it indicates a configuration problem
              val fieldName = getFieldName(avg.column)
              throw new IllegalStateException(
                s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT. " +
                  s"This indicates supportCompletePushDown() may not be returning false correctly. " +
                  s"Check the SupportsPushDownAggregates implementation in IndexTables4SparkScanBuilder."
              )

            case min: Min =>
              val fieldName = getFieldName(min.column)
              val fieldType = getFieldType(fieldName)
              if (result.hasAggregations()) {
                val minResult = result.getAggregation(aggName).asInstanceOf[io.indextables.tantivy4java.aggregation.MinResult]
                val minValue: Any = if (minResult != null) {
                  // tantivy4java returns double - convert to appropriate type based on source field type
                  fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(minResult.getMin).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(minResult.getMin)
                      java.lang.Long.valueOf(longVal)
                    case FloatType =>
                      val floatVal: Float = minResult.getMin.toFloat
                      java.lang.Float.valueOf(floatVal)
                    case DoubleType =>
                      minResult.getMin
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for MIN on '$fieldName': $fieldType, returning as Double")
                      minResult.getMin
                  }
                } else {
                  fieldType match {
                    case IntegerType => java.lang.Integer.valueOf(0)
                    case LongType    => java.lang.Long.valueOf(0L)
                    case FloatType   => java.lang.Float.valueOf(0.0f)
                    case DoubleType  => 0.0
                    case _           => java.lang.Long.valueOf(0L)
                  }
                }
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: MIN result for '$aggName': $minValue")
                aggregationResults += minValue
              } else {
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: No MIN aggregation result for '$aggName'")
                aggregationResults += (fieldType match {
                  case IntegerType => java.lang.Integer.valueOf(0)
                  case LongType    => java.lang.Long.valueOf(0L)
                  case FloatType   => java.lang.Float.valueOf(0.0f)
                  case DoubleType  => 0.0
                  case _           => java.lang.Long.valueOf(0L)
                })
              }

            case max: Max =>
              val fieldName = getFieldName(max.column)
              val fieldType = getFieldType(fieldName)
              if (result.hasAggregations()) {
                val maxResult = result.getAggregation(aggName).asInstanceOf[io.indextables.tantivy4java.aggregation.MaxResult]
                val maxValue: Any = if (maxResult != null) {
                  // tantivy4java returns double - convert to appropriate type based on source field type
                  fieldType match {
                    case IntegerType =>
                      val intVal: Int = Math.round(maxResult.getMax).toInt
                      java.lang.Integer.valueOf(intVal)
                    case LongType =>
                      val longVal: Long = Math.round(maxResult.getMax)
                      java.lang.Long.valueOf(longVal)
                    case FloatType =>
                      val floatVal: Float = maxResult.getMax.toFloat
                      java.lang.Float.valueOf(floatVal)
                    case DoubleType =>
                      maxResult.getMax
                    case _ =>
                      logger.debug(s"AGGREGATION TYPE: Unexpected field type for MAX on '$fieldName': $fieldType, returning as Double")
                      maxResult.getMax
                  }
                } else {
                  fieldType match {
                    case IntegerType => java.lang.Integer.valueOf(0)
                    case LongType    => java.lang.Long.valueOf(0L)
                    case FloatType   => java.lang.Float.valueOf(0.0f)
                    case DoubleType  => 0.0
                    case _           => java.lang.Long.valueOf(0L)
                  }
                }
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: MAX result for '$aggName': $maxValue")
                aggregationResults += maxValue
              } else {
                logger.debug(s"SIMPLE AGGREGATE EXECUTION: No MAX aggregation result for '$aggName'")
                aggregationResults += (fieldType match {
                  case IntegerType => java.lang.Integer.valueOf(0)
                  case LongType    => java.lang.Long.valueOf(0L)
                  case FloatType   => java.lang.Float.valueOf(0.0f)
                  case DoubleType  => 0.0
                  case _           => java.lang.Long.valueOf(0L)
                })
              }

            case other =>
              logger.warn(s"SIMPLE AGGREGATE EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
              aggregationResults += 0L
          }
      }

      // Check if we have any matching documents by looking for a COUNT or COUNT(*) aggregation
      // For splits with no matches, we should not emit a row to avoid polluting MIN/MAX with zeros
      val hasMatchingDocs = {
        // Find the index of COUNT or COUNT(*) aggregation
        val countIndex = partition.aggregation.aggregateExpressions.zipWithIndex.collectFirst {
          case (_: Count, idx) => idx
          case (_: CountStar, idx) => idx
        }

        countIndex match {
          case Some(idx) if idx < aggregationResults.length =>
            // Check if the COUNT result is 0
            aggregationResults(idx) match {
              case count: java.lang.Long => count != 0L
              case count: Long => count != 0L
              case _ => true // If not a Long, assume we have data
            }
          case _ =>
            // No COUNT aggregation present, we can't determine if split is empty
            // Assume it has data to be safe
            true
        }
      }

      if (!hasMatchingDocs) {
        logger.debug(s"SIMPLE AGGREGATE EXECUTION: No matching documents in split ${partition.split.path}, skipping result row")
        Array.empty[InternalRow]
      } else {
        // Create a single row with all aggregation results
        val row = InternalRow.fromSeq(aggregationResults.toSeq)
        logger.debug(s"SIMPLE AGGREGATE EXECUTION: Generated result row with ${aggregationResults.length} values")
        logger.debug(s"SIMPLE AGGREGATE EXECUTION: Result values for split ${partition.split.path}: ${aggregationResults.mkString(", ")}")
        logger.debug(s"SIMPLE AGGREGATE EXECUTION: Returning 1 row from split ${partition.split.path}")
        Array(row)
      }

    } catch {
      case e: Exception =>
        logger.error(s"SIMPLE AGGREGATE EXECUTION: Failed to execute simple aggregation for split ${partition.split.path}", e)
        logger.error(s"SIMPLE AGGREGATE EXECUTION: Exception message: ${e.getMessage}")
        e.printStackTrace()
        Array.empty[InternalRow]
    }
  }

  /** Create SplitMetadata from the existing split information. */
  private def createSplitMetadataFromSplit(): QuickwitSplit.SplitMetadata =
    io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
      partition.split,
      partition.tablePath.toString
    )

  /** Get the Spark DataType for a field from the schema */
  private def getFieldType(fieldName: String): DataType =
    partition.schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.debug(s"AGGREGATION TYPE: Field '$fieldName' not found in schema, defaulting to LongType")
        LongType
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

  /** Read fast fields from docMappingJson metadata in the split */
  private def getFastFieldsFromDocMapping(): Set[String] = {
    partition.split.docMappingJson match {
      case Some(mappingJson) =>
        try {
          import com.fasterxml.jackson.databind.JsonNode
          import io.indextables.spark.util.JsonUtil
          import scala.jdk.CollectionConverters._

          val docMapping = JsonUtil.mapper.readTree(mappingJson)

          if (docMapping.isArray) {
            docMapping.asScala.flatMap { fieldNode =>
              val fieldName = Option(fieldNode.get("name")).map(_.asText())
              val isFast = Option(fieldNode.get("fast")).map(_.asBoolean()).getOrElse(false)

              if (isFast && fieldName.isDefined) Some(fieldName.get) else None
            }.toSet
          } else {
            Set.empty[String]
          }
        } catch {
          case e: Exception =>
            logger.debug(s"DOC MAPPING: Failed to parse docMappingJson: ${e.getMessage}")
            Set.empty[String]
        }

      case None =>
        logger.debug("DOC MAPPING: No docMappingJson in split")
        Set.empty[String]
    }
  }
}
