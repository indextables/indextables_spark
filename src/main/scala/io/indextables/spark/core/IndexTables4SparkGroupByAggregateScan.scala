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

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.apache.spark.broadcast.Broadcast
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

import io.indextables.spark.transaction.TransactionLog
import io.indextables.tantivy4java.aggregation.TermsResult
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitCacheManager
import io.indextables.tantivy4java.split.SplitMatchAllQuery
import org.slf4j.LoggerFactory

/**
 * Specialized scan for GROUP BY aggregation operations. Implements distributed GROUP BY aggregation using tantivy's
 * terms aggregation capabilities.
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
  indexQueryFilters: Array[Any] = Array.empty)
    extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateScan])

  logger.debug(s"GROUP BY AGGREGATE SCAN: Created with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")
  pushedFilters.foreach(f => logger.debug(s"GROUP BY AGGREGATE SCAN: Filter: $f"))
  indexQueryFilters.foreach(f => logger.debug(s"GROUP BY AGGREGATE SCAN: IndexQuery Filter: $f"))

  override def readSchema(): StructType = {
    val resultSchema = createGroupBySchema(aggregation, groupByColumns)
    logger.debug(s"GROUP BY readSchema(): Returning schema with ${resultSchema.fields.length} fields: ${resultSchema.fieldNames.mkString(", ")}")
    resultSchema
  }

  override def toBatch: Batch = {
    // Update broadcast locality information before partition planning
    // This helps ensure preferred locations are accurate for GROUP BY operations
    try {
      val sparkContext = sparkSession.sparkContext
      logger.info("Updating broadcast locality before GROUP BY aggregate partition planning")
      io.indextables.spark.storage.BroadcastSplitLocalityManager.updateBroadcastLocality(sparkContext)
      logger.info("Broadcast locality update completed for GROUP BY aggregate scan")
    } catch {
      case ex: Exception =>
        logger.warn("Failed to update broadcast locality information for GROUP BY aggregate", ex)
    }

    new IndexTables4SparkGroupByAggregateBatch(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      config,
      aggregation,
      groupByColumns,
      indexQueryFilters
    )
  }

  override def description(): String = {
    val groupByDesc = groupByColumns.mkString(", ")
    val aggDesc     = aggregation.aggregateExpressions.map(_.toString).mkString(", ")
    s"IndexTables4SparkGroupByAggregateScan[groupBy=[$groupByDesc], aggregations=[$aggDesc]]"
  }

  /** Create schema for GROUP BY aggregation results. */
  private def createGroupBySchema(aggregation: Aggregation, groupByColumns: Array[String]): StructType = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.debug(s"GROUP BY SCHEMA: Creating schema for GROUP BY on columns: ${groupByColumns.mkString(", ")}")

    // Start with GROUP BY columns
    logger.info(
      s"GROUP BY SCHEMA: Looking up field types from schema with ${schema.fields.length} fields: ${schema.fields
          .map(f => s"${f.name}:${f.dataType}")
          .mkString(", ")}"
    )

    // IMPORTANT: Spark expects GROUP BY columns to be named group_col_0, group_col_1, etc.
    // See org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown lines 413-421
    val groupByFields = groupByColumns.zipWithIndex.map {
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

  /** Get the data type of a column from an expression. */
  private def getColumnDataType(column: org.apache.spark.sql.connector.expressions.Expression)
    : org.apache.spark.sql.types.DataType = {
    import org.apache.spark.sql.types.LongType

    // Extract field name using the same logic as getFieldName()
    val fieldName = if (column.getClass.getSimpleName == "FieldReference") {
      // For FieldReference, toString() returns the field name directly
      column.toString
    } else {
      // Fallback to ExpressionUtils
      io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    }

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
  indexQueryFilters: Array[Any] = Array.empty)
    extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateBatch])

  logger.debug(s"GROUP BY BATCH: Created batch with ${pushedFilters.length} filters and ${indexQueryFilters.length} IndexQuery filters")

  override def planInputPartitions(): Array[InputPartition] = {
    logger.debug(s"GROUP BY BATCH: Planning input partitions for GROUP BY aggregation")

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

    // Create one partition per filtered split for distributed GROUP BY processing
    filteredSplits.map { split =>
      new IndexTables4SparkGroupByAggregatePartition(
        split,
        pushedFilters,
        config,
        aggregation,
        groupByColumns,
        transactionLog.getTablePath(),
        schema,
        indexQueryFilters
      )
    }.toArray
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
      indexQueryFilters
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
  val indexQueryFilters: Array[Any] = Array.empty)
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
   * Provide preferred locations for this GROUP BY aggregate partition based on split cache locality. Uses the same
   * broadcast-based locality information as regular scan partitions.
   */
  override def preferredLocations(): Array[String] = {
    import io.indextables.spark.storage.{BroadcastSplitLocalityManager, SplitLocationRegistry}

    logger.info(s"🎯 [GROUP-BY-AGG] preferredLocations() called for split: ${split.path}")

    val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(split.path)
    if (preferredHosts.nonEmpty) {
      logger.info(s"🎯 [GROUP-BY-AGG] Using broadcast preferred hosts: ${preferredHosts.mkString(", ")}")
      preferredHosts
    } else {
      logger.info(s"🎯 [GROUP-BY-AGG] No broadcast hosts found, trying legacy registry")
      // Fallback to legacy registry for compatibility
      val legacyHosts = SplitLocationRegistry.getPreferredHosts(split.path)
      if (legacyHosts.nonEmpty) {
        logger.info(s"🎯 [GROUP-BY-AGG] Using legacy preferred hosts: ${legacyHosts.mkString(", ")}")
        legacyHosts
      } else {
        logger.info(s"🎯 [GROUP-BY-AGG] No preferred hosts found - letting Spark decide")
        // No cache history available, let Spark decide
        Array.empty[String]
      }
    }
  }
}

/** Reader factory for GROUP BY aggregation partitions. */
class IndexTables4SparkGroupByAggregateReaderFactory(
  sparkSession: SparkSession,
  pushedFilters: Array[Filter],
  config: Map[String, String], // Direct config instead of broadcast
  aggregation: Aggregation,
  groupByColumns: Array[String],
  schema: StructType,
  indexQueryFilters: Array[Any] = Array.empty)
    extends org.apache.spark.sql.connector.read.PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkGroupByAggregateReaderFactory])

  logger.debug(s"GROUP BY READER FACTORY: Created with ${indexQueryFilters.length} IndexQuery filters")

  override def createReader(partition: org.apache.spark.sql.connector.read.InputPartition)
    : org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] =
    partition match {
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

  // Helper function to get config from broadcast with defaults
  private def getConfig(configKey: String, default: String = ""): String = {
    val broadcasted = partition.config
    val value       = broadcasted.getOrElse(configKey, default)
    Option(value).getOrElse(default)
  }

  private def getConfigOption(configKey: String): Option[String] = {
    val broadcasted = partition.config
    // Try both the original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
    broadcasted.get(configKey).orElse(broadcasted.get(configKey.toLowerCase))
  }

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

  override def close(): Unit =
    logger.debug(s"GROUP BY READER: Closing GROUP BY reader")

  /** Initialize the GROUP BY aggregation by executing terms aggregation via tantivy4java. */
  private def initialize(): Unit = {
    logger.debug(s"GROUP BY READER: Initializing GROUP BY aggregation for split: ${partition.split.path}")

    try {
      // Execute GROUP BY aggregation using tantivy4java
      val results = executeGroupByAggregation()
      groupByResults = results.iterator
      logger.debug(s"GROUP BY READER: GROUP BY aggregation completed with ${results.length} groups")
    } catch {
      case e: Exception =>
        logger.warn(s"GROUP BY READER: Failed to execute GROUP BY aggregation", e)
        // Return empty results on failure
        groupByResults = Iterator.empty
    }
  }

  /** Execute GROUP BY aggregation using tantivy4java terms aggregation. */
  private def executeGroupByAggregation(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.unsafe.types.UTF8String
    import io.indextables.tantivy4java.split.{SplitMatchAllQuery, SplitAggregation}
    import io.indextables.tantivy4java.aggregation.{
      CountAggregation,
      SumAggregation,
      AverageAggregation,
      MinAggregation,
      MaxAggregation,
      TermsAggregation
    }
    import scala.collection.mutable.ArrayBuffer
    import scala.collection.JavaConverters._

    logger.debug(
      s"GROUP BY EXECUTION: Starting terms aggregation for GROUP BY columns: ${partition.groupByColumns.mkString(", ")}"
    )
    logger.debug(s"GROUP BY EXECUTION: Split path: ${partition.split.path}")
    logger.debug(s"GROUP BY EXECUTION: Aggregation expressions: ${partition.aggregation.aggregateExpressions.length}")

    try {
      // Create cache configuration from broadcast config
      val splitCacheConfig = createCacheConfig()
      val cacheManager     = SplitCacheManager.getInstance(splitCacheConfig.toJavaCacheConfig())

      logger.debug(s"GROUP BY EXECUTION: Creating searcher for split: ${partition.split.path}")

      // Resolve relative path from AddAction against table path using utility
      val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        partition.split.path,
        partition.tablePath.toString
      )

      // Normalize s3a:// to s3:// for tantivy4java compatibility
      val splitPath = resolvedPath.replace("s3a://", "s3://")

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

      // Get the searcher from the engine
      val searcher = cacheManager.createSplitSearcher(splitPath, splitMetadata)

      // Support both single and multi-column GROUP BY
      if (partition.groupByColumns.length >= 1) {
        val groupByColumns = partition.groupByColumns
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

        // Combine pushed filters and IndexQuery filters
        val allFilters = partition.pushedFilters ++ partition.indexQueryFilters

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
                  val groupByValues = fieldValues.zipWithIndex.map {
                    case (value, idx) =>
                      val fieldName = groupByColumns(idx)
                      val fieldType = getFieldType(fieldName)
                      convertStringValueToSpark(value, fieldType)
                  }
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
                  val groupByValue      = convertBucketKeyToSpark(bucket, groupByColumns(0))
                  val aggregationValues = calculateAggregationValuesFromSubAggregations(bucket, partition.aggregation)

                  val keyString = if (bucket.getKeyAsString != null) bucket.getKeyAsString else "null"
                  logger.debug(s"GROUP BY EXECUTION: Group '$keyString' has ${bucket.getDocCount} documents")

                  // Combine GROUP BY value with aggregation results
                  InternalRow.fromSeq(Seq(groupByValue) ++ aggregationValues)
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

    } catch {
      case e: Exception =>
        logger.warn(s"GROUP BY EXECUTION: Failed to execute GROUP BY aggregation", e)
        throw e
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
        // Convert date string to days since epoch (Int)
        // Handles both "YYYY-MM-DD" and ISO datetime formats like "2024-01-02T00:00:00Z"
        val epochDate = LocalDate.of(1970, 1, 1)
        try {
          val localDate = if (keyAsString.contains("T")) {
            // ISO datetime format - check if it has timezone indicator
            if (keyAsString.endsWith("Z") || keyAsString.contains("+") ||
                (keyAsString.length > 19 && keyAsString.charAt(19) == '-')) {
              // Instant format: "2024-01-02T00:00:00Z" or with offset
              java.time.Instant.parse(keyAsString).atZone(java.time.ZoneOffset.UTC).toLocalDate
            } else {
              // LocalDateTime format: "2024-01-02T00:00:00"
              java.time.LocalDateTime.parse(keyAsString).toLocalDate
            }
          } else {
            // Simple date format: "2024-01-02"
            LocalDate.parse(keyAsString)
          }
          ChronoUnit.DAYS.between(epochDate, localDate).toInt
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Cannot convert '$keyAsString' to DateType: ${e.getMessage}", e)
        }
      case Some(org.apache.spark.sql.types.TimestampType) =>
        // Convert timestamp to microseconds since epoch (Long)
        // Handles: numeric micros, ISO instant, LocalDateTime, URL-encoded strings
        try {
          // First check if it's already a numeric value (microseconds since epoch)
          if (keyAsString.forall(c => c.isDigit || c == '-')) {
            keyAsString.toLong
          } else {
            // URL-decode if needed (e.g., %3A -> :)
            val decoded = java.net.URLDecoder.decode(keyAsString, "UTF-8")
            if (decoded.endsWith("Z") || decoded.contains("+") ||
                (decoded.length > 19 && decoded.charAt(19) == '-')) {
              // ISO Instant format
              val instant = java.time.Instant.parse(decoded)
              instant.toEpochMilli * 1000L + (instant.getNano / 1000L) % 1000L
            } else if (decoded.contains("T")) {
              // LocalDateTime format
              val localDateTime = java.time.LocalDateTime.parse(decoded)
              io.indextables.spark.util.TimestampUtils.toMicros(localDateTime)
            } else {
              throw new IllegalArgumentException(s"Unrecognized timestamp format: $keyAsString")
            }
          }
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Cannot convert '$keyAsString' to TimestampType: ${e.getMessage}", e)
        }
      case _ =>
        // Default to string
        UTF8String.fromString(keyAsString)
    }
  }

  /** Calculate aggregation values from bucket data. Currently only supports COUNT aggregations with GROUP BY. */
  private def calculateAggregationValues(
    bucket: io.indextables.tantivy4java.aggregation.TermsResult.TermsBucket,
    aggregation: Aggregation
  ): Array[Any] = {
    // This method is now deprecated - metric aggregations are calculated separately
    // Keep for backward compatibility with COUNT-only queries
    import org.apache.spark.sql.connector.expressions.aggregate._

    if (bucket == null) {
      logger.warn(s"GROUP BY EXECUTION: Bucket is null in calculateAggregationValues")
      return Array(0L)
    }

    if (aggregation == null || aggregation.aggregateExpressions == null) {
      logger.warn(s"GROUP BY EXECUTION: Aggregation or aggregateExpressions is null")
      return Array(0L)
    }

    aggregation.aggregateExpressions.map { aggExpr =>
      if (aggExpr == null) {
        logger.warn(s"GROUP BY EXECUTION: Aggregate expression is null")
        0L
      } else {
        aggExpr match {
          case _: Count | _: CountStar =>
            // For COUNT/COUNT(*), use the document count from the bucket
            bucket.getDocCount.toLong

          case _: Sum | _: Avg | _: Min | _: Max =>
            // These should now be handled by separate metric aggregations
            logger.debug(
              s"GROUP BY EXECUTION: Metric aggregation ${aggExpr.getClass.getSimpleName} should be handled separately"
            )
            bucket.getDocCount.toLong // Fallback

          case other =>
            logger.debug(s"GROUP BY EXECUTION: Unknown aggregation type: ${other.getClass.getSimpleName}")
            0L
        }
      }
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
        // Convert date string to days since epoch (Int)
        // Handles both "YYYY-MM-DD" and ISO datetime formats like "2024-01-02T00:00:00Z"
        val epochDate = LocalDate.of(1970, 1, 1)
        try {
          val localDate = if (value.contains("T")) {
            // ISO datetime format - check if it has timezone indicator
            if (value.endsWith("Z") || value.contains("+") ||
                (value.length > 19 && value.charAt(19) == '-')) {
              // Instant format: "2024-01-02T00:00:00Z" or with offset
              java.time.Instant.parse(value).atZone(java.time.ZoneOffset.UTC).toLocalDate
            } else {
              // LocalDateTime format: "2024-01-02T00:00:00"
              java.time.LocalDateTime.parse(value).toLocalDate
            }
          } else {
            // Simple date format: "2024-01-02"
            LocalDate.parse(value)
          }
          ChronoUnit.DAYS.between(epochDate, localDate).toInt
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Cannot convert '$value' to DateType: ${e.getMessage}", e)
        }
      case TimestampType =>
        // Convert timestamp to microseconds since epoch (Long)
        // Handles: numeric micros, ISO instant, LocalDateTime, URL-encoded strings
        try {
          // First check if it's already a numeric value (microseconds since epoch)
          if (value.forall(c => c.isDigit || c == '-')) {
            value.toLong
          } else {
            // URL-decode if needed (e.g., %3A -> :)
            val decoded = java.net.URLDecoder.decode(value, "UTF-8")
            if (decoded.endsWith("Z") || decoded.contains("+") ||
                (decoded.length > 19 && decoded.charAt(19) == '-')) {
              // ISO Instant format
              val instant = java.time.Instant.parse(decoded)
              instant.toEpochMilli * 1000L + (instant.getNano / 1000L) % 1000L
            } else if (decoded.contains("T")) {
              // LocalDateTime format
              val localDateTime = java.time.LocalDateTime.parse(decoded)
              io.indextables.spark.util.TimestampUtils.toMicros(localDateTime)
            } else {
              throw new IllegalArgumentException(s"Unrecognized timestamp format: $value")
            }
          }
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Cannot convert '$value' to TimestampType: ${e.getMessage}", e)
        }
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
}
