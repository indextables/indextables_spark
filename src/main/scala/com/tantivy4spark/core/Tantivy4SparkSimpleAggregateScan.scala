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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, Batch, InputPartition}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructType, StructField, LongType, DoubleType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.broadcast.Broadcast
import com.tantivy4spark.transaction.TransactionLog
import org.slf4j.LoggerFactory
import com.tantivy4java.{QuickwitSplit, SplitCacheManager}
import scala.jdk.CollectionConverters._

/**
 * Specialized scan for simple aggregations (no GROUP BY).
 * Handles queries like SELECT COUNT(*) FROM table, SELECT SUM(price) FROM table, etc.
 */
class Tantivy4SparkSimpleAggregateScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String],  // Direct config instead of broadcast
  aggregation: Aggregation
) extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkSimpleAggregateScan])

  println(s"🔍 SIMPLE AGGREGATE SCAN: Created with ${pushedFilters.length} filters")
  pushedFilters.foreach(f => println(s"🔍 SIMPLE AGGREGATE SCAN: Filter: $f"))

  override def readSchema(): StructType = {
    createSimpleAggregateSchema(aggregation)
  }

  override def toBatch: Batch = {
    println(s"🔍 SIMPLE AGGREGATE SCAN: toBatch() called, creating batch")
    new Tantivy4SparkSimpleAggregateBatch(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      config,
      aggregation
    )
  }

  override def description(): String = {
    val aggDesc = aggregation.aggregateExpressions.map(_.toString).mkString(", ")
    s"Tantivy4SparkSimpleAggregateScan[aggregations=[$aggDesc]]"
  }

  /**
   * Create schema for simple aggregation results.
   */
  private def createSimpleAggregateSchema(aggregation: Aggregation): StructType = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    logger.info(s"🔍 SIMPLE AGGREGATE SCHEMA: Creating schema for ${aggregation.aggregateExpressions.length} aggregations")

    val aggregationFields = aggregation.aggregateExpressions.zipWithIndex.map { case (aggExpr, index) =>
      val (columnName, dataType) = aggExpr match {
        case _: Count =>
          (s"count", LongType)
        case _: CountStar =>
          (s"count(*)", LongType)
        case sum: Sum =>
          // For Sum, infer data type from the column
          val columnDataType = getColumnDataType(sum.column)
          (s"sum", columnDataType)
        case _: Avg =>
          // For Avg, result is typically Double
          (s"avg", DoubleType)
        case min: Min =>
          // For Min, data type matches the column
          val columnDataType = getColumnDataType(min.column)
          (s"min", columnDataType)
        case max: Max =>
          // For Max, data type matches the column
          val columnDataType = getColumnDataType(max.column)
          (s"max", columnDataType)
        case other =>
          logger.warn(s"Unknown aggregation type: ${other.getClass.getSimpleName}")
          (s"agg_$index", LongType)
      }
      StructField(columnName, dataType, nullable = true)
    }

    val resultSchema = StructType(aggregationFields)
    logger.info(s"🔍 SIMPLE AGGREGATE SCHEMA: Created schema with ${resultSchema.fields.length} fields: ${resultSchema.fieldNames.mkString(", ")}")
    resultSchema
  }

  /**
   * Get the data type of a column from an expression.
   */
  private def getColumnDataType(column: org.apache.spark.sql.connector.expressions.Expression): org.apache.spark.sql.types.DataType = {
    import org.apache.spark.sql.types.LongType

    // Extract field name and look it up in schema
    val fieldName = extractFieldNameFromExpression(column)
    schema.fields.find(_.name == fieldName) match {
      case Some(field) => field.dataType
      case None =>
        logger.warn(s"Could not find field '$fieldName' in schema, defaulting to LongType")
        LongType
    }
  }

  /**
   * Extract field name from expression.
   */
  private def extractFieldNameFromExpression(expression: org.apache.spark.sql.connector.expressions.Expression): String = {
    // Use toString and try to extract field name
    val exprStr = expression.toString
    if (exprStr.startsWith("FieldReference(")) {
      val pattern = """FieldReference\(([^)]+)\)""".r
      pattern.findFirstMatchIn(exprStr) match {
        case Some(m) => m.group(1)
        case None =>
          logger.warn(s"Could not extract field name from expression: $expression")
          "unknown_field"
      }
    } else {
      logger.warn(s"Unsupported expression type for field extraction: $expression")
      "unknown_field"
    }
  }
}

/**
 * Batch implementation for simple aggregations.
 */
class Tantivy4SparkSimpleAggregateBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String],  // Direct config instead of broadcast
  aggregation: Aggregation
) extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkSimpleAggregateBatch])

  println(s"🔍 SIMPLE AGGREGATE BATCH: Created batch with ${pushedFilters.length} filters")

  override def planInputPartitions(): Array[InputPartition] = {
    logger.info(s"🔍 SIMPLE AGGREGATE BATCH: Planning input partitions for simple aggregation")

    // Get all splits from transaction log
    val allSplits = transactionLog.listFiles()
    logger.info(s"🔍 SIMPLE AGGREGATE BATCH: Found ${allSplits.length} total splits")

    // Apply data skipping using the same logic as regular scan by creating a helper scan instance
    // Use the full table schema to ensure proper field type detection for data skipping
    val helperScan = new Tantivy4SparkScan(
      sparkSession, transactionLog, schema, pushedFilters, options, None, config, Array.empty
    )
    val filteredSplits = helperScan.applyDataSkipping(allSplits, pushedFilters)
    logger.info(s"🔍 SIMPLE AGGREGATE BATCH: After data skipping: ${filteredSplits.length} splits")

    // Create one partition per filtered split for distributed aggregation processing
    filteredSplits.map { split =>
      new Tantivy4SparkSimpleAggregatePartition(
        split,
        schema,
        pushedFilters,
        config,
        aggregation,
        transactionLog.getTablePath()
      )
    }.toArray
  }

  override def createReaderFactory(): org.apache.spark.sql.connector.read.PartitionReaderFactory = {
    logger.info(s"🔍 SIMPLE AGGREGATE BATCH: Creating reader factory for simple aggregation")

    new Tantivy4SparkSimpleAggregateReaderFactory(
      sparkSession,
      pushedFilters,
      config,
      aggregation
    )
  }
}

/**
 * Input partition for simple aggregation processing.
 */
class Tantivy4SparkSimpleAggregatePartition(
  val split: com.tantivy4spark.transaction.AddAction,
  val schema: StructType,
  val pushedFilters: Array[Filter],
  val config: Map[String, String],  // Direct config instead of broadcast
  val aggregation: Aggregation,
  val tablePath: org.apache.hadoop.fs.Path
) extends InputPartition {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkSimpleAggregatePartition])

  logger.info(s"🔍 SIMPLE AGGREGATE PARTITION: Created partition for split: ${split.path}")
  logger.info(s"🔍 SIMPLE AGGREGATE PARTITION: Table path: ${tablePath}")
  logger.info(s"🔍 SIMPLE AGGREGATE PARTITION: Aggregations: ${aggregation.aggregateExpressions.map(_.toString).mkString(", ")}")

  /**
   * Provide preferred locations for this aggregate partition based on split cache locality.
   * Uses the same broadcast-based locality information as regular scan partitions.
   */
  override def preferredLocations(): Array[String] = {
    import com.tantivy4spark.storage.{BroadcastSplitLocalityManager, SplitLocationRegistry}

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

/**
 * Reader factory for simple aggregation partitions.
 */
class Tantivy4SparkSimpleAggregateReaderFactory(
  sparkSession: SparkSession,
  pushedFilters: Array[Filter],
  config: Map[String, String],  // Direct config instead of broadcast
  aggregation: Aggregation
) extends org.apache.spark.sql.connector.read.PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkSimpleAggregateReaderFactory])

  override def createReader(partition: org.apache.spark.sql.connector.read.InputPartition): org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] = {
    partition match {
      case simpleAggPartition: Tantivy4SparkSimpleAggregatePartition =>
        logger.info(s"🔍 SIMPLE AGGREGATE READER FACTORY: Creating reader for simple aggregate partition")

        new Tantivy4SparkSimpleAggregateReader(
          simpleAggPartition,
          sparkSession
        )
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
  }
}

/**
 * Reader for simple aggregation partitions that executes aggregations using tantivy4java.
 */
class Tantivy4SparkSimpleAggregateReader(
  partition: Tantivy4SparkSimpleAggregatePartition,
  sparkSession: SparkSession
) extends org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkSimpleAggregateReader])
  private var aggregateResults: Iterator[org.apache.spark.sql.catalyst.InternalRow] = _
  private var isInitialized = false

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

  override def get(): org.apache.spark.sql.catalyst.InternalRow = {
    aggregateResults.next()
  }

  override def close(): Unit = {
    logger.info(s"🔍 SIMPLE AGGREGATE READER: Closing simple aggregate reader")
  }

  /**
   * Initialize the simple aggregation by executing aggregation via tantivy4java.
   */
  private def initialize(): Unit = {
    logger.info(s"🔍 SIMPLE AGGREGATE READER: Initializing simple aggregation for split: ${partition.split.path}")

    try {
      // Execute simple aggregation using tantivy4java
      val results = executeSimpleAggregation()
      aggregateResults = results.iterator
      logger.info(s"🔍 SIMPLE AGGREGATE READER: Simple aggregation completed with ${results.length} result(s)")
    } catch {
      case e: Exception =>
        logger.error(s"🔍 SIMPLE AGGREGATE READER: Failed to execute simple aggregation", e)
        // Return empty results on failure
        aggregateResults = Iterator.empty
    }
  }

  /**
   * Execute simple aggregation using tantivy4java aggregations.
   */
  private def executeSimpleAggregation(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.unsafe.types.UTF8String
    import com.tantivy4java._
    import scala.collection.mutable.ArrayBuffer
    import scala.collection.JavaConverters._

    logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Starting simple aggregation")
    logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Split path: ${partition.split.path}")
    logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Aggregation expressions: ${partition.aggregation.aggregateExpressions.length}")

    try {
      // Create cache configuration from config
      val cacheConfig = com.tantivy4spark.util.ConfigUtils.createSplitCacheConfig(
        partition.config,
        Some(partition.tablePath.toString)
      )

      logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Creating searcher for split: ${partition.split.path}")
      logger.info(s"🔍 PATH DEBUG: partition.split.path = '${partition.split.path}'")
      logger.info(s"🔍 PATH DEBUG: partition.tablePath = '${partition.tablePath}'")
      logger.info(s"🔍 PATH DEBUG: startsWith('/') = ${partition.split.path.startsWith("/")}")
      logger.info(s"🔍 PATH DEBUG: contains('://') = ${partition.split.path.contains("://")}")

      // Resolve relative path from AddAction against table path using utility
      val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        partition.split.path,
        partition.tablePath.toString
      )

      logger.info(s"🔍 PATH DEBUG: resolvedPath = '${resolvedPath}'")

      // Convert s3a:// to s3:// for tantivy4java compatibility
      val splitPath = resolvedPath.replace("s3a://", "s3://")

      logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Resolved split path: ${splitPath}")
      logger.info(s"🔍 PATH DEBUG: final splitPath = '${splitPath}'")

      // Create split metadata from the split
      val splitMetadata = createSplitMetadataFromSplit()

      // Create SplitSearchEngine for filter conversion and schema access
      val splitSearchEngine = com.tantivy4spark.search.SplitSearchEngine.fromSplitFileWithMetadata(
        partition.schema, splitPath, splitMetadata, cacheConfig
      )

      // Get the internal searcher for aggregation operations
      val searcher = splitSearchEngine.getSplitSearcher()

      logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Searcher created successfully")

      // Get schema field names for filter validation
      val splitSchema = splitSearchEngine.getSchema()
      val splitFieldNames = try {
        import scala.jdk.CollectionConverters._
        val fieldNames = splitSchema.getFieldNames().asScala.toSet
        logger.info(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
        fieldNames
      } catch {
        case e: Exception =>
          logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
          Set.empty[String]
      }

      // Convert pushed filters to SplitQuery
      logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Converting ${partition.pushedFilters.length} pushed filters to query")
      partition.pushedFilters.foreach(f => logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Filter: $f"))

      val splitQuery = if (partition.pushedFilters.nonEmpty) {
        // Create options map from config for field configuration
        import scala.jdk.CollectionConverters._
        val optionsFromConfig = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)

        val queryObj = if (splitFieldNames.nonEmpty) {
          val validatedQuery = FiltersToQueryConverter.convertToSplitQuery(
            partition.pushedFilters, splitSearchEngine, Some(splitFieldNames), Some(optionsFromConfig)
          )
          logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Created SplitQuery with schema validation: ${validatedQuery.getClass.getSimpleName}")
          validatedQuery
        } else {
          val fallbackQuery = FiltersToQueryConverter.convertToSplitQuery(
            partition.pushedFilters, splitSearchEngine, None, Some(optionsFromConfig)
          )
          logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Created SplitQuery without schema validation: ${fallbackQuery.getClass.getSimpleName}")
          fallbackQuery
        }
        queryObj
      } else {
        logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: No filters, using match-all query")
        new SplitMatchAllQuery()
      }

      // Create individual aggregations for each expression
      import org.apache.spark.sql.connector.expressions.aggregate._
      val aggregationResults = ArrayBuffer[Any]()

      partition.aggregation.aggregateExpressions.zipWithIndex.foreach { case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            // For COUNT, execute query with filters applied
            logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Executing COUNT aggregation with filters")
            val result = searcher.search(splitQuery, Int.MaxValue)
            val count = result.getHits().size()
            logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: COUNT result: $count")
            aggregationResults += count.toLong

          case sum: Sum =>
            val fieldName = getFieldName(sum.column)
            logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Executing SUM aggregation for field '$fieldName' with filters")
            val sumAgg = new com.tantivy4java.SumAggregation(fieldName)
            val result = searcher.search(splitQuery, 0, s"sum_agg", sumAgg)

            if (result.hasAggregations()) {
              val sumResult = result.getAggregation("sum_agg").asInstanceOf[com.tantivy4java.SumResult]
              val sumValue = if (sumResult != null) sumResult.getSum.toLong else 0L
              logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: SUM result for '$fieldName': $sumValue")
              aggregationResults += sumValue
            } else {
              logger.warn(s"🔍 SIMPLE AGGREGATE EXECUTION: No SUM aggregation result for '$fieldName'")
              aggregationResults += 0L
            }

          case avg: Avg =>
            // AVG should be automatically transformed by Spark into SUM + COUNT when supportCompletePushDown=false
            // If we receive AVG directly, it indicates a configuration problem
            val fieldName = getFieldName(avg.column)
            throw new IllegalStateException(
              s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT. " +
              s"This indicates supportCompletePushDown() may not be returning false correctly. " +
              s"Check the SupportsPushDownAggregates implementation in Tantivy4SparkScanBuilder."
            )

          case min: Min =>
            val fieldName = getFieldName(min.column)
            logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Executing MIN aggregation for field '$fieldName' with filters")
            val minAgg = new com.tantivy4java.MinAggregation(fieldName)
            val result = searcher.search(splitQuery, 0, s"min_agg", minAgg)

            if (result.hasAggregations()) {
              val minResult = result.getAggregation("min_agg").asInstanceOf[com.tantivy4java.MinResult]
              val minValue = if (minResult != null) minResult.getMin.toLong else 0L
              logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: MIN result for '$fieldName': $minValue")
              aggregationResults += minValue
            } else {
              logger.warn(s"🔍 SIMPLE AGGREGATE EXECUTION: No MIN aggregation result for '$fieldName'")
              aggregationResults += 0L
            }

          case max: Max =>
            val fieldName = getFieldName(max.column)
            logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Executing MAX aggregation for field '$fieldName' with filters")
            val maxAgg = new com.tantivy4java.MaxAggregation(fieldName)
            val result = searcher.search(splitQuery, 0, s"max_agg", maxAgg)

            if (result.hasAggregations()) {
              val maxResult = result.getAggregation("max_agg").asInstanceOf[com.tantivy4java.MaxResult]
              val maxValue = if (maxResult != null) maxResult.getMax.toLong else 0L
              logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: MAX result for '$fieldName': $maxValue")
              aggregationResults += maxValue
            } else {
              logger.warn(s"🔍 SIMPLE AGGREGATE EXECUTION: No MAX aggregation result for '$fieldName'")
              aggregationResults += 0L
            }

          case other =>
            logger.warn(s"🔍 SIMPLE AGGREGATE EXECUTION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
            aggregationResults += 0L
        }
      }

      // Create a single row with all aggregation results
      val row = InternalRow.fromSeq(aggregationResults.toSeq)
      logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Generated result row with ${aggregationResults.length} values")
      Array(row)

    } catch {
      case e: Exception =>
        logger.error(s"🔍 SIMPLE AGGREGATE EXECUTION: Failed to execute simple aggregation", e)
        e.printStackTrace()
        Array.empty[InternalRow]
    }
  }

  /**
   * Create SplitMetadata from the existing split information.
   */
  private def createSplitMetadataFromSplit(): QuickwitSplit.SplitMetadata = {
    // Extract metadata from the split path or transaction log
    val splitId = partition.split.path.split("/").last.replace(".split", "")

    // Get the real metadata from the transaction log split entry
    val addAction = partition.split

    // Use the real footer ranges from the split if available
    val (footerStartOffset, footerEndOffset) = if (addAction.footerStartOffset.isDefined && addAction.footerEndOffset.isDefined) {
      (addAction.footerStartOffset.get, addAction.footerEndOffset.get)
    } else {
      // Fallback: try to read the split metadata from the file
      try {
        val splitMetadata = QuickwitSplit.readSplitMetadata(partition.split.path)
        if (splitMetadata != null && splitMetadata.hasFooterOffsets()) {
          (splitMetadata.getFooterStartOffset(), splitMetadata.getFooterEndOffset())
        } else {
          logger.warn(s"🔍 SIMPLE AGGREGATE EXECUTION: No footer offsets available for split: ${partition.split.path}")
          (0L, 1024L) // Minimal fallback
        }
      } catch {
        case e: Exception =>
          logger.error(s"🔍 SIMPLE AGGREGATE EXECUTION: Failed to read split metadata from: ${partition.split.path}", e)
          (0L, 1024L) // Minimal fallback
      }
    }

    logger.info(s"🔍 SIMPLE AGGREGATE EXECUTION: Using footer offsets: $footerStartOffset-$footerEndOffset for split: ${partition.split.path}")

    // Create metadata with real values from the transaction log
    new QuickwitSplit.SplitMetadata(
      splitId,                          // splitId
      "tantivy4spark-index",           // indexUid (default, AddAction doesn't have this field)
      0L,                              // partitionId (default, AddAction doesn't have this field)
      "tantivy4spark-source",          // sourceId (default, AddAction doesn't have this field)
      "tantivy4spark-node",            // nodeId (default, AddAction doesn't have this field)
      addAction.numRecords.getOrElse(1000L), // numDocs from transaction (using numRecords field)
      addAction.size,                  // uncompressedSizeBytes from transaction (using size field)
      null,                            // timeRangeStart (AddAction doesn't have this field)
      null,                            // timeRangeEnd (AddAction doesn't have this field)
      addAction.modificationTime / 1000, // createTimestamp (using modificationTime)
      "Mature",                        // maturity (default, AddAction doesn't have this field)
      addAction.tags.getOrElse(Map.empty[String, String]).keySet.asJava, // tags (convert from Map keys)
      footerStartOffset,               // footerStartOffset - REAL VALUE
      footerEndOffset,                 // footerEndOffset - REAL VALUE
      0L,                              // deleteOpstamp (default, AddAction doesn't have this field)
      0,                               // numMergeOps (default, AddAction doesn't have this field)
      "doc-mapping-uid",               // docMappingUid (default, AddAction doesn't have this field)
      addAction.docMappingJson.orNull, // docMappingJson - REAL VALUE from AddAction
      java.util.Collections.emptyList[String]() // skippedSplits
    )
  }

  /**
   * Extract field name from column expression for aggregations
   */
  private def getFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String = {
    // Check if it's a FieldReference by class name (like in ScanBuilder)
    if (column.getClass.getSimpleName == "FieldReference") {
      // For FieldReference, toString() returns the field name directly
      val fieldName = column.toString
      logger.info(s"🔍 FIELD EXTRACTION: Successfully extracted field name '$fieldName' from FieldReference")
      fieldName
    } else {
      // Fallback to the existing method
      val fieldName = extractFieldNameFromExpression(column)
      if (fieldName == "unknown_field") {
        throw new UnsupportedOperationException(s"Complex column expressions not supported for aggregation: $column")
      }
      fieldName
    }
  }

  /**
   * Extract field name from expression.
   */
  private def extractFieldNameFromExpression(expression: org.apache.spark.sql.connector.expressions.Expression): String = {
    // Use toString and try to extract field name
    val exprStr = expression.toString
    if (exprStr.startsWith("FieldReference(")) {
      val pattern = """FieldReference\(([^)]+)\)""".r
      pattern.findFirstMatchIn(exprStr) match {
        case Some(m) => m.group(1)
        case None =>
          logger.warn(s"Could not extract field name from expression: $expression")
          "unknown_field"
      }
    } else {
      logger.warn(s"Unsupported expression type for field extraction: $expression")
      "unknown_field"
    }
  }
}