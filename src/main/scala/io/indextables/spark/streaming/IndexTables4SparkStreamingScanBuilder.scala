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

package io.indextables.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.core.IndexTables4SparkScanBuilder
import io.indextables.spark.transaction.TransactionLog

import org.slf4j.LoggerFactory

/**
 * Streaming Scan Builder for IndexTables4Spark.
 *
 * This class builds streaming scans by delegating filter pushdown logic to the
 * existing batch scan builder, ensuring consistent filter handling between
 * batch and streaming modes.
 *
 * Key differences from batch scan builder:
 * - Does NOT implement SupportsPushDownLimit (streaming has no limit)
 * - Does NOT implement SupportsPushDownAggregates (streaming returns rows, not aggregates)
 * - build() returns IndexTables4SparkStreamingScan instead of batch scan
 *
 * @param sparkSession Active Spark session
 * @param transactionLog Transaction log interface
 * @param schema Full table schema
 * @param options Read options
 * @param config Configuration map
 */
class IndexTables4SparkStreamingScanBuilder(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    schema: StructType,
    options: CaseInsensitiveStringMap,
    config: Map[String, String]
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(getClass)

  // Delegate filter handling to existing batch scan builder
  // This ensures identical filter classification logic between batch and streaming
  private val batchScanBuilder = new IndexTables4SparkScanBuilder(
    sparkSession, transactionLog, schema, options, config
  )

  // Track required schema (column pruning)
  private var requiredSchema: StructType = schema

  // Track pushed filters locally for access
  private var _pushedFilters: Array[Filter] = Array.empty

  // ============================================================================
  // SupportsPushDownFilters - Delegate to batch scan builder
  // ============================================================================

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.debug(s"STREAMING SCAN BUILDER: pushFilters called with ${filters.length} filters")
    val unsupported = batchScanBuilder.pushFilters(filters)
    _pushedFilters = batchScanBuilder.pushedFilters()
    logger.debug(s"STREAMING SCAN BUILDER: ${_pushedFilters.length} filters pushed, ${unsupported.length} unsupported")
    unsupported
  }

  override def pushedFilters(): Array[Filter] = {
    // Return locally tracked filters if available, otherwise get from batch builder
    if (_pushedFilters.nonEmpty) _pushedFilters else batchScanBuilder.pushedFilters()
  }

  // ============================================================================
  // SupportsPushDownV2Filters - Delegate to batch scan builder
  // ============================================================================

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.debug(s"STREAMING SCAN BUILDER: pushPredicates called with ${predicates.length} predicates")
    batchScanBuilder.pushPredicates(predicates)
  }

  override def pushedPredicates(): Array[Predicate] = {
    batchScanBuilder.pushedPredicates()
  }

  // ============================================================================
  // SupportsPushDownRequiredColumns
  // ============================================================================

  override def pruneColumns(requiredSchema: StructType): Unit = {
    logger.debug(s"STREAMING SCAN BUILDER: pruneColumns called with ${requiredSchema.fields.length} columns")
    this.requiredSchema = requiredSchema
    batchScanBuilder.pruneColumns(requiredSchema)
  }

  // ============================================================================
  // Build - Returns Streaming Scan
  // ============================================================================

  override def build(): Scan = {
    logger.info(s"STREAMING SCAN BUILDER: Building streaming scan with ${pushedFilters().length} pushed filters")

    // Extract IndexQuery filters using the same mechanism as batch
    val indexQueryFilters = extractIndexQueryFilters()
    logger.debug(s"STREAMING SCAN BUILDER: Extracted ${indexQueryFilters.length} IndexQuery filters")

    new IndexTables4SparkStreamingScan(
      sparkSession = sparkSession,
      tablePath = transactionLog.getTablePath().toString,
      transactionLog = transactionLog,
      readSchema = requiredSchema,
      fullTableSchema = schema,
      partitionColumns = transactionLog.getPartitionColumns(),
      pushedFilters = pushedFilters(),
      indexQueryFilters = indexQueryFilters,
      config = config
    )
  }

  /**
   * Extract IndexQuery filters from the current logical plan.
   *
   * Uses the same ThreadLocal mechanism as batch scan builder to retrieve
   * IndexQuery expressions that have been transformed by V2IndexQueryExpressionRule.
   */
  private def extractIndexQueryFilters(): Array[Any] = {
    // Try to get IndexQuery filters from ThreadLocal storage
    // This is set by V2IndexQueryExpressionRule during logical plan optimization
    try {
      val relationOpt = IndexTables4SparkScanBuilder.getCurrentRelation()
      relationOpt.flatMap { relation =>
        val queries = IndexTables4SparkScanBuilder.getIndexQueries(relation)
        if (queries.nonEmpty) Some(queries.toArray) else None
      }.getOrElse(Array.empty[Any])
    } catch {
      case e: Exception =>
        logger.debug(s"Could not extract IndexQuery filters: ${e.getMessage}")
        Array.empty[Any]
    }
  }
}
