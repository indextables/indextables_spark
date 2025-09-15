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
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownLimit}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.TransactionLog
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

class Tantivy4SparkScanBuilder(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    schema: StructType,
    options: CaseInsensitiveStringMap,
    broadcastConfig: Broadcast[Map[String, String]]
) extends ScanBuilder 
    with SupportsPushDownFilters 
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScanBuilder])
  // Filters that have been pushed down and will be applied by the data source
  private var _pushedFilters = Array.empty[Filter]
  // Store IndexQuery filters separately since they don't extend Filter
  private var _pushedIndexQueryFilters = Array.empty[Any]
  private var requiredSchema = schema
  private var _limit: Option[Int] = None

  override def build(): Scan = {
    new Tantivy4SparkScan(sparkSession, transactionLog, requiredSchema, _pushedFilters, options, _limit, broadcastConfig, _pushedIndexQueryFilters)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.info(s"🔍 PUSHFILTERS DEBUG: pushFilters called with ${filters.length} regular filters")
    filters.foreach(filter => logger.info(s"  - Input filter: $filter"))
    
    // Get IndexQuery filters that were extracted by the V2IndexQueryExpressionRule
    val extractedIndexQueryFilters = com.tantivy4spark.catalyst.V2IndexQueryExpressionRule.getExtractedFilters()
    
    logger.info(s"🔍 PUSHFILTERS DEBUG: Retrieved ${extractedIndexQueryFilters.length} IndexQuery filters from V2IndexQueryExpressionRule")
    if (extractedIndexQueryFilters.nonEmpty) {
      extractedIndexQueryFilters.foreach(filter => logger.info(s"  - Retrieved: $filter"))
    }
    
    // Combine regular filters with extracted IndexQuery filters
    val allFilters: Array[Any] = filters.asInstanceOf[Array[Any]] ++ extractedIndexQueryFilters
    
    val (supported, unsupported) = allFilters.partition(isSupportedFilter)
    
    // Store regular Spark filters and IndexQuery filters separately
    val supportedFilters = supported.collect { case f: Filter => f }
    val supportedIndexQueryFilters = supported.filterNot(_.isInstanceOf[Filter])
    
    // Store them in separate fields to avoid type casting issues
    _pushedFilters = supportedFilters
    _pushedIndexQueryFilters = supportedIndexQueryFilters
    
    
    logger.info(s"Filter pushdown summary:")
    logger.info(s"  - ${supported.length} filters FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(filter => logger.info(s"    ✓ PUSHED: $filter"))
    
    logger.info(s"  - ${unsupported.length} filters NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(filter => logger.info(s"    ✗ NOT PUSHED: $filter"))
    
    // Critical: Return ONLY unsupported filters (and filter out any that aren't actually Spark Filters)
    // This tells Spark that supported filters are FULLY HANDLED by the data source
    // and Spark should NOT re-apply them after reading data
    unsupported.collect { case f: Filter => f }
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
  }

  override def pushLimit(limit: Int): Boolean = {
    _limit = Some(limit)
    logger.info(s"Pushed limit: $limit")
    true // We support limit pushdown
  }

  private def isSupportedFilter(filter: Any): Boolean = {
    import org.apache.spark.sql.sources._
    import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}
    
    filter match {
      case _: EqualTo => true
      case _: EqualNullSafe => true
      case _: GreaterThan => true
      case _: GreaterThanOrEqual => true
      case _: LessThan => true
      case _: LessThanOrEqual => true
      case _: In => true
      case _: IsNull => true
      case _: IsNotNull => true
      case _: And => true
      case _: Or => true
      case _: Not => true
      case _: StringStartsWith => false  // Reject pushdown, let Tantivy handle first pass, Spark final filter
      case _: StringEndsWith => false   // Reject pushdown, let Tantivy handle first pass, Spark final filter
      case _: StringContains => true
      case _: IndexQueryFilter => true  // Add support for IndexQueryFilter
      case _: IndexQueryAllFilter => true  // Add support for IndexQueryAllFilter
      case _ => false
    }
  }
}
