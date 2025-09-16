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
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.mutable
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
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScanBuilder])
  // Filters that have been pushed down and will be applied by the data source
  private var _pushedFilters = Array.empty[Filter]
  // Store IndexQuery filters separately since they don't extend Filter
  private var _pushedIndexQueryFilters = Array.empty[Any]
  private var requiredSchema = schema
  private var _limit: Option[Int] = None

  // Check for IndexQuery information stored in session state during initialization
  checkForIndexQueryInContext()

  override def build(): Scan = {
    new Tantivy4SparkScan(sparkSession, transactionLog, requiredSchema, _pushedFilters, options, _limit, broadcastConfig, _pushedIndexQueryFilters)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.error(s"🔍 PUSHFILTERS DEBUG: pushFilters called with ${filters.length} filters")
    filters.foreach(filter => logger.error(s"  - Input filter: $filter (${filter.getClass.getSimpleName})"))

    // Convert fake StringContains filters to IndexQueryFilter objects
    val convertedFilters = mutable.ArrayBuffer.empty[Any]
    val regularFilters = mutable.ArrayBuffer.empty[Filter]

    filters.foreach { filter =>
      convertTantivyFilter(filter) match {
        case Some(indexQueryFilter) =>
          logger.error(s"🔍 PUSHFILTERS DEBUG: Converted fake filter to IndexQueryFilter: $indexQueryFilter")
          convertedFilters += indexQueryFilter
        case None =>
          regularFilters += filter
      }
    }

    // Combine regular filters with converted IndexQuery filters
    val allFilters: Array[Any] = (regularFilters ++ convertedFilters).toArray

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

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.info(s"🔍 PUSHPREDICATES DEBUG: pushPredicates called with ${predicates.length} predicates")
    predicates.foreach(predicate => logger.info(s"  - Input predicate: $predicate (${predicate.getClass.getSimpleName})"))

    // Convert predicates that we can handle and extract IndexQuery information
    val (supported, unsupported) = predicates.partition(isSupportedPredicate)

    // Store supported predicates - for now, just log them
    logger.info(s"Predicate pushdown summary:")
    logger.info(s"  - ${supported.length} predicates FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(predicate => logger.info(s"    ✓ PUSHED: $predicate"))

    logger.info(s"  - ${unsupported.length} predicates NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(predicate => logger.info(s"    ✗ NOT PUSHED: $predicate"))

    // Return only unsupported predicates - Spark will re-evaluate these
    unsupported
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pushedPredicates(): Array[Predicate] = Array.empty  // V2 interface method

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
  }

  override def pushLimit(limit: Int): Boolean = {
    _limit = Some(limit)
    logger.info(s"Pushed limit: $limit")
    true // We support limit pushdown
  }

  private def convertTantivyFilter(filter: Filter): Option[Any] = {
    import org.apache.spark.sql.sources.StringContains
    import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    logger.error(s"🔍 convertTantivyFilter: Checking filter: $filter (class: ${filter.getClass.getSimpleName})")

    filter match {
      case StringContains(attribute, value) =>
        logger.error(s"🔍 convertTantivyFilter: StringContains detected - attribute: $attribute, value: $value")

        if (value.startsWith("__TANTIVY_INDEXQUERY__:")) {
          // Extract IndexQuery information from fake StringContains
          val parts = value.stripPrefix("__TANTIVY_INDEXQUERY__:").split(":", 2)
          if (parts.length == 2) {
            val columnName = parts(0)
            val queryString = parts(1)
            logger.error(s"🔍 convertTantivyFilter: SUCCESS - Detected IndexQuery fake filter - column: $columnName, query: $queryString")
            Some(IndexQueryFilter(columnName, queryString))
          } else {
            logger.warn(s"🔍 convertTantivyFilter: FAIL - Invalid IndexQuery fake filter format: $value")
            None
          }
        } else if (value.startsWith("__TANTIVY_INDEXALL__:")) {
          // Extract IndexQueryAll information from fake StringContains
          val queryString = value.stripPrefix("__TANTIVY_INDEXALL__:")
          logger.error(s"🔍 convertTantivyFilter: SUCCESS - Detected IndexQueryAll fake filter - query: $queryString")
          Some(IndexQueryAllFilter(queryString))
        } else {
          logger.error(s"🔍 convertTantivyFilter: Regular StringContains, not a fake filter")
          None
        }

      case _ =>
        // Regular filter, not a fake Tantivy filter
        logger.error(s"🔍 convertTantivyFilter: Not a StringContains filter, ignoring")
        None
    }
  }

  private def isSupportedFilter(filter: Any): Boolean = {
    import org.apache.spark.sql.sources._
    import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    filter match {
      case _: EqualTo => true
      case _: EqualNullSafe => true
      case _: GreaterThan => false  // Range queries require fast fields - defer to Spark
      case _: GreaterThanOrEqual => false  // Range queries require fast fields - defer to Spark
      case _: LessThan => false  // Range queries require fast fields - defer to Spark
      case _: LessThanOrEqual => false  // Range queries require fast fields - defer to Spark
      case _: In => true
      case _: IsNull => true
      case _: IsNotNull => true
      case _: And => true
      case _: Or => true
      case _: Not => true  // NOW SUPPORTED - NOT operators are fully handled
      case _: StringStartsWith => false  // Tantivy does best-effort, Spark applies final filtering
      case _: StringEndsWith => false   // Tantivy does best-effort, Spark applies final filtering
      case _: StringContains => true
      case _: IndexQueryFilter => true  // Add support for IndexQueryFilter
      case _: IndexQueryAllFilter => true  // Add support for IndexQueryAllFilter
      case _ => false
    }
  }

  private def isSupportedPredicate(predicate: Predicate): Boolean = {
    // For V2 predicates, we need to inspect the actual predicate type
    // For now, let's accept all predicates and see what we get
    logger.info(s"🔍 isSupportedPredicate: Checking predicate $predicate")

    // TODO: Implement proper predicate type checking based on Spark's V2 Predicate types
    true  // Accept all for now to see what comes through
  }

  private def checkForIndexQueryInContext(): Unit = {
    logger.error(s"🔍 SCANBUILDER DEBUG: checkForIndexQueryInContext called")

    // Check the IndexQueryRegistry for any stored IndexQuery information
    import com.tantivy4spark.filters.IndexQueryRegistry

    IndexQueryRegistry.getCurrentQueryId() match {
      case Some(queryId) =>
        logger.error(s"🔍 SCANBUILDER DEBUG: Found current query ID: $queryId")
        val storedIndexQueries = IndexQueryRegistry.getIndexQueriesForQuery(queryId)
        if (storedIndexQueries.nonEmpty) {
          _pushedIndexQueryFilters = storedIndexQueries.toArray
          logger.error(s"🔍 SCANBUILDER DEBUG: Retrieved ${storedIndexQueries.length} IndexQuery filters from registry")
          storedIndexQueries.foreach(filter => logger.error(s"  - Retrieved IndexQuery: $filter"))
        } else {
          logger.error(s"🔍 SCANBUILDER DEBUG: No IndexQuery filters found for query $queryId")
        }
      case None =>
        logger.error(s"🔍 SCANBUILDER DEBUG: No current query ID found in IndexQueryRegistry")
    }
  }
}
