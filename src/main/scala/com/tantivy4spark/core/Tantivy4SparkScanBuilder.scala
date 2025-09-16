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
  private var requiredSchema = schema
  private var _limit: Option[Int] = None

  // Generate instance key for this ScanBuilder to retrieve IndexQueries
  private val instanceKey = {
    val tablePath = transactionLog.getTablePath().toString
    val executionId = Option(sparkSession.sparkContext.getLocalProperty("spark.sql.execution.id"))
    Tantivy4SparkScanBuilder.generateInstanceKey(tablePath, executionId)
  }


  override def build(): Scan = {
    // DIRECT EXTRACTION: Extract IndexQuery expressions directly from the current logical plan
    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

    logger.error(s"🔍 BUILD DEBUG: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters directly from plan")
    extractedIndexQueryFilters.foreach(filter => logger.error(s"  - Extracted IndexQuery: $filter"))

    new Tantivy4SparkScan(sparkSession, transactionLog, requiredSchema, _pushedFilters, options, _limit, broadcastConfig, extractedIndexQueryFilters)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.error(s"🔍 PUSHFILTERS DEBUG: pushFilters called with ${filters.length} filters")
    filters.foreach(filter => logger.error(s"  - Input filter: $filter (${filter.getClass.getSimpleName})"))

    // Since IndexQuery expressions are now handled directly by the V2IndexQueryExpressionRule,
    // we only need to handle regular Spark filters here.
    val (supported, unsupported) = filters.partition(isSupportedFilter)

    // Store supported filters
    _pushedFilters = supported

    logger.info(s"Filter pushdown summary:")
    logger.info(s"  - ${supported.length} filters FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(filter => logger.info(s"    ✓ PUSHED: $filter"))

    logger.info(s"  - ${unsupported.length} filters NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(filter => logger.info(s"    ✗ NOT PUSHED: $filter"))

    // Return only unsupported filters - Spark will re-evaluate these after reading data
    unsupported
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


  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    filter match {
      case EqualTo(attribute, _) => isFieldSuitableForExactMatching(attribute)
      case EqualNullSafe(attribute, _) => isFieldSuitableForExactMatching(attribute)
      case _: GreaterThan => false  // Range queries require fast fields - defer to Spark
      case _: GreaterThanOrEqual => false  // Range queries require fast fields - defer to Spark
      case _: LessThan => false  // Range queries require fast fields - defer to Spark
      case _: LessThanOrEqual => false  // Range queries require fast fields - defer to Spark
      case _: In => true
      case _: IsNull => true
      case _: IsNotNull => true
      case And(left, right) => isSupportedFilter(left) && isSupportedFilter(right)
      case Or(left, right) => isSupportedFilter(left) && isSupportedFilter(right)
      case Not(child) => isSupportedFilter(child)  // NOT is supported only if child is supported
      case _: StringStartsWith => false  // Tantivy does best-effort, Spark applies final filtering
      case _: StringEndsWith => false   // Tantivy does best-effort, Spark applies final filtering
      case _: StringContains => true
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

  /**
   * Check if a field is suitable for exact matching at the data source level.
   * String fields (raw tokenizer) support exact matching.
   * Text fields (default tokenizer) should be filtered by Spark for exact matches.
   */
  private def isFieldSuitableForExactMatching(attribute: String): Boolean = {
    // Check the field type configuration from broadcast options
    val broadcastConfigMap = broadcastConfig.value
    val fieldTypeKey = s"spark.tantivy4spark.indexing.typemap.$attribute"
    val fieldType = broadcastConfigMap.get(fieldTypeKey)

    fieldType match {
      case Some("string") =>
        logger.info(s"🔍 Field '$attribute' configured as 'string' - supporting exact matching")
        true
      case Some("text") =>
        logger.info(s"🔍 Field '$attribute' configured as 'text' - deferring exact matching to Spark")
        false
      case Some(other) =>
        logger.info(s"🔍 Field '$attribute' configured as '$other' - supporting exact matching")
        true
      case None =>
        // No explicit configuration - assume string type (new default)
        logger.info(s"🔍 Field '$attribute' has no type configuration - assuming 'string', supporting exact matching")
        true
    }
  }


  /**
   * Extract IndexQuery expressions directly using the companion object storage.
   * This eliminates the need for global registry by using instance-scoped storage.
   */
  private def extractIndexQueriesFromCurrentPlan(): Array[Any] = {
    logger.error(s"🔍 EXTRACT DEBUG: Starting direct IndexQuery extraction using instance key: $instanceKey")

    // Method 1: Get IndexQueries stored by V2IndexQueryExpressionRule for this instance
    val storedQueries = Tantivy4SparkScanBuilder.getIndexQueries(instanceKey)
    if (storedQueries.nonEmpty) {
      logger.error(s"🔍 EXTRACT DEBUG: Found ${storedQueries.length} IndexQuery filters from instance storage")
      storedQueries.foreach(q => logger.error(s"  - Instance IndexQuery: $q"))
      return storedQueries.toArray
    }

    // Method 2: Fall back to registry (temporary until we fully eliminate it)
    import com.tantivy4spark.filters.IndexQueryRegistry
    IndexQueryRegistry.getCurrentQueryId() match {
      case Some(queryId) =>
        val registryQueries = IndexQueryRegistry.getIndexQueriesForQuery(queryId)
        if (registryQueries.nonEmpty) {
          logger.error(s"🔍 EXTRACT DEBUG: Found ${registryQueries.length} IndexQuery filters from registry as fallback")
          registryQueries.foreach(q => logger.error(s"  - Registry IndexQuery: $q"))
          return registryQueries.toArray
        }
      case None =>
        logger.error(s"🔍 EXTRACT DEBUG: No query ID available in registry")
    }

    logger.error(s"🔍 EXTRACT DEBUG: No IndexQuery filters found using any method")
    Array.empty[Any]
  }
}

/**
 * Companion object for ScanBuilder to store IndexQuery information.
 * This provides a clean mechanism for V2IndexQueryExpressionRule to pass
 * IndexQuery expressions directly to the ScanBuilder without a global registry.
 */
object Tantivy4SparkScanBuilder {
  import scala.collection.concurrent

  // Thread-safe storage for IndexQuery expressions scoped by DataSource instance
  private val instanceIndexQueries = concurrent.TrieMap[String, scala.collection.mutable.Buffer[Any]]()

  /**
   * Store IndexQuery expressions for a specific DataSource instance.
   * The key should be unique per query execution to avoid conflicts.
   */
  def storeIndexQueries(instanceKey: String, indexQueries: Seq[Any]): Unit = {
    val buffer = instanceIndexQueries.getOrElseUpdate(instanceKey, scala.collection.mutable.Buffer.empty)
    buffer.clear()
    buffer ++= indexQueries
    println(s"🔍 ScanBuilder: Stored ${indexQueries.length} IndexQuery expressions for instance $instanceKey")
  }

  /**
   * Retrieve IndexQuery expressions for a specific DataSource instance.
   */
  def getIndexQueries(instanceKey: String): Seq[Any] = {
    val queries = instanceIndexQueries.getOrElse(instanceKey, scala.collection.mutable.Buffer.empty).toSeq
    println(s"🔍 ScanBuilder: Retrieved ${queries.length} IndexQuery expressions for instance $instanceKey")
    queries
  }

  /**
   * Clear IndexQuery expressions for a specific DataSource instance.
   */
  def clearIndexQueries(instanceKey: String): Unit = {
    instanceIndexQueries.remove(instanceKey)
    println(s"🔍 ScanBuilder: Cleared IndexQuery expressions for instance $instanceKey")
  }

  /**
   * Generate a unique instance key for a DataSource relation.
   */
  def generateInstanceKey(tablePath: String, executionId: Option[String]): String = {
    // Use a more deterministic key based only on table path to avoid timing issues
    val cleanPath = tablePath.replace('/', '_').replace('\\', '_')
    cleanPath // Remove execution ID dependency for now
  }
}
