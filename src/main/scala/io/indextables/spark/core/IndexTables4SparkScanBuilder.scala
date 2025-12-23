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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownAggregates,
  SupportsPushDownFilters,
  SupportsPushDownLimit,
  SupportsPushDownRequiredColumns,
  SupportsPushDownV2Filters
}
import org.apache.spark.sql.sources.{Filter, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

class IndexTables4SparkScanBuilder(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  config: Map[String, String] // Direct config instead of broadcast
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit
    with SupportsPushDownAggregates {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkScanBuilder])

  logger.debug(s"SCAN BUILDER CREATED: NEW ScanBuilder instance ${System.identityHashCode(this)}")
  // Filters that have been pushed down and will be applied by the data source
  private var _pushedFilters      = Array.empty[Filter]
  private var _unsupportedFilters = Array.empty[Filter] // Track filters Spark will re-evaluate
  private var requiredSchema      = schema
  private var _limit: Option[Int] = None

  // Aggregate pushdown state
  private var _pushedAggregation: Option[Aggregation] = None
  private var _pushedGroupBy: Option[Array[String]]   = None

  // IMPORTANT: Do NOT capture relation at construction time!
  // ScanBuilders may be created before V2IndexQueryExpressionRule runs.
  // Instead, look up the relation from ThreadLocal at usage time (in build() and pushFilters()).
  // ThreadLocal is set by V2IndexQueryExpressionRule during optimization.

  override def build(): Scan = {
    val aggregation = _pushedAggregation

    // Look up relation from ThreadLocal at usage time (not at construction time!)
    val relationForIndexQuery = IndexTables4SparkScanBuilder.getCurrentRelation()

    logger.debug(s"BUILD: ScanBuilder.build() called on instance ${System.identityHashCode(this)}")
    logger.debug(s"BUILD: Aggregation present: ${aggregation.isDefined}, filters: ${_pushedFilters.length}")

    // CRITICAL: Handle multiple optimization passes
    // Spark runs V2ScanRelationPushDown multiple times, creating fresh ScanBuilders each time
    // Only builders with Filter nodes get pushFilters() called
    // The FINAL builder (which might not have pushFilters called) is the one executed
    // Solution: If instance filters are empty, try retrieving from relation object storage
    val effectiveFilters = if (_pushedFilters.nonEmpty) {
      logger.debug(s"BUILD: Using instance variable filters: ${_pushedFilters.length}")
      _pushedFilters
    } else {
      // Instance filters empty - try relation object storage
      relationForIndexQuery match {
        case Some(relation) =>
          val relationFilters = IndexTables4SparkScanBuilder.getPushedFilters(relation)
          logger.debug(s"BUILD: Instance filters empty, retrieved ${relationFilters.length} from relation object: ${System.identityHashCode(relation)}")
          relationFilters
        case None =>
          logger.warn(s"BUILD: Instance filters empty and no relation in ThreadLocal, cannot retrieve stored filters")
          Array.empty[Filter]
      }
    }

    logger.debug(s"BUILD: Effective filters count: ${effectiveFilters.length}")
    effectiveFilters.foreach(filter => logger.debug(s"BUILD:   - Effective filter: $filter"))

    // Create the scan
    val scan = aggregation match {
      case Some(agg) =>
        logger.debug(s"BUILD: Creating aggregate scan for pushed aggregation")
        createAggregateScan(agg, effectiveFilters)
      case None =>
        // Regular scan
        logger.debug(s"BUILD: Creating regular scan (no aggregation pushdown)")

        // CRITICAL: Check if unsupported filters blocked aggregate pushdown
        // If there are unsupported filters, Spark won't even call pushAggregation(),
        // which means any aggregation in the query will produce incorrect results.
        // We check if the query contains an aggregation and fail if so.
        //
        // NOTE: Just like effectiveFilters, we must also retrieve unsupported filters from
        // the relation object storage because build() may be called on a different ScanBuilder
        // instance than the one where pushFilters() was called.
        val effectiveUnsupportedFilters = if (_unsupportedFilters.nonEmpty) {
          logger.debug(s"BUILD: Using instance variable unsupported filters: ${_unsupportedFilters.length}")
          _unsupportedFilters
        } else {
          relationForIndexQuery match {
            case Some(relation) =>
              val filters = IndexTables4SparkScanBuilder.getUnsupportedFilters(relation)
              logger.debug(s"BUILD: Retrieved ${filters.length} unsupported filters from relation object: ${System.identityHashCode(relation)}")
              filters
            case None =>
              logger.debug(s"BUILD: No relation in ThreadLocal, cannot retrieve unsupported filters")
              Array.empty[Filter]
          }
        }

        // Check for unsupported filters that would block aggregate pushdown.
        // When Spark has unsupported filters, it won't call pushAggregation(), which means
        // any aggregation in the query will produce incorrect results (due to our default limit).
        val hasAggregateInPlan = detectAggregateInQueryPlan()
        if (effectiveUnsupportedFilters.nonEmpty && hasAggregateInPlan) {
          val unsupportedDesc = effectiveUnsupportedFilters.map(_.toString).mkString(", ")
          // Build specific guidance for string pattern filters
          val hasStringStartsWith    = effectiveUnsupportedFilters.exists(_.isInstanceOf[StringStartsWith])
          val hasStringEndsWith      = effectiveUnsupportedFilters.exists(_.isInstanceOf[StringEndsWith])
          val hasStringContains      = effectiveUnsupportedFilters.exists(_.isInstanceOf[StringContains])
          val hasStringPatternFilter = hasStringStartsWith || hasStringEndsWith || hasStringContains

          val stringPatternHint = if (hasStringPatternFilter) {
            val patternTypes = Seq(
              if (hasStringStartsWith) "stringStartsWith" else "",
              if (hasStringEndsWith) "stringEndsWith" else "",
              if (hasStringContains) "stringContains" else ""
            ).filter(_.nonEmpty)

            s" To enable string pattern pushdown, set " +
              s"spark.indextables.filter.stringPattern.pushdown=true (enables all patterns) " +
              s"or individually: ${patternTypes.map(t => s"spark.indextables.filter.$t.pushdown=true").mkString(", ")}."
          } else ""

          throw new IllegalStateException(
            s"Aggregate pushdown blocked by unsupported filter(s): [$unsupportedDesc]. " +
              s"IndexTables4Spark requires aggregate pushdown for correct COUNT/SUM/AVG/MIN/MAX results. " +
              s"The filter type(s) used are not fully supported, which prevents aggregate optimization. " +
              s"Supported filter types: EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, In, IsNotNull, And, Or, Not. " +
              s"Unsupported: IsNull, JSON field null checks." +
              stringPatternHint
          )
        }

        // DIRECT EXTRACTION: Extract IndexQuery expressions directly from the current logical plan
        val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

        logger.debug(
          s"BUILD DEBUG: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters directly from plan"
        )
        extractedIndexQueryFilters.foreach(filter => logger.debug(s"  - Extracted IndexQuery: $filter"))

        logger.debug(s"BUILD: Creating IndexTables4SparkScan on instance ${System.identityHashCode(this)} with ${effectiveFilters.length} pushed filters")
        effectiveFilters.foreach(filter => logger.debug(s"BUILD:   - Creating scan with filter: $filter"))
        new IndexTables4SparkScan(
          sparkSession,
          transactionLog,
          requiredSchema,
          effectiveFilters,
          options,
          _limit,
          config,
          extractedIndexQueryFilters
        )
    }

    // CRITICAL: Clear stored filters AND IndexQueries after scan is created to prevent filter pollution across queries
    // The relation-based storage is meant to share filters across optimization passes of the SAME query,
    // not to carry filters from one query to the next query on the same DataFrame
    relationForIndexQuery.foreach { relation =>
      IndexTables4SparkScanBuilder.clearPushedFilters(relation)
      IndexTables4SparkScanBuilder.clearIndexQueries(relation)
      logger.debug(s"BUILD: Cleared stored filters and IndexQueries for relation: ${System.identityHashCode(relation)}")
    }

    scan
  }

  /** Create an aggregate scan for pushed aggregations. */
  private def createAggregateScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan =
    _pushedGroupBy match {
      case Some(groupByColumns) =>
        // GROUP BY aggregation
        logger.info(
          s"AGGREGATE SCAN: Creating GROUP BY aggregation scan for columns: ${groupByColumns.mkString(", ")}"
        )
        createGroupByAggregateScan(aggregation, groupByColumns, effectiveFilters)
      case None =>
        // Simple aggregation without GROUP BY
        // Check if we can use transaction log count optimization
        if (canUseTransactionLogCount(aggregation, effectiveFilters)) {
          logger.debug(s"AGGREGATE SCAN: Using transaction log count optimization")
          logger.debug(s"AGGREGATE SCAN: Using transaction log count optimization")
          createTransactionLogCountScan(aggregation, effectiveFilters)
        } else {
          logger.debug(s"AGGREGATE SCAN: Creating simple aggregation scan")
          logger.debug(s"AGGREGATE SCAN: Creating simple aggregation scan")
          createSimpleAggregateScan(aggregation, effectiveFilters)
        }
    }

  /** Create a GROUP BY aggregation scan. */
  private def createGroupByAggregateScan(
    aggregation: Aggregation,
    groupByColumns: Array[String],
    effectiveFilters: Array[Filter]
  ): Scan = {
    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

    // Check if we can use transaction log optimization for partition-only GROUP BY COUNT
    if (canUseTransactionLogGroupByCount(aggregation, groupByColumns, effectiveFilters)) {
      val hasAggregations = aggregation.aggregateExpressions().nonEmpty
      logger.info(
        s"Using transaction log optimization for partition-only GROUP BY COUNT, hasAggregations=$hasAggregations"
      )
      new TransactionLogCountScan(
        sparkSession,
        transactionLog,
        effectiveFilters,
        options,
        config,
        Some(groupByColumns), // Pass GROUP BY columns for grouped aggregation
        hasAggregations,      // Indicate if this has aggregations or is just DISTINCT
        Some(schema)          // Pass table schema for proper type conversion
      )
    } else {
      // Regular GROUP BY scan using tantivy aggregations
      new IndexTables4SparkGroupByAggregateScan(
        sparkSession,
        transactionLog,
        schema,
        effectiveFilters,
        options,
        config,
        aggregation,
        groupByColumns,
        extractedIndexQueryFilters
      )
    }
  }

  /** Create a simple aggregation scan (no GROUP BY). */
  private def createSimpleAggregateScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan = {

    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()
    new IndexTables4SparkSimpleAggregateScan(
      sparkSession,
      transactionLog,
      schema,
      effectiveFilters,
      options,
      config,
      aggregation,
      extractedIndexQueryFilters
    )
  }

  /** Check if we can optimize COUNT queries using transaction log. */
  private def canUseTransactionLogCount(aggregation: Aggregation, effectiveFilters: Array[Filter]): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar}

    aggregation.aggregateExpressions.foreach(expr =>
      logger.debug(s"SCAN BUILDER: Aggregate expression: $expr (${expr.getClass.getSimpleName})")
    )
    logger.debug(s"SCAN BUILDER: Number of effective filters: ${effectiveFilters.length}")
    effectiveFilters.foreach(filter => logger.debug(s"SCAN BUILDER: Effective filter: $filter"))

    // Extract IndexQuery filters to check if we have any
    val indexQueryFilters = extractIndexQueriesFromCurrentPlan()
    logger.debug(s"SCAN BUILDER: Number of IndexQuery filters: ${indexQueryFilters.length}")

    // Transaction log optimization requires:
    // 1. Exactly ONE aggregate expression (no mixing COUNT with MAX, AVG, SUM, MIN)
    // 2. That expression must be COUNT or COUNT(*)
    // 3. Filters must be only on partition columns (or no filters)
    //
    // IMPORTANT: If there are multiple aggregates (e.g., SELECT COUNT(*), MAX(field)),
    // we CANNOT use transaction log for COUNT because:
    // - MAX/AVG/MIN/SUM require searching tantivy
    // - COUNT must count the SAME documents that MAX/AVG/MIN/SUM operate on
    // - Therefore, ALL aggregates must go to tantivy together
    val result = aggregation.aggregateExpressions.length == 1 && {
      aggregation.aggregateExpressions.head match {
        case _: Count =>
          // Transaction log optimization works when filters are only on partition columns
          // Range filters (>=, <=, <, >) on partition columns are valid because partition pruning
          // ensures all documents in a split have the same partition value
          val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty
          logger.debug(s"SCAN BUILDER: COUNT - Can use transaction log optimization: $hasOnlyPartitionFilters (filters: ${effectiveFilters.mkString(", ")})")
          hasOnlyPartitionFilters
        case _: CountStar =>
          // Transaction log optimization works when filters are only on partition columns
          // Range filters (>=, <=, <, >) on partition columns are valid because partition pruning
          // ensures all documents in a split have the same partition value
          val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty
          logger.debug(s"SCAN BUILDER: COUNT(*) - Can use transaction log optimization: $hasOnlyPartitionFilters (filters: ${effectiveFilters.mkString(", ")})")
          hasOnlyPartitionFilters
        case _ =>
          logger.debug(s"SCAN BUILDER: Not a COUNT aggregation, cannot use transaction log")
          false
      }
    }

    if (!result && aggregation.aggregateExpressions.length > 1) {
      logger.debug(s"SCAN BUILDER: Multiple aggregates present (${aggregation.aggregateExpressions.length}) - routing all aggregates to tantivy including COUNT")
    }

    logger.debug(s"SCAN BUILDER: canUseTransactionLogCount returning: $result")
    result
  }

  /** Check if we can optimize GROUP BY partition columns COUNT using transaction log. */
  private def canUseTransactionLogGroupByCount(
    aggregation: Aggregation,
    groupByColumns: Array[String],
    effectiveFilters: Array[Filter]
  ): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar}

    // Check 1: All GROUP BY columns must be partition columns
    val partitionColumns               = transactionLog.getPartitionColumns()
    val allGroupByColumnsArePartitions = groupByColumns.forall(partitionColumns.contains)

    if (!allGroupByColumnsArePartitions) {
      return false
    }

    // Check 2: Only COUNT aggregations are supported
    // IMPORTANT: If there are ANY non-COUNT aggregates (MAX, AVG, MIN, SUM),
    // we CANNOT use transaction log because:
    // - MAX/AVG/MIN/SUM require searching tantivy
    // - COUNT must count the SAME documents that MAX/AVG/MIN/SUM operate on
    // - Therefore, ALL aggregates must go to tantivy together
    val onlyCountAggregations = aggregation.aggregateExpressions.forall {
      case _: Count | _: CountStar => true
      case _                       => false
    }

    if (!onlyCountAggregations) {
      logger.debug(s"SCAN BUILDER: GROUP BY has non-COUNT aggregates - routing all aggregates to tantivy")
      return false
    }

    // Check 3: Only partition filters are allowed (or no filters), AND no IndexQuery filters
    val indexQueryFilters       = extractIndexQueriesFromCurrentPlan()
    val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty

    if (!hasOnlyPartitionFilters) {
      return false
    }

    true
  }

  /** Create a specialized scan that returns count from transaction log. */
  private def createTransactionLogCountScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan =
    new TransactionLogCountScan(
      sparkSession,
      transactionLog,
      effectiveFilters,
      options,
      config,
      None,        // No GROUP BY columns for simple count
      true,        // hasAggregations
      Some(schema) // Pass table schema for proper type conversion
    )

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    import org.apache.spark.sql.sources._

    logger.debug(
      s"PUSHFILTERS: pushFilters called on instance ${System.identityHashCode(this)} with ${filters.length} filters"
    )
    filters.foreach { filter =>
      logger.debug(s"PUSHFILTERS:   - Input filter: $filter (${filter.getClass.getSimpleName})")
    }

    // Since IndexQuery expressions are now handled directly by the V2IndexQueryExpressionRule,
    // we only need to handle regular Spark filters here.
    val (supported, unsupported) = filters.partition(isSupportedFilter)

    // NOTE: IsNull and IsNotNull are marked as "supported" for regular fields (not JSON fields)
    // in isSupportedFilter. The query converter uses wildcardQuery to properly filter these:
    // - IsNotNull(field) → wildcardQuery(field, "*") - matches docs where field has a value
    // - IsNull(field) → for now returns allQuery() (TODO: implement proper null handling)
    // This allows aggregate pushdown to work correctly with null/not-null filters.

    // Store filters in instance variables
    _pushedFilters = supported
    _unsupportedFilters = unsupported

    // CRITICAL FIX: Store by relation object (not table path) to survive across multiple optimization passes
    // Spark runs V2ScanRelationPushDown multiple times, creating fresh ScanBuilders each time
    // Only the builders that have Filter nodes get pushFilters() called
    // But the final ScanBuilder (which might not have pushFilters called) is the one executed
    // Different optimization passes may use different Table instances with different paths
    // Solution: Store filters by DataSourceV2Relation object (same approach as IndexQueries)
    // All ScanBuilders share the same relation object even across optimization passes
    IndexTables4SparkScanBuilder.getCurrentRelation() match {
      case Some(relation) =>
        IndexTables4SparkScanBuilder.storePushedFilters(relation, supported)
        IndexTables4SparkScanBuilder.storeUnsupportedFilters(relation, unsupported)
        logger.debug(
          s"PUSHFILTERS: Stored ${supported.length} supported, ${unsupported.length} unsupported filters by relation object: ${System
              .identityHashCode(relation)}"
        )
      case None =>
        logger.warn(s"PUSHFILTERS: No relation in ThreadLocal, cannot store filters for future ScanBuilder instances")
    }

    logger.debug(s"PUSHFILTERS: Supported=${supported.length}, Unsupported=${unsupported.length}")
    supported.foreach(filter => logger.debug(s"PUSHFILTERS:   ✓ SUPPORTED: $filter"))
    unsupported.foreach(filter => logger.debug(s"PUSHFILTERS:   ✗ UNSUPPORTED: $filter"))

    logger.info(s"Filter pushdown summary:")
    logger.info(s"  - ${supported.length} filters FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(filter => logger.info(s"    ✓ PUSHED: $filter"))

    logger.info(s"  - ${unsupported.length} filters NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(filter => logger.info(s"    ✗ NOT PUSHED: $filter"))

    // Return only unsupported filters - Spark will re-evaluate these after reading data
    unsupported
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.debug(s"PUSHPREDICATES DEBUG: pushPredicates called with ${predicates.length} predicates")
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

  override def pushedPredicates(): Array[Predicate] = Array.empty // V2 interface method

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
  }

  override def pushLimit(limit: Int): Boolean = {
    _limit = Some(limit)
    logger.info(s"Pushed limit: $limit")
    true // We support limit pushdown
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    // Return false to allow Spark to handle final aggregation combining partial results
    // This enables proper distributed aggregation where:
    // - AVG is transformed to SUM + COUNT by Spark
    // - Partial results from each partition are combined correctly
    logger.debug(s"AGGREGATE PUSHDOWN: supportCompletePushDown called - returning false for distributed aggregation")
    false
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    logger.debug(s"AGGREGATE PUSHDOWN: Received aggregation request: $aggregation")
    logger.debug(s"AGGREGATE PUSHDOWN: Number of pushed filters: ${_pushedFilters.length}")
    _pushedFilters.foreach(f => logger.debug(s"AGGREGATE PUSHDOWN: Pushed filter: $f"))

    // Check if this is a GROUP BY aggregation
    val groupByExpressions = aggregation.groupByExpressions()
    val hasGroupBy         = groupByExpressions != null && groupByExpressions.nonEmpty

    logger.debug(s"GROUP BY CHECK: hasGroupBy = $hasGroupBy")
    if (hasGroupBy) {
      logger.debug(s"GROUP BY DETECTED: Found ${groupByExpressions.length} GROUP BY expressions")
      logger.debug(s"GROUP BY DETECTED: Found ${groupByExpressions.length} GROUP BY expressions")
      groupByExpressions.foreach(expr => logger.debug(s"GROUP BY EXPRESSION: $expr"))

      // Extract GROUP BY column names
      val groupByColumns = groupByExpressions.map(extractFieldNameFromExpression)
      logger.debug(s"GROUP BY COLUMNS: ${groupByColumns.mkString(", ")}")

      // Validate GROUP BY columns are supported - throw exception if not
      logger.debug(s"GROUP BY VALIDATION: About to check areGroupByColumnsSupported")
      validateGroupByColumnsOrThrow(groupByColumns)
      logger.debug(s"GROUP BY VALIDATION: areGroupByColumnsSupported passed")

      // Check if aggregation is compatible with GROUP BY - throw exception if not
      logger.debug(s"GROUP BY VALIDATION: About to check isAggregationCompatibleWithGroupBy")
      validateAggregationCompatibilityOrThrow(aggregation)
      logger.debug(s"GROUP BY VALIDATION: isAggregationCompatibleWithGroupBy passed")

      // Store GROUP BY information
      _pushedGroupBy = Some(groupByColumns)
      logger.debug(s"GROUP BY PUSHDOWN: ACCEPTED - GROUP BY will be pushed down")
    } else {
      logger.debug(s"SIMPLE AGGREGATION: No GROUP BY expressions found")
    }

    // Validate aggregation is supported (both simple and GROUP BY)
    logger.debug(s"AGGREGATE PUSHDOWN: About to check isAggregationSupported")
    if (!isAggregationSupported(aggregation)) {
      logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - aggregation not supported")
      return false
    }
    logger.debug(s"AGGREGATE PUSHDOWN: isAggregationSupported passed")

    // Check if filters are compatible with aggregate pushdown
    logger.debug(s"AGGREGATE PUSHDOWN: About to check areFiltersCompatibleWithAggregation")
    try {
      if (!areFiltersCompatibleWithAggregation()) {
        logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - filters not compatible")
        return false
      }
      logger.debug(s"AGGREGATE PUSHDOWN: areFiltersCompatibleWithAggregation passed")
    } catch {
      case e: IllegalArgumentException =>
        logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - ${e.getMessage}")
        return false
    }

    // Store for later use in build()
    _pushedAggregation = Some(aggregation)
    logger.debug(s"AGGREGATE PUSHDOWN: ✅ ACCEPTED - aggregation will be pushed down")
    logger.debug(s"AGGREGATE PUSHDOWN: Returning true")
    true
  }

  /** Extract field name from Spark expression for GROUP BY detection. */
  private def extractFieldNameFromExpression(expression: org.apache.spark.sql.connector.expressions.Expression)
    : String = {
    // Use toString and try to extract field name
    val exprStr = expression.toString
    logger.debug(s"FIELD EXTRACTION: Expression string: '$exprStr'")
    logger.debug(s"FIELD EXTRACTION: Expression class: ${expression.getClass.getSimpleName}")

    // Check if it's a FieldReference by class name
    if (expression.getClass.getSimpleName == "FieldReference") {
      // For FieldReference, toString() returns the field name directly
      val fieldName = exprStr
      logger.debug(s"FIELD EXTRACTION: Extracted field name from FieldReference: '$fieldName'")
      fieldName
    } else if (exprStr.startsWith("FieldReference(")) {
      // Fallback for other FieldReference string formats
      val pattern = """FieldReference\(([^)]+)\)""".r
      pattern.findFirstMatchIn(exprStr) match {
        case Some(m) =>
          val fieldName = m.group(1)
          logger.debug(s"FIELD EXTRACTION: Extracted field name from pattern: '$fieldName'")
          fieldName
        case None =>
          logger.debug(s"FIELD EXTRACTION: Could not extract field name from expression: $expression")
          logger.warn(s"Could not extract field name from expression: $expression")
          "unknown_field"
      }
    } else {
      logger.debug(s"FIELD EXTRACTION: Unsupported expression type for field extraction: $expression")
      logger.warn(s"Unsupported expression type for field extraction: $expression")
      "unknown_field"
    }
  }

  // Configuration helpers for string pattern filter pushdown
  // Helper to get config value from both options (reader options) and config (session config)
  private def getConfigValue(key: String): Option[String] =
    // First check reader options, then session config
    Option(options.get(key)).orElse(config.get(key))

  // Master switch to enable all string pattern pushdowns at once
  private def isAllStringPatternPushdownEnabled: Boolean =
    getConfigValue("spark.indextables.filter.stringPattern.pushdown")
      .map(_.toLowerCase == "true")
      .getOrElse(false)

  private def isStringContainsPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringContains.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isStringStartsWithPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringStartsWith.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isStringEndsWithPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringEndsWith.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    filter match {
      case EqualTo(attribute, _)            => isFieldSuitableForExactMatching(attribute)
      case EqualNullSafe(attribute, _)      => isFieldSuitableForExactMatching(attribute)
      case GreaterThan(attribute, _)        => true // Support range on all fields (both regular and JSON)
      case GreaterThanOrEqual(attribute, _) => true // Support range on all fields (both regular and JSON)
      case LessThan(attribute, _)           => true // Support range on all fields (both regular and JSON)
      case LessThanOrEqual(attribute, _)    => true // Support range on all fields (both regular and JSON)
      case _: In                            => true
      case _: IsNull => false // Tantivy doesn't index nulls, can't filter for null values - Spark must post-filter
      case IsNotNull(attribute) => !attribute.contains(".") // Supported for regular fields (wildcardQuery handles this)
      case And(left, right)     => isSupportedFilter(left) && isSupportedFilter(right)
      case Or(left, right)      => isSupportedFilter(left) && isSupportedFilter(right)
      case Not(child)           => isSupportedFilter(child) // NOT is supported only if child is supported
      case _: StringStartsWith => isStringStartsWithPushdownEnabled // Enabled via config, Tantivy prefix queries are efficient
      case _: StringEndsWith => isStringEndsWithPushdownEnabled // Enabled via config, less efficient than prefix
      case _: StringContains => isStringContainsPushdownEnabled // Enabled via config, least efficient (full scan)
      case _                 => false
    }
  }

  /** Check if an attribute references a nested JSON field (contains dot notation). */
  private def isNestedJsonField(attribute: String): Boolean =
    attribute.contains(".")

  private def isSupportedPredicate(predicate: Predicate): Boolean = {
    // For V2 predicates, we need to inspect the actual predicate type
    // For now, let's accept all predicates and see what we get
    logger.debug(s"isSupportedPredicate: Checking predicate $predicate")

    // TODO: Implement proper predicate type checking based on Spark's V2 Predicate types
    true // Accept all for now to see what comes through
  }

  /**
   * Check if a field is suitable for exact matching at the data source level. String fields (raw tokenizer) support
   * exact matching. Text fields (default tokenizer) should be filtered by Spark for exact matches. Nested JSON fields
   * (containing dots) are always supported for pushdown via JsonPredicateTranslator.
   */
  private def isFieldSuitableForExactMatching(attribute: String): Boolean = {
    // Check if this is a nested field (JSON field)
    if (attribute.contains(".")) {
      logger.debug(s"Field '$attribute' is a nested JSON field - supporting pushdown via JsonPredicateTranslator")
      return true
    }

    // Check the field type configuration for top-level fields
    val fieldTypeKey = s"spark.indextables.indexing.typemap.$attribute"
    val fieldType    = config.get(fieldTypeKey)

    fieldType match {
      case Some("string") =>
        logger.debug(s"Field '$attribute' configured as 'string' - supporting exact matching")
        true
      case Some("text") =>
        logger.debug(s"Field '$attribute' configured as 'text' - deferring exact matching to Spark")
        false
      case Some(other) =>
        logger.debug(s"Field '$attribute' configured as '$other' - supporting exact matching")
        true
      case None =>
        // No explicit configuration - assume string type (new default)
        logger.debug(s"Field '$attribute' has no type configuration - assuming 'string', supporting exact matching")
        true
    }
  }

  /**
   * Detect if the current query plan contains an Aggregate operator. This is used to fail fast when aggregate pushdown
   * is blocked by unsupported filters.
   *
   * We inspect the query execution context to find Aggregate nodes in the logical plan.
   */
  private def detectAggregateInQueryPlan(): Boolean = {
    // Detect aggregation by examining the required schema
    // When an aggregate like COUNT(*) is used, the schema will have aggregate-like column names
    // or the schema will be radically different from the original table schema

    val aggregatePatterns = Seq("count(", "sum(", "avg(", "min(", "max(", "count_")

    // Check if required schema column names look like aggregate results
    val schemaLooksLikeAggregate = requiredSchema.fieldNames.exists { name =>
      val lowerName = name.toLowerCase
      aggregatePatterns.exists(lowerName.contains)
    }

    if (schemaLooksLikeAggregate) {
      logger.debug(
        s"Detected aggregate in query plan via schema inspection: ${requiredSchema.fieldNames.mkString(", ")}"
      )
      return true
    }

    // Fallback: check if the required schema is empty (COUNT(*) case)
    // or has significantly fewer columns than the original schema
    if (requiredSchema.isEmpty || (schema.length > 2 && requiredSchema.length == 1)) {
      // Could be an aggregate query - be conservative and assume yes
      logger.debug(s"Schema suggests possible aggregate query: original=${schema.length} cols, required=${requiredSchema.length} cols")
      return true
    }

    // Try string matching on relation as last resort
    try
      IndexTables4SparkScanBuilder.getCurrentRelation() match {
        case Some(relation) =>
          val planStr = relation.toString.toLowerCase
          val hasAgg = planStr.contains("aggregate") ||
            aggregatePatterns.exists(planStr.contains)
          if (hasAgg) {
            logger.debug("Detected aggregate via string matching on relation")
          }
          hasAgg
        case None =>
          false
      }
    catch {
      case _: Exception => false
    }
  }

  /**
   * Extract IndexQuery expressions directly using the companion object storage. This eliminates the need for global
   * registry by using instance-scoped storage.
   */
  private def extractIndexQueriesFromCurrentPlan(): Array[Any] = {
    logger.debug(s"EXTRACT DEBUG: Starting direct IndexQuery extraction")

    // Look up relation from ThreadLocal at usage time (not at construction time!)
    val relationForIndexQuery = IndexTables4SparkScanBuilder.getCurrentRelation()

    // Method 1: Get IndexQueries stored by V2IndexQueryExpressionRule for this relation object
    relationForIndexQuery match {
      case Some(relation) =>
        val storedQueries = IndexTables4SparkScanBuilder.getIndexQueries(relation)
        if (storedQueries.nonEmpty) {
          logger.debug(s"EXTRACT DEBUG: Found ${storedQueries.length} IndexQuery filters from relation storage")
          storedQueries.foreach(q => logger.debug(s"  - Relation IndexQuery: $q"))
          return storedQueries.toArray
        }
      case None =>
        logger.debug(s"EXTRACT DEBUG: No relation object available from ThreadLocal")
    }

    // Method 2: Fall back to registry (temporary until we fully eliminate it)
    import io.indextables.spark.filters.IndexQueryRegistry
    IndexQueryRegistry.getCurrentQueryId() match {
      case Some(queryId) =>
        val registryQueries = IndexQueryRegistry.getIndexQueriesForQuery(queryId)
        if (registryQueries.nonEmpty) {
          logger.debug(s"EXTRACT DEBUG: Found ${registryQueries.length} IndexQuery filters from registry as fallback")
          registryQueries.foreach(q => logger.debug(s"  - Registry IndexQuery: $q"))
          return registryQueries.toArray
        }
      case None =>
        logger.debug(s"EXTRACT DEBUG: No query ID available in registry")
    }

    logger.debug(s"EXTRACT DEBUG: No IndexQuery filters found using any method")
    Array.empty[Any]
  }

  /** Check if the aggregation is supported for pushdown. */
  private def isAggregationSupported(aggregation: Aggregation): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar, Sum, Avg, Min, Max}

    logger.debug(s"AGGREGATE VALIDATION: Checking ${aggregation.aggregateExpressions.length} aggregate expressions")
    aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (expr, index) =>
        logger.debug(s"AGGREGATE VALIDATION: Expression $index: $expr (${expr.getClass.getSimpleName})")
    }

    val result = aggregation.aggregateExpressions.forall { expr =>
      val isSupported = expr match {
        case _: Count =>
          logger.debug(s"AGGREGATE VALIDATION: COUNT aggregation is supported")
          logger.debug(s"AGGREGATE VALIDATION: COUNT aggregation is supported")
          true
        case _: CountStar =>
          logger.debug(s"AGGREGATE VALIDATION: COUNT(*) aggregation is supported")
          logger.debug(s"AGGREGATE VALIDATION: COUNT(*) aggregation is supported")
          true
        case sum: Sum =>
          val fieldName   = getFieldName(sum.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: SUM on field '$fieldName' supported: $isSupported")
          isSupported
        case avg: Avg =>
          val fieldName   = getFieldName(avg.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: AVG on field '$fieldName' supported: $isSupported")
          isSupported
        case min: Min =>
          val fieldName   = getFieldName(min.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: MIN on field '$fieldName' supported: $isSupported")
          isSupported
        case max: Max =>
          val fieldName   = getFieldName(max.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: MAX on field '$fieldName' supported: $isSupported")
          isSupported
        case other =>
          logger.debug(s"AGGREGATE VALIDATION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
          logger.debug(s"AGGREGATE VALIDATION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
          false
      }
      logger.debug(s"AGGREGATE VALIDATION: Expression $expr supported: $isSupported")
      isSupported
    }
    logger.debug(s"AGGREGATE VALIDATION: Overall aggregation supported: $result")
    result
  }

  /** Extract field name from an aggregate expression column. */
  private def getFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String = {
    // Use existing extractFieldNameFromExpression method
    val fieldName = extractFieldNameFromExpression(column)
    if (fieldName == "unknown_field") {
      throw new UnsupportedOperationException(s"Complex column expressions not supported for aggregation: $column")
    }
    fieldName
  }

  /** Check if a field is numeric and marked as fast in the schema. */
  private def isNumericFastField(fieldName: String): Boolean = {
    // Get actual fast fields from the schema/docMappingJson
    val fastFields = getActualFastFieldsFromSchema()

    // For nested JSON fields (containing dots), check if the field itself OR its parent struct is marked as fast
    if (fieldName.contains(".")) {
      val parentField  = fieldName.substring(0, fieldName.lastIndexOf('.'))
      val isFieldFast  = fastFields.contains(fieldName)
      val isParentFast = fastFields.contains(parentField)

      if (isFieldFast || isParentFast) {
        logger.debug(s"FAST FIELD VALIDATION: ✓ Nested field '$fieldName' accepted (field_fast=$isFieldFast, parent_fast=$isParentFast, parent='$parentField')")
        return true
      } else {
        logger.debug(s"FAST FIELD VALIDATION: ✗ Neither '$fieldName' nor parent '$parentField' marked as fast (available: ${fastFields.mkString(", ")})")
        return false
      }
    }

    // For top-level fields, check directly
    if (!fastFields.contains(fieldName)) {
      logger.debug(
        s"FAST FIELD VALIDATION: Field '$fieldName' is not marked as fast in schema (available fast fields: ${fastFields
            .mkString(", ")})"
      )
      return false
    }

    // Check if top-level field is numeric
    schema.fields.find(_.name == fieldName) match {
      case Some(field) if isNumericType(field.dataType) =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' is numeric and fast - supported")
        true
      case Some(field) =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' is not numeric (${field.dataType}) - not supported")
        false
      case None =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' not found in schema - not supported")
        false
    }
  }

  /** Check if a DataType is numeric. */
  private def isNumericType(dataType: org.apache.spark.sql.types.DataType): Boolean = {
    import org.apache.spark.sql.types._
    dataType match {
      case _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType => true
      case _                                                                            => false
    }
  }

  /**
   * Check if current filters are compatible with aggregate pushdown. This validates fast field configuration and throws
   * exceptions for validation failures.
   */
  private def areFiltersCompatibleWithAggregation(): Boolean = {
    // If there are no filters, aggregation is compatible
    if (_pushedFilters.isEmpty) {
      return true
    }

    // Check if filter types are supported and if filter fields are fast fields
    _pushedFilters.foreach { filter =>
      val isFilterTypeSupported = filter match {
        // Supported filter types
        case _: org.apache.spark.sql.sources.EqualTo            => true
        case _: org.apache.spark.sql.sources.GreaterThan        => true
        case _: org.apache.spark.sql.sources.LessThan           => true
        case _: org.apache.spark.sql.sources.GreaterThanOrEqual => true
        case _: org.apache.spark.sql.sources.LessThanOrEqual    => true
        case _: org.apache.spark.sql.sources.In                 => true
        case _: org.apache.spark.sql.sources.IsNull             => true
        case _: org.apache.spark.sql.sources.IsNotNull          => true
        case _: org.apache.spark.sql.sources.And                => true
        case _: org.apache.spark.sql.sources.Or                 => true
        case _: org.apache.spark.sql.sources.StringContains     => true
        case _: org.apache.spark.sql.sources.StringStartsWith   => true
        case _: org.apache.spark.sql.sources.StringEndsWith     => true

        // Unsupported filter types that would break aggregation accuracy
        case filter if filter.getClass.getSimpleName.contains("RLike") =>
          logger.debug(s"FILTER COMPATIBILITY: Regular expression filter not supported for aggregation: $filter")
          false
        case other =>
          logger.debug(s"FILTER COMPATIBILITY: Unknown filter type, assuming supported: $other")
          true
      }

      // If filter type is supported, validate that filter fields are fast fields
      // For aggregate pushdown, ALL filters require fast field configuration to ensure correctness
      // This will throw IllegalArgumentException if validation fails
      if (isFilterTypeSupported) {
        validateFilterFieldsAreFast(filter)
      } else {
        throw new IllegalArgumentException(s"Filter type not supported for aggregation pushdown: $filter")
      }
    }

    true
  }

  /** Get partition columns from the transaction log metadata. */
  private def getPartitionColumns(): Set[String] =
    try {
      val metadata = transactionLog.getMetadata()
      metadata.partitionColumns.toSet
    } catch {
      case e: Exception =>
        logger.debug(s"PARTITION COLUMNS: Failed to get partition columns from transaction log: ${e.getMessage}")
        Set.empty
    }

  /**
   * Get fast fields from the actual table schema/docMappingJson, not from configuration. This reads the transaction log
   * to determine which fields are actually configured as fast.
   */
  private def getActualFastFieldsFromSchema(): Set[String] =
    try {
      logger.debug("Reading actual fast fields from transaction log")

      // Read existing files from transaction log to get docMappingJson
      val existingFiles = transactionLog.listFiles()
      val existingDocMapping = existingFiles
        .flatMap(_.docMappingJson)
        .headOption // Get the first available doc mapping

      if (existingDocMapping.isDefined) {
        logger.debug("Found doc mapping, parsing fast fields")

        // Parse the docMappingJson to extract fast field information
        import com.fasterxml.jackson.databind.JsonNode
        import io.indextables.spark.util.JsonUtil
        import scala.jdk.CollectionConverters._

        val mappingJson = existingDocMapping.get
        logger.debug(s"Full docMappingJson: ${mappingJson
            .take(500)}${if (mappingJson.length > 500) "..." else ""}")
        val docMapping = JsonUtil.mapper.readTree(mappingJson)

        if (docMapping.isArray) {
          val fastFields = docMapping.asScala.flatMap { fieldNode =>
            val fieldName = Option(fieldNode.get("name")).map(_.asText())
            val isFast = Option(fieldNode.get("fast"))
              .map(_.asBoolean())
              .getOrElse(false)
            val fieldType = Option(fieldNode.get("type")).map(_.asText()).getOrElse("unknown")

            logger.debug(s"Field entry: name=${fieldName.getOrElse("N/A")}, fast=$isFast, type=$fieldType")

            if (isFast && fieldName.isDefined) {
              logger.debug(s"✓ Found fast field: ${fieldName.get}")
              Some(fieldName.get)
            } else {
              None
            }
          }.toSet

          logger.debug(s"Actual fast fields from schema: ${fastFields.mkString(", ")}")
          fastFields
        } else {
          logger.debug("Doc mapping is not an array - unexpected format")
          Set.empty[String]
        }
      } else {
        logger.debug("No doc mapping found - likely new table, falling back to configuration-based validation")
        // Fall back to configuration-based validation for new tables
        val fastFieldsStr = config
          .get("spark.indextables.indexing.fastfields")
          .getOrElse("")
        if (fastFieldsStr.nonEmpty) {
          fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
        } else {
          Set.empty[String]
        }
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Failed to read fast fields from schema: ${e.getMessage}")
        // Fall back to configuration-based validation
        val fastFieldsStr = config
          .get("spark.indextables.indexing.fastfields")
          .getOrElse("")
        if (fastFieldsStr.nonEmpty) {
          fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
        } else {
          Set.empty[String]
        }
    }

  private def validateFilterFieldsAreFast(filter: org.apache.spark.sql.sources.Filter): Boolean = {
    // Get actual fast fields from the schema/docMappingJson
    val fastFields = getActualFastFieldsFromSchema()

    // Get partition columns - these don't need to be fast since they're handled by transaction log
    val partitionColumns = getPartitionColumns()

    // Extract field names from filter
    val filterFields = extractFieldNamesFromFilter(filter)

    // Exclude partition columns from fast field validation
    val nonPartitionFilterFields = filterFields -- partitionColumns

    logger.debug(s"FILTER VALIDATION: Filter: $filter")
    logger.debug(s"FILTER VALIDATION: All filter fields: ${filterFields.mkString(", ")}")
    logger.debug(s"FILTER VALIDATION: Non-partition filter fields: ${nonPartitionFilterFields.mkString(", ")}")
    logger.debug(s"FILTER VALIDATION: Fast fields from schema: ${fastFields.mkString(", ")}")

    // If all filter fields are partition columns, we don't need fast fields (transaction log optimization)
    if (nonPartitionFilterFields.isEmpty) {
      logger.debug(s"FILTER VALIDATION: All filters are on partition columns - no fast fields required")
      return true
    }

    // Check if all non-partition filter fields are configured as fast fields
    // For nested JSON fields (e.g., "metadata.field1"), check if either the field itself
    // OR its parent (e.g., "metadata") is marked as fast
    val missingFastFields = nonPartitionFilterFields.filterNot { fieldName =>
      if (fieldName.contains(".")) {
        // Nested field - check both field and parent
        val parentField  = fieldName.substring(0, fieldName.lastIndexOf('.'))
        val isFieldFast  = fastFields.contains(fieldName)
        val isParentFast = fastFields.contains(parentField)
        isFieldFast || isParentFast
      } else {
        // Top-level field - direct check
        fastFields.contains(fieldName)
      }
    }
    logger.debug(s"FILTER VALIDATION DEBUG: missingFastFields=$missingFastFields")

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"

      logger.debug(s"FILTER FAST FIELD VALIDATION: Missing fast fields for filter: $columnList")
      logger.debug(s"FILTER FAST FIELD VALIDATION: Current fast fields from schema: $currentFastFields")
      logger.debug(s"FILTER FAST FIELD VALIDATION: Filter rejected for aggregation pushdown: $filter")

      throw new IllegalArgumentException(
        s"""COUNT aggregation with filters requires fast field configuration.
           |
           |Missing fast fields for filter columns: $columnList
           |
           |The table schema shows these fields are not configured as fast fields.
           |With the new default behavior, numeric, string, and date fields should be fast by default.
           |
           |Current fast fields in schema: $currentFastFields
           |Required fast fields: ${(fastFields ++ missingFastFields).toSeq.distinct.mkString("'", "', '", "'")}
           |
           |Filter: $filter""".stripMargin
      )
    }

    true
  }

  /** Extract field names from a Spark Filter. */
  private def extractFieldNamesFromFilter(filter: org.apache.spark.sql.sources.Filter): Set[String] =
    filter match {
      case f: org.apache.spark.sql.sources.EqualTo            => Set(f.attribute)
      case f: org.apache.spark.sql.sources.GreaterThan        => Set(f.attribute)
      case f: org.apache.spark.sql.sources.LessThan           => Set(f.attribute)
      case f: org.apache.spark.sql.sources.GreaterThanOrEqual => Set(f.attribute)
      case f: org.apache.spark.sql.sources.LessThanOrEqual    => Set(f.attribute)
      case f: org.apache.spark.sql.sources.In                 => Set(f.attribute)
      case f: org.apache.spark.sql.sources.IsNull             => Set(f.attribute)
      case f: org.apache.spark.sql.sources.IsNotNull          => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringContains     => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringStartsWith   => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringEndsWith     => Set(f.attribute)
      case f: org.apache.spark.sql.sources.And =>
        extractFieldNamesFromFilter(f.left) ++ extractFieldNamesFromFilter(f.right)
      case f: org.apache.spark.sql.sources.Or =>
        extractFieldNamesFromFilter(f.left) ++ extractFieldNamesFromFilter(f.right)
      case other =>
        logger.debug(s"FILTER FIELD EXTRACTION: Unknown filter type, cannot extract fields: $other")
        Set.empty[String]
    }

  /** Check if GROUP BY columns are supported for pushdown. */
  private def areGroupByColumnsSupported(groupByColumns: Array[String]): Boolean = {
    logger.debug(s"GROUP BY VALIDATION: Checking ${groupByColumns.length} columns: ${groupByColumns.mkString(", ")}")
    logger.debug(s"GROUP BY VALIDATION: Schema fields: ${schema.fields.map(_.name).mkString(", ")}")

    groupByColumns.forall { columnName =>
      logger.debug(s"GROUP BY VALIDATION: Checking column '$columnName'")
      // Check if the column exists in the schema
      val fieldExists = schema.fields.exists(_.name == columnName)
      logger.debug(s"GROUP BY VALIDATION: Field '$columnName' exists: $fieldExists")
      if (!fieldExists) {
        logger.debug(s"GROUP BY VALIDATION: Field '$columnName' not found in schema")
        logger.debug(s"GROUP BY VALIDATION: Field '$columnName' not found in schema")
        return false
      }

      // For GROUP BY, we need fields that can be used for terms aggregation
      // String fields work well for grouping
      schema.fields.find(_.name == columnName) match {
        case Some(field) =>
          import org.apache.spark.sql.types._
          // ALL GROUP BY fields must be fast fields for tantivy4java TermsAggregation
          val fastFields = getActualFastFieldsFromSchema()
          val isFast     = fastFields.contains(columnName)

          field.dataType match {
            case StringType =>
              if (isFast) {
                logger.debug(s"GROUP BY VALIDATION: Fast string field '$columnName' is supported for GROUP BY")
                logger.debug(s"GROUP BY VALIDATION: Fast string field '$columnName' is supported for GROUP BY")
                true
              } else {
                logger.info(
                  s"GROUP BY VALIDATION: String field '$columnName' must be fast field for tantivy4java GROUP BY"
                )
                false
              }
            case IntegerType | LongType =>
              if (isFast) {
                logger.debug(s"GROUP BY VALIDATION: Fast numeric field '$columnName' is supported for GROUP BY")
                logger.debug(s"GROUP BY VALIDATION: Fast numeric field '$columnName' is supported for GROUP BY")
                true
              } else {
                logger.info(
                  s"GROUP BY VALIDATION: Numeric field '$columnName' must be fast field for efficient GROUP BY"
                )
                logger.debug(
                  s"GROUP BY VALIDATION: Numeric field '$columnName' must be fast field for efficient GROUP BY"
                )
                false
              }
            case DateType | TimestampType =>
              if (isFast) {
                logger.debug(
                  s"GROUP BY VALIDATION: Fast date/timestamp field '$columnName' is supported for GROUP BY"
                )
                logger.debug(
                  s"GROUP BY VALIDATION: Fast date/timestamp field '$columnName' is supported for GROUP BY"
                )
                true
              } else {
                logger.info(
                  s"GROUP BY VALIDATION: Date/timestamp field '$columnName' must be fast field for GROUP BY"
                )
                logger.debug(
                  s"GROUP BY VALIDATION: Date/timestamp field '$columnName' must be fast field for GROUP BY"
                )
                false
              }
            case _ =>
              logger.debug(s"GROUP BY VALIDATION: Field type ${field.dataType} not supported for GROUP BY")
              logger.debug(s"GROUP BY VALIDATION: Field type ${field.dataType} not supported for GROUP BY")
              false
          }
        case None => false
      }
    }
  }

  /** Check if the current aggregation is compatible with GROUP BY. */
  private def isAggregationCompatibleWithGroupBy(aggregation: Aggregation): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    logger.debug(s"GROUP BY COMPATIBILITY: Checking ${aggregation.aggregateExpressions.length} aggregate expressions")

    // Check each aggregate expression - for GROUP BY, ALL aggregated fields must be fast fields
    val result = aggregation.aggregateExpressions.forall { expr =>
      val isCompatible = expr match {
        case _: Count =>
          logger.debug(s"GROUP BY COMPATIBILITY: COUNT is compatible with GROUP BY (no field required)")
          true
        case _: CountStar =>
          logger.debug(s"GROUP BY COMPATIBILITY: COUNT(*) is compatible with GROUP BY (no field required)")
          true
        case sum: Sum =>
          val fieldName = getFieldName(sum.column)
          val isFast    = isNumericFastField(fieldName)
          if (isFast) {
            logger.debug(s"GROUP BY COMPATIBILITY: SUM($fieldName) is compatible with GROUP BY (fast field)")
            true
          } else {
            logger.debug(s"GROUP BY COMPATIBILITY: SUM($fieldName) requires fast field for GROUP BY")
            logger.debug(s"GROUP BY COMPATIBILITY: SUM($fieldName) requires fast field for GROUP BY")
            false
          }
        case avg: Avg =>
          val fieldName = getFieldName(avg.column)
          val isFast    = isNumericFastField(fieldName)
          if (isFast) {
            logger.debug(s"GROUP BY COMPATIBILITY: AVG($fieldName) is compatible with GROUP BY (fast field)")
            true
          } else {
            logger.debug(s"GROUP BY COMPATIBILITY: AVG($fieldName) requires fast field for GROUP BY")
            logger.debug(s"GROUP BY COMPATIBILITY: AVG($fieldName) requires fast field for GROUP BY")
            false
          }
        case min: Min =>
          val fieldName = getFieldName(min.column)
          val isFast    = isNumericFastField(fieldName)
          if (isFast) {
            logger.debug(s"GROUP BY COMPATIBILITY: MIN($fieldName) is compatible with GROUP BY (fast field)")
            true
          } else {
            logger.debug(s"GROUP BY COMPATIBILITY: MIN($fieldName) requires fast field for GROUP BY")
            logger.debug(s"GROUP BY COMPATIBILITY: MIN($fieldName) requires fast field for GROUP BY")
            false
          }
        case max: Max =>
          val fieldName = getFieldName(max.column)
          val isFast    = isNumericFastField(fieldName)
          if (isFast) {
            logger.debug(s"GROUP BY COMPATIBILITY: MAX($fieldName) is compatible with GROUP BY (fast field)")
            true
          } else {
            logger.debug(s"GROUP BY COMPATIBILITY: MAX($fieldName) requires fast field for GROUP BY")
            logger.debug(s"GROUP BY COMPATIBILITY: MAX($fieldName) requires fast field for GROUP BY")
            false
          }
        case other =>
          logger.info(
            s"GROUP BY COMPATIBILITY: Unsupported aggregation type with GROUP BY: ${other.getClass.getSimpleName}"
          )
          false
      }
      logger.debug(s"GROUP BY COMPATIBILITY: Expression $expr compatible: $isCompatible")
      isCompatible
    }
    logger.debug(s"GROUP BY COMPATIBILITY: Overall compatibility: $result")
    result
  }

  /** Check if a filter is a partition filter. */
  private def isPartitionFilter(filter: org.apache.spark.sql.sources.Filter): Boolean = {
    val partitionColumns  = getPartitionColumns()
    val referencedColumns = getFilterReferencedColumns(filter)
    referencedColumns.nonEmpty && referencedColumns.forall(partitionColumns.contains)
  }

  /** Get columns referenced by a filter. */
  private def getFilterReferencedColumns(filter: org.apache.spark.sql.sources.Filter): Set[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, _)            => Set(attribute)
      case EqualNullSafe(attribute, _)      => Set(attribute)
      case GreaterThan(attribute, _)        => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _)           => Set(attribute)
      case LessThanOrEqual(attribute, _)    => Set(attribute)
      case In(attribute, _)                 => Set(attribute)
      case IsNull(attribute)                => Set(attribute)
      case IsNotNull(attribute)             => Set(attribute)
      case StringStartsWith(attribute, _)   => Set(attribute)
      case StringEndsWith(attribute, _)     => Set(attribute)
      case StringContains(attribute, _)     => Set(attribute)
      case And(left, right)                 => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right)                  => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child)                       => getFilterReferencedColumns(child)
      case _                                => Set.empty[String]
    }
  }

  /** Validate GROUP BY columns and throw a descriptive exception if validation fails. */
  private def validateGroupByColumnsOrThrow(groupByColumns: Array[String]): Unit = {
    val missingFastFields = scala.collection.mutable.ArrayBuffer[String]()

    // Read actual fast fields from transaction log (docMappingJson), not from configuration
    val fastFields = getActualFastFieldsFromSchema()

    groupByColumns.foreach { columnName =>
      // Check if the column exists in the schema
      schema.fields.find(_.name == columnName) match {
        case Some(field) =>
          import org.apache.spark.sql.types._
          // Check if field is configured as fast field
          if (!fastFields.contains(columnName)) {
            missingFastFields += columnName
          }
        case None =>
          throw new IllegalArgumentException(
            s"GROUP BY column '$columnName' not found in schema. Available columns: ${schema.fields.map(_.name).mkString(", ")}"
          )
      }
    }

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"

      throw new IllegalArgumentException(
        s"""GROUP BY requires fast field configuration for efficient aggregation.
           |
           |Missing fast fields for GROUP BY columns: $columnList
           |
           |To fix this issue, configure these columns as fast fields:
           |  .option("spark.indexfiles.indexing.fastfields", "${(fastFields ++ missingFastFields).toSeq.distinct
            .mkString(",")}")
           |
           |Current fast fields: $currentFastFields
           |Required fast fields: ${(fastFields ++ missingFastFields).toSeq.distinct
            .mkString("'", "', '", "'")}""".stripMargin
      )
    }
  }

  /** Validate aggregation compatibility with GROUP BY and throw a descriptive exception if validation fails. */
  private def validateAggregationCompatibilityOrThrow(aggregation: Aggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    val missingFastFields = scala.collection.mutable.ArrayBuffer[String]()

    // Use broadcast config instead of options for merged configuration
    val fastFieldsStr = config
      .get("spark.indextables.indexing.fastfields")
      .getOrElse("")

    val fastFields = if (fastFieldsStr.nonEmpty) {
      fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
    } else {
      Set.empty[String]
    }

    aggregation.aggregateExpressions.foreach { expr =>
      expr match {
        case _: Count | _: CountStar =>
        // COUNT and COUNT(*) don't require specific fast fields
        case sum: Sum =>
          val fieldName = getFieldName(sum.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case avg: Avg =>
          val fieldName = getFieldName(avg.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case min: Min =>
          val fieldName = getFieldName(min.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case max: Max =>
          val fieldName = getFieldName(max.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Aggregation function '${other.getClass.getSimpleName}' is not supported with GROUP BY operations"
          )
      }
    }

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"
      val allRequiredFields = (fastFields ++ missingFastFields).toSeq.distinct

      throw new IllegalArgumentException(
        s"""GROUP BY with aggregation functions requires fast field configuration for efficient processing.
           |
           |Missing fast fields for aggregation columns: $columnList
           |
           |To fix this issue, configure these columns as fast fields:
           |  .option("spark.indexfiles.indexing.fastfields", "${allRequiredFields.mkString(",")}")
           |
           |Current fast fields: $currentFastFields
           |Required fast fields: ${allRequiredFields.mkString("'", "', '", "'")}""".stripMargin
      )
    }
  }
}

/**
 * Companion object for ScanBuilder to store IndexQuery information. This provides a clean mechanism for
 * V2IndexQueryExpressionRule to pass IndexQuery expressions directly to the ScanBuilder without a global registry.
 */
object IndexTables4SparkScanBuilder {
  import java.util.WeakHashMap

  import scala.collection.JavaConverters._

  // WeakHashMap using DataSourceV2Relation object as key
  // The relation object is passed from V2IndexQueryExpressionRule and accessible during planning
  // WeakHashMap allows GC to clean up entries when relations are no longer referenced
  private val relationIndexQueries: WeakHashMap[AnyRef, Seq[Any]] = new WeakHashMap[AnyRef, Seq[Any]]()

  // SOLUTION: Store regular pushed filters by relation object (same approach as IndexQueries)
  // This solves the multi-pass optimization issue where different ScanBuilder instances
  // are created across optimization passes, but they all share the same DataSourceV2Relation
  private val relationPushedFilters: WeakHashMap[AnyRef, Array[Filter]] = new WeakHashMap[AnyRef, Array[Filter]]()

  // Store unsupported filters by relation object for aggregate exception detection
  private val relationUnsupportedFilters: WeakHashMap[AnyRef, Array[Filter]] = new WeakHashMap[AnyRef, Array[Filter]]()

  // ThreadLocal to pass the actual relation object from V2 rule to ScanBuilder
  // This works even with AQE because the same relation object is used throughout planning
  // Lifecycle: V2 rule checks relation identity → clears if different → sets new relation → ScanBuilder gets it
  // We track the relation's identity hash (stable within query, different for self-joins) to avoid clearing mid-query
  private val currentRelation: ThreadLocal[Option[AnyRef]] = ThreadLocal.withInitial(() => None)
  private val currentRelationId: ThreadLocal[Option[Int]]  = ThreadLocal.withInitial(() => None)

  /** Set the current relation object for this thread (called by V2IndexQueryExpressionRule). */
  def setCurrentRelation(relation: AnyRef): Unit = {
    // Use relation object identity - this is stable within a query execution
    // and correctly handles self-joins (which get different relation instances via newInstance())
    val relationId = System.identityHashCode(relation)
    currentRelation.set(Some(relation))
    currentRelationId.set(Some(relationId))
  }

  /** Get the current relation object for this thread (called by ScanBuilder). */
  def getCurrentRelation(): Option[AnyRef] =
    currentRelation.get()

  /**
   * Clear the current relation object for this thread, but only if it's a different relation. This allows the same
   * relation to be reused across multiple optimization phases.
   */
  def clearCurrentRelationIfDifferent(newRelation: Option[AnyRef]): Unit = {
    val currentId = currentRelationId.get()
    val newId     = newRelation.map(System.identityHashCode)

    if (currentId.isDefined && newId.isDefined && currentId != newId) {
      // Different relation - clear the old one
      currentRelation.remove()
      currentRelationId.remove()
    }
    // else if (currentId.isDefined && newId.isEmpty): Don't clear!
    // The V2IndexQueryExpressionRule is called on many plan types (LocalRelation, Project, etc.)
    // that don't contain DataSourceV2Relation. We should NOT clear the ThreadLocal just because
    // the current plan node doesn't have a relation - it was set by an earlier plan node that did.
    // Only clear when we encounter a DIFFERENT relation.
    // else: same relation or no new relation - keep ThreadLocal for reuse
  }

  /** Clear the current relation object for this thread (for tests). */
  def clearCurrentRelation(): Unit = {
    currentRelation.remove()
    currentRelationId.remove()
  }

  /** Store IndexQuery expressions for a specific relation object. */
  def storeIndexQueries(relation: AnyRef, indexQueries: Seq[Any]): Unit =
    relationIndexQueries.synchronized {
      relationIndexQueries.put(relation, indexQueries)
    }

  /** Retrieve IndexQuery expressions for a specific relation object. */
  def getIndexQueries(relation: AnyRef): Seq[Any] =
    relationIndexQueries.synchronized {
      Option(relationIndexQueries.get(relation)).getOrElse(Seq.empty)
    }

  /** Clear IndexQuery expressions for a specific relation object. */
  def clearIndexQueries(relation: AnyRef): Unit =
    relationIndexQueries.synchronized {
      relationIndexQueries.remove(relation)
    }

  /** Get cache statistics for monitoring. */
  def getCacheStats(): String = {
    val size = relationIndexQueries.synchronized(relationIndexQueries.size())
    s"IndexQuery Cache Stats - Size: $size (WeakHashMap)"
  }

  /** Store pushed filters for a specific relation object. */
  def storePushedFilters(relation: AnyRef, filters: Array[Filter]): Unit =
    relationPushedFilters.synchronized {
      relationPushedFilters.put(relation, filters)
    }

  /** Retrieve pushed filters for a specific relation object. */
  def getPushedFilters(relation: AnyRef): Array[Filter] =
    relationPushedFilters.synchronized {
      Option(relationPushedFilters.get(relation)).getOrElse(Array.empty)
    }

  /** Clear pushed filters for a specific relation object. */
  def clearPushedFilters(relation: AnyRef): Unit =
    relationPushedFilters.synchronized {
      relationPushedFilters.remove(relation)
    }

  /** Store unsupported filters for a specific relation object. */
  def storeUnsupportedFilters(relation: AnyRef, filters: Array[Filter]): Unit =
    relationUnsupportedFilters.synchronized {
      relationUnsupportedFilters.put(relation, filters)
    }

  /** Retrieve unsupported filters for a specific relation object. */
  def getUnsupportedFilters(relation: AnyRef): Array[Filter] =
    relationUnsupportedFilters.synchronized {
      Option(relationUnsupportedFilters.get(relation)).getOrElse(Array.empty)
    }

}
