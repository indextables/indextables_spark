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

import org.apache.spark.sql.sources._

import io.indextables.spark.config.IndexTables4SparkSQLConf
import io.indextables.spark.exceptions.IndexQueryParseException
import io.indextables.spark.filters.{IndexQueryAllFilter, IndexQueryFilter}
import io.indextables.spark.json.{JsonPredicateTranslator, SparkSchemaToTantivyMapper}
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.util.TimestampUtils
import io.indextables.tantivy4java.core.{FieldType, Index, Schema}
import io.indextables.tantivy4java.query.{Occur, Query}
import io.indextables.tantivy4java.split.{
  SplitBooleanQuery,
  SplitExistsQuery,
  SplitMatchAllQuery,
  SplitQuery,
  SplitTermQuery
}
import org.slf4j.LoggerFactory

// Data class for storing range optimization information
case class RangeInfo(
  min: Option[Any],
  max: Option[Any],
  minInclusive: Boolean,
  maxInclusive: Boolean)

object FiltersToQueryConverter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Store range optimization information in ThreadLocal for thread-safety
  // This ensures concurrent query planning from different Spark tasks doesn't cause data races
  private val rangeOptimizations: ThreadLocal[scala.collection.mutable.Map[String, RangeInfo]] =
    ThreadLocal.withInitial(() => scala.collection.mutable.Map[String, RangeInfo]())

  // Cache for JsonPredicateTranslator to avoid repeated object creation per filter
  // Key: (sparkSchema hashCode, options hashCode), Value: (jsonMapper, jsonTranslator)
  private val jsonTranslatorCache
    : ThreadLocal[scala.collection.mutable.Map[Int, (SparkSchemaToTantivyMapper, JsonPredicateTranslator)]] =
    ThreadLocal.withInitial(() =>
      scala.collection.mutable.Map[Int, (SparkSchemaToTantivyMapper, JsonPredicateTranslator)]()
    )

  /** Get or create a cached JsonPredicateTranslator for the given schema and options. */
  private def getJsonTranslator(
    sparkSchema: org.apache.spark.sql.types.StructType,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): JsonPredicateTranslator = {
    val cacheKey = sparkSchema.hashCode() * 31 + options.hashCode()
    val cache    = jsonTranslatorCache.get()

    cache
      .getOrElseUpdate(
        cacheKey, {
          val tantivyOptions = options.map(opts => new io.indextables.spark.core.IndexTables4SparkOptions(opts))
          val jsonMapper     = new SparkSchemaToTantivyMapper(tantivyOptions.orNull)
          val jsonTranslator = new JsonPredicateTranslator(sparkSchema, jsonMapper)
          (jsonMapper, jsonTranslator)
        }
      )
      ._2
  }

  /**
   * Optimize range filters by detecting >= and < operations on the same field and storing the optimization information
   * for later use.
   */
  def optimizeRangeFilters(filters: Array[Filter]): Array[Filter] = {
    // Clear previous optimizations (thread-local)
    rangeOptimizations.get().clear()

    // Group filters by field name for range detection
    val filtersByField = filters.groupBy {
      case GreaterThan(attr, _)        => Some(attr)
      case GreaterThanOrEqual(attr, _) => Some(attr)
      case LessThan(attr, _)           => Some(attr)
      case LessThanOrEqual(attr, _)    => Some(attr)
      case _                           => None
    }

    val nonRangeFilters = filtersByField.getOrElse(None, Array.empty)
    val rangeFilters    = filtersByField - None

    // Detect range combinations and store optimization info
    val remainingFilters = scala.collection.mutable.ArrayBuffer[Filter]()

    rangeFilters.foreach {
      case (fieldOpt, fieldFilters) =>
        fieldOpt match {
          case Some(field) if fieldFilters.length > 1 =>
            // Check if we have a combination that can be optimized
            val rangeInfo = analyzeRangeFilters(fieldFilters)
            if (rangeInfo.min.isDefined || rangeInfo.max.isDefined) {
              // Store optimization info and exclude these filters from regular processing
              rangeOptimizations.get()(field) = rangeInfo
              queryLog(s"Detected range optimization for field '$field': $rangeInfo")
            } else {
              // Keep filters as-is if no optimization possible
              remainingFilters ++= fieldFilters
            }
          case Some(_) =>
            // Single range filter, keep as-is
            remainingFilters ++= fieldFilters
          case None =>
          // Should not happen given our grouping
        }
    }

    // Return non-range filters plus any remaining range filters
    nonRangeFilters ++ remainingFilters
  }

  /**
   * Remove IsNotNull filters that are redundant because a value-bearing sibling filter already references the same
   * attribute. A document matching EqualTo(X, v) or GreaterThan(X, v) inherently has a non-null X, so the existence
   * check is meaningless.
   */
  private[core] def removeRedundantIsNotNull(filters: Array[Filter]): Array[Filter] = {
    import org.apache.spark.sql.sources._

    def getValueAttributes(f: Filter): Set[String] = f match {
      case EqualTo(attr, _)            => Set(attr)
      case EqualNullSafe(attr, _)      => Set(attr)
      case GreaterThan(attr, _)        => Set(attr)
      case GreaterThanOrEqual(attr, _) => Set(attr)
      case LessThan(attr, _)           => Set(attr)
      case LessThanOrEqual(attr, _)    => Set(attr)
      case In(attr, _)                 => Set(attr)
      case StringStartsWith(attr, _)   => Set(attr)
      case StringEndsWith(attr, _)     => Set(attr)
      case StringContains(attr, _)     => Set(attr)
      case And(left, right)            => getValueAttributes(left) ++ getValueAttributes(right)
      case Or(left, right)             => getValueAttributes(left) ++ getValueAttributes(right)
      case Not(child)                  => getValueAttributes(child)
      case _                           => Set.empty
    }

    val attributesWithValues = filters.flatMap(getValueAttributes).toSet

    if (attributesWithValues.isEmpty) {
      filters
    } else {
      val (removed, kept) = filters.partition {
        case IsNotNull(attr) => attributesWithValues.contains(attr)
        case _               => false
      }
      if (removed.nonEmpty) {
        queryLog(s"Removed ${removed.length} redundant IsNotNull filter(s): ${removed.mkString(", ")}")
      }
      kept
    }
  }

  /** Analyze range filters for a field and extract range information. */
  private def analyzeRangeFilters(filters: Array[Filter]): RangeInfo = {
    var min: Option[Any] = None
    var max: Option[Any] = None
    var minInclusive     = true
    var maxInclusive     = true

    filters.foreach {
      case GreaterThan(_, value) =>
        min = Some(value)
        minInclusive = false
      case GreaterThanOrEqual(_, value) =>
        min = Some(value)
        minInclusive = true
      case LessThan(_, value) =>
        max = Some(value)
        maxInclusive = false
      case LessThanOrEqual(_, value) =>
        max = Some(value)
        maxInclusive = true
      case _ =>
      // Ignore non-range filters
    }

    RangeInfo(min, max, minInclusive, maxInclusive)
  }

  /**
   * Transform an IndexQuery string to handle leading wildcards automatically.
   *
   * Tantivy's default query parser doesn't support leading wildcards (e.g., `*Configuration`). However, explicit field
   * syntax (e.g., `fieldname:*Configuration`) uses the wildcard query builder directly, which does support leading
   * wildcards.
   *
   * This method automatically transforms ALL leading wildcard terms in a query when we know the target field, making
   * leading wildcard queries work transparently for users.
   *
   * Examples:
   *   - Input: `*Configuration`, field: `message` -> Output: `message:*Configuration`
   *   - Input: `*Broker*`, field: `content` -> Output: `content:*Broker*`
   *   - Input: `config*`, field: `title` -> Output: `config*` (no change - trailing wildcard works)
   *   - Input: `title:*test`, field: `title` -> Output: `title:*test` (already has field prefix)
   *   - Input: `bob AND *wildcard*`, field: `msg` -> Output: `bob AND msg:*wildcard*`
   *   - Input: `bob AND *ildcar* OR (sally AND *oogl*)`, field: `msg` -> Output: `bob AND msg:*ildcar* OR (sally AND
   *     msg:*oogl*)`
   *
   * @param queryString
   *   The original query string
   * @param fieldName
   *   The target field name for the IndexQuery
   * @return
   *   The transformed query string with field prefix added to leading wildcard terms
   */
  def transformLeadingWildcardQuery(queryString: String, fieldName: String): String = {
    val trimmed = queryString.trim

    // If no wildcards at all, return as-is (fast path)
    if (!trimmed.contains("*") && !trimmed.contains("?")) {
      return trimmed
    }

    // Use regex to find leading wildcard terms that don't already have a field prefix.
    // Pattern matches:
    //   - A wildcard term starting with * or ?
    //   - That is NOT preceded by a word character and colon (field prefix like "field:")
    //   - The term continues with word characters and may contain more wildcards
    //
    // Negative lookbehind (?<!\w:) ensures we don't match terms that already have field prefix
    // Word boundary or start ensures we match complete terms
    //
    // Examples that SHOULD match (no field prefix):
    //   "*config" at start of string
    //   " *config" after space
    //   "(*config" after open paren
    //
    // Examples that should NOT match (already has field prefix):
    //   "field:*config"
    //
    val leadingWildcardPattern = """(?<!\w:)(?<=^|[\s(])([*?][\w*?]+)""".r

    val transformed = leadingWildcardPattern.replaceAllIn(
      trimmed,
      m => {
        val wildcardTerm = m.group(1)
        s"$fieldName:$wildcardTerm"
      }
    )

    if (transformed != trimmed) {
      queryLog(s"Transformed leading wildcard query '$trimmed' to '$transformed'")
    }

    transformed
  }

  /** Check if a query string contains a leading wildcard pattern. Useful for logging and diagnostics. */
  def hasLeadingWildcard(queryString: String): Boolean = {
    val trimmed = queryString.trim
    trimmed.startsWith("*") || trimmed.startsWith("?")
  }

  /** Create an optimized range query from stored range information using SplitRangeQuery. */
  private def createOptimizedRangeQuery(
    field: String,
    rangeInfo: RangeInfo,
    schema: Schema,
    splitSearchEngine: SplitSearchEngine
  ): Option[SplitQuery] =
    try {
      val fieldType = getFieldType(schema, field)
      val minValue  = rangeInfo.min.map(convertSparkValueToTantivy(_, fieldType))
      val maxValue  = rangeInfo.max.map(convertSparkValueToTantivy(_, fieldType))

      // Convert values to string format for SplitRangeQuery
      def formatValueForRangeQuery(value: Any): String =
        value match {
          case dateString: String =>
            // For date fields, use YYYY-MM-DD format strings directly
            dateString
          case other => other.toString
        }

      // Determine the tantivy field type for SplitRangeQuery
      val tantivyFieldType = fieldType match {
        case FieldType.DATE     => "date"
        case FieldType.INTEGER  => "i64"
        case FieldType.UNSIGNED => "u64"
        case FieldType.FLOAT    => "f64"
        case FieldType.IP_ADDR  => "ip"
        case _                  => "str"
      }

      // Create range bounds
      val lowerBound = minValue match {
        case Some(value) =>
          val valueStr = formatValueForRangeQuery(value)
          if (rangeInfo.minInclusive) {
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(valueStr)
          } else {
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(valueStr)
          }
        case None => io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded()
      }

      val upperBound = maxValue match {
        case Some(value) =>
          val valueStr = formatValueForRangeQuery(value)
          if (rangeInfo.maxInclusive) {
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(valueStr)
          } else {
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(valueStr)
          }
        case None => io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded()
      }

      val rangeQuery =
        new io.indextables.tantivy4java.split.SplitRangeQuery(field, lowerBound, upperBound, tantivyFieldType)
      queryLog(s"Creating optimized SplitRangeQuery: $rangeQuery")
      Some(rangeQuery)
    } catch {
      case e: Exception =>
        queryLog(s"Failed to create optimized range query for field '$field': ${e.getMessage}")
        None
    }

  /**
   * Safely execute a function with a Schema copy to avoid Arc reference counting issues. This creates an independent
   * Schema copy that can be used safely even if the original is closed.
   */
  private def withSchemaCopy[T](splitSearchEngine: SplitSearchEngine)(f: Schema => T): T = {
    val originalSchema = splitSearchEngine.getSchema()
    try {
      val schemaCopy = originalSchema.copy()
      try
        f(schemaCopy)
      finally
        schemaCopy.close()
    } finally
      // CRITICAL: Close the original schema to prevent native memory leak
      // Schema.getSchema() returns a NEW Schema object each time with its own native pointer
      originalSchema.close()
  }

  /**
   * Convert Spark filters to a tantivy4java SplitQuery object.
   *
   * @param filters
   *   Array of Spark filters to convert
   * @param splitSearchEngine
   *   The split search engine for query conversion
   * @param schemaFieldNames
   *   Optional set of valid field names for schema validation
   * @param options
   *   Optional configuration options for field type resolution
   * @return
   *   A SplitQuery object representing the combined filters
   */
  def convertToSplitQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]] = None,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] = None
  ): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery() // Match-all query using object type
    }

    // Debug logging to understand what filters we receive
    queryLog(s"FiltersToQueryConverter received ${filters.length} filters for SplitQuery conversion:")
    filters.zipWithIndex.foreach {
      case (filter, idx) =>
        queryLog(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
    }

    // Optimize range filters before conversion
    val rangeOptimized = optimizeRangeFilters(filters)

    // Remove redundant IsNotNull filters: when a value-bearing filter (EqualTo, GreaterThan, In,
    // etc.) references the same attribute, IsNotNull is semantically meaningless — a document
    // matching a value filter inherently has a non-null value for that attribute.
    val optimizedFilters = removeRedundantIsNotNull(rangeOptimized)

    // Filter out filters that reference non-existent fields
    val validFilters = schemaFieldNames match {
      case Some(fieldNames) =>
        queryLog(s"Schema validation enabled with fields: ${fieldNames.mkString(", ")}")
        val valid = optimizedFilters.filter(filter => isFilterValidForSchema(filter, fieldNames))
        queryLog(s"Schema validation results: ${valid.length}/${optimizedFilters.length} filters passed validation")
        valid
      case None =>
        queryLog("No schema validation - using all filters")
        optimizedFilters // No schema validation if fieldNames not provided
    }

    if (validFilters.length < optimizedFilters.length) {
      val skippedCount = optimizedFilters.length - validFilters.length
      logger.info(s"Schema validation: Skipped $skippedCount filters due to field validation")
    }

    // Convert filters to SplitQuery objects using safe schema copy
    val splitQueries = withSchemaCopy(splitSearchEngine) { schema =>
      validFilters.flatMap(filter => convertFilterToSplitQuery(filter, schema, splitSearchEngine, options)).toList
    }

    // Add optimized range queries for fields that had multiple range conditions
    val optimizedRangeQueries = withSchemaCopy(splitSearchEngine) { schema =>
      rangeOptimizations.get().toList.flatMap {
        case (field, rangeInfo) =>
          createOptimizedRangeQuery(field, rangeInfo, schema, splitSearchEngine).toList
      }
    }

    val allQueries = splitQueries ++ optimizedRangeQueries

    if (allQueries.isEmpty) {
      new SplitMatchAllQuery()
    } else if (allQueries.length == 1) {
      allQueries.head
    } else {
      // Combine multiple queries with AND logic using SplitBooleanQuery
      val boolQuery = new SplitBooleanQuery()
      allQueries.foreach(query => boolQuery.addMust(query))
      boolQuery
    }
  }

  /**
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java SplitQuery object.
   *
   * @param filters
   *   Array of mixed filters (Spark Filter or custom filters) to convert
   * @param splitSearchEngine
   *   The split search engine for query conversion
   * @param schemaFieldNames
   *   Optional set of valid field names for schema validation
   * @return
   *   A SplitQuery object representing the combined filters
   */
  def convertToSplitQuery(
    filters: Array[Any],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]],
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery() // Match-all query using object type
    }

    // Debug logging to understand what filters we receive
    queryLog(s"FiltersToQueryConverter received ${filters.length} mixed filters for SplitQuery conversion:")
    filters.zipWithIndex.foreach {
      case (filter, idx) =>
        queryLog(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
    }

    // Separate Spark filters from custom filters
    // This ensures Spark filters go through the Array[Filter] version which has proper JSON field handling
    val sparkFilters  = filters.collect { case f: Filter => f }
    val customFilters = filters.filterNot(_.isInstanceOf[Filter])

    // Use existing method for Spark filters with options support
    val sparkQuery = if (sparkFilters.nonEmpty) {
      convertToSplitQuery(sparkFilters, splitSearchEngine, schemaFieldNames, options)
    } else {
      new SplitMatchAllQuery()
    }

    // Process custom filters (IndexQuery, etc.) through the mixed filter path
    if (customFilters.nonEmpty) {
      // Filter out custom filters that reference non-existent fields
      val validCustomFilters = schemaFieldNames match {
        case Some(fieldNames) =>
          customFilters.filter(filter => isMixedFilterValidForSchema(filter, fieldNames))
        case None =>
          customFilters
      }

      if (validCustomFilters.nonEmpty) {
        val customQueries = withSchemaCopy(splitSearchEngine) { schema =>
          validCustomFilters.flatMap(filter => convertMixedFilterToSplitQuery(filter, splitSearchEngine, schema, options))
        }

        // Combine both queries if we have both types
        if (sparkFilters.nonEmpty && customQueries.nonEmpty) {
          val combinedQuery = new SplitBooleanQuery()
          combinedQuery.addMust(sparkQuery)
          customQueries.foreach(q => combinedQuery.addMust(q))
          combinedQuery
        } else if (customQueries.nonEmpty) {
          if (customQueries.length == 1) {
            customQueries.head
          } else {
            val boolQuery = new SplitBooleanQuery()
            customQueries.foreach(q => boolQuery.addMust(q))
            boolQuery
          }
        } else {
          sparkQuery
        }
      } else {
        sparkQuery
      }
    } else {
      sparkQuery
    }
  }

  /**
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java Query object.
   *
   * @param filters
   *   Array of mixed filters (Spark Filter or custom filters) to convert
   * @param splitSearchEngine
   *   The split search engine for query conversion
   * @param schemaFieldNames
   *   Optional set of valid field names for schema validation
   * @return
   *   A Query object representing the combined filters
   */
  def convertToQuery(
    filters: Array[Any],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]]
  ): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Debug logging to understand what filters we receive
    queryLog(s"FiltersToQueryConverter received ${filters.length} mixed filters:")
    filters.zipWithIndex.foreach {
      case (filter, idx) =>
        queryLog(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
    }

    // Filter out filters that reference non-existent fields
    val validFilters = schemaFieldNames match {
      case Some(fieldNames) =>
        queryLog(s"Schema validation enabled with fields: ${fieldNames.mkString(", ")}")
        val valid = filters.filter(filter => isMixedFilterValidForSchema(filter, fieldNames))
        queryLog(s"Schema validation results: ${valid.length}/${filters.length} filters passed validation")
        valid
      case None =>
        queryLog("No schema validation - using all filters")
        filters // No schema validation if fieldNames not provided
    }

    if (validFilters.length < filters.length) {
      val skippedCount = filters.length - validFilters.length
      logger.info(s"Schema validation: Skipped $skippedCount filters due to field validation")
    }

    val queries = withSchemaCopy(splitSearchEngine) { schema =>
      validFilters
        .flatMap(filter => Option(convertMixedFilterToQuery(filter, splitSearchEngine, schema)))
        .filter(_ != null)
    }

    if (queries.isEmpty) {
      Query.allQuery()
    } else if (queries.length == 1) {
      queries.head
    } else {
      // Combine multiple queries with AND logic
      val occurQueries = queries.map(query => new Query.OccurQuery(Occur.MUST, query)).toList
      Query.booleanQuery(occurQueries.asJava)
    }
  }

  /**
   * Convert Spark filters to a tantivy4java Query object.
   *
   * @param filters
   *   Array of Spark filters to convert
   * @param splitSearchEngine
   *   The split search engine for query conversion
   * @param schemaFieldNames
   *   Optional set of valid field names for schema validation
   * @return
   *   A Query object representing the combined filters
   */
  def convertToQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]] = None
  ): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Debug logging to understand what filters we receive
    queryLog(s"FiltersToQueryConverter received ${filters.length} filters:")
    filters.zipWithIndex.foreach {
      case (filter, idx) =>
        queryLog(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
    }

    // Filter out filters that reference non-existent fields
    val validFilters = schemaFieldNames match {
      case Some(fieldNames) =>
        queryLog(s"Schema validation enabled with fields: ${fieldNames.mkString(", ")}")
        val valid = filters.filter(filter => isFilterValidForSchema(filter, fieldNames))
        queryLog(s"Schema validation results: ${valid.length}/${filters.length} filters passed validation")
        valid
      case None =>
        queryLog("No schema validation - using all filters")
        filters // No schema validation if fieldNames not provided
    }

    if (validFilters.length < filters.length) {
      val skippedCount = filters.length - validFilters.length
      logger.info(s"Schema validation: Skipped $skippedCount filters due to field validation")
    }

    val queries = withSchemaCopy(splitSearchEngine) { schema =>
      validFilters.flatMap(filter => Option(convertFilterToQuery(filter, splitSearchEngine, schema))).filter(_ != null)
    }

    if (queries.isEmpty) {
      Query.allQuery()
    } else if (queries.length == 1) {
      queries.head
    } else {
      // Combine multiple queries with AND logic
      val occurQueries = queries.map(query => new Query.OccurQuery(Occur.MUST, query)).toList
      Query.booleanQuery(occurQueries.asJava)
    }
  }

  /** Test-only method for backward compatibility with debug tests. Uses direct Query methods for simple testing. */
  def convertToQuery(filters: Array[Filter], schema: Schema): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    val queries = filters.flatMap(filter => Option(convertFilterToQuerySimple(filter, schema))).filter(_ != null)

    if (queries.isEmpty) {
      Query.allQuery()
    } else if (queries.length == 1) {
      queries.head
    } else {
      // Combine multiple queries with AND logic
      val occurQueries = queries.map(query => new Query.OccurQuery(Occur.MUST, query)).toList
      Query.booleanQuery(occurQueries.asJava)
    }
  }

  /** Simple filter conversion for tests - uses direct Query methods without parseQuery. */
  private def convertFilterToQuerySimple(filter: Filter, schema: Schema): Query =
    filter match {
      case EqualTo(attribute, value) =>
        val fieldType = getFieldType(schema, attribute)
        if (fieldType == FieldType.TEXT) {
          import scala.jdk.CollectionConverters._
          val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
          Query.phraseQuery(schema, attribute, words)
        } else if (isNumericFieldType(fieldType)) {
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.termQuery(schema, attribute, convertedValue)
        } else {
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.termQuery(schema, attribute, convertedValue)
        }
      case IsNotNull(attribute) =>
        // Use wildcard query to match only documents where this field has an indexed value.
        // Tantivy only indexes non-null values, so fieldname:* matches all docs with non-null values.
        // Use lenient mode (true) to avoid errors for fields that might not exist in schema.
        Query.wildcardQuery(schema, attribute, "*", true)
      case _ =>
        Query.allQuery() // Simplified for tests
    }

  private def isFilterValidForSchema(filter: Filter, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._

    def getFilterFieldNames(f: Filter): Set[String] = f match {
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
      case And(left, right)                 => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Or(left, right)                  => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Not(child)                       => getFilterFieldNames(child)
      case _                                => Set.empty
    }

    val filterFields = getFilterFieldNames(filter)
    // For nested JSON fields (e.g., "user.name"), check if the root field exists (e.g., "user")
    val isValid = filterFields.forall { fieldName =>
      if (fieldName.contains(".")) {
        // Nested field: check if root field exists in schema
        val rootField = fieldName.split("\\.", 2)(0)
        fieldNames.contains(rootField)
      } else {
        // Top-level field: check directly
        fieldNames.contains(fieldName)
      }
    }

    if (!isValid) {
      val missingFields = filterFields.filterNot { fieldName =>
        if (fieldName.contains(".")) {
          val rootField = fieldName.split("\\.", 2)(0)
          fieldNames.contains(rootField)
        } else {
          fieldNames.contains(fieldName)
        }
      }
      queryLog(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }

    isValid
  }

  private def queryLog(msg: String): Unit =
    logger.debug(msg)

  /**
   * Count the number of unique fields that would be searched by a query. Uses tantivy4java's
   * SplitQuery.countQueryFields() for accurate AST-based analysis.
   *
   * @param queryString
   *   The Tantivy query string to analyze
   * @param schema
   *   The Tantivy schema for field resolution
   * @return
   *   The number of unique fields the query would search
   */
  def countQueryFields(queryString: String, schema: Schema): Int = {
    import io.indextables.tantivy4java.split.SplitQuery
    try
      SplitQuery.countQueryFields(queryString, schema)
    catch {
      case e: Exception =>
        logger.warn(s"Failed to count query fields for '$queryString': ${e.getMessage}")
        // On error, return schema field count as conservative estimate (assumes all fields searched)
        schema.getFieldNames.size()
    }
  }

  /**
   * Validate that an _indexall query is safe to execute on the given schema. Uses tantivy4java's accurate field
   * counting to determine how many fields would actually be searched. Throws IllegalArgumentException if the query
   * would search more fields than allowed.
   *
   * @param queryString
   *   The query string from the IndexQueryAll filter
   * @param schema
   *   The Tantivy schema with field information
   * @param options
   *   Optional configuration options containing the maxUnqualifiedFields setting
   */
  private def validateIndexQueryAllSafety(
    queryString: String,
    schema: Schema,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): Unit = {
    val maxFields = options
      .map(
        _.getInt(
          IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS,
          IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS_DEFAULT
        )
      )
      .getOrElse(IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS_DEFAULT)

    // Skip validation if disabled (maxFields <= 0)
    if (maxFields <= 0) return

    // Use tantivy4java's accurate field counting
    val searchedFieldCount = countQueryFields(queryString, schema)

    if (searchedFieldCount > maxFields) {
      val fieldSample = schema.getFieldNames.asScala.take(10).mkString(", ")
      val totalFields = schema.getFieldNames.size()
      val moreFields  = if (totalFields > 10) s" ... and ${totalFields - 10} more" else ""

      throw new IllegalArgumentException(
        s"""_indexall query would search $searchedFieldCount fields (limit: $maxFields).
           |
           |Query: _indexall indexquery '$queryString'
           |
           |To fix, qualify your search with specific field names:
           |  _indexall indexquery 'field1:$queryString'
           |  _indexall indexquery 'field1:term OR field2:term'
           |  _indexall indexquery 'title:"exact phrase"'
           |
           |Or increase the limit (if searching many fields is intentional):
           |  spark.conf.set("${IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS}", "$searchedFieldCount")
           |
           |Available fields: $fieldSample$moreFields""".stripMargin
      )
    }
  }

  /**
   * Thread-local cache for temporary index directory to avoid repeated directory creation. Each thread gets its own
   * temp directory that is reused across multiple parseQuery calls. The directory is cleaned up when the thread
   * terminates or on JVM shutdown.
   */
  private val tempIndexDir: ThreadLocal[java.nio.file.Path] = ThreadLocal.withInitial { () =>
    import java.nio.file.Files
    val tempDir = Files.createTempDirectory("tantivy4spark_parsequery_")

    // Register cleanup on JVM shutdown
    Runtime.getRuntime.addShutdownHook(new Thread(() => cleanupTempDir(tempDir)))

    tempDir
  }

  /** Clean up a temp directory and all its contents. */
  private def cleanupTempDir(tempDir: java.nio.file.Path): Unit =
    try {
      import java.nio.file.{Path, Files}
      import java.nio.file.attribute.BasicFileAttributes
      import java.nio.file.SimpleFileVisitor
      import java.nio.file.FileVisitResult

      if (Files.exists(tempDir)) {
        Files.walkFileTree(
          tempDir,
          new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
              Files.delete(file)
              FileVisitResult.CONTINUE
            }
            override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
              Files.delete(dir)
              FileVisitResult.CONTINUE
            }
          }
        )
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to clean up temp directory $tempDir: ${e.getMessage}")
    }

  /**
   * Create a temporary index from schema for parseQuery operations. Uses thread-local cached temp directory to avoid
   * repeated directory creation overhead.
   */
  private def withTemporaryIndex[T](schema: Schema)(f: Index => T): T = {
    val tempDir = tempIndexDir.get()
    // Create a unique subdirectory for this index to avoid conflicts
    val indexDir = java.nio.file.Files.createTempDirectory(tempDir, "idx_")
    try {
      val tempIndex = new Index(schema, indexDir.toAbsolutePath.toString)
      try
        f(tempIndex)
      finally
        tempIndex.close()
    } finally
      // Clean up this specific index subdirectory
      cleanupTempDir(indexDir)
  }

  /** Convert a single Spark Filter to a tantivy4java Query. */
  private def convertFilterToQuery(
    filter: Filter,
    splitSearchEngine: SplitSearchEngine,
    schema: Schema,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] = None
  ): Query = {
    try {
      filter match {
        case EqualTo(attribute, value) =>
          queryLog(s"Creating EqualTo query: $attribute = $value")
          val fieldType = getFieldType(schema, attribute)
          queryLog(s"Field '$attribute' has type: $fieldType, isNumeric: ${isNumericFieldType(fieldType)}")
          val query = if (fieldType == FieldType.TEXT) {
            // For TEXT fields, use Index.parseQuery for reliable text matching
            queryLog(s"Field '$attribute' is TEXT, using Index.parseQuery for text matching")
            val queryString = s""""${value.toString}""""
            val fieldNames  = List(attribute).asJava
            queryLog(s"Parsing query: $queryString on fields: [$attribute]")
            withTemporaryIndex(schema)(index => index.parseQuery(queryString, fieldNames))
          } else if (isNumericFieldType(fieldType)) {
            // For numeric fields (INTEGER, FLOAT, DATE), use term query for equality
            queryLog(s"Field '$attribute' is numeric $fieldType, using termQuery for equality")
            val convertedValue = convertSparkValueToTantivy(value, fieldType)
            logger.info(s"Creating term query for numeric equality: field='$attribute', fieldType=$fieldType, value=$convertedValue")
            val result = Query.termQuery(schema, attribute, convertedValue)
            queryLog(s"Successfully created term query: ${result.getClass.getSimpleName}")
            result
          } else {
            // For other non-TEXT fields (BOOLEAN, BYTES), use Index.parseQuery with field specification
            queryLog(s"Field '$attribute' is $fieldType, using Index.parseQuery")
            val convertedValue = convertSparkValueToTantivy(value, fieldType)
            logger.info(s"Using Index.parseQuery for: field='$attribute', value=$convertedValue (${convertedValue.getClass.getSimpleName})")
            val queryString = convertedValue.toString
            val fieldNames  = List(attribute).asJava
            queryLog(s"Parsing query: $queryString on fields: [$attribute]")
            withTemporaryIndex(schema)(index => index.parseQuery(queryString, fieldNames))
          }
          queryLog(s"Created Query: ${query.getClass.getSimpleName} for field '$attribute' with value '$value'")
          query

        case EqualNullSafe(attribute, value) =>
          if (value == null) {
            queryLog(s"Creating EqualNullSafe query for null: $attribute IS NULL")
            // For null values, we could return a query that matches no documents
            // or handle this differently based on requirements
            Query.allQuery() // TODO: Implement proper null handling
          } else {
            queryLog(s"Creating EqualNullSafe query: $attribute = $value")
            val fieldType = getFieldType(schema, attribute)
            if (isNumericFieldType(fieldType)) {
              // For numeric fields, use term query for equality
              queryLog(s"Field '$attribute' is numeric $fieldType, using termQuery for equality")
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              Query.termQuery(schema, attribute, convertedValue)
            } else {
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              val queryString    = convertedValue.toString
              val fieldNames     = List(attribute).asJava
              withTemporaryIndex(schema)(index => index.parseQuery(queryString, fieldNames))
            }
          }

        case GreaterThan(attribute, value) =>
          queryLog(s"Creating GreaterThan query: $attribute > $value")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.UNSIGNED) {
            throw new IllegalArgumentException(
              s"Range query on field '$attribute' is not supported: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Use EqualTo for exact matching instead."
            )
          }
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          logger.info(s"Creating GreaterThan range query: field='$attribute', fieldType=$fieldType, min=$convertedValue (exclusive)")
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, false, true)

        case GreaterThanOrEqual(attribute, value) =>
          queryLog(s"Creating GreaterThanOrEqual query: $attribute >= $value")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.UNSIGNED) {
            throw new IllegalArgumentException(
              s"Range query on field '$attribute' is not supported: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Use EqualTo for exact matching instead."
            )
          }
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, true, true)

        case LessThan(attribute, value) =>
          queryLog(s"Creating LessThan query: $attribute < $value")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.UNSIGNED) {
            throw new IllegalArgumentException(
              s"Range query on field '$attribute' is not supported: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Use EqualTo for exact matching instead."
            )
          }
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, false)

        case LessThanOrEqual(attribute, value) =>
          queryLog(s"Creating LessThanOrEqual query: $attribute <= $value")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.UNSIGNED) {
            throw new IllegalArgumentException(
              s"Range query on field '$attribute' is not supported: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Use EqualTo for exact matching instead."
            )
          }
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, true)

        case In(attribute, values) =>
          queryLog(s"Creating In query: $attribute IN [${values.mkString(", ")}]")
          val fieldType = getFieldType(schema, attribute)
          if (shouldUseTokenizedQuery(attribute, options)) {
            // For TEXT fields with default tokenizer, create OR query with Index.parseQuery for each value
            queryLog(s"Field '$attribute' uses tokenized search, using OR of Index.parseQuery for IN query")
            val parseQueries = values.map { value =>
              val queryString = s""""${value.toString}""""
              val fieldNames  = List(attribute).asJava
              withTemporaryIndex(schema)(index => index.parseQuery(queryString, fieldNames))
            }
            val occurQueries = parseQueries.map(query => new Query.OccurQuery(Occur.SHOULD, query)).toList
            Query.booleanQuery(occurQueries.asJava)
          } else if (isNumericFieldType(fieldType)) {
            // For numeric fields, create OR query with term queries for each value
            queryLog(s"Field '$attribute' is numeric $fieldType, using OR of termQueries for IN query")
            val termQueries = values.map { value =>
              val convertedValue = convertSparkValueToTantivy(value, fieldType)
              Query.termQuery(schema, attribute, convertedValue)
            }
            val occurQueries = termQueries.map(query => new Query.OccurQuery(Occur.SHOULD, query)).toList
            Query.booleanQuery(occurQueries.asJava)
          } else {
            // For STRING fields (raw tokenizer) and other non-tokenized fields, use term set query with converted values
            queryLog(s"Field '$attribute' uses exact matching ($fieldType), using termSetQuery")
            val convertedValues = values.map(value => convertSparkValueToTantivy(value, fieldType))
            if (fieldType == FieldType.BOOLEAN) {
              queryLog(s"Boolean IN query - converted values: ${convertedValues.mkString("[", ", ", "]")}")
              convertedValues.foreach(v => queryLog(s"  Value: $v (${v.getClass.getSimpleName})"))
            }
            val valuesList = convertedValues.toList.asJava.asInstanceOf[java.util.List[Object]]
            Query.termSetQuery(schema, attribute, valuesList)
          }

        case IsNull(attribute) =>
          queryLog(s"Creating IsNull query: $attribute IS NULL")
          // IS NULL = All documents EXCEPT those where field exists
          // Pattern: MUST(AllQuery) + MUST_NOT(ExistsQuery)
          // Requires FAST field - validated on driver side
          val existsQuery = Query.existsQuery(schema, attribute)
          val occurQueries = List(
            new Query.OccurQuery(Occur.MUST, Query.allQuery()),
            new Query.OccurQuery(Occur.MUST_NOT, existsQuery)
          )
          Query.booleanQuery(occurQueries.asJava)

        case IsNotNull(attribute) =>
          queryLog(s"Creating IsNotNull query: $attribute IS NOT NULL")
          // Use ExistsQuery to match documents where field has an indexed value
          // Requires FAST field - validated on driver side
          Query.existsQuery(schema, attribute)

        case And(left, right) =>
          queryLog(s"Creating And query: $left AND $right")
          val leftQuery  = convertFilterToQuery(left, splitSearchEngine, schema, options)
          val rightQuery = convertFilterToQuery(right, splitSearchEngine, schema, options)
          val occurQueries = List(
            new Query.OccurQuery(Occur.MUST, leftQuery),
            new Query.OccurQuery(Occur.MUST, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)

        case Or(left, right) =>
          queryLog(s"Creating Or query: $left OR $right")
          val leftQuery  = convertFilterToQuery(left, splitSearchEngine, schema, options)
          val rightQuery = convertFilterToQuery(right, splitSearchEngine, schema, options)
          val occurQueries = List(
            new Query.OccurQuery(Occur.SHOULD, leftQuery),
            new Query.OccurQuery(Occur.SHOULD, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)

        case Not(child) =>
          queryLog(s"Creating Not query: NOT $child")
          val childQuery = convertFilterToQuery(child, splitSearchEngine, schema, options)
          // For NOT queries, we need both MUST (match all) and MUST_NOT (exclude) clauses
          val allQuery = Query.allQuery()
          val occurQueries = java.util.Arrays.asList(
            new Query.OccurQuery(Occur.MUST, allQuery),
            new Query.OccurQuery(Occur.MUST_NOT, childQuery)
          )
          Query.booleanQuery(occurQueries)

        case StringStartsWith(attribute, value) =>
          queryLog(s"Creating StringStartsWith query: $attribute starts with '$value'")
          // For TEXT fields (default tokenizer), use parseQuery which handles prefix at token level.
          // For STRING fields (raw tokenizer), use regexQuery for exact prefix matching.
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            queryLog(s"StringStartsWith on TEXT field: using parseQuery for '$value*'")
            val pattern    = value + "*"
            val fieldNames = List(attribute).asJava
            withTemporaryIndex(schema)(index => index.parseQuery(pattern, fieldNames))
          } else {
            // Escape special regex chars and build prefix regex pattern
            val escapedValue = java.util.regex.Pattern.quote(value)
            val regexPattern = escapedValue + ".*"
            queryLog(s"StringStartsWith on STRING field: using regexQuery for '$regexPattern'")
            Query.regexQuery(schema, attribute, regexPattern)
          }

        case StringEndsWith(attribute, value) =>
          queryLog(s"Creating StringEndsWith query: $attribute ends with '$value'")
          // For TEXT fields (default tokenizer), use parseQuery which handles suffix at token level.
          // For STRING fields (raw tokenizer), use regexQuery for exact suffix matching.
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            queryLog(s"StringEndsWith on TEXT field: using parseQuery for '*$value'")
            val pattern    = "*" + value
            val fieldNames = List(attribute).asJava
            withTemporaryIndex(schema)(index => index.parseQuery(pattern, fieldNames))
          } else {
            // Escape special regex chars and build suffix regex pattern
            val escapedValue = java.util.regex.Pattern.quote(value)
            val regexPattern = ".*" + escapedValue
            queryLog(s"StringEndsWith on STRING field: using regexQuery for '$regexPattern'")
            Query.regexQuery(schema, attribute, regexPattern)
          }

        case StringContains(attribute, value) =>
          queryLog(s"Creating StringContains query: $attribute contains '$value'")
          // For TEXT fields (default tokenizer), use a plain terms query since
          // tokenization will handle the matching. For STRING fields (raw tokenizer),
          // use a regex query with .*value.*.
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            queryLog(s"StringContains on TEXT field: using phrase query for '$value'")
            val words = List(value.toString).asJava.asInstanceOf[java.util.List[Object]]
            Query.phraseQuery(schema, attribute, words)
          } else {
            // Escape special regex chars and build contains regex pattern
            val escapedValue = java.util.regex.Pattern.quote(value)
            val regexPattern = ".*" + escapedValue + ".*"
            queryLog(s"StringContains on STRING field: using regexQuery for '$regexPattern'")
            Query.regexQuery(schema, attribute, regexPattern)
          }

        case _ =>
          logger.warn(s"Unsupported filter: $filter, falling back to match-all")
          Query.allQuery()
      }
    } catch {
      case e: RuntimeException
          if e.getMessage != null && (e.getMessage
            .contains("Schema is closed") || e.getMessage.contains("Schema is invalid")) =>
        logger.warn(s"Cannot convert filter $filter - schema unavailable: ${e.getMessage}")
        Query.allQuery() // Fallback to match-all query when schema is closed
      case e: Exception =>
        logger.error(s"Failed to convert filter $filter to Query: ${e.getMessage}", e)
        Query.allQuery() // Fallback to match-all query
    }
  }

  /**
   * Check if a mixed filter (Spark Filter or custom filter) is valid for the schema.
   *
   * Returns false for filters that reference non-existent fields, allowing graceful degradation. Driver-side validation
   * in IndexTables4SparkScanBuilder.validateIndexQueryFieldsExist() handles throwing descriptive errors for IndexQuery
   * filters with invalid fields BEFORE tasks are created. This executor-side check is a safety net that should never be
   * hit if driver validation ran.
   */
  private def isMixedFilterValidForSchema(filter: Any, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._
    import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    def getMixedFilterFieldNames(f: Any): Set[String] = f match {
      // Handle standard Spark filters
      case sparkFilter: Filter =>
        sparkFilter match {
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
          case And(left, right)                 => getMixedFilterFieldNames(left) ++ getMixedFilterFieldNames(right)
          case Or(left, right)                  => getMixedFilterFieldNames(left) ++ getMixedFilterFieldNames(right)
          case Not(child)                       => getMixedFilterFieldNames(child)
          case _                                => Set.empty
        }
      // Handle custom filters
      case indexQuery: IndexQueryFilter       => Set(indexQuery.columnName)
      case indexQueryAll: IndexQueryAllFilter => Set.empty // No specific field references
      // Handle V2 filters
      case indexQueryV2: io.indextables.spark.filters.IndexQueryV2Filter => Set(indexQueryV2.columnName)
      case _: io.indextables.spark.filters.IndexQueryAllV2Filter         => Set.empty // No specific field references
      // Handle MixedBooleanFilter types
      case mixedFilter: io.indextables.spark.filters.MixedBooleanFilter => mixedFilter.references().toSet
      case _                                                            => Set.empty
    }

    val filterFields = getMixedFilterFieldNames(filter)
    val isValid      = filterFields.subsetOf(fieldNames)

    if (!isValid) {
      val missingFields = filterFields -- fieldNames
      // Log warning - driver-side validation should have caught this before tasks were created
      // If we reach here, it indicates either:
      // 1. A code path that bypasses driver validation (bug)
      // 2. Schema mismatch between driver and executor (configuration issue)
      logger.warn(
        s"Executor-side filter validation: filter $filter references non-existent fields: ${missingFields.mkString(", ")}. " +
          s"This should have been caught by driver-side validation."
      )
      queryLog(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }

    isValid
  }

  /** Convert a mixed filter (Spark Filter or custom filter) to a Query object. */
  private def convertMixedFilterToQuery(
    filter: Any,
    splitSearchEngine: SplitSearchEngine,
    schema: Schema
  ): Query = {
    import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    filter match {
      // Handle standard Spark filters
      case sparkFilter: Filter =>
        convertFilterToQuery(sparkFilter, splitSearchEngine, schema)

      // Handle custom IndexQuery filters
      // Note: Field validation happens earlier in isMixedFilterValidForSchema
      case indexQuery: IndexQueryFilter =>
        queryLog(s"Converting custom IndexQueryFilter: ${indexQuery.columnName} indexquery '${indexQuery.queryString}'")

        // Use parseQuery with the specified field
        val fieldNames = List(indexQuery.columnName).asJava
        queryLog(s"Executing parseQuery: '${indexQuery.queryString}' on field '${indexQuery.columnName}'")

        withTemporaryIndex(schema) { index =>
          try
            index.parseQuery(indexQuery.queryString, fieldNames)
          catch {
            case e: Exception =>
              // Throw descriptive exception instead of silently falling back to match-all
              throw IndexQueryParseException.forField(indexQuery.queryString, indexQuery.columnName, e)
          }
        }

      // Handle custom IndexQueryAll filters
      case indexQueryAll: IndexQueryAllFilter =>
        // Safety check for unqualified queries on wide tables
        validateIndexQueryAllSafety(indexQueryAll.queryString, schema, None)

        queryLog(s"Converting custom IndexQueryAllFilter: indexqueryall('${indexQueryAll.queryString}')")

        // Use single-argument parseQuery for all-fields search
        queryLog(s"Executing parseQuery across all fields: '${indexQueryAll.queryString}'")

        withTemporaryIndex(schema) { index =>
          try
            index.parseQuery(indexQueryAll.queryString)
          catch {
            case e: Exception =>
              // Throw descriptive exception instead of silently falling back to match-all
              throw IndexQueryParseException.forAllFields(indexQueryAll.queryString, e)
          }
        }

      // Handle MixedBooleanFilter types - preserves OR/AND/NOT structure from IndexQuery expressions
      case mixedFilter: io.indextables.spark.filters.MixedBooleanFilter =>
        convertMixedBooleanFilterToQuery(mixedFilter, splitSearchEngine, schema, None)

      case _ =>
        logger.warn(s"Unsupported mixed filter: $filter (${filter.getClass.getSimpleName}), falling back to match-all")
        Query.allQuery()
    }
  }

  /**
   * Convert a MixedBooleanFilter tree to a Query. Recursively handles Or, And, Not combinations of IndexQuery and Spark
   * filters. This preserves the original boolean structure instead of flattening to AND.
   */
  private def convertMixedBooleanFilterToQuery(
    filter: io.indextables.spark.filters.MixedBooleanFilter,
    splitSearchEngine: SplitSearchEngine,
    schema: Schema,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): Query = {
    import io.indextables.spark.filters._

    filter match {
      case MixedIndexQuery(indexQueryFilter) =>
        queryLog(s"MixedBooleanFilter->Query: Converting MixedIndexQuery: ${indexQueryFilter.columnName} indexquery '${indexQueryFilter.queryString}'")
        val fieldNames = List(indexQueryFilter.columnName).asJava
        withTemporaryIndex(schema) { index =>
          try
            index.parseQuery(indexQueryFilter.queryString, fieldNames)
          catch {
            case e: Exception =>
              logger.warn(s"Failed to parse indexquery '${indexQueryFilter.queryString}': ${e.getMessage}")
              Query.allQuery()
          }
        }

      case MixedIndexQueryAll(indexQueryAllFilter) =>
        // Safety check for unqualified queries on wide tables
        validateIndexQueryAllSafety(indexQueryAllFilter.queryString, schema, options)

        queryLog(s"MixedBooleanFilter->Query: Converting MixedIndexQueryAll: '${indexQueryAllFilter.queryString}'")
        withTemporaryIndex(schema) { index =>
          try
            index.parseQuery(indexQueryAllFilter.queryString)
          catch {
            case e: Exception =>
              logger.warn(s"Failed to parse indexqueryall '${indexQueryAllFilter.queryString}': ${e.getMessage}")
              Query.allQuery()
          }
        }

      case MixedSparkFilter(sparkFilter) =>
        queryLog(s"MixedBooleanFilter->Query: Converting MixedSparkFilter: $sparkFilter")
        convertFilterToQuery(sparkFilter, splitSearchEngine, schema)

      case MixedOrFilter(left, right) =>
        queryLog(s"MixedBooleanFilter->Query: Converting MixedOrFilter - combining with SHOULD (OR)")
        val leftQuery  = convertMixedBooleanFilterToQuery(left, splitSearchEngine, schema, options)
        val rightQuery = convertMixedBooleanFilterToQuery(right, splitSearchEngine, schema, options)
        val occurQueries = List(
          new Query.OccurQuery(Occur.SHOULD, leftQuery),
          new Query.OccurQuery(Occur.SHOULD, rightQuery)
        )
        Query.booleanQuery(occurQueries.asJava)

      case MixedAndFilter(left, right) =>
        queryLog(s"MixedBooleanFilter->Query: Converting MixedAndFilter - combining with MUST (AND)")
        val leftQuery  = convertMixedBooleanFilterToQuery(left, splitSearchEngine, schema, options)
        val rightQuery = convertMixedBooleanFilterToQuery(right, splitSearchEngine, schema, options)
        val occurQueries = List(
          new Query.OccurQuery(Occur.MUST, leftQuery),
          new Query.OccurQuery(Occur.MUST, rightQuery)
        )
        Query.booleanQuery(occurQueries.asJava)

      case MixedNotFilter(child) =>
        queryLog(s"MixedBooleanFilter->Query: Converting MixedNotFilter - using MUST_NOT")
        val childQuery = convertMixedBooleanFilterToQuery(child, splitSearchEngine, schema, options)
        // For NOT queries, we need both MUST (match all) and MUST_NOT (exclude) clauses
        val allQuery = Query.allQuery()
        val occurQueries = java.util.Arrays.asList(
          new Query.OccurQuery(Occur.MUST, allQuery),
          new Query.OccurQuery(Occur.MUST_NOT, childQuery)
        )
        Query.booleanQuery(occurQueries)
    }
  }

  /** Get the field type from the schema for a given field name. */
  private def getFieldType(schema: Schema, fieldName: String): FieldType =
    try {
      val fieldInfo = schema.getFieldInfo(fieldName)
      fieldInfo.getType()
    } catch {
      case e: Exception =>
        logger.warn(s"Could not determine field type for '$fieldName', defaulting to TEXT: ${e.getMessage}")
        FieldType.TEXT
    }

  /** Check if a field type is numeric (should use range queries instead of term queries for equality) */
  private def isNumericFieldType(fieldType: FieldType): Boolean =
    fieldType match {
      case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE | FieldType.IP_ADDR => true
      case _                                                                        => false
    }

  /** Convert Spark values to tantivy4java compatible values for filtering */
  private def convertSparkValueToTantivy(value: Any, fieldType: FieldType): Any = {
    if (value == null) return null

    fieldType match {
      case FieldType.DATE =>
        // Convert to ISO datetime format for tantivy DATE fields
        // For timestamps, preserve full datetime; for dates, use date-only format
        // IMPORTANT: Use UTC timezone to match how data is indexed
        import java.time.{LocalDate, LocalDateTime, ZoneOffset, Instant}
        import java.time.format.DateTimeFormatter
        value match {
          case ts: java.sql.Timestamp =>
            // For timestamps, convert to UTC and use RFC3339 format: YYYY-MM-DDTHH:MM:SSZ
            // This matches how timestamps are indexed (using UTC) and tantivy's expected format
            val instant        = ts.toInstant()
            val utcDateTime    = instant.atOffset(ZoneOffset.UTC)
            val dateTimeString = utcDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            queryLog(s"DATE conversion (Timestamp): $value -> $dateTimeString (RFC3339 UTC)")
            dateTimeString
          case date: java.sql.Date =>
            // For dates, use date-only format: YYYY-MM-DD
            val dateString = date.toLocalDate().toString
            queryLog(s"DATE conversion (Date): $value -> $dateString")
            dateString
          case dateStr: String =>
            // String dates - pass through as-is
            queryLog(s"DATE conversion (String): $value -> $dateStr")
            dateStr
          case daysSinceEpoch: Int =>
            // Convert days since epoch to LocalDate
            val epochDate  = LocalDate.of(1970, 1, 1)
            val dateString = epochDate.plusDays(daysSinceEpoch.toLong).toString
            queryLog(s"DATE conversion (Days): $value -> $dateString")
            dateString
          case microseconds: Long =>
            // Spark timestamps are stored as microseconds since epoch
            // Convert to ISO datetime format
            val seconds        = microseconds / 1000000L
            val nanos          = ((microseconds % 1000000L) * 1000L).toInt
            val instant        = java.time.Instant.ofEpochSecond(seconds, nanos)
            val localDateTime  = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
            val dateTimeString = localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            queryLog(s"DATE conversion (Long microseconds): $value -> $dateTimeString")
            dateTimeString
          case other =>
            queryLog(s"DATE conversion: Unexpected type ${other.getClass.getSimpleName}, trying to parse as string")
            other.toString
        }
      case FieldType.INTEGER =>
        // Keep original types for range queries - tantivy4java handles type conversion internally
        val result = value match {
          case ts: java.sql.Timestamp => TimestampUtils.toMicros(ts)           // Convert to microseconds as Long
          case date: java.sql.Date    => date.getTime / (24 * 60 * 60 * 1000L) // Convert to days since epoch as Long
          case i: java.lang.Integer   => i                                     // Keep as Integer for range queries
          case l: java.lang.Long      => l                                     // Keep as Long
          case other                  => other
        }
        queryLog(
          s"INTEGER conversion: $value (${value.getClass.getSimpleName}) -> $result (${result.getClass.getSimpleName})"
        )
        result
      case FieldType.BOOLEAN =>
        val booleanResult = value match {
          case b: java.lang.Boolean => b.booleanValue()
          case b: Boolean           => b
          case i: java.lang.Integer => i != 0
          case l: java.lang.Long    => l != 0
          case s: String            => s.toLowerCase == "true" || s == "1"
          case other => throw new IllegalArgumentException(s"Cannot convert $other to Boolean for field type BOOLEAN")
        }
        // Ensure we return a Java Boolean object that tantivy4java expects
        val convertedValue = java.lang.Boolean.valueOf(booleanResult)
        queryLog(s"Boolean conversion: $value (${value.getClass.getSimpleName}) -> $convertedValue (${convertedValue.getClass.getSimpleName})")
        convertedValue
      case FieldType.FLOAT =>
        value match {
          case f: java.lang.Float   => f.floatValue()
          case d: java.lang.Double  => d.doubleValue()
          case i: java.lang.Integer => i.doubleValue()
          case l: java.lang.Long    => l.doubleValue()
          case s: String =>
            try s.toDouble
            catch { case _: Exception => throw new IllegalArgumentException(s"Cannot convert string '$s' to Float") }
          case other => other
        }
      case FieldType.IP_ADDR =>
        // IP addresses are passed as strings (IPv4 or IPv6 format)
        val ipStr = value.toString
        queryLog(s"IP_ADDR conversion: $value -> $ipStr")
        ipStr
      case _ =>
        // For other types (TEXT, BYTES), pass through as-is
        value
    }
  }

  /** Convert a Spark Filter to a SplitQuery object. */
  private def convertFilterToSplitQuery(
    filter: Filter,
    schema: Schema,
    splitSearchEngine: SplitSearchEngine,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] = None
  ): Option[SplitQuery] = {
    import org.apache.spark.sql.sources._

    // Check if this is a nested field filter (JSON field) first
    // Use cached JSON predicate translator to avoid repeated object creation
    // Note: sparkSchema may be null in some contexts (e.g., MixedBooleanFilter conversion)
    val sparkSchema = splitSearchEngine.getSparkSchema()
    if (sparkSchema != null) {
      val jsonTranslator = getJsonTranslator(sparkSchema, options)

      // Try to translate as JSON field filter using parseQuery syntax
      jsonTranslator.translateFilterToParseQuery(filter) match {
        case Some(queryString) =>
          logger.debug(s"JSON FILTER: Translating nested field filter to parseQuery: $queryString")
          try {
            val parsedQuery = splitSearchEngine.parseQuery(queryString)
            logger.debug(s"JSON FILTER: Successfully parsed query: $queryString")
            return Some(parsedQuery)
          } catch {
            case e: Exception =>
              logger.warn(s"JSON FILTER: Failed to parse JSON field query '$queryString': ${e.getMessage}")
            // Fall through to regular filter handling
          }
        case None =>
          logger.debug(s"JSON FILTER: Filter not recognized as nested field filter: $filter")
        // Not a nested field filter, continue with regular filter handling
      }
    } else {
      logger.debug(s"JSON FILTER: Spark schema not available, skipping JSON field translation for filter: $filter")
    }

    filter match {
      case EqualTo(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // EqualTo should always use exact matching regardless of field type
        // For text fields, use phrase query syntax with quotes to ensure exact phrase matching
        // For date fields, use range queries for proper date matching
        // For other fields, use term queries
        if (shouldUseTokenizedQuery(attribute, options)) {
          // Use parseQuery with quoted syntax for exact phrase matching on tokenized text fields
          val quotedValue = "\"" + convertedValue.toString.replace("\"", "\\\"") + "\""
          val queryString = s"$attribute:$quotedValue"
          Some(splitSearchEngine.parseQuery(queryString))
        } else if (fieldType == FieldType.DATE) {
          // For date/timestamp fields, use range query for proper datetime matching
          convertedValue match {
            case dateTimeString: String if dateTimeString.contains("T") =>
              // Full datetime (timestamp) - create a small range around the exact time
              // For timestamp equality, we need a very narrow range (1 microsecond window)
              import java.time.{OffsetDateTime, LocalDateTime, ZoneOffset}
              import java.time.format.DateTimeFormatter

              // Parse the RFC3339 datetime string (may have Z or +00:00 suffix)
              val (localDateTime, startStr) =
                if (dateTimeString.endsWith("Z") || dateTimeString.contains("+") || dateTimeString.contains("-")) {
                  // RFC3339 format with timezone - parse and extract local datetime
                  val offsetDateTime = OffsetDateTime.parse(dateTimeString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                  val ldt            = offsetDateTime.toLocalDateTime
                  (ldt, dateTimeString) // Use original RFC3339 string for query
                } else {
                  // ISO_LOCAL_DATE_TIME format without timezone
                  val ldt = LocalDateTime.parse(dateTimeString, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                  (ldt, dateTimeString)
                }

              // For equality matching, create a 1 microsecond range to match exact value
              // Add 1 nanosecond to get exclusive upper bound that still matches just this value
              val endDateTime = localDateTime.plusNanos(1000) // 1 microsecond
              val endStr = if (dateTimeString.endsWith("Z")) {
                endDateTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
              } else if (
                dateTimeString.contains("+") || (dateTimeString.contains("-") && dateTimeString.lastIndexOf("-") > 10)
              ) {
                // Has timezone offset - preserve it
                val offset = OffsetDateTime.parse(dateTimeString).getOffset
                endDateTime.atOffset(offset).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
              } else {
                endDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
              }

              val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
                attribute,
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(startStr),
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(endStr),
                "date"
              )
              queryLog(s"Creating datetime equality SplitRangeQuery: [$startStr TO $endStr)")
              Some(rangeQuery)
            case dateString: String =>
              // Date-only - create a range covering the entire day
              import java.time.LocalDate
              val localDate    = LocalDate.parse(dateString)
              val nextDay      = localDate.plusDays(1)
              val startDateStr = localDate.toString
              val endDateStr   = nextDay.toString

              val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
                attribute,
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(startDateStr),
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(endDateStr),
                "date"
              )
              queryLog(s"Creating date equality SplitRangeQuery: [$startDateStr TO $endDateStr)")
              Some(rangeQuery)
            case other =>
              queryLog(s"Unexpected date value type: ${other.getClass.getSimpleName}, falling back to term query")
              Some(new SplitTermQuery(attribute, convertedValue.toString))
          }
        } else {
          // For string fields and other types, use exact term matching
          Some(new SplitTermQuery(attribute, convertedValue.toString))
        }

      case EqualNullSafe(attribute, value) if value != null =>
        convertFilterToSplitQuery(EqualTo(attribute, value), schema, splitSearchEngine, options)

      case In(attribute, values) if values.nonEmpty =>
        val fieldType = getFieldType(schema, attribute)
        if (shouldUseTokenizedQuery(attribute, options)) {
          // For TEXT fields with default tokenizer, use parseQuery with quoted syntax for exact phrase matching
          val parseQueries = values.map { value =>
            val quotedValue = "\"" + value.toString.replace("\"", "\\\"") + "\""
            val queryString = s"$attribute:$quotedValue"
            splitSearchEngine.parseQuery(queryString)
          }.toList

          // Create boolean query with OR logic for IN clause - now works correctly with tantivy4java fix
          val boolQuery = new SplitBooleanQuery()
          parseQueries.foreach(query => boolQuery.addShould(query))
          Some(boolQuery)
        } else {
          // For STRING fields (raw tokenizer) and other non-tokenized fields, use term queries
          val termQueries = values.map { value =>
            val converted = convertSparkValueToTantivy(value, fieldType)
            new SplitTermQuery(attribute, converted.toString)
          }.toList

          // Create boolean query with OR logic for IN clause - now works correctly with tantivy4java fix
          val boolQuery = new SplitBooleanQuery()
          termQueries.foreach(query => boolQuery.addShould(query))
          Some(boolQuery)
        }

      case IsNotNull(attribute) =>
        val fieldType = getFieldType(schema, attribute)
        queryLog(s"Creating IsNotNull query for '$attribute' (fieldType=$fieldType)")
        if (fieldType == FieldType.TEXT || fieldType == FieldType.JSON) {
          // TEXT/JSON fields: wildcard doesn't work, use SplitExistsQuery (requires FAST field).
          // Driver-side isSupportedFilter already verified the field is fast.
          queryLog(s"Creating IsNotNull SplitExistsQuery for TEXT/JSON field: $attribute IS NOT NULL")
          Some(new SplitExistsQuery(attribute))
        } else {
          try
            Some(splitSearchEngine.parseQuery(s"$attribute:*"))
          catch {
            case e: Exception =>
              queryLog(s"Failed to create IsNotNull wildcard query for '$attribute', falling back to SplitExistsQuery: ${e.getMessage}")
              Some(new SplitExistsQuery(attribute))
          }
        }

      case IsNull(attribute) =>
        queryLog(s"Creating IsNull query via boolean negation: $attribute IS NULL")
        // IS NULL = All documents EXCEPT those where field exists
        // For non-TEXT fields, use parseQuery("field:*") to find docs with values,
        // then negate. For TEXT fields, fall back to SplitExistsQuery (requires fast field).
        val isNullFieldType = getFieldType(schema, attribute)
        if (isNullFieldType == FieldType.TEXT || isNullFieldType == FieldType.JSON) {
          // TEXT/JSON fields: can't reliably check existence without fast fields.
          // Use SplitExistsQuery as best-effort (requires fast field).
          val boolQuery = new SplitBooleanQuery()
          boolQuery.addMust(new SplitMatchAllQuery())
          boolQuery.addMustNot(new SplitExistsQuery(attribute))
          Some(boolQuery)
        } else {
          try {
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addMust(new SplitMatchAllQuery())
            boolQuery.addMustNot(splitSearchEngine.parseQuery(s"$attribute:*"))
            Some(boolQuery)
          } catch {
            case e: Exception =>
              queryLog(s"Failed to create IsNull wildcard query for '$attribute': ${e.getMessage}")
              val boolQuery = new SplitBooleanQuery()
              boolQuery.addMust(new SplitMatchAllQuery())
              boolQuery.addMustNot(new SplitExistsQuery(attribute))
              Some(boolQuery)
          }
        }

      // For complex operations like range queries, wildcard queries, etc., fall back to string parsing
      case GreaterThan(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Validate field type supports range operations
        fieldType match {
          case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE | FieldType.IP_ADDR => // OK
          case FieldType.UNSIGNED =>
            logger.warn(
              s"Range query on field '$attribute' rejected: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Filter will be applied by Spark instead."
            )
            return None
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use SplitRangeQuery for DATE and IP_ADDR fields to avoid parseQuery parsing issues
        try {
          val tantivyFieldType = fieldType match {
            case FieldType.DATE    => "date"
            case FieldType.INTEGER => "i64"
            case FieldType.FLOAT   => "f64"
            case FieldType.IP_ADDR => "ip"
            case _                 => "str"
          }
          val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
            attribute,
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(convertedValue.toString),
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded(),
            tantivyFieldType
          )
          queryLog(s"Creating GreaterThan SplitRangeQuery: $attribute > $convertedValue")
          Some(rangeQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create SplitRangeQuery for GreaterThan: ${e.getMessage}")
            None
        }

      case GreaterThanOrEqual(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Validate field type supports range operations
        fieldType match {
          case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE | FieldType.IP_ADDR => // OK
          case FieldType.UNSIGNED =>
            logger.warn(
              s"Range query on field '$attribute' rejected: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Filter will be applied by Spark instead."
            )
            return None
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use SplitRangeQuery for DATE and IP_ADDR fields to avoid parseQuery parsing issues
        try {
          val tantivyFieldType = fieldType match {
            case FieldType.DATE    => "date"
            case FieldType.INTEGER => "i64"
            case FieldType.FLOAT   => "f64"
            case FieldType.IP_ADDR => "ip"
            case _                 => "str"
          }
          val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
            attribute,
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(convertedValue.toString),
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded(),
            tantivyFieldType
          )
          queryLog(s"Creating GreaterThanOrEqual SplitRangeQuery: $attribute >= $convertedValue")
          Some(rangeQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create SplitRangeQuery for GreaterThanOrEqual: ${e.getMessage}")
            None
        }

      case LessThan(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Check if field is configured as a fast field - range queries only work on fast fields
        // First, check the Tantivy schema directly (most reliable source)
        val isFastFieldInSchema =
          try {
            val fieldInfo = schema.getFieldInfo(attribute)
            fieldInfo != null && fieldInfo.isFast()
          } catch {
            case _: Exception => false
          }

        // Fall back to checking options (for backward compatibility)
        val isFastFieldInOptions = options
          .map { opts =>
            Option(opts.get("spark.indextables.indexing.fastfields"))
              .map(_.split(",").map(_.trim).contains(attribute))
              .getOrElse(false)
          }
          .getOrElse(false)

        val isFastField = isFastFieldInSchema || isFastFieldInOptions

        // For date and IP address fields, be more permissive since they're often used for range queries
        val isDateOrIpFieldWorkaround = fieldType == FieldType.DATE || fieldType == FieldType.IP_ADDR
        val shouldAllowQuery          = isFastField || isDateOrIpFieldWorkaround

        logger.debug(s"LessThan range query check for '$attribute': isFastFieldInSchema=$isFastFieldInSchema, isFastFieldInOptions=$isFastFieldInOptions, isDateOrIpField=$isDateOrIpFieldWorkaround, shouldAllow=$shouldAllowQuery")

        if (!shouldAllowQuery) {
          logger.warn(
            s"Range query on field '$attribute' requires fast field configuration - deferring to Spark filtering"
          )
          return None
        }

        // Validate field type supports range operations
        fieldType match {
          case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE | FieldType.IP_ADDR => // OK
          case FieldType.UNSIGNED =>
            logger.warn(
              s"Range query on field '$attribute' rejected: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Filter will be applied by Spark instead."
            )
            return None
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use SplitRangeQuery for DATE and IP_ADDR fields to avoid parseQuery parsing issues
        try {
          val tantivyFieldType = fieldType match {
            case FieldType.DATE    => "date"
            case FieldType.INTEGER => "i64"
            case FieldType.FLOAT   => "f64"
            case FieldType.IP_ADDR => "ip"
            case _                 => "str"
          }
          val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
            attribute,
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded(),
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(convertedValue.toString),
            tantivyFieldType
          )
          queryLog(s"Creating LessThan SplitRangeQuery: $attribute < $convertedValue")
          Some(rangeQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create SplitRangeQuery for LessThan: ${e.getMessage}")
            None
        }

      case LessThanOrEqual(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Check if field is configured as a fast field - range queries only work on fast fields
        // First, check the Tantivy schema directly (most reliable source)
        val isFastFieldInSchema =
          try {
            val fieldInfo = schema.getFieldInfo(attribute)
            fieldInfo != null && fieldInfo.isFast()
          } catch {
            case _: Exception => false
          }

        // Fall back to checking options (for backward compatibility)
        val isFastFieldInOptions = options
          .map { opts =>
            Option(opts.get("spark.indextables.indexing.fastfields"))
              .map(_.split(",").map(_.trim).contains(attribute))
              .getOrElse(false)
          }
          .getOrElse(false)

        val isFastField = isFastFieldInSchema || isFastFieldInOptions

        // For date and IP address fields, be more permissive since they're often used for range queries
        val isDateOrIpFieldWorkaround = fieldType == FieldType.DATE || fieldType == FieldType.IP_ADDR
        val shouldAllowQuery          = isFastField || isDateOrIpFieldWorkaround

        logger.debug(s"LessThanOrEqual range query check for '$attribute': isFastFieldInSchema=$isFastFieldInSchema, isFastFieldInOptions=$isFastFieldInOptions, isDateOrIpField=$isDateOrIpFieldWorkaround, shouldAllow=$shouldAllowQuery")

        if (!shouldAllowQuery) {
          logger.warn(
            s"Range query on field '$attribute' requires fast field configuration - deferring to Spark filtering"
          )
          return None
        }

        // Validate field type supports range operations
        fieldType match {
          case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE | FieldType.IP_ADDR => // OK
          case FieldType.UNSIGNED =>
            logger.warn(
              s"Range query on field '$attribute' rejected: field uses exact_only indexing mode " +
                s"(values stored as U64 hashes). Filter will be applied by Spark instead."
            )
            return None
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use SplitRangeQuery for DATE and IP_ADDR fields to avoid parseQuery parsing issues
        try {
          val tantivyFieldType = fieldType match {
            case FieldType.DATE    => "date"
            case FieldType.INTEGER => "i64"
            case FieldType.FLOAT   => "f64"
            case FieldType.IP_ADDR => "ip"
            case _                 => "str"
          }
          val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
            attribute,
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.unbounded(),
            io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(convertedValue.toString),
            tantivyFieldType
          )
          queryLog(s"Creating LessThanOrEqual SplitRangeQuery: $attribute <= $convertedValue")
          Some(rangeQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create SplitRangeQuery for LessThanOrEqual: ${e.getMessage}")
            None
        }

      case StringStartsWith(attribute, value) =>
        // Use parseQuery with explicit field:pattern format for prefix matching
        try {
          val queryString = s"$attribute:$value*"
          queryLog(s"StringStartsWith: creating parseQuery for '$queryString'")
          Some(splitSearchEngine.parseQuery(queryString))
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create parseQuery for StringStartsWith: ${e.getMessage}")
            None
        }

      case StringEndsWith(attribute, value) =>
        // Use parseQuery with explicit field:pattern format for suffix matching
        try {
          val queryString = s"$attribute:*$value"
          queryLog(s"StringEndsWith: creating parseQuery for '$queryString'")
          Some(splitSearchEngine.parseQuery(queryString))
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create parseQuery for StringEndsWith: ${e.getMessage}")
            None
        }

      case StringContains(attribute, value) =>
        // Use parseQuery with explicit field:pattern format for contains matching
        try {
          val queryString = s"$attribute:*$value*"
          queryLog(s"StringContains: creating parseQuery for '$queryString'")
          Some(splitSearchEngine.parseQuery(queryString))
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create parseQuery for StringContains: ${e.getMessage}")
            None
        }

      case And(left, right) =>
        // Handle AND by combining both sides with MUST logic
        val leftQuery  = convertFilterToSplitQuery(left, schema, splitSearchEngine, options)
        val rightQuery = convertFilterToSplitQuery(right, schema, splitSearchEngine, options)

        (leftQuery, rightQuery) match {
          case (Some(lq), Some(rq)) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addMust(lq)
            boolQuery.addMust(rq)
            Some(boolQuery)
          case _ =>
            None // Fall back to Query conversion if either side can't be converted
        }

      case Or(left, right) =>
        // Handle OR by combining both sides with SHOULD logic
        val leftQuery  = convertFilterToSplitQuery(left, schema, splitSearchEngine, options)
        val rightQuery = convertFilterToSplitQuery(right, schema, splitSearchEngine, options)

        (leftQuery, rightQuery) match {
          case (Some(lq), Some(rq)) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addShould(lq)
            boolQuery.addShould(rq)
            Some(boolQuery)
          case _ =>
            None // Fall back to Query conversion if either side can't be converted
        }

      case Not(child) =>
        // Handle NOT by using MUST(match_all) + MUST_NOT(child) logic.
        // A boolean query with only MUST_NOT and no positive clause returns zero
        // results in Tantivy (same as Lucene). Adding a MUST(match_all) ensures
        // the query starts with all documents and then excludes matches.
        val childQuery = convertFilterToSplitQuery(child, schema, splitSearchEngine, options)

        childQuery match {
          case Some(cq) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addMust(new SplitMatchAllQuery())
            boolQuery.addMustNot(cq)
            Some(boolQuery)
          case None =>
            None // Fall back to Query conversion if child can't be converted
        }

      case _ =>
        queryLog(s"Unsupported filter type for SplitQuery conversion: $filter")
        None
    }
  }

  /** Convert a mixed filter (Spark Filter or custom filter) to a SplitQuery object. */
  private def convertMixedFilterToSplitQuery(
    filter: Any,
    splitSearchEngine: SplitSearchEngine,
    schema: Schema,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] = None
  ): Option[SplitQuery] =
    filter match {
      case sparkFilter: Filter =>
        convertFilterToSplitQuery(sparkFilter, schema, splitSearchEngine, options)

      case IndexQueryFilter(columnName, queryString) =>
        // Parse the custom IndexQuery using the split searcher with field-specific parsing
        // Note: Field validation happens earlier in isMixedFilterValidForSchema
        // Note: Driver-side validation should have already caught syntax errors
        //
        // Transform leading wildcard queries (e.g., *Configuration) to use explicit field syntax
        // (e.g., columnName:*Configuration) which Tantivy's wildcard query builder supports
        val transformedQuery = transformLeadingWildcardQuery(queryString, columnName)
        queryLog(
          s"Converting IndexQueryFilter to SplitQuery: field='$columnName', query='$queryString'" +
            (if (transformedQuery != queryString) s" (transformed to '$transformedQuery')" else "")
        )
        try {
          val parsedQuery = splitSearchEngine.parseQuery(transformedQuery, columnName)
          // splitSearchEngine.parseQuery() returns null on parse failures
          if (parsedQuery == null) {
            throw IndexQueryParseException.forField(
              queryString,
              columnName,
              new RuntimeException("parseQuery returned null - invalid query syntax")
            )
          }
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: IndexQueryParseException => throw e
          case e: Exception                =>
            // Wrap tantivy exception in IndexQueryParseException for user-friendly error
            throw IndexQueryParseException.forField(queryString, columnName, e)
        }

      case IndexQueryAllFilter(queryString) =>
        // Safety check for unqualified queries on wide tables
        validateIndexQueryAllSafety(queryString, schema, options)

        // Parse the custom IndexQueryAll using the split searcher with ALL fields
        // Note: Driver-side validation should have already caught syntax errors
        import scala.collection.JavaConverters._
        val allFieldNames = schema.getFieldNames
        queryLog(s"Converting IndexQueryAllFilter to search across ${allFieldNames.size()} fields: ${allFieldNames.asScala.mkString(", ")}")
        try {
          val parsedQuery = splitSearchEngine.parseQuery(queryString, allFieldNames)
          // splitSearchEngine.parseQuery() returns null on parse failures
          if (parsedQuery == null) {
            throw IndexQueryParseException.forAllFields(
              queryString,
              new RuntimeException("parseQuery returned null - invalid query syntax")
            )
          }
          Some(parsedQuery)
        } catch {
          case e: IndexQueryParseException => throw e
          case e: Exception                =>
            // Wrap tantivy exception in IndexQueryParseException for user-friendly error
            throw IndexQueryParseException.forAllFields(queryString, e)
        }

      case indexQueryV2: io.indextables.spark.filters.IndexQueryV2Filter =>
        // Handle V2 IndexQuery expressions from temp views
        // Note: Field validation happens earlier in isMixedFilterValidForSchema
        // Note: Driver-side validation should have already caught syntax errors
        //
        // Transform leading wildcard queries (e.g., *Configuration) to use explicit field syntax
        val transformedQuery = transformLeadingWildcardQuery(indexQueryV2.queryString, indexQueryV2.columnName)
        queryLog(
          s"Converting IndexQueryV2Filter to SplitQuery: field='${indexQueryV2.columnName}', query='${indexQueryV2.queryString}'" +
            (if (transformedQuery != indexQueryV2.queryString) s" (transformed to '$transformedQuery')" else "")
        )
        try {
          val parsedQuery = splitSearchEngine.parseQuery(transformedQuery, indexQueryV2.columnName)
          // splitSearchEngine.parseQuery() returns null on parse failures
          if (parsedQuery == null) {
            throw IndexQueryParseException.forField(
              indexQueryV2.queryString,
              indexQueryV2.columnName,
              new RuntimeException("parseQuery returned null - invalid query syntax")
            )
          }
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: IndexQueryParseException => throw e
          case e: Exception                =>
            // Wrap tantivy exception in IndexQueryParseException for user-friendly error
            throw IndexQueryParseException.forField(indexQueryV2.queryString, indexQueryV2.columnName, e)
        }

      case indexQueryAllV2: io.indextables.spark.filters.IndexQueryAllV2Filter =>
        // Safety check for unqualified queries on wide tables
        validateIndexQueryAllSafety(indexQueryAllV2.queryString, schema, options)

        // Handle V2 IndexQueryAll expressions from temp views
        // Note: Driver-side validation should have already caught syntax errors
        queryLog(s"Converting IndexQueryAllV2Filter to SplitQuery: query='${indexQueryAllV2.queryString}'")
        try {
          val parsedQuery = splitSearchEngine.parseQuery(indexQueryAllV2.queryString)
          // splitSearchEngine.parseQuery() returns null on parse failures
          if (parsedQuery == null) {
            throw IndexQueryParseException.forAllFields(
              indexQueryAllV2.queryString,
              new RuntimeException("parseQuery returned null - invalid query syntax")
            )
          }
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: IndexQueryParseException => throw e
          case e: Exception                =>
            // Wrap tantivy exception in IndexQueryParseException for user-friendly error
            throw IndexQueryParseException.forAllFields(indexQueryAllV2.queryString, e)
        }

      // Handle MixedBooleanFilter types - preserves OR/AND/NOT structure from IndexQuery expressions
      case mixedFilter: io.indextables.spark.filters.MixedBooleanFilter =>
        convertMixedBooleanFilterToSplitQuery(mixedFilter, splitSearchEngine, schema, options)

      case _ =>
        queryLog(s"Unsupported mixed filter type for SplitQuery conversion: $filter")
        None
    }

  /**
   * Convert a MixedBooleanFilter tree to a SplitQuery. Recursively handles Or, And, Not combinations of IndexQuery and
   * Spark filters. This preserves the original boolean structure instead of flattening to AND.
   */
  private def convertMixedBooleanFilterToSplitQuery(
    filter: io.indextables.spark.filters.MixedBooleanFilter,
    splitSearchEngine: SplitSearchEngine,
    schema: Schema,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap] = None
  ): Option[SplitQuery] = {
    import io.indextables.spark.filters._

    filter match {
      case MixedIndexQuery(indexQueryFilter) =>
        // Delegate to existing IndexQueryFilter handling
        // Transform leading wildcard queries (e.g., *Configuration) to use explicit field syntax
        val transformedQuery = transformLeadingWildcardQuery(indexQueryFilter.queryString, indexQueryFilter.columnName)
        queryLog(
          s"MixedBooleanFilter: Converting MixedIndexQuery: ${indexQueryFilter.columnName} indexquery '${indexQueryFilter.queryString}'" +
            (if (transformedQuery != indexQueryFilter.queryString) s" (transformed to '$transformedQuery')" else "")
        )
        try {
          val parsedQuery = splitSearchEngine.parseQuery(transformedQuery, indexQueryFilter.columnName)
          queryLog(s"MixedBooleanFilter: SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"MixedBooleanFilter: SplitQuery parsing failed: ${e.getMessage}")
            None
        }

      case MixedIndexQueryAll(indexQueryAllFilter) =>
        // Safety check for unqualified queries on wide tables
        validateIndexQueryAllSafety(indexQueryAllFilter.queryString, schema, options)

        // Delegate to existing IndexQueryAllFilter handling - search across ALL fields
        import scala.collection.JavaConverters._
        val allFieldNames = schema.getFieldNames
        queryLog(s"MixedBooleanFilter: Converting MixedIndexQueryAll: '${indexQueryAllFilter.queryString}' across ${allFieldNames.size()} fields: ${allFieldNames.asScala.mkString(", ")}")
        try {
          val parsedQuery = splitSearchEngine.parseQuery(indexQueryAllFilter.queryString, allFieldNames)
          queryLog(s"MixedBooleanFilter: SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"MixedBooleanFilter: SplitQuery parsing failed: ${e.getMessage}")
            None
        }

      case MixedSparkFilter(sparkFilter) =>
        // Delegate to existing Spark Filter handling
        queryLog(s"MixedBooleanFilter: Converting MixedSparkFilter: $sparkFilter")
        convertFilterToSplitQuery(sparkFilter, schema, splitSearchEngine, options)

      case MixedOrFilter(left, right) =>
        queryLog(s"MixedBooleanFilter: Converting MixedOrFilter - combining with SHOULD (OR)")
        val leftQuery  = convertMixedBooleanFilterToSplitQuery(left, splitSearchEngine, schema, options)
        val rightQuery = convertMixedBooleanFilterToSplitQuery(right, splitSearchEngine, schema, options)

        (leftQuery, rightQuery) match {
          case (Some(lq), Some(rq)) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addShould(lq) // OR = SHOULD
            boolQuery.addShould(rq)
            queryLog(s"MixedBooleanFilter: Created OR boolean query with left=${lq.getClass.getSimpleName}, right=${rq.getClass.getSimpleName}")
            Some(boolQuery)
          case (Some(q), None) =>
            queryLog(s"MixedBooleanFilter: OR - only left side converted, using single query")
            Some(q)
          case (None, Some(q)) =>
            queryLog(s"MixedBooleanFilter: OR - only right side converted, using single query")
            Some(q)
          case (None, None) =>
            queryLog(s"MixedBooleanFilter: OR - neither side could be converted")
            None
        }

      case MixedAndFilter(left, right) =>
        queryLog(s"MixedBooleanFilter: Converting MixedAndFilter - combining with MUST (AND)")
        val leftQuery  = convertMixedBooleanFilterToSplitQuery(left, splitSearchEngine, schema, options)
        val rightQuery = convertMixedBooleanFilterToSplitQuery(right, splitSearchEngine, schema, options)

        (leftQuery, rightQuery) match {
          case (Some(lq), Some(rq)) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addMust(lq) // AND = MUST
            boolQuery.addMust(rq)
            queryLog(s"MixedBooleanFilter: Created AND boolean query with left=${lq.getClass.getSimpleName}, right=${rq.getClass.getSimpleName}")
            Some(boolQuery)
          case (Some(q), None) =>
            queryLog(s"MixedBooleanFilter: AND - only left side converted, using single query")
            Some(q)
          case (None, Some(q)) =>
            queryLog(s"MixedBooleanFilter: AND - only right side converted, using single query")
            Some(q)
          case (None, None) =>
            queryLog(s"MixedBooleanFilter: AND - neither side could be converted")
            None
        }

      case MixedNotFilter(child) =>
        queryLog(s"MixedBooleanFilter: Converting MixedNotFilter - using MUST_NOT")
        convertMixedBooleanFilterToSplitQuery(child, splitSearchEngine, schema, options) match {
          case Some(childQuery) =>
            val boolQuery = new SplitBooleanQuery()
            boolQuery.addMustNot(childQuery) // NOT = MUST_NOT
            queryLog(s"MixedBooleanFilter: Created NOT boolean query with child=${childQuery.getClass.getSimpleName}")
            Some(boolQuery)
          case None =>
            queryLog(s"MixedBooleanFilter: NOT - child could not be converted")
            None
        }
    }
  }

  /**
   * Determine whether a field should use tokenized queries based on its indexing configuration. Uses the field type
   * configuration to distinguish between exact (string) and tokenized (text) matching.
   */
  private def shouldUseTokenizedQuery(
    fieldName: String,
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): Boolean =
    try
      options match {
        case Some(opts) =>
          val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(opts)
          val fieldConfig    = tantivyOptions.getFieldIndexingConfig(fieldName)

          // According to tantivy4java team:
          // - TEXT fields use "default" tokenizer (tokenized/split)
          // - STRING fields use "raw" tokenizer (exact preservation)
          val fieldType = fieldConfig.fieldType.getOrElse("string") // Default to string type

          fieldType == "text" // Only use tokenized queries for "text" type fields
        case None =>
          // No options available - default to exact matching for backward compatibility
          false
      }
    catch {
      case ex: Exception =>
        logger.warn(
          s"Failed to determine field configuration for '$fieldName', defaulting to exact matching: ${ex.getMessage}"
        )
        false // Default to exact matching on error
    }
}
