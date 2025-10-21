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

import io.indextables.spark.filters.{IndexQueryAllFilter, IndexQueryFilter}
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.tantivy4java.core.{FieldType, Index, Schema}
import io.indextables.tantivy4java.query.{Occur, Query}
import io.indextables.tantivy4java.split.{SplitBooleanQuery, SplitMatchAllQuery, SplitQuery, SplitTermQuery}
import org.slf4j.LoggerFactory

// Data class for storing range optimization information
case class RangeInfo(
  min: Option[Any],
  max: Option[Any],
  minInclusive: Boolean,
  maxInclusive: Boolean)

object FiltersToQueryConverter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Store range optimization information globally
  private val rangeOptimizations = scala.collection.mutable.Map[String, RangeInfo]()

  /**
   * Optimize range filters by detecting >= and < operations on the same field and storing the optimization information
   * for later use.
   */
  def optimizeRangeFilters(filters: Array[Filter]): Array[Filter] = {
    // Clear previous optimizations
    rangeOptimizations.clear()

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
              rangeOptimizations(field) = rangeInfo
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
    val schemaCopy     = originalSchema.copy()
    try
      f(schemaCopy)
    finally
      schemaCopy.close()
  }

  /** Convert Spark filters to a tantivy4java SplitQuery object using the new API. */
  def convertToSplitQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine): SplitQuery =
    convertToSplitQuery(filters, splitSearchEngine, None, None)

  /** Convert Spark filters to a tantivy4java SplitQuery object with field configuration. */
  def convertToSplitQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    options: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): SplitQuery =
    convertToSplitQuery(filters, splitSearchEngine, None, Some(options))

  /** Convert Spark filters to a tantivy4java Query object (legacy API). */
  def convertToQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine): Query =
    convertToQuery(filters, splitSearchEngine, None)

  /** Convert mixed filters (Spark Filter + custom filters) to a tantivy4java Query object. */
  def convertToQuery(filters: Array[Any], splitSearchEngine: SplitSearchEngine): Query =
    convertToQuery(filters, splitSearchEngine, None)

  /** Convert Spark filters to a tantivy4java SplitQuery object with schema field validation. */
  def convertToSplitQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]]
  ): SplitQuery =
    convertToSplitQuery(filters, splitSearchEngine, schemaFieldNames, None)

  /** Convert Spark filters to a tantivy4java SplitQuery object with schema field validation and field configuration. */
  def convertToSplitQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]],
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery() // Match-all query using object type
    }

    // Debug logging to understand what filters we receive
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} filters for SplitQuery conversion:")
    filters.zipWithIndex.foreach {
      case (filter, idx) =>
        queryLog(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
    }

    // Optimize range filters before conversion
    val optimizedFilters = optimizeRangeFilters(filters)

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
      rangeOptimizations.toList.flatMap {
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
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java SplitQuery object with schema field
   * validation.
   */
  def convertToSplitQuery(
    filters: Array[Any],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]]
  ): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery() // Match-all query using object type
    }

    // Debug logging to understand what filters we receive
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} mixed filters for SplitQuery conversion:")
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

    // Convert filters to SplitQuery objects
    val splitQueries = withSchemaCopy(splitSearchEngine) { schema =>
      validFilters.flatMap(filter => convertMixedFilterToSplitQuery(filter, splitSearchEngine, schema))
    }

    if (splitQueries.isEmpty) {
      new SplitMatchAllQuery()
    } else if (splitQueries.length == 1) {
      splitQueries.head
    } else {
      // Combine multiple queries with AND logic using SplitBooleanQuery
      val boolQuery = new SplitBooleanQuery()
      splitQueries.foreach(query => boolQuery.addMust(query))
      boolQuery
    }
  }

  /**
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java Query object with schema field validation.
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
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} mixed filters:")
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
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java SplitQuery object with schema field
   * validation and options.
   */
  def convertToSplitQuery(
    filters: Array[Any],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]],
    options: Option[org.apache.spark.sql.util.CaseInsensitiveStringMap]
  ): SplitQuery = {
    // Separate Spark filters from custom filters
    val sparkFilters  = filters.collect { case f: Filter => f }
    val customFilters = filters.filterNot(_.isInstanceOf[Filter])

    // Use existing method for Spark filters with options support
    val sparkQuery = if (sparkFilters.nonEmpty) {
      convertToSplitQuery(sparkFilters, splitSearchEngine, schemaFieldNames, options)
    } else {
      new SplitMatchAllQuery()
    }

    // For now, process custom filters without options (since existing method doesn't support it)
    // This can be enhanced later if needed
    if (customFilters.nonEmpty) {
      val customQuery = convertToSplitQuery(customFilters.toArray, splitSearchEngine, schemaFieldNames)

      // Combine both queries if we have both types
      if (sparkFilters.nonEmpty) {
        val combinedQuery = new SplitBooleanQuery()
        combinedQuery.addMust(sparkQuery)
        combinedQuery.addMust(customQuery)
        combinedQuery
      } else {
        customQuery
      }
    } else {
      sparkQuery
    }
  }

  /** Convert Spark filters to a tantivy4java Query object with schema field validation. */
  def convertToQuery(
    filters: Array[Filter],
    splitSearchEngine: SplitSearchEngine,
    schemaFieldNames: Option[Set[String]]
  ): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Debug logging to understand what filters we receive
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} filters:")
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
      case IsNotNull(_) =>
        Query.allQuery()
      case _ =>
        Query.allQuery() // Simplified for tests
    }

  private def isFilterValidForSchema(filter: Filter, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._

    def getFilterFieldNames(f: Filter): Set[String] = f match {
      case EqualTo(attribute, _)              => Set(attribute)
      case EqualNullSafe(attribute, _)        => Set(attribute)
      case GreaterThan(attribute, _)          => Set(attribute)
      case GreaterThanOrEqual(attribute, _)   => Set(attribute)
      case LessThan(attribute, _)             => Set(attribute)
      case LessThanOrEqual(attribute, _)      => Set(attribute)
      case In(attribute, _)                   => Set(attribute)
      case IsNull(attribute)                  => Set(attribute)
      case IsNotNull(attribute)               => Set(attribute)
      case StringStartsWith(attribute, _)     => Set(attribute)
      case StringEndsWith(attribute, _)       => Set(attribute)
      case StringContains(attribute, _)       => Set(attribute)
      case indexQuery: IndexQueryFilter       => Set(indexQuery.columnName)
      case indexQueryAll: IndexQueryAllFilter => Set.empty // No specific field references
      case And(left, right)                   => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Or(left, right)                    => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Not(child)                         => getFilterFieldNames(child)
      case _                                  => Set.empty
    }

    val filterFields = getFilterFieldNames(filter)
    val isValid      = filterFields.subsetOf(fieldNames)

    if (!isValid) {
      val missingFields = filterFields -- fieldNames
      queryLog(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }

    isValid
  }

  private def queryLog(msg: String): Unit =
    logger.debug(msg)

  /**
   * Create a temporary index from schema for parseQuery operations. Uses the same schema so tokenizer configuration
   * should be consistent.
   */
  private def withTemporaryIndex[T](schema: Schema)(f: Index => T): T = {
    import java.nio.file.Files
    val tempDir = Files.createTempDirectory("tantivy4spark_parsequery_")
    try {
      val tempIndex = new Index(schema, tempDir.toAbsolutePath.toString)
      try
        f(tempIndex)
      finally
        tempIndex.close()
    } finally
      // Clean up temp directory
      try {
        import java.nio.file.{Path, Files}
        import java.nio.file.attribute.BasicFileAttributes
        import java.nio.file.SimpleFileVisitor
        import java.nio.file.FileVisitResult

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
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to clean up temp directory $tempDir: ${e.getMessage}")
      }
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
          val fieldType      = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          logger.info(s"Creating GreaterThan range query: field='$attribute', fieldType=$fieldType, min=$convertedValue (exclusive)")
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, false, true)

        case GreaterThanOrEqual(attribute, value) =>
          queryLog(s"Creating GreaterThanOrEqual query: $attribute >= $value")
          val fieldType      = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, true, true)

        case LessThan(attribute, value) =>
          queryLog(s"Creating LessThan query: $attribute < $value")
          val fieldType      = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, false)

        case LessThanOrEqual(attribute, value) =>
          queryLog(s"Creating LessThanOrEqual query: $attribute <= $value")
          val fieldType      = getFieldType(schema, attribute)
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
          // For IsNull, we could return a query that matches no documents
          // TODO: Implement proper null handling if needed
          Query.allQuery()

        case IsNotNull(attribute) =>
          queryLog(s"Creating IsNotNull query: $attribute IS NOT NULL")
          // For NotNull queries, just use AllQuery as requested
          queryLog(s"Using AllQuery for IsNotNull on field '$attribute'")
          Query.allQuery()

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
          val pattern = value + "*"
          queryLog(s"StringStartsWith pattern: '$pattern'")
          val fieldNames = List(attribute).asJava
          withTemporaryIndex(schema)(index => index.parseQuery(pattern, fieldNames))

        case StringEndsWith(attribute, value) =>
          queryLog(s"Creating StringEndsWith query: $attribute ends with '$value'")
          val pattern = "*" + value
          queryLog(s"StringEndsWith pattern: '$pattern'")
          Query.wildcardQuery(schema, attribute, pattern, true)

        case StringContains(attribute, value) =>
          queryLog(s"Creating StringContains query: $attribute contains '$value'")
          val pattern = "*" + value + "*"
          queryLog(s"StringContains pattern: '$pattern'")
          Query.wildcardQuery(schema, attribute, pattern, true)

        case indexQuery: IndexQueryFilter =>
          queryLog(s"Creating IndexQuery: ${indexQuery.columnName} indexquery '${indexQuery.queryString}'")

          // Validate that the field exists in the schema
          val fieldExists =
            try {
              val fieldInfo = schema.getFieldInfo(indexQuery.columnName)
              true
            } catch {
              case _: Exception =>
                logger.warn(s"IndexQuery field '${indexQuery.columnName}' not found in schema, skipping")
                false
            }

          if (!fieldExists) {
            // Return match-all query if field doesn't exist (graceful degradation)
            queryLog(s"Field '${indexQuery.columnName}' not found, using match-all query")
            Query.allQuery()
          } else {
            // Use parseQuery with the specified field
            val fieldNames = List(indexQuery.columnName).asJava
            queryLog(s"Executing parseQuery: '${indexQuery.queryString}' on field '${indexQuery.columnName}'")

            withTemporaryIndex(schema) { index =>
              try
                index.parseQuery(indexQuery.queryString, fieldNames)
              catch {
                case e: Exception =>
                  logger.warn(s"Failed to parse indexquery '${indexQuery.queryString}': ${e.getMessage}")
                  // Fallback to match-all on parse failure
                  Query.allQuery()
              }
            }
          }

        case indexQueryAll: IndexQueryAllFilter =>
          queryLog(s"Creating IndexQueryAll: indexqueryall('${indexQueryAll.queryString}')")

          // Use single-argument parseQuery for all-fields search
          queryLog(s"Executing parseQuery across all fields: '${indexQueryAll.queryString}'")

          withTemporaryIndex(schema) { index =>
            try
              index.parseQuery(indexQueryAll.queryString)
            catch {
              case e: Exception =>
                logger.warn(s"Failed to parse indexqueryall '${indexQueryAll.queryString}': ${e.getMessage}")
                // Fallback to match-all on parse failure
                Query.allQuery()
            }
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

  /** Check if a mixed filter (Spark Filter or custom filter) is valid for the schema. */
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
      case _                                  => Set.empty
    }

    val filterFields = getMixedFilterFieldNames(filter)
    val isValid      = filterFields.subsetOf(fieldNames)

    if (!isValid) {
      val missingFields = filterFields -- fieldNames
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
      case indexQuery: IndexQueryFilter =>
        queryLog(s"Converting custom IndexQueryFilter: ${indexQuery.columnName} indexquery '${indexQuery.queryString}'")

        // Validate that the field exists in the schema
        val fieldExists =
          try {
            val fieldInfo = schema.getFieldInfo(indexQuery.columnName)
            true
          } catch {
            case _: Exception =>
              logger.warn(s"IndexQuery field '${indexQuery.columnName}' not found in schema, skipping")
              false
          }

        if (!fieldExists) {
          // Return match-all query if field doesn't exist (graceful degradation)
          queryLog(s"Field '${indexQuery.columnName}' not found, using match-all query")
          Query.allQuery()
        } else {
          // Use parseQuery with the specified field
          val fieldNames = List(indexQuery.columnName).asJava
          queryLog(s"Executing parseQuery: '${indexQuery.queryString}' on field '${indexQuery.columnName}'")

          withTemporaryIndex(schema) { index =>
            try
              index.parseQuery(indexQuery.queryString, fieldNames)
            catch {
              case e: Exception =>
                logger.warn(s"Failed to parse indexquery '${indexQuery.queryString}': ${e.getMessage}")
                // Fallback to match-all on parse failure
                Query.allQuery()
            }
          }
        }

      // Handle custom IndexQueryAll filters
      case indexQueryAll: IndexQueryAllFilter =>
        queryLog(s"Converting custom IndexQueryAllFilter: indexqueryall('${indexQueryAll.queryString}')")

        // Use single-argument parseQuery for all-fields search
        queryLog(s"Executing parseQuery across all fields: '${indexQueryAll.queryString}'")

        withTemporaryIndex(schema) { index =>
          try
            index.parseQuery(indexQueryAll.queryString)
          catch {
            case e: Exception =>
              logger.warn(s"Failed to parse indexqueryall '${indexQueryAll.queryString}': ${e.getMessage}")
              // Fallback to match-all on parse failure
              Query.allQuery()
          }
        }

      case _ =>
        logger.warn(s"Unsupported mixed filter: $filter (${filter.getClass.getSimpleName}), falling back to match-all")
        Query.allQuery()
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
      case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE => true
      case _                                                    => false
    }

  /** Convert Spark values to tantivy4java compatible values for filtering */
  private def convertSparkValueToTantivy(value: Any, fieldType: FieldType): Any = {
    if (value == null) return null

    fieldType match {
      case FieldType.DATE =>
        // Convert to YYYY-MM-DD date string format for SplitRangeQuery compatibility
        // Based on SplitDateRangeQueryTest: uses simple date strings like "2021-01-01"
        import java.time.{LocalDateTime, LocalDate}
        val localDate = value match {
          case ts: java.sql.Timestamp =>
            ts.toLocalDateTime().toLocalDate // Get date part only
          case date: java.sql.Date =>
            date.toLocalDate() // Direct conversion to LocalDate
          case dateStr: String =>
            // Parse string date to LocalDate
            LocalDate.parse(dateStr)
          case daysSinceEpoch: Int =>
            // Convert days since epoch to LocalDate
            val epochDate = LocalDate.of(1970, 1, 1)
            epochDate.plusDays(daysSinceEpoch.toLong)
          case other =>
            queryLog(s"DATE conversion: Unexpected type ${other.getClass.getSimpleName}, trying to parse as string")
            LocalDate.parse(other.toString)
        }
        val dateString = localDate.toString // This gives YYYY-MM-DD format
        queryLog(s"DATE conversion: $value -> $dateString")
        dateString
      case FieldType.INTEGER =>
        // Keep original types for range queries - tantivy4java handles type conversion internally
        val result = value match {
          case ts: java.sql.Timestamp => ts.getTime                            // Convert to milliseconds as Long
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

    filter match {
      case EqualTo(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // EqualTo should always use exact matching regardless of field type
        // For text fields, use phrase query syntax with quotes to ensure exact phrase matching
        // For date fields, use range queries for proper date matching
        // For other fields, use term queries
        if (fieldType == FieldType.TEXT) {
          // Use parseQuery with quoted syntax for exact phrase matching on tokenized text fields
          val quotedValue = "\"" + convertedValue.toString.replace("\"", "\\\"") + "\""
          val queryString = s"$attribute:$quotedValue"
          Some(splitSearchEngine.parseQuery(queryString))
        } else if (fieldType == FieldType.DATE) {
          // For date fields, use SplitRangeQuery covering the entire day
          convertedValue match {
            case dateString: String =>
              // For date equality, create a range covering the entire day using date strings
              // Based on SplitDateRangeQueryTest: use YYYY-MM-DD format strings with "date" field type
              import java.time.LocalDate
              val localDate    = LocalDate.parse(dateString)
              val nextDay      = localDate.plusDays(1)
              val startDateStr = localDate.toString // YYYY-MM-DD format
              val endDateStr   = nextDay.toString   // YYYY-MM-DD format

              val rangeQuery = new io.indextables.tantivy4java.split.SplitRangeQuery(
                attribute,
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.inclusive(startDateStr), // Include start of day
                io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound.exclusive(endDateStr), // Exclude start of next day
                "date" // Use "date" field type as shown in SplitDateRangeQueryTest
              )
              queryLog(s"Creating date equality SplitRangeQuery with date strings: [$startDateStr TO $endDateStr) for $localDate")
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

      case IsNotNull(_) =>
        Some(new SplitMatchAllQuery()) // Match all for IsNotNull

      // For complex operations like range queries, wildcard queries, etc., fall back to string parsing
      case GreaterThan(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Use SplitRangeQuery for range operations
        val tantivyFieldType = fieldType match {
          case FieldType.INTEGER => "i64"
          case FieldType.FLOAT   => "f64"
          case FieldType.DATE    => "date"
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use query string parsing for range queries - parseQuery can handle range syntax
        try {
          val queryString = s"$attribute:>$convertedValue"
          queryLog(s"Creating GreaterThan query using parseQuery: $queryString")
          val parsedQuery = splitSearchEngine.parseQuery(queryString)
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create range query using parseQuery for GreaterThan: ${e.getMessage}")
            None // Fall back completely if parsing fails
        }

      case GreaterThanOrEqual(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Use SplitRangeQuery for range operations
        val tantivyFieldType = fieldType match {
          case FieldType.INTEGER => "i64"
          case FieldType.FLOAT   => "f64"
          case FieldType.DATE    => "date"
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use query string parsing for range queries - parseQuery can handle range syntax
        try {
          val queryString = s"$attribute:>=$convertedValue"
          queryLog(s"Creating GreaterThanOrEqual query using parseQuery: $queryString")
          val parsedQuery = splitSearchEngine.parseQuery(queryString)
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create range query using parseQuery for GreaterThanOrEqual: ${e.getMessage}")
            None // Fall back completely if parsing fails
        }

      case LessThan(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Check if field is configured as a fast field - range queries only work on fast fields
        queryLog(s"🔍 DEBUG: Checking fast field config for '$attribute', options = $options")
        val isFastField = options
          .map { opts =>
            val fastFieldsStr = Option(opts.get("spark.indextables.indexing.fastfields"))
            queryLog(s"🔍 DEBUG: fastfields config = '$fastFieldsStr'")
            fastFieldsStr
              .map(_.split(",").map(_.trim).contains(attribute))
              .getOrElse(false)
          }
          .getOrElse(false)

        // For date fields, be more permissive since they're often used for range queries
        // TODO: This is a temporary workaround until fast field config is persisted in transaction log
        val isDateFieldWorkaround = fieldType == FieldType.DATE
        val shouldAllowQuery      = isFastField || isDateFieldWorkaround

        queryLog(s"🔍 DEBUG: isFastField for '$attribute' = $isFastField, isDateField = $isDateFieldWorkaround, shouldAllow = $shouldAllowQuery")

        if (!shouldAllowQuery) {
          queryLog(s"Range query on field '$attribute' requires fast field configuration - deferring to Spark filtering")
          return None
        }

        // Use SplitRangeQuery for range operations
        val tantivyFieldType = fieldType match {
          case FieldType.INTEGER => "i64"
          case FieldType.FLOAT   => "f64"
          case FieldType.DATE    => "date"
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use query string parsing for range queries - parseQuery can handle range syntax
        try {
          val queryString = s"$attribute:<$convertedValue"
          queryLog(s"Creating LessThan query using parseQuery: $queryString")
          val parsedQuery = splitSearchEngine.parseQuery(queryString)
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create range query using parseQuery for LessThan: ${e.getMessage}")
            None // Fall back completely if parsing fails
        }

      case LessThanOrEqual(attribute, value) =>
        val fieldType      = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)

        // Check if field is configured as a fast field - range queries only work on fast fields
        val isFastField = options
          .map { opts =>
            Option(opts.get("spark.indextables.indexing.fastfields"))
              .map(_.split(",").map(_.trim).contains(attribute))
              .getOrElse(false)
          }
          .getOrElse(false)

        // For date fields, be more permissive since they're often used for range queries
        // TODO: This is a temporary workaround until fast field config is persisted in transaction log
        val isDateFieldWorkaround = fieldType == FieldType.DATE
        val shouldAllowQuery      = isFastField || isDateFieldWorkaround

        if (!shouldAllowQuery) {
          queryLog(s"Range query on field '$attribute' requires fast field configuration - deferring to Spark filtering")
          return None
        }

        // Use SplitRangeQuery for range operations
        val tantivyFieldType = fieldType match {
          case FieldType.INTEGER => "i64"
          case FieldType.FLOAT   => "f64"
          case FieldType.DATE    => "date"
          case _ =>
            queryLog(s"Unsupported field type for range query: $fieldType")
            return None
        }

        // Use query string parsing for range queries - parseQuery can handle range syntax
        try {
          val queryString = s"$attribute:<=$convertedValue"
          queryLog(s"Creating LessThanOrEqual query using parseQuery: $queryString")
          val parsedQuery = splitSearchEngine.parseQuery(queryString)
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"Failed to create range query using parseQuery for LessThanOrEqual: ${e.getMessage}")
            None // Fall back completely if parsing fails
        }

      case StringStartsWith(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries

      case StringEndsWith(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries

      case StringContains(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries

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
        // Handle NOT by using pure MUST_NOT logic - now works correctly with tantivy4java fix
        val childQuery = convertFilterToSplitQuery(child, schema, splitSearchEngine, options)

        childQuery match {
          case Some(cq) =>
            val boolQuery = new SplitBooleanQuery()
            // With tantivy4java fix, pure mustNot queries work correctly
            boolQuery.addMustNot(cq) // Documents that don't match the child condition
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
    schema: Schema
  ): Option[SplitQuery] =
    filter match {
      case sparkFilter: Filter =>
        convertFilterToSplitQuery(sparkFilter, schema, splitSearchEngine)

      case IndexQueryFilter(columnName, queryString) =>
        // Parse the custom IndexQuery using the split searcher with field-specific parsing
        queryLog(s"Converting IndexQueryFilter to SplitQuery: field='$columnName', query='$queryString'")
        try {
          // Use the field-specific parseQuery method that takes field names list
          val parsedQuery = splitSearchEngine.parseQuery(queryString, columnName)
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"SplitQuery parsing failed: ${e.getMessage}")
            // Return None to fall back to legacy Query API
            None
        }

      case IndexQueryAllFilter(queryString) =>
        // Parse the custom IndexQueryAll using the split searcher with ALL fields
        // Get all field names from the schema to search across all fields
        import scala.collection.JavaConverters._
        val allFieldNames = schema.getFieldNames // Already returns java.util.List[String]
        queryLog(s"Converting IndexQueryAllFilter to search across ${allFieldNames.size()} fields: ${allFieldNames.asScala.mkString(", ")}")
        Some(splitSearchEngine.parseQuery(queryString, allFieldNames))

      case indexQueryV2: io.indextables.spark.filters.IndexQueryV2Filter =>
        // Handle V2 IndexQuery expressions from temp views
        queryLog(s"Converting IndexQueryV2Filter to SplitQuery: field='${indexQueryV2.columnName}', query='${indexQueryV2.queryString}'")
        try {
          // Use the field-specific parseQuery method that takes field names list
          val parsedQuery = splitSearchEngine.parseQuery(indexQueryV2.queryString, indexQueryV2.columnName)
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"SplitQuery parsing failed: ${e.getMessage}")
            // Return None to fall back to legacy Query API
            None
        }

      case indexQueryAllV2: io.indextables.spark.filters.IndexQueryAllV2Filter =>
        // Handle V2 IndexQueryAll expressions from temp views
        queryLog(s"Converting IndexQueryAllV2Filter to SplitQuery: query='${indexQueryAllV2.queryString}'")
        try {
          val parsedQuery = splitSearchEngine.parseQuery(indexQueryAllV2.queryString)
          queryLog(s"SplitQuery parsing result: ${parsedQuery.getClass.getSimpleName}")
          Some(parsedQuery)
        } catch {
          case e: Exception =>
            queryLog(s"SplitQuery parsing failed: ${e.getMessage}")
            None
        }

      case _ =>
        queryLog(s"Unsupported mixed filter type for SplitQuery conversion: $filter")
        None
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

  /**
   * Create a tokenized text query for text fields. This tokenizes the input string and creates a boolean query where
   * all tokens must be present.
   */
  private def createTokenizedTextQuery(
    fieldName: String,
    inputString: String,
    splitSearchEngine: SplitSearchEngine
  ): Option[SplitQuery] =
    try {
      // Use tantivy4java's tokenize method to tokenize the input string
      import scala.jdk.CollectionConverters._
      val tokens = splitSearchEngine.getSplitSearcher().tokenize(fieldName, inputString).asScala.toList

      if (tokens.isEmpty) {
        logger.warn(s"Tokenization of '$inputString' for field '$fieldName' resulted in no tokens")
        Some(new SplitMatchAllQuery()) // Return match-all if no tokens
      } else if (tokens.length == 1) {
        // Single token - use a simple term query
        Some(new SplitTermQuery(fieldName, tokens.head))
      } else {
        // Multiple tokens - create a boolean query where all tokens must be present (AND logic)
        val boolQuery = new SplitBooleanQuery()
        tokens.foreach { token =>
          val termQuery = new SplitTermQuery(fieldName, token)
          boolQuery.addMust(termQuery)
        }
        Some(boolQuery)
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to tokenize '$inputString' for field '$fieldName': ${ex.getMessage}", ex)
        // Fall back to exact term matching if tokenization fails
        Some(new SplitTermQuery(fieldName, inputString))
    }
}
