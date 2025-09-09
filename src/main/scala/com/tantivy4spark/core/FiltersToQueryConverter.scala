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

import org.apache.spark.sql.sources._
import com.tantivy4java.{Query, SplitQuery, SplitMatchAllQuery, SplitTermQuery, SplitBooleanQuery, Schema, Occur, FieldType, Index}
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._
import com.tantivy4spark.search.SplitSearchEngine
import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

object FiltersToQueryConverter {
  
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  /**
   * Safely execute a function with a Schema copy to avoid Arc reference counting issues.
   * This creates an independent Schema copy that can be used safely even if the original is closed.
   */
  private def withSchemaCopy[T](splitSearchEngine: SplitSearchEngine)(f: Schema => T): T = {
    val originalSchema = splitSearchEngine.getSchema()
    val schemaCopy = originalSchema.copy()
    try {
      f(schemaCopy)
    } finally {
      schemaCopy.close()
    }
  }

  /**
   * Convert Spark filters to a tantivy4java SplitQuery object using the new API.
   */
  def convertToSplitQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine): SplitQuery = {
    convertToSplitQuery(filters, splitSearchEngine, None)
  }

  /**
   * Convert Spark filters to a tantivy4java Query object (legacy API).
   */
  def convertToQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine): Query = {
    convertToQuery(filters, splitSearchEngine, None)
  }
  
  /**
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java Query object.
   */
  def convertToQuery(filters: Array[Any], splitSearchEngine: SplitSearchEngine): Query = {
    convertToQuery(filters, splitSearchEngine, None)
  }
  
  /**
   * Convert Spark filters to a tantivy4java SplitQuery object with schema field validation.
   */
  def convertToSplitQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine, schemaFieldNames: Option[Set[String]]): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery()  // Match-all query using object type
    }

    // Debug logging to understand what filters we receive  
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} filters for SplitQuery conversion:")
    filters.zipWithIndex.foreach { case (filter, idx) =>
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

    // Convert filters to SplitQuery objects using safe schema copy
    val splitQueries = withSchemaCopy(splitSearchEngine) { schema =>
      validFilters.flatMap(filter => convertFilterToSplitQuery(filter, schema))
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
   * Convert mixed filters (Spark Filter + custom filters) to a tantivy4java SplitQuery object with schema field validation.
   */
  def convertToSplitQuery(filters: Array[Any], splitSearchEngine: SplitSearchEngine, schemaFieldNames: Option[Set[String]]): SplitQuery = {
    if (filters.isEmpty) {
      return new SplitMatchAllQuery()  // Match-all query using object type
    }

    // Debug logging to understand what filters we receive  
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} mixed filters for SplitQuery conversion:")
    filters.zipWithIndex.foreach { case (filter, idx) =>
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
  def convertToQuery(filters: Array[Any], splitSearchEngine: SplitSearchEngine, schemaFieldNames: Option[Set[String]]): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Debug logging to understand what filters we receive  
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} mixed filters:")
    filters.zipWithIndex.foreach { case (filter, idx) =>
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
      validFilters.flatMap(filter => Option(convertMixedFilterToQuery(filter, splitSearchEngine, schema))).filter(_ != null)
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
   * Convert Spark filters to a tantivy4java Query object with schema field validation.
   */
  def convertToQuery(filters: Array[Filter], splitSearchEngine: SplitSearchEngine, schemaFieldNames: Option[Set[String]]): Query = {
    if (filters.isEmpty) {
      return Query.allQuery()
    }

    // Debug logging to understand what filters we receive  
    queryLog(s"🔍 FiltersToQueryConverter received ${filters.length} filters:")
    filters.zipWithIndex.foreach { case (filter, idx) =>
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

  /**
   * Test-only method for backward compatibility with debug tests.
   * Uses direct Query methods for simple testing.
   */
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

  /**
   * Simple filter conversion for tests - uses direct Query methods without parseQuery.
   */
  private def convertFilterToQuerySimple(filter: Filter, schema: Schema): Query = {
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
  }


  private def isFilterValidForSchema(filter: Filter, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._
    
    def getFilterFieldNames(f: Filter): Set[String] = f match {
      case EqualTo(attribute, _) => Set(attribute)
      case EqualNullSafe(attribute, _) => Set(attribute)
      case GreaterThan(attribute, _) => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _) => Set(attribute)
      case LessThanOrEqual(attribute, _) => Set(attribute)
      case In(attribute, _) => Set(attribute)
      case IsNull(attribute) => Set(attribute)
      case IsNotNull(attribute) => Set(attribute)
      case StringStartsWith(attribute, _) => Set(attribute)
      case StringEndsWith(attribute, _) => Set(attribute)
      case StringContains(attribute, _) => Set(attribute)
      case indexQuery: IndexQueryFilter => Set(indexQuery.columnName)
      case indexQueryAll: IndexQueryAllFilter => Set.empty // No specific field references
      case And(left, right) => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Or(left, right) => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Not(child) => getFilterFieldNames(child)
      case _ => Set.empty
    }
    
    val filterFields = getFilterFieldNames(filter)
    val isValid = filterFields.subsetOf(fieldNames)
    
    if (!isValid) {
      val missingFields = filterFields -- fieldNames
      queryLog(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }
    
    isValid
  }

  private def queryLog(msg: String): Unit = {
      logger.warn(msg)
  }

  /**
   * Create a temporary index from schema for parseQuery operations.
   * Uses the same schema so tokenizer configuration should be consistent.
   */
  private def withTemporaryIndex[T](schema: Schema)(f: Index => T): T = {
    import java.nio.file.Files
    val tempDir = Files.createTempDirectory("tantivy4spark_parsequery_")
    try {
      val tempIndex = new Index(schema, tempDir.toAbsolutePath.toString)
      try {
        f(tempIndex)
      } finally {
        tempIndex.close()
      }
    } finally {
      // Clean up temp directory
      try {
        import java.nio.file.{Path, Files}
        import java.nio.file.attribute.BasicFileAttributes
        import java.nio.file.SimpleFileVisitor
        import java.nio.file.FileVisitResult

        Files.walkFileTree(tempDir, new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        })
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to clean up temp directory ${tempDir}: ${e.getMessage}")
      }
    }
  }

  /**
   * Convert a single Spark Filter to a tantivy4java Query.
   */
  private def convertFilterToQuery(filter: Filter, splitSearchEngine: SplitSearchEngine, schema: Schema): Query = {
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
            val fieldNames = List(attribute).asJava
            queryLog(s"Parsing query: $queryString on fields: [$attribute]")
            withTemporaryIndex(schema) { index =>
              index.parseQuery(queryString, fieldNames)
            }
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
            val fieldNames = List(attribute).asJava
            queryLog(s"Parsing query: $queryString on fields: [$attribute]")
            withTemporaryIndex(schema) { index =>
              index.parseQuery(queryString, fieldNames)
            }
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
              val queryString = convertedValue.toString
              val fieldNames = List(attribute).asJava
              withTemporaryIndex(schema) { index =>
                index.parseQuery(queryString, fieldNames)
              }
            }
          }
        
        case GreaterThan(attribute, value) =>
          queryLog(s"Creating GreaterThan query: $attribute > $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          logger.info(s"Creating GreaterThan range query: field='$attribute', fieldType=$fieldType, min=$convertedValue (exclusive)")
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, false, true)
        
        case GreaterThanOrEqual(attribute, value) =>
          queryLog(s"Creating GreaterThanOrEqual query: $attribute >= $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, convertedValue, null, true, true)
        
        case LessThan(attribute, value) =>
          queryLog(s"Creating LessThan query: $attribute < $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, false)
        
        case LessThanOrEqual(attribute, value) =>
          queryLog(s"Creating LessThanOrEqual query: $attribute <= $value")
          val fieldType = getFieldType(schema, attribute)
          val convertedValue = convertSparkValueToTantivy(value, fieldType)
          Query.rangeQuery(schema, attribute, fieldType, null, convertedValue, true, true)
        
        case In(attribute, values) =>
          queryLog(s"Creating In query: $attribute IN [${values.mkString(", ")}]")
          val fieldType = getFieldType(schema, attribute)
          if (fieldType == FieldType.TEXT) {
            // For TEXT fields, create OR query with Index.parseQuery for each value
            queryLog(s"Field '$attribute' is TEXT, using OR of Index.parseQuery for IN query")
            val parseQueries = values.map { value =>
              val queryString = s""""${value.toString}""""
              val fieldNames = List(attribute).asJava
              withTemporaryIndex(schema) { index =>
                index.parseQuery(queryString, fieldNames)
              }
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
            // For other fields (BOOLEAN, BYTES), use term set query with converted values
            queryLog(s"Field '$attribute' is $fieldType, using termSetQuery")
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
          val leftQuery = convertFilterToQuery(left, splitSearchEngine, schema)
          val rightQuery = convertFilterToQuery(right, splitSearchEngine, schema)
          val occurQueries = List(
            new Query.OccurQuery(Occur.MUST, leftQuery),
            new Query.OccurQuery(Occur.MUST, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)
        
        case Or(left, right) =>
          queryLog(s"Creating Or query: $left OR $right")
          val leftQuery = convertFilterToQuery(left, splitSearchEngine, schema)
          val rightQuery = convertFilterToQuery(right, splitSearchEngine, schema)
          val occurQueries = List(
            new Query.OccurQuery(Occur.SHOULD, leftQuery),
            new Query.OccurQuery(Occur.SHOULD, rightQuery)
          )
          Query.booleanQuery(occurQueries.asJava)
        
        case Not(child) =>
          queryLog(s"Creating Not query: NOT $child")
          val childQuery = convertFilterToQuery(child, splitSearchEngine, schema)
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
          withTemporaryIndex(schema) { index =>
            index.parseQuery(pattern, fieldNames)
          }
        
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
          val fieldExists = try {
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
              try {
                index.parseQuery(indexQuery.queryString, fieldNames)
              } catch {
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
            try {
              index.parseQuery(indexQueryAll.queryString)
            } catch {
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
      case e: RuntimeException if e.getMessage != null && (e.getMessage.contains("Schema is closed") || e.getMessage.contains("Schema is invalid")) =>
        logger.warn(s"Cannot convert filter $filter - schema unavailable: ${e.getMessage}")
        Query.allQuery() // Fallback to match-all query when schema is closed
      case e: Exception =>
        logger.error(s"Failed to convert filter $filter to Query: ${e.getMessage}", e)
        Query.allQuery() // Fallback to match-all query
    }
  }
  
  /**
   * Check if a mixed filter (Spark Filter or custom filter) is valid for the schema.
   */
  private def isMixedFilterValidForSchema(filter: Any, fieldNames: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._
    import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}
    
    def getMixedFilterFieldNames(f: Any): Set[String] = f match {
      // Handle standard Spark filters
      case sparkFilter: Filter =>
        sparkFilter match {
          case EqualTo(attribute, _) => Set(attribute)
          case EqualNullSafe(attribute, _) => Set(attribute)
          case GreaterThan(attribute, _) => Set(attribute)
          case GreaterThanOrEqual(attribute, _) => Set(attribute)
          case LessThan(attribute, _) => Set(attribute)
          case LessThanOrEqual(attribute, _) => Set(attribute)
          case In(attribute, _) => Set(attribute)
          case IsNull(attribute) => Set(attribute)
          case IsNotNull(attribute) => Set(attribute)
          case StringStartsWith(attribute, _) => Set(attribute)
          case StringEndsWith(attribute, _) => Set(attribute)
          case StringContains(attribute, _) => Set(attribute)
          case And(left, right) => getMixedFilterFieldNames(left) ++ getMixedFilterFieldNames(right)
          case Or(left, right) => getMixedFilterFieldNames(left) ++ getMixedFilterFieldNames(right)
          case Not(child) => getMixedFilterFieldNames(child)
          case _ => Set.empty
        }
      // Handle custom filters
      case indexQuery: IndexQueryFilter => Set(indexQuery.columnName)
      case indexQueryAll: IndexQueryAllFilter => Set.empty // No specific field references
      case _ => Set.empty
    }
    
    val filterFields = getMixedFilterFieldNames(filter)
    val isValid = filterFields.subsetOf(fieldNames)
    
    if (!isValid) {
      val missingFields = filterFields -- fieldNames
      queryLog(s"Filter $filter references non-existent fields: ${missingFields.mkString(", ")}")
    }
    
    isValid
  }
  
  /**
   * Convert a mixed filter (Spark Filter or custom filter) to a Query object.
   */
  private def convertMixedFilterToQuery(filter: Any, splitSearchEngine: SplitSearchEngine, schema: Schema): Query = {
    import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}
    
    filter match {
      // Handle standard Spark filters
      case sparkFilter: Filter =>
        convertFilterToQuery(sparkFilter, splitSearchEngine, schema)
      
      // Handle custom IndexQuery filters  
      case indexQuery: IndexQueryFilter =>
        queryLog(s"Converting custom IndexQueryFilter: ${indexQuery.columnName} indexquery '${indexQuery.queryString}'")
        
        // Validate that the field exists in the schema
        val fieldExists = try {
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
            try {
              index.parseQuery(indexQuery.queryString, fieldNames)
            } catch {
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
          try {
            index.parseQuery(indexQueryAll.queryString)
          } catch {
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
  
  /**
   * Get the field type from the schema for a given field name.
   */
  private def getFieldType(schema: Schema, fieldName: String): FieldType = {
    try {
      val fieldInfo = schema.getFieldInfo(fieldName)
      fieldInfo.getType()
    } catch {
      case e: Exception =>
        logger.warn(s"Could not determine field type for '$fieldName', defaulting to TEXT: ${e.getMessage}")
        FieldType.TEXT
    }
  }
  
  /**
   * Check if a field type is numeric (should use range queries instead of term queries for equality)
   */
  private def isNumericFieldType(fieldType: FieldType): Boolean = {
    fieldType match {
      case FieldType.INTEGER | FieldType.FLOAT | FieldType.DATE => true
      case _ => false
    }
  }
  
  /**
   * Convert Spark values to tantivy4java compatible values for filtering
   */
  private def convertSparkValueToTantivy(value: Any, fieldType: FieldType): Any = {
    if (value == null) return null
    
    fieldType match {
      case FieldType.DATE =>
        // Convert to LocalDateTime for proper date field querying
        import java.time.LocalDateTime
        import java.time.LocalDate
        value match {
          case ts: java.sql.Timestamp => 
            ts.toLocalDateTime() // Direct conversion to LocalDateTime
          case date: java.sql.Date => 
            date.toLocalDate().atStartOfDay() // Convert to LocalDateTime at start of day
          case dateStr: String =>
            // Parse string date to LocalDateTime
            val localDate = LocalDate.parse(dateStr)
            localDate.atStartOfDay()
          case daysSinceEpoch: Int =>
            // Convert days since epoch to LocalDateTime
            val epochDate = LocalDate.of(1970, 1, 1)
            val localDate = epochDate.plusDays(daysSinceEpoch.toLong)
            localDate.atStartOfDay()
          case other => 
            queryLog(s"DATE conversion: Unexpected type ${other.getClass.getSimpleName}, trying to parse as string")
            val localDate = LocalDate.parse(other.toString)
            localDate.atStartOfDay()
        }
      case FieldType.INTEGER =>
        // Keep original types for range queries - tantivy4java handles type conversion internally
        val result = value match {
          case ts: java.sql.Timestamp => ts.getTime // Convert to milliseconds as Long
          case date: java.sql.Date => date.getTime / (24 * 60 * 60 * 1000L) // Convert to days since epoch as Long
          case i: java.lang.Integer => i // Keep as Integer for range queries
          case l: java.lang.Long => l // Keep as Long 
          case other => other
        }
        queryLog(s"INTEGER conversion: $value (${value.getClass.getSimpleName}) -> $result (${result.getClass.getSimpleName})")
        result
      case FieldType.BOOLEAN =>
        val booleanResult = value match {
          case b: java.lang.Boolean => b.booleanValue()
          case b: Boolean => b
          case i: java.lang.Integer => i != 0
          case l: java.lang.Long => l != 0
          case s: String => s.toLowerCase == "true" || s == "1"
          case other => throw new IllegalArgumentException(s"Cannot convert $other to Boolean for field type BOOLEAN")
        }
        // Ensure we return a Java Boolean object that tantivy4java expects
        val convertedValue = java.lang.Boolean.valueOf(booleanResult)
        queryLog(s"Boolean conversion: $value (${value.getClass.getSimpleName}) -> $convertedValue (${convertedValue.getClass.getSimpleName})")
        convertedValue
      case FieldType.FLOAT =>
        value match {
          case f: java.lang.Float => f.floatValue()
          case d: java.lang.Double => d.doubleValue()
          case i: java.lang.Integer => i.doubleValue()
          case l: java.lang.Long => l.doubleValue()
          case s: String => try { s.toDouble } catch { case _: Exception => throw new IllegalArgumentException(s"Cannot convert string '$s' to Float") }
          case other => other
        }
      case _ =>
        // For other types (TEXT, BYTES), pass through as-is
        value
    }
  }

  /**
   * Convert a Spark Filter to a SplitQuery object.
   */
  private def convertFilterToSplitQuery(filter: Filter, schema: Schema): Option[SplitQuery] = {
    import org.apache.spark.sql.sources._
    
    filter match {
      case EqualTo(attribute, value) =>
        val fieldType = getFieldType(schema, attribute)
        val convertedValue = convertSparkValueToTantivy(value, fieldType)
        Some(new SplitTermQuery(attribute, convertedValue.toString))
      
      case EqualNullSafe(attribute, value) if value != null =>
        convertFilterToSplitQuery(EqualTo(attribute, value), schema)
      
      case In(attribute, values) if values.nonEmpty =>
        val fieldType = getFieldType(schema, attribute)
        val termQueries = values.map { value =>
          val converted = convertSparkValueToTantivy(value, fieldType)
          new SplitTermQuery(attribute, converted.toString)
        }.toList
        
        // Create boolean query with OR logic for IN clause
        val boolQuery = new SplitBooleanQuery()
        termQueries.foreach(query => boolQuery.addShould(query))
        Some(boolQuery)
      
      case IsNotNull(_) =>
        Some(new SplitMatchAllQuery()) // Match all for IsNotNull
      
      // For complex operations like range queries, wildcard queries, etc., fall back to string parsing
      case GreaterThan(attribute, value) =>
        val convertedValue = convertSparkValueToTantivy(value, getFieldType(schema, attribute))
        // For now, use string parsing for complex queries - can be enhanced with SplitRangeQuery later
        None // Will fall back to legacy Query conversion or string parsing
      
      case GreaterThanOrEqual(attribute, value) =>
        val convertedValue = convertSparkValueToTantivy(value, getFieldType(schema, attribute))
        None // Will fall back to legacy Query conversion
      
      case LessThan(attribute, value) =>
        val convertedValue = convertSparkValueToTantivy(value, getFieldType(schema, attribute))
        None // Will fall back to legacy Query conversion
      
      case LessThanOrEqual(attribute, value) =>
        val convertedValue = convertSparkValueToTantivy(value, getFieldType(schema, attribute))
        None // Will fall back to legacy Query conversion
      
      case StringStartsWith(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries
      
      case StringEndsWith(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries
      
      case StringContains(attribute, value) =>
        None // Will fall back to string parsing for wildcard queries
      
      case _ =>
        queryLog(s"Unsupported filter type for SplitQuery conversion: $filter")
        None
    }
  }

  /**
   * Convert a mixed filter (Spark Filter or custom filter) to a SplitQuery object.
   */
  private def convertMixedFilterToSplitQuery(filter: Any, splitSearchEngine: SplitSearchEngine, schema: Schema): Option[SplitQuery] = {
    filter match {
      case sparkFilter: Filter =>
        convertFilterToSplitQuery(sparkFilter, schema)
      
      case IndexQueryFilter(columnName, queryString) =>
        // Parse the custom IndexQuery using the split searcher
        Some(splitSearchEngine.parseQuery(s"$columnName:($queryString)"))
      
      case IndexQueryAllFilter(queryString) =>
        // Parse the custom IndexQueryAll using the split searcher
        Some(splitSearchEngine.parseQuery(queryString))
      
      case _ =>
        queryLog(s"Unsupported mixed filter type for SplitQuery conversion: $filter")
        None
    }
  }
}
