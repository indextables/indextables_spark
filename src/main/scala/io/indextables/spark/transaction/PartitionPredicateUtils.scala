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

package io.indextables.spark.transaction

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.{And => CatalystAnd, Or => CatalystOr, Not => CatalystNot}
import org.apache.spark.sql.catalyst.expressions.{EqualTo => CatalystEqualTo, GreaterThan => CatalystGreaterThan}
import org.apache.spark.sql.catalyst.expressions.{GreaterThanOrEqual => CatalystGreaterThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{LessThan => CatalystLessThan, LessThanOrEqual => CatalystLessThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{In => CatalystIn, IsNull => CatalystIsNull, IsNotNull => CatalystIsNotNull}
import org.apache.spark.sql.catalyst.expressions.{StartsWith => CatalystStartsWith}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import org.slf4j.LoggerFactory

/**
 * Utilities for parsing, validating, and evaluating partition predicates. This class extracts common logic used by
 * commands that need to filter partitions based on WHERE clauses (e.g., MERGE SPLITS, DROP PARTITIONS).
 */
object PartitionPredicateUtils {

  private val logger = LoggerFactory.getLogger(PartitionPredicateUtils.getClass)

  /**
   * Parse partition predicates from raw WHERE clause text and validate that they only reference partition columns.
   *
   * @param predicates
   *   Raw predicate strings from the WHERE clause
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param sparkSession
   *   SparkSession for parsing expressions
   * @return
   *   Sequence of parsed and validated Expression objects
   * @throws IllegalArgumentException
   *   if predicates reference non-partition columns or cannot be parsed
   */
  def parseAndValidatePredicates(
    predicates: Seq[String],
    partitionSchema: StructType,
    sparkSession: SparkSession
  ): Seq[Expression] = {

    // If no partition columns are defined, reject any WHERE clauses
    if (partitionSchema.isEmpty && predicates.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause not supported for non-partitioned tables. Partition predicates: ${predicates.mkString(", ")}"
      )
    }

    predicates.flatMap { predicate =>
      try {
        val expression = sparkSession.sessionState.sqlParser.parseExpression(predicate)
        validatePartitionColumnReferences(expression, partitionSchema)
        Some(expression)
      } catch {
        case ex: IllegalArgumentException =>
          // Re-throw validation errors as-is
          throw ex
        case ex: Exception =>
          logger.error(s"Failed to parse partition predicate: $predicate", ex)
          throw new IllegalArgumentException(s"Invalid partition predicate: $predicate", ex)
      }
    }
  }

  /**
   * Validate that the expression only references partition columns.
   *
   * @param expression
   *   The expression to validate
   * @param partitionSchema
   *   Schema defining the valid partition columns
   * @throws IllegalArgumentException
   *   if the expression references non-partition columns
   */
  def validatePartitionColumnReferences(expression: Expression, partitionSchema: StructType): Unit = {
    val partitionColumns  = partitionSchema.fieldNames.toSet
    val referencedColumns = expression.references.map(_.name).toSet

    // Check if any referenced columns are not partition columns
    val invalidColumns = referencedColumns -- partitionColumns
    if (invalidColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause references non-partition columns: ${invalidColumns.mkString(", ")}. " +
          s"Only partition columns are allowed: ${partitionColumns.mkString(", ")}"
      )
    }

    // Check if WHERE clause is empty (no column references)
    if (referencedColumns.isEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause must reference at least one partition column. " +
          s"Available partition columns: ${partitionColumns.mkString(", ")}"
      )
    }
  }

  /**
   * Create an InternalRow from partition values for predicate evaluation.
   *
   * @param partitionValues
   *   Map of partition column names to values
   * @param partitionSchema
   *   Schema defining the partition columns
   * @return
   *   InternalRow with partition values
   */
  def createRowFromPartitionValues(
    partitionValues: Map[String, String],
    partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fieldNames.map { fieldName =>
      partitionValues.get(fieldName) match {
        case Some(value) => UTF8String.fromString(value)
        case None        => null
      }
    }
    InternalRow.fromSeq(values)
  }

  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references and cast literals to UTF8String.
   *
   * @param expression
   *   The expression to resolve
   * @param schema
   *   Schema to resolve against
   * @return
   *   Resolved expression ready for evaluation
   */
  def resolveExpression(expression: Expression, schema: StructType): Expression =
    expression.transform {
      case unresolvedAttr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
        val fieldName  = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field      = schema(fieldIndex)
        org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
      case literal: org.apache.spark.sql.catalyst.expressions.Literal =>
        // Cast all literals to UTF8String since partition values are stored as strings
        literal.dataType match {
          case StringType => literal
          case _          =>
            // Convert non-string literals to UTF8String for comparison with partition values
            org.apache.spark.sql.catalyst.expressions.Literal(UTF8String.fromString(literal.value.toString), StringType)
        }
    }

  /**
   * Evaluate whether a partition matches the given predicates.
   *
   * @param partitionValues
   *   The partition values to evaluate
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param predicates
   *   Parsed predicate expressions
   * @return
   *   true if the partition matches all predicates
   */
  def evaluatePredicates(
    partitionValues: Map[String, String],
    partitionSchema: StructType,
    predicates: Seq[Expression]
  ): Boolean = {
    if (predicates.isEmpty) return true

    val row = createRowFromPartitionValues(partitionValues, partitionSchema)
    predicates.forall { predicate =>
      try {
        val resolvedPredicate = resolveExpression(predicate, partitionSchema)
        resolvedPredicate.eval(row).asInstanceOf[Boolean]
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to evaluate predicate against partition $partitionValues: ${ex.getMessage}")
          false // Conservative: exclude partition if evaluation fails
      }
    }
  }

  /**
   * Filter AddActions by partition predicates.
   *
   * @param addActions
   *   The AddActions to filter
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param predicates
   *   Parsed predicate expressions
   * @return
   *   AddActions that match the predicates
   */
  def filterAddActionsByPredicates(
    addActions: Seq[AddAction],
    partitionSchema: StructType,
    predicates: Seq[Expression]
  ): Seq[AddAction] = {
    if (predicates.isEmpty) return addActions

    val filtered = addActions.filter { action =>
      evaluatePredicates(action.partitionValues, partitionSchema, predicates)
    }

    val prunedCount = addActions.length - filtered.length
    if (prunedCount > 0) {
      logger.info(s"Partition predicate filtering: matched ${filtered.length} of ${addActions.length} splits")
    }

    filtered
  }

  /**
   * Build a partition schema from partition column names. All partition columns are treated as StringType for
   * evaluation purposes since partition values are stored as strings in the transaction log.
   *
   * @param partitionColumns
   *   Sequence of partition column names
   * @return
   *   StructType with all columns as StringType
   */
  def buildPartitionSchema(partitionColumns: Seq[String]): StructType =
    StructType(partitionColumns.map(name => StructField(name, StringType, nullable = true)))

  /**
   * Convert Catalyst Expression predicates to Spark sources Filter objects for Avro manifest pruning.
   *
   * This enables commands like PREWARM, DROP PARTITIONS, and MERGE SPLITS to benefit from partition-based manifest
   * pruning when using the Avro state format.
   *
   * @param predicates
   *   Parsed Catalyst Expression predicates
   * @return
   *   Sequence of Spark sources Filter objects (unsupported expressions are filtered out)
   */
  def expressionsToFilters(predicates: Seq[Expression]): Seq[Filter] =
    predicates.flatMap(expressionToFilter)

  /**
   * Convert a single Catalyst Expression to a Spark sources Filter.
   *
   * @param expression
   *   Catalyst Expression to convert
   * @return
   *   Some(Filter) if conversion is supported, None otherwise
   */
  def expressionToFilter(expression: Expression): Option[Filter] =
    expression match {
      // Equality: column = value
      case CatalystEqualTo(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        Some(EqualTo(nameParts.mkString("."), value))
      case CatalystEqualTo(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        Some(EqualTo(nameParts.mkString("."), value))

      // Greater than: column > value
      case CatalystGreaterThan(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        Some(GreaterThan(nameParts.mkString("."), value))
      case CatalystGreaterThan(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        Some(LessThan(nameParts.mkString("."), value))

      // Greater than or equal: column >= value
      case CatalystGreaterThanOrEqual(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        Some(GreaterThanOrEqual(nameParts.mkString("."), value))
      case CatalystGreaterThanOrEqual(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        Some(LessThanOrEqual(nameParts.mkString("."), value))

      // Less than: column < value
      case CatalystLessThan(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        Some(LessThan(nameParts.mkString("."), value))
      case CatalystLessThan(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        Some(GreaterThan(nameParts.mkString("."), value))

      // Less than or equal: column <= value
      case CatalystLessThanOrEqual(UnresolvedAttribute(nameParts), Literal(value, _)) =>
        Some(LessThanOrEqual(nameParts.mkString("."), value))
      case CatalystLessThanOrEqual(Literal(value, _), UnresolvedAttribute(nameParts)) =>
        Some(GreaterThanOrEqual(nameParts.mkString("."), value))

      // IN: column IN (value1, value2, ...)
      case CatalystIn(UnresolvedAttribute(nameParts), values) =>
        val literalValues = values.collect { case Literal(v, _) => v }
        if (literalValues.length == values.length) {
          Some(In(nameParts.mkString("."), literalValues.toArray))
        } else {
          None // Some values are not literals
        }

      // IS NULL: column IS NULL
      case CatalystIsNull(UnresolvedAttribute(nameParts)) =>
        Some(IsNull(nameParts.mkString(".")))

      // IS NOT NULL: column IS NOT NULL
      case CatalystIsNotNull(UnresolvedAttribute(nameParts)) =>
        Some(IsNotNull(nameParts.mkString(".")))

      // LIKE 'prefix%': column LIKE 'prefix%'
      case CatalystStartsWith(UnresolvedAttribute(nameParts), Literal(prefix, StringType)) =>
        Some(StringStartsWith(nameParts.mkString("."), prefix.toString))

      // AND: left AND right
      case CatalystAnd(left, right) =>
        (expressionToFilter(left), expressionToFilter(right)) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None)    => Some(l) // Use what we can convert
          case (None, Some(r))    => Some(r)
          case (None, None)       => None
        }

      // OR: left OR right
      case CatalystOr(left, right) =>
        // For OR, both sides must convert successfully
        (expressionToFilter(left), expressionToFilter(right)) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case _                  => None // Can't partially convert OR
        }

      // NOT: NOT expr
      case CatalystNot(child) =>
        expressionToFilter(child).map(Not)

      case _ =>
        logger.debug(s"Cannot convert expression to Filter for manifest pruning: ${expression.getClass.getSimpleName}")
        None
    }
}
