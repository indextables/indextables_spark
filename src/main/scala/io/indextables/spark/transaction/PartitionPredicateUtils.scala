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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And => CatalystAnd, Not => CatalystNot, Or => CatalystOr}
import org.apache.spark.sql.catalyst.expressions.{EqualTo => CatalystEqualTo, GreaterThan => CatalystGreaterThan}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.{GreaterThanOrEqual => CatalystGreaterThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{
  In => CatalystIn,
  IsNotNull => CatalystIsNotNull,
  IsNull => CatalystIsNull
}
import org.apache.spark.sql.catalyst.expressions.{
  LessThan => CatalystLessThan,
  LessThanOrEqual => CatalystLessThanOrEqual
}
import org.apache.spark.sql.catalyst.expressions.{StartsWith => CatalystStartsWith}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
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
   * Create an InternalRow from partition values for predicate evaluation. Partition values stored as strings in the
   * transaction log are converted to their actual typed values based on the partition schema's data types.
   *
   * @param partitionValues
   *   Map of partition column names to values (stored as strings in transaction log)
   * @param partitionSchema
   *   Schema defining the partition columns with their real data types
   * @return
   *   InternalRow with typed partition values
   */
  def createRowFromPartitionValues(
    partitionValues: Map[String, String],
    partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fields.map { field =>
      partitionValues.get(field.name) match {
        case Some(value) => convertPartitionValue(value, field.dataType)
        case None        => null
      }
    }
    InternalRow.fromSeq(values)
  }

  /**
   * Convert a partition value string to a typed value for InternalRow storage. This is the inverse of Spark's partition
   * value serialization: partition values are stored as strings in the transaction log but need to be converted to their
   * actual types for correct comparison semantics.
   *
   * @param value
   *   String representation of the partition value
   * @param dataType
   *   Target data type from the partition schema
   * @return
   *   Typed value suitable for InternalRow storage
   */
  private def convertPartitionValue(value: String, dataType: DataType): Any =
    if (value == null) return null
    try
      dataType match {
        case StringType    => UTF8String.fromString(value)
        case IntegerType   => value.toInt
        case LongType      => value.toLong
        case FloatType     => value.toFloat
        case DoubleType    => value.toDouble
        case BooleanType   => value.toBoolean
        case ShortType     => value.toShort
        case ByteType      => value.toByte
        case _: DecimalType =>
          Decimal(new java.math.BigDecimal(value))
        case DateType =>
          // Date partition values are stored as YYYY-MM-DD strings
          val localDate = java.time.LocalDate.parse(value)
          localDate.toEpochDay.toInt
        case TimestampType =>
          // Timestamp partition values may be ISO-8601 or JDBC format
          try {
            val instant = java.time.Instant.parse(value)
            java.time.Duration.between(java.time.Instant.EPOCH, instant).getSeconds * 1000000L +
              instant.getNano / 1000L
          } catch {
            case _: Exception =>
              val ts = java.sql.Timestamp.valueOf(value)
              ts.getTime * 1000L + (ts.getNanos % 1000000) / 1000L
          }
        case _ =>
          // Unknown type - fall back to string
          logger.debug(s"Unknown partition column type ${dataType.simpleString}, using string representation")
          UTF8String.fromString(value)
      }
    catch {
      case e: Exception =>
        logger.warn(s"Failed to convert partition value '$value' to ${dataType.simpleString}: ${e.getMessage}, falling back to string")
        UTF8String.fromString(value)
    }

  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references and ensure type-correct literal
   * handling. Uses a two-pass approach:
   *
   * '''Pass 1''': Resolves UnresolvedAttributes to BoundReferences with the correct data type from the schema.
   *
   * '''Pass 2''': Coerces literals to match the data type of their paired BoundReference in comparison expressions.
   * This column-context-aware coercion handles mixed-type schemas correctly:
   *   - For `month > 9` where month is IntegerType: literal stays as IntegerType
   *   - For `date > 5` where date is StringType: literal is converted to UTF8String("5")
   *
   * This approach replaces the previous schema-global decision (all-string vs typed) which could cause
   * ClassCastException for cross-type comparisons in mixed-type schemas.
   *
   * @param expression
   *   The expression to resolve
   * @param schema
   *   Schema to resolve against (with real partition column types)
   * @return
   *   Resolved expression ready for evaluation
   */
  def resolveExpression(expression: Expression, schema: StructType): Expression = {
    // First pass: resolve UnresolvedAttributes to BoundReferences with correct types
    val withBoundRefs = expression.transform {
      case unresolvedAttr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
        val fieldName  = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field      = schema(fieldIndex)
        org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
    }

    // Second pass: coerce literals to match the data type of their paired BoundReference
    // in comparison expressions. This handles mixed-type schemas correctly:
    // - For `month > 9` (IntegerType column): literal stays as IntegerType
    // - For `date > 5` (StringType column): literal is converted to UTF8String
    // Without this column-context-aware coercion, a mixed-type schema would leave
    // cross-type comparisons unresolved, causing ClassCastException at eval time
    // (caught by evaluatePredicates' catch block but resulting in silent partition exclusion).
    withBoundRefs.transform {
      case CatalystGreaterThan(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, lit: Literal) =>
        CatalystGreaterThan(ref, coerceLiteralToType(lit, ref.dataType))
      case CatalystGreaterThan(lit: Literal, ref: org.apache.spark.sql.catalyst.expressions.BoundReference) =>
        CatalystGreaterThan(coerceLiteralToType(lit, ref.dataType), ref)
      case CatalystGreaterThanOrEqual(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, lit: Literal) =>
        CatalystGreaterThanOrEqual(ref, coerceLiteralToType(lit, ref.dataType))
      case CatalystGreaterThanOrEqual(lit: Literal, ref: org.apache.spark.sql.catalyst.expressions.BoundReference) =>
        CatalystGreaterThanOrEqual(coerceLiteralToType(lit, ref.dataType), ref)
      case CatalystLessThan(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, lit: Literal) =>
        CatalystLessThan(ref, coerceLiteralToType(lit, ref.dataType))
      case CatalystLessThan(lit: Literal, ref: org.apache.spark.sql.catalyst.expressions.BoundReference) =>
        CatalystLessThan(coerceLiteralToType(lit, ref.dataType), ref)
      case CatalystLessThanOrEqual(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, lit: Literal) =>
        CatalystLessThanOrEqual(ref, coerceLiteralToType(lit, ref.dataType))
      case CatalystLessThanOrEqual(lit: Literal, ref: org.apache.spark.sql.catalyst.expressions.BoundReference) =>
        CatalystLessThanOrEqual(coerceLiteralToType(lit, ref.dataType), ref)
      case CatalystEqualTo(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, lit: Literal) =>
        CatalystEqualTo(ref, coerceLiteralToType(lit, ref.dataType))
      case CatalystEqualTo(lit: Literal, ref: org.apache.spark.sql.catalyst.expressions.BoundReference) =>
        CatalystEqualTo(coerceLiteralToType(lit, ref.dataType), ref)
      case CatalystIn(ref: org.apache.spark.sql.catalyst.expressions.BoundReference, values) =>
        CatalystIn(ref, values.map {
          case lit: Literal => coerceLiteralToType(lit, ref.dataType)
          case other        => other
        })
    }
  }

  /**
   * Coerce a literal to match a target data type. If the literal already matches or no safe
   * conversion exists, the literal is returned unchanged. For StringType targets, non-string
   * literals are converted to their string representation as UTF8String.
   *
   * @param literal
   *   The literal to coerce
   * @param targetType
   *   The target data type (typically from a BoundReference)
   * @return
   *   A literal with a value compatible with the target type
   */
  private def coerceLiteralToType(literal: Literal, targetType: DataType): Literal =
    if (literal.dataType == targetType) {
      literal
    } else {
      targetType match {
        case StringType =>
          // Convert any literal to its string representation for string column comparisons
          literal.value match {
            case null => Literal(null, StringType)
            case _    => Literal(UTF8String.fromString(literal.value.toString), StringType)
          }
        case IntegerType =>
          // Try to convert string literal to integer
          try {
            val strVal = literal.value match {
              case u: UTF8String => u.toString
              case other         => other.toString
            }
            Literal(strVal.toInt, IntegerType)
          } catch {
            case _: NumberFormatException => literal // Can't convert, leave as-is
          }
        case LongType =>
          try {
            val strVal = literal.value match {
              case u: UTF8String => u.toString
              case other         => other.toString
            }
            Literal(strVal.toLong, LongType)
          } catch {
            case _: NumberFormatException => literal
          }
        case DoubleType =>
          try {
            val strVal = literal.value match {
              case u: UTF8String => u.toString
              case other         => other.toString
            }
            Literal(strVal.toDouble, DoubleType)
          } catch {
            case _: NumberFormatException => literal
          }
        case FloatType =>
          try {
            val strVal = literal.value match {
              case u: UTF8String => u.toString
              case other         => other.toString
            }
            Literal(strVal.toFloat, FloatType)
          } catch {
            case _: NumberFormatException => literal
          }
        case _ =>
          // For other types, leave as-is; evaluatePredicates' catch block provides safety
          literal
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
   * Build a partition schema from partition column names using real types from the full table schema. This enables
   * type-aware comparisons (e.g., numeric comparisons instead of lexicographic string comparisons) for partition
   * predicates.
   *
   * @param partitionColumns
   *   Sequence of partition column names
   * @param fullSchema
   *   Optional full table schema (from MetadataAction.schemaString). If provided, partition columns will use their real
   *   types. If None, falls back to StringType for all columns (legacy behavior).
   * @return
   *   StructType with partition columns using their real types (or StringType if schema is unavailable)
   */
  def buildPartitionSchema(partitionColumns: Seq[String], fullSchema: Option[StructType] = None): StructType =
    fullSchema match {
      case Some(schema) =>
        StructType(partitionColumns.map { name =>
          try {
            val fieldIndex = schema.fieldIndex(name)
            schema(fieldIndex)
          } catch {
            case _: IllegalArgumentException =>
              // Partition column not found in full schema - fall back to StringType
              logger.warn(s"Partition column '$name' not found in full schema, falling back to StringType")
              StructField(name, StringType, nullable = true)
          }
        })
      case None =>
        StructType(partitionColumns.map(name => StructField(name, StringType, nullable = true)))
    }

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
