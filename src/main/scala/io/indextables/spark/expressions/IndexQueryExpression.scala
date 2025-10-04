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

package io.indextables.spark.expressions

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Expression for the INDEXQUERY operator that represents a direct Tantivy query.
 *
 * Usage: column_name indexquery 'query_string'
 *
 * This expression is primarily designed for pushdown to the Tantivy data source. When evaluated in Spark (fallback), it
 * returns true for all rows since the actual filtering should happen at the data source level.
 */
case class IndexQueryExpression(
  left: Expression, // Column reference
  right: Expression // Query string literal
) extends BinaryExpression
    with Predicate {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false // Predicates are typically not nullable

  // Mark as non-deterministic to prevent Spark from optimizing away the filter
  // This ensures the V2IndexQueryExpressionRule gets a chance to process it
  override lazy val deterministic: Boolean = false

  override def prettyName: String = "indexquery"

  override def sql: String = s"(${left.sql} indexquery ${right.sql})"

  override def toString: String = s"($left indexquery $right)"

  // For pushdown, we primarily care about the structure, not evaluation
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    // This should rarely be called since the expression should be pushed down
    // If called, return true as a safe fallback (filtering happens at source)
    true

  // Override eval to handle cases where child expressions cannot be evaluated
  override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any =
    // This should rarely be called since the expression should be pushed down
    // If called, return true as a safe fallback (filtering happens at source)
    true

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    // Code generation for the rare case this isn't pushed down
    // Return a literal true value
    ExprCode.forNonNullValue(JavaCode.literal("true", dataType))

  /** Extract the column name from the left expression. */
  def getColumnName: Option[String] = left match {
    case attr: AttributeReference       => Some(attr.name)
    case UnresolvedAttribute(nameParts) => Some(nameParts.mkString("."))
    case _                              => None
  }

  /** Extract the query string from the right expression. */
  def getQueryString: Option[String] = right match {
    case Literal(value: UTF8String, StringType) => Some(value.toString)
    case Literal(value: String, StringType)     => Some(value)
    case _                                      => None
  }

  /** Check if this expression can be converted to a pushdown filter. */
  def canPushDown: Boolean =
    getColumnName.isDefined && getQueryString.isDefined

  override def checkInputDataTypes(): TypeCheckResult = {
    val rightCheck = right.dataType match {
      case StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Right side of indexquery must be a string literal, got ${right.dataType}")
    }

    rightCheck
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression,
    newRight: Expression
  ): IndexQueryExpression =
    copy(left = newLeft, right = newRight)
}
