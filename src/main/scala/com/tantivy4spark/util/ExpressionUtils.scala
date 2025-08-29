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

package com.tantivy4spark.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import com.tantivy4spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}
import com.tantivy4spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

object ExpressionUtils {
  
  /**
   * Convert Catalyst expressions to IndexQueryFilter for pushdown.
   * This is used during filter pushdown to convert IndexQueryExpression to IndexQueryFilter.
   * 
   * Note: Since Filter is sealed in Spark, IndexQueryFilter doesn't extend Filter.
   * Instead, it's a separate case class that contains the filter information.
   */
  def expressionToIndexQueryFilter(expr: Expression): Option[IndexQueryFilter] = {
    expr match {
      case IndexQueryExpression(left, right) =>
        for {
          columnName <- extractColumnName(left)
          queryString <- extractStringLiteral(right)
        } yield IndexQueryFilter(columnName, queryString)
      case _ => None
    }
  }
  
  /**
   * Convert an IndexQueryFilter back to an IndexQueryExpression.
   * Useful for testing and reverse conversion scenarios.
   */
  def filterToExpression(filter: IndexQueryFilter): IndexQueryExpression = {
    val leftExpr = UnresolvedAttribute(Seq(filter.columnName))
    val rightExpr = Literal(UTF8String.fromString(filter.queryString), StringType)
    IndexQueryExpression(leftExpr, rightExpr)
  }
  
  /**
   * Convert Catalyst expressions to IndexQueryAllFilter for pushdown.
   * This is used during filter pushdown to convert IndexQueryAllExpression to IndexQueryAllFilter.
   */
  def expressionToIndexQueryAllFilter(expr: Expression): Option[IndexQueryAllFilter] = {
    expr match {
      case IndexQueryAllExpression(child) =>
        extractStringLiteral(child).map(IndexQueryAllFilter.apply)
      case _ => None
    }
  }
  
  /**
   * Convert an IndexQueryAllFilter back to an IndexQueryAllExpression.
   * Useful for testing and reverse conversion scenarios.
   */
  def filterToIndexQueryAllExpression(filter: IndexQueryAllFilter): IndexQueryAllExpression = {
    val queryExpr = Literal(UTF8String.fromString(filter.queryString), StringType)
    IndexQueryAllExpression(queryExpr)
  }
  
  /**
   * Extract column name from various expression types.
   */
  def extractColumnName(expr: Expression): Option[String] = {
    expr match {
      case attr: AttributeReference => Some(attr.name)
      case UnresolvedAttribute(nameParts) => Some(nameParts.mkString("."))
      case _ => None
    }
  }
  
  /**
   * Extract string literal value from expressions.
   */
  def extractStringLiteral(expr: Expression): Option[String] = {
    expr match {
      case Literal(value: UTF8String, StringType) => Some(value.toString)
      case Literal(value: String, StringType) => Some(value)
      case _ => None
    }
  }
  
  /**
   * Check if an expression is a valid IndexQueryExpression that can be pushed down.
   */
  def isValidIndexQuery(expr: Expression): Boolean = {
    expr match {
      case iq: IndexQueryExpression => iq.canPushDown
      case iq: IndexQueryAllExpression => iq.canPushDown
      case _ => false
    }
  }
  
  /**
   * Extract all IndexQuery expressions from a complex expression tree.
   * Returns both IndexQueryExpression and IndexQueryAllExpression instances.
   * Useful for analyzing complex WHERE clauses with multiple indexquery operators.
   */
  def extractIndexQueries(expr: Expression): Seq[Expression] = {
    expr match {
      case iq: IndexQueryExpression => Seq(iq)
      case iq: IndexQueryAllExpression => Seq(iq)
      case And(left, right) => extractIndexQueries(left) ++ extractIndexQueries(right)
      case Or(left, right) => extractIndexQueries(left) ++ extractIndexQueries(right)
      case Not(child) => extractIndexQueries(child)
      case _ => Seq.empty
    }
  }
  
  /**
   * Extract only IndexQueryExpression instances from a complex expression tree.
   * Maintains backward compatibility for existing code.
   */
  def extractIndexQueryExpressions(expr: Expression): Seq[IndexQueryExpression] = {
    expr match {
      case iq: IndexQueryExpression => Seq(iq)
      case And(left, right) => extractIndexQueryExpressions(left) ++ extractIndexQueryExpressions(right)
      case Or(left, right) => extractIndexQueryExpressions(left) ++ extractIndexQueryExpressions(right)
      case Not(child) => extractIndexQueryExpressions(child)
      case _ => Seq.empty
    }
  }
  
  /**
   * Extract only IndexQueryAllExpression instances from a complex expression tree.
   */
  def extractIndexQueryAllExpressions(expr: Expression): Seq[IndexQueryAllExpression] = {
    expr match {
      case iq: IndexQueryAllExpression => Seq(iq)
      case And(left, right) => extractIndexQueryAllExpressions(left) ++ extractIndexQueryAllExpressions(right)
      case Or(left, right) => extractIndexQueryAllExpressions(left) ++ extractIndexQueryAllExpressions(right)
      case Not(child) => extractIndexQueryAllExpressions(child)
      case _ => Seq.empty
    }
  }
  
  /**
   * Validate that an IndexQueryExpression has the correct structure and types.
   */
  def validateIndexQueryExpression(expr: IndexQueryExpression): Either[String, Unit] = {
    val columnCheck = expr.getColumnName match {
      case Some(_) => scala.util.Right(())
      case None => scala.util.Left(s"Invalid column reference in indexquery: ${expr.left}")
    }
    
    if (columnCheck.isLeft) return columnCheck
    
    val queryCheck = expr.getQueryString match {
      case Some(query) if query.nonEmpty => scala.util.Right(())
      case Some(_) => scala.util.Left("Query string cannot be empty")
      case None => scala.util.Left(s"Invalid query string in indexquery: ${expr.right}")
    }
    
    queryCheck
  }
  
  /**
   * Validate that an IndexQueryAllExpression has the correct structure and types.
   */
  def validateIndexQueryAllExpression(expr: IndexQueryAllExpression): Either[String, Unit] = {
    val queryCheck = expr.getQueryString match {
      case Some(query) if query.nonEmpty => scala.util.Right(())
      case Some(_) => scala.util.Left("Query string cannot be empty")
      case None => scala.util.Left(s"Invalid query string in indexqueryall: ${expr.child}")
    }
    
    queryCheck
  }
}