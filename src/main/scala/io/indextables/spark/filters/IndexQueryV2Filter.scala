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

package io.indextables.spark.filters

import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * V2-compatible marker expression for IndexQuery operations.
 *
 * This expression serves as a bridge between Catalyst expressions and V2 DataSource filters. It implements Expression
 * (so it can be used in logical plans) and provides methods to convert to Filter objects for V2 pushdown.
 *
 * During V2 pushdown:
 *   1. These expressions get detected in filter conditions 2. They get converted to IndexQueryFilter objects via
 *      toFilter() 3. The IndexQueryFilter gets pushed down to the data source 4. If not pushed down, they safely
 *      evaluate to true
 */
case class IndexQueryV2Filter(columnName: String, queryString: String) extends Expression with Predicate {

  override def dataType: DataType        = BooleanType
  override def nullable: Boolean         = false
  override def children: Seq[Expression] = Seq.empty

  // Extract the query information for pushdown conversion
  def getColumnName: Option[String]  = Some(columnName)
  def getQueryString: Option[String] = Some(queryString)

  /** Convert this V2 marker to a standard IndexQueryFilter for pushdown */
  def toFilter(): Option[IndexQueryFilter] =
    for {
      columnName  <- getColumnName
      queryString <- getQueryString
    } yield IndexQueryFilter(columnName, queryString)

  // Evaluation: return true (safe fallback if not pushed down)
  override def eval(input: InternalRow): Any = true

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    ExprCode.forNonNullValue(JavaCode.literal("true", dataType))

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    // No children, so return this unchanged
    this

  override def toString: String = s"IndexQueryV2Filter($columnName, $queryString)"
}

/** V2-compatible marker expression for IndexQueryAll operations. */
case class IndexQueryAllV2Filter(queryString: String) extends Expression with Predicate {

  override def dataType: DataType        = BooleanType
  override def nullable: Boolean         = false
  override def children: Seq[Expression] = Seq.empty

  // Extract the query information for pushdown conversion
  def getQueryString: Option[String] = Some(queryString)

  /** Convert this V2 marker to a standard IndexQueryAllFilter for pushdown */
  def toFilter(): Option[IndexQueryAllFilter] =
    Some(IndexQueryAllFilter(queryString))

  // Evaluation: return true (safe fallback if not pushed down)
  override def eval(input: InternalRow): Any = true

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    ExprCode.forNonNullValue(JavaCode.literal("true", dataType))

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    // No children, so return this unchanged
    this

  override def toString: String = s"IndexQueryAllV2Filter($queryString)"
}
