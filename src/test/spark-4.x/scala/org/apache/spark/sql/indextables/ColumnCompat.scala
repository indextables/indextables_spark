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

package org.apache.spark.sql.indextables

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{ColumnNodeToExpressionConverter, ExpressionUtils}
import org.apache.spark.sql.Column

/**
 * Test-only Column <-> catalyst Expression conversions (spark-4.x variant). In Spark 4, Column wraps a ColumnNode; the
 * converter (private[sql], hence this package) eagerly produces the catalyst Expression. (ExpressionUtils.expression is
 * NOT used for expr: it returns a lazy ColumnNodeExpression wrapper that our pattern-matching helpers wouldn't
 * recognize.)
 */
object ColumnCompat {
  def expr(c: Column): Expression   = ColumnNodeToExpressionConverter(c.node)
  def column(e: Expression): Column = ExpressionUtils.column(e)
}
