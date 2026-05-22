/*
 * Bridge to Spark 4's package-private classic.ExpressionUtils.
 *
 * Spark 4 reworked Column to wrap a ColumnNode rather than an Expression, and
 * relegated the Expression<->Column converters to classic.ExpressionUtils
 * which is private[sql]. Tests and integration code that build Columns from
 * Catalyst Expression instances go through this object instead.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression

object IndexTablesColumnBridge {
  def column(expr: Expression): Column = classic.ExpressionUtils.column(expr)
  def expression(col: Column): Expression = classic.ExpressionUtils.expression(col)
}
