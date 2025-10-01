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

package io.indextables.spark.conversion

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Converts Catalyst expressions to Spark Filter objects for filter pushdown. This enables proper handling of regular
 * filter expressions in CatalystScan.
 */
object CatalystToSparkFilterConverter {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Convert a Catalyst expression to a Spark Filter. Returns Some(filter) if conversion is successful, None otherwise.
   */
  def convertExpression(expr: expressions.Expression): Option[sources.Filter] =
    expr match {
      // Comparison filters
      case expressions.EqualTo(expressions.AttributeReference(name, _, _, _), expressions.Literal(value, _)) =>
        Some(sources.EqualTo(name, value))

      case expressions.EqualTo(expressions.Literal(value, _), expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.EqualTo(name, value))

      case expressions.GreaterThan(expressions.AttributeReference(name, _, _, _), expressions.Literal(value, _)) =>
        Some(sources.GreaterThan(name, value))

      case expressions.GreaterThan(expressions.Literal(value, _), expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.LessThan(name, value))

      case expressions.GreaterThanOrEqual(
            expressions.AttributeReference(name, _, _, _),
            expressions.Literal(value, _)
          ) =>
        Some(sources.GreaterThanOrEqual(name, value))

      case expressions.GreaterThanOrEqual(
            expressions.Literal(value, _),
            expressions.AttributeReference(name, _, _, _)
          ) =>
        Some(sources.LessThanOrEqual(name, value))

      case expressions.LessThan(expressions.AttributeReference(name, _, _, _), expressions.Literal(value, _)) =>
        Some(sources.LessThan(name, value))

      case expressions.LessThan(expressions.Literal(value, _), expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.GreaterThan(name, value))

      case expressions.LessThanOrEqual(expressions.AttributeReference(name, _, _, _), expressions.Literal(value, _)) =>
        Some(sources.LessThanOrEqual(name, value))

      case expressions.LessThanOrEqual(expressions.Literal(value, _), expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.GreaterThanOrEqual(name, value))

      // Null checks
      case expressions.IsNull(expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.IsNull(name))

      case expressions.IsNotNull(expressions.AttributeReference(name, _, _, _)) =>
        Some(sources.IsNotNull(name))

      // String operations
      case expressions.Contains(
            expressions.AttributeReference(name, _, _, _),
            expressions.Literal(value, StringType)
          ) =>
        Some(sources.StringContains(name, value.toString))

      case expressions.StartsWith(
            expressions.AttributeReference(name, _, _, _),
            expressions.Literal(value, StringType)
          ) =>
        Some(sources.StringStartsWith(name, value.toString))

      case expressions.EndsWith(
            expressions.AttributeReference(name, _, _, _),
            expressions.Literal(value, StringType)
          ) =>
        Some(sources.StringEndsWith(name, value.toString))

      // IN predicate
      case expressions.In(expressions.AttributeReference(name, _, _, _), values) =>
        val literalValues = values.collect { case expressions.Literal(value, _) => value }
        if (literalValues.length == values.length) {
          Some(sources.In(name, literalValues.toArray))
        } else {
          logger.warn(s"IN predicate contains non-literal values, cannot convert: $expr")
          None
        }

      // Logical operations
      case expressions.And(left, right) =>
        for {
          leftFilter  <- convertExpression(left)
          rightFilter <- convertExpression(right)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter  <- convertExpression(left)
          rightFilter <- convertExpression(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        convertExpression(child).map(sources.Not)

      // Default case
      case _ =>
        logger.debug(s"Cannot convert expression to Spark filter: $expr (${expr.getClass.getSimpleName})")
        None
    }
}
