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

package io.indextables.spark.catalyst

import java.util.WeakHashMap

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}

import io.indextables.spark.expressions.{
  BucketAggregationConfig,
  DateHistogramConfig,
  DateHistogramExpression,
  HistogramConfig,
  HistogramExpression,
  RangeConfig,
  RangeExpression
}
import org.slf4j.LoggerFactory

/**
 * Catalyst rule to transform bucket aggregation expressions (DateHistogram, Histogram, Range) in GROUP BY clauses for
 * V2 DataSource pushdown.
 *
 * Problem: Spark's V2 aggregate pushdown doesn't handle custom function expressions in GROUP BY. When Spark sees `GROUP
 * BY indextables_histogram(price, 50)`, it tries to evaluate the expression directly, but our expressions extend
 * Unevaluable and fail code generation.
 *
 * Solution: This rule intercepts Aggregate nodes with bucket expressions and:
 *   1. Replaces the bucket expression with the underlying field reference (e.g., price) 2. Stores the bucket
 *      configuration for the ScanBuilder to retrieve 3. Marks the field so ScanBuilder knows to use bucket aggregation
 *      instead of terms aggregation
 *
 * The ScanBuilder then recognizes the stored bucket config and executes the appropriate tantivy4java bucket aggregation
 * (DateHistogramAggregation, HistogramAggregation, RangeAggregation).
 */
object V2BucketExpressionRule extends Rule[LogicalPlan] {

  private val logger = LoggerFactory.getLogger(V2BucketExpressionRule.getClass)

  // Storage for bucket configurations keyed by relation object
  // Using WeakHashMap so configs are automatically GC'd when relations are no longer referenced
  // This prevents stale configs from affecting subsequent queries (fixes "unexpected column count" errors)
  private val bucketConfigStorage = new WeakHashMap[DataSourceV2Relation, BucketAggregationConfig]()

  // Special marker prefix for bucket group columns
  val BUCKET_GROUP_MARKER = "__bucket_group__"

  /** Retrieve stored bucket configuration for a relation. */
  def getBucketConfig(relation: DataSourceV2Relation): Option[BucketAggregationConfig] =
    bucketConfigStorage.synchronized {
      Option(bucketConfigStorage.get(relation))
    }

  /** Retrieve stored bucket configuration by relation hash code (deprecated, kept for compatibility). */
  def getBucketConfigByHash(relationHashCode: Int): Option[BucketAggregationConfig] =
    bucketConfigStorage.synchronized {
      // Search for matching relation by hash code (slower but maintains compatibility)
      import scala.jdk.CollectionConverters._
      bucketConfigStorage.asScala.collectFirst {
        case (rel, config) if System.identityHashCode(rel) == relationHashCode => config
      }
    }

  /** Store bucket configuration for a relation. */
  def storeBucketConfig(relation: DataSourceV2Relation, config: BucketAggregationConfig): Unit =
    bucketConfigStorage.synchronized {
      logger.debug(s"V2BucketExpressionRule: Storing bucket config for relation ${System.identityHashCode(relation)}: ${config.description}")
      bucketConfigStorage.put(relation, config)
    }

  /** Clear bucket configuration for a relation. */
  def clearBucketConfig(relation: DataSourceV2Relation): Unit =
    bucketConfigStorage.synchronized {
      bucketConfigStorage.remove(relation)
    }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logger.debug(s"V2BucketExpressionRule: Processing plan: ${plan.getClass.getSimpleName}")

    plan.transformUp {
      case agg @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
        logger.debug(s"V2BucketExpressionRule: Found Aggregate node")
        logger.debug(
          s"V2BucketExpressionRule: Grouping expressions: ${groupingExpressions.map(_.toString).mkString(", ")}"
        )

        // Find bucket expressions in grouping expressions
        val bucketExprs = groupingExpressions.flatMap(findBucketExpression)

        if (bucketExprs.isEmpty) {
          logger.debug(s"V2BucketExpressionRule: No bucket expressions found, skipping")
          agg
        } else {
          logger.debug(s"V2BucketExpressionRule: Found ${bucketExprs.length} bucket expression(s)")

          // Only support single bucket expression for now
          if (bucketExprs.length > 1) {
            logger.warn(s"V2BucketExpressionRule: Multiple bucket expressions not supported, using first one")
          }

          val bucketExpr = bucketExprs.head
          logger.debug(s"V2BucketExpressionRule: Processing bucket expression: $bucketExpr")

          // Find the V2 relation in the plan tree
          val v2Relation = findV2Relation(child)

          v2Relation match {
            case Some(relation) if isCompatibleV2DataSource(relation) =>
              logger.debug(s"V2BucketExpressionRule: Found compatible V2 DataSource")

              // Extract bucket config and field reference
              val (bucketConfig, fieldRef) = extractBucketConfig(bucketExpr)

              // Transform the aggregate to use the field reference instead of bucket expression
              val transformedGrouping = groupingExpressions.map(expr => transformBucketExpression(expr, fieldRef))

              // Transform aggregate expressions (SELECT list) to use field reference
              val transformedAggExprs = aggregateExpressions.map { expr =>
                transformAggregateExpression(expr, bucketExpr, fieldRef)
              }

              logger.debug(
                s"V2BucketExpressionRule: Transformed grouping: ${transformedGrouping.map(_.toString).mkString(", ")}"
              )
              logger.debug(
                s"V2BucketExpressionRule: Transformed aggregates: ${transformedAggExprs.map(_.toString).mkString(", ")}"
              )

              // Create the transformed Aggregate using makeCopy for Spark version compatibility
              // Spark 4.x added a 4th parameter (hint: Option[AggregateHint]) to the Aggregate case class,
              // so direct constructor calls and copy() with named params fail with NoSuchMethodError.
              // makeCopy takes all constructor args and handles version differences.
              val originalArgs = agg.productIterator.toArray.map(_.asInstanceOf[AnyRef])
              val newArgs = Array[AnyRef](
                transformedGrouping,   // Replace groupingExpressions (position 0)
                transformedAggExprs,   // Replace aggregateExpressions (position 1)
                child                  // Keep child (position 2)
              ) ++ originalArgs.drop(3) // Preserve any additional args (e.g., hint in Spark 4.x)
              val transformedAgg = agg.makeCopy(newArgs).asInstanceOf[Aggregate]

              // Only store bucket config AFTER successful transformation
              // This prevents stale configs from polluting subsequent queries if transformation fails
              storeBucketConfig(relation, bucketConfig)

              // CRITICAL: Also set the current relation in ThreadLocal for ScanBuilder to retrieve
              // This is the same pattern used by V2IndexQueryExpressionRule
              import io.indextables.spark.core.IndexTables4SparkScanBuilder
              IndexTables4SparkScanBuilder.setCurrentRelation(relation)
              logger.info(s"V2BucketExpressionRule: Stored bucket config and set current relation: ${System.identityHashCode(relation)}")

              transformedAgg

            case _ =>
              logger.debug(s"V2BucketExpressionRule: No compatible V2 DataSource found, skipping transformation")
              agg
          }
        }
    }
  }

  /** Find bucket expression within an expression tree. */
  private def findBucketExpression(expr: Expression): Option[Expression] =
    expr match {
      case dh: DateHistogramExpression => Some(dh)
      case h: HistogramExpression      => Some(h)
      case r: RangeExpression          => Some(r)
      case _                           =>
        // Check children recursively
        expr.children.flatMap(findBucketExpression).headOption
    }

  /** Find the V2 relation in the plan tree. */
  private def findV2Relation(plan: LogicalPlan): Option[DataSourceV2Relation] =
    plan match {
      case relation: DataSourceV2Relation => Some(relation)
      case alias: SubqueryAlias           => findV2Relation(alias.child)
      case _                              => plan.children.flatMap(findV2Relation).headOption
    }

  /** Check if this is a compatible V2 DataSource (IndexTables4Spark). */
  private def isCompatibleV2DataSource(relation: DataSourceV2Relation): Boolean =
    relation.table.getClass.getName.contains("indextables") ||
      relation.table.getClass.getName.contains("tantivy4spark") ||
      relation.table.name().contains("indextables") ||
      relation.table.name().contains("tantivy4spark")

  /** Extract bucket configuration and field reference from a bucket expression. */
  private def extractBucketConfig(expr: Expression): (BucketAggregationConfig, AttributeReference) =
    expr match {
      case dh: DateHistogramExpression =>
        val fieldRef = extractFieldReference(dh.field)
        val config = DateHistogramConfig(
          fieldName = fieldRef.name,
          interval = dh.interval,
          offset = dh.offset,
          minDocCount = dh.minDocCount
        )
        // Keep the original field reference - the data type will be handled during execution
        (config, fieldRef)

      case h: HistogramExpression =>
        val fieldRef = extractFieldReference(h.field)
        val config = HistogramConfig(
          fieldName = fieldRef.name,
          interval = h.interval,
          offset = h.offset,
          minDocCount = h.minDocCount
        )
        // Keep the original field reference - the data type will be handled during execution
        (config, fieldRef)

      case r: RangeExpression =>
        val fieldRef = extractFieldReference(r.field)
        val config = RangeConfig(
          fieldName = fieldRef.name,
          ranges = r.ranges
        )
        // Keep the original field reference - the data type will be handled during execution
        (config, fieldRef)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported bucket expression type: ${expr.getClass.getName}")
    }

  /** Extract field reference from a bucket expression's field argument. */
  private def extractFieldReference(fieldExpr: Expression): AttributeReference =
    fieldExpr match {
      case attr: AttributeReference => attr
      case _                        =>
        // If not already an AttributeReference, try to find one in the expression tree
        fieldExpr
          .find(_.isInstanceOf[AttributeReference])
          .map(_.asInstanceOf[AttributeReference])
          .getOrElse {
            throw new IllegalArgumentException(
              s"Cannot extract field reference from expression: $fieldExpr (${fieldExpr.getClass.getName})"
            )
          }
    }

  /** Transform a grouping expression by replacing bucket expression with field reference. */
  private def transformBucketExpression(expr: Expression, fieldRef: AttributeReference): Expression =
    expr match {
      case _: DateHistogramExpression => fieldRef
      case _: HistogramExpression     => fieldRef
      case _: RangeExpression         => fieldRef
      case other                      =>
        // Recursively transform children
        other.withNewChildren(other.children.map(transformBucketExpression(_, fieldRef)))
    }

  /** Transform aggregate expressions (SELECT list) by replacing bucket expression references. */
  private def transformAggregateExpression(
    expr: NamedExpression,
    bucketExpr: Expression,
    fieldRef: AttributeReference
  ): NamedExpression =
    expr match {
      case alias @ Alias(child, name) =>
        // If the alias wraps the bucket expression, replace with field reference
        val transformedChild = transformBucketExpressionInTree(child, bucketExpr, fieldRef)
        if (transformedChild eq child) {
          alias
        } else {
          Alias(transformedChild, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
        }

      case attr: Attribute =>
        attr

      case other =>
        // For other named expressions, try to transform
        val transformed = transformBucketExpressionInTree(other, bucketExpr, fieldRef)
        transformed match {
          case ne: NamedExpression => ne
          case _                   => other
        }
    }

  /** Transform bucket expression references in an expression tree. */
  private def transformBucketExpressionInTree(
    expr: Expression,
    bucketExpr: Expression,
    fieldRef: AttributeReference
  ): Expression =
    expr match {
      // Direct match - replace bucket expression with field reference
      case e if isSameBucketExpression(e, bucketExpr) => fieldRef

      // Recursive transformation
      case other =>
        val newChildren = other.children.map(transformBucketExpressionInTree(_, bucketExpr, fieldRef))
        if (newChildren.zip(other.children).forall { case (n, o) => n eq o }) {
          other
        } else {
          other.withNewChildren(newChildren)
        }
    }

  /** Check if two expressions represent the same bucket expression. */
  private def isSameBucketExpression(expr1: Expression, expr2: Expression): Boolean =
    (expr1, expr2) match {
      case (dh1: DateHistogramExpression, dh2: DateHistogramExpression) =>
        dh1.interval == dh2.interval && dh1.offset == dh2.offset && dh1.minDocCount == dh2.minDocCount
      case (h1: HistogramExpression, h2: HistogramExpression) =>
        h1.interval == h2.interval && h1.offset == h2.offset && h1.minDocCount == h2.minDocCount
      case (r1: RangeExpression, r2: RangeExpression) =>
        r1.ranges == r2.ranges
      case _ =>
        false
    }
}
