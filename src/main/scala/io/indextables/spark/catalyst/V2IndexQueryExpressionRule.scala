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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import io.indextables.spark.expressions.{IndexQueryAllExpression, IndexQueryExpression}
import io.indextables.spark.filters._
import org.slf4j.LoggerFactory

/**
 * Catalyst rule to convert IndexQuery expressions to V2-compatible filters.
 *
 * This rule handles V2 DataSource API paths by intercepting IndexQueryExpression and IndexQueryAllExpression before
 * they reach Spark's built-in expression-to-filter translation mechanism, and directly converts them to
 * pushdown-compatible filters.
 *
 * The conversion process:
 *   1. Detects IndexQueryExpression/IndexQueryAllExpression in V2 filter conditions 2. Converts them directly to
 *      IndexQueryV2Filter and IndexQueryAllV2Filter expressions 3. These V2Filter expressions implement the required
 *      interfaces for V2 pushdown 4. The filters are recognized by ScanBuilder and properly pushed down
 */
object V2IndexQueryExpressionRule extends Rule[LogicalPlan] {

  private val logger = LoggerFactory.getLogger(V2IndexQueryExpressionRule.getClass)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logger.debug(s"V2IndexQueryExpressionRule: apply() called on plan: ${plan.getClass.getSimpleName}")

    // Find the relation in this plan (if any) to check if we need to clear ThreadLocal
    import io.indextables.spark.core.IndexTables4SparkScanBuilder
    val relationInPlan = plan.collectFirst {
      case relation: DataSourceV2Relation if isCompatibleV2DataSource(relation) => relation
    }

    // Clear ThreadLocal only if we're starting a new query (different relation ID)
    IndexTables4SparkScanBuilder.clearCurrentRelationIfDifferent(relationInPlan)

    // CRITICAL: Detect and clear stale IndexQueries from a previous failed query.
    // V2IndexQueryExpressionRule is a resolution rule (runs during analysis), not an optimization
    // rule. It runs once per query during analysis, then the optimizer runs (calling build()).
    // If build() throws (e.g., non-existent field), its cleanup (lines 460-464) is skipped,
    // leaving stale IndexQueries in the WeakHashMap. When the next query's analysis starts,
    // this rule runs again. If the "consumed" flag is true (build() read the IndexQueries),
    // they are stale and should be cleared before processing the new query.
    if (IndexTables4SparkScanBuilder.wereIndexQueriesConsumed()) {
      relationInPlan.orElse(IndexTables4SparkScanBuilder.getCurrentRelation()).foreach { relation =>
        IndexTables4SparkScanBuilder.clearIndexQueries(relation)
        logger.debug(s"V2IndexQueryExpressionRule: Cleared stale IndexQueries for relation ${System.identityHashCode(relation)}")
      }
      IndexTables4SparkScanBuilder.resetIndexQueriesConsumed()
    }

    val result = plan.transformUp {
      case filter @ Filter(condition, child: DataSourceV2Relation) =>
        logger.debug(
          s"V2IndexQueryExpressionRule: Found Filter with DataSourceV2Relation - table: ${child.table.name()}"
        )
        logger.debug(s"V2IndexQueryExpressionRule: Condition: $condition")
        logger.debug(s"V2IndexQueryExpressionRule: Child table: ${child.table.name()}")

        // Only apply to V2 DataSource relations
        if (isCompatibleV2DataSource(child)) {
          logger.debug(s"V2IndexQueryExpressionRule: Compatible V2 DataSource detected")
          val convertedCondition = convertIndexQueryExpressions(condition, child)

          if (convertedCondition != condition) {
            logger.debug(s"V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
            Filter(convertedCondition, child)
          } else {
            logger.debug(s"V2IndexQueryExpressionRule: No conversion needed")
            filter
          }
        } else {
          logger.debug(s"V2IndexQueryExpressionRule: Not compatible V2 DataSource - REJECTING IndexQuery")
          // Check if condition contains IndexQuery to warn user
          if (containsIndexQueryExpression(condition)) {
            logger.warn(
              s"⚠️  WARNING: IndexQuery expression detected but rejected because table is not compatible V2 DataSource"
            )
            logger.warn(s"⚠️  Table class: ${child.table.getClass.getName}, Table name: ${child.table.name()}")
          }
          filter
        }
      case filter @ Filter(condition, child: SubqueryAlias) =>
        // Handle SubqueryAlias wrapping a DataSourceV2Relation (temp views)
        // Use collectFirst to find the DataSourceV2Relation anywhere in the subtree,
        // not just as a direct child. This handles cases where the user applies
        // DataFrame filters before creating the temp view, resulting in:
        // Filter(indexquery) -> SubqueryAlias -> Filter(other) -> DataSourceV2Relation
        val v2RelationOpt = child.collectFirst { case r: DataSourceV2Relation => r }

        // CRITICAL FIX: For CTE references, the SubqueryAlias child might not directly contain
        // the DataSourceV2Relation (it uses CTERelationRef instead). In this case, if we have
        // an IndexQuery condition, we should use the relation already in ThreadLocal that was
        // set when processing the underlying temp view earlier in this same query.
        val effectiveRelationOpt = v2RelationOpt.orElse {
          // Only fall back to ThreadLocal if we have an IndexQuery to process
          if (containsIndexQueryExpression(condition)) {
            IndexTables4SparkScanBuilder.getCurrentRelation().flatMap {
              case r: DataSourceV2Relation if isCompatibleV2DataSource(r) =>
                logger.debug(s"V2IndexQueryExpressionRule: Using relation from ThreadLocal for CTE reference: id=${System.identityHashCode(r)}")
                Some(r)
              case _ => None
            }
          } else {
            None
          }
        }

        effectiveRelationOpt match {
          case Some(v2Relation) =>
            logger.debug(s"V2IndexQueryExpressionRule: Found Filter with SubqueryAlias containing DataSourceV2Relation")
            logger.debug(s"V2IndexQueryExpressionRule: Condition: $condition")
            logger.debug(s"V2IndexQueryExpressionRule: Child table: ${v2Relation.table.name()}")

            // Only apply to V2 DataSource relations
            if (isCompatibleV2DataSource(v2Relation)) {
              logger.debug(s"V2IndexQueryExpressionRule: Compatible V2 DataSource detected (via SubqueryAlias)")
              val convertedCondition = convertIndexQueryExpressions(condition, v2Relation)

              if (convertedCondition != condition) {
                logger.debug(
                  s"V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition"
                )
                Filter(convertedCondition, child)
              } else {
                logger.debug(s"V2IndexQueryExpressionRule: No conversion needed")
                filter
              }
            } else {
              logger.debug(s"V2IndexQueryExpressionRule: Not compatible V2 DataSource - REJECTING IndexQuery")
              // Check if condition contains IndexQuery to warn user
              if (containsIndexQueryExpression(condition)) {
                logger.warn(s"⚠️  WARNING: IndexQuery expression detected but rejected because table is not compatible V2 DataSource")
                logger.warn(
                  s"⚠️  Table class: ${v2Relation.table.getClass.getName}, Table name: ${v2Relation.table.name()}"
                )
              }
              filter
            }
          case None =>
            logger.debug(
              s"V2IndexQueryExpressionRule: Found Filter with SubqueryAlias but no DataSourceV2Relation in subtree"
            )
            filter
        }
      case filter @ Filter(condition, child) =>
        logger.debug(s"V2IndexQueryExpressionRule: Found Filter with non-V2 relation: ${child.getClass.getSimpleName}")
        // For other plans, don't modify
        filter

      // Handle other plan types that might contain filters
      case plan: LogicalPlan =>
        // Look for IndexQuery expressions anywhere in the plan's expressions
        val hasIndexQuery = plan.expressions.exists(containsIndexQueryExpression)
        if (hasIndexQuery) {
          logger.debug(
            s"V2IndexQueryExpressionRule: Found IndexQuery expressions in ${plan.getClass.getSimpleName} plan"
          )

          // Find the DataSourceV2Relation in the plan tree
          val v2Relations = plan.collect { case relation: DataSourceV2Relation => relation }
          v2Relations.find(isCompatibleV2DataSource) match {
            case Some(relation) =>
              logger.debug(s"V2IndexQueryExpressionRule: Found compatible V2 DataSource in plan tree")

              // Transform the plan to convert IndexQuery expressions
              plan.transformExpressions {
                case expr if containsIndexQueryExpression(expr) =>
                  convertIndexQueryExpressions(expr, relation)
              }

            case None =>
              logger.debug(s"V2IndexQueryExpressionRule: No compatible V2 DataSource found in plan tree")
              plan
          }
        } else {
          plan
        }
    }

    result
  }

  /** Check if an expression contains IndexQuery expressions recursively */
  private def containsIndexQueryExpression(expr: Expression): Boolean =
    expr match {
      case _: IndexQueryExpression    => true
      case _: IndexQueryAllExpression => true
      case _                          => expr.children.exists(containsIndexQueryExpression)
    }

  /**
   * Check if an expression is ONLY IndexQuery expressions (no Spark predicates). Used to identify NOT(IndexQuery)
   * patterns that should be replaced with Literal(true).
   *
   * Returns true for:
   *   - IndexQueryExpression
   *   - IndexQueryAllExpression
   *   - NOT(IndexQueryExpression)
   *   - AND/OR combinations of only IndexQuery expressions
   *
   * Returns false if the expression contains any non-IndexQuery predicates.
   */
  private def isOnlyIndexQueryExpression(expr: Expression): Boolean = {
    import org.apache.spark.sql.catalyst.expressions.{And, Or, Not}
    expr match {
      case _: IndexQueryExpression    => true
      case _: IndexQueryAllExpression => true
      case Not(child)                 => isOnlyIndexQueryExpression(child)
      case And(left, right)           => isOnlyIndexQueryExpression(left) && isOnlyIndexQueryExpression(right)
      case Or(left, right)            => isOnlyIndexQueryExpression(left) && isOnlyIndexQueryExpression(right)
      case _                          => false
    }
  }

  /** Check if this is a compatible V2 DataSource (IndexTables4Spark) */
  private def isCompatibleV2DataSource(relation: DataSourceV2Relation): Boolean =
    // Check if this is a IndexTables4Spark V2 table (support both old and new package names)
    relation.table.getClass.getName.contains("indextables") ||
      relation.table.getClass.getName.contains("tantivy4spark") ||
      relation.table.name().contains("indextables") ||
      relation.table.name().contains("tantivy4spark")

  /**
   * Convert IndexQuery expressions and store them for the ScanBuilder to retrieve.
   *
   * This method now builds a mixed boolean filter tree that preserves the full OR/AND structure when IndexQuery
   * expressions are combined with boolean operators. This fixes the bug where: (url indexquery 'community') OR (url
   * indexquery 'curl') Was incorrectly converted to AND semantics instead of OR.
   */
  private def convertIndexQueryExpressions(
    expr: Expression,
    relation: DataSourceV2Relation
  ): Expression = {
    import org.apache.spark.sql.catalyst.expressions.{And, Or, Not, Literal}
    import io.indextables.spark.core.IndexTables4SparkScanBuilder
    import io.indextables.spark.filters._

    // CRITICAL: ALWAYS set ThreadLocal with relation object so ScanBuilder can retrieve it
    // This is needed for BOTH IndexQuery expressions AND regular filter pushdown
    // This works because Catalyst optimization and ScanBuilder creation happen on same thread
    // Even with AQE, the same relation object is reused throughout planning
    logger.debug(s"V2IndexQueryExpressionRule: Setting current relation: ${System.identityHashCode(relation)}")
    IndexTables4SparkScanBuilder.setCurrentRelation(relation)

    // Check if expression contains any IndexQuery expressions
    if (!containsIndexQueryExpression(expr)) {
      return expr // No IndexQuery, nothing to do
    }

    // Try to build a mixed boolean filter tree that preserves the full structure
    val mixedFilter = buildMixedBooleanFilter(expr)

    mixedFilter match {
      case Some(filter) =>
        // Store the mixed boolean filter
        IndexTables4SparkScanBuilder.storeIndexQueries(relation, Seq(filter))
        logger.debug(s"V2IndexQueryExpressionRule: Stored mixed boolean filter: $filter for relation ${System.identityHashCode(relation)}")
        // CRITICAL FIX: Do NOT return Literal(true) for the whole expression!
        // We must preserve non-IndexQuery predicates (like partition filters) for Spark's
        // partition pruning. Only replace IndexQuery expressions with Literal(true).
        // Example: "load_date = '2024-01-01' AND message indexquery 'error'" should become
        //          "load_date = '2024-01-01' AND true" so Spark can still prune partitions.
        //
        // IMPORTANT: We use transformDown to handle NOT(anything with IndexQuery) BEFORE
        // transforming the IndexQuery itself. This prevents incorrect semantics:
        //
        // Example: NOT (col indexquery 'value' AND id='12345')
        // Without special handling: NOT(true AND id='12345') = NOT(id='12345') ← WRONG!
        // With special handling: NOT(...) → true (Tantivy handles full NOT via MixedBooleanFilter)
        //
        // This is safe because:
        // 1. Tantivy evaluates the full NOT expression via MixedBooleanFilter
        // 2. Spark sees 'true' for the NOT part, so it doesn't incorrectly filter
        // 3. Other predicates outside the NOT are preserved for partition pruning
        val transformedExpr = expr.transformDown {
          // If NOT contains ANY IndexQuery, replace entire NOT with true
          // (Tantivy handles the full NOT via MixedBooleanFilter)
          case Not(child) if containsIndexQueryExpression(child) => Literal(true)
          case _: IndexQueryExpression                           => Literal(true)
          case _: IndexQueryAllExpression                        => Literal(true)
        }
        transformedExpr

      case None =>
        // Fall back to original approach for complex cases
        // This handles cases we can't fully represent in the mixed tree
        logger.debug(s"V2IndexQueryExpressionRule: Falling back to flat IndexQuery extraction")
        val indexQueries = scala.collection.mutable.Buffer[Any]()

        val transformedExpr = expr.transformUp {
          case indexQuery: IndexQueryExpression =>
            logger.debug(s"V2IndexQueryExpressionRule: Found IndexQueryExpression: $indexQuery")

            (extractColumnNameForV2(indexQuery), extractQueryStringForV2(indexQuery)) match {
              case (Some(columnName), Some(queryString)) =>
                if (columnName == "_indexall") {
                  logger.debug(s"V2IndexQueryExpressionRule: Storing _indexall IndexQuery")
                  indexQueries += IndexQueryAllFilter(queryString)
                } else {
                  logger.debug(s"V2IndexQueryExpressionRule: Storing IndexQuery")
                  indexQueries += IndexQueryFilter(columnName, queryString)
                }
                Literal(true)
              case _ =>
                logger.debug(
                  s"V2IndexQueryExpressionRule: Unable to extract column/query from IndexQuery, using Literal(true)"
                )
                Literal(true)
            }

          case indexQueryAll: IndexQueryAllExpression =>
            logger.debug(s"V2IndexQueryExpressionRule: Found IndexQueryAllExpression: $indexQueryAll")

            indexQueryAll.getQueryString match {
              case Some(queryString) =>
                logger.debug(s"V2IndexQueryExpressionRule: Storing IndexQueryAll")
                indexQueries += IndexQueryAllFilter(queryString)
                Literal(true)
              case _ =>
                logger.debug(
                  s"V2IndexQueryExpressionRule: Unable to extract query from IndexQueryAll, using Literal(true)"
                )
                Literal(true)
            }
        }

        // Store the collected IndexQueries for this relation object (if any)
        if (indexQueries.nonEmpty) {
          IndexTables4SparkScanBuilder.storeIndexQueries(relation, indexQueries.toSeq)
          logger.debug(
            s"V2IndexQueryExpressionRule: Stored ${indexQueries.length} IndexQuery expressions for relation ${System.identityHashCode(relation)}"
          )
        }

        transformedExpr
    }
  }

  /**
   * Build a mixed boolean filter tree from a Catalyst expression. Returns Some(filter) if successful, None if the
   * expression structure can't be represented (e.g., complex expressions we don't support).
   */
  private def buildMixedBooleanFilter(expr: Expression): Option[MixedBooleanFilter] = {
    import org.apache.spark.sql.catalyst.expressions.{And, Or, Not, Literal => CatLiteral}
    import io.indextables.spark.filters._

    expr match {
      // IndexQuery expressions
      case indexQuery: IndexQueryExpression =>
        (extractColumnNameForV2(indexQuery), extractQueryStringForV2(indexQuery)) match {
          case (Some(columnName), Some(queryString)) =>
            if (columnName == "_indexall") {
              Some(MixedIndexQueryAll(IndexQueryAllFilter(queryString)))
            } else {
              Some(MixedIndexQuery(IndexQueryFilter(columnName, queryString)))
            }
          case _ => None
        }

      case indexQueryAll: IndexQueryAllExpression =>
        indexQueryAll.getQueryString.map(qs => MixedIndexQueryAll(IndexQueryAllFilter(qs)))

      // Boolean combinations
      case Or(left, right) =>
        for {
          leftFilter  <- buildMixedBooleanFilter(left)
          rightFilter <- buildMixedBooleanFilter(right)
        } yield MixedOrFilter(leftFilter, rightFilter)

      case And(left, right) =>
        for {
          leftFilter  <- buildMixedBooleanFilter(left)
          rightFilter <- buildMixedBooleanFilter(right)
        } yield MixedAndFilter(leftFilter, rightFilter)

      case Not(child) =>
        buildMixedBooleanFilter(child).map(MixedNotFilter)

      // Convert supported Catalyst expressions to Spark Filters
      case _ =>
        catalystExprToSparkFilter(expr).map(f => MixedSparkFilter(f))
    }
  }

  /** Convert a Catalyst expression to a Spark Filter. Returns None if the expression type is not supported. */
  private def catalystExprToSparkFilter(expr: Expression): Option[org.apache.spark.sql.sources.Filter] = {
    import org.apache.spark.sql.catalyst.expressions.{
      AttributeReference,
      Literal,
      EqualTo => CatEqualTo,
      GreaterThan => CatGreaterThan,
      GreaterThanOrEqual => CatGreaterThanOrEqual,
      LessThan => CatLessThan,
      LessThanOrEqual => CatLessThanOrEqual,
      In => CatIn,
      IsNull => CatIsNull,
      IsNotNull => CatIsNotNull,
      And => CatAnd,
      Or => CatOr,
      Not => CatNot
    }
    import org.apache.spark.sql.sources

    expr match {
      case CatEqualTo(AttributeReference(name, _, _, _), Literal(value, _)) =>
        Some(sources.EqualTo(name, unwrapValue(value)))

      case CatEqualTo(Literal(value, _), AttributeReference(name, _, _, _)) =>
        Some(sources.EqualTo(name, unwrapValue(value)))

      case CatGreaterThan(AttributeReference(name, _, _, _), Literal(value, _)) =>
        Some(sources.GreaterThan(name, unwrapValue(value)))

      case CatGreaterThan(Literal(value, _), AttributeReference(name, _, _, _)) =>
        Some(sources.LessThan(name, unwrapValue(value)))

      case CatGreaterThanOrEqual(AttributeReference(name, _, _, _), Literal(value, _)) =>
        Some(sources.GreaterThanOrEqual(name, unwrapValue(value)))

      case CatGreaterThanOrEqual(Literal(value, _), AttributeReference(name, _, _, _)) =>
        Some(sources.LessThanOrEqual(name, unwrapValue(value)))

      case CatLessThan(AttributeReference(name, _, _, _), Literal(value, _)) =>
        Some(sources.LessThan(name, unwrapValue(value)))

      case CatLessThan(Literal(value, _), AttributeReference(name, _, _, _)) =>
        Some(sources.GreaterThan(name, unwrapValue(value)))

      case CatLessThanOrEqual(AttributeReference(name, _, _, _), Literal(value, _)) =>
        Some(sources.LessThanOrEqual(name, unwrapValue(value)))

      case CatLessThanOrEqual(Literal(value, _), AttributeReference(name, _, _, _)) =>
        Some(sources.GreaterThanOrEqual(name, unwrapValue(value)))

      case CatIn(AttributeReference(name, _, _, _), values) =>
        val extractedValues = values.collect { case Literal(v, _) => unwrapValue(v) }
        if (extractedValues.length == values.length) {
          Some(sources.In(name, extractedValues.toArray))
        } else None

      case CatIsNull(AttributeReference(name, _, _, _)) =>
        Some(sources.IsNull(name))

      case CatIsNotNull(AttributeReference(name, _, _, _)) =>
        Some(sources.IsNotNull(name))

      // Handle <> as Not(EqualTo)
      case CatNot(CatEqualTo(AttributeReference(name, _, _, _), Literal(value, _))) =>
        Some(sources.Not(sources.EqualTo(name, unwrapValue(value))))

      case CatNot(CatEqualTo(Literal(value, _), AttributeReference(name, _, _, _))) =>
        Some(sources.Not(sources.EqualTo(name, unwrapValue(value))))

      case CatAnd(left, right) =>
        for {
          leftFilter  <- catalystExprToSparkFilter(left)
          rightFilter <- catalystExprToSparkFilter(right)
        } yield sources.And(leftFilter, rightFilter)

      case CatOr(left, right) =>
        for {
          leftFilter  <- catalystExprToSparkFilter(left)
          rightFilter <- catalystExprToSparkFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case CatNot(child) =>
        catalystExprToSparkFilter(child).map(sources.Not)

      case _ => None // Unsupported expression type
    }
  }

  /** Unwrap Spark internal types to Java types for Filter compatibility. */
  private def unwrapValue(value: Any): Any = value match {
    case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
    case other                                          => other
  }

  /**
   * Extract column name from IndexQueryExpression with V2-specific handling. In V2 context, column names may come
   * through as Literal expressions from SQL parsing.
   */
  private def extractColumnNameForV2(indexQuery: IndexQueryExpression): Option[String] = {
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
    import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
    import org.apache.spark.unsafe.types.UTF8String
    import org.apache.spark.sql.types.StringType

    indexQuery.left match {
      // Standard cases (same as base IndexQueryExpression)
      case attr: AttributeReference       => Some(attr.name)
      case UnresolvedAttribute(nameParts) => Some(nameParts.mkString("."))

      // V2-specific: Accept Literal expressions as column names in V2 parsing context
      case Literal(value: UTF8String, StringType) => Some(value.toString)
      case Literal(value: String, StringType)     => Some(value)

      case _ => None
    }
  }

  /** Extract query string from IndexQueryExpression with V2-specific handling. */
  private def extractQueryStringForV2(indexQuery: IndexQueryExpression): Option[String] =
    // Use the base implementation which already handles Literal expressions correctly
    indexQuery.getQueryString

}
