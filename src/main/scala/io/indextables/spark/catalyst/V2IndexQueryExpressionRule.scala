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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import io.indextables.spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}

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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(s"🔍 V2IndexQueryExpressionRule.apply() called with plan: ${plan.getClass.getSimpleName}")

    val result = plan.transformUp {
      case filter @ Filter(condition, child: DataSourceV2Relation) =>
        println(s"🔍 V2IndexQueryExpressionRule: Found Filter with DataSourceV2Relation")
        println(s"🔍 V2IndexQueryExpressionRule: Condition: $condition")
        println(s"🔍 V2IndexQueryExpressionRule: Child table: ${child.table.name()}")

        // Only apply to V2 DataSource relations
        if (isCompatibleV2DataSource(child)) {
          println(s"🔍 V2IndexQueryExpressionRule: Compatible V2 DataSource detected")
          val convertedCondition = convertIndexQueryExpressions(condition, child)

          if (convertedCondition != condition) {
            println(s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
            Filter(convertedCondition, child)
          } else {
            println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
            filter
          }
        } else {
          println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource - REJECTING IndexQuery")
          // Check if condition contains IndexQuery to warn user
          if (containsIndexQueryExpression(condition)) {
            println(s"⚠️  WARNING: IndexQuery expression detected but rejected because table is not compatible V2 DataSource")
            println(s"⚠️  Table class: ${child.table.getClass.getName}, Table name: ${child.table.name()}")
          }
          filter
        }
      case filter @ Filter(condition, child: SubqueryAlias) =>
        // Handle SubqueryAlias wrapping a DataSourceV2Relation (temp views)
        child.child match {
          case v2Relation: DataSourceV2Relation =>
            println(s"🔍 V2IndexQueryExpressionRule: Found Filter with SubqueryAlias wrapping DataSourceV2Relation")
            println(s"🔍 V2IndexQueryExpressionRule: Condition: $condition")
            println(s"🔍 V2IndexQueryExpressionRule: Child table: ${v2Relation.table.name()}")

            // Only apply to V2 DataSource relations
            if (isCompatibleV2DataSource(v2Relation)) {
              println(s"🔍 V2IndexQueryExpressionRule: Compatible V2 DataSource detected (via SubqueryAlias)")
              val convertedCondition = convertIndexQueryExpressions(condition, v2Relation)

              if (convertedCondition != condition) {
                println(s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
                Filter(convertedCondition, child)
              } else {
                println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
                filter
              }
            } else {
              println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource - REJECTING IndexQuery")
              // Check if condition contains IndexQuery to warn user
              if (containsIndexQueryExpression(condition)) {
                println(s"⚠️  WARNING: IndexQuery expression detected but rejected because table is not compatible V2 DataSource")
                println(s"⚠️  Table class: ${v2Relation.table.getClass.getName}, Table name: ${v2Relation.table.name()}")
              }
              filter
            }
          case view: View =>
            // Handle View wrapping a DataSourceV2Relation (temp views)
            view.child match {
              case v2Relation: DataSourceV2Relation =>
                println(s"🔍 V2IndexQueryExpressionRule: Found Filter with SubqueryAlias wrapping View wrapping DataSourceV2Relation")
                println(s"🔍 V2IndexQueryExpressionRule: Condition: $condition")
                println(s"🔍 V2IndexQueryExpressionRule: Child table: ${v2Relation.table.name()}")

                // Only apply to V2 DataSource relations
                if (isCompatibleV2DataSource(v2Relation)) {
                  println(s"🔍 V2IndexQueryExpressionRule: Compatible V2 DataSource detected (via SubqueryAlias->View)")
                  val convertedCondition = convertIndexQueryExpressions(condition, v2Relation)

                  if (convertedCondition != condition) {
                    println(
                      s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition"
                    )
                    Filter(convertedCondition, child)
                  } else {
                    println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
                    filter
                  }
                } else {
                  println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource - REJECTING IndexQuery")
                  // Check if condition contains IndexQuery to warn user
                  if (containsIndexQueryExpression(condition)) {
                    println(s"⚠️  WARNING: IndexQuery expression detected but rejected because table is not compatible V2 DataSource")
                    println(s"⚠️  Table class: ${v2Relation.table.getClass.getName}, Table name: ${v2Relation.table.name()}")
                  }
                  filter
                }
              case _ =>
                println(s"🔍 V2IndexQueryExpressionRule: Found Filter with SubqueryAlias wrapping View wrapping non-V2 relation: ${view.child.getClass.getSimpleName}")
                filter
            }
          case _ =>
            println(s"🔍 V2IndexQueryExpressionRule: Found Filter with SubqueryAlias wrapping non-V2 relation: ${child.child.getClass.getSimpleName}")
            filter
        }
      case filter @ Filter(condition, child) =>
        println(s"🔍 V2IndexQueryExpressionRule: Found Filter with non-V2 relation: ${child.getClass.getSimpleName}")
        // For other plans, don't modify
        filter

      // Handle other plan types that might contain filters
      case plan: LogicalPlan =>
        // Look for IndexQuery expressions anywhere in the plan's expressions
        val hasIndexQuery = plan.expressions.exists(containsIndexQueryExpression)
        if (hasIndexQuery) {
          println(s"🔍 V2IndexQueryExpressionRule: Found IndexQuery expressions in ${plan.getClass.getSimpleName} plan")

          // Find the DataSourceV2Relation in the plan tree
          val v2Relations = plan.collect { case relation: DataSourceV2Relation => relation }
          v2Relations.find(isCompatibleV2DataSource) match {
            case Some(relation) =>
              println(s"🔍 V2IndexQueryExpressionRule: Found compatible V2 DataSource in plan tree")

              // Transform the plan to convert IndexQuery expressions
              plan.transformExpressions {
                case expr if containsIndexQueryExpression(expr) =>
                  convertIndexQueryExpressions(expr, relation)
              }

            case None =>
              println(s"🔍 V2IndexQueryExpressionRule: No compatible V2 DataSource found in plan tree")
              plan
          }
        } else {
          plan
        }
    }

    println(s"🔍 V2IndexQueryExpressionRule.apply() completed")
    result
  }

  /** Check if an expression contains IndexQuery expressions recursively */
  private def containsIndexQueryExpression(expr: Expression): Boolean =
    expr match {
      case _: IndexQueryExpression    => true
      case _: IndexQueryAllExpression => true
      case _                          => expr.children.exists(containsIndexQueryExpression)
    }

  /** Check if this is a compatible V2 DataSource (IndexTables4Spark) */
  private def isCompatibleV2DataSource(relation: DataSourceV2Relation): Boolean =
    // Check if this is a IndexTables4Spark V2 table (support both old and new package names)
    relation.table.getClass.getName.contains("indextables") ||
      relation.table.getClass.getName.contains("tantivy4spark") ||
      relation.table.name().contains("indextables") ||
      relation.table.name().contains("tantivy4spark")

  /**
   * Convert IndexQuery expressions and store them for the ScanBuilder to retrieve. This eliminates the fake filter
   * mechanism in favor of direct storage.
   */
  private def convertIndexQueryExpressions(
    expr: Expression,
    relation: DataSourceV2Relation
  ): Expression = {
    println(s"🔍 V2IndexQueryExpressionRule: convertIndexQueryExpressions called with expression: $expr")

    // Generate stable relation key from output attributes
    val relationKey = generateRelationKeyForV2(relation)
    println(s"🔍 V2IndexQueryExpressionRule: Generated relationKey='$relationKey'")

    val indexQueries = scala.collection.mutable.Buffer[Any]()

    val transformedExpr = expr.transformUp {
      case indexQuery: IndexQueryExpression =>
        println(s"🔍 V2IndexQueryExpressionRule: Found IndexQueryExpression: $indexQuery")

        (extractColumnNameForV2(indexQuery), extractQueryStringForV2(indexQuery)) match {
          case (Some(columnName), Some(queryString)) =>
            import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

            if (columnName == "_indexall") {
              println(s"🔍 V2IndexQueryExpressionRule: Storing _indexall IndexQuery")
              indexQueries += IndexQueryAllFilter(queryString)
            } else {
              println(s"🔍 V2IndexQueryExpressionRule: Storing IndexQuery")
              indexQueries += IndexQueryFilter(columnName, queryString)
            }

            // Return Literal(true) so the condition is always satisfied at expression level
            import org.apache.spark.sql.catalyst.expressions.Literal
            Literal(true)
          case _ =>
            println(
              s"🔍 V2IndexQueryExpressionRule: Unable to extract column/query from IndexQuery, using Literal(true)"
            )
            import org.apache.spark.sql.catalyst.expressions.Literal
            Literal(true)
        }

      case indexQueryAll: IndexQueryAllExpression =>
        println(s"🔍 V2IndexQueryExpressionRule: Found IndexQueryAllExpression: $indexQueryAll")

        indexQueryAll.getQueryString match {
          case Some(queryString) =>
            println(s"🔍 V2IndexQueryExpressionRule: Storing IndexQueryAll")
            import io.indextables.spark.filters.IndexQueryAllFilter
            indexQueries += IndexQueryAllFilter(queryString)

            // Return Literal(true) so the condition is always satisfied at expression level
            import org.apache.spark.sql.catalyst.expressions.Literal
            Literal(true)
          case _ =>
            println(s"🔍 V2IndexQueryExpressionRule: Unable to extract query from IndexQueryAll, using Literal(true)")
            import org.apache.spark.sql.catalyst.expressions.Literal
            Literal(true)
        }
    }

    // Store the collected IndexQueries for this relation
    if (indexQueries.nonEmpty) {
      import io.indextables.spark.core.IndexTables4SparkScanBuilder
      IndexTables4SparkScanBuilder.storeIndexQueries(relationKey, indexQueries.toSeq)
      println(
        s"🔍 V2IndexQueryExpressionRule: Stored ${indexQueries.length} IndexQuery expressions for relationKey='$relationKey'"
      )
    }

    transformedExpr
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

  /** Generate a stable relation key from the relation's output attributes. */
  private def generateRelationKeyForV2(relation: DataSourceV2Relation): String = {
    println(s"INDEXQUERY_DEBUG 🔑 V2 RULE KEY GEN: Generating key from relation output")

    // Convert output attributes to StructType to match what ScanBuilder uses
    import org.apache.spark.sql.types.{StructType, StructField}
    val schema = StructType(relation.output.map(attr =>
      StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    ))

    // Use the same key generation logic as ScanBuilder
    import io.indextables.spark.core.IndexTables4SparkScanBuilder
    val key = IndexTables4SparkScanBuilder.generateRelationKey(schema)

    println(s"INDEXQUERY_DEBUG 🔑 V2 RULE KEY GEN: Generated key='$key' from ${relation.output.length} attributes")
    key
  }

}
