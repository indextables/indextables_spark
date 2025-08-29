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

package com.tantivy4spark.catalyst

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import com.tantivy4spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}
import com.tantivy4spark.filters.{IndexQueryV2Filter, IndexQueryAllV2Filter, IndexQueryFilter, IndexQueryAllFilter}
import scala.collection.mutable

/**
 * Catalyst rule to convert IndexQuery expressions to V2-compatible filters.
 * 
 * This rule handles V2 DataSource API paths by intercepting IndexQueryExpression
 * and IndexQueryAllExpression before they reach Spark's built-in expression-to-filter
 * translation mechanism, and directly injects appropriate filters into the
 * ScanBuilder via a custom interface.
 * 
 * The conversion process:
 * 1. Detects IndexQueryExpression/IndexQueryAllExpression in V2 filter conditions
 * 2. Converts them to custom IndexQueryV2Filter marker expressions
 * 3. These markers are detected by our ScanBuilder and converted to IndexQueryFilter objects
 * 4. The filters are then pushed down via the normal V2 pushdown mechanism
 */
object V2IndexQueryExpressionRule extends Rule[LogicalPlan] {
  
  // ThreadLocal to store extracted IndexQuery filters for the current thread/query
  private val extractedFilters = new ThreadLocal[mutable.ArrayBuffer[Any]] {
    override def initialValue(): mutable.ArrayBuffer[Any] = 
      mutable.ArrayBuffer.empty[Any]
  }
  
  /**
   * Get the IndexQuery filters extracted for the current thread.
   * This is called by the ScanBuilder during pushdown.
   */
  def getExtractedFilters(): Array[Any] = {
    val filters = extractedFilters.get().toArray
    extractedFilters.get().clear() // Clear after retrieval to avoid reuse
    filters
  }
  
  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(s"🔍 V2IndexQueryExpressionRule.apply() called with plan: ${plan.getClass.getSimpleName}")
    
    val result = plan.transformUp {
      case filter@Filter(condition, child: DataSourceV2Relation) =>
        println(s"🔍 V2IndexQueryExpressionRule: Found Filter with DataSourceV2Relation")
        println(s"🔍 V2IndexQueryExpressionRule: Condition: $condition")
        println(s"🔍 V2IndexQueryExpressionRule: Child table: ${child.table.name()}")
        
        // Only apply to V2 DataSource relations
        if (isCompatibleV2DataSource(child)) {
          println(s"🔍 V2IndexQueryExpressionRule: Compatible V2 DataSource detected")
          val (convertedCondition, extractedIndexQueryFilters) = convertExpressionAndExtractFilters(condition, child)
          
          println(s"🔍 V2IndexQueryExpressionRule: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters")
          extractedIndexQueryFilters.foreach(f => println(s"  - Extracted: $f"))
          
          // Store the extracted filters in ThreadLocal for the ScanBuilder to pick up
          extractedFilters.get() ++= extractedIndexQueryFilters
          
          if (convertedCondition != condition) {
            println(s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
            Filter(convertedCondition, child)
          } else {
            println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
            filter
          }
        } else {
          println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource")
          filter
        }
      case filter@Filter(condition, child: SubqueryAlias) =>
        // Handle SubqueryAlias wrapping a DataSourceV2Relation (temp views)
        child.child match {
          case v2Relation: DataSourceV2Relation =>
            println(s"🔍 V2IndexQueryExpressionRule: Found Filter with SubqueryAlias wrapping DataSourceV2Relation")
            println(s"🔍 V2IndexQueryExpressionRule: Condition: $condition")
            println(s"🔍 V2IndexQueryExpressionRule: Child table: ${v2Relation.table.name()}")
            
            // Only apply to V2 DataSource relations
            if (isCompatibleV2DataSource(v2Relation)) {
              println(s"🔍 V2IndexQueryExpressionRule: Compatible V2 DataSource detected (via SubqueryAlias)")
              val (convertedCondition, extractedIndexQueryFilters) = convertExpressionAndExtractFilters(condition, v2Relation)
              
              println(s"🔍 V2IndexQueryExpressionRule: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters")
              extractedIndexQueryFilters.foreach(f => println(s"  - Extracted: $f"))
              
              // Store the extracted filters in ThreadLocal for the ScanBuilder to pick up
              extractedFilters.get() ++= extractedIndexQueryFilters
              
              if (convertedCondition != condition) {
                println(s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
                Filter(convertedCondition, child)
              } else {
                println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
                filter
              }
            } else {
              println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource")
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
                  val (convertedCondition, extractedIndexQueryFilters) = convertExpressionAndExtractFilters(condition, v2Relation)
                  
                  println(s"🔍 V2IndexQueryExpressionRule: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters")
                  extractedIndexQueryFilters.foreach(f => println(s"  - Extracted: $f"))
                  
                  // Store the extracted filters in ThreadLocal for the ScanBuilder to pick up
                  extractedFilters.get() ++= extractedIndexQueryFilters
                  
                  if (convertedCondition != condition) {
                    println(s"🔍 V2IndexQueryExpressionRule: Condition was converted from $condition to $convertedCondition")
                    Filter(convertedCondition, child)
                  } else {
                    println(s"🔍 V2IndexQueryExpressionRule: No conversion needed")
                    filter
                  }
                } else {
                  println(s"🔍 V2IndexQueryExpressionRule: Not compatible V2 DataSource")
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
      case filter@Filter(condition, child) =>
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
              
              // Process all expressions in this plan
              val processedExpressions = plan.expressions.map { expr =>
                val (convertedExpr, extractedIndexQueryFilters) = convertExpressionAndExtractFilters(expr, relation)
                
                if (extractedIndexQueryFilters.nonEmpty) {
                  println(s"🔍 V2IndexQueryExpressionRule: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters from expression")
                  extractedIndexQueryFilters.foreach(f => println(s"  - Extracted: $f"))
                  
                  // Store the extracted filters in ThreadLocal for the ScanBuilder to pick up
                  extractedFilters.get() ++= extractedIndexQueryFilters
                }
                
                convertedExpr
              }
              
              // Transform the plan with the converted expressions if any changed
              if (processedExpressions != plan.expressions) {
                println(s"🔍 V2IndexQueryExpressionRule: Transforming plan expressions")
                plan.withNewChildren(plan.children).transformExpressions {
                  case expr if plan.expressions.contains(expr) =>
                    val index = plan.expressions.indexOf(expr)
                    if (index >= 0 && index < processedExpressions.length) {
                      processedExpressions(index)
                    } else {
                      expr
                    }
                }
              } else {
                plan
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
  
  /**
   * Check if an expression contains IndexQuery expressions recursively
   */
  private def containsIndexQueryExpression(expr: Expression): Boolean = {
    expr match {
      case _: IndexQueryExpression => true
      case _: IndexQueryAllExpression => true
      case _ => expr.children.exists(containsIndexQueryExpression)
    }
  }
  
  /**
   * Check if this is a compatible V2 DataSource (Tantivy4Spark)
   */
  private def isCompatibleV2DataSource(relation: DataSourceV2Relation): Boolean = {
    // Check if this is a Tantivy4Spark V2 table
    relation.table.getClass.getName.contains("tantivy4spark") ||
    relation.table.name().contains("tantivy4spark")
  }
  
  /**
   * Convert IndexQuery expressions to V2-compatible marker expressions AND extract filters
   */
  private def convertExpressionAndExtractFilters(expr: Expression, relation: DataSourceV2Relation): 
    (Expression, mutable.ArrayBuffer[Any]) = {
    
    println(s"🔍 V2IndexQueryExpressionRule: convertExpressionAndExtractFilters called with expression: $expr")
    println(s"🔍 V2IndexQueryExpressionRule: Expression type: ${expr.getClass.getName}")
    println(s"🔍 V2IndexQueryExpressionRule: Expression children: ${expr.children}")
    expr.children.foreach(child => println(s"  - Child: $child (${child.getClass.getName})"))
    
    val extractedFilters = mutable.ArrayBuffer.empty[Any]
    val availableAttributes = relation.output
    
    val convertedExpr = expr.transformUp {
      case indexQuery: IndexQueryExpression =>
        println(s"🔍 V2IndexQueryExpressionRule: Found IndexQueryExpression: $indexQuery")
        // Extract the filter information before conversion using V2-specific extraction
        for {
          columnName <- extractColumnNameForV2(indexQuery)
          queryString <- extractQueryStringForV2(indexQuery)
        } {
          // Check if this is an _indexall virtual column query (should be converted to IndexQueryAllFilter)
          if (columnName == "_indexall") {
            val indexQueryAllFilter = IndexQueryAllFilter(queryString)
            extractedFilters += indexQueryAllFilter
            println(s"🔍 V2IndexQueryExpressionRule: Converting _indexall IndexQuery to IndexQueryAllFilter")
          } else {
            val indexQueryFilter = IndexQueryFilter(columnName, queryString)
            extractedFilters += indexQueryFilter
          }
        }
        
        // Convert IndexQueryExpression to a built-in expression that Spark can translate to Filter
        // Use IsNotNull as a dummy expression that will trigger pushFilters() call
        // The actual IndexQuery filtering will be handled via ThreadLocal
        import org.apache.spark.sql.catalyst.expressions.{IsNotNull, AttributeReference}
        indexQuery.children.headOption match {
          case Some(attr: AttributeReference) =>
            println(s"🔍 V2IndexQueryExpressionRule: Converting IndexQuery to IsNotNull($attr) as bridge")
            IsNotNull(attr)
          case _ =>
            // Fallback - create a marker expression
            IndexQueryV2Filter(indexQuery)
        }
        
      case indexQueryAll: IndexQueryAllExpression =>
        println(s"🔍 V2IndexQueryExpressionRule: Found IndexQueryAllExpression: $indexQueryAll")
        // Extract the filter information before conversion
        indexQueryAll.getQueryString.foreach { queryString =>
          val indexQueryAllFilter = IndexQueryAllFilter(queryString)
          extractedFilters += indexQueryAllFilter
        }
        
        // Convert IndexQueryAllExpression to a built-in expression that Spark can translate to Filter
        // Since IndexQueryAll searches all fields, we'll use IsNotNull on the first available column as a bridge
        import org.apache.spark.sql.catalyst.expressions.{IsNotNull, Literal}
        println(s"🔍 V2IndexQueryExpressionRule: Available attributes count: ${availableAttributes.length}")
        availableAttributes.foreach(attr => println(s"  - Available attribute: $attr"))
        
        if (availableAttributes.nonEmpty) {
          val firstColumn = availableAttributes.head
          println(s"🔍 V2IndexQueryExpressionRule: Converting IndexQueryAll to IsNotNull($firstColumn) as bridge")
          IsNotNull(firstColumn)
        } else {
          // Fallback to a simple literal if no columns are available
          println(s"🔍 V2IndexQueryExpressionRule: Converting IndexQueryAll to Literal(true) as fallback bridge")
          Literal(true)
        }
    }
    
    (convertedExpr, extractedFilters)
  }
  
  /**
   * Extract column name from IndexQueryExpression with V2-specific handling.
   * In V2 context, column names may come through as Literal expressions from SQL parsing.
   */
  private def extractColumnNameForV2(indexQuery: IndexQueryExpression): Option[String] = {
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
    import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
    import org.apache.spark.unsafe.types.UTF8String
    import org.apache.spark.sql.types.StringType
    
    indexQuery.left match {
      // Standard cases (same as base IndexQueryExpression)
      case attr: AttributeReference => Some(attr.name)
      case UnresolvedAttribute(nameParts) => Some(nameParts.mkString("."))
      
      // V2-specific: Accept Literal expressions as column names in V2 parsing context
      case Literal(value: UTF8String, StringType) => Some(value.toString)
      case Literal(value: String, StringType) => Some(value)
      
      case _ => None
    }
  }
  
  /**
   * Extract query string from IndexQueryExpression with V2-specific handling.
   */
  private def extractQueryStringForV2(indexQuery: IndexQueryExpression): Option[String] = {
    // Use the base implementation which already handles Literal expressions correctly
    indexQuery.getQueryString
  }
}