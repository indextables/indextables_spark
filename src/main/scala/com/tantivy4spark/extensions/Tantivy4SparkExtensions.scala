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

package com.tantivy4spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import com.tantivy4spark.sql.Tantivy4SparkSqlParser
import com.tantivy4spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}
import com.tantivy4spark.catalyst.V2IndexQueryExpressionRule

/**
 * Spark session extensions for Tantivy4Spark.
 *
 * This class registers custom parsers, optimizers, and other extensions with Spark to enable Tantivy4Spark-specific
 * functionality.
 *
 * Extensions provided:
 *   - Custom SQL parser for indexquery operator
 *   - FLUSH TANTIVY4SPARK SEARCHER CACHE command
 *
 * To use these extensions, configure your SparkSession:
 *
 * spark.conf.set("spark.sql.extensions", "com.tantivy4spark.extensions.Tantivy4SparkExtensions")
 *
 * Or in spark-defaults.conf: spark.sql.extensions=com.tantivy4spark.extensions.Tantivy4SparkExtensions
 */
class Tantivy4SparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject our custom parser to handle indexquery operator and custom commands
    extensions.injectParser((session, parser) => new Tantivy4SparkSqlParser(parser))

    // Register the tantivy4spark_indexquery function
    extensions.injectFunction(
      (
        FunctionIdentifier("tantivy4spark_indexquery"),
        new ExpressionInfo(
          "com.tantivy4spark.expressions.IndexQueryExpression",
          "tantivy4spark_indexquery",
          "tantivy4spark_indexquery(column, query) - Creates an IndexQuery expression for the given column and query string."
        ),
        (children: Seq[Expression]) =>
          if (children.length == 2) {
            IndexQueryExpression(children(0), children(1))
          } else {
            throw new IllegalArgumentException("tantivy4spark_indexquery requires exactly 2 arguments")
          }
      )
    )

    // Register the tantivy4spark_indexqueryall function
    extensions.injectFunction(
      (
        FunctionIdentifier("tantivy4spark_indexqueryall"),
        new ExpressionInfo(
          "com.tantivy4spark.expressions.IndexQueryAllExpression",
          "tantivy4spark_indexqueryall",
          "tantivy4spark_indexqueryall(query) - Creates an IndexQueryAll expression for searching across all fields."
        ),
        (children: Seq[Expression]) =>
          if (children.length == 1) {
            IndexQueryAllExpression(children(0))
          } else {
            throw new IllegalArgumentException("tantivy4spark_indexqueryall requires exactly 1 argument")
          }
      )
    )

    // Register V2 IndexQuery expression conversion rule in resolution phase
    // This runs during analysis, before scan planning
    extensions.injectResolutionRule(session => V2IndexQueryExpressionRule)

    // Future: Add planning strategies
    // extensions.injectPlannerStrategy { session =>
    //   // Custom planner strategies can go here
    // }
  }
}
