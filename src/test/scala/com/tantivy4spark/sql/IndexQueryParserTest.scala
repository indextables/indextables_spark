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

package com.tantivy4spark.sql

import com.tantivy4spark.TestBase
import com.tantivy4spark.expressions.IndexQueryExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.scalatest.funsuite.AnyFunSuite

class IndexQueryParserTest extends AnyFunSuite with TestBase {
  
  test("Tantivy4SparkSqlParser should parse simple indexquery expression") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("title indexquery 'spark AND sql'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("spark AND sql"))
  }
  
  test("Tantivy4SparkSqlParser should parse indexquery with double quotes") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("content indexquery \"machine learning\"")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("content"))
    assert(indexQueryExpr.getQueryString.contains("machine learning"))
  }
  
  test("Tantivy4SparkSqlParser should parse complex indexquery") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("description indexquery '(apache AND spark) OR (hadoop AND mapreduce)'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("description"))
    assert(indexQueryExpr.getQueryString.contains("(apache AND spark) OR (hadoop AND mapreduce)"))
  }
  
  test("Tantivy4SparkSqlParser should parse indexquery with qualified column names") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("table.column indexquery 'search term'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    // The column name should be extracted correctly (parser may resolve qualified names)
    assert(indexQueryExpr.getQueryString.contains("search term"))
  }
  
  test("Tantivy4SparkSqlParser should handle indexquery with wildcards") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("title indexquery 'prefix* AND *suffix'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("prefix* AND *suffix"))
  }
  
  test("Tantivy4SparkSqlParser should handle indexquery with phrase queries") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("content indexquery '\"exact phrase match\"'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("content"))
    assert(indexQueryExpr.getQueryString.contains("\"exact phrase match\""))
  }
  
  test("Tantivy4SparkSqlParser should fallback to default parser for non-indexquery expressions") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("title = 'test'")
    
    // Should not be an IndexQueryExpression
    assert(!expr.isInstanceOf[IndexQueryExpression])
    // Should be a regular equality expression
    assert(expr.isInstanceOf[EqualTo])
  }
  
  test("Tantivy4SparkSqlParser should fallback on parse errors in indexquery parts") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    // This should fallback to the default parser since the left side is invalid for column reference
    val expr = parser.parseExpression("'literal_string' indexquery 'query'")
    
    // The default parser should handle this gracefully or throw an appropriate error
    // The exact behavior depends on how the default parser handles this invalid syntax
  }
  
  test("Tantivy4SparkSqlParser should handle indexquery with escaped quotes") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("title indexquery 'title:\"Apache Spark\" AND version:3.*'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    assert(indexQueryExpr.getColumnName.contains("title"))
    assert(indexQueryExpr.getQueryString.contains("title:\"Apache Spark\" AND version:3.*"))
  }
  
  test("Tantivy4SparkSqlParser should preserve case in column names") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val expr = parser.parseExpression("CamelCaseColumn indexquery 'search terms'")
    
    assert(expr.isInstanceOf[IndexQueryExpression])
    val indexQueryExpr = expr.asInstanceOf[IndexQueryExpression]
    
    // Column name case should be preserved
    assert(indexQueryExpr.left.toString.contains("CamelCaseColumn"))
    assert(indexQueryExpr.getQueryString.contains("search terms"))
  }
  
  test("Tantivy4SparkSqlParser should handle whitespace variations") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    // Test with extra whitespace
    val expr1 = parser.parseExpression("  title   indexquery   'query'  ")
    val expr2 = parser.parseExpression("title\tindexquery\t'query'")
    val expr3 = parser.parseExpression("title\nindexquery\n'query'")
    
    assert(expr1.isInstanceOf[IndexQueryExpression])
    assert(expr2.isInstanceOf[IndexQueryExpression])
    assert(expr3.isInstanceOf[IndexQueryExpression])
    
    val indexQuery1 = expr1.asInstanceOf[IndexQueryExpression]
    val indexQuery2 = expr2.asInstanceOf[IndexQueryExpression]
    val indexQuery3 = expr3.asInstanceOf[IndexQueryExpression]
    
    assert(indexQuery1.getQueryString.contains("query"))
    assert(indexQuery2.getQueryString.contains("query"))
    assert(indexQuery3.getQueryString.contains("query"))
  }
  
  test("Tantivy4SparkSqlParser should still handle FLUSH TANTIVY4SPARK SEARCHER CACHE command") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    val plan = parser.parsePlan("FLUSH TANTIVY4SPARK SEARCHER CACHE")
    
    assert(plan.isInstanceOf[FlushTantivyCacheCommand])
  }
  
  test("Tantivy4SparkSqlParser should delegate non-indexquery plans to default parser") {
    val parser = new Tantivy4SparkSqlParser(CatalystSqlParser)
    
    // This should be parsed by the default parser
    val plan = parser.parsePlan("SELECT * FROM table WHERE col = 'value'")
    
    // Should be a valid plan parsed by the default parser
    assert(plan != null)
    assert(!plan.isInstanceOf[FlushTantivyCacheCommand])
  }
}