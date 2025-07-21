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

package com.tantivy4spark.core

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import com.tantivy4spark.search.TantivySearchEngine
import com.tantivy4spark.storage.S3OptimizedReader
import com.amazonaws.services.s3.AmazonS3

class TantivyFileReader(
    partitionedFile: PartitionedFile,
    requiredSchema: StructType,
    filters: Seq[org.apache.spark.sql.sources.Filter],
    options: Map[String, String],
    hadoopConf: Configuration,
    s3Client: Option[AmazonS3] = None
) {
  
  private val actualSchema = if (requiredSchema.isEmpty) {
    inferSchemaFromIndex()
  } else {
    requiredSchema
  }
  
  private val searchEngine = {
    println(s"[DEBUG] TantivyFileReader creating search engine with schema: ${actualSchema.fields.map(_.name).mkString(", ")}")
    new TantivySearchEngine(options, Some(actualSchema))
  }
  private val s3Reader = new S3OptimizedReader(hadoopConf, options, s3Client)
  
  private def inferSchemaFromIndex(): StructType = {
    import com.tantivy4spark.config.TantivyConfig
    import scala.util.{Success, Failure}
    
    val indexPath = partitionedFile.filePath.toString
    println(s"[DEBUG] Inferring schema from Tantivy index at: $indexPath")
    
    TantivyConfig.inferSchemaFromIndex(indexPath) match {
      case Success(schema) =>
        println(s"[DEBUG] Inferred schema with ${schema.fields.length} fields: ${schema.fields.map(_.name).mkString(", ")}")
        schema
      case Failure(exception) =>
        println(s"[WARNING] Failed to infer schema from index: ${exception.getMessage}")
        println(s"[DEBUG] Falling back to default schema")
        createDefaultSchema()
    }
  }
  
  private def createDefaultSchema(): StructType = {
    import org.apache.spark.sql.types._
    
    new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("content", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true)
    ))
  }
  
  def read(): Iterator[InternalRow] = {
    val query = buildQueryFromFilters(filters)
    val searchResults = searchEngine.search(query, partitionedFile.filePath.toString)
    
    searchResults.flatMap { result =>
      s3Reader.readWithPredictiveIO(result.dataLocation, actualSchema)
    }
  }
  
  def getSchema(): StructType = actualSchema
  
  private def buildQueryFromFilters(filters: Seq[org.apache.spark.sql.sources.Filter]): String = {
    import org.apache.spark.sql.sources._
    
    if (filters.isEmpty) {
      return "*"
    }
    
    val filterClauses = filters.map {
      case EqualTo(attribute, value) => 
        s"$attribute:${escapeValue(value.toString)}"
      
      case GreaterThan(attribute, value) => 
        s"$attribute:{${value.toString} TO *}"
      
      case GreaterThanOrEqual(attribute, value) => 
        s"$attribute:[${value.toString} TO *]"
      
      case LessThan(attribute, value) => 
        s"$attribute:{* TO ${value.toString}}"
      
      case LessThanOrEqual(attribute, value) => 
        s"$attribute:[* TO ${value.toString}]"
      
      case In(attribute, values) => 
        val valueList = values.map(v => escapeValue(v.toString)).mkString(" OR ")
        s"$attribute:($valueList)"
      
      case IsNull(attribute) => 
        s"NOT _exists_:$attribute"
      
      case IsNotNull(attribute) => 
        s"_exists_:$attribute"
      
      case StringStartsWith(attribute, value) => 
        s"$attribute:${escapeValue(value.toString)}*"
      
      case StringEndsWith(attribute, value) => 
        s"$attribute:*${escapeValue(value.toString)}"
      
      case StringContains(attribute, value) => 
        s"$attribute:*${escapeValue(value.toString)}*"
      
      case And(left, right) => 
        s"(${buildQueryFromFilters(Seq(left))}) AND (${buildQueryFromFilters(Seq(right))})"
      
      case Or(left, right) => 
        s"(${buildQueryFromFilters(Seq(left))}) OR (${buildQueryFromFilters(Seq(right))})"
      
      case Not(child) => 
        s"NOT (${buildQueryFromFilters(Seq(child))})"
      
      case _ => 
        // For unsupported filters, return a match-all query to avoid breaking the search
        "*"
    }
    
    // Join multiple filter clauses with AND
    filterClauses.filter(_ != "*").mkString(" AND ") match {
      case "" => "*"
      case query => query
    }
  }
  
  private def escapeValue(value: String): String = {
    // Escape special characters in Tantivy/Lucene query syntax
    value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace(":", "\\:")
      .replace("(", "\\(")
      .replace(")", "\\)")
      .replace("[", "\\[")
      .replace("]", "\\]")
      .replace("{", "\\{")
      .replace("}", "\\}")
      .replace("~", "\\~")
      .replace("^", "\\^")
      .replace("*", "\\*")
      .replace("?", "\\?")
      .replace("+", "\\+")
      .replace("-", "\\-")
      .replace("!", "\\!")
      .replace("|", "\\|")
      .replace("&", "\\&")
      .replace("/", "\\/")
  }
}