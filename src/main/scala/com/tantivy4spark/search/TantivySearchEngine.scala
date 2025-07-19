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

package com.tantivy4spark.search

import com.tantivy4spark.storage.DataLocation
import com.tantivy4spark.native.TantivyNative
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

case class SearchResult(
    docId: String,
    score: Float,
    dataLocation: DataLocation,
    highlights: Map[String, String] = Map.empty
)

case class TantivyIndex(
    name: String,
    fields: Map[String, String],
    segmentPaths: List[String]
)

case class TantivySearchHit(
    score: Double,
    document: Map[String, Any],
    snippet: Map[String, String]
)

case class TantivySearchResponse(
    hits: List[TantivySearchHit],
    totalHits: Long,
    elapsedTimeMicros: Long
)

class TantivySearchEngine(options: Map[String, String]) {
  
  private val indexCache = new mutable.LinkedHashMap[String, TantivyIndex]()
  private val maxResults = options.getOrElse("max.results", "1000").toInt
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  // Native engine management
  private var configId: Option[Long] = None
  private var engineId: Option[Long] = None
  
  // Initialize Tantivy configuration
  private def initializeConfig(): Long = {
    val config = Map(
      "base_path" -> options.getOrElse("tantivy.base.path", "./tantivy-data"),
      "index_config" -> Map(
        "index_id" -> options.getOrElse("index.id", "default_index"),
        "doc_mapping" -> Map(
          "mode" -> "strict"
        )
      )
    )
    
    val configJson = objectMapper.writeValueAsString(config)
    TantivyNative.createConfig(configJson)
  }
  
  def search(query: String, indexPath: String): Iterator[SearchResult] = {
    ensureInitialized(indexPath)
    
    engineId match {
      case Some(id) =>
        Try {
          val resultsJson = TantivyNative.search(id, query, maxResults)
          parseSearchResults(resultsJson)
        } match {
          case Success(results) => results
          case Failure(exception) =>
            println(s"Search failed: ${exception.getMessage}")
            Iterator.empty
        }
      case None =>
        println("Search engine not initialized")
        Iterator.empty
    }
  }
  
  private def ensureInitialized(indexPath: String): Unit = {
    if (configId.isEmpty) {
      configId = Some(initializeConfig())
    }
    
    if (engineId.isEmpty && configId.isDefined) {
      val id = TantivyNative.createSearchEngine(configId.get, indexPath)
      if (id >= 0) {
        engineId = Some(id)
      }
    }
  }
  
  private def parseSearchResults(jsonResponse: String): Iterator[SearchResult] = {
    Try {
      val response = objectMapper.readValue(jsonResponse, classOf[TantivySearchResponse])
      response.hits.map { hit =>
        SearchResult(
          docId = hit.document.getOrElse("_id", "").toString,
          score = hit.score.toFloat,
          dataLocation = DataLocation(
            bucket = extractBucket(hit.document),
            key = extractKey(hit.document),
            offset = extractOffset(hit.document),
            length = extractLength(hit.document)
          ),
          highlights = hit.snippet
        )
      }.iterator
    }.getOrElse(Iterator.empty)
  }
  
  private def extractBucket(document: Map[String, Any]): String = {
    document.getOrElse("_bucket", "default-bucket").toString
  }
  
  private def extractKey(document: Map[String, Any]): String = {
    document.getOrElse("_key", "").toString
  }
  
  private def extractOffset(document: Map[String, Any]): Long = {
    document.getOrElse("_offset", 0L) match {
      case n: Number => n.longValue()
      case s: String => Try(s.toLong).getOrElse(0L)
      case _ => 0L
    }
  }
  
  private def extractLength(document: Map[String, Any]): Long = {
    document.getOrElse("_length", 0L) match {
      case n: Number => n.longValue()
      case s: String => Try(s.toLong).getOrElse(0L)
      case _ => 0L
    }
  }
  
  private def loadOrCacheIndex(indexPath: String): TantivyIndex = {
    indexCache.get(indexPath) match {
      case Some(index) => index
      case None =>
        val index = loadIndexFromPath(indexPath)
        // Simple LRU: remove oldest if cache is full
        if (indexCache.size >= 10) {
          indexCache.remove(indexCache.head._1)
        }
        indexCache.put(indexPath, index)
        index
    }
  }
  
  private def loadIndexFromPath(indexPath: String): TantivyIndex = {
    // TODO: Implement actual Tantivy index loading
    // This would read the index metadata and segment information
    TantivyIndex(
      name = extractIndexName(indexPath),
      fields = Map.empty, // Would be populated from index metadata
      segmentPaths = List.empty // Would be populated from index structure
    )
  }
  
  def searchWithFilters(query: String, indexPath: String, filters: Map[String, String]): Iterator[SearchResult] = {
    val filterQuery = if (filters.nonEmpty) {
      val filterClauses = filters.map { case (field, value) => s"$field:$value" }.mkString(" AND ")
      s"($query) AND ($filterClauses)"
    } else {
      query
    }
    
    search(filterQuery, indexPath)
  }
  
  private def extractIndexName(indexPath: String): String = {
    indexPath.split("/").last.replace(".tantv", "")
  }
  
  def createIndex(indexName: String, schema: Map[String, String], indexPath: String): Unit = {
    // TODO: Implement index creation
    // This would create the Tantivy index structure with proper field mappings
  }
  
  def refreshIndex(indexPath: String): Unit = {
    indexCache.remove(indexPath)
    // Recreate search engine to pick up index changes
    engineId.foreach(TantivyNative.destroySearchEngine)
    engineId = None
  }
  
  def close(): Unit = {
    engineId.foreach(TantivyNative.destroySearchEngine)
    configId.foreach(TantivyNative.destroyConfig)
    engineId = None
    configId = None
  }
}