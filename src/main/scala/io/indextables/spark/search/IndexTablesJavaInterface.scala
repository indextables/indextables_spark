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

package io.indextables.spark.search

import io.indextables.tantivy4java.core.Tantivy
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory
import org.apache.spark.sql.catalyst.InternalRow
import java.util.concurrent.ConcurrentHashMap

/**
 * Simplified adapter that delegates to TantivyDirectInterface instances. Maintains compatibility with the old
 * handle-based API.
 */
object TantivyJavaAdapter {
  private val logger = LoggerFactory.getLogger(TantivyJavaAdapter.getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Simple interface storage for compatibility
  private val interfaces   = new ConcurrentHashMap[java.lang.Long, TantivyDirectInterface]()
  private val indexCounter = new java.util.concurrent.atomic.AtomicLong(1)

  def ensureLibraryLoaded(): Boolean =
    try {
      Tantivy.initialize()
      true
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize Tantivy4Java", e)
        false
    }

  def createIndex(schemaJson: String): Long =
    try {
      logger.info(s"Creating index with schema: $schemaJson")
      val sparkSchema = SchemaConverter.tantivyToSparkSchema(schemaJson)
      val interface   = new TantivyDirectInterface(sparkSchema)

      val indexId = indexCounter.getAndIncrement()
      interfaces.put(indexId, interface)

      logger.info(s"Created index with ID: $indexId")
      indexId
    } catch {
      case e: Exception =>
        logger.error("Failed to create index", e)
        -1L
    }

  def createIndexFromComponents(schemaJson: String, componentsJson: String): Long =
    // For tantivy4java, just create a new index - can't restore from components
    createIndex(schemaJson)

  def addDocument(indexHandle: Long, documentJson: String): Boolean =
    try
      Option(interfaces.get(indexHandle)) match {
        case Some(interface) =>
          val docMap = mapper.readValue(documentJson, classOf[Map[String, Any]])
          val row    = RowConverter.jsonToInternalRow(docMap, interface.schema)
          interface.addDocument(row)
          true
        case None =>
          logger.error(s"Index not found: $indexHandle")
          false
      }
    catch {
      case e: Exception =>
        logger.error("Failed to add document", e)
        false
    }

  def commit(indexHandle: Long): Boolean =
    try
      Option(interfaces.get(indexHandle)) match {
        case Some(interface) =>
          interface.commit()
          true
        case None =>
          logger.error(s"Index not found: $indexHandle")
          false
      }
    catch {
      case e: Exception =>
        logger.error("Failed to commit", e)
        false
    }

  def search(
    indexHandle: Long,
    query: String,
    limit: Int
  ): String =
    try
      Option(interfaces.get(indexHandle)) match {
        case Some(interface) =>
          val results = interface.search(query, limit)
          val hits = results.map { row =>
            val docMap = RowConverter.internalRowToMap(row, interface.schema)
            Map("doc" -> docMap, "score" -> 1.0)
          }.toList
          mapper.writeValueAsString(Map("hits" -> hits))
        case None =>
          logger.error(s"Index not found: $indexHandle")
          mapper.writeValueAsString(Map("hits" -> List.empty))
      }
    catch {
      case e: Exception =>
        logger.error("Failed to search", e)
        mapper.writeValueAsString(Map("hits" -> List.empty))
    }

  def searchAll(indexHandle: Long, limit: Int): String =
    search(indexHandle, "*:*", limit)

  def closeIndex(indexHandle: Long): Boolean =
    try
      Option(interfaces.remove(indexHandle)) match {
        case Some(interface) =>
          interface.close()
          true
        case None =>
          logger.warn(s"Index not found for closing: $indexHandle")
          true
      }
    catch {
      case e: Exception =>
        logger.error("Failed to close index", e)
        false
    }

  // Note: getIndexComponents removed - using splits instead of archive components
}

// Compatibility object that maintains the same interface as the old TantivyNative
object TantivyNative {
  def ensureLibraryLoaded(): Boolean        = TantivyJavaAdapter.ensureLibraryLoaded()
  def createIndex(schemaJson: String): Long = TantivyJavaAdapter.createIndex(schemaJson)
  def createIndexFromComponents(schemaJson: String, componentsJson: String): Long =
    TantivyJavaAdapter.createIndexFromComponents(schemaJson, componentsJson)
  def addDocument(indexHandle: Long, documentJson: String): Boolean =
    TantivyJavaAdapter.addDocument(indexHandle, documentJson)
  def commit(indexHandle: Long): Boolean = TantivyJavaAdapter.commit(indexHandle)
  def search(
    indexHandle: Long,
    query: String,
    limit: Int
  ): String =
    TantivyJavaAdapter.search(indexHandle, query, limit)
  def searchAll(indexHandle: Long, limit: Int): String =
    TantivyJavaAdapter.searchAll(indexHandle, limit)
  def closeIndex(indexHandle: Long): Boolean = TantivyJavaAdapter.closeIndex(indexHandle)
  // Note: getIndexComponents removed - using splits instead of archive components
}
