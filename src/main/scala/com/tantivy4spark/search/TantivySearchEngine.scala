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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.slf4j.LoggerFactory

class TantivySearchEngine private (private val directInterface: TantivyDirectInterface) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivySearchEngine])
  
  // Primary constructor for creating new search engines
  def this(schema: StructType) = this(new TantivyDirectInterface(schema))

  def addDocument(row: InternalRow): Unit = {
    directInterface.addDocument(row)
  }

  def addDocuments(rows: Iterator[InternalRow]): Unit = {
    directInterface.addDocuments(rows)
  }

  def commit(): Unit = {
    directInterface.commit()
  }
  
  def commitAndGetComponents(): Map[String, Array[Byte]] = {
    commit()
    directInterface.getIndexComponents()
  }

  def searchAll(limit: Int = 10000): Array[InternalRow] = {
    directInterface.searchAll(limit)
  }

  def search(query: String, limit: Int = 100): Array[InternalRow] = {
    directInterface.search(query, limit)
  }

  override def close(): Unit = {
    directInterface.close()
  }
}

object TantivySearchEngine {
  private val logger = LoggerFactory.getLogger(TantivySearchEngine.getClass)
  
  /**
   * Creates a TantivySearchEngine from pre-existing ZIP-based index components.
   * Uses the new ZIP-based restoration that contains real tantivy index files.
   */
  def fromIndexComponents(schema: StructType, components: Map[String, Array[Byte]]): TantivySearchEngine = {
    logger.info(s"Creating TantivySearchEngine from ${components.size} ZIP-based components")
    
    // Create a TantivyDirectInterface that restores from ZIP components
    val restoredInterface = TantivyDirectInterface.fromIndexComponents(schema, components)
    
    // Use the private constructor to wrap the restored interface
    new TantivySearchEngine(restoredInterface)
  }
  
  /**
   * Creates a TantivySearchEngine from a direct interface (for optimization).
   */
  def fromDirectInterface(directInterface: TantivyDirectInterface): TantivySearchEngine = {
    new TantivySearchEngine(directInterface)
  }
}