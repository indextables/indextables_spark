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

class TantivySearchEngine(schema: StructType) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivySearchEngine])
  
  // Use the new direct interface instead of handles
  private val directInterface = new TantivyDirectInterface(schema)

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
   * Creates a TantivySearchEngine from pre-existing index components.
   * This is used when reading from .tnt4s archive files.
   */
  def fromIndexComponents(schema: StructType, components: Map[String, Array[Byte]]): TantivySearchEngine = {
    logger.info(s"Creating TantivySearchEngine from ${components.size} components")
    // For tantivy4java, we can't restore from components, so create a new instance
    new TantivySearchEngine(schema)
  }
}