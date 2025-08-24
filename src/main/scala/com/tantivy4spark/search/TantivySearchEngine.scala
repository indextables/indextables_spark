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
  
  def commitAndCreateSplit(outputPath: String, partitionId: Long, nodeId: String): String = {
    // Use commitAndClose to follow write-only pattern for production
    directInterface.commitAndClose()
    
    try {
      // Delay cleanup of temporary directory until after split creation
      directInterface.delayCleanupForSplit()
      
      // Get temporary directory path from the direct interface
      val tempIndexPath = directInterface.getIndexPath()
      
      // Use SplitManager to create split from the temporary index
      import com.tantivy4spark.storage.SplitManager
      val metadata = SplitManager.createSplit(tempIndexPath, outputPath, partitionId, nodeId)
      
      // Return the split file path
      outputPath
      
    } finally {
      // Force cleanup now that split creation is complete
      directInterface.forceCleanup()
    }
  }

  def searchAll(limit: Int = Int.MaxValue): Array[InternalRow] = {
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
   * Creates a TantivySearchEngine from a split file.
   * This replaces the previous ZIP-based component system.
   */
  def fromSplitFile(schema: StructType, splitPath: String): SplitSearchEngine = {
    logger.info(s"Creating SplitSearchEngine from split file: $splitPath")
    
    import com.tantivy4spark.storage.SplitCacheConfig
    SplitSearchEngine.fromSplitFile(schema, splitPath, SplitCacheConfig())
  }
  
  /**
   * Creates a TantivySearchEngine from a split file with custom cache configuration.
   */
  def fromSplitFileWithCache(schema: StructType, splitPath: String, cacheConfig: com.tantivy4spark.storage.SplitCacheConfig): SplitSearchEngine = {
    logger.info(s"Creating SplitSearchEngine from split file with cache: $splitPath")
    
    SplitSearchEngine.fromSplitFile(schema, splitPath, cacheConfig)
  }
  
  /**
   * Creates a TantivySearchEngine from a direct interface (for write operations).
   */
  def fromDirectInterface(directInterface: TantivyDirectInterface): TantivySearchEngine = {
    new TantivySearchEngine(directInterface)
  }
}