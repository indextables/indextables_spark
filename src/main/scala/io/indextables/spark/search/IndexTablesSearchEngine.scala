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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TantivySearchEngine private (
  private val directInterface: TantivyDirectInterface,
  private val options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
  private val configMap: Map[String, String] = Map.empty)
    extends AutoCloseable {

  // Primary constructor for creating new search engines (schema only)
  def this(schema: StructType) = this(new TantivyDirectInterface(schema))

  // Constructor with cloud storage support (Map-based config - fast path, no HadoopConf)
  def this(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    config: Map[String, String]
  ) =
    this(
      {
        // Extract working directory from configuration hierarchy with auto-detection
        val workingDirectory = Option(options.get("spark.indextables.indexWriter.tempDirectoryPath"))
          .orElse(config.get("spark.indextables.indexWriter.tempDirectoryPath"))
          .orElse(io.indextables.spark.storage.SplitCacheConfig.getDefaultTempPath())

        new TantivyDirectInterface(schema, None, options, config, None, workingDirectory)
      },
      options,
      config
    )

  def addDocument(row: InternalRow): Unit =
    directInterface.addDocument(row)

  def addDocuments(rows: Iterator[InternalRow]): Unit =
    directInterface.addDocuments(rows)

  def commit(): Unit =
    directInterface.commit()

  def commitAndCreateSplit(
    outputPath: String,
    partitionId: Long,
    nodeId: String
  ): (String, io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata) = {
    // Use commitAndClose to follow write-only pattern for production
    directInterface.commitAndClose()

    try {
      // Delay cleanup of temporary directory until after split creation
      directInterface.delayCleanupForSplit()

      // Get temporary directory path from the direct interface
      val tempIndexPath = directInterface.getIndexPath()

      // Use SplitManager to create split from the temporary index with Map-based config (fast path)
      import io.indextables.spark.storage.SplitManager
      val metadata = SplitManager.createSplit(tempIndexPath, outputPath, partitionId, nodeId, configMap)

      // Return both the split file path and metadata with footer offsets
      (outputPath, metadata)

    } finally
      // Force cleanup now that split creation is complete
      directInterface.forceCleanup()
  }

  // Search methods removed - use SplitSearchEngine for reading from splits
  // The write-only architecture means TantivySearchEngine only creates indexes,
  // not reads from them

  override def close(): Unit =
    directInterface.close()
}

object TantivySearchEngine {

  /** Creates a TantivySearchEngine from a direct interface (for write operations). */
  def fromDirectInterface(directInterface: TantivyDirectInterface): TantivySearchEngine =
    new TantivySearchEngine(directInterface)
}
