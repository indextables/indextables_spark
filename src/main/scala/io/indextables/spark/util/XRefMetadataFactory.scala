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

package io.indextables.spark.util

import io.indextables.spark.transaction.AddXRefAction
import io.indextables.tantivy4java.xref.XRefMetadata
import org.slf4j.LoggerFactory

/**
 * Factory for creating XRefMetadata from AddXRefAction transaction log entries.
 *
 * Used for opening FuseXRef splits with the new XRefSearcher API.
 */
object XRefMetadataFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates XRefMetadata from an AddXRefAction.
   *
   * Since AddXRefAction doesn't contain all the fields needed for a full XRefMetadata,
   * this method creates a minimal XRefMetadata with the required footer offsets and
   * split information needed for XRefSearcher.open().
   *
   * @param xrefAction
   *   The transaction log XRef add action
   * @return
   *   XRefMetadata instance suitable for XRefSearcher.open()
   */
  def fromAddXRefAction(xrefAction: AddXRefAction): XRefMetadata = {
    logger.debug(s"Creating XRefMetadata for XRef ${xrefAction.xrefId}")

    // Parse the stored JSON metadata if available
    // If the XRef was built with the new FuseXRef API, it should have full metadata stored
    // For backward compatibility, we create a minimal metadata from action fields

    try {
      // Try to parse the JSON if we stored it (future enhancement)
      // For now, create from the action fields
      createMinimalMetadata(xrefAction)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse XRef metadata JSON, creating minimal: ${e.getMessage}")
        createMinimalMetadata(xrefAction)
    }
  }

  /**
   * Create a minimal XRefMetadata from AddXRefAction fields.
   *
   * This creates the minimum metadata needed for XRefSearcher.open() to work:
   * - Footer offsets for efficient file opening
   * - Source split information for result interpretation
   */
  private def createMinimalMetadata(xrefAction: AddXRefAction): XRefMetadata = {
    // Create a JSON string that XRefMetadata.fromJson can parse
    // This mirrors the structure returned by XRefSplit.build()
    val splitRegistryJson = {
      val splits = xrefAction.sourceSplitPaths.map { path =>
        val fileName = io.indextables.spark.xref.XRefStorageUtils.extractFileName(path)
        s"""{"uri":"$path","split_id":"$fileName","num_docs":0,"footer_start":0,"footer_end":0}"""
      }.mkString(",")
      s"""{"splits":[$splits]}"""
    }

    val metadataJson = s"""{
      "format_version": 1,
      "xref_id": "${xrefAction.xrefId}",
      "index_uid": "tantivy4spark-xref-index",
      "split_registry": $splitRegistryJson,
      "fields": [],
      "total_terms": ${xrefAction.totalTerms},
      "build_stats": {
        "build_duration_ms": ${xrefAction.buildDurationMs},
        "bytes_read": 0,
        "output_size_bytes": ${xrefAction.size},
        "compression_ratio": 1.0,
        "splits_processed": ${xrefAction.sourceSplitCount},
        "splits_skipped": 0,
        "unique_terms": ${xrefAction.totalTerms}
      },
      "created_at": ${xrefAction.createdTime / 1000},
      "footer_start_offset": ${xrefAction.footerStartOffset},
      "footer_end_offset": ${xrefAction.footerEndOffset}
    }"""

    try {
      XRefMetadata.fromJson(metadataJson)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create XRefMetadata from JSON: ${e.getMessage}")
        throw new RuntimeException(s"Failed to create XRefMetadata for ${xrefAction.xrefId}", e)
    }
  }
}
