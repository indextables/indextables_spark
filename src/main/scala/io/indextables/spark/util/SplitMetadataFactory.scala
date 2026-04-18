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

import java.time.Instant

import scala.jdk.CollectionConverters._

import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

/**
 * Factory for creating QuickwitSplit.SplitMetadata from AddAction transaction log entries.
 *
 * Handles footer offset extraction with fallback logic for splits that don't have pre-computed offsets stored in the
 * transaction log.
 */
object SplitMetadataFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates SplitMetadata from an AddAction.
   *
   * @param addAction
   *   The transaction log add action
   * @param tablePath
   *   The base table path (for relative split path resolution)
   * @return
   *   QuickwitSplit.SplitMetadata instance
   */
  def fromAddAction(
    addAction: AddAction,
    tablePath: String
  ): QuickwitSplit.SplitMetadata = {

    val splitId = extractSplitId(addAction.path)

    // Extract or compute footer offsets
    val (footerStartOffset, footerEndOffset) = extractFooterOffsets(addAction, tablePath)

    // Use splitTags (tantivy4java tags) when available, fall back to tags map keys
    val tags: java.util.Set[String] = addAction.splitTags match {
      case Some(st) => st.asJava
      case None     => addAction.tags.getOrElse(Map.empty[String, String]).keySet.asJava
    }

    new QuickwitSplit.SplitMetadata(
      splitId,                                                      // splitId
      "tantivy4spark-index",                                        // indexUid
      0L,                                                           // partitionId
      "tantivy4spark-source",                                       // sourceId
      "tantivy4spark-node",                                         // nodeId
      toLongSafe(addAction.numRecords),                             // numDocs
      toLongSafe(addAction.uncompressedSizeBytes, addAction.size),  // uncompressedSizeBytes
      addAction.timeRangeStart.map(Instant.parse).orNull,           // timeRangeStart
      addAction.timeRangeEnd.map(Instant.parse).orNull,             // timeRangeEnd
      System.currentTimeMillis() / 1000,                            // createTimestamp
      "Mature",                                                     // maturity
      tags,                                                         // tags
      footerStartOffset,                                            // footerStartOffset
      footerEndOffset,                                              // footerEndOffset
      toLongSafe(addAction.deleteOpstamp),                          // deleteOpstamp
      addAction.numMergeOps.getOrElse(0),                           // numMergeOps
      "doc-mapping-uid",                                            // docMappingUid
      addAction.docMappingJson.orNull,                              // docMappingJson
      java.util.Collections.emptyList[QuickwitSplit.SkippedSplit]() // skippedSplits
    )
  }

  /** Safely convert Option[Any] to Long, handling Integer/Long type mismatches from JSON deserialization. */
  private def toLongSafe(opt: Option[Any], fallback: Long = 0L): Long = opt match {
    case Some(l: Long)              => l
    case Some(i: Int)               => i.toLong
    case Some(i: java.lang.Integer) => i.toLong
    case Some(l: java.lang.Long)    => l
    case Some(other) =>
      try other.toString.toLong
      catch { case _: NumberFormatException => fallback }
    case None => fallback
  }

  /**
   * Extracts split ID from split path.
   *
   * @param splitPath
   *   Full or relative split path
   * @return
   *   Split ID (filename without .split extension)
   */
  private def extractSplitId(splitPath: String): String =
    splitPath.split("/").last.replace(".split", "")

  /**
   * Extracts footer offsets from AddAction.
   *
   * Uses pre-computed offsets from transaction log if available, otherwise uses defaults.
   *
   * @param addAction
   *   The add action containing split metadata
   * @param tablePath
   *   The table base path (unused, kept for API consistency)
   * @return
   *   Tuple of (footerStartOffset, footerEndOffset)
   */
  private def extractFooterOffsets(
    addAction: AddAction,
    tablePath: String
  ): (Long, Long) =
    // Use pre-computed offsets from transaction log if available
    if (addAction.footerStartOffset.isDefined && addAction.footerEndOffset.isDefined) {
      val start = addAction.footerStartOffset.get
      val end   = addAction.footerEndOffset.get
      logger.debug(s"Using pre-computed footer offsets for ${addAction.path}: $start-$end")
      (start, end)
    } else {
      // Use defaults when offsets not available — warn since callers previously threw on missing offsets
      logger.warn(s"Footer offsets not in transaction log for ${addAction.path}, using defaults (0, 0)")
      (0L, 0L)
    }

  /**
   * Batch creates SplitMetadata from multiple AddActions.
   *
   * @param addActions
   *   Sequence of add actions
   * @param tablePath
   *   The base table path
   * @return
   *   Sequence of SplitMetadata instances
   */
  def fromAddActions(
    addActions: Seq[AddAction],
    tablePath: String
  ): Seq[QuickwitSplit.SplitMetadata] =
    addActions.map(action => fromAddAction(action, tablePath))
}
