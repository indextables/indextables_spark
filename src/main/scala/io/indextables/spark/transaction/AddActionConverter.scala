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

package io.indextables.spark.transaction

import scala.jdk.CollectionConverters._

import io.indextables.jni.txlog.{TxLogFileEntry, TxLogSkipAction}

/**
 * Converts between native `TxLogFileEntry` (from tantivy4java JNI) and Scala `AddAction` (used
 * throughout IndexTables4Spark).
 *
 * Also converts `TxLogSkipAction` to Scala `SkipAction`.
 */
object AddActionConverter {

  /**
   * Convert a native TxLogFileEntry to a Scala AddAction.
   */
  def toAddAction(entry: TxLogFileEntry): AddAction = {
    val partitionValues = entry.getPartitionValues.asScala.toMap
    val minValues       = if (entry.getMinValues.isEmpty) None else Some(entry.getMinValues.asScala.toMap)
    val maxValues       = if (entry.getMaxValues.isEmpty) None else Some(entry.getMaxValues.asScala.toMap)
    val numRecords      = if (entry.getNumRecords >= 0) Some(entry.getNumRecords) else None

    val footerStart = if (entry.getHasFooterOffsets && entry.getFooterStartOffset >= 0) Some(entry.getFooterStartOffset) else None
    val footerEnd   = if (entry.getHasFooterOffsets && entry.getFooterEndOffset >= 0) Some(entry.getFooterEndOffset) else None

    // splitTags: List<String> from native → Set[String] in Scala
    val splitTags = if (entry.getSplitTags.isEmpty) None else Some(entry.getSplitTags.asScala.toSet)

    val deleteOpstamp       = if (entry.hasDeleteOpstamp) Some(entry.getDeleteOpstamp) else None
    val numMergeOps         = if (entry.getNumMergeOps > 0) Some(entry.getNumMergeOps) else None
    val docMappingJson      = Option(entry.getDocMappingJson)
    val docMappingRef       = Option(entry.getDocMappingRef)
    val uncompressedSize    = if (entry.getUncompressedSizeBytes >= 0) Some(entry.getUncompressedSizeBytes) else None
    val timeRangeStart      = if (entry.getTimeRangeStart >= 0) Some(java.time.Instant.ofEpochMilli(entry.getTimeRangeStart / 1000).toString) else None
    val timeRangeEnd        = if (entry.getTimeRangeEnd >= 0) Some(java.time.Instant.ofEpochMilli(entry.getTimeRangeEnd / 1000).toString) else None
    val companionSrcFiles   = if (entry.getCompanionSourceFiles.isEmpty) None else Some(entry.getCompanionSourceFiles.asScala.toSeq)
    val companionDeltaVer   = if (entry.getCompanionDeltaVersion >= 0) Some(entry.getCompanionDeltaVersion) else None
    val companionFastField  = Option(entry.getCompanionFastFieldMode)

    AddAction(
      path = entry.getPath,
      partitionValues = partitionValues,
      size = entry.getSize,
      modificationTime = entry.getModificationTime,
      dataChange = entry.isDataChange,
      stats = Option(entry.getStats),
      minValues = minValues,
      maxValues = maxValues,
      numRecords = numRecords,
      footerStartOffset = footerStart,
      footerEndOffset = footerEnd,
      hasFooterOffsets = entry.getHasFooterOffsets,
      splitTags = splitTags,
      deleteOpstamp = deleteOpstamp,
      numMergeOps = numMergeOps,
      docMappingJson = docMappingJson,
      docMappingRef = docMappingRef,
      uncompressedSizeBytes = uncompressedSize,
      timeRangeStart = timeRangeStart,
      timeRangeEnd = timeRangeEnd,
      companionSourceFiles = companionSrcFiles,
      companionDeltaVersion = companionDeltaVer,
      companionFastFieldMode = companionFastField
    )
  }

  /**
   * Convert a sequence of native TxLogFileEntry objects to Scala AddActions.
   */
  def toAddActions(entries: java.util.List[TxLogFileEntry]): Seq[AddAction] =
    entries.asScala.map(toAddAction).toSeq

  /**
   * Convert a native TxLogSkipAction to a Scala SkipAction.
   */
  def toSkipAction(nativeSkip: TxLogSkipAction): SkipAction =
    SkipAction(
      path = nativeSkip.getPath,
      skipTimestamp = nativeSkip.getSkipTimestamp,
      reason = nativeSkip.getReason,
      operation = Option(nativeSkip.getOperation).getOrElse("unknown"),
      partitionValues = if (nativeSkip.getPartitionValues.isEmpty) None else Some(nativeSkip.getPartitionValues.asScala.toMap),
      size = if (nativeSkip.getSize >= 0) Some(nativeSkip.getSize) else None,
      retryAfter = if (nativeSkip.getRetryAfter >= 0) Some(nativeSkip.getRetryAfter) else None,
      skipCount = nativeSkip.getSkipCount
    )

  /**
   * Convert a sequence of native TxLogSkipAction objects to Scala SkipActions.
   */
  def toSkipActions(nativeSkips: java.util.List[TxLogSkipAction]): Seq[SkipAction] =
    nativeSkips.asScala.map(toSkipAction).toSeq
}
