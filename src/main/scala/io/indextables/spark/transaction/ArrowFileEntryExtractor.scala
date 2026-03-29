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

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import io.indextables.spark.util.JsonUtil

/**
 * Extracts AddAction objects from an Arrow ColumnarBatch produced by nativeListFilesArrowFfi.
 *
 * The Arrow schema has 19 fixed columns + N dynamic partition columns (partition:{name}).
 * This extractor reads Arrow vectors directly — no JSON parsing per row.
 */
object ArrowFileEntryExtractor {

  private val mapper = JsonUtil.mapper

  /**
   * Extract AddAction objects from an Arrow batch.
   *
   * @param batch           Arrow ColumnarBatch from native FFI
   * @param partitionColumns Partition column names (for dynamic partition:{name} columns)
   * @return Seq of AddAction, one per row
   */
  def extract(batch: ColumnarBatch, partitionColumns: Seq[String]): Seq[AddAction] = {
    val numRows = batch.numRows()
    if (numRows == 0) return Seq.empty

    // Extract underlying Arrow FieldVectors for direct access
    val root = (0 until batch.numCols()).map { i =>
      batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector
    }

    // Fixed columns (indices 0-18)
    val pathVec = root(0).asInstanceOf[VarCharVector]
    val sizeVec = root(1).asInstanceOf[BigIntVector]
    val modTimeVec = root(2).asInstanceOf[BigIntVector]
    val dataChangeVec = root(3).asInstanceOf[BitVector]
    val numRecordsVec = root(4).asInstanceOf[BigIntVector]
    val footerStartVec = root(5).asInstanceOf[BigIntVector]
    val footerEndVec = root(6).asInstanceOf[BigIntVector]
    val hasFooterVec = root(7).asInstanceOf[BitVector]
    val deleteOpstampVec = root(8).asInstanceOf[BigIntVector]
    val splitTagsVec = root(9) // ListVector of Utf8
    val numMergeOpsVec = root(10).asInstanceOf[IntVector]
    val docMappingJsonVec = root(11).asInstanceOf[VarCharVector]
    val docMappingRefVec = root(12).asInstanceOf[VarCharVector]
    val uncompressedSizeVec = root(13).asInstanceOf[BigIntVector]
    val timeRangeStartVec = root(14).asInstanceOf[BigIntVector]
    val timeRangeEndVec = root(15).asInstanceOf[BigIntVector]
    val companionSourceFilesVec = root(16) // ListVector of Utf8
    val companionDeltaVersionVec = root(17).asInstanceOf[BigIntVector]
    val companionFastFieldModeVec = root(18).asInstanceOf[VarCharVector]

    // Dynamic partition columns start at index 19
    val partColVecs = partitionColumns.indices.map(i => root(19 + i).asInstanceOf[VarCharVector])

    // Optional stats columns follow partition columns (present when includeStats=true)
    val statsBaseIdx = 19 + partitionColumns.size
    val hasStatsColumns = batch.numCols() > statsBaseIdx
    val minValuesVec = if (hasStatsColumns) Some(root(statsBaseIdx).asInstanceOf[VarCharVector]) else None
    val maxValuesVec = if (hasStatsColumns && batch.numCols() > statsBaseIdx + 1)
      Some(root(statsBaseIdx + 1).asInstanceOf[VarCharVector]) else None

    val result = new Array[AddAction](numRows)

    var i = 0
    while (i < numRows) {
      // Build partition values map from dynamic columns
      val partitionValues = if (partitionColumns.nonEmpty) {
        val map = scala.collection.mutable.Map[String, String]()
        var j = 0
        while (j < partitionColumns.length) {
          if (!partColVecs(j).isNull(i)) {
            map.put(partitionColumns(j), new String(partColVecs(j).get(i)))
          }
          j += 1
        }
        map.toMap
      } else Map.empty[String, String]

      // Extract split tags from List<Utf8>
      val splitTags = if (splitTagsVec.isNull(i)) None
      else {
        splitTagsVec match {
          case lv: ListVector =>
            val obj = lv.getObject(i)
            if (obj != null) {
              val list = obj.asInstanceOf[java.util.List[_]]
              Some(list.asScala.map(_.toString).toSet)
            } else None
          case _ => None
        }
      }

      // Extract companion source files from List<Utf8>
      val companionSourceFiles = if (companionSourceFilesVec.isNull(i)) None
      else {
        companionSourceFilesVec match {
          case lv: ListVector =>
            val obj = lv.getObject(i)
            if (obj != null) {
              val list = obj.asInstanceOf[java.util.List[_]]
              Some(list.asScala.map(_.toString).toSeq)
            } else None
          case _ => None
        }
      }

      require(!pathVec.isNull(i), s"path column must not be null at row $i")

      result(i) = AddAction(
        path = new String(pathVec.get(i)),
        partitionValues = partitionValues,
        size = sizeVec.get(i),
        modificationTime = modTimeVec.get(i),
        dataChange = dataChangeVec.get(i) != 0,
        stats = None,
        minValues = minValuesVec.flatMap { v =>
          if (v.isNull(i)) None
          else Some(mapper.readValue(new String(v.get(i)), classOf[java.util.Map[String, String]]).asScala.toMap)
        },
        maxValues = maxValuesVec.flatMap { v =>
          if (v.isNull(i)) None
          else Some(mapper.readValue(new String(v.get(i)), classOf[java.util.Map[String, String]]).asScala.toMap)
        },
        numRecords = if (numRecordsVec.isNull(i)) None else Some(numRecordsVec.get(i)),
        footerStartOffset = if (footerStartVec.isNull(i) || footerStartVec.get(i) <= 0) None else Some(footerStartVec.get(i)),
        footerEndOffset = if (footerEndVec.isNull(i) || footerEndVec.get(i) <= 0) None else Some(footerEndVec.get(i)),
        hasFooterOffsets = if (!hasFooterVec.isNull(i)) hasFooterVec.get(i) != 0
          else !footerStartVec.isNull(i) && footerStartVec.get(i) > 0
            && !footerEndVec.isNull(i) && footerEndVec.get(i) > 0,
        deleteOpstamp = if (deleteOpstampVec.isNull(i)) None else Some(deleteOpstampVec.get(i)),
        splitTags = splitTags,
        numMergeOps = if (numMergeOpsVec.isNull(i)) None else Some(numMergeOpsVec.get(i)),
        docMappingJson = if (docMappingJsonVec.isNull(i)) None else Some(new String(docMappingJsonVec.get(i))),
        docMappingRef = if (docMappingRefVec.isNull(i)) None else Some(new String(docMappingRefVec.get(i))),
        uncompressedSizeBytes = if (uncompressedSizeVec.isNull(i)) None else Some(uncompressedSizeVec.get(i)),
        timeRangeStart = if (timeRangeStartVec.isNull(i)) None else Some(timeRangeStartVec.get(i).toString),
        timeRangeEnd = if (timeRangeEndVec.isNull(i)) None else Some(timeRangeEndVec.get(i).toString),
        companionSourceFiles = companionSourceFiles,
        companionDeltaVersion = if (companionDeltaVersionVec.isNull(i)) None else Some(companionDeltaVersionVec.get(i)),
        companionFastFieldMode = if (companionFastFieldModeVec.isNull(i)) None else Some(new String(companionFastFieldModeVec.get(i)))
      )

      i += 1
    }

    result.toSeq
  }
}
