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

import scala.collection.mutable

import org.slf4j.LoggerFactory

/**
 * Index structure for efficient partition pruning. Pre-groups files by partition values
 * and provides O(1) lookup for equality filters.
 *
 * Benefits:
 * - Pre-grouping: O(unique_partitions) evaluations instead of O(files)
 * - Column index: O(1) equality lookups, O(k) IN lookups
 *
 * @param allFiles All AddAction files
 * @param byPartition Files grouped by their partition values
 * @param columnValueIndex Per-column index mapping value -> files
 * @param partitionColumns List of partition column names
 */
case class PartitionIndex(
  allFiles: Seq[AddAction],
  byPartition: Map[Map[String, String], Seq[AddAction]],
  columnValueIndex: Map[String, Map[String, Set[Map[String, String]]]],
  partitionColumns: Seq[String]
) {

  private val logger = LoggerFactory.getLogger(classOf[PartitionIndex])

  /**
   * Get all unique partition value combinations.
   */
  def uniquePartitions: Set[Map[String, String]] = byPartition.keySet

  /**
   * Get files for a specific partition value combination.
   */
  def getFilesForPartition(partitionValues: Map[String, String]): Seq[AddAction] =
    byPartition.getOrElse(partitionValues, Seq.empty)

  /**
   * Get all partitions that have a specific value for a column.
   * O(1) lookup using the column value index.
   *
   * @param column The partition column name
   * @param value The value to match
   * @return Set of partition value maps that match
   */
  def getPartitionsForEquality(column: String, value: String): Set[Map[String, String]] =
    columnValueIndex.get(column) match {
      case Some(valueIndex) => valueIndex.getOrElse(value, Set.empty)
      case None => Set.empty
    }

  /**
   * Get all partitions that have any of the specified values for a column.
   * O(k) lookup where k is the number of values.
   *
   * @param column The partition column name
   * @param values The values to match
   * @return Set of partition value maps that match any value
   */
  def getPartitionsForIn(column: String, values: Seq[String]): Set[Map[String, String]] =
    columnValueIndex.get(column) match {
      case Some(valueIndex) =>
        values.flatMap(v => valueIndex.getOrElse(v, Set.empty)).toSet
      case None => Set.empty
    }

  /**
   * Get files for all partitions in the given set.
   */
  def getFilesForPartitions(partitions: Set[Map[String, String]]): Seq[AddAction] =
    partitions.toSeq.flatMap(p => byPartition.getOrElse(p, Seq.empty))

  /**
   * Get the total number of files.
   */
  def totalFiles: Int = allFiles.size

  /**
   * Get the number of unique partitions.
   */
  def partitionCount: Int = byPartition.size

  /**
   * Check if the index has any partitions.
   */
  def isEmpty: Boolean = byPartition.isEmpty
}

/**
 * Companion object for building PartitionIndex instances.
 */
object PartitionIndex {

  private val logger = LoggerFactory.getLogger(PartitionIndex.getClass)

  /**
   * Build a PartitionIndex from a sequence of AddActions.
   *
   * @param addActions All files to index
   * @param partitionColumns List of partition column names
   * @return A new PartitionIndex
   */
  def build(addActions: Seq[AddAction], partitionColumns: Seq[String]): PartitionIndex = {
    if (partitionColumns.isEmpty || addActions.isEmpty) {
      return PartitionIndex(
        allFiles = addActions,
        byPartition = Map.empty,
        columnValueIndex = Map.empty,
        partitionColumns = partitionColumns
      )
    }

    // Group files by their partition values
    val byPartition = addActions.groupBy { action =>
      partitionColumns.map(col => col -> action.partitionValues.getOrElse(col, null)).toMap
    }

    // Build per-column value index
    // Maps: column -> value -> Set[partitionValues]
    val columnValueIndex = mutable.Map[String, mutable.Map[String, mutable.Set[Map[String, String]]]]()

    partitionColumns.foreach { col =>
      columnValueIndex(col) = mutable.Map[String, mutable.Set[Map[String, String]]]()
    }

    byPartition.keys.foreach { partitionValues =>
      partitionColumns.foreach { col =>
        val value = partitionValues.getOrElse(col, null)
        if (value != null) {
          val valueIndex = columnValueIndex(col)
          valueIndex.getOrElseUpdate(value, mutable.Set.empty) += partitionValues
        }
      }
    }

    // Convert to immutable structures
    val immutableColumnIndex = columnValueIndex.map { case (col, valueMap) =>
      col -> valueMap.map { case (value, partitions) =>
        value -> partitions.toSet
      }.toMap
    }.toMap

    logger.debug(s"Built PartitionIndex: ${addActions.size} files, ${byPartition.size} unique partitions, " +
      s"${partitionColumns.size} partition columns")

    PartitionIndex(
      allFiles = addActions,
      byPartition = byPartition,
      columnValueIndex = immutableColumnIndex,
      partitionColumns = partitionColumns
    )
  }

  /**
   * Build an empty PartitionIndex.
   */
  def empty: PartitionIndex = PartitionIndex(
    allFiles = Seq.empty,
    byPartition = Map.empty,
    columnValueIndex = Map.empty,
    partitionColumns = Seq.empty
  )
}
