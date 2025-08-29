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


package com.tantivy4spark.core

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, AddAction, PartitionPruning}
import com.tantivy4spark.storage.SplitLocationRegistry
import org.apache.spark.broadcast.Broadcast
// Removed unused imports
import org.slf4j.LoggerFactory

class Tantivy4SparkScan(
    transactionLog: TransactionLog,
    readSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None,
    broadcastConfig: Broadcast[Map[String, String]]
) extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScan])

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val addActions = transactionLog.listFiles()
    
    // Apply comprehensive data skipping (includes both partition pruning and min/max filtering)
    val filteredActions = applyDataSkipping(addActions, pushedFilters)
    
    logger.info(s"Planning ${filteredActions.length} partitions from ${addActions.length} total files")
    
    val partitions = filteredActions.zipWithIndex.map { case (addAction, index) =>
      val partition = new Tantivy4SparkInputPartition(addAction, readSchema, pushedFilters, index, limit)
      val preferredHosts = partition.preferredLocations()
      if (preferredHosts.nonEmpty) {
        logger.info(s"Partition $index (${addAction.path}) has preferred hosts: ${preferredHosts.mkString(", ")}")
      } else {
        logger.debug(s"Partition $index (${addAction.path}) has no cache locality information")
      }
      partition
    }
    
    val totalPreferred = partitions.count(_.preferredLocations().nonEmpty)
    logger.info(s"Split cache locality: $totalPreferred of ${partitions.length} partitions have preferred host assignments")
    
    partitions.toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val tablePath = transactionLog.getTablePath()
    new Tantivy4SparkReaderFactory(readSchema, limit, broadcastConfig, tablePath)
  }

  private def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    if (filters.isEmpty) {
      return addActions
    }

    val partitionColumns = transactionLog.getPartitionColumns()
    val initialCount = addActions.length
    
    // Step 1: Apply partition pruning
    val partitionPrunedActions = if (partitionColumns.nonEmpty) {
      val pruned = PartitionPruning.prunePartitions(addActions, partitionColumns, filters)
      val prunedCount = addActions.length - pruned.length
      if (prunedCount > 0) {
        logger.info(s"Partition pruning: filtered out $prunedCount of ${addActions.length} split files")
      }
      pruned
    } else {
      addActions
    }
    
    // Step 2: Apply min/max value skipping on remaining files
    val nonPartitionFilters = filters.filterNot { filter =>
      // Only apply min/max skipping to non-partition columns to avoid double filtering
      getFilterReferencedColumns(filter).exists(partitionColumns.contains)
    }
    
    val finalActions = if (nonPartitionFilters.nonEmpty) {
      val skipped = partitionPrunedActions.filterNot { addAction =>
        nonPartitionFilters.exists(filter => shouldSkipFile(addAction, filter))
      }
      val skippedCount = partitionPrunedActions.length - skipped.length
      if (skippedCount > 0) {
        logger.info(s"Data skipping (min/max): filtered out $skippedCount of ${partitionPrunedActions.length} files")
      }
      skipped
    } else {
      partitionPrunedActions
    }

    val totalSkipped = initialCount - finalActions.length
    if (totalSkipped > 0) {
      logger.info(s"Total data skipping: ${initialCount} files -> ${finalActions.length} files (skipped $totalSkipped total)")
    }

    finalActions
  }
  
  private def getFilterReferencedColumns(filter: Filter): Set[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, _) => Set(attribute)
      case EqualNullSafe(attribute, _) => Set(attribute)
      case GreaterThan(attribute, _) => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _) => Set(attribute)
      case LessThanOrEqual(attribute, _) => Set(attribute)
      case In(attribute, _) => Set(attribute)
      case IsNull(attribute) => Set(attribute)
      case IsNotNull(attribute) => Set(attribute)
      case StringStartsWith(attribute, _) => Set(attribute)
      case StringEndsWith(attribute, _) => Set(attribute)
      case StringContains(attribute, _) => Set(attribute)
      case And(left, right) => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right) => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child) => getFilterReferencedColumns(child)
      case _ => Set.empty
    }
  }

  private def shouldSkipFile(addAction: AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    (addAction.minValues, addAction.maxValues) match {
      case (Some(minVals), Some(maxVals)) =>
        filter match {
          case EqualTo(attribute, value) =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                valueStr < min || valueStr > max
              case _ => false
            }
          case GreaterThan(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) => max.toString <= value.toString
              case None => false
            }
          case LessThan(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) => min.toString >= value.toString
              case None => false
            }
          case GreaterThanOrEqual(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) => max.toString < value.toString
              case None => false
            }
          case LessThanOrEqual(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) => min.toString > value.toString
              case None => false
            }
          case _ => false
        }
      case _ => false
    }
  }
}