package com.tantivy4spark.core

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
// Removed unused imports
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

class Tantivy4SparkScan(
    transactionLog: TransactionLog,
    readSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap
) extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScan])

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val addActions = transactionLog.listFiles()
    val filteredActions = applyDataSkipping(addActions, pushedFilters)
    
    logger.info(s"Planning ${filteredActions.length} partitions from ${addActions.length} total files")
    
    filteredActions.zipWithIndex.map { case (addAction, index) =>
      new Tantivy4SparkInputPartition(addAction, readSchema, pushedFilters, options, index)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new Tantivy4SparkReaderFactory(readSchema, options)
  }

  private def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    if (filters.isEmpty) {
      return addActions
    }

    val skippedCount = addActions.count { addAction =>
      filters.exists(filter => shouldSkipFile(addAction, filter))
    }

    val remainingActions = addActions.filterNot { addAction =>
      filters.exists(filter => shouldSkipFile(addAction, filter))
    }

    logger.info(s"Data skipping: ${skippedCount} files skipped, ${remainingActions.length} files remaining")
    remainingActions
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