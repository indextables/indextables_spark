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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsReportStatistics, Statistics}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructType, DateType, IntegerType, LongType, FloatType, DoubleType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, AddAction, PartitionPruning}
import com.tantivy4spark.storage.{SplitLocationRegistry, BroadcastSplitLocalityManager}
import com.tantivy4spark.prewarm.PreWarmManager
import org.apache.spark.broadcast.Broadcast
// Removed unused imports
import org.slf4j.LoggerFactory

class Tantivy4SparkScan(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    readSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None,
    broadcastConfig: Broadcast[Map[String, String]],
    indexQueryFilters: Array[Any] = Array.empty
) extends Scan with Batch with SupportsReportStatistics {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScan])

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val addActions = transactionLog.listFiles()
    
    // Update broadcast locality information for better scheduling
    // This helps ensure preferred locations are accurate during partition planning
    try {
      // Access the SparkContext from the SparkSession
      val sparkContext = sparkSession.sparkContext
      println(s"🔄 [DRIVER-SCAN] Updating broadcast locality before partition planning")
      BroadcastSplitLocalityManager.updateBroadcastLocality(sparkContext)
      println(s"🔄 [DRIVER-SCAN] Broadcast locality update completed")
      logger.debug("Updated broadcast locality information for partition planning")
    } catch {
      case ex: Exception =>
        println(s"❌ [DRIVER-SCAN] Failed to update broadcast locality information: ${ex.getMessage}")
        logger.warn("Failed to update broadcast locality information", ex)
    }
    
    // Apply comprehensive data skipping (includes both partition pruning and min/max filtering)
    val filteredActions = applyDataSkipping(addActions, pushedFilters)
    
    // Check if pre-warm is enabled
    val broadcastConfigMap = broadcastConfig.value
    val isPreWarmEnabled = broadcastConfigMap.getOrElse("spark.tantivy4spark.cache.prewarm.enabled", "true").toBoolean
    
    // Execute pre-warm phase if enabled
    if (isPreWarmEnabled && filteredActions.nonEmpty) {
      try {
        val sparkContext = sparkSession.sparkContext
        logger.info(s"🔥 Pre-warm enabled: initiating cache warming for ${filteredActions.length} splits")
        
        // Combine regular filters with IndexQuery filters for pre-warming
        val allFilters = pushedFilters.asInstanceOf[Array[Any]] ++ indexQueryFilters
        
        val preWarmResult = PreWarmManager.executePreWarm(
          sparkContext,
          filteredActions,
          readSchema,
          allFilters,
          broadcastConfig,
          isPreWarmEnabled
        )
        
        if (preWarmResult.warmupInitiated) {
          logger.info(s"🔥 Pre-warm completed: ${preWarmResult.totalWarmupsCreated} warmup tasks across ${preWarmResult.warmupAssignments.size} hosts")
          println(s"🔥 [DRIVER-PREWARM] Pre-warm completed: ${preWarmResult.totalWarmupsCreated} tasks across ${preWarmResult.warmupAssignments.size} hosts")
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Pre-warm failed but continuing with query execution: ${ex.getMessage}", ex)
          println(s"⚠️  [DRIVER-PREWARM] Pre-warm failed but continuing: ${ex.getMessage}")
      }
    }
    
    logger.warn(s"🔍 SCAN DEBUG: Planning ${filteredActions.length} partitions from ${addActions.length} total files")
    
    println(s"🗺️  [DRIVER-SCAN] Planning ${filteredActions.length} partitions")
    
    val partitions = filteredActions.zipWithIndex.map { case (addAction, index) =>
      println(s"🗺️  [DRIVER-SCAN] Creating partition $index for split: ${addAction.path}")
      val partition = new Tantivy4SparkInputPartition(addAction, readSchema, pushedFilters, index, limit, indexQueryFilters)
      val preferredHosts = partition.preferredLocations()
      if (preferredHosts.nonEmpty) {
        println(s"🗺️  [DRIVER-SCAN] Partition $index (${addAction.path}) has preferred hosts: ${preferredHosts.mkString(", ")}")
        logger.info(s"Partition $index (${addAction.path}) has preferred hosts: ${preferredHosts.mkString(", ")}")
      } else {
        println(s"🗺️  [DRIVER-SCAN] Partition $index (${addAction.path}) has no cache locality information")
        logger.debug(s"Partition $index (${addAction.path}) has no cache locality information")
      }
      partition
    }
    
    val totalPreferred = partitions.count(_.preferredLocations().nonEmpty)
    println(s"🗺️  [DRIVER-SCAN] Split cache locality summary: $totalPreferred of ${partitions.length} partitions have preferred host assignments")
    logger.info(s"Split cache locality: $totalPreferred of ${partitions.length} partitions have preferred host assignments")
    
    partitions.toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val tablePath = transactionLog.getTablePath()
    new Tantivy4SparkReaderFactory(readSchema, limit, broadcastConfig, tablePath)
  }

  override def estimateStatistics(): Statistics = {
    try {
      logger.info("Estimating statistics for Tantivy4Spark table")
      val addActions = transactionLog.listFiles()

      // Apply the same data skipping logic used in planInputPartitions to get accurate statistics
      // for the filtered dataset that will actually be read
      val filteredActions = applyDataSkipping(addActions, pushedFilters)

      val statistics = Tantivy4SparkStatistics.fromAddActions(filteredActions)

      logger.info(s"Table statistics: ${statistics.sizeInBytes().orElse(0L)} bytes, ${statistics.numRows().orElse(0L)} rows")

      statistics
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to estimate statistics: ${ex.getMessage}", ex)
        // Return unknown statistics rather than failing the query
        Tantivy4SparkStatistics.unknown()
    }
  }

  private def applyDataSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    logger.warn(s"🔍 DATA SKIPPING DEBUG: applyDataSkipping called with ${addActions.length} files and ${filters.length} filters")
    filters.foreach(f => logger.warn(s"🔍 DATA SKIPPING DEBUG: Filter: $f"))
    
    if (filters.isEmpty) {
      logger.warn(s"🔍 DATA SKIPPING DEBUG: No filters, returning all ${addActions.length} files")
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
      val skipped = partitionPrunedActions.filter { addAction =>
        // Improved data skipping logic that handles OR predicates correctly
        canFileMatchFilters(addAction, nonPartitionFilters)
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

  // TODO: Fix options flow from read operations to scan for proper field type detection
  // /**
  //  * Check if a field is configured as a text field (tokenized) for data skipping purposes.
  //  * Uses the indexing configuration to determine field type.
  //  * Currently disabled due to options flow issues.
  //  */
  // private def isTextFieldForTokenization(fieldName: String): Boolean = {
  //   // Implementation commented out - options don't flow correctly from read to scan
  //   false
  // }

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

  /**
   * Determine if a file can potentially match the given filters.
   * This implements proper OR/AND logic for data skipping.
   */
  private def canFileMatchFilters(addAction: AddAction, filters: Array[Filter]): Boolean = {
    import org.apache.spark.sql.sources._

    // If no min/max values available, conservatively keep the file
    if (addAction.minValues.isEmpty || addAction.maxValues.isEmpty) {
      return true
    }

    // A file can match if ANY of the top-level filters can match
    // This correctly handles cases where filters are combined with OR at the top level
    filters.exists(filter => canFilterMatchFile(addAction, filter))
  }

  /**
   * Determine if a single filter can potentially match a file.
   * Handles complex nested AND/OR logic correctly.
   */
  private def canFilterMatchFile(addAction: AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    filter match {
      case And(left, right) =>
        // For AND: both sides must be able to match
        canFilterMatchFile(addAction, left) && canFilterMatchFile(addAction, right)

      case Or(left, right) =>
        // For OR: at least one side must be able to match
        canFilterMatchFile(addAction, left) || canFilterMatchFile(addAction, right)

      case Not(child) =>
        // For NOT: if the child cannot match, then NOT child can match
        // Conservative approach: if we can't determine, keep the file
        !canFilterMatchFile(addAction, child)

      case _ =>
        // For all other filters, use the existing shouldSkipFile logic (inverted)
        !shouldSkipFile(addAction, filter)
    }
  }

  private def shouldSkipFile(addAction: AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    import java.time.LocalDate

    (addAction.minValues, addAction.maxValues) match {
      case (Some(minVals), Some(maxVals)) =>
        filter match {
          case EqualTo(attribute, value) =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            logger.warn(s"🔍 DATA SKIPPING DEBUG: EqualTo filter for $attribute = $value")
            logger.warn(s"🔍 DATA SKIPPING DEBUG: minVal=$minVal, maxVal=$maxVal")
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val (convertedValue, convertedMin, convertedMax) = convertValuesForComparison(attribute, value, min, max)

                // Simple lexicographic comparison: skip if value is outside [min, max] range
                // EqualTo should always be exact equality regardless of field type
                val shouldSkip = convertedValue.compareTo(convertedMin) < 0 || convertedValue.compareTo(convertedMax) > 0

                logger.warn(s"🔍 DATA SKIPPING DEBUG: convertedValue=$convertedValue, convertedMin=$convertedMin, convertedMax=$convertedMax")
                logger.warn(s"🔍 DATA SKIPPING DEBUG: Lexicographic comparison, shouldSkip=$shouldSkip")
                shouldSkip
              case _ =>
                logger.warn(s"🔍 DATA SKIPPING DEBUG: No min/max values found, not skipping")
                false
            }
          case GreaterThan(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) => 
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max)
                convertedMax.compareTo(convertedValue) <= 0
              case None => false
            }
          case LessThan(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) => 
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "")
                convertedMin.compareTo(convertedValue) >= 0
              case None => false
            }
          case GreaterThanOrEqual(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) => 
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max)
                convertedMax.compareTo(convertedValue) < 0
              case None => false
            }
          case LessThanOrEqual(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) =>
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "")
                convertedMin.compareTo(convertedValue) > 0
              case None => false
            }
          case StringStartsWith(attribute, value) =>
            // For startsWith, check if any string in [min, max] could start with the value
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                // Skip if the prefix is lexicographically greater than max value
                // or if max value is shorter than prefix and doesn't start with it
                val shouldSkip = valueStr.compareTo(max) > 0 ||
                                (!max.startsWith(valueStr) && max.compareTo(valueStr) < 0)
                logger.warn(s"🔍 DATA SKIPPING DEBUG: StringStartsWith($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case StringEndsWith(attribute, value) =>
            // For endsWith, this is harder to optimize with min/max, so be conservative
            // Only skip if we can determine with certainty
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                // Very conservative: only skip if min and max are identical and don't end with value
                val shouldSkip = min == max && !min.endsWith(valueStr)
                logger.warn(s"🔍 DATA SKIPPING DEBUG: StringEndsWith($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case StringContains(attribute, value) =>
            // For contains, be conservative - only skip if min==max and doesn't contain value
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) =>
                val valueStr = value.toString
                val shouldSkip = min == max && !min.contains(valueStr)
                logger.warn(s"🔍 DATA SKIPPING DEBUG: StringContains($attribute, '$value') - min='$min', max='$max', shouldSkip=$shouldSkip")
                shouldSkip
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def convertValuesForComparison(attribute: String, filterValue: Any, minValue: String, maxValue: String): (Comparable[Any], Comparable[Any], Comparable[Any]) = {
    import java.time.LocalDate
    import java.sql.Date
    
    // Find the field data type in the schema
    val fieldType = readSchema.fields.find(_.name == attribute).map(_.dataType)
    
    // logger.info(s"🔍 TYPE CONVERSION DEBUG: attribute=$attribute, filterValue=$filterValue (${filterValue.getClass.getSimpleName}), fieldType=$fieldType")
    // logger.info(s"🔍 TYPE CONVERSION DEBUG: minValue=$minValue, maxValue=$maxValue")
    
    fieldType match {
      case Some(DateType) =>
        // For DateType, the table stores values as days since epoch (integer)
        logger.warn(s"🔍 DATE CONVERSION: Processing DateType field $attribute")
        logger.warn(s"🔍 DATE CONVERSION: filterValue=$filterValue (${filterValue.getClass.getSimpleName})")
        try {
          val filterDaysSinceEpoch = filterValue match {
            case dateStr: String =>
              logger.warn(s"🔍 DATE CONVERSION: Parsing string date: $dateStr")
              val filterDate = LocalDate.parse(dateStr)
              val epochDate = LocalDate.of(1970, 1, 1)
              val days = epochDate.until(filterDate).getDays
              logger.warn(s"🔍 DATE CONVERSION: String '$dateStr' -> LocalDate '$filterDate' -> days since epoch: $days")
              days
            case sqlDate: Date =>
              logger.warn(s"🔍 DATE CONVERSION: Converting SQL Date: $sqlDate")
              // Use direct calculation from milliseconds since epoch
              val millisSinceEpoch = sqlDate.getTime
              val daysSinceEpoch = (millisSinceEpoch / (24 * 60 * 60 * 1000)).toInt
              logger.warn(s"🔍 DATE CONVERSION: SQL Date '$sqlDate' -> millis=$millisSinceEpoch -> days since epoch: $daysSinceEpoch")
              daysSinceEpoch
            case intVal: Int => 
              logger.warn(s"🔍 DATE CONVERSION: Using int value directly: $intVal")
              intVal
            case _ =>
              logger.warn(s"🔍 DATE CONVERSION: Fallback parsing toString: ${filterValue.toString}")
              val filterDate = LocalDate.parse(filterValue.toString)
              val epochDate = LocalDate.of(1970, 1, 1)
              val days = epochDate.until(filterDate).getDays
              logger.warn(s"🔍 DATE CONVERSION: Fallback '${filterValue.toString}' -> LocalDate '$filterDate' -> days since epoch: $days")
              days
          }
          
          val minDays = minValue.toInt
          val maxDays = maxValue.toInt
          // logger.info(s"🔍 DATE CONVERSION RESULT: filterDaysSinceEpoch=$filterDaysSinceEpoch, minDays=$minDays, maxDays=$maxDays")
          (filterDaysSinceEpoch.asInstanceOf[Comparable[Any]], minDays.asInstanceOf[Comparable[Any]], maxDays.asInstanceOf[Comparable[Any]])
        } catch {
          case ex: Exception =>
            logger.warn(s"🔍 DATE CONVERSION FAILED: $filterValue (${filterValue.getClass.getSimpleName}) - ${ex.getMessage}")
            // Fallback to string comparison
            (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
        }
        
      case Some(IntegerType) =>
        // Convert integer values for proper numeric comparison
        logger.info(s"🔍 INTEGER CONVERSION: Processing IntegerType field $attribute")
        try {
          val filterInt = filterValue.toString.toInt
          val minInt = minValue.toInt
          val maxInt = maxValue.toInt
          logger.info(s"🔍 INTEGER CONVERSION RESULT: filterInt=$filterInt, minInt=$minInt, maxInt=$maxInt")
          (filterInt.asInstanceOf[Comparable[Any]], minInt.asInstanceOf[Comparable[Any]], maxInt.asInstanceOf[Comparable[Any]])
        } catch {
          case ex: Exception =>
            logger.warn(s"🔍 INTEGER CONVERSION FAILED: $filterValue - ${ex.getMessage}")
            (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
        }
        
      case Some(LongType) =>
        // Convert long values for proper numeric comparison
        logger.info(s"🔍 LONG CONVERSION: Processing LongType field $attribute")
        try {
          val filterLong = filterValue.toString.toLong
          val minLong = minValue.toLong
          val maxLong = maxValue.toLong
          logger.info(s"🔍 LONG CONVERSION RESULT: filterLong=$filterLong, minLong=$minLong, maxLong=$maxLong")
          (filterLong.asInstanceOf[Comparable[Any]], minLong.asInstanceOf[Comparable[Any]], maxLong.asInstanceOf[Comparable[Any]])
        } catch {
          case ex: Exception =>
            logger.warn(s"🔍 LONG CONVERSION FAILED: $filterValue - ${ex.getMessage}")
            (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
        }
        
      case Some(FloatType) =>
        // Convert float values for proper numeric comparison
        logger.info(s"🔍 FLOAT CONVERSION: Processing FloatType field $attribute")
        try {
          val filterFloat = filterValue.toString.toFloat
          val minFloat = minValue.toFloat
          val maxFloat = maxValue.toFloat
          logger.info(s"🔍 FLOAT CONVERSION RESULT: filterFloat=$filterFloat, minFloat=$minFloat, maxFloat=$maxFloat")
          (filterFloat.asInstanceOf[Comparable[Any]], minFloat.asInstanceOf[Comparable[Any]], maxFloat.asInstanceOf[Comparable[Any]])
        } catch {
          case ex: Exception =>
            logger.warn(s"🔍 FLOAT CONVERSION FAILED: $filterValue - ${ex.getMessage}")
            (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
        }
        
      case Some(DoubleType) =>
        // Convert double values for proper numeric comparison
        logger.info(s"🔍 DOUBLE CONVERSION: Processing DoubleType field $attribute")
        try {
          val filterDouble = filterValue.toString.toDouble
          val minDouble = minValue.toDouble
          val maxDouble = maxValue.toDouble
          logger.info(s"🔍 DOUBLE CONVERSION RESULT: filterDouble=$filterDouble, minDouble=$minDouble, maxDouble=$maxDouble")
          (filterDouble.asInstanceOf[Comparable[Any]], minDouble.asInstanceOf[Comparable[Any]], maxDouble.asInstanceOf[Comparable[Any]])
        } catch {
          case ex: Exception =>
            logger.warn(s"🔍 DOUBLE CONVERSION FAILED: $filterValue - ${ex.getMessage}")
            (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
        }
        
      case _ =>
        // For other data types (strings, etc.), use string comparison
        logger.info(s"🔍 STRING CONVERSION: Using string comparison for $attribute")
        (filterValue.toString.asInstanceOf[Comparable[Any]], minValue.asInstanceOf[Comparable[Any]], maxValue.asInstanceOf[Comparable[Any]])
    }
  }
}