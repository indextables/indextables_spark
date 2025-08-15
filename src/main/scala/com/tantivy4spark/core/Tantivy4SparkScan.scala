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
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
import com.tantivy4spark.bloom.{BloomFilterManager, BloomFilterStorage, TextTokenizer}
// Removed unused imports
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

    // Apply traditional min/max data skipping
    val traditionalSkipped = addActions.filterNot { addAction =>
      filters.exists(filter => shouldSkipFile(addAction, filter))
    }

    // Apply bloom filter-based text search skipping
    val bloomFilterSkipped = applyBloomFilterSkipping(traditionalSkipped, filters)

    val totalSkipped = addActions.length - bloomFilterSkipped.length
    logger.info(s"Data skipping: ${addActions.length} files -> ${bloomFilterSkipped.length} files (skipped $totalSkipped)")

    bloomFilterSkipped
  }
  
  /**
   * Apply bloom filter-based file skipping for text search acceleration
   * This is where the Splunk-style optimization happens
   */
  private def applyBloomFilterSkipping(addActions: Seq[AddAction], filters: Array[Filter]): Seq[AddAction] = {
    val bloomFilterManager = new BloomFilterManager()
    val bloomFilterStorage = BloomFilterStorage.getInstance
    val textTokenizer = new TextTokenizer()
    
    // Extract text search terms from filters
    val textSearchTerms = extractTextSearchTerms(filters)
    
    if (textSearchTerms.isEmpty) {
      logger.debug("No text search terms found in filters, skipping bloom filter optimization")
      return addActions
    }
    
    logger.info(s"Applying bloom filter skipping with search terms: ${textSearchTerms.mkString(", ")}")
    
    // Preload bloom filters for batch processing (S3 optimization)
    val fileBloomFilters = addActions.flatMap { addAction =>
      addAction.bloomFilters.map(addAction.path -> _)
    }.toMap
    
    if (fileBloomFilters.nonEmpty) {
      bloomFilterStorage.preloadBloomFilters(fileBloomFilters)
    }
    
    // Filter files using bloom filters
    val candidateFiles = addActions.filter { addAction =>
      addAction.bloomFilters match {
        case Some(encodedFilters) =>
          try {
            val bloomFilters = bloomFilterStorage.decodeBloomFilters(encodedFilters, addAction.path)
            bloomFilterManager.mightContainAnyTerm(bloomFilters, textSearchTerms)
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to decode bloom filters for ${addAction.path}, including file conservatively", e)
              true // Include file if bloom filter processing fails
          }
        case None =>
          true // Include file if no bloom filters available
      }
    }
    
    val skippedByBloom = addActions.length - candidateFiles.length
    logger.info(s"Bloom filter skipping: ${addActions.length} -> ${candidateFiles.length} files (skipped $skippedByBloom by bloom filters)")
    
    candidateFiles
  }
  
  /**
   * Extract text search terms from Spark filters
   * Handles Contains, StringStartsWith, StringEndsWith, etc.
   */
  private def extractTextSearchTerms(filters: Array[Filter]): Set[String] = {
    import org.apache.spark.sql.sources._
    
    val terms = scala.collection.mutable.Set[String]()
    
    filters.foreach {
      case StringContains(_, value) => terms += value
      case StringStartsWith(_, value) => terms += value
      case StringEndsWith(_, value) => terms += value
      case EqualTo(_, value: String) => terms += value
      case _ => // Other filter types don't contribute to text search
    }
    
    logger.debug(s"Extracted ${terms.size} text search terms from ${filters.length} filters")
    terms.toSet
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