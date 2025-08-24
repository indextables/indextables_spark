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

package com.tantivy4spark.optimize

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.slf4j.LoggerFactory
import com.tantivy4spark.config.Tantivy4SparkSQLConf

/**
 * An execution node that optimizes writes to Tantivy4Spark tables by adding an adaptive shuffle
 * to target a specific number of records per split file.
 * 
 * This is similar to Delta Lake's DeltaOptimizedWriterExec, but adapted for Tantivy4Spark's
 * split-based architecture.
 * 
 * @param child The child execution plan
 * @param partitionColumns The partition columns for hash partitioning (if any)
 * @param targetRecordsPerSplit Target number of records per output split file
 */
case class Tantivy4SparkOptimizedWriterExec(
    child: SparkPlan,
    partitionColumns: Seq[String] = Seq.empty,
    targetRecordsPerSplit: Long = 1000000L
) extends UnaryExecNode {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkOptimizedWriterExec])

  override def output: Seq[Attribute] = child.output

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "inputRecords" -> SQLMetrics.createMetric(sparkContext, "number of input records"),
    "outputPartitions" -> SQLMetrics.createMetric(sparkContext, "number of output partitions"),
    "avgRecordsPerPartition" -> SQLMetrics.createMetric(sparkContext, "average records per partition")
  )

  /**
   * Calculate the optimal number of shuffle partitions based on the target records per split.
   */
  private def calculateOptimalPartitions(): Int = {
    val spark = SparkSession.active
    
    // Try to estimate the total number of records - use a simple heuristic
    val estimatedRows = try {
      // Use the number of current partitions as a rough estimate
      val currentPartitions = child.execute().getNumPartitions
      val estimatePerPartition = 10000L // Assume 10k records per partition as baseline
      currentPartitions * estimatePerPartition
    } catch {
      case e: Exception =>
        logger.warn("Could not estimate row count from plan", e)
        100000L // Default fallback estimate
    }

    // Calculate target partitions based on records per split (using ceiling to ensure proper coverage)
    val targetPartitions = Math.max(1, Math.ceil(estimatedRows.toDouble / targetRecordsPerSplit.toDouble).toInt)
    
    // Apply min/max constraints from configuration
    val maxPartitions = spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS)
      .map(_.toInt).getOrElse(2000)
    val minPartitions = spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_MIN_SHUFFLE_PARTITIONS)
      .map(_.toInt).getOrElse(1)
    
    val optimalPartitions = Math.min(Math.max(targetPartitions, minPartitions), maxPartitions)
    
    logger.info(s"Optimized write: estimated $estimatedRows records, " +
      s"target $targetRecordsPerSplit per split, " +
      s"using $optimalPartitions partitions")
    
    optimalPartitions
  }

  override protected def doExecute() = {
    val optimalPartitions = calculateOptimalPartitions()
    
    // Update metrics
    val inputRecordsMetric = longMetric("inputRecords")
    val outputPartitionsMetric = longMetric("outputPartitions")
    val avgRecordsPerPartitionMetric = longMetric("avgRecordsPerPartition")
    
    outputPartitionsMetric.set(optimalPartitions)

    // Create appropriate partitioning strategy
    val partitioning = if (partitionColumns.nonEmpty) {
      // Hash partitioning on partition columns (like Delta Lake)
      val resolver = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      val partitionExprs = partitionColumns.map { colName =>
        output.find(attr => resolver(colName, attr.name)).getOrElse {
          throw new IllegalArgumentException(s"Partition column '$colName' not found in output: ${output.map(_.name)}")
        }
      }
      HashPartitioning(partitionExprs, optimalPartitions)
    } else {
      // Round-robin partitioning for non-partitioned tables
      RoundRobinPartitioning(optimalPartitions)
    }

    // Create the shuffle exchange
    val shuffleExec = ShuffleExchangeExec(partitioning, child)
    val shuffledRDD = shuffleExec.execute()

    // Add instrumentation to track actual record distribution
    shuffledRDD.mapPartitionsWithIndex { (partitionId, iter) =>
      var recordCount = 0L
      val result = iter.map { row =>
        recordCount += 1
        inputRecordsMetric.add(1)
        row
      }
      
      // Log partition statistics (for debugging/monitoring)
      if (recordCount > 0) {
        logger.debug(s"Partition $partitionId contains $recordCount records")
      }
      
      result
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): Tantivy4SparkOptimizedWriterExec = {
    copy(child = newChild)
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$nodeName
       |Target records per split: $targetRecordsPerSplit
       |Partition columns: ${partitionColumns.mkString(", ")}
       |""".stripMargin
  }
}