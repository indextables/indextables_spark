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

package io.indextables.spark.optimize

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.slf4j.LoggerFactory
import io.indextables.spark.config.Tantivy4SparkSQLConf

/**
 * An execution node that optimizes writes to Tantivy4Spark tables by adding an adaptive shuffle to target a specific
 * number of records per split file.
 *
 * This is similar to Delta Lake's DeltaOptimizedWriterExec, but adapted for Tantivy4Spark's split-based architecture.
 *
 * @param child
 *   The child execution plan
 * @param partitionColumns
 *   The partition columns for hash partitioning (if any)
 * @param targetRecordsPerSplit
 *   Target number of records per output split file
 */
case class Tantivy4SparkOptimizedWriterExec(
  child: SparkPlan,
  partitionColumns: Seq[String] = Seq.empty,
  targetRecordsPerSplit: Long = 1000000L)
    extends UnaryExecNode {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkOptimizedWriterExec])

  override def output: Seq[Attribute] = child.output

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "inputRecords"           -> SQLMetrics.createMetric(sparkContext, "number of input records"),
    "outputPartitions"       -> SQLMetrics.createMetric(sparkContext, "number of output partitions"),
    "avgRecordsPerPartition" -> SQLMetrics.createMetric(sparkContext, "average records per partition")
  )

  /** Calculate the optimal number of shuffle partitions based on the target records per split. */
  private def calculateOptimalPartitions(): Int = {
    logger.warn(s"🔍 CALCULATE PARTITIONS DEBUG: Starting with targetRecordsPerSplit=$targetRecordsPerSplit")
    val spark = SparkSession.active

    // Try to get actual row count for better accuracy
    val estimatedRows =
      try {
        // For small datasets, we can afford to count the actual rows
        val currentPartitions = child.execute().getNumPartitions

        // If dataset is small (few partitions), count actual rows for accuracy
        if (currentPartitions <= 20) {
          val rdd         = child.execute()
          val actualCount = rdd.count()
          logger.info(s"Actual row count for small dataset: $actualCount")
          actualCount
        } else {
          // For larger datasets, use improved heuristic based on statistics
          val estimatePerPartition = 50000L // More reasonable baseline for larger datasets
          val estimate             = currentPartitions * estimatePerPartition
          logger.info(s"Estimated row count for large dataset: $estimate ($currentPartitions partitions)")
          estimate
        }
      } catch {
        case e: Exception =>
          logger.warn("Could not estimate row count from plan", e)
          100000L // Default fallback estimate
      }

    // Calculate target partitions based on records per split (using ceiling to ensure proper coverage)
    val targetPartitions = Math.max(1, Math.ceil(estimatedRows.toDouble / targetRecordsPerSplit.toDouble).toInt)

    // Apply min/max constraints from configuration
    val maxPartitions = spark.conf
      .getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS)
      .map(_.toInt)
      .getOrElse(2000)
    val minPartitions = spark.conf
      .getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_MIN_SHUFFLE_PARTITIONS)
      .map(_.toInt)
      .getOrElse(1)

    val optimalPartitions = Math.min(Math.max(targetPartitions, minPartitions), maxPartitions)

    logger.info(
      s"Optimized write: estimated $estimatedRows records, " +
        s"target $targetRecordsPerSplit per split, " +
        s"using $optimalPartitions partitions"
    )

    optimalPartitions
  }

  override protected def doExecute() = {
    logger.warn(s"🔍 OPTIMIZED WRITE DEBUG: doExecute() called with targetRecordsPerSplit=$targetRecordsPerSplit")
    val optimalPartitions = calculateOptimalPartitions()

    // Update metrics
    val inputRecordsMetric           = longMetric("inputRecords")
    val outputPartitionsMetric       = longMetric("outputPartitions")
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

    // Set descriptive names for Spark UI
    val jobGroup = "tantivy4spark-optimized-write"
    val jobDescription =
      s"Optimized Write: ${shuffleExec.outputPartitioning.numPartitions} partitions, target $targetRecordsPerSplit records/split"
    val stageName = s"Optimized Write: ${shuffleExec.outputPartitioning.numPartitions} partitions"

    sparkContext.setJobGroup(jobGroup, jobDescription, interruptOnCancel = true)

    // Add instrumentation to track actual record distribution
    val optimizedRDD =
      try {
        shuffledRDD.setName(stageName)
        shuffledRDD
          .mapPartitionsWithIndex { (partitionId, iter) =>
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
          .setName("Optimized Write Results")
      } finally
        sparkContext.clearJobGroup()

    optimizedRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): Tantivy4SparkOptimizedWriterExec =
    copy(child = newChild)

  override def verboseStringWithOperatorId(): String =
    s"""
       |$nodeName
       |Target records per split: $targetRecordsPerSplit
       |Partition columns: ${partitionColumns.mkString(", ")}
       |""".stripMargin
}
