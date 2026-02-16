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

package io.indextables.spark.core

import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, SortOrder}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RequiresDistributionAndOrdering}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLog
import io.indextables.spark.write.{OptimizedWriteConfig, WriteSizeEstimator}
import org.slf4j.LoggerFactory

/**
 * Optimized write implementation that mixes in RequiresDistributionAndOrdering to request a shuffle before writing.
 * This produces well-sized splits by clustering data by partition columns and using an advisory partition size based on
 * transaction log history or a sampling ratio.
 *
 * All commit, writer factory, data writer, merge-on-write, and purge-on-write logic is inherited unchanged from
 * IndexTables4SparkStandardWrite.
 */
class IndexTables4SparkOptimizedWrite(
  @transient transactionLog: TransactionLog,
  tablePath: Path,
  @transient writeInfo: LogicalWriteInfo,
  serializedOptions: Map[String, String],
  @transient hadoopConf: org.apache.hadoop.conf.Configuration,
  isOverwrite: Boolean,
  config: OptimizedWriteConfig)
    extends IndexTables4SparkStandardWrite(
      transactionLog,
      tablePath,
      writeInfo,
      serializedOptions,
      hadoopConf,
      isOverwrite
    )
    with RequiresDistributionAndOrdering {

  @transient private val optLogger = LoggerFactory.getLogger(classOf[IndexTables4SparkOptimizedWrite])

  // Capture first schema field name eagerly (writeInfo is @transient, unavailable after serialization)
  private val firstSchemaField: String = writeInfo.schema().fieldNames.head

  private lazy val advisorySize: Long = {
    val estimator = new WriteSizeEstimator(transactionLog, config)
    estimator.calculateAdvisoryPartitionSize()
  }

  private lazy val defaultParallelism: Int =
    org.apache.spark.sql.SparkSession.active.sparkContext.defaultParallelism

  override def requiredDistribution(): Distribution =
    config.distributionMode match {
      case "hash" | "balanced" =>
        if (partitionColumns.nonEmpty) {
          val exprs = partitionColumns.map(col => Expressions.column(col).asInstanceOf[Expression]).toArray
          optLogger.info(s"Requesting clustered distribution on columns: ${partitionColumns.mkString(", ")} (mode=${config.distributionMode})")
          Distributions.clustered(exprs)
        } else {
          // Unpartitioned: cluster by a single column just to trigger a shuffle so AQE can
          // coalesce using the advisory size. Spark 3.5 rejects empty clustering + advisory > 0
          // (DistributionAndOrderingUtils treats empty clustering as "no distribution"), so we
          // need at least one expression.
          val expr = Array(Expressions.column(firstSchemaField).asInstanceOf[Expression])
          optLogger.info(s"Requesting clustered distribution on column: $firstSchemaField (unpartitioned, mode=${config.distributionMode})")
          Distributions.clustered(expr)
        }
      case _ =>
        optLogger.info("Using unspecified distribution (mode=none)")
        Distributions.unspecified()
    }

  override def distributionStrictlyRequired(): Boolean =
    config.distributionMode == "balanced"

  override def requiredOrdering(): Array[SortOrder] = Array.empty

  override def requiredNumPartitions(): Int =
    if (config.distributionMode == "balanced") {
      optLogger.info(s"Balanced mode: requiredNumPartitions=$defaultParallelism (defaultParallelism)")
      defaultParallelism
    } else {
      0 // Let Spark decide
    }

  override def advisoryPartitionSizeInBytes(): Long =
    config.distributionMode match {
      case "hash" =>
        optLogger.info(s"Advisory partition size: ${config.targetSplitSizeString} ($advisorySize bytes)")
        advisorySize
      case _ =>
        // balanced: uses requiredNumPartitions instead
        // none: Spark doesn't support advisory size with UnspecifiedDistribution
        0L
    }
}
