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

package io.indextables.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import io.indextables.spark.transaction.TransactionLogInterface

/**
 * Streaming Scan implementation for IndexTables4Spark.
 *
 * This class implements Spark's Scan interface for streaming reads and provides
 * the MicroBatchStream when toMicroBatchStream() is called.
 *
 * @param sparkSession Active Spark session
 * @param tablePath Path to the IndexTables4Spark table
 * @param transactionLog Transaction log interface
 * @param readSchema Schema for reading data (may be column-pruned)
 * @param fullTableSchema Full table schema for type lookup
 * @param partitionColumns Partition column names
 * @param pushedFilters Filters pushed down from Spark
 * @param indexQueryFilters IndexQuery filters for full-text search
 * @param config Configuration map
 */
class IndexTables4SparkStreamingScan(
    sparkSession: SparkSession,
    tablePath: String,
    transactionLog: TransactionLogInterface,
    readSchema: StructType,
    fullTableSchema: StructType,
    partitionColumns: Seq[String],
    pushedFilters: Array[Filter],
    indexQueryFilters: Array[Any],
    config: Map[String, String]
) extends Scan {

  override def readSchema(): StructType = readSchema

  /**
   * Creates a MicroBatchStream for streaming execution.
   *
   * @param checkpointLocation Checkpoint location for offset persistence (managed by Spark)
   * @return MicroBatchStream implementation
   */
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new IndexTables4SparkMicroBatchStream(
      sparkSession = sparkSession,
      tablePath = tablePath,
      transactionLog = transactionLog,
      readSchema = readSchema,
      fullTableSchema = fullTableSchema,
      partitionColumns = partitionColumns,
      pushedFilters = pushedFilters,
      indexQueryFilters = indexQueryFilters,
      config = config
    )
  }

  override def description(): String = {
    s"IndexTables4SparkStreamingScan[path=$tablePath, filters=${pushedFilters.length} pushed, " +
      s"indexQueries=${indexQueryFilters.length}]"
  }
}
