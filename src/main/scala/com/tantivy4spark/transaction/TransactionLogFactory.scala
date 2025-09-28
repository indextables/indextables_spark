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

package com.tantivy4spark.transaction

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._

/**
 * Factory for creating transaction log instances.
 * Automatically selects optimized or standard implementation based on configuration.
 */
object TransactionLogFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a transaction log instance with automatic optimization selection.
   *
   * @param tablePath The path to the table
   * @param spark The Spark session
   * @param options Configuration options
   * @return A TransactionLog instance (optimized or standard)
   */
  def create(
      tablePath: Path,
      spark: SparkSession,
      options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
  ): TransactionLog = {

    // Check if optimization is explicitly disabled
    val useOptimized = options.getBoolean("spark.indextables.transaction.optimized.enabled",
                                          options.getBoolean("spark.tantivy4spark.transaction.optimized.enabled", true))

    // Always use optimized implementation - the old TransactionLog class is deprecated
    logger.info(s"Creating transaction log for $tablePath (useOptimized setting: $useOptimized)")

    // Create adapter that wraps OptimizedTransactionLog to match TransactionLog interface
    new TransactionLogAdapter(new OptimizedTransactionLog(tablePath, spark, options), spark, options)
  }
}

/**
 * Adapter to make OptimizedTransactionLog compatible with existing TransactionLog interface.
 * This allows seamless integration without changing existing code.
 */
class TransactionLogAdapter(
  private val optimizedLog: OptimizedTransactionLog,
  spark: SparkSession,
  options: CaseInsensitiveStringMap
) extends TransactionLog(
  optimizedLog.getTablePath(),
  spark,
  new CaseInsensitiveStringMap(
    (options.asCaseSensitiveMap().asScala + ("spark.tantivy4spark.transaction.allowDirectUsage" -> "true")).asJava
  )
) {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogAdapter])

  override def getTablePath(): Path = optimizedLog.getTablePath()

  override def close(): Unit = {
    optimizedLog.close()
  }

  override def initialize(schema: org.apache.spark.sql.types.StructType): Unit = {
    optimizedLog.initialize(schema)
  }

  override def initialize(schema: org.apache.spark.sql.types.StructType, partitionColumns: Seq[String]): Unit = {
    optimizedLog.initialize(schema, partitionColumns)
  }

  override def addFile(addAction: AddAction): Long = {
    optimizedLog.addFiles(Seq(addAction))
  }

  override def addFiles(addActions: Seq[AddAction]): Long = {
    optimizedLog.addFiles(addActions)
  }

  override def overwriteFiles(addActions: Seq[AddAction]): Long = {
    optimizedLog.overwriteFiles(addActions)
  }

  override def listFiles(): Seq[AddAction] = {
    optimizedLog.listFiles()
  }

  override def getTotalRowCount(): Long = {
    optimizedLog.getTotalRowCount()
  }

  override def getPartitionColumns(): Seq[String] = {
    optimizedLog.getPartitionColumns()
  }

  override def isPartitioned(): Boolean = {
    optimizedLog.isPartitioned()
  }

  override def getMetadata(): MetadataAction = {
    optimizedLog.getMetadata()
  }

  // Note: Some methods like mergeFiles, getLatestVersion, readVersion, getVersions
  // are not in the base TransactionLog class but could be added if needed.
  // The OptimizedTransactionLog handles these operations internally.
}