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

package io.indextables.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.slf4j.LoggerFactory

/**
 * SQL command to invalidate transaction log cache for specific tables or globally.
 *
 * Syntax:
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR '/path/to/table'
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR table_name
 *
 * Examples:
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE -- invalidate all caches globally
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR '/path/to/my_table' -- invalidate cache for specific table
 *   - INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR my_table -- invalidate cache for specific table
 *
 * This command:
 *   1. Invalidates cached transaction log data (versions, files, metadata) 2. Forces subsequent reads to fetch fresh
 *      data from storage 3. Can target specific tables or invalidate all caches globally 4. Returns statistics about
 *      cache state before and after invalidation
 */
abstract class InvalidateTransactionLogCacheCommandBase extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("result", StringType)(),
    AttributeReference("cache_hits_before", LongType)(),
    AttributeReference("cache_misses_before", LongType)(),
    AttributeReference("hit_rate_before", StringType)()
  )
}

/** INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE command implementation for Spark SQL. */
case class InvalidateTransactionLogCacheCommand(
  override val child: LogicalPlan,
  tablePath: Option[String] = None)
    extends InvalidateTransactionLogCacheCommandBase
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[InvalidateTransactionLogCacheCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): InvalidateTransactionLogCacheCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] =
    tablePath match {
      case Some(path) =>
        // Invalidate cache for specific table
        invalidateTableCache(path, sparkSession)
      case None =>
        // Global invalidation - invalidate all executor caches
        logger.info("Global transaction log cache invalidation requested")
        invalidateAllExecutorCaches(sparkSession)
        Seq(
          Row(
            "GLOBAL",
            "Global cache invalidation completed - all executor caches invalidated",
            0L,
            0L,
            "N/A"
          )
        )
    }

  private def invalidateTableCache(path: String, sparkSession: SparkSession): Seq[Row] =
    try {
      // Resolve the table path
      val resolvedPath = resolveTablePath(path, sparkSession)

      // Create a temporary TransactionLog to access the cache
      val transactionLog = TransactionLogFactory.create(
        resolvedPath,
        sparkSession,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      )

      try {
        // Try to access the transaction log to validate it exists and has been initialized
        // This will throw an exception if the transaction log doesn't exist or is invalid
        transactionLog.listFiles()
        transactionLog.getMetadata()

        // If we get here without an exception, the transaction log exists and is valid

        // Get cache statistics before invalidation
        val statsBefore = transactionLog.getCacheStats()

        val (hitsBefore, missesBefore, hitRateBefore) = statsBefore match {
          case Some(stats) =>
            (stats.hits, stats.misses, f"${stats.hitRate * 100}%.1f%%")
          case None =>
            (0L, 0L, "Cache disabled")
        }

        // Invalidate the cache
        transactionLog.invalidateCache()

        // Also invalidate executor caches via RDD operation
        // This works for both distributed and non-distributed transaction logs
        logger.info(s"Invalidating executor caches via RDD operation for table: $path")
        invalidateExecutorCachesForTable(sparkSession, Some(resolvedPath))

        val result = statsBefore match {
          case Some(_) => "Transaction log cache invalidated successfully (driver and executors)"
          case None    => "Transaction log cache is disabled for this table"
        }

        logger.info(s"Invalidated transaction log cache for table: $path")

        Seq(
          Row(
            resolvedPath.toString,
            result,
            hitsBefore,
            missesBefore,
            hitRateBefore
          )
        )
      } finally
        transactionLog.close()
    } catch {
      case e: IllegalArgumentException =>
        val errorMsg = s"Table or path not found: $path"
        logger.warn(errorMsg, e)
        Seq(
          Row(
            path,
            errorMsg,
            0L,
            0L,
            "N/A"
          )
        )
      case e: Exception =>
        val errorMsg = s"Failed to invalidate cache: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(
          Row(
            path,
            errorMsg,
            0L,
            0L,
            "N/A"
          )
        )
    }

  /** Resolve table path from string path or table identifier. */
  private def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path =
    if (
      pathOrTable.startsWith("/") || pathOrTable.startsWith("s3://") || pathOrTable.startsWith("s3a://") ||
      pathOrTable.startsWith("hdfs://") || pathOrTable.startsWith("file://")
    ) {
      // It's a path
      new Path(pathOrTable)
    } else {
      // Try to resolve as table identifier
      try {
        val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(pathOrTable)
        val catalog         = sparkSession.sessionState.catalog
        if (catalog.tableExists(tableIdentifier)) {
          val tableMetadata = catalog.getTableMetadata(tableIdentifier)
          new Path(tableMetadata.location)
        } else {
          throw new IllegalArgumentException(s"Table not found: $pathOrTable")
        }
      } catch {
        case _: Exception =>
          // If it fails as a table identifier, treat it as a path
          new Path(pathOrTable)
      }
    }

  /**
   * Invalidate executor caches for a specific table via RDD operation.
   * This ensures cache invalidation happens on all executors.
   *
   * @param spark SparkSession
   * @param tablePathOpt Optional table path (None for global invalidation)
   */
  private def invalidateExecutorCachesForTable(
    spark: SparkSession,
    tablePathOpt: Option[Path]
  ): Unit = {
    val tablePathStr = tablePathOpt.map(_.toString).getOrElse("*")

    try {
      // Run a dummy RDD operation to execute on all executors
      val numPartitions = spark.sparkContext.defaultParallelism
      spark.sparkContext
        .parallelize(1 to numPartitions, numPartitions)
        .foreach { _ =>
          // Runs on executor
          if (tablePathStr == "*") {
            io.indextables.spark.transaction.TransactionFileCache.invalidate("") // Invalidate all
          } else {
            io.indextables.spark.transaction.TransactionFileCache.invalidate(tablePathStr)
          }
        }

      logger.info(s"Successfully invalidated executor caches for table: $tablePathStr")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to invalidate executor caches for table $tablePathStr: ${e.getMessage}", e)
    }
  }

  /**
   * Invalidate all executor caches globally.
   * Used for global cache invalidation command.
   *
   * @param spark SparkSession
   */
  private def invalidateAllExecutorCaches(spark: SparkSession): Unit = {
    logger.info("Invalidating all executor caches globally")
    invalidateExecutorCachesForTable(spark, None)
  }
}

object InvalidateTransactionLogCacheCommand {

  /**
   * Alternate constructor that converts a provided path or table identifier into the correct child LogicalPlan node.
   */
  def apply(
    path: Option[String]
  ): InvalidateTransactionLogCacheCommand = {
    // Create a simple leaf plan for the command
    val plan = UnresolvedTransactionLogCacheTarget(path, "INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE")
    InvalidateTransactionLogCacheCommand(plan, path)
  }
}

/** Placeholder for unresolved transaction log cache target. */
case class UnresolvedTransactionLogCacheTarget(
  path: Option[String],
  commandName: String)
    extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}
