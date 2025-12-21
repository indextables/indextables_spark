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

import java.util.{Collections => JCollections}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import io.indextables.spark.transaction.{AddAction, PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.util.ConfigUtils
import io.indextables.tantivy4java.split.{SplitInfo, SplitMatchAllQuery}

import org.slf4j.LoggerFactory

/**
 * SQL command to prewarm prescan filter caches for a table.
 *
 * Syntax:
 *   PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/path'
 *   PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/path' ON FIELDS(title, author)
 *   PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/path' WHERE date = '2024-01-01'
 *   PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/path' ON FIELDS(title) WHERE region = 'us-east'
 *
 * This command:
 *   1. Reads the transaction log to get all active splits
 *   2. Optionally filters by partition predicates (WHERE clause)
 *   3. Loads split footers (term dictionaries) into the cache
 *   4. Returns statistics about the prewarm operation
 *
 * @param pathOption Optional path to the table
 * @param tableOption Optional table identifier
 * @param fields Fields to prewarm (empty = all indexed fields)
 * @param wherePredicates Partition predicates to filter splits
 */
case class PrewarmPrescanFiltersCommand(
  pathOption: Option[String],
  tableOption: Option[TableIdentifier],
  fields: Seq[String],
  wherePredicates: Seq[String]
) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmPrescanFiltersCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("splits_processed", LongType, nullable = false)(),
    AttributeReference("splits_cached", LongType, nullable = false)(),
    AttributeReference("cache_errors", LongType, nullable = false)(),
    AttributeReference("time_ms", LongType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Executing PREWARM INDEXTABLES PRESCAN FILTERS command")

    try {
      // Resolve the table path
      val tablePath = resolveTablePath(sparkSession).getOrElse {
        throw new IllegalArgumentException(
          "PREWARM INDEXTABLES PRESCAN FILTERS requires a table path or identifier"
        )
      }

      logger.info(s"Prewarming prescan filters for table: $tablePath")

      // Get cache config from Spark configuration
      val sparkConf = sparkSession.sparkContext.getConf
      val configMap = sparkConf.getAll.toMap
      val cacheConfig = ConfigUtils.createSplitCacheConfig(configMap, Some(tablePath))

      // Read the transaction log to get all active splits
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap[String, String]())
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), sparkSession, options)

      try {
        var allSplits = transactionLog.listFiles()

        logger.info(s"Found ${allSplits.length} total splits in transaction log")

        // Get partition schema for predicate filtering
        val partitionColumns = transactionLog.getPartitionColumns()
        val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(partitionColumns)

        // Apply partition predicates if WHERE clause provided
        if (wherePredicates.nonEmpty) {
          val parsedPredicates = PartitionPredicateUtils.parseAndValidatePredicates(
            wherePredicates,
            partitionSchema,
            sparkSession
          )
          allSplits = PartitionPredicateUtils.filterAddActionsByPredicates(
            allSplits,
            partitionSchema,
            parsedPredicates
          )
          logger.info(s"After partition filtering: ${allSplits.length} splits")
        }

        // Filter splits that have footer offsets (required for prescan)
        val prescannableSplits = allSplits.filter(_.footerStartOffset.isDefined)
        val unprescannableSplits = allSplits.length - prescannableSplits.length

        if (unprescannableSplits > 0) {
          logger.warn(s"$unprescannableSplits splits lack footer offsets and cannot be prewarmed")
        }

        if (prescannableSplits.isEmpty) {
          val timeMs = System.currentTimeMillis() - startTime
          return Seq(Row(
            "no_action",
            0L,
            0L,
            0L,
            timeMs,
            "No splits available for prewarming"
          ))
        }

        // Get docMappingJson from first AddAction (all splits in same table share the same mapping)
        val docMappingJson = prescannableSplits.headOption.flatMap(_.docMappingJson).getOrElse {
          throw new IllegalStateException("Splits missing docMappingJson - table may not support prescan")
        }

        // Prewarm the splits
        val result = prewarmSplits(prescannableSplits, docMappingJson, cacheConfig)

        val timeMs = System.currentTimeMillis() - startTime

        logger.info(s"Prewarm completed in ${timeMs}ms: " +
          s"${result.cached} of ${result.processed} splits cached, ${result.errors} errors")

        Seq(Row(
          "success",
          result.processed.toLong,
          result.cached.toLong,
          result.errors.toLong,
          timeMs,
          s"Prewarmed ${result.cached} splits (${result.errors} errors) in ${timeMs}ms"
        ))
      } finally {
        transactionLog.close()
      }

    } catch {
      case ex: Exception =>
        val timeMs = System.currentTimeMillis() - startTime
        logger.error("Error prewarming prescan filters", ex)
        Seq(Row(
          "failed",
          0L,
          0L,
          1L,
          timeMs,
          s"Prewarm failed: ${ex.getMessage}"
        ))
    }
  }

  /**
   * Prewarm splits by loading their footer data into the cache.
   *
   * @param splits Splits to prewarm
   * @param docMappingJson Document mapping JSON for the table
   * @param cacheConfig Cache configuration
   * @return PrewarmResult with statistics
   */
  private def prewarmSplits(
    splits: Seq[AddAction],
    docMappingJson: String,
    cacheConfig: SplitCacheConfig
  ): PrewarmResult = {
    // Get the global cache manager
    val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

    // Build SplitInfo objects with their paths for error reporting
    // Use footerEndOffset as the fileSize (not action.size which may be incorrect from merge)
    val splitInfosWithPath = splits.flatMap { action =>
      for {
        footerStart <- action.footerStartOffset
        footerEnd <- action.footerEndOffset
      } yield (action.path, new SplitInfo(action.path, footerStart, footerEnd))
    }

    // Determine concurrency based on available processors
    val concurrency = Math.min(Runtime.getRuntime.availableProcessors() * 4, splitInfosWithPath.length)
    val executor = Executors.newFixedThreadPool(Math.max(1, concurrency))

    var cached = 0
    var errors = 0

    try {
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

      // Prewarm in parallel using a simple MatchAll query to load the FST data
      val futures = splitInfosWithPath.map { case (path, splitInfo) =>
        Future {
          try {
            // Use prescanSplits with MatchAll query to load the footer/FST data
            val splitList = JCollections.singletonList(splitInfo)
            cacheManager.prescanSplits(splitList, docMappingJson, new SplitMatchAllQuery())
            true
          } catch {
            case ex: Exception =>
              logger.warn(s"Failed to prewarm split $path: ${ex.getMessage}")
              false
          }
        }
      }

      // Wait for all futures with a reasonable timeout
      val timeoutMs = 300000L // 5 minutes max
      val results = Await.result(Future.sequence(futures), timeoutMs.milliseconds)

      cached = results.count(_ == true)
      errors = results.count(_ == false)

    } finally {
      executor.shutdown()
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          executor.shutdownNow()
      }
    }

    PrewarmResult(processed = splitInfosWithPath.length, cached = cached, errors = errors)
  }

  /**
   * Resolve the table path from pathOption or tableOption.
   *
   * @param sparkSession SparkSession for catalog resolution
   * @return Some(path) for a specific table, None if not specified
   */
  private def resolveTablePath(sparkSession: SparkSession): Option[String] = {
    if (pathOption.isDefined) {
      pathOption
    } else if (tableOption.isDefined) {
      // Use the table identifier as the path
      Some(tableOption.get.unquotedString)
    } else {
      None
    }
  }
}

/**
 * Result of a prewarm operation.
 *
 * @param processed Total splits processed
 * @param cached Successfully cached splits
 * @param errors Splits that failed to cache
 */
case class PrewarmResult(
  processed: Int,
  cached: Int,
  errors: Int
)
