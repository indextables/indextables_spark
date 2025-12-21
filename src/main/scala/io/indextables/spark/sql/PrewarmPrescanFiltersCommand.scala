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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import io.indextables.spark.transaction.{AddAction, PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.util.{ConfigUtils, JsonUtil}
import io.indextables.tantivy4java.split.{SplitBooleanQuery, SplitInfo, SplitQuery, SplitTermQuery}

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

        // Determine which fields to prewarm
        val fieldsToWarm = if (fields.nonEmpty) {
          fields
        } else {
          // Parse docMappingJson to get all field names
          extractFieldNamesFromDocMapping(docMappingJson)
        }

        logger.info(s"Prewarming ${fieldsToWarm.length} fields: ${fieldsToWarm.mkString(", ")}")

        // Build a query that touches each field to load its FST data
        val prewarmQuery = buildPrewarmQuery(fieldsToWarm)

        // Prewarm the splits
        val result = prewarmSplits(prescannableSplits, docMappingJson, prewarmQuery, cacheConfig)

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
   * Extract field names from docMappingJson.
   * The docMappingJson is a JSON array of field definitions with "name" properties.
   */
  private def extractFieldNamesFromDocMapping(docMappingJson: String): Seq[String] = {
    try {
      val mapping = JsonUtil.mapper.readTree(docMappingJson)

      if (mapping.isArray) {
        // docMappingJson is an array of field definitions
        mapping.elements().asScala.flatMap { field =>
          Option(field.get("name")).map(_.asText())
        }.toSeq
      } else if (mapping.has("fields")) {
        // Alternative format with "fields" object
        val fields = mapping.get("fields")
        fields.fieldNames().asScala.toSeq
      } else {
        logger.warn(s"Unrecognized docMappingJson format, cannot extract field names")
        Seq.empty
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to parse docMappingJson for field extraction: ${ex.getMessage}")
        Seq.empty
    }
  }

  /**
   * Build a prescan query that touches each field to load its FST data.
   * Uses term queries with a dummy value that will trigger FST lookups.
   */
  private def buildPrewarmQuery(fieldsToWarm: Seq[String]): SplitQuery = {
    if (fieldsToWarm.isEmpty) {
      // Fallback: use a simple term query on a common field
      new SplitTermQuery("_", "_prewarm_")
    } else if (fieldsToWarm.length == 1) {
      // Single field - just use a term query
      new SplitTermQuery(fieldsToWarm.head, "_prewarm_")
    } else {
      // Multiple fields - combine with OR (SHOULD) to touch all FSTs
      val boolQuery = new SplitBooleanQuery()
      fieldsToWarm.foreach { fieldName =>
        val termQuery = new SplitTermQuery(fieldName, "_prewarm_")
        boolQuery.addShould(termQuery)
      }
      boolQuery
    }
  }

  /**
   * Prewarm splits by loading their footer data into the cache.
   *
   * @param splits Splits to prewarm
   * @param docMappingJson Document mapping JSON for the table
   * @param prewarmQuery Query that touches each field to load FST data
   * @param cacheConfig Cache configuration
   * @return PrewarmResult with statistics
   */
  private def prewarmSplits(
    splits: Seq[AddAction],
    docMappingJson: String,
    prewarmQuery: SplitQuery,
    cacheConfig: SplitCacheConfig
  ): PrewarmResult = {
    // Get the global cache manager
    val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

    // Build SplitInfo objects
    // Use footerEndOffset as the fileSize (not action.size which may be incorrect from merge)
    val splitInfos = splits.flatMap { action =>
      for {
        footerStart <- action.footerStartOffset
        footerEnd <- action.footerEndOffset
      } yield new SplitInfo(action.path, footerStart, footerEnd)
    }

    if (splitInfos.isEmpty) {
      return PrewarmResult(processed = 0, cached = 0, errors = 0)
    }

    // Make a single call to prescanSplits with all splits
    // Parallelism is handled internally by tantivy4java/Rust
    try {
      val splitList = splitInfos.asJava
      val results = cacheManager.prescanSplits(splitList, docMappingJson, prewarmQuery)

      // Count successes and errors from the results
      val cached = results.asScala.count(r => r.couldHaveResults() || !hasError(r))
      val errors = results.asScala.count(r => hasError(r))

      PrewarmResult(processed = splitInfos.length, cached = cached, errors = errors)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to prewarm splits: ${ex.getMessage}", ex)
        PrewarmResult(processed = splitInfos.length, cached = 0, errors = splitInfos.length)
    }
  }

  /**
   * Check if a prescan result has an error.
   */
  private def hasError(result: io.indextables.tantivy4java.split.PrescanResult): Boolean = {
    try {
      val status = result.getStatus
      status.toString != "SUCCESS"
    } catch {
      case _: Exception => false
    }
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
