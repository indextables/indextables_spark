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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.prewarm.IndexComponentMapping
import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.{PartitionPredicateUtils, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils, ProtocolNormalizer, SplitMetadataFactory}
import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.slf4j.LoggerFactory

/**
 * SQL command to prewarm index caches across all executors.
 *
 * Syntax:
 * {{{
 * PREWARM INDEXTABLES CACHE 's3://bucket/path'
 *    [FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM, DOC_STORE)]
 *    [ON FIELDS (field1, field2, fieldN)]
 *    [WITH PERWORKER PARALLELISM OF N]
 *    [WHERE partition_col = 'value']
 * }}}
 *
 * Segment aliases are mapped to tantivy4java IndexComponent:
 *   - TERM_DICT / TERM_DICTIONARY -> IndexComponent.TERM
 *   - FAST_FIELD / FASTFIELD -> IndexComponent.FASTFIELD
 *   - POSTINGS / POSTING_LISTS -> IndexComponent.POSTINGS
 *   - FIELD_NORM / FIELDNORM -> IndexComponent.FIELDNORM
 *   - DOC_STORE / STORE -> IndexComponent.STORE
 *
 * Default segments (when not specified): TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM (excludes DOC_STORE)
 * Default fields: All fields in the index
 * Default parallelism: 2 splits per task
 *
 * @param tablePath Table path or identifier
 * @param segments Sequence of segment names to prewarm (empty = use defaults)
 * @param fields Optional sequence of field names to prewarm (None = all fields)
 * @param splitsPerTask Number of splits per Spark task (default: 2)
 * @param wherePredicates Partition predicates for filtering splits
 */
case class PrewarmCacheCommand(
  tablePath: String,
  segments: Seq[String],
  fields: Option[Seq[String]],
  splitsPerTask: Int,
  wherePredicates: Seq[String],
  failOnMissingField: Boolean = true
) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("splits_prewarmed", IntegerType, nullable = false)(),
    AttributeReference("segments", StringType, nullable = false)(),
    AttributeReference("fields", StringType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("skipped_fields", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Starting prewarm cache for table: $tablePath")
    val startTime = System.currentTimeMillis()

    val sc = sparkSession.sparkContext

    // Resolve segment aliases to IndexComponent set
    val resolvedSegments: Set[IndexComponent] = if (segments.isEmpty) {
      logger.info("Using default segments: TERM, FASTFIELD, POSTINGS, FIELDNORM")
      IndexComponentMapping.defaultComponents
    } else {
      val resolved = segments.map { seg =>
        IndexComponentMapping.aliasToComponent.getOrElse(
          seg,
          throw new IllegalArgumentException(
            s"Unknown segment type: $seg. Valid types: ${IndexComponentMapping.aliasToComponent.keys.mkString(", ")}"
          )
        )
      }.toSet
      logger.info(s"Using specified segments: ${resolved.map(_.name()).mkString(", ")}")
      resolved
    }

    // Get session config for credentials and cache settings
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(sparkSession.sparkContext.hadoopConfiguration)
    val sessionConfig = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Create transaction log to read splits
    val transactionLog = TransactionLogFactory.create(
      new Path(tablePath),
      sparkSession,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    try {
      // Get partition schema for predicate validation
      val metadata        = transactionLog.getMetadata()
      val partitionSchema = StructType(metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true)))

      // Get all active splits
      var addActions = transactionLog.listFiles()
      logger.info(s"Found ${addActions.length} splits in transaction log")

      // Apply partition predicates if specified
      if (wherePredicates.nonEmpty) {
        val parsedPredicates =
          PartitionPredicateUtils.parseAndValidatePredicates(wherePredicates, partitionSchema, sparkSession)
        addActions = PartitionPredicateUtils.filterAddActionsByPredicates(addActions, partitionSchema, parsedPredicates)
        logger.info(s"After partition filtering: ${addActions.length} splits")
      }

      if (addActions.isEmpty) {
        logger.info("No splits to prewarm")
        return Seq(
          Row(
            "none",
            0,
            resolvedSegments.map(_.name()).toSeq.sorted.mkString(","),
            fields.map(_.mkString(",")).getOrElse("all"),
            0L,
            "no_splits",
            null
          )
        )
      }

      // Get available hosts and assign splits
      val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sc)
      val splitPaths     = addActions.map(_.path)
      val assignments    = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

      // Group splits by assigned host
      val splitsByHost = addActions
        .groupBy(action => assignments.getOrElse(action.path, "any"))
        .filter(_._1 != "any")

      logger.info(s"Distributing prewarm across ${splitsByHost.size} hosts")

      // Read failOnMissingField config from session (can be overridden by command parameter)
      val effectiveFailOnMissingField = sessionConfig
        .get("spark.indextables.prewarm.failOnMissingField")
        .map(_.toBoolean)
        .getOrElse(failOnMissingField)

      // Create prewarm tasks with batching based on splitsPerTask
      val prewarmTasks = splitsByHost.flatMap {
        case (hostname, hostSplits) =>
          hostSplits.grouped(splitsPerTask).zipWithIndex.map {
            case (batch, batchIndex) =>
              PrewarmTask(
                batchIndex = batchIndex,
                hostname = hostname,
                addActions = batch,
                tablePath = tablePath,
                segments = resolvedSegments,
                fields = fields,
                failOnMissingField = effectiveFailOnMissingField
              )
          }
      }.toSeq

      logger.info(s"Created ${prewarmTasks.length} prewarm tasks (${splitsPerTask} splits per task)")

      // Broadcast config for executor access
      val broadcastConfig = sc.broadcast(sessionConfig)

      // Execute prewarm tasks on executors
      val jobGroup       = s"tantivy4spark-prewarm-${System.currentTimeMillis()}"
      val jobDescription = s"Prewarming ${addActions.length} splits across ${splitsByHost.size} hosts"

      sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

      // Create (task, preferredLocations) tuples for makeRDD
      // This ensures prewarm tasks run on the same hosts where queries will execute
      val tasksWithLocations = prewarmTasks.map { task =>
        (task, Seq(task.hostname))
      }

      val taskResults =
        try
          sc.makeRDD(tasksWithLocations)
            .setName(s"Prewarm Cache: ${addActions.length} splits")
            .map { task =>
              executePrewarmTask(task, broadcastConfig.value)
            }
            .collect()
            .toSeq
        finally
          sc.clearJobGroup()

      // Aggregate results by host
      val hostResults = taskResults
        .groupBy(_.hostname)
        .map {
          case (hostname, results) =>
            val totalSplits   = results.map(_.splitsPrewarmed).sum
            val totalDuration = results.map(_.durationMs).max // Use max since tasks run in parallel
            val allSkipped    = results.flatMap(_.skippedFields).distinct
            val hasErrors     = results.exists(_.status.startsWith("error"))
            val hasPartial    = results.exists(_.status == "partial")
            val overallStatus = if (hasErrors) "error" else if (hasPartial) "partial" else "success"
            val segmentsStr   = results.head.segments
            val fieldsStr     = results.head.fields
            val skippedStr    = if (allSkipped.isEmpty) null else allSkipped.mkString(",")

            Row(hostname, totalSplits, segmentsStr, fieldsStr, totalDuration, overallStatus, skippedStr)
        }
        .toSeq

      val totalDuration = System.currentTimeMillis() - startTime
      logger.info(s"Prewarm completed in ${totalDuration}ms: ${addActions.length} splits across ${hostResults.length} hosts")

      hostResults

    } finally
      transactionLog.close()
  }

  /**
   * Execute prewarm for a task (batch of splits) on an executor.
   */
  private def executePrewarmTask(
    task: PrewarmTask,
    config: Map[String, String]
  ): PrewarmTaskResult = {
    val taskStartTime = System.currentTimeMillis()
    val hostname      = java.net.InetAddress.getLocalHost.getHostName
    val taskLogger    = LoggerFactory.getLogger(classOf[PrewarmCacheCommand])

    try {
      // Create cache config
      val cacheConfig  = ConfigUtils.createSplitCacheConfig(config, Some(task.tablePath))
      val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

      var skippedFields: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty
      var prewarmedCount = 0

      task.addActions.foreach { addAction =>
        try {
          // Normalize path for tantivy4java compatibility
          // Check if path is already a full URL (S3 or Azure) or a relative path
          val fullPath = if (ProtocolNormalizer.isS3Path(addAction.path) || ProtocolNormalizer.isAzurePath(addAction.path)) {
            addAction.path
          } else {
            s"${task.tablePath}/${addAction.path}"
          }
          val actualPath = ProtocolNormalizer.normalizeAllProtocols(fullPath)

          // Create split metadata from AddAction
          val splitMetadata = SplitMetadataFactory.fromAddAction(addAction, task.tablePath)

          // Create split searcher
          val splitSearcher = cacheManager.createSplitSearcher(actualPath, splitMetadata)

          try {
            task.fields match {
              case Some(requestedFields) =>
                // Field-specific preloading
                val availableFields = extractFieldsFromMetadata(addAction)

                val (validFields, invalidFields) = if (availableFields.nonEmpty) {
                  requestedFields.partition(f => availableFields.contains(f.toLowerCase))
                } else {
                  // Can't validate - assume all fields are valid
                  (requestedFields, Seq.empty[String])
                }

                // Handle invalid fields based on failOnMissingField setting
                if (invalidFields.nonEmpty) {
                  if (task.failOnMissingField) {
                    throw new IllegalArgumentException(
                      s"Field(s) not found in split ${addAction.path}: ${invalidFields.mkString(", ")}. " +
                        s"Available fields: ${availableFields.mkString(", ")}"
                    )
                  } else {
                    taskLogger.warn(s"Skipping missing fields for split ${addAction.path}: ${invalidFields.mkString(", ")}")
                    skippedFields ++= invalidFields
                  }
                }

                if (validFields.nonEmpty) {
                  // Preload each component for the valid fields using preloadFields()
                  task.segments.foreach { component =>
                    taskLogger.debug(s"Preloading ${component.name()} for fields ${validFields.mkString(",")} in split $actualPath")
                    val future = splitSearcher.preloadFields(component, validFields: _*)
                    future.join()
                  }
                }

              case None =>
                // Full component preloading (all fields)
                val componentsArray = task.segments.toArray
                taskLogger.debug(s"Preloading components ${componentsArray.map(_.name()).mkString(",")} for all fields in split $actualPath")
                val prewarmFuture = splitSearcher.preloadComponents(componentsArray: _*)
                prewarmFuture.join()
            }

            prewarmedCount += 1

            // Record prewarm completion for catch-up tracking
            DriverSplitLocalityManager.recordPrewarmCompletion(
              addAction.path,
              hostname,
              task.segments,
              task.fields.map(_.toSet)
            )

          } finally
            splitSearcher.close()

        } catch {
          case e: IllegalArgumentException if task.failOnMissingField =>
            // Re-throw field validation errors in fail-fast mode
            throw e
          case e: Exception =>
            taskLogger.warn(s"Failed to prewarm split ${addAction.path}: ${e.getMessage}")
        }
      }

      val duration = System.currentTimeMillis() - taskStartTime
      val status   = if (skippedFields.isEmpty) "success" else "partial"

      PrewarmTaskResult(
        hostname = hostname,
        splitsPrewarmed = prewarmedCount,
        segments = task.segments.map(_.name()).toSeq.sorted.mkString(","),
        fields = task.fields.map(_.mkString(",")).getOrElse("all"),
        durationMs = duration,
        status = status,
        skippedFields = if (skippedFields.isEmpty) None else Some(skippedFields.toSeq.distinct.mkString(","))
      )
    } catch {
      case e: Exception =>
        taskLogger.error(s"Prewarm task failed on host $hostname: ${e.getMessage}", e)
        val duration = System.currentTimeMillis() - taskStartTime
        PrewarmTaskResult(
          hostname = hostname,
          splitsPrewarmed = 0,
          segments = task.segments.map(_.name()).toSeq.sorted.mkString(","),
          fields = task.fields.map(_.mkString(",")).getOrElse("all"),
          durationMs = duration,
          status = s"error: ${e.getMessage}",
          skippedFields = None
        )
    }
  }

  /**
   * Extract field names from AddAction metadata (docMappingJson).
   */
  private def extractFieldsFromMetadata(addAction: io.indextables.spark.transaction.AddAction): Set[String] =
    addAction.docMappingJson match {
      case Some(json) =>
        try {
          // Parse docMappingJson to extract field names
          // Format: {"field_mappings":[{"name":"field1",...},{"name":"field2",...}]}
          val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
          val root = mapper.readTree(json)
          val fieldMappings = root.path("field_mappings")
          if (fieldMappings.isArray) {
            fieldMappings.elements().asScala.flatMap { fm =>
              Option(fm.path("name").asText(null))
            }.map(_.toLowerCase).toSet
          } else {
            Set.empty[String]
          }
        } catch {
          case e: Exception =>
            logger.debug(s"Failed to parse docMappingJson for field extraction: ${e.getMessage}")
            Set.empty[String]
        }
      case None =>
        Set.empty[String]
    }
}

/** Internal task representation for prewarm distribution. */
private[sql] case class PrewarmTask(
  batchIndex: Int,
  hostname: String,
  addActions: Seq[io.indextables.spark.transaction.AddAction],
  tablePath: String,
  segments: Set[IndexComponent],
  fields: Option[Seq[String]],
  failOnMissingField: Boolean)
    extends Serializable

/** Result from a single prewarm task execution. */
private[sql] case class PrewarmTaskResult(
  hostname: String,
  splitsPrewarmed: Int,
  segments: String,
  fields: String,
  durationMs: Long,
  status: String,
  skippedFields: Option[String])
    extends Serializable
