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
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{PartitionPredicateUtils, RemoveAction, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils}
import org.slf4j.LoggerFactory

/**
 * SQL command to drop (remove) partitions from an IndexTable.
 *
 * Syntax: DROP INDEXTABLES PARTITIONS FROM ('/path/to/table' | table_name) WHERE partition_predicates
 *
 * Examples:
 *   - DROP INDEXTABLES PARTITIONS FROM '/path/to/table' WHERE year = '2023'
 *   - DROP INDEXTABLES PARTITIONS FROM my_table WHERE date = '2024-01-01'
 *   - DROP INDEXTABLES PARTITIONS FROM 's3://bucket/table' WHERE region = 'us-east' AND year > '2020'
 *   - DROP INDEXTABLES PARTITIONS FROM my_db.my_table WHERE status = 'inactive' OR created < '2023-01-01'
 *   - DROP INDEXTABLES PARTITIONS FROM my_table WHERE month BETWEEN 1 AND 6
 *
 * This command:
 *   1. Parses and validates the WHERE clause to ensure only partition columns are referenced 2. Finds all splits
 *      matching the partition predicates 3. Adds RemoveAction entries to the transaction log for matching splits 4.
 *      Does NOT physically delete the split files (use PURGE INDEXTABLE for that) 5. Removed splits become eligible for
 *      cleanup after retention period expires
 *
 * Requirements:
 *   - WHERE clause is mandatory
 *   - WHERE clause must reference only partition columns
 *   - At least one partition column must be referenced
 *   - Table must have partition columns defined
 */
case class DropPartitionsCommand(
  override val child: LogicalPlan,
  userPartitionPredicates: Seq[String])
    extends RunnableCommand
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[DropPartitionsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("status", StringType)(),
    AttributeReference("partitions_dropped", LongType)(),
    AttributeReference("splits_removed", LongType)(),
    AttributeReference("total_size_bytes", LongType)(),
    AttributeReference("message", StringType)()
  )

  override protected def withNewChildInternal(newChild: LogicalPlan): DropPartitionsCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {

    // Validate that WHERE clause is provided
    if (userPartitionPredicates.isEmpty) {
      throw new IllegalArgumentException(
        "DROP INDEXTABLES PARTITIONS requires a WHERE clause. " +
          "Use PURGE INDEXTABLE to remove all data or specify partition predicates."
      )
    }

    // Resolve table path from child logical plan
    val tablePath =
      try
        resolveTablePath(child, sparkSession)
      catch {
        case e: IllegalArgumentException =>
          val pathStr = child match {
            case resolved: UnresolvedDeltaPathOrIdentifier =>
              resolved.path.getOrElse(resolved.tableIdentifier.map(_.toString).getOrElse("unknown"))
            case _ => "unknown"
          }
          logger.info(s"Table or path not found: $pathStr")
          return Seq(Row(pathStr, "error", null, null, null, s"Table or path does not exist: ${e.getMessage}"))
      }

    // Extract and merge configuration with proper precedence
    val hadoopConf    = sparkSession.sparkContext.hadoopConfiguration
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Create transaction log with PATH_READ_WRITE credentials since drop partitions
    // performs write operations (removing split files and updating transaction log)
    import scala.jdk.CollectionConverters._
    val writeConfigs = mergedConfigs + ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")
    val transactionLog =
      TransactionLogFactory.create(tablePath, sparkSession, new CaseInsensitiveStringMap(writeConfigs.asJava))

    // Invalidate cache to ensure fresh read of transaction log state
    transactionLog.invalidateCache()

    try {
      // Check if transaction log is initialized
      val metadata =
        try
          transactionLog.getMetadata()
        catch {
          case _: Exception =>
            logger.info(s"No transaction log found at: $tablePath")
            return Seq(Row(tablePath.toString, "error", null, null, null, "Not a valid IndexTables4Spark table"))
        }

      // Validate table has partition columns
      if (metadata.partitionColumns.isEmpty) {
        throw new IllegalArgumentException(
          s"Cannot drop partitions from non-partitioned table. " +
            s"Table at $tablePath has no partition columns defined."
        )
      }

      logger.info(
        s"Found ${metadata.partitionColumns.size} partition columns: ${metadata.partitionColumns.mkString(", ")}"
      )

      // Build partition schema
      val partitionSchema = PartitionPredicateUtils.buildPartitionSchema(metadata.partitionColumns)

      // Parse and validate predicates (will throw if invalid columns are referenced)
      val parsedPredicates = PartitionPredicateUtils.parseAndValidatePredicates(
        userPartitionPredicates,
        partitionSchema,
        sparkSession
      )

      // Convert predicates to Spark Filters for Avro manifest pruning
      val partitionFilters = PartitionPredicateUtils.expressionsToFilters(parsedPredicates)
      logger.info(s"Converted ${partitionFilters.length} of ${parsedPredicates.length} predicates to Spark Filters for manifest pruning")

      // Get files with manifest-level pruning if filters are available
      val allFiles = if (partitionFilters.nonEmpty) {
        val files = transactionLog.listFilesWithPartitionFilters(partitionFilters)
        logger.info(s"Found ${files.length} split files using Avro manifest pruning")
        files
      } else {
        val files = transactionLog.listFiles()
        logger.info(s"Found ${files.length} split files in transaction log")
        files
      }

      if (allFiles.isEmpty) {
        logger.info(s"No files found in table: $tablePath")
        return Seq(Row(tablePath.toString, "no_action", 0L, 0L, 0L, "Table is empty"))
      }

      // Apply in-memory partition filtering for precise matching
      // (manifest pruning may include files from overlapping partition ranges)
      val matchingFiles = PartitionPredicateUtils.filterAddActionsByPredicates(
        allFiles,
        partitionSchema,
        parsedPredicates
      )
      logger.info(s"After in-memory partition filtering: ${matchingFiles.length} matching splits")

      if (matchingFiles.isEmpty) {
        logger.info(s"No partitions match the specified predicates: ${userPartitionPredicates.mkString(", ")}")
        return Seq(
          Row(
            tablePath.toString,
            "no_action",
            0L,
            0L,
            0L,
            s"No partitions match predicates: ${userPartitionPredicates.mkString(", ")}"
          )
        )
      }

      // Count unique partitions being dropped
      val uniquePartitions = matchingFiles.map(_.partitionValues).distinct
      logger.info(s"Dropping ${uniquePartitions.size} partitions containing ${matchingFiles.length} splits")

      // Log details of files to be removed
      uniquePartitions.foreach { partitionValues =>
        val filesInPartition = matchingFiles.filter(_.partitionValues == partitionValues)
        val totalSize        = filesInPartition.map(_.size).sum
        logger.info(s"  Partition $partitionValues: ${filesInPartition.length} splits, $totalSize bytes")
      }

      // Create RemoveActions for all matching files
      val deletionTimestamp = System.currentTimeMillis()
      val removeActions = matchingFiles.map { file =>
        RemoveAction(
          path = file.path,
          deletionTimestamp = Some(deletionTimestamp),
          dataChange = true, // This is a data change operation
          extendedFileMetadata = Some(true),
          partitionValues = Some(file.partitionValues),
          size = Some(file.size),
          tags = file.tags
        )
      }

      // Commit the remove actions to transaction log
      val version   = transactionLog.commitRemoveActions(removeActions)
      val totalSize = matchingFiles.map(_.size).sum

      logger.info(
        s"Successfully dropped ${uniquePartitions.size} partitions (${matchingFiles.length} splits, $totalSize bytes) in transaction version $version"
      )

      Seq(
        Row(
          tablePath.toString,
          "success",
          uniquePartitions.size.toLong,
          matchingFiles.length.toLong,
          totalSize,
          s"Dropped ${uniquePartitions.size} partitions containing ${matchingFiles.length} splits. " +
            s"Files will be eligible for cleanup after retention period expires. " +
            s"Run PURGE INDEXTABLE to physically delete the files."
        )
      )

    } finally
      transactionLog.close()
  }

  /** Resolve table path from logical plan child. */
  private def resolveTablePath(child: LogicalPlan, sparkSession: SparkSession): Path =
    child match {
      case resolved: UnresolvedDeltaPathOrIdentifier =>
        resolved.path match {
          case Some(pathStr) => new Path(pathStr)
          case None =>
            resolved.tableIdentifier match {
              case Some(tableId) =>
                val catalog = sparkSession.sessionState.catalog
                if (catalog.tableExists(tableId)) {
                  val tableMetadata = catalog.getTableMetadata(tableId)
                  new Path(tableMetadata.location)
                } else {
                  throw new IllegalArgumentException(s"Table not found: $tableId")
                }
              case None =>
                throw new IllegalArgumentException("Either path or table identifier must be specified")
            }
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported child plan type: ${child.getClass.getSimpleName}")
    }
}

object DropPartitionsCommand {

  /** Create a DropPartitionsCommand from path or table identifier. */
  def apply(
    path: Option[String],
    tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
    userPartitionPredicates: Seq[String]
  ): DropPartitionsCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, "DROP PARTITIONS")
    DropPartitionsCommand(plan, userPartitionPredicates)
  }
}
