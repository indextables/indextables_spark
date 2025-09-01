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

package com.tantivy4spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{StringType, LongType, StructType, StructField}
import org.apache.spark.unsafe.types.UTF8String
import com.tantivy4spark.transaction.{TransactionLog, AddAction, RemoveAction}
import com.tantivy4spark.storage.SplitManager
import com.tantivy4spark.io.{CloudStorageProviderFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ThreadLocalRandom
import com.tantivy4java.QuickwitSplit
import scala.jdk.CollectionConverters._

/**
 * SQL command to merge small split files into larger ones.
 * Modeled after Delta Lake's OPTIMIZE command structure and behavior.
 * 
 * Syntax: MERGE SPLITS ('/path/to/table' | table_name) [WHERE partition_predicates] 
 *         [TARGET SIZE target_size] [PRECOMMIT]
 * 
 * Examples:
 * - MERGE SPLITS '/path/to/table'
 * - MERGE SPLITS my_table WHERE partition_col = 'value'
 * - MERGE SPLITS my_table TARGET SIZE 5368709120  -- 5GB in bytes
 * - MERGE SPLITS '/path/to/table' WHERE year = 2023 TARGET SIZE 2147483648  -- 2GB
 * - MERGE SPLITS events PRECOMMIT  -- Pre-commit merge (framework complete, core implementation pending)
 * 
 * This command:
 * 1. Merges only within partitions (follows Delta Lake OPTIMIZE pattern)
 * 2. Selects mergeable splits in transaction log order
 * 3. Concatenates splits up to configurable target size (default 5GB)
 * 4. Assumes merged split size equals sum of input splits
 * 5. Does not merge splits already at target size
 * 6. Uses atomic REMOVE+ADD operations in transaction log
 * 7. Ensures queries after merge only read merged splits
 * 8. PRECOMMIT option: Merges splits during write process before transaction log commit
 *    (eliminates small file problems at source - framework complete, core logic pending)
 */
abstract class MergeSplitsCommandBase extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("metrics", StringType)()
  )

  /**
   * Default target size for merged splits (5GB in bytes)
   */
  val DEFAULT_TARGET_SIZE: Long = 5L * 1024L * 1024L * 1024L // 5GB

  protected def validateTargetSize(targetSize: Long): Unit = {
    if (targetSize <= 0) {
      throw new IllegalArgumentException(s"Target size must be positive, got: $targetSize")
    }
    if (targetSize < 1024 * 1024) { // 1MB minimum
      throw new IllegalArgumentException(s"Target size must be at least 1MB, got: $targetSize")
    }
  }
}

/**
 * MERGE SPLITS command implementation for Spark SQL.
 */
case class MergeSplitsCommand(
    override val child: LogicalPlan,
    userPartitionPredicates: Seq[String],
    targetSize: Option[Long],
    preCommitMerge: Boolean = false
) extends MergeSplitsCommandBase with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): MergeSplitsCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Validate target size first (for all cases)
    val actualTargetSize = targetSize.getOrElse(DEFAULT_TARGET_SIZE)
    validateTargetSize(actualTargetSize)
    
    // Handle pre-commit merge early (before any table path resolution)
    if (preCommitMerge) {
      logger.info("PRE-COMMIT MERGE: Executing pre-commit merge functionality")
      return Seq(Row("PRE-COMMIT MERGE", "PRE-COMMIT MERGE: Functionality pending implementation"))
    }
    
    // Resolve table path from child logical plan
    val tablePath = try {
      resolveTablePath(child, sparkSession)
    } catch {
      case e: IllegalArgumentException =>
        // Handle non-existent table gracefully
        val pathStr = child match {
          case resolved: UnresolvedDeltaPathOrIdentifier =>
            resolved.path.getOrElse(resolved.tableIdentifier.map(_.toString).getOrElse("unknown"))
          case _ => "unknown"
        }
        logger.info(s"Table or path not found: $pathStr")
        return Seq(Row(pathStr, "No splits merged - table or path does not exist"))
    }
    
    // Create transaction log
    val transactionLog = new TransactionLog(tablePath, sparkSession)
    
    try {
      // Check if transaction log is initialized
      val hasMetadata = try {
        transactionLog.getMetadata()
        true
      } catch {
        case _: Exception => false
      }
      
      if (!hasMetadata) {
        logger.info(s"No transaction log found at: $tablePath")
        return Seq(Row(tablePath.toString, "No splits merged - not a valid Tantivy4Spark table"))
      }
      
      // Validate table has files
      val files = transactionLog.listFiles()
      if (files.isEmpty) {
        logger.info(s"No files found in table: $tablePath")
        return Seq(Row(tablePath.toString, "No splits to merge - table is empty"))
      }

      new MergeSplitsExecutor(
        sparkSession,
        transactionLog,
        tablePath,
        userPartitionPredicates,
        actualTargetSize,
        preCommitMerge
      ).merge()
    } finally {
      transactionLog.close()
    }
  }
  
  /**
   * Resolve table path from logical plan child.
   */
  private def resolveTablePath(child: LogicalPlan, sparkSession: SparkSession): Path = {
    child match {
      case resolved: UnresolvedDeltaPathOrIdentifier =>
        resolved.path match {
          case Some(pathStr) => new Path(pathStr)
          case None =>
            resolved.tableIdentifier match {
              case Some(tableId) =>
                // Try to resolve table identifier to path
                val catalog = sparkSession.sessionState.catalog
                if (catalog.tableExists(tableId)) {
                  val tableMetadata = catalog.getTableMetadata(tableId)
                  new Path(tableMetadata.location)
                } else {
                  throw new IllegalArgumentException(s"Table not found: ${tableId}")
                }
              case None =>
                throw new IllegalArgumentException("Either path or table identifier must be specified")
            }
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported child plan type: ${child.getClass.getSimpleName}")
    }
  }
}

object MergeSplitsCommand {
  /**
   * Alternate constructor that converts a provided path or table identifier into the
   * correct child LogicalPlan node.
   */
  def apply(
      path: Option[String],
      tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
      userPartitionPredicates: Seq[String],
      targetSize: Option[Long],
      preCommitMerge: Boolean
  ): MergeSplitsCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, "MERGE SPLITS")
    MergeSplitsCommand(plan, userPartitionPredicates, targetSize, preCommitMerge)
  }
}

/**
 * Executor for merge splits operation.
 * Follows Delta Lake's OptimizeExecutor pattern.
 */
class MergeSplitsExecutor(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    tablePath: Path,
    partitionPredicates: Seq[String],
    targetSize: Long,
    preCommitMerge: Boolean = false
) {
  
  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])
  
  /**
   * Smart string ordering that handles numeric values correctly.
   * Tries numeric comparison first, falls back to string comparison.
   */
  private val smartStringOrdering: Ordering[String] = new Ordering[String] {
    override def compare(x: String, y: String): Int = {
      try {
        // Try to parse both as doubles
        val xNum = x.toDouble
        val yNum = y.toDouble
        xNum.compare(yNum)
      } catch {
        case _: NumberFormatException =>
          // Fall back to string comparison
          x.compare(y)
      }
    }
  }
  
  def merge(): Seq[Row] = {
    if (preCommitMerge) {
      logger.info(s"Starting PRE-COMMIT MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes")
      return performPreCommitMerge()
    }
    
    logger.info(s"Starting MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes")

    // Get current metadata to understand partition schema
    val metadata = transactionLog.getMetadata()
    val partitionSchema = StructType(metadata.partitionColumns.map(name => 
      StructField(name, StringType, nullable = true)))
      
    // If no partition columns are defined in metadata, skip partition validation
    if (metadata.partitionColumns.isEmpty) {
      logger.info("No partition columns defined in metadata - treating table as non-partitioned")
    }

    // Get current files from transaction log (in order they were added)
    val currentFiles = transactionLog.listFiles().sortBy(_.modificationTime)
    logger.info(s"Found ${currentFiles.length} split files in transaction log")
    currentFiles.foreach { file =>
      logger.info(s"  File: ${file.path} (${file.size} bytes, partition: ${file.partitionValues})")
    }

    // Group files by partition (merge only within partitions - Delta Lake pattern)
    val partitionsToMerge = currentFiles.groupBy(_.partitionValues).toSeq
    logger.info(s"Found ${partitionsToMerge.length} partitions to potentially merge")
    partitionsToMerge.foreach { case (partitionValues, files) =>
      logger.info(s"  Partition $partitionValues: ${files.length} files")
    }

    // Apply partition predicates if specified
    val filteredPartitions = if (partitionPredicates.nonEmpty) {
      applyPartitionPredicates(partitionsToMerge, partitionSchema)
    } else {
      partitionsToMerge
    }

    // Find mergeable splits within each partition
    val mergeGroups = filteredPartitions.flatMap { case (partitionValues, files) =>
      logger.info(s"Processing partition $partitionValues with ${files.length} files:")
      files.foreach { file =>
        logger.info(s"  File: ${file.path} (${file.size} bytes)")
      }
      
      val groups = findMergeableGroups(partitionValues, files)
      logger.info(s"Found ${groups.length} potential merge groups in partition $partitionValues")
      
      // Double-check: filter out any single-file groups that might have slipped through
      println(s"MERGE DEBUG: Before filtering: ${groups.length} groups")
      groups.foreach { group =>
        println(s"MERGE DEBUG:   Group has ${group.files.length} files: ${group.files.map(_.path).mkString(", ")}")
      }
      val validGroups = groups.filter(_.files.length >= 2)
      println(s"MERGE DEBUG: After filtering: ${validGroups.length} valid groups")
      if (groups.length != validGroups.length) {
        logger.warn(s"Filtered out ${groups.length - validGroups.length} single-file groups from partition $partitionValues")
      }
      
      validGroups.foreach { group =>
        logger.info(s"  Valid merge group: ${group.files.length} files (${group.files.map(_.size).sum} bytes total)")
        group.files.foreach { file =>
          logger.info(s"    - ${file.path} (${file.size} bytes)")
        }
      }
      
      validGroups
    }

    logger.info(s"Found ${mergeGroups.length} merge groups containing ${mergeGroups.map(_.files.length).sum} files")
    
    // Final safety check: ensure no single-file groups exist
    val singleFileGroups = mergeGroups.filter(_.files.length < 2)
    if (singleFileGroups.nonEmpty) {
      logger.error(s"CRITICAL: Found ${singleFileGroups.length} single-file groups that should have been filtered out!")
      singleFileGroups.foreach { group =>
        logger.error(s"  Single-file group: ${group.files.head.path} in partition ${group.partitionValues}")
      }
      throw new IllegalStateException(s"Internal error: Found ${singleFileGroups.length} single-file merge groups")
    }

    if (mergeGroups.isEmpty) {
      logger.info("No splits require merging")
      return Seq(Row(tablePath.toString, "No splits merged - all splits are already optimal size"))
    }

    // Execute merges
    val results = mergeGroups.map(executeMergeGroup)
    
    val totalMergedFiles = results.map(_.mergedFiles).sum
    val totalMergeGroups = results.length
    val totalOriginalSize = results.map(_.originalSize).sum  
    val totalMergedSize = results.map(_.mergedSize).sum

    logger.info(s"MERGE SPLITS completed: merged $totalMergedFiles files into $totalMergeGroups new splits")
    logger.info(s"Size change: ${totalOriginalSize} bytes -> ${totalMergedSize} bytes")

    Seq(Row(
      tablePath.toString,
      s"Merged $totalMergedFiles files into $totalMergeGroups splits. " +
      s"Original size: $totalOriginalSize bytes, new size: $totalMergedSize bytes"
    ))
  }

  /**
   * Performs pre-commit merge where splits are merged before being added to the transaction log.
   * In this mode, original fragmental splits are deleted after the merged split is uploaded
   * and the transaction log never sees the original splits.
   */
  private def performPreCommitMerge(): Seq[Row] = {
    logger.info("PRE-COMMIT MERGE: This functionality merges splits before they appear in transaction log")
    logger.info("PRE-COMMIT MERGE: Original fragmental splits are deleted and never logged")
    
    // For pre-commit merge, we need to work with pending/staging splits rather than committed ones
    // This would typically integrate with the write path to merge splits during the commit process
    
    // TODO: Implement actual pre-commit merge logic that:
    // 1. Identifies pending splits that haven't been committed yet
    // 2. Groups them by partition 
    // 3. Merges groups that exceed fragmentation thresholds
    // 4. Deletes original fragmental splits from storage
    // 5. Commits only the merged splits to transaction log
    
    logger.warn("PRE-COMMIT MERGE: Implementation pending - this is a placeholder")
    
    Seq(Row(tablePath.toString, "PRE-COMMIT MERGE: Functionality pending implementation"))
  }

  /**
   * Find groups of files that should be merged within a partition.
   * Follows bin packing approach similar to Delta Lake's OPTIMIZE.
   * Only creates groups with 2+ files to satisfy tantivy4java merge requirements.
   */
  private def findMergeableGroups(
      partitionValues: Map[String, String], 
      files: Seq[AddAction]
  ): Seq[MergeGroup] = {
    
    val groups = ArrayBuffer[MergeGroup]()
    val currentGroup = ArrayBuffer[AddAction]()
    var currentGroupSize = 0L
    
    // Filter out files that are already at or above target size
    val mergeableFiles = files.filter { file =>
      if (file.size >= targetSize) {
        logger.debug(s"Skipping file ${file.path} (size: ${file.size}) - already at target size")
        false
      } else {
        true
      }
    }
    
    println(s"MERGE DEBUG: Found ${mergeableFiles.length} files eligible for merging (< $targetSize bytes)")
    
    // If we have fewer than 2 mergeable files, no groups can be created
    if (mergeableFiles.length < 2) {
      println(s"MERGE DEBUG: Cannot create merge groups - need at least 2 files but found ${mergeableFiles.length}")
      return groups.toSeq
    }

    for ((file, index) <- mergeableFiles.zipWithIndex) {
      println(s"MERGE DEBUG: Processing file ${index+1}/${mergeableFiles.length}: ${file.path} (${file.size} bytes)")
      
      // Check if adding this file would exceed target size
      if (currentGroupSize > 0 && currentGroupSize + file.size > targetSize) {
        println(s"MERGE DEBUG: Adding ${file.path} (${file.size} bytes) to current group (${currentGroupSize} bytes) would exceed target ($targetSize bytes)")
        
        // Current group is full, save it if it has multiple files
        println(s"MERGE DEBUG: Current group has ${currentGroup.length} files before saving")
        if (currentGroup.length > 1) {
          groups += MergeGroup(partitionValues, currentGroup.clone().toSeq)
          println(s"MERGE DEBUG: ✓ Created merge group with ${currentGroup.length} files (${currentGroupSize} bytes): ${currentGroup.map(_.path).mkString(", ")}")
        } else {
          println(s"MERGE DEBUG: ✗ Discarding single-file group: ${currentGroup.head.path} (${currentGroupSize} bytes)")
        }
        
        // Start new group
        currentGroup.clear()
        currentGroup += file
        currentGroupSize = file.size
        println(s"MERGE DEBUG: Started new group with ${file.path} (${file.size} bytes)")
      } else {
        // Add file to current group
        currentGroup += file
        currentGroupSize += file.size
        println(s"MERGE DEBUG: Added ${file.path} (${file.size} bytes) to current group. Group now has ${currentGroup.length} files (${currentGroupSize} bytes total)")
      }
    }

    // Handle remaining group - only save if it has multiple files
    if (currentGroup.length > 1) {
      groups += MergeGroup(partitionValues, currentGroup.clone().toSeq)
      logger.debug(s"✓ Created final merge group with ${currentGroup.length} files (${currentGroupSize} bytes): ${currentGroup.map(_.path).mkString(", ")}")
    } else if (currentGroup.length == 1) {
      logger.debug(s"✗ Discarding final single-file group: ${currentGroup.head.path} (${currentGroupSize} bytes)")
    } else {
      logger.debug(s"No remaining group to process")
    }

    println(s"MERGE DEBUG: Created ${groups.length} merge groups from ${mergeableFiles.length} mergeable files")
    groups.toSeq
  }

  /**
   * Execute merge for a single group of splits.
   * Uses atomic REMOVE+ADD transaction operations like Delta Lake OPTIMIZE.
   */
  private def executeMergeGroup(mergeGroup: MergeGroup): MergeResult = {
    val startTime = System.currentTimeMillis()
    
    logger.info(s"Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")
    
    try {
      // Create merged split using SplitManager
      val mergedSplit = createMergedSplit(mergeGroup)
      
      // Prepare transaction actions (REMOVE + ADD pattern from Delta Lake)
      val removeActions = mergeGroup.files.map { file =>
        RemoveAction(
          path = file.path,
          deletionTimestamp = Some(startTime),
          dataChange = false, // This is compaction, not data change
          extendedFileMetadata = Some(true),
          partitionValues = Some(file.partitionValues),
          size = Some(file.size),
          tags = file.tags
        )
      }

      // Merge statistics from input files without reading file contents
      val (mergedMinValues, mergedMaxValues, mergedNumRecords) = mergeStatistics(mergeGroup.files)

      val addAction = AddAction(
        path = mergedSplit.path,
        partitionValues = mergeGroup.partitionValues,
        size = mergedSplit.size,
        modificationTime = startTime,
        dataChange = false, // This is compaction, not data change
        stats = None, // Statistics are stored in minValues/maxValues/numRecords fields
        tags = Some(Map(
          "operation" -> "optimize", // Delta Lake standard operation name
          "operationParameters" -> "merge_splits",
          "merged_files_count" -> mergeGroup.files.length.toString,
          "merged_from" -> mergeGroup.files.map(_.path).mkString(","),
          "target_size" -> targetSize.toString
        )),
        minValues = mergedMinValues,
        maxValues = mergedMaxValues,
        numRecords = mergedNumRecords
      )

      // Commit atomic REMOVE+ADD transaction using the new method
      val version = transactionLog.commitMergeSplits(removeActions, Seq(addAction))
      
      // Invalidate cache after transaction log update to ensure fresh file listing
      transactionLog.invalidateCache()
      
      logger.info(s"Successfully committed atomic REMOVE+ADD merge operation at version $version")
      
      val originalSize = mergeGroup.files.map(_.size).sum
      logger.info(s"Successfully merged ${mergeGroup.files.length} files into ${mergedSplit.path}")
      
      MergeResult(
        mergedFiles = mergeGroup.files.length,
        originalSize = originalSize,
        mergedSize = mergedSplit.size,
        mergedPath = mergedSplit.path
      )
      
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to merge group in partition ${mergeGroup.partitionValues}", ex)
        throw ex
    }
  }

  /**
   * Create a new merged split by physically merging split files using tantivy4java.
   * This uses QuickwitSplit.mergeSplits() API for actual merge implementation.
   * For testing with mock data, it falls back to size-based simulation.
   */
  private def createMergedSplit(mergeGroup: MergeGroup): MergedSplitInfo = {
    // Validate group has at least 2 files (required by tantivy4java)
    if (mergeGroup.files.length < 2) {
      throw new IllegalArgumentException(s"Cannot merge group with ${mergeGroup.files.length} files - at least 2 required")
    }
    
    // Generate new split path with UUID for uniqueness
    val uuid = java.util.UUID.randomUUID().toString
    val partitionPath = if (mergeGroup.partitionValues.isEmpty) "" else {
      mergeGroup.partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
    }
    val mergedPath = s"$partitionPath${uuid}.split"
    
    // Create full paths for input splits and output split
    // Handle S3 paths specially to preserve the s3:// scheme
    val isS3Path = tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")
    
    val inputSplitPaths = mergeGroup.files.map { file =>
      if (isS3Path) {
        // For S3 paths, construct the URL directly
        val baseUri = tablePath.toString.replaceAll("/$", "") // Remove trailing slash if present
        s"$baseUri/${file.path}"
      } else {
        // For local/HDFS paths, use Path concatenation
        val fullPath = new Path(tablePath, file.path)
        fullPath.toString
      }
    }.asJava
    
    val outputSplitPath = if (isS3Path) {
      // For S3 paths, construct the URL directly
      val baseUri = tablePath.toString.replaceAll("/$", "") // Remove trailing slash if present
      s"$baseUri/$mergedPath"
    } else {
      // For local/HDFS paths, use Path concatenation
      new Path(tablePath, mergedPath).toString
    }
    
    logger.info(s"Merging ${inputSplitPaths.size()} splits into $outputSplitPath")
    logger.debug(s"Input splits: ${inputSplitPaths.asScala.mkString(", ")}")
    
    logger.info("Attempting to merge splits using Tantivy4Java merge functionality")
    
    // Create merge configuration
    val mergeConfig = new QuickwitSplit.MergeConfig(
      "merged-index-uid", // indexUid
      "tantivy4spark",   // sourceId  
      "merge-node"      // nodeId
    )
    
    // Perform the actual merge using tantivy4java - NO FALLBACKS, NO SIMULATIONS
    logger.info(s"Calling QuickwitSplit.mergeSplits() with ${inputSplitPaths.size()} input paths")
    val metadata = QuickwitSplit.mergeSplits(inputSplitPaths, outputSplitPath, mergeConfig)
    
    logger.info(s"Successfully merged splits: ${metadata.getNumDocs} documents, ${metadata.getUncompressedSizeBytes} bytes")
    logger.debug(s"Merge metadata: split_id=${metadata.getSplitId}, merge_ops=${metadata.getNumMergeOps}")
    
    MergedSplitInfo(mergedPath, metadata.getUncompressedSizeBytes)
  }

  /**
   * Apply partition predicates to filter which partitions should be processed.
   * Follows Delta Lake pattern of parsing WHERE clause expressions.
   */
  private def applyPartitionPredicates(
      partitions: Seq[(Map[String, String], Seq[AddAction])],
      partitionSchema: StructType
  ): Seq[(Map[String, String], Seq[AddAction])] = {
    
    // If no partition columns are defined, reject any WHERE clauses
    if (partitionSchema.isEmpty && partitionPredicates.nonEmpty) {
      throw new IllegalArgumentException(s"WHERE clause not supported for non-partitioned tables. Partition predicates: ${partitionPredicates.mkString(", ")}")
    }
    
    val parsedPredicates = partitionPredicates.flatMap { predicate =>
      try {
        val expression = sparkSession.sessionState.sqlParser.parseExpression(predicate)
        validatePartitionColumnReferences(expression, partitionSchema)
        Some(expression)
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to parse partition predicate: $predicate", ex)
          throw new IllegalArgumentException(s"Invalid partition predicate: $predicate", ex)
      }
    }

    if (parsedPredicates.isEmpty) return partitions

    partitions.filter { case (partitionValues, _) =>
      val row = createRowFromPartitionValues(partitionValues, partitionSchema)
      parsedPredicates.forall { predicate =>
        try {
          // Resolve the expression against the partition schema before evaluation
          val resolvedPredicate = resolveExpression(predicate, partitionSchema)
          resolvedPredicate.eval(row).asInstanceOf[Boolean]
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to evaluate predicate $predicate on partition $partitionValues", ex)
            false
        }
      }
    }
  }

  /**
   * Validate that the expression only references partition columns.
   */
  private def validatePartitionColumnReferences(expression: Expression, partitionSchema: StructType): Unit = {
    val partitionColumns = partitionSchema.fieldNames.toSet
    val referencedColumns = expression.references.map(_.name).toSet
    
    val invalidColumns = referencedColumns -- partitionColumns
    if (invalidColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause references non-partition columns: ${invalidColumns.mkString(", ")}. " +
        s"Only partition columns are allowed: ${partitionColumns.mkString(", ")}"
      )
    }
  }

  /**
   * Create an InternalRow from partition values for predicate evaluation.
   */
  private def createRowFromPartitionValues(
      partitionValues: Map[String, String], 
      partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fieldNames.map { fieldName =>
      partitionValues.get(fieldName) match {
        case Some(value) => UTF8String.fromString(value)
        case None => null
      }
    }
    InternalRow.fromSeq(values)
  }
  
  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references.
   */
  private def resolveExpression(expression: Expression, schema: StructType): Expression = {
    expression.transform {
      case unresolvedAttr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
        val fieldName = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field = schema(fieldIndex)
        org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
    }
  }

  /**
   * Aggregate min values from multiple files by taking the minimum of each field.
   * This preserves data skipping statistics without reading file contents.
   */
  private def aggregateMinValues(files: Seq[AddAction]): Option[Map[String, String]] = {
    val allMinValues = files.flatMap(_.minValues.getOrElse(Map.empty))
    if (allMinValues.nonEmpty) {
      // Group by field name and take the minimum value for each field
      val mergedStats = allMinValues.groupBy(_._1).map { case (fieldName, fieldValues) =>
        val minValue = fieldValues.map(_._2).min(smartStringOrdering)
        fieldName -> minValue
      }
      Some(mergedStats)
    } else None
  }

  /**
   * Aggregate max values from multiple files by taking the maximum of each field.
   * This preserves data skipping statistics without reading file contents.
   */
  private def aggregateMaxValues(files: Seq[AddAction]): Option[Map[String, String]] = {
    val allMaxValues = files.flatMap(_.maxValues.getOrElse(Map.empty))
    if (allMaxValues.nonEmpty) {
      // Group by field name and take the maximum value for each field
      val mergedStats = allMaxValues.groupBy(_._1).map { case (fieldName, fieldValues) =>
        val maxValue = fieldValues.map(_._2).max(smartStringOrdering)
        fieldName -> maxValue
      }
      Some(mergedStats)
    } else None
  }

  /**
   * Aggregate record counts by summing across all files.
   * This maintains accurate row count statistics.
   */
  private def aggregateNumRecords(files: Seq[AddAction]): Option[Long] = {
    val allRecords = files.flatMap(_.numRecords)
    if (allRecords.nonEmpty) {
      Some(allRecords.sum)
    } else {
      None
    }
  }

  /**
   * Merge statistics from multiple files without reading file contents.
   * This is critical for maintaining Delta Lake-style data skipping performance.
   */
  private def mergeStatistics(files: Seq[AddAction]): (Option[Map[String, String]], Option[Map[String, String]], Option[Long]) = {
    logger.debug(s"Merging statistics from ${files.length} files")
    
    val minValues = aggregateMinValues(files)
    val maxValues = aggregateMaxValues(files)
    val numRecords = aggregateNumRecords(files)
    
    logger.debug(s"Merged statistics: ${numRecords.getOrElse("unknown")} records, " +
      s"${minValues.map(_.size).getOrElse(0)} min values, ${maxValues.map(_.size).getOrElse(0)} max values")
    
    (minValues, maxValues, numRecords)
  }
}

/**
 * Group of files that should be merged together.
 */
case class MergeGroup(
    partitionValues: Map[String, String],
    files: Seq[AddAction]
)

/**
 * Result of merging a group of splits.
 */
case class MergeResult(
    mergedFiles: Int,
    originalSize: Long,
    mergedSize: Long,
    mergedPath: String
)

/**
 * Information about a newly created merged split.
 */
case class MergedSplitInfo(path: String, size: Long)

/**
 * Placeholder for unresolved table path or identifier.
 * Similar to Delta Lake's UnresolvedDeltaPathOrIdentifier.
 */
case class UnresolvedDeltaPathOrIdentifier(
    path: Option[String],
    tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
    commandName: String
) extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}