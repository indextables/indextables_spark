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

package io.indextables.spark.transaction

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

/**
 * Common interface for all transaction log implementations.
 *
 * This trait defines the public API that all transaction log implementations must provide, ensuring consistency across
 * simple and optimized implementations.
 *
 * Implementations:
 *   - TransactionLog: Simple sequential implementation with basic caching
 *   - OptimizedTransactionLog: Advanced implementation with parallel operations, enhanced caching, and async updates
 */
trait TransactionLogInterface extends AutoCloseable {

  /**
   * Gets the table path for this transaction log.
   *
   * @return
   *   The Hadoop Path to the table
   */
  def getTablePath(): Path

  /**
   * Initializes the transaction log with a schema.
   *
   * Creates the transaction log directory and writes initial protocol and metadata actions. This method should only be
   * called once when creating a new table.
   *
   * @param schema
   *   The table schema to initialize with
   */
  def initialize(schema: StructType): Unit

  /**
   * Initializes the transaction log with a schema and partition columns.
   *
   * Creates the transaction log directory and writes initial protocol and metadata actions with partitioning
   * information.
   *
   * @param schema
   *   The table schema to initialize with
   * @param partitionColumns
   *   The partition column names (must exist in schema)
   */
  def initialize(schema: StructType, partitionColumns: Seq[String]): Unit

  /**
   * Adds a single file to the transaction log.
   *
   * @param addAction
   *   The add action describing the file to add
   * @return
   *   The transaction version number for this operation
   */
  def addFile(addAction: AddAction): Long = addFiles(Seq(addAction))

  /**
   * Adds multiple files to the transaction log in a single transaction.
   *
   * This is more efficient than calling addFile() multiple times as it creates a single transaction version.
   *
   * @param addActions
   *   Sequence of add actions describing files to add
   * @return
   *   The transaction version number for this operation
   */
  def addFiles(addActions: Seq[AddAction]): Long

  /**
   * Overwrites all files in the transaction log with new files.
   *
   * This operation logically removes all existing files and adds the new files in a single atomic transaction. The
   * existing files are marked as removed but not physically deleted.
   *
   * @param addActions
   *   Sequence of add actions for the new files
   * @return
   *   The transaction version number for this operation
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long

  /**
   * Removes a file from the transaction log.
   *
   * The file is marked as removed in the transaction log but not physically deleted from storage. This allows
   * time-travel queries and recovery scenarios.
   *
   * @param path
   *   The path to the file to remove
   * @param deletionTimestamp
   *   The timestamp when the file was logically deleted
   * @return
   *   The transaction version number for this operation
   */
  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long

  /**
   * Lists all currently visible files in the transaction log.
   *
   * This method returns only files that have been added and not removed. It respects overwrite operations and returns
   * only files visible at the current version.
   *
   * @return
   *   Sequence of add actions for all visible files
   */
  def listFiles(): Seq[AddAction]

  /**
   * Lists all visible files in the table with partition filter optimization.
   *
   * When partition filters are provided and the checkpoint is in Avro state format, the reader can prune entire
   * manifest files that don't contain matching partitions, significantly reducing I/O for large tables with many
   * partitions.
   *
   * @param partitionFilters
   *   Partition filters for manifest pruning (only used with Avro state format)
   * @return
   *   Sequence of add actions for all visible files
   */
  def listFilesWithPartitionFilters(partitionFilters: Seq[Filter]): Seq[AddAction] = listFiles()

  /**
   * Gets the total row count across all visible files.
   *
   * This sums the record counts from all visible AddActions in the transaction log.
   *
   * @return
   *   Total number of rows in the table
   */
  def getTotalRowCount(): Long

  /**
   * Gets the table schema from the metadata action.
   *
   * @return
   *   The table schema, or None if not found
   */
  def getSchema(): Option[StructType]

  /**
   * Gets the partition columns for this table.
   *
   * @return
   *   Sequence of partition column names
   */
  def getPartitionColumns(): Seq[String]

  /**
   * Checks if this table is partitioned.
   *
   * @return
   *   true if the table has partition columns, false otherwise
   */
  def isPartitioned(): Boolean

  /**
   * Gets the metadata action for this table.
   *
   * @return
   *   The metadata action containing schema and partition information
   */
  def getMetadata(): MetadataAction

  /**
   * Gets the protocol action for this table.
   *
   * @return
   *   The protocol action containing min reader/writer versions
   */
  def getProtocol(): ProtocolAction

  /**
   * Asserts that the table is readable with the current protocol version.
   *
   * @throws RuntimeException
   *   if the table protocol version is too new to read
   */
  def assertTableReadable(): Unit

  /**
   * Asserts that the table is writable with the current protocol version.
   *
   * @throws RuntimeException
   *   if the table protocol version is too new to write
   */
  def assertTableWritable(): Unit

  /**
   * Upgrades the table protocol to support newer features.
   *
   * This operation writes a new protocol action with higher min reader/writer versions.
   *
   * @param newMinReaderVersion
   *   The new minimum reader version
   * @param newMinWriterVersion
   *   The new minimum writer version
   */
  def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit

  /**
   * Commits a merge splits operation atomically.
   *
   * This operation removes the input splits and adds the merged split in a single transaction. This ensures that
   * readers either see the old splits or the new merged split, never an inconsistent state.
   *
   * @param removeActions
   *   Sequence of remove actions for splits being merged
   * @param addActions
   *   Sequence of add actions for new merged splits
   * @return
   *   The transaction version number for this operation
   */
  def commitMergeSplits(removeActions: Seq[RemoveAction], addActions: Seq[AddAction]): Long =
    // Default implementation: create a single transaction with removes followed by adds
    // Both TransactionLog and OptimizedTransactionLog override this with custom implementations
    throw new UnsupportedOperationException("commitMergeSplits must be implemented by subclass")

  /**
   * Commits remove actions to mark files as logically deleted.
   *
   * This operation marks files as removed in the transaction log without physically deleting them. The files become
   * invisible to queries but remain on disk for recovery and time-travel purposes. Physical deletion occurs when
   * running PURGE INDEXTABLE after the retention period expires.
   *
   * @param removeActions
   *   Sequence of remove actions for files to mark as deleted
   * @return
   *   The transaction version number for this operation
   */
  def commitRemoveActions(removeActions: Seq[RemoveAction]): Long =
    // Default implementation can be overridden by subclasses
    throw new UnsupportedOperationException("commitRemoveActions must be implemented by subclass")

  /**
   * Gets all actions from the latest checkpoint.
   *
   * This returns the consolidated state from the checkpoint file, which contains all AddActions visible at the
   * checkpoint version. This is different from readVersion() which only reads a single transaction file.
   *
   * @return
   *   Option containing all actions from the checkpoint, or None if no checkpoint exists
   */
  def getCheckpointActions(): Option[Seq[Action]] =
    throw new UnsupportedOperationException("getCheckpointActions must be implemented by subclass")

  /**
   * Gets all transaction version numbers in the log.
   *
   * @return
   *   Sequence of version numbers, sorted ascending
   */
  def getVersions(): Seq[Long] =
    throw new UnsupportedOperationException("getVersions must be implemented by subclass")

  /**
   * Reads a specific version of the transaction log.
   *
   * This reads the individual version file (e.g., 00000000000000000005.json), not the checkpoint file.
   *
   * @param version
   *   The version number to read
   * @return
   *   Sequence of actions in that version
   */
  def readVersion(version: Long): Seq[Action] =
    throw new UnsupportedOperationException("readVersion must be implemented by subclass")
}
