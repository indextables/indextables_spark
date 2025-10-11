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

import scala.collection.mutable

import org.slf4j.LoggerFactory

/**
 * Reduces distributed transaction actions into final state.
 * Handles REMOVE/ADD merging across partitions following Delta Lake semantics.
 *
 * This object implements the core logic for combining transaction log actions
 * from multiple executors into a consistent final state. It handles:
 * - ADD actions: Add files to the table state
 * - REMOVE actions: Remove files from the table state
 * - Metadata/Protocol actions: Track table metadata
 *
 * The reducer supports both partition-local reduction (map-side aggregation)
 * for memory efficiency and global reduction (final state computation).
 */
object DistributedStateReducer extends Serializable {

  // Use lazy val for logger to ensure it's created on each executor
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Merge actions from multiple executors into final state.
   * Applies Delta Lake REMOVE/ADD semantics to compute visible files.
   *
   * This is the final reduction step that runs on the driver after
   * collecting results from all executors.
   *
   * CRITICAL: Actions must be applied in version order to maintain
   * correct transaction semantics!
   *
   * @param checkpointActions Actions from the most recent checkpoint (baseline state)
   * @param incrementalActions Versioned actions from transaction files after checkpoint
   * @return Final sequence of AddAction representing all visible files
   */
  def reduceToFinalState(
    checkpointActions: Seq[AddAction],
    incrementalActions: Seq[VersionedAction]
  ): Seq[AddAction] = {
    logger.debug(s"Reducing to final state: ${checkpointActions.length} checkpoint actions, " +
      s"${incrementalActions.length} incremental actions")

    // Start with checkpoint state
    // Use mutable Map for efficient updates
    val activeFiles = mutable.Map[String, AddAction]()
    checkpointActions.foreach { add =>
      activeFiles(add.path) = add
    }

    logger.debug(s"Initial state from checkpoint: ${activeFiles.size} files")

    // CRITICAL: Sort actions by version to maintain transaction order
    // This ensures that actions are applied in the same order they were written
    val sortedActions = incrementalActions.sortBy(_.version)

    var addCount = 0
    var removeCount = 0

    sortedActions.foreach { versionedAction =>
      versionedAction.action match {
        case add: AddAction =>
          activeFiles(add.path) = add
          addCount += 1

        case remove: RemoveAction =>
          activeFiles.remove(remove.path)
          removeCount += 1

        case _: MetadataAction =>
          // Metadata actions don't affect file list
          // They're tracked separately in the transaction log

        case _: ProtocolAction =>
          // Protocol actions don't affect file list
          // They're tracked separately in the transaction log

        case _: SkipAction =>
          // Skip actions don't affect file list
          // They're tracked for skipped files management

        case other =>
          logger.warn(s"Ignoring unknown action type during reduction: ${other.getClass.getName}")
      }
    }

    val finalFiles = activeFiles.values.toSeq
    logger.info(s"Final state: ${finalFiles.length} files (applied $addCount adds, $removeCount removes)")

    finalFiles
  }

  /**
   * Partition-local reduce for map-side aggregation.
   * Reduces memory pressure before shuffle by consolidating actions locally.
   *
   * This runs on executors before collecting results to the driver.
   * It performs local deduplication and state consolidation to reduce
   * the amount of data that needs to be shuffled.
   *
   * @param actions Iterator of versioned actions from partition
   * @return Map of file path to most recent versioned action for that path
   */
  def partitionLocalReduce(actions: Iterator[VersionedAction]): Map[String, VersionedAction] = {
    val localState = mutable.Map[String, VersionedAction]()
    var actionCount = 0

    actions.foreach { versionedAction =>
      actionCount += 1
      versionedAction.action match {
        case add: AddAction =>
          // Keep most recent ADD action for each path (highest version)
          val key = add.path
          localState.get(key) match {
            case Some(existing) if existing.version >= versionedAction.version =>
              // Keep existing (higher version)
            case _ =>
              // Update with new version
              localState(key) = versionedAction
          }

        case remove: RemoveAction =>
          // Keep most recent REMOVE action for each path (highest version)
          val key = remove.path
          localState.get(key) match {
            case Some(existing) if existing.version >= versionedAction.version =>
              // Keep existing (higher version)
            case _ =>
              // Update with new version
              localState(key) = versionedAction
          }

        case metadata: MetadataAction =>
          // Store metadata with unique key
          val key = s"__metadata__${metadata.id}"
          localState(key) = versionedAction

        case protocol: ProtocolAction =>
          // Store protocol with unique key
          val key = s"__protocol__${protocol.minReaderVersion}_${protocol.minWriterVersion}"
          localState(key) = versionedAction

        case skip: SkipAction =>
          // Store skip actions with unique key
          val key = s"__skip__${skip.path}_${skip.skipTimestamp}"
          localState(key) = versionedAction

        case other =>
          logger.warn(s"Ignoring unknown action type in partition reduce: ${other.getClass.getName}")
      }
    }

    logger.debug(s"Partition-local reduce: processed $actionCount actions, " +
      s"reduced to ${localState.size} unique actions")

    localState.toMap
  }

  /**
   * Extract only AddAction entries from reduced state.
   * Filters out removed files and non-file actions.
   *
   * @param reducedState Map from partition/global reduction
   * @return Sequence of AddAction representing visible files
   */
  def extractAddActions(reducedState: Map[String, VersionedAction]): Seq[AddAction] = {
    reducedState.values.collect {
      case VersionedAction(add: AddAction, _) => add
    }.toSeq
  }

  /**
   * Compute statistics about the reduction.
   * Useful for monitoring and debugging.
   *
   * @param checkpointActions Checkpoint actions
   * @param incrementalActions Incremental versioned actions
   * @param finalFiles Final file list
   * @return Statistics map
   */
  def computeReductionStats(
    checkpointActions: Seq[AddAction],
    incrementalActions: Seq[VersionedAction],
    finalFiles: Seq[AddAction]
  ): Map[String, Any] = {
    val addActions = incrementalActions.count(_.action.isInstanceOf[AddAction])
    val removeActions = incrementalActions.count(_.action.isInstanceOf[RemoveAction])
    val metadataActions = incrementalActions.count(_.action.isInstanceOf[MetadataAction])
    val protocolActions = incrementalActions.count(_.action.isInstanceOf[ProtocolAction])

    Map(
      "checkpointFiles" -> checkpointActions.length,
      "incrementalActions" -> incrementalActions.length,
      "incrementalAdds" -> addActions,
      "incrementalRemoves" -> removeActions,
      "incrementalMetadata" -> metadataActions,
      "incrementalProtocol" -> protocolActions,
      "finalFiles" -> finalFiles.length,
      "netChange" -> (finalFiles.length - checkpointActions.length)
    )
  }
}
