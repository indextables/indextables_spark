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

package io.indextables.spark.io.merge

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListMap}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

/**
 * A two-level priority queue for download requests that maintains batch ordering.
 *
 * Priority semantics:
 *   - Batches are ordered by submission time (lower batchId = earlier submission = higher priority)
 *   - Downloads from the highest-priority incomplete batch are served first
 *   - When the highest-priority batch is exhausted, the next batch becomes highest priority
 *   - This ensures first-submitted batches complete before later batches while maximizing throughput
 *
 * Thread-safety: All operations are thread-safe for concurrent access.
 */
class PriorityDownloadQueue {

  private val logger = LoggerFactory.getLogger(classOf[PriorityDownloadQueue])

  // ConcurrentSkipListMap maintains natural ordering (lowest batchId first = highest priority)
  // Each entry maps batchId -> queue of remaining downloads for that batch
  private val batches = new ConcurrentSkipListMap[Long, ConcurrentLinkedQueue[DownloadRequest]]()

  // Monotonically increasing batch ID generator
  private val batchIdGenerator = new AtomicLong(0)

  // Track total pending downloads for monitoring
  private val pendingCount = new AtomicLong(0)

  /** Generate a new unique batch ID. Lower IDs have higher priority. */
  def nextBatchId(): Long =
    batchIdGenerator.incrementAndGet()

  /**
   * Submit a batch of downloads to the queue.
   *
   * All downloads in the batch are assigned the batch's ID and added to the queue. The batch will be processed in
   * priority order based on its ID (lower = higher priority).
   *
   * @param batch
   *   The download batch to submit
   */
  def submitBatch(batch: DownloadBatch): Unit = {
    if (batch.downloads.isEmpty) {
      logger.debug(s"Ignoring empty batch ${batch.batchId}")
      return
    }

    val queue = new ConcurrentLinkedQueue[DownloadRequest]()

    // Add all downloads with the batch ID
    batch.downloads.foreach(request => queue.offer(request.withBatchId(batch.batchId)))

    // Add to the batch map
    batches.put(batch.batchId, queue)
    pendingCount.addAndGet(batch.downloads.size)

    logger.debug(s"Submitted batch ${batch.batchId} with ${batch.downloads.size} downloads")
  }

  /**
   * Poll the next download request from the highest-priority batch.
   *
   * Returns the next request from the batch with the lowest batchId (earliest submission). If that batch is empty, it
   * is removed and the next batch becomes the highest priority.
   *
   * @return
   *   The next download request, or None if all batches are empty
   */
  def poll(): Option[DownloadRequest] = {
    // Get the first (lowest batchId = highest priority) entry
    var entry = batches.firstEntry()

    while (entry != null) {
      val queue   = entry.getValue
      val batchId = entry.getKey

      // Try to poll from this batch's queue
      val request = queue.poll()

      if (request != null) {
        pendingCount.decrementAndGet()
        logger.trace(s"Polled request from batch $batchId: ${request.sourcePath}")
        // Check if batch is now empty and remove it
        if (queue.isEmpty) {
          batches.remove(batchId)
          logger.debug(s"Batch $batchId exhausted after poll, removing from queue")
        }
        return Some(request)
      } else {
        // This batch is exhausted, remove it and try the next batch
        batches.remove(batchId)
        logger.debug(s"Batch $batchId exhausted, removing from queue")
        entry = batches.firstEntry()
      }
    }

    // No batches with pending downloads
    None
  }

  /**
   * Cancel all pending downloads for a specific batch.
   *
   * This is called when a merge operation fails and we want to stop downloading remaining files for that batch. Any
   * downloads already in progress will complete, but no new downloads from this batch will be started.
   *
   * @param batchId
   *   The ID of the batch to cancel
   * @return
   *   Number of downloads cancelled
   */
  def cancelBatch(batchId: Long): Int = {
    val queue = batches.remove(batchId)

    if (queue != null) {
      val cancelledCount = queue.size()
      pendingCount.addAndGet(-cancelledCount)
      logger.info(s"Cancelled batch $batchId with $cancelledCount pending downloads")
      cancelledCount
    } else {
      logger.debug(s"Batch $batchId not found for cancellation (already completed or never submitted)")
      0
    }
  }

  /** Get the number of pending downloads across all batches. */
  def pendingDownloads: Long =
    pendingCount.get()

  /** Get the number of active batches (batches with pending downloads). */
  def activeBatches: Int =
    batches.size()

  /** Check if the queue is empty. */
  def isEmpty: Boolean =
    batches.isEmpty

  /** Get statistics about the current queue state. */
  def getStats: PriorityQueueStats = {
    val batchStats = batches.asScala.map {
      case (batchId, queue) =>
        BatchStats(batchId, queue.size())
    }.toSeq

    PriorityQueueStats(
      totalPending = pendingCount.get(),
      activeBatches = batches.size(),
      batchStats = batchStats
    )
  }

  /** Clear all batches from the queue. */
  def clear(): Unit = {
    batches.clear()
    pendingCount.set(0)
    logger.info("Cleared all batches from download queue")
  }
}

/** Statistics for a single batch in the queue. */
case class BatchStats(batchId: Long, pendingDownloads: Int)

/** Statistics for the entire priority queue. */
case class PriorityQueueStats(
  totalPending: Long,
  activeBatches: Int,
  batchStats: Seq[BatchStats]) {

  /** Get the batch ID with highest priority (lowest ID). */
  def highestPriorityBatchId: Option[Long] =
    batchStats.headOption.map(_.batchId)

  override def toString: String =
    s"PriorityQueueStats(totalPending=$totalPending, activeBatches=$activeBatches, " +
      s"batches=${batchStats.map(b => s"${b.batchId}:${b.pendingDownloads}").mkString("[", ", ", "]")})"
}
