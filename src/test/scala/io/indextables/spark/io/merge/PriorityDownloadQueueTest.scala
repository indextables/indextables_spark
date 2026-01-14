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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PriorityDownloadQueueTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var queue: PriorityDownloadQueue = _

  override def beforeEach(): Unit = {
    queue = new PriorityDownloadQueue()
  }

  test("empty queue returns None on poll") {
    queue.poll() shouldBe None
    queue.isEmpty shouldBe true
    queue.pendingDownloads shouldBe 0
  }

  test("single batch submission and retrieval") {
    val batchId = queue.nextBatchId()
    val requests = (0 until 3).map { i =>
      DownloadRequest(s"s3://bucket/file$i.split", s"/tmp/file$i.split", 1000L, batchId, i)
    }

    val batch = DownloadBatch(batchId, System.currentTimeMillis(), requests)
    queue.submitBatch(batch)

    queue.pendingDownloads shouldBe 3
    queue.activeBatches shouldBe 1

    // Poll all items
    val polled = (0 until 3).flatMap(_ => queue.poll())
    polled.size shouldBe 3
    polled.map(_.index).toSet shouldBe Set(0, 1, 2)

    queue.isEmpty shouldBe true
    queue.pendingDownloads shouldBe 0
  }

  test("batches are processed in FIFO order") {
    // Submit batch 1
    val batchId1 = queue.nextBatchId()
    val requests1 = (0 until 2).map { i =>
      DownloadRequest(s"s3://bucket/batch1-file$i.split", s"/tmp/batch1-file$i.split", 1000L, batchId1, i)
    }
    queue.submitBatch(DownloadBatch(batchId1, System.currentTimeMillis(), requests1))

    // Submit batch 2
    val batchId2 = queue.nextBatchId()
    val requests2 = (0 until 2).map { i =>
      DownloadRequest(s"s3://bucket/batch2-file$i.split", s"/tmp/batch2-file$i.split", 1000L, batchId2, i)
    }
    queue.submitBatch(DownloadBatch(batchId2, System.currentTimeMillis(), requests2))

    queue.pendingDownloads shouldBe 4
    queue.activeBatches shouldBe 2

    // First two polls should be from batch 1
    val first = queue.poll().get
    first.batchId shouldBe batchId1

    val second = queue.poll().get
    second.batchId shouldBe batchId1

    // Next two should be from batch 2
    val third = queue.poll().get
    third.batchId shouldBe batchId2

    val fourth = queue.poll().get
    fourth.batchId shouldBe batchId2

    queue.isEmpty shouldBe true
  }

  test("cancel batch removes pending downloads") {
    val batchId1 = queue.nextBatchId()
    val requests1 = (0 until 5).map { i =>
      DownloadRequest(s"s3://bucket/file$i.split", s"/tmp/file$i.split", 1000L, batchId1, i)
    }
    queue.submitBatch(DownloadBatch(batchId1, System.currentTimeMillis(), requests1))

    val batchId2 = queue.nextBatchId()
    val requests2 = (0 until 3).map { i =>
      DownloadRequest(s"s3://bucket/other$i.split", s"/tmp/other$i.split", 1000L, batchId2, i)
    }
    queue.submitBatch(DownloadBatch(batchId2, System.currentTimeMillis(), requests2))

    queue.pendingDownloads shouldBe 8

    // Cancel batch 1
    val cancelled = queue.cancelBatch(batchId1)
    cancelled shouldBe 5

    queue.pendingDownloads shouldBe 3
    queue.activeBatches shouldBe 1

    // Remaining items should be from batch 2
    val remaining = (0 until 3).flatMap(_ => queue.poll())
    remaining.size shouldBe 3
    remaining.foreach(_.batchId shouldBe batchId2)
  }

  test("cancel non-existent batch returns 0") {
    val cancelled = queue.cancelBatch(999L)
    cancelled shouldBe 0
  }

  test("empty batch is ignored") {
    val batchId = queue.nextBatchId()
    val batch = DownloadBatch(batchId, System.currentTimeMillis(), Seq.empty)
    queue.submitBatch(batch)

    queue.isEmpty shouldBe true
    queue.pendingDownloads shouldBe 0
  }

  test("concurrent poll from multiple threads") {
    val batchId = queue.nextBatchId()
    val numRequests = 100
    val requests = (0 until numRequests).map { i =>
      DownloadRequest(s"s3://bucket/file$i.split", s"/tmp/file$i.split", 1000L, batchId, i)
    }
    queue.submitBatch(DownloadBatch(batchId, System.currentTimeMillis(), requests))

    val executor = Executors.newFixedThreadPool(10)
    val polledItems = new java.util.concurrent.ConcurrentLinkedQueue[DownloadRequest]()
    val latch = new CountDownLatch(numRequests)

    // Start 10 threads polling concurrently
    (0 until 10).foreach { _ =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          var continue = true
          while (continue) {
            queue.poll() match {
              case Some(req) =>
                polledItems.add(req)
                latch.countDown()
              case None =>
                continue = false
            }
          }
        }
      })
    }

    latch.await(10, TimeUnit.SECONDS)
    executor.shutdown()

    // All items should have been polled exactly once
    polledItems.size() shouldBe numRequests
    val indices = new java.util.HashSet[Int]()
    polledItems.forEach(r => indices.add(r.index))
    indices.size() shouldBe numRequests
  }

  test("getStats returns correct statistics") {
    val batchId1 = queue.nextBatchId()
    queue.submitBatch(DownloadBatch(batchId1, System.currentTimeMillis(),
      (0 until 5).map(i => DownloadRequest(s"s3://b/f$i", s"/t/f$i", 100L, batchId1, i))))

    val batchId2 = queue.nextBatchId()
    queue.submitBatch(DownloadBatch(batchId2, System.currentTimeMillis(),
      (0 until 3).map(i => DownloadRequest(s"s3://b/g$i", s"/t/g$i", 100L, batchId2, i))))

    val stats = queue.getStats
    stats.totalPending shouldBe 8
    stats.activeBatches shouldBe 2
    stats.batchStats.size shouldBe 2
    stats.highestPriorityBatchId shouldBe Some(batchId1)
  }

  test("clear removes all batches") {
    val batchId = queue.nextBatchId()
    queue.submitBatch(DownloadBatch(batchId, System.currentTimeMillis(),
      (0 until 10).map(i => DownloadRequest(s"s3://b/f$i", s"/t/f$i", 100L, batchId, i))))

    queue.pendingDownloads shouldBe 10

    queue.clear()

    queue.isEmpty shouldBe true
    queue.pendingDownloads shouldBe 0
    queue.activeBatches shouldBe 0
  }

  test("batchId generation is monotonically increasing") {
    val ids = (0 until 100).map(_ => queue.nextBatchId())
    ids shouldBe sorted
    ids.toSet.size shouldBe 100 // All unique
  }
}
