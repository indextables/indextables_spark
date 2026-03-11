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

package org.apache.spark.sql.indextables

import io.indextables.tantivy4java.memory.NativeMemoryAccountant

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Thread-dispatching memory accountant that bridges tantivy4java's process-global
 * memory pool with Spark's per-task unified memory manager.
 *
 * Placed in `org.apache.spark.sql.indextables` to access `private[spark]`
 * `TaskContext.taskMemoryManager()`, following the same pattern as
 * [[OutputMetricsUpdater]].
 *
 * On each `acquireMemory`/`releaseMemory` call, resolves the current thread's
 * [[TaskContext]] to find the correct [[TaskMemoryManager]] and delegates to it.
 * Since native code runs on Spark task threads, this correctly charges each
 * allocation to the right task.
 *
 * If called outside a Spark task context (no [[TaskContext]] on the thread),
 * the allocation is denied (returns 0) and an ERROR is logged with a stack
 * trace. This makes untracked allocations immediately visible as bugs rather
 * than silently hiding them.
 *
 * On task completion, any outstanding unreleased memory is forcibly released
 * back to the TaskMemoryManager to prevent progressive pool shrinkage.
 *
 * Design follows the pattern established by DataFusion Comet's
 * `CometTaskMemoryManager`, adapted for tantivy4java's single-accountant
 * process-global model.
 */
class SparkUnifiedMemoryAccountant extends NativeMemoryAccountant {

  import SparkUnifiedMemoryAccountant._

  /** Per-task MemoryConsumer, keyed by taskAttemptId. Created lazily, cleaned up on task completion. */
  private val consumers = new ConcurrentHashMap[Long, TaskConsumer]()

  override def acquireMemory(bytes: Long): Long = {
    val tc = resolveConsumer()
    if (tc == null) return 0 // Denied — no task context
    val acquired = tc.tmm.acquireExecutionMemory(bytes, tc.consumer)
    tc.used.addAndGet(acquired)
    acquired
  }

  override def releaseMemory(bytes: Long): Unit = {
    val tc = resolveConsumer()
    if (tc == null) return // No task context — nothing to release against
    // Cap release to what Spark actually granted. tantivy4java may request to release
    // more than was acquired (best-effort pattern: Spark grants partial, Rust uses full).
    val currentUsed = tc.used.get()
    val toRelease   = math.min(bytes, currentUsed)
    if (toRelease > 0) {
      tc.used.addAndGet(-toRelease)
      tc.tmm.releaseExecutionMemory(toRelease, tc.consumer)
    }
    if (bytes > currentUsed) {
      logger.debug(
        s"Native release of $bytes bytes exceeds tracked usage of $currentUsed bytes " +
          "(partial grant). Released $toRelease to Spark.")
    }
  }

  private def resolveConsumer(): TaskConsumer = {
    val ctx = TaskContext.get()
    if (ctx == null) {
      // Shutdown hooks (e.g., SplitCacheManager cleanup) release memory outside task context.
      // This is expected and not a bug — demote to debug for shutdown threads.
      val threadName = Thread.currentThread().getName
      if (threadName.contains("Shutdown") || threadName.contains("shutdown")) {
        logger.debug(
          s"Native memory release on shutdown thread '$threadName' outside task context — " +
            "memory will be reclaimed by process exit.")
      } else {
        logger.error(
          "Native memory acquire/release called outside Spark task context. " +
            "This is a bug — all native allocations must occur on a task thread.",
          new IllegalStateException("No TaskContext"))
      }
      return null
    }
    val taskId = ctx.taskAttemptId()
    consumers.computeIfAbsent(taskId, _ => {
      val tmm = ctx.taskMemoryManager()
      val tc  = new TaskConsumer(tmm, taskId)
      // Release any outstanding native memory when the task completes,
      // preventing progressive off-heap pool shrinkage if Rust defers
      // releases past task lifetime.
      ctx.addTaskCompletionListener[Unit] { _ =>
        val removed = consumers.remove(taskId)
        if (removed != null) {
          val outstanding = removed.used.get()
          if (outstanding > 0) {
            removed.tmm.releaseExecutionMemory(outstanding, removed.consumer)
            logger.debug(s"Released $outstanding outstanding native bytes on task $taskId completion")
          }
        }
      }
      tc
    })
  }
}

object SparkUnifiedMemoryAccountant {

  private val logger = LoggerFactory.getLogger(classOf[SparkUnifiedMemoryAccountant])

  /**
   * Per-task wrapper holding a MemoryConsumer registered with the task's TaskMemoryManager.
   */
  private class TaskConsumer(val tmm: TaskMemoryManager, taskId: Long) {
    val used = new AtomicLong()

    val consumer: MemoryConsumer = new MemoryConsumer(tmm, 0, MemoryMode.OFF_HEAP) {
      // tantivy4java uses best-effort pattern: if the pool denies memory,
      // operations proceed with an empty (untracked) reservation.
      // No spill callback is needed.
      override def spill(size: Long, trigger: MemoryConsumer): Long = 0

      override def toString: String = s"IndextablesNativeMemoryConsumer(taskId=$taskId)"
    }
  }
}
