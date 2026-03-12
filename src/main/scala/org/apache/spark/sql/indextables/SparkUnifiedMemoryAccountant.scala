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
    val newUsed = tc.used.addAndGet(-bytes)
    // Cap release to what Spark actually granted to avoid driving the pool counter
    // below its true floor. Should not happen after tantivy4java 0.32.5 bug fix
    // (release capped to granted in JvmMemoryPool), but guard defensively.
    val toRelease = if (newUsed < 0) {
      logger.warn(
        s"Native memory used counter went negative ($newUsed) after releasing $bytes bytes. " +
          "Clamping to 0.")
      // CAS loop: only reset to 0 if no concurrent acquire has already corrected it
      var prev = newUsed
      while (prev < 0 && !tc.used.compareAndSet(prev, 0)) {
        prev = tc.used.get()
      }
      bytes + newUsed // == previousUsed, the amount Spark actually granted
    } else {
      bytes
    }
    if (toRelease > 0) {
      tc.tmm.releaseExecutionMemory(toRelease, tc.consumer)
    }
  }

  private def resolveConsumer(): TaskConsumer = {
    val ctx = TaskContext.get()
    if (ctx == null) {
      // tantivy4java 0.32.5+ calls NativeMemoryManager.shutdown() before the
      // SplitCacheManager shutdown hook, so this path should only be hit for
      // genuinely unexpected non-task-thread allocations.
      logger.error(
        "Native memory acquire/release called outside Spark task context. " +
          "This is a bug — all native allocations must occur on a task thread.",
        new IllegalStateException("No TaskContext"))
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
