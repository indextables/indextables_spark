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

package io.indextables.spark.memory

import io.indextables.tantivy4java.memory.NativeMemoryManager
import org.slf4j.LoggerFactory

/**
 * Singleton that initializes tantivy4java's native memory pool with Spark's
 * unified memory manager. Call [[ensureInitialized]] from any executor code
 * path — it is idempotent after the first successful call.
 *
 * The accountant is process-global (tantivy4java uses a Rust OnceLock), but
 * [[SparkUnifiedMemoryAccountant]] dispatches each acquire/release to the
 * current thread's TaskContext, so memory is correctly charged per-task.
 */
object NativeMemoryInitializer {

  private val logger = LoggerFactory.getLogger(getClass)

  @volatile private var initialized = false

  /** Configuration key to disable native memory integration (default: enabled). */
  val NATIVE_MEMORY_ENABLED_KEY = "spark.indextables.native.memory.enabled"

  /**
   * Initialize the native memory pool with Spark's unified memory manager.
   *
   * Must be called while a TaskContext is active on the current thread so the
   * accountant can resolve the task's TaskMemoryManager.
   *
   * Safe to call from multiple threads — only the first call configures the pool.
   */
  def ensureInitialized(): Unit = {
    if (!initialized) synchronized {
      if (!initialized) {
        // Ensure the native library is loaded before calling NativeMemoryManager.
        // Tantivy.class static init extracts the .dylib/.so from the jar and calls System.load().
        // NativeMemoryManager's own static init uses System.loadLibrary() which won't find the
        // jar-embedded library — loading Tantivy first ensures the native symbols are available.
        Class.forName("io.indextables.tantivy4java.core.Tantivy")

        val enabled = isEnabled
        if (enabled) {
          // Warn if off-heap memory is not enabled — without it, all acquireExecutionMemory
          // calls return 0 and native allocations proceed untracked.
          checkOffHeapEnabled()

          val accountant = new org.apache.spark.sql.indextables.SparkUnifiedMemoryAccountant()
          val success = NativeMemoryManager.setAccountant(accountant)
          if (success) {
            logger.info("Native memory pool initialized with Spark unified memory manager")
          } else {
            logger.debug("Native memory pool already configured (another thread initialized first)")
          }
        } else {
          logger.info("Native memory integration disabled via {}", NATIVE_MEMORY_ENABLED_KEY)
        }
        initialized = true
      }
    }
  }

  private def isEnabled: Boolean = {
    val env = org.apache.spark.SparkEnv.get
    if (env == null) {
      // SparkEnv not available (e.g., in unit tests) — default to enabled
      return true
    }
    env.conf.getBoolean(NATIVE_MEMORY_ENABLED_KEY, defaultValue = true)
  }

  private def checkOffHeapEnabled(): Unit = {
    val env = org.apache.spark.SparkEnv.get
    if (env == null) return
    val offHeapEnabled = env.conf.getBoolean("spark.memory.offHeap.enabled", defaultValue = false)
    if (!offHeapEnabled) {
      logger.warn(
        "Native memory integration is enabled but spark.memory.offHeap.enabled=false. " +
          "All native memory requests will receive 0-byte grants. " +
          "Set spark.memory.offHeap.enabled=true and spark.memory.offHeap.size to a " +
          "non-zero value (e.g., '4g') for Spark to manage native memory allocations.")
    } else {
      val offHeapSize = env.conf.getSizeAsBytes("spark.memory.offHeap.size", "0")
      if (offHeapSize <= 0) {
        logger.warn(
          "Native memory integration is enabled and spark.memory.offHeap.enabled=true, " +
            "but spark.memory.offHeap.size is 0. Set it to a non-zero value (e.g., '4g').")
      } else {
        logger.info(s"Off-heap memory pool: ${offHeapSize / 1024 / 1024}MB available for native allocations")
      }
    }
  }
}
