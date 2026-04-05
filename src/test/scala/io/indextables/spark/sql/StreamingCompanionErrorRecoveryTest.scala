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

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.sql.{Row, SparkSession}

import io.indextables.spark.TestBase

/**
 * Tests for streaming companion error recovery: exponential backoff, consecutive error limits, error counter reset, and
 * metrics tracking.
 *
 * Uses a controllable syncFn lambda to simulate failures without requiring actual Delta/Iceberg tables. The parquet
 * source format is used so cheapSourceVersion always returns None, ensuring syncFn is called every cycle.
 */
class StreamingCompanionErrorRecoveryTest extends TestBase with io.indextables.spark.testutils.FileCleanupHelper {

  override protected def deleteRecursively(file: File): Unit = super[FileCleanupHelper].deleteRecursively(file)

  private val tempDirs                 = new java.util.concurrent.CopyOnWriteArrayList[File]()
  private var sharedSourcePath: String = _

  private def makeTempDir(prefix: String): String = {
    val dir = Files.createTempDirectory(prefix).toFile
    tempDirs.add(dir)
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedSourcePath = makeTempDir("streaming-src")
  }

  override def afterAll(): Unit = {
    tempDirs.forEach(deleteRecursively)
    super.afterAll()
  }

  private def makeCommand(destPath: String): SyncToExternalCommand =
    SyncToExternalCommand(
      sourceFormat = "parquet",
      sourcePath = sharedSourcePath,
      destPath = destPath,
      indexingModes = Map.empty,
      fastFieldMode = "HYBRID",
      targetInputSize = None,
      dryRun = false
    )

  /**
   * Runs the streaming manager on a daemon thread. Returns the thread. The manager will call syncFn on every cycle
   * because parquet cheapSourceVersion always returns None.
   */
  private def runOnThread(
    manager: StreamingCompanionManager
  ): Thread = {
    val thread = new Thread(() =>
      try manager.runStreaming(spark)
      catch { case _: RuntimeException => }
    )
    thread.setDaemon(true)
    thread.start()
    thread
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Consecutive error limit
  // ═══════════════════════════════════════════════════════════════════════════

  test("stream should stop after maxConsecutiveErrors") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "3")
    try {
      val destPath  = makeTempDir("streaming-dest")
      val callCount = new AtomicInteger(0)

      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        callCount.incrementAndGet()
        throw new RuntimeException("simulated failure")
      }

      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 100L, syncFn)

      val ex = intercept[RuntimeException] {
        manager.runStreaming(spark)
      }

      ex.getMessage should include("consecutive errors")
      callCount.get() shouldBe 3
    } finally
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
  }

  test("maxConsecutiveErrors config set to 2 should stop after 2 errors") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "2")
    try {
      val destPath  = makeTempDir("streaming-dest2")
      val callCount = new AtomicInteger(0)

      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        callCount.incrementAndGet()
        throw new RuntimeException("simulated failure")
      }

      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 100L, syncFn)

      val ex = intercept[RuntimeException] {
        manager.runStreaming(spark)
      }

      ex.getMessage should include("consecutive errors")
      callCount.get() shouldBe 2
    } finally
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Error counter reset on success
  // ═══════════════════════════════════════════════════════════════════════════

  test("successful sync should reset consecutive error counter") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "3")
    try {
      val destPath  = makeTempDir("streaming-reset")
      val callCount = new AtomicInteger(0)

      // Pattern: fail, fail, succeed, fail, fail, succeed, fail, fail, succeed...
      // If error counter wasn't reset, the 3rd failure would abort.
      // Since it IS reset by successes, the stream continues past 6+ calls.
      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        val n = callCount.incrementAndGet()
        if (n % 3 == 0) {
          Seq.empty // every 3rd call succeeds, resetting consecutive errors
        } else {
          throw new RuntimeException(s"simulated failure #$n")
        }
      }

      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 100L, syncFn)

      // Run on a thread with timeout — if the error counter isn't reset properly,
      // the stream would abort with RuntimeException after 3 consecutive errors,
      // which would happen before the timeout.
      val thread = new Thread(() =>
        try manager.runStreaming(spark)
        catch { case _: RuntimeException => }
      )
      thread.setDaemon(true)
      thread.start()

      // Poll until enough fail-fail-succeed cycles complete
      val deadline = System.currentTimeMillis() + 10000
      while (callCount.get() < 9 && System.currentTimeMillis() < deadline) Thread.sleep(50)
      thread.interrupt()
      thread.join(5000L)

      // Should have completed many cycles (at least 9 = 3 rounds of fail-fail-succeed)
      callCount.get() should be >= 6
    } finally
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Exponential backoff timing
  // ═══════════════════════════════════════════════════════════════════════════

  test("error backoff should increase sleep time exponentially") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "4")
    spark.conf.set("spark.indextables.companion.stream.errorBackoffMultiplier", "2")
    try {
      val destPath   = makeTempDir("streaming-backoff")
      val timestamps = new java.util.concurrent.CopyOnWriteArrayList[Long]()

      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        timestamps.add(System.currentTimeMillis())
        throw new RuntimeException("simulated failure")
      }

      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 500L, syncFn)

      intercept[RuntimeException] {
        manager.runStreaming(spark)
      }

      // 4 calls, 3 intervals between them
      timestamps.size() shouldBe 4

      val ts        = (0 until timestamps.size()).map(timestamps.get)
      val intervals = ts.sliding(2).map { case Seq(a, b) => b - a }.toSeq

      // With multiplier=2 and pollInterval=500ms:
      // After error 1: sleep = 2^1 * 500 = 1000ms
      // After error 2: sleep = 2^2 * 500 = 2000ms
      // After error 3: sleep = 2^3 * 500 = 4000ms
      // Error 4 hits maxConsecutiveErrors and throws immediately
      // 80% of expected value to allow JVM jitter while catching broken backoff
      intervals(0) should be >= 800L  // first backoff: ~1000ms (2^1 * 500)
      intervals(1) should be >= 1600L // second backoff: ~2000ms (2^2 * 500)
      // Current impl is sleep-then-check, so the 3rd interval also includes backoff sleep.
      // Asserting this explicitly so a change to check-then-sleep would be caught.
      intervals(2) should be >= 3200L // third backoff: ~4000ms (2^3 * 500)
    } finally {
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
      spark.conf.unset("spark.indextables.companion.stream.errorBackoffMultiplier")
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Metrics tracking
  // ═══════════════════════════════════════════════════════════════════════════

  test("exactly maxConsecutiveErrors sync calls should be made before abort") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "5")
    try {
      val destPath  = makeTempDir("streaming-exact-count")
      val callCount = new AtomicInteger(0)

      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        callCount.incrementAndGet()
        throw new RuntimeException("simulated failure")
      }

      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 100L, syncFn)

      intercept[RuntimeException] {
        manager.runStreaming(spark)
      }

      callCount.get() shouldBe 5
    } finally
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
  }

  test("successful sync cycles should complete without errors") {
    val destPath  = makeTempDir("streaming-success")
    val callCount = new AtomicInteger(0)

    val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
      _,
      _,
      _
    ) => {
      callCount.incrementAndGet()
      Seq.empty
    }

    val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 100L, syncFn)

    val thread = runOnThread(manager)

    // Poll until enough successful cycles complete
    val deadline = System.currentTimeMillis() + 10000
    while (callCount.get() < 5 && System.currentTimeMillis() < deadline) Thread.sleep(50)
    thread.interrupt()
    thread.join(5000L)

    thread.isAlive shouldBe false
    callCount.get() should be >= 5
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Interruption during backoff
  // ═══════════════════════════════════════════════════════════════════════════

  test("interrupt during backoff sleep should exit cleanly") {
    spark.conf.set("spark.indextables.companion.stream.maxConsecutiveErrors", "100")
    try {
      val destPath      = makeTempDir("streaming-interrupt")
      val firstCallDone = new AtomicBoolean(false)

      val syncFn: (SparkSession, Long, Option[Long]) => Seq[Row] = (
        _,
        _,
        _
      ) => {
        firstCallDone.set(true)
        throw new RuntimeException("simulated failure")
      }

      // Long poll interval so backoff sleep is long enough to interrupt reliably
      val manager = new StreamingCompanionManager(makeCommand(destPath), pollIntervalMs = 30000L, syncFn)
      val thread  = runOnThread(manager)

      // Wait for the first sync call to complete (enters backoff sleep)
      val deadline = System.currentTimeMillis() + 10000
      while (!firstCallDone.get() && System.currentTimeMillis() < deadline) Thread.sleep(100)
      firstCallDone.get() shouldBe true

      // Interrupt during backoff sleep
      Thread.sleep(500) // small buffer to ensure thread entered sleep
      thread.interrupt()
      thread.join(5000L)
      thread.isAlive shouldBe false
    } finally
      spark.conf.unset("spark.indextables.companion.stream.maxConsecutiveErrors")
  }
}
