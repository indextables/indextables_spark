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

package io.indextables.spark.sync

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the `/local_disk0` scratch-leak fix in `SyncTaskExecutor`:
 *   - the pure orphan-sweep core (`sweepOrphanedDirs`) and its config parsing (`resolveScratchMaxAgeMinutes`)
 *   - the underlying kill-safe cleanup mechanism (TaskContext completion/failure listeners firing even when a
 *     task's own code path never reaches a `finally` block)
 *
 * No cloud credentials needed -- runs entirely on local filesystem / local Spark.
 */
class SyncTaskExecutorScratchCleanupTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("SyncTaskExecutorScratchCleanupTest")
      .master("local[2]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def waitUntil(maxWaitMs: Long)(condition: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + maxWaitMs
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(25)
    }
    condition
  }

  // ---- sweepOrphanedDirs (pure core) ----

  test("sweepOrphanedDirs removes only sync-* dirs older than the threshold") {
    val base = Files.createTempDirectory("scratch-sweep-test").toFile
    try {
      val now = System.currentTimeMillis()

      val oldOrphan = new File(base, "sync-old-orphan")
      oldOrphan.mkdirs()
      oldOrphan.setLastModified(now - (7L * 60L * 60L * 1000L)) // 7h old

      val freshInFlight = new File(base, "sync-fresh-in-flight")
      freshInFlight.mkdirs()
      freshInFlight.setLastModified(now) // just touched, e.g. an active download

      val unrelatedDir = new File(base, "not-a-sync-dir")
      unrelatedDir.mkdirs()
      unrelatedDir.setLastModified(now - (7L * 60L * 60L * 1000L))

      val maxAgeMillis = 6L * 60L * 60L * 1000L // 6h
      val removed      = SyncTaskExecutor.sweepOrphanedDirs(base, "sync-", maxAgeMillis, now)

      removed.map(_.getName) shouldBe Seq("sync-old-orphan")
      oldOrphan.exists() shouldBe false
      freshInFlight.exists() shouldBe true
      unrelatedDir.exists() shouldBe true
    } finally {
      def deleteAll(f: File): Unit = {
        if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteAll))
        f.delete()
      }
      deleteAll(base)
    }
  }

  test("sweepOrphanedDirs no-ops when the base directory doesn't exist") {
    val missing = new File(Files.createTempDirectory("scratch-sweep-missing").toFile, "does-not-exist")
    SyncTaskExecutor.sweepOrphanedDirs(missing, "sync-", 1000L, System.currentTimeMillis()) shouldBe Seq.empty
  }

  // ---- resolveScratchMaxAgeMinutes (config parsing) ----

  test("resolveScratchMaxAgeMinutes defaults to 360 when the key is absent") {
    SyncTaskExecutor.resolveScratchMaxAgeMinutes(Map.empty) shouldBe 360L
  }

  test("resolveScratchMaxAgeMinutes respects a valid override") {
    SyncTaskExecutor.resolveScratchMaxAgeMinutes(
      Map("spark.indextables.companion.sync.localScratch.maxAgeMinutes" -> "42")
    ) shouldBe 42L
  }

  test("resolveScratchMaxAgeMinutes falls back to the default on a garbage value, without throwing") {
    SyncTaskExecutor.resolveScratchMaxAgeMinutes(
      Map("spark.indextables.companion.sync.localScratch.maxAgeMinutes" -> "not-a-number")
    ) shouldBe 360L
  }

  // ---- kill-safe cleanup mechanism ----

  test(
    "TaskContext completion/failure listeners clean up scratch even when the task's own code " +
      "never reaches a finally block"
  ) {
    val tempDir = Files.createTempDirectory("kill-safety-test").toFile
    Files.write(new File(tempDir, "marker.txt").toPath, "x".getBytes)
    tempDir.exists() shouldBe true

    val rdd = spark.sparkContext.parallelize(Seq(1), 1)
    val taskFunc = (tc: TaskContext, _: Iterator[Int]) => {
      // Mirrors SyncTaskExecutor.execute()'s registration pattern: cleanup is wired via TaskContext
      // listeners immediately, before any of the task's own (fallible) work runs.
      tc.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          def deleteAll(f: File): Unit = {
            if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteAll))
            f.delete()
          }
          deleteAll(tempDir)
        }
      })
      tc.addTaskFailureListener(new TaskFailureListener {
        override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
          def deleteAll(f: File): Unit = {
            if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteAll))
            f.delete()
          }
          deleteAll(tempDir)
        }
      })
      // Simulate a task that is killed/fails before it can reach any cleanup code of its own —
      // no try/finally in this closure at all.
      throw new RuntimeException("simulated task failure - no finally block reached")
      1
    }

    intercept[SparkException] {
      spark.sparkContext.runJob(rdd, taskFunc)
    }

    waitUntil(5000)(!tempDir.exists()) shouldBe true
  }
}
