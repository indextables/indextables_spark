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

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * End-to-end streaming tests for BUILD INDEXTABLES COMPANION ... WITH STREAMING.
 *
 * Validates the full incremental sync cycle against a local Delta table:
 *   1. Initial full sync populates the companion index. 2. A commit appended while the stream is running is picked up
 *      in the next cycle. 3. A no-changes poll leaves the companion unmodified. 4. A second new commit is picked up by
 *      a subsequent cycle. 5. A restart resumes from the last synced version without re-indexing existing data.
 */
class StreamingCompanionEndToEndTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("StreamingCompanionEndToEndTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def withTempDirs(f: (String, String) => Unit): Unit = {
    val root = Files.createTempDirectory("streaming-e2e").toFile
    try {
      val deltaPath = new File(root, "delta").getAbsolutePath
      val indexPath = new File(root, "index").getAbsolutePath
      flushCaches()
      f(deltaPath, indexPath)
    } finally
      deleteRecursively(root)
  }

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  /**
   * Read the companion index and return its row count.
   *
   * Flushes the split cache before every call so each read reflects the latest on-disk state rather than a cached
   * snapshot. Returns 0 when the index does not yet exist.
   */
  private def countCompanionRows(indexPath: String): Long = {
    flushCaches()
    try
      spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "100000")
        .load(indexPath)
        .count()
    catch {
      case _: Exception => 0L
    }
  }

  /**
   * Poll `condition` every `pollMs` ms until it returns true or `timeoutMs` elapses. Returns true when the condition
   * was satisfied within the timeout.
   */
  private def waitUntil(timeoutMs: Long, pollMs: Long = 500)(condition: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      if (condition) return true
      Thread.sleep(pollMs)
    }
    false
  }

  private def makeDeltaCommand(sourcePath: String, destPath: String): SyncToExternalCommand =
    SyncToExternalCommand(
      sourceFormat = "delta",
      sourcePath = sourcePath,
      destPath = destPath,
      indexingModes = Map.empty,
      fastFieldMode = "HYBRID",
      targetInputSize = None,
      dryRun = false
    )

  // ── Tests ─────────────────────────────────────────────────────────────────────

  test("streaming Delta companion: initial sync then two incremental commits are indexed") {
    withTempDirs { (deltaPath, indexPath) =>
      val ss = spark; import ss.implicits._

      // Step 1: Write initial Delta table (version 0, 2 rows).
      Seq((1, "alice"), (2, "bob"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(deltaPath)

      // Step 2: Launch streaming sync with a 2-second poll interval.
      val command = makeDeltaCommand(deltaPath, indexPath).copy(streamingPollIntervalMs = Some(2000L))
      val thread  = new Thread(() => command.run(spark))
      thread.setDaemon(true)
      thread.start()

      try {
        // Step 3: Wait for the initial full sync to complete (2 rows expected).
        val initialSynced = waitUntil(30000)(countCompanionRows(indexPath) == 2)
        withClue("initial full sync should complete within 30 s") {
          initialSynced shouldBe true
        }

        // Step 4: Append batch 2 while the stream is running (Delta version 1).
        //   The streaming manager detects the version change on the next cheap probe
        //   and immediately runs an incremental cycle.
        Seq((3, "charlie"), (4, "dave"))
          .toDF("id", "name")
          .write
          .format("delta")
          .mode("append")
          .save(deltaPath)

        // Step 5: Validate the back-to-back incremental cycle indexed the 2 new rows.
        val secondSynced = waitUntil(30000)(countCompanionRows(indexPath) == 4)
        withClue("streaming should pick up batch 2 (4 total rows) within 30 s") {
          secondSynced shouldBe true
        }

        // Step 6: Confirm idle — wait 3 poll cycles (6 s) and verify the count is stable.
        Thread.sleep(6000)
        withClue("row count should remain 4 after a no-changes idle period") {
          countCompanionRows(indexPath) shouldBe 4
        }

        // Step 7: Append batch 3 (Delta version 2, 1 new row).
        Seq((5, "eve"))
          .toDF("id", "name")
          .write
          .format("delta")
          .mode("append")
          .save(deltaPath)

        // Step 8: Validate the stream picks up batch 3 (5 total rows).
        val thirdSynced = waitUntil(30000)(countCompanionRows(indexPath) == 5)
        withClue("streaming should pick up batch 3 (5 total rows) within 30 s") {
          thirdSynced shouldBe true
        }

      } finally {
        thread.interrupt()
        thread.join(5000)
      }
    }
  }

  test("streaming Delta companion: restart resumes from last synced version") {
    withTempDirs { (deltaPath, indexPath) =>
      val ss = spark; import ss.implicits._

      // Write initial Delta table (version 0, 2 rows).
      Seq((1, "alice"), (2, "bob"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(deltaPath)

      val command = makeDeltaCommand(deltaPath, indexPath)

      // Run one sync directly (not via streaming) so the companion has exactly 2 rows
      // and the persisted lastSyncedVersion is set to version 0.  This avoids the
      // timing race of interrupting a streaming thread at the right moment.
      command.run(spark)
      countCompanionRows(indexPath) shouldBe 2

      // Append charlie (Delta version 1) — the direct sync didn't cover this.
      Seq((3, "charlie"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("append")
        .save(deltaPath)

      // Start a streaming session.  readLastSyncedVersionFromLog reads version 0 from the
      // companion log, so the session resumes incrementally rather than re-indexing
      // the 2 already-indexed rows.
      val streamingCommand = command.copy(streamingPollIntervalMs = Some(2000L))
      val thread           = new Thread(() => streamingCommand.run(spark))
      thread.setDaemon(true)
      thread.start()

      try {
        val resumeSynced = waitUntil(30000)(countCompanionRows(indexPath) == 3)
        withClue("streaming resume should index the missing commit (charlie) within 30 s") {
          resumeSynced shouldBe true
        }
      } finally {
        thread.interrupt()
        thread.join(5000)
      }
    }
  }
}
