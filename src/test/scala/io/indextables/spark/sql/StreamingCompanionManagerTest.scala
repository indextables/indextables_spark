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

import java.nio.file.Files

import io.indextables.spark.TestBase

/**
 * Tests for StreamingCompanionManager lifecycle and interruption behavior.
 */
class StreamingCompanionManagerTest extends TestBase {

  /** Minimal valid SyncToExternalCommand pointing at local temp paths. */
  private def makeCommand(sourcePath: String, destPath: String): SyncToExternalCommand =
    SyncToExternalCommand(
      sourceFormat = "parquet",
      sourcePath = sourcePath,
      destPath = destPath,
      indexingModes = Map.empty,
      fastFieldMode = "HYBRID",
      targetInputSize = None,
      dryRun = false
    )

  test("runStreaming stops cleanly when thread is interrupted during poll sleep") {
    val sourceDir = Files.createTempDirectory("streaming-source").toString
    val indexDir  = Files.createTempDirectory("streaming-index").toString

    // Long poll interval so the thread spends most of its time in Thread.sleep,
    // giving the interrupt a reliable target.
    val manager = new StreamingCompanionManager(makeCommand(sourceDir, indexDir), pollIntervalMs = 60000L)
    val thread  = new Thread(() => manager.runStreaming(spark))
    thread.setDaemon(true)
    thread.start()

    // The first sync cycle fails immediately (no parquet files in the empty source dir),
    // increments the error counter, then enters the 60-second poll sleep.
    // We wait 10 seconds to ensure the cycle has definitely completed — including any
    // Spark/JVM initialization overhead on a cold session — before interrupting so that
    // the interrupt reliably lands in Thread.sleep rather than inside Spark internals
    // (where it could be silently consumed).
    Thread.sleep(10000)
    thread.interrupt()

    // Once interrupted during the poll sleep the thread exits almost immediately.
    thread.join(5000L)
    thread.isAlive shouldBe false
  }

  test("runStreaming returns Seq.empty via SyncToExternalCommand.run when streamingPollIntervalMs is set") {
    val sourceDir = Files.createTempDirectory("streaming-source2").toString
    val indexDir  = Files.createTempDirectory("streaming-index2").toString

    val command = makeCommand(sourceDir, indexDir).copy(streamingPollIntervalMs = Some(60000L))
    var result: Seq[org.apache.spark.sql.Row] = null

    val thread = new Thread(() => {
      result = command.run(spark)
    })
    thread.setDaemon(true)
    thread.start()

    Thread.sleep(2000)
    thread.interrupt()
    thread.join(5000L)

    thread.isAlive shouldBe false
    // After the thread is joined, result should have been set to Seq.empty
    result shouldBe Seq.empty
  }
}
