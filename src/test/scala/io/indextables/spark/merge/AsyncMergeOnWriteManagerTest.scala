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

package io.indextables.spark.merge

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class AsyncMergeOnWriteManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit =
    // Reset manager before each test for isolation
    AsyncMergeOnWriteManager.reset()

  override def afterEach(): Unit =
    // Clean up after each test
    AsyncMergeOnWriteManager.reset()

  test("configure initializes manager with custom settings") {
    val config = AsyncMergeOnWriteConfig(
      maxConcurrentBatches = 5,
      shutdownTimeoutMs = 60000L
    )

    AsyncMergeOnWriteManager.configure(config)

    // Verify configuration by checking available slots
    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 5
  }

  test("configure is idempotent with same settings") {
    val config = AsyncMergeOnWriteConfig(maxConcurrentBatches = 3)

    AsyncMergeOnWriteManager.configure(config)
    AsyncMergeOnWriteManager.configure(config) // Should not throw

    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 3
  }

  test("isMergeInProgress returns false for unknown table") {
    AsyncMergeOnWriteManager.isMergeInProgress("s3://bucket/unknown-table") shouldBe false
  }

  test("getActiveJobCount returns 0 initially") {
    AsyncMergeOnWriteManager.getActiveJobCount shouldBe 0
  }

  test("getAvailableBatchSlots returns configured value") {
    val config = AsyncMergeOnWriteConfig(maxConcurrentBatches = 7)
    AsyncMergeOnWriteManager.configure(config)

    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 7
  }

  test("getAllJobStatus returns empty initially") {
    AsyncMergeOnWriteManager.getAllJobStatus shouldBe empty
  }

  test("getActiveJob returns None for unknown job") {
    AsyncMergeOnWriteManager.getActiveJob("unknown-job-id") shouldBe None
  }

  test("getCompletedJob returns None for unknown job") {
    AsyncMergeOnWriteManager.getCompletedJob("unknown-job-id") shouldBe None
  }

  test("reset clears all state") {
    val config = AsyncMergeOnWriteConfig(maxConcurrentBatches = 10)
    AsyncMergeOnWriteManager.configure(config)

    // Slots should be 10 after configure
    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 10

    // Reset should restore to defaults
    AsyncMergeOnWriteManager.reset()

    // After reset, it should use default (3) when ensureInitialized is called
    // getAvailableBatchSlots triggers ensureInitialized
    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 3
  }

  test("awaitCompletion returns true for unknown table (no job to wait for)") {
    AsyncMergeOnWriteManager.awaitCompletion("s3://bucket/unknown-table", 100L) shouldBe true
  }

  test("shutdown completes gracefully when no jobs") {
    AsyncMergeOnWriteManager.configure(AsyncMergeOnWriteConfig.default)
    AsyncMergeOnWriteManager.shutdown(1000L) shouldBe true
  }

  test("MergeJobStatus toString values") {
    MergeJobStatus.Running.toString shouldBe "RUNNING"
    MergeJobStatus.Completed.toString shouldBe "COMPLETED"
    MergeJobStatus.Failed.toString shouldBe "FAILED"
    MergeJobStatus.Cancelled.toString shouldBe "CANCELLED"
  }

  test("MergeJobStatusReport is serializable") {
    val report = MergeJobStatus.MergeJobStatusReport(
      jobId = "test-job",
      tablePath = "s3://bucket/table",
      status = "RUNNING",
      totalGroups = 10,
      completedGroups = 5,
      totalBatches = 3,
      completedBatches = 2,
      durationMs = 1000L,
      errorMessage = None
    )

    // Test that it can be serialized (basic check)
    noException should be thrownBy {
      import java.io._
      val baos = new ByteArrayOutputStream()
      val oos  = new ObjectOutputStream(baos)
      oos.writeObject(report)
      oos.close()

      val bais         = new ByteArrayInputStream(baos.toByteArray)
      val ois          = new ObjectInputStream(bais)
      val deserialized = ois.readObject().asInstanceOf[MergeJobStatus.MergeJobStatusReport]
      ois.close()

      deserialized.jobId shouldBe "test-job"
      deserialized.tablePath shouldBe "s3://bucket/table"
    }
  }

  test("AsyncMergeJob tracks progress via atomic fields") {
    import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

    val job = AsyncMergeJob(
      jobId = "test-job",
      tablePath = "s3://bucket/table",
      startTime = System.currentTimeMillis(),
      totalMergeGroups = 100,
      totalBatches = 10,
      batchSize = 10,
      completedGroups = new AtomicInteger(0),
      completedBatches = new AtomicInteger(0),
      status = new AtomicReference[MergeJobStatus](MergeJobStatus.Running)
    )

    job.completedGroups.get() shouldBe 0
    job.completedBatches.get() shouldBe 0
    job.status.get() shouldBe MergeJobStatus.Running

    // Simulate progress updates
    job.completedGroups.set(50)
    job.completedBatches.set(5)
    job.status.set(MergeJobStatus.Completed)

    job.completedGroups.get() shouldBe 50
    job.completedBatches.get() shouldBe 5
    job.status.get() shouldBe MergeJobStatus.Completed
  }

  test("AsyncMergeJobResult has all expected fields") {
    val result = AsyncMergeJobResult(
      jobId = "test-job",
      tablePath = "s3://bucket/table",
      totalMergeGroups = 100,
      completedGroups = 100,
      totalBatches = 10,
      completedBatches = 10,
      durationMs = 5000L,
      success = true,
      errorMessage = None,
      completionTime = Some(System.currentTimeMillis())
    )

    result.success shouldBe true
    result.errorMessage shouldBe None
    result.completionTime.isDefined shouldBe true
  }

  test("AsyncMergeJobResult with error") {
    val result = AsyncMergeJobResult(
      jobId = "failed-job",
      tablePath = "s3://bucket/table",
      totalMergeGroups = 100,
      completedGroups = 30,
      totalBatches = 10,
      completedBatches = 3,
      durationMs = 2000L,
      success = false,
      errorMessage = Some("Connection timeout"),
      completionTime = Some(System.currentTimeMillis())
    )

    result.success shouldBe false
    result.errorMessage shouldBe Some("Connection timeout")
    result.completedGroups shouldBe 30 // Partial completion before failure
  }
}
