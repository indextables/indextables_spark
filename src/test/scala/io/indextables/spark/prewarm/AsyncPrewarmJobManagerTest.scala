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

package io.indextables.spark.prewarm

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

/**
 * Unit tests for AsyncPrewarmJobManager.
 *
 * Tests the singleton's concurrency limiting, job tracking, progress tracking, and cleanup behavior.
 */
class AsyncPrewarmJobManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit =
    // Reset manager before each test
    AsyncPrewarmJobManager.reset()

  override def afterEach(): Unit =
    AsyncPrewarmJobManager.reset()

  test("configure should initialize manager with specified settings") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 3, completedJobRetentionMs = 60000L)

    AsyncPrewarmJobManager.getAvailableSlots shouldBe 3
    AsyncPrewarmJobManager.getActiveJobCount shouldBe 0
  }

  test("tryStartJob should start a job successfully") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val jobCompleted = new CountDownLatch(1)
    val result = AsyncPrewarmJobManager.tryStartJob(
      jobId = "test-job-1",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        jobCompleted.countDown()
        AsyncPrewarmJobResult(
          jobId = "test-job-1",
          tablePath = "s3://bucket/table",
          hostname = "host1",
          totalSplits = 10,
          splitsPrewarmed = 10,
          durationMs = 100,
          success = true,
          errorMessage = None
        )
      }
    )

    result.isRight shouldBe true
    result.toOption.get.jobId shouldBe "test-job-1"

    // Wait for job to complete
    jobCompleted.await(5, TimeUnit.SECONDS) shouldBe true
  }

  test("tryStartJob should reject when at max capacity") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val jobStarted     = new CountDownLatch(1)
    val jobCanComplete = new CountDownLatch(1)

    // Start first job (blocking)
    val result1 = AsyncPrewarmJobManager.tryStartJob(
      jobId = "job-1",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        jobStarted.countDown()
        jobCanComplete.await(10, TimeUnit.SECONDS)
        AsyncPrewarmJobResult("job-1", "s3://bucket/table", "host1", 10, 10, 100, true, None)
      }
    )
    result1.isRight shouldBe true

    // Wait for first job to actually start
    jobStarted.await(5, TimeUnit.SECONDS) shouldBe true

    // Try to start second job - should be rejected
    val result2 = AsyncPrewarmJobManager.tryStartJob(
      jobId = "job-2",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => AsyncPrewarmJobResult("job-2", "s3://bucket/table", "host1", 5, 5, 50, true, None)
    )

    result2.isLeft shouldBe true
    result2.left.toOption.get should include("capacity")

    // Let first job complete
    jobCanComplete.countDown()
  }

  test("tryStartJob should reject duplicate job IDs") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 2)

    val jobCompleted = new CountDownLatch(1)
    val result1 = AsyncPrewarmJobManager.tryStartJob(
      jobId = "duplicate-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        Thread.sleep(100)
        jobCompleted.countDown()
        AsyncPrewarmJobResult("duplicate-job", "s3://bucket/table", "host1", 10, 10, 100, true, None)
      }
    )
    result1.isRight shouldBe true

    // Try to start job with same ID
    val result2 = AsyncPrewarmJobManager.tryStartJob(
      jobId = "duplicate-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => AsyncPrewarmJobResult("duplicate-job", "s3://bucket/table", "host1", 5, 5, 50, true, None)
    )

    result2.isLeft shouldBe true
    result2.left.toOption.get should include("already exists")

    // Wait for job to complete
    jobCompleted.await(5, TimeUnit.SECONDS) shouldBe true
  }

  test("getActiveJob should return running jobs") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val jobStarted     = new CountDownLatch(1)
    val jobCanComplete = new CountDownLatch(1)

    AsyncPrewarmJobManager.tryStartJob(
      jobId = "active-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        jobStarted.countDown()
        jobCanComplete.await(10, TimeUnit.SECONDS)
        AsyncPrewarmJobResult("active-job", "s3://bucket/table", "host1", 10, 10, 100, true, None)
      }
    )

    jobStarted.await(5, TimeUnit.SECONDS) shouldBe true

    val activeJob = AsyncPrewarmJobManager.getActiveJob("active-job")
    activeJob.isDefined shouldBe true
    activeJob.get.jobId shouldBe "active-job"
    activeJob.get.totalSplits shouldBe 10

    jobCanComplete.countDown()
  }

  test("getAllJobStatus should return both active and completed jobs") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 2, completedJobRetentionMs = 60000L)

    val job1Completed   = new CountDownLatch(1)
    val job2Started     = new CountDownLatch(1)
    val job2CanComplete = new CountDownLatch(1)

    // Start first job (completes quickly)
    AsyncPrewarmJobManager.tryStartJob(
      jobId = "completed-job",
      tablePath = "s3://bucket/table1",
      hostname = "host1",
      totalSplits = 5,
      work = () => {
        job1Completed.countDown()
        AsyncPrewarmJobResult("completed-job", "s3://bucket/table1", "host1", 5, 5, 50, true, None)
      }
    )

    job1Completed.await(5, TimeUnit.SECONDS) shouldBe true
    Thread.sleep(200) // Give time for job to move to completed

    // Start second job (stays running)
    AsyncPrewarmJobManager.tryStartJob(
      jobId = "running-job",
      tablePath = "s3://bucket/table2",
      hostname = "host2",
      totalSplits = 10,
      work = () => {
        job2Started.countDown()
        job2CanComplete.await(10, TimeUnit.SECONDS)
        AsyncPrewarmJobResult("running-job", "s3://bucket/table2", "host2", 10, 10, 100, true, None)
      }
    )

    job2Started.await(5, TimeUnit.SECONDS) shouldBe true

    val allJobs = AsyncPrewarmJobManager.getAllJobStatus
    allJobs.size should be >= 2

    val completedJob = allJobs.find(_.jobId == "completed-job")
    completedJob.isDefined shouldBe true
    completedJob.get.status shouldBe "COMPLETED"

    val runningJob = allJobs.find(_.jobId == "running-job")
    runningJob.isDefined shouldBe true
    runningJob.get.status shouldBe "RUNNING"

    job2CanComplete.countDown()
  }

  test("incrementProgress should update job progress") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val progress     = new AtomicInteger(0)
    val jobCompleted = new CountDownLatch(1)

    AsyncPrewarmJobManager.tryStartJob(
      jobId = "progress-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        for (i <- 1 to 10) {
          val count = AsyncPrewarmJobManager.incrementProgress("progress-job")
          progress.set(count)
          Thread.sleep(10)
        }
        jobCompleted.countDown()
        AsyncPrewarmJobResult("progress-job", "s3://bucket/table", "host1", 10, 10, 100, true, None)
      }
    )

    jobCompleted.await(5, TimeUnit.SECONDS) shouldBe true
    progress.get() shouldBe 10
  }

  test("cancelJob should mark job as cancelled") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val jobStarted     = new CountDownLatch(1)
    val checkCancelled = new CountDownLatch(1)
    val wasCancelled   = new AtomicInteger(0)

    AsyncPrewarmJobManager.tryStartJob(
      jobId = "cancel-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 100,
      work = () => {
        jobStarted.countDown()
        checkCancelled.await(10, TimeUnit.SECONDS)
        if (AsyncPrewarmJobManager.isJobCancelled("cancel-job")) {
          wasCancelled.set(1)
        }
        AsyncPrewarmJobResult("cancel-job", "s3://bucket/table", "host1", 100, 0, 100, false, Some("Cancelled"))
      }
    )

    jobStarted.await(5, TimeUnit.SECONDS) shouldBe true

    val cancelled = AsyncPrewarmJobManager.cancelJob("cancel-job")
    cancelled shouldBe true

    checkCancelled.countDown()
    Thread.sleep(200)

    wasCancelled.get() shouldBe 1
  }

  test("isJobCancelled should return false for non-cancelled jobs") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val jobStarted     = new CountDownLatch(1)
    val jobCanComplete = new CountDownLatch(1)

    AsyncPrewarmJobManager.tryStartJob(
      jobId = "normal-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 10,
      work = () => {
        jobStarted.countDown()
        jobCanComplete.await(10, TimeUnit.SECONDS)
        AsyncPrewarmJobResult("normal-job", "s3://bucket/table", "host1", 10, 10, 100, true, None)
      }
    )

    jobStarted.await(5, TimeUnit.SECONDS) shouldBe true

    AsyncPrewarmJobManager.isJobCancelled("normal-job") shouldBe false
    AsyncPrewarmJobManager.isJobCancelled("nonexistent-job") shouldBe false

    jobCanComplete.countDown()
  }

  test("reset should clear all state") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 2)

    val jobCompleted = new CountDownLatch(1)
    AsyncPrewarmJobManager.tryStartJob(
      jobId = "reset-test-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => {
        jobCompleted.countDown()
        AsyncPrewarmJobResult("reset-test-job", "s3://bucket/table", "host1", 5, 5, 50, true, None)
      }
    )

    jobCompleted.await(5, TimeUnit.SECONDS) shouldBe true
    Thread.sleep(100)

    // Verify we have some state
    AsyncPrewarmJobManager.getAllJobStatus.nonEmpty shouldBe true

    // Reset
    AsyncPrewarmJobManager.reset()

    // Verify state is cleared
    AsyncPrewarmJobManager.getAllJobStatus.isEmpty shouldBe true
    AsyncPrewarmJobManager.getActiveJobCount shouldBe 0
  }

  test("completed jobs should be available for getCompletedJob") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1, completedJobRetentionMs = 60000L)

    val jobCompleted = new CountDownLatch(1)
    AsyncPrewarmJobManager.tryStartJob(
      jobId = "get-completed-job",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => {
        jobCompleted.countDown()
        AsyncPrewarmJobResult("get-completed-job", "s3://bucket/table", "host1", 5, 5, 50, true, None)
      }
    )

    jobCompleted.await(5, TimeUnit.SECONDS) shouldBe true
    Thread.sleep(200) // Give time for completion processing

    val completed = AsyncPrewarmJobManager.getCompletedJob("get-completed-job")
    completed.isDefined shouldBe true
    completed.get.success shouldBe true
    completed.get.splitsPrewarmed shouldBe 5
  }

  test("job slots should be released after completion") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val job1Completed = new CountDownLatch(1)
    val job2Completed = new CountDownLatch(1)

    // Start first job
    AsyncPrewarmJobManager.tryStartJob(
      jobId = "slot-test-1",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => {
        job1Completed.countDown()
        AsyncPrewarmJobResult("slot-test-1", "s3://bucket/table", "host1", 5, 5, 50, true, None)
      }
    )

    job1Completed.await(5, TimeUnit.SECONDS) shouldBe true
    Thread.sleep(200)

    // Second job should succeed (slot released)
    val result2 = AsyncPrewarmJobManager.tryStartJob(
      jobId = "slot-test-2",
      tablePath = "s3://bucket/table",
      hostname = "host1",
      totalSplits = 5,
      work = () => {
        job2Completed.countDown()
        AsyncPrewarmJobResult("slot-test-2", "s3://bucket/table", "host1", 5, 5, 50, true, None)
      }
    )

    result2.isRight shouldBe true
    job2Completed.await(5, TimeUnit.SECONDS) shouldBe true
  }
}
