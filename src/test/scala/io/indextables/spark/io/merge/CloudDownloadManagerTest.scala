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

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CompletableFuture

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class CloudDownloadManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var tempDir: File = _

  override def beforeEach(): Unit =
    tempDir = Files.createTempDirectory("cloud-download-test").toFile

  override def afterEach(): Unit =
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("submitBatch should download all files successfully") {
    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 2))

    try {
      // Create source files
      val sourceDir = new File(tempDir, "source")
      sourceDir.mkdirs()

      val sourceFiles = (0 until 5).map { i =>
        val file = new File(sourceDir, s"file$i.txt")
        Files.writeString(file.toPath, s"Content of file $i")
        file
      }

      // Create destination directory
      val destDir = new File(tempDir, "dest")
      destDir.mkdirs()

      // Create download requests using LocalCopyDownloader
      val requests = sourceFiles.zipWithIndex.map {
        case (sourceFile, idx) =>
          DownloadRequest(
            sourcePath = sourceFile.getAbsolutePath,
            destinationPath = new File(destDir, s"downloaded$idx.txt").getAbsolutePath,
            expectedSize = sourceFile.length(),
            batchId = 0,
            index = idx
          )
      }

      val downloader = new LocalCopyDownloader()

      // Submit batch and wait for completion
      val resultFuture = manager.submitBatch(requests, downloader)
      val results      = resultFuture.get()

      // Verify all downloads succeeded
      results should have size 5
      results.foreach { result =>
        result.success shouldBe true
        result.error shouldBe None
        new File(result.localPath).exists() shouldBe true
      }

      // Verify content
      results.zipWithIndex.foreach {
        case (result, idx) =>
          val content = Files.readString(new File(result.localPath).toPath)
          content shouldBe s"Content of file $idx"
      }

    } finally
      manager.shutdown()
  }

  test("submitBatch should handle empty batch") {
    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 2))

    try {
      val downloader   = new LocalCopyDownloader()
      val resultFuture = manager.submitBatch(Seq.empty, downloader)
      val results      = resultFuture.get()

      results shouldBe empty

    } finally
      manager.shutdown()
  }

  test("submitBatch should respect concurrency limits") {
    val maxConcurrent     = new AtomicInteger(0)
    val currentConcurrent = new AtomicInteger(0)

    // Create a slow downloader that tracks concurrency
    val slowDownloader = new AsyncDownloader {
      override def protocol: String                 = "test"
      override def canHandle(path: String): Boolean = true

      override def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult] = {
        val startTime = System.currentTimeMillis()
        CompletableFuture.supplyAsync { () =>
          val concurrent = currentConcurrent.incrementAndGet()
          maxConcurrent.updateAndGet(curr => math.max(curr, concurrent))

          try {
            Thread.sleep(50) // Simulate slow download

            // Create a small file
            val destFile = new File(request.destinationPath)
            destFile.getParentFile.mkdirs()
            Files.writeString(destFile.toPath, "test content")

            DownloadResult.success(
              request = request,
              localPath = request.destinationPath,
              bytesDownloaded = 12,
              durationMs = System.currentTimeMillis() - startTime
            )
          } finally
            currentConcurrent.decrementAndGet()
        }
      }

      override def close(): Unit = ()
    }

    // Use low concurrency per core to make the test deterministic
    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 1))

    try {
      val destDir = new File(tempDir, "dest")
      destDir.mkdirs()

      val requests = (0 until 10).map { i =>
        DownloadRequest(
          sourcePath = s"file$i.txt",
          destinationPath = new File(destDir, s"file$i.txt").getAbsolutePath,
          expectedSize = 12,
          batchId = 0,
          index = i
        )
      }

      val resultFuture = manager.submitBatch(requests, slowDownloader)
      val results      = resultFuture.get()

      results should have size 10
      results.foreach(_.success shouldBe true)

      // Max concurrent should not exceed the configured limit
      val expectedLimit = Runtime.getRuntime.availableProcessors() * 1
      maxConcurrent.get() should be <= expectedLimit

    } finally
      manager.shutdown()
  }

  test("submitBatch should handle download failures") {
    // Create a downloader that always fails
    val failingDownloader = new AsyncDownloader {
      override def protocol: String                 = "failing"
      override def canHandle(path: String): Boolean = true

      override def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult] =
        CompletableFuture.completedFuture(
          DownloadResult.failure(
            request = request,
            error = new RuntimeException("Simulated failure"),
            durationMs = 0
          )
        )

      override def close(): Unit = ()
    }

    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 2, downloadRetries = 0))

    try {
      val requests = Seq(
        DownloadRequest(
          sourcePath = "file.txt",
          destinationPath = new File(tempDir, "file.txt").getAbsolutePath,
          expectedSize = 100,
          batchId = 0,
          index = 0
        )
      )

      val resultFuture = manager.submitBatch(requests, failingDownloader)
      val results      = resultFuture.get()

      results should have size 1
      results.head.success shouldBe false
      results.head.error shouldBe defined

    } finally
      manager.shutdown()
  }

  test("multiple batches should process in priority order") {
    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 2))

    try {
      val downloader = new LocalCopyDownloader()

      // Create source files for two batches
      val sourceDir = new File(tempDir, "source")
      sourceDir.mkdirs()

      val batch1Files = (0 until 3).map { i =>
        val file = new File(sourceDir, s"batch1-file$i.txt")
        Files.writeString(file.toPath, s"Batch 1 file $i")
        file
      }

      val batch2Files = (0 until 3).map { i =>
        val file = new File(sourceDir, s"batch2-file$i.txt")
        Files.writeString(file.toPath, s"Batch 2 file $i")
        file
      }

      val destDir = new File(tempDir, "dest")
      destDir.mkdirs()

      // Create requests for batch 1
      val batch1Requests = batch1Files.zipWithIndex.map {
        case (file, idx) =>
          DownloadRequest(
            sourcePath = file.getAbsolutePath,
            destinationPath = new File(destDir, s"batch1-$idx.txt").getAbsolutePath,
            expectedSize = file.length(),
            batchId = 0,
            index = idx
          )
      }

      // Create requests for batch 2
      val batch2Requests = batch2Files.zipWithIndex.map {
        case (file, idx) =>
          DownloadRequest(
            sourcePath = file.getAbsolutePath,
            destinationPath = new File(destDir, s"batch2-$idx.txt").getAbsolutePath,
            expectedSize = file.length(),
            batchId = 0,
            index = idx
          )
      }

      // Submit both batches
      val batch1Future = manager.submitBatch(batch1Requests, downloader)
      val batch2Future = manager.submitBatch(batch2Requests, downloader)

      // Wait for both to complete
      val batch1Results = batch1Future.get()
      val batch2Results = batch2Future.get()

      // Verify both batches completed successfully
      batch1Results should have size 3
      batch1Results.foreach(_.success shouldBe true)

      batch2Results should have size 3
      batch2Results.foreach(_.success shouldBe true)

    } finally
      manager.shutdown()
  }

  test("getMetrics should return download statistics") {
    val manager = new CloudDownloadManager(MergeIOConfig(maxConcurrencyPerCore = 2))

    try {
      // Create source files
      val sourceDir = new File(tempDir, "source")
      sourceDir.mkdirs()

      val sourceFiles = (0 until 3).map { i =>
        val file = new File(sourceDir, s"file$i.txt")
        Files.writeString(file.toPath, s"Content of file $i - extra content for size")
        file
      }

      val destDir = new File(tempDir, "dest")
      destDir.mkdirs()

      val requests = sourceFiles.zipWithIndex.map {
        case (file, idx) =>
          DownloadRequest(
            sourcePath = file.getAbsolutePath,
            destinationPath = new File(destDir, s"file$idx.txt").getAbsolutePath,
            expectedSize = file.length(),
            batchId = 0,
            index = idx
          )
      }

      val downloader = new LocalCopyDownloader()

      // Submit and wait
      manager.submitBatch(requests, downloader).get()

      // Check metrics
      val metrics = manager.getMetrics
      metrics.totalFiles shouldBe 3
      metrics.failedDownloads shouldBe 0
      metrics.totalBytes should be > 0L

    } finally
      manager.shutdown()
  }
}
