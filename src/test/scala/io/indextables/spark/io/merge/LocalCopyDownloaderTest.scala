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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class LocalCopyDownloaderTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var tempDir: File = _

  override def beforeEach(): Unit =
    tempDir = Files.createTempDirectory("local-copy-test").toFile

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

  test("protocol returns file") {
    val downloader = new LocalCopyDownloader()
    downloader.protocol shouldBe "file"
    downloader.close()
  }

  test("canHandle returns true for local paths") {
    val downloader = new LocalCopyDownloader()

    downloader.canHandle("/path/to/file") shouldBe true
    downloader.canHandle("/tmp/test.split") shouldBe true
    downloader.canHandle("file:///path/to/file") shouldBe true

    downloader.close()
  }

  test("canHandle returns false for cloud paths") {
    val downloader = new LocalCopyDownloader()

    downloader.canHandle("s3://bucket/path") shouldBe false
    downloader.canHandle("s3a://bucket/path") shouldBe false
    downloader.canHandle("abfss://container@account.blob.core.windows.net/path") shouldBe false
    downloader.canHandle("wasbs://container@account.blob.core.windows.net/path") shouldBe false

    downloader.close()
  }

  test("downloadAsync copies file successfully") {
    val downloader = new LocalCopyDownloader()

    // Create source file
    val sourceFile = new File(tempDir, "source.txt")
    val content    = "Hello, World! This is test content for the local copy downloader."
    Files.writeString(sourceFile.toPath, content)

    // Create destination path
    val destFile = new File(tempDir, "dest.txt")

    val request = DownloadRequest(
      sourcePath = sourceFile.getAbsolutePath,
      destinationPath = destFile.getAbsolutePath,
      expectedSize = sourceFile.length(),
      batchId = 1,
      index = 0
    )

    val result = downloader.downloadAsync(request).get()

    result.success shouldBe true
    result.error shouldBe None
    result.bytesDownloaded shouldBe content.length
    result.localPath shouldBe destFile.getAbsolutePath
    result.request shouldBe request

    // Verify content was copied
    Files.readString(destFile.toPath) shouldBe content

    downloader.close()
  }

  test("downloadAsync creates parent directories") {
    val downloader = new LocalCopyDownloader()

    // Create source file
    val sourceFile = new File(tempDir, "source.txt")
    Files.writeString(sourceFile.toPath, "Test content")

    // Create destination with nested directories
    val destFile = new File(tempDir, "level1/level2/level3/dest.txt")

    val request = DownloadRequest(
      sourcePath = sourceFile.getAbsolutePath,
      destinationPath = destFile.getAbsolutePath,
      expectedSize = sourceFile.length(),
      batchId = 1,
      index = 0
    )

    val result = downloader.downloadAsync(request).get()

    result.success shouldBe true
    destFile.exists() shouldBe true
    Files.readString(destFile.toPath) shouldBe "Test content"

    downloader.close()
  }

  test("downloadAsync returns failure for missing source file") {
    val downloader = new LocalCopyDownloader()

    val request = DownloadRequest(
      sourcePath = "/nonexistent/path/to/file.txt",
      destinationPath = new File(tempDir, "dest.txt").getAbsolutePath,
      expectedSize = 100,
      batchId = 1,
      index = 0
    )

    val result = downloader.downloadAsync(request).get()

    result.success shouldBe false
    result.error shouldBe defined
    result.error.get.getMessage should include("Source file does not exist")

    downloader.close()
  }

  test("downloadAsync handles file:// scheme") {
    val downloader = new LocalCopyDownloader()

    // Create source file
    val sourceFile = new File(tempDir, "source.txt")
    Files.writeString(sourceFile.toPath, "File scheme test")

    // Use file:// scheme
    val request = DownloadRequest(
      sourcePath = s"file://${sourceFile.getAbsolutePath}",
      destinationPath = new File(tempDir, "dest.txt").getAbsolutePath,
      expectedSize = sourceFile.length(),
      batchId = 1,
      index = 0
    )

    val result = downloader.downloadAsync(request).get()

    result.success shouldBe true
    Files.readString(new File(result.localPath).toPath) shouldBe "File scheme test"

    downloader.close()
  }

  test("downloadAsync copies large files efficiently") {
    val downloader = new LocalCopyDownloader()

    // Create a larger source file (1MB)
    val sourceFile = new File(tempDir, "large.bin")
    val content    = Array.fill(1024 * 1024)(65.toByte) // 1MB of 'A'
    Files.write(sourceFile.toPath, content)

    val destFile = new File(tempDir, "large-copy.bin")

    val request = DownloadRequest(
      sourcePath = sourceFile.getAbsolutePath,
      destinationPath = destFile.getAbsolutePath,
      expectedSize = sourceFile.length(),
      batchId = 1,
      index = 0
    )

    val result = downloader.downloadAsync(request).get()

    result.success shouldBe true
    result.bytesDownloaded shouldBe content.length
    destFile.length() shouldBe content.length

    // Verify content
    val copied = Files.readAllBytes(destFile.toPath)
    copied should have length content.length
    copied.take(100) shouldBe content.take(100)

    downloader.close()
  }
}
