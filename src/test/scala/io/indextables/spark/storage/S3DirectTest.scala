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

package io.indextables.spark.storage

import java.net.ServerSocket

import scala.util.Using

import io.findify.s3mock.S3Mock
import io.indextables.spark.io.{CloudStorageConfig, S3CloudStorageProvider}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterAll

/**
 * Direct test of S3 cloud storage provider using S3Mock. Tests the cloud storage abstraction without going through
 * DataFrame APIs.
 */
class S3DirectTest extends TestBase with BeforeAndAfterAll {

  private val TEST_BUCKET                        = "test-tantivy-bucket"
  private val ACCESS_KEY                         = "test-access-key"
  private val SECRET_KEY                         = "test-secret-key"
  private var s3MockPort: Int                    = _
  private var s3Mock: S3Mock                     = _
  private var s3Provider: S3CloudStorageProvider = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Find available port
    s3MockPort = findAvailablePort()

    // Start S3Mock server
    s3Mock = S3Mock(port = s3MockPort, dir = "/tmp/s3")
    s3Mock.start

    // Create S3 provider with mock configuration
    val config = CloudStorageConfig(
      awsAccessKey = Some(ACCESS_KEY),
      awsSecretKey = Some(SECRET_KEY),
      awsEndpoint = Some(s"http://localhost:$s3MockPort"),
      awsRegion = Some("us-east-1"),
      awsPathStyleAccess = true
    )

    s3Provider = new S3CloudStorageProvider(config)

    println(s"✅ S3Mock server started on port $s3MockPort")
  }

  override def afterAll(): Unit = {
    if (s3Provider != null) {
      s3Provider.close()
    }
    if (s3Mock != null) {
      s3Mock.stop
    }
    super.afterAll()
  }

  private def findAvailablePort(): Int =
    Using.resource(new ServerSocket(0))(socket => socket.getLocalPort)

  test("should write and read files directly to S3") {
    val testPath    = s"s3://$TEST_BUCKET/test-file.txt"
    val testContent = "Hello from S3 cloud storage!"

    // Create bucket first (S3Mock requires this)
    s3Provider.createDirectory(s"s3://$TEST_BUCKET/")

    // Write file
    s3Provider.writeFile(testPath, testContent.getBytes("UTF-8"))

    // Check existence
    s3Provider.exists(testPath) shouldBe true

    // Read file
    val readContent = new String(s3Provider.readFile(testPath), "UTF-8")
    readContent shouldBe testContent

    // Get file info
    val fileInfo = s3Provider.getFileInfo(testPath)
    fileInfo should not be empty
    fileInfo.get.size shouldBe testContent.length

    println(s"✅ Successfully wrote and read file from S3: $testPath")
  }

  test("should handle parallel file operations") {
    val numFiles = 10
    val paths    = (1 to numFiles).map(i => s"s3://$TEST_BUCKET/parallel/file-$i.txt")

    // Write files in parallel
    paths.zipWithIndex.foreach {
      case (path, i) =>
        val content = s"Content for file $i"
        s3Provider.writeFile(path, content.getBytes("UTF-8"))
    }

    // Read files in parallel
    val contents = s3Provider.readFilesParallel(paths)
    contents.size shouldBe numFiles

    // Verify contents
    paths.zipWithIndex.foreach {
      case (path, i) =>
        val expectedContent = s"Content for file $i"
        val actualContent   = new String(contents(path), "UTF-8")
        actualContent shouldBe expectedContent
    }

    // Check existence in parallel
    val existsMap = s3Provider.existsParallel(paths)
    existsMap.values.forall(_ == true) shouldBe true

    println(s"✅ Successfully handled $numFiles parallel file operations")
  }

  test("should list files in S3 bucket") {
    val basePath = s"s3://$TEST_BUCKET/listing/"

    // Create some test files
    (1 to 5).foreach { i =>
      val path = s"${basePath}file-$i.txt"
      s3Provider.writeFile(path, s"File $i content".getBytes("UTF-8"))
    }

    // List files
    val files = s3Provider.listFiles(s"s3://$TEST_BUCKET/listing", recursive = true)
    files.size should be >= 5

    files.foreach(file => println(s"  Found file: ${file.path} (${file.size} bytes)"))

    println(s"✅ Successfully listed ${files.size} files in S3 bucket")
  }
}
