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

package io.indextables.spark

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Base class for cloud AWS S3 integration tests.
 *
 * This base class provides Spark session setup without any mock S3 configuration, allowing tests to connect to real AWS
 * S3 using standard AWS credentials.
 *
 * Unlike TestBase, this class does NOT set any S3Mock endpoints or configurations that would interfere with real AWS S3
 * connections.
 */
abstract class CloudS3TestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear global split cache before each test to avoid schema pollution
    try {
      import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch {
      case _: Exception => // Ignore if cache clearing fails
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create Spark session without any mock S3 configuration
    spark = SparkSession
      .builder()
      .appName("CloudS3IntegrationTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      // Enable IndexTables4Spark extensions for IndexQuery support
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Ensure we don't inherit any mock configurations from system properties
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") // Use SSL for real S3
      // Configure driver binding for local testing
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    println(s"🚀 CloudS3TestBase: Spark session created for real AWS S3 testing")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      println(s"🛑 CloudS3TestBase: Spark session stopped")
    }
    super.afterAll()
  }

  /** Create a temporary directory for test data. */
  protected def withTempPath(f: File => Unit): Unit = {
    val tempDir = Files.createTempDirectory("tantivy4spark-test")
    try
      f(tempDir.toFile)
    finally {
      // Clean up temp directory
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir.toFile)
    }
  }

  /** Generate a unique test ID for avoiding conflicts in S3. */
  protected def generateTestId(): String =
    java.util.UUID.randomUUID().toString.substring(0, 8)

  /**
   * Check if real AWS credentials are available. This is a utility method for tests to check credential availability.
   */
  protected def hasAwsCredentials(): Boolean =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")
      credFile.exists()
    } catch {
      case _: Exception => false
    }
}
