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

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.Properties

import scala.util.Using

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

    // Load AWS credentials and configure Spark + Hadoop for native transaction log
    loadAndConfigureAwsCredentials()

    println(s"CloudS3TestBase: Spark session created for real AWS S3 testing")
  }

  /**
   * Load AWS credentials from ~/.aws/credentials and set them in Spark and Hadoop config. The native transaction log
   * requires explicit credential configuration via spark.indextables.aws.* keys (it does not auto-resolve from Hadoop's
   * filesystem API).
   */
  private def loadAndConfigureAwsCredentials(): Unit =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile))(fis => props.load(fis))

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          // Set credentials in Spark conf (used by ConfigNormalization)
          spark.conf.set("spark.indextables.aws.accessKey", accessKey)
          spark.conf.set("spark.indextables.aws.secretKey", secretKey)
          spark.conf.set("spark.indextables.aws.region", "us-east-2")

          // Also set in Hadoop conf (used by ConfigNormalization.extractTantivyConfigsFromHadoop)
          val hadoopConf = spark.sparkContext.hadoopConfiguration
          hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
          hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
          hadoopConf.set("spark.indextables.aws.region", "us-east-2")

          println(s"CloudS3TestBase: AWS credentials loaded from ~/.aws/credentials")
        }
      }
    } catch {
      case _: Exception => // Ignore if credential loading fails
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
