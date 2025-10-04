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

package io.indextables.spark.io

import java.util.{Collections => JCollections}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.TestBase

/** Test cloud storage provider configuration and factory */
class CloudStorageProviderTest extends TestBase {

  test("should extract S3 configuration from Spark config") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Set S3 configuration in Hadoop config (simulating Spark conf)
    hadoopConf.set("spark.indextables.aws.accessKey", "test-access-key")
    hadoopConf.set("spark.indextables.aws.secretKey", "test-secret-key")
    hadoopConf.set("spark.indextables.s3.endpoint", "http://localhost:9090")
    hadoopConf.set("spark.indextables.s3.pathStyleAccess", "true")
    hadoopConf.set("spark.indextables.aws.region", "us-west-2")

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())

    // Create provider - should pick up config from Hadoop conf
    val provider = CloudStorageProviderFactory.createProvider(
      "s3://test-bucket/path",
      options,
      hadoopConf
    )

    provider.getProviderType shouldBe "s3"

    // Clean up
    provider.close()

    println("✅ Successfully extracted S3 configuration from Spark config")
  }

  test("should prioritize DataFrame options over Spark config") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Set S3 configuration in Hadoop config
    hadoopConf.set("spark.indextables.aws.accessKey", "hadoop-access-key")
    hadoopConf.set("spark.indextables.aws.secretKey", "hadoop-secret-key")

    // Set different values in DataFrame options
    val optionsMap = new java.util.HashMap[String, String]()
    optionsMap.put("spark.indextables.aws.accessKey", "options-access-key")
    optionsMap.put("spark.indextables.aws.secretKey", "options-secret-key")
    optionsMap.put("spark.indextables.s3.endpoint", "http://localhost:8080")

    val options = new CaseInsensitiveStringMap(optionsMap)

    // Create provider - should prioritize options over Hadoop conf
    val provider = CloudStorageProviderFactory.createProvider(
      "s3://test-bucket/path",
      options,
      hadoopConf
    )

    provider.getProviderType shouldBe "s3"

    // Clean up
    provider.close()

    println("✅ Successfully prioritized DataFrame options over Spark config")
  }

  test("should create Hadoop provider for non-S3 paths") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val options    = new CaseInsensitiveStringMap(JCollections.emptyMap())

    // Test HDFS path
    val hdfsProvider = CloudStorageProviderFactory.createProvider(
      "hdfs://namenode/path",
      options,
      hadoopConf
    )
    hdfsProvider.getProviderType shouldBe "hadoop"
    hdfsProvider.close()

    // Test local file path
    val fileProvider = CloudStorageProviderFactory.createProvider(
      "file:///tmp/path",
      options,
      hadoopConf
    )
    fileProvider.getProviderType shouldBe "hadoop"
    fileProvider.close()

    // Test relative path
    val localProvider = CloudStorageProviderFactory.createProvider(
      "/tmp/path",
      options,
      hadoopConf
    )
    localProvider.getProviderType shouldBe "hadoop"
    localProvider.close()

    println("✅ Successfully created Hadoop providers for non-S3 paths")
  }

  test("should support parallel operations in Hadoop provider") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val options    = new CaseInsensitiveStringMap(JCollections.emptyMap())

    val provider = CloudStorageProviderFactory.createProvider(
      "/tmp",
      options,
      hadoopConf
    )

    // Create test files
    val testPaths = (1 to 3).map(i => s"/tmp/cloud-provider-test-$i.txt")

    testPaths.zipWithIndex.foreach {
      case (path, i) =>
        provider.writeFile(path, s"Test content $i".getBytes("UTF-8"))
    }

    // Test parallel read
    val contents = provider.readFilesParallel(testPaths)
    contents.size shouldBe 3

    testPaths.zipWithIndex.foreach {
      case (path, i) =>
        val content = new String(contents(path), "UTF-8")
        content shouldBe s"Test content $i"
    }

    // Test parallel existence check
    val existsMap = provider.existsParallel(testPaths)
    existsMap.values.forall(_ == true) shouldBe true

    // Clean up
    testPaths.foreach(provider.deleteFile)
    provider.close()

    println("✅ Successfully tested parallel operations in Hadoop provider")
  }
}
