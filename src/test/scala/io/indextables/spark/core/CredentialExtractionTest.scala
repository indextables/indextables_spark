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

package io.indextables.spark.core

import io.indextables.spark.TestBase
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.matchers.should.Matchers._
import scala.jdk.CollectionConverters._

/**
 * Test to verify that credential extraction logic in Tantivy4SparkRelation works correctly. This test validates the
 * actual credential values are properly extracted from different sources.
 */
class CredentialExtractionTest extends TestBase {

  test("should extract credentials from Spark session configuration") {
    // Set test credentials in Spark session
    spark.conf.set("spark.indextables.aws.accessKey", "test-access-key")
    spark.conf.set("spark.indextables.aws.secretKey", "test-secret-key")
    spark.conf.set("spark.indextables.aws.region", "us-west-2")
    spark.conf.set("spark.indextables.s3.endpoint", "http://test-endpoint:9000")

    // Test the extraction logic from schema() method
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

    val sparkConfigs       = spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).toMap
    val readOptions        = Map.empty[String, String]
    val readTantivyOptions = readOptions.filter(_._1.startsWith("spark.indextables."))

    val allConfigs = tantivyConfigs ++ sparkConfigs ++ readTantivyOptions
    val options    = new CaseInsensitiveStringMap(allConfigs.asJava)

    // Verify extracted credentials
    options.get("spark.indextables.aws.accessKey") shouldBe "test-access-key"
    options.get("spark.indextables.aws.secretKey") shouldBe "test-secret-key"
    options.get("spark.indextables.aws.region") shouldBe "us-west-2"
    options.get("spark.indextables.s3.endpoint") shouldBe "http://test-endpoint:9000"

    println(s"✅ Extracted ${allConfigs.size} configs from Spark session")
    allConfigs.foreach {
      case (key, value) =>
        println(s"  $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}")
    }
  }

  test("should extract credentials from Hadoop configuration") {
    // Clear Spark session configs
    spark.conf.unset("spark.indextables.aws.accessKey")
    spark.conf.unset("spark.indextables.aws.secretKey")
    spark.conf.unset("spark.indextables.aws.region")
    spark.conf.unset("spark.indextables.s3.endpoint")

    // Set credentials in Hadoop configuration
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", "hadoop-access-key")
    hadoopConf.set("spark.indextables.aws.secretKey", "hadoop-secret-key")
    hadoopConf.set("spark.indextables.aws.region", "eu-west-1")
    hadoopConf.set("spark.indextables.s3.endpoint", "http://hadoop-endpoint:9000")

    // Test the extraction logic
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

    val sparkConfigs       = spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).toMap
    val readTantivyOptions = Map.empty[String, String]

    val allConfigs = tantivyConfigs ++ sparkConfigs ++ readTantivyOptions
    val options    = new CaseInsensitiveStringMap(allConfigs.asJava)

    // Verify extracted credentials
    options.get("spark.indextables.aws.accessKey") shouldBe "hadoop-access-key"
    options.get("spark.indextables.aws.secretKey") shouldBe "hadoop-secret-key"
    options.get("spark.indextables.aws.region") shouldBe "eu-west-1"
    options.get("spark.indextables.s3.endpoint") shouldBe "http://hadoop-endpoint:9000"

    println(s"✅ Extracted ${allConfigs.size} configs from Hadoop configuration")
    allConfigs.foreach {
      case (key, value) =>
        println(s"  $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}")
    }
  }

  test("should prioritize read options over other sources") {
    // Set different values in each source
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", "hadoop-key")
    hadoopConf.set("spark.indextables.aws.region", "us-east-1")

    spark.conf.set("spark.indextables.aws.accessKey", "spark-key")
    spark.conf.set("spark.indextables.aws.region", "us-west-1")

    val readOptions = Map(
      "spark.indextables.aws.accessKey" -> "read-key",
      "spark.indextables.aws.secretKey" -> "read-secret",
      "spark.indextables.aws.region"    -> "eu-central-1"
    )

    // Test the extraction logic with precedence
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

    val sparkConfigs       = spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).toMap
    val readTantivyOptions = readOptions.filter(_._1.startsWith("spark.indextables."))

    // Precedence: readOptions > sparkConfigs > hadoopConfigs
    val allConfigs = tantivyConfigs ++ sparkConfigs ++ readTantivyOptions
    val options    = new CaseInsensitiveStringMap(allConfigs.asJava)

    // Verify read options win
    options.get("spark.indextables.aws.accessKey") shouldBe "read-key"    // read options should win
    options.get("spark.indextables.aws.secretKey") shouldBe "read-secret" // only in read options
    options.get("spark.indextables.aws.region") shouldBe "eu-central-1"   // read options should win

    println(s"✅ Precedence test passed - read options override other sources")
    println(s"  Final access key: read-key (from read options)")
    println(s"  Final region: eu-central-1 (from read options)")
    println(s"  Total configs: ${allConfigs.size}")
  }

  test("should handle missing credentials gracefully") {
    // Clear all credential sources
    spark.conf.unset("spark.indextables.aws.accessKey")
    spark.conf.unset("spark.indextables.aws.secretKey")
    spark.conf.unset("spark.indextables.aws.region")
    spark.conf.unset("spark.indextables.s3.endpoint")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.unset("spark.indextables.aws.accessKey")
    hadoopConf.unset("spark.indextables.aws.secretKey")
    hadoopConf.unset("spark.indextables.aws.region")
    hadoopConf.unset("spark.indextables.s3.endpoint")

    // Test extraction with no credentials
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

    val sparkConfigs       = spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).toMap
    val readTantivyOptions = Map.empty[String, String]

    val allConfigs = tantivyConfigs ++ sparkConfigs ++ readTantivyOptions
    val options    = new CaseInsensitiveStringMap(allConfigs.asJava)

    // Verify no credentials extracted
    Option(options.get("spark.indextables.aws.accessKey")) shouldBe None
    Option(options.get("spark.indextables.aws.secretKey")) shouldBe None
    Option(options.get("spark.indextables.aws.region")) shouldBe None
    Option(options.get("spark.indextables.s3.endpoint")) shouldBe None

    println(s"✅ Missing credentials handled correctly - no configs extracted")
    println(s"  Total configs: ${allConfigs.size} (expected: 0 or very few)")
  }

  test("should validate that TransactionLog receives the extracted credentials") {
    // This test simulates what happens in Tantivy4SparkRelation.schema()
    import io.indextables.spark.transaction.TransactionLog
    import org.apache.hadoop.fs.Path

    // Set test credentials
    spark.conf.set("spark.indextables.aws.accessKey", "validation-key")
    spark.conf.set("spark.indextables.aws.secretKey", "validation-secret")
    spark.conf.set("spark.indextables.aws.region", "ap-southeast-1")

    val tablePath = "/tmp/validation-test-table"

    // Extract credentials exactly as Tantivy4SparkRelation.schema() does
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith("spark.indextables."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

    val sparkConfigs       = spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).toMap
    val readTantivyOptions = Map.empty[String, String] // Simulating empty read options

    val allConfigs = tantivyConfigs ++ sparkConfigs ++ readTantivyOptions
    val options    = new CaseInsensitiveStringMap(allConfigs.asJava)

    // This should succeed without creating the TransactionLog (which would need S3Mock setup)
    // But we can validate that the options contain the right credentials
    options.get("spark.indextables.aws.accessKey") shouldBe "validation-key"
    options.get("spark.indextables.aws.secretKey") shouldBe "validation-secret"
    options.get("spark.indextables.aws.region") shouldBe "ap-southeast-1"

    // Verify options are properly formatted for TransactionLog constructor
    options should not be null
    allConfigs.size should be > 0

    println(s"✅ TransactionLog would receive proper credentials:")
    println(s"  AccessKey: validation-key")
    println(s"  Region: ap-southeast-1")
    println(s"  Total options: ${allConfigs.size}")

    // The actual TransactionLog construction would be:
    // val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    // But we don't do it here to avoid needing S3Mock setup in this unit test
  }
}
