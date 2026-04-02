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

package io.indextables.spark.transaction

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConfigMapperTest extends AnyFunSuite with Matchers {

  // ------------------------------------------------------------------------------------
  // Helper to build a CaseInsensitiveStringMap from Scala pairs
  // ------------------------------------------------------------------------------------
  private def csMap(entries: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(entries.toMap.asJava)

  // ------------------------------------------------------------------------------------
  // Both overloads should produce identical output
  // ------------------------------------------------------------------------------------
  test("both overloads produce identical output for the same input") {
    val input = Map(
      "spark.indextables.aws.accessKey"                         -> "AKID",
      "spark.indextables.aws.secretKey"                         -> "SECRET",
      "spark.indextables.aws.region"                            -> "us-west-2",
      "spark.indextables.azure.accountName"                     -> "myaccount",
      "spark.indextables.transaction.cache.enabled"             -> "false",
      "spark.indextables.transaction.cache.ttl.ms"              -> "60000",
      "spark.indextables.transaction.cache.version.ttl.ms"      -> "120000",
      "spark.indextables.transaction.cache.snapshot.ttl.ms"     -> "240000",
      "spark.indextables.transaction.cache.fileList.ttl.ms"     -> "30000",
      "spark.indextables.transaction.cache.metadata.ttl.ms"     -> "900000",
      "spark.indextables.transaction.cache.version.capacity"    -> "500",
      "spark.indextables.transaction.cache.snapshot.capacity"   -> "50",
      "spark.indextables.transaction.cache.fileList.capacity"   -> "25",
      "spark.indextables.transaction.maxConcurrentReads"        -> "48",
      "spark.indextables.checkpoint.interval"                   -> "5"
    )

    val fromCsMap   = ConfigMapper.toNativeConfig(csMap(input.toSeq: _*))
    val fromScalaMap = ConfigMapper.toNativeConfig(input)

    fromCsMap.asScala.toMap shouldBe fromScalaMap.asScala.toMap
  }

  // ------------------------------------------------------------------------------------
  // AWS credential mapping
  // ------------------------------------------------------------------------------------
  test("AWS credentials are mapped correctly") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.aws.accessKey"     -> "AKID",
      "spark.indextables.aws.secretKey"     -> "SECRET",
      "spark.indextables.aws.sessionToken"  -> "TOKEN",
      "spark.indextables.aws.region"        -> "eu-west-1",
      "spark.indextables.aws.endpoint"      -> "http://localhost:9000",
      "spark.indextables.aws.forcePathStyle" -> "true"
    ))

    config.get("aws_access_key_id")     shouldBe "AKID"
    config.get("aws_secret_access_key") shouldBe "SECRET"
    config.get("aws_session_token")     shouldBe "TOKEN"
    config.get("aws_region")            shouldBe "eu-west-1"
    config.get("aws_endpoint")          shouldBe "http://localhost:9000"
    config.get("aws_force_path_style")  shouldBe "true"
  }

  // ------------------------------------------------------------------------------------
  // S3 alternate keys with priority
  // ------------------------------------------------------------------------------------
  test("S3 alternate endpoint does not override primary AWS endpoint") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.aws.endpoint" -> "http://primary:9000",
      "spark.indextables.s3.endpoint"  -> "http://alternate:9000"
    ))

    config.get("aws_endpoint") shouldBe "http://primary:9000"
  }

  test("S3 alternate endpoint is used when primary AWS endpoint is absent") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.s3.endpoint" -> "http://alternate:9000"
    ))

    config.get("aws_endpoint") shouldBe "http://alternate:9000"
  }

  test("S3 pathStyleAccess sets aws_force_path_style when true") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.s3.pathStyleAccess" -> "true"
    ))

    config.get("aws_force_path_style") shouldBe "true"
  }

  test("S3 pathStyleAccess does not set aws_force_path_style when false") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.s3.pathStyleAccess" -> "false"
    ))

    config.containsKey("aws_force_path_style") shouldBe false
  }

  // ------------------------------------------------------------------------------------
  // Azure credential mapping
  // ------------------------------------------------------------------------------------
  test("Azure credentials are mapped correctly") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.azure.accountName" -> "myaccount",
      "spark.indextables.azure.accountKey"  -> "mykey",
      "spark.indextables.azure.bearerToken" -> "mytoken"
    ))

    config.get("azure_account_name") shouldBe "myaccount"
    config.get("azure_access_key")   shouldBe "mykey"
    config.get("azure_bearer_token") shouldBe "mytoken"
  }

  // ------------------------------------------------------------------------------------
  // Transaction log cache configuration
  // ------------------------------------------------------------------------------------
  test("cache configuration keys are mapped with correct native names") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.transaction.cache.enabled"            -> "false",
      "spark.indextables.transaction.cache.ttl.ms"             -> "60000",
      "spark.indextables.transaction.cache.version.ttl.ms"     -> "120000",
      "spark.indextables.transaction.cache.snapshot.ttl.ms"    -> "240000",
      "spark.indextables.transaction.cache.fileList.ttl.ms"    -> "30000",
      "spark.indextables.transaction.cache.metadata.ttl.ms"    -> "900000",
      "spark.indextables.transaction.cache.version.capacity"   -> "500",
      "spark.indextables.transaction.cache.snapshot.capacity"   -> "50",
      "spark.indextables.transaction.cache.fileList.capacity"  -> "25"
    ))

    config.get("cache.enabled")           shouldBe "false"
    config.get("cache.ttl.ms")            shouldBe "60000"
    config.get("cache.version.ttl.ms")    shouldBe "120000"
    config.get("cache.snapshot.ttl.ms")   shouldBe "240000"
    config.get("cache.file_list.ttl.ms")  shouldBe "30000"
    config.get("cache.metadata.ttl.ms")   shouldBe "900000"
    config.get("cache.version.capacity")  shouldBe "500"
    config.get("cache.snapshot.capacity") shouldBe "50"
    config.get("cache.file_list.capacity") shouldBe "25"
  }

  // ------------------------------------------------------------------------------------
  // Concurrency and checkpoint
  // ------------------------------------------------------------------------------------
  test("maxConcurrentReads is mapped correctly") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.transaction.maxConcurrentReads" -> "48"
    ))

    config.get("max_concurrent_reads") shouldBe "48"
  }

  test("checkpoint interval is mapped correctly") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.checkpoint.interval" -> "5"
    ))

    config.get("checkpoint_interval") shouldBe "5"
  }

  // ------------------------------------------------------------------------------------
  // Empty / missing keys
  // ------------------------------------------------------------------------------------
  test("empty options produce empty config") {
    val config = ConfigMapper.toNativeConfig(Map.empty[String, String])
    config.size() shouldBe 0
  }

  test("unrelated keys are not included in native config") {
    val config = ConfigMapper.toNativeConfig(Map(
      "spark.indextables.indexWriter.heapSize" -> "200M",
      "spark.sql.shuffle.partitions"           -> "100"
    ))

    config.size() shouldBe 0
  }

  // ------------------------------------------------------------------------------------
  // CaseInsensitiveStringMap overload — case insensitivity
  // ------------------------------------------------------------------------------------
  test("CaseInsensitiveStringMap overload handles case-insensitive keys") {
    val config = ConfigMapper.toNativeConfig(csMap(
      "SPARK.INDEXTABLES.AWS.ACCESSKEY" -> "AKID",
      "Spark.Indextables.Aws.Region"    -> "us-east-1"
    ))

    config.get("aws_access_key_id") shouldBe "AKID"
    config.get("aws_region")        shouldBe "us-east-1"
  }
}
