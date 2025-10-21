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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration

import io.indextables.spark.testutils.{TestDualCredentialProvider, TestV1CredentialProvider, TestV2CredentialProvider}
import io.indextables.spark.utils.CredentialProviderFactory
import io.indextables.spark.TestBase
import org.slf4j.LoggerFactory

/**
 * Integration tests for custom AWS credential provider functionality Tests the complete flow from configuration to
 * S3CloudStorageProvider creation
 */
class CustomCredentialProviderIntegrationTest extends TestBase {

  private val logger = LoggerFactory.getLogger(classOf[CustomCredentialProviderIntegrationTest])

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear cache before each test
    CredentialProviderFactory.clearCache()
  }

  override def afterEach(): Unit = {
    // Clear cache after each test
    CredentialProviderFactory.clearCache()
    super.afterEach()
  }

  test("V1 provider integration should work end-to-end") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestV1CredentialProvider].getName,
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.s3.pathStyleAccess"           -> "true",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    hadoopConf.set("test.aws.accessKey", "V1_INTEGRATION_ACCESS")
    hadoopConf.set("test.aws.secretKey", "V1_INTEGRATION_SECRET")
    hadoopConf.set("test.aws.sessionToken", "V1_INTEGRATION_SESSION")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    // Test CloudStorageConfig extraction
    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    config.awsCredentialsProviderClass shouldBe Some(classOf[TestV1CredentialProvider].getName)
    config.awsEndpoint shouldBe Some("http://localhost:9090")
    config.awsRegion shouldBe Some("us-east-1")
    config.awsPathStyleAccess shouldBe true

    // Test S3CloudStorageProvider creation
    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    // Clean up
    provider.close()
  }

  test("V2 provider integration should work end-to-end") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestV2CredentialProvider].getName,
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.aws.region"                   -> "us-west-2"
    )

    val hadoopConf = new Configuration()
    hadoopConf.set("test.aws.accessKey", "V2_INTEGRATION_ACCESS")
    hadoopConf.set("test.aws.secretKey", "V2_INTEGRATION_SECRET")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    config.awsCredentialsProviderClass shouldBe Some(classOf[TestV2CredentialProvider].getName)
    config.awsEndpoint shouldBe Some("http://localhost:9090")
    config.awsRegion shouldBe Some("us-west-2")

    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    provider.close()
  }

  test("dual provider integration should work end-to-end") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestDualCredentialProvider].getName,
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.aws.region"                   -> "eu-west-1"
    )

    val hadoopConf = new Configuration()
    hadoopConf.set("test.aws.accessKey", "DUAL_INTEGRATION_ACCESS")
    hadoopConf.set("test.aws.secretKey", "DUAL_INTEGRATION_SECRET")
    hadoopConf.set("test.aws.sessionToken", "DUAL_INTEGRATION_SESSION")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    config.awsCredentialsProviderClass shouldBe Some(classOf[TestDualCredentialProvider].getName)

    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    provider.close()
  }

  test("configuration precedence should work correctly") {
    val options = Map(
      // Options should take precedence over Hadoop config
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestV1CredentialProvider].getName,
      "spark.indextables.aws.accessKey"                -> "OPTIONS_ACCESS",
      "spark.indextables.aws.secretKey"                -> "OPTIONS_SECRET",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    // These should be overridden by options
    hadoopConf.set("spark.indextables.aws.credentialsProviderClass", classOf[TestV2CredentialProvider].getName)
    hadoopConf.set("spark.indextables.aws.accessKey", "HADOOP_ACCESS")
    hadoopConf.set("spark.indextables.aws.secretKey", "HADOOP_SECRET")
    // Configure test credentials for the provider
    hadoopConf.set("test.aws.accessKey", "OPTIONS_ACCESS")
    hadoopConf.set("test.aws.secretKey", "OPTIONS_SECRET")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    // Options should take precedence
    config.awsCredentialsProviderClass shouldBe Some(classOf[TestV1CredentialProvider].getName)
    config.awsAccessKey shouldBe Some("OPTIONS_ACCESS")
    config.awsSecretKey shouldBe Some("OPTIONS_SECRET")

    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    provider.close()
  }

  test("fallback to explicit credentials should work") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> "com.nonexistent.Provider",
      "spark.indextables.aws.accessKey"                -> "FALLBACK_ACCESS",
      "spark.indextables.aws.secretKey"                -> "FALLBACK_SECRET",
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    config.awsCredentialsProviderClass shouldBe Some("com.nonexistent.Provider")
    config.awsAccessKey shouldBe Some("FALLBACK_ACCESS")
    config.awsSecretKey shouldBe Some("FALLBACK_SECRET")

    // Should fallback to explicit credentials when custom provider fails
    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    provider.close()
  }

  test("fallback to default provider should work") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> "com.nonexistent.Provider",
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    val config =
      CloudStorageProviderFactory.extractCloudConfig(optionsMap, hadoopConf, ProtocolBasedIOFactory.S3Protocol)

    config should not be null
    config.awsCredentialsProviderClass shouldBe Some("com.nonexistent.Provider")
    config.awsAccessKey shouldBe None
    config.awsSecretKey shouldBe None

    // Should fallback to default credentials provider when both custom provider and explicit credentials fail
    val provider = new S3CloudStorageProvider(config, hadoopConf)
    provider should not be null

    provider.close()
  }

  test("provider factory creation should work") {
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestV1CredentialProvider].getName,
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    hadoopConf.set("test.aws.accessKey", "FACTORY_ACCESS")
    hadoopConf.set("test.aws.secretKey", "FACTORY_SECRET")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    // Test CloudStorageProviderFactory.createProvider
    val provider = CloudStorageProviderFactory.createProvider(
      "s3://test-bucket/test-path",
      optionsMap,
      hadoopConf
    )

    provider should not be null
    provider.isInstanceOf[S3CloudStorageProvider] shouldBe true
    provider.getProviderType shouldBe "s3"

    provider.close()
  }

  test("end-to-end with options should work") {
    logger.info("Testing end-to-end custom credential provider via options")

    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[TestDualCredentialProvider].getName,
      "spark.indextables.s3.endpoint"                  -> "http://localhost:9090",
      "spark.indextables.s3.pathStyleAccess"           -> "true",
      "spark.indextables.aws.region"                   -> "us-east-1"
    )

    val hadoopConf = new Configuration()
    hadoopConf.set("test.aws.accessKey", "END_TO_END_ACCESS")
    hadoopConf.set("test.aws.secretKey", "END_TO_END_SECRET")
    hadoopConf.set("test.aws.sessionToken", "END_TO_END_SESSION")

    val optionsMap = new CaseInsensitiveStringMap(options.asJava)

    // Test complete flow
    val provider = CloudStorageProviderFactory.createProvider(
      "s3://test-bucket/end-to-end-test",
      optionsMap,
      hadoopConf
    )

    provider should not be null
    provider.isInstanceOf[S3CloudStorageProvider] shouldBe true

    // Test path normalization
    val normalizedPath = provider.normalizePathForTantivy("s3a://test-bucket/test-file")
    normalizedPath shouldBe "s3://test-bucket/test-file"

    provider.close()
  }
}
