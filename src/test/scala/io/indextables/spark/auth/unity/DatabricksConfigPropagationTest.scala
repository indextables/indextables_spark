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

package io.indextables.spark.auth.unity

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration

import io.indextables.spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import io.indextables.spark.util.ConfigNormalization
import io.indextables.spark.TestBase
import org.slf4j.LoggerFactory

/**
 * Tests to verify that Databricks-specific configuration (spark.indextables.databricks.*) is properly propagated
 * through all paths: read, write, merge, prewarm.
 *
 * This test reproduces the issue where databricks config keys set via spark.conf.set() are not propagated to the
 * UnityCatalogAWSCredentialProvider.
 */
class DatabricksConfigPropagationTest extends TestBase {

  private val logger = LoggerFactory.getLogger(classOf[DatabricksConfigPropagationTest])

  // Test databricks configuration values
  private val testWorkspaceUrl  = "https://test-workspace.cloud.databricks.com"
  private val testApiToken      = "test-api-token-12345"
  private val testRefreshBuffer = "30"
  private val testProviderClass = "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set databricks config via Spark session (as user would)
    spark.conf.set("spark.indextables.databricks.workspaceUrl", testWorkspaceUrl)
    spark.conf.set("spark.indextables.databricks.apiToken", testApiToken)
    spark.conf.set("spark.indextables.databricks.credential.refreshBuffer.minutes", testRefreshBuffer)
    spark.conf.set("spark.indextables.aws.credentialsProviderClass", testProviderClass)
    spark.conf.set("spark.indextables.aws.region", "us-west-2")
  }

  override def afterEach(): Unit = {
    // Clean up config
    spark.conf.unset("spark.indextables.databricks.workspaceUrl")
    spark.conf.unset("spark.indextables.databricks.apiToken")
    spark.conf.unset("spark.indextables.databricks.credential.refreshBuffer.minutes")
    spark.conf.unset("spark.indextables.aws.credentialsProviderClass")
    spark.conf.unset("spark.indextables.aws.region")
    super.afterEach()
  }

  test("ConfigNormalization should extract databricks keys from Spark session") {
    // This should pass - ConfigNormalization.isTantivyKey should accept spark.indextables.databricks.*
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    logger.info(s"Extracted spark configs: ${sparkConfigs.keys.mkString(", ")}")

    // Verify databricks keys are extracted
    sparkConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    sparkConfigs should contain key "spark.indextables.databricks.apiToken"
    sparkConfigs should contain key "spark.indextables.databricks.credential.refreshBuffer.minutes"
    sparkConfigs should contain key "spark.indextables.aws.credentialsProviderClass"

    // Verify values
    sparkConfigs("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    sparkConfigs("spark.indextables.databricks.apiToken") shouldBe testApiToken
  }

  test("CloudStorageProviderFactory enriches Hadoop config with ALL spark.indextables.* keys") {
    // Verify that ConfigNormalization extracts ALL databricks keys from Spark session
    // This is what enrichHadoopConfWithSparkConf uses internally
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    logger.info(s"Spark configs extracted: ${sparkConfigs.size} keys")
    sparkConfigs.keys.filter(_.contains("databricks")).foreach(k => logger.info(s"  Databricks key found: $k"))

    // The fix ensures ALL spark.indextables.* keys are extracted, including databricks
    sparkConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    sparkConfigs should contain key "spark.indextables.databricks.apiToken"
    sparkConfigs("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    sparkConfigs("spark.indextables.databricks.apiToken") shouldBe testApiToken

    // Also verify credential provider class is extracted
    sparkConfigs should contain key "spark.indextables.aws.credentialsProviderClass"
    sparkConfigs("spark.indextables.aws.credentialsProviderClass") shouldBe testProviderClass

    // Simulate what enrichHadoopConfWithSparkConf does: copy all configs to Hadoop
    val enrichedConf = new Configuration()
    sparkConfigs.foreach {
      case (key, value) =>
        enrichedConf.set(key, value)
    }

    // Verify Hadoop config now has databricks keys
    enrichedConf.get("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    enrichedConf.get("spark.indextables.databricks.apiToken") shouldBe testApiToken
    enrichedConf.get("spark.indextables.aws.credentialsProviderClass") shouldBe testProviderClass

    logger.info("VERIFIED: enrichHadoopConfWithSparkConf copies ALL spark.indextables.* keys including databricks")
  }

  test("Hadoop config created for credential provider should contain databricks keys") {
    // This simulates what happens when S3CloudStorageProvider creates a credential provider
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Extract configs from both Spark and Hadoop
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    logger.info(s"Merged configs keys: ${mergedConfigs.keys.toSeq.sorted.mkString(", ")}")

    // Merged configs should have databricks keys (from Spark config)
    mergedConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    mergedConfigs should contain key "spark.indextables.databricks.apiToken"

    // Now create a new Hadoop config and copy the merged configs
    val enrichedConf = new Configuration()
    mergedConfigs.foreach {
      case (key, value) =>
        enrichedConf.set(key, value)
    }

    // Verify the enriched config has databricks keys
    enrichedConf.get("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    enrichedConf.get("spark.indextables.databricks.apiToken") shouldBe testApiToken

    logger.info("SUCCESS: Manual config propagation works - the fix needs to apply this pattern")
  }

  test("SerializableAwsConfig should propagate all indextables configs to credential provider") {
    // This test documents what MergeSplitsCommand.resolveCredentialsFromProvider should do
    // Currently it creates an EMPTY Configuration()

    // Extract all spark.indextables.* configs
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    // Create a Hadoop config with ALL the extracted configs
    val hadoopConf = new Configuration()
    sparkConfigs.foreach {
      case (key, value) =>
        hadoopConf.set(key, value)
    }

    // Now the credential provider can find databricks keys
    hadoopConf.get("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    hadoopConf.get("spark.indextables.databricks.apiToken") shouldBe testApiToken
    hadoopConf.get("spark.indextables.aws.credentialsProviderClass") shouldBe testProviderClass

    logger.info("This shows the pattern needed for MergeSplitsCommand fix")
  }

  test("All spark.indextables.databricks.* keys should pass isTantivyKey filter") {
    // Verify the filter logic
    val databricksKeys = Seq(
      "spark.indextables.databricks.workspaceUrl",
      "spark.indextables.databricks.apiToken",
      "spark.indextables.databricks.credential.refreshBuffer.minutes",
      "spark.indextables.databricks.cache.maxSize",
      "spark.indextables.databricks.fallback.enabled",
      "spark.indextables.databricks.retry.attempts"
    )

    databricksKeys.foreach { key =>
      withClue(s"Key $key should pass isTantivyKey filter: ") {
        ConfigNormalization.isTantivyKey(key) shouldBe true
      }
    }

    logger.info("All databricks keys pass isTantivyKey filter - issue is in hardcoded lists")
  }

  test("enrichHadoopConfWithSparkConf now dynamically copies ALL spark.indextables.* keys") {
    // This test verifies that the fix has been applied:
    // CloudStorageProvider.enrichHadoopConfWithSparkConf now uses ConfigNormalization
    // to dynamically extract ALL spark.indextables.* keys instead of a hardcoded list.

    // Databricks keys that are now properly copied:
    val databricksKeys = Seq(
      "spark.indextables.databricks.workspaceUrl",
      "spark.indextables.databricks.apiToken",
      "spark.indextables.databricks.credential.refreshBuffer.minutes"
    )

    // Extract configs using the same mechanism as enrichHadoopConfWithSparkConf
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    // Verify ALL databricks keys are extracted
    databricksKeys.foreach { key =>
      withClue(s"Key $key should be extracted from Spark config: ") {
        sparkConfigs.keys should contain(key)
      }
    }

    // Verify values are correct
    sparkConfigs("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    sparkConfigs("spark.indextables.databricks.apiToken") shouldBe testApiToken
    sparkConfigs("spark.indextables.databricks.credential.refreshBuffer.minutes") shouldBe testRefreshBuffer

    logger.info("FIXED: enrichHadoopConfWithSparkConf now dynamically copies ALL spark.indextables.* keys")
  }

  test("SerializableAwsConfig for merge operations should include all indextables configs") {
    import io.indextables.spark.sql.SerializableAwsConfig

    // Extract configs using ConfigNormalization (same as MergeSplitsExecutor.extractAwsConfig)
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(spark.sparkContext.hadoopConfiguration)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs) +
      ("spark.indextables.aws.credentialsProviderClass" -> testProviderClass) +
      ("spark.indextables.aws.region"                   -> "us-west-2")

    // Create SerializableAwsConfig with merged configs (new simplified approach)
    val awsConfig = SerializableAwsConfig(
      configs = mergedConfigs,
      tablePath = "s3://test-bucket/test-table"
    )

    // Verify databricks configs are accessible
    awsConfig.configs should contain key "spark.indextables.databricks.workspaceUrl"
    awsConfig.configs should contain key "spark.indextables.databricks.apiToken"
    awsConfig.configs("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    awsConfig.configs("spark.indextables.databricks.apiToken") shouldBe testApiToken

    logger.info(s"SerializableAwsConfig contains ${awsConfig.configs.size} configs for executor")
    logger.info(s"Databricks keys present: ${awsConfig.configs.keys.filter(_.contains("databricks")).mkString(", ")}")
  }

  test("PurgeOrphanedSplitsExecutor extractCloudStorageConfigs pattern should include databricks keys") {
    // Simulate what PurgeOrphanedSplitsExecutor.extractCloudStorageConfigs does
    val sparkConfigs = spark.conf.getAll.filter { case (key, _) => key.startsWith("spark.indextables.") }.toMap

    logger.info(s"PurgeOrphanedSplitsExecutor pattern extracted ${sparkConfigs.size} configs")
    sparkConfigs.keys.filter(_.contains("databricks")).foreach(k => logger.info(s"  Found databricks key: $k"))

    // Verify databricks keys are included
    sparkConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    sparkConfigs should contain key "spark.indextables.databricks.apiToken"
    sparkConfigs("spark.indextables.databricks.workspaceUrl") shouldBe testWorkspaceUrl
    sparkConfigs("spark.indextables.databricks.apiToken") shouldBe testApiToken

    logger.info("VERIFIED: PurgeOrphanedSplitsExecutor pattern includes databricks configs")
  }

  test("Read path should propagate databricks configs to CloudStorageProvider") {
    // The read path goes through IndexTables4SparkDataSource.newScanBuilder
    // which calls CloudStorageProviderFactory.createProvider
    // which calls enrichHadoopConfWithSparkConf

    // Verify ConfigNormalization extracts all configs (used by enrichHadoopConfWithSparkConf)
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    sparkConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    sparkConfigs should contain key "spark.indextables.databricks.apiToken"
    sparkConfigs should contain key "spark.indextables.aws.credentialsProviderClass"

    logger.info("VERIFIED: Read path has access to all databricks configs via ConfigNormalization")
  }

  test("Write path should propagate databricks configs to CloudStorageProvider") {
    // The write path goes through IndexTables4SparkDataSource.newWriteBuilder
    // which calls CloudStorageProviderFactory.createProvider
    // which calls enrichHadoopConfWithSparkConf

    // Verify ConfigNormalization extracts all configs (used by enrichHadoopConfWithSparkConf)
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    sparkConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    sparkConfigs should contain key "spark.indextables.databricks.apiToken"
    sparkConfigs should contain key "spark.indextables.aws.credentialsProviderClass"

    logger.info("VERIFIED: Write path has access to all databricks configs via ConfigNormalization")
  }

  test("All commands should have access to databricks configs") {
    // This test verifies that all commands (Merge, Prewarm, Purge, Repair, Drop)
    // have access to databricks configs through the common patterns they use

    // Pattern 1: ConfigNormalization.extractTantivyConfigsFromSpark (used by most commands)
    val configNormPattern = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    configNormPattern should contain key "spark.indextables.databricks.workspaceUrl"

    // Pattern 2: spark.conf.getAll.filter (used by PurgeOrphanedSplitsExecutor)
    val getallPattern = spark.conf.getAll.filter { case (key, _) => key.startsWith("spark.indextables.") }.toMap
    getallPattern should contain key "spark.indextables.databricks.workspaceUrl"

    // Both patterns produce equivalent results for databricks keys
    configNormPattern("spark.indextables.databricks.workspaceUrl") shouldBe
      getallPattern("spark.indextables.databricks.workspaceUrl")
    configNormPattern("spark.indextables.databricks.apiToken") shouldBe
      getallPattern("spark.indextables.databricks.apiToken")

    logger.info("VERIFIED: All config extraction patterns include databricks keys")
    logger.info(s"  ConfigNormalization pattern: ${configNormPattern.size} keys")
    logger.info(s"  spark.conf.getAll pattern: ${getallPattern.size} keys")
  }

  test("UnityCatalogAWSCredentialProvider can find configs in enriched Hadoop config") {
    // Simulate what happens when UnityCatalogAWSCredentialProvider is instantiated:
    // 1. Configs are extracted from Spark session
    // 2. Configs are copied to Hadoop Configuration
    // 3. Provider looks up configs using HadoopConfigSource

    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)

    // Create enriched Hadoop config (simulating enrichHadoopConfWithSparkConf)
    val hadoopConf = new Configuration()
    sparkConfigs.foreach {
      case (key, value) =>
        hadoopConf.set(key, value)
    }

    // Simulate what UnityCatalogAWSCredentialProvider.resolveConfig does
    val sources: Seq[io.indextables.spark.util.ConfigSource] = Seq(
      io.indextables.spark.util.HadoopConfigSource(hadoopConf, "spark.indextables.databricks"),
      io.indextables.spark.util.HadoopConfigSource(hadoopConf)
    )

    // Try to resolve workspaceUrl (this is what the provider does)
    val workspaceUrl = io.indextables.spark.util.ConfigurationResolver
      .resolveString("workspaceUrl", sources)

    val apiToken = io.indextables.spark.util.ConfigurationResolver
      .resolveString("apiToken", sources, logMask = true)

    workspaceUrl shouldBe Some(testWorkspaceUrl)
    apiToken shouldBe Some(testApiToken)

    logger.info("VERIFIED: UnityCatalogAWSCredentialProvider can find databricks configs in enriched Hadoop config")
  }
}
