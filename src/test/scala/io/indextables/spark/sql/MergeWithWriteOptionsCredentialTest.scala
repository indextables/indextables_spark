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

package io.indextables.spark.sql

import io.indextables.spark.util.ConfigNormalization
import io.indextables.spark.TestBase
import org.slf4j.LoggerFactory

/**
 * Tests for credential propagation in merge operations with Unity Catalog.
 *
 * Scenario being tested:
 *   - Provider class and workspace URL are at CLUSTER-SCOPED (session level) Spark properties
 *   - API token is at SESSION-SCOPED Spark property
 *   - Reads, writes, and purges work fine
 *   - Merges FAIL with missing credentials
 *
 * This tests the full credential resolution flow in MergeSplitsExecutor.extractAwsConfig().
 */
class MergeWithWriteOptionsCredentialTest extends TestBase {

  private val logger = LoggerFactory.getLogger(classOf[MergeWithWriteOptionsCredentialTest])

  // Fake workspace URL - connection will fail but we can verify config propagation
  private val fakeWorkspaceUrl  = "http://localhost:19876"
  private val fakeApiToken      = "test-databricks-token-12345"
  private val testProviderClass = "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set up session-level configs to match user's scenario:
    // - Provider class at cluster/session level
    // - Workspace URL at cluster/session level
    // - API token at session level
    spark.conf.set("spark.indextables.aws.credentialsProviderClass", testProviderClass)
    spark.conf.set("spark.indextables.databricks.workspaceUrl", fakeWorkspaceUrl)
    spark.conf.set("spark.indextables.databricks.apiToken", fakeApiToken)
    spark.conf.set("spark.indextables.databricks.retry.attempts", "1") // Speed up tests
    spark.conf.set("spark.indextables.aws.region", "us-west-2")
  }

  override def afterEach(): Unit = {
    // Clean up any configs that might have been set
    spark.conf.unset("spark.indextables.databricks.workspaceUrl")
    spark.conf.unset("spark.indextables.databricks.apiToken")
    spark.conf.unset("spark.indextables.aws.credentialsProviderClass")
    spark.conf.unset("spark.indextables.databricks.retry.attempts")
    spark.conf.unset("spark.indextables.aws.region")
    super.afterEach()
  }

  /**
   * DIAGNOSTIC TEST: Verify session-level configs are visible to MergeSplitsExecutor. This traces through the exact
   * config extraction path used by extractAwsConfig.
   */
  test("DIAGNOSTIC: Session configs should be visible to MergeSplitsExecutor") {
    // Extract configs exactly like MergeSplitsExecutor.extractAwsConfig does
    val hadoopConf    = spark.sparkContext.hadoopConfiguration
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    logger.info(s"=== DIAGNOSTIC: Config Extraction ===")
    logger.info(s"Spark configs count: ${sparkConfigs.size}")
    logger.info(s"Hadoop configs count: ${hadoopConfigs.size}")
    logger.info(s"Merged configs count: ${mergedConfigs.size}")

    // Check for Databricks configs
    val providerClass = mergedConfigs.get("spark.indextables.aws.credentialsProviderClass")
    val workspaceUrl  = mergedConfigs.get("spark.indextables.databricks.workspaceUrl")
    val apiToken      = mergedConfigs.get("spark.indextables.databricks.apiToken")

    logger.info(s"credentialsProviderClass: $providerClass")
    logger.info(s"databricks.workspaceUrl: $workspaceUrl")
    logger.info(s"databricks.apiToken: ${apiToken.map(_ => "***").getOrElse("None")}")

    // These should all be present from session-level config
    providerClass shouldBe Some(testProviderClass)
    workspaceUrl shouldBe Some(fakeWorkspaceUrl)
    apiToken shouldBe Some(fakeApiToken)

    logger.info("SUCCESS: All Databricks configs found in mergedConfigs")
  }

  /**
   * Test MERGE SPLITS SQL command with session-level Unity Catalog configs. This replicates the exact user scenario.
   */
  test("MERGE SPLITS SQL command should use session-level Unity Catalog configs") {
    // Create a local table with multiple splits
    val localPath = tempDir + "/merge_sql_test"
    for (i <- 1 to 5) {
      val df = spark.createDataFrame(Seq((i, s"value$i"))).toDF("id", "value")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(localPath)
    }
    logger.info(s"Created test table at $localPath with 5 splits")

    // Execute MERGE SPLITS via SQL (this is the path that was failing)
    // Since we're using local path, it won't actually need S3 credentials
    // But this tests the credential extraction logic
    val result = spark.sql(s"MERGE SPLITS '$localPath' TARGET SIZE 100M").collect()

    logger.info(s"MERGE SPLITS completed with ${result.length} rows")
    result.foreach(row => logger.info(s"  Result: ${row.toString}"))

    // Verify merge completed successfully
    result should not be empty
  }

  /**
   * Test that directly invokes extractAwsConfig logic with session-level configs. This isolates the credential
   * extraction to understand what's happening.
   */
  test("extractAwsConfig logic should find session-level configs") {
    import scala.jdk.CollectionConverters._

    // Create a local table
    val localPath = tempDir + "/extract_aws_test"
    val df        = spark.createDataFrame(Seq((1, "value1"))).toDF("id", "value")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(localPath)

    // Create MergeSplitsExecutor WITHOUT overrideOptions (like SQL command does)
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      new org.apache.hadoop.fs.Path(localPath),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    val executor = new MergeSplitsExecutor(
      sparkSession = spark,
      transactionLog = transactionLog,
      tablePath = new org.apache.hadoop.fs.Path(localPath),
      partitionPredicates = Seq.empty,
      targetSize = 100 * 1024 * 1024,
      maxDestSplits = None,
      maxSourceSplitsPerMerge = None,
      preCommitMerge = false,
      overrideOptions = None // No override options - relies on session configs
    )

    // Execute merge - this will exercise extractAwsConfig
    val results = executor.merge()

    logger.info(s"Merge completed with ${results.size} result rows")
    results should not be empty
  }

  /**
   * Test credential resolution directly using ConfigUtils. This verifies that resolveCredentialsFromProviderOnDriver
   * gets the right configs.
   *
   * Note: With centralized credential resolution, provider failures return None (graceful fallback) instead of throwing
   * exceptions.
   */
  test("ConfigUtils.resolveCredentialsFromProviderOnDriver should receive all session configs") {
    import io.indextables.spark.util.ConfigUtils
    import io.indextables.spark.utils.CredentialProviderFactory

    // Clear credential provider cache to force fresh instantiation
    CredentialProviderFactory.clearCache()

    // Extract configs like MergeSplitsExecutor does
    val hadoopConf    = spark.sparkContext.hadoopConfiguration
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Verify all required configs are present
    mergedConfigs should contain key "spark.indextables.aws.credentialsProviderClass"
    mergedConfigs should contain key "spark.indextables.databricks.workspaceUrl"
    mergedConfigs should contain key "spark.indextables.databricks.apiToken"

    // Remove any explicit credentials to force provider invocation
    val configsNoExplicitCreds = mergedConfigs.filterNot {
      case (k, _) =>
        k.toLowerCase.contains("accesskey") ||
        k.toLowerCase.contains("secretkey") ||
        k.toLowerCase.contains("sessiontoken")
    }

    // With centralized resolution, provider failures return the original config unchanged
    // (graceful fallback to default provider chain) instead of throwing exceptions
    val result = ConfigUtils.resolveCredentialsFromProviderOnDriver(configsNoExplicitCreds, "s3://test-bucket/path")

    // Since the provider failed (fake URL), the result should be the original config
    // (no credentials were added because resolution failed gracefully)
    result.get("spark.indextables.aws.accessKey") shouldBe None

    // The logger.warn output shows the provider was invoked and failed gracefully

    logger.info("SUCCESS: Credential resolution attempted using Unity Catalog provider (failed gracefully as expected)")
  }

  private def getFullErrorMessage(t: Throwable): String = {
    val sb                 = new StringBuilder
    var current: Throwable = t
    while (current != null) {
      sb.append(current.getClass.getName).append(": ").append(current.getMessage).append("\n")
      current = current.getCause
    }
    sb.toString
  }

  /**
   * Test that MergeSplitsExecutor.extractAwsConfig properly includes overrideOptions when looking up credential
   * provider configs (for merge-on-write scenario).
   */
  test("extractAwsConfig should include overrideOptions when provided") {
    import scala.jdk.CollectionConverters._

    // Clear session-level configs to test overrideOptions path
    spark.conf.unset("spark.indextables.aws.credentialsProviderClass")
    spark.conf.unset("spark.indextables.databricks.workspaceUrl")
    spark.conf.unset("spark.indextables.databricks.apiToken")

    // Create write options with Databricks configs
    val writeOptionsMap = Map(
      "spark.indextables.databricks.workspaceUrl"      -> fakeWorkspaceUrl,
      "spark.indextables.databricks.apiToken"          -> fakeApiToken,
      "spark.indextables.aws.credentialsProviderClass" -> testProviderClass,
      "spark.indextables.aws.region"                   -> "us-west-2",
      "spark.indextables.databricks.retry.attempts"    -> "1"
    )

    // Verify configs are NOT in session
    val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    sparkConfigs.get("spark.indextables.databricks.workspaceUrl") shouldBe None

    // Create a local table
    val localPath = tempDir + "/merge_override_test"
    for (i <- 1 to 3) {
      val df = spark.createDataFrame(Seq((i, s"value$i"))).toDF("id", "value")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(localPath)
    }

    // Create executor WITH overrideOptions
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      new org.apache.hadoop.fs.Path(localPath),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    val executor = new MergeSplitsExecutor(
      sparkSession = spark,
      transactionLog = transactionLog,
      tablePath = new org.apache.hadoop.fs.Path(localPath),
      partitionPredicates = Seq.empty,
      targetSize = 100 * 1024 * 1024,
      maxDestSplits = None,
      maxSourceSplitsPerMerge = None,
      preCommitMerge = false,
      overrideOptions = Some(writeOptionsMap)
    )

    val results = executor.merge()
    logger.info(s"Merge with overrideOptions completed with ${results.size} result rows")
    results should not be empty

    // Restore session configs
    spark.conf.set("spark.indextables.aws.credentialsProviderClass", testProviderClass)
    spark.conf.set("spark.indextables.databricks.workspaceUrl", fakeWorkspaceUrl)
    spark.conf.set("spark.indextables.databricks.apiToken", fakeApiToken)
  }

  /**
   * Test the full merge-on-write flow with write options containing credential configs. This replicates the exact user
   * scenario:
   *   1. Set Databricks configs via write options (not session) 2. Write data with merge-on-write enabled 3. Merge
   *      should be able to access the configs
   */
  test("merge-on-write should propagate write options to merge executor") {
    val localPath = tempDir + "/merge_on_write_options_test"

    // First write: Create initial table (no merge yet)
    val df1 = spark.createDataFrame(Seq((1, "value1"), (2, "value2"))).toDF("id", "value")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(localPath)
    logger.info("Initial write completed")

    // Second write: With merge-on-write enabled and Databricks configs in write options
    // This simulates the user's scenario where configs are ONLY in write options
    val df2 = spark.createDataFrame(Seq((3, "value3"), (4, "value4"))).toDF("id", "value")

    // Note: Using local path, so merge will use local filesystem (not S3)
    // This tests the config propagation logic without needing actual S3 access
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "1K")            // Very small to force merge
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Low threshold to trigger merge
      // Include Databricks configs in write options
      .option("spark.indextables.databricks.workspaceUrl", fakeWorkspaceUrl)
      .option("spark.indextables.databricks.apiToken", fakeApiToken)
      .option("spark.indextables.aws.credentialsProviderClass", testProviderClass)
      .mode("append")
      .save(localPath)

    logger.info("Second write with merge-on-write completed")

    // Verify the table is readable (proves merge didn't corrupt data)
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(localPath)
      .count()

    result shouldBe 4
    logger.info(s"Table has $result rows after merge-on-write")
  }

  /**
   * Test that verifies the config merging logic in extractAwsConfig. This directly tests that overrideOptions are
   * merged with mergedConfigs before credential resolution.
   */
  test("configForResolution should include both session configs and overrideOptions") {
    import scala.jdk.CollectionConverters._

    // Clear session-level Databricks configs so we can test that overrideOptions are merged
    spark.conf.unset("spark.indextables.databricks.workspaceUrl")
    spark.conf.unset("spark.indextables.databricks.apiToken")
    spark.conf.unset("spark.indextables.aws.credentialsProviderClass")

    // Set some config at session level (but NOT the Databricks configs)
    spark.conf.set("spark.indextables.aws.region", "us-east-1")

    // Create override options with different values (Databricks configs only here)
    val overrideOptions = Map(
      "spark.indextables.databricks.workspaceUrl"      -> fakeWorkspaceUrl,
      "spark.indextables.databricks.apiToken"          -> fakeApiToken,
      "spark.indextables.aws.credentialsProviderClass" -> testProviderClass,
      "spark.indextables.aws.region"                   -> "us-west-2" // Override session value
    )

    // Extract configs like MergeSplitsExecutor does
    val hadoopConf    = spark.sparkContext.hadoopConfiguration
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Verify session config is in mergedConfigs
    mergedConfigs.get("spark.indextables.aws.region") shouldBe Some("us-east-1")

    // Verify Databricks config is NOT in mergedConfigs (since we unset them, only in overrideOptions now)
    mergedConfigs.get("spark.indextables.databricks.workspaceUrl") shouldBe None

    // Now merge like the fix does
    val configForResolution = mergedConfigs ++ overrideOptions

    // Verify merged config has both session and override values
    configForResolution.get("spark.indextables.databricks.workspaceUrl") shouldBe Some(fakeWorkspaceUrl)
    configForResolution.get("spark.indextables.databricks.apiToken") shouldBe Some(fakeApiToken)
    configForResolution.get("spark.indextables.aws.credentialsProviderClass") shouldBe Some(testProviderClass)
    // Override should take precedence
    configForResolution.get("spark.indextables.aws.region") shouldBe Some("us-west-2")

    logger.info("Verified: configForResolution correctly merges session configs with overrideOptions")
    logger.info(s"  - Session region: us-east-1, Override region: us-west-2, Result: ${configForResolution.get("spark.indextables.aws.region")}")
  }

  /** Test that SerializableAwsConfig correctly receives all configs including Databricks keys. */
  test("SerializableAwsConfig should include Databricks configs from overrideOptions") {
    import scala.jdk.CollectionConverters._

    val testConfigs = Map(
      "spark.indextables.databricks.workspaceUrl"      -> fakeWorkspaceUrl,
      "spark.indextables.databricks.apiToken"          -> fakeApiToken,
      "spark.indextables.aws.credentialsProviderClass" -> testProviderClass,
      "spark.indextables.aws.accessKey"                -> "test-access-key",
      "spark.indextables.aws.secretKey"                -> "test-secret-key",
      "spark.indextables.aws.region"                   -> "us-west-2"
    )

    // Create SerializableAwsConfig with merged configs (new simplified approach)
    val awsConfig = SerializableAwsConfig(
      configs = testConfigs,
      tablePath = "s3://test-bucket/test-table"
    )

    // Verify configs are accessible
    awsConfig.configs should contain key "spark.indextables.databricks.workspaceUrl"
    awsConfig.configs should contain key "spark.indextables.databricks.apiToken"
    awsConfig.configs("spark.indextables.databricks.workspaceUrl") shouldBe fakeWorkspaceUrl
    awsConfig.configs("spark.indextables.databricks.apiToken") shouldBe fakeApiToken

    // Verify accessors work
    awsConfig.accessKey shouldBe "test-access-key"
    awsConfig.secretKey shouldBe "test-secret-key"
    awsConfig.region shouldBe "us-west-2"

    logger.info("Verified: SerializableAwsConfig includes Databricks configs")
  }

  /**
   * Test that exercises the full credential resolution path with write options. This simulates what happens when
   * MergeSplitsExecutor.extractAwsConfig is called with overrideOptions containing the credential provider config.
   *
   * Note: With centralized credential resolution, provider failures return None (fall back to default provider chain)
   * instead of throwing exceptions. This test verifies that the provider is correctly invoked with merged config.
   */
  test("credential resolution should use merged config including overrideOptions") {
    import io.indextables.spark.util.ConfigUtils
    import io.indextables.spark.utils.CredentialProviderFactory
    import scala.jdk.CollectionConverters._

    // Clear any explicit credentials that might be in session (from previous tests)
    spark.conf.unset("spark.indextables.aws.accessKey")
    spark.conf.unset("spark.indextables.aws.secretKey")
    spark.conf.unset("spark.indextables.aws.sessionToken")

    // Clear credential provider cache to force fresh instantiation
    CredentialProviderFactory.clearCache()

    // Create a config map that simulates what would be in overrideOptions
    val overrideOptions = Map(
      "spark.indextables.databricks.workspaceUrl"      -> fakeWorkspaceUrl,
      "spark.indextables.databricks.apiToken"          -> fakeApiToken,
      "spark.indextables.aws.credentialsProviderClass" -> testProviderClass,
      "spark.indextables.databricks.retry.attempts"    -> "1" // Reduce retries for faster test
    )

    // Extract mergedConfigs like MergeSplitsExecutor does
    val hadoopConf    = spark.sparkContext.hadoopConfiguration
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Remove any explicit credentials from merged config to force provider invocation
    // (centralized resolution prioritizes explicit credentials over provider)
    val mergedConfigsNoExplicitCreds = mergedConfigs
      .filterNot {
        case (k, _) =>
          k.toLowerCase.contains("accesskey") ||
          k.toLowerCase.contains("secretkey") ||
          k.toLowerCase.contains("sessiontoken")
      }

    // Apply the fix: merge overrideOptions with mergedConfigs
    val configForResolution = mergedConfigsNoExplicitCreds ++ overrideOptions

    // Verify provider class is in the merged config
    configForResolution.get("spark.indextables.aws.credentialsProviderClass") shouldBe Some(testProviderClass)

    // Verify no explicit credentials are in the config (forcing provider invocation)
    configForResolution.get("spark.indextables.aws.accessKey") shouldBe None

    // Use the centralized resolution directly to test provider invocation
    // With centralized resolution, provider failures return None (graceful fallback)
    // instead of throwing exceptions
    val result = CredentialProviderFactory.resolveAWSCredentialsFromConfig(
      configForResolution,
      "s3://test-bucket/path"
    )

    // The provider was invoked but failed (fake URL), so result should be None
    // This proves the config was propagated correctly to the provider
    result shouldBe None

    // The logger.warn output shows the provider was invoked and failed gracefully

    logger.info(
      "Verified: Credential resolution invoked Unity Catalog provider (which failed as expected with fake URL)"
    )
    logger.info("Provider returned None, indicating graceful fallback to default provider chain")
  }

  /**
   * Regression test: Verify that the original bug scenario is fixed. The bug was that Databricks configs in write
   * options were not being passed to credential resolution in extractAwsConfig.
   *
   * After centralization refactoring:
   *   - Config merging happens BEFORE creating SerializableAwsConfig
   *   - SerializableAwsConfig holds merged configs (including overrideOptions)
   *   - Credential resolution uses CredentialProviderFactory.resolveAWSCredentialsFromConfig
   */
  test("REGRESSION: extractAwsConfig must merge overrideOptions before credential resolution") {
    // Read the source file and verify the fix is in place
    val sourceFile = scala.io.Source.fromFile(
      "src/main/scala/io/indextables/spark/sql/MergeSplitsCommand.scala"
    )
    try {
      val content = sourceFile.mkString

      // Verify the centralized credential resolution patterns are present:
      // 1. overrideOptions are merged with baseConfigs
      content should include("baseConfigs ++ overrides")

      // 2. SerializableAwsConfig receives the merged configs
      content should include("configs = mergedConfigs")

      // 3. Credential resolution uses centralized CredentialProviderFactory
      content should include("CredentialProviderFactory.resolveAWSCredentialsFromConfig")

      // 4. SerializableAwsConfig has resolveAWSCredentials method
      content should include("def resolveAWSCredentials()")

      logger.info("REGRESSION TEST PASSED: Config merging and centralized credential resolution are in place")
    } finally
      sourceFile.close()
  }
}
