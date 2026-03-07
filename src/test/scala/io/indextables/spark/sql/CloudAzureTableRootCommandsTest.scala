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

import java.util.UUID

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.Row

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.CloudAzureTestBase

/**
 * Real Azure Blob Storage integration tests for multi-region table root commands.
 *
 * Tests SET/UNSET/DESCRIBE TABLE ROOTS against real Azure Blob Storage.
 *
 * Credentials are loaded from ~/.azure/credentials file or environment variables.
 */
class CloudAzureTableRootCommandsTest extends CloudAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private var azureBasePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasAzureCredentials()) {
      val storageAccount = getStorageAccount.getOrElse("unknown")
      azureBasePath = s"wasbs://$testContainer@$storageAccount.blob.core.windows.net/table-root-test-$testRunId"

      // Initialize SplitConversionThrottle for tests
      _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
        maxParallelism = Runtime.getRuntime.availableProcessors() max 1
      )

      println(s"Test base path: $azureBasePath")
    } else {
      println(s"No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    if (hasAzureCredentials()) {
      println(s"Test data left at $azureBasePath (will be auto-cleaned by lifecycle policies)")
    }
    super.afterAll()
  }

  /** Create a test IndexTables table and write sample data to Azure. */
  private def createTestTable(tablePath: String): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    val data = Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"))
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)
  }

  /** Enable companion mode on an existing IndexTables table. */
  private def enableCompanionMode(tablePath: String): Unit = {
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    optionsMap.put("spark.indextables.databricks.credential.operation", "PATH_READ_WRITE")
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      val metadata = transactionLog.getMetadata()
      val updatedConfig = metadata.configuration +
        ("indextables.companion.enabled"         -> "true") +
        ("indextables.companion.sourceTablePath" -> s"$azureBasePath/source") +
        ("indextables.companion.sourceFormat"    -> "delta")
      val updatedMetadata = metadata.copy(configuration = updatedConfig)
      transactionLog.commitSyncActions(Seq.empty, Seq.empty, Some(updatedMetadata))
      transactionLog.invalidateCache()
    } finally
      transactionLog.close()
  }

  /** Read metadata configuration from a table. */
  private def readMetadataConfig(tablePath: String): Map[String, String] = {
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      transactionLog.invalidateCache()
      transactionLog.getMetadata().configuration
    } finally
      transactionLog.close()
  }

  test("Real Azure: SET TABLE ROOT should register a root in metadata") {
    assume(hasAzureCredentials(), "Azure credentials required for real Azure test")

    val tablePath = s"$azureBasePath/set-root-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    val result = spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-west-1' = 'wasbs://container@account.blob.core.windows.net/data' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("eu-west-1")

    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.eu-west-1") shouldBe "wasbs://container@account.blob.core.windows.net/data"
    config should contain key "indextables.companion.tableRoots.eu-west-1.timestamp"
  }

  test("Real Azure: UNSET TABLE ROOT should remove a root from metadata") {
    assume(hasAzureCredentials(), "Azure credentials required for real Azure test")

    val tablePath = s"$azureBasePath/unset-root-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Set a root first
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-west-1' = 'wasbs://container@account.blob.core.windows.net/data' AT '$tablePath'"
    )

    // Verify it exists
    val configBefore = readMetadataConfig(tablePath)
    configBefore should contain key "indextables.companion.tableRoots.eu-west-1"

    // Unset it
    val result = spark.sql(
      s"UNSET INDEXTABLES TABLE ROOT 'eu-west-1' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("removed")

    // Verify removal
    val configAfter = readMetadataConfig(tablePath)
    configAfter should not contain key("indextables.companion.tableRoots.eu-west-1")
    configAfter should not contain key("indextables.companion.tableRoots.eu-west-1.timestamp")
  }

  test("Real Azure: DESCRIBE TABLE ROOTS should list registered roots") {
    assume(hasAzureCredentials(), "Azure credentials required for real Azure test")

    val tablePath = s"$azureBasePath/describe-roots-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register two roots
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-central-1' = 'wasbs://container@account.blob.core.windows.net/data' AT '$tablePath'"
    )

    val result = spark.sql(
      s"DESCRIBE INDEXTABLES TABLE ROOTS AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 2

    result.schema.fieldNames should contain theSameElementsAs Seq("root_name", "root_path", "set_timestamp")

    // Rows are sorted by name
    rows(0).getString(0) shouldBe "eu-central-1"
    rows(1).getString(0) shouldBe "us-west-2"
  }
}
