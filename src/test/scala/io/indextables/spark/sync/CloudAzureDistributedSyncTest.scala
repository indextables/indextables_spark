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

package io.indextables.spark.sync

import java.util.UUID

import org.apache.spark.sql.SparkSession

import io.indextables.spark.CloudAzureTestBase

/**
 * Real Azure Blob Storage integration tests for the distributed log read feature of BUILD INDEXTABLES COMPANION.
 *
 * Validates that the distributed transaction log scanning (DistributedSourceScanner) works correctly
 * with real Azure paths, credentials, and Delta/Parquet data. Tests cover:
 *   - Azure credential propagation to executors via broadcast variables
 *   - Azure path normalization (wasbs:// → az:// conversion)
 *   - Distributed vs non-distributed path equivalence on real Azure storage
 *   - Incremental sync with distributed enabled on Azure
 *   - Fallback behavior when distributed scan fails
 *
 * These tests require:
 *   1. Azure credentials (via ~/.azure/credentials, system properties, or environment variables)
 *   2. An Azure Blob Storage container for test data
 *
 * Tests are automatically skipped if credentials are not available.
 */
class CloudAzureDistributedSyncTest extends CloudAzureTestBase {

  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private lazy val hasAzureWasbs: Boolean =
    try {
      Class.forName("org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  private lazy val hasDeltaSparkDataSource: Boolean =
    try {
      Class.forName("io.delta.sql.DeltaSparkSessionExtension")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  // Use wasbs:// (Blob endpoint) instead of abfss:// (DFS endpoint) because storage
  // accounts with BlobStorageEvents or SoftDelete enabled reject DFS requests with 409.
  private def azureBasePath: String = {
    val account = getStorageAccount.getOrElse("devstoreaccount1")
    s"wasbs://$testContainer@$account.blob.core.windows.net/dist-sync-test-$testRunId"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasDeltaSparkDataSource) {
      val account = getStorageAccount
      val key     = getAccountKey
      val connStr = getConnectionString

      spark.stop()
      val builder = SparkSession
        .builder()
        .appName("CloudAzureDistributedSyncTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config(
          "spark.sql.extensions",
          "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

      account.foreach(a => builder.config("spark.indextables.azure.accountName", a))
      key.foreach(k => builder.config("spark.indextables.azure.accountKey", k))
      connStr.foreach(c => builder.config("spark.indextables.azure.connectionString", c))

      spark = builder.getOrCreate()
      spark.sparkContext.setLogLevel("WARN")

      // Set Hadoop credentials for wasbs:// for Delta Lake writes
      (account, key) match {
        case (Some(a), Some(k)) =>
          val hadoopConf = spark.sparkContext.hadoopConfiguration
          hadoopConf.set(s"fs.azure.account.key.$a.blob.core.windows.net", k)
        case _ =>
      }
    }
  }

  // ─── Delta: Distributed vs Non-Distributed Equivalence ───

  test("distributed and non-distributed Delta sync should produce identical results on Azure") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath   = s"$azureBasePath/delta_compare"
    val distPath    = s"$azureBasePath/companion_dist"
    val nonDistPath = s"$azureBasePath/companion_nondist"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", "active", 100.0),
      (2, "foo bar baz", "inactive", 200.0),
      (3, "test content", "active", 300.0),
      (4, "spark delta", "active", 150.0),
      (5, "companion mode", "inactive", 250.0)
    ).toDF("id", "content", "status", "score")

    df.repartition(3).write.format("delta").mode("overwrite").save(deltaPath)

    // Build with distributed enabled (default)
    val distResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$distPath'"
    ).collect()

    // Build with distributed disabled
    spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
    try {
      val nonDistResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$nonDistPath'"
      ).collect()

      distResult(0).getString(2) shouldBe "success"
      nonDistResult(0).getString(2) shouldBe "success"
      distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6)
    } finally {
      spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
    }
  }

  // ─── Delta: Distributed Sync on Azure ───

  test("distributed Delta sync should succeed on Azure") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath = s"$azureBasePath/delta_dist"
    val indexPath = s"$azureBasePath/companion_delta_dist"

    val _spark = spark
    import _spark.implicits._
    val df = (0 until 50).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
      .toDF("id", "name", "score", "active")

    df.repartition(5).write.format("delta").mode("overwrite").save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(4) should be > 0
    result(0).getInt(6) shouldBe 5
  }

  test("distributed incremental Delta sync should detect new files on Azure") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath = s"$azureBasePath/delta_incr"
    val indexPath = s"$azureBasePath/companion_incr"

    val _spark = spark
    import _spark.implicits._

    // Initial write
    (0 until 20).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(2)
      .write.format("delta").mode("overwrite").save(deltaPath)

    val result1 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result1(0).getString(2) shouldBe "success"

    // Append more data
    (100 until 110).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(1)
      .write.format("delta").mode("append").save(deltaPath)

    // Incremental sync
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result2(0).getString(2) shouldBe "success"
    result2(0).getInt(6) shouldBe 1
  }

  test("distributed Delta sync with partitions should preserve partition values on Azure") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath = s"$azureBasePath/delta_partitioned"
    val indexPath = s"$azureBasePath/companion_partitioned"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "alice", "east"),
      (2, "bob", "west"),
      (3, "carol", "east"),
      (4, "dave", "west"),
      (5, "eve", "north")
    ).toDF("id", "name", "region")

    df.write.format("delta").partitionBy("region").mode("overwrite").save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"

    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val files = txLog.listFiles()
      files should not be empty
      files.foreach { file =>
        file.partitionValues should contain key "region"
        file.companionSourceFiles shouldBe defined
      }
    } finally
      txLog.close()
  }

  // ─── Parquet: Distributed Sync on Azure ───

  test("distributed Parquet sync should succeed on Azure") {
    assume(
      hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or Azure WASBS driver not available - skipping test"
    )

    val parquetPath = s"$azureBasePath/parquet_dist"
    val indexPath   = s"$azureBasePath/companion_parquet_dist"

    val _spark = spark
    import _spark.implicits._
    val df = (0 until 30).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")

    df.repartition(3).write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) shouldBe 3
  }

  test("distributed and non-distributed Parquet sync should produce identical results on Azure") {
    assume(
      hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or Azure WASBS driver not available - skipping test"
    )

    val parquetPath = s"$azureBasePath/parquet_compare"
    val distPath    = s"$azureBasePath/companion_parquet_dist_cmp"
    val nonDistPath = s"$azureBasePath/companion_parquet_nondist_cmp"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "hello world", 100.0), (2, "foo bar", 200.0), (3, "test data", 300.0))
      .toDF("id", "content", "score")
      .write.parquet(parquetPath)

    val distResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$distPath'"
    ).collect()

    spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
    try {
      val nonDistResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$nonDistPath'"
      ).collect()

      distResult(0).getString(2) shouldBe "success"
      nonDistResult(0).getString(2) shouldBe "success"
      distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6)
    } finally {
      spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
    }
  }

  // ─── Re-sync: no changes should return no_action ───

  test("re-sync with no changes should return no_action on Azure Delta") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath = s"$azureBasePath/delta_resync"
    val indexPath = s"$azureBasePath/companion_resync"

    val _spark = spark
    import _spark.implicits._
    (0 until 20).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(2)
      .write.format("delta").mode("overwrite").save(deltaPath)

    // First sync — should succeed
    val result1 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result1(0).getString(2) shouldBe "success"
    result1(0).getInt(6) should be > 0

    // Second sync with no changes — should return no_action
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result2(0).getString(2) shouldBe "no_action"
    result2(0).getInt(6) shouldBe 0
  }

  test("re-sync with no changes should return no_action on Azure Parquet") {
    assume(
      hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or Azure WASBS driver not available - skipping test"
    )

    val parquetPath = s"$azureBasePath/parquet_resync"
    val indexPath   = s"$azureBasePath/companion_parquet_resync"

    val _spark = spark
    import _spark.implicits._
    (0 until 15).map(i => (i.toLong, s"name_$i"))
      .toDF("id", "name")
      .repartition(2)
      .write.parquet(parquetPath)

    // First sync — should succeed
    val result1 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    ).collect()
    result1(0).getString(2) shouldBe "success"
    result1(0).getInt(6) should be > 0

    // Second sync with no changes — should return no_action
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    ).collect()
    result2(0).getString(2) shouldBe "no_action"
    result2(0).getInt(6) shouldBe 0
  }

  // ─── Fallback behavior ───

  test("distributed fallback should work when Delta has no checkpoint on Azure") {
    assume(
      hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test"
    )

    val deltaPath = s"$azureBasePath/delta_no_checkpoint"
    val indexPath = s"$azureBasePath/companion_no_checkpoint"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "single row")).toDF("id", "content")
      .write.format("delta").mode("overwrite").save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) should be > 0
  }
}
