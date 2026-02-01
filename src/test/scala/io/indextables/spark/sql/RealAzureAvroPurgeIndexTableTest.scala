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

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for PURGE INDEXTABLE command with Avro state format.
 *
 * Tests critical functionality specific to Azure Blob Storage with Avro:
 *   - Modification time handling from Azure Blob Storage API with Avro state directories
 *   - State directory preservation during purge
 *   - Orphaned split file cleanup
 *   - Partitioned table handling with Avro format
 *
 * Credentials are loaded from ~/.azure/credentials file or environment variables.
 */
class RealAzureAvroPurgeIndexTableTest extends RealAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private var azureBasePath: String                                  = _
  private var provider: io.indextables.spark.io.CloudStorageProvider = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Use Avro format (the new default) - this is the key difference from JSON test
    spark.conf.set("spark.indextables.state.format", "avro")

    if (hasAzureCredentials()) {
      val storageAccount = getStorageAccount.getOrElse("unknown")
      azureBasePath = s"wasbs://$testContainer@$storageAccount.blob.core.windows.net/avro-purge-test-$testRunId"

      // Initialize CloudStorageProvider for direct Azure operations
      val emptyMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      provider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        azureBasePath,
        emptyMap,
        spark.sparkContext.hadoopConfiguration
      )

      println(s"📍 Test base path: $azureBasePath")
      println(s"✅ Using Avro state format")
    } else {
      println(s"⚠️  No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (hasAzureCredentials() && provider != null) {
      try {
        println(s"📍 Test data left at $azureBasePath (will be auto-cleaned by lifecycle policies)")
        provider.close()
      } catch {
        case ex: Exception =>
          println(s"⚠️  Failed to close provider: ${ex.getMessage}")
      }
    }
    super.afterAll()
  }

  test("Avro: PURGE INDEXTABLE should preserve Avro state directories on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val testPath  = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/avro_state_preservation"

    // Write data multiple times to trigger checkpoint
    val sparkSession = spark
    import sparkSession.implicits._

    // Write 6 times to trigger checkpoint (interval=5)
    (1 to 6).foreach { i =>
      val data = Seq((i, s"test$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify Avro state directory was created by checking _last_checkpoint
    val lastCheckpointPath = s"$tablePath/_transaction_log/_last_checkpoint"
    assert(provider.exists(lastCheckpointPath), "_last_checkpoint should exist after checkpoint")
    println(s"✅ _last_checkpoint exists on Azure")

    // Create orphaned files
    val orphan1Path = s"$tablePath/orphan1_${UUID.randomUUID()}.split"
    val orphan2Path = s"$tablePath/orphan2_${UUID.randomUUID()}.split"
    provider.writeFile(orphan1Path, new Array[Byte](0))
    provider.writeFile(orphan2Path, new Array[Byte](0))

    // Run PURGE with DRY RUN to verify it finds orphans but doesn't touch state
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    println(s"📊 PURGE DRY RUN results: ${metrics.getLong(1)} orphaned files found")

    // Verify _last_checkpoint is still there
    assert(provider.exists(lastCheckpointPath), "_last_checkpoint should be preserved after PURGE DRY RUN")

    // Verify data integrity
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 6, s"Table should still have 6 rows, got ${afterRead.count()}")

    println("✅ Avro state preserved correctly during PURGE on Azure")
  }

  test("Avro: PURGE INDEXTABLE should handle Azure modification times correctly") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val testPath  = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/avro_mod_time_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test1"), (2, "test2"), (3, "test3")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files - Azure will set modification time to current time
    val orphan1Path = s"$tablePath/orphan1_${UUID.randomUUID()}.split"
    val orphan2Path = s"$tablePath/orphan2_${UUID.randomUUID()}.split"

    provider.writeFile(orphan1Path, new Array[Byte](0))
    provider.writeFile(orphan2Path, new Array[Byte](0))

    // Verify we can READ modification times from Azure Blob Storage
    val orphan1Info = provider.getFileInfo(orphan1Path).get
    val orphan2Info = provider.getFileInfo(orphan2Path).get

    println(s"📅 Orphan1 Azure modification time: ${new java.util.Date(orphan1Info.modificationTime)}")
    println(s"📅 Orphan2 Azure modification time: ${new java.util.Date(orphan2Info.modificationTime)}")

    // Verify modification times are recent (within last minute)
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphan1Info.modificationTime) < 60000,
      s"Azure modification time should be recent, got ${orphan1Info.modificationTime}"
    )

    // Purge with 24 hour retention (minimum) - files are too recent to delete
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS").collect()
    val metrics = result(0).getStruct(1)

    // Verify purge found orphaned files but didn't delete them (too recent)
    // Note: With Avro state format, checkpointing may create additional orphaned files
    assert(metrics.getLong(1) >= 2, s"Expected at least 2 orphaned files found, got ${metrics.getLong(1)}")
    assert(
      metrics.getLong(2) == 0,
      s"Expected 0 files deleted (too recent), got ${metrics.getLong(2)}"
    )

    // Verify both files still exist (not deleted due to retention)
    assert(provider.exists(orphan1Path), "Recent orphan1 should be kept")
    assert(provider.exists(orphan2Path), "Recent orphan2 should be kept")

    // Verify table data is intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3, "Table should still have 3 rows")

    println("✅ Avro: Azure modification times correctly handled")
  }

  test("Avro: PURGE INDEXTABLE should handle partitioned tables with Avro state on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val testPath  = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/avro_partitioned_test"

    // Write partitioned data with checkpoint to trigger Avro state
    val sparkSession = spark
    import sparkSession.implicits._

    // Write 6 times to trigger checkpoint
    (1 to 6).foreach { i =>
      val data = Seq(
        (i, "Alice", "2024-01-01"),
        (i + 10, "Bob", "2024-01-02")
      ).toDF("id", "name", "date")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .partitionBy("date")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify _last_checkpoint exists (indicates checkpoint was created)
    val lastCheckpointPath = s"$tablePath/_transaction_log/_last_checkpoint"
    assert(provider.exists(lastCheckpointPath), "_last_checkpoint should exist")
    println(s"✅ Checkpoint created for partitioned table with Avro state")

    // Create orphaned files in different partitions
    val orphan1Path = s"$tablePath/date=2024-01-01/orphaned_${UUID.randomUUID()}.split"
    val orphan2Path = s"$tablePath/date=2024-01-02/orphaned_${UUID.randomUUID()}.split"

    provider.writeFile(orphan1Path, new Array[Byte](0))
    provider.writeFile(orphan2Path, new Array[Byte](0))

    println(s"📂 Created orphaned files in Azure partitions:")
    println(s"   - $orphan1Path")
    println(s"   - $orphan2Path")

    // Use DRY RUN to verify recursive listing finds files in partitions
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both orphaned files were found via recursive Azure listing in partitions
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    // Note: With Avro state format, checkpointing may create additional orphaned files from old splits
    assert(
      metrics.getLong(1) >= 2,
      s"Expected at least 2 orphaned files found in Azure partitions, got ${metrics.getLong(1)}"
    )

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(provider.exists(orphan1Path), "Orphan 1 should still exist after DRY RUN")
    assert(provider.exists(orphan2Path), "Orphan 2 should still exist after DRY RUN")

    // Verify _last_checkpoint is preserved
    assert(provider.exists(lastCheckpointPath), "_last_checkpoint should be preserved")

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 12, s"Table should have 12 rows (6 writes × 2 rows), got ${afterRead.count()}")

    println("✅ Avro: Azure partitioned table with Avro state handled correctly")
  }
}
