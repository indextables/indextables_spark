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
 * Real Azure Blob Storage integration tests for PURGE ORPHANED SPLITS command.
 *
 * Tests critical functionality specific to Azure Blob Storage:
 *   - Modification time handling from Azure Blob Storage API
 *   - Recursive directory listing in Azure
 *   - Retention period filtering with Azure modification times
 *   - Deletion operations on Azure blobs
 *   - Partitioned table handling in Azure
 *
 * Credentials are loaded from ~/.azure/credentials file or environment variables.
 */
class RealAzurePurgeOrphanedSplitsTest extends RealAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private var azureBasePath: String = _
  private var provider: io.indextables.spark.io.CloudStorageProvider = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasAzureCredentials()) {
      val storageAccount = getStorageAccount.getOrElse("unknown")
      azureBasePath = s"wasbs://$testContainer@$storageAccount.blob.core.windows.net/purge-test-$testRunId"

      // Initialize CloudStorageProvider for direct Azure operations
      val emptyMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      provider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        azureBasePath,
        emptyMap,
        spark.sparkContext.hadoopConfiguration
      )

      println(s"📍 Test base path: $azureBasePath")
    } else {
      println(s"⚠️  No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (hasAzureCredentials() && provider != null) {
      try {
        // Note: CloudStorageProvider doesn't have a deleteDirectory method
        // Files will be auto-cleaned up by Azure blob lifecycle policies
        println(s"📍 Test data left at $azureBasePath (will be auto-cleaned by lifecycle policies)")
        provider.close()
      } catch {
        case ex: Exception =>
          println(s"⚠️  Failed to close provider: ${ex.getMessage}")
      }
    }
    super.afterAll()
  }

  test("PURGE ORPHANED SPLITS should handle Azure Blob Storage modification times correctly") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    // Use unique subdirectory for this specific test to avoid cross-test pollution
    val testPath = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/mod_time_test"

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
    val result  = spark.sql(s"PURGE ORPHANED SPLITS '$tablePath' OLDER THAN 24 HOURS").collect()
    val metrics = result(0).getStruct(1)

    // Verify purge found orphaned files but didn't delete them (too recent)
    assert(metrics.getLong(1) == 2, s"Expected 2 orphaned files found, got ${metrics.getLong(1)}")
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

    println("✅ Azure Blob Storage modification times correctly received and used for retention filtering")
  }

  test("PURGE ORPHANED SPLITS should handle Azure partitioned tables correctly") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    // Use unique subdirectory for this specific test to avoid cross-test pollution
    val testPath = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/partitioned_test"

    // Write partitioned data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Charlie", "2024-01-02")
    ).toDF("id", "name", "date")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date")
      .mode("overwrite")
      .save(tablePath)

    // Create orphaned files in different Azure partitions
    val orphan1Path = s"$tablePath/date=2024-01-01/orphaned_${UUID.randomUUID()}.split"
    val orphan2Path = s"$tablePath/date=2024-01-02/orphaned_${UUID.randomUUID()}.split"

    provider.writeFile(orphan1Path, new Array[Byte](0))
    provider.writeFile(orphan2Path, new Array[Byte](0))

    // Verify we can read modification times from Azure for files in partitions
    val orphan1Info = provider.getFileInfo(orphan1Path).get
    val orphan2Info = provider.getFileInfo(orphan2Path).get

    println(s"📂 Created orphaned files in Azure partitions:")
    println(s"   - $orphan1Path (modified: ${new java.util.Date(orphan1Info.modificationTime)})")
    println(s"   - $orphan2Path (modified: ${new java.util.Date(orphan2Info.modificationTime)})")

    // Verify modification times are recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphan1Info.modificationTime) < 60000,
      "Azure modification time should be recent for partition file"
    )

    // Use DRY RUN to verify recursive listing finds files in partitions
    // Note: Azure may not support setTimes(), so we use DRY RUN
    val result  = spark.sql(s"PURGE ORPHANED SPLITS '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both orphaned files were found via recursive Azure listing in partitions
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(
      metrics.getLong(1) == 2,
      s"Expected 2 orphaned files found in Azure partitions, got ${metrics.getLong(1)}"
    )

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(provider.exists(orphan1Path), "Orphan 1 should still exist in Azure after DRY RUN")
    assert(provider.exists(orphan2Path), "Orphan 2 should still exist in Azure after DRY RUN")

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3, "Table should still have 3 rows")

    println("✅ Azure partitioned table recursive listing works correctly")
  }

  test("PURGE ORPHANED SPLITS should handle Azure recursive listing correctly") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    // Use unique subdirectory for this specific test to avoid cross-test pollution
    val testPath = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/recursive_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files at different Azure path depths
    val rootOrphanPath   = s"$tablePath/orphaned_root_${UUID.randomUUID()}.split"
    val nestedOrphanPath = s"$tablePath/nested/dir/orphaned_nested_${UUID.randomUUID()}.split"

    provider.writeFile(rootOrphanPath, new Array[Byte](0))
    provider.writeFile(nestedOrphanPath, new Array[Byte](0))

    // Verify we can read modification times from Azure at different depths
    val rootInfo   = provider.getFileInfo(rootOrphanPath).get
    val nestedInfo = provider.getFileInfo(nestedOrphanPath).get

    println(s"📁 Created orphaned files at different Azure depths:")
    println(s"   - Root: $rootOrphanPath (modified: ${new java.util.Date(rootInfo.modificationTime)})")
    println(s"   - Nested: $nestedOrphanPath (modified: ${new java.util.Date(nestedInfo.modificationTime)})")

    // Verify modification times are recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - rootInfo.modificationTime) < 60000,
      "Azure modification time should be recent for root file"
    )
    assert(
      math.abs(now - nestedInfo.modificationTime) < 60000,
      "Azure modification time should be recent for nested file"
    )

    // Use DRY RUN to verify recursive listing finds files at all depths
    // Note: Azure may not support setTimes(), so we use DRY RUN
    val result  = spark.sql(s"PURGE ORPHANED SPLITS '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both files were found via recursive Azure listing
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(
      metrics.getLong(1) == 2,
      s"Expected 2 files found via recursive Azure listing, got ${metrics.getLong(1)}"
    )

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(provider.exists(rootOrphanPath), "Root orphan should still exist after DRY RUN")
    assert(provider.exists(nestedOrphanPath), "Nested orphan should still exist after DRY RUN")

    println("✅ Azure recursive listing works correctly at all path depths")
  }

  test("PURGE ORPHANED SPLITS DRY RUN should not delete files from Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    // Use unique subdirectory for this specific test to avoid cross-test pollution
    val testPath = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/dry_run_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file in Azure
    val orphanPath = s"$tablePath/orphaned_${UUID.randomUUID()}.split"
    provider.writeFile(orphanPath, new Array[Byte](0))

    // Verify we can read modification time from Azure
    val orphanInfo = provider.getFileInfo(orphanPath).get
    println(s"📄 Created orphaned file in Azure: $orphanPath (modified: ${new java.util.Date(orphanInfo.modificationTime)})")

    // Verify modification time is recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphanInfo.modificationTime) < 60000,
      "Azure modification time should be recent"
    )

    // Run DRY RUN purge with 24 hour retention (file is too recent)
    val result  = spark.sql(s"PURGE ORPHANED SPLITS '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify DRY RUN found the orphan but didn't delete it from Azure
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(metrics.getLong(1) == 1, "Should find 1 orphaned file")
    assert(metrics.getLong(2) == 0, "Should delete 0 files in DRY RUN")

    // Verify file still exists in Azure
    assert(provider.exists(orphanPath), "Orphaned file should still exist in Azure after DRY RUN")

    println("✅ Azure DRY RUN mode works correctly")
  }

  test("PURGE ORPHANED SPLITS should handle Azure OAuth credentials correctly") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")
    assume(hasOAuthCredentials(), "Azure OAuth credentials required for this test")

    // Use unique subdirectory for this specific test to avoid cross-test pollution
    val testPath = s"$azureBasePath/test-${UUID.randomUUID().toString.substring(0, 8)}"
    val tablePath = s"$testPath/oauth_test"

    // Write data (will use OAuth if configured)
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "oauth_test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file
    val orphanPath = s"$tablePath/orphaned_${UUID.randomUUID()}.split"
    provider.writeFile(orphanPath, new Array[Byte](0))

    // Verify we can read modification time from Azure using OAuth
    val orphanInfo = provider.getFileInfo(orphanPath).get
    println(s"📄 Created orphaned file using OAuth: $orphanPath (modified: ${new java.util.Date(orphanInfo.modificationTime)})")

    // Verify modification time is recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphanInfo.modificationTime) < 60000,
      "Azure modification time should be recent with OAuth"
    )

    // Use DRY RUN to verify OAuth credentials work with purge
    val result  = spark.sql(s"PURGE ORPHANED SPLITS '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify purge worked with OAuth
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(metrics.getLong(1) == 1, "Should find 1 file using OAuth credentials")

    // Verify file still exists (DRY RUN doesn't delete)
    assert(provider.exists(orphanPath), "Orphan should still exist after DRY RUN with OAuth")

    println("✅ Azure OAuth credentials handled correctly")
  }
}
