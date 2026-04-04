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

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.utils.CredentialProviderFactory

/**
 * Regression tests for the Unity Catalog companion build credential isolation bug.
 *
 * Bug: When BUILD INDEXTABLES COMPANION was run for a UC Delta/Iceberg source table,
 * SyncToExternalCommand injected spark.indextables.iceberg.uc.tableId (the SOURCE table's ID)
 * into mergedConfigs, then passed those configs to TransactionLogFactory.create() for the
 * DESTINATION txlog. TransactionLogFactory.create() called resolveCredentialsOnDriver() which
 * hit Priority 1.5 (table-based credentials), using the source table's ID to fetch credentials
 * scoped to the source table's S3 path — not the destination. This caused auth failures when
 * writing the companion index transaction log.
 *
 * Fix: TransactionLogFactory.create() strips spark.indextables.iceberg.uc.tableId before calling
 * resolveCredentialsOnDriver(), falling through to Priority 2 (path-based credential resolution)
 * which correctly fetches credentials for the destination txlog path.
 *
 * See also: MockTableCredentialProvider in TestCredentialProviders.scala, which encodes the
 * resolution path in the access key string for assertions.
 */
class TransactionLogFactoryCredentialIsolationTest extends TestBase {

  private val mockProviderClass =
    "io.indextables.spark.testutils.MockTableCredentialProvider"

  /**
   * Documents the bug: resolveCredentialsOnDriver with uc.tableId present triggers Priority 1.5
   * and returns source-table-scoped credentials (wrong for destination).
   *
   * This test documents the pre-fix behavior so the regression is clearly understood.
   * It does NOT call TransactionLogFactory — it tests CredentialProviderFactory directly.
   */
  test("resolveCredentialsOnDriver uses table credentials when uc.tableId is present (documents bug path)") {
    val sourceTableId = "source-uc-table-id-abc123"
    val destPath      = "s3://companion-index-dest/my-index"

    val configs = Map(
      "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass,
      "spark.indextables.iceberg.uc.tableId"           -> sourceTableId
    )

    val resolved = CredentialProviderFactory.resolveCredentialsOnDriver(configs, destPath)

    // Priority 1.5 fires: uses the source table ID, not the destination path
    val accessKey = resolved.getOrElse("spark.indextables.aws.accessKey", "")
    accessKey should startWith("table-based-key:")
    accessKey should include(sourceTableId)
    accessKey should not include "companion-index-dest"
  }

  /**
   * Documents the fix: resolveCredentialsOnDriver without uc.tableId falls through to Priority 2
   * and returns path-based credentials (correct for destination).
   */
  test("resolveCredentialsOnDriver uses path credentials when uc.tableId is absent (correct for destination)") {
    val destPath = "s3://companion-index-dest/my-index"

    val configsWithoutTableId = Map(
      "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass
      // Note: no spark.indextables.iceberg.uc.tableId
    )

    val resolved = CredentialProviderFactory.resolveCredentialsOnDriver(configsWithoutTableId, destPath)

    // Priority 2 fires: uses the destination path, not a table ID
    val accessKey = resolved.getOrElse("spark.indextables.aws.accessKey", "")
    accessKey should startWith("path-based-key:")
    accessKey should include("companion-index-dest")
    accessKey should not include "table-based-key"
  }

  /**
   * Core regression test: TransactionLogFactory.create() must NOT use the source uc.tableId to
   * resolve credentials for the destination txlog. Before the fix, the source table's credentials
   * were fetched (table-based path) and used for the destination, causing auth failures.
   *
   * After the fix, uc.tableId is stripped before resolveCredentialsOnDriver is called, so
   * path-based credentials (correct for the destination) are used instead.
   */
  test("TransactionLogFactory.create() does not use source uc.tableId for destination credential resolution") {
    import io.indextables.spark.testutils.MockTableCredentialProvider
    MockTableCredentialProvider.resetCounts()

    withTempPath { destPath =>
      val sourceTableId = "source-uc-table-id-xyz789"

      // Simulate the configs that SyncToExternalCommand passes to TransactionLogFactory:
      // mergedConfigs contains the source table's uc.tableId alongside the provider class.
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass,
          "spark.indextables.iceberg.uc.tableId"           -> sourceTableId
        ).asJava
      )

      // After the fix: TransactionLogFactory strips uc.tableId before calling resolveCredentialsOnDriver,
      // so Priority 1.5 (table-based) is bypassed and Priority 2 (path-based) is used instead.
      // If the fix is removed, tableCredentialCallCount would be > 0.
      val txlog = TransactionLogFactory.create(new Path(destPath), spark, options)
      try {
        txlog.initialize(getTestSchema())
      } finally
        txlog.close()

      // Table-based credentials must NOT have been invoked for the destination txlog
      MockTableCredentialProvider.tableCredentialCallCount.get() shouldBe 0
      // Path-based credentials must have been used at least once (construction + initialize)
      MockTableCredentialProvider.pathCredentialCallCount.get() should be > 0
    }
  }

  /**
   * Integration-level regression test: TransactionLogFactory.create() must succeed (not throw)
   * when uc.tableId is present in the options, and must create a usable txlog at the destination.
   *
   * Before the fix, if the table-based credential resolution returned source-scoped credentials
   * that were incompatible with the destination, the txlog initialization would fail on the first
   * actual storage operation (e.g., initialize() writing to the destination path).
   */
  test("TransactionLogFactory.create() succeeds with uc.tableId in configs and produces a writable txlog") {
    withTempPath { destPath =>
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass,
          "spark.indextables.iceberg.uc.tableId"           -> "source-uc-table-id-for-integration"
        ).asJava
      )

      val txlog = TransactionLogFactory.create(new Path(destPath), spark, options)
      try {
        // initialize() writes to the destination path; if wrong credentials were used this
        // would fail for cloud paths. For local paths it always succeeds — but if the fix is
        // absent and table-based credential resolution threw an exception during create(),
        // we would never reach this point.
        txlog.initialize(getTestSchema())
        txlog.getSchema() shouldBe defined
      } finally
        txlog.close()
    }
  }

  /**
   * Verifies that uc.tableId in the options is still available for SOURCE reads (not stripped
   * from the configs used to initialize IcebergSourceReader / DeltaSourceReader).
   *
   * The fix strips uc.tableId only inside TransactionLogFactory.create() (for destination auth);
   * the original mergedConfigs in SyncToExternalCommand must retain it for source credential
   * resolution via resolveCredentials(mergedConfigs, sourcePath).
   */
  test("uc.tableId is still usable for source credential resolution after the fix") {
    val sourceTableId = "source-uc-table-id-for-read"
    val sourcePath    = "s3://source-delta-table/data"

    // Source credential resolution (happens in SyncToExternalCommand, NOT stripped)
    val sourceConfigs = Map(
      "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass,
      "spark.indextables.iceberg.uc.tableId"           -> sourceTableId
    )

    val sourceResolved = CredentialProviderFactory.resolveCredentialsOnDriver(sourceConfigs, sourcePath)
    val accessKey      = sourceResolved.getOrElse("spark.indextables.aws.accessKey", "")

    // Table-based credentials are correct for reading the source table
    accessKey should startWith("table-based-key:")
    accessKey should include(sourceTableId)
  }

  /**
   * Regression test: NativeTransactionLog must re-invoke the credential provider before each
   * write operation, not just once at construction time.
   *
   * This matches the old S3CloudStorageProvider behaviour, which wrapped the provider in
   * V1ToV2CredentialsProviderAdapter so the AWS SDK re-invoked it before every storage call.
   * Without per-write refresh, credentials (typical UC TTL ~1 hour) expire during long-running
   * companion builds, causing auth failures.
   *
   * We verify by counting how many times MockTableCredentialProvider.getCredentials() is called
   * across multiple writes. With the fix, the path-based provider must be invoked at least once
   * per write; without it (static credentials only), the count stays at 0 after construction.
   */
  test("NativeTransactionLog re-invokes the credential provider on each write (not just at construction)") {
    import io.indextables.spark.testutils.MockTableCredentialProvider
    MockTableCredentialProvider.resetCounts()

    withTempPath { destPath =>
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass
        ).asJava
      )

      val txlog = TransactionLogFactory.create(new Path(destPath), spark, options)
      try {
        // Baseline: record how many times the provider was called during create()
        val callsAfterCreate = MockTableCredentialProvider.pathCredentialCallCount.get()

        // First write: initialize
        txlog.initialize(getTestSchema())
        val callsAfterInit = MockTableCredentialProvider.pathCredentialCallCount.get()
        callsAfterInit should be > callsAfterCreate

        // Second write: addFiles
        val addAction = io.indextables.spark.transaction.AddAction(
          path = "file://dummy/part-00000.split",
          partitionValues = Map.empty,
          size = 100L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(1L)
        )
        txlog.addFiles(Seq(addAction))
        val callsAfterAdd = MockTableCredentialProvider.pathCredentialCallCount.get()
        callsAfterAdd should be > callsAfterInit

        // Each write increments the counter: credential provider is re-invoked per write
        MockTableCredentialProvider.pathCredentialCallCount.get() should be >= 2
      } finally
        txlog.close()
    }
  }

  /**
   * Regression test: NativeTransactionLog must re-invoke the credential provider before each
   * READ operation (listFiles, getSnapshot), not just before writes.
   *
   * The old S3CloudStorageProvider re-invoked the credential provider before every S3 operation,
   * including reads (listFiles triggered S3 calls on each invocation). Without per-read refresh,
   * long-running Spark queries against companion tables will fail after ~1 hour when UC
   * credentials in nativeConfig expire.
   *
   * Both listFilesArrow() and getOrRefreshSnapshot() now call refreshCredentials() at entry,
   * matching the write-path pattern. This test verifies both paths.
   */
  test("NativeTransactionLog re-invokes the credential provider on each read operation") {
    import io.indextables.spark.testutils.MockTableCredentialProvider
    MockTableCredentialProvider.resetCounts()

    withTempPath { destPath =>
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.aws.credentialsProviderClass" -> mockProviderClass
        ).asJava
      )

      val txlog = TransactionLogFactory.create(new Path(destPath), spark, options)
      try {
        txlog.initialize(getTestSchema())
        val callsAfterInit = MockTableCredentialProvider.pathCredentialCallCount.get()

        // Read 1: listFiles() — goes through listFilesArrow() which now calls refreshCredentials()
        txlog.listFiles()
        val callsAfterListFiles = MockTableCredentialProvider.pathCredentialCallCount.get()
        callsAfterListFiles should be > callsAfterInit

        // Read 2: getSchema() — goes through getOrRefreshSnapshot() which now calls refreshCredentials()
        txlog.getSchema()
        val callsAfterGetSchema = MockTableCredentialProvider.pathCredentialCallCount.get()
        callsAfterGetSchema should be > callsAfterListFiles

        // Credential provider re-invoked per read: total call count reflects both writes and reads
        MockTableCredentialProvider.pathCredentialCallCount.get() should be >= 3
      } finally
        txlog.close()
    }
  }
}
