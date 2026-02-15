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

import java.io.File
import java.net.{HttpURLConnection, URL}
import java.nio.file.Files
import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.TransactionLogFactory

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Local integration tests for BUILD INDEXTABLES COMPANION FOR ICEBERG.
 *
 * Requires Docker containers running locally:
 *   cd docker/iceberg-local && docker compose up -d && ./seed-data.sh
 *
 * This starts:
 *   - REST catalog at http://localhost:8181
 *   - MinIO (S3-compatible) at http://localhost:9000
 *   - Pre-seeded table: default.test_events (12 rows, 3 partitions, 2 snapshots)
 *
 * Tests are automatically skipped if the REST catalog is not reachable.
 */
class LocalIcebergSyncIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll
  with IcebergTestSupport {

  private val REST_URI = "http://localhost:8181"
  private val MINIO_ENDPOINT = "http://localhost:9000"
  private val MINIO_ACCESS_KEY = "admin"
  private val MINIO_SECRET_KEY = "password"
  private val TABLE_IDENTIFIER = "default.test_events"

  private var spark: SparkSession = _
  private var tempDir: File = _
  private var restAvailable: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Check if the REST catalog is reachable
    restAvailable = isRestCatalogReachable()
    if (!restAvailable) {
      println(s"Iceberg REST catalog not reachable at $REST_URI - " +
        "start with: cd docker/iceberg-local && docker compose up -d && ./seed-data.sh")
    }

    tempDir = Files.createTempDirectory("iceberg-sync-test-").toFile
    tempDir.deleteOnExit()

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("LocalIcebergSyncIntegrationTest")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // MinIO S3 credentials
      .config("spark.indextables.aws.accessKey", MINIO_ACCESS_KEY)
      .config("spark.indextables.aws.secretKey", MINIO_SECRET_KEY)
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", MINIO_ENDPOINT)
      .config("spark.indextables.s3.pathStyleAccess", "true")
      // Iceberg catalog config
      .config("spark.indextables.iceberg.catalogType", "rest")
      .config("spark.indextables.iceberg.uri", REST_URI)
      .config("spark.indextables.iceberg.warehouse", "s3://warehouse/")
      .config("spark.indextables.iceberg.s3Endpoint", MINIO_ENDPOINT)
      .config("spark.indextables.iceberg.s3PathStyleAccess", "true")
      // Hadoop S3A config for reading/writing index splits if on S3
      .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (tempDir != null) deleteRecursively(tempDir)
    super.afterAll()
  }

  private def isRestCatalogReachable(): Boolean = {
    try {
      val conn = new URL(s"$REST_URI/v1/config").openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)
      conn.setRequestMethod("GET")
      val code = conn.getResponseCode
      conn.disconnect()
      code >= 200 && code < 500
    } catch {
      case _: Exception => false
    }
  }

  private def newIndexPath(): String = {
    val path = new File(tempDir, UUID.randomUUID().toString.substring(0, 8))
    path.getAbsolutePath
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  // --- Tests ---

  test("build companion from local Iceberg REST catalog") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    val row = rows(0)
    row.getString(2) shouldBe "success"
    row.isNullAt(3) shouldBe false // source_version (snapshot ID)
    row.getInt(4) should be > 0    // splits_created
    row.getInt(6) should be > 0    // parquet_files_indexed
  }

  test("source_version is Iceberg snapshot ID (non-null)") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    )

    val row = result.collect()(0)
    row.getString(2) shouldBe "success"
    val snapshotId = row.getAs[Long](3)
    snapshotId should be > 0L
  }

  test("DRY RUN does not create splits") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath' DRY RUN"
    )

    result.collect()(0).getString(2) shouldBe "dry_run"

    // No files should be created
    val indexDir = new File(indexPath)
    assert(!indexDir.exists() || indexDir.listFiles().isEmpty)
  }

  test("transaction log metadata records sourceFormat=iceberg") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val metadata = txLog.getMetadata()
      metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      metadata.configuration("indextables.companion.enabled") shouldBe "true"
    } finally {
      txLog.close()
    }
  }

  test("re-sync with same snapshot returns no_action") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    // First sync
    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    // Re-sync should detect no new files
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    )
    result.collect()(0).getString(2) shouldBe "no_action"
  }

  test("read-back from companion returns correct count") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    val count = companionDf.count()
    count shouldBe 12L // seed-data.sh inserts 10 + 2 rows
  }

  test("read-back data has expected columns") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    val columns = companionDf.columns.toSet
    columns should contain("event_id")
    columns should contain("event_type")
    columns should contain("user_name")
    columns should contain("score")
    columns should contain("region")
  }

  test("read-back supports filter pushdown") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    val filtered = companionDf.filter(companionDf("event_type") === "click")
    val clickCount = filtered.count()
    clickCount should be > 0L
    clickCount should be < 12L
  }

  test("partition columns detected from Iceberg table") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    )

    result.collect()(0).getString(2) shouldBe "success"

    // Verify the companion has the partition column in its schema
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    companionDf.columns should contain("region")
  }

  test("aggregation by partition column (GROUP BY region)") {
    assume(restAvailable, "REST catalog not available")

    val indexPath = newIndexPath()

    // Build companion from partitioned Iceberg table
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '$TABLE_IDENTIFIER' " +
        s"AT LOCATION '$indexPath'"
    )
    result.collect()(0).getString(2) shouldBe "success"

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    // GROUP BY partition column with COUNT(*)
    val aggDf = companionDf.groupBy("region").count().orderBy("region")
    val aggRows = aggDf.collect()

    // Seed data: eu-west=3, us-east=5, us-west=4 (sorted alphabetically)
    aggRows.length shouldBe 3

    val regionCounts = aggRows.map(r => r.getString(0) -> r.getLong(1)).toMap
    regionCounts("eu-west") shouldBe 3L
    regionCounts("us-east") shouldBe 5L
    regionCounts("us-west") shouldBe 4L

    // Total should be 12
    regionCounts.values.sum shouldBe 12L
  }
}
