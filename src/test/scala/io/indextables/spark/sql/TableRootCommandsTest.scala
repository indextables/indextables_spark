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

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{MetadataAction, TransactionLogFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for multi-region table root commands:
 *   - SET INDEXTABLES TABLE ROOT
 *   - UNSET INDEXTABLES TABLE ROOT
 *   - DESCRIBE INDEXTABLES TABLE ROOTS
 *   - BUILD COMPANION with TABLE ROOTS clause
 *   - Read-time designator root selection
 */
class TableRootCommandsTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with io.indextables.spark.testutils.FileCleanupHelper {

  private var spark: SparkSession = _
  private var tempDir: File       = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("TableRootCommandsTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    // Initialize SplitConversionThrottle for tests
    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )

    tempDir = Files.createTempDirectory("table-root-cmd-test").toFile
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }

  /** Create a test IndexTables table and write sample data. */
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
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .mode("overwrite")
      .save(tablePath)
  }

  /**
   * Enable companion mode on an existing IndexTables table by writing a MetadataAction with companion.enabled = true.
   */
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
        ("indextables.companion.sourceTablePath" -> "/tmp/source") +
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

  // ===== SQL Parsing Tests =====

  private def parseSetTableRoot(sql: String): SetTableRootCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan      = sqlParser.parsePlan(sql)
    assert(
      plan.isInstanceOf[SetTableRootCommand],
      s"Expected SetTableRootCommand, got ${plan.getClass.getSimpleName}"
    )
    plan.asInstanceOf[SetTableRootCommand]
  }

  private def parseUnsetTableRoot(sql: String): UnsetTableRootCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan      = sqlParser.parsePlan(sql)
    assert(
      plan.isInstanceOf[UnsetTableRootCommand],
      s"Expected UnsetTableRootCommand, got ${plan.getClass.getSimpleName}"
    )
    plan.asInstanceOf[UnsetTableRootCommand]
  }

  private def parseDescribeTableRoots(sql: String): DescribeTableRootsCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan      = sqlParser.parsePlan(sql)
    assert(
      plan.isInstanceOf[DescribeTableRootsCommand],
      s"Expected DescribeTableRootsCommand, got ${plan.getClass.getSimpleName}"
    )
    plan.asInstanceOf[DescribeTableRootsCommand]
  }

  private def parseSyncToExternal(sql: String): SyncToExternalCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan      = sqlParser.parsePlan(sql)
    assert(
      plan.isInstanceOf[SyncToExternalCommand],
      s"Expected SyncToExternalCommand, got ${plan.getClass.getSimpleName}"
    )
    plan.asInstanceOf[SyncToExternalCommand]
  }

  // --- Test 1: SET TABLE ROOT - register a root, verify in metadata ---

  test("SET TABLE ROOT should register a root in metadata") {
    val tablePath = new File(tempDir, "set_root_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    val result = spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("us-west-2")

    // Verify in metadata
    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.us-west-2") shouldBe "s3://bucket-west/data"
    config should contain key "indextables.companion.tableRoots.us-west-2.timestamp"
  }

  // --- Test 2: SET TABLE ROOT overwrite - update existing root path ---

  test("SET TABLE ROOT should overwrite an existing root") {
    val tablePath = new File(tempDir, "overwrite_root_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Set initial root
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-old/data' AT '$tablePath'"
    )

    // Overwrite with new path
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-new/data' AT '$tablePath'"
    )

    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.us-west-2") shouldBe "s3://bucket-new/data"
  }

  // --- Test 3: UNSET TABLE ROOT - remove an existing root ---

  test("UNSET TABLE ROOT should remove an existing root") {
    val tablePath = new File(tempDir, "unset_root_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Set a root first
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )

    // Verify it exists
    val configBefore = readMetadataConfig(tablePath)
    configBefore should contain key "indextables.companion.tableRoots.us-west-2"

    // Unset it
    val result = spark.sql(
      s"UNSET INDEXTABLES TABLE ROOT 'us-west-2' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("removed")

    // Verify removal
    val configAfter = readMetadataConfig(tablePath)
    configAfter should not contain key("indextables.companion.tableRoots.us-west-2")
    configAfter should not contain key("indextables.companion.tableRoots.us-west-2.timestamp")
  }

  // --- Test 4: UNSET TABLE ROOT non-existent - idempotent, no error ---

  test("UNSET TABLE ROOT should succeed for non-existent root") {
    val tablePath = new File(tempDir, "unset_nonexistent_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    val result = spark.sql(
      s"UNSET INDEXTABLES TABLE ROOT 'does-not-exist' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("no-op")
  }

  // --- Test 5: DESCRIBE TABLE ROOTS - list registered roots ---

  test("DESCRIBE TABLE ROOTS should list registered roots") {
    val tablePath = new File(tempDir, "describe_roots_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register two roots
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-central-1' = 's3://bucket-eu/data' AT '$tablePath'"
    )

    val result = spark.sql(
      s"DESCRIBE INDEXTABLES TABLE ROOTS AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 2

    // Verify schema
    result.schema.fieldNames should contain theSameElementsAs Seq("root_name", "root_path", "set_timestamp")

    // Verify content (sorted by name)
    rows(0).getString(0) shouldBe "eu-central-1"
    rows(0).getString(1) shouldBe "s3://bucket-eu/data"
    rows(1).getString(0) shouldBe "us-west-2"
    rows(1).getString(1) shouldBe "s3://bucket-west/data"
  }

  // --- Test 6: DESCRIBE TABLE ROOTS empty - no roots registered ---

  test("DESCRIBE TABLE ROOTS should return empty result when no roots exist") {
    val tablePath = new File(tempDir, "describe_empty_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    val result = spark.sql(
      s"DESCRIBE INDEXTABLES TABLE ROOTS AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 0
  }

  // --- Test 7: SET TABLE ROOT on non-companion table - should error ---

  test("SET TABLE ROOT on non-companion table should fail") {
    val tablePath = new File(tempDir, "non_companion_test").getAbsolutePath
    createTestTable(tablePath)

    val exception = intercept[RuntimeException] {
      spark
        .sql(
          s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket/data' AT '$tablePath'"
        )
        .collect()
    }

    exception.getMessage should include("companion")
  }

  // --- Test 8: Designator validation - reject dots, spaces, empty string ---

  test("SET TABLE ROOT should reject invalid designator names with dots") {
    val exception = intercept[IllegalArgumentException] {
      parseSetTableRoot(
        "SET INDEXTABLES TABLE ROOT 'us.west.2' = 's3://bucket/data' AT '/tmp/table'"
      )
    }
    exception.getMessage should include("Invalid table root name")
  }

  test("SET TABLE ROOT should reject invalid designator names with spaces") {
    val exception = intercept[IllegalArgumentException] {
      parseSetTableRoot(
        "SET INDEXTABLES TABLE ROOT 'us west 2' = 's3://bucket/data' AT '/tmp/table'"
      )
    }
    exception.getMessage should include("Invalid table root name")
  }

  test("SET TABLE ROOT should reject empty designator name") {
    val exception = intercept[IllegalArgumentException] {
      parseSetTableRoot(
        "SET INDEXTABLES TABLE ROOT '' = 's3://bucket/data' AT '/tmp/table'"
      )
    }
    exception.getMessage should include("Invalid table root name")
  }

  test("UNSET TABLE ROOT should reject invalid designator names") {
    val exception = intercept[IllegalArgumentException] {
      parseUnsetTableRoot(
        "UNSET INDEXTABLES TABLE ROOT 'bad.name' AT '/tmp/table'"
      )
    }
    exception.getMessage should include("Invalid table root name")
  }

  test("SET TABLE ROOT should accept valid designator with hyphens and underscores") {
    val cmd = parseSetTableRoot(
      "SET INDEXTABLES TABLE ROOT 'us-west_2' = 's3://bucket/data' AT '/tmp/table'"
    )
    cmd.rootName shouldBe "us-west_2"
    cmd.rootPath shouldBe "s3://bucket/data"
    cmd.tablePath shouldBe "/tmp/table"
  }

  // --- Test 9: BUILD COMPANION with TABLE ROOTS - roots stored in metadata ---

  test("BUILD COMPANION with TABLE ROOTS should parse correctly") {
    val cmd = parseSyncToExternal(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' " +
        "TABLE ROOTS ('us-west-2' = 's3://west/data', 'eu-central-1' = 's3://eu/data') " +
        "AT LOCATION '/tmp/index'"
    )
    cmd.tableRoots shouldBe Map(
      "us-west-2"    -> "s3://west/data",
      "eu-central-1" -> "s3://eu/data"
    )
    cmd.sourcePath shouldBe "/tmp/delta"
    cmd.destPath shouldBe "/tmp/index"
  }

  test("BUILD COMPANION without TABLE ROOTS should have empty map") {
    val cmd = parseSyncToExternal(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.tableRoots shouldBe empty
  }

  // --- Test 10: Read-time root selection - designator selects correct root ---

  test("Read-time designator should resolve to registered root path") {
    val tablePath = new File(tempDir, "read_designator_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register a root
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = '/tmp/west-data' AT '$tablePath'"
    )

    // Read the metadata to verify root is stored
    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.us-west-2") shouldBe "/tmp/west-data"

    // The actual read-time designator selection happens in ScanBuilder.effectiveConfig.
    // We verify the metadata is correctly stored; full integration would require
    // a companion table with actual parquet files at the designated root.
  }

  // --- Test 11: Read-time missing designator - fail with error listing available roots ---

  test("Read-time missing designator should fail with available roots listed") {
    val tablePath = new File(tempDir, "missing_designator_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register some roots
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://west/data' AT '$tablePath'"
    )
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-central-1' = 's3://eu/data' AT '$tablePath'"
    )

    // Try to read with a non-existent designator - this tests the ScanBuilder error path.
    // We need to set the config and try to create a ScanBuilder.
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    optionsMap.put("spark.indextables.companion.tableRootDesignator", "ap-southeast-1")
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      val metadata = transactionLog.getMetadata()
      val schema = org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
        )
      )

      val configMap = optionsMap.asScala.toMap ++ Map(
        "spark.indextables.companion.tableRootDesignator" -> "ap-southeast-1"
      )

      val scanBuilder = new io.indextables.spark.core.IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        schema,
        options,
        configMap
      )

      // The effectiveConfig is lazy, so it triggers when we try to build the scan
      val exception = intercept[IllegalArgumentException] {
        scanBuilder.build()
      }
      exception.getMessage should include("ap-southeast-1")
      exception.getMessage should include("not found")
      exception.getMessage should include("eu-central-1")
      exception.getMessage should include("us-west-2")
    } finally
      transactionLog.close()
  }

  // --- Test 12: Read-time no designator - backwards compatible, use default root ---

  test("Read-time without designator should use default source path resolution") {
    val tablePath = new File(tempDir, "no_designator_test").getAbsolutePath
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register a root but don't set a designator
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://west/data' AT '$tablePath'"
    )

    // Read without designator - should fall through to default sourceTablePath
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    // Do NOT set tableRootDesignator
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      val metadata = transactionLog.getMetadata()
      val schema = org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
        )
      )

      import scala.jdk.CollectionConverters._
      val configMap = optionsMap.asScala.toMap
      val scanBuilder = new io.indextables.spark.core.IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        schema,
        options,
        configMap
      )

      // Build should succeed without designator - falls back to default path
      // (it will use sourceTablePath="/tmp/source" which won't have actual data,
      // but the point is it doesn't throw an error about missing designator)
      val scan = scanBuilder.build()
      scan should not be null
    } finally
      transactionLog.close()
  }

  // --- Test 13: TANTIVY4SPARK keyword - all commands work with both keywords ---

  test("SET TABLE ROOT should work with TANTIVY4SPARK keyword") {
    val cmd = parseSetTableRoot(
      "SET TANTIVY4SPARK TABLE ROOT 'us-west-2' = 's3://bucket/data' AT '/tmp/table'"
    )
    cmd.rootName shouldBe "us-west-2"
    cmd.rootPath shouldBe "s3://bucket/data"
  }

  test("UNSET TABLE ROOT should work with TANTIVY4SPARK keyword") {
    val cmd = parseUnsetTableRoot(
      "UNSET TANTIVY4SPARK TABLE ROOT 'us-west-2' AT '/tmp/table'"
    )
    cmd.rootName shouldBe "us-west-2"
  }

  test("DESCRIBE TABLE ROOTS should work with TANTIVY4SPARK keyword") {
    val cmd = parseDescribeTableRoots(
      "DESCRIBE TANTIVY4SPARK TABLE ROOTS AT '/tmp/table'"
    )
    cmd.tablePath shouldBe "/tmp/table"
  }

  test("SET TABLE ROOT should parse with table identifier") {
    val cmd = parseSetTableRoot(
      "SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket/data' AT my_db.my_table"
    )
    cmd.rootName shouldBe "us-west-2"
    cmd.rootPath shouldBe "s3://bucket/data"
    cmd.tablePath shouldBe "my_db.my_table"
  }

  test("DESCRIBE TABLE ROOTS should parse with table identifier") {
    val cmd = parseDescribeTableRoots(
      "DESCRIBE INDEXTABLES TABLE ROOTS AT my_db.my_table"
    )
    cmd.tablePath shouldBe "my_db.my_table"
  }

  test("BUILD COMPANION TABLE ROOTS should reject invalid root names") {
    val exception = intercept[IllegalArgumentException] {
      parseSyncToExternal(
        "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' " +
          "TABLE ROOTS ('bad.name' = 's3://data') " +
          "AT LOCATION '/tmp/index'"
      )
    }
    exception.getMessage should include("Invalid table root name")
  }
}
