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
import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for Arrow FFI read parity and distributed sync parity on companion tables.
 *
 * Validates:
 *   - Distributed vs non-distributed Iceberg companion builds produce identical results
 *   - Arrow FFI enabled vs disabled reads return the same count and aggregation values
 *   - Iceberg companion aggregation parity with Arrow FFI toggle
 */
class CompanionDistributedArrowFfiTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionDistributedArrowFfiTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ── Schema ───────────────────────────────────────────────────────────────────

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.required(3, "score", Types.DoubleType.get())
  )

  private val testRows: Seq[Row] = Seq(
    Row(1L, "alice", 85.0),
    Row(2L, "bob", 90.5),
    Row(3L, "charlie", 75.2),
    Row(4L, "dave", 80.8),
    Row(5L, "eve", 95.1),
    Row(6L, "frank", 70.3),
    Row(7L, "grace", 88.7),
    Row(8L, "heidi", 92.4),
    Row(9L, "ivan", 78.9),
    Row(10L, "judy", 83.6)
  )

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-dist-arrow-ffi").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def readCompanion(indexPath: String): DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)

  private def createDeltaSource(deltaPath: String): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
    df.write.format("delta").save(deltaPath)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Distributed vs non-distributed Iceberg companion parity
  // ═══════════════════════════════════════════════════════════════════════════

  test("distributed and non-distributed should produce identical Iceberg companion") {
    val root         = Files.createTempDirectory("iceberg-dist-parity").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val distPath     = new File(root, "index-dist").getAbsolutePath
    val nonDistPath  = new File(root, "index-nondist").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      // Create Iceberg table
      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Write parquet data and register as snapshot
      val parquetDir = new File(root, "parquet-data").getAbsolutePath
      val df         = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
      df.coalesce(1).write.parquet(s"file://$parquetDir")

      val parquetFiles = new File(parquetDir)
        .listFiles()
        .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

      val table    = server.catalog.loadTable(tableId)
      val appendOp = table.newAppend()
      parquetFiles.foreach { pf =>
        appendOp.appendFile(
          DataFiles
            .builder(table.spec())
            .withPath(s"file://${pf.getAbsolutePath}")
            .withFileSizeInBytes(pf.length())
            .withRecordCount(testRows.size.toLong)
            .withFormat(FileFormat.PARQUET)
            .build()
        )
      }
      appendOp.commit()

      // Configure Spark for embedded catalog
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Build with distributed enabled (default)
      val distResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.test' AT LOCATION '$distPath'"
      ).collect()

      flushCaches()

      // Build with distributed disabled
      spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
      try {
        val nonDistResult = spark.sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.test' AT LOCATION '$nonDistPath'"
        ).collect()

        distResult(0).getString(2) shouldBe "success"
        nonDistResult(0).getString(2) shouldBe "success"
        distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6) // same parquet_files_indexed
      } finally
        spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Arrow FFI parity on Delta companion reads
  // ═══════════════════════════════════════════════════════════════════════════

  test("companion read with Arrow FFI enabled should match disabled") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val buildResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      ).collect()
      buildResult(0).getString(2) shouldBe "success"

      flushCaches()

      // Read with FFI enabled (default)
      val dfEnabled = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
      val countEnabled = dfEnabled.count()
      val sumEnabled   = dfEnabled.agg(sum("score")).collect()(0).getDouble(0)

      flushCaches()

      // Read with FFI disabled
      val dfDisabled = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(indexPath)
      val countDisabled = dfDisabled.count()
      val sumDisabled   = dfDisabled.agg(sum("score")).collect()(0).getDouble(0)

      countEnabled shouldBe countDisabled
      sumEnabled shouldBe sumDisabled +- 0.01
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion aggregation parity with Arrow FFI toggle
  // ═══════════════════════════════════════════════════════════════════════════

  test("Iceberg companion aggregation parity with Arrow FFI toggle") {
    val root         = Files.createTempDirectory("iceberg-ffi-parity").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      // Create Iceberg table
      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "ffi_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Write parquet data and register as snapshot
      val parquetDir = new File(root, "parquet-data").getAbsolutePath
      val df         = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
      df.coalesce(1).write.parquet(s"file://$parquetDir")

      val parquetFiles = new File(parquetDir)
        .listFiles()
        .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

      val table    = server.catalog.loadTable(tableId)
      val appendOp = table.newAppend()
      parquetFiles.foreach { pf =>
        appendOp.appendFile(
          DataFiles
            .builder(table.spec())
            .withPath(s"file://${pf.getAbsolutePath}")
            .withFileSizeInBytes(pf.length())
            .withRecordCount(testRows.size.toLong)
            .withFormat(FileFormat.PARQUET)
            .build()
        )
      }
      appendOp.commit()

      // Configure Spark for embedded catalog
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Build companion with FASTFIELDS MODE HYBRID
      val buildResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.ffi_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      ).collect()
      buildResult(0).getString(2) shouldBe "success"

      flushCaches()

      // Read with FFI enabled (default)
      val dfFfiOn = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)
      val countFfiOn = dfFfiOn.count()
      val sumFfiOn   = dfFfiOn.agg(sum("score")).collect()(0).getDouble(0)

      flushCaches()

      // Read with FFI disabled
      val dfFfiOff = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(indexPath)
      val countFfiOff = dfFfiOff.count()
      val sumFfiOff   = dfFfiOff.agg(sum("score")).collect()(0).getDouble(0)

      countFfiOn shouldBe countFfiOff
      sumFfiOn shouldBe sumFfiOff +- 0.01
    } finally {
      try { spark.conf.unset("spark.indextables.iceberg.catalogType") }
      catch { case _: Exception => }
      try { spark.conf.unset("spark.indextables.iceberg.uri") }
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }
}
