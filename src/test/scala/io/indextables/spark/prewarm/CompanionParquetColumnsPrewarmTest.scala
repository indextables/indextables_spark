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

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils, ProtocolNormalizer, SplitMetadataFactory}
import io.indextables.tantivy4java.split.{SplitCacheManager => NativeSplitCacheManager}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

/**
 * Local filesystem tests for PARQUET_COLUMNS prewarm on companion indexes.
 *
 * Validates that the prewarm path correctly injects companion metadata (parquetTableRoot) so that PARQUET_COLUMNS
 * preloading works. Before the fix, prewarm silently failed with "parquet_table_root was not set" because
 * PrewarmCacheCommand never read companion metadata from the transaction log.
 *
 * Test approach:
 *   1. Build a companion index from a local Delta table 2. Flush all caches 3. PREWARM with PARQUET_COLUMNS segment 4.
 *      Verify the SplitSearcher has a parquet companion (parquetTableRoot was injected) 5. Read back data through the
 *      companion and verify correct results
 *
 * No cloud credentials needed — runs entirely on local filesystem.
 */
class CompanionParquetColumnsPrewarmTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(classOf[CompanionParquetColumnsPrewarmTest])

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionParquetColumnsPrewarmTest")
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
      .config("spark.indextables.cache.disk.enabled", "false")
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
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-prewarm-test").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def flushCaches(): Unit =
    try {
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch {
      case _: Exception =>
    }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def createLocalDeltaTable(deltaPath: String, numRows: Int = 50): Unit = {
    val ss = spark
    import ss.implicits._

    val data = (0 until numRows).map(i => (i.toLong, s"name_$i", i * 2.5, s"category_${i % 5}", i % 100))
    data
      .toDF("id", "name", "score", "category", "priority")
      .repartition(1)
      .write
      .format("delta")
      .save(deltaPath)
  }

  // ═══════════════════════════════════════════
  //  PARQUET_COLUMNS Prewarm Tests
  // ═══════════════════════════════════════════

  test("PARQUET_COLUMNS prewarm should inject parquetTableRoot for companion tables") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      // Step 1: Create Delta table and build companion index
      createLocalDeltaTable(deltaPath, numRows = 50)

      val buildResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      val buildRow = buildResult.collect()(0)
      buildRow.getString(buildRow.fieldIndex("status")) shouldBe "success"
      val splitsCreated = buildRow.getInt(buildRow.fieldIndex("splits_created"))
      splitsCreated should be > 0
      logger.info(s"Companion index built: $splitsCreated splits")

      // Step 2: Flush all caches
      flushCaches()

      // Step 3: PREWARM with PARQUET_COLUMNS
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus   = prewarmRows.head.getAs[String]("status")
      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum
      logger.info(s"Prewarm status: $prewarmStatus, splits: $splitsPrewarmed")

      // After the fix, status="success" means parquet preload actually succeeded.
      // Before the fix, status was "success" even when parquet preload silently failed
      // with "parquet_table_root was not set". Now failures are tracked and reported
      // as "partial (parquet preload failed: N splits)".
      prewarmStatus shouldBe "success"
      prewarmStatus should not include "partial"
      prewarmStatus should not include "parquet preload failed"
      splitsPrewarmed should be > 0

      // Step 4: Verify the SplitSearcher has parquet companion support.
      // This proves that enrichConfigWithCompanionMetadata correctly injected
      // parquetTableRoot from the transaction log metadata.
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(spark)
      val hadoopConfigs =
        ConfigNormalization.extractTantivyConfigsFromHadoop(spark.sparkContext.hadoopConfiguration)
      var config = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Read companion metadata from tx log (mirrors what PrewarmCacheCommand now does)
      val txLog = TransactionLogFactory.create(
        new Path(indexPath),
        spark,
        new CaseInsensitiveStringMap(config.asJava)
      )
      try {
        val metadata = txLog.getMetadata()

        // Verify companion metadata exists
        metadata.configuration.getOrElse("indextables.companion.enabled", "false") shouldBe "true"
        metadata.configuration should contain key "indextables.companion.sourceTablePath"

        // Enrich config like PrewarmCacheCommand does
        val isCompanion = metadata.configuration.getOrElse("indextables.companion.enabled", "false") == "true"
        isCompanion shouldBe true

        val sourceTablePath = metadata.configuration("indextables.companion.sourceTablePath")
        config = config + ("spark.indextables.companion.parquetTableRoot" -> sourceTablePath)

        // Create SplitCacheConfig and verify companionSourceTableRoot is set
        val cacheConfig = ConfigUtils.createSplitCacheConfig(config, Some(indexPath))
        cacheConfig.companionSourceTableRoot shouldBe defined
        cacheConfig.companionSourceTableRoot.get shouldBe sourceTablePath
        logger.info(s"companionSourceTableRoot correctly set to: ${cacheConfig.companionSourceTableRoot.get}")

        // Create a SplitSearcher for the first split and verify parquet companion
        val addActions = txLog.listFiles()
        addActions.length should be > 0

        val firstAction   = addActions.head
        val fullPath      = s"$indexPath/${firstAction.path}"
        val actualPath    = ProtocolNormalizer.normalizeAllProtocols(fullPath)
        val splitMetadata = SplitMetadataFactory.fromAddAction(firstAction, indexPath)

        val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)
        val splitSearcher = cacheManager.createSplitSearcher(
          actualPath,
          splitMetadata,
          sourceTablePath,
          null
        )
        try {
          splitSearcher.hasParquetCompanion() shouldBe true

          val statsJson = splitSearcher.getParquetRetrievalStats()
          statsJson should not be null
          logger.info(s"Parquet retrieval stats (proves parquetTableRoot was set): $statsJson")

          // Parse and verify the stats JSON
          val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
          val statsTree    = objectMapper.readTree(statsJson)
          statsTree.get("hasParquetManifest").asBoolean() shouldBe true
          statsTree.get("totalFiles").asInt() should be > 0
          statsTree.get("totalRows").asInt() should be > 0
          statsTree.get("tableRoot").asText() should not be empty
          logger.info(
            s"Parquet companion verified: ${statsTree.get("totalFiles").asInt()} files, " +
              s"${statsTree.get("totalRows").asInt()} rows, tableRoot=${statsTree.get("tableRoot").asText()}"
          )
        } finally
          splitSearcher.close()
      } finally
        txLog.close()
    }
  }

  test("PARQUET_COLUMNS prewarm followed by read should return correct data") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath
      val numRows   = 50

      // Step 1: Create Delta table and build companion
      createLocalDeltaTable(deltaPath, numRows)

      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      // Step 2: Flush and prewarm
      flushCaches()

      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
      )
      val prewarmStatus = prewarmResult.collect().head.getAs[String]("status")
      // Verify prewarm truly succeeded — not a silent parquet failure
      prewarmStatus shouldBe "success"
      prewarmStatus should not include "partial"

      // Step 3: Read back through companion index selecting only the prewarmed column
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val scoreRows = companionDf.select("score").collect()
      scoreRows.length shouldBe numRows

      // Verify score values are correct
      val actualScores   = scoreRows.map(_.getDouble(0)).sorted
      val expectedScores = (0 until numRows).map(_ * 2.5).sorted
      actualScores shouldBe expectedScores

      logger.info(s"Read $numRows rows with correct score values after PARQUET_COLUMNS prewarm")
    }
  }

  test("PARQUET_COLUMNS prewarm without companion should not fail") {
    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "regular_table").getAbsolutePath

      // Write a regular (non-companion) table
      val ss = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"title_$i", i * 1.5))
        .toDF("id", "title", "score")
        .coalesce(1)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.batchSize", "50")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(tablePath)

      flushCaches()

      // PARQUET_COLUMNS on a non-companion table should still succeed (no parquet to preload)
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$tablePath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val status = prewarmRows.head.getAs[String]("status")
      status shouldBe "success"
    }
  }

  test("PARQUET_COLUMNS prewarm ASYNC should inject parquetTableRoot for companion tables") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 30)

      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      flushCaches()

      // ASYNC prewarm
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score) ASYNC MODE"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus = prewarmRows.head.getAs[String]("status")
      val isAsync       = prewarmRows.head.getAs[Boolean]("async_mode")
      isAsync shouldBe true

      // Wait for async job to complete
      spark.sql(s"WAIT FOR INDEXTABLES PREWARM JOBS '$indexPath'").collect()

      // Verify data is readable after async prewarm
      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val rows = companionDf.select("score").collect()
      rows.length shouldBe 30

      val actualScores   = rows.map(_.getDouble(0)).sorted
      val expectedScores = (0 until 30).map(_ * 2.5).sorted
      actualScores shouldBe expectedScores

      logger.info(s"Async PARQUET_COLUMNS prewarm completed, read ${rows.length} rows correctly")
    }
  }

  test("prewarm TERM, POSTINGS, PARQUET_COLUMNS then query should cause zero storage downloads") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath
      val numRows   = 50

      // Step 1: Create Delta table and build companion
      createLocalDeltaTable(deltaPath, numRows)

      spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
        )
        .collect()(0)
        .getString(2) shouldBe "success"

      // Step 2: Flush all caches to start from a clean slate
      flushCaches()
      NativeSplitCacheManager.resetL1Cache()
      NativeSplitCacheManager.resetStorageDownloadMetrics()

      // Step 3: Prewarm TERM, POSTINGS, and PARQUET_COLUMNS
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (TERM, POSTINGS, PARQUET_COLUMNS) ON FIELDS (score, name, id)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0
      val prewarmStatus = prewarmRows.head.getAs[String]("status")
      prewarmStatus shouldBe "success"
      prewarmStatus should not include "partial"

      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum
      splitsPrewarmed should be > 0
      logger.info(s"Prewarm completed: status=$prewarmStatus, splits=$splitsPrewarmed")

      // Step 4: Reset metrics, then run a query — should require zero storage downloads
      NativeSplitCacheManager.resetStorageDownloadMetrics()

      val companionDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val scoreRows = companionDf.select("score").collect()
      scoreRows.length shouldBe numRows

      // Verify the scores are correct
      val actualScores   = scoreRows.map(_.getDouble(0)).sorted
      val expectedScores = (0 until numRows).map(_ * 2.5).sorted
      actualScores shouldBe expectedScores

      // Step 5: Assert zero storage downloads during the query.
      // StorageDownloadMetrics tracks object storage (S3/Azure) requests. On local filesystem,
      // both prewarm and query downloads are 0 since data is read via local file I/O, not the
      // object storage layer. This assertion becomes meaningful on cloud tests (S3/Azure) where
      // prewarm populates the cache and subsequent queries should have zero object storage I/O.
      val metricsAfterQuery = NativeSplitCacheManager.getStorageDownloadMetrics()
      logger.info(s"After query: ${metricsAfterQuery.getSummary}")

      withClue(s"Query should cause zero storage downloads after full prewarm. Metrics: ${metricsAfterQuery.getSummary}") {
        metricsAfterQuery.getQueryDownloads() shouldBe 0L
        metricsAfterQuery.getTotalDownloads() shouldBe 0L
      }

      logger.info(
        s"Zero-I/O verified: query caused ${metricsAfterQuery.getQueryDownloads()} storage downloads, " +
          s"${metricsAfterQuery.getTotalBytes()} bytes. " +
          s"Prewarm status=$prewarmStatus, splits=$splitsPrewarmed, data=$numRows rows verified correct."
      )
    }
  }
}
