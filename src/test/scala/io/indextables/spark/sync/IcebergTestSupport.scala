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
import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.catalog.TableIdentifier

/**
 * Configuration for an Iceberg catalog test endpoint.
 *
 * @param catalogType
 *   Catalog type: "rest", "glue", or "hive"
 * @param uri
 *   Catalog URI (REST endpoint or Hive Metastore thrift URI)
 * @param warehouse
 *   Warehouse location (S3/Azure path)
 * @param token
 *   Bearer token for REST catalog authentication
 * @param credential
 *   OAuth credential for REST catalog
 * @param region
 *   AWS region (for Glue catalog)
 * @param tableIdentifier
 *   Iceberg table identifier (e.g., "default.test_events")
 */
case class IcebergCatalogTestConfig(
  catalogType: String,
  uri: Option[String] = None,
  warehouse: Option[String] = None,
  token: Option[String] = None,
  credential: Option[String] = None,
  region: Option[String] = None,
  s3Endpoint: Option[String] = None,
  s3PathStyleAccess: Option[String] = None,
  tableIdentifier: String = "default.test_events")

/**
 * Shared trait for Iceberg integration tests.
 *
 * Loads Iceberg catalog configuration from `~/.iceberg/credentials` (INI-style) or environment variables. Provides
 * helpers to configure and clear Spark session properties for Iceberg catalog access.
 *
 * INI file format (`~/.iceberg/credentials`):
 * {{{
 * [rest]
 * uri = https://iceberg-rest-catalog.example.com/api
 * warehouse = s3://my-iceberg-warehouse/rest
 * token = eyJ...
 * table = default.test_events
 *
 * [glue]
 * region = us-east-1
 * warehouse = s3://my-iceberg-warehouse/glue
 * table = default.test_events
 *
 * [hms]
 * uri = thrift://hms-server:9083
 * warehouse = s3://my-iceberg-warehouse/hms
 * table = default.test_events
 * }}}
 *
 * Environment variable fallback (REST only): ICEBERG_REST_URI, ICEBERG_WAREHOUSE, ICEBERG_TEST_TABLE,
 * ICEBERG_REST_TOKEN
 */
trait IcebergTestSupport {

  protected var restCatalogConfig: Option[IcebergCatalogTestConfig] = None
  protected var glueCatalogConfig: Option[IcebergCatalogTestConfig] = None
  protected var hmsCatalogConfig: Option[IcebergCatalogTestConfig]  = None

  /** Load Iceberg credentials from `~/.iceberg/credentials` and environment variables. Call this in `beforeAll()`. */
  protected def loadIcebergCredentials(): Unit = {
    loadFromFile()
    loadFromEnvironment()
  }

  private def loadFromFile(): Unit = {
    val credPath = Paths.get(System.getProperty("user.home"), ".iceberg", "credentials")
    if (!Files.exists(credPath)) {
      println("Iceberg credentials file not found at: " + credPath)
      return
    }

    try {
      val lines                          = Files.readAllLines(credPath).asScala
      var currentSection: Option[String] = None
      val sections = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, String]]()

      for (line <- lines) {
        val trimmed = line.trim
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          currentSection = Some(trimmed.substring(1, trimmed.length - 1).toLowerCase)
          sections.getOrElseUpdate(currentSection.get, scala.collection.mutable.Map.empty)
        } else if (trimmed.nonEmpty && !trimmed.startsWith("#") && trimmed.contains("=")) {
          currentSection.foreach { section =>
            val parts = trimmed.split("=", 2)
            if (parts.length == 2) {
              sections(section) += (parts(0).trim -> parts(1).trim)
            }
          }
        }
      }

      // Parse REST section
      sections.get("rest").foreach { props =>
        restCatalogConfig = Some(
          IcebergCatalogTestConfig(
            catalogType = "rest",
            uri = props.get("uri"),
            warehouse = props.get("warehouse"),
            token = props.get("token"),
            credential = props.get("credential"),
            s3Endpoint = props.get("s3_endpoint"),
            s3PathStyleAccess = props.get("s3_path_style_access"),
            tableIdentifier = props.getOrElse("table", "default.test_events")
          )
        )
        println("Loaded Iceberg REST catalog config from ~/.iceberg/credentials")
      }

      // Parse Glue section
      sections.get("glue").foreach { props =>
        glueCatalogConfig = Some(
          IcebergCatalogTestConfig(
            catalogType = "glue",
            warehouse = props.get("warehouse"),
            region = props.get("region"),
            tableIdentifier = props.getOrElse("table", "default.test_events")
          )
        )
        println("Loaded Iceberg Glue catalog config from ~/.iceberg/credentials")
      }

      // Parse HMS section
      sections.get("hms").foreach { props =>
        hmsCatalogConfig = Some(
          IcebergCatalogTestConfig(
            catalogType = "hive",
            uri = props.get("uri"),
            warehouse = props.get("warehouse"),
            tableIdentifier = props.getOrElse("table", "default.test_events")
          )
        )
        println("Loaded Iceberg HMS catalog config from ~/.iceberg/credentials")
      }
    } catch {
      case ex: Exception =>
        println(s"Failed to read Iceberg credentials: ${ex.getMessage}")
    }
  }

  private def loadFromEnvironment(): Unit =
    // Only populate REST config from env vars if not already loaded from file
    if (restCatalogConfig.isEmpty) {
      val uri         = Option(System.getenv("ICEBERG_REST_URI"))
      val warehouse   = Option(System.getenv("ICEBERG_WAREHOUSE"))
      val table       = Option(System.getenv("ICEBERG_TEST_TABLE")).getOrElse("default.test_events")
      val token       = Option(System.getenv("ICEBERG_REST_TOKEN"))
      val s3Endpoint  = Option(System.getenv("ICEBERG_S3_ENDPOINT"))
      val s3PathStyle = Option(System.getenv("ICEBERG_S3_PATH_STYLE_ACCESS"))

      if (uri.isDefined) {
        restCatalogConfig = Some(
          IcebergCatalogTestConfig(
            catalogType = "rest",
            uri = uri,
            warehouse = warehouse,
            token = token,
            s3Endpoint = s3Endpoint,
            s3PathStyleAccess = s3PathStyle,
            tableIdentifier = table
          )
        )
        println("Loaded Iceberg REST catalog config from environment variables")
      }
    }

  /** Configure Spark session with Iceberg catalog properties. */
  protected def configureSparkForCatalog(spark: SparkSession, config: IcebergCatalogTestConfig): Unit = {
    spark.conf.set("spark.indextables.iceberg.catalogType", config.catalogType)
    config.uri.foreach(v => spark.conf.set("spark.indextables.iceberg.uri", v))
    config.warehouse.foreach(v => spark.conf.set("spark.indextables.iceberg.warehouse", v))
    config.token.foreach(v => spark.conf.set("spark.indextables.iceberg.token", v))
    config.credential.foreach(v => spark.conf.set("spark.indextables.iceberg.credential", v))
    config.region.foreach(v => spark.conf.set("spark.indextables.aws.region", v))
    config.s3Endpoint.foreach(v => spark.conf.set("spark.indextables.iceberg.s3Endpoint", v))
    config.s3PathStyleAccess.foreach(v => spark.conf.set("spark.indextables.iceberg.s3PathStyleAccess", v))
  }

  /** Clear Iceberg-specific Spark configuration properties. */
  protected def clearSparkIcebergConfig(spark: SparkSession): Unit = {
    val keys = Seq(
      "spark.indextables.iceberg.catalogType",
      "spark.indextables.iceberg.uri",
      "spark.indextables.iceberg.warehouse",
      "spark.indextables.iceberg.token",
      "spark.indextables.iceberg.credential",
      "spark.indextables.iceberg.s3Endpoint",
      "spark.indextables.iceberg.s3PathStyleAccess"
    )
    keys.foreach { key =>
      try spark.conf.unset(key)
      catch { case _: Exception => }
    }
  }
}

/**
 * Shared base trait for companion integration tests. Provides SparkSession lifecycle, cache flushing,
 * temp directory management, and companion reading — the 5 helpers that were previously duplicated across
 * every Companion*Test file.
 *
 * Mix in with `AnyFunSuite with Matchers with BeforeAndAfterAll with FileCleanupHelper` and call
 * `initSpark(appName)` from your `beforeAll()`, or override `beforeAll()` to use the default setup.
 */
trait CompanionTestBase extends org.scalatest.BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {
  this: org.scalatest.funsuite.AnyFunSuite =>

  protected var spark: SparkSession = _

  /** Override to customize the app name. Default: the test class simple name. */
  protected def appName: String = getClass.getSimpleName

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName(appName)
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

  protected def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  protected def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-test").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  protected def readCompanion(indexPath: String): org.apache.spark.sql.DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
}

/**
 * Shared helper for appending parquet files to an Iceberg table in tests.
 *
 * Writes to a staging directory then moves files into a common `data/` directory so
 * `extractTableBasePath()` derives the correct storage root for companion reads.
 */
trait IcebergSnapshotHelper {
  protected def spark: SparkSession

  def appendIcebergSnapshot(
    server: EmbeddedIcebergRestServer,
    tableId: TableIdentifier,
    rows: Seq[Row],
    schema: StructType,
    rootDir: File,
    batchId: Int,
    partitionPath: Option[String] = None
  ): Seq[String] = {
    val stagingDir = new File(rootDir, s"staging/batch-$batchId")
    val df         = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.coalesce(1).write.parquet(s"file://${stagingDir.getAbsolutePath}")

    val dataDir = new File(rootDir, "data")
    dataDir.mkdirs()

    val parquetFiles = stagingDir
      .listFiles()
      .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)
      .map { src =>
        val dest = new File(dataDir, src.getName)
        java.nio.file.Files.move(src.toPath, dest.toPath)
        dest
      }

    val table    = server.catalog.loadTable(tableId)
    val appendOp = table.newAppend()
    val paths = parquetFiles.map { pf =>
      val path = s"file://${pf.getAbsolutePath}"
      val builder = DataFiles
        .builder(table.spec())
        .withPath(path)
        .withFileSizeInBytes(pf.length())
        .withRecordCount(rows.size.toLong)
        .withFormat(FileFormat.PARQUET)
      partitionPath.foreach(builder.withPartitionPath)
      appendOp.appendFile(builder.build())
      path
    }
    appendOp.commit()
    paths.toSeq
  }
}
