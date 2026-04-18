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

package io.indextables.spark.catalog

import java.io.File
import java.nio.file.Files
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  NamespaceChange,
  Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.catalog.IndexTablesCatalog
import io.indextables.spark.storage.SplitConversionThrottle
import io.indextables.spark.testutils.FileCleanupHelper

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for IndexTablesCatalog / IndexTables4SparkCatalog.
 *
 * Uses a Delta source catalog (spark_catalog → DeltaCatalog) and the IndexTablesCatalog as
 * the "indextables" named catalog. Tests write IndexTables data locally, set TBLPROPERTIES on
 * the Delta source, then read via the catalog.
 */
class IndexTablesCatalogTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with FileCleanupHelper {

  protected var spark: SparkSession  = _
  protected var tempDir: String      = _

  private val IT_FORMAT = "io.indextables.provider.IndexTablesProvider"

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    tempDir = Files.createTempDirectory("catalog-test").toString

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("IndexTablesCatalogTest")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse-catalog").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      // Delta as the source (spark_catalog)
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // IndexTablesCatalog as the companion index catalog
      .config("spark.sql.catalog.indextables", "io.indextables.catalog.IndexTablesCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      // Dummy AWS settings so local transaction log doesn't complain
      .config("spark.indextables.aws.accessKey", "test-key")
      .config("spark.indextables.aws.secretKey", "test-secret")
      .config("spark.indextables.aws.sessionToken", "test-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SplitConversionThrottle.initialize(maxParallelism = Runtime.getRuntime.availableProcessors() max 1)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (tempDir != null) deleteRecursively(new File(tempDir))
  }

  // ---------------------------------------------------------------------------
  // Helper: write a small IndexTables table locally and return its path
  // ---------------------------------------------------------------------------
  private def writeIndexTable(subdir: String): String = {
    val path = s"$tempDir/$subdir"
    val ss = spark; import ss.implicits._
    val df = Seq(
      ("alice", "alice@example.com"),
      ("bob",   "bob@example.com")
    ).toDF("name", "email")

    df.write
      .mode("overwrite")
      .format(IT_FORMAT)
      .save(path)
    path
  }

  // ---------------------------------------------------------------------------
  // Helper: create a Delta source table in "default" namespace, set TBLPROPERTIES
  // ---------------------------------------------------------------------------
  private def createDeltaSourceTable(tableName: String, indexPath: String): Unit = {
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_$tableName"
    val df = Seq(("alice", "alice@example.com")).toDF("name", "email")
    df.write.format("delta").mode("overwrite").save(deltaPath)

    // Register as managed table via SQL
    spark.sql(s"DROP TABLE IF EXISTS default.$tableName")
    spark.sql(s"""
      CREATE TABLE default.$tableName
      USING delta
      LOCATION '$deltaPath'
    """)

    // Set the companion index properties
    spark.sql(s"""
      ALTER TABLE default.$tableName SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = '$indexPath',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)
  }

  // ---------------------------------------------------------------------------
  // 1. Public alias is instantiable and is a subtype of IndexTables4SparkCatalog
  // ---------------------------------------------------------------------------

  test("IndexTablesCatalog public alias is a subtype of IndexTables4SparkCatalog") {
    val catalog = new IndexTablesCatalog()
    catalog shouldBe a[IndexTables4SparkCatalog]
  }

  // ---------------------------------------------------------------------------
  // 2. loadTable with 3-segment path (catalog.ns.table)
  // ---------------------------------------------------------------------------

  test("loadTable resolves 3-segment identifier and returns IndexTables4SparkTable") {
    val indexPath = writeIndexTable("test_3seg")
    createDeltaSourceTable("t3seg", indexPath)

    val ident = Identifier.of(Array("spark_catalog", "default"), "t3seg")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    // Override relative path to '.' so the resolved path == indexPath exactly
    val table = catalog.loadTable(ident)
    table should not be null
    table.schema().fieldNames should contain allOf("name", "email")
  }

  // ---------------------------------------------------------------------------
  // 3. loadTable with 4-segment path (catalog.ns1.ns2.table)
  // ---------------------------------------------------------------------------

  test("loadTable resolves 4-segment identifier (multi-level namespace)") {
    val indexPath = writeIndexTable("test_4seg")
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_4seg"
    Seq(("alice", "alice@example.com")).toDF("name", "email")
      .write.format("delta").mode("overwrite").save(deltaPath)

    // Create a two-level namespace by saving to a path-based location
    spark.sql("DROP TABLE IF EXISTS default.t4seg")
    spark.sql(s"""
      CREATE TABLE default.t4seg
      USING delta
      LOCATION '$deltaPath'
    """)
    spark.sql(s"""
      ALTER TABLE default.t4seg SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = '$indexPath',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)

    // Identifier: spark_catalog / default . t4seg  (namespace has 2 segments including catalog)
    val ident   = Identifier.of(Array("spark_catalog", "default"), "t4seg")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]
    val table   = catalog.loadTable(ident)
    table should not be null
  }

  // ---------------------------------------------------------------------------
  // 4. loadTable with empty namespace → AnalysisException
  // ---------------------------------------------------------------------------

  test("loadTable with empty namespace throws IllegalArgumentException") {
    val ident   = Identifier.of(Array.empty, "mytable")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    val ex = intercept[IllegalArgumentException] {
      catalog.loadTable(ident)
    }
    ex.getMessage should include("IndexTablesCatalog")
    ex.getMessage should include("source_catalog")
  }

  // ---------------------------------------------------------------------------
  // 5. loadTable: source table missing required TBLPROPERTIES
  // ---------------------------------------------------------------------------

  test("loadTable throws IllegalArgumentException with ALTER TABLE hint when properties missing") {
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_noprops"
    Seq(("alice", "alice@example.com")).toDF("name", "email")
      .write.format("delta").mode("overwrite").save(deltaPath)

    spark.sql("DROP TABLE IF EXISTS default.noprops")
    spark.sql(s"""
      CREATE TABLE default.noprops
      USING delta
      LOCATION '$deltaPath'
    """)
    // No TBLPROPERTIES set

    val ident   = Identifier.of(Array("spark_catalog", "default"), "noprops")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    val ex = intercept[IllegalArgumentException] {
      catalog.loadTable(ident)
    }
    ex.getMessage should include("indextables.companion.indexroot")
    ex.getMessage should include("ALTER TABLE")
  }

  // ---------------------------------------------------------------------------
  // 6. loadTable: source catalog not a TableCatalog
  // ---------------------------------------------------------------------------

  test("loadTable throws UnsupportedOperationException for non-TableCatalog source") {
    // Register a catalog that does NOT implement TableCatalog
    spark.conf.set("spark.sql.catalog.nocatalog", classOf[NonTableCatalogPlugin].getName)

    val ident   = Identifier.of(Array("nocatalog", "default"), "mytable")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    val ex = intercept[UnsupportedOperationException] {
      catalog.loadTable(ident)
    }
    ex.getMessage should include("NonTableCatalogPlugin")
    ex.getMessage should include("TableCatalog")
  }

  // ---------------------------------------------------------------------------
  // 7. loadTable: resolved path has no transaction log
  // ---------------------------------------------------------------------------

  test("loadTable throws IllegalArgumentException when resolved path has no transaction log") {
    val nonExistentPath = s"$tempDir/does_not_exist"
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_notxlog"
    Seq(("alice", "alice@example.com")).toDF("name", "email")
      .write.format("delta").mode("overwrite").save(deltaPath)

    spark.sql("DROP TABLE IF EXISTS default.notxlog")
    spark.sql(s"""
      CREATE TABLE default.notxlog
      USING delta
      LOCATION '$deltaPath'
    """)
    spark.sql(s"""
      ALTER TABLE default.notxlog SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = '$nonExistentPath',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)

    val ident   = Identifier.of(Array("spark_catalog", "default"), "notxlog")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    val ex = intercept[IllegalArgumentException] {
      catalog.loadTable(ident)
    }
    ex.getMessage should include(nonExistentPath)
    ex.getMessage should include("No transaction log found")
  }

  // ---------------------------------------------------------------------------
  // 8. createTable → UnsupportedOperationException
  // ---------------------------------------------------------------------------

  test("createTable throws UnsupportedOperationException") {
    val ident   = Identifier.of(Array("spark_catalog", "default"), "newtable")
    val catalog = spark.sessionState.catalogManager.catalog("indextables").asInstanceOf[TableCatalog]

    val ex = intercept[UnsupportedOperationException] {
      catalog.createTable(ident, new StructType(), Array.empty[Transform], new java.util.HashMap[String, String]())
    }
    ex.getMessage should include("IndexTablesProvider")
    ex.getMessage should include("BUILD INDEXTABLES COMPANION")
  }

  // ---------------------------------------------------------------------------
  // 9. listNamespaces → empty array (no exception)
  // ---------------------------------------------------------------------------

  test("listNamespaces returns empty array without throwing") {
    val catalog = spark.sessionState.catalogManager
      .catalog("indextables")
      .asInstanceOf[org.apache.spark.sql.connector.catalog.SupportsNamespaces]

    val result = catalog.listNamespaces()
    result shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // 10. Path caching: second loadTable call uses cache
  // ---------------------------------------------------------------------------

  test("loadTable uses cached path on second call — source catalog called only once") {
    val indexPath = writeIndexTable("test_cache")

    // Wire up the shared counter and table map so reflectively-created instances see them
    CountingTableCatalog.sharedCounter.set(0)
    CountingTableCatalog.pendingProperties = Some(
      Map(
        "cachedtable" -> Map(
          IndexTableResolver.PROP_INDEX_ROOT_PREFIX -> indexPath,
          IndexTableResolver.PROP_RELATIVE_PATH     -> "."
        )
      )
    )
    // Register under a unique name to avoid a stale cached instance from prior tests
    spark.conf.set("spark.sql.catalog.cachetest", classOf[CountingTableCatalog].getName)

    val innerCatalog = new IndexTables4SparkCatalog()
    innerCatalog.initialize("testcache", new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))

    val ident = Identifier.of(Array("cachetest"), "cachedtable")

    // First call: path resolved from CountingTableCatalog (counter → 1)
    val table1 = innerCatalog.loadTable(ident)
    // Second call: path served from cache (counter stays at 1)
    val table2 = innerCatalog.loadTable(ident)

    CountingTableCatalog.sharedCounter.get() shouldBe 1
    table1.schema().fieldNames shouldBe table2.schema().fieldNames
  }

  // ---------------------------------------------------------------------------
  // 11. End-to-end: write IndexTables, set TBLPROPERTIES, read via catalog
  // ---------------------------------------------------------------------------

  test("end-to-end: spark.read.table reads companion index via named catalog") {
    val indexPath = writeIndexTable("test_e2e")
    createDeltaSourceTable("te2e", indexPath)

    val df = spark.read.table("indextables.spark_catalog.default.te2e")
    df.count() shouldBe 2L
    df.columns should contain allOf("name", "email")

    val names = df.select("name").collect().map(_.getString(0)).toSet
    names should contain("alice")
    names should contain("bob")
  }

  // ---------------------------------------------------------------------------
  // 12. Region-specific property wins over fallback when region matches
  // ---------------------------------------------------------------------------

  test("region-specific indexroot property used when it matches detected region") {
    val indexPath = writeIndexTable("test_regionkey")
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_regionkey"
    Seq(("alice", "alice@example.com")).toDF("name", "email")
      .write.format("delta").mode("overwrite").save(deltaPath)

    spark.sql("DROP TABLE IF EXISTS default.regionkey")
    spark.sql(s"""
      CREATE TABLE default.regionkey
      USING delta
      LOCATION '$deltaPath'
    """)
    // Both region-specific and fallback set; region-specific must win
    spark.sql(s"""
      ALTER TABLE default.regionkey SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}.us-east-1' = '$indexPath',
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = 's3://wrong-bucket/should-not-be-used',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)

    val df = spark.read.table("indextables.spark_catalog.default.regionkey")
    df.count() shouldBe 2L
  }

  // ---------------------------------------------------------------------------
  // 13. Fallback indexroot used when detected region has no specific key
  // ---------------------------------------------------------------------------

  test("fallback indexroot used when no region-specific key matches") {
    val indexPath = writeIndexTable("test_fallback")
    val ss = spark; import ss.implicits._
    val deltaPath = s"$tempDir/delta_fallback"
    Seq(("alice", "alice@example.com")).toDF("name", "email")
      .write.format("delta").mode("overwrite").save(deltaPath)

    spark.sql("DROP TABLE IF EXISTS default.fallbacktest")
    spark.sql(s"""
      CREATE TABLE default.fallbacktest
      USING delta
      LOCATION '$deltaPath'
    """)
    // Only other-region and fallback — us-east-1 won't match eu-west-1
    spark.sql(s"""
      ALTER TABLE default.fallbacktest SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}.eu-west-1' = 's3://eu-bucket/indexes',
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = '$indexPath',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)

    val df = spark.read.table("indextables.spark_catalog.default.fallbacktest")
    df.count() shouldBe 2L
  }

  // ---------------------------------------------------------------------------
  // 14. Filter pushdown works through the catalog
  // ---------------------------------------------------------------------------

  test("SQL WHERE filter via catalog returns correct filtered rows") {
    val indexPath = writeIndexTable("test_filter")
    createDeltaSourceTable("tfilter", indexPath)

    val df = spark.sql(
      "SELECT * FROM indextables.spark_catalog.default.tfilter WHERE name = 'alice'"
    )
    df.count() shouldBe 1L
    df.collect().head.getAs[String]("name") shouldBe "alice"
  }

  // ---------------------------------------------------------------------------
  // 15. IndexQuery works through the catalog
  // ---------------------------------------------------------------------------

  test("indexquery via catalog returns matching rows") {
    val path = s"$tempDir/tiq"
    val ss = spark; import ss.implicits._
    Seq(
      ("hello world", "doc1"),
      ("goodbye world", "doc2")
    ).toDF("content", "id")
      .write.mode("overwrite")
      .format(IT_FORMAT)
      .option("spark.indextables.indexing.typemap.content", "text")
      .save(path)

    val deltaPath = s"$tempDir/delta_tiq"
    Seq(("hello world", "doc1"), ("goodbye world", "doc2")).toDF("content", "id")
      .write.format("delta").mode("overwrite").save(deltaPath)

    spark.sql("DROP TABLE IF EXISTS default.tiq")
    spark.sql(s"""
      CREATE TABLE default.tiq
      USING delta
      LOCATION '$deltaPath'
    """)
    spark.sql(s"""
      ALTER TABLE default.tiq SET TBLPROPERTIES (
        '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = '$path',
        '${IndexTableResolver.PROP_RELATIVE_PATH}' = '.'
      )
    """)

    // Use collect() on a fresh query each time to avoid Spark's analyzed-plan cache:
    // calling df.count() first would cache the plan with Literal(true) in place of the
    // IndexQueryExpression, so a subsequent df.collect() would see no IndexQuery to push down.
    val rows = spark.sql(
      "SELECT * FROM indextables.spark_catalog.default.tiq WHERE content indexquery 'hello'"
    ).collect()
    rows should have length 1
    rows.head.getAs[String]("id") shouldBe "doc1"
  }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * A minimal CatalogPlugin that does NOT implement TableCatalog, used to test the
 * "source catalog is not a TableCatalog" error path.
 */
class NonTableCatalogPlugin extends org.apache.spark.sql.connector.catalog.CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "nocatalog"
}

/** Companion object to pass test state into CountingTableCatalog instances. */
object CountingTableCatalog {
  var pendingProperties: Option[Map[String, Map[String, String]]] = None
  // Shared counter accessible from reflectively-created instances
  val sharedCounter: AtomicInteger = new AtomicInteger(0)
}

/**
 * A minimal TableCatalog backed by an in-memory map, with a call counter for cache validation.
 */
class CountingTableCatalog(
  tables: Map[String, Map[String, String]],
  counter: AtomicInteger
) extends TableCatalog {

  // No-arg constructor needed for Spark's reflective instantiation.
  // Uses the companion-object sharedCounter so the test can observe increments.
  def this() = this(
    CountingTableCatalog.pendingProperties.getOrElse(Map.empty),
    CountingTableCatalog.sharedCounter
  )

  private var catalogNameInternal: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogNameInternal = name
  }

  override def name(): String = catalogNameInternal

  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty

  override def loadTable(ident: Identifier): Table = {
    counter.incrementAndGet()
    val props = tables.getOrElse(
      ident.name(),
      throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(
        s"Table '${ident.name()}' not found"
      )
    )
    new PropertiesOnlyTable(props)
  }

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): Table = throw new UnsupportedOperationException("read-only mock")

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("read-only mock")

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("read-only mock")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("read-only mock")
}

/**
 * A minimal Table implementation that exposes a fixed properties map, used to simulate
 * a source table with TBLPROPERTIES in unit tests.
 */
class PropertiesOnlyTable(props: Map[String, String]) extends Table {
  override def name(): String = "mock-table"

  override def schema(): StructType = StructType(Seq.empty)

  override def capabilities(): util.Set[org.apache.spark.sql.connector.catalog.TableCapability] =
    java.util.Collections.emptySet()

  override def properties(): util.Map[String, String] = props.asJava
}
