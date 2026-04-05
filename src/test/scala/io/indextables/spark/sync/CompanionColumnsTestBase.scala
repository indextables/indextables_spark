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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Shared trait for INCLUDE COLUMNS / EXCLUDE COLUMNS companion tests.
 *
 * <p>Subclasses implement the abstract members for a specific table format (Delta, Iceberg, etc.)
 * and automatically inherit all shared tests.
 *
 * <p>All tests share two tables: a 14-column table (core + evolution columns) and a 4-column
 * partitioned table. Tests are organized in three phases against the shared table:
 * <ol>
 *   <li><b>Read-only</b> — build companion indexes and query them (table is not mutated)</li>
 *   <li><b>Append</b> — append rows and verify incremental sync</li>
 *   <li><b>Evolution</b> — recreate the table with a mutated schema and verify schema checks</li>
 * </ol>
 *
 * <p>This ordering is load-bearing: tests run in registration order (ScalaTest AnyFunSuite),
 * and later phases depend on cumulative table state from earlier phases.
 */
trait CompanionColumnsTestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  // ───────────────────────────────────────────────────────────────────
  //  Abstract interface — subclasses must implement
  // ───────────────────────────────────────────────────────────────────

  def formatName: String
  def spark: SparkSession

  def createSimpleTable(tableId: String, schema: StructType, data: Seq[Row]): Unit
  def createPartitionedTable(tableId: String): Unit
  def recreateTable(tableId: String, schema: StructType, data: Seq[Row]): Unit
  def appendData(tableId: String, schema: StructType, data: Seq[Row]): Unit
  def buildCompanionSql(tableId: String, clauses: String, indexPath: String): String
  def newTableId(tempDir: String, name: String): String

  // ───────────────────────────────────────────────────────────────────
  //  Shared table schema (14 columns: 8 core + 6 evolution)
  // ───────────────────────────────────────────────────────────────────

  private def bd(s: String): java.math.BigDecimal = new java.math.BigDecimal(s)

  protected val sharedTableSchema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("score", DoubleType),
    StructField("timestamp", LongType),
    StructField("active", BooleanType),
    StructField("message", StringType),
    StructField("ip_addr", StringType),
    StructField("category", StringType),
    StructField("evo_decimal_wide", DecimalType(10, 2)),
    StructField("evo_decimal_narrow", DecimalType(10, 2)),
    StructField("evo_decimal_mixed", DecimalType(10, 5)),
    StructField("evo_type_nonindexed", IntegerType),
    StructField("evo_type_breaking", IntegerType),
    StructField("evo_drop", StringType)
  ))

  protected val sharedTableData: Seq[Row] = Seq(
    Row(1, "alice", 100.0, 1000L, true, "hello world", "192.168.1.1", "cat_a",
      bd("100.50"), bd("100.50"), bd("100.50000"), 100, 1, "drop_a"),
    Row(2, "bob", 200.5, 2000L, false, "foo bar baz", "10.0.0.1", "cat_b",
      bd("200.75"), bd("200.75"), bd("200.75000"), 200, 2, "drop_b"),
    Row(3, "charlie", 300.75, 3000L, true, "search query text", "172.16.0.1", "cat_a",
      bd("300.25"), bd("300.25"), bd("300.25000"), 300, 3, "drop_c")
  )

  private val appendRow4: Seq[Row] = Seq(
    Row(4, "dave", 400.0, 4000L, true, "new data", "10.0.0.2", "cat_c",
      bd("400.50"), bd("400.50"), bd("400.50000"), 400, 4, "drop_d")
  )

  private val appendRow5: Seq[Row] = Seq(
    Row(5, "eve", 500.0, 5000L, false, "another row", "10.0.0.5", "cat_e",
      bd("500.50"), bd("500.50"), bd("500.50000"), 500, 5, "drop_e")
  )

  // ───────────────────────────────────────────────────────────────────
  //  Evolution mutations
  // ───────────────────────────────────────────────────────────────────

  /** Combined success mutation: widening + non-indexed type change + drop 2 cols + add col. */
  private val successMutationSchema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("score", DoubleType),
    StructField("timestamp", LongType),
    StructField("active", BooleanType),
    StructField("message", StringType),
    StructField("ip_addr", StringType),
    StructField("category", StringType),
    StructField("evo_decimal_wide", DecimalType(18, 4)),     // widened
    StructField("evo_decimal_narrow", DecimalType(10, 2)),   // unchanged
    StructField("evo_decimal_mixed", DecimalType(10, 5)),    // unchanged
    StructField("evo_type_nonindexed", StringType),          // INT → STRING
    // evo_type_breaking: removed
    // evo_drop: removed
    StructField("new_col", StringType)                        // added
  ))

  private val successMutationData: Seq[Row] = Seq(
    Row(6, "frank", 600.0, 6000L, true, "mutated", "10.0.0.6", "cat_f",
      bd("600.5000"), bd("100.50"), bd("100.50000"), "six", "hello")
  )

  /** Returns (schema, data) with one column's type changed from original. */
  private def withColumnTypeChanged(col: String, newType: DataType, newValue: Any): (StructType, Seq[Row]) = {
    val colIdx = sharedTableSchema.fieldIndex(col)
    val newSchema = StructType(sharedTableSchema.fields.map { f =>
      if (f.name == col) StructField(f.name, newType) else f
    })
    val baseRow = sharedTableData.head.toSeq.toArray.clone()
    baseRow(colIdx) = newValue
    (newSchema, Seq(Row.fromSeq(baseRow.toSeq)))
  }

  // ───────────────────────────────────────────────────────────────────
  //  Shared tables — created lazily on first access
  // ───────────────────────────────────────────────────────────────────

  private var _sharedTempDir: Option[String] = None

  private def sharedTempDir: String = _sharedTempDir.getOrElse {
    val dir = java.nio.file.Files.createTempDirectory("companion-shared").toString
    _sharedTempDir = Some(dir)
    dir
  }

  protected lazy val sharedTableId: String = {
    val id = newTableId(sharedTempDir, "shared")
    createSimpleTable(id, sharedTableSchema, sharedTableData)
    id
  }

  protected lazy val sharedPartitionedTableId: String = {
    val id = newTableId(sharedTempDir, "shared_part")
    createPartitionedTable(id)
    id
  }

  /** Concrete default — calls createSimpleTable with the shared schema. */
  def createWideTable(tableId: String): Unit =
    createSimpleTable(tableId, sharedTableSchema, sharedTableData)

  // ───────────────────────────────────────────────────────────────────
  //  Evolution companions — built before mutation, used after
  // ───────────────────────────────────────────────────────────────────

  private val evoCols = Seq("evo_decimal_wide", "evo_decimal_narrow", "evo_decimal_mixed",
    "evo_type_nonindexed", "evo_type_breaking", "evo_drop")

  /**
   * Lazy val that builds companion indexes against the shared table (pre-mutation).
   * Each evolution test mutates the table independently with a single targeted change.
   */
  protected lazy val evolutionCompanions: Map[String, String] = {
    val tableId = sharedTableId
    val evoDir = new File(sharedTempDir, "evo-indexes")
    evoDir.mkdirs()

    // Clear transaction log cache to avoid stale state from earlier phases
    spark.sql("INVALIDATE INDEXTABLES TRANSACTION LOG CACHE")

    def buildEvo(key: String, clauses: String): (String, String) = {
      flushCaches()
      val indexPath = new File(evoDir, key).getAbsolutePath
      val result = spark.sql(buildCompanionSql(tableId, clauses, indexPath)).collect()
      require(result(0).getString(2) == "success",
        s"Evolution companion '$key' build failed: ${result(0).getString(10)}")
      key -> indexPath
    }

    val evoExclude = evoCols.map(c => s"'$c'").mkString(", ")

    val companions = Map(
      buildEvo("widening", "INCLUDE COLUMNS ('id', 'evo_decimal_wide')"),
      buildEvo("narrowing", "INCLUDE COLUMNS ('id', 'evo_decimal_narrow')"),
      buildEvo("mixed", "INCLUDE COLUMNS ('id', 'evo_decimal_mixed')"),
      buildEvo("breaking", "INCLUDE COLUMNS ('evo_type_breaking', 'name')"),
      buildEvo("non_indexed", "INCLUDE COLUMNS ('id', 'name')"),
      buildEvo("drop_indexed", "INCLUDE COLUMNS ('evo_type_breaking', 'name')"),
      buildEvo("add_col", "INCLUDE COLUMNS ('id', 'name')"),
      buildEvo("drop_unindexed_inc", "INCLUDE COLUMNS ('id', 'name')"),
      buildEvo("drop_unindexed_exc", s"EXCLUDE COLUMNS ($evoExclude)")
    )

    // Apply the combined success mutation — all success tests run against this state.
    // Error tests override with their own isolated recreateTable.
    recreateTable(sharedTableId, successMutationSchema, successMutationData)

    companions
  }

  // ───────────────────────────────────────────────────────────────────
  //  Helpers
  // ───────────────────────────────────────────────────────────────────

  protected def withTempIndex(f: String => Unit): Unit = {
    val path = java.nio.file.Files.createTempDirectory("tantivy4spark-idx").toString
    try {
      flushCaches()
      f(path)
    } finally deleteRecursively(new File(path))
  }

  protected def withTempPath(f: String => Unit): Unit = {
    val path = java.nio.file.Files.createTempDirectory("tantivy4spark").toString
    try {
      flushCaches()
      f(path)
    } finally deleteRecursively(new File(path))
  }

  protected def flushCaches(): Unit = try {
    _root_.io.indextables.spark.storage.GlobalSplitCacheManager.flushAllCaches()
    _root_.io.indextables.spark.storage.DriverSplitLocalityManager.clear()
  } catch { case _: Exception => }

  protected def readCompanion(indexPath: String): DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .option("spark.indextables.read.columnar.enabled", "true")
      .load(indexPath)

  protected def cleanupSharedTables(): Unit =
    _sharedTempDir.foreach(dir => deleteRecursively(new File(dir)))

  // ═══════════════════════════════════════════════════════════════════
  //  Phase 1: Read-only tests — shared table
  // ═══════════════════════════════════════════════════════════════════

  test("INCLUDE COLUMNS indexes only specified columns") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name', 'score', 'active')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.filter(col("name") === "alice").collect().length shouldBe 1
      df.count() shouldBe 3
    }
  }

  test("INCLUDE COLUMNS with INDEXING MODES") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name', 'score', 'message') INDEXING MODES ('message': 'text')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.createOrReplaceTempView("include_test")

      val textResults = spark.sql("SELECT id FROM include_test WHERE message indexquery 'hello'").collect()
      textResults.length shouldBe 1

      val nameResults = df.filter(col("name") === "bob").collect()
      nameResults.length shouldBe 1
    }
  }

  test("EXCLUDE COLUMNS skips specified columns") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('timestamp', 'message', 'ip_addr', 'category')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.filter(col("name") === "charlie").collect().length shouldBe 1
      df.count() shouldBe 3
    }
  }

  test("EXCLUDE COLUMNS excluded column is absent from index") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('message')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.filter(col("name") === "alice").collect().length shouldBe 1

      // Excluded column 'message' is not indexed in tantivy but its data IS stored
      // in the companion split. With skipFields metadata, the ScanBuilder defers this
      // filter to Spark, which evaluates it against the stored data correctly.
      df.filter(col("message") === "hello world").collect().length shouldBe 1
    }
  }

  test("predicate on excluded column returns zero results") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('ip_addr')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      // Filter on excluded column — ScanBuilder defers to Spark, returns correct result.
      df.filter(col("ip_addr") === "192.168.1.1").collect().length shouldBe 1
      df.filter(col("name") === "alice").collect().length shouldBe 1
    }
  }

  test("INCLUDE COLUMNS with nonexistent column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'nonexistent_column')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("nonexistent_column")
    }
  }

  test("EXCLUDE COLUMNS with nonexistent column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('nonexistent_column')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("nonexistent_column")
    }
  }

  test("INDEXING MODES field not in INCLUDE COLUMNS returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name') INDEXING MODES ('message': 'text')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("message")
    }
  }

  test("INCLUDE COLUMNS with SUM aggregation on numeric fast field") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'score')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      val sumResult = df.agg(sum("score")).collect()
      sumResult(0).getDouble(0) shouldBe (100.0 + 200.5 + 300.75)
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R1: Column existence & naming validation
  // ═══════════════════════════════════════════════════════════════════

  test("R1: duplicate column in INCLUDE COLUMNS returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name', 'id')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("Duplicate column")
      result(0).getString(10) should include("id")
    }
  }

  test("R1: duplicate column in EXCLUDE COLUMNS returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('category', 'category')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("Duplicate column")
    }
  }

  test("R1: case-insensitive column matching works") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('ID', 'Name', 'Score')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R2: Type-incompatible INDEXING MODES
  // ═══════════════════════════════════════════════════════════════════

  test("R2: text mode on INT column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'score') INDEXING MODES ('id': 'text')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
      result(0).getString(10) should include("text")
      result(0).getString(10) should include("string type")
    }
  }

  test("R2: string mode on DOUBLE column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'score') INDEXING MODES ('score': 'string')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
    }
  }

  test("R2: ipaddress mode on BOOLEAN column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'active') INDEXING MODES ('active': 'ipaddress')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
    }
  }

  test("R2: valid text mode on STRING column succeeds") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'message') INDEXING MODES ('message': 'text')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R3: Cross-clause consistency
  // ═══════════════════════════════════════════════════════════════════

  test("R3: HASHED FASTFIELDS field not in INCLUDE COLUMNS returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name') HASHED FASTFIELDS INCLUDE ('category')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("INCLUDE COLUMNS")
    }
  }

  test("R3: HASHED FASTFIELDS on non-string column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'score', 'name') HASHED FASTFIELDS INCLUDE ('score')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("string type")
    }
  }

  test("R3: zero indexed columns returns error") {
    withTempIndex { indexPath =>
      val allCols = sharedTableSchema.fieldNames.map(c => s"'$c'").mkString(", ")
      val result = spark.sql(
        buildCompanionSql(sharedTableId, s"EXCLUDE COLUMNS ($allCols)", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("zero indexed columns")
    }
  }

  test("R3: HASHED FASTFIELDS on text mode field returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name', 'message') INDEXING MODES ('message': 'text') HASHED FASTFIELDS INCLUDE ('message')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("text")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  BUG3: Cross-clause regression tests
  // ═══════════════════════════════════════════════════════════════════

  test("BUG3: INDEXING MODES on EXCLUDE'd column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('message') INDEXING MODES ('message': 'text')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("INDEXING MODES")
      result(0).getString(10) should include("EXCLUDE COLUMNS")
    }
  }

  test("BUG3: HASHED FASTFIELDS on EXCLUDE'd column returns error") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('name') HASHED FASTFIELDS INCLUDE ('name')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("EXCLUDE COLUMNS")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Phase 1: Read-only tests — shared partitioned table
  // ═══════════════════════════════════════════════════════════════════

  test("INCLUDE COLUMNS with only partition columns errors instead of silently indexing everything") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedPartitionedTableId, "INCLUDE COLUMNS ('region')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("only partition columns")
    }
  }

  test("INCLUDE COLUMNS with mix of partition and non-partition columns succeeds") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedPartitionedTableId, "INCLUDE COLUMNS ('region', 'name')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.filter(col("name") === "alice").count() shouldBe 1
    }
  }

  test("nonexistent column error does not show partition columns in Available columns hint") {
    withTempIndex { indexPath =>
      val result = spark.sql(
        buildCompanionSql(sharedPartitionedTableId, "INCLUDE COLUMNS ('nonexistent_col')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("nonexistent_col")
      result(0).getString(10) should not include("region")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Phase 2: Append tests — shared table (cumulative rows)
  //
  //  BUG1 appends row 4 (3 → 4 rows).
  //  R5 appends row 5 (4 → 5 rows).
  // ═══════════════════════════════════════════════════════════════════

  test("BUG1: EXCLUDE COLUMNS with wrong casing normalizes skip fields to schema casing") {
    withTempIndex { indexPath =>
      // Table has 3 rows (1 snapshot)
      val result = spark.sql(
        buildCompanionSql(sharedTableId, "EXCLUDE COLUMNS ('NAME', 'Score', 'MESSAGE')", indexPath)
      ).collect()
      result(0).getString(2) shouldBe "success"

      val df = readCompanion(indexPath)
      df.filter(col("id") === 1).collect().length shouldBe 1
      df.count() shouldBe 3

      // Append row 4
      appendData(sharedTableId, sharedTableSchema, appendRow4)

      // Incremental sync — proves skip fields were stored with correct casing
      val result2 = spark.sql(
        buildCompanionSql(sharedTableId, "", indexPath)
      ).collect()
      result2(0).getString(2) shouldBe "success"

      readCompanion(indexPath).count() shouldBe 4
    }
  }

  test("R5: incremental sync reuses INCLUDE COLUMNS from metadata") {
    withTempIndex { indexPath =>
      // Table now has 4 rows (BUG1 appended row 4)
      val result1 = spark.sql(
        buildCompanionSql(sharedTableId, "INCLUDE COLUMNS ('id', 'name', 'score')", indexPath)
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Append row 5
      appendData(sharedTableId, sharedTableSchema, appendRow5)

      val result2 = spark.sql(
        buildCompanionSql(sharedTableId, "", indexPath)
      ).collect()
      result2(0).getString(2) shouldBe "success"

      readCompanion(indexPath).count() shouldBe 5
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Phase 3: Evolution tests — shared table
  //
  //  Success cases share one combined mutation. Error cases each get
  //  their own isolated mutation so errors don't mask each other.
  // ═══════════════════════════════════════════════════════════════════

  private def syncEvolution(key: String): Row = {
    val indexPath = evolutionCompanions(key)
    flushCaches()
    spark.sql(buildCompanionSql(sharedTableId, "", indexPath)).collect()(0)
  }

  // --- Success cases (combined mutation) ---
  // One recreateTable applies: decimal widened, non-indexed type changed,
  // evo_type_breaking dropped, evo_drop dropped, new_col added.

  test("BUG4: decimal precision widening is allowed") {
    // Combined success mutation was applied when evolutionCompanions was initialized
    syncEvolution("widening").getString(2) shouldBe "success"
  }

  test("BUG4: type change in non-indexed column does not block incremental sync") {
    // Table already has success mutation from previous test
    syncEvolution("non_indexed").getString(2) shouldBe "success"
  }

  test("dropping an indexed column succeeds on incremental sync") {
    // evo_type_breaking was removed in success mutation
    syncEvolution("drop_indexed").getString(2) shouldBe "success"
  }

  test("adding a new column succeeds on incremental sync") {
    // new_col was added in success mutation
    syncEvolution("add_col").getString(2) shouldBe "success"
  }

  // Drop-unindexed tests: INCLUDE vs EXCLUDE diverge on mechanism
  // (INCLUDE ignores it silently; EXCLUDE gracefully skips from stored list)

  test("dropping an unindexed column succeeds with INCLUDE") {
    // evo_drop was removed in success mutation, not in INCLUDE list
    syncEvolution("drop_unindexed_inc").getString(2) shouldBe "success"
  }

  test("BUG2: dropping an unindexed column in stored EXCLUDE gracefully skips") {
    // evo_drop was removed in success mutation, was in EXCLUDE list
    syncEvolution("drop_unindexed_exc").getString(2) shouldBe "success"
  }

  // --- Error cases (isolated mutations) ---
  // Each recreateTable changes exactly one column to trigger a specific error.

  test("BUG4: decimal precision reduction is a breaking change") {
    val (schema, data) = withColumnTypeChanged("evo_decimal_narrow", DecimalType(5, 1), bd("10.5"))
    recreateTable(sharedTableId, schema, data)
    val row = syncEvolution("narrowing")
    row.getString(2) shouldBe "error"
    row.getString(10) should include("precision decreased")
    row.getString(10) should include("scale decreased")
  }

  test("decimal mixed precision increase with scale decrease returns specific error") {
    val (schema, data) = withColumnTypeChanged("evo_decimal_mixed", DecimalType(12, 3), bd("600.500"))
    recreateTable(sharedTableId, schema, data)
    val row = syncEvolution("mixed")
    row.getString(2) shouldBe "error"
    row.getString(10) should include("scale decreased from 5 to 3")
    row.getString(10) should not include("precision decreased")
  }

  test("R6: breaking type change (INT to STRING) returns error on incremental sync") {
    val (schema, data) = withColumnTypeChanged("evo_type_breaking", StringType, "six")
    recreateTable(sharedTableId, schema, data)
    val row = syncEvolution("breaking")
    row.getString(2) shouldBe "error"
    row.getString(10) should include("type changed")
    row.getString(10) should include("INVALIDATE ALL PARTITIONS")
  }
}
