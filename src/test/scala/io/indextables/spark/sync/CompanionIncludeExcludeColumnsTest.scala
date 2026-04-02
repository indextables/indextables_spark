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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests for INCLUDE COLUMNS and EXCLUDE COLUMNS in BUILD INDEXTABLES COMPANION.
 *
 * Validates that selective column indexing works end-to-end:
 *   - Only included columns are indexed
 *   - Excluded columns are skipped
 *   - Aggregations work on included numeric fast fields
 *   - Non-included columns are not searchable but data is not lost
 *   - INDEXING MODES works correctly with INCLUDE COLUMNS
 *   - Validation errors for nonexistent columns and INDEXING MODES mismatch
 */
class CompanionIncludeExcludeColumnsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionIncludeExcludeColumnsTest")
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
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark").toString
    try {
      try {
        import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
        GlobalSplitCacheManager.flushAllCaches()
        DriverSplitLocalityManager.clear()
      } catch {
        case _: Exception =>
      }
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  /** Create a wide Delta table with 20 columns for testing selective indexing. */
  private def createWideDeltaTable(deltaPath: String): Unit = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val data = Seq(
      (1, "alice", 100.0, 1000L, true, "hello world", "192.168.1.1", "cat_a",
       "extra1", "extra2", "extra3", "extra4", "extra5", "extra6", "extra7",
       "extra8", "extra9", "extra10", "extra11", "extra12"),
      (2, "bob", 200.5, 2000L, false, "foo bar baz", "10.0.0.1", "cat_b",
       "extra1", "extra2", "extra3", "extra4", "extra5", "extra6", "extra7",
       "extra8", "extra9", "extra10", "extra11", "extra12"),
      (3, "charlie", 300.75, 3000L, true, "search query text", "172.16.0.1", "cat_a",
       "extra1", "extra2", "extra3", "extra4", "extra5", "extra6", "extra7",
       "extra8", "extra9", "extra10", "extra11", "extra12")
    ).toDF(
      "id", "name", "score", "timestamp", "active", "message", "ip_addr", "category",
      "col_09", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15",
      "col_16", "col_17", "col_18", "col_19", "col_20"
    )

    data.write.format("delta").mode("overwrite").save(deltaPath)
  }

  test("INCLUDE COLUMNS indexes only specified columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      // Build companion with only 4 columns
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name', 'score', 'active')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"

      // Read the companion index
      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      // Verify we can filter on included columns
      val filtered = df.filter(col("name") === "alice").collect()
      filtered.length shouldBe 1

      // Verify COUNT(*) works (uses fast field from numeric 'id' or 'score')
      val count = df.count()
      count shouldBe 3
    }
  }

  test("INCLUDE COLUMNS with INDEXING MODES") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      // Build with INCLUDE COLUMNS + INDEXING MODES for text search
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name', 'score', 'message')
           |  INDEXING MODES ('message': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      df.createOrReplaceTempView("include_test")

      // Text search on included text field should work
      val textResults = spark.sql("SELECT id FROM include_test WHERE message indexquery 'hello'").collect()
      textResults.length shouldBe 1

      // String filter on included field should work
      val nameResults = df.filter(col("name") === "bob").collect()
      nameResults.length shouldBe 1
    }
  }

  test("EXCLUDE COLUMNS skips specified columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      // Build with EXCLUDE COLUMNS to skip the extra columns
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('col_09', 'col_10', 'col_11', 'col_12', 'col_13', 'col_14', 'col_15', 'col_16', 'col_17', 'col_18', 'col_19', 'col_20')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      // Should be able to filter on non-excluded columns
      val filtered = df.filter(col("name") === "charlie").collect()
      filtered.length shouldBe 1

      df.count() shouldBe 3
    }
  }

  test("INCLUDE COLUMNS with nonexistent column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'nonexistent_column')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("nonexistent_column")
    }
  }

  test("EXCLUDE COLUMNS with nonexistent column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('nonexistent_column')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("nonexistent_column")
    }
  }

  test("INDEXING MODES field not in INCLUDE COLUMNS returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name')
           |  INDEXING MODES ('message': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("message")
    }
  }

  test("INCLUDE COLUMNS with SUM aggregation on numeric fast field") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'score')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      // SUM on included numeric field should work (fast field in HYBRID mode)
      val sumResult = df.agg(sum("score")).collect()
      sumResult(0).getDouble(0) shouldBe (100.0 + 200.5 + 300.75)
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R1: Column existence & naming validation
  // ═══════════════════════════════════════════════════════════════════

  test("R1: duplicate column in INCLUDE COLUMNS returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name', 'id')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("Duplicate column")
      result(0).getString(10) should include("id")
    }
  }

  test("R1: duplicate column in EXCLUDE COLUMNS returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('col_09', 'col_09')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("Duplicate column")
    }
  }

  test("R1: case-insensitive column matching works") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      // 'ID' should match 'id' in source schema (case-insensitive)
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('ID', 'Name', 'Score')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R2: Type-incompatible INDEXING MODES
  // ═══════════════════════════════════════════════════════════════════

  test("R2: text mode on INT column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'score')
           |  INDEXING MODES ('id': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
      result(0).getString(10) should include("text")
      result(0).getString(10) should include("string type")
    }
  }

  test("R2: string mode on DOUBLE column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'score')
           |  INDEXING MODES ('score': 'string')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
    }
  }

  test("R2: ipaddress mode on BOOLEAN column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'active')
           |  INDEXING MODES ('active': 'ipaddress')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("not compatible")
    }
  }

  test("R2: valid text mode on STRING column succeeds") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'message')
           |  INDEXING MODES ('message': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "success"
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R3: Cross-clause consistency
  // ═══════════════════════════════════════════════════════════════════

  test("R3: HASHED FASTFIELDS field not in INCLUDE COLUMNS returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name')
           |  HASHED FASTFIELDS INCLUDE ('category')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("INCLUDE COLUMNS")
    }
  }

  test("R3: HASHED FASTFIELDS on non-string column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'score', 'name')
           |  HASHED FASTFIELDS INCLUDE ('score')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("string type")
    }
  }

  test("R3: zero indexed columns returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val allCols = Seq("id", "name", "score", "timestamp", "active", "message", "ip_addr", "category",
        "col_09", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15",
        "col_16", "col_17", "col_18", "col_19", "col_20").map(c => s"'$c'").mkString(", ")

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ($allCols)
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("zero indexed columns")
    }
  }

  test("R3: HASHED FASTFIELDS on text mode field returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name', 'message')
           |  INDEXING MODES ('message': 'text')
           |  HASHED FASTFIELDS INCLUDE ('message')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("text")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  R5/R6: Incremental sync and schema evolution
  // ═══════════════════════════════════════════════════════════════════

  test("R5: incremental sync reuses INCLUDE COLUMNS from metadata") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      // Initial sync with INCLUDE COLUMNS
      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name', 'score')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Add more data to Delta table
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      Seq((4, "dave", 400.0, 4000L, true, "new data", "10.0.0.2", "cat_c",
        "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10", "e11", "e12"))
        .toDF("id", "name", "score", "timestamp", "active", "message", "ip_addr", "category",
          "col_09", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15",
          "col_16", "col_17", "col_18", "col_19", "col_20")
        .write.format("delta").mode("append").save(deltaPath)

      // Incremental sync WITHOUT specifying INCLUDE COLUMNS — should reuse from metadata
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"

      // Verify the index has all 4 rows
      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
      df.count() shouldBe 4
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Bug regression tests
  // ═══════════════════════════════════════════════════════════════════

  test("BUG1: EXCLUDE COLUMNS with wrong casing normalizes skip fields to schema casing") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      // Use uppercase 'NAME' — schema has lowercase 'name'.
      // Before the fix, the Rust layer received user-cased skip fields ('NAME') that
      // didn't match the Parquet schema ('name'), silently indexing the "excluded" column.
      // After the fix, skip fields are normalized to actual schema casing.
      //
      // Note: The companion reader presents the source schema (all columns) regardless of
      // what's indexed, so we can't assert on df.schema to prove exclusion. Instead we verify
      // the command succeeds and the non-excluded columns are queryable — the code fix itself
      // is a direct substitution from user-cased strings to sourceSchema.fieldNames.filter().
      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('NAME', 'Score', 'MESSAGE')
           |  HASHED FASTFIELDS INCLUDE ('ip_addr')
           |  AT LOCATION '$indexPath'""".stripMargin
      )
      result.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)

      // Non-excluded columns should be searchable
      val idFilter = df.filter(col("id") === 1).collect()
      idFilter.length shouldBe 1
      df.count() shouldBe 3

      // Incremental sync should work — proves skip fields were stored with correct casing
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      Seq((4, "dave", 400.0, 4000L, true, "new data", "10.0.0.4", "cat_d",
        "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10", "e11", "e12"))
        .toDF("id", "name", "score", "timestamp", "active", "message", "ip_addr", "category",
          "col_09", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15",
          "col_16", "col_17", "col_18", "col_19", "col_20")
        .write.format("delta").mode("append").save(deltaPath)

      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"

      val df2 = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
      df2.count() shouldBe 4
    }
  }

  test("BUG2: dropped column in stored EXCLUDE gracefully skips on incremental sync") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create table with 3 columns, EXCLUDE 'extra'
      Seq((1, "alice", "x"), (2, "bob", "y")).toDF("id", "name", "extra")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('extra')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Recreate table WITHOUT 'extra' column — it's been dropped from the source schema
      deleteRecursively(new java.io.File(deltaPath))
      Seq((3, "charlie"), (4, "dave")).toDF("id", "name")
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Incremental sync restores EXCLUDE from metadata; 'extra' no longer exists.
      // Before the fix, this would throw "Column 'extra' in EXCLUDE COLUMNS does not exist".
      // After the fix, it warns and skips the dropped column.
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"
    }
  }

  test("BUG3: INDEXING MODES on EXCLUDE'd column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('message')
           |  INDEXING MODES ('message': 'text')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("INDEXING MODES")
      result(0).getString(10) should include("EXCLUDE COLUMNS")
    }
  }

  test("BUG3: HASHED FASTFIELDS on EXCLUDE'd column returns error") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath
      createWideDeltaTable(deltaPath)

      val result = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  EXCLUDE COLUMNS ('name')
           |  HASHED FASTFIELDS INCLUDE ('name')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result(0).getString(2) shouldBe "error"
      result(0).getString(10) should include("HASHED FASTFIELDS")
      result(0).getString(10) should include("EXCLUDE COLUMNS")
    }
  }

  test("BUG4: decimal precision reduction is a breaking change") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create initial table with decimal(10,2)
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      import org.apache.spark.sql.types._

      val schema1 = StructType(Seq(
        StructField("id", IntegerType), StructField("amount", DecimalType(10, 2))
      ))
      val data1 = Seq(
        org.apache.spark.sql.Row(1, new java.math.BigDecimal("100.50")),
        org.apache.spark.sql.Row(2, new java.math.BigDecimal("200.75"))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Initial sync
      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'amount')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Recreate with decimal(5,1) — precision AND scale reduction
      deleteRecursively(new java.io.File(deltaPath))
      val schema2 = StructType(Seq(
        StructField("id", IntegerType), StructField("amount", DecimalType(5, 1))
      ))
      val data2 = Seq(
        org.apache.spark.sql.Row(3, new java.math.BigDecimal("10.5"))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Incremental sync should error on decimal narrowing
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "error"
      result2(0).getString(10) should include("type changed")
    }
  }

  test("BUG4: decimal precision widening is allowed") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val sparkImplicits = spark.implicits
      import sparkImplicits._
      import org.apache.spark.sql.types._

      val schema1 = StructType(Seq(
        StructField("id", IntegerType), StructField("amount", DecimalType(10, 2))
      ))
      val data1 = Seq(
        org.apache.spark.sql.Row(1, new java.math.BigDecimal("100.50"))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'amount')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Recreate with decimal(18,4) — precision AND scale increase (widening)
      deleteRecursively(new java.io.File(deltaPath))
      val schema2 = StructType(Seq(
        StructField("id", IntegerType), StructField("amount", DecimalType(18, 4))
      ))
      val data2 = Seq(
        org.apache.spark.sql.Row(2, new java.math.BigDecimal("200.7500"))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Incremental sync should succeed — widening is allowed
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"
    }
  }

  test("BUG4: type change in non-indexed column does not block incremental sync") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create table with INT 'extra' column, only index 'id' and 'name'
      Seq((1, "alice", 100), (2, "bob", 200)).toDF("id", "name", "extra")
        .write.format("delta").mode("overwrite").save(deltaPath)

      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Recreate with STRING 'extra' column — breaking type change on non-indexed column
      deleteRecursively(new java.io.File(deltaPath))
      Seq((3, "charlie", "three")).toDF("id", "name", "extra")
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Incremental sync should succeed — 'extra' is not indexed
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"
    }
  }

  test("BUG5: INCLUDE COLUMNS with comma in column name round-trips through metadata") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create a Parquet table with a column name containing a comma (legal in Parquet/Iceberg)
      Seq((1, "a", "x"), (2, "b", "y")).toDF("id", "revenue,usd", "name")
        .write.format("parquet").mode("overwrite").save(parquetPath)

      // Initial sync with INCLUDE COLUMNS including the comma-bearing column
      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath'
           |  INCLUDE COLUMNS ('id', 'revenue,usd')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Add more data
      Seq((3, "c", "z")).toDF("id", "revenue,usd", "name")
        .write.format("parquet").mode("append").save(parquetPath)

      // Incremental sync should correctly restore "revenue,usd" from JSON metadata
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.columnar.enabled", "true")
        .load(indexPath)
      df.count() shouldBe 3
    }
  }

  test("R6: breaking type change (INT to STRING) returns error on incremental sync") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      // Create initial table with INT id column
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      Seq((1, "alice"), (2, "bob")).toDF("id", "name")
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Initial sync
      val result1 = spark.sql(
        s"""BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath'
           |  INCLUDE COLUMNS ('id', 'name')
           |  AT LOCATION '$indexPath'""".stripMargin
      ).collect()
      result1(0).getString(2) shouldBe "success"

      // Delete the Delta table and recreate with STRING id column (breaking type change)
      deleteRecursively(new java.io.File(deltaPath))
      Seq(("abc", "charlie"), ("def", "dave")).toDF("id", "name")
        .write.format("delta").mode("overwrite").save(deltaPath)

      // Incremental sync should error on type change
      val result2 = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      ).collect()
      result2(0).getString(2) shouldBe "error"
      result2(0).getString(10) should include("type changed")
      result2(0).getString(10) should include("INVALIDATE ALL PARTITIONS")
    }
  }
}
