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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Reproduction test for tantivy4java bug: Schema.getFieldInfo() returns FieldType.TEXT
 * for all Str fields regardless of tokenizer. Fields with the "raw" tokenizer (string fields)
 * should return FieldType.STRING (value 11), but they incorrectly return FieldType.TEXT (1).
 *
 * This causes FiltersToQueryConverter to generate phrase queries (full_text with phrase mode)
 * instead of term queries for string fields, which produces incorrect results for values
 * containing special characters (backslashes, colons, parentheses, etc.).
 *
 * See TANTIVY4JAVA_STRING_FIELD_TYPE_BUG.md for root cause details.
 */
class StringFieldQueryTypeBugTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val FORMAT = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("StringFieldQueryTypeBugTest")
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
    val path = Files.createTempDirectory("tantivy4spark-string-bug").toString
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

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Simple hostname values — baseline
  // ═══════════════════════════════════════════════════════════════════

  test("EqualTo on string field in companion split should return only matching rows") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "companion").getAbsolutePath

      spark.sql(s"""CREATE TABLE delta.`$deltaPath` (
                   |  id LONG,
                   |  hostname STRING,
                   |  category STRING,
                   |  message STRING
                   |) USING DELTA""".stripMargin)

      spark.sql(s"""INSERT INTO delta.`$deltaPath` VALUES
                   |  (1, 'BLA33SL01', 'production',  'System started successfully'),
                   |  (2, 'BLA33SL02', 'staging',     'Disk space warning detected'),
                   |  (3, 'BLA33SL01', 'production',  'CPU utilization normal'),
                   |  (4, 'XYZ99AB03', 'development', 'Scheduled maintenance window'),
                   |  (5, 'BLA33SL01', 'production',  'Network connectivity restored')
                   |""".stripMargin)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' " +
          s"INDEXING MODES ('message': 'text') AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.companion.requireAggregatePushdown", "false")
        .load(indexPath)

      df.count() shouldBe 5

      val filtered = df.filter(col("hostname") === "BLA33SL01").collect()
      println(s"hostname='BLA33SL01': ${filtered.length} rows (expected 3)")
      filtered.length shouldBe 3
      filtered.foreach(_.getAs[String]("hostname") shouldBe "BLA33SL01")
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Values with special characters — Windows paths, process names
  //  These are the real-world values that break with the phrase query
  //  path because the query parser interprets backslashes, colons, etc.
  // ═══════════════════════════════════════════════════════════════════

  test("EqualTo on string field with backslash values should return correct results") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_bs").getAbsolutePath
      val indexPath = new File(tempDir, "companion_bs").getAbsolutePath

      val ss = spark
      import ss.implicits._

      // Create Delta table with Windows-style paths (backslashes)
      Seq(
        (1L, """C:\Windows\System32\svchost.exe""", "svchost started"),
        (2L, """C:\Windows\System32\cmd.exe""", "command prompt opened"),
        (3L, """C:\Windows\System32\svchost.exe""", "svchost network activity"),
        (4L, """C:\Program Files\app.exe""", "application launched"),
        (5L, """C:\Windows\System32\cmd.exe""", "command executed")
      ).toDF("id", "process_executable", "message")
        .write
        .format("delta")
        .save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' " +
          s"INDEXING MODES ('message': 'text') AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.companion.requireAggregatePushdown", "false")
        .load(indexPath)

      df.count() shouldBe 5

      // Filter on process_executable (string field) with a backslash-containing value
      // With the bug, parseQuery("process_executable:\"C:\Windows\System32\svchost.exe\"")
      // may misinterpret backslashes as escape characters, returning wrong results.
      // With the fix, SplitTermQuery("process_executable", "C:\Windows\System32\svchost.exe")
      // does exact byte matching.
      val svchost = df
        .filter(col("process_executable") === """C:\Windows\System32\svchost.exe""")
        .collect()
      println(s"process_executable='C:\\Windows\\System32\\svchost.exe': ${svchost.length} rows (expected 2)")
      svchost.length shouldBe 2
      svchost.foreach(_.getAs[String]("process_executable") shouldBe """C:\Windows\System32\svchost.exe""")

      val cmd = df
        .filter(col("process_executable") === """C:\Windows\System32\cmd.exe""")
        .collect()
      println(s"process_executable='C:\\Windows\\System32\\cmd.exe': ${cmd.length} rows (expected 2)")
      cmd.length shouldBe 2
    }
  }

  test("EqualTo on string field with parentheses and special chars should return correct results") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_sp").getAbsolutePath
      val indexPath = new File(tempDir, "companion_sp").getAbsolutePath

      val ss = spark
      import ss.implicits._

      // Create Delta table with values containing query parser metacharacters
      Seq(
        (1L, "user@example.com", "admin (root)", "login event"),
        (2L, "admin@corp.local", "service [sys]", "service start"),
        (3L, "user@example.com", "admin (root)", "logout event"),
        (4L, "test+user@dev.io", "app {daemon}", "test event"),
        (5L, "admin@corp.local", "monitor~1", "health check")
      ).toDF("id", "email", "account_name", "message")
        .write
        .format("delta")
        .save(deltaPath)

      val syncResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' " +
          s"INDEXING MODES ('message': 'text') AT LOCATION '$indexPath'"
      )
      syncResult.collect()(0).getString(2) shouldBe "success"

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.read.companion.requireAggregatePushdown", "false")
        .load(indexPath)

      // Filter on email with @ character
      val byEmail = df.filter(col("email") === "user@example.com").collect()
      println(s"email='user@example.com': ${byEmail.length} rows (expected 2)")
      byEmail.length shouldBe 2

      // Filter on account_name with parentheses
      val byAccount = df.filter(col("account_name") === "admin (root)").collect()
      println(s"account_name='admin (root)': ${byAccount.length} rows (expected 2)")
      byAccount.length shouldBe 2

      // Filter on account_name with brackets
      val byBrackets = df.filter(col("account_name") === "service [sys]").collect()
      println(s"account_name='service [sys]': ${byBrackets.length} rows (expected 1)")
      byBrackets.length shouldBe 1

      // Filter on email with + character
      val byPlus = df.filter(col("email") === "test+user@dev.io").collect()
      println(s"email='test+user@dev.io': ${byPlus.length} rows (expected 1)")
      byPlus.length shouldBe 1

      // Filter on account_name with ~ character
      val byTilde = df.filter(col("account_name") === "monitor~1").collect()
      println(s"account_name='monitor~1': ${byTilde.length} rows (expected 1)")
      byTilde.length shouldBe 1
    }
  }
}
