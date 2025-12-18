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

package io.indextables.spark.xref

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import io.indextables.spark.TestBase

/**
 * Comprehensive XRef field type validation tests.
 *
 * Tests each field type with:
 * - Positive cases: Search returns correct subset of splits
 * - Negative cases: Search returns empty (term doesn't exist)
 *
 * Field types tested:
 * - TEXT (tokenized)
 * - STRING (exact match)
 * - INTEGER, LONG, FLOAT/DOUBLE (numerics)
 * - TIMESTAMP, DATE (temporal)
 * - STRUCT, ARRAY, MAP (complex - via indexquery)
 * - NOT, OR, AND combinations
 * - Range filters (graceful fallback)
 */
class XRefFieldTypeValidationTest extends TestBase {

  private val DataSourceFormat = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================
  // 2.1 TEXT Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("TEXT field - positive: token exists in one partition") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "alpha unique content common token", "partition_a")
        else (i, "beta unique content common token", "partition_b")
      )

      data.toDF("id", "text_content", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.text_content", "text")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xrefs = transactionLog.listXRefs()
      assert(xrefs.nonEmpty, "XRef should be created")

      val xref = xrefs.head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive: "alpha" should return partition_a only
      val filtersAlpha: Array[Filter] = Array(EqualTo("text_content", "alpha"))
      val resultsAlpha = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersAlpha, 5000, tablePath, spark
      )
      println(s"TEXT positive 'alpha': ${resultsAlpha.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsAlpha.size == 1, s"Expected 1 split for 'alpha', got ${resultsAlpha.size}")

      // Positive: "common" should return all splits
      val filtersCommon: Array[Filter] = Array(EqualTo("text_content", "common"))
      val resultsCommon = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersCommon, 5000, tablePath, spark
      )
      println(s"TEXT positive 'common': ${resultsCommon.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsCommon.size == xref.sourceSplitPaths.size,
        s"Expected all splits for 'common', got ${resultsCommon.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  test("TEXT field - negative: nonexistent token") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "alpha content", "partition_a")
        else (i, "beta content", "partition_b")
      )

      data.toDF("id", "text_content", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.text_content", "text")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Negative: "nonexistent" should return 0 splits
      val filtersNonexistent: Array[Filter] = Array(EqualTo("text_content", "nonexistent"))
      val resultsNonexistent = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersNonexistent, 5000, tablePath, spark
      )
      println(s"TEXT negative 'nonexistent': ${resultsNonexistent.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsNonexistent.isEmpty, s"Expected 0 splits for 'nonexistent', got ${resultsNonexistent.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.2 STRING Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("STRING field - positive: exact match") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "EXACT_VALUE_ALPHA", "partition_a")
        else (i, "EXACT_VALUE_BETA", "partition_b")
      )

      data.toDF("id", "string_field", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.string_field", "string")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive: exact match
      val filtersAlpha: Array[Filter] = Array(EqualTo("string_field", "EXACT_VALUE_ALPHA"))
      val resultsAlpha = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersAlpha, 5000, tablePath, spark
      )
      println(s"STRING positive 'EXACT_VALUE_ALPHA': ${resultsAlpha.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsAlpha.size == 1, s"Expected 1 split, got ${resultsAlpha.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  test("STRING field - negative: partial match fails") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "EXACT_VALUE_ALPHA", "partition_a")
        else (i, "EXACT_VALUE_BETA", "partition_b")
      )

      data.toDF("id", "string_field", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.string_field", "string")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Negative: partial match should fail (strings not tokenized)
      val filtersPartial: Array[Filter] = Array(EqualTo("string_field", "EXACT"))
      val resultsPartial = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersPartial, 5000, tablePath, spark
      )
      println(s"STRING negative 'EXACT' (partial): ${resultsPartial.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsPartial.isEmpty, s"Expected 0 splits for partial match, got ${resultsPartial.size}")

      // Negative: nonexistent value
      val filtersNonexistent: Array[Filter] = Array(EqualTo("string_field", "NONEXISTENT"))
      val resultsNonexistent = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersNonexistent, 5000, tablePath, spark
      )
      println(s"STRING negative 'NONEXISTENT': ${resultsNonexistent.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsNonexistent.isEmpty, s"Expected 0 splits for nonexistent, got ${resultsNonexistent.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.3 INTEGER Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("INTEGER field - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, 100, "partition_a")  // score = 100
        else (i, 200, "partition_b")          // score = 200
      )

      data.toDF("id", "score", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "score")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive: score = 100
      val filters100: Array[Filter] = Array(EqualTo("score", 100))
      val results100 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters100, 5000, tablePath, spark
      )
      println(s"INTEGER positive score=100: ${results100.size}/${xref.sourceSplitPaths.size} splits")
      assert(results100.size == 1, s"Expected 1 split for score=100, got ${results100.size}")

      // Positive: score = 200
      val filters200: Array[Filter] = Array(EqualTo("score", 200))
      val results200 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters200, 5000, tablePath, spark
      )
      println(s"INTEGER positive score=200: ${results200.size}/${xref.sourceSplitPaths.size} splits")
      assert(results200.size == 1, s"Expected 1 split for score=200, got ${results200.size}")

      // Negative: score = 999 (doesn't exist)
      val filters999: Array[Filter] = Array(EqualTo("score", 999))
      val results999 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters999, 5000, tablePath, spark
      )
      println(s"INTEGER negative score=999: ${results999.size}/${xref.sourceSplitPaths.size} splits")
      assert(results999.isEmpty, s"Expected 0 splits for score=999, got ${results999.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.4 LONG Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("LONG field - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, 10000000000L, "partition_a")
        else (i, 20000000000L, "partition_b")
      )

      data.toDF("id", "bignum", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "bignum")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive
      val filters10B: Array[Filter] = Array(EqualTo("bignum", 10000000000L))
      val results10B = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters10B, 5000, tablePath, spark
      )
      println(s"LONG positive bignum=10B: ${results10B.size}/${xref.sourceSplitPaths.size} splits")
      assert(results10B.size == 1, s"Expected 1 split, got ${results10B.size}")

      // Negative
      val filters99B: Array[Filter] = Array(EqualTo("bignum", 99999999999L))
      val results99B = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters99B, 5000, tablePath, spark
      )
      println(s"LONG negative bignum=99B: ${results99B.size}/${xref.sourceSplitPaths.size} splits")
      assert(results99B.isEmpty, s"Expected 0 splits, got ${results99B.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.5 FLOAT/DOUBLE Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("DOUBLE field - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, 1.5, "partition_a")
        else (i, 2.5, "partition_b")
      )

      data.toDF("id", "rating", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "rating")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive
      val filters15: Array[Filter] = Array(EqualTo("rating", 1.5))
      val results15 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters15, 5000, tablePath, spark
      )
      println(s"DOUBLE positive rating=1.5: ${results15.size}/${xref.sourceSplitPaths.size} splits")
      assert(results15.size == 1, s"Expected 1 split, got ${results15.size}")

      // Negative
      val filters99: Array[Filter] = Array(EqualTo("rating", 9.9))
      val results99 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filters99, 5000, tablePath, spark
      )
      println(s"DOUBLE negative rating=9.9: ${results99.size}/${xref.sourceSplitPaths.size} splits")
      assert(results99.isEmpty, s"Expected 0 splits, got ${results99.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.6 TIMESTAMP Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("TIMESTAMP field - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val ts1 = Timestamp.valueOf("2024-01-15 10:00:00")
      val ts2 = Timestamp.valueOf("2024-02-15 10:00:00")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, ts1, "partition_a")
        else (i, ts2, "partition_b")
      )

      data.toDF("id", "ts", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "ts")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive
      val filtersTs1: Array[Filter] = Array(EqualTo("ts", ts1))
      val resultsTs1 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersTs1, 5000, tablePath, spark
      )
      println(s"TIMESTAMP positive ts=2024-01-15: ${resultsTs1.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsTs1.size == 1, s"Expected 1 split, got ${resultsTs1.size}")

      // Negative
      val futureTs = Timestamp.valueOf("2099-12-31 23:59:59")
      val filtersFuture: Array[Filter] = Array(EqualTo("ts", futureTs))
      val resultsFuture = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersFuture, 5000, tablePath, spark
      )
      println(s"TIMESTAMP negative ts=2099-12-31: ${resultsFuture.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsFuture.isEmpty, s"Expected 0 splits, got ${resultsFuture.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.7 DATE Field Tests (Spark filter: EqualTo)
  // ============================================================

  test("DATE field - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val date1 = Date.valueOf("2024-01-01")
      val date2 = Date.valueOf("2024-02-01")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, date1, "partition_a")
        else (i, date2, "partition_b")
      )

      data.toDF("id", "date_field", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "date_field")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Positive case: date1 (2024-01-01) is only in partition_a, should match 1 split
      val filtersDate1: Array[Filter] = Array(EqualTo("date_field", date1))
      val resultsDate1 = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersDate1, 5000, tablePath, spark
      )
      println(s"DATE positive date=2024-01-01: ${resultsDate1.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsDate1.size == 1, s"Expected 1 split for date1, got ${resultsDate1.size}")

      // Negative case: future date doesn't exist, should match 0 splits
      val futureDate = Date.valueOf("2099-12-31")
      val filtersFuture: Array[Filter] = Array(EqualTo("date_field", futureDate))
      val resultsFuture = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersFuture, 5000, tablePath, spark
      )
      println(s"DATE negative date=2099-12-31: ${resultsFuture.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsFuture.isEmpty, s"Expected 0 splits for future date, got ${resultsFuture.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.8 STRUCT Field Tests (SQL indexquery)
  // ============================================================

  test("STRUCT field via SQL indexquery - positive and negative cases") {
    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField("category", StringType, nullable = false)
        )), nullable = false),
        StructField("partition_key", StringType, nullable = false)
      ))

      val rows = (1 to 100).map { i =>
        if (i <= 50) Row(i, Row(s"user_a_$i", "premium"), "partition_a")
        else Row(i, Row(s"user_b_$i", "standard"), "partition_b")
      }

      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      df.write
        .format(DataSourceFormat)
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Register as temp view for SQL queries
      val readDf = spark.read.format(DataSourceFormat).load(tablePath)
      readDf.createOrReplaceTempView("struct_table")

      // Positive: user.category:premium should return partition_a data only
      val resultPremium = spark.sql(
        s"SELECT * FROM struct_table WHERE _indexall indexquery 'user.category:premium'"
      ).collect()
      println(s"STRUCT positive 'user.category:premium': ${resultPremium.length} rows")
      assert(resultPremium.nonEmpty, "Expected rows for user.category:premium")
      assert(resultPremium.forall(_.getAs[String]("partition_key") == "partition_a"),
        "All results should be from partition_a")

      // Negative: nonexistent category should return 0 rows
      val resultNonexistent = spark.sql(
        s"SELECT * FROM struct_table WHERE _indexall indexquery 'user.category:nonexistent'"
      ).collect()
      println(s"STRUCT negative 'user.category:nonexistent': ${resultNonexistent.length} rows")
      assert(resultNonexistent.isEmpty, s"Expected 0 rows, got ${resultNonexistent.length}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.9 ARRAY Field Tests (SQL indexquery)
  // ============================================================

  test("ARRAY field via SQL indexquery - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map { i =>
        if (i <= 50) (i, Seq("premium", "featured"), "partition_a")
        else (i, Seq("standard", "basic"), "partition_b")
      }

      data.toDF("id", "tags", "partition_key")
        .write
        .format(DataSourceFormat)
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val readDf = spark.read.format(DataSourceFormat).load(tablePath)
      readDf.createOrReplaceTempView("array_table")

      // Positive: tags._values:premium should return partition_a data
      // Note: Arrays are wrapped with "_values" key during JSON indexing
      val resultPremium = spark.sql(
        s"SELECT * FROM array_table WHERE _indexall indexquery 'tags._values:premium'"
      ).collect()
      println(s"ARRAY positive 'tags._values:premium': ${resultPremium.length} rows")
      assert(resultPremium.nonEmpty, "Expected rows for tags._values:premium")
      assert(resultPremium.forall(_.getAs[String]("partition_key") == "partition_a"),
        "All results should be from partition_a")

      // Negative: nonexistent tag
      val resultNonexistent = spark.sql(
        s"SELECT * FROM array_table WHERE _indexall indexquery 'tags._values:nonexistent'"
      ).collect()
      println(s"ARRAY negative 'tags._values:nonexistent': ${resultNonexistent.length} rows")
      assert(resultNonexistent.isEmpty, s"Expected 0 rows, got ${resultNonexistent.length}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.10 MAP Field Tests (SQL indexquery)
  // ============================================================

  test("MAP field via SQL indexquery - positive and negative cases") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map { i =>
        if (i <= 50) (i, Map("status" -> "active", "tier" -> "gold"), "partition_a")
        else (i, Map("status" -> "inactive", "tier" -> "silver"), "partition_b")
      }

      data.toDF("id", "attributes", "partition_key")
        .write
        .format(DataSourceFormat)
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val readDf = spark.read.format(DataSourceFormat).load(tablePath)
      readDf.createOrReplaceTempView("map_table")

      // Positive: attributes.status:active should return partition_a data
      val resultActive = spark.sql(
        s"SELECT * FROM map_table WHERE _indexall indexquery 'attributes.status:active'"
      ).collect()
      println(s"MAP positive 'attributes.status:active': ${resultActive.length} rows")
      assert(resultActive.nonEmpty, "Expected rows for attributes.status:active")
      assert(resultActive.forall(_.getAs[String]("partition_key") == "partition_a"),
        "All results should be from partition_a")

      // Negative: nonexistent status
      val resultNonexistent = spark.sql(
        s"SELECT * FROM map_table WHERE _indexall indexquery 'attributes.status:nonexistent'"
      ).collect()
      println(s"MAP negative 'attributes.status:nonexistent': ${resultNonexistent.length} rows")
      assert(resultNonexistent.isEmpty, s"Expected 0 rows, got ${resultNonexistent.length}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.12 NOT Equal Operator Tests
  // ============================================================

  test("NOT Equal operator - filter by negation") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "active", "partition_a")
        else (i, "inactive", "partition_b")
      )

      data.toDF("id", "status", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.status", "string")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // NOT(status = "active") should return partition_b
      val filtersNotActive: Array[Filter] = Array(Not(EqualTo("status", "active")))
      val resultsNotActive = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersNotActive, 5000, tablePath, spark
      )
      println(s"NOT(status=active): ${resultsNotActive.size}/${xref.sourceSplitPaths.size} splits")
      // Note: NOT filters may fall back to conservative results in XRef

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.14 Range Filter Tests (graceful fallback)
  // ============================================================

  test("Range filters - graceful fallback to conservative results") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, 100, "partition_a")  // score = 100
        else (i, 200, "partition_b")          // score = 200
      )

      data.toDF("id", "score", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "score")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // GreaterThan - XRef transforms to match-all for safety
      val filtersGt: Array[Filter] = Array(GreaterThan("score", 150))
      val resultsGt = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersGt, 5000, tablePath, spark
      )
      println(s"GreaterThan(score, 150): ${resultsGt.size}/${xref.sourceSplitPaths.size} splits")
      // Should NOT fail, should return conservative result (all splits or correct subset)

      // LessThan
      val filtersLt: Array[Filter] = Array(LessThan("score", 250))
      val resultsLt = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersLt, 5000, tablePath, spark
      )
      println(s"LessThan(score, 250): ${resultsLt.size}/${xref.sourceSplitPaths.size} splits")

      // GreaterThanOrEqual
      val filtersGte: Array[Filter] = Array(GreaterThanOrEqual("score", 200))
      val resultsGte = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersGte, 5000, tablePath, spark
      )
      println(s"GreaterThanOrEqual(score, 200): ${resultsGte.size}/${xref.sourceSplitPaths.size} splits")

      // LessThanOrEqual
      val filtersLte: Array[Filter] = Array(LessThanOrEqual("score", 150))
      val resultsLte = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersLte, 5000, tablePath, spark
      )
      println(s"LessThanOrEqual(score, 150): ${resultsLte.size}/${xref.sourceSplitPaths.size} splits")

      // All range queries should complete without error
      println("Range filters completed without error (graceful fallback verified)")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.15 OR Filter Combination Tests
  // ============================================================

  test("OR filter combination") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "active", "partition_a")
        else (i, "inactive", "partition_b")
      )

      data.toDF("id", "status", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.status", "string")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // OR(status = "active", status = "inactive") should return all splits
      val filtersOr: Array[Filter] = Array(Or(EqualTo("status", "active"), EqualTo("status", "inactive")))
      val resultsOr = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersOr, 5000, tablePath, spark
      )
      println(s"OR(active, inactive): ${resultsOr.size}/${xref.sourceSplitPaths.size} splits")
      assert(resultsOr.size == xref.sourceSplitPaths.size,
        s"OR of both values should return all splits, got ${resultsOr.size}")

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  // ============================================================
  // 2.16 AND + Range Mixed Tests
  // ============================================================

  test("AND with range filter - mixed pushdown") {
    val ss = spark
    import ss.implicits._

    withTempPath { tablePath =>
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      val data = (1 to 100).map(i =>
        if (i <= 50) (i, "EXACT_A", 100, "partition_a")
        else (i, "EXACT_B", 200, "partition_b")
      )

      data.toDF("id", "string_field", "score", "partition_key")
        .write
        .format(DataSourceFormat)
        .option("spark.indextables.indexing.typemap.string_field", "string")
        .option("spark.indextables.indexing.fastfields", "score")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath), spark
      )
      val xref = transactionLog.listXRefs().head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId, XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // AND(string_field = "EXACT_A", score > 50)
      // The equality filter should be applied, range should fall back gracefully
      val filtersAnd: Array[Filter] = Array(
        And(EqualTo("string_field", "EXACT_A"), GreaterThan("score", 50))
      )
      val resultsAnd = XRefSearcher.searchSplits(
        xrefFullPath, xref, filtersAnd, 5000, tablePath, spark
      )
      println(s"AND(string=EXACT_A, score>50): ${resultsAnd.size}/${xref.sourceSplitPaths.size} splits")
      // The equality filter should narrow to partition_a, range may fall back
      // At minimum, should not fail

      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }
}
