/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package io.indextables.spark.core

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{MetadataAction, TransactionLog}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{count, min, sum}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * Tests for the aggregate guard that prevents silently incorrect aggregate
 * results when aggregate pushdown fails.
 */
class CompanionAggregateGuardTest extends TestBase {

  private def markAsCompanionTable(testPath: String, includeSourcePath: Boolean = false): Unit = {
    val txLog = new TransactionLog(
      new Path(testPath),
      spark,
      new CaseInsensitiveStringMap(
        Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
      )
    )
    try {
      val currentMetadata = txLog.getMetadata()
      var companionConfig = currentMetadata.configuration + ("indextables.companion.enabled" -> "true")
      if (includeSourcePath) {
        companionConfig = companionConfig + ("indextables.companion.sourceTablePath" -> testPath)
      }
      val updatedMetadata = MetadataAction(
        id = java.util.UUID.randomUUID().toString,
        name = currentMetadata.name,
        description = currentMetadata.description,
        format = currentMetadata.format,
        schemaString = currentMetadata.schemaString,
        partitionColumns = currentMetadata.partitionColumns,
        configuration = companionConfig,
        createdTime = Some(System.currentTimeMillis())
      )
      txLog.commitSyncActions(Seq.empty, Seq.empty, Some(updatedMetadata))
    } finally {
      txLog.close()
    }
  }

  test("companion table should fail on aggregate without pushdown") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with 'content' as a TEXT field (not fast by default).
      // MIN/MAX on non-fast fields are rejected by pushAggregation().
      val df = (0 until 500).map(i => (i.toLong, s"item $i content")).toDF("id", "content")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(testPath)

      // Mark the table as companion mode via transaction log metadata
      markAsCompanionTable(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // MIN on a non-fast TEXT field causes pushAggregation() to reject.
      // On a companion table, the companion guard catches this and throws
      // instead of silently returning wrong results (truncated by default limit).
      val ex = intercept[Exception] {
        readDf.agg(min($"content")).collect()
      }

      // The error may be wrapped in a SparkException
      val message = Option(ex.getMessage).getOrElse("") +
        Option(ex.getCause).map(c => " " + c.getMessage).getOrElse("")
      assert(
        message.contains("Aggregate query cannot proceed without aggregate pushdown"),
        s"Expected aggregate guard error, got: ${ex.getMessage}"
      )
    }
  }

  test("non-companion table should also fail on aggregate without pushdown") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with 'content' as a TEXT field (not fast by default).
      // MIN/MAX on non-fast fields are rejected by pushAggregation().
      // This is a regular (non-companion) table.
      val df = (0 until 500).map(i => (i.toLong, s"item $i content")).toDF("id", "content")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(testPath)

      // Do NOT mark as companion - this is a regular table

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // MIN on a non-fast TEXT field causes pushAggregation() to reject.
      // The guard should fire for ALL tables, not just companion tables.
      val ex = intercept[Exception] {
        readDf.agg(min($"content")).collect()
      }

      val message = Option(ex.getMessage).getOrElse("") +
        Option(ex.getCause).map(c => " " + c.getMessage).getOrElse("")
      assert(
        message.contains("Aggregate query cannot proceed without aggregate pushdown"),
        s"Expected aggregate guard error, got: ${ex.getMessage}"
      )
    }
  }

  test("aggregate guard can be disabled via config") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with 'content' as a TEXT field
      val df = (0 until 500).map(i => (i.toLong, s"item $i content")).toDF("id", "content")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(testPath)

      // Mark the table as companion mode
      markAsCompanionTable(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.companion.requireAggregatePushdown", "false")
        .load(testPath)

      // With the guard disabled, the query should proceed (even if results may be incorrect)
      val result = readDf.agg(min($"content")).collect()
      assert(result.length == 1, s"Expected 1 result row, got ${result.length}")
    }
  }

  test("companion table COUNT(*) should prefer numeric fast field") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with a numeric fast field — COUNT(*) should prefer it
      // over string fields for performance (compact, fast for counting)
      val df = (0 until 100).map(i => (i.toLong, s"item$i", i * 10)).toDF("id", "name", "score")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(testPath)

      markAsCompanionTable(testPath, includeSourcePath = true)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // Unfiltered COUNT(*) uses TransactionLogCountScan
      val result = readDf.agg(count("*")).collect()
      assert(result.length == 1)
      assert(result(0).getLong(0) == 100L, s"Expected COUNT(*)=100, got ${result(0).getLong(0)}")

      // Filtered COUNT(*) goes through SimpleAggregateScan and picks numeric fast field
      val filteredCount = readDf.filter($"score" >= 500).agg(count("*")).collect()
      assert(filteredCount.length == 1)
      assert(filteredCount(0).getLong(0) == 50L, s"Expected COUNT(*)=50, got ${filteredCount(0).getLong(0)}")

      // COUNT(*) + SUM in one query
      val combined = readDf.filter($"score" >= 500).agg(count("*"), sum("score")).collect()
      assert(combined.length == 1)
      assert(combined(0).getLong(0) == 50L, s"Expected COUNT(*)=50, got ${combined(0).getLong(0)}")
    }
  }
}
