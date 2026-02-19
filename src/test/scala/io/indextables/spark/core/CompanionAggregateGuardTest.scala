/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package io.indextables.spark.core

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{FileFormat, MetadataAction, TransactionLog}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * Tests for the companion aggregate guard that prevents silently incorrect aggregate
 * results on companion-mode tables when aggregate pushdown fails.
 */
class CompanionAggregateGuardTest extends TestBase {

  private def markAsCompanionTable(testPath: String): Unit = {
    val txLog = new TransactionLog(
      new Path(testPath),
      spark,
      new CaseInsensitiveStringMap(
        Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
      )
    )
    try {
      val currentMetadata = txLog.getMetadata()
      val updatedMetadata = MetadataAction(
        id = java.util.UUID.randomUUID().toString,
        name = currentMetadata.name,
        description = currentMetadata.description,
        format = currentMetadata.format,
        schemaString = currentMetadata.schemaString,
        partitionColumns = currentMetadata.partitionColumns,
        configuration = currentMetadata.configuration + ("indextables.companion.enabled" -> "true"),
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
        message.contains("companion-mode table"),
        s"Expected companion aggregate guard error, got: ${ex.getMessage}"
      )
    }
  }

  test("companion aggregate guard can be disabled via config") {
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
}
