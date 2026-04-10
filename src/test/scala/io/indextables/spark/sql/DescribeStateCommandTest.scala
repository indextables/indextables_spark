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

package io.indextables.spark.sql

import org.apache.spark.sql.types.{StringType => SparkStringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}
import io.indextables.spark.TestBase

class DescribeStateCommandTest extends TestBase {

  test("DESCRIBE STATE should parse correctly") {
    val plan1 = spark.sessionState.sqlParser.parsePlan("DESCRIBE INDEXTABLES STATE '/path/to/table'")
    plan1 shouldBe a[DescribeStateCommand]
    plan1.asInstanceOf[DescribeStateCommand].tablePath shouldBe "/path/to/table"

    val plan2 = spark.sessionState.sqlParser.parsePlan("DESCRIBE TANTIVY4SPARK STATE '/another/path'")
    plan2 shouldBe a[DescribeStateCommand]
    plan2.asInstanceOf[DescribeStateCommand].tablePath shouldBe "/another/path"
  }

  test("DESCRIBE STATE should return 'none' when no checkpoint exists") {
    withTempPath { tempPath =>
      // Create transaction log directory but no checkpoint
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try
        cloudProvider.createDirectory(txLogPath.toString)
      finally
        cloudProvider.close()

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "none"
      result(0).getAs[String]("status") shouldBe "No checkpoint found"
    }
  }

  test("DESCRIBE STATE should describe Avro state format") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Initialize table and add files via the transaction log API
      val txLog = TransactionLogFactory.create(tablePath, spark)
      try {
        txLog.initialize(getTestSchema())

        val addActions = (1 to 100).map(i => createTestAddAction(s"file$i.split"))
        txLog.addFiles(addActions)
      } finally
        txLog.close()

      // Create a checkpoint so _last_checkpoint exists
      spark.sql(s"CHECKPOINT INDEXTABLES '$tempPath'").collect()

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "avro-state"
      result(0).getAs[Long]("version") should be >= 1L
      result(0).getAs[Long]("num_files") shouldBe 100L
      result(0).getAs[String]("status") shouldBe "OK"
    }
  }

  test("DESCRIBE STATE should report correct file count after removals") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Initialize table and add files
      val txLog = TransactionLogFactory.create(tablePath, spark)
      try {
        txLog.initialize(getTestSchema())

        val addActions = (1 to 100).map(i => createTestAddAction(s"file$i.split"))
        txLog.addFiles(addActions)

        // Remove 5 files
        (1 to 5).foreach(i => txLog.removeFile(s"file$i.split"))
      } finally
        txLog.close()

      // Create a checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$tempPath'").collect()

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "avro-state"
      // After removing 5 files from 100, we should have 95 visible files
      result(0).getAs[Long]("num_files") shouldBe 95L
      result(0).getAs[String]("status") shouldBe "OK"
    }
  }

  test("DESCRIBE STATE should handle missing table gracefully") {
    val result = spark.sql("DESCRIBE INDEXTABLES STATE '/nonexistent/path'").collect()

    result should have length 1
    // When the path doesn't exist or has no checkpoint, returns "none" or "error" format
    val format = result(0).getAs[String]("format")
    (format == "none" || format == "error") shouldBe true
  }

  // Helper methods

  override protected def getTestSchema(): StructType =
    new StructType()
      .add(StructField("id", SparkStringType))
      .add(StructField("content", SparkStringType))

  private def createTestAddAction(
    path: String,
    partitionValues: Map[String, String] = Map.empty
  ): AddAction =
    AddAction(
      path = path,
      partitionValues = partitionValues,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L)
    )
}
