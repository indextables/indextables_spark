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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * Parquet-format implementation of the companion INCLUDE/EXCLUDE COLUMNS test suite.
 */
class ParquetCompanionColumnsTest extends CompanionColumnsTestBase {

  override def formatName: String = "parquet"

  private var _spark: SparkSession = _
  override def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    _spark = SparkSession
      .builder()
      .appName("ParquetCompanionColumnsTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
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

  override def afterAll(): Unit = {
    cleanupSharedTables()
    if (_spark != null) _spark.stop()
  }

  override def newTableId(tempDir: String, name: String): String =
    new File(tempDir, name).getAbsolutePath

  override def createPartitionedTable(tableId: String): Unit = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    Seq(
      (1, "alice", 100.0, "region_a"),
      (2, "bob", 200.5, "region_b"),
      (3, "charlie", 300.75, "region_a")
    ).toDF("id", "name", "score", "region")
      .repartition(1, col("region"))
      .write.partitionBy("region").parquet(tableId)
  }

  override def createSimpleTable(tableId: String, schema: StructType, data: Seq[Row]): Unit =
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .coalesce(1).write.parquet(tableId)

  override def recreateTable(tableId: String, schema: StructType, data: Seq[Row]): Unit = {
    deleteRecursively(new File(tableId))
    createSimpleTable(tableId, schema, data)
  }

  override def appendData(tableId: String, schema: StructType, data: Seq[Row]): Unit =
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .coalesce(1).write.mode("append").parquet(tableId)

  override def buildCompanionSql(tableId: String, clauses: String, indexPath: String): String = {
    val c = if (clauses.nonEmpty) s" $clauses" else ""
    s"BUILD INDEXTABLES COMPANION FOR PARQUET '$tableId'$c AT LOCATION '$indexPath'"
  }
}
