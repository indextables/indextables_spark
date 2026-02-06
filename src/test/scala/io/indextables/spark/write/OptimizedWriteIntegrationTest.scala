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

package io.indextables.spark.write

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.TransactionLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering

class OptimizedWriteIntegrationTest extends TestBase {

  private val FORMAT = "io.indextables.spark.core.IndexTables4SparkTableProvider"
  private val GB = 1024L * 1024 * 1024

  // Helper to create a TransactionLog for direct API testing
  private def createTxLog(tablePath: Path): TransactionLog = {
    import scala.jdk.CollectionConverters._
    new TransactionLog(tablePath, spark, new org.apache.spark.sql.util.CaseInsensitiveStringMap(
      Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
    ))
  }

  // Helper to create a WriteBuilder for API testing
  private def createWriteBuilder(
    tablePath: Path,
    txLog: TransactionLog,
    extraOptions: Map[String, String] = Map.empty,
    schemaFields: Seq[(String, String)] = Seq("id" -> "long", "text" -> "string")
  ): io.indextables.spark.core.IndexTables4SparkWriteBuilder = {
    import scala.jdk.CollectionConverters._
    val baseOptions = Map(
      "spark.indextables.aws.accessKey" -> "test",
      "spark.indextables.aws.secretKey" -> "test",
      "spark.indextables.s3.pathStyleAccess" -> "true",
      "spark.indextables.aws.region" -> "us-east-1",
      "spark.indextables.s3.endpoint" -> "http://localhost:10101"
    )
    val writeOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
      (baseOptions ++ extraOptions).asJava
    )
    var writeSchema = new org.apache.spark.sql.types.StructType()
    schemaFields.foreach { case (name, typ) => writeSchema = writeSchema.add(name, typ) }

    val writeInfo = new org.apache.spark.sql.connector.write.LogicalWriteInfo {
      override def queryId(): String = "test-query"
      override def schema(): org.apache.spark.sql.types.StructType = writeSchema
      override def options(): org.apache.spark.sql.util.CaseInsensitiveStringMap = writeOptions
    }

    new io.indextables.spark.core.IndexTables4SparkWriteBuilder(
      txLog, tablePath, writeInfo, writeOptions, spark.sparkContext.hadoopConfiguration
    )
  }

  // ===== E2E Write/Read Tests =====

  test("disabled by default - standard write behavior") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_disabled"
      val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100
    }
  }

  test("enabled write produces correct data with all values intact") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_enabled"
      val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE, "1G")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200

      // Verify actual data values survive the optimized write path
      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 200L).toArray
    }
  }

  test("enabled write with distribution mode none") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_none_mode"
      val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "none")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100
    }
  }

  test("partitioned table with optimized write - data integrity per partition") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_partitioned"
      val df = spark.range(0, 300)
        .selectExpr("id", "CAST(id AS STRING) as text", "CAST(id % 3 AS STRING) as category")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE, "1G")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 300

      // Verify each partition has the correct count
      result.filter("category = '0'").count() shouldBe 100
      result.filter("category = '1'").count() shouldBe 100
      result.filter("category = '2'").count() shouldBe 100

      // Verify data values within a partition
      val cat0Ids = result.filter("category = '0'").select("id").collect().map(_.getLong(0)).sorted
      cat0Ids shouldBe (0L until 300L).filter(_ % 3 == 0).toArray
    }
  }

  test("append after initial write preserves all data") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_append"

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .save(tablePath)

      // Append write
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write
        .mode("append")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200

      // Verify both batches present
      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 200L).toArray
    }
  }

  // ===== WriteBuilder Type Selection Tests =====

  test("WriteBuilder creates StandardWrite when disabled - not RequiresDistributionAndOrdering") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_builder")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog, Map(
        OptimizedWriteConfig.KEY_ENABLED -> "false"
      ))
      val write = builder.build()

      write.getClass.getSimpleName shouldBe "IndexTables4SparkStandardWrite"
      write shouldNot be(a[RequiresDistributionAndOrdering])

      txLog.close()
    }
  }

  test("WriteBuilder creates OptimizedWrite when enabled - is RequiresDistributionAndOrdering") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_builder_opt")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog, Map(
        OptimizedWriteConfig.KEY_ENABLED -> "true"
      ))
      val write = builder.build()

      write.getClass.getSimpleName shouldBe "IndexTables4SparkOptimizedWrite"
      write shouldBe a[RequiresDistributionAndOrdering]

      txLog.close()
    }
  }

  // ===== RequiresDistributionAndOrdering API Behavior Tests =====

  test("partitioned + hash: clustered distribution with correct partition column expressions") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_dist_partitioned")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          "__partition_columns" -> """["category"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "category" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Distribution should be ClusteredDistribution
      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      // Clustering expressions should contain exactly the partition column
      val clustered = dist.asInstanceOf[ClusteredDistribution]
      val clusteringExprs = clustered.clustering()
      clusteringExprs should have length 1
      clusteringExprs(0) shouldBe a[NamedReference]
      clusteringExprs(0).asInstanceOf[NamedReference].fieldNames() shouldBe Array("category")

      // Ordering should be empty
      write.requiredOrdering() shouldBe empty

      // Distribution should not be strictly required
      write.distributionStrictlyRequired() shouldBe false

      // Advisory size should be sampling-mode value (no history): targetSize / ratio
      val expectedAdvisory = (GB / 1.1).toLong
      write.advisoryPartitionSizeInBytes() shouldBe expectedAdvisory

      txLog.close()
    }
  }

  test("partitioned + hash + multiple partition columns: all columns in clustering") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_dist_multi_part")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          "__partition_columns" -> """["region","date"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "region" -> "string", "date" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      val clustered = dist.asInstanceOf[ClusteredDistribution]
      val clusteringExprs = clustered.clustering()
      clusteringExprs should have length 2
      clusteringExprs(0).asInstanceOf[NamedReference].fieldNames() shouldBe Array("region")
      clusteringExprs(1).asInstanceOf[NamedReference].fieldNames() shouldBe Array("date")

      // Advisory should be set (> 0) since we have partition columns + hash mode
      write.advisoryPartitionSizeInBytes() should be > 0L

      txLog.close()
    }
  }

  test("partitioned + mode=none: unspecified distribution and advisory=0") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_dist_none_mode")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "none",
          "__partition_columns" -> """["category"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "category" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Even with partition columns, mode=none should give unspecified
      val dist = write.requiredDistribution()
      dist shouldBe a[UnspecifiedDistribution]

      // Advisory must be 0 with UnspecifiedDistribution (Spark constraint)
      write.advisoryPartitionSizeInBytes() shouldBe 0L

      write.distributionStrictlyRequired() shouldBe false
      write.requiredOrdering() shouldBe empty

      txLog.close()
    }
  }

  test("unpartitioned + hash: clustered distribution on first schema column with advisory size") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_dist_unpartitioned")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog, Map(
        OptimizedWriteConfig.KEY_ENABLED -> "true"
      ))
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Unpartitioned tables cluster by a single column to trigger a shuffle.
      // Spark 3.5 rejects empty clustering + advisory > 0, so we use the first schema
      // column — just enough to enter the shuffle branch while keeping hash cost O(1).
      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      val clustered = dist.asInstanceOf[ClusteredDistribution]
      val clusteringExprs = clustered.clustering()
      // Default schema is (id: long, text: string) - should cluster by first column only
      clusteringExprs should have length 1
      clusteringExprs(0).asInstanceOf[NamedReference].fieldNames() shouldBe Array("id")

      // Advisory size should be set (not 0) since we have a clustered distribution
      val expectedAdvisory = (GB / 1.1).toLong
      write.advisoryPartitionSizeInBytes() shouldBe expectedAdvisory

      write.distributionStrictlyRequired() shouldBe false
      write.requiredOrdering() shouldBe empty

      txLog.close()
    }
  }

  test("advisory size uses configured targetSplitSize and samplingRatio") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_advisory_custom")
      val txLog = createTxLog(tablePath)

      val targetSize = 2L * GB
      val ratio = 2.0

      val builder = createWriteBuilder(tablePath, txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "2G",
          OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "2.0",
          "__partition_columns" -> """["category"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "category" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Sampling mode (empty tx log): advisory = targetSize / ratio = 2GB / 2.0 = 1GB
      val expectedAdvisory = (targetSize / ratio).toLong
      write.advisoryPartitionSizeInBytes() shouldBe expectedAdvisory
      expectedAdvisory shouldBe GB // sanity check

      txLog.close()
    }
  }

  // ===== Consolidation Tests =====

  test("consolidation: optimized write produces fewer splits than non-optimized with many input partitions") {
    withTempPath { path =>
      val nonOptPath = s"file://$path/non_optimized"
      val optPath = s"file://$path/optimized"

      // Create small data repartitioned to many partitions - simulates
      // data arriving in many small chunks from a wide shuffle
      val df = spark.range(0, 300)
        .repartition(50)
        .selectExpr("id", "CAST(id AS STRING) as text", "CAST(id % 3 AS STRING) as category")

      // Non-optimized write: 50 input partitions, each writing per-partition-value splits
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .save(nonOptPath)

      // Optimized write: AQE should coalesce 200 shuffle partitions down
      // to ~3 (one per partition value) since data is tiny vs 1G target
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE, "1G")
        .save(optPath)

      // Verify data integrity in both cases
      spark.read.format(FORMAT).load(nonOptPath).count() shouldBe 300
      spark.read.format(FORMAT).load(optPath).count() shouldBe 300

      // Count splits in each table
      val nonOptTxLog = createTxLog(new Path(nonOptPath))
      val optTxLog = createTxLog(new Path(optPath))

      val nonOptSplitCount = nonOptTxLog.listFiles().length
      val optSplitCount = optTxLog.listFiles().length

      // Non-optimized: 50 input tasks × up to 3 partition values = many splits
      // Optimized: AQE coalesces tiny data → ~3 splits (one per partition value)
      withClue(s"non-optimized=$nonOptSplitCount splits, optimized=$optSplitCount splits: ") {
        optSplitCount should be < nonOptSplitCount
        optSplitCount should be <= 6 // At most a few (3 partitions, maybe some AQE variance)
      }

      nonOptTxLog.close()
      optTxLog.close()
    }
  }

  test("consolidation: unpartitioned small data consolidates to few splits with optimized write") {
    withTempPath { path =>
      val nonOptPath = s"file://$path/non_optimized"
      val optPath = s"file://$path/optimized"

      // Small data repartitioned to many partitions
      val df = spark.range(0, 100)
        .repartition(50)
        .selectExpr("id", "CAST(id AS STRING) as text")

      // Non-optimized: 50 input partitions → ~50 splits
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(nonOptPath)

      // Optimized: ClusteredDistribution(all columns) triggers AQE rebalance,
      // AQE coalesces based on 1G advisory → should produce very few splits
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE, "1G")
        .save(optPath)

      // Verify data integrity
      spark.read.format(FORMAT).load(nonOptPath).count() shouldBe 100
      spark.read.format(FORMAT).load(optPath).count() shouldBe 100

      val nonOptTxLog = createTxLog(new Path(nonOptPath))
      val optTxLog = createTxLog(new Path(optPath))

      val nonOptSplitCount = nonOptTxLog.listFiles().length
      val optSplitCount = optTxLog.listFiles().length

      // Non-optimized: 50 input partitions → many splits
      // Optimized: AQE coalesces tiny data with 1G advisory → very few splits
      withClue(s"non-optimized=$nonOptSplitCount splits, optimized=$optSplitCount splits: ") {
        optSplitCount should be < nonOptSplitCount
        optSplitCount should be <= 5 // 100 tiny rows with 1G target → ~1 split
      }

      nonOptTxLog.close()
      optTxLog.close()
    }
  }

  // ===== WriteBuilder / API Tests =====

  test("toBatch returns self for OptimizedWrite") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_tobatch")
      val txLog = createTxLog(tablePath)

      val builder = createWriteBuilder(tablePath, txLog, Map(
        OptimizedWriteConfig.KEY_ENABLED -> "true"
      ))
      val write = builder.build()
      val batch = write.toBatch

      // toBatch should return the same instance (inherited from StandardWrite)
      batch shouldBe theSameInstanceAs(write)

      txLog.close()
    }
  }
}
