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

import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLog
import io.indextables.spark.TestBase

class OptimizedWriteIntegrationTest extends TestBase {

  private val FORMAT = "io.indextables.spark.core.IndexTables4SparkTableProvider"
  private val GB     = 1024L * 1024 * 1024

  // Helper to create a TransactionLog for direct API testing
  private def createTxLog(tablePath: Path): TransactionLog = {
    import scala.jdk.CollectionConverters._
    new TransactionLog(
      tablePath,
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(
        Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
      )
    )
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
      "spark.indextables.aws.accessKey"      -> "test",
      "spark.indextables.aws.secretKey"      -> "test",
      "spark.indextables.s3.pathStyleAccess" -> "true",
      "spark.indextables.aws.region"         -> "us-east-1",
      "spark.indextables.s3.endpoint"        -> "http://localhost:10101"
    )
    val writeOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
      (baseOptions ++ extraOptions).asJava
    )
    var writeSchema = new org.apache.spark.sql.types.StructType()
    schemaFields.foreach { case (name, typ) => writeSchema = writeSchema.add(name, typ) }

    val writeInfo = new org.apache.spark.sql.connector.write.LogicalWriteInfo {
      override def queryId(): String                                             = "test-query"
      override def schema(): org.apache.spark.sql.types.StructType               = writeSchema
      override def options(): org.apache.spark.sql.util.CaseInsensitiveStringMap = writeOptions
    }

    new io.indextables.spark.core.IndexTables4SparkWriteBuilder(
      txLog,
      tablePath,
      writeInfo,
      writeOptions,
      spark.sparkContext.hadoopConfiguration
    )
  }

  // ===== E2E Write/Read Tests =====

  test("disabled by default - standard write behavior") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_disabled"
      val df        = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

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
      val df        = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

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
      val df        = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

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
      val df = spark
        .range(0, 300)
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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED -> "false"
        )
      )
      val write = builder.build()

      write.getClass.getSimpleName shouldBe "IndexTables4SparkStandardWrite"
      write shouldNot be(a[RequiresDistributionAndOrdering])

      txLog.close()
    }
  }

  test("WriteBuilder creates OptimizedWrite when enabled - is RequiresDistributionAndOrdering") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_builder_opt")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true"
        )
      )
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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          "__partition_columns"            -> """["category"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "category" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Distribution should be ClusteredDistribution
      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      // Clustering expressions should contain exactly the partition column
      val clustered       = dist.asInstanceOf[ClusteredDistribution]
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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true",
          "__partition_columns"            -> """["region","date"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "region" -> "string", "date" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      val clustered       = dist.asInstanceOf[ClusteredDistribution]
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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "none",
          "__partition_columns"                      -> """["category"]"""
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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true"
        )
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Unpartitioned tables cluster by a single column to trigger a shuffle.
      // Spark 3.5 rejects empty clustering + advisory > 0, so we use the first schema
      // column — just enough to enter the shuffle branch while keeping hash cost O(1).
      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      val clustered       = dist.asInstanceOf[ClusteredDistribution]
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
      val txLog     = createTxLog(tablePath)

      val targetSize = 2L * GB
      val ratio      = 2.0

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "2G",
          OptimizedWriteConfig.KEY_SAMPLING_RATIO    -> "2.0",
          "__partition_columns"                      -> """["category"]"""
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
      val optPath    = s"file://$path/optimized"

      // Create small data repartitioned to many partitions - simulates
      // data arriving in many small chunks from a wide shuffle
      val df = spark
        .range(0, 300)
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
      val optTxLog    = createTxLog(new Path(optPath))

      val nonOptSplitCount = nonOptTxLog.listFiles().length
      val optSplitCount    = optTxLog.listFiles().length

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
      val optPath    = s"file://$path/optimized"

      // Small data repartitioned to many partitions
      val df = spark
        .range(0, 100)
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
      val optTxLog    = createTxLog(new Path(optPath))

      val nonOptSplitCount = nonOptTxLog.listFiles().length
      val optSplitCount    = optTxLog.listFiles().length

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
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED -> "true"
        )
      )
      val write = builder.build()
      val batch = write.toBatch

      // toBatch should return the same instance (inherited from StandardWrite)
      batch shouldBe theSameInstanceAs(write)

      txLog.close()
    }
  }

  // ===== Balanced Mode API Tests =====

  test("balanced mode: requiredNumPartitions equals defaultParallelism") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_balanced_num_parts")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced"
        )
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      val expected = spark.sparkContext.defaultParallelism
      write.requiredNumPartitions() shouldBe expected
      write.requiredNumPartitions() should be > 0

      txLog.close()
    }
  }

  test("balanced mode: advisoryPartitionSizeInBytes is 0") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_balanced_advisory")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced"
        )
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      // Balanced mode uses requiredNumPartitions, not advisory size
      write.advisoryPartitionSizeInBytes() shouldBe 0L

      txLog.close()
    }
  }

  test("balanced mode: distributionStrictlyRequired is true") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_balanced_strict")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced"
        )
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      write.distributionStrictlyRequired() shouldBe true

      txLog.close()
    }
  }

  test("balanced mode: uses clustered distribution like hash mode") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_balanced_dist")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        extraOptions = Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced",
          "__partition_columns"                      -> """["category"]"""
        ),
        schemaFields = Seq("id" -> "long", "text" -> "string", "category" -> "string")
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      val dist = write.requiredDistribution()
      dist shouldBe a[ClusteredDistribution]

      txLog.close()
    }
  }

  test("hash mode: requiredNumPartitions is 0 and distributionStrictlyRequired is false") {
    withTempPath { path =>
      val tablePath = new Path(s"file://$path/test_hash_num_parts")
      val txLog     = createTxLog(tablePath)

      val builder = createWriteBuilder(
        tablePath,
        txLog,
        Map(
          OptimizedWriteConfig.KEY_ENABLED           -> "true",
          OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "hash"
        )
      )
      val write = builder.build().asInstanceOf[RequiresDistributionAndOrdering]

      write.requiredNumPartitions() shouldBe 0
      write.distributionStrictlyRequired() shouldBe false

      txLog.close()
    }
  }

  // ===== Split Rolling E2E Tests =====

  test("balanced mode: write produces correct data") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_balanced_write"
      val df        = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200

      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 200L).toArray
    }
  }

  test("balanced mode with partitions: data integrity per partition") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_balanced_partitioned"
      val df = spark
        .range(0, 300)
        .selectExpr("id", "CAST(id AS STRING) as text", "CAST(id % 3 AS STRING) as category")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 300
      result.filter("category = '0'").count() shouldBe 100
      result.filter("category = '1'").count() shouldBe 100
      result.filter("category = '2'").count() shouldBe 100
    }
  }

  test("split rolling: small maxSplitSize produces multiple splits per task") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_rolling"

      // First write to establish history for bytes-per-row estimation
      // Use minRowsForEstimation=100 so the first write qualifies for history mode
      val df1 = spark.range(0, 1000).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "hash")
        .save(tablePath)

      val txLogBefore          = createTxLog(new Path(tablePath))
      val firstWriteSplitCount = txLogBefore.listFiles().length
      txLogBefore.close()

      // Second write with balanced mode and small maxSplitSize to trigger rolling
      // minRowsForEstimation=100 ensures the first write's history qualifies
      val df2 = spark.range(1000, 5000).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write
        .mode("append")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .option(OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE, "10K")
        .option(OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST, "100") // Low threshold for test
        .save(tablePath)

      // Verify split count increased significantly due to rolling
      val txLog       = createTxLog(new Path(tablePath))
      val allSplits   = txLog.listFiles()
      val parallelism = spark.sparkContext.defaultParallelism

      // Rolling should produce more splits than defaultParallelism
      // (each task rolls multiple times with 10K max split size)
      withClue(
        s"Total splits: ${allSplits.length}, firstWrite=$firstWriteSplitCount, " +
          s"parallelism=$parallelism - rolling should produce more splits than tasks: "
      ) {
        val secondWriteSplits = allSplits.length - firstWriteSplitCount
        secondWriteSplits should be > parallelism
      }

      // Verify metadata-level record counts match expectations
      val totalMetadataRecords = allSplits.flatMap(_.numRecords).sum
      withClue(s"Metadata numRecords sum across ${allSplits.length} splits: ") {
        totalMetadataRecords shouldBe 5000
      }

      // Use .limit(Int.MaxValue) to push a large limit through to the reader,
      // overriding the default per-split limit of 250
      val result         = spark.read.format(FORMAT).load(tablePath)
      val collectedCount = result.select("id").limit(Int.MaxValue).collect().length
      withClue(s"Collected rows (splits=${allSplits.length}, parallelism=$parallelism): ") {
        collectedCount shouldBe 5000
      }

      txLog.close()
    }
  }

  test("split rolling: data integrity - all IDs present with no duplicates") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_rolling_integrity"

      // First write to establish history
      val df1 = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "hash")
        .save(tablePath)

      // Second write with rolling
      val df2 = spark.range(500, 3000).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write
        .mode("append")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .option(OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE, "10K")
        .option(OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST, "100")
        .save(tablePath)

      // Verify every single ID is present, no duplicates, no gaps
      val result = spark.read.format(FORMAT).load(tablePath)
      val ids    = result.select("id").limit(Int.MaxValue).collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 3000L).toArray

      // Also verify text column survived rolling
      val textsForId = result.filter("id = 1500").select("text").limit(Int.MaxValue).collect()
      textsForId should have length 1
      textsForId(0).getString(0) shouldBe "1500"
    }
  }

  test("split rolling: partitioned table rolls correctly per partition") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_rolling_partitioned"

      // First write to establish history
      val df1 = spark
        .range(0, 600)
        .selectExpr("id", "CAST(id AS STRING) as text", "CAST(id % 3 AS STRING) as category")
      df1.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "hash")
        .save(tablePath)

      val txLogBefore          = createTxLog(new Path(tablePath))
      val firstWriteSplitCount = txLogBefore.listFiles().length
      txLogBefore.close()

      // Second write with rolling on partitioned table
      val df2 = spark
        .range(600, 3600)
        .selectExpr("id", "CAST(id AS STRING) as text", "CAST(id % 3 AS STRING) as category")
      df2.write
        .mode("append")
        .format(FORMAT)
        .partitionBy("category")
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .option(OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE, "10K")
        .option(OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST, "100")
        .save(tablePath)

      // Verify rolling produced more splits than tasks
      val txLog             = createTxLog(new Path(tablePath))
      val allSplits         = txLog.listFiles()
      val secondWriteSplits = allSplits.length - firstWriteSplitCount
      val parallelism       = spark.sparkContext.defaultParallelism
      withClue(s"Partitioned rolling: secondWriteSplits=$secondWriteSplits, parallelism=$parallelism: ") {
        secondWriteSplits should be > parallelism
      }

      // Verify data integrity across all partitions
      val result = spark.read.format(FORMAT).load(tablePath)
      val allIds = result.select("id").limit(Int.MaxValue).collect().map(_.getLong(0)).sorted
      allIds shouldBe (0L until 3600L).toArray

      // Verify per-partition counts
      result.filter("category = '0'").count() shouldBe 1200
      result.filter("category = '1'").count() shouldBe 1200
      result.filter("category = '2'").count() shouldBe 1200

      txLog.close()
    }
  }

  test("balanced mode: empty tasks handled gracefully when data < defaultParallelism") {
    withTempPath { path =>
      val tablePath   = s"file://$path/test_empty_tasks"
      val parallelism = spark.sparkContext.defaultParallelism

      // Write very few rows - fewer than defaultParallelism tasks
      // Some tasks will receive no data at all
      val df = spark.range(0, 1).selectExpr("id", "CAST(id AS STRING) as text")
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .save(tablePath)

      // Verify data integrity - single row should survive
      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 1
      val ids = result.select("id").collect().map(_.getLong(0))
      ids shouldBe Array(0L)

      // Verify split count - should be <= parallelism (most tasks produced empty commits)
      val txLog  = createTxLog(new Path(tablePath))
      val splits = txLog.listFiles()
      withClue(s"With 1 row across $parallelism tasks, splits=${splits.length}: ") {
        splits.length should be >= 1
        splits.length should be <= parallelism
      }
      txLog.close()
    }
  }

  test("split rolling: progressive output metrics are set correctly") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_progressive_metrics"

      // First write to establish history
      val df1 = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "hash")
        .save(tablePath)

      // Second write with rolling - capture Spark job metrics via listener
      @volatile var totalOutputBytes: Long   = 0L
      @volatile var totalOutputRecords: Long = 0L
      val metricsListener = new org.apache.spark.scheduler.SparkListener {
        override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
          val metrics = taskEnd.taskMetrics
          if (metrics != null) {
            totalOutputBytes += metrics.outputMetrics.bytesWritten
            totalOutputRecords += metrics.outputMetrics.recordsWritten
          }
        }
      }
      spark.sparkContext.addSparkListener(metricsListener)

      try {
        val df2 = spark.range(500, 3000).selectExpr("id", "CAST(id AS STRING) as text")
        df2.write
          .mode("append")
          .format(FORMAT)
          .option(OptimizedWriteConfig.KEY_ENABLED, "true")
          .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
          .option(OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE, "10K")
          .option(OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST, "100")
          .save(tablePath)

        // Wait for async listener events to be delivered
        Thread.sleep(2000)

        // Verify that output metrics were reported for the write tasks
        // The rolling code calls updateOutputMetrics after each roll AND at final commit,
        // so the final metrics should reflect the total bytes/records across all rolled splits
        withClue(s"Output records from task metrics (bytes=$totalOutputBytes): ") {
          totalOutputRecords shouldBe 2500 // second write: 500..3000
        }
        totalOutputBytes should be > 0L
      } finally
        spark.sparkContext.removeSparkListener(metricsListener)
    }
  }

  test("split rolling: first write without history does not roll (no estimation available)") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_no_history_no_roll"

      // First write with balanced mode - no history means calculateMaxRowsPerSplit returns None
      val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(OptimizedWriteConfig.KEY_ENABLED, "true")
        .option(OptimizedWriteConfig.KEY_DISTRIBUTION_MODE, "balanced")
        .option(OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE, "1K")
        .save(tablePath)

      // Verify data integrity
      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200

      // Without history, no __maxRowsPerSplit is injected, so no rolling occurs.
      // The number of splits should equal the number of tasks (defaultParallelism).
      val txLog       = createTxLog(new Path(tablePath))
      val splits      = txLog.listFiles()
      val parallelism = spark.sparkContext.defaultParallelism
      withClue(s"Without history, splits (${splits.length}) should be <= defaultParallelism ($parallelism): ") {
        splits.length should be <= parallelism
      }
      txLog.close()
    }
  }
}
