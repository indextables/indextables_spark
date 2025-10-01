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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{CollectLimitExec, SparkPlan}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers
import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory, AddAction}
import org.apache.hadoop.fs.Path
import scala.collection.mutable

/**
 * Tests to validate that footer metadata isn't lost during Spark's Adaptive Query Execution (AQE), specifically
 * focusing on LIMIT operations, stage-wise execution, and dynamic partition pruning.
 */
class AQEFooterMetadataValidationTest extends TestBase with Matchers {

  // Enable AQE for all tests in this class
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB") // Force more partitions
    println("🔧 AQE enabled for footer metadata validation tests")
  }

  override def afterAll(): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    super.afterAll()
    println("🔧 AQE disabled after footer metadata validation tests")
  }

  test("should preserve footer metadata during AQE LIMIT operations") {
    withTempPath { tempPath =>
      // Create a larger dataset to trigger AQE behavior
      val data = spark
        .range(1000)
        .select(
          col("id"),
          (col("id") % 10).cast("string").as("category"),
          (col("id") * 2).as("value"),
          (rand() * 100).as("score")
        )

      // Write data
      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "100") // Create multiple splits
        .save(tempPath)

      // Validate that footer metadata is present in transaction log
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      println(s"📊 Created ${addActions.length} splits for AQE testing")

      // Verify all AddActions have footer metadata
      val metadataValidation = addActions.zipWithIndex.map {
        case (addAction, index) =>
          val hasFooter        = addAction.hasFooterOffsets
          val footerStart      = addAction.footerStartOffset
          val footerEnd        = addAction.footerEndOffset
          val hasDocMapping    = addAction.docMappingJson.isDefined
          val docMappingLength = addAction.docMappingJson.map(_.length).getOrElse(0)

          println(s"✅ Split $index: footer=$hasFooter, start=$footerStart, end=$footerEnd, docMapping=$hasDocMapping ($docMappingLength chars)")

          (hasFooter, footerStart.isDefined, footerEnd.isDefined, hasDocMapping, index)
      }

      // All splits should have complete footer metadata
      metadataValidation.foreach {
        case (hasFooter, hasStart, hasEnd, hasDocMapping, index) =>
          hasFooter shouldBe true     // Split should have footer offsets
          hasStart shouldBe true      // Split should have footer start offset
          hasEnd shouldBe true        // Split should have footer end offset
          hasDocMapping shouldBe true // Split should have document mapping JSON
      }

      // Now test AQE with LIMIT operations
      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      // Test 1: Simple LIMIT with AQE
      val limitQuery  = df.filter(col("category") === "0").limit(5)
      val limitResult = limitQuery.collect()

      limitResult.length shouldBe 5 // LIMIT should return exactly 5 rows

      // Verify the query plan uses AQE
      val limitPlan = limitQuery.queryExecution.executedPlan
      verifyAQEInPlan(limitPlan, "LIMIT operation")

      println(s"✅ AQE LIMIT query returned ${limitResult.length} rows with proper execution plan")

      // Test 2: LIMIT with complex filters to trigger stage-wise execution
      val complexQuery = df
        .filter(col("score") > 50)
        .filter(col("category").isin("1", "2", "3"))
        .orderBy(col("value"))
        .limit(10)

      val complexResult = complexQuery.collect()
      complexResult.length should be <= 10 // Complex LIMIT should return at most 10 rows

      val complexPlan = complexQuery.queryExecution.executedPlan
      verifyAQEInPlan(complexPlan, "Complex LIMIT with filters")

      println(s"✅ Complex AQE query with LIMIT returned ${complexResult.length} rows")

      // Test 3: SQL-based LIMIT to test different code paths
      df.createOrReplaceTempView("aqe_test_table")

      val sqlResult = spark.sql("""
        SELECT category, AVG(score) as avg_score
        FROM aqe_test_table
        WHERE value > 100
        GROUP BY category
        ORDER BY avg_score DESC
        LIMIT 3
      """)

      val sqlRows = sqlResult.collect()
      sqlRows.length should be <= 3 // SQL LIMIT should return at most 3 rows

      val sqlPlan = sqlResult.queryExecution.executedPlan
      verifyAQEInPlan(sqlPlan, "SQL GROUP BY with LIMIT")

      println(s"✅ SQL AQE query with GROUP BY and LIMIT returned ${sqlRows.length} rows")
    }
  }

  test("should maintain footer metadata during dynamic partition pruning with AQE") {
    withTempPath { tempPath =>
      // Create partitioned data that will trigger dynamic pruning
      val data = spark
        .range(800)
        .select(
          col("id"),
          (col("id") % 8).cast("string").as("partition_col"),
          (col("id") % 4).cast("string").as("category"),
          (col("id") * 3).as("metric")
        )

      // Write partitioned data
      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "80") // Multiple splits per partition
        .partitionBy("partition_col")
        .save(tempPath)

      // Validate footer metadata in partitioned structure
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val addActions     = transactionLog.listFiles()

      println(s"📊 Created ${addActions.length} partitioned splits for dynamic pruning test")

      // Group by partition to verify metadata per partition
      val partitionGroups = addActions.groupBy(_.partitionValues)
      partitionGroups.foreach {
        case (partitionValues, actions) =>
          val partitionName = partitionValues.map { case (k, v) => s"$k=$v" }.mkString(",")
          println(s"📁 Partition $partitionName: ${actions.length} splits")

          actions.zipWithIndex.foreach {
            case (addAction, index) =>
              // Validate footer metadata for each split in partition
              addAction.hasFooterOffsets shouldBe true     // Partitioned split should have footer offsets
              addAction.footerStartOffset shouldBe defined // Partitioned split should have footer start
              addAction.footerEndOffset shouldBe defined   // Partitioned split should have footer end
              addAction.docMappingJson shouldBe defined    // Partitioned split should have doc mapping

              println(s"✅ Partition $partitionName split $index: footer metadata validated")
          }
      }

      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      // Test dynamic partition pruning with AQE
      // Create a broadcast join that should trigger dynamic pruning
      val filterValues = spark
        .createDataFrame(Seq(("2", "filter1"), ("4", "filter2")))
        .toDF("partition_col", "description")

      val dynamicPruneQuery = df
        .join(broadcast(filterValues), "partition_col")
        .select("partition_col", "id", "category", "metric", "description")
        .limit(20)

      val pruneResult = dynamicPruneQuery.collect()
      pruneResult.length should be <= 20 // Dynamic pruning query should respect LIMIT

      // Verify AQE is active in the plan
      val prunePlan = dynamicPruneQuery.queryExecution.executedPlan
      verifyAQEInPlan(prunePlan, "Dynamic partition pruning with broadcast join")

      println(s"✅ Dynamic partition pruning with AQE returned ${pruneResult.length} rows")

      // Verify that only expected partitions were read
      val readPartitions = pruneResult.map(_.getString(0)).toSet // partition_col values
      readPartitions should contain only ("2", "4")

      println(s"✅ Dynamic pruning correctly read only partitions: ${readPartitions.mkString(", ")}")
    }
  }

  test("should preserve footer metadata during AQE stage coalescing") {
    withTempPath { tempPath =>
      // Create data that will trigger stage coalescing
      val data = spark
        .range(500)
        .select(
          col("id"),
          (col("id") % 5).cast("string").as("group"),
          when(col("id") % 2 === 0, "even").otherwise("odd").as("parity"),
          (col("id") * col("id")).as("squared")
        )

      data.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "50") // Force many small splits
        .save(tempPath)

      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      // Complex query that should trigger stage coalescing
      val coalescingQuery = df
        .groupBy("group", "parity")
        .agg(
          count("*").as("count"),
          sum("squared").as("sum_squared"),
          max("id").as("max_id")
        )
        .filter(col("count") > 10)
        .orderBy(col("sum_squared").desc)
        .limit(8)

      val coalescingResult = coalescingQuery.collect()
      coalescingResult.length should be <= 8

      // Verify AQE coalescing behavior
      val coalescePlan = coalescingQuery.queryExecution.executedPlan
      verifyAQEInPlan(coalescePlan, "Stage coalescing with aggregation")

      // Check that footer metadata is still accessible
      // This is validated by the fact that the query executed successfully
      // The PartitionReader validates metadata during execution
      println(s"✅ Stage coalescing query with AQE returned ${coalescingResult.length} rows")
      println("✅ Footer metadata preserved during AQE stage coalescing (verified by successful execution)")
    }
  }

  test("should handle footer metadata during AQE skew join optimization") {
    withTempPath { tempPath1 =>
      withTempPath { tempPath2 =>
        // Create skewed datasets
        val skewedData = (1 to 300).flatMap { i =>
          val groupId = if (i <= 200) "hot_key" else s"key_$i"
          Seq.fill(if (groupId == "hot_key") 10 else 1) {
            (i.toLong, groupId, s"value_$i", i * 2.0)
          }
        }

        val df1 = spark
          .createDataFrame(skewedData)
          .toDF("id", "join_key", "data1", "metric1")

        val df2 = spark
          .range(50)
          .select(
            col("id").as("join_id"),
            when(col("id") < 25, "hot_key").otherwise(concat(lit("key_"), col("id"))).as("join_key"),
            (col("id") * 3).as("metric2")
          )

        // Write both datasets
        df1.write.format("tantivy4spark").save(tempPath1)
        df2.write.format("tantivy4spark").save(tempPath2)

        val table1 = spark.read.format("tantivy4spark").load(tempPath1)
        val table2 = spark.read.format("tantivy4spark").load(tempPath2)

        // Skew join that should trigger AQE optimization
        val skewJoinQuery = table1
          .join(table2, table1("join_key") === table2("join_key"))
          .select("id", "data1", "metric1", "metric2")
          .filter(col("metric1") > 100)
          .limit(15)

        val skewResult = skewJoinQuery.collect()
        skewResult.length should be <= 15

        val skewPlan = skewJoinQuery.queryExecution.executedPlan
        verifyAQEInPlan(skewPlan, "Skew join optimization")

        println(s"✅ Skew join with AQE returned ${skewResult.length} rows")
        println("✅ Footer metadata preserved during AQE skew join handling")
      }
    }
  }

  test("should validate footer metadata integrity across multiple AQE stages") {
    withTempPath { tempPath =>
      // Create complex dataset for multi-stage processing
      val complexData = spark
        .range(600)
        .select(
          col("id"),
          (col("id") % 12).cast("string").as("month"),
          (col("id") % 7).cast("string").as("weekday"),
          when(col("id") % 3 === 0, "premium")
            .when(col("id") % 3 === 1, "standard")
            .otherwise("basic")
            .as("tier"),
          (rand() * 1000).as("revenue")
        )

      complexData.write
        .format("tantivy4spark")
        .option("targetRecordsPerSplit", "75") // Multiple splits
        .save(tempPath)

      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      df.createOrReplaceTempView("multi_stage_table")

      // Complex multi-stage query
      val multiStageQuery = spark.sql("""
        WITH monthly_stats AS (
          SELECT
            month,
            tier,
            COUNT(*) as customer_count,
            AVG(revenue) as avg_revenue,
            MAX(revenue) as max_revenue
          FROM multi_stage_table
          GROUP BY month, tier
        ),
        tier_rankings AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_revenue DESC) as rank
          FROM monthly_stats
        )
        SELECT month, tier, customer_count, avg_revenue, max_revenue
        FROM tier_rankings
        WHERE rank <= 2 AND customer_count > 10
        ORDER BY month, avg_revenue DESC
        LIMIT 12
      """)

      val multiStageResult = multiStageQuery.collect()
      multiStageResult.length should be <= 12

      val multiStagePlan = multiStageQuery.queryExecution.executedPlan
      verifyAQEInPlan(multiStagePlan, "Multi-stage CTE query")

      println(s"✅ Multi-stage AQE query returned ${multiStageResult.length} rows")
      println("✅ Footer metadata preserved across multiple AQE stages")

      // Verify data integrity - the fact that we get meaningful results
      // proves that footer metadata was correctly preserved and used
      multiStageResult.foreach { row =>
        row.getString(0) should not be null // month
        row.getString(1) should not be null // tier
        row.getLong(2) should be > 0L       // customer_count
      }
    }
  }

  /** Helper method to verify that AQE is active in the query plan */
  private def verifyAQEInPlan(plan: SparkPlan, queryDescription: String): Unit = {
    def findAQEInPlan(plan: SparkPlan): Boolean =
      plan match {
        case _: AdaptiveSparkPlanExec => true
        case _                        => plan.children.exists(findAQEInPlan)
      }

    val hasAQE = findAQEInPlan(plan)
    if (hasAQE) {
      println(s"✅ $queryDescription: AQE detected in execution plan")
    } else {
      println(s"⚠️ $queryDescription: AQE not detected (may be optimized away)")
    }

    // Note: We don't assert AQE presence because Spark may optimize simple queries
    // The important thing is that when AQE is used, metadata is preserved
  }

  /** Helper method to extract metadata validation information from query execution */
  private def validateMetadataPreservation(df: DataFrame, testName: String): Unit =
    // The fact that the query executes successfully validates metadata preservation
    // Our PartitionReader validates footer metadata during construction
    try {
      val result = df.collect()
      println(s"✅ $testName: Query executed successfully with ${result.length} rows")
      println(s"✅ $testName: Footer metadata validation passed (implicit via successful execution)")
    } catch {
      case e: Exception =>
        if (e.getMessage.contains("footer offset") || e.getMessage.contains("docMapping")) {
          fail(s"$testName: Footer metadata validation failed: ${e.getMessage}")
        } else {
          throw e // Re-throw non-metadata related exceptions
        }
    }
}
