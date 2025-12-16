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

import org.apache.spark.sql.sources.{EqualTo, Filter, StringContains}
import org.scalatest.BeforeAndAfterEach

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.AddXRefAction

class XRefSearcherTest extends TestBase with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    XRefSearcher.resetAvailabilityCheck()
  }

  // Test availability check
  test("isAvailable should return true when XRef API is available") {
    // Reset and check
    XRefSearcher.resetAvailabilityCheck()
    val available = XRefSearcher.isAvailable()
    // XRef API is now implemented via tantivy4java
    assert(available, "XRef API should be available")
  }

  test("isAvailable should be idempotent") {
    XRefSearcher.resetAvailabilityCheck()
    val first = XRefSearcher.isAvailable()
    val second = XRefSearcher.isAvailable()
    assert(first == second)
  }

  // Test searchSplits behavior when API not available
  test("searchSplits should return all source splits when API not available") {
    XRefSearcher.resetAvailabilityCheck()

    val sourceSplits = Seq(
      "/table/split1.split",
      "/table/split2.split",
      "/table/split3.split"
    )
    val xref = AddXRefAction(
      path = "_xrefsplits/test/test-xref-001.split",
      xrefId = "test-xref-001",
      sourceSplitPaths = sourceSplits,
      sourceSplitCount = sourceSplits.size,
      size = 10000L,
      totalTerms = 1000L,
      footerStartOffset = 0L,
      footerEndOffset = 100L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 1000L,
      maxSourceSplits = 100
    )

    // Use filter-based API
    val filters: Array[Filter] = Array(EqualTo("content", "test query"))

    val results = XRefSearcher.searchSplits(
      xrefPath = "/table/_xrefsplits/test/test-xref-001.split",
      xref = xref,
      filters = filters,
      timeoutMs = 5000,
      tablePath = "/table",
      sparkSession = spark
    )

    // Should return all source split filenames when API not available
    assert(results.size == 3)
    assert(results.contains("split1.split"))
    assert(results.contains("split2.split"))
    assert(results.contains("split3.split"))
  }

  test("searchSplits should extract filenames correctly") {
    XRefSearcher.resetAvailabilityCheck()

    val sourceSplits = Seq(
      "s3://bucket/table/date=2024-01-01/part-00001.split",
      "s3://bucket/table/date=2024-01-02/part-00002.split"
    )
    val xref = AddXRefAction(
      path = "_xrefsplits/test/test-xref-002.split",
      xrefId = "test-xref-002",
      sourceSplitPaths = sourceSplits,
      sourceSplitCount = sourceSplits.size,
      size = 5000L,
      totalTerms = 500L,
      footerStartOffset = 0L,
      footerEndOffset = 50L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 500L,
      maxSourceSplits = 100
    )

    // Use filter-based API
    val filters: Array[Filter] = Array(StringContains("content", "search term"))

    val results = XRefSearcher.searchSplits(
      xrefPath = "s3://bucket/table/_xrefsplits/test/test-xref-002.split",
      xref = xref,
      filters = filters,
      timeoutMs = 5000,
      tablePath = "s3://bucket/table",
      sparkSession = spark
    )

    // Should extract just filenames
    assert(results.size == 2)
    assert(results.contains("part-00001.split"))
    assert(results.contains("part-00002.split"))
  }

  // ============================================================================
  // Integration tests that validate XRef search actually filters splits
  // ============================================================================

  test("XRef search should return only splits containing the queried term") {
    val ss = spark // stable identifier for implicits
    import ss.implicits._

    withTempPath { tablePath =>
      // Create data with UNIQUE single-token terms in each partition so we can verify filtering
      // Standard tokenizers split on underscores, so use single tokens without underscores
      // Partition A: contains "alphaunique" only
      // Partition B: contains "betaunique" only
      // Both: contain "sharedterm"
      val dataA = (1 to 100).toSeq.map(i => (i, s"alphaunique content $i sharedterm", "partition_a"))
      val dataB = (1 to 100).toSeq.map(i => (i, s"betaunique content $i sharedterm", "partition_b"))

      val dfA = dataA.toDF("id", "content", "partition_key")
      val dfB = dataB.toDF("id", "content", "partition_key")

      // Write partitioned data
      // IMPORTANT: Configure content as "text" field so it gets tokenized for term-based search
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

      dfA.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      dfB.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .partitionBy("partition_key")
        .mode("append")
        .save(tablePath)

      // Build XRef
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Get XRef metadata from transaction log and verify XRef was built
      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath),
        spark
      )
      val xrefs = transactionLog.listXRefs()
      assert(xrefs.nonEmpty, "XRef should be created")

      val xref = xrefs.head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId,
        XRefConfig.fromSparkSession(spark).storage.directory
      )

      // Reset availability check to use actual implementation
      XRefSearcher.resetAvailabilityCheck()

      // Test 1: Query for content = "alphaunique" should only return partition_a splits
      // Note: tantivy indexes text as lowercase tokens
      val filtersA: Array[Filter] = Array(EqualTo("content", "alphaunique"))
      val resultsA = XRefSearcher.searchSplits(
        xrefPath = xrefFullPath,
        xref = xref,
        filters = filtersA,
        timeoutMs = 5000,
        tablePath = tablePath,
        sparkSession = spark
      )

      // Should NOT return all splits - only partition_a splits
      assert(resultsA.size < xref.sourceSplitPaths.size,
        s"Query for content=alphaunique should filter splits, but got ${resultsA.size} of ${xref.sourceSplitPaths.size}")

      // Test 2: Query for content = "betaunique" should only return partition_b splits
      val filtersB: Array[Filter] = Array(EqualTo("content", "betaunique"))
      val resultsB = XRefSearcher.searchSplits(
        xrefPath = xrefFullPath,
        xref = xref,
        filters = filtersB,
        timeoutMs = 5000,
        tablePath = tablePath,
        sparkSession = spark
      )

      assert(resultsB.size < xref.sourceSplitPaths.size,
        s"Query for content=betaunique should filter splits, but got ${resultsB.size} of ${xref.sourceSplitPaths.size}")

      // Test 3: Query for content = "sharedterm" should return all splits (present in both partitions)
      val filtersCommon: Array[Filter] = Array(EqualTo("content", "sharedterm"))
      val resultsCommon = XRefSearcher.searchSplits(
        xrefPath = xrefFullPath,
        xref = xref,
        filters = filtersCommon,
        timeoutMs = 5000,
        tablePath = tablePath,
        sparkSession = spark
      )

      assert(resultsCommon.size == xref.sourceSplitPaths.size,
        s"Query for content=sharedterm should return all splits, but got ${resultsCommon.size} of ${xref.sourceSplitPaths.size}")

      // Cleanup
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  test("XRef search should return empty for non-existent term") {
    val ss = spark // stable identifier for implicits
    import ss.implicits._

    withTempPath { tablePath =>
      // Create simple test data
      val data = (1 to 100).toSeq.map(i => (i, s"searchable content $i", "test_partition"))
      val df = data.toDF("id", "content", "partition_key")

      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "1")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      // Build XRef
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Get XRef metadata
      val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tablePath),
        spark
      )
      val xrefs = transactionLog.listXRefs()
      assert(xrefs.nonEmpty, "Should have at least one XRef")

      val xref = xrefs.head
      val xrefFullPath = XRefStorageUtils.getXRefFullPathString(
        tablePath, xref.xrefId,
        XRefConfig.fromSparkSession(spark).storage.directory
      )

      XRefSearcher.resetAvailabilityCheck()

      // Query for a term that definitely doesn't exist
      val filters: Array[Filter] = Array(EqualTo("content", "NONEXISTENT_TERM_XYZZY_12345"))
      val results = XRefSearcher.searchSplits(
        xrefPath = xrefFullPath,
        xref = xref,
        filters = filters,
        timeoutMs = 5000,
        tablePath = tablePath,
        sparkSession = spark
      )

      // Should return empty (or very few) - not all splits
      assert(results.size < xref.sourceSplitPaths.size,
        s"Query for non-existent term should not return all ${xref.sourceSplitPaths.size} splits, got ${results.size}")

      // Cleanup
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }
}
