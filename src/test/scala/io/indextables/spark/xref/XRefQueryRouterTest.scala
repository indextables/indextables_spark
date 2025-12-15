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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{AddAction, AddXRefAction, TransactionLogFactory}

class XRefQueryRouterTest extends TestBase with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Reset XRef searcher availability check for each test
    XRefSearcher.resetAvailabilityCheck()
  }

  // Helper to create test AddAction
  private def createAddAction(path: String, numRecords: Option[Long] = None): AddAction =
    AddAction(
      path = path,
      partitionValues = Map.empty,
      size = 1024L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = numRecords
    )

  // Helper to create test AddXRefAction
  private def createXRefAction(
    xrefId: String,
    sourceSplits: Seq[String],
    termCount: Long = 1000L
  ): AddXRefAction =
    AddXRefAction(
      path = s"_xrefsplits/test/$xrefId.split",
      xrefId = xrefId,
      sourceSplitPaths = sourceSplits,
      sourceSplitCount = sourceSplits.size,
      size = termCount * 10,
      totalTerms = termCount,
      footerStartOffset = 0L,
      footerEndOffset = 100L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 1000L,
      maxSourceSplits = 100
    )

  test("XRefQueryRouter should return all splits when XRef query is disabled") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = false), // Disabled
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = Seq(
          createAddAction("/test/split1.split"),
          createAddAction("/test/split2.split"),
          createAddAction("/test/split3.split")
        )

        val result = router.routeQuery(candidateSplits, Array.empty[Filter])

        assert(!result.usedXRef)
        assert(result.candidateSplits.size == 3)
        assert(result.fallbackReason.contains("XRef query disabled"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should return all splits when below minSplitsForXRef threshold") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 10), // High threshold
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = Seq(
          createAddAction("/test/split1.split"),
          createAddAction("/test/split2.split"),
          createAddAction("/test/split3.split")
        )

        val result = router.routeQuery(candidateSplits, Array.empty[Filter])

        assert(!result.usedXRef)
        assert(result.candidateSplits.size == 3)
        assert(result.fallbackReason.exists(_.contains("Below threshold")))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should return all splits when no searchable query") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // Empty filters - no searchable query
        val result = router.routeQuery(candidateSplits, Array.empty[Filter])

        assert(!result.usedXRef)
        assert(result.candidateSplits.size == 5)
        assert(result.fallbackReason.contains("No searchable query"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should return all splits when no XRefs available") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // EqualTo filter with string value - searchable
        val filters = Array[Filter](EqualTo("content", "test value"))
        val result = router.routeQuery(candidateSplits, filters)

        assert(!result.usedXRef)
        assert(result.candidateSplits.size == 5)
        assert(result.fallbackReason.contains("No XRefs available"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should extract query from EqualTo filter") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // EqualTo filter with string value
        val filters = Array[Filter](EqualTo("content", "search term"))
        val result = router.routeQuery(candidateSplits, filters)

        // Should process but fall back due to no XRefs
        assert(!result.usedXRef)
        assert(result.fallbackReason.contains("No XRefs available"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should extract query from StringContains filter") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // StringContains filter
        val filters = Array[Filter](StringContains("content", "substring"))
        val result = router.routeQuery(candidateSplits, filters)

        // Should process but fall back due to no XRefs
        assert(!result.usedXRef)
        assert(result.fallbackReason.contains("No XRefs available"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should extract query from nested And filter") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // Nested And filter
        val filters = Array[Filter](
          And(EqualTo("content", "term1"), StringStartsWith("content", "prefix"))
        )
        val result = router.routeQuery(candidateSplits, filters)

        // Should process but fall back due to no XRefs
        assert(!result.usedXRef)
        assert(result.fallbackReason.contains("No XRefs available"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefQueryRouter should not extract query from numeric filters") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(enabled = true, minSplitsForXRef = 1),
          storage = XRefStorageConfig()
        )

        val router = new XRefQueryRouter(transactionLog, config, spark)
        val candidateSplits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

        // Only numeric filters - no searchable query
        val filters = Array[Filter](
          GreaterThan("id", 10),
          LessThan("id", 100)
        )
        val result = router.routeQuery(candidateSplits, filters)

        assert(!result.usedXRef)
        assert(result.fallbackReason.contains("No searchable query"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefRoutingResult should correctly report metrics") {
    val splits = (1 to 10).map(i => createAddAction(s"/test/split$i.split"))

    val result = XRefRoutingResult(
      candidateSplits = splits.take(3),
      skippedSplits = 7,
      xrefsQueried = 2,
      xrefQueryTimeMs = 150L,
      usedXRef = true,
      fallbackReason = None
    )

    assert(result.candidateSplits.size == 3)
    assert(result.skippedSplits == 7)
    assert(result.xrefsQueried == 2)
    assert(result.xrefQueryTimeMs == 150L)
    assert(result.usedXRef)
    assert(result.fallbackReason.isEmpty)
  }

  test("XRefRoutingResult should include fallback reason when not used") {
    val splits = (1 to 5).map(i => createAddAction(s"/test/split$i.split"))

    val result = XRefRoutingResult(
      candidateSplits = splits,
      skippedSplits = 0,
      xrefsQueried = 0,
      xrefQueryTimeMs = 5L,
      usedXRef = false,
      fallbackReason = Some("XRef query disabled")
    )

    assert(result.candidateSplits.size == 5)
    assert(result.skippedSplits == 0)
    assert(!result.usedXRef)
    assert(result.fallbackReason.contains("XRef query disabled"))
  }
}
