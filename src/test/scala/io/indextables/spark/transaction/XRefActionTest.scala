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

package io.indextables.spark.transaction

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.util.JsonUtil

class XRefActionTest extends TestBase {

  // ============================================================================
  // AddXRefAction Tests
  // ============================================================================

  test("AddXRefAction should serialize to JSON correctly") {
    val action = AddXRefAction(
      path = "_xrefsplits/kmpq/xref-abc123.split",
      xrefId = "abc123",
      sourceSplitPaths = Seq("part-00000-def.split", "date=2024-01-15/part-00001-ghi.split"),
      sourceSplitCount = 2,
      size = 15728640L,
      totalTerms = 125000L,
      footerStartOffset = 15700000L,
      footerEndOffset = 15728640L,
      createdTime = 1702600000000L,
      buildDurationMs = 5432L,
      maxSourceSplits = 1024
    )

    val json = JsonUtil.mapper.writeValueAsString(action)

    json should include("\"path\":\"_xrefsplits/kmpq/xref-abc123.split\"")
    json should include("\"xrefId\":\"abc123\"")
    json should include("\"sourceSplitCount\":2")
    json should include("\"size\":15728640")
    json should include("\"totalTerms\":125000")
    json should include("\"footerStartOffset\":15700000")
    json should include("\"footerEndOffset\":15728640")
    json should include("\"maxSourceSplits\":1024")
    json should include("part-00000-def.split")
    json should include("date=2024-01-15/part-00001-ghi.split")
  }

  test("AddXRefAction should deserialize from JSON correctly") {
    val json =
      """{
        |  "path": "_xrefsplits/wxyz/xref-def456.split",
        |  "xrefId": "def456",
        |  "sourceSplitPaths": ["split1.split", "split2.split", "split3.split"],
        |  "sourceSplitCount": 3,
        |  "size": 20000000,
        |  "totalTerms": 200000,
        |  "footerStartOffset": 19900000,
        |  "footerEndOffset": 20000000,
        |  "createdTime": 1702700000000,
        |  "buildDurationMs": 3000,
        |  "maxSourceSplits": 2048
        |}""".stripMargin

    val action = JsonUtil.mapper.readValue(json, classOf[AddXRefAction])

    action.path shouldBe "_xrefsplits/wxyz/xref-def456.split"
    action.xrefId shouldBe "def456"
    action.sourceSplitPaths should have length 3
    action.sourceSplitPaths should contain("split1.split")
    action.sourceSplitPaths should contain("split2.split")
    action.sourceSplitPaths should contain("split3.split")
    action.sourceSplitCount shouldBe 3
    action.size shouldBe 20000000L
    action.totalTerms shouldBe 200000L
    action.footerStartOffset shouldBe 19900000L
    action.footerEndOffset shouldBe 20000000L
    action.createdTime shouldBe 1702700000000L
    action.buildDurationMs shouldBe 3000L
    action.maxSourceSplits shouldBe 2048
  }

  test("AddXRefAction should round-trip through JSON") {
    val original = AddXRefAction(
      path = "_xrefsplits/abcd/xref-test123.split",
      xrefId = "test123",
      sourceSplitPaths = Seq("a.split", "b.split", "c.split", "d.split"),
      sourceSplitCount = 4,
      size = 50000000L,
      totalTerms = 500000L,
      footerStartOffset = 49000000L,
      footerEndOffset = 50000000L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 10000L,
      maxSourceSplits = 1024
    )

    val json        = JsonUtil.mapper.writeValueAsString(original)
    val deserialized = JsonUtil.mapper.readValue(json, classOf[AddXRefAction])

    deserialized shouldBe original
  }

  // ============================================================================
  // RemoveXRefAction Tests
  // ============================================================================

  test("RemoveXRefAction should serialize to JSON correctly") {
    val action = RemoveXRefAction(
      path = "_xrefsplits/kmpq/xref-abc123.split",
      xrefId = "abc123",
      deletionTimestamp = 1702600500000L,
      reason = "replaced"
    )

    val json = JsonUtil.mapper.writeValueAsString(action)

    json should include("\"path\":\"_xrefsplits/kmpq/xref-abc123.split\"")
    json should include("\"xrefId\":\"abc123\"")
    json should include("\"deletionTimestamp\":1702600500000")
    json should include("\"reason\":\"replaced\"")
  }

  test("RemoveXRefAction should deserialize from JSON correctly") {
    val json =
      """{
        |  "path": "_xrefsplits/wxyz/xref-old.split",
        |  "xrefId": "old",
        |  "deletionTimestamp": 1702800000000,
        |  "reason": "source_changed"
        |}""".stripMargin

    val action = JsonUtil.mapper.readValue(json, classOf[RemoveXRefAction])

    action.path shouldBe "_xrefsplits/wxyz/xref-old.split"
    action.xrefId shouldBe "old"
    action.deletionTimestamp shouldBe 1702800000000L
    action.reason shouldBe "source_changed"
  }

  test("RemoveXRefAction should round-trip through JSON") {
    val original = RemoveXRefAction(
      path = "_xrefsplits/test/xref-remove.split",
      xrefId = "remove",
      deletionTimestamp = System.currentTimeMillis(),
      reason = "explicit"
    )

    val json        = JsonUtil.mapper.writeValueAsString(original)
    val deserialized = JsonUtil.mapper.readValue(json, classOf[RemoveXRefAction])

    deserialized shouldBe original
  }

  // ============================================================================
  // TransactionLog XRef Integration Tests
  // ============================================================================

  test("should add XRef to transaction log") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add some data splits first
        val addAction = AddAction(
          path = "part-00000-abc.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        )
        transactionLog.addFile(addAction)

        // Add XRef
        val xrefAction = AddXRefAction(
          path = "_xrefsplits/kmpq/xref-test.split",
          xrefId = "test",
          sourceSplitPaths = Seq("part-00000-abc.split"),
          sourceSplitCount = 1,
          size = 5000000L,
          totalTerms = 50000L,
          footerStartOffset = 4900000L,
          footerEndOffset = 5000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 1000L,
          maxSourceSplits = 1024
        )

        val version = transactionLog.addXRef(xrefAction)
        version should be > 0L

        // Verify XRef is listed
        val xrefs = transactionLog.listXRefs()
        xrefs should have length 1
        xrefs.head.xrefId shouldBe "test"
        xrefs.head.sourceSplitCount shouldBe 1
      } finally
        transactionLog.close()
    }
  }

  test("should remove XRef from transaction log") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add XRef
        val xrefAction = AddXRefAction(
          path = "_xrefsplits/abcd/xref-toremove.split",
          xrefId = "toremove",
          sourceSplitPaths = Seq("split1.split"),
          sourceSplitCount = 1,
          size = 1000000L,
          totalTerms = 10000L,
          footerStartOffset = 900000L,
          footerEndOffset = 1000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 500L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(xrefAction)

        // Verify XRef exists
        transactionLog.listXRefs() should have length 1

        // Remove XRef
        transactionLog.removeXRef("_xrefsplits/abcd/xref-toremove.split", "explicit")

        // Verify XRef is removed
        transactionLog.listXRefs() shouldBe empty

        // Verify removed XRef is tracked
        val removedXRefs = transactionLog.listRemovedXRefs()
        removedXRefs should have length 1
        removedXRefs.head.xrefId shouldBe "toremove"
        removedXRefs.head.reason shouldBe "explicit"
      } finally
        transactionLog.close()
    }
  }

  test("should atomically replace XRef") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add original XRef
        val originalXRef = AddXRefAction(
          path = "_xrefsplits/kmpq/xref-original.split",
          xrefId = "original",
          sourceSplitPaths = Seq("split1.split", "split2.split"),
          sourceSplitCount = 2,
          size = 2000000L,
          totalTerms = 20000L,
          footerStartOffset = 1900000L,
          footerEndOffset = 2000000L,
          createdTime = System.currentTimeMillis() - 10000,
          buildDurationMs = 1000L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(originalXRef)

        // Create replacement XRef
        val removeAction = RemoveXRefAction(
          path = "_xrefsplits/kmpq/xref-original.split",
          xrefId = "original",
          deletionTimestamp = System.currentTimeMillis(),
          reason = "replaced"
        )

        val newXRef = AddXRefAction(
          path = "_xrefsplits/wxyz/xref-replacement.split",
          xrefId = "replacement",
          sourceSplitPaths = Seq("split1.split", "split2.split", "split3.split"),
          sourceSplitCount = 3,
          size = 3000000L,
          totalTerms = 30000L,
          footerStartOffset = 2900000L,
          footerEndOffset = 3000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 1500L,
          maxSourceSplits = 1024
        )

        // Atomically replace
        val version = transactionLog.replaceXRef(removeAction, newXRef)
        version should be > 0L

        // Verify only new XRef exists
        val xrefs = transactionLog.listXRefs()
        xrefs should have length 1
        xrefs.head.xrefId shouldBe "replacement"
        xrefs.head.sourceSplitCount shouldBe 3
      } finally
        transactionLog.close()
    }
  }

  test("should find XRef covering a specific split") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add XRef covering multiple splits
        val xrefAction = AddXRefAction(
          path = "_xrefsplits/test/xref-covering.split",
          xrefId = "covering",
          sourceSplitPaths = Seq(
            "part-00000-aaa.split",
            "date=2024-01-15/part-00001-bbb.split",
            "part-00002-ccc.split"
          ),
          sourceSplitCount = 3,
          size = 5000000L,
          totalTerms = 50000L,
          footerStartOffset = 4900000L,
          footerEndOffset = 5000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 2000L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(xrefAction)

        // Find XRef for covered split (should find by filename)
        val xref1 = transactionLog.getXRefForSplit("part-00000-aaa.split")
        xref1 shouldBe defined
        xref1.get.xrefId shouldBe "covering"

        // Find XRef for partitioned split (should find by filename)
        val xref2 = transactionLog.getXRefForSplit("some/other/path/part-00001-bbb.split")
        xref2 shouldBe defined
        xref2.get.xrefId shouldBe "covering"

        // Non-covered split should return None
        val xref3 = transactionLog.getXRefForSplit("part-99999-unknown.split")
        xref3 shouldBe empty
      } finally
        transactionLog.close()
    }
  }

  test("should get splits not covered by any XRef") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add some data splits
        val split1 = AddAction(
          path = "part-00000-covered.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
        val split2 = AddAction(
          path = "part-00001-uncovered.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
        val split3 = AddAction(
          path = "part-00002-covered.split",
          partitionValues = Map.empty,
          size = 3000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )

        transactionLog.addFile(split1)
        transactionLog.addFile(split2)
        transactionLog.addFile(split3)

        // Add XRef covering only split1 and split3
        val xrefAction = AddXRefAction(
          path = "_xrefsplits/test/xref-partial.split",
          xrefId = "partial",
          sourceSplitPaths = Seq("part-00000-covered.split", "part-00002-covered.split"),
          sourceSplitCount = 2,
          size = 3000000L,
          totalTerms = 30000L,
          footerStartOffset = 2900000L,
          footerEndOffset = 3000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 1000L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(xrefAction)

        // Get uncovered splits
        val uncovered = transactionLog.getSplitsWithoutXRef()
        uncovered should have length 1
        uncovered.head.path shouldBe "part-00001-uncovered.split"
      } finally
        transactionLog.close()
    }
  }

  test("should commit mixed data and XRef actions atomically") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add initial data
        val initialSplit = AddAction(
          path = "part-00000-initial.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
        transactionLog.addFile(initialSplit)

        // Commit mixed transaction
        val dataRemove = RemoveAction(
          path = "part-00000-initial.split",
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = true,
          extendedFileMetadata = None,
          partitionValues = None,
          size = Some(1000L)
        )

        val dataAdd = AddAction(
          path = "part-00001-new.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )

        val xrefAdd = AddXRefAction(
          path = "_xrefsplits/test/xref-mixed.split",
          xrefId = "mixed",
          sourceSplitPaths = Seq("part-00001-new.split"),
          sourceSplitCount = 1,
          size = 1000000L,
          totalTerms = 10000L,
          footerStartOffset = 900000L,
          footerEndOffset = 1000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 500L,
          maxSourceSplits = 1024
        )

        val version = transactionLog.commitWithXRefUpdates(
          dataRemoveActions = Seq(dataRemove),
          dataAddActions = Seq(dataAdd),
          xrefRemoveActions = Seq.empty,
          xrefAddActions = Seq(xrefAdd)
        )

        version should be > 0L

        // Verify data state
        val files = transactionLog.listFiles()
        files should have length 1
        files.head.path shouldBe "part-00001-new.split"

        // Verify XRef state
        val xrefs = transactionLog.listXRefs()
        xrefs should have length 1
        xrefs.head.xrefId shouldBe "mixed"
      } finally
        transactionLog.close()
    }
  }

  test("should handle multiple XRefs") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add first XRef
        val xref1 = AddXRefAction(
          path = "_xrefsplits/aaaa/xref-first.split",
          xrefId = "first",
          sourceSplitPaths = Seq("split1.split", "split2.split"),
          sourceSplitCount = 2,
          size = 2000000L,
          totalTerms = 20000L,
          footerStartOffset = 1900000L,
          footerEndOffset = 2000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 1000L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(xref1)

        // Add second XRef
        val xref2 = AddXRefAction(
          path = "_xrefsplits/bbbb/xref-second.split",
          xrefId = "second",
          sourceSplitPaths = Seq("split3.split", "split4.split", "split5.split"),
          sourceSplitCount = 3,
          size = 3000000L,
          totalTerms = 30000L,
          footerStartOffset = 2900000L,
          footerEndOffset = 3000000L,
          createdTime = System.currentTimeMillis(),
          buildDurationMs = 1500L,
          maxSourceSplits = 1024
        )
        transactionLog.addXRef(xref2)

        // Verify both XRefs exist
        val xrefs = transactionLog.listXRefs()
        xrefs should have length 2
        xrefs.map(_.xrefId) should contain allOf ("first", "second")

        // Total source split count
        val totalCovered = xrefs.map(_.sourceSplitCount).sum
        totalCovered shouldBe 5
      } finally
        transactionLog.close()
    }
  }
}
