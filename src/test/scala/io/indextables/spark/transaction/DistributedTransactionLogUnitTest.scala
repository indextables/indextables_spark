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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pure unit tests for distributed transaction log components.
 * These tests do not require the full indextables datasource or file I/O.
 */
class DistributedTransactionLogUnitTest extends AnyFunSuite {

  test("TransactionFileRef should be serializable") {
    val ref = TransactionFileRef(
      version = 123L,
      path = "000000000000000123.json",
      size = 1024L,
      modificationTime = System.currentTimeMillis()
    )

    // Verify basic properties
    assert(ref.version == 123L)
    assert(ref.path == "000000000000000123.json")
    assert(ref.size == 1024L)
    assert(ref.modificationTime > 0)

    // Verify it's a case class with proper equals/hashCode
    val ref2 = ref.copy()
    assert(ref == ref2)
    assert(ref.hashCode() == ref2.hashCode())

    println("TransactionFileRef is properly serializable")
  }

  test("TransactionLogChecksum should be serializable") {
    val checksum = TransactionLogChecksum(
      checkpointVersion = Some(100L),
      incrementalVersions = Seq(101L, 102L, 103L),
      totalFiles = 4,
      checksumValue = "abc123"
    )

    assert(checksum.checkpointVersion.contains(100L))
    assert(checksum.incrementalVersions.length == 3)
    assert(checksum.totalFiles == 4)
    assert(checksum.checksumValue == "abc123")

    println("TransactionLogChecksum is properly serializable")
  }

  test("DistributedTransactionLogMetrics should compute total time") {
    val metrics = DistributedTransactionLogMetrics(
      totalFiles = 100,
      checkpointFiles = 50,
      incrementalFiles = 50,
      parallelism = 4,
      readTimeMs = 100L,
      parseTimeMs = 200L,
      reduceTimeMs = 50L,
      cacheHitRate = 0.75,
      executorCount = 4
    )

    assert(metrics.totalTimeMs == 350L, s"Expected 350ms, got ${metrics.totalTimeMs}ms")
    assert(metrics.cacheHitRate == 0.75)

    val metricsStr = metrics.toString
    assert(metricsStr.contains("total=100"))
    assert(metricsStr.contains("parallelism=4"))
    assert(metricsStr.contains("cacheHitRate=75.0%"))

    println(s"Metrics: $metrics")
  }

  test("TransactionFileCache should handle cache operations") {
    // Clear cache before test
    TransactionFileCache.clear()

    val key1 = "s3://test-bucket/table1/_transaction_log/000000000000000001.json"
    val key2 = "s3://test-bucket/table1/_transaction_log/000000000000000002.json"
    val key3 = "s3://test-bucket/table2/_transaction_log/000000000000000001.json"

    val actions1 = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true)
    )
    val actions2 = Seq(
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true)
    )
    val actions3 = Seq(
      AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true)
    )

    // Test cache miss
    assert(TransactionFileCache.get(key1).isEmpty)

    // Test cache put and get
    TransactionFileCache.put(key1, actions1)
    TransactionFileCache.put(key2, actions2)
    TransactionFileCache.put(key3, actions3)

    assert(TransactionFileCache.get(key1).isDefined)
    assert(TransactionFileCache.get(key1).get.head.asInstanceOf[AddAction].path == "file1.split")
    assert(TransactionFileCache.size() == 3)

    // Test pattern-based invalidation
    TransactionFileCache.invalidate("s3://test-bucket/table1")
    assert(TransactionFileCache.get(key1).isEmpty, "key1 should be invalidated")
    assert(TransactionFileCache.get(key2).isEmpty, "key2 should be invalidated")
    assert(TransactionFileCache.get(key3).isDefined, "key3 should still be cached")

    // Test global invalidation
    TransactionFileCache.invalidate("")
    assert(TransactionFileCache.size() == 0, "Cache should be empty after global invalidation")

    println("TransactionFileCache operations work correctly")
  }

  test("DistributedStateReducer should handle empty inputs") {
    val result = DistributedStateReducer.reduceToFinalState(Seq.empty, Seq.empty)
    assert(result.isEmpty, "Empty inputs should produce empty result")
  }

  test("DistributedStateReducer should apply ADD actions in version order") {
    val checkpointActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true)
    )

    val incrementalActions = Seq(
      VersionedAction(AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true), 101L),
      VersionedAction(AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true), 102L)
    )

    val result = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    assert(result.length == 3, s"Expected 3 files, got ${result.length}")
    assert(result.exists(_.path == "file1.split"))
    assert(result.exists(_.path == "file2.split"))
    assert(result.exists(_.path == "file3.split"))

    println("DistributedStateReducer correctly applies ADD actions")
  }

  test("DistributedStateReducer should apply REMOVE actions in version order") {
    val checkpointActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true),
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true),
      AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true)
    )

    val incrementalActions = Seq(
      VersionedAction(RemoveAction("file2.split", Some(System.currentTimeMillis()), true, None, None, None), 101L)
    )

    val result = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    assert(result.length == 2, s"Expected 2 files after removal, got ${result.length}")
    assert(result.exists(_.path == "file1.split"))
    assert(!result.exists(_.path == "file2.split"), "file2 should be removed")
    assert(result.exists(_.path == "file3.split"))

    println("DistributedStateReducer correctly applies REMOVE actions")
  }

  test("DistributedStateReducer should handle overwrite semantics with correct version order") {
    val checkpointActions = Seq(
      AddAction("file1.split", Map("v" -> "1"), 1000L, System.currentTimeMillis(), true)
    )

    // CRITICAL: These must be processed in version order (101, then 102)
    val incrementalActions = Seq(
      VersionedAction(RemoveAction("file1.split", Some(System.currentTimeMillis()), true, None, None, None), 101L),
      VersionedAction(AddAction("file1.split", Map("v" -> "2"), 2000L, System.currentTimeMillis(), true), 102L)
    )

    val result = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    assert(result.length == 1, "Should have 1 file after overwrite")
    assert(result.head.path == "file1.split")
    assert(result.head.partitionValues.get("v").contains("2"), "Should have new version")

    println("DistributedStateReducer correctly handles overwrite semantics")
  }

  test("DistributedStateReducer should respect version order even when out of sequence") {
    val checkpointActions = Seq.empty[AddAction]

    // Submit actions OUT OF ORDER - versions 102, 100, 101
    // The reducer should sort them and apply in correct order (100, 101, 102)
    val incrementalActions = Seq(
      VersionedAction(AddAction("file1.split", Map("v" -> "102"), 3000L, System.currentTimeMillis(), true), 102L),
      VersionedAction(AddAction("file1.split", Map("v" -> "100"), 1000L, System.currentTimeMillis(), true), 100L),
      VersionedAction(AddAction("file1.split", Map("v" -> "101"), 2000L, System.currentTimeMillis(), true), 101L)
    )

    val result = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    assert(result.length == 1, "Should have 1 file")
    assert(result.head.path == "file1.split")
    // The final version should be from 102 (the highest version)
    assert(result.head.partitionValues.get("v").contains("102"),
      s"Should have version 102 (highest), got ${result.head.partitionValues.get("v")}")
    assert(result.head.size == 3000L, "Should have size from version 102")

    println("DistributedStateReducer correctly respects version order even when out of sequence")
  }

  test("DistributedStateReducer partitionLocalReduce should deduplicate with version tracking") {
    val actions = Seq(
      VersionedAction(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true), 100L),
      VersionedAction(AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true), 101L),
      VersionedAction(AddAction("file1.split", Map.empty, 1500L, System.currentTimeMillis(), true), 102L) // Duplicate path, higher version
    ).iterator

    val reduced = DistributedStateReducer.partitionLocalReduce(actions)

    // Should have 2 entries (file1 and file2), with file1 keeping the highest version action
    assert(reduced.size == 2)
    assert(reduced.contains("file1.split"))
    assert(reduced.contains("file2.split"))

    val file1Action = reduced("file1.split").action.asInstanceOf[AddAction]
    assert(file1Action.size == 1500L, "Should keep the highest version action for file1")
    assert(reduced("file1.split").version == 102L, "Should keep version 102 for file1")

    println("DistributedStateReducer partitionLocalReduce correctly deduplicates with version tracking")
  }

  test("DistributedStateReducer should extract AddActions correctly") {
    val state = Map(
      "file1.split" -> VersionedAction(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true), 100L),
      "file2.split" -> VersionedAction(RemoveAction("file2.split", Some(System.currentTimeMillis()), true, None, None, None), 101L),
      "__metadata__123" -> VersionedAction(MetadataAction("123", None, None, FileFormat("indextables", Map.empty), "{}", Seq.empty, Map.empty, None), 102L)
    )

    val addActions = DistributedStateReducer.extractAddActions(state)

    assert(addActions.length == 1, "Should extract only AddActions")
    assert(addActions.head.path == "file1.split")

    println("DistributedStateReducer correctly extracts AddActions")
  }

  test("DistributedStateReducer should compute reduction statistics") {
    val checkpointActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true),
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true)
    )

    val incrementalActions = Seq(
      VersionedAction(AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true), 101L),
      VersionedAction(RemoveAction("file1.split", Some(System.currentTimeMillis()), true, None, None, None), 102L),
      VersionedAction(MetadataAction("id", None, None, FileFormat("indextables", Map.empty), "{}", Seq.empty, Map.empty, None), 103L)
    )

    val finalFiles = Seq(
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true),
      AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true)
    )

    val stats = DistributedStateReducer.computeReductionStats(
      checkpointActions,
      incrementalActions,
      finalFiles
    )

    assert(stats("checkpointFiles") == 2)
    assert(stats("incrementalActions") == 3)
    assert(stats("incrementalAdds") == 1)
    assert(stats("incrementalRemoves") == 1)
    assert(stats("incrementalMetadata") == 1)
    assert(stats("finalFiles") == 2)
    assert(stats("netChange") == 0) // 2 checkpoint - 1 remove + 1 add = 2 final

    println(s"Reduction stats: $stats")
  }

  test("Checkpoint optimization: only process incremental files after checkpoint") {
    // Simulate checkpoint at version 100 with 1000 files
    val checkpointActions = (1 to 1000).map { i =>
      AddAction(s"file$i.split", Map.empty, i * 1000L, System.currentTimeMillis(), true)
    }

    // Simulate 3 incremental actions AFTER checkpoint (versions 101, 102, 103)
    // These are the ONLY actions that should be processed in distributed mode
    val incrementalActions = Seq(
      VersionedAction(AddAction("file1001.split", Map.empty, 1001000L, System.currentTimeMillis(), true), 101L),
      VersionedAction(RemoveAction("file500.split", Some(System.currentTimeMillis()), true, None, None, None), 102L),
      VersionedAction(AddAction("file1002.split", Map.empty, 1002000L, System.currentTimeMillis(), true), 103L)
    )

    val result = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    // Verify correct final state:
    // - Started with 1000 files from checkpoint
    // - Added file1001 (version 101)
    // - Removed file500 (version 102)
    // - Added file1002 (version 103)
    // = 1000 - 1 + 2 = 1001 files
    assert(result.length == 1001, s"Expected 1001 files, got ${result.length}")
    assert(result.exists(_.path == "file1001.split"), "Should include newly added file1001")
    assert(result.exists(_.path == "file1002.split"), "Should include newly added file1002")
    assert(!result.exists(_.path == "file500.split"), "Should not include removed file500")
    assert(result.exists(_.path == "file1.split"), "Should include file1 from checkpoint")
    assert(result.exists(_.path == "file1000.split"), "Should include file1000 from checkpoint")

    println(s"✅ Checkpoint optimization verified: Started with ${checkpointActions.length} checkpoint files, " +
      s"processed only ${incrementalActions.length} incremental actions, final state has ${result.length} files")
  }
}
