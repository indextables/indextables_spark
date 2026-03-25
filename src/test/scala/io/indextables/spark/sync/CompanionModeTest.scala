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

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, RemoveAction, TransactionLogFactory}
import io.indextables.spark.TestBase

/**
 * Tests for Parquet Companion Mode functionality.
 *
 * Validates:
 *   - AddAction companion fields (companionSourceFiles, companionDeltaVersion, companionFastFieldMode)
 *   - Transaction log round-trip with companion fields
 *   - Mixed companion and regular entry storage
 *   - Transaction log storage and retrieval of companion fields
 *   - Write guard: rejection of normal writes to companion-mode tables
 *   - SyncConfig and SyncIndexingGroup serialization
 */
class CompanionModeTest extends TestBase {

  // --- AddAction Companion Field Tests ---

  test("AddAction should support companion fields") {
    val action = AddAction(
      path = "companion-0-abc123.split",
      partitionValues = Map("date" -> "2024-01-01"),
      size = 50000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(1000L),
      companionSourceFiles = Some(Seq("part-00001.parquet", "part-00002.parquet")),
      companionDeltaVersion = Some(42L),
      companionFastFieldMode = Some("HYBRID")
    )

    action.companionSourceFiles shouldBe Some(Seq("part-00001.parquet", "part-00002.parquet"))
    action.companionDeltaVersion shouldBe Some(42L)
    action.companionFastFieldMode shouldBe Some("HYBRID")
  }

  test("AddAction companion fields should default to None") {
    val action = AddAction(
      path = "regular-split.split",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true
    )

    action.companionSourceFiles shouldBe None
    action.companionDeltaVersion shouldBe None
    action.companionFastFieldMode shouldBe None
  }

  test("AddAction companion fields should support all fast field modes") {
    Seq("HYBRID", "DISABLED", "PARQUET_ONLY").foreach { mode =>
      val action = AddAction(
        path = s"split-$mode.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        companionFastFieldMode = Some(mode)
      )
      action.companionFastFieldMode shouldBe Some(mode)
    }
  }

  // --- Transaction Log Round-Trip Tests for Companion Fields ---

  test("transaction log round-trip should preserve companion fields") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val original = AddAction(
          path = "companion-split.split",
          partitionValues = Map("year" -> "2024"),
          size = 100000L,
          modificationTime = 1700000000000L,
          dataChange = true,
          numRecords = Some(500L),
          companionSourceFiles = Some(Seq("year=2024/part-00001.parquet", "year=2024/part-00002.parquet")),
          companionDeltaVersion = Some(10L),
          companionFastFieldMode = Some("HYBRID")
        )

        transactionLog.addFile(original)

        val files = transactionLog.listFiles()
        files should have length 1

        val restored = files.head
        restored.companionSourceFiles shouldBe Some(Seq("year=2024/part-00001.parquet", "year=2024/part-00002.parquet"))
        restored.companionDeltaVersion shouldBe Some(10L)
        restored.companionFastFieldMode shouldBe Some("HYBRID")
        restored.path shouldBe original.path
        restored.partitionValues shouldBe original.partitionValues
        restored.size shouldBe original.size
        restored.numRecords shouldBe original.numRecords
      } finally
        transactionLog.close()
    }
  }

  test("transaction log round-trip with None companion fields") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val original = AddAction(
          path = "regular-split.split",
          partitionValues = Map.empty,
          size = 10000L,
          modificationTime = 1700000000000L,
          dataChange = true,
          numRecords = Some(100L)
        )

        transactionLog.addFile(original)

        val files = transactionLog.listFiles()
        files should have length 1

        val restored = files.head
        restored.companionSourceFiles shouldBe None
        restored.companionDeltaVersion shouldBe None
        restored.companionFastFieldMode shouldBe None
      } finally
        transactionLog.close()
    }
  }

  test("transaction log should store mixed companion and regular entries") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        transactionLog.addFile(AddAction(
          path = "companion-1.split",
          partitionValues = Map("date" -> "2024-01-01"),
          size = 50000L,
          modificationTime = 1700000000000L,
          dataChange = true,
          numRecords = Some(1000L),
          companionSourceFiles =
            Some(Seq("date=2024-01-01/part-00001.parquet", "date=2024-01-01/part-00002.parquet")),
          companionDeltaVersion = Some(42L),
          companionFastFieldMode = Some("HYBRID")
        ))

        transactionLog.addFile(AddAction(
          path = "companion-2.split",
          partitionValues = Map("date" -> "2024-01-02"),
          size = 60000L,
          modificationTime = 1700000100000L,
          dataChange = true,
          numRecords = Some(2000L),
          companionSourceFiles = Some(Seq("date=2024-01-02/part-00003.parquet")),
          companionDeltaVersion = Some(42L),
          companionFastFieldMode = Some("PARQUET_ONLY")
        ))

        transactionLog.addFile(AddAction(
          path = "regular.split",
          partitionValues = Map.empty,
          size = 30000L,
          modificationTime = 1700000200000L,
          dataChange = true,
          numRecords = Some(500L)
        ))

        val files = transactionLog.listFiles()
        files should have size 3

        // Verify companion entry 1
        val entry1 = files.find(_.path == "companion-1.split").get
        entry1.companionSourceFiles shouldBe Some(
          Seq("date=2024-01-01/part-00001.parquet", "date=2024-01-01/part-00002.parquet")
        )
        entry1.companionDeltaVersion shouldBe Some(42L)
        entry1.companionFastFieldMode shouldBe Some("HYBRID")

        // Verify companion entry 2
        val entry2 = files.find(_.path == "companion-2.split").get
        entry2.companionSourceFiles shouldBe Some(Seq("date=2024-01-02/part-00003.parquet"))
        entry2.companionDeltaVersion shouldBe Some(42L)
        entry2.companionFastFieldMode shouldBe Some("PARQUET_ONLY")

        // Verify regular entry has no companion fields
        val entry3 = files.find(_.path == "regular.split").get
        entry3.companionSourceFiles shouldBe None
        entry3.companionDeltaVersion shouldBe None
        entry3.companionFastFieldMode shouldBe None
      } finally
        transactionLog.close()
    }
  }

  // --- Transaction Log Integration Tests ---

  test("transaction log should store and retrieve AddActions with companion fields") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val addAction = AddAction(
          path = "companion-split-001.split",
          partitionValues = Map.empty,
          size = 75000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(300L),
          companionSourceFiles = Some(Seq("part-00001.parquet", "part-00002.parquet")),
          companionDeltaVersion = Some(5L),
          companionFastFieldMode = Some("HYBRID")
        )

        val version = transactionLog.addFile(addAction)
        version should be >= 1L

        // Read back and verify
        val files = transactionLog.listFiles()
        files should have length 1

        val retrieved = files.head
        retrieved.path shouldBe "companion-split-001.split"
        retrieved.companionSourceFiles shouldBe Some(Seq("part-00001.parquet", "part-00002.parquet"))
        retrieved.companionDeltaVersion shouldBe Some(5L)
        retrieved.companionFastFieldMode shouldBe Some("HYBRID")
      } finally
        transactionLog.close()
    }
  }

  test("transaction log should handle mixed companion and regular AddActions") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add a companion split
        val companionAction = AddAction(
          path = "companion.split",
          partitionValues = Map.empty,
          size = 50000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(200L),
          companionSourceFiles = Some(Seq("part-00001.parquet")),
          companionDeltaVersion = Some(1L),
          companionFastFieldMode = Some("HYBRID")
        )
        transactionLog.addFile(companionAction)

        // Add a regular split (no companion fields)
        val regularAction = AddAction(
          path = "regular.split",
          partitionValues = Map.empty,
          size = 30000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        )
        transactionLog.addFile(regularAction)

        val files = transactionLog.listFiles()
        files should have length 2

        val companion = files.find(_.path == "companion.split").get
        companion.companionSourceFiles shouldBe defined
        companion.companionDeltaVersion shouldBe Some(1L)

        val regular = files.find(_.path == "regular.split").get
        regular.companionSourceFiles shouldBe None
        regular.companionDeltaVersion shouldBe None
      } finally
        transactionLog.close()
    }
  }

  test("commitMergeSplits should preserve companion fields in new AddActions") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Simulate a BUILD COMPANION commit: add companion splits via commitMergeSplits
        val removes = Seq.empty[RemoveAction]
        val adds = Seq(
          AddAction(
            path = "companion-group-0.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = 40000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(150L),
            companionSourceFiles = Some(Seq("date=2024-01-01/part-00001.parquet")),
            companionDeltaVersion = Some(3L),
            companionFastFieldMode = Some("HYBRID")
          ),
          AddAction(
            path = "companion-group-1.split",
            partitionValues = Map("date" -> "2024-01-02"),
            size = 60000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(250L),
            companionSourceFiles =
              Some(Seq("date=2024-01-02/part-00002.parquet", "date=2024-01-02/part-00003.parquet")),
            companionDeltaVersion = Some(3L),
            companionFastFieldMode = Some("HYBRID")
          )
        )

        transactionLog.commitMergeSplits(removes, adds)

        val files = transactionLog.listFiles()
        files should have length 2

        val group0 = files.find(_.path == "companion-group-0.split").get
        group0.companionSourceFiles shouldBe Some(Seq("date=2024-01-01/part-00001.parquet"))
        group0.companionDeltaVersion shouldBe Some(3L)

        val group1 = files.find(_.path == "companion-group-1.split").get
        group1.companionSourceFiles shouldBe Some(
          Seq("date=2024-01-02/part-00002.parquet", "date=2024-01-02/part-00003.parquet")
        )
        group1.companionDeltaVersion shouldBe Some(3L)
      } finally
        transactionLog.close()
    }
  }

  // --- Write Guard Tests ---

  test("write guard should reject normal writes to companion-mode table") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Create a companion split (simulating BUILD COMPANION)
        val companionAction = AddAction(
          path = "companion-split.split",
          partitionValues = Map.empty,
          size = 50000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(200L),
          companionSourceFiles = Some(Seq("part-00001.parquet")),
          companionDeltaVersion = Some(1L),
          companionFastFieldMode = Some("HYBRID")
        )
        transactionLog.addFile(companionAction)
      } finally
        transactionLog.close()

      // Now try to write normally - should be rejected
      val df = createTestDataFrame()
      val ex = intercept[Exception] {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tempPath)
      }
      ex.getMessage should include("companion")
    }
  }

  // --- SyncConfig and SyncIndexingGroup Serialization Tests ---

  test("SyncConfig should be serializable") {
    val config = SyncConfig(
      indexingModes = Map("title" -> "text", "status" -> "string"),
      fastFieldMode = "HYBRID",
      storageConfig = Map("spark.indextables.aws.accessKey" -> "key1", "spark.indextables.aws.secretKey" -> "secret1"),
      splitTablePath = "s3://bucket/index",
      writerHeapSize = 4L * 1024L * 1024L * 1024L, // 4GB
      readerBatchSize = 16384
    )

    // Verify basic serialization by writing/reading through Java serialization
    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(config)
    oos.close()

    val bais         = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois          = new java.io.ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[SyncConfig]

    deserialized.indexingModes shouldBe config.indexingModes
    deserialized.fastFieldMode shouldBe config.fastFieldMode
    deserialized.splitTablePath shouldBe config.splitTablePath
    deserialized.writerHeapSize shouldBe 4L * 1024L * 1024L * 1024L
    deserialized.readerBatchSize shouldBe 16384
  }

  test("SyncConfig should have 2GB default writerHeapSize and 8192 default readerBatchSize") {
    val config = SyncConfig(
      indexingModes = Map.empty,
      fastFieldMode = "HYBRID",
      storageConfig = Map.empty,
      splitTablePath = "s3://bucket/index"
    )
    config.writerHeapSize shouldBe 2L * 1024L * 1024L * 1024L
    config.readerBatchSize shouldBe 8192
  }

  test("SyncIndexingGroup should be serializable") {
    val group = SyncIndexingGroup(
      parquetFiles = Seq("s3://bucket/delta/part-00001.parquet", "s3://bucket/delta/part-00002.parquet"),
      parquetTableRoot = "s3://bucket/delta",
      partitionValues = Map("date" -> "2024-01-01"),
      groupIndex = 0
    )

    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(group)
    oos.close()

    val bais         = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois          = new java.io.ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[SyncIndexingGroup]

    deserialized.parquetFiles shouldBe group.parquetFiles
    deserialized.parquetTableRoot shouldBe group.parquetTableRoot
    deserialized.partitionValues shouldBe group.partitionValues
    deserialized.groupIndex shouldBe 0
  }

  test("SyncTaskResult should be serializable") {
    val result = SyncTaskResult(
      addAction = AddAction(
        path = "companion.split",
        partitionValues = Map.empty,
        size = 50000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        numRecords = Some(100L),
        companionSourceFiles = Some(Seq("a.parquet")),
        companionDeltaVersion = Some(1L),
        companionFastFieldMode = Some("HYBRID")
      ),
      bytesDownloaded = 100000L,
      bytesUploaded = 50000L,
      parquetFilesIndexed = 3
    )

    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(result)
    oos.close()

    val bais         = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois          = new java.io.ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[SyncTaskResult]

    deserialized.bytesDownloaded shouldBe 100000L
    deserialized.bytesUploaded shouldBe 50000L
    deserialized.parquetFilesIndexed shouldBe 3
    deserialized.addAction.companionSourceFiles shouldBe Some(Seq("a.parquet"))
  }

  // --- Incremental Sync Logic Tests ---

  test("companion splits should track max delta version for incremental sync") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Simulate initial sync at delta version 5
        transactionLog.commitMergeSplits(
          Seq.empty,
          Seq(
            AddAction(
              path = "split-v5-a.split",
              partitionValues = Map.empty,
              size = 10000L,
              modificationTime = System.currentTimeMillis(),
              dataChange = true,
              numRecords = Some(50L),
              companionSourceFiles = Some(Seq("file1.parquet")),
              companionDeltaVersion = Some(5L),
              companionFastFieldMode = Some("HYBRID")
            ),
            AddAction(
              path = "split-v5-b.split",
              partitionValues = Map.empty,
              size = 20000L,
              modificationTime = System.currentTimeMillis(),
              dataChange = true,
              numRecords = Some(100L),
              companionSourceFiles = Some(Seq("file2.parquet")),
              companionDeltaVersion = Some(5L),
              companionFastFieldMode = Some("HYBRID")
            )
          )
        )

        // Simulate incremental sync at delta version 8
        transactionLog.commitMergeSplits(
          Seq.empty,
          Seq(
            AddAction(
              path = "split-v8.split",
              partitionValues = Map.empty,
              size = 15000L,
              modificationTime = System.currentTimeMillis(),
              dataChange = true,
              numRecords = Some(75L),
              companionSourceFiles = Some(Seq("file3.parquet")),
              companionDeltaVersion = Some(8L),
              companionFastFieldMode = Some("HYBRID")
            )
          )
        )

        val files = transactionLog.listFiles()
        files should have length 3

        // Derive lastSyncedVersion as max of all companionDeltaVersion
        val versions = files.flatMap(_.companionDeltaVersion)
        versions should have length 3
        versions.max shouldBe 8L
      } finally
        transactionLog.close()
    }
  }

  test("invalidation should identify companion splits containing removed parquet files") {
    // This tests the logic pattern used in SyncToExternalCommand.computeIncrementalChanges()
    val existingSplits = Seq(
      AddAction(
        path = "split-a.split",
        partitionValues = Map("date" -> "2024-01-01"),
        size = 10000L,
        modificationTime = 1700000000000L,
        dataChange = true,
        companionSourceFiles = Some(Seq("date=2024-01-01/part-00001.parquet", "date=2024-01-01/part-00002.parquet")),
        companionDeltaVersion = Some(5L),
        companionFastFieldMode = Some("HYBRID")
      ),
      AddAction(
        path = "split-b.split",
        partitionValues = Map("date" -> "2024-01-02"),
        size = 20000L,
        modificationTime = 1700000000000L,
        dataChange = true,
        companionSourceFiles = Some(Seq("date=2024-01-02/part-00003.parquet")),
        companionDeltaVersion = Some(5L),
        companionFastFieldMode = Some("HYBRID")
      )
    )

    // Simulate removal of part-00001.parquet
    val removedFiles = Set("date=2024-01-01/part-00001.parquet")

    // Find affected splits
    val affectedSplits = existingSplits.filter { split =>
      split.companionSourceFiles.exists(_.exists(removedFiles.contains))
    }

    affectedSplits should have length 1
    affectedSplits.head.path shouldBe "split-a.split"

    // Compute remaining valid files for affected splits
    val remainingFiles = affectedSplits.head.companionSourceFiles.get.filterNot(removedFiles.contains)
    remainingFiles shouldBe Seq("date=2024-01-01/part-00002.parquet")
  }

  // --- ConfigUtils Companion Config Test ---

  test("ConfigUtils should read companion parquetTableRoot config") {
    import io.indextables.spark.util.ConfigUtils

    val config = Map(
      "spark.indextables.companion.parquetTableRoot" -> "s3://bucket/my_delta_table"
    )

    val cacheConfig = ConfigUtils.createSplitCacheConfig(config)
    cacheConfig.companionSourceTableRoot shouldBe Some("s3://bucket/my_delta_table")
  }

  test("ConfigUtils should return None for missing companion config") {
    import io.indextables.spark.util.ConfigUtils

    val config = Map(
      "spark.indextables.aws.accessKey" -> "test-key"
    )

    val cacheConfig = ConfigUtils.createSplitCacheConfig(config)
    cacheConfig.companionSourceTableRoot shouldBe None
  }
}
