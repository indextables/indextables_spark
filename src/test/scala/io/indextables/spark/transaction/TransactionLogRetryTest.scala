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

import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.compression.{CompressionUtils, GzipCompressionCodec}

/**
 * Tests for transaction log concurrent write retry logic.
 *
 * These tests validate that:
 * 1. Conflicts are actually detected and retried
 * 2. Retry metrics correctly track conflict occurrences
 * 3. The retry mechanism works on real cloud storage (S3/Azure tests in separate files)
 */
class TransactionLogRetryTest extends TestBase {

  private def createAddAction(index: Int): AddAction =
    AddAction(
      path = s"file$index.split",
      partitionValues = Map.empty,
      size = 1000L + index,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L + index)
    )

  test("TxRetryConfig should have correct default values") {
    val config = TxRetryConfig()
    config.maxAttempts shouldBe 10
    config.baseDelayMs shouldBe 100
    config.maxDelayMs shouldBe 5000
  }

  test("TxRetryConfig should accept custom values") {
    val config = TxRetryConfig(maxAttempts = 20, baseDelayMs = 50, maxDelayMs = 10000)
    config.maxAttempts shouldBe 20
    config.baseDelayMs shouldBe 50
    config.maxDelayMs shouldBe 10000
  }

  test("TransactionConflictException should have correct properties") {
    val exception = new TransactionConflictException(
      "Test message",
      version = 5L,
      attemptsMade = 3
    )

    exception.getMessage shouldBe "Test message"
    exception.version shouldBe 5L
    exception.attemptsMade shouldBe 3

    // Test with cause
    val cause = new RuntimeException("Root cause")
    val exceptionWithCause = new TransactionConflictException(
      "Test with cause",
      version = 10L,
      attemptsMade = 5,
      cause = cause
    )

    exceptionWithCause.getCause shouldBe cause
    exceptionWithCause.version shouldBe 10L
    exceptionWithCause.attemptsMade shouldBe 5
  }

  test("TxRetryMetrics should track conflict information correctly") {
    val metrics = TxRetryMetrics(
      attemptsMade = 3,
      conflictsEncountered = 2,
      finalVersion = 5L,
      conflictedVersions = Seq(3L, 4L)
    )

    metrics.attemptsMade shouldBe 3
    metrics.conflictsEncountered shouldBe 2
    metrics.finalVersion shouldBe 5L
    metrics.conflictedVersions shouldBe Seq(3L, 4L)
  }

  test("metrics should be recorded for successful writes without conflicts") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val addAction = createAddAction(1)
        val version = transactionLog.addFiles(Seq(addAction))
        version shouldBe 1L

        // Metrics should be present even for non-conflict writes
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
        metrics.get.attemptsMade shouldBe 1
        metrics.get.conflictsEncountered shouldBe 0
        metrics.get.conflictedVersions shouldBe empty
        metrics.get.finalVersion shouldBe 1L
      } finally {
        transactionLog.close()
      }
    }
  }

  test("writeFileIfNotExists should return false when file already exists") {
    withTempPath { tempPath =>
      // This test verifies the fundamental building block of conflict detection:
      // the conditional write (writeFileIfNotExists) returns false when file exists
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val testFile = new Path(tempPath, "test-conflict.json").toString
        val content1 = "first write".getBytes("UTF-8")
        val content2 = "second write".getBytes("UTF-8")

        // First write should succeed
        val firstWriteResult = cloudProvider.writeFileIfNotExists(testFile, content1)
        firstWriteResult shouldBe true
        println(s"First writeFileIfNotExists: $firstWriteResult (expected true)")

        // Second write should fail (file already exists)
        val secondWriteResult = cloudProvider.writeFileIfNotExists(testFile, content2)
        secondWriteResult shouldBe false
        println(s"Second writeFileIfNotExists: $secondWriteResult (expected false)")

        // File should contain first write's content
        val actualContent = new String(cloudProvider.readFile(testFile), "UTF-8")
        actualContent shouldBe "first write"

        println("SUCCESS: writeFileIfNotExists correctly returns false when file exists")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("FORCED CONFLICT: df.write then out-of-band upload then df.write verifies version skipping") {
    // This test verifies the out-of-band version file is respected:
    // 1. Write a transaction using regular df.write semantics (creates version 1)
    // 2. Write a log file out of band and manually upload (creates version 2)
    // 3. Write a transaction using regular df.write semantics
    //
    // Step 3 will create a new TransactionLog that reads disk, sees versions 0,1,2,
    // and writes to version 3. This verifies that:
    // - Our dummy version 2 file is preserved (not overwritten)
    // - The second df.write succeeded at version 3
    // - The transaction log correctly includes the out-of-band AddAction
    withTempPath { tempPath =>
      val tablePath = tempPath

      // Step 1: Write initial table using df.write
      val df1 = spark.createDataFrame(Seq(
        (1L, "first"),
        (2L, "second")
      )).toDF("id", "value")

      df1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      println(s"Step 1: Wrote initial table to $tablePath")

      // Step 2: Pre-create version 2 file out-of-band (simulating concurrent writer)
      val transactionLogDir = new Path(tablePath, "_transaction_log")
      val version2File = new Path(transactionLogDir, f"${2}%020d.json")

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tablePath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val dummyContent = """{"add":{"path":"DUMMY_CONFLICT_MARKER.split","partitionValues":{},"size":999,"modificationTime":1700000000000,"dataChange":true}}"""
        val compressedContent = CompressionUtils.writeTransactionFile(
          dummyContent.getBytes("UTF-8"),
          Some(new GzipCompressionCodec())
        )
        cloudProvider.writeFile(version2File.toString, compressedContent)

        cloudProvider.exists(version2File.toString) shouldBe true
        println(s"Step 2: Pre-created version 2 file out-of-band: $version2File")

        // Step 3: Write again using df.write (append mode)
        // This creates a NEW TransactionLog that reads disk and sees version 2
        val df2 = spark.createDataFrame(Seq(
          (3L, "third"),
          (4L, "fourth")
        )).toDF("id", "value")

        df2.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)

        println(s"Step 3: Appended to table")

        // Verify the outcome:
        // 1. Our dummy version 2 file should still exist (preserved, not overwritten)
        cloudProvider.exists(version2File.toString) shouldBe true
        val version2Content = CompressionUtils.readTransactionFile(cloudProvider.readFile(version2File.toString))
        new String(version2Content, "UTF-8") should include("DUMMY_CONFLICT_MARKER")
        println("Verified: Version 2 file preserved with our dummy content")

        // 2. Verify version 3 exists (the actual write went to version 3, skipping version 2)
        val version3File = new Path(transactionLogDir, f"${3}%020d.json")
        cloudProvider.exists(version3File.toString) shouldBe true
        println("Verified: Version 3 file exists (df.write correctly skipped version 2)")

        // 3. Check transaction log structure includes all files
        val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
        try {
          transactionLog.invalidateCache()
          val files = transactionLog.listFiles()

          // The dummy conflict marker file should be in the file list
          files.exists(_.path == "DUMMY_CONFLICT_MARKER.split") shouldBe true
          println("Verified: Dummy conflict marker file is in transaction log")

          println("SUCCESS: Out-of-band version file was preserved, df.write wrote to version 3")
        } finally {
          transactionLog.close()
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("FORCED CONFLICT: same TransactionLog instance with out-of-band file should trigger retry") {
    // This test GUARANTEES a conflict by:
    // 1. Creating a TransactionLog with protocol checking DISABLED (prevents getProtocol from reading disk)
    // 2. Doing a write to initialize versionCounter
    // 3. Pre-creating the NEXT version file out-of-band
    // 4. Calling addFiles() on the SAME TransactionLog instance
    //
    // With protocol checking disabled, addFiles() won't call getProtocol() -> getLatestVersion()
    // which would update the version counter. This ensures the conflict is detected.
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Create TransactionLog with protocol checking DISABLED
      val options = new CaseInsensitiveStringMap(
        java.util.Map.of(
          "spark.indextables.protocol.checkEnabled", "false"
        )
      )
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        transactionLog.initialize(getTestSchema())

        // Step 1: Do a real write to initialize versionCounter
        val addAction1 = createAddAction(100)
        val version1 = transactionLog.addFiles(Seq(addAction1))
        println(s"Step 1: First write completed at version $version1")
        version1 shouldBe 1L

        // After this write, versionCounter is at version1 (e.g., 1)
        // Next write will try version1 + 1 (e.g., 2)

        // Step 2: Pre-create the NEXT version file out-of-band
        val transactionLogDir = new Path(tempPath, "_transaction_log")
        val conflictVersion = version1 + 1
        val conflictFile = new Path(transactionLogDir, f"$conflictVersion%020d.json")

        val cloudProvider = CloudStorageProviderFactory.createProvider(
          tempPath,
          new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val dummyContent = s"""{"add":{"path":"OUT_OF_BAND_FILE.split","partitionValues":{},"size":888,"modificationTime":1700000000000,"dataChange":true}}"""
          val compressedContent = CompressionUtils.writeTransactionFile(
            dummyContent.getBytes("UTF-8"),
            Some(new GzipCompressionCodec())
          )
          cloudProvider.writeFile(conflictFile.toString, compressedContent)

          cloudProvider.exists(conflictFile.toString) shouldBe true
          println(s"Step 2: Pre-created conflict file at version $conflictVersion: $conflictFile")

          // Step 3: Try to write again using the SAME TransactionLog instance
          // Since protocol checking is disabled, getProtocol() won't be called
          // The versionCounter will increment from version1 to conflictVersion
          // writeFileIfNotExists will return FALSE (file exists)
          // Retry logic MUST kick in
          val addAction2 = createAddAction(200)
          val version2 = transactionLog.addFiles(Seq(addAction2))

          println(s"Step 3: Second write completed at version $version2")

          // Get metrics - there MUST be a conflict
          val metrics = transactionLog.getLastRetryMetrics()
          metrics shouldBe defined

          println(s"Metrics: attempts=${metrics.get.attemptsMade}, conflicts=${metrics.get.conflictsEncountered}, finalVersion=${metrics.get.finalVersion}, conflictedVersions=${metrics.get.conflictedVersions}")

          // ASSERT: Conflict MUST have been detected and handled
          withClue(s"Expected conflict at version $conflictVersion but got metrics: $metrics") {
            metrics.get.conflictsEncountered should be >= 1
            metrics.get.attemptsMade should be >= 2
            metrics.get.conflictedVersions should contain(conflictVersion)
          }

          // The final version should be conflict version + 1
          version2 shouldBe (conflictVersion + 1)
          println(s"SUCCESS: Conflict at version $conflictVersion detected, retried to version $version2")

          // Verify both files are in the transaction log
          transactionLog.invalidateCache()
          val files = transactionLog.listFiles()
          files.exists(_.path == "file100.split") shouldBe true
          files.exists(_.path == "file200.split") shouldBe true
          files.exists(_.path == "OUT_OF_BAND_FILE.split") shouldBe true
          println("Verified: All files present in transaction log")
        } finally {
          cloudProvider.close()
        }
      } finally {
        transactionLog.close()
      }
    }
  }

  test("transaction log retry: concurrent writes should all eventually succeed") {
    // This test verifies that when conflicts occur (via concurrent writes),
    // all writes eventually succeed through the retry mechanism.
    // NOTE: On local filesystem, conflicts may not occur due to OS-level serialization.
    // Real conflict testing happens on S3/Azure with conditional put semantics.
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val numConcurrentWrites = 5  // 5 concurrent writes for local tests
        val successCount = new AtomicInteger(0)
        val failureCount = new AtomicInteger(0)
        val totalConflicts = new AtomicInteger(0)
        val versions = new ListBuffer[Long]()
        val allMetrics = new ListBuffer[TxRetryMetrics]()
        val latch = new CountDownLatch(numConcurrentWrites)

        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
          Executors.newFixedThreadPool(numConcurrentWrites)
        )

        // Start all writes at roughly the same time
        val startLatch = new CountDownLatch(1)
        (1 to numConcurrentWrites).foreach { i =>
          Future {
            startLatch.await()
            try {
              val addAction = createAddAction(i)
              val version = transactionLog.addFiles(Seq(addAction))
              synchronized {
                versions += version
                transactionLog.getLastRetryMetrics().foreach { m =>
                  allMetrics += m
                  totalConflicts.addAndGet(m.conflictsEncountered)
                }
              }
              successCount.incrementAndGet()
            } catch {
              case e: Exception =>
                failureCount.incrementAndGet()
                e.printStackTrace()
            } finally {
              latch.countDown()
            }
          }
        }

        startLatch.countDown()
        val completed = latch.await(120, TimeUnit.SECONDS)
        completed shouldBe true

        // All writes MUST succeed (this is the key assertion)
        successCount.get() shouldBe numConcurrentWrites
        failureCount.get() shouldBe 0

        // All versions must be unique
        versions.distinct.size shouldBe numConcurrentWrites

        // Log conflict statistics
        println(s"Concurrent writes test: ${successCount.get()} succeeded, ${failureCount.get()} failed")
        println(s"Total conflicts encountered: ${totalConflicts.get()}")
        if (totalConflicts.get() > 0) {
          println(s"CONFLICTS DETECTED AND HANDLED via retry!")
          allMetrics.filter(_.conflictsEncountered > 0).foreach { m =>
            println(s"  Version ${m.finalVersion}: ${m.attemptsMade} attempts, conflicted at: ${m.conflictedVersions.mkString(",")}")
          }
        } else {
          println("Note: No conflicts on local filesystem (expected - real tests on S3/Azure)")
        }

        // Verify all files present
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.size shouldBe numConcurrentWrites
      } finally {
        transactionLog.close()
      }
    }
  }

  test("concurrent appends should all succeed with retry - metrics tracked") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val numConcurrentWrites = 5
        val successCount = new AtomicInteger(0)
        val failureCount = new AtomicInteger(0)
        val totalConflicts = new AtomicInteger(0)
        val errors = new ListBuffer[Throwable]()
        val versions = new ListBuffer[Long]()
        val allMetrics = new ListBuffer[TxRetryMetrics]()
        val latch = new CountDownLatch(numConcurrentWrites)

        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
          Executors.newFixedThreadPool(numConcurrentWrites)
        )

        // Start all writes at roughly the same time
        val startLatch = new CountDownLatch(1)
        val futures = (1 to numConcurrentWrites).map { i =>
          Future {
            startLatch.await() // Wait for all threads to be ready
            try {
              val addAction = createAddAction(i)
              val version = transactionLog.addFiles(Seq(addAction))
              synchronized {
                versions += version
                // Collect metrics from each write
                transactionLog.getLastRetryMetrics().foreach { m =>
                  allMetrics += m
                  totalConflicts.addAndGet(m.conflictsEncountered)
                }
              }
              successCount.incrementAndGet()
            } catch {
              case e: Exception =>
                failureCount.incrementAndGet()
                synchronized {
                  errors += e
                }
            } finally {
              latch.countDown()
            }
          }
        }

        // Release all threads at once
        startLatch.countDown()

        // Wait for all writes to complete (with longer timeout)
        val completed = latch.await(120, TimeUnit.SECONDS)
        completed shouldBe true

        // Report any errors for debugging
        if (errors.nonEmpty) {
          errors.foreach(e => println(s"Error during concurrent write: ${e.getMessage}"))
        }

        // All writes should have succeeded
        if (successCount.get() < numConcurrentWrites) {
          println(s"Success: ${successCount.get()}, Failures: ${failureCount.get()}")
          errors.foreach(_.printStackTrace())
        }
        successCount.get() shouldBe numConcurrentWrites
        failureCount.get() shouldBe 0

        // All versions should be unique
        versions.distinct.size shouldBe numConcurrentWrites

        // Log conflict statistics for debugging
        println(s"Total conflicts encountered across all threads: ${totalConflicts.get()}")
        println(s"Metrics collected: ${allMetrics.size}")
        allMetrics.foreach { m =>
          if (m.conflictsEncountered > 0) {
            println(s"  Write to version ${m.finalVersion}: ${m.conflictsEncountered} conflicts, ${m.attemptsMade} attempts, conflicted at: ${m.conflictedVersions.mkString(",")}")
          }
        }

        // The metrics should be present for all writes
        allMetrics.size shouldBe numConcurrentWrites

        // Verify all files are in the transaction log
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.size shouldBe numConcurrentWrites

        // Verify each file was written
        (1 to numConcurrentWrites).foreach { i =>
          files.exists(_.path == s"file$i.split") shouldBe true
        }
      } finally {
        transactionLog.close()
      }
    }
  }

  test("two transaction log instances writing sequentially should both succeed") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val transactionLog = TransactionLogFactory.create(tablePath, spark)
      try {
        transactionLog.initialize(getTestSchema())

        // First write
        val version1 = transactionLog.addFiles(Seq(createAddAction(100)))
        version1 shouldBe 1L

        val metrics1 = transactionLog.getLastRetryMetrics()
        metrics1 shouldBe defined
        println(s"Write 1: attempts=${metrics1.get.attemptsMade}, conflicts=${metrics1.get.conflictsEncountered}, finalVersion=${metrics1.get.finalVersion}")

        // Second write
        val version2 = transactionLog.addFiles(Seq(createAddAction(200)))
        version2 shouldBe 2L

        val metrics2 = transactionLog.getLastRetryMetrics()
        metrics2 shouldBe defined
        println(s"Write 2: attempts=${metrics2.get.attemptsMade}, conflicts=${metrics2.get.conflictsEncountered}, finalVersion=${metrics2.get.finalVersion}")

        // Verify both files were written
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.exists(_.path == "file100.split") shouldBe true
        files.exists(_.path == "file200.split") shouldBe true
      } finally {
        transactionLog.close()
      }
    }
  }

  test("commitMergeSplits should record metrics") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add initial files
        val addActions = (1 to 3).map(createAddAction)
        transactionLog.addFiles(addActions)

        // Create remove actions for the files we want to merge
        val removeActions = addActions.map { add =>
          RemoveAction(
            path = add.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = Some(add.partitionValues),
            size = Some(add.size)
          )
        }

        // Create the merged file action
        val mergedAction = AddAction(
          path = "merged.split",
          partitionValues = Map.empty,
          size = 3000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(300L)
        )

        // Commit merge (version 0 = init, version 1 = addFiles, version 2 = commitMergeSplits)
        val version = transactionLog.commitMergeSplits(removeActions, Seq(mergedAction))
        println(s"commitMergeSplits returned version: $version")
        version should be >= 1L  // Should be at least version 1

        // Verify metrics were recorded
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
        metrics.get.attemptsMade should be >= 1
        println(s"commitMergeSplits metrics: attempts=${metrics.get.attemptsMade}, conflicts=${metrics.get.conflictsEncountered}, finalVersion=${metrics.get.finalVersion}")

        // Verify the merge was applied
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()

        // The original files should be removed
        addActions.foreach { add =>
          files.exists(_.path == add.path) shouldBe false
        }

        // The merged file should be present
        files.exists(_.path == "merged.split") shouldBe true
      } finally {
        transactionLog.close()
      }
    }
  }

  test("commitRemoveActions should record metrics") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Add initial files
        val addActions = (1 to 3).map(createAddAction)
        transactionLog.addFiles(addActions)

        // Create remove actions
        val removeActions = addActions.map { add =>
          RemoveAction(
            path = add.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = Some(add.partitionValues),
            size = Some(add.size)
          )
        }

        // Commit removes (version 0 = init, version 1 = addFiles, version 2 = commitRemoveActions)
        val version = transactionLog.commitRemoveActions(removeActions)
        println(s"commitRemoveActions returned version: $version")
        version should be >= 1L  // Should be at least version 1

        // Verify metrics were recorded
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
        metrics.get.attemptsMade should be >= 1
        println(s"commitRemoveActions metrics: attempts=${metrics.get.attemptsMade}, conflicts=${metrics.get.conflictsEncountered}, finalVersion=${metrics.get.finalVersion}")

        // Verify the removes were applied
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()

        // The original files should be removed
        addActions.foreach { add =>
          files.exists(_.path == add.path) shouldBe false
        }
      } finally {
        transactionLog.close()
      }
    }
  }

  test("retry configuration should be read from options") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Create transaction log with custom retry config
      val options = new CaseInsensitiveStringMap(
        java.util.Map.of(
          TransactionLog.RETRY_MAX_ATTEMPTS, "5",
          TransactionLog.RETRY_BASE_DELAY_MS, "200",
          TransactionLog.RETRY_MAX_DELAY_MS, "3000"
        )
      )
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        transactionLog.initialize(getTestSchema())

        // Verify the configuration keys are correctly defined
        TransactionLog.RETRY_MAX_ATTEMPTS shouldBe "spark.indextables.transaction.retry.maxAttempts"
        TransactionLog.RETRY_BASE_DELAY_MS shouldBe "spark.indextables.transaction.retry.baseDelayMs"
        TransactionLog.RETRY_MAX_DELAY_MS shouldBe "spark.indextables.transaction.retry.maxDelayMs"

        // Verify normal operation works with custom config
        val addAction = createAddAction(1)
        val version = transactionLog.addFiles(Seq(addAction))
        version shouldBe 1L

        // Metrics should be present
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
      } finally {
        transactionLog.close()
      }
    }
  }
}
