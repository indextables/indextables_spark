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

import java.util.UUID
import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.RealS3TestBase
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.compression.{CompressionUtils, GzipCompressionCodec}

/**
 * Real S3 tests for transaction log concurrent write retry logic.
 *
 * These tests validate the retry mechanism on actual S3 infrastructure,
 * including S3's conditional put semantics (If-None-Match: *).
 *
 * IMPORTANT: These tests require AWS credentials and will create/delete
 * real objects in S3. Configure via:
 *   - ~/.aws/credentials file
 *   - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 *   - System properties: test.s3.bucket, test.s3.prefix
 *
 * Skip these tests if AWS credentials are not available.
 */
class RealS3TransactionLogRetryTest extends RealS3TestBase {

  // Test bucket and prefix - configure via system properties or use defaults
  // Default bucket: test-tantivy4sparkbucket in us-east-2 (same as other RealS3 tests)
  private val testBucket = Option(System.getProperty("test.s3.bucket")).getOrElse("test-tantivy4sparkbucket")
  private val testPrefix = Option(System.getProperty("test.s3.prefix")).getOrElse("retry-tests")

  private def getTestSchema(): StructType =
    StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("value", StringType, nullable = true)
    ))

  private def createAddAction(index: Int): AddAction =
    AddAction(
      path = s"file$index.split",
      partitionValues = Map.empty,
      size = 1000L + index,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L + index)
    )

  private def generateTestPath(): String = {
    val testId = UUID.randomUUID().toString.take(8)
    s"s3a://$testBucket/$testPrefix/test-$testId"
  }

  private def cleanupPath(path: String): Unit = {
    try {
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        path,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )
      try {
        // Delete all files in the path
        val files = cloudProvider.listFiles(path)
        files.foreach { file =>
          try { cloudProvider.deleteFile(file.path) } catch { case _: Exception => }
        }
      } finally {
        cloudProvider.close()
      }
    } catch {
      case e: Exception =>
        println(s"Warning: Failed to cleanup test path $path: ${e.getMessage}")
    }
  }

  test("S3: metrics should be recorded for successful writes without conflicts") {
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()
    val tablePath = new Path(testPath)

    try {
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val addAction = createAddAction(1)
        val version = transactionLog.addFiles(Seq(addAction))
        version shouldBe 1L

        // Metrics should be present
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
        metrics.get.attemptsMade shouldBe 1
        metrics.get.conflictsEncountered shouldBe 0
        metrics.get.finalVersion shouldBe 1L

        println(s"S3 write succeeded: attempts=${metrics.get.attemptsMade}, version=${metrics.get.finalVersion}")
      } finally {
        transactionLog.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }

  test("S3: concurrent appends from multiple threads should all succeed with retry") {
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()
    val tablePath = new Path(testPath)

    try {
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
                synchronized { errors += e }
            } finally {
              latch.countDown()
            }
          }
        }

        // Release all threads at once
        startLatch.countDown()

        // Wait for all writes to complete (longer timeout for S3)
        val completed = latch.await(180, TimeUnit.SECONDS)
        completed shouldBe true

        // Report results
        println(s"S3 concurrent writes: success=${successCount.get()}, failures=${failureCount.get()}")
        println(s"Total conflicts encountered: ${totalConflicts.get()}")
        allMetrics.foreach { m =>
          if (m.conflictsEncountered > 0) {
            println(s"  Version ${m.finalVersion}: ${m.conflictsEncountered} conflicts, ${m.attemptsMade} attempts")
          }
        }

        // All writes should have succeeded
        successCount.get() shouldBe numConcurrentWrites
        failureCount.get() shouldBe 0

        // All versions should be unique
        versions.distinct.size shouldBe numConcurrentWrites

        // On S3, we expect to see conflicts due to actual concurrent writes
        // The retry mechanism should handle them
        if (totalConflicts.get() > 0) {
          println(s"SUCCESS: S3 conflict detection and retry worked! ${totalConflicts.get()} conflicts were resolved.")
        } else {
          println("Note: No conflicts detected - writes may have been serialized by timing")
        }

        // Verify all files are present
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.size shouldBe numConcurrentWrites
      } finally {
        transactionLog.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }

  test("S3: commitMergeSplits should record metrics") {
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()
    val tablePath = new Path(testPath)

    try {
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

        // Create merged file action
        val mergedAction = AddAction(
          path = "merged.split",
          partitionValues = Map.empty,
          size = 3000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(300L)
        )

        // Commit merge
        val version = transactionLog.commitMergeSplits(removeActions, Seq(mergedAction))
        version should be >= 1L

        // Verify metrics
        val metrics = transactionLog.getLastRetryMetrics()
        metrics shouldBe defined
        println(s"S3 commitMergeSplits: version=$version, attempts=${metrics.get.attemptsMade}")

        // Verify merge applied
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.exists(_.path == "merged.split") shouldBe true
        addActions.foreach { add =>
          files.exists(_.path == add.path) shouldBe false
        }
      } finally {
        transactionLog.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }

  test("S3: writeFileIfNotExists should return false when object already exists") {
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()

    try {
      // This test verifies S3's conditional put (If-None-Match: *) works correctly
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        testPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val testFile = s"$testPath/test-conflict.json"
        val content1 = "first write".getBytes("UTF-8")
        val content2 = "second write".getBytes("UTF-8")

        // First write should succeed
        val firstWriteResult = cloudProvider.writeFileIfNotExists(testFile, content1)
        firstWriteResult shouldBe true
        println(s"S3: First writeFileIfNotExists: $firstWriteResult (expected true)")

        // Second write should fail (object already exists)
        val secondWriteResult = cloudProvider.writeFileIfNotExists(testFile, content2)
        secondWriteResult shouldBe false
        println(s"S3: Second writeFileIfNotExists: $secondWriteResult (expected false)")

        // Object should contain first write's content
        val actualContent = new String(cloudProvider.readFile(testFile), "UTF-8")
        actualContent shouldBe "first write"

        println("S3 SUCCESS: writeFileIfNotExists correctly returns false when object exists")
      } finally {
        cloudProvider.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }

  test("S3: concurrent writes should all eventually succeed via retry") {
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()
    val tablePath = new Path(testPath)

    try {
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        // Use more concurrent writes to increase conflict likelihood on S3
        val numConcurrentWrites = 10
        val successCount = new AtomicInteger(0)
        val failureCount = new AtomicInteger(0)
        val totalConflicts = new AtomicInteger(0)
        val versions = new ListBuffer[Long]()
        val allMetrics = new ListBuffer[TxRetryMetrics]()
        val latch = new CountDownLatch(numConcurrentWrites)

        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
          Executors.newFixedThreadPool(numConcurrentWrites)
        )

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
                synchronized { println(s"S3 Error: ${e.getMessage}") }
            } finally {
              latch.countDown()
            }
          }
        }

        startLatch.countDown()
        val completed = latch.await(300, TimeUnit.SECONDS)
        completed shouldBe true

        // All writes MUST succeed - this is the critical assertion
        println(s"S3 concurrent writes: success=${successCount.get()}, failures=${failureCount.get()}")
        successCount.get() shouldBe numConcurrentWrites
        failureCount.get() shouldBe 0

        // All versions must be unique
        versions.distinct.size shouldBe numConcurrentWrites

        // Log conflict statistics
        println(s"S3 Total conflicts: ${totalConflicts.get()}")
        if (totalConflicts.get() > 0) {
          println(s"S3 SUCCESS: Conflicts detected and handled via retry!")
          allMetrics.filter(_.conflictsEncountered > 0).foreach { m =>
            println(s"  S3 Version ${m.finalVersion}: ${m.attemptsMade} attempts, ${m.conflictsEncountered} conflicts")
          }
        }

        // Verify all files present
        transactionLog.invalidateCache()
        val files = transactionLog.listFiles()
        files.size shouldBe numConcurrentWrites
      } finally {
        transactionLog.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }

  test("S3 FORCED CONFLICT: out-of-band file upload should trigger retry") {
    // This test GUARANTEES a conflict on S3 by:
    // 1. Creating a TransactionLog with protocol checking DISABLED (prevents getProtocol from reading disk)
    // 2. Doing a write to initialize versionCounter
    // 3. Pre-creating the NEXT version file out-of-band directly on S3
    // 4. Calling addFiles() on the SAME TransactionLog instance
    //
    // With protocol checking disabled, addFiles() won't call getProtocol() -> getLatestVersion()
    // which would update the version counter. S3's conditional put will detect the conflict.
    assume(hasAwsCredentials(), "AWS credentials not available")

    val testPath = generateTestPath()
    val tablePath = new Path(testPath)

    try {
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
        println(s"S3 Step 1: First write completed at version $version1")
        version1 shouldBe 1L

        // After this write, versionCounter is at version1 (1)
        // Next write will try version1 + 1 (2)

        // Step 2: Pre-create the NEXT version file out-of-band directly on S3
        val transactionLogDir = new Path(testPath, "_transaction_log")
        val conflictVersion = version1 + 1
        val conflictFile = new Path(transactionLogDir, f"$conflictVersion%020d.json")

        val cloudProvider = CloudStorageProviderFactory.createProvider(
          testPath,
          new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val dummyContent = s"""{"add":{"path":"S3_OUT_OF_BAND_FILE.split","partitionValues":{},"size":888,"modificationTime":1700000000000,"dataChange":true}}"""
          val compressedContent = CompressionUtils.writeTransactionFile(
            dummyContent.getBytes("UTF-8"),
            Some(new GzipCompressionCodec())
          )
          cloudProvider.writeFile(conflictFile.toString, compressedContent)

          cloudProvider.exists(conflictFile.toString) shouldBe true
          println(s"S3 Step 2: Pre-created conflict file at version $conflictVersion: $conflictFile")

          // Step 3: Try to write again using the SAME TransactionLog instance
          // Since protocol checking is disabled, getProtocol() won't be called
          // The versionCounter will increment from version1 to conflictVersion
          // S3's writeFileIfNotExists will return FALSE (object exists via If-None-Match)
          // Retry logic MUST kick in
          val addAction2 = createAddAction(200)
          val version2 = transactionLog.addFiles(Seq(addAction2))

          println(s"S3 Step 3: Second write completed at version $version2")

          // Get metrics - there MUST be a conflict
          val metrics = transactionLog.getLastRetryMetrics()
          metrics shouldBe defined

          println(s"S3 Metrics: attempts=${metrics.get.attemptsMade}, conflicts=${metrics.get.conflictsEncountered}, finalVersion=${metrics.get.finalVersion}, conflictedVersions=${metrics.get.conflictedVersions}")

          // ASSERT: Conflict MUST have been detected and handled
          withClue(s"S3: Expected conflict at version $conflictVersion but got metrics: $metrics") {
            metrics.get.conflictsEncountered should be >= 1
            metrics.get.attemptsMade should be >= 2
            metrics.get.conflictedVersions should contain(conflictVersion)
          }

          // The final version should be conflict version + 1
          version2 shouldBe (conflictVersion + 1)
          println(s"S3 SUCCESS: Conflict at version $conflictVersion detected, retried to version $version2")

          // Verify all files are in the transaction log
          transactionLog.invalidateCache()
          val files = transactionLog.listFiles()
          files.exists(_.path == "file100.split") shouldBe true
          files.exists(_.path == "file200.split") shouldBe true
          files.exists(_.path == "S3_OUT_OF_BAND_FILE.split") shouldBe true
          println("S3 Verified: All files present in transaction log")
        } finally {
          cloudProvider.close()
        }
      } finally {
        transactionLog.close()
      }
    } finally {
      cleanupPath(testPath)
    }
  }
}
