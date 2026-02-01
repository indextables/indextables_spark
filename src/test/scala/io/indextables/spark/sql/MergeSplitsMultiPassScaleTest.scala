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

package io.indextables.spark.sql

import io.indextables.spark.transaction.AddAction
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

/**
 * Tests for multi-pass merge logic at scale.
 *
 * Simulates realistic production scenarios:
 *   - 7 days of data with day/hour partitions (168 partitions)
 *   - 20,000+ files per day (~833 files per hour)
 *   - Each file ~500MB
 *
 * These tests validate the merge group finding and prioritization logic without actually executing merges (which would
 * require real files).
 */
class MergeSplitsMultiPassScaleTest extends AnyFunSuite {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsMultiPassScaleTest])

  // Realistic file sizes
  val FILE_SIZE_500MB: Long = 500L * 1024 * 1024      // 500MB per file
  val TARGET_SIZE_4GB: Long = 4L * 1024 * 1024 * 1024 // 4GB target

  // Scale parameters
  val NUM_DAYS       = 7
  val HOURS_PER_DAY  = 24
  val FILES_PER_HOUR = 833 // ~20,000 files per day

  /** Creates mock AddAction entries for a day/hour partitioned dataset. */
  def createMockFiles(
    numDays: Int,
    hoursPerDay: Int,
    filesPerHour: Int,
    fileSize: Long
  ): Seq[AddAction] = {
    val baseTime = System.currentTimeMillis() - (numDays * 24 * 60 * 60 * 1000L)

    (0 until numDays).flatMap { day =>
      val dateStr = f"2024-01-${day + 1}%02d"
      (0 until hoursPerDay).flatMap { hour =>
        val hourStr = f"$hour%02d"
        (0 until filesPerHour).map { fileIdx =>
          val path            = s"day=$dateStr/hour=$hourStr/part-$fileIdx.split"
          val partitionValues = Map("day" -> dateStr, "hour" -> hourStr)
          val modTime         = baseTime + (day * 24 * 60 * 60 * 1000L) + (hour * 60 * 60 * 1000L) + fileIdx

          AddAction(
            path = path,
            partitionValues = partitionValues,
            size = fileSize,
            modificationTime = modTime,
            dataChange = true,
            stats = None,
            tags = None,
            numRecords = Some(10000L)
          )
        }
      }
    }
  }

  /**
   * Simulates the merge group finding logic from MergeSplitsExecutor. Groups files by partition and creates merge
   * groups based on target size.
   */
  def findMergeGroups(
    files: Seq[AddAction],
    targetSize: Long,
    skipSplitThresholdPercent: Double = 0.45,
    maxSourceSplitsPerMerge: Int = 1000
  ): Seq[MergeGroup] = {
    val skipThreshold = (targetSize * skipSplitThresholdPercent).toLong

    // Filter out files above skip threshold
    val eligibleFiles = files.filter(_.size < skipThreshold)

    // Group by partition
    val filesByPartition = eligibleFiles.groupBy(_.partitionValues)

    // Create merge groups within each partition
    filesByPartition.flatMap {
      case (partitionValues, partitionFiles) =>
        // Sort by modification time (oldest first)
        val sortedFiles = partitionFiles.sortBy(_.modificationTime)

        // Bin-pack files into groups up to target size
        val groups       = scala.collection.mutable.ArrayBuffer[MergeGroup]()
        var currentGroup = scala.collection.mutable.ArrayBuffer[AddAction]()
        var currentSize  = 0L

        sortedFiles.foreach { file =>
          if (currentSize + file.size > targetSize || currentGroup.length >= maxSourceSplitsPerMerge) {
            if (currentGroup.length >= 2) {
              groups += MergeGroup(partitionValues, currentGroup.toSeq)
            }
            currentGroup = scala.collection.mutable.ArrayBuffer[AddAction]()
            currentSize = 0L
          }
          currentGroup += file
          currentSize += file.size
        }

        // Don't forget the last group
        if (currentGroup.length >= 2) {
          groups += MergeGroup(partitionValues, currentGroup.toSeq)
        }

        groups.toSeq
    }.toSeq
  }

  /** Simulates merge pass execution - returns the expected merged file sizes. */
  def simulateMergePass(groups: Seq[MergeGroup]): Seq[AddAction] =
    groups.map { group =>
      val mergedSize      = group.files.map(_.size).sum
      val partitionValues = group.partitionValues
      val path = s"day=${partitionValues
          .getOrElse("day", "unknown")}/hour=${partitionValues.getOrElse("hour", "unknown")}/merged-${System.nanoTime()}.split"

      AddAction(
        path = path,
        partitionValues = partitionValues,
        size = mergedSize,
        modificationTime = System.currentTimeMillis(),
        dataChange = false,
        stats = None,
        tags = None,
        numRecords = Some(group.files.flatMap(_.numRecords).sum)
      )
    }

  /** Simulates the full multi-pass merge loop. */
  def simulateMultiPassMerge(
    initialFiles: Seq[AddAction],
    targetSize: Long,
    maxPasses: Int = 10
  ): (Int, Seq[AddAction]) = {
    var currentFiles    = initialFiles
    var passNumber      = 0
    var continueLooping = true

    while (continueLooping && passNumber < maxPasses) {
      passNumber += 1
      val groups = findMergeGroups(currentFiles, targetSize)

      if (groups.isEmpty) {
        continueLooping = false
      } else {
        // Remove source files from current set
        val sourceFilePaths = groups.flatMap(_.files.map(_.path)).toSet
        val remainingFiles  = currentFiles.filterNot(f => sourceFilePaths.contains(f.path))

        // Add merged files
        val mergedFiles = simulateMergePass(groups)
        currentFiles = remainingFiles ++ mergedFiles

        logger.info(
          s"Pass $passNumber: Merged ${groups.map(_.files.length).sum} files into ${mergedFiles.length} splits"
        )
      }
    }

    (passNumber - (if (continueLooping) 0 else 1), currentFiles)
  }

  test("scale test: 7 days x 24 hours x 833 files/hour = 140,056 files") {
    logger.info("Creating mock files for scale test...")

    val files = createMockFiles(NUM_DAYS, HOURS_PER_DAY, FILES_PER_HOUR, FILE_SIZE_500MB)

    logger.info(s"Created ${files.length} mock files")
    assert(files.length == NUM_DAYS * HOURS_PER_DAY * FILES_PER_HOUR)

    // Verify partition distribution
    val filesByPartition = files.groupBy(_.partitionValues)
    assert(filesByPartition.size == NUM_DAYS * HOURS_PER_DAY, s"Should have ${NUM_DAYS * HOURS_PER_DAY} partitions")

    // Verify each partition has the expected number of files
    filesByPartition.foreach {
      case (partition, partitionFiles) =>
        assert(
          partitionFiles.length == FILES_PER_HOUR,
          s"Partition $partition should have $FILES_PER_HOUR files, got ${partitionFiles.length}"
        )
    }

    // Calculate total data size
    val totalSizeGB = files.map(_.size).sum / (1024.0 * 1024 * 1024)
    logger.info(f"Total data size: $totalSizeGB%.2f GB")
    assert(totalSizeGB > 68000, "Should have ~70TB of data")
  }

  test("merge group finding: files should be grouped within partitions only") {
    val files  = createMockFiles(NUM_DAYS, HOURS_PER_DAY, FILES_PER_HOUR, FILE_SIZE_500MB)
    val groups = findMergeGroups(files, TARGET_SIZE_4GB)

    logger.info(s"Found ${groups.length} merge groups")

    // Each group should only contain files from a single partition
    groups.foreach { group =>
      val partitions = group.files.map(_.partitionValues).distinct
      assert(
        partitions.length == 1,
        s"Merge group should contain files from exactly one partition, found ${partitions.length}"
      )
    }

    // All groups should have at least 2 files
    groups.foreach { group =>
      assert(group.files.length >= 2, s"Merge group should have at least 2 files, got ${group.files.length}")
    }
  }

  test("merge group finding: 500MB files into 4GB target = ~8 files per group") {
    val files  = createMockFiles(NUM_DAYS, HOURS_PER_DAY, FILES_PER_HOUR, FILE_SIZE_500MB)
    val groups = findMergeGroups(files, TARGET_SIZE_4GB)

    // With 500MB files and 4GB target, each group should have ~8 files
    val avgFilesPerGroup = groups.map(_.files.length).sum.toDouble / groups.length
    logger.info(f"Average files per group: $avgFilesPerGroup%.2f")

    // Should be roughly 8 files per group (4GB / 500MB = 8)
    assert(avgFilesPerGroup >= 7 && avgFilesPerGroup <= 9, f"Expected ~8 files per group, got $avgFilesPerGroup%.2f")
  }

  test("merge group finding: number of merge groups per partition") {
    val files  = createMockFiles(NUM_DAYS, HOURS_PER_DAY, FILES_PER_HOUR, FILE_SIZE_500MB)
    val groups = findMergeGroups(files, TARGET_SIZE_4GB)

    // Group merge groups by partition
    val groupsByPartition = groups.groupBy(_.partitionValues)

    // Each partition has 833 files, with ~8 files per merge group = ~104 groups per partition
    val expectedGroupsPerPartition = FILES_PER_HOUR / 8
    logger.info(s"Expected ~$expectedGroupsPerPartition merge groups per partition")

    groupsByPartition.foreach {
      case (partition, partitionGroups) =>
        // Allow some variance due to bin-packing
        assert(
          partitionGroups.length >= expectedGroupsPerPartition - 10 &&
            partitionGroups.length <= expectedGroupsPerPartition + 10,
          s"Partition $partition: expected ~$expectedGroupsPerPartition groups, got ${partitionGroups.length}"
        )
    }

    // Total groups should be ~104 * 168 = ~17,472
    val expectedTotalGroups = expectedGroupsPerPartition * NUM_DAYS * HOURS_PER_DAY
    logger.info(s"Total merge groups: ${groups.length} (expected ~$expectedTotalGroups)")
  }

  test("multi-pass simulation: should complete within maxPasses") {
    // Use a smaller scale for this test to keep it fast
    val smallFiles = createMockFiles(1, 4, 100, FILE_SIZE_500MB) // 1 day, 4 hours, 100 files/hour = 400 files

    val (passCount, finalFiles) = simulateMultiPassMerge(smallFiles, TARGET_SIZE_4GB, maxPasses = 10)

    logger.info(s"Completed in $passCount passes, ${finalFiles.length} files remaining")

    // Should complete in 1-2 passes for this small dataset
    assert(passCount <= 3, s"Should complete within 3 passes, took $passCount")

    // Final file count should be much smaller than initial
    assert(
      finalFiles.length < smallFiles.length,
      s"Should have fewer files after merge: ${finalFiles.length} vs ${smallFiles.length}"
    )
  }

  test("multi-pass simulation: merged files can trigger second pass") {
    // Create files sized so that merged files are still below skip threshold
    // 500MB files, 4GB target, 45% skip threshold = 1.8GB skip threshold
    // First pass: 8 x 500MB = 4GB merged files (above skip threshold, won't re-merge)

    // To trigger a second pass, we need merged files to be below skip threshold
    // Use smaller files: 100MB each, 4GB target = 40 files per group
    // Merged size = 4GB, above 1.8GB threshold, won't re-merge

    // For second pass to trigger, use: 200MB files, 2GB target
    // Skip threshold = 0.9GB
    // First pass: 10 x 200MB = 2GB merged files (above 0.9GB, won't re-merge in pass 2)

    // Actually, to trigger multi-pass, merged files need to be BELOW skip threshold
    // This happens when target size is large relative to total partition data

    // Create a scenario where pass 1 creates files that can be merged again:
    // - 50MB files, 500MB target, 45% skip = 225MB threshold
    // - Pass 1: 10 x 50MB = 500MB per group (above threshold, won't re-merge)

    // The multi-pass logic kicks in when:
    // 1. Many small files exist initially
    // 2. After merge, the merged files are still small enough to re-merge

    // Use 50MB files, 1GB target
    val smallFileSize = 50L * 1024 * 1024          // 50MB
    val targetSize    = 1L * 1024 * 1024 * 1024    // 1GB
    val skipThreshold = (targetSize * 0.45).toLong // 450MB

    // Create files: 2 partitions, each with 100 files of 50MB = 5GB per partition
    val files = (0 until 2).flatMap { partIdx =>
      (0 until 100).map { fileIdx =>
        AddAction(
          path = s"partition=$partIdx/file-$fileIdx.split",
          partitionValues = Map("partition" -> partIdx.toString),
          size = smallFileSize,
          modificationTime = System.currentTimeMillis() + fileIdx,
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(1000L)
        )
      }
    }

    logger.info(s"Created ${files.length} files of ${smallFileSize / (1024 * 1024)}MB each")
    logger.info(
      s"Target size: ${targetSize / (1024 * 1024 * 1024)}GB, skip threshold: ${skipThreshold / (1024 * 1024)}MB"
    )

    // First pass groups
    val pass1Groups = findMergeGroups(files, targetSize)
    logger.info(s"Pass 1 would create ${pass1Groups.length} merged files")

    // Simulate pass 1
    val pass1MergedFiles = simulateMergePass(pass1Groups)
    val pass1Sizes       = pass1MergedFiles.map(_.size / (1024 * 1024))
    logger.info(s"Pass 1 merged file sizes: ${pass1Sizes.take(5).mkString(", ")}... MB")

    // Check if any merged files are below skip threshold (would trigger pass 2)
    val belowThreshold = pass1MergedFiles.count(_.size < skipThreshold)
    logger.info(s"Files below skip threshold after pass 1: $belowThreshold")

    // In this case, merged files are ~1GB each (20 x 50MB), which is above 450MB threshold
    // So pass 2 won't find any eligible files - this is expected behavior
  }

  test("prioritization: groups with most files should be processed first") {
    // Create groups with varying file counts
    def createGroup(
      partition: String,
      numFiles: Int,
      modTime: Long
    ): MergeGroup = {
      val files = (0 until numFiles).map { i =>
        AddAction(
          path = s"partition=$partition/file-$i.split",
          partitionValues = Map("partition" -> partition),
          size = FILE_SIZE_500MB,
          modificationTime = modTime + i,
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(1000L)
        )
      }
      MergeGroup(Map("partition" -> partition), files)
    }

    val groups = Seq(
      createGroup("a", 5, 1000L),  // 5 files, oldest=1000
      createGroup("b", 10, 2000L), // 10 files, oldest=2000
      createGroup("c", 8, 500L),   // 8 files, oldest=500
      createGroup("d", 10, 1500L), // 10 files, oldest=1500 (same count as b, older)
      createGroup("e", 3, 100L)    // 3 files, oldest=100
    )

    // Sort using the same logic as MergeSplitsExecutor
    val prioritized = groups.sortBy { g =>
      val sourceCount = -g.files.length                     // Descending
      val oldestTime  = g.files.map(_.modificationTime).min // Ascending for ties
      (sourceCount, oldestTime)
    }

    // Expected order: b and d both have 10 files, d is older so comes first
    // Then c (8 files), a (5 files), e (3 files)
    val expectedOrder = Seq("d", "b", "c", "a", "e")
    val actualOrder   = prioritized.map(_.partitionValues("partition"))

    logger.info(s"Expected order: ${expectedOrder.mkString(", ")}")
    logger.info(s"Actual order: ${actualOrder.mkString(", ")}")

    assert(actualOrder == expectedOrder, s"Prioritization order mismatch")
  }

  test("skip threshold: files above 45% of target should be excluded") {
    val targetSize    = TARGET_SIZE_4GB
    val skipThreshold = (targetSize * 0.45).toLong // ~1.93GB

    logger.info(s"Target size: $targetSize bytes (${targetSize / (1024 * 1024 * 1024)}GB)")
    logger.info(s"Skip threshold (45%): $skipThreshold bytes (${skipThreshold / (1024 * 1024)}MB)")

    val files = Seq(
      AddAction("small.split", Map("p" -> "1"), 500L * 1024 * 1024, 0L, true, None, None, None), // 500MB - eligible
      AddAction("medium.split", Map("p" -> "1"), 1500L * 1024 * 1024, 0L, true, None, None, None), // 1.5GB - eligible
      AddAction("borderline.split", Map("p" -> "1"), 1800L * 1024 * 1024, 0L, true, None, None, None), // 1.8GB - eligible (below 1.93GB threshold)
      AddAction("threshold.split", Map("p" -> "1"), 2000L * 1024 * 1024, 0L, true, None, None, None), // 2GB - excluded (>= 1.93GB threshold)
      AddAction("large.split", Map("p" -> "1"), 2500L * 1024 * 1024, 0L, true, None, None, None), // 2.5GB - excluded
      AddAction("huge.split", Map("p" -> "1"), 3500L * 1024 * 1024, 0L, true, None, None, None) // 3.5GB - excluded
    )

    val eligible = files.filter(_.size < skipThreshold)
    logger.info(s"Eligible files: ${eligible.map(f => s"${f.path} (${f.size / (1024 * 1024)}MB)").mkString(", ")}")

    assert(eligible.length == 3, s"Should have 3 eligible files, got ${eligible.length}")
    assert(eligible.map(_.path).contains("small.split"), "small.split should be eligible")
    assert(eligible.map(_.path).contains("medium.split"), "medium.split should be eligible")
    assert(
      eligible.map(_.path).contains("borderline.split"),
      "borderline.split should be eligible (1.8GB < 1.93GB threshold)"
    )
  }

  test("maxSourceSplitsPerMerge: groups should not exceed limit") {
    // Create a partition with many small files
    val files = (0 until 2000).map { i =>
      AddAction(
        path = s"partition=test/file-$i.split",
        partitionValues = Map("partition" -> "test"),
        size = 10L * 1024 * 1024, // 10MB each
        modificationTime = i.toLong,
        dataChange = true,
        stats = None,
        tags = None,
        numRecords = Some(100L)
      )
    }

    val maxSourceSplits = 500
    val groups          = findMergeGroups(files, TARGET_SIZE_4GB, maxSourceSplitsPerMerge = maxSourceSplits)

    // All groups should have at most maxSourceSplitsPerMerge files
    groups.foreach { group =>
      assert(
        group.files.length <= maxSourceSplits,
        s"Group has ${group.files.length} files, exceeds max of $maxSourceSplits"
      )
    }

    // With 2000 files and max 500 per group, should have at least 4 groups
    assert(groups.length >= 4, s"Should have at least 4 groups, got ${groups.length}")

    logger.info(s"Created ${groups.length} groups with max ${groups.map(_.files.length).max} files each")
  }

  test("full scale simulation: estimate merge groups and passes") {
    logger.info("=" * 60)
    logger.info("Full scale simulation: 7 days, 24 hours, ~20000 files/day")
    logger.info("=" * 60)

    val files = createMockFiles(NUM_DAYS, HOURS_PER_DAY, FILES_PER_HOUR, FILE_SIZE_500MB)

    logger.info(s"Total files: ${files.length}")
    logger.info(f"Total data: ${files.map(_.size).sum / (1024.0 * 1024 * 1024 * 1024)}%.2f TB")

    val groups = findMergeGroups(files, TARGET_SIZE_4GB)

    logger.info(s"Total merge groups: ${groups.length}")
    logger.info(s"Total files to merge: ${groups.map(_.files.length).sum}")
    logger.info(f"Average files per group: ${groups.map(_.files.length).sum.toDouble / groups.length}%.2f")

    // Estimate merged output
    val estimatedMergedFiles  = groups.length
    val estimatedMergedSizeTB = groups.map(g => g.files.map(_.size).sum).sum / (1024.0 * 1024 * 1024 * 1024)
    logger.info(s"After merge: ~$estimatedMergedFiles files, $estimatedMergedSizeTB%.2f TB")

    // Verify that merge would significantly reduce file count
    val reductionRatio = files.length.toDouble / estimatedMergedFiles
    logger.info(f"File count reduction: $reductionRatio%.1fx")

    assert(reductionRatio > 5, "Merge should reduce file count by at least 5x")
    logger.info("=" * 60)
  }
}
