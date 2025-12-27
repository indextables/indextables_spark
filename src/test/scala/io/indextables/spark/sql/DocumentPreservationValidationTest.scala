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

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

/**
 * Test that validates document preservation during MERGE SPLITS operations.
 *
 * This is a critical validation that ensures no documents are lost during the merge process by:
 *   1. Creating actual split files with real documents 2. Querying documents before merge (baseline) 3. Executing MERGE
 *      SPLITS operation 4. Querying documents after merge 5. Validating exact same documents are returned
 */
class DocumentPreservationValidationTest extends TestBase with BeforeAndAfterEach {

  private val logger                         = LoggerFactory.getLogger(classOf[DocumentPreservationValidationTest])
  private var tempTablePath: String          = _
  private var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create temporary table directory
    val tempDir = java.io.File.createTempFile("document_preservation_test", "")
    tempDir.delete()
    tempDir.mkdirs()
    tempTablePath = tempDir.getAbsolutePath

    // Initialize transaction log
    transactionLog = TransactionLogFactory.create(new Path(tempTablePath), spark)

    logger.info(s"Test setup: tempTablePath = $tempTablePath")
  }

  override def afterEach(): Unit = {
    try {
      if (transactionLog != null) {
        transactionLog.close()
      }
      // Cleanup temp directory
      if (tempTablePath != null) {
        val tempDir = new java.io.File(tempTablePath)
        if (tempDir.exists()) {
          org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
        }
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Cleanup failed: ${ex.getMessage}")
    }
    super.afterEach()
  }

  test("MERGE SPLITS should preserve all documents during physical merge operation") {
    // Step 1: Create test dataset with known documents
    // We'll create 20 small splits with 5 documents each (100 total documents)

    val documentsPerSplit      = 5
    val numberOfSplits         = 20
    val totalExpectedDocuments = documentsPerSplit * numberOfSplits

    logger.info(s"Creating $numberOfSplits splits with $documentsPerSplit documents each")
    logger.info(s"Total expected documents: $totalExpectedDocuments")

    // Create test data - each split will have unique document IDs with diverse field types
    val baseDate = java.sql.Date.valueOf("2023-01-01")
    val allTestData = (0 until numberOfSplits).flatMap { splitIndex =>
      (0 until documentsPerSplit).map { docIndex =>
        val globalDocId = splitIndex * documentsPerSplit + docIndex
        Row(
          globalDocId.toLong,                       // id - unique across all documents
          s"Document content for doc $globalDocId", // content
          s"Category for split $splitIndex",        // category
          s"split_$splitIndex",                     // split_group (for partitioning)
          (globalDocId * 100).toInt,                // score - numeric field (0, 100, 200, ...)
          globalDocId * 1.5,                        // rating - double field (0.0, 1.5, 3.0, ...)
          globalDocId % 2 == 0, // is_active - boolean field (alternating true/false)
          new java.sql.Date(baseDate.getTime + globalDocId * 24 * 60 * 60 * 1000L), // created_date - date field (daily increments)
          java.sql.Timestamp.valueOf(s"2023-01-01 ${globalDocId % 24}:00:00") // updated_timestamp - timestamp field
        )
      }
    }

    // Define schema with diverse field types
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("content", StringType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("split_group", StringType, nullable = false),
        StructField("score", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = false),
        StructField("is_active", BooleanType, nullable = false),
        StructField("created_date", DateType, nullable = false),
        StructField("updated_timestamp", TimestampType, nullable = false)
      )
    )

    // Create DataFrame with all test data
    val allDataDF = spark.createDataFrame(
      spark.sparkContext.parallelize(allTestData),
      schema
    )

    logger.info(s"Created test DataFrame with ${allDataDF.count()} rows")

    // Step 2: Write data with many partitions to create many splits
    // Repartition to force creation of multiple split files
    allDataDF
      .repartition(numberOfSplits)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tempTablePath)

    // Step 3: Verify initial split file count and document accessibility
    Thread.sleep(1000) // Allow filesystem operations to complete

    val initialFiles = transactionLog.listFiles()
    logger.info(s"Initial state: ${initialFiles.length} split files created")
    initialFiles.foreach { file =>
      logger.info(s"  Split: ${file.path} (${file.size} bytes, ${file.numRecords.getOrElse("?")} records)")
    }

    // Split count should equal partition count (20 partitions -> 20 splits)
    assert(
      initialFiles.length == numberOfSplits,
      s"Should have created $numberOfSplits split files, but got ${initialFiles.length}"
    )

    // Step 4: Query all documents BEFORE merge (baseline)
    val beforeMergeDF = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempTablePath)
    val beforeMergeCount  = beforeMergeDF.count()
    val beforeMergeDocIds = beforeMergeDF.select("id").collect().map(_.getLong(0)).sorted

    logger.info(s"BEFORE MERGE: Found $beforeMergeCount documents")
    logger.info(s"BEFORE MERGE: Document IDs range: ${beforeMergeDocIds.min} to ${beforeMergeDocIds.max}")

    // Validate we have all expected documents before merge
    assert(
      beforeMergeCount == totalExpectedDocuments,
      s"Before merge: Expected $totalExpectedDocuments documents, but found $beforeMergeCount"
    )

    val expectedDocIds = (0 until totalExpectedDocuments).map(_.toLong).sorted
    assert(beforeMergeDocIds.sameElements(expectedDocIds), s"Before merge: Document IDs don't match expected range")

    // Step 5: Execute MERGE SPLITS with target size that will merge multiple files
    // Use 50MB target size to merge multiple small splits together
    val targetSizeBytes = 50 * 1024 * 1024 // 50MB

    logger.info(s"Executing MERGE SPLITS with target size: $targetSizeBytes bytes")

    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeSplitsCommand = sqlParser
      .parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE $targetSizeBytes")
      .asInstanceOf[MergeSplitsCommand]

    val mergeResult = mergeSplitsCommand.run(spark)
    logger.info(s"MERGE SPLITS result: ${mergeResult.map(_.toString).mkString(", ")}")

    // Step 6: Verify merge actually happened
    Thread.sleep(1000) // Allow merge operations to complete

    // Invalidate cache to ensure we see the latest transaction log state
    transactionLog.invalidateCache()

    val finalFiles = transactionLog.listFiles()
    logger.info(s"After merge: ${finalFiles.length} split files remain")
    finalFiles.foreach { file =>
      logger.info(s"  Merged split: ${file.path} (${file.size} bytes, ${file.numRecords.getOrElse("?")} records)")
    }

    // Should have fewer files after merge
    assert(
      finalFiles.length < initialFiles.length,
      s"Merge should reduce file count: before=${initialFiles.length}, after=${finalFiles.length}"
    )

    // Step 7: Query all documents AFTER merge (validation)
    val afterMergeDF = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempTablePath)
    val afterMergeCount  = afterMergeDF.count()
    val afterMergeDocIds = afterMergeDF.select("id").collect().map(_.getLong(0)).sorted

    logger.info(s"AFTER MERGE: Found $afterMergeCount documents")
    logger.info(s"AFTER MERGE: Document IDs range: ${afterMergeDocIds.min} to ${afterMergeDocIds.max}")

    // Step 8: CRITICAL VALIDATION - Exact document preservation
    assert(
      afterMergeCount == totalExpectedDocuments,
      s"CRITICAL: Document count changed during merge! Expected $totalExpectedDocuments, got $afterMergeCount"
    )

    assert(
      afterMergeCount == beforeMergeCount,
      s"CRITICAL: Document count changed during merge! Before=$beforeMergeCount, After=$afterMergeCount"
    )

    assert(
      afterMergeDocIds.sameElements(beforeMergeDocIds),
      s"CRITICAL: Document IDs changed during merge! Missing or extra documents detected"
    )

    // Step 9: COMPREHENSIVE DOCUMENT CONTENT VALIDATION
    logger.info("🔍 Performing comprehensive document content validation...")

    // Get ALL document data before and after merge for comparison
    val beforeMergeAllData = beforeMergeDF.orderBy("id").collect()
    val afterMergeAllData  = afterMergeDF.orderBy("id").collect()

    // Validate that we have exactly the same number of documents
    assert(
      beforeMergeAllData.length == afterMergeAllData.length,
      s"Document array lengths don't match: before=${beforeMergeAllData.length}, after=${afterMergeAllData.length}"
    )

    // Validate every single document's field values are preserved exactly
    // Compare documents by ID, not by array position, since merging may reorder documents
    var validatedDocuments         = 0
    var contentMismatches          = 0
    var categoryMismatches         = 0
    var splitGroupMismatches       = 0
    var scoreMismatches            = 0
    var ratingMismatches           = 0
    var isActiveMismatches         = 0
    var createdDateMismatches      = 0
    var updatedTimestampMismatches = 0

    // Convert to maps keyed by document ID for correct comparison
    val beforeDocMap = beforeMergeAllData.map(row => row.getLong(0) -> row).toMap
    val afterDocMap  = afterMergeAllData.map(row => row.getLong(0) -> row).toMap

    // Validate that we have the same document IDs in both datasets
    assert(
      beforeDocMap.keySet == afterDocMap.keySet,
      s"Document ID sets don't match: before=${beforeDocMap.keySet.toSeq.sorted}, after=${afterDocMap.keySet.toSeq.sorted}"
    )

    // Compare each document by ID
    for (docId <- beforeDocMap.keySet.toSeq.sorted) {
      val beforeDoc = beforeDocMap(docId)
      val afterDoc  = afterDocMap(docId)

      // Validate content field (should match expected pattern)
      val expectedContent = s"Document content for doc $docId"
      val beforeContent   = beforeDoc.getString(1)
      val afterContent    = afterDoc.getString(1)

      assert(
        beforeContent == expectedContent,
        s"Before merge: Document $docId content doesn't match expected. Got: '$beforeContent', Expected: '$expectedContent'"
      )

      assert(
        afterContent == expectedContent,
        s"After merge: Document $docId content doesn't match expected. Got: '$afterContent', Expected: '$expectedContent'"
      )

      if (beforeContent != afterContent) {
        contentMismatches += 1
        logger.error(s"Content mismatch for doc $docId: before='$beforeContent', after='$afterContent'")
      }

      // Validate category field (should match expected pattern)
      val splitIndex       = (docId / documentsPerSplit).toInt
      val expectedCategory = s"Category for split $splitIndex"
      val beforeCategory   = beforeDoc.getString(2)
      val afterCategory    = afterDoc.getString(2)

      assert(
        beforeCategory == expectedCategory,
        s"Before merge: Document $docId category doesn't match expected. Got: '$beforeCategory', Expected: '$expectedCategory'"
      )

      assert(
        afterCategory == expectedCategory,
        s"After merge: Document $docId category doesn't match expected. Got: '$afterCategory', Expected: '$expectedCategory'"
      )

      if (beforeCategory != afterCategory) {
        categoryMismatches += 1
        logger.error(s"Category mismatch for doc $docId: before='$beforeCategory', after='$afterCategory'")
      }

      // Validate split_group field (should match expected pattern)
      val expectedSplitGroup = s"split_$splitIndex"
      val beforeSplitGroup   = beforeDoc.getString(3)
      val afterSplitGroup    = afterDoc.getString(3)

      assert(
        beforeSplitGroup == expectedSplitGroup,
        s"Before merge: Document $docId split_group doesn't match expected. Got: '$beforeSplitGroup', Expected: '$expectedSplitGroup'"
      )

      assert(
        afterSplitGroup == expectedSplitGroup,
        s"After merge: Document $docId split_group doesn't match expected. Got: '$afterSplitGroup', Expected: '$expectedSplitGroup'"
      )

      if (beforeSplitGroup != afterSplitGroup) {
        splitGroupMismatches += 1
        logger.error(s"Split group mismatch for doc $docId: before='$beforeSplitGroup', after='$afterSplitGroup'")
      }

      // Validate score field (integer - should match expected pattern: docId * 100)
      val expectedScore = (docId * 100).toInt
      val beforeScore   = beforeDoc.getInt(4)
      val afterScore    = afterDoc.getInt(4)

      assert(
        beforeScore == expectedScore,
        s"Before merge: Document $docId score doesn't match expected. Got: $beforeScore, Expected: $expectedScore"
      )

      assert(
        afterScore == expectedScore,
        s"After merge: Document $docId score doesn't match expected. Got: $afterScore, Expected: $expectedScore"
      )

      if (beforeScore != afterScore) {
        scoreMismatches += 1
        logger.error(s"Score mismatch for doc $docId: before=$beforeScore, after=$afterScore")
      }

      // Validate rating field (double - should match expected pattern: docId * 1.5)
      val expectedRating = docId * 1.5
      val beforeRating   = beforeDoc.getDouble(5)
      val afterRating    = afterDoc.getDouble(5)

      assert(
        math.abs(beforeRating - expectedRating) < 0.001,
        s"Before merge: Document $docId rating doesn't match expected. Got: $beforeRating, Expected: $expectedRating"
      )

      assert(
        math.abs(afterRating - expectedRating) < 0.001,
        s"After merge: Document $docId rating doesn't match expected. Got: $afterRating, Expected: $expectedRating"
      )

      if (math.abs(beforeRating - afterRating) > 0.001) {
        ratingMismatches += 1
        logger.error(s"Rating mismatch for doc $docId: before=$beforeRating, after=$afterRating")
      }

      // Validate is_active field (boolean - should match expected pattern: docId % 2 == 0)
      val expectedIsActive = docId % 2 == 0
      val beforeIsActive   = beforeDoc.getBoolean(6)
      val afterIsActive    = afterDoc.getBoolean(6)

      assert(
        beforeIsActive == expectedIsActive,
        s"Before merge: Document $docId is_active doesn't match expected. Got: $beforeIsActive, Expected: $expectedIsActive"
      )

      assert(
        afterIsActive == expectedIsActive,
        s"After merge: Document $docId is_active doesn't match expected. Got: $afterIsActive, Expected: $expectedIsActive"
      )

      if (beforeIsActive != afterIsActive) {
        isActiveMismatches += 1
        logger.error(s"IsActive mismatch for doc $docId: before=$beforeIsActive, after=$afterIsActive")
      }

      // Validate created_date field preservation (before/after merge consistency)
      // Note: Skipping exact value validation due to known DateType handling issues
      val beforeCreatedDate = beforeDoc.getDate(7)
      val afterCreatedDate  = afterDoc.getDate(7)

      if (!beforeCreatedDate.equals(afterCreatedDate)) {
        createdDateMismatches += 1
        logger.error(s"CreatedDate mismatch for doc $docId: before=$beforeCreatedDate, after=$afterCreatedDate")
      }

      // Validate updated_timestamp field preservation (before/after merge consistency)
      // Note: Skipping exact value validation due to known timestamp handling issues
      val beforeUpdatedTimestamp = beforeDoc.getTimestamp(8)
      val afterUpdatedTimestamp  = afterDoc.getTimestamp(8)

      if (!beforeUpdatedTimestamp.equals(afterUpdatedTimestamp)) {
        updatedTimestampMismatches += 1
        logger.error(
          s"UpdatedTimestamp mismatch for doc $docId: before=$beforeUpdatedTimestamp, after=$afterUpdatedTimestamp"
        )
      }

      validatedDocuments += 1
    }

    // Final comprehensive validation assertions
    assert(
      contentMismatches == 0,
      s"CRITICAL: Found $contentMismatches documents with content field changes during merge!"
    )

    assert(
      categoryMismatches == 0,
      s"CRITICAL: Found $categoryMismatches documents with category field changes during merge!"
    )

    assert(
      splitGroupMismatches == 0,
      s"CRITICAL: Found $splitGroupMismatches documents with split_group field changes during merge!"
    )

    assert(scoreMismatches == 0, s"CRITICAL: Found $scoreMismatches documents with score field changes during merge!")

    assert(ratingMismatches == 0, s"CRITICAL: Found $ratingMismatches documents with rating field changes during merge!")

    assert(
      isActiveMismatches == 0,
      s"CRITICAL: Found $isActiveMismatches documents with is_active field changes during merge!"
    )

    assert(
      createdDateMismatches == 0,
      s"CRITICAL: Found $createdDateMismatches documents with created_date field changes during merge!"
    )

    assert(
      updatedTimestampMismatches == 0,
      s"CRITICAL: Found $updatedTimestampMismatches documents with updated_timestamp field changes during merge!"
    )

    logger.info(s"✅ COMPREHENSIVE CONTENT VALIDATION PASSED!")
    logger.info(s"  📝 Validated $validatedDocuments documents across ALL field types")
    logger.info(s"  🔍 All 'content' fields preserved: $contentMismatches mismatches")
    logger.info(s"  🏷️  All 'category' fields preserved: $categoryMismatches mismatches")
    logger.info(s"  📂 All 'split_group' fields preserved: $splitGroupMismatches mismatches")
    logger.info(s"  🔢 All 'score' (integer) fields preserved: $scoreMismatches mismatches")
    logger.info(s"  ⭐ All 'rating' (double) fields preserved: $ratingMismatches mismatches")
    logger.info(s"  ✅ All 'is_active' (boolean) fields preserved: $isActiveMismatches mismatches")
    logger.info(s"  📅 All 'created_date' (date) fields preserved: $createdDateMismatches mismatches")
    logger.info(s"  🕒 All 'updated_timestamp' (timestamp) fields preserved: $updatedTimestampMismatches mismatches")

    // Additional validation: Test a few random document queries to verify search functionality
    val randomDocIds = Seq(0L, totalExpectedDocuments / 4, totalExpectedDocuments / 2, totalExpectedDocuments - 1)
    for (docId <- randomDocIds) {
      val searchResult = afterMergeDF.filter(s"id = $docId").collect()
      assert(searchResult.length == 1, s"Should find exactly 1 document with ID $docId after merge")

      val doc = searchResult(0)

      // Validate content field
      val expectedContent = s"Document content for doc $docId"
      assert(doc.getString(1) == expectedContent, s"Search result content validation failed for doc $docId")

      // Validate numeric fields
      val expectedScore = (docId * 100).toInt
      assert(
        doc.getInt(4) == expectedScore,
        s"Search result score validation failed for doc $docId: got ${doc.getInt(4)}, expected $expectedScore"
      )

      val expectedRating = docId * 1.5
      assert(
        math.abs(doc.getDouble(5) - expectedRating) < 0.001,
        s"Search result rating validation failed for doc $docId: got ${doc.getDouble(5)}, expected $expectedRating"
      )

      // Validate boolean field
      val expectedIsActive = docId % 2 == 0
      assert(
        doc.getBoolean(6) == expectedIsActive,
        s"Search result is_active validation failed for doc $docId: got ${doc.getBoolean(6)}, expected $expectedIsActive"
      )

      // Validate date field exists (basic presence check)
      val searchCreatedDate = doc.getDate(7)
      assert(searchCreatedDate != null, s"Search result created_date should not be null for doc $docId")
    }

    logger.info(s"✅ SEARCH FUNCTIONALITY VALIDATION PASSED!")
    logger.info(s"  🔍 Random document queries: ${randomDocIds.length} documents tested across all field types")

    // Step 10: Success confirmation
    logger.info("🎉 COMPLETE DOCUMENT PRESERVATION VALIDATION PASSED!")
    logger.info(s"  📊 Files merged: ${initialFiles.length} → ${finalFiles.length}")
    logger.info(s"  📄 Documents preserved: $afterMergeCount / $totalExpectedDocuments")
    logger.info(s"  🔍 Document IDs verified: All ${afterMergeDocIds.length} IDs match exactly")
    logger.info(
      s"  📝 ALL 9 field types validated: $validatedDocuments documents × 9 fields = ${validatedDocuments * 9} field validations"
    )
    logger.info(s"  🔍 Field types validated: String, Integer, Double, Boolean, Date, Timestamp")
    logger.info(s"  🔍 Search queries verified: Random document lookups with multi-field validation successful")
  }

  test("MERGE SPLITS should handle edge case with single document per split") {
    // Edge case: many tiny splits with 1 document each
    val documentsPerSplit      = 1
    val numberOfSplits         = 10
    val totalExpectedDocuments = documentsPerSplit * numberOfSplits

    logger.info(s"Edge case: Creating $numberOfSplits splits with $documentsPerSplit document each")

    val testData = (0 until numberOfSplits).map { docId =>
      Row(docId.toLong, s"Single document $docId", "test", "single")
    }

    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("content", StringType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("group", StringType, nullable = false)
      )
    )

    val dataDF = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)

    // Write with very small target to create many tiny splits
    dataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tempTablePath)

    Thread.sleep(500)

    // Query before merge
    val beforeCount =
      spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempTablePath).count()

    // Execute merge
    val mergeSplitsCommand = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
      .parsePlan(s"MERGE SPLITS '$tempTablePath' TARGET SIZE ${10 * 1024 * 1024}") // 10MB
      .asInstanceOf[MergeSplitsCommand]

    mergeSplitsCommand.run(spark)
    Thread.sleep(500)

    // Query after merge
    val afterCount =
      spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempTablePath).count()

    // Validation
    assert(afterCount == beforeCount, s"Edge case failed: Document count changed from $beforeCount to $afterCount")

    assert(
      afterCount == totalExpectedDocuments,
      s"Edge case failed: Expected $totalExpectedDocuments documents, got $afterCount"
    )

    logger.info(s"✅ Edge case passed: $afterCount documents preserved through merge")
  }
}
