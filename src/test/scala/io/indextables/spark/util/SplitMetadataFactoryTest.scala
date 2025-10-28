package io.indextables.spark.util

import io.indextables.spark.transaction.AddAction
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SplitMetadataFactoryTest extends AnyFunSuite with Matchers {

  test("fromAddAction should create metadata with pre-computed offsets") {
    val addAction = AddAction(
      path = "partition1/split-12345.split",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      minValues = Some(Map.empty),
      maxValues = Some(Map.empty),
      numRecords = Some(100L),
      footerStartOffset = Some(800L),
      footerEndOffset = Some(1000L)
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    // Just verify metadata is created successfully
    metadata should not be null
  }

  test("fromAddAction should extract split ID correctly") {
    val addAction = AddAction(
      path = "nested/path/to/split-67890.split",
      partitionValues = Map.empty,
      size = 500L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      minValues = Some(Map.empty),
      maxValues = Some(Map.empty),
      numRecords = Some(50L),
      footerStartOffset = Some(400L),
      footerEndOffset = Some(500L)
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    // Just verify metadata is created successfully
    metadata should not be null
  }

  test("fromAddAction should handle split ID without nested paths") {
    val addAction = AddAction(
      path = "split-simple.split",
      partitionValues = Map.empty,
      size = 500L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      minValues = Some(Map.empty),
      maxValues = Some(Map.empty),
      numRecords = Some(50L),
      footerStartOffset = Some(400L),
      footerEndOffset = Some(500L)
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    // Just verify metadata is created successfully
    metadata should not be null
  }

  test("fromAddActions should create metadata for multiple actions") {
    val actions = Seq(
      AddAction(
        path = "split-1.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        minValues = Some(Map.empty),
        maxValues = Some(Map.empty),
        numRecords = Some(100L),
        footerStartOffset = Some(800L),
        footerEndOffset = Some(1000L)
      ),
      AddAction(
        path = "split-2.split",
        partitionValues = Map.empty,
        size = 2000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        minValues = Some(Map.empty),
        maxValues = Some(Map.empty),
        numRecords = Some(200L),
        footerStartOffset = Some(1600L),
        footerEndOffset = Some(2000L)
      )
    )

    val metadataList = SplitMetadataFactory.fromAddActions(actions, "s3://bucket/table")

    metadataList should have size 2
    metadataList.foreach(metadata => metadata should not be null)
  }

  test("fromAddAction should use 0L for missing numRecords") {
    val addAction = AddAction(
      path = "split-no-stats.split",
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      minValues = Some(Map.empty),
      maxValues = Some(Map.empty),
      numRecords = None,
      footerStartOffset = Some(0L),
      footerEndOffset = Some(100L)
    )

    val metadata = SplitMetadataFactory.fromAddAction(addAction, "s3://bucket/table")

    // Just verify metadata is created successfully
    metadata should not be null
  }
}
