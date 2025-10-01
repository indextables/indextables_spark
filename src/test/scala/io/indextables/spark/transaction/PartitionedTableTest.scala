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

import io.indextables.spark.TestBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import java.nio.file.Files

class PartitionedTableTest extends TestBase with BeforeAndAfterEach {

  private var testTempDir: java.nio.file.Path = _
  private var tablePath: Path                 = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testTempDir = Files.createTempDirectory("tantivy_partition_test_")
    tablePath = new Path(testTempDir.toUri)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (testTempDir != null) {
      // Clean up temp directory
      Files
        .walk(testTempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  test("should initialize transaction log with partition columns") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType)
      )
    )

    val partitionColumns = Seq("year", "month")
    val transactionLog   = TransactionLogFactory.create(tablePath, spark)

    transactionLog.initialize(schema, partitionColumns)

    // Verify partition columns are stored
    val retrievedPartitionColumns = transactionLog.getPartitionColumns()
    assert(retrievedPartitionColumns == partitionColumns)
    assert(transactionLog.isPartitioned())
  }

  test("should validate partition columns exist in schema") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    val invalidPartitionColumns = Seq("nonexistent_column")
    val transactionLog          = TransactionLogFactory.create(tablePath, spark)

    // Should throw exception for invalid partition columns
    assertThrows[IllegalArgumentException] {
      transactionLog.initialize(schema, invalidPartitionColumns)
    }
  }

  test("should extract partition values from rows") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("year", IntegerType),
        StructField("month", StringType),
        StructField("active", BooleanType)
      )
    )

    val partitionColumns = Seq("year", "month", "active")

    // Create test data
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(1, "Alice", 2023, "01", true),
          Row(2, "Bob", 2023, "02", false),
          Row(3, "Charlie", 2024, "01", true)
        )
      ),
      schema
    )

    val _        = df.collect()
    val firstRow = df.queryExecution.toRdd.first()

    val partitionValues = PartitionUtils.extractPartitionValues(firstRow, schema, partitionColumns)

    assert(partitionValues("year") == "2023")
    assert(partitionValues("month") == "01")
    assert(partitionValues("active") == "true")
  }

  test("should create ADD actions with partition values") {
    val transactionLog = TransactionLogFactory.create(tablePath, spark)

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("year", IntegerType)
      )
    )

    val partitionColumns = Seq("year")
    transactionLog.initialize(schema, partitionColumns)

    // Create ADD action with partition values
    val addAction = AddAction(
      path = "year=2023/file1.split",
      partitionValues = Map("year" -> "2023"),
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L)
    )

    transactionLog.addFile(addAction)

    // Verify partition values are preserved
    val files = transactionLog.listFiles()
    assert(files.length == 1)
    assert(files.head.partitionValues("year") == "2023")
  }

  test("should prune partitions based on filters") {
    val addActions = Seq(
      AddAction(
        "year=2022/file1.split",
        Map("year" -> "2022"),
        1000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(100L)
      ),
      AddAction(
        "year=2023/file2.split",
        Map("year" -> "2023"),
        1000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(200L)
      ),
      AddAction(
        "year=2024/file3.split",
        Map("year" -> "2024"),
        1000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(150L)
      )
    )

    val partitionColumns = Seq("year")

    // Test equality filter
    val equalFilter: Array[org.apache.spark.sql.sources.Filter] =
      Array(org.apache.spark.sql.sources.EqualTo("year", "2023"))
    val prunedEqual = PartitionPruning.prunePartitions(addActions, partitionColumns, equalFilter)
    assert(prunedEqual.length == 1)
    assert(prunedEqual.head.partitionValues("year") == "2023")

    // Test IN filter
    val inFilter: Array[org.apache.spark.sql.sources.Filter] =
      Array(org.apache.spark.sql.sources.In("year", Array("2022", "2024")))
    val prunedIn = PartitionPruning.prunePartitions(addActions, partitionColumns, inFilter)
    assert(prunedIn.length == 2)
    assert(prunedIn.exists(_.partitionValues("year") == "2022"))
    assert(prunedIn.exists(_.partitionValues("year") == "2024"))

    // Test greater than filter
    val gtFilter: Array[org.apache.spark.sql.sources.Filter] =
      Array(org.apache.spark.sql.sources.GreaterThan("year", "2022"))
    val prunedGt = PartitionPruning.prunePartitions(addActions, partitionColumns, gtFilter)
    assert(prunedGt.length == 2)
    assert(!prunedGt.exists(_.partitionValues("year") == "2022"))
  }

  test("should handle multiple partition columns") {
    val addActions = Seq(
      AddAction(
        "year=2023/month=01/file1.split",
        Map("year" -> "2023", "month" -> "01"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "year=2023/month=02/file2.split",
        Map("year" -> "2023", "month" -> "02"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "year=2024/month=01/file3.split",
        Map("year" -> "2024", "month" -> "01"),
        1000L,
        System.currentTimeMillis(),
        true
      )
    )

    val partitionColumns = Seq("year", "month")

    // Test compound filter (year=2023 AND month=01)
    val compoundFilter: Array[org.apache.spark.sql.sources.Filter] = Array(
      org.apache.spark.sql.sources.And(
        org.apache.spark.sql.sources.EqualTo("year", "2023"),
        org.apache.spark.sql.sources.EqualTo("month", "01")
      )
    )

    val pruned = PartitionPruning.prunePartitions(addActions, partitionColumns, compoundFilter)
    assert(pruned.length == 1)
    assert(pruned.head.partitionValues("year") == "2023")
    assert(pruned.head.partitionValues("month") == "01")
  }

  test("should support overwrite mode with partitions") {
    val transactionLog = TransactionLogFactory.create(tablePath, spark)

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("value", StringType),
        StructField("year", IntegerType)
      )
    )

    transactionLog.initialize(schema, Seq("year"))

    // Initial data with multiple partitions
    val initialActions = Seq(
      AddAction(
        "year=2022/file1.split",
        Map("year" -> "2022"),
        1000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(50L)
      ),
      AddAction(
        "year=2023/file2.split",
        Map("year" -> "2023"),
        1000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(75L)
      )
    )
    transactionLog.addFiles(initialActions)

    // Overwrite with new partitioned data
    val overwriteActions = Seq(
      AddAction(
        "year=2023/file3.split",
        Map("year" -> "2023"),
        2000L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(100L)
      ),
      AddAction(
        "year=2024/file4.split",
        Map("year" -> "2024"),
        1500L,
        System.currentTimeMillis(),
        true,
        numRecords = Some(125L)
      )
    )
    transactionLog.overwriteFiles(overwriteActions)

    // Verify overwrite results
    val files = transactionLog.listFiles()
    assert(files.length == 2)
    assert(files.exists(f => f.partitionValues("year") == "2023" && f.path.contains("file3")))
    assert(files.exists(f => f.partitionValues("year") == "2024"))
    assert(!files.exists(f => f.partitionValues("year") == "2022")) // Original data should be removed
  }

  test("should generate partition statistics") {
    val addActions = Seq(
      AddAction(
        "year=2023/month=01/file1.split",
        Map("year" -> "2023", "month" -> "01"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "year=2023/month=01/file2.split",
        Map("year" -> "2023", "month" -> "01"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "year=2023/month=02/file3.split",
        Map("year" -> "2023", "month" -> "02"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "year=2024/month=01/file4.split",
        Map("year" -> "2024", "month" -> "01"),
        1000L,
        System.currentTimeMillis(),
        true
      )
    )

    val partitionColumns = Seq("year", "month")
    val stats            = PartitionPruning.getPartitionStatistics(addActions, partitionColumns)

    assert(stats("partitioned") == true)
    assert(stats("totalFiles") == 4)
    assert(stats("uniquePartitions") == 3) // (2023,01), (2023,02), (2024,01)

    val distribution = stats("partitionDistribution").asInstanceOf[Map[Map[String, String], Int]]
    assert(distribution(Map("year" -> "2023", "month" -> "01")) == 2) // 2 files
    assert(distribution(Map("year" -> "2023", "month" -> "02")) == 1) // 1 file
    assert(distribution(Map("year" -> "2024", "month" -> "01")) == 1) // 1 file
  }

  test("should handle partition path creation") {
    val partitionValues  = Map("year" -> "2023", "month" -> "01", "day" -> "15")
    val partitionColumns = Seq("year", "month", "day")

    val path = PartitionUtils.createPartitionPath(partitionValues, partitionColumns)
    assert(path == "year=2023/month=01/day=15")
  }

  test("should escape special characters in partition values") {
    val partitionValues  = Map("category" -> "tech/news", "region" -> "US West")
    val partitionColumns = Seq("category", "region")

    val path = PartitionUtils.createPartitionPath(partitionValues, partitionColumns)
    assert(path == "category=tech%2Fnews/region=US%20West")
  }

  test("should validate supported partition types") {
    // Supported types
    assert(PartitionUtils.isSupportedPartitionType(StringType))
    assert(PartitionUtils.isSupportedPartitionType(IntegerType))
    assert(PartitionUtils.isSupportedPartitionType(LongType))
    assert(PartitionUtils.isSupportedPartitionType(BooleanType))
    assert(PartitionUtils.isSupportedPartitionType(DateType))
    assert(PartitionUtils.isSupportedPartitionType(TimestampType))
    assert(PartitionUtils.isSupportedPartitionType(DecimalType(10, 2)))

    // Unsupported types
    assert(!PartitionUtils.isSupportedPartitionType(ArrayType(StringType)))
    assert(!PartitionUtils.isSupportedPartitionType(MapType(StringType, StringType)))
    assert(!PartitionUtils.isSupportedPartitionType(StructType(Seq(StructField("x", IntegerType)))))
  }
}
