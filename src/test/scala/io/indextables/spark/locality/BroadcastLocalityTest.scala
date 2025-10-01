package io.indextables.spark.locality

import io.indextables.spark.TestBase
import io.indextables.spark.storage.BroadcastSplitLocalityManager
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

class BroadcastLocalityTest extends TestBase with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // Clear locality tracking between tests
    BroadcastSplitLocalityManager.clearAll()
    super.afterEach()
  }

  test("BroadcastSplitLocalityManager should track and broadcast split locality") {
    println("🧪 [TEST] Starting broadcast locality test")

    val splitPath1 = "s3://test-bucket/dataset/split1.split"
    val splitPath2 = "s3://test-bucket/dataset/split2.split"
    val host1      = "spark-worker-1"
    val host2      = "spark-worker-2"

    // Initially, no broadcast info should be available
    val initialHosts1 = BroadcastSplitLocalityManager.getPreferredHosts(splitPath1)
    val initialHosts2 = BroadcastSplitLocalityManager.getPreferredHosts(splitPath2)
    assert(initialHosts1.isEmpty)
    assert(initialHosts2.isEmpty)

    // Record some split accesses
    println("🧪 [TEST] Recording split accesses...")
    BroadcastSplitLocalityManager.recordSplitAccess(splitPath1, host1)
    BroadcastSplitLocalityManager.recordSplitAccess(splitPath1, host2)
    BroadcastSplitLocalityManager.recordSplitAccess(splitPath2, host1)

    // Check stats before broadcast
    val statsBeforeBroadcast = BroadcastSplitLocalityManager.getLocalityStats()

    // Force a broadcast update
    println("🧪 [TEST] Forcing broadcast update...")
    BroadcastSplitLocalityManager.forceBroadcastUpdate(spark.sparkContext)

    // Check stats after broadcast
    val statsAfterBroadcast = BroadcastSplitLocalityManager.getLocalityStats()

    // Now check that broadcast info is available
    val broadcastHosts1 = BroadcastSplitLocalityManager.getPreferredHosts(splitPath1)
    val broadcastHosts2 = BroadcastSplitLocalityManager.getPreferredHosts(splitPath2)

    println(s"🧪 [TEST] Split1 hosts: ${broadcastHosts1.mkString(", ")}")
    println(s"🧪 [TEST] Split2 hosts: ${broadcastHosts2.mkString(", ")}")

    // Verify the results
    assert(broadcastHosts1.length == 2)
    assert(broadcastHosts1.contains(host1))
    assert(broadcastHosts1.contains(host2))

    assert(broadcastHosts2.length == 1)
    assert(broadcastHosts2.contains(host1))

    println("🧪 [TEST] Broadcast locality test completed successfully!")
  }

  test("should demonstrate end-to-end locality tracking during read operations") {
    println("🧪 [TEST] Starting end-to-end locality test")

    withTempPath { tempDir =>
      val schema = StructType(
        Array(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false)
        )
      )

      val data = Seq(
        Row(1, "Alice"),
        Row(2, "Bob"),
        Row(3, "Charlie")
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data using Tantivy4Spark
      val tablePath = tempDir.toString
      println(s"🧪 [TEST] Writing data to: $tablePath")
      df.write.format("tantivy4spark").save(tablePath)

      // Force a broadcast update to collect any locality information
      println("🧪 [TEST] Forcing broadcast update before read...")
      BroadcastSplitLocalityManager.forceBroadcastUpdate(spark.sparkContext)

      // Read data back using V2 API - this should trigger locality tracking and preferredLocations calls
      println("🧪 [TEST] Reading data back using V2 DataSource API...")
      val readDf  = spark.read.format("io.indextables.spark.core.Tantivy4SparkTableProvider").load(tablePath)
      val results = readDf.collect()

      // Verify data was read correctly
      assert(results.length == 3)
      assert(results.map(_.getAs[String]("name")).sorted.deep == Array("Alice", "Bob", "Charlie").deep)

      // Force another broadcast update to see if locality was recorded during read
      println("🧪 [TEST] Forcing broadcast update after read...")
      BroadcastSplitLocalityManager.forceBroadcastUpdate(spark.sparkContext)

      // Get final stats
      val finalStats = BroadcastSplitLocalityManager.getLocalityStats()

      println("🧪 [TEST] End-to-end locality test completed!")
    }
  }
}
