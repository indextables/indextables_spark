package io.indextables.spark.core

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for GROUP BY aggregate pushdown functionality. Tests the core GROUP BY components without requiring Spark
 * aggregation objects.
 */
class GroupByAggregatePushdownTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("GroupByAggregatePushdownTest")
      .master("local[2]")
      .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("GROUP BY scan components should be creatable") {
    val schema = StructType(
      Seq(
        StructField("category", StringType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("count", IntegerType, nullable = false)
      )
    )

    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.indexing.fastfields" -> "count"
      ).asJava
    )

    val transactionLog  = createMockTransactionLog()
    val broadcastConfig = spark.sparkContext.broadcast(Map[String, String]())

    // Test that we can create the scan builder
    val scanBuilder = new IndexTables4SparkScanBuilder(
      spark,
      transactionLog,
      schema,
      options,
      broadcastConfig.value
    )

    // Test scan builder creation
    assert(scanBuilder != null, "ScanBuilder should be created successfully")

    // Test that pushAggregation method exists and is callable
    // We can't test with real aggregations due to final classes, but we can test the structure
    val pushAggregationMethod = scanBuilder.getClass.getDeclaredMethod(
      "pushAggregation",
      classOf[org.apache.spark.sql.connector.expressions.aggregate.Aggregation]
    )
    assert(pushAggregationMethod != null, "pushAggregation method should exist")

    // Test that the method is public
    assert(java.lang.reflect.Modifier.isPublic(pushAggregationMethod.getModifiers), "pushAggregation should be public")

    // Test that supportCompletePushDown method exists
    val supportMethod = scanBuilder.getClass.getDeclaredMethod(
      "supportCompletePushDown",
      classOf[org.apache.spark.sql.connector.expressions.aggregate.Aggregation]
    )
    assert(supportMethod != null, "supportCompletePushDown method should exist")
  }

  test("GROUP BY options validation helper should exist") {
    val schema = StructType(
      Seq(
        StructField("category", StringType, nullable = false),
        StructField("count", IntegerType, nullable = false)
      )
    )

    val options         = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val transactionLog  = createMockTransactionLog()
    val broadcastConfig = spark.sparkContext.broadcast(Map[String, String]())

    val _scanBuilder = new IndexTables4SparkScanBuilder(
      spark,
      transactionLog,
      schema,
      options,
      broadcastConfig.value
    )

    // Verify that the class has the expected IndexTables4SparkOptions integration
    val optionsClass = Class.forName("io.indextables.spark.core.IndexTables4SparkOptions")
    assert(optionsClass != null, "IndexTables4SparkOptions class should be available")

    // Verify we can create options instance
    val tantivyOptions = optionsClass.getConstructor(classOf[CaseInsensitiveStringMap]).newInstance(options)
    assert(tantivyOptions != null, "Should be able to create IndexTables4SparkOptions instance")
  }

  // Helper methods for creating mock objects

  private def createMockTransactionLog(): TransactionLog =
    // Create a mock transaction log that returns empty file list using factory
    TransactionLogFactory.create(new Path("/mock/path"), spark)
}
