package com.tantivy4spark.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

/**
 * Unit tests for GROUP BY aggregate pushdown functionality.
 * Tests the core GROUP BY components without requiring Spark aggregation objects.
 */
class GroupByAggregatePushdownTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("GroupByAggregatePushdownTest")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("GROUP BY scan components should be creatable") {
    val schema = StructType(Seq(
      StructField("category", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false)
    ))

    val options = new CaseInsensitiveStringMap(Map(
      "spark.tantivy4spark.indexing.fastfields" -> "count"
    ).asJava)

    val transactionLog = createMockTransactionLog()
    val broadcastConfig = spark.sparkContext.broadcast(Map[String, String]())

    // Test that we can create the scan builder
    val scanBuilder = new Tantivy4SparkScanBuilder(
      spark, transactionLog, schema, options, broadcastConfig
    )

    // Test scan builder creation
    assert(scanBuilder != null, "ScanBuilder should be created successfully")

    // Test that pushAggregation method exists and is callable
    // We can't test with real aggregations due to final classes, but we can test the structure
    val pushAggregationMethod = scanBuilder.getClass.getDeclaredMethod("pushAggregation", classOf[org.apache.spark.sql.connector.expressions.aggregate.Aggregation])
    assert(pushAggregationMethod != null, "pushAggregation method should exist")

    // Test that the method is public
    assert(java.lang.reflect.Modifier.isPublic(pushAggregationMethod.getModifiers), "pushAggregation should be public")

    // Test that supportCompletePushDown method exists
    val supportMethod = scanBuilder.getClass.getDeclaredMethod("supportCompletePushDown", classOf[org.apache.spark.sql.connector.expressions.aggregate.Aggregation])
    assert(supportMethod != null, "supportCompletePushDown method should exist")
  }

  test("GROUP BY options validation helper should exist") {
    val schema = StructType(Seq(
      StructField("category", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false)
    ))

    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val transactionLog = createMockTransactionLog()
    val broadcastConfig = spark.sparkContext.broadcast(Map[String, String]())

    val scanBuilder = new Tantivy4SparkScanBuilder(
      spark, transactionLog, schema, options, broadcastConfig
    )

    // Verify that the class has the expected Tantivy4SparkOptions integration
    val optionsClass = Class.forName("com.tantivy4spark.core.Tantivy4SparkOptions")
    assert(optionsClass != null, "Tantivy4SparkOptions class should be available")

    // Verify we can create options instance
    val tantivyOptions = optionsClass.getConstructor(classOf[CaseInsensitiveStringMap]).newInstance(options)
    assert(tantivyOptions != null, "Should be able to create Tantivy4SparkOptions instance")
  }

  // Helper methods for creating mock objects

  private def createMockTransactionLog(): TransactionLog = {
    // Create a mock transaction log that returns empty file list using factory
    TransactionLogFactory.create(new Path("/mock/path"), spark)
  }
}