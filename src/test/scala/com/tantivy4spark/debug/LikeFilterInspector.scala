package com.tantivy4spark.debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

object LikeFilterInspector {
  def main(args: Array[String]): Unit = {
    System.setProperty("user.name", System.getProperty("user.name", "testuser"))
    
    val spark = SparkSession.builder()
      .appName("LikeFilterInspector")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    
    import spark.implicits._
    
    // Create sample data
    val df = Seq(
      ("apple dog", 1),
      ("banana cat", 2),
      ("dog food", 3)
    ).toDF("text", "id")
    
    df.createOrReplaceTempView("test_table")
    
    // Test LIKE query
    val likeQuery = spark.sql("SELECT * FROM test_table WHERE text LIKE '%dog%'")
    
    println("\n=== SQL LIKE Query Plan ===")
    likeQuery.explain(true)
    
    println("\n=== Logical Plan ===")
    println(likeQuery.queryExecution.logical)
    
    println("\n=== Analyzed Plan ===")
    println(likeQuery.queryExecution.analyzed)
    
    println("\n=== Optimized Plan ===")
    println(likeQuery.queryExecution.optimizedPlan)
    
    // Compare with DataFrame contains
    val containsQuery = df.filter($"text".contains("dog"))
    
    println("\n=== DataFrame contains() Query Plan ===")
    containsQuery.explain(true)
    
    println("\n=== Contains Logical Plan ===")
    println(containsQuery.queryExecution.logical)
    
    spark.stop()
  }
}