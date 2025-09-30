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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

class LimitPushdownTest extends TestBase {

  test("should support SQL LIMIT statement pushdown") {
    withTempPath { tempPath =>
      // Create test data with more records than typical limit
      val data = spark.range(100).select(
        col("id"),
        (col("id") % 10).cast("string").as("category"),
        (col("id") * 2).as("value")
      )
      
      // Write data
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Create temporary view for SQL queries
      spark.read
        .format("tantivy4spark")
        .load(tempPath)
        .createOrReplaceTempView("test_table")

      // Test SQL LIMIT pushdown
      val sqlResult = spark.sql("SELECT * FROM test_table LIMIT 5")
      val collectedResults = sqlResult.collect()
      
      // Should return exactly 5 rows due to LIMIT pushdown
      collectedResults.length shouldBe 5
      
      println(s"✅ SQL LIMIT 5 returned ${collectedResults.length} rows")
    }
  }

  test("should support DataFrame limit() method pushdown") {
    withTempPath { tempPath =>
      val data = spark.range(50).select(
        col("id"),
        (col("id") % 5).cast("string").as("group"),
        (col("id") + 100).as("score")
      )
      
      // Write data
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Read with DataFrame limit
      val df = spark.read
        .format("tantivy4spark")
        .load(tempPath)
        
      val limitedResult = df.limit(3).collect()
      
      // Should return exactly 3 rows due to limit pushdown
      limitedResult.length shouldBe 3
      
      println(s"✅ DataFrame limit(3) returned ${limitedResult.length} rows")
    }
  }

  test("should handle queries without limits using Int.MaxValue") {
    withTempPath { tempPath =>
      val data = spark.range(10).select(
        col("id"),
        (col("id") * 3).as("triple")
      )
      
      // Write data
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Read without any limit
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)
        .collect()
      
      // Should return all 10 rows (no artificial limit)
      result.length shouldBe 10
      
      println(s"✅ Query without limit returned ${result.length} rows (expected 10)")
    }
  }

  test("should push down limit with filters") {
    withTempPath { tempPath =>
      val data = spark.range(30).select(
        col("id"),
        (col("id") % 3).cast("string").as("mod3"),
        (col("id") * 10).as("times10")
      )
      
      // Write data
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Create temporary view
      spark.read
        .format("tantivy4spark")
        .load(tempPath)
        .createOrReplaceTempView("test_filter_table")

      // Test LIMIT with WHERE clause
      val sqlResult = spark.sql("""
        SELECT * FROM test_filter_table 
        WHERE mod3 = '1' 
        LIMIT 2
      """)
      
      val results = sqlResult.collect()
      
      // Should return exactly 2 rows that match the filter
      results.length shouldBe 2
      results.foreach { row =>
        row.getString(row.fieldIndex("mod3")) shouldBe "1"
      }
      
      println(s"✅ SQL LIMIT 2 with filter returned ${results.length} rows")
    }
  }

  test("should validate ScanBuilder limit pushdown implementation") {
    withTempPath { tempPath =>
      // Write some test data
      val data = spark.range(20).toDF("id")
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Read the transaction log to get the schema
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      import org.apache.spark.sql.types.DataType
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val metadata = transactionLog.getMetadata()
      val schema = DataType.fromJson(metadata.schemaString).asInstanceOf[org.apache.spark.sql.types.StructType]
      
      // Create options map
      import org.apache.spark.sql.util.CaseInsensitiveStringMap
      import scala.jdk.CollectionConverters._
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      
      // Test ScanBuilder limit pushdown methods
      val emptyBroadcastConfig = spark.sparkContext.broadcast(Map.empty[String, String])
      val scanBuilder = new Tantivy4SparkScanBuilder(spark, transactionLog, schema, options, emptyBroadcastConfig.value)
      
      // Push a limit - should return true indicating support
      val limitPushed = scanBuilder.pushLimit(10)
      limitPushed shouldBe true
      
      println("✅ ScanBuilder limit pushdown methods work correctly")
    }
  }
}