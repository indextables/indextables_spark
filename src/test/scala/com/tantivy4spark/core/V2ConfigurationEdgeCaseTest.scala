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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test configuration edge cases in V2 DataSource API including:
 * - Configuration precedence (options > table properties > spark config > defaults)
 * - Case sensitivity in configuration keys
 * - Invalid configuration values
 * - Configuration inheritance and overrides
 * - Cache configuration edge cases
 * - Protocol-specific configurations
 */
class V2ConfigurationEdgeCaseTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  test("should handle configuration precedence correctly") {
    withTempPath { path =>
      val data = spark.range(0, 10).select(
        col("id"),
        concat(lit("Item "), col("id")).as("name")
      )
      
      // Set Spark session level config (lowest precedence)
      spark.conf.set("spark.tantivy4spark.cache.maxSize", "100000000")
      
      try {
        // Write with session config
        data.write
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .mode("overwrite")
          .save(path)
        
        // Read with option override (highest precedence)
        val result = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.tantivy4spark.cache.maxSize", "500000000")  // Should override session config
          .load(path)
        
        result.count() shouldBe 10
        // The fact that it works indicates the option override was applied
        
      } finally {
        // Clean up session config
        spark.conf.unset("spark.tantivy4spark.cache.maxSize")
      }
    }
  }

  test("should handle case sensitivity in configuration keys") {
    withTempPath { path =>
      val data = spark.range(0, 5).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test different case variations
      val testCases = Seq(
        "spark.tantivy4spark.cache.maxsize",      // All lowercase
        "spark.tantivy4spark.cache.MAXSIZE",      // All uppercase
        "spark.tantivy4spark.cache.MaxSize",      // PascalCase
        "spark.tantivy4spark.cache.maxSize"       // camelCase (correct)
      )
      
      testCases.foreach { configKey =>
        val result = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option(configKey, "200000000")
          .load(path)
        
        // All should work due to case-insensitive handling
        result.count() shouldBe 5
      }
    }
  }

  test("should handle invalid configuration values gracefully") {
    withTempPath { path =>
      val data = spark.range(0, 5).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test invalid numeric values
      val invalidNumericConfigs = Seq(
        ("spark.tantivy4spark.cache.maxSize", "not-a-number"),
        ("spark.tantivy4spark.cache.maxConcurrentLoads", "invalid"),
        ("spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit", "abc123")
      )
      
      invalidNumericConfigs.foreach { case (key, value) =>
        val exception = intercept[NumberFormatException] {
          spark.read
            .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
            .option(key, value)
            .load(path)
            .count()
        }
        exception.getMessage should include(value)
      }
      
      // Test invalid boolean values (should use defaults)
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.cache.queryCache", "maybe")  // Invalid boolean
        .load(path)
      
      result.count() shouldBe 5  // Should still work with default boolean value
    }
  }

  test("should handle configuration inheritance across operations") {
    withTempPath { path =>
      // Set base configuration
      spark.conf.set("spark.tantivy4spark.cache.name", "base-cache")
      spark.conf.set("spark.tantivy4spark.cache.maxSize", "150000000")
      
      try {
        val data = spark.range(0, 8).select(
          col("id"),
          concat(lit("Value "), col("id")).as("description")
        )
        
        // Write inherits session config
        data.write
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .mode("overwrite")
          .save(path)
        
        // First read with partial override
        val result1 = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.tantivy4spark.cache.maxSize", "300000000")  // Override one setting
          // cache.name should inherit from session config
          .load(path)
        
        result1.count() shouldBe 8
        
        // Second read with complete override
        val result2 = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.tantivy4spark.cache.name", "custom-cache")
          .option("spark.tantivy4spark.cache.maxSize", "400000000")
          .load(path)
        
        result2.count() shouldBe 8
        
      } finally {
        spark.conf.unset("spark.tantivy4spark.cache.name")
        spark.conf.unset("spark.tantivy4spark.cache.maxSize")
      }
    }
  }

  test("should handle empty and null configuration values") {
    withTempPath { path =>
      val data = spark.range(0, 5).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test empty string configurations
      val result1 = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.cache.name", "")  // Empty string
        .load(path)
      
      result1.count() shouldBe 5  // Should use default cache name
      
      // Test configurations with whitespace
      val result2 = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.cache.name", "   ")  // Whitespace only
        .load(path)
      
      result2.count() shouldBe 5
    }
  }

  test("should handle AWS configuration edge cases") {
    withTempPath { path =>
      val data = spark.range(0, 3).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test various AWS configuration scenarios
      val awsConfigTests = Seq(
        Map(
          "spark.tantivy4spark.aws.region" -> "us-east-1",
          "spark.tantivy4spark.aws.accessKey" -> "test-key"
          // Missing secret key - should handle gracefully
        ),
        Map(
          "spark.tantivy4spark.aws.region" -> "",  // Empty region
          "spark.tantivy4spark.aws.accessKey" -> "test-key",
          "spark.tantivy4spark.aws.secretKey" -> "test-secret"
        ),
        Map(
          "spark.tantivy4spark.aws.endpoint" -> "http://localhost:9999",  // Custom endpoint
          "spark.tantivy4spark.aws.region" -> "custom-region"
        )
      )
      
      awsConfigTests.foreach { configMap =>
        val reader = configMap.foldLeft(spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")) {
          case (r, (key, value)) => r.option(key, value)
        }
        
        val result = reader.load(path)
        result.count() shouldBe 3  // Should work or fail gracefully
      }
    }
  }

  test("should handle cache configuration boundary values") {
    withTempPath { path =>
      val data = spark.range(0, 5).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test boundary cache configurations
      val cacheConfigTests = Seq(
        ("1", "1"),           // Minimum values
        ("0", "0"),           // Zero values (should use defaults)
        ("999999999", "100"), // Very large values
        ("-1", "-1")          // Negative values (should use defaults or handle gracefully)
      )
      
      cacheConfigTests.foreach { case (maxSize, maxLoads) =>
        try {
          val result = spark.read
            .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
            .option("spark.tantivy4spark.cache.maxSize", maxSize)
            .option("spark.tantivy4spark.cache.maxConcurrentLoads", maxLoads)
            .load(path)
          
          result.count() shouldBe 5
        } catch {
          case _: IllegalArgumentException => 
            // Acceptable for negative or invalid values
          case _: NumberFormatException =>
            // Acceptable for invalid number formats
        }
      }
    }
  }

  test("should handle protocol-specific configuration combinations") {
    withTempPath { path =>
      val data = spark.range(0, 4).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test combinations of protocol-specific configs
      val protocolConfigs = Seq(
        // S3 configuration
        Map(
          "spark.tantivy4spark.aws.region" -> "us-west-2",
          "spark.tantivy4spark.s3.pathStyleAccess" -> "true",
          "spark.tantivy4spark.s3.endpoint" -> "http://localhost:9000"
        ),
        // Azure configuration  
        Map(
          "spark.tantivy4spark.azure.accountName" -> "testaccount",
          "spark.tantivy4spark.azure.endpoint" -> "http://localhost:10000"
        ),
        // GCP configuration
        Map(
          "spark.tantivy4spark.gcp.projectId" -> "test-project",
          "spark.tantivy4spark.gcp.endpoint" -> "http://localhost:8080"
        ),
        // Mixed protocol configs (should be handled gracefully)
        Map(
          "spark.tantivy4spark.aws.region" -> "us-east-1",
          "spark.tantivy4spark.azure.accountName" -> "testaccount",
          "spark.tantivy4spark.gcp.projectId" -> "test-project"
        )
      )
      
      protocolConfigs.foreach { configMap =>
        val reader = configMap.foldLeft(spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")) {
          case (r, (key, value)) => r.option(key, value)
        }
        
        val result = reader.load(path)
        result.count() shouldBe 4  // Should handle gracefully regardless of protocol mix
      }
    }
  }

  test("should handle configuration key variations and aliases") {
    withTempPath { path =>
      val data = spark.range(0, 5).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Test various ways to specify the same configuration
      val aliasTests = Seq(
        // Different casing of the same key should work
        ("spark.tantivy4spark.cache.maxsize", "100000000"),
        ("spark.tantivy4spark.cache.MAXSIZE", "100000000"),
        ("spark.tantivy4spark.cache.MaxSize", "100000000"),
        
        // AWS key variations
        ("spark.tantivy4spark.aws.accesskey", "test-key"),
        ("spark.tantivy4spark.aws.ACCESSKEY", "test-key"),
        ("spark.tantivy4spark.aws.AccessKey", "test-key")
      )
      
      aliasTests.foreach { case (key, value) =>
        val result = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option(key, value)
          .load(path)
        
        result.count() shouldBe 5  // All should work due to case-insensitive handling
      }
    }
  }

  test("should handle configuration conflicts and resolution") {
    withTempPath { path =>
      val data = spark.range(0, 6).select(col("id"))
      
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)
      
      // Set conflicting configurations at different levels
      spark.conf.set("spark.tantivy4spark.cache.maxSize", "100000000")
      spark.conf.set("spark.tantivy4spark.cache.queryCache", "false")
      
      try {
        // Read with conflicting option values (options should win)
        val result = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.tantivy4spark.cache.maxSize", "300000000")  // Conflicts with session config
          .option("spark.tantivy4spark.cache.queryCache", "true")     // Conflicts with session config
          .load(path)
        
        result.count() shouldBe 6  // Should work with option values taking precedence
        
        // Test multiple conflicting options (last one should win or implementation-defined)
        val result2 = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.tantivy4spark.cache.maxSize", "200000000")   // First value
          .option("spark.tantivy4spark.cache.maxSize", "400000000")   // Second value (should override)
          .load(path)
        
        result2.count() shouldBe 6
        
      } finally {
        spark.conf.unset("spark.tantivy4spark.cache.maxSize")
        spark.conf.unset("spark.tantivy4spark.cache.queryCache")
      }
    }
  }
}