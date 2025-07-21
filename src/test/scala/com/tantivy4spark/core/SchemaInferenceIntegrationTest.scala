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

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import com.tantivy4spark.transaction.TransactionLog
import java.nio.file.{Files, Paths}

class SchemaInferenceIntegrationTest extends AnyFunSuite {
  
  test("should persist schema in transaction log for new dataset") {
    val tempDir = Files.createTempDirectory("tantivy-schema-test").toString
    val options = Map[String, String]()
    
    val originalSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("timestamp", TimestampType, nullable = false)
    ))
    
    try {
      // Create transaction log and append schema
      val txnLog = new TransactionLog(tempDir, options)
      txnLog.appendSchemaEntry(originalSchema)
      txnLog.commit()
      
      // Try to infer schema from transaction log
      val newTxnLog = new TransactionLog(tempDir, options)
      val inferredSchema = newTxnLog.inferSchemaFromTransactionLog(tempDir)
      
      assert(inferredSchema.isDefined)
      assert(inferredSchema.get.fields.length == originalSchema.fields.length)
      assert(inferredSchema.get.fieldNames.toSet == originalSchema.fieldNames.toSet)
      
    } finally {
      // Cleanup
      try {
        Files.walk(Paths.get(tempDir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
  
  test("should validate schema compatibility for writes") {
    val tempDir = Files.createTempDirectory("tantivy-schema-test").toString
    val options = Map[String, String]()
    
    val originalSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    
    try {
      // Create initial transaction log with schema
      val txnLog = new TransactionLog(tempDir, options)
      txnLog.appendSchemaEntry(originalSchema)
      txnLog.commit()
      
      // Test compatible schema (adds nullable field)
      val compatibleSchema = StructType(Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("email", StringType, nullable = true) // New nullable field
      ))
      
      val compatibleResult = SchemaCompatibilityValidator.validateSchemaForWrite(tempDir, compatibleSchema, options)
      assert(compatibleResult.isCompatible)
      assert(compatibleResult.warnings.nonEmpty)
      
      // Test incompatible schema (removes field)
      val incompatibleSchema = StructType(Array(
        StructField("id", LongType, nullable = false)
        // Missing 'name' field
      ))
      
      val incompatibleResult = SchemaCompatibilityValidator.validateSchemaForWrite(tempDir, incompatibleSchema, options)
      // Should still be compatible with default settings (non-strict mode)
      assert(incompatibleResult.isCompatible)
      assert(incompatibleResult.warnings.exists(_.contains("removed")))
      
    } finally {
      // Cleanup
      try {
        Files.walk(Paths.get(tempDir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
  
  test("should enforce strict schema validation when configured") {
    val tempDir = Files.createTempDirectory("tantivy-schema-test").toString
    val strictOptions = Map(
      "schema.compatibility.strict" -> "true"
    )
    
    val originalSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    
    try {
      // Create initial transaction log with schema
      val txnLog = new TransactionLog(tempDir, strictOptions)
      txnLog.appendSchemaEntry(originalSchema)
      txnLog.commit()
      
      // Test schema with removed field in strict mode
      val incompatibleSchema = StructType(Array(
        StructField("id", LongType, nullable = false)
        // Missing 'name' field
      ))
      
      val result = SchemaCompatibilityValidator.validateSchemaForWrite(tempDir, incompatibleSchema, strictOptions)
      assert(!result.isCompatible)
      assert(result.errors.nonEmpty)
      assert(result.errors.exists(_.contains("remove")))
      
    } finally {
      // Cleanup
      try {
        Files.walk(Paths.get(tempDir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
  
  test("should handle type promotions correctly") {
    val originalSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("score", FloatType, nullable = true)
    ))
    
    val promotedSchema = StructType(Array(
      StructField("id", LongType, nullable = false), // Int -> Long promotion
      StructField("score", DoubleType, nullable = true) // Float -> Double promotion
    ))
    
    val result = SchemaCompatibilityValidator.validateSchemaCompatibility(originalSchema, promotedSchema, false, true)
    assert(result.isCompatible)
    assert(result.warnings.exists(_.contains("Safe type promotion")))
  }
  
  test("should reject unsafe type changes") {
    val originalSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    
    val incompatibleSchema = StructType(Array(
      StructField("id", StringType, nullable = false), // Long -> String is not safe
      StructField("name", IntegerType, nullable = true) // String -> Int is not safe
    ))
    
    val result = SchemaCompatibilityValidator.validateSchemaCompatibility(originalSchema, incompatibleSchema, false, true)
    assert(!result.isCompatible)
    assert(result.errors.length >= 2)
  }
  
  test("should handle nullable to non-nullable field changes") {
    val originalSchema = StructType(Array(
      StructField("id", LongType, nullable = true)
    ))
    
    val nonNullableSchema = StructType(Array(
      StructField("id", LongType, nullable = false) // nullable -> non-nullable
    ))
    
    val result = SchemaCompatibilityValidator.validateSchemaCompatibility(originalSchema, nonNullableSchema, false, true)
    assert(!result.isCompatible)
    assert(result.errors.exists(_.contains("nullable to non-nullable")))
  }
}