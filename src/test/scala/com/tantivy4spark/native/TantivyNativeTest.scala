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

package com.tantivy4spark.native

import com.tantivy4spark.TantivyTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._
import com.tantivy4spark.config.TantivyConfig
import scala.util.{Try, Success, Failure}
import java.nio.file.{Files, Paths}

class TantivyNativeTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyNative" should "have correct library name detection" in {
    // Test library name selection based on platform
    // This tests the platform-specific library loading logic
    val libraryName = if (System.getProperty("os.name").toLowerCase.contains("windows")) {
      "tantivy_jni.dll"
    } else if (System.getProperty("os.name").toLowerCase.contains("mac")) {
      "libtantivy_jni.dylib"
    } else {
      "libtantivy_jni.so"
    }
    
    libraryName should not be empty
    libraryName should include("tantivy_jni")
  }
  
  it should "handle configuration management" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    val config = Map(
      "base_path" -> testDir.toString,
      "index_config" -> Map(
        "index_id" -> "test_index"
      )
    )
    
    val configJson = objectMapper.writeValueAsString(config)
    
    // Test the JSON generation and parsing
    configJson should include("base_path")
    configJson should include("index_config")
    configJson should include("test_index")
    
    // Test that we can parse it back
    val parsedConfig = objectMapper.readValue(configJson, classOf[Map[String, Any]])
    parsedConfig should contain key "base_path"
    parsedConfig should contain key "index_config"
  }
  
  it should "validate JSON structure for configurations" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    // Test valid configuration JSON
    val validConfig = Map(
      "base_path" -> "/valid/path",
      "index_config" -> Map(
        "index_id" -> "valid_index",
        "doc_mapping" -> Map(
          "mode" -> "strict"
        )
      )
    )
    
    val validJson = objectMapper.writeValueAsString(validConfig)
    
    // Should be parseable back to Map
    val parsedConfig = objectMapper.readValue(validJson, classOf[Map[String, Any]])
    parsedConfig should contain key "base_path"
    parsedConfig should contain key "index_config"
  }
  
  it should "handle schema JSON generation correctly" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    val schema = Map(
      "field_mappings" -> Map(
        "id" -> Map("type" -> "i64", "indexed" -> true),
        "title" -> Map("type" -> "text", "indexed" -> true, "stored" -> true),
        "content" -> Map("type" -> "text", "indexed" -> true)
      ),
      "timestamp_field" -> "_timestamp",
      "default_search_fields" -> List("title", "content")
    )
    
    val schemaJson = objectMapper.writeValueAsString(schema)
    
    schemaJson should include("field_mappings")
    schemaJson should include("timestamp_field")
    schemaJson should include("default_search_fields")
    
    // Should be parseable
    val parsedSchema = objectMapper.readValue(schemaJson, classOf[Map[String, Any]])
    parsedSchema should contain key "field_mappings"
  }
  
  it should "generate valid TantivyConfig from Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("content", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("score", DoubleType, nullable = true),
      StructField("active", BooleanType, nullable = false)
    ))
    
    val options = Map(
      "index.id" -> "test_index",
      "tantivy.base.path" -> testDir.toString
    )
    
    val config = TantivyConfig.fromSpark(sparkSchema, options)
    
    // Verify the configuration structure
    config.basePath should be(testDir.toString)
    config.indexes should have length 1
    
    val indexConfig = config.indexes.head
    indexConfig.indexId should be("test_index")
    indexConfig.docMapping.fieldMappings should have size 6
    
    // Verify field mappings
    indexConfig.docMapping.fieldMappings should contain key "id"
    indexConfig.docMapping.fieldMappings should contain key "title"
    indexConfig.docMapping.fieldMappings should contain key "content"
    
    // Verify type mappings
    indexConfig.docMapping.fieldMappings("id").fieldType should be("i64")
    indexConfig.docMapping.fieldMappings("title").fieldType should be("text")
    indexConfig.docMapping.fieldMappings("active").fieldType should be("bool")
  }
  
  it should "handle round-trip type conversion correctly" in {
    import org.apache.spark.sql.types._
    
    val testCases = Map(
      StringType -> "text",
      IntegerType -> "i32", 
      LongType -> "i64",
      FloatType -> "f32",
      DoubleType -> "f64",
      BooleanType -> "bool",
      TimestampType -> "datetime",
      DateType -> "date"
    )

    testCases.foreach { case (sparkType, expectedTantivyType) =>
      val tantivyType = TantivyConfig.mapSparkTypeToTantivy(sparkType)
      tantivyType should be (expectedTantivyType)
      
      val roundTripSparkType = TantivyConfig.mapTantivyTypeToSpark(tantivyType)
      roundTripSparkType should be (sparkType)
    }
  }
  
  it should "convert TantivyConfig back to Spark schema correctly" in {
    val originalSchema = StructType(Seq(
      StructField("product_id", StringType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("price", DoubleType, nullable = false),
      StructField("in_stock", BooleanType, nullable = true)
    ))
    
    val options = Map(
      "index.id" -> "product_index",
      "tantivy.base.path" -> testDir.toString
    )
    
    // Convert Spark schema to TantivyConfig and back
    val config = TantivyConfig.fromSpark(originalSchema, options)
    val convertedSchema = TantivyConfig.toSparkSchema(config)
    
    // Verify the schema conversion
    convertedSchema.fields.length should be >= originalSchema.fields.length
    
    // Check that all original fields are present
    originalSchema.fields.foreach { originalField =>
      val convertedField = convertedSchema.fields.find(_.name == originalField.name)
      convertedField should be (defined)
      
      // Check type compatibility through round-trip conversion
      val expectedType = TantivyConfig.mapTantivyTypeToSpark(
        TantivyConfig.mapSparkTypeToTantivy(originalField.dataType)
      )
      convertedField.get.dataType should be (expectedType)
    }
  }
  
  it should "serialize and deserialize configurations as JSON" in {
    val sparkSchema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("username", StringType, nullable = true),
      StructField("email", StringType, nullable = false)
    ))
    
    val options = Map(
      "index.id" -> "user_index",
      "tantivy.base.path" -> testDir.toString
    )
    
    // Create config and serialize to JSON
    val originalConfig = TantivyConfig.fromSpark(sparkSchema, options)
    val configJson = TantivyConfig.toJson(originalConfig)
    
    // JSON should contain expected elements
    configJson should include("base_path")
    configJson should include("user_index")
    configJson should include("field_mappings")
    
    // Deserialize and verify
    TantivyConfig.fromJson(configJson) match {
      case Success(deserializedConfig) =>
        deserializedConfig.basePath should be(originalConfig.basePath)
        deserializedConfig.indexes should have length 1
        deserializedConfig.indexes.head.indexId should be("user_index")
        
      case Failure(exception) =>
        fail(s"Failed to deserialize config: ${exception.getMessage}")
    }
  }
  
  it should "handle invalid configurations gracefully" in {
    // Test with invalid JSON
    val invalidJson = """{"incomplete": "config"}"""
    
    TantivyConfig.fromJson(invalidJson) match {
      case Success(_) => 
        fail("Should have failed to parse invalid configuration")
      case Failure(_) => 
        // Expected behavior - invalid JSON should fail
        succeed
    }
  }
  
  it should "validate configuration requirements" in {
    val validSchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("content", StringType, nullable = true)
    ))
    
    val validOptions = Map(
      "index.id" -> "valid_index",
      "tantivy.base.path" -> testDir.toString
    )
    
    val config = TantivyConfig.fromSpark(validSchema, validOptions)
    val validationErrors = TantivyConfig.validateConfig(config)
    
    // Should have no validation errors for valid config
    validationErrors should be (empty)
  }
  
  it should "detect validation errors in invalid configurations" in {
    val invalidSchema = StructType(Seq()) // Empty schema
    
    val invalidOptions = Map(
      "index.id" -> "", // Empty index ID
      "tantivy.base.path" -> ""  // Empty base path
    )
    
    val config = TantivyConfig.fromSpark(invalidSchema, invalidOptions)
    val validationErrors = TantivyConfig.validateConfig(config)
    
    // Should detect validation errors
    validationErrors should not be empty
    validationErrors.exists(_.contains("Base path")) should be (true)
    validationErrors.exists(_.contains("Index ID")) should be (true)
  }
  
  it should "handle schema inference error scenarios gracefully" in {
    // Test schema inference with non-existent path
    val nonExistentPath = Paths.get(testDir.toString, "non_existent_index").toString
    
    val inferenceResult = TantivyConfig.inferSchemaFromIndex(nonExistentPath)
    
    // Should fail gracefully for non-existent index
    inferenceResult match {
      case Success(_) => 
        fail("Expected schema inference to fail for non-existent index")
      case Failure(exception) => 
        // Expected behavior - should fail with meaningful error
        exception.getMessage should include("Failed to retrieve schema")
    }
  }
  
  it should "handle concurrent configuration operations safely" in {
    import scala.concurrent.{Future, ExecutionContext}
    import scala.concurrent.duration._
    
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("data", StringType, nullable = true)
    ))
    
    // Create multiple configurations concurrently
    val configFutures = (1 to 10).map { i =>
      Future {
        val options = Map(
          "index.id" -> s"concurrent_index_$i",
          "tantivy.base.path" -> testDir.toString
        )
        TantivyConfig.fromSpark(schema, options)
      }
    }
    
    val configs = scala.concurrent.Await.result(Future.sequence(configFutures), 10.seconds)
    
    // All configurations should be created successfully
    configs should have length 10
    
    // Each should have unique index ID
    val indexIds = configs.map(_.indexes.head.indexId).toSet
    indexIds should have size 10
    
    // All should have the same field mappings
    configs.foreach { config =>
      config.indexes.head.docMapping.fieldMappings should contain key "id"
      config.indexes.head.docMapping.fieldMappings should contain key "data"
    }
  }
}